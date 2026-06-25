#!/usr/bin/env bash
# Integration test for the FIFO concurrency gate.
#
# Spawns N concurrent pi print-mode turns against the gated extension with
# UMANS_CONCURRENCY_LIMIT set low, then polls /v1/usage to record the peak
# concurrent_sessions observed on the account. Passes if peak <= limit (the
# gate is serializing) and all turns eventually succeed.
#
# This proves the gate actually throttles real outbound requests end-to-end,
# not just that the code compiles.
#
# Usage:
#   UMANS_API_KEY=uk-... ./test-concurrency-gate.sh [limit] [jobs]
set -u

LIMIT="${1:-2}"
JOBS="${2:-4}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXT="$HERE/index.ts"
PI_BIN=pi

# Resolve API key (env or auth.json — pi does this, but we need it for /v1/usage polling).
API_KEY="${UMANS_API_KEY:-}"
if [ -z "$API_KEY" ]; then
  API_KEY=$(node -e "try{const a=require('/Users/piercarlocadoppi/.pi/agent/auth.json');console.log(a.umans&&a.umans.access||'')}catch(e){}" 2>/dev/null || echo "")
fi
if [ -z "$API_KEY" ]; then
  echo "FATAL: no UMANS_API_KEY and no umans entry in auth.json" >&2
  exit 2
fi

BASE_URL="${UMANS_BASE_URL:-https://api.code.umans.ai}"

usage_snapshot() {
  curl -s --max-time 5 "$BASE_URL/v1/usage" \
    -H "Authorization: Bearer $API_KEY" \
    -H "Accept: application/json" \
    -H "User-Agent: pi-umans-provider/test" \
    | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{try{const j=JSON.parse(s);const c=(j.usage&&j.usage.concurrent_sessions!=null)?j.usage.concurrent_sessions:"?";const l=(j.limits&&j.limits.concurrency&&j.limits.concurrency.limit!=null)?j.limits.concurrency.limit:"?";console.log(c+","+l)}catch(e){console.log("?,?")}})'
}

echo "umans concurrency gate test: limit=$LIMIT, jobs=$JOBS"
echo "baseline concurrent_sessions: $(usage_snapshot)"
echo

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
PEAK_FILE="$TMP/peak"
echo 0 > "$PEAK_FILE"

# Poller: samples concurrent_sessions every 200ms while jobs run.
POLL_PID=""
(
  PEAK=0
  while true; do
    CUR=$(usage_snapshot | cut -d, -f1)
    if [ "$CUR" != "?" ] && [ "$CUR" != "" ]; then
      if [ "$CUR" -gt "$PEAK" ] 2>/dev/null; then PEAK=$CUR; fi
    fi
    echo $PEAK > "$PEAK_FILE"
    sleep 0.1
  done
) &
POLL_PID=$!

# Launch JOBS concurrent pi turns. Each is a real model round-trip. Use
# medium thinking + a longer prompt so each turn runs long enough that the
# N concurrent turns genuinely overlap (and the poller can catch the peak).
echo "launching $JOBS concurrent pi turns..."
START=$(date +%s.%N)
PIDS=()
for i in $(seq 1 "$JOBS"); do
  (
    UMANS_CONCURRENCY_LIMIT="$LIMIT" \
    "$PI_BIN" --no-extensions -e "$EXT" --no-session --no-tools \
      --model umans/umans-flash --thinking medium \
      -p "Count from 1 to 30, one number per line, then on the last line write: JOB$i done" >"$TMP/job$i.out" 2>"$TMP/job$i.err"
  ) &
  PIDS+=($!)
done

# Wait for all jobs.
FAIL=0
for i in "${!PIDS[@]}"; do
  if ! wait "${PIDS[$i]}"; then
    FAIL=$((FAIL+1))
    echo "  job$((i+1)) FAILED (exit $?): $(tail -c 200 "$TMP/job$((i+1)).err" 2>/dev/null)"
  fi
done
END=$(date +%s.%N)
kill "$POLL_PID" 2>/dev/null || true
wait "$POLL_PID" 2>/dev/null || true

PEAK=$(cat "$PEAK_FILE")
SECS=$(awk -v a="$START" -v b="$END" 'BEGIN{printf "%.1f", b-a}')

echo
echo "----------------------------------------"
echo "peak concurrent_sessions observed: $PEAK (limit $LIMIT)"
echo "jobs: $JOBS, failed: $FAIL, wall: ${SECS}s"
echo "----------------------------------------"

# Show one job's output as a sanity check.
echo "sample job1 output: $(head -c 80 "$TMP/job1.out" 2>/dev/null)"
echo

PASS=1
if ! [ "$PEAK" -le "$LIMIT" ] 2>/dev/null; then
  echo "FAIL: peak $PEAK exceeded limit $LIMIT — gate did NOT serialize requests"
  PASS=0
fi
if [ "$FAIL" -gt 0 ]; then
  echo "FAIL: $FAIL/$JOBS jobs failed"
  PASS=0
fi
# COV-MED-1: guard against a vacuous pass. If peak stayed 0 and at least 2 jobs
# ran, the poller never observed a non-zero sample (e.g. all jobs finished
# before the first poll landed, or /usage was unreachable) — peak 0 <= limit
# would pass trivially without proving the gate serialized anything. Require
# either a non-zero peak OR fewer than 2 jobs completed.
if [ "$PEAK" -eq 0 ] 2>/dev/null && [ "$JOBS" -ge 2 ] && [ "$FAIL" -lt "$JOBS" ]; then
  echo "FAIL: peak 0 with $JOBS jobs — vacuous pass (no non-zero sample observed)"
  PASS=0
fi

if [ "$PASS" = 1 ]; then
  echo "PASS: peak concurrent_sessions ($PEAK) stayed within limit ($LIMIT)"
  exit 0
fi
exit 1
