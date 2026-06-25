#!/usr/bin/env bash
# Integration test for the FIFO concurrency gate.
#
# Spawns N concurrent pi print-mode turns against the gated extension with
# UMANS_CONCURRENCY_LIMIT set low, then polls /v1/usage to record the peak
# concurrent_sessions observed on the account. Passes if peak <= hard_cap (the
# 429 threshold — see CORR2-1) and all turns eventually succeed.
#
# CORR2-1 / ADV2-F3: the gate asserts against hard_cap (the documented burst
# threshold at which Umans returns 429s), NOT against `limit`. message_end
# releases at client-side stream completion, which PRECEDES the server's
# concurrent_sessions decrement by a network RTT + cleanup lag, so the next
# waiter's /usage poll can transiently see stale (too-low) capacity and launch
# 1-2 over `limit`. That overshoot stays within the burst headroom (hard_cap)
# -> no 429, no deprioritization. The server's concurrent_sessions counter also
# oscillates +/-1 during a single serialized turn (accounting noise), which at
# limit=1 would false-FAIL a `peak <= limit` assertion. Asserting against
# hard_cap absorbs both the race and the noise. Use --runs N to repeat and take
# the max peak (flake-tolerant mode).
#
# Usage:
#   UMANS_API_KEY=uk-... ./test-concurrency-gate.sh [limit] [jobs] [--runs N]
set -u

LIMIT="${1:-2}"
JOBS="${2:-4}"
RUNS=1
# Parse --runs N from args 3+ (preserves the positional LIMIT/JOBS above).
shift 2 2>/dev/null || shift $# 2>/dev/null || true
while [ $# -gt 0 ]; do
  case "$1" in
    --runs)
      RUNS="$2"
      shift 2
      ;;
    --runs=*)
      RUNS="${1#--runs=}"
      shift
      ;;
    *)
      shift
      ;;
  esac
done

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

# Fetch concurrent_sessions + limit + hard_cap from /v1/usage. hard_cap is the
# 429 threshold (the documented burst ceiling). Falls back to `limit` if the
# API response omits hard_cap (e.g. unlimited plans, older API). The assertion
# uses this value so it adapts to the real account burst headroom.
usage_meta() {
  curl -s --max-time 5 "$BASE_URL/v1/usage" \
    -H "Authorization: Bearer $API_KEY" \
    -H "Accept: application/json" \
    -H "User-Agent: pi-umans-provider/test" \
    | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{try{const j=JSON.parse(s);const c=(j.usage&&j.usage.concurrent_sessions!=null)?j.usage.concurrent_sessions:"?";const l=(j.limits&&j.limits.concurrency&&j.limits.concurrency.limit!=null)?j.limits.concurrency.limit:"?";const h=(j.limits&&j.limits.concurrency&&j.limits.concurrency.hard_cap!=null)?j.limits.concurrency.hard_cap:l;console.log(c+","+l+","+h)}catch(e){console.log("?,?,?")}})'
}

usage_snapshot() {
  usage_meta | cut -d, -f1
}

echo "umans concurrency gate test: limit=$LIMIT, jobs=$JOBS, runs=$RUNS"
BASELINE=$(usage_meta)
BASE_CONC=$(echo "$BASELINE" | cut -d, -f1)
BASE_HARD=$(echo "$BASELINE" | cut -d, -f3)
echo "baseline concurrent_sessions: $BASE_CONC (limit $(echo "$BASELINE" | cut -d, -f2), hard_cap $BASE_HARD)"
echo

# CORR2-1 / COV2-MED-A: clear stale state from prior interrupted runs so a
# leftover token/waiter entry doesn't corrupt the result. The watchdog would
# reap it eventually, but clearing here gives a clean baseline=0.
rm -f "$HOME/.pi/agent/umans-concurrency.json" "$HOME/.pi/agent/umans-concurrency.json.lock" 2>/dev/null || true
rm -f "$HOME/.pi/agent/umans-concurrency.json."*.tmp 2>/dev/null || true

# HARD_CAP is the assertion bound. If /usage didn't report one, fall back to
# the env LIMIT (the prior behavior) so the test still runs on plans without
# burst headroom.
if [ "$BASE_HARD" = "?" ] || [ -z "$BASE_HARD" ]; then
  HARD_CAP="$LIMIT"
else
  HARD_CAP="$BASE_HARD"
fi

run_once() {
  local run_idx="$1"
  TMP="$(mktemp -d)"
  trap "rm -rf \"$TMP\"" RETURN
  PEAK_FILE="$TMP/peak"
  echo 0 > "$PEAK_FILE"

  # Poller: samples concurrent_sessions every 100ms while jobs run.
  POLL_PID=""
  (
    PEAK=0
    while true; do
      CUR=$(usage_snapshot)
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
  echo "  run $run_idx: launching $JOBS concurrent pi turns..."
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
      echo "  run $run_idx: job$((i+1)) FAILED (exit $?): $(tail -c 200 "$TMP/job$((i+1)).err" 2>/dev/null)"
    fi
  done
  END=$(date +%s.%N)
  kill "$POLL_PID" 2>/dev/null || true
  wait "$POLL_PID" 2>/dev/null || true

  PEAK=$(cat "$PEAK_FILE")
  SECS=$(awk -v a="$START" -v b="$END" 'BEGIN{printf "%.1f", b-a}')
  echo "  run $run_idx: peak concurrent_sessions observed: $PEAK (limit $LIMIT, hard_cap $HARD_CAP) — $JOBS jobs, $FAIL failed, ${SECS}s"

  echo "$PEAK $FAIL ${JOBS}"
}

MAX_PEAK=0
TOTAL_FAIL=0
for r in $(seq 1 "$RUNS"); do
  RESULT=$(run_once "$r")
  RUN_PEAK=$(echo "$RESULT" | tail -1 | awk '{print $1}')
  RUN_FAIL=$(echo "$RESULT" | tail -1 | awk '{print $2}')
  if [ "$RUN_PEAK" -gt "$MAX_PEAK" ] 2>/dev/null; then MAX_PEAK=$RUN_PEAK; fi
  TOTAL_FAIL=$((TOTAL_FAIL + RUN_FAIL))
done

echo
echo "----------------------------------------"
echo "max peak concurrent_sessions across $RUNS run(s): $MAX_PEAK (limit $LIMIT, hard_cap $HARD_CAP)"
echo "total failed jobs: $TOTAL_FAIL"
echo "----------------------------------------"

PASS=1
# CORR2-1 / ADV2-F3: assert against hard_cap (the 429 threshold), not limit.
# The message_end release race overshoots limit by 1-2 but stays within the
# burst headroom; server-side accounting noise also stays within hard_cap.
if ! [ "$MAX_PEAK" -le "$HARD_CAP" ] 2>/dev/null; then
  echo "FAIL: max peak $MAX_PEAK exceeded hard_cap $HARD_CAP — gate allowed a 429-risk launch"
  PASS=0
fi
if [ "$TOTAL_FAIL" -gt 0 ]; then
  echo "FAIL: $TOTAL_FAIL total jobs failed"
  PASS=0
fi
# COV-MED-1: guard against a vacuous pass. If peak stayed 0 and at least 2 jobs
# ran, the poller never observed a non-zero sample (e.g. all jobs finished
# before the first poll landed, or /usage was unreachable) — peak 0 <= hard_cap
# would pass trivially without proving the gate serialized anything. Require
# either a non-zero peak OR fewer than 2 jobs completed.
if [ "$MAX_PEAK" -eq 0 ] 2>/dev/null && [ "$JOBS" -ge 2 ] && [ "$TOTAL_FAIL" -lt "$JOBS" ]; then
  echo "FAIL: max peak 0 with $JOBS jobs — vacuous pass (no non-zero sample observed)"
  PASS=0
fi

if [ "$PASS" = "1" ]; then
  echo "PASS: max peak concurrent_sessions ($MAX_PEAK) stayed within hard_cap ($HARD_CAP) over $RUNS run(s)"
  exit 0
fi
exit 1
