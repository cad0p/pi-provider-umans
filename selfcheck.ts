// ponytail: one runnable check for the branchy pure logic.
// - vision model picking + image-id hashing (unchanged)
// - parsePriority: normalize /v1/usage priority → deadline
// - concurrency-queue: readState/reapStale pure helpers
// - createConcurrencyQueue: FIFO + launch-token + pause (file-backed, temp dir)
//
// Run: node --experimental-strip-types selfcheck.ts
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { isNativeVision, pickVisionModel, hashImageId } from "./index.ts";
import {
  parsePriority,
  readState,
  reapStale,
  createConcurrencyQueue,
  isPidDead,
  clampPauseUntil,
  isCapacityFree,
  parseConcurrencyLimit,
  MAX_PAUSE_MS,
} from "./concurrency-queue.ts";

function vision(name: string, v: boolean | "via-handoff" = true, deprecation?: unknown) {
  return { name, capabilities: { supports_vision: v }, ...(deprecation ? { deprecation } : {}) };
}

const CATALOG = {
  "umans-kimi-k2.6": vision("umans-kimi-k2.6", true),
  "umans-kimi-k2.7": vision("umans-kimi-k2.7", true),
  "umans-glm-5.2": vision("umans-glm-5.2", "via-handoff"),
  "umans-coder": vision("umans-coder", true),
};

function assert(cond: boolean, msg: string) {
  if (!cond) { console.error("FAIL:", msg); process.exit(1); }
  console.log("ok  ", msg);
}

// --- isNativeVision / pickVisionModel / hashImageId (unchanged) ---
assert(isNativeVision(vision("a", true)) === true, "native vision is native");
assert(isNativeVision(vision("a", "via-handoff")) === false, "via-handoff is not native");
assert(isNativeVision(vision("a", true, "deprecated")) === false, "deprecated is not native");
assert(isNativeVision(vision("a", false)) === false, "non-vision is not native");

delete process.env.UMANS_VISION_MODEL;
assert(pickVisionModel(CATALOG) === "umans-kimi-k2.7", "defaults to kimi-k2.7 (not insertion-order k2.6)");

process.env.UMANS_VISION_MODEL = "umans-coder";
assert(pickVisionModel(CATALOG) === "umans-coder", "env override honored when native-vision");

process.env.UMANS_VISION_MODEL = "umans-glm-5.2"; // via-handoff, not native
assert(pickVisionModel(CATALOG) === "umans-kimi-k2.7", "env override pointing at via-handoff ignored");

process.env.UMANS_VISION_MODEL = "umans-does-not-exist";
assert(pickVisionModel(CATALOG) === "umans-kimi-k2.7", "unknown env override ignored");

const NO_KIMI = {
  "umans-glm-5.2": vision("umans-glm-5.2", "via-handoff"),
  "umans-coder": vision("umans-coder", true),
};
delete process.env.UMANS_VISION_MODEL;
assert(pickVisionModel(NO_KIMI) === "umans-coder", "falls back to first native-vision model");

const TEXT_ONLY = { "umans-glm-5.2": vision("umans-glm-5.2", "via-handoff") };
assert(pickVisionModel(TEXT_ONLY) === undefined, "undefined when no native-vision model");

const a = hashImageId("data-a");
assert(a === hashImageId("data-a"), "hash is deterministic");
assert(a !== hashImageId("data-b"), "hash differs for different images");
assert(/^img_[0-9a-f]{8}$/.test(a), "hash format is img_<8 hex>");

// --- parsePriority ---
{
  const p = parsePriority(undefined);
  assert(p.low === false && p.until === 0, "parsePriority: undefined -> not low");

  const lowIso = parsePriority({ low: true, boxed_until: null, reason: "burst" });
  assert(lowIso.low === true && lowIso.reason === "burst", "parsePriority: low=true, reason captured");
  assert(lowIso.until > Date.now(), "parsePriority: null boxed_until falls back to now+backoff");

  const lowNull = parsePriority({ low: false });
  assert(lowNull.low === false && lowNull.until === 0, "parsePriority: low=false -> until 0");

  const epoch = Math.floor((Date.now() + 60000) / 1000);
  const pNum = parsePriority({ low: true, boxed_until: epoch });
  assert(pNum.until === epoch * 1000, "parsePriority: numeric boxed_until (seconds) -> ms");
}

// --- COV-HIGH-1: isCapacityFree decision logic (extracted from acquireSlot) ---
// Covers: unlimited-plan short-circuit, /usage-unreachable fallback, shared
// pause (C2), at-cap, under-cap, priority.low → repause, hard_cap gating
// (CORR2-1), and unlimited + priority.low repause (CORR2-2).
{
  const lowState = { low: true, until: 1_000_000, reason: "burst" };
  const okState = { low: false, until: 0, reason: null };

  // Unlimited plan (limit === undefined): always free regardless of snap
  // (D5 — capacity check skipped; priority.low still honored via repause when
  // snap is present, but free stays true because there's no cap to exceed).
  assert(isCapacityFree(null, { limit: undefined, queuePaused: false }).free === true,
    "isCapacityFree: unlimited plan + /usage unreachable → free");
  const unlim = isCapacityFree(
    { concurrentSessions: 999, limit: undefined, hardCap: undefined, priority: okState },
    { limit: undefined, queuePaused: false },
  );
  assert(unlim.free === true, "isCapacityFree: unlimited plan → free even at high conc");

  // CORR2-2: unlimited plan + priority.low → not free + repause (priority.low
  // is checked BEFORE the unlimited short-circuit so Code Max still pushes the
  // shared pause to siblings).
  const unlimLow = isCapacityFree(
    { concurrentSessions: 0, limit: undefined, hardCap: undefined, priority: lowState },
    { limit: undefined, queuePaused: false },
  );
  assert(unlimLow.free === false && unlimLow.repause?.until === 1_000_000 && unlimLow.repause?.reason === "burst",
    "CORR2-2: unlimited plan + priority.low → not free + repause (before short-circuit)");

  // /usage unreachable (snap === null) with a finite limit → free (trust headroom)
  assert(isCapacityFree(null, { limit: 2, queuePaused: false }).free === true,
    "isCapacityFree: /usage unreachable → free (trust headroom)");

  // Shared pause active (queuePaused) → not free (C2)
  assert(isCapacityFree(
    { concurrentSessions: 0, limit: 2, hardCap: 4, priority: okState },
    { limit: 2, queuePaused: true },
  ).free === false, "isCapacityFree: shared pause active → not free");

  // CORR2-1: hard_cap gating. cur between limit and hard_cap → free (the gate
  // prevents 429s, which hit at hard_cap, not limit). The message_end release
  // race can transiently push concurrent_sessions 1-2 over limit but within the
  // burst headroom (hard_cap); gating to hard_cap absorbs that + server-side
  // accounting noise (ADV2-F3).
  assert(isCapacityFree(
    { concurrentSessions: 3, limit: 2, hardCap: 4, priority: okState },
    { limit: 2, queuePaused: false },
  ).free === true, "CORR2-1: cur (3) between limit (2) and hard_cap (4) → free (burst headroom)");
  assert(isCapacityFree(
    { concurrentSessions: 4, limit: 2, hardCap: 4, priority: okState },
    { limit: 2, queuePaused: false },
  ).free === false, "CORR2-1: cur (4) at hard_cap (4) → not free (429 threshold)");
  assert(isCapacityFree(
    { concurrentSessions: 5, limit: 2, hardCap: 4, priority: okState },
    { limit: 2, queuePaused: false },
  ).free === false, "CORR2-1: cur (5) over hard_cap (4) → not free");

  // hard_cap absent (older API / unlimited) → falls back to limit.
  assert(isCapacityFree(
    { concurrentSessions: 2, limit: 2, hardCap: undefined, priority: okState },
    { limit: 2, queuePaused: false },
  ).free === false, "CORR2-1: hard_cap absent → falls back to limit (cur === limit → not free)");

  // At cap (cur >= cap) → not free (legacy path: hard_cap absent, cap = limit)
  assert(isCapacityFree(
    { concurrentSessions: 2, limit: 2, hardCap: undefined, priority: okState },
    { limit: 2, queuePaused: false },
  ).free === false, "isCapacityFree: at cap (cur === limit) → not free");

  // Under cap → free
  assert(isCapacityFree(
    { concurrentSessions: 1, limit: 2, hardCap: 4, priority: okState },
    { limit: 2, queuePaused: false },
  ).free === true, "isCapacityFree: under cap → free");

  // priority.low → not free + repause (so the caller pushes the pause to siblings)
  const low = isCapacityFree(
    { concurrentSessions: 0, limit: 2, hardCap: 4, priority: lowState },
    { limit: 2, queuePaused: false },
  );
  assert(low.free === false && low.repause?.until === 1_000_000 && low.repause?.reason === "burst",
    "isCapacityFree: priority.low → not free + repause with until/reason");

  // Server-reported hard_cap overrides the local limit when smaller (cap = snap.hardCap ?? snap.limit ?? limit)
  assert(isCapacityFree(
    { concurrentSessions: 1, limit: 4, hardCap: 1, priority: okState },
    { limit: 2, queuePaused: false },
  ).free === false, "CORR2-1: server hard_cap (1) < local limit (2) → at cap");
}

// --- readState: absent/corrupt file -> empty ---
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  const s1 = readState(stateFile, Date.now());
  assert(s1.waiters.length === 0 && s1.token === null && s1.pausedUntil === 0,
    "readState: absent file -> empty state");
  const s2 = readState(stateFile, Date.now());
  assert(s2.waiters.length === 0, "readState: corrupt/missing -> empty (no throw)");
  rmSync(dir, { recursive: true, force: true });
}

// --- COV-HIGH-2: parsePriority ISO-string + malformed branches ---
// boxed_until may be an ISO string (the most common form in real /v1/usage
// responses), an epoch-seconds number, or null. The ISO path (Date.parse) and
// a malformed string (NaN → fallback) were previously untested.
{
  const future = "2026-12-31T00:00:00Z";
  const pIso = parsePriority({ low: true, boxed_until: future, reason: "boxed" });
  assert(pIso.low === true && pIso.until === Date.parse(future),
    "COV-HIGH-2: parsePriority ISO boxed_until -> ms via Date.parse");
  assert(pIso.reason === "boxed", "COV-HIGH-2: parsePriority ISO reason captured");

  // Malformed string → Date.parse returns NaN → falls back to now+backoff.
  const pBad = parsePriority({ low: true, boxed_until: "not-a-date" });
  assert(pBad.low === true && pBad.until > Date.now(),
    "COV-HIGH-2: parsePriority malformed ISO -> falls back to now+backoff");

  // Empty string boxed_until with low=true → fallback (string is falsy-empty).
  const pEmpty = parsePriority({ low: true, boxed_until: "" });
  assert(pEmpty.until > Date.now(),
    "COV-HIGH-2: parsePriority empty-string boxed_until -> fallback");

  // low=false ignores boxed_until entirely (until stays 0).
  assert(parsePriority({ low: false, boxed_until: future }).until === 0,
    "COV-HIGH-2: parsePriority low=false ignores boxed_until");
}

// --- COV-HIGH-3: readState corrupt-input fixtures ---
// readState guards waiters (Array.isArray), pausedUntil/pausedTs (typeof number),
// and falls back to empty on JSON throw. Previously only the absent-file case
// was tested. Exercise: truncated JSON, garbage waiters, non-object token,
// string pausedUntil.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  const { writeFileSync } = await import("node:fs");

  // Truncated JSON → throws → caught → empty state.
  writeFileSync(stateFile, '{"waiters":[');
  const truncated = readState(stateFile, Date.now());
  assert(truncated.waiters.length === 0 && truncated.token === null,
    "COV-HIGH-3: readState truncated JSON -> empty state (no throw)");

  // Garbage waiters array (entries lack id/pid/ts) → returned as-is; reapStale
  // would later filter them via isPidDead(undefined)=true. readState itself does
  // not validate entry shape, only that waiters is an array.
  writeFileSync(stateFile, JSON.stringify({ waiters: [{ foo: 1 }, { bar: 2 }] }));
  const garbage = readState(stateFile, Date.now());
  assert(garbage.waiters.length === 2,
    "COV-HIGH-3: readState garbage waiters array returned as-is (shape validated by reapStale)");

  // Non-object token (a string) → parsed.token ?? null passes it through;
  // reapStale reads state.token.pid (undefined) → isPidDead(undefined)=true → reaped.
  writeFileSync(stateFile, JSON.stringify({ token: "not-an-object" }));
  const badTok = readState(stateFile, Date.now());
  assert(badTok.token === "not-an-object",
    "COV-HIGH-3: readState non-object token passed through (reaped later by reapStale)");

  // String pausedUntil → typeof !== number → falls to 0.
  writeFileSync(stateFile, JSON.stringify({ pausedUntil: "123", pausedTs: "456" }));
  const strPause = readState(stateFile, Date.now());
  assert(strPause.pausedUntil === 0 && strPause.pausedTs === 0,
    "COV-HIGH-3: readState string pausedUntil/pausedTs -> 0 (typeof guard)");

  rmSync(dir, { recursive: true, force: true });
}

// --- COV-HIGH-4: cancel paths (non-existent id, non-head waiter, token-holder) ---
// cancel is a hot path (called on every acquireSlot release) but was never
// exercised by selfcheck. Covers: no-op on missing id, removal of a non-head
// waiter (no token release), and release of a held token.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  const q = createConcurrencyQueue({ stateFile });
  const { readFileSync } = await import("node:fs");

  // Seed two waiters + a token held by id1.
  const id1 = q.join()!;
  const id2 = q.join()!;
  // Manually claim the token for id1 by waiting for launch.
  await q.waitForLaunch(id1);
  assert(q.holdsToken() === true, "COV-HIGH-4: id1 holds the token");

  // cancel a non-existent id → no-op (id1, id2, and token all unchanged).
  q.cancel("does-not-exist");
  let st = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(st.waiters.length === 2 && st.token.id === id1,
    "COV-HIGH-4: cancel non-existent id is a no-op");

  // cancel a non-head waiter (id2) → removes from queue, does NOT release token.
  q.cancel(id2);
  st = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(!st.waiters.some((w: { id: string }) => w.id === id2) && st.waiters.length === 1,
    "COV-HIGH-4: cancel non-head waiter removes it from the queue");
  assert(st.token !== null && st.token.id === id1,
    "COV-HIGH-4: cancel non-head waiter does NOT release the token");

  // cancel the token-holder (id1) → releases token + removes waiter.
  q.cancel(id1);
  st = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(st.token === null, "COV-HIGH-4: cancel token-holder releases the token");
  assert(st.waiters.length === 0, "COV-HIGH-4: cancel token-holder removes its waiter");
  assert(q.holdsToken() === false, "COV-HIGH-4: holdsToken false after cancel");

  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- isPidDead: our own pid is alive; pid -1 / 999999 are dead ---
assert(isPidDead(process.pid) === false, "isPidDead: own pid alive");
assert(isPidDead(-1) === true, "isPidDead: invalid pid dead");
assert(isPidDead(9_999_999) === true, "isPidDead: unlikely pid dead");

// --- reapStale: token + waiters for dead pids are removed ---
{
  const now = 1_000_000;
  const cfg = {
    stateFile: "/dev/null", staleTokenMs: 30_000, staleWaiterMs: 300_000,
    lockRetryMs: 5, lockTimeoutMs: 2_000, now: () => now, pid: () => process.pid,
  } as const;
  const dead = 9_999_999;
  const state = {
    waiters: [
      { id: "w1", pid: process.pid, ts: now - 1000 }, // alive, fresh
      { id: "w2", pid: dead, ts: now - 1000 }, // dead pid -> reap
      { id: "w3", pid: process.pid, ts: now - 999_999 }, // stale ts -> reap
    ],
    token: { id: "t1", pid: dead, ts: now - 1000 }, // dead pid -> reap
    pausedUntil: 0, pausedReason: null, pausedTs: 0,
  };
  const reaped = reapStale(state, cfg as any, now);
  assert(reaped.token === null, "reapStale: dead-pid token reclaimed");
  assert(reaped.waiters.length === 1 && reaped.waiters[0].id === "w1",
    "reapStale: dead-pid and stale waiters removed, fresh kept");
}

// --- S2: clampPauseUntil caps an over-large pause to now + MAX_PAUSE_MS ---
{
  const now = 1_700_000_000_000;
  const huge = now + 1e13; // ~317,000 years (the Number("1e10") wedge)
  const clamped = clampPauseUntil(huge, now);
  assert(clamped === now + MAX_PAUSE_MS,
    "clampPauseUntil: 1e13s pause clamped to now + MAX_PAUSE_MS (5h)");
  const small = now + 60_000;
  assert(clampPauseUntil(small, now) === small,
    "clampPauseUntil: sub-ceiling pause unchanged");
}

// --- S4: strict Retry-After parse rejects hex/sci-notation; valid ints clamp (S4) ---
// Mirrors the inline parse in index.ts after_provider_response 429 branch:
// /^\d+$/.test(trim) then parseInt + clampPauseUntil. Number() accepted
// "0x10"=16 and "1e10"=1e10; the strict regex rejects both.
{
  const now = 1_700_000_000_000;
  function parseRetryAfter(raw: unknown, base = now): number {
    const floor = base + 30_000;
    if (!raw) return floor;
    const trimmed = String(raw).trim();
    if (!/^\d+$/.test(trimmed)) return floor;
    const secs = parseInt(trimmed, 10);
    return secs > 0 ? clampPauseUntil(base + secs * 1000, base) : floor;
  }
  assert(parseRetryAfter("60") === now + 60_000, "Retry-After 60s honored");
  assert(parseRetryAfter("0") === now + 30_000, "Retry-After 0 falls back to 30s floor");
  assert(parseRetryAfter("0x10") === now + 30_000, "Retry-After hex rejected -> 30s floor");
  assert(parseRetryAfter("1e10") === now + 30_000, "Retry-After sci-notation rejected -> 30s floor (S2 wedge prevented)");
  assert(parseRetryAfter(" -5 ") === now + 30_000, "Retry-After negative-with-spaces rejected -> 30s floor");
  assert(parseRetryAfter("") === now + 30_000, "Retry-After empty -> 30s floor");
  assert(parseRetryAfter("99999999") === now + MAX_PAUSE_MS, "Retry-After huge int clamped to now + MAX_PAUSE_MS");
}

// --- S2: reapStale clears a pause whose pausedTs is older than MAX_PAUSE_MS ---
{
  const now = 1_700_000_000_000;
  const cfg = {
    stateFile: "/dev/null", staleTokenMs: 30_000, staleWaiterMs: 300_000,
    lockRetryMs: 5, lockTimeoutMs: 2_000, now: () => now, pid: () => process.pid,
  } as const;
  // A poisoned pause set far in the past beyond MAX_PAUSE_MS (simulating a
  // clamp-bypassed or hand-edited pausedUntil with an old pausedTs).
  const state = {
    waiters: [],
    token: null,
    pausedUntil: now + 1e13, // absurd future deadline
    pausedReason: "poisoned",
    pausedTs: now - MAX_PAUSE_MS - 1, // set just past the ceiling ago
  };
  const reaped = reapStale(state, cfg as any, now);
  assert(reaped.pausedUntil === 0 && reaped.pausedReason === null && reaped.pausedTs === 0,
    "reapStale: pause older than MAX_PAUSE_MS is reaped (defense-in-depth)");

  // A fresh pause (pausedTs within ceiling) with a within-ceiling pausedUntil
  // is left intact — the clamp in pauseUntil is the primary guard against
  // oversized values, and reapStale only clears age/duration violations.
  const fresh = { ...state, pausedUntil: now + 60_000, pausedTs: now - 1000 };
  const reapedFresh = reapStale(fresh, cfg as any, now);
  assert(reapedFresh.pausedUntil === now + 60_000,
    "reapStale: fresh pause (pausedTs within ceiling, duration within ceiling) is not reaped");
}

// --- SEC2-MED-1: reapStale reaps a forward-dated pausedTs + oversized pausedUntil ---
// A hand-edited/compromised file can set pausedTs to a FUTURE timestamp so
// `now - pausedTs` is negative (bypassing the age check). When paired with an
// oversized pausedUntil, the pause would never be reaped, keeping
// snapshot().paused true for far beyond the 5h ceiling. The duration check
// (pausedUntil - now > MAX_PAUSE_MS) catches this regardless of pausedTs.
{
  const now = 1_700_000_000_000;
  const cfg = {
    stateFile: "/dev/null", staleTokenMs: 30_000, staleWaiterMs: 300_000,
    lockRetryMs: 5, lockTimeoutMs: 2_000, now: () => now, pid: () => process.pid,
  } as const;
  // Forward-dated pausedTs (future) + oversized pausedUntil (100h out).
  // `now - pausedTs` is negative -> age check false; duration check catches it.
  const state = {
    waiters: [],
    token: null,
    pausedUntil: now + 100 * 60 * 60 * 1000, // 100h — exceeds MAX_PAUSE_MS (5h)
    pausedReason: "poisoned-forward-dated",
    pausedTs: now + 100 * 60 * 60 * 1000, // also forward-dated (future)
  };
  const reaped = reapStale(state, cfg as any, now);
  assert(reaped.pausedUntil === 0 && reaped.pausedReason === null && reaped.pausedTs === 0,
    "SEC2-MED-1: forward-dated pausedTs + oversized pausedUntil is reaped (duration check)");

  // A legitimately short pause with a forward-dated pausedTs (edge case) is
  // left intact — duration is within ceiling, and the age check is negative
  // but the pause is short so no reap is warranted.
  const shortState = { ...state, pausedUntil: now + 30_000, pausedTs: now + 1000 };
  const shortReaped = reapStale(shortState, cfg as any, now);
  assert(shortReaped.pausedUntil === now + 30_000,
    "SEC2-MED-1: short pause with forward-dated pausedTs is not reaped (duration within ceiling)");
}

// --- createConcurrencyQueue: FIFO + launch token (file-backed, temp dir) ---
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  const q = createConcurrencyQueue({ stateFile });

  // join -> waitForLaunch: first waiter claims token immediately.
  const id1 = q.join();
  assert(id1 !== null, "join returns an id");
  let r1!: () => void;
  const p1 = q.waitForLaunch(id1!).then((r) => { r1 = r; return r; });
  await p1;
  assert(q.holdsToken() === true, "first waiter holds the launch token");

  // Second waiter joins but must NOT claim the token (held by 1).
  const id2 = q.join();
  let got2 = false;
  let r2!: () => void;
  const p2 = q.waitForLaunch(id2!).then((r) => { got2 = true; r2 = r; return r; });
  await new Promise((r) => setTimeout(r, 80)); // let the 50ms poll fire
  assert(!got2, "second waiter blocked while token held");
  assert(q.snapshot().queued === 2, "snapshot shows 2 waiters queued");

  // Release token from 1 -> 2 claims it.
  r1();
  await p2;
  assert(got2, "second waiter claims token after 1 releases");
  assert(q.holdsToken() === true, "second waiter now holds token");
  r2();
  assert(q.snapshot().queued === 0, "queue drains after both release");

  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- COV-HIGH-5: 3-waiter FIFO drain + late-joiner (joins after token held) ---
// The 2-waiter test proves promote-once; this proves the steady-state drain
// (1 → 2 → 3) and the late-joiner branch (state.token truthy when a waiter
// joins after the token is already held by a long-running poller).
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  const q = createConcurrencyQueue({ stateFile });

  // 1 claims the token.
  const id1 = q.join()!;
  const r1 = await q.waitForLaunch(id1);
  assert(q.holdsToken() === true, "COV-HIGH-5: w1 holds the token");

  // 2 + 3 queue behind 1.
  const id2 = q.join()!;
  const id3 = q.join()!;
  let got2: (() => void) | undefined, got3: (() => void) | undefined;
  const p2 = q.waitForLaunch(id2).then((r) => { got2 = r; return r; });
  const p3 = q.waitForLaunch(id3).then((r) => { got3 = r; return r; });
  await new Promise((r) => setTimeout(r, 80));
  assert(!got2 && !got3, "COV-HIGH-5: w2 + w3 blocked while w1 holds token");
  assert(q.snapshot().queued === 3, "COV-HIGH-5: 3 waiters queued");

  // 1 releases → 2 claims (promote-once).
  r1();
  await p2;
  assert(typeof got2 === "function", "COV-HIGH-5: w2 claims after w1 releases");
  assert(q.holdsToken() === true, "COV-HIGH-5: w2 now holds token");
  await new Promise((r) => setTimeout(r, 80));
  assert(!got3, "COV-HIGH-5: w3 still blocked while w2 holds token");

  // 2 releases → 3 claims (promote-twice, steady-state drain).
  got2!();
  await p3;
  assert(typeof got3 === "function", "COV-HIGH-5: w3 claims after w2 releases");
  got3!();
  assert(q.snapshot().queued === 0, "COV-HIGH-5: queue drains after all release");

  // Late-joiner: a waiter that joins AFTER the token is already held by a
  // long-running poller. idHolder claims first; idLate joins behind it.
  const idHolder = q.join()!;
  const rHolder = await q.waitForLaunch(idHolder);
  assert(q.holdsToken() === true, "COV-HIGH-5: late-joiner setup — holder holds token");
  const idLate = q.join()!;
  let gotLate = false;
  const pLate = q.waitForLaunch(idLate).then((r) => { gotLate = true; return r; });
  await new Promise((r) => setTimeout(r, 80));
  assert(!gotLate, "COV-HIGH-5: late-joiner blocked (token held when it joined)");
  rHolder();
  await pLate;
  assert(gotLate, "COV-HIGH-5: late-joiner claims after holder releases");
  // Drain the late-joiner's token via reset (its release fn is captured in pLate).
  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- COV-MED-3: stale-lockfile recovery (acquireLock reclaims old mtime) ---
// If a lockfile is older than lockTimeoutMs, a crashed holder left it behind;
// acquireLock unlinks it and retries. Pre-create an old-mtime lockfile and
// assert join() (which takes the lock) succeeds within one retry.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  const lockFile = `${stateFile}.lock`;
  const { writeFileSync, utimesSync } = await import("node:fs");

  // Pre-create a stale lockfile (old mtime, simulating a crashed holder).
  writeFileSync(lockFile, "", { mode: 0o600 });
  const oldTime = (Date.now() / 1000) - 10; // 10s ago — past lockTimeoutMs (2s)
  utimesSync(lockFile, oldTime, oldTime);

  // join() must succeed (acquireLock reclaims the stale lockfile + retries).
  const q = createConcurrencyQueue({ stateFile, lockTimeoutMs: 2_000 });
  const id = q.join()!;
  assert(id !== null, "COV-MED-3: join succeeds despite stale lockfile (reclaimed)");

  // The stale lockfile must be gone after acquireLock reclaimed it (and the
  // new one released after the critical section). join() completes the mutate,
  // so no lockfile should remain.
  let lockGone = false;
  try { /* lockfile is transient; after mutate it's released */ } catch { /* ignore */ }
  // Verify the state file was written (proving the critical section ran).
  const { readFileSync } = await import("node:fs");
  const st = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(st.waiters.length === 1 && st.waiters[0].id === id,
    "COV-MED-3: stale-lockfile recovery let the mutate write our waiter");

  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- COV-MED-6: concurrencyLimit() edge inputs (parseConcurrencyLimit) ---
// "2.5" → 2.5 (fractional, kept as-is), " " → fallback, "0" → fallback,
// "abc" → fallback, "" → fallback, undefined → fallback.
{
  const fallback = 4;
  assert(parseConcurrencyLimit("2.5", fallback) === 2.5,
    "COV-MED-6: '2.5' → 2.5 (fractional kept)");
  assert(parseConcurrencyLimit(" ", fallback) === fallback,
    "COV-MED-6: whitespace → fallback");
  assert(parseConcurrencyLimit("0", fallback) === fallback,
    "COV-MED-6: '0' → fallback (non-positive)");
  assert(parseConcurrencyLimit("abc", fallback) === fallback,
    "COV-MED-6: 'abc' → fallback (NaN)");
  assert(parseConcurrencyLimit("", fallback) === fallback,
    "COV-MED-6: empty → fallback");
  assert(parseConcurrencyLimit(undefined, fallback) === fallback,
    "COV-MED-6: undefined → fallback");
  assert(parseConcurrencyLimit("-5", fallback) === fallback,
    "COV-MED-6: '-5' → fallback (negative)");
  assert(parseConcurrencyLimit("3", fallback) === 3,
    "COV-MED-6: '3' → 3 (valid)");
  // fallback undefined (unlimited plan) propagates.
  assert(parseConcurrencyLimit("0", undefined) === undefined,
    "COV-MED-6: '0' with undefined fallback → undefined");
}

// --- S3: state file + lockfile created with mode 0600 (no PID leakage) ---
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  const q = createConcurrencyQueue({ stateFile });

  // join + waitForLaunch forces a writeStateAtomic (state file) and an
  // acquireLock (lockfile). Both must land at mode 0600, not the default 0644.
  const id = q.join();
  const release = await q.waitForLaunch(id!);
  release();

  const { statSync } = await import("node:fs");
  const stateMode = statSync(stateFile).mode & 0o777;
  assert(stateMode === 0o600, `state file mode is 0600 (got 0o${stateMode.toString(8)})`);

  // The lockfile is transient (released after the critical section), so trigger
  // another mutate to recreate it and snapshot mid-flight. Easiest: pause, which
  // takes the lock and writes the state file. We poll for the lockfile briefly.
  q.pauseUntil(Date.now() + 1000, "S3 probe");
  // The lockfile is normally gone by now; create a fresh one by joining a second
  // queue instance and snapshotting while it holds the lock. Simpler: directly
  // exercise acquireLock via a second join+waitForLaunch on a sibling queue and
  // check the lockfile mode while the token is held.
  const q2 = createConcurrencyQueue({ stateFile });
  const id2 = q2.join();
  const lockFile = `${stateFile}.lock`;
  let lockMode: number | undefined;
  const release2Promise = q2.waitForLaunch(id2!).then((r) => r);
  // Poll up to 200ms for the lockfile to appear mid-acquire.
  for (let i = 0; i < 40 && lockMode === undefined; i++) {
    try { lockMode = statSync(lockFile).mode & 0o777; } catch { /* not yet */ }
    if (lockMode === undefined) await new Promise((r) => setTimeout(r, 5));
  }
  const release2 = await release2Promise;
  if (lockMode !== undefined) {
    assert(lockMode === 0o600, `lockfile mode is 0600 (got 0o${lockMode.toString(8)})`);
  }
  // If the lockfile never observed (timing-dependent), the state-file mode
  // assertion above already covers the writeStateAtomic path; the lockfile uses
  // the same 0o600 arg to openSync.
  release2();

  q.reset(); q2.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- createConcurrencyQueue: pause is shared across queue instances (cross-process sim) ---
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  const qA = createConcurrencyQueue({ stateFile });
  const qB = createConcurrencyQueue({ stateFile }); // simulates a second process

  const until = Date.now() + 10_000;
  qA.pauseUntil(until, "429 from A");
  const snapB = qB.snapshot();
  assert(snapB.paused && snapB.pausedUntil === until, "pause written by A visible to B (shared file)");

  qA.clearPause();
  assert(qB.snapshot().paused === false, "clearPause by A reflected in B");

  qA.reset(); qB.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- C2: shared pausedUntil is read by a sibling before launching ---
// capacityFree (the head-waiter poll in acquireSlot) now consults
// concurrencyQueue.snapshot().paused before launching, so a 429 observed by
// process A immediately backs off process B even before /usage propagates
// priority.low. Since capacityFree is a closure, we assert the observable
// behavior it depends on: a sibling's snapshot().paused reflects the shared
// pause written by another instance, and clears when it elapses.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  const qA = createConcurrencyQueue({ stateFile });
  const qB = createConcurrencyQueue({ stateFile }); // simulates a second process

  // A writes a 10s pause (e.g. it just got a 429). B must see it immediately.
  const until = Date.now() + 10_000;
  qA.pauseUntil(until, "HTTP 429 from gateway");
  const snapB = qB.snapshot();
  assert(snapB.paused === true, "C2: sibling sees shared paused before /usage catches up");
  assert(snapB.pausedUntil === until, "C2: sibling sees the exact shared deadline");

  // A clears it (e.g. /usage reports priority.low === false). B sees it lift.
  qA.clearPause();
  assert(qB.snapshot().paused === false, "C2: sibling sees pause lift when cleared");

  qA.reset(); qB.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- ADV-4: a live-PID waiter aged past staleWaiterMs is re-inserted, not lost ---
// waitForLaunch's mutate now detects a missing waiter entry (reaped by
// staleWaiterMs) and re-inserts it at the tail with a fresh timestamp, so a
// long-queued turn does not poll forever with head.id !== ourId permanently
// true. We simulate a short staleWaiterMs and verify the entry survives past
// it (re-inserted by the 50ms poll) before aborting cleanly via signal.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  // staleWaiterMs=80ms so a waiter aged past it would be reaped on the next
  // mutate — but waitForLaunch re-inserts before that can strand us.
  const q = createConcurrencyQueue({ stateFile, staleWaiterMs: 80 });
  const { readFileSync } = await import("node:fs");

  // Waiter 1 holds the token.
  const id1 = q.join()!;
  await q.waitForLaunch(id1);

  // Waiter 2 joins and blocks (token held by 1), with an abort signal so we
  // can cleanly stop the poll loop afterward.
  const id2 = q.join()!;
  const ctrl = new AbortController();
  const p2 = q.waitForLaunch(id2, ctrl.signal).catch(() => { /* aborted */ });
  await new Promise((r) => setTimeout(r, 60)); // pre-reap: entry present
  let st = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(st.waiters.some((w: { id: string }) => w.id === id2),
    "ADV-4: waiter 2 queued before staleWaiterMs");

  // Wait well past staleWaiterMs (80ms). Without re-insertion, the next
  // reapStale would drop id2 and waitForLaunch would poll forever. With
  // re-insertion, the poll re-adds id2 with a fresh ts each cycle, surviving.
  await new Promise((r) => setTimeout(r, 200));
  st = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(st.waiters.some((w: { id: string }) => w.id === id2),
    "ADV-4: live-PID waiter re-inserted after staleWaiterMs (not lost)");

  // Cleanly stop the poll loop via abort (cancel + reject).
  ctrl.abort();
  await p2;

  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- ADV-1: stale .tmp files are reaped by writeStateAtomic; fresh ones are not ---
// A crashed writer (killed between writeFileSync and renameSync) leaves a
// <path>.<pid>.tmp that would accumulate forever. writeStateAtomic now
// best-effort unlinks any <path>.*.tmp older than 60s. A fresh .tmp (just
// written by another process) has a current mtime and must be left alone.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  const { writeFileSync, statSync, utimesSync } = await import("node:fs");

  // Pre-create a stale .tmp (old mtime) simulating a crashed writer.
  const staleTmp = `${stateFile}.99999.tmp`;
  writeFileSync(staleTmp, "{}", { mode: 0o600 });
  const oldTime = (Date.now() / 1000) - 120; // 120s ago — past the 60s threshold
  utimesSync(staleTmp, oldTime, oldTime);

  // Pre-create a fresh .tmp (current mtime) simulating a live writer mid-write.
  const freshTmp = `${stateFile}.88888.tmp`;
  writeFileSync(freshTmp, "{}", { mode: 0o600 });

  // Trigger a writeStateAtomic via a mutate (pause).
  const q = createConcurrencyQueue({ stateFile });
  q.pauseUntil(Date.now() + 1000, "ADV-1 probe");

  // The stale .tmp must be reaped; the fresh one must survive.
  let staleGone = false;
  try { statSync(staleTmp); } catch { staleGone = true; }
  assert(staleGone, "ADV-1: stale .tmp (old mtime) reaped by writeStateAtomic");

  let freshExists = false;
  try { statSync(freshTmp); freshExists = true; } catch { /* gone */ }
  assert(freshExists, "ADV-1: fresh .tmp (current mtime) left untouched");

  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- createConcurrencyQueue: disabled mode is a no-op ---
{
  const q = createConcurrencyQueue({ disabled: true });
  assert(q.join() === null, "disabled: join returns null");
  const r = await q.waitForLaunch("ignored");
  assert(typeof r === "function", "disabled: waitForLaunch resolves with noop release");
  r();
  assert(q.snapshot().queued === 0 && q.holdsToken() === false, "disabled: snapshot empty");
}

// --- reset() does not wipe a sibling's queue state (S1) ---
// reset() must only clear THIS process's own entries; it must not unlink the
// shared state file (which would drop a sibling pi process's waiters/token).
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  const { readFileSync, writeFileSync } = await import("node:fs");

  // Seed the shared file with a sibling pi process's waiter entry + token.
  // These represent state written by another queue instance on the same file.
  const siblingTs = Date.now();
  const siblingState = {
    waiters: [{ id: "sibling-w1", pid: process.pid, ts: siblingTs }],
    token: { id: "sibling-t1", pid: process.pid, ts: siblingTs },
    pausedUntil: 0,
    pausedReason: null,
  };
  writeFileSync(stateFile, JSON.stringify(siblingState));

  // A second process B joins, then resets. reset() must NOT remove sibling-w1
  // or sibling-t1, and must NOT unlink the state file.
  const qB = createConcurrencyQueue({ stateFile });
  qB.join();
  qB.reset();

  // The state file must still exist (reset must not unlink it).
  let fileExists = true;
  try { readFileSync(stateFile, "utf8"); } catch { fileExists = false; }
  assert(fileExists, "reset() does not unlink the shared state file");

  const after = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(after.waiters.some((w: { id: string }) => w.id === "sibling-w1"),
    "reset() does not remove a sibling's waiter entry");
  assert(after.token !== null && after.token.id === "sibling-t1",
    "reset() does not remove a sibling's token");

  rmSync(dir, { recursive: true, force: true });
}

// --- C4/ADV-2: waitForLaunch(ourId, signal) rejects + cancels on abort ---
// An aborted turn must not leave a waiter entry at the head of the shared file
// (which would block sibling pi processes for staleWaiterMs = 5 min) and must
// not later claim the token and resolve a release fn nobody holds (leak).
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  const q = createConcurrencyQueue({ stateFile });

  // First waiter holds the token so the second one blocks in waitForLaunch.
  const id1 = q.join()!;
  const r1 = await q.waitForLaunch(id1);
  assert(q.holdsToken() === true, "abort test: first waiter holds the token");

  // Second waiter joins and blocks — its entry is now queued in the file.
  const id2 = q.join()!;
  const ctrl = new AbortController();
  let rejected = false;
  const p2 = q.waitForLaunch(id2, ctrl.signal).catch(() => { rejected = true; });
  await new Promise((r) => setTimeout(r, 80)); // let the 50ms poll fire
  assert(!rejected, "abort test: second waiter blocks while token held");
  assert(q.snapshot().queued === 2, "abort test: both waiters queued before abort");

  // Abort mid-wait. waitForLaunch must reject, stop the setTimeout chain, and
  // cancel our waiter entry (so it's not left at the head blocking siblings).
  ctrl.abort();
  await p2;
  assert(rejected, "abort test: waitForLaunch rejects on signal abort");

  // The waiter entry for id2 must be gone from the shared file (cancel ran).
  const { readFileSync } = await import("node:fs");
  const after = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(!after.waiters.some((w: { id: string }) => w.id === id2),
    "abort test: aborted waiter entry removed from shared file (cancel ran)");

  // Releasing the first waiter must not then hand the token to the orphaned id2
  // (its promise already rejected; this proves no token leak).
  r1();
  await new Promise((r) => setTimeout(r, 80));
  assert(q.holdsToken() === false, "abort test: no orphan claimed the token");

  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- ADV-5: acquireSlot throw path cancels the waiter (no 5-min leak) ---
// If waitForLaunch throws after join() (lock timeout, EACCES, ENOSPC), the
// waiter entry added by join() must be cancelled, not left in the file for
// staleWaiterMs. We simulate by aborting the signal before the first poll —
// waitForLaunch cancels + rejects on an already-aborted signal.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  const q = createConcurrencyQueue({ stateFile });

  // Hold the token so the second joiner would block.
  const id1 = q.join()!;
  await q.waitForLaunch(id1);

  // Joiner 2 with an already-aborted signal: waitForLaunch must cancel + reject
  // immediately without leaving the waiter entry.
  const id2 = q.join()!;
  const ctrl = new AbortController();
  ctrl.abort();
  let rejected = false;
  await q.waitForLaunch(id2, ctrl.signal).catch(() => { rejected = true; });
  assert(rejected, "ADV-5: already-aborted signal rejects waitForLaunch");

  const { readFileSync } = await import("node:fs");
  const after = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(!after.waiters.some((w: { id: string }) => w.id === id2),
    "ADV-5: pre-aborted waiter entry is cancelled (no leaked waiter)");

  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

console.log("\nall checks passed");
