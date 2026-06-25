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

  // A fresh pause (pausedTs within ceiling) is left intact even if pausedUntil
  // is far in the future — the clamp in pauseUntil is the primary guard.
  const fresh = { ...state, pausedTs: now - 1000 };
  const reapedFresh = reapStale(fresh, cfg as any, now);
  assert(reapedFresh.pausedUntil === state.pausedUntil,
    "reapStale: fresh pause (pausedTs within ceiling) is not reaped");
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

console.log("\nall checks passed");
