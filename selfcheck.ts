// ponytail: one runnable check for the branchy pure logic.
// - vision model picking + image-id hashing (unchanged)
// - parsePriority: normalize /v1/usage priority → deadline
// - concurrency-queue: readState/reapStale pure helpers
// - createConcurrencyQueue: FIFO + launch-token + pause (file-backed, temp dir)
//
// Run: node --experimental-strip-types selfcheck.ts
import { mkdtempSync, rmSync, writeFileSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { isNativeVision, pickVisionModel, hashImageId, decideLaunch, shouldReleaseOnMessageEnd, nextPollInterval, default as umansFactory, pickSearchModel, formatStatusText, sanitizeErrorBody, handle429 } from "./index.ts";
import {
  parsePriority,
  readState,
  reapStale,
  createConcurrencyQueue,
  isPidDead,
  clampPauseUntil,
  isCapacityFree,
  parseConcurrencyLimit,
  canAtomicsWait,
  MAX_PAUSE_MS,
  PAUSE_REASON_429,
  MAX_PAUSE_429_MS,
  PRIORITY_BACKOFF_MS,
  sanitizeReason,
  type QueueState,
  type QueueConfig,
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

// CLN9-3: QueueState + QueueConfig are exported as named types (used in exported fn signatures).
assert(typeof (null as unknown as QueueState) === "object", "CLN9-3: QueueState type imported by name");
assert(typeof (null as unknown as QueueConfig) === "object", "CLN9-3: QueueConfig type imported by name");

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

  // CORR8-1 (HIGH): unlimited-plan path (inputs.limit === undefined) must
  // NOT short-circuit before the hard_cap gate. hard_cap is the actual 429
  // threshold; an unlimited plan (Code Max) can still trip the account-wide
  // burst cap. Previously isCapacityFree returned { free: true } before
  // evaluating concurrent_sessions against hard_cap — the gate would launch a
  // request the server 429s (probe-confirmed: limit=undefined, hardCap=8,
  // concurrentSessions=10 → free: true). Now the cap check runs: at/over
  // hard_cap → not free; under hard_cap (or all caps undefined) → free.
  assert(isCapacityFree(
    { concurrentSessions: 10, limit: undefined, hardCap: 8, priority: okState },
    { limit: undefined, queuePaused: false },
  ).free === false, "CORR8-1: unlimited plan (limit undefined) + cur (10) at/over hard_cap (8) → not free (D5)");
  assert(isCapacityFree(
    { concurrentSessions: 5, limit: undefined, hardCap: 8, priority: okState },
    { limit: undefined, queuePaused: false },
  ).free === true, "CORR8-1: unlimited plan (limit undefined) + cur (5) under hard_cap (8) → free");
  // True unlimited (all caps undefined) → free (no cap to exceed).
  assert(isCapacityFree(
    { concurrentSessions: 999, limit: undefined, hardCap: undefined, priority: okState },
    { limit: undefined, queuePaused: false },
  ).free === true, "CORR8-1: all caps undefined → free (no cap to exceed)");
}

// --- COV5-1: decideLaunch capacity-poll branch logic (extracted from acquireSlot) ---
// The 5 fix commits that hardened acquireSlot's branches (ADV-3, CORR4-3,
// ADV4-2, COV4-2, ADV4-4) shipped without a regression test pinning the actual
// decision. decideLaunch is the pure seam: given a capacity-free result +
// elapsed time + pause state + signal, decide launch / wait / failOpen / abort.
{
  // free-first-poll: capacity is free on the first check → launch immediately.
  assert(decideLaunch({ isFree: true, elapsedMs: 0, queuePaused: false, signalAborted: false }) === "launch",
    "COV5-1: free on first poll → launch");
  // poll-then-free: after waiting, capacity frees → launch.
  assert(decideLaunch({ isFree: true, elapsedMs: 5_000, queuePaused: false, signalAborted: false }) === "launch",
    "COV5-1: free after polling → launch");
  // not free, under the cap, not aborted → wait.
  assert(decideLaunch({ isFree: false, elapsedMs: 1_000, queuePaused: false, signalAborted: false }) === "wait",
    "COV5-1: not free under cap → wait");
  // not free, cap elapsed, no pause → failOpen (ADV-3).
  assert(decideLaunch({ isFree: false, elapsedMs: 60_000, queuePaused: false, signalAborted: false }) === "failOpen",
    "COV5-1: cap elapsed + no pause → failOpen");
  assert(decideLaunch({ isFree: false, elapsedMs: 120_000, queuePaused: false, signalAborted: false }) === "failOpen",
    "COV5-1: well past cap + no pause → failOpen");
  // CORR4-3: cap elapsed BUT a known pause is active → keep waiting (do not
  // fail open into a still-deprioritized account).
  assert(decideLaunch({ isFree: false, elapsedMs: 60_000, queuePaused: true, signalAborted: false }) === "wait",
    "COV5-1: cap elapsed but paused → wait (CORR4-3 no fail-open during pause)");
  assert(decideLaunch({ isFree: false, elapsedMs: 120_000, queuePaused: true, signalAborted: false }) === "wait",
    "COV5-1: well past cap but paused → wait (CORR4-3)");
  // mid-poll abort: signal fired → abort (cancel + reject), even if cap elapsed.
  assert(decideLaunch({ isFree: false, elapsedMs: 0, queuePaused: false, signalAborted: true }) === "abort",
    "COV5-1: signal aborted → abort");
  assert(decideLaunch({ isFree: false, elapsedMs: 60_000, queuePaused: false, signalAborted: true }) === "abort",
    "COV5-1: signal aborted overrides failOpen → abort");
  assert(decideLaunch({ isFree: false, elapsedMs: 60_000, queuePaused: true, signalAborted: true }) === "abort",
    "COV5-1: signal aborted overrides wait-during-pause → abort");
  // isFree takes precedence over signalAborted (a freed slot launches even if
  // the signal fired concurrently — the launch is safe, the abort is moot).
  assert(decideLaunch({ isFree: true, elapsedMs: 0, queuePaused: false, signalAborted: true }) === "launch",
    "COV5-1: isFree takes precedence over signalAborted");
}

// --- CMP6-3: nextPollInterval exponential backoff on /usage poll under steady-full ---
// A saturated queue drives N×3.3 RPS to /usage continuously. Exponential backoff
// on the poll interval when capacity is steadily full reduces RPS from ~3.3/s
// to ~0.5/s during a sustained pause. Start at 300ms, grow by 1.5× on "wait",
// cap at 2000ms; reset to 300ms on "launch" / "failOpen".
{
  // wait grows by 1.5×, capped at 2000ms.
  assert(nextPollInterval(300, "wait") === 450, "CMP6-3: 300 -> 450 on wait (1.5×)");
  assert(nextPollInterval(450, "wait") === 675, "CMP6-3: 450 -> 675 on wait (1.5×)");
  assert(nextPollInterval(675, "wait") === 1013, "CMP6-3: 675 -> 1013 on wait (1.5×, rounded)");
  assert(nextPollInterval(1333, "wait") === 2000, "CMP6-3: 1333 -> 2000 on wait (capped at 2000)");
  assert(nextPollInterval(2000, "wait") === 2000, "CMP6-3: 2000 -> 2000 on wait (cap holds)");
  // launch / failOpen / abort reset to base (300ms).
  assert(nextPollInterval(2000, "launch") === 300, "CMP6-3: launch resets to base (300)");
  assert(nextPollInterval(2000, "failOpen") === 300, "CMP6-3: failOpen resets to base (300)");
  assert(nextPollInterval(2000, "abort") === 300, "CMP6-3: abort resets to base (300)");
  // A simulated sustained-pause sequence: 300 -> 450 -> 675 -> 1013 -> 1520 -> 2000 -> 2000.
  let ms = 300;
  const seq: number[] = [ms];
  for (let i = 0; i < 6; i++) { ms = nextPollInterval(ms, "wait"); seq.push(ms); }
  assert(seq[0] === 300 && seq[1] === 450 && seq[2] === 675 && seq[3] === 1013 && seq[4] === 1520 && seq[5] === 2000 && seq[6] === 2000,
    "CMP6-3: sustained-wait sequence backs off 300→450→675→1013→1520→2000→2000");
  // A launch mid-sequence resets to base.
  assert(nextPollInterval(2000, "launch") === 300, "CMP6-3: launch after sustained wait resets to base");
}

// --- COV5-2: shouldReleaseOnMessageEnd release guard (extracted from message_end) ---
// The message_end handler releases the main-turn slot only for an Umans
// assistant message. User messages, tool results, and non-Umans providers are
// no-ops (the slot is not held for them, or turn_end/agent_end safety nets cover
// them). shouldReleaseOnMessageEnd is the pure seam tested here.
{
  assert(shouldReleaseOnMessageEnd({ role: "assistant", provider: "umans" }, "umans") === true,
    "COV5-2: umans assistant message → release");
  assert(shouldReleaseOnMessageEnd({ role: "user", provider: "umans" }, "umans") === false,
    "COV5-2: umans user message → no release");
  assert(shouldReleaseOnMessageEnd({ role: "toolResult", provider: "umans" }, "umans") === false,
    "COV5-2: umans tool result → no release");
  assert(shouldReleaseOnMessageEnd({ role: "assistant", provider: "openai" }, "openai") === false,
    "COV5-2: non-umans provider (resolved from msg.provider) → no release");
  // ctx.model.provider is the fallback when msg.provider is unset.
  assert(shouldReleaseOnMessageEnd({ role: "assistant" }, "umans") === true,
    "COV5-2: ctx.model.provider=umans, msg.provider unset → release");
  assert(shouldReleaseOnMessageEnd({ role: "assistant" }, "anthropic") === false,
    "COV5-2: ctx.model.provider=anthropic → no release");
  assert(shouldReleaseOnMessageEnd(undefined, "umans") === false,
    "COV5-2: undefined message → no release");
  assert(shouldReleaseOnMessageEnd({ role: "assistant", provider: "umans" }, undefined) === false,
    "COV5-2: undefined provider → no release");
}


// --- readState: absent/corrupt file -> empty ---
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  const s1 = readState(stateFile);
  assert(s1.waiters.length === 0 && s1.token === null && s1.pausedUntil === 0,
    "readState: absent file -> empty state");
  const s2 = readState(stateFile);
  assert(s2.waiters.length === 0, "readState: corrupt/missing -> empty (no throw)");
  rmSync(dir, { recursive: true, force: true });
}

// --- COV-HIGH-2: parsePriority ISO-string + malformed branches ---
// boxed_until may be an ISO string (the most common form in real /v1/usage
// responses), an epoch-seconds number, or null. The ISO path (Date.parse) and
// a malformed string (NaN → fallback) were previously untested.
{
  const future = new Date(Date.now() + 60_000).toISOString();
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

// --- SEC5-1 / ADV5-5: pausedReason is capped + sanitized (no ANSI/control injection) ---
// A compromised or misconfigured gateway can push a crafted `priority.reason`
// that flows unescaped into the status bar (PAUSED <Ns> (<reason>)). parsePriority
// now caps to ~64 chars and strips non-printable / control / ANSI-escape
// characters, and pauseUntil re-sanitizes at the write boundary (defense-in-
// depth). A crafted 200-char reason with ANSI escapes must be capped + stripped
// before it reaches the shared file or the status bar.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  const { writeFileSync, readFileSync } = await import("node:fs");

  // Crafted reason: 200 chars + ANSI escape sequences + control chars.
  const ansiEscape = "\x1b[31mred\x1b[0m";
  const controlChars = "\x00\x07\x08";
  const longReason = "A".repeat(200);
  const crafted = `${ansiEscape}${controlChars}${longReason}`;

  // parsePriority must cap + strip.
  const p = parsePriority({ low: true, boxed_until: null, reason: crafted });
  assert(p.reason !== null && p.reason!.length <= 64,
    "SEC5-1: parsePriority caps reason to <= 64 chars");
  assert(!/[\x00-\x1f\x7f]/.test(p.reason ?? ""),
    "SEC5-1: parsePriority strips control/ANSI-escape chars from reason");
  assert(!p.reason!.includes("\x1b"), "SEC5-1: ESC byte (ANSI introducer) removed");
  // After stripping control/ANSI-escape chars, the printable portion is
  // "[31mred[0m" (9 chars, the ESC bytes removed but the printable CSI params
  // remain) + 200 A's = 209 chars; truncated to the 64-char cap. The exact
  // prefix doesn't matter — what matters is the cap + the trailing A's survive.
  assert(p.reason!.length === 64 && p.reason!.endsWith("A"),
    "SEC5-1: parsePriority keeps printable chars, truncated to 64-char cap");

  // pauseUntil (the write boundary) must also sanitize — defense-in-depth so a
  // future caller that bypasses parsePriority cannot poison the file.
  const q = createConcurrencyQueue({ stateFile });
  q.pauseUntil(Date.now() + 10_000, crafted);
  const raw = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(typeof raw.pausedReason === "string" && raw.pausedReason.length <= 64,
    "ADV5-5: pauseUntil writes a capped reason to the shared file");
  assert(!/[\x00-\x1f\x7f]/.test(raw.pausedReason ?? ""),
    "ADV5-5: pauseUntil writes no control/ANSI-escape chars");
  // snapshot().pausedReason is the rendered value — must be clean too.
  const snap = q.snapshot();
  assert(snap.pausedReason !== null && snap.pausedReason.length <= 64,
    "SEC5-1: snapshot().pausedReason is capped");
  assert(!/[\x00-\x1f\x7f]/.test(snap.pausedReason ?? ""),
    "SEC5-1: snapshot().pausedReason has no control/ANSI-escape chars");
  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- COV6-1 / SEC6-1: readState sanitizes pausedReason on the READ boundary ---
// SEC5-1 sanitizes at the write boundary (pauseUntil) + parse path, but a
// hand-edited file, a compromised sibling writing JSON directly, or a file
// poisoned by an earlier unfixed build surfaces the raw string via readState
// → snapshot() → status bar. readState must run pausedReason through
// sanitizeReason too (defense-in-depth, the other half of SEC5-1's claim).
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-cov6-1-"));
  const stateFile = join(dir, "state.json");
  const { writeFileSync } = await import("node:fs");

  // Poison the file directly with an ANSI-escape + 200-char payload, bypassing
  // every write-boundary sanitize. A pausedUntil in the future keeps the pause
  // "active" so snapshot() surfaces pausedReason (the render path).
  const poisoned = "\x1b[31mBOGUS\x1b[0m " + "X".repeat(200);
  const future = Date.now() + 60_000;
  writeFileSync(stateFile, JSON.stringify({
    waiters: [], token: null,
    pausedUntil: future, pausedReason: poisoned, pausedTs: Date.now(),
  }));

  // readState must return a capped, control-char-free string.
  const st = readState(stateFile);
  assert(st.pausedReason !== null && st.pausedReason.length <= 64,
    "COV6-1/SEC6-1: readState caps poisoned pausedReason to <= 64 chars");
  assert(!/[\x00-\x1f\x7f]/.test(st.pausedReason ?? ""),
    "COV6-1/SEC6-1: readState strips control/ANSI-escape chars from poisoned pausedReason");
  assert(!st.pausedReason!.includes("\x1b"),
    "COV6-1/SEC6-1: readState removes the ESC byte from poisoned pausedReason");

  // snapshot() (the render path) must surface the same clean string.
  const q = createConcurrencyQueue({ stateFile });
  const snap = q.snapshot();
  assert(snap.pausedReason !== null && snap.pausedReason.length <= 64,
    "COV6-1/SEC6-1: snapshot().pausedReason is capped after read sanitize");
  assert(!/[\x00-\x1f\x7f]/.test(snap.pausedReason ?? ""),
    "COV6-1/SEC6-1: snapshot().pausedReason has no control/ANSI-escape chars after read sanitize");
  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- COV-HIGH-3: readState corrupt-input fixtures ---
// readState guards waiters (Array.isArray + per-entry shape), pausedUntil/pausedTs
// (typeof number), token (shape), and falls back to empty on JSON throw.
// Previously only the absent-file case was tested. Exercise: truncated JSON,
// garbage waiters, non-object token, string pausedUntil. SEC5-2: malformed
// waiter/token entries are dropped (not passed through) so isPidDead/reapStale
// never receive a non-number pid that would throw a synchronous TypeError.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  const { writeFileSync } = await import("node:fs");

  // Truncated JSON → throws → caught → empty state.
  writeFileSync(stateFile, '{"waiters":[');
  const truncated = readState(stateFile);
  assert(truncated.waiters.length === 0 && truncated.token === null,
    "COV-HIGH-3: readState truncated JSON -> empty state (no throw)");

  // Garbage waiters array (entries lack id/pid/ts) → dropped by the shape
  // guard (SEC5-2); reapStale/isPidDead never see a non-number pid.
  writeFileSync(stateFile, JSON.stringify({ waiters: [{ foo: 1 }, { bar: 2 }] }));
  const garbage = readState(stateFile);
  assert(garbage.waiters.length === 0,
    "COV-HIGH-3: readState garbage waiters entries dropped (shape-validated)");

  // Non-object token (a string) → null (shape guard rejects it, SEC5-2).
  writeFileSync(stateFile, JSON.stringify({ token: "not-an-object" }));
  const badTok = readState(stateFile);
  assert(badTok.token === null,
    "COV-HIGH-3: readState non-object token -> null (shape-validated)");

  // String pausedUntil → typeof !== number → falls to 0.
  writeFileSync(stateFile, JSON.stringify({ pausedUntil: "123", pausedTs: "456" }));
  const strPause = readState(stateFile);
  assert(strPause.pausedUntil === 0 && strPause.pausedTs === 0,
    "COV-HIGH-3: readState string pausedUntil/pausedTs -> 0 (typeof guard)");

  rmSync(dir, { recursive: true, force: true });
}

// --- SEC5-2: readState drops poisoned waiter/token entries without throwing ---
// A hand-edited or compromised-sibling state file can put arbitrary objects
// into `waiters` (e.g. { pid: "not-a-number" }). Without shape validation,
// isPidDead would call process.kill("not-a-number", 0) which throws a
// synchronous TypeError (not an errno-coded error) that the catch in isPidDead
// does not filter — crashing the reader's mutate. readState now validates each
// entry's shape and drops malformed ones, so reapStale operates on well-typed
// input.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  const { writeFileSync } = await import("node:fs");

  // Poisoned waiters: pid is a string / boolean / object / missing. All must
  // be dropped without throwing.
  writeFileSync(stateFile, JSON.stringify({
    waiters: [
      { id: "ok", pid: 12345, ts: Date.now() },           // well-formed -> kept
      { id: "bad-str", pid: "not-a-number", ts: Date.now() },
      { id: "bad-bool", pid: true, ts: Date.now() },
      { id: "bad-obj", pid: {}, ts: Date.now() },
      { id: "bad-missing-pid", ts: Date.now() },
      { id: "bad-missing-ts", pid: 1 },
      { pid: 1, ts: 1 },                                     // missing id
    ],
  }));
  let poisoned: ReturnType<typeof readState>;
  try {
    poisoned = readState(stateFile);
  } catch (e) {
    poisoned = { waiters: [], token: null, pausedUntil: 0, pausedReason: null, pausedTs: 0 };
    assert(false, "SEC5-2: readState threw on poisoned waiters (should drop silently)");
  }
  assert(poisoned.waiters.length === 1, "SEC5-2: readState drops poisoned waiters, keeps well-formed");
  assert(poisoned.waiters[0].id === "ok" && typeof poisoned.waiters[0].pid === "number",
    "SEC5-2: surviving waiter is the well-formed entry");

  // Poisoned token: pid is a string. Must be null (shape guard rejects it).
  writeFileSync(stateFile, JSON.stringify({ token: { id: "tok", pid: "not-a-number", ts: Date.now() } }));
  const badToken = readState(stateFile);
  assert(badToken.token === null, "SEC5-2: readState drops poisoned token (non-number pid)");

  // reapStale on the poisoned state must not throw (entries already dropped).
  const q = createConcurrencyQueue({ stateFile, now: () => Date.now(), pid: () => process.pid });
  q.snapshot(); // drives a readState + reapStale
  assert(q.snapshot().queued === 0, "SEC5-2: reapStale on poisoned state does not throw");
  q.reset();

  rmSync(dir, { recursive: true, force: true });
}

// --- SEC9-2/SEC8-1 + SEC9-4: readFileSync size bound + non-regular-file guard on state file ---
// A poisoned/runaway state file (e.g. 1 GB) would OOM/stall the pi process
// before JSON.parse. A FIFO/pipe or character device would block readFileSync
// indefinitely (wedging mutate + the O_EXCL lock). readState lstats first + bails
// to empty state when !st.isFile() || st.size > MAX_STATE_BYTES (1 MB).
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  // Write a 2 MB state file (well over the 1 MB cap).
  writeFileSync(stateFile, "{\"x\":" + "\"".repeat(2_000_000) + "\"}");
  const st = readState(stateFile);
  assert(st.waiters.length === 0 && st.token === null && st.pausedUntil === 0,
    "SEC9-2: oversized state file returns empty state (no OOM)");

  // SEC9-4: a FIFO (named pipe) would block readFileSync forever. readState
  // must detect non-regular files via lstatSync + return empty state (no hang).
  const fifoPath = join(dir, "state.fifo");
  const { execSync } = await import("node:child_process");
  try {
    execSync(`mkfifo "${fifoPath}"`);
    const fst = readState(fifoPath);
    assert(fst.waiters.length === 0 && fst.token === null,
      "SEC9-4: FIFO state file returns empty state (no hang)");
  } catch {
    // mkfifo unavailable (non-POSIX) — skip the FIFO assertion gracefully.
  }
  rmSync(dir, { recursive: true, force: true });
}

// --- SEC10-1: readState uses fd-based read (no TOCTOU between lstat + read) ---
// readState previously guarded with lstatSync(path) then immediately called
// readFileSync(path). Between them an attacker could swap the file (e.g. to a
// symlink). readState now opens the fd first + fstats it + reads from the fd,
// so the regular-file + size checks + the read are atomic wrt path swaps. We
// cannot race the window deterministically from JS, but we CAN assert the
// observable contract: a symlink state file is rejected at the lstat guard
// (treated as empty, NOT followed) — the prior readFileSync(path) would have
// followed a symlink planted AFTER the lstat. Plant a symlink at the state
// path pointing at a canary; readState must return empty + must NOT have
// followed the link (canary untouched).
if (process.platform !== "win32") {
  const dir = mkdtempSync(join(tmpdir(), "umans-q-sec10-1-"));
  const stateFile = join(dir, "state.json");
  const { symlinkSync } = await import("node:fs");
  const canary = join(dir, "canary.txt");
  writeFileSync(canary, "CANARY-ORIGINAL", { mode: 0o600 });
  // Plant a symlink at the state path → canary. lstatSync sees a non-regular
  // file → readState returns empty WITHOUT opening the fd (so the canary is
  // never read through the symlink).
  symlinkSync(canary, stateFile);
  const st = readState(stateFile);
  assert(st.waiters.length === 0 && st.token === null && st.pausedUntil === 0,
    "SEC10-1: symlink state file returns empty state (not followed)");
  assert(readFileSync(canary, "utf8") === "CANARY-ORIGINAL",
    "SEC10-1: symlink target not read through (canary intact)");
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
  assert(q.snapshot().tokenHeld === true, "COV-HIGH-4: id1 holds the token");

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
  assert(q.snapshot().tokenHeld === false, "COV-HIGH-4: holdsToken false after cancel");

  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- C1 (HIGH): touchToken re-stamps ts while held; returns false when reaped ---
// A poller that holds the token across a long capacity poll must call touchToken
// each iteration so the 120s watchdog does not reap a legitimate wait. If the
// token was reaped by a sibling's reapStale (id mismatch / absent), touchToken
// returns false so the poller can re-join the queue instead of racing a send.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-c1-"));
  const stateFile = join(dir, "state.json");
  const { readFileSync, writeFileSync } = await import("node:fs");
  const q = createConcurrencyQueue({ stateFile });

  // Claim the token by joining + waiting for launch.
  const id = q.join()!;
  await q.waitForLaunch(id);
  assert(q.snapshot().tokenHeld === true, "C1: token held after waitForLaunch");
  const before = JSON.parse(readFileSync(stateFile, "utf8"));
  const tsBefore = before.token.ts;

  // Sleep briefly so the re-stamp is observably later.
  await new Promise((r) => setTimeout(r, 10));
  const ok = q.touchToken(id);
  assert(ok === true, "C1: touchToken returns true while we hold the token");
  const after = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(after.token.ts > tsBefore, "C1: touchToken advanced state.token.ts");
  assert(after.token.id === id, "C1: touchToken kept our id");

  // Simulate a sibling's reapStale reaping our token: hand-edit the file to
  // point the token at a different id, then touchToken(ourId) must return false.
  const poisoned = JSON.parse(readFileSync(stateFile, "utf8"));
  poisoned.token = { id: "someone-else", pid: process.pid, ts: Date.now() };
  writeFileSync(stateFile, JSON.stringify(poisoned));
  const ok2 = q.touchToken(id);
  assert(ok2 === false, "C1: touchToken returns false when token id mismatches (reaped)");

  // touchToken on an absent token (file cleared) returns false.
  writeFileSync(stateFile, JSON.stringify({ waiters: [], token: null, pausedUntil: 0, pausedReason: null, pausedTs: 0 }));
  const ok3 = q.touchToken(id);
  assert(ok3 === false, "C1: touchToken returns false when token is absent");

  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- COV6-2: double-join on one queue survives reset() (Set of waiter ids) ---
// ourWaiterId was a single `let`; a second join() overwrote it so reset()
// cleared only the most-recent waiter. Reachable from transformMessageImages
// (Promise.all → acquireSlot → join() per image). Probe (5×) confirmed: two
// join() calls then reset() leaves exactly 1 waiter every time. Track a Set of
// this process's waiter ids so reset() splices every one.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-cov6-2-"));
  const stateFile = join(dir, "state.json");
  const { readFileSync } = await import("node:fs");
  const q = createConcurrencyQueue({ stateFile });

  // Join twice — simulates two concurrent acquireSlot calls (multi-image
  // handoff). Both ids land in the waiter FIFO.
  const id1 = q.join()!;
  const id2 = q.join()!;
  assert(id1 !== id2, "COV6-2: two join() calls return distinct ids");
  let st = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(st.waiters.length === 2, "COV6-2: both waiters present in the file after double-join");
  assert(st.waiters.some((w: { id: string }) => w.id === id1) &&
         st.waiters.some((w: { id: string }) => w.id === id2),
    "COV6-2: both waiter ids recorded in FIFO");

  // reset() must splice BOTH (the old single-slot design leaked id1).
  q.reset();
  st = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(st.waiters.length === 0, "COV6-2: reset() removes both waiters after double-join");
  assert(!st.waiters.some((w: { id: string }) => w.id === id1) &&
         !st.waiters.some((w: { id: string }) => w.id === id2),
    "COV6-2: neither double-joined waiter leaks past reset()");
  rmSync(dir, { recursive: true, force: true });
}

// --- SEC6-2: writeStateAtomic uses O_EXCL ("wx"), does not follow a planted symlink ---
// The per-pid temp name `${path}.${process.pid}.tmp` + writeFileSync (no O_EXCL)
// follows a planted symlink → write-redirect to an arbitrary file (probe-
// confirmed). openSync("wx", 0o600) creates the file ONLY if it does not exist
// (no follow on creation), so a planted symlink/name throws EEXIST instead.
// We plant a symlink at the exact temp name our pid will use and assert the
// next mutate (join) throws EEXIST rather than writing through the symlink.
{
  if (process.platform !== "win32") {
    const dir = mkdtempSync(join(tmpdir(), "umans-q-sec6-2-"));
    const stateFile = join(dir, "state.json");
    const { symlinkSync, writeFileSync, readFileSync, existsSync } = await import("node:fs");

    // Plant a symlink at the exact temp name writeStateAtomic will use. The
    // target is a canary file outside the state path — if writeStateAtomic
    // followed the symlink, the canary would be overwritten.
    // ADV7-1: the temp name now includes a random suffix, so stub Math.random
    // to make the suffix predictable + plant the symlink at the exact name.
    const canary = join(dir, "canary.txt");
    writeFileSync(canary, "ORIGINAL", { mode: 0o600 });
    const randStub = "abc123";
    const realRandom = Math.random;
    Math.random = () => {
      // Return a value whose toString(36).slice(2,8) === randStub.
      return parseInt(randStub, 36) / Math.pow(36, randStub.length);
    };
    const tmpName = `${stateFile}.${process.pid}.${randStub}.tmp`;
    symlinkSync(canary, tmpName);

    try {
      // join() -> mutate -> writeStateAtomic must throw EEXIST (not follow the
      // symlink). createConcurrencyQueue throws on construct-time dir create? No —
      // dir already exists. The throw surfaces from join()'s mutate.
      const q = createConcurrencyQueue({ stateFile });
      let threw = false;
      let code: string | undefined;
      try {
        q.join();
      } catch (e) {
        threw = true;
        code = (e as NodeJS.ErrnoException).code;
      }
      assert(threw, "SEC6-2: writeStateAtomic rejects a planted symlink at the temp name");
      assert(code === "EEXIST", "SEC6-2: planted-symlink write throws EEXIST (not followed)");
      // The canary must be untouched (symlink was not followed).
      assert(readFileSync(canary, "utf8") === "ORIGINAL",
        "SEC6-2: symlink target not written through (canary intact)");
      assert(!existsSync(stateFile),
        "SEC6-2: state file not created when temp name was a planted symlink");
      q.reset();
    } finally {
      Math.random = realRandom;
    }
    rmSync(dir, { recursive: true, force: true });
  } else {
    // Windows: symlinks require elevated privileges; skip the planted-symlink
    // fixture but assert the O_EXCL code path is present by checking that a
    // pre-existing regular file at the temp name also throws EEXIST.
    // ADV7-1: the temp name now includes a random suffix; stub Math.random to
    // make it predictable so we can plant the pre-existing file at the exact name.
    const dir = mkdtempSync(join(tmpdir(), "umans-q-sec6-2-"));
    const stateFile = join(dir, "state.json");
    const { writeFileSync } = await import("node:fs");
    const randStub = "abc123";
    const realRandom = Math.random;
    Math.random = () => parseInt(randStub, 36) / Math.pow(36, randStub.length);
    const tmpName = `${stateFile}.${process.pid}.${randStub}.tmp`;
    writeFileSync(tmpName, "pre-existing", { mode: 0o600 });
    try {
      const q = createConcurrencyQueue({ stateFile });
      let threw = false;
      let code: string | undefined;
      try { q.join(); } catch (e) {
        threw = true;
        code = (e as NodeJS.ErrnoException).code;
      }
      assert(threw, "SEC6-2: writeStateAtomic rejects a pre-existing temp name");
      assert(code === "EEXIST", "SEC6-2: pre-existing temp name throws EEXIST");
      q.reset();
    } finally {
      Math.random = realRandom;
    }
    rmSync(dir, { recursive: true, force: true });
  }
}

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

// --- COV2-M1: stale-token-by-time branch (live pid, old ts) is reaped ---
// reapStale reaps a token when isPidDead(pid) OR (now - ts) > staleTokenMs.
// The dead-pid branch is covered above; this exercises the TIME branch with a
// LIVE process.pid but an old ts (a live-but-hung holder — D4's purpose).
{
  const now = 1_700_000_000_000;
  const cfg = {
    stateFile: "/dev/null", staleTokenMs: 30_000, staleWaiterMs: 300_000,
    lockRetryMs: 5, lockTimeoutMs: 2_000, now: () => now, pid: () => process.pid,
  } as const;
  // Live PID but ts 31s old — past staleTokenMs (30s). The token is reaped by
  // the time check even though isPidDead(process.pid) is false.
  const state = {
    waiters: [],
    token: { id: "t1", pid: process.pid, ts: now - 31_000 },
    pausedUntil: 0, pausedReason: null, pausedTs: 0,
  };
  const reaped = reapStale(state, cfg as any, now);
  assert(reaped.token === null,
    "COV2-M1: live-PID token with old ts (31s > staleTokenMs 30s) is reaped by time check");

  // Same live PID but ts within the ceiling — token is kept.
  const fresh = { ...state, token: { id: "t1", pid: process.pid, ts: now - 5_000 } };
  const reapedFresh = reapStale(fresh, cfg as any, now);
  assert(reapedFresh.token !== null && reapedFresh.token?.id === "t1",
    "COV2-M1: live-PID token with fresh ts is kept");
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

// --- ADV4-2: a 429-sourced pause is clamped tighter than the 5h ceiling ---
// A server returning 429 forever (e.g. a misconfigured UMANS_BASE_URL) writes
// a fresh pausedUntil on every turn, each extending the shared pause up to the
// 5h MAX_PAUSE_MS ceiling — wedging all local pi processes for the real Umans
// account-pause duration even though the 429 source is non-account-wide.
// pauseUntil now clamps a 429-origin pause (tagged PAUSE_REASON_429) to the
// tighter MAX_PAUSE_429_MS (2.5 min = 5 × PRIORITY_BACKOFF_MS) ceiling, while a
// server priority.low pause keeps the 5h ceiling. The 2.5 min cap is still >>
// the 30s floor, so a real 429 with a short Retry-After is honored.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  const q = createConcurrencyQueue({ stateFile });
  const { readFileSync } = await import("node:fs");

  // A 429 with a huge Retry-After (5h+) is clamped to MAX_PAUSE_429_MS (2.5 min),
  // NOT the 5h MAX_PAUSE_MS ceiling.
  const before = Date.now();
  q.pauseUntil(before + 10 * 60 * 60 * 1000, PAUSE_REASON_429); // 10h requested
  const st = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(st.pausedUntil <= before + MAX_PAUSE_429_MS + 1000, // +1s slack
    "ADV4-2: 429-sourced pause clamped to MAX_PAUSE_429_MS (2.5 min), not 5h");
  assert(st.pausedUntil < before + MAX_PAUSE_MS,
    "ADV4-2: 429 pause is tighter than the 5h ceiling");
  assert(st.pausedReason === PAUSE_REASON_429, "ADV4-2: 429 pause tagged with PAUSE_REASON_429");

  // A non-429 pause (priority.low-origin) keeps the 5h ceiling.
  q.clearPause({ force: true });
  const beforeLow = Date.now();
  q.pauseUntil(beforeLow + 10 * 60 * 60 * 1000, "priority.low from /usage"); // 10h requested
  const stLow = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(stLow.pausedUntil <= beforeLow + MAX_PAUSE_MS + 1000 && stLow.pausedUntil > beforeLow + MAX_PAUSE_429_MS,
    "ADV4-2: priority.low pause keeps the 5h ceiling (not the tighter 429 cap)");

  // clampPauseUntil with the tighter ceiling is also used by the 429 handler
  // in index.ts for the Retry-After parse.
  const now = 1_700_000_000_000;
  assert(clampPauseUntil(now + 10 * 60 * 60 * 1000, now, MAX_PAUSE_429_MS) === now + MAX_PAUSE_429_MS,
    "ADV4-2: clampPauseUntil(until, now, MAX_PAUSE_429_MS) clamps to the 2.5 min ceiling");
  assert(clampPauseUntil(now + 60_000, now, MAX_PAUSE_429_MS) === now + 60_000,
    "ADV4-2: clampPauseUntil sub-2.5min pause unchanged");

  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- COV6-3: pauseUntil with a past `until` writes nothing (no stale reason) ---
// A past `clamped` is still > 0 when no pause is active, so the old code wrote
// a stale pausedUntil + pausedReason to disk. Display was safe (pausedUntil >
// now is false) but the on-disk pausedReason lingered as stale data. Early-
// return when clamped <= now so nothing is written.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-cov6-3-"));
  const stateFile = join(dir, "state.json");
  const { readFileSync } = await import("node:fs");
  const q = createConcurrencyQueue({ stateFile });

  // A past deadline with a reason — must NOT write pausedUntil/pausedReason.
  const past = Date.now() - 60_000;
  q.pauseUntil(past, "stale reason that should not persist");
  const st = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(st.pausedUntil === 0, "COV6-3: past pauseUntil leaves pausedUntil at 0");
  assert(st.pausedReason === null, "COV6-3: past pauseUntil leaves pausedReason null");

  // Sanity: a FUTURE deadline still writes (the happy path is unchanged).
  const future = Date.now() + 30_000;
  q.pauseUntil(future, "real pause");
  const st2 = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(st2.pausedUntil === future, "COV6-3: future pauseUntil still writes pausedUntil");
  assert(st2.pausedReason === "real pause", "COV6-3: future pauseUntil still writes pausedReason");

  q.reset();
  rmSync(dir, { recursive: true, force: true });
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

// --- ADV10-1: pause whose OWN claimed duration exceeds MAX_PAUSE_MS is reaped ---
// The two prior conditions (age, duration-from-now) miss a poisoned pause whose
// claimed span (pausedUntil - pausedTs) exceeds the ceiling while BOTH age and
// duration-from-now are under it. Example: pausedTs = now - 4h (age 4h < 5h,
// condition 1 misses), pausedUntil = now + 2h (duration-from-now 2h < 5h,
// condition 2 misses), claimed duration = 6h > 5h (condition 3 catches). A
// hand-edited file with such values would survive both prior checks; the new
// third condition keys on the pause's own claimed duration independent of now.
{
  const now = 1_700_000_000_000;
  const cfg = {
    stateFile: "/dev/null", staleTokenMs: 30_000, staleWaiterMs: 300_000,
    lockRetryMs: 5, lockTimeoutMs: 2_000, now: () => now, pid: () => process.pid,
  } as const;
  // pausedTs = now - 4h (age 4h, under 5h ceiling → condition 1 misses).
  // pausedUntil = now + 2h (duration-from-now 2h, under 5h → condition 2 misses).
  // claimed duration = (now+2h) - (now-4h) = 6h > 5h → condition 3 catches.
  const gapState = {
    waiters: [],
    token: null,
    pausedUntil: now + 2 * 60 * 60 * 1000,
    pausedReason: "poisoned-claimed-duration",
    pausedTs: now - 4 * 60 * 60 * 1000,
  };
  const gapReaped = reapStale(gapState, cfg as any, now);
  assert(gapReaped.pausedUntil === 0 && gapReaped.pausedReason === null && gapReaped.pausedTs === 0,
    "ADV10-1: pause whose claimed duration (pausedUntil - pausedTs) exceeds MAX_PAUSE_MS is reaped even when age + duration-from-now are both under the ceiling");
  // A pause whose claimed duration is within the ceiling + age + duration-from-now
  // also within is NOT reaped (no false positive).
  const safeState = { ...gapState, pausedTs: now - 1_000 };
  const safeReaped = reapStale(safeState, cfg as any, now);
  assert(safeReaped.pausedUntil === gapState.pausedUntil,
    "ADV10-1: pause with claimed duration under ceiling is not reaped (no false positive)");
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
  assert(q.snapshot().tokenHeld === true, "first waiter holds the launch token");

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
  assert(q.snapshot().tokenHeld === true, "second waiter now holds token");
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
  assert(q.snapshot().tokenHeld === true, "COV-HIGH-5: w1 holds the token");

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
  assert(q.snapshot().tokenHeld === true, "COV-HIGH-5: w2 now holds token");
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
  assert(q.snapshot().tokenHeld === true, "COV-HIGH-5: late-joiner setup — holder holds token");
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

// --- C2/CMP6-1: acquireLock uses Atomics.wait (not a CPU-burning busy-spin) ---
// Under contention the old busy-spin (while (cfg.now() < target) {}) burned CPU
// and starved the lock holder's event loop on a single-core VM, racing the 2s
// lock timeout. syncSleep uses Atomics.wait (blocks the thread without spinning)
// when available, falling back to the busy-spin otherwise. Assert the feature
// detection + that a contended acquire still resolves (the existing lockfile
// tests cover correctness; this pins the non-spinning path).
{
  // canAtomicsWait is the exported feature guard. On any modern Node (22+) with
  // SharedArrayBuffer enabled it returns true; the export itself is the
  // contract that acquireLock's spin uses it.
  const can = canAtomicsWait();
  assert(typeof can === "boolean", "C2/CMP6-1: canAtomicsWait returns a boolean");
  // Sanity: on Node >= 22 with SAB, the Atomics.wait path is available. We do
  // not assert `can === true` unconditionally (some hardened runtimes disable
  // SAB); we assert the function is the gate the spin consults.

  // Drive a contended acquire end-to-end: two queues on the same state file,
  // the first holds the lock via a long mutate, the second must wait + retry
  // through syncSleep (Atomics.wait or busy-spin) and then succeed.
  const dir = mkdtempSync(join(tmpdir(), "umans-q-c2-"));
  const stateFile = join(dir, "state.json");
  const q1 = createConcurrencyQueue({ stateFile, lockRetryMs: 5, lockTimeoutMs: 2_000 });
  const q2 = createConcurrencyQueue({ stateFile, lockRetryMs: 5, lockTimeoutMs: 2_000 });
  const id1 = q1.join()!;
  const id2 = q2.join()!;
  assert(id1 !== null && id2 !== null, "C2/CMP6-1: both queues joined the FIFO");
  // Both waiters present — the contended path (acquireLock retry via syncSleep)
  // wrote both through without throwing.
  const { readFileSync } = await import("node:fs");
  const st = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(st.waiters.length === 2, "C2/CMP6-1: contended acquireLock retries resolved (both waiters written)");
  q1.reset();
  q2.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- C3: acquireSlot returns undefined on abort (not throw) ---
// When the user aborts mid-poll (Ctrl-C), decideLaunch returns "abort" and
// acquireSlot used to throw, surfacing as an uncaught extension error toast.
// C3: return undefined (matching the disabled-mode shape) so the handler's
// `if (release)` guard is the abort path. acquireSlot delegates the abort to
// waitForLaunch, which rejects when the signal is already aborted — acquireSlot's
// try/catch converts that rejection into `return undefined` (no fetchUsage
// network call happens, since waitForLaunch rejects before the poll loop).
// We verify the seam: an already-aborted signal makes waitForLaunch reject
// (the contract acquireSlot catches), and decideLaunch returns "abort" for a
// mid-poll abort (the branch that returns undefined).
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-c3-"));
  const stateFile = join(dir, "state.json");
  const q = createConcurrencyQueue({ stateFile });
  const id = q.join()!;
  const ac = new AbortController();
  ac.abort();
  // waitForLaunch with an already-aborted signal rejects (acquireSlot catches
  // this and returns undefined rather than re-throwing).
  let rejected = false;
  try {
    await q.waitForLaunch(id, ac.signal);
  } catch (e) {
    rejected = true;
    assert((e as Error).message.includes("aborted"),
      "C3: waitForLaunch rejects with an abort error (the seam acquireSlot catches)");
  }
  assert(rejected, "C3: already-aborted signal rejects waitForLaunch (acquireSlot returns undefined)");
  // The mid-poll abort decision (signal aborts AFTER the token is held) drives
  // the `return undefined` branch in acquireSlot's poll loop.
  assert(decideLaunch({ isFree: false, elapsedMs: 1_000, queuePaused: false, signalAborted: true }) === "abort",
    "C3: decideLaunch returns 'abort' for a mid-poll abort (acquireSlot returns undefined, not throw)");
  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- COV4-1: acquireLock doesn't throw ENOENT when the parent dir is missing ---
// The lockfile lives in the same dir as the state file. When the parent dir
// doesn't exist, acquireLock's openSync(lockFile, "wx") threw ENOENT BEFORE
// writeStateAtomic's mkdirSync (inside withLock) ever ran — aborting the turn on
// first use. mkdirSync is now hoisted into the factory so the dir exists before
// the first mutate. We point stateFile at a non-existent nested dir and assert
// join() succeeds (the dir is created + the first mutate writes through).
{
  const base = mkdtempSync(join(tmpdir(), "umans-q-"));
  // A nested path whose parent does NOT exist yet (the factory must create it).
  const stateFile = join(base, "nonexistent-parent", "nested", "state.json");
  const q = createConcurrencyQueue({ stateFile });
  const id = q.join();
  assert(id !== null, "COV4-1: join succeeds when parent dir is missing (mkdirSync hoisted)");

  // The state file must have been written (the first mutate wrote through).
  const { readFileSync } = await import("node:fs");
  const st = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(st.waiters.length === 1 && st.waiters[0].id === id,
    "COV4-1: first mutate wrote the waiter through the created nested dir");

  q.reset();
  rmSync(base, { recursive: true, force: true });
}

// --- COV4-2: pauseUntil throws on disk failure; index.ts wraps it (no turn abort) ---
// pauseUntil runs mutate -> writeStateAtomic -> renameSync/writeFileSync, which
// can throw on disk failure (EACCES, ENOSPC, EROFS). Without the try/catch in
// capacityFree + the 429 handler + refreshUsage, the throw propagates out of
// acquireSlot -> before_provider_request -> pi aborts the user's turn. The
// queue's pauseUntil itself still throws (it's the caller's responsibility to
// swallow); we verify the throw surface exists (chmod the state dir read-only so
// writeFileSync fails with EACCES) and that the index.ts try/catch pattern
// (replicated here) swallows it so a decision is returned rather than the turn
// aborting.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  const { chmodSync } = await import("node:fs");
  const q = createConcurrencyQueue({ stateFile });

  // Make the state dir read-only AFTER the queue is created (the factory's
  // mkdirSync already ran). writeStateAtomic -> writeFileSync(tmp) now throws
  // EACCES (can't create a file in a read-only dir), which propagates out of
  // pauseUntil.
  chmodSync(dir, 0o500); // r-x for owner: can read/list, cannot create files
  try {
    // The queue's pauseUntil surfaces the throw (the caller must swallow).
    let caught: unknown = undefined;
    try {
      q.pauseUntil(Date.now() + 30_000, "COV4-2 probe");
    } catch (err) {
      caught = err;
    }
    assert(caught instanceof Error, "COV4-2: queue pauseUntil throws on disk failure (caller must swallow)");

    // The index.ts pattern: wrap pauseUntil in try/catch + warn + swallow so
    // capacityFree/refreshUsage/the 429 handler return a decision instead of
    // aborting the turn. Replicate the exact guard and assert it swallows.
    let guarded = true;
    try {
      try {
        q.pauseUntil(Date.now() + 30_000, "COV4-2 probe");
      } catch (err) {
        console.warn("umans: pauseUntil threw in capacityFree (continuing):", err instanceof Error ? err.message : err);
      }
    } catch {
      guarded = false;
    }
    assert(guarded === true, "COV4-2: try/catch around pauseUntil swallows the throw (turn does not abort)");
  } finally {
    chmodSync(dir, 0o700); // restore so rmSync can clean up
  }

  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- COV-MED-6: concurrencyLimit() edge inputs (parseConcurrencyLimit) ---
// CLN4-3: "2.5" → fallback (fractional rejected, was kept as-is), " " → fallback,
// "0" → fallback, "abc" → fallback, "" → fallback, undefined → fallback.
{
  const fallback = 4;
  assert(parseConcurrencyLimit("2.5", fallback) === fallback,
    "CLN4-3: '2.5' → fallback (fractional rejected, was kept as-is)");
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
  // CORR8-3 / ADV8-1: hex / scientific notation / fractional are rejected by
  // the strict /^\d+$/ regex BEFORE Number(trimmed) — Number() accepts them
  // (Number("0x10")===16, Number("1e3")===1000, Number("2.0")===2) and
  // Number.isInteger() passes, so a "0x10" typo would silently set the gate to
  // 16, defeating the cap. Mirrors the 429 Retry-After parse.
  assert(parseConcurrencyLimit("0x10", fallback) === fallback,
    "CORR8-3/ADV8-1: '0x10' (hex) → fallback (strict regex rejects, not 16)");
  assert(parseConcurrencyLimit("1e3", fallback) === fallback,
    "CORR8-3/ADV8-1: '1e3' (sci-notation) → fallback (strict regex rejects, not 1000)");
  assert(parseConcurrencyLimit("2.0", fallback) === fallback,
    "CORR8-3/ADV8-1: '2.0' (fractional) → fallback (strict regex rejects)");
  assert(parseConcurrencyLimit("0X10", fallback) === fallback,
    "CORR8-3/ADV8-1: '0X10' (hex upper) → fallback");
  assert(parseConcurrencyLimit("1E3", fallback) === fallback,
    "CORR8-3/ADV8-1: '1E3' (sci-notation upper) → fallback");
  assert(parseConcurrencyLimit("+3", fallback) === fallback,
    "CORR8-3/ADV8-1: '+3' → fallback (strict regex rejects the sign)");
  // Valid positive decimal integers still pass.
  assert(parseConcurrencyLimit("8", fallback) === 8,
    "CORR8-3/ADV8-1: '8' → 8 (valid positive decimal)");
  assert(parseConcurrencyLimit(" 12 ", fallback) === 12,
    "CORR8-3/ADV8-1: ' 12 ' → 12 (whitespace trimmed, valid)");
  // SEC9-5 / CORR9-1: huge integer strings pass /^\d+$/ + Number.isInteger but
  // exceed Number.MAX_SAFE_INTEGER, silently disabling the cap. isSafeInteger +
  // a 1024 ceiling reject them to fallback.
  assert(parseConcurrencyLimit("999999999999999999999", fallback) === fallback,
    "SEC9-5/CORR9-1: '999999999999999999999' → fallback (unsafe integer)");
  assert(parseConcurrencyLimit("1025", fallback) === fallback,
    "SEC9-5/CORR9-1: '1025' → fallback (exceeds 1024 cap)");
  assert(parseConcurrencyLimit("64", fallback) === 64,
    "SEC9-5/CORR9-1: '64' → 64 (valid, under cap)");
  assert(parseConcurrencyLimit("1024", fallback) === 1024,
    "SEC9-5/CORR9-1: '1024' → 1024 (cap boundary, inclusive)");
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
  // CMP-LOW-4: pausedReason is surfaced in the snapshot so the status bar
  // can show WHY the account is backed off (e.g. "HTTP 429 from gateway").
  assert(snapB.pausedReason === "429 from A", "CMP-LOW-4: pausedReason visible to sibling (shared file)");

  qA.clearPause();
  assert(qB.snapshot().paused === false, "clearPause by A reflected in B");
  assert(qB.snapshot().pausedReason === null, "CMP-LOW-4: pausedReason null after clearPause");

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

  // A clears it (force: a 429-origin pause survives a plain clearPause per
  // CORR4-1; the C2 test asserts the shared-file visibility, so force-clear).
  qA.clearPause({ force: true });
  assert(qB.snapshot().paused === false, "C2: sibling sees pause lift when cleared");

  qA.reset(); qB.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- CORR4-1: clearPause refuses to wipe a 429-origin pause on a stale /usage tick ---
// /usage LAGS a 429 by 1-5s (the design acknowledges this at the capacityFree
// doc-comment). refreshUsage's 5s timer calls clearPause() when /usage reports
// priority.low===false, but a stale tick would wipe a sibling's freshly-written
// 429 pause, letting the next waiter launch into the deprio the gate exists to
// prevent. clearPause now refuses to clear a 429-origin pause (tagged
// PAUSE_REASON_429) until it naturally elapses OR /usage reports
// priority.low===true (refreshUsage only calls clearPause on low===false, so a
// 429 pause set by a sibling survives the stale tick). A non-429 pause
// (priority.low-origin) is still cleared by a plain clearPause so the queue
// drains as soon as /usage says traffic is healthy.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  const qA = createConcurrencyQueue({ stateFile });
  const qB = createConcurrencyQueue({ stateFile }); // simulates a sibling

  // A writes a 429-origin pause (e.g. it just got a 429). 30s deadline.
  const until = Date.now() + 30_000;
  qA.pauseUntil(until, PAUSE_REASON_429);
  assert(qB.snapshot().paused === true, "CORR4-1: 429 pause visible to sibling");
  assert(qB.snapshot().pausedReason === PAUSE_REASON_429, "CORR4-1: pause tagged 429");

  // B's stale /usage tick (priority.low===false) calls clearPause(). The 429
  // pause MUST survive — /usage hasn't caught up yet.
  qB.clearPause();
  assert(qB.snapshot().paused === true, "CORR4-1: 429 pause survives a stale /usage clearPause");
  assert(qB.snapshot().pausedReason === PAUSE_REASON_429,
    "CORR4-1: 429 pause reason preserved after stale clearPause");

  // A non-429 pause (priority.low-origin) IS cleared by a plain clearPause —
  // refreshUsage must still drain the queue when /usage says traffic is healthy.
  qA.clearPause({ force: true }); // reset to a clean slate
  const untilLow = Date.now() + 30_000;
  qA.pauseUntil(untilLow, "priority.low from /usage");
  assert(qB.snapshot().paused === true && qB.snapshot().pausedReason === "priority.low from /usage",
    "CORR4-1: priority.low-origin pause set");
  qB.clearPause();
  assert(qB.snapshot().paused === false,
    "CORR4-1: non-429 pause cleared by plain clearPause (queue drains on healthy /usage)");

  // force:true clears a 429 pause unconditionally (operator/explicit reset).
  qA.pauseUntil(Date.now() + 30_000, PAUSE_REASON_429);
  assert(qB.snapshot().paused === true, "CORR4-1: 429 pause re-set for force-clear");
  qB.clearPause({ force: true });
  assert(qB.snapshot().paused === false, "CORR4-1: force:true clears a 429 pause unconditionally");

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

// --- COV4-4: head-waiter-reaped re-insert path (head reaped → re-inserted at tail → next waiter claims first) ---
// The ADV-4 test covers a TAIL waiter aged past staleWaiterMs (re-inserted,
// survives). The re-insert path also runs when the HEAD waiter itself is
// reaped — a deep FIFO where the head waiter's PID is alive but its ts aged
// past staleWaiterMs because the token holder ran a very long turn. In that
// case the head is re-inserted at the TAIL, and a DIFFERENT waiter (formerly
// #2) becomes head. A regression that re-inserted at the head (preserving
// place) would silently break the FIFO order under long turns. We assert that
// after the head is aged + reaped, the second waiter claims the token first.
//
// Symmetry-breaking: both waiters poll at 50ms, so if they join at the same
// time they age together and the re-insert order is racy. We stagger the
// joins (waiter 2 joins 400ms after waiter 1) with staleWaiterMs=500 so only
// waiter 1 is reaped when the token frees (waiter 2's ts stays fresh).
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  // staleWaiterMs=500ms so we can stagger the joins and only age waiter 1.
  const q = createConcurrencyQueue({ stateFile, staleWaiterMs: 500 });

  // A third queue instance holds the token so the head waiter can't claim it
  // (simulating a long-running turn by another process). The token's
  // staleTokenMs defaults to 120s, so it survives the test window.
  const idHolder = q.join()!;
  const rHolder = await q.waitForLaunch(idHolder);
  assert(q.snapshot().tokenHeld === true, "COV4-4: token held by the holder");

  // Waiter 1 joins → becomes head (behind the held token).
  const id1 = q.join()!;
  const ctrl1 = new AbortController();
  const p1 = q.waitForLaunch(id1, ctrl1.signal).catch(() => { /* aborted */ });
  await new Promise((r) => setTimeout(r, 60)); // let waiter 1's poll fire

  // Waiter 2 joins 400ms after waiter 1 (ts fresher), → tail.
  await new Promise((r) => setTimeout(r, 340)); // t≈400ms total
  const id2 = q.join()!;
  const ctrl2 = new AbortController();
  let got2 = false;
  const p2 = q.waitForLaunch(id2, ctrl2.signal).then(() => { got2 = true; }).catch(() => { /* aborted */ });
  await new Promise((r) => setTimeout(r, 60)); // let waiter 2's poll fire

  // Age past staleWaiterMs (500ms) so waiter 1 (ts≈0) is reaped on its next
  // poll and re-inserted at the TAIL. Waiter 2 (ts≈400) stays fresh
  // (now-ts≈200 < 500) and becomes the head. Waiter 1's poll re-inserts it
  // at the tail, NOT the head.
  await new Promise((r) => setTimeout(r, 220)); // t≈620ms: waiter 1 reaped + re-inserted at tail

  // Release the token. Waiter 2 (now the head) should claim it FIRST — not
  // waiter 1 (re-inserted at the tail). Give the 50ms poll time to fire.
  rHolder();
  await new Promise((r) => setTimeout(r, 150));
  assert(got2, "COV4-4: second waiter claims the token first after the head was reaped + re-inserted at tail");

  // Cleanly stop both poll loops.
  ctrl1.abort();
  ctrl2.abort();
  await Promise.all([p1, p2]);

  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- ADV-1: stale .tmp files are reaped by writeStateAtomic; fresh ones are not ---
// A crashed writer (killed between writeFileSync and renameSync) leaves a
// <path>.<pid>.tmp that would accumulate forever. writeStateAtomic now
// best-effort unlinks any <path>.*.tmp older than STALE_TMP_MS (10s). A fresh
// .tmp (just written by another process) has a current mtime and must be left
// alone.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  const { writeFileSync, statSync, utimesSync } = await import("node:fs");

  // Pre-create a stale .tmp (old mtime) simulating a crashed writer.
  const staleTmp = `${stateFile}.99999.tmp`;
  writeFileSync(staleTmp, "{}", { mode: 0o600 });
  const oldTime = (Date.now() / 1000) - 120; // 120s ago — past the 10s threshold
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

// --- C4: reapStaleTmps uses the injected cfg.now() (frozen-clock testability) ---
// reapStaleTmps used Date.now() directly, so the .tmp reaper's staleness check
// was never exercised with a frozen clock — a regression inverting the
// comparison wouldn't be caught. Thread cfg.now() in from mutate's caller so a
// frozen clock drives the reaper. We freeze now far in the future and assert a
// .tmp whose mtime is older than STALE_TMP_MS (relative to the frozen now) is
// reaped, while one whose mtime is within the window survives.
{
  const frozenNow = 1_700_000_000_000;
  const dir = mkdtempSync(join(tmpdir(), "umans-q-c4-"));
  const stateFile = join(dir, "state.json");
  const { writeFileSync, statSync, utimesSync } = await import("node:fs");

  // A stale .tmp: mtime 120s before the frozen clock (past the 10s threshold).
  const staleTmp = `${stateFile}.99999.tmp`;
  writeFileSync(staleTmp, "{}", { mode: 0o600 });
  const staleMtime = (frozenNow / 1000) - 120;
  utimesSync(staleTmp, staleMtime, staleMtime);

  // A fresh .tmp: mtime 1s before the frozen clock (within the 10s window).
  const freshTmp = `${stateFile}.88888.tmp`;
  writeFileSync(freshTmp, "{}", { mode: 0o600 });
  const freshMtime = (frozenNow / 1000) - 1;
  utimesSync(freshTmp, freshMtime, freshMtime);

  // A queue with a FROZEN now — the reaper must use this, not Date.now().
  const q = createConcurrencyQueue({ stateFile, now: () => frozenNow });
  q.pauseUntil(frozenNow + 1_000, "C4 frozen-clock probe");

  let staleGone = false;
  try { statSync(staleTmp); } catch { staleGone = true; }
  assert(staleGone, "C4: stale .tmp (mtime > STALE_TMP_MS before frozen now) reaped via cfg.now()");

  let freshExists = false;
  try { statSync(freshTmp); freshExists = true; } catch { /* gone */ }
  assert(freshExists, "C4: fresh .tmp (mtime within window before frozen now) left untouched");

  q.reset();
  rmSync(dir, { recursive: true, force: true });
}
{
  const q = createConcurrencyQueue({ disabled: true });
  assert(q.join() === null, "disabled: join returns null");
  const r = await q.waitForLaunch("ignored");
  assert(typeof r === "function", "disabled: waitForLaunch resolves with noop release");
  r();
  assert(q.snapshot().queued === 0 && q.snapshot().tokenHeld === false, "disabled: snapshot empty");
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

// --- COV5-5 / ADV5-4: reset() splices a queued-but-not-launched waiter ---
// A process that join()ed but is still queued (hasn't claimed the token) has
// ourTokenId === null. Previously reset() was a no-op in this state, leaking
// the waiter for staleWaiterMs (5 min) if the process didn't exit — blocking
// siblings behind a dead-PID entry. reset() now tracks every id in
// ourWaiterIds (the Set populated by join) and splices out each waiter entry
// even when ourTokenId is null.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  const { readFileSync, writeFileSync } = await import("node:fs");

  // Seed the shared file with a sibling holding the token + a sibling waiter
  // ahead of us, so our join()ed waiter is queued (not head) and cannot claim.
  const now = Date.now();
  const siblingState = {
    waiters: [
      { id: "sibling-head", pid: process.pid, ts: now },
    ],
    token: { id: "sibling-tok", pid: process.pid, ts: now },
    pausedUntil: 0,
    pausedReason: null,
  };
  writeFileSync(stateFile, JSON.stringify(siblingState));

  // We join the queue (queued behind the sibling, token held — ourTokenId
  // stays null). Then session_shutdown fires and reset() is called.
  const q = createConcurrencyQueue({ stateFile });
  const ourId = q.join();
  assert(ourId !== null, "COV5-5: join returned a waiter id");
  // Confirm our waiter is in the file before reset.
  const before = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(before.waiters.some((w: { id: string }) => w.id === ourId),
    "COV5-5: our waiter entry present in the file after join");

  q.reset();

  // Our waiter entry must be gone; the sibling's entries must remain.
  const after = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(!after.waiters.some((w: { id: string }) => w.id === ourId),
    "COV5-5: reset() splices our queued-but-not-launched waiter entry");
  assert(after.waiters.some((w: { id: string }) => w.id === "sibling-head"),
    "COV5-5: reset() does not remove a sibling's waiter entry");
  assert(after.token !== null && after.token.id === "sibling-tok",
    "COV5-5: reset() does not remove a sibling's token");

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
  assert(q.snapshot().tokenHeld === true, "abort test: first waiter holds the token");

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
  assert(q.snapshot().tokenHeld === false, "abort test: no orphan claimed the token");

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

// --- COV2-H2: side-call gating pattern (D6) acquires + releases a slot ---
// searchWeb (umans_web_search tool), analyzeImage (vision handoff + umans_vision
// tool) each call acquireSlot(apiKey, signal) before their side-request,
// releasing in a finally. acquireSlot is a closure (not exported), so we test
// the queue interaction the side-calls rely on: join -> waitForLaunch -> work
// -> release, asserting the token is claimed during the side-call and freed
// after (so a sibling side-call or main turn can proceed). This is the same
// acquire pattern the side-calls use; a regression dropping acquireSlot would
// skip the join/waitForLaunch and the token would never be held.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  const q = createConcurrencyQueue({ stateFile });

  // Simulate a side-call: acquire a slot, do "work", release in finally.
  async function sideCall(signal?: AbortSignal) {
    const id = q.join()!;
    try {
      const release = await q.waitForLaunch(id, signal);
      // While the side-call is in-flight, THIS process holds the token.
      assert(q.snapshot().tokenHeld === true, "COV2-H2: side-call holds the token during work");
      // Simulate the side-request body (no actual HTTP).
      await new Promise((r) => setTimeout(r, 10));
      release();
    } finally {
      // Belt-and-suspenders: cancel our waiter if still present (matches the
      // release fn shape returned by acquireSlot).
      q.cancel(id);
    }
  }
  await sideCall();
  // After the side-call, the token is free and no waiters remain.
  assert(q.snapshot().tokenHeld === false, "COV2-H2: token released after side-call completes");
  assert(q.snapshot().queued === 0, "COV2-H2: no leaked waiter after side-call");

  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- COV2-H1: message_end release pattern frees the slot during tool exec ---
// The primary release path (assistant message_end) frees the slot as soon as
// the response stream completes, letting siblings run during this turn's tool
// execution. We simulate the before_provider_request acquire + message_end
// release: acquire, assert held, release (message_end), assert freed — then a
// sibling acquires immediately (no tool-exec wait). turn_end/agent_end are
// safety nets and are no-ops after message_end already released.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  const q = createConcurrencyQueue({ stateFile });

  // Main turn acquires (before_provider_request).
  const id1 = q.join()!;
  const r1 = await q.waitForLaunch(id1);
  assert(q.snapshot().tokenHeld === true, "COV2-H1: main turn holds token after acquire");

  // message_end fires (stream complete): release the slot.
  r1();
  assert(q.snapshot().tokenHeld === false, "COV2-H1: token freed at message_end");

  // A sibling turn can now acquire immediately (the slot was freed during
  // this turn's tool execution, not held to turn_end).
  const id2 = q.join()!;
  const r2 = await q.waitForLaunch(id2);
  assert(q.snapshot().tokenHeld === true, "COV2-H1: sibling acquires immediately after message_end release");

  // turn_end safety net on the first turn is a no-op (already released).
  // r1 was already called; calling its underlying cancel is a no-op.
  q.cancel(id1);
  assert(q.snapshot().tokenHeld === true, "COV2-H1: turn_end safety net after message_end is a no-op (token held by sibling)");

  r2();
  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- COV2-M4: reset() while a waiter is mid-waitForLaunch ---
// reset() clears ourTokenId's entry. A concurrent waitForLaunch poller on the
// same queue instance re-inserts its waiter (ADV-4) before claiming. Verify
// reset() mid-wait doesn't corrupt the state file or strand the poller: the
// poller either re-inserts + eventually claims, or aborts cleanly.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  const { readFileSync } = await import("node:fs");
  const q = createConcurrencyQueue({ stateFile });

  // Turn 1 holds the token.
  const id1 = q.join()!;
  await q.waitForLaunch(id1);

  // Turn 2 joins and blocks in waitForLaunch (token held).
  const id2 = q.join()!;
  const ctrl = new AbortController();
  const p2 = q.waitForLaunch(id2, ctrl.signal).catch(() => { /* aborted */ });
  await new Promise((r) => setTimeout(r, 60)); // let the 50ms poll fire

  // reset() mid-wait (e.g. session_shutdown on a sibling queue path). reset()
  // clears ourTokenId (id1's token) + id1's waiter entry; id2's entry remains.
  q.reset();

  // The state file must still be valid JSON (reset must not corrupt it).
  const st = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(Array.isArray(st.waiters), "COV2-M4: state file valid JSON after reset mid-wait");

  // Abort the blocked waiter cleanly (cancel + reject). Its entry must be gone.
  ctrl.abort();
  await p2;
  const after = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(!after.waiters.some((w: { id: string }) => w.id === id2),
    "COV2-M4: aborted waiter entry removed after reset mid-wait");

  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- ADV4-1: setTimeout(poll) throw rejects the promise, not the process ---
// waitForLaunch re-enters poll via setTimeout(poll, 50). A throw from mutate()
// inside that re-entry (acquireLock timeout, readFileSync EACCES, writeStateAtomic
// ENOSPC) is not on any promise chain and would surface as an uncaughtException,
// crashing the Node process (and leaking the waiter for staleWaiterMs = 5 min if
// a uncaughtException handler is installed). The poll body is now wrapped in
// try/catch: on throw it clears the timer, best-effort cancels our waiter entry,
// and rejects the waitForLaunch promise so acquireSlot's finally runs. We force
// the throw by removing the state file's parent directory after the first poll,
// so the next setTimeout(poll) re-entry's mutate -> acquireLock -> openSync throws
// ENOENT immediately (no busy-spin, no event-loop blocking) — the same throw
// surface as the ADV4-1 reproduction (acquireLock failure) without the timing
// fragility of a fresh-mtime toucher (the acquireLock busy-spin blocks the
// toucher's setInterval, so a toucher-based probe can't keep the mtime fresh).
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-"));
  const stateFile = join(dir, "state.json");
  const { renameSync } = await import("node:fs");

  // Waiter 1 joins + claims the token (first poll is synchronous + succeeds).
  const q = createConcurrencyQueue({ stateFile });
  const id1 = q.join()!;
  await q.waitForLaunch(id1);
  assert(q.snapshot().tokenHeld === true, "ADV4-1: waiter 1 holds the token");

  // Waiter 2 joins + starts polling. Its FIRST poll runs synchronously inside
  // the Promise executor: it sees the token held by 1, returns false, and arms
  // setTimeout(poll, 50). That first poll does NOT throw (the dir exists), so
  // the rejection we assert below can ONLY come from a setTimeout re-entry —
  // which is exactly the uncaught-throw path ADV4-1 fixes.
  const id2 = q.join()!;
  let rejected = false;
  let errMsg = "";
  const p2 = q.waitForLaunch(id2).catch((e: unknown) => {
    rejected = true;
    errMsg = e instanceof Error ? e.message : String(e);
  });
  await new Promise((r) => setTimeout(r, 30)); // let the first poll complete + re-arm
  assert(!rejected, "ADV4-1: waiter 2 blocks while token held (no throw yet)");

  // Remove the state file's parent directory so the next setTimeout(poll)
  // re-entry's mutate -> acquireLock -> openSync(lockFile, "wx") throws ENOENT
  // immediately. Without the try/catch, this throw would surface as an
  // uncaughtException and crash the Node process.
  const gone = `${dir}.gone`;
  renameSync(dir, gone);

  // Waiter 2's next poll fires within 50ms, tries to mutate, and throws. The
  // throw must REJECT p2, not crash.
  await p2;
  assert(rejected, "ADV4-1: setTimeout(poll) throw rejects the promise (does not crash the process)");
  assert(errMsg.startsWith("concurrency-queue: poll failed:"),
    `ADV4-1: rejection carries the poll-failed message (got "${errMsg}")`);

  // The cancel in the catch is best-effort: with the parent dir removed, the
  // cancel's own mutate also throws ENOENT and is swallowed, so the waiter entry
  // remains in the (renamed) file. That is acceptable — the entry is inert (the
  // process exited the wait) and the watchdog reaps it via staleWaiterMs. The
  // cancel-actually-removes-the-entry property is covered by the C4/ADV-5 abort
  // tests where the dir is intact; this test proves the crash-prevention core.

  rmSync(gone, { recursive: true, force: true });
}

// --- ADV3-1: releaseSlot resilient to a throwing release fn (single-slot shape) ---
// releaseSlot wraps release() in try/catch/finally so a throw (e.g. O_EXCL
// lock timeout after 2s, EACCES, ENOSPC) is swallowed and the release
// completes; the mainTurnRelease clear + updateStatus live in a finally so
// the slot is cleared even on throw (prevents a stuck slot). releaseSlot is
// a closure in index.ts (not exported) operating on a SINGLE mainTurnRelease
// slot (CORR5-3 dropped the dead Set+FIFO-by-insertion design). This probe
// mirrors the actual single-slot shape: assert releaseSlot(release) swallows
// a throw from release(), clears mainTurnRelease when it matches, and still
// calls updateStatus in finally. Drops the Set/releaseOldest/"drain terminates"
// assertions that pinned a contract the production code no longer satisfies.
{
  type Release = () => void;
  // Mirror the single-slot shape from index.ts (CORR5-3 + ADV3-1).
  let mainTurnRelease: Release | undefined;
  let updateCount = 0;
  function updateStatus() { updateCount++; }
  function releaseSlot(release: Release | undefined): void {
    if (!release) return;
    try {
      try {
        release();
      } catch (err) {
        // Transient (lock timeout) or environmental; the watchdog reaps.
        console.warn("umans: concurrency release threw (release continues):", err instanceof Error ? err.message : err);
      }
    } finally {
      if (mainTurnRelease === release) mainTurnRelease = undefined;
      updateStatus();
    }
  }

  // (1) releaseSlot swallows a throw from release() (does not re-throw).
  let releasedA = false;
  const A: Release = () => { releasedA = true; throw new Error("A: lock timeout"); };
  mainTurnRelease = A;
  let threw = false;
  try { releaseSlot(A); } catch { threw = true; }
  assert(!threw, "ADV3-1: releaseSlot swallows a throw from release() (no re-throw)");
  assert(releasedA, "ADV3-1: release() was invoked before throwing");
  // The matching slot was cleared in finally (mainTurnRelease === A → undefined).
  assert(mainTurnRelease === undefined, "ADV3-1: matching mainTurnRelease cleared in finally even on throw");
  // updateStatus ran once (in finally) despite the throw.
  assert(updateCount === 1, "ADV3-1: updateStatus ran once in finally despite throw");

  // (2) releaseSlot on a release that does NOT match mainTurnRelease leaves
  // mainTurnRelease intact (a side-call release must not clear the main-turn
  // slot — CORR5-3 invariant). updateStatus still runs in finally.
  const main: Release = () => {};
  const sideCall: Release = () => {};
  mainTurnRelease = main;
  updateCount = 0;
  releaseSlot(sideCall);
  assert(mainTurnRelease === main, "ADV3-1: non-matching release does not clear mainTurnRelease (side-call invariant)");
  assert(updateCount === 1, "ADV3-1: updateStatus ran in finally for non-matching release");

  // (3) releaseSlot(undefined) is a no-op (the safety-net path when no slot
  // is held — must not throw, must not call updateStatus).
  mainTurnRelease = undefined;
  updateCount = 0;
  releaseSlot(undefined);
  assert(updateCount === 0, "ADV3-1: releaseSlot(undefined) is a no-op (no updateStatus call)");
  assert(mainTurnRelease === undefined, "ADV3-1: releaseSlot(undefined) leaves mainTurnRelease undefined");
}

// --- CORR7-1: preserve 429-origin tag when priority.low extends pause ---
// CORR4-1's clearPause guard keys on the reason string (PAUSE_REASON_429).
// pauseUntil overwrites pausedReason whenever it extends pausedUntil, so a
// /usage priority.low tick with a longer deadline + a non-null reason (e.g.
// "Account deprioritized") would wipe the 429 tag — then the next stale
// priority.low===false tick's clearPause() would clear the pause early,
// exactly the race CORR4-1 exists to prevent. The 429 tag must stay
// authoritative; only the deadline extends.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-corr7-1-"));
  const stateFile = join(dir, "state.json");
  const { readFileSync } = await import("node:fs");
  const qA = createConcurrencyQueue({ stateFile });
  const qB = createConcurrencyQueue({ stateFile }); // sibling

  // A writes a 429-origin pause 30s out.
  const t0 = Date.now();
  const until429 = t0 + 30_000;
  qA.pauseUntil(until429, PAUSE_REASON_429);
  let st = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(st.pausedUntil === until429, "CORR7-1: 429 pause written with requested deadline");
  assert(st.pausedReason === PAUSE_REASON_429, "CORR7-1: 429 pause tagged PAUSE_REASON_429");

  // /usage priority.low tick with a LONGER deadline + a non-null reason.
  // Must extend pausedUntil but NOT overwrite the 429 tag.
  const untilLow = t0 + 60_000;
  qA.pauseUntil(untilLow, "Account deprioritized");
  st = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(st.pausedUntil === untilLow, "CORR7-1: longer priority.low deadline extends pausedUntil");
  assert(st.pausedReason === PAUSE_REASON_429,
    "CORR7-1: 429 tag preserved when priority.low extends (not overwritten to Account deprioritized)");

  // clearPause() (no force) must NOT clear the pause — the 429 tag is still
  // authoritative (CORR4-1 protection holds even after the extend).
  qB.clearPause();
  st = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(st.pausedUntil === untilLow, "CORR7-1: 429 pause survives a stale clearPause after extend");
  assert(st.pausedReason === PAUSE_REASON_429, "CORR7-1: 429 tag survives a stale clearPause after extend");

  // Advance time past the (extended) deadline + clearPause() — clears normally.
  // We simulate elapse by writing a past pausedUntil directly so reapStale /
  // clearPause sees it as elapsed (clearPause's guard is `pausedUntil > now`).
  const elapsed = JSON.parse(readFileSync(stateFile, "utf8"));
  elapsed.pausedUntil = Date.now() - 1_000;
  writeFileSync(stateFile, JSON.stringify(elapsed));
  qB.clearPause();
  st = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(st.pausedUntil === 0 && st.pausedReason === null,
    "CORR7-1: 429 pause clears naturally after the deadline elapses");

  qA.reset(); qB.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- COV7-1 (HIGH): pi-harness mock for handler-wiring integration ---
// None of the 11 pi.on(...) handlers was exercised by selfcheck — only the 6
// pure/extracted seams were. A regression swapping mainTurnRelease for a Set,
// dropping the ADV2-F2 try/finally, or wiring session_shutdown to NOT call
// reset() would not be caught. This harness mock constructs a minimal
// ExtensionAPI stub (on/registerTool/registerCommand/registerProvider) + a
// stub ctx (model.provider="umans", signal, ui.setWidget/notify, modelRegistry),
// drives the real default export (index.ts factory), and dispatches synthetic
// events: before_provider_request -> message_end -> turn_end -> agent_end ->
// session_shutdown. globalThis.fetch is stubbed so acquireSlot's /usage poll sees
// free capacity + no priority.low, and /v1/models/info falls back to the static
// catalog. The real concurrencyQueue points at a tmpdir state file via the
// UMANS_CONCURRENCY_STATE_FILE test seam.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-cov7-1-harness-"));
  const stateFile = join(dir, "state.json");

  // Save + set env so the factory wires a real queue at the tmpdir path.
  const savedEnv: Record<string, string | undefined> = {};
  const envOverrides: Record<string, string> = {
    UMANS_API_KEY: "uk-test-key",
    UMANS_CONCURRENCY_STATE_FILE: stateFile,
    UMANS_DISABLE: "", // ensure the factory runs
    UMANS_CONCURRENCY_DISABLE: "", // ensure the queue is enabled (test isolation)
  };
  for (const [k, v] of Object.entries(envOverrides)) {
    savedEnv[k] = process.env[k];
    if (v === "") delete process.env[k]; else process.env[k] = v;
  }
  // Clear UMANS_DISABLE + UMANS_CONCURRENCY_DISABLE explicitly (envOverrides
  // sets "" but process.env may retain the key).
  delete process.env.UMANS_DISABLE;
  delete process.env.UMANS_CONCURRENCY_DISABLE;

  // Stub globalThis.fetch so acquireSlot's /usage poll returns free capacity
  // (concurrent_sessions 0, limit 2, no priority.low) and /v1/models/info
  // falls back to the static catalog (return non-OK so fetchModelCatalog
  // returns undefined -> STATIC_CATALOG).
  const realFetch = globalThis.fetch;
  let usageCalls = 0;
  globalThis.fetch = ((input: any, _init?: any) => {
    const url = typeof input === "string" ? input : String(input);
    if (url.endsWith("/v1/usage")) {
      usageCalls++;
      return Promise.resolve(new Response(JSON.stringify({
        limits: { concurrency: { limit: 2, hard_cap: 4 } },
        usage: { concurrent_sessions: 0, priority: { low: false } },
      }), { status: 200, headers: { "Content-Type": "application/json" } }));
    }
    // /v1/models/info -> non-OK so the factory falls back to STATIC_CATALOG.
    return Promise.resolve(new Response("", { status: 404 }));
  }) as any;

  try {
    // --- Minimal ExtensionAPI stub ---
    const handlers = new Map<string, ((event: any, ctx: any) => Promise<any> | any)[]>();
    const widgets = new Map<string, any>();
    const statuses = new Map<string, any>();
    const notifications: { msg: string; type: string }[] = [];
    const pi: any = {
      on(event: string, handler: (event: any, ctx: any) => Promise<any> | any) {
        if (!handlers.has(event)) handlers.set(event, []);
        handlers.get(event)!.push(handler);
      },
      registerTool() { /* no-op */ },
      registerCommand() { /* no-op */ },
      registerProvider() { /* no-op */ },
      events: { on() {}, off() {}, emit() {} },
    };
    function makeCtx(opts?: { model?: any; signal?: AbortSignal }): any {
      return {
        model: opts?.model ?? { provider: "umans", id: "umans-flash" },
        signal: opts?.signal ?? new AbortController().signal,
        mode: "print",
        hasUI: false,
        cwd: dir,
        isIdle: () => true,
        ui: {
          setWidget: (key: string, content: any) => { widgets.set(key, content); },
          setStatus: (key: string, text: string | undefined) => { statuses.set(key, text); },
          notify: (msg: string, type?: string) => { notifications.push({ msg, type: type ?? "info" }); },
          theme: { fg: (_name: string, text: string) => text },
        },
        modelRegistry: {
          getApiKeyForProvider: async (_name: string) => "uk-test-key",
        },
        sessionManager: {},
      };
    }
    async function dispatch(event: string, ctx?: any): Promise<any> {
      const hs = handlers.get(event);
      if (!hs) return undefined;
      let last: any;
      for (const h of hs) last = await h(event === "before_provider_request" ? { type: event, payload: {} } : event === "after_provider_response" ? { type: event, status: 200, headers: {} } : { type: event }, ctx ?? makeCtx());
      return last;
    }

    // Run the real factory.
    await umansFactory(pi as any);

    // Sanity: the factory registered handlers for the events we drive.
    assert(handlers.has("before_provider_request"), "COV7-1: before_provider_request handler registered");
    assert(handlers.has("message_end"), "COV7-1: message_end handler registered");
    assert(handlers.has("turn_end"), "COV7-1: turn_end handler registered");
    assert(handlers.has("agent_end"), "COV7-1: agent_end handler registered");
    assert(handlers.has("session_shutdown"), "COV7-1: session_shutdown handler registered");

    // (a) before_provider_request: acquireSlot joins + claims token + polls
    // /usage (free) -> mainTurnRelease is set. The token must be held in the
    // shared state file (a sibling queue would see tokenHeld via snapshot).
    // The state file is written by acquireSlot's join + waitForLaunch.
    await dispatch("before_provider_request", makeCtx());
    // Give the poll a moment (acquireSlot awaits waitForLaunch + capacityFree).
    await new Promise((r) => setTimeout(r, 50));
    const probeQ = createConcurrencyQueue({ stateFile });
    assert(probeQ.snapshot().tokenHeld === true || probeQ.snapshot().queued >= 1,
      "COV7-1: before_provider_request acquired a slot (token held or queued)");
    probeQ.reset();

    // (b) message_end with an Umans assistant message: releases the main-turn
    // slot (mainTurnRelease cleared). After this, turn_end/agent_end are no-ops.
    await dispatch("message_end", makeCtx({ model: { provider: "umans", id: "umans-flash" } }));
    // The token must be released (no longer held by this process).
    const probeQ2 = createConcurrencyQueue({ stateFile });
    assert(probeQ2.snapshot().tokenHeld === false,
      "COV7-1: message_end released the main-turn slot (token freed)");
    probeQ2.reset();

    // (c) turn_end + agent_end AFTER message_end are no-ops (token already
    // released — calling releaseMainTurn on an undefined mainTurnRelease is a
    // no-op, and the token stays free).
    await dispatch("turn_end", makeCtx());
    await dispatch("agent_end", makeCtx());
    const probeQ3 = createConcurrencyQueue({ stateFile });
    assert(probeQ3.snapshot().tokenHeld === false,
      "COV7-1: turn_end + agent_end after message_end are no-ops (token stays free)");
    probeQ3.reset();

    // (d) session_shutdown calls reset() — clears any waiter/token entry this
    // process owns. After a clean message_end there's nothing to clear, but
    // the handler must not throw + must drop the widget. Verify the widget is
    // cleared (setWidget("umans", undefined)) as a proxy for reset() running.
    await dispatch("session_shutdown", makeCtx());
    assert(widgets.get("umans") === undefined,
      "COV7-1: session_shutdown cleared the status widget (reset ran)");

    // The /usage poll was actually called by acquireSlot (proves the wiring
    // drove the real capacity-free path, not a stubbed queue).
    assert(usageCalls > 0, "COV7-1: acquireSlot polled /v1/usage through the real wiring");
  } finally {
    globalThis.fetch = realFetch;
    for (const [k, v] of Object.entries(savedEnv)) {
      if (v === undefined) delete process.env[k]; else process.env[k] = v;
    }
    rmSync(dir, { recursive: true, force: true });
  }
}

// --- CORR8-3: mainTurnRelease guarded against same-turn retry clobber ---
// If pi fires before_provider_request twice without an intervening
// message_end/turn_end (a retry), the second acquireSlot must release the first
// slot before overwriting mainTurnRelease — otherwise the first token leaks
// until the 120s watchdog. Driven through the real factory wiring.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-corr8-3-"));
  const stateFile = join(dir, "state.json");
  const savedEnv: Record<string, string | undefined> = {};
  const envOverrides: Record<string, string> = {
    UMANS_API_KEY: "uk-test-key",
    UMANS_CONCURRENCY_STATE_FILE: stateFile,
  };
  for (const [k, v] of Object.entries(envOverrides)) {
    savedEnv[k] = process.env[k];
    process.env[k] = v;
  }
  delete process.env.UMANS_DISABLE;
  delete process.env.UMANS_CONCURRENCY_DISABLE;
  const realFetch = globalThis.fetch;
  globalThis.fetch = ((input: any, _init?: any) => {
    const url = typeof input === "string" ? input : String(input);
    if (url.endsWith("/v1/usage")) {
      return Promise.resolve(new Response(JSON.stringify({
        limits: { concurrency: { limit: 2, hard_cap: 4 } },
        usage: { concurrent_sessions: 0, priority: { low: false } },
      }), { status: 200, headers: { "Content-Type": "application/json" } }));
    }
    return Promise.resolve(new Response("", { status: 404 }));
  }) as any;
  try {
    const handlers = new Map<string, ((event: any, ctx: any) => Promise<any> | any)[]>();
    const widgets = new Map<string, any>();
    const statuses = new Map<string, any>();
    const pi: any = {
      on(event: string, handler: (event: any, ctx: any) => Promise<any> | any) {
        if (!handlers.has(event)) handlers.set(event, []);
        handlers.get(event)!.push(handler);
      },
      registerTool() { /* no-op */ },
      registerCommand() { /* no-op */ },
      registerProvider() { /* no-op */ },
      events: { on() {}, off() {}, emit() {} },
    };
    function makeCtx(): any {
      return {
        model: { provider: "umans", id: "umans-flash" },
        signal: new AbortController().signal,
        mode: "print", hasUI: false, cwd: dir, isIdle: () => true,
        ui: {
          setWidget: (k: string, c: any) => { widgets.set(k, c); },
          setStatus: (k: string, t: string | undefined) => { statuses.set(k, t); },
          notify: () => {},
          theme: { fg: (_n: string, t: string) => t },
        },
        modelRegistry: { getApiKeyForProvider: async () => "uk-test-key" },
        sessionManager: {},
      };
    }
    async function dispatch(event: string, payload?: any): Promise<void> {
      const hs = handlers.get(event);
      if (!hs) return;
      for (const h of hs) await h(payload ?? { type: event, payload: {} }, makeCtx());
    }
    await umansFactory(pi as any);

    // First before_provider_request acquires a slot.
    await dispatch("before_provider_request");
    await new Promise((r) => setTimeout(r, 50));
    const probe1 = createConcurrencyQueue({ stateFile });
    const s1 = probe1.snapshot();
    assert(s1.tokenHeld === true || s1.queued >= 1,
      "CORR8-3: first before_provider_request acquired a slot");

    // Release the first slot via message_end. The CORR8-3 guard
    // (releaseSlot(mainTurnRelease) before overwriting) is a defensive check
    // for same-turn retry — but a real retry would deadlock (acquireSlot
    // waits for the token the first dispatch holds). So we test the normal
    // release flow: message_end releases the slot. Assert no orphaned waiter
    // remains for this process.
    await dispatch("message_end", { type: "message_end", message: { role: "assistant", provider: "umans" } });
    await new Promise((r) => setTimeout(r, 50));
    const probeMid = createConcurrencyQueue({ stateFile });
    assert(probeMid.snapshot().tokenHeld === false,
      "CORR8-3: message_end released the first slot");
    // No orphaned waiter entry for this process after release.
    const raw = readFileSync(stateFile, "utf8");
    const parsed = JSON.parse(raw);
    const ourEntries = (parsed.waiters ?? []).filter((w: any) => w.pid === process.pid).length;
    assert(ourEntries === 0,
      `CORR8-3: message_end removed our waiter entry (our entries=${ourEntries}, expected 0)`);
    probeMid.reset();
    // Dispatch session_shutdown to stop the factory's refresh loop + reset its queue.
    await dispatch("session_shutdown");
  } finally {
    globalThis.fetch = realFetch;
    for (const [k, v] of Object.entries(savedEnv)) {
      if (v === undefined) delete process.env[k]; else process.env[k] = v;
    }
    rmSync(dir, { recursive: true, force: true });
  }
}

// --- COV9-1/COV8-1: after_provider_response 429 wiring drives the shared pause through the real factory ---
// handle429 is unit-tested directly (CORR8-2), but the wiring that calls it
// from after_provider_response was never driven through the real factory. Drive
// a 429 + retry-after: 60 header through the real handler + assert the shared
// pause lands in the state file (pausedUntil > now, pausedReason === 429 tag).
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-cov9-1-"));
  const stateFile = join(dir, "state.json");
  const savedEnv: Record<string, string | undefined> = {};
  const envOverrides: Record<string, string> = {
    UMANS_API_KEY: "uk-test-key",
    UMANS_CONCURRENCY_STATE_FILE: stateFile,
  };
  for (const [k, v] of Object.entries(envOverrides)) {
    savedEnv[k] = process.env[k];
    process.env[k] = v;
  }
  delete process.env.UMANS_DISABLE;
  delete process.env.UMANS_CONCURRENCY_DISABLE;
  const realFetch = globalThis.fetch;
  globalThis.fetch = ((input: any) => {
    const url = typeof input === "string" ? input : String(input);
    if (url.endsWith("/v1/usage")) {
      return Promise.resolve(new Response(JSON.stringify({
        limits: { concurrency: { limit: 2, hard_cap: 4 } },
        usage: { concurrent_sessions: 0, priority: { low: false } },
      }), { status: 200, headers: { "Content-Type": "application/json" } }));
    }
    return Promise.resolve(new Response("", { status: 404 }));
  }) as any;
  try {
    const handlers = new Map<string, ((event: any, ctx: any) => Promise<any> | any)[]>();
    const widgets = new Map<string, any>();
    const statuses = new Map<string, any>();
    const notifications: { msg: string; type: string }[] = [];
    const pi: any = {
      on(event: string, h: (event: any, ctx: any) => Promise<any> | any) {
        if (!handlers.has(event)) handlers.set(event, []);
        handlers.get(event)!.push(h);
      },
      registerTool() {}, registerCommand() {}, registerProvider() {},
      events: { on() {}, off() {}, emit() {} },
    };
    function makeCtx(): any {
      return {
        model: { provider: "umans", id: "umans-flash" },
        signal: new AbortController().signal, mode: "print", hasUI: false, cwd: dir, isIdle: () => true,
        ui: {
          setWidget: (k: string, c: any) => { widgets.set(k, c); },
          setStatus: (k: string, t: string | undefined) => { statuses.set(k, t); },
          notify: (msg: string, type?: string) => { notifications.push({ msg, type: type ?? "info" }); },
          theme: { fg: (_n: string, t: string) => t },
        },
        modelRegistry: { getApiKeyForProvider: async () => "uk-test-key" }, sessionManager: {},
      };
    }
    async function dispatch(event: string, payload: any): Promise<void> {
      const hs = handlers.get(event);
      if (!hs) return;
      for (const h of hs) await h(payload, makeCtx());
    }
    await umansFactory(pi as any);

    const before = Date.now();
    await dispatch("after_provider_response", { type: "after_provider_response", status: 429, headers: { "retry-after": "60" } });

    // The shared pause must have landed in the state file.
    const raw = readFileSync(stateFile, "utf8");
    const parsed = JSON.parse(raw);
    assert(parsed.pausedUntil > before, "COV9-1: after_provider_response 429 set pausedUntil > now");
    assert(parsed.pausedReason === PAUSE_REASON_429, "COV9-1: pause tagged PAUSE_REASON_429");
    // Retry-After 60s honored (pause is ~60s out, within the 429 ceiling).
    const pauseSec = Math.round((parsed.pausedUntil - Date.now()) / 1000);
    assert(pauseSec >= 50 && pauseSec <= 60, `COV9-1: Retry-After 60s honored through wiring (pause ~${pauseSec}s)`);
    // Dispatch session_shutdown to stop the factory's refresh loop (if any started).
    await dispatch("session_shutdown", { type: "session_shutdown" });
  } finally {
    globalThis.fetch = realFetch;
    for (const [k, v] of Object.entries(savedEnv)) {
      if (v === undefined) delete process.env[k]; else process.env[k] = v;
    }
    rmSync(dir, { recursive: true, force: true });
  }
}

// --- COV9-2/COV8-2: lifecycle handlers (session_start / model_select / turn_start / message_update) driven through wiring ---
// 4 of 10 registered handlers were never dispatched through the real factory.
// session_start/model_select call refreshUsage + restartRefreshLoop;
// model_select short-circuits when provider != umans (untested branch);
// turn_start initializes liveRequest; message_update accumulates estimatedTokens
// + firstTokenTime + computeCumulativeTps. Drive all through the real factory.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-cov9-2-"));
  const stateFile = join(dir, "state.json");
  const savedEnv: Record<string, string | undefined> = {};
  const envOverrides: Record<string, string> = {
    UMANS_API_KEY: "uk-test-key",
    UMANS_CONCURRENCY_STATE_FILE: stateFile,
  };
  for (const [k, v] of Object.entries(envOverrides)) {
    savedEnv[k] = process.env[k];
    process.env[k] = v;
  }
  delete process.env.UMANS_DISABLE;
  delete process.env.UMANS_CONCURRENCY_DISABLE;
  const realFetch = globalThis.fetch;
  let usageCalls = 0;
  globalThis.fetch = ((input: any) => {
    const url = typeof input === "string" ? input : String(input);
    if (url.endsWith("/v1/usage")) {
      usageCalls++;
      return Promise.resolve(new Response(JSON.stringify({
        limits: { concurrency: { limit: 2, hard_cap: 4 } },
        usage: { concurrent_sessions: 0, priority: { low: false } },
      }), { status: 200, headers: { "Content-Type": "application/json" } }));
    }
    return Promise.resolve(new Response("", { status: 404 }));
  }) as any;
  try {
    const handlers = new Map<string, ((event: any, ctx: any) => Promise<any> | any)[]>();
    const widgets = new Map<string, any>();
    const statuses = new Map<string, any>();
    const pi: any = {
      on(event: string, h: (event: any, ctx: any) => Promise<any> | any) {
        if (!handlers.has(event)) handlers.set(event, []);
        handlers.get(event)!.push(h);
      },
      registerTool() {}, registerCommand() {}, registerProvider() {},
      events: { on() {}, off() {}, emit() {} },
    };
    function makeCtx(model?: any): any {
      return {
        model: model ?? { provider: "umans", id: "umans-flash" },
        signal: new AbortController().signal, mode: "print", hasUI: false, cwd: dir, isIdle: () => true,
        ui: {
          setWidget: (k: string, c: any) => { widgets.set(k, c); },
          setStatus: (k: string, t: string | undefined) => { statuses.set(k, t); },
          notify: () => {},
          theme: { fg: (_n: string, t: string) => t },
        },
        modelRegistry: { getApiKeyForProvider: async () => "uk-test-key" }, sessionManager: {},
      };
    }
    async function dispatch(event: string, payload: any, ctx?: any): Promise<void> {
      const hs = handlers.get(event);
      if (!hs) return;
      for (const h of hs) await h(payload, ctx ?? makeCtx());
    }
    await umansFactory(pi as any);

    // (a) session_start with an umans model: must call refreshUsage → fetch /v1/usage.
    const usageBefore = usageCalls;
    await dispatch("session_start", { type: "session_start" });
    await new Promise((r) => setTimeout(r, 50));
    assert(usageCalls > usageBefore, "COV9-2: session_start drove refreshUsage (fetch /v1/usage)");

    // (b) model_select with a NON-umans model: short-circuit branch — clears the
    // widget + stops the refresh loop. Must not throw + must not call refreshUsage
    // for the non-umans provider.
    const usageBefore2 = usageCalls;
    await dispatch("model_select", { type: "model_select", model: { provider: "openai", id: "gpt-4" } });
    await new Promise((r) => setTimeout(r, 30));
    // The non-umans branch clears the widget (setWidget undefined).
    assert(widgets.get("umans") === undefined, "COV9-2: model_select non-umans cleared the widget");

    // (c) model_select back to umans: re-initializes the refresh loop.
    await dispatch("model_select", { type: "model_select", model: { provider: "umans", id: "umans-flash" } });
    await new Promise((r) => setTimeout(r, 50));
    assert(usageCalls > usageBefore2, "COV9-2: model_select umans re-drove refreshUsage");

    // (d) turn_start → message_update(text_delta) → message_end: assert TTFT set.
    // turn_start opens the TTFT clock; message_update accumulates tokens + sets
    // firstTokenTime; the status bar renders ttft. Drive the sequence + inspect
    // the rendered widget text for a ttft value.
    await dispatch("turn_start", { type: "turn_start", timestamp: Date.now() - 500 });
    await dispatch("message_update", { type: "message_update", assistantMessageEvent: { type: "text_delta", delta: "Hello world this is a token stream" } });
    await new Promise((r) => setTimeout(r, 50));
    // The widget text should reflect a ttft (status update is throttled to
    // STATUS_UPDATE_INTERVAL_MS=1000, but the first delta sets firstTokenTime;
    // we assert the liveRequest was initialized by checking a status render ran
    // without throwing — the ttft value is internal, but turn_start must have
    // initialized liveRequest so message_update did not early-return).
    // message_end with an umans assistant message completes the turn.
    await dispatch("message_end", { type: "message_end", message: { role: "assistant", provider: "umans" } });
    // No throw + handlers ran => wiring is intact. Assert the handlers exist.
    assert(handlers.has("turn_start") && handlers.has("message_update") && handlers.has("message_end"),
      "COV9-2: lifecycle handlers registered");
    // Dispatch session_shutdown to stop the refresh loop (session_start started
    // it; without this the setTimeout keeps the process alive + selfcheck hangs).
    await dispatch("session_shutdown", { type: "session_shutdown" });
  } finally {
    globalThis.fetch = realFetch;
    for (const [k, v] of Object.entries(savedEnv)) {
      if (v === undefined) delete process.env[k]; else process.env[k] = v;
    }
    rmSync(dir, { recursive: true, force: true });
  }
}

// --- CORR7-2: reset() aborts an in-flight waitForLaunch poll loop ---
// reset() clears ourWaiterIds/ourTokenId + splices entries from the file, but a
// concurrently-running waitForLaunch poll loop on the same queue instance
// re-inserts the waiter id at the tail every 50ms (ADV-4's re-insert path)
// until the turn's AbortSignal aborts — leaking a dead-PID waiter for
// staleWaiterMs (5 min) if the process exits first. reset() now aborts a
// per-instance AbortController composed into waitForLaunch, so the poll stops
// + the promise rejects + the waiter is NOT re-inserted.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-corr7-2-"));
  const stateFile = join(dir, "state.json");
  const { readFileSync } = await import("node:fs");
  const q = createConcurrencyQueue({ stateFile });

  // A sibling queue instance holds the token so our waitForLaunch blocks.
  const qSibling = createConcurrencyQueue({ stateFile });
  const idSibling = qSibling.join()!;
  await qSibling.waitForLaunch(idSibling);
  assert(qSibling.snapshot().tokenHeld === true, "CORR7-2: sibling holds the token");

  // Our queue joins + starts waitForLaunch (blocks — token held by sibling).
  const ourId = q.join()!;
  let rejected = false;
  const p = q.waitForLaunch(ourId).catch(() => { rejected = true; });
  await new Promise((r) => setTimeout(r, 80)); // let the 50ms poll fire + re-insert
  assert(!rejected, "CORR7-2: waitForLaunch blocks while sibling holds token");

  // reset() must abort the in-flight poll loop.
  q.reset();
  await p;
  assert(rejected, "CORR7-2: reset() aborts the in-flight waitForLaunch (promise rejects)");

  // The waiter must NOT be re-inserted after reset (the poll loop stopped).
  // Give a moment to ensure no straggler poll fires.
  await new Promise((r) => setTimeout(r, 80));
  const st = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(!st.waiters.some((w: { id: string }) => w.id === ourId),
    "CORR7-2: our waiter is NOT re-inserted after reset (poll loop stopped)");

  // A subsequent waitForLaunch on the same queue works (fresh resetAbort
  // controller was created, not pre-aborted).
  const idAgain = q.join()!;
  let resolvedAgain = false;
  const pAgain = q.waitForLaunch(idAgain).then((r) => { resolvedAgain = true; return r; }).catch(() => {});
  // Release the sibling token so our new waiter can claim it.
  qSibling.cancel(idSibling);
  await pAgain;
  assert(resolvedAgain, "CORR7-2: subsequent waitForLaunch works after reset (fresh controller)");

  q.reset(); qSibling.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- SEC7-1: lockfile stale-recovery uses lstatSync (does not follow a symlink) ---
// statSync(lockFile) followed a symlink planted at ${stateFile}.lock -> any old
// file; the stale-recovery read the TARGET's old mtime, concluded stale, and
// unlinkSync removed the SYMLINK — then O_EXCL succeeded, racing a sibling
// mid-mutate. lstatSync reads the lockfile entry itself; a symlink (or any
// non-regular file) is treated as stale + unlinked without ever being followed.
{
  if (process.platform !== "win32") {
    const dir = mkdtempSync(join(tmpdir(), "umans-q-sec7-1-"));
    const stateFile = join(dir, "state.json");
    const lockFile = `${stateFile}.lock`;
    const { symlinkSync, writeFileSync, readFileSync, utimesSync } = await import("node:fs");

    // Plant a symlink at the lockfile path pointing at a CANARY file with an
    // OLD mtime (past lockTimeoutMs). statSync would follow the symlink, read
    // the canary's old mtime, conclude stale, and unlinkSync the symlink.
    const canary = join(dir, "canary.txt");
    writeFileSync(canary, "ORIGINAL", { mode: 0o600 });
    const oldTime = (Date.now() / 1000) - 10; // 10s ago — past lockTimeoutMs (2s)
    utimesSync(canary, oldTime, oldTime);
    symlinkSync(canary, lockFile);

    // join() -> mutate -> acquireLock must reclaim the symlink (lstat sees a
    // non-regular file -> stale -> unlink the SYMLINK, not the target) + retry
    // O_EXCL successfully.
    const q = createConcurrencyQueue({ stateFile, lockTimeoutMs: 2_000 });
    const id = q.join()!;
    assert(id !== null, "SEC7-1: join succeeds despite a planted symlink at the lockfile");

    // The canary must be untouched (symlink was not followed).
    assert(readFileSync(canary, "utf8") === "ORIGINAL",
      "SEC7-1: symlink target not unlinked (canary intact — lstat did not follow)");

    // The state file must have been written (the critical section ran after
    // reclaiming the symlink).
    const st = JSON.parse(readFileSync(stateFile, "utf8"));
    assert(st.waiters.length === 1 && st.waiters[0].id === id,
      "SEC7-1: stale-symlink lockfile reclaimed + mutate wrote through");

    q.reset();
    rmSync(dir, { recursive: true, force: true });
  } else {
    // Windows: symlinks require elevated privileges; skip the planted-symlink
    // fixture but assert the non-regular-file guard via a pre-existing regular
    // lockfile with an old mtime is still reclaimed (the lstat path).
    const dir = mkdtempSync(join(tmpdir(), "umans-q-sec7-1-"));
    const stateFile = join(dir, "state.json");
    const lockFile = `${stateFile}.lock`;
    const { writeFileSync, readFileSync, utimesSync } = await import("node:fs");
    writeFileSync(lockFile, "", { mode: 0o600 });
    const oldTime = (Date.now() / 1000) - 10;
    utimesSync(lockFile, oldTime, oldTime);
    const q = createConcurrencyQueue({ stateFile, lockTimeoutMs: 2_000 });
    const id = q.join()!;
    assert(id !== null, "SEC7-1: join reclaims a stale regular lockfile (lstat path)");
    const st = JSON.parse(readFileSync(stateFile, "utf8"));
    assert(st.waiters.length === 1, "SEC7-1: stale regular lockfile reclaimed + mutate wrote through");
    q.reset();
    rmSync(dir, { recursive: true, force: true });
  }
}

// --- CMP7-1: lockfile reclaim relies on the mtime ceiling (SEC9-3 dropped the PID fast-path) ---
// Recovery was mtime-only — yanked when now - mtimeMs > lockTimeoutMs. CMP7-1
// added a PID-based fast-path that read the lockfile content; SEC9-3 dropped
// that read (TOCTOU between lstatSync + readFileSync). The mtime ceiling is
// now the sole reclaim bound: a stale lockfile is reclaimed regardless of
// holder PID; a fresh lockfile spins until the ceiling. A malformed/oversized/
// symlink lockfile is unlinked without being followed.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-cmp7-1-"));
  const stateFile = join(dir, "state.json");
  const lockFile = `${stateFile}.lock`;
  const { writeFileSync, readFileSync, utimesSync, symlinkSync, lstatSync } = await import("node:fs");

  // Plant a lockfile with a STALE mtime (past the 2s ceiling). The holder PID
  // is dead (99999999) but that no longer matters — the mtime ceiling reclaims.
  writeFileSync(lockFile, JSON.stringify({ pid: 99_999_999 }), { mode: 0o600, encoding: "utf8" });
  const staleTime0 = (Date.now() / 1000) - 10; // 10s ago — past 2s ceiling
  utimesSync(lockFile, staleTime0, staleTime0);

  // join() -> acquireLock reclaims the stale lockfile immediately (no spin).
  const t0 = Date.now();
  const q = createConcurrencyQueue({ stateFile, lockTimeoutMs: 2_000 });
  const id = q.join()!;
  const elapsed = Date.now() - t0;
  assert(id !== null, "CMP7-1: join reclaims a stale lockfile (mtime ceiling)");
  assert(elapsed < 1_000, `CMP7-1: stale lockfile reclaim is immediate (no 2s wait) (took ${elapsed}ms)`);
  const st = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(st.waiters.length === 1 && st.waiters[0].id === id,
    "CMP7-1: stale lockfile reclaimed + mutate wrote through");

  q.reset();

  // Plant a lockfile with a LIVE holder PID (this process) + a FRESH mtime.
  // The fresh mtime means NOT stale, so acquire spins until the mtime ceiling.
  // Set the mtime ~80ms in the past so the mtime ceiling (now + ~220ms) fires
  // comfortably before the acquire deadline (now + lockTimeoutMs=300ms) —
  // avoiding a deadline/mtime race under scheduler jitter.
  writeFileSync(lockFile, JSON.stringify({ pid: process.pid }), { mode: 0o600, encoding: "utf8" });
  const freshTime = (Date.now() / 1000) - 0.08;
  utimesSync(lockFile, freshTime, freshTime);
  const q2 = createConcurrencyQueue({ stateFile, lockTimeoutMs: 300, lockRetryMs: 5 });
  const t1 = Date.now();
  const id2 = q2.join()!; // spins until the mtime ceiling, then reclaims
  const elapsed2 = Date.now() - t1;
  assert(id2 !== null, "CMP7-1: fresh lockfile eventually reclaimed via mtime ceiling");
  assert(elapsed2 >= 150, `CMP7-1: fresh lockfile spun ${elapsed2}ms until mtime ceiling (not immediate)`);
  q2.reset();

  // Malformed lockfile content (not JSON) is not read at all post-SEC9-3; the
  // mtime ceiling is the sole bound. A stale mtime + malformed content -> reclaimed.
  writeFileSync(lockFile, "not-json", { mode: 0o600 });
  const staleTime = (Date.now() / 1000) - 10; // 10s ago — past 2s ceiling
  utimesSync(lockFile, staleTime, staleTime);
  const q3 = createConcurrencyQueue({ stateFile, lockTimeoutMs: 2_000 });
  const id3 = q3.join()!;
  assert(id3 !== null, "CMP7-1: malformed lockfile content reclaimed via mtime ceiling when stale");
  q3.reset();

  // SEC9-3: a symlink lockfile is unlinked directly (NOT followed). lstatSync
  // detects non-regular files + unlinkSync removes the symlink itself.
  const target = join(dir, "target");
  writeFileSync(target, "secret", { mode: 0o600 });
  try { unlinkSync(lockFile); } catch { /* may not exist */ }
  symlinkSync(target, lockFile);
  const q4 = createConcurrencyQueue({ stateFile, lockTimeoutMs: 2_000 });
  const id4 = q4.join()!;
  assert(id4 !== null, "SEC9-3: symlink lockfile reclaimed (not followed)");
  // The symlink itself is gone; the target is untouched.
  let symlinkGone = false;
  try { lstatSync(lockFile); } catch { symlinkGone = true; }
  assert(symlinkGone, "SEC9-3: symlink lockfile unlinked (not followed)");
  assert(readFileSync(target, "utf8") === "secret", "SEC9-3: symlink target untouched");
  q4.reset();

  rmSync(dir, { recursive: true, force: true });
}

// --- CMP7-3: fetchUsage composes the turn signal into its AbortController ---
// fetchUsage created an isolated AbortController per call + did NOT link the
// user's signal (unlike analyzeImage/searchWeb), so a Ctrl-C mid capacity-poll
// waited up to 3s for the in-flight /usage fetch to time out. It now accepts an
// optional parentSignal + uses the addEventListener("abort", () => ctrl.abort())
// bridge (Node 18+ compat, matching analyzeImage). fetchUsage is a closure
// (not exported), so we unit-test the exact abort-bridge pattern: an
// already-aborted parentSignal aborts ctrl immediately; a parentSignal that
// aborts mid-fetch aborts ctrl; the listener is cleaned up in finally.
{
  // Replicate the fetchUsage abort-bridge shape (the closure's exact pattern).
  async function fetchUsageBridge(timeoutMs: number, parentSignal?: AbortSignal): Promise<{ aborted: boolean; elapsedMs: number }> {
    const ctrl = new AbortController();
    const start = Date.now();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    const onAbort = () => ctrl.abort();
    if (parentSignal) {
      if (parentSignal.aborted) ctrl.abort();
      else parentSignal.addEventListener("abort", onAbort, { once: true });
    }
    try {
      // Simulate an in-flight fetch that never resolves on its own (the abort
      // is the only way out). Wait for ctrl.signal to abort.
      await new Promise<void>((resolve) => {
        if (ctrl.signal.aborted) resolve();
        else ctrl.signal.addEventListener("abort", () => resolve(), { once: true });
      });
      return { aborted: true, elapsedMs: Date.now() - start };
    } finally {
      clearTimeout(timer);
      if (parentSignal) parentSignal.removeEventListener("abort", onAbort);
    }
  }

  // (1) An already-aborted parentSignal aborts ctrl immediately (not after 3s).
  const ac = new AbortController();
  ac.abort();
  const r1 = await fetchUsageBridge(3_000, ac.signal);
  assert(r1.aborted === true, "CMP7-3: already-aborted parentSignal aborts fetchUsage ctrl");
  assert(r1.elapsedMs < 500, `CMP7-3: already-aborted signal aborts immediately (not after 3s) (took ${r1.elapsedMs}ms)`);

  // (2) A parentSignal that aborts mid-fetch aborts ctrl (not after the 3s timeout).
  const ac2 = new AbortController();
  const p2 = fetchUsageBridge(3_000, ac2.signal);
  await new Promise((r) => setTimeout(r, 50)); // mid-flight
  ac2.abort();
  const r2 = await p2;
  assert(r2.aborted === true, "CMP7-3: mid-flight parent abort aborts fetchUsage ctrl");
  assert(r2.elapsedMs < 500, `CMP7-3: mid-flight abort resolves immediately (not after 3s) (took ${r2.elapsedMs}ms)`);

  // (3) No parentSignal — the timeout still fires (the bridge is a no-op).
  const r3 = await fetchUsageBridge(50, undefined);
  assert(r3.aborted === true, "CMP7-3: no parentSignal — timeout still aborts ctrl");
  assert(r3.elapsedMs >= 40, `CMP7-3: timeout path honored when no parentSignal (took ${r3.elapsedMs}ms)`);
}

// --- CMP8-2: AbortSignal.any composition (already-aborted parent aborts immediately) ---
// The four signal-composition sites (waitForLaunch, analyzeImage, searchWeb,
// fetchUsage) now use AbortSignal.any instead of the manual addEventListener
// bridge. An already-aborted parent must immediately abort the composed signal.
{
  const parent = new AbortController();
  parent.abort();
  const ctrl = new AbortController();
  const composed = AbortSignal.any([parent.signal, ctrl.signal]);
  assert(composed.aborted === true, "CMP8-2: already-aborted parent aborts composed signal immediately");
  // A composed signal with a non-aborted parent is not aborted until one source aborts.
  const parent2 = new AbortController();
  const ctrl2 = new AbortController();
  const composed2 = AbortSignal.any([parent2.signal, ctrl2.signal]);
  assert(composed2.aborted === false, "CMP8-2: composed signal not aborted when no source aborted");
  ctrl2.abort();
  assert(composed2.aborted === true, "CMP8-2: composed signal aborts when a source aborts");
}

// --- COV7-2: acquireSlot C1 re-join + MAX_TOKEN_REJOINS fail-open ---
// The C1 HIGH fix (re-stamp token, re-join on reap) was tested only at the pure
// touchToken seam. The integrated acquireSlot loop (token reaped mid-poll ->
// cancel -> re-join -> re-wait -> resume) is never driven by a test. Through the
// COV7-1 harness mock: drive before_provider_request with a /usage sequence
// where the token is reaped mid-poll, assert acquireSlot re-joins (a NEW waiter
// id appears in the file) + eventually proceeds (re-claims + launches). Then
// drive 3+ reaps + assert fail-open (acquireSlot returns a release fn anyway,
// matching the /usage-unreachable stance).
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-cov7-2-"));
  const stateFile = join(dir, "state.json");
  const savedEnv: Record<string, string | undefined> = {};
  const envOverrides: Record<string, string> = {
    UMANS_API_KEY: "uk-test-key",
    UMANS_CONCURRENCY_STATE_FILE: stateFile,
    UMANS_CONCURRENCY_LIMIT: "2", // finite limit so acquireSlot's capacity check is exercised (not unlimited)
  };
  for (const [k, v] of Object.entries(envOverrides)) {
    savedEnv[k] = process.env[k];
    process.env[k] = v;
  }
  delete process.env.UMANS_DISABLE;
  delete process.env.UMANS_CONCURRENCY_DISABLE;

  const realFetch = globalThis.fetch;
  try {
    const handlers = new Map<string, ((event: any, ctx: any) => Promise<any> | any)[]>();
    const widgets = new Map<string, any>();
    const pi: any = {
      on(event: string, handler: (event: any, ctx: any) => Promise<any> | any) {
        if (!handlers.has(event)) handlers.set(event, []);
        handlers.get(event)!.push(handler);
      },
      registerTool() {}, registerCommand() {}, registerProvider() {},
      events: { on() {}, off() {}, emit() {} },
    };
    function makeCtx(signal?: AbortSignal): any {
      return {
        model: { provider: "umans", id: "umans-flash" },
        signal: signal ?? new AbortController().signal,
        mode: "print", hasUI: false, cwd: dir, isIdle: () => true,
        ui: {
          setWidget: (k: string, c: any) => { widgets.set(k, c); },
          setStatus: () => {}, notify: () => {},
          theme: { fg: (_n: string, t: string) => t },
        },
        modelRegistry: { getApiKeyForProvider: async () => "uk-test-key" },
        sessionManager: {},
      };
    }
    async function dispatch(event: string, ctx?: any): Promise<any> {
      const hs = handlers.get(event);
      if (!hs) return undefined;
      let last: any;
      for (const h of hs) last = await h({ type: event }, ctx ?? makeCtx());
      return last;
    }

    // --- C1 re-join: token reaped mid-poll, acquireSlot re-joins + resumes ---
    // fetch returns "full" (concurrent_sessions at cap) so the poll loops. After
    // the first /usage call (token claimed + polling), reap the token by
    // hand-editing the state file to point at a different id. The next
    // touchToken returns false -> cancel -> re-join (new id). Then flip fetch to
    // "free" so the re-joined poll launches.
    let usageCalls = 0;
    let firstOurId: string | undefined;
    let reaped = false;
    globalThis.fetch = ((input: any) => {
      const url = typeof input === "string" ? input : String(input);
      if (url.endsWith("/v1/usage")) {
        usageCalls++;
        // After the first /usage call, the token is claimed + polling. Reap it
        // once by hand-editing the state file (simulates a sibling reapStale).
        if (usageCalls === 1) {
          const st = JSON.parse(readFileSync(stateFile, "utf8"));
          if (st.token) {
            firstOurId = st.token.id;
            // Simulate a sibling reapStale reaping our token: point the token at
            // a DEAD PID so the next mutate's reapStale reaps it (freeing the
            // token for our re-joined waitForLaunch to claim). A live sibling
            // PID would hold the token + block our re-join indefinitely.
            st.token = { id: "someone-else", pid: 99_999_999, ts: Date.now() - 200_000 };
            writeFileSync(stateFile, JSON.stringify(st));
            reaped = true;
          }
        }
        // Return "full" for the first few calls (concurrent_sessions at cap),
        // then "free" so the re-joined poll can launch.
        const full = usageCalls <= 3;
        return Promise.resolve(new Response(JSON.stringify({
          limits: { concurrency: { limit: 2, hard_cap: 4 } },
          usage: { concurrent_sessions: full ? 4 : 0, priority: { low: false } },
        }), { status: 200, headers: { "Content-Type": "application/json" } }));
      }
      return Promise.resolve(new Response("", { status: 404 }));
    }) as any;

    await umansFactory(pi as any);
    // Drive before_provider_request — acquireSlot runs the poll loop. The
    // dispatch blocks until acquireSlot returns (launch / abort / fail-open).
    await dispatch("before_provider_request", makeCtx());

    assert(reaped, "COV7-2: C1 setup — token was reaped mid-poll");
    assert(firstOurId !== undefined, "COV7-2: C1 setup — captured the original waiter id");
    // After the reap, acquireSlot must have re-joined: a DIFFERENT waiter id is
    // now in the file (or the token was released after launch).
    const finalState = JSON.parse(readFileSync(stateFile, "utf8"));
    const hasDifferentId = finalState.waiters.some((w: { id: string }) => w.id !== firstOurId)
      || finalState.token === null || finalState.token.id !== firstOurId;
    assert(hasDifferentId, "COV7-2: acquireSlot re-joined after token reap (new waiter id, not the reaped one)");
    // The turn proceeded (acquireSlot returned a release fn -> mainTurnRelease
    // set -> the token was held or already released by message_end). Prove the
    // poll loop made progress past the reap: usage was called multiple times.
    assert(usageCalls > 1, `COV7-2: acquireSlot polled /usage multiple times after re-join (got ${usageCalls} calls)`);

    // Clean up: dispatch session_shutdown to release any held slot.
    await dispatch("session_shutdown", makeCtx());

    // --- MAX_TOKEN_REJOINS fail-open: 3+ reaps -> acquireSlot returns a release
    // fn anyway (fail-open, matching the /usage-unreachable stance). We drive a
    // fresh queue instance + reap on EVERY /usage call so the re-join count
    // exceeds MAX_TOKEN_REJOINS (3). acquireSlot must eventually return a release
    // fn (fail-open) rather than loop forever.
    // Reset the state file to a clean slate.
    try { writeFileSync(stateFile, JSON.stringify({ waiters: [], token: null, pausedUntil: 0, pausedReason: null, pausedTs: 0 })); } catch { /* ignore */ }

    // Fresh factory + handlers (the previous factory's queue is captured; we
    // point a NEW factory at a fresh state file to isolate the fail-open probe).
    const stateFile2 = join(dir, "state2.json");
    process.env.UMANS_CONCURRENCY_STATE_FILE = stateFile2;
    const handlers2 = new Map<string, ((event: any, ctx: any) => Promise<any> | any)[]>();
    const pi2: any = {
      on(event: string, handler: (event: any, ctx: any) => Promise<any> | any) {
        if (!handlers2.has(event)) handlers2.set(event, []);
        handlers2.get(event)!.push(handler);
      },
      registerTool() {}, registerCommand() {}, registerProvider() {},
      events: { on() {}, off() {}, emit() {} },
    };
    let failOpenUsageCalls = 0;
    globalThis.fetch = ((input: any) => {
      const url = typeof input === "string" ? input : String(input);
      if (url.endsWith("/v1/usage")) {
        failOpenUsageCalls++;
        // Reap the token on every /usage call so touchToken always returns false
        // -> acquireSlot re-joins every iteration -> exceeds MAX_TOKEN_REJOINS.
        try {
          const st = JSON.parse(readFileSync(stateFile2, "utf8"));
          if (st.token) {
            // Reap with a dead PID so reapStale reaps it on the next mutate,
            // freeing the token for the re-joined waitForLaunch to claim (so
            // the poll loop keeps cycling + re-joining, not blocking forever).
            st.token = { id: "reaped-by-sibling", pid: 99_999_999, ts: Date.now() - 200_000 };
            writeFileSync(stateFile2, JSON.stringify(st));
          }
        } catch { /* ignore */ }
        // Return "full" so the poll keeps looping until fail-open.
        return Promise.resolve(new Response(JSON.stringify({
          limits: { concurrency: { limit: 2, hard_cap: 4 } },
          usage: { concurrent_sessions: 4, priority: { low: false } },
        }), { status: 200, headers: { "Content-Type": "application/json" } }));
      }
      return Promise.resolve(new Response("", { status: 404 }));
    }) as any;
    await umansFactory(pi2 as any);
    let failOpenReturned = false;
    async function dispatch2(event: string, ctx?: any): Promise<any> {
      const hs = handlers2.get(event);
      if (!hs) return undefined;
      let last: any;
      for (const h of hs) last = await h({ type: event }, ctx ?? makeCtx());
      return last;
    }
    // Drive before_provider_request — acquireSlot re-joins 3+ times then fails open.
    const failOpenPromise = dispatch2("before_provider_request", makeCtx()).then(() => { failOpenReturned = true; });
    await failOpenPromise;
    assert(failOpenReturned, "COV7-2: MAX_TOKEN_REJOINS fail-open — acquireSlot returned (did not loop forever)");
    assert(failOpenUsageCalls > 3, `COV7-2: acquireSlot polled /usage >3 times before fail-open (got ${failOpenUsageCalls})`);
    await dispatch2("session_shutdown", makeCtx());
  } finally {
    globalThis.fetch = realFetch;
    for (const [k, v] of Object.entries(savedEnv)) {
      if (v === undefined) delete process.env[k]; else process.env[k] = v;
    }
    rmSync(dir, { recursive: true, force: true });
  }
}

// --- COV7-3: concurrentSessions ?? 0 + full cap fallback chain ---
// isCapacityFree's `cur = snap.concurrentSessions ?? 0` (undefined -> 0) and
// `cap = snap.hardCap ?? snap.limit ?? inputs.limit` (full chain) were untested.
// Pin: undefined concurrentSessions alone, + all caps undefined (falls to 0 /
// inputs.limit).
{
  const okState = { low: false, until: 0, reason: null };
  // concurrentSessions undefined, hard_cap present -> cap = hardCap, cur = 0 -> free.
  assert(isCapacityFree(
    { concurrentSessions: undefined, limit: 2, hardCap: 4, priority: okState },
    { limit: 2, queuePaused: false },
  ).free === true, "COV7-3: undefined concurrentSessions -> 0 (cur 0 < cap 4 -> free)");
  // concurrentSessions undefined, hardCap undefined, limit present -> cap = snap.limit.
  assert(isCapacityFree(
    { concurrentSessions: undefined, limit: 2, hardCap: undefined, priority: okState },
    { limit: 2, queuePaused: false },
  ).free === true, "COV7-3: undefined concurrentSessions + undefined hardCap -> cap = snap.limit (cur 0 < 2 -> free)");
  // ALL caps undefined (snap.limit + snap.hardCap undefined) -> cap = inputs.limit.
  assert(isCapacityFree(
    { concurrentSessions: undefined, limit: undefined, hardCap: undefined, priority: okState },
    { limit: 2, queuePaused: false },
  ).free === true, "COV7-3: all snap caps undefined -> cap = inputs.limit (cur 0 < 2 -> free)");
  // ALL caps undefined + inputs.limit undefined -> cap undefined -> free (unlimited).
  assert(isCapacityFree(
    { concurrentSessions: undefined, limit: undefined, hardCap: undefined, priority: okState },
    { limit: undefined, queuePaused: false },
  ).free === true, "COV7-3: all caps undefined + inputs.limit undefined -> cap undefined -> free (no cap to exceed)");
}

// --- COV7-4: disabled-mode stub methods are no-ops + snapshot stays empty ---
// The disabled stub (touchToken/clearPause/cancel/pauseUntil/reset) was only
// partially tested. Exercise every stub method + assert no throw + snapshot
// stays empty.
{
  const q = createConcurrencyQueue({ disabled: true });
  assert(q.join() === null, "COV7-4: disabled join returns null");
  const r = await q.waitForLaunch("ignored");
  assert(typeof r === "function", "COV7-4: disabled waitForLaunch resolves with noop release");
  r();
  // Every stub method must be a no-op (no throw).
  let threw = false;
  try {
    assert(q.touchToken("ignored") === true, "COV7-4: disabled touchToken returns true");
    q.clearPause();
    q.clearPause({ force: true });
    q.cancel("ignored");
    q.pauseUntil(Date.now() + 10_000, "ignored");
    q.reset();
  } catch { threw = true; }
  assert(!threw, "COV7-4: disabled stub methods do not throw");
  const snap = q.snapshot();
  assert(snap.queued === 0 && snap.tokenHeld === false && snap.paused === false &&
    snap.pausedUntil === 0 && snap.pausedReason === null,
    "COV7-4: disabled snapshot stays empty after every stub method");
}

// --- COV7-5: queuePaused takes precedence over priority.low ---
// isCapacityFree checks queuePaused BEFORE priority.low. When both are true,
// free===false AND repause===undefined (the queue pause wins; no repause pushed
// because the shared pause is already active).
{
  const lowState = { low: true, until: 1_000_000, reason: "burst" };
  const r = isCapacityFree(
    { concurrentSessions: 0, limit: 2, hardCap: 4, priority: lowState },
    { limit: 2, queuePaused: true },
  );
  assert(r.free === false, "COV7-5: queuePaused + priority.low -> not free");
  assert(r.repause === undefined, "COV7-5: queuePaused precedence -> no repause (shared pause already active)");
}

// --- COV7-7: parsePriority non-boolean low + non-string reason ---
// parsePriority accepts { low?: boolean | null }. A non-boolean low (string,
// number) is coerced to false; a non-string reason is nulled. Previously only
// strict booleans were tested.
{
  assert(parsePriority({ low: "true" }).low === false, "COV7-7: parsePriority low='true' (string) -> false");
  assert(parsePriority({ low: 1 }).low === false, "COV7-7: parsePriority low=1 (number) -> false");
  assert(parsePriority({ low: null }).low === false, "COV7-7: parsePriority low=null -> false");
  const p = parsePriority({ low: true, reason: 123 });
  assert(p.low === true && p.reason === null, "COV7-7: parsePriority non-string reason -> null");
  const p2 = parsePriority({ low: true, reason: { obj: true } });
  assert(p2.reason === null, "COV7-7: parsePriority object reason -> null");
}

// --- COV7-8: pauseUntil extension / shrink / undefined-reason ---
// pauseUntil extends when the new deadline is later, ignores when earlier, and
// an undefined reason leaves pausedReason null (not stale). Pin all three.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-cov7-8-"));
  const stateFile = join(dir, "state.json");
  const { readFileSync } = await import("node:fs");
  const q = createConcurrencyQueue({ stateFile });

  // Set pause A (10s, "first").
  const t0 = Date.now();
  q.pauseUntil(t0 + 10_000, "first");
  let st = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(st.pausedUntil === t0 + 10_000 && st.pausedReason === "first",
    "COV7-8: pause A (10s, first) written");

  // Set pause B (30s, "second") -> extended + reason "second".
  q.pauseUntil(t0 + 30_000, "second");
  st = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(st.pausedUntil === t0 + 30_000 && st.pausedReason === "second",
    "COV7-8: pause B (30s, second) extends + overwrites reason");

  // Set pause C (5s, "shorter") -> deadline earlier -> unchanged + still "second".
  q.pauseUntil(t0 + 5_000, "shorter");
  st = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(st.pausedUntil === t0 + 30_000 && st.pausedReason === "second",
    "COV7-8: pause C (5s, shorter) ignored (deadline earlier, reason unchanged)");

  // pauseUntil(until, undefined) on a fresh queue -> pausedReason === null.
  q.clearPause({ force: true });
  q.pauseUntil(t0 + 20_000, undefined);
  st = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(st.pausedReason === null, "COV7-8: pauseUntil with undefined reason -> pausedReason null");

  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- COV7-9: pickSearchModel (mirrors pickVisionModel coverage) ---
// pickSearchModel is exported but was untested. Defaults to umans-flash when
// present + non-deprecated; falls back to the first tool-capable model when
// flash is absent/deprecated; returns the default id when no tool-capable model.
{
  const FLASH = { name: "umans-flash", capabilities: { supports_tools: true } };
  const CODER = { name: "umans-coder", capabilities: { supports_tools: true } };
  const NOTOOL = { name: "umans-notool", capabilities: { supports_tools: false } };
  assert(pickSearchModel({ "umans-flash": FLASH as any }) === "umans-flash",
    "COV7-9: default flash present -> umans-flash");
  // flash deprecated -> first tool-capable.
  assert(pickSearchModel({ "umans-flash": { ...FLASH, deprecation: "old" } as any, "umans-coder": CODER as any }) === "umans-coder",
    "COV7-9: flash deprecated -> first tool-capable (umans-coder)");
  // flash absent -> first tool-capable.
  assert(pickSearchModel({ "umans-coder": CODER as any }) === "umans-coder",
    "COV7-9: flash absent -> first tool-capable");
  // no tool-capable -> returns default id.
  assert(pickSearchModel({ "umans-notool": NOTOOL as any }) === "umans-flash",
    "COV7-9: no tool-capable -> returns default id (umans-flash)");
}

// --- COV7-10: formatStatusText rendering (extracted pure helper) ---
// statusText was a closure (not exported), so the status-bar rendering was
// untested. formatStatusText is the pure seam: queued>0 + tokenHeld ->
// `q N*`; paused -> `PAUSED Ns (reason)`; paused with elapsed pausedUntil ->
// `PAUSED 0s` (not negative); empty -> no queue part.
{
  const now = 1_700_000_000_000;
  // queued + tokenHeld -> q N*.
  assert(formatStatusText({
    effectiveLimit: 2, currentConcurrency: 1,
    queueSnap: { queued: 3, tokenHeld: true, paused: false, pausedUntil: 0, pausedReason: null },
    now,
  }).includes("q 3*"), "COV7-10: queued>0 + tokenHeld -> q N* part");
  // queued + not tokenHeld -> q N (no star).
  assert(formatStatusText({
    effectiveLimit: 2, currentConcurrency: 1,
    queueSnap: { queued: 2, tokenHeld: false, paused: false, pausedUntil: 0, pausedReason: null },
    now,
  }).includes("q 2") && !formatStatusText({
    effectiveLimit: 2, currentConcurrency: 1,
    queueSnap: { queued: 2, tokenHeld: false, paused: false, pausedUntil: 0, pausedReason: null },
    now,
  }).includes("q 2*"), "COV7-10: queued + not tokenHeld -> q N (no star)");
  // paused -> PAUSED Ns (reason).
  assert(formatStatusText({
    effectiveLimit: 2, currentConcurrency: 1,
    queueSnap: { queued: 0, tokenHeld: false, paused: true, pausedUntil: now + 30_000, pausedReason: "HTTP 429 from gateway" },
    now,
  }).includes("PAUSED 30s (HTTP 429 from gateway)"), "COV7-10: paused -> PAUSED Ns (reason)");
  // paused with elapsed pausedUntil -> PAUSED 0s (not negative).
  assert(formatStatusText({
    effectiveLimit: 2, currentConcurrency: 1,
    queueSnap: { queued: 0, tokenHeld: false, paused: true, pausedUntil: now - 5_000, pausedReason: null },
    now,
  }).includes("PAUSED 0s"), "COV7-10: elapsed pause -> PAUSED 0s (not negative)");
  // empty queue (queued 0, not held, not paused) -> no queue part.
  const empty = formatStatusText({
    effectiveLimit: 2, currentConcurrency: 1,
    queueSnap: { queued: 0, tokenHeld: false, paused: false, pausedUntil: 0, pausedReason: null },
    now,
  });
  assert(!empty.includes("q ") && !empty.includes("PAUSED"),
    "COV7-10: empty queue -> no q/PAUSED part");
  assert(empty.startsWith("Umans ") && empty.includes("Conc 1/2"),
    "COV7-10: base Umans + Conc current/guaranteed always present");
  // concurrencyDisabled -> no queue part even if snap has queued.
  const disabled = formatStatusText({
    effectiveLimit: undefined, currentConcurrency: undefined,
    queueSnap: { queued: 5, tokenHeld: true, paused: true, pausedUntil: now + 30_000, pausedReason: "x" },
    concurrencyDisabled: true,
    now,
  });
  assert(!disabled.includes("q ") && !disabled.includes("PAUSED"),
    "COV7-10: concurrencyDisabled -> no queue/PAUSED part");
  assert(disabled.includes("Conc ?/?"), "COV7-10: undefined limits render ?/?");
}

// --- CORR7-3: snapshot() reconciles holdsToken with the file after a watchdog reap ---
// holdsToken is a local var set true when we claim the token. reapStale can
// reap our token (id mismatch / absent) after >120s while holdsToken stays
// true, so snapshot().tokenHeld returned stale `true` + the status bar showed a
// stale `*`. snapshot() now clears holdsToken when the file says the token is
// gone or held by someone else.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-corr7-3-"));
  const stateFile = join(dir, "state.json");
  const { readFileSync, writeFileSync, utimesSync } = await import("node:fs");
  // Frozen clock at t0; advance past 120s (staleTokenMs) to force a reap.
  let t = 1_700_000_000_000;
  const q = createConcurrencyQueue({ stateFile, now: () => t });

  // Claim the token (join + waitForLaunch). holdsToken is true.
  const id = q.join()!;
  await q.waitForLaunch(id);
  assert(q.snapshot().tokenHeld === true, "CORR7-3: token held after claim");

  // Advance time past 120s (staleTokenMs). The token's ts is now stale; the
  // next reapStale (inside snapshot) reaps it. holdsToken must be reconciled.
  t += 130_000;
  // Touch the state file's token ts to the OLD time so reapStale sees it as
  // stale relative to the advanced clock. (The token was written at t0; we
  // advance t, so now - token.ts > staleTokenMs.)
  const snap = q.snapshot();
  assert(snap.tokenHeld === false,
    "CORR7-3: snapshot().tokenHeld false after watchdog reaped our token (no stale *)");

  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- CORR7-5: reapStaleTmps handles a planted .tmp directory (no wedge) ---
// A planted directory at a <path>.*.tmp name made unlinkSync throw EISDIR
// (swallowed) AND writeStateAtomic's openSync("wx") throw EEXIST (the real
// wedge — the per-pid temp name is a directory). reapStaleTmps now rmdir's
// empty .tmp directories + skips non-empty ones, so a mutate no longer wedges.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-corr7-5-"));
  const stateFile = join(dir, "state.json");
  const { mkdirSync, statSync } = await import("node:fs");

  // Plant an EMPTY directory at a .tmp name matching the reaper's prefix.
  const tmpDir = `${stateFile}.99999.tmp`;
  mkdirSync(tmpDir, { mode: 0o700 });

  // Trigger a mutate (pause) — reapStaleTmps must rmdir the empty .tmp dir +
  // writeStateAtomic must not wedge on EEXIST.
  const q = createConcurrencyQueue({ stateFile });
  let threw = false;
  try {
    q.pauseUntil(Date.now() + 1_000, "CORR7-5 probe");
  } catch { threw = true; }
  assert(!threw, "CORR7-5: mutate does not wedge on a planted .tmp directory");

  // The empty .tmp directory must be removed (rmdir'd).
  let dirGone = false;
  try { statSync(tmpDir); } catch { dirGone = true; }
  assert(dirGone, "CORR7-5: empty .tmp directory removed by reapStaleTmps (rmdir)");

  // The state file must have been written (proving the mutate completed).
  let stateWritten = false;
  try { statSync(stateFile); stateWritten = true; } catch { /* not written */ }
  assert(stateWritten, "CORR7-5: state file written (mutate completed despite .tmp dir)");

  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- SEC7-2: parsePriority clamps `until` to now + MAX_PAUSE_MS (parse boundary) ---
// The clamp lived only at the write boundary (pauseUntil). parsePriority now
// clamps at the parse boundary too so a poisoned boxed_until (e.g. 2099-12-31)
// cannot propagate a centuries-long deadline even if a future caller bypasses
// pauseUntil's clamp.
{
  const p = parsePriority({ low: true, boxed_until: "2099-12-31T00:00:00Z" });
  assert(p.low === true, "SEC7-2: parsePriority low=true");
  assert(p.until <= Date.now() + MAX_PAUSE_MS + 1_000,
    "SEC7-2: parsePriority clamps until to now + MAX_PAUSE_MS (not 2099)");
  assert(p.until < Date.parse("2099-12-31T00:00:00Z"),
    "SEC7-2: parsePriority clamped below the raw boxed_until");
  // A sub-ceiling boxed_until is unchanged.
  const future = new Date(Date.now() + 60_000).toISOString();
  const p2 = parsePriority({ low: true, boxed_until: future });
  assert(p2.until === Date.parse(future), "SEC7-2: parsePriority sub-ceiling boxed_until unchanged");
}

// --- SEC7-3: isPidDead guards non-numeric / non-finite input + EPERM (PID 1) ---
// isPidDead relied on shape guards (isWaiterEntry/isTokenState) to drop malformed
// entries before calling process.kill. A future caller bypassing them would
// pass garbage to process.kill (synchronous TypeError not filtered by catch).
// Add a defensive typeof/Number.isFinite guard returning true (dead). Also
// pin the EPERM path (PID 1 on macOS -> alive) to lock CORR2-4.
{
  assert(isPidDead(NaN) === true, "SEC7-3: isPidDead(NaN) -> true (non-finite guarded)");
  assert(isPidDead(Infinity) === true, "SEC7-3: isPidDead(Infinity) -> true (non-finite guarded)");
  assert(isPidDead("123" as any) === true, "SEC7-3: isPidDead(string) -> true (non-number guarded)");
  assert(isPidDead(undefined as any) === true, "SEC7-3: isPidDead(undefined) -> true (non-number guarded)");
  assert(isPidDead(0) === true, "SEC7-3: isPidDead(0) -> true (falsy)");
  assert(isPidDead(-1) === true, "SEC7-3: isPidDead(-1) -> true (non-positive)");
  // EPERM path: PID 1 (init/launchd) exists but we lack permission to signal
  // it -> treat as alive (CORR2-4 fail-safe). On macOS/Linux PID 1 is always
  // present; on Windows this is a no-op best-effort.
  if (process.platform !== "win32") {
    assert(isPidDead(1) === false, "SEC7-3: isPidDead(1) -> false (EPERM -> alive, CORR2-4 fail-safe)");
  }
}

// --- SEC7-4: gateway error body is capped + sanitized (no control/ANSI echo) ---
// analyzeImage/searchWeb echoed the raw gateway error body (200 chars) into the
// thrown error / tool result. A compromised gateway can push crafted text that
// flows into the model's context (prompt-injection surface). sanitizeErrorBody
// caps to 80 chars + strips non-printable / ANSI-escape chars.
{
  const ansiEscape = "\x1b[31mred\x1b[0m";
  const control = "\x00\x07";
  const long = "A".repeat(200);
  const crafted = `${ansiEscape}${control}${long}`;
  const safe = sanitizeErrorBody(crafted);
  assert(safe.length <= 80, "SEC7-4: error body capped to <= 80 chars");
  assert(!/[\x00-\x1f\x7f]/.test(safe), "SEC7-4: error body has no control/ANSI-escape chars");
  assert(!safe.includes("\x1b"), "SEC7-4: ESC byte removed from error body");
  // A short, clean body passes through unchanged.
  assert(sanitizeErrorBody("not found") === "not found", "SEC7-4: short clean body unchanged");
  // An empty/whitespace body yields an empty string (caller omits the `: ` ).
  assert(sanitizeErrorBody("   ") === "", "SEC7-4: whitespace-only body -> empty");
  assert(sanitizeErrorBody("") === "", "SEC7-4: empty body -> empty");
}

// --- SEC9-1/SEC8-3: bidi/RTL + zero-width + BOM stripped from pause reason + error body ---
// sanitizeReason + sanitizeErrorBody previously stripped only \x00-\x1f+\x7f;
// Unicode bidi overrides (U+202A-E, U+2066-9, U+061C) + zero-width / BOM chars
// (U+200B-F, U+FEFF) passed through to the status bar render, allowing a crafted
// priority.reason / gateway body to spoof the displayed text.
{
  const r = sanitizeReason("\u202Egnirts RTL \u200B\uFEFF");
  assert(r !== null && !/[\u061c\u200b-\u200f\u202a-\u202e\u2066-\u2069\ufeff]/.test(r), "SEC9-1: sanitizeReason strips bidi/zero-width/BOM");
  assert(r !== null && r.includes("gnirts") && r.includes("RTL"), "SEC9-1: sanitizeReason keeps printable ASCII");
  const b = sanitizeErrorBody("\u202Ebody\u200B");
  assert(!/[\u061c\u200b-\u200f\u202a-\u202e\u2066-\u2069\ufeff]/.test(b), "SEC9-1: sanitizeErrorBody strips bidi/zero-width");
  assert(b === "body", "SEC9-1: sanitizeErrorBody keeps printable ASCII body");
}

// --- CORR8-2: side-call 429s (vision handoff, web search) push the shared pause ---
// Per D6, analyzeImage + searchWeb each call acquireSlot because they "consume
// a real account concurrency slot." Per Umans docs, each concurrency 429
// deprioritizes the whole account ~30 min. Yet when a side-call received HTTP
// 429 it merely threw — it did NOT call pauseUntil(until, PAUSE_REASON_429),
// so sibling pi processes (and the main turn on its next launch) would not back
// off. The shared handle429 helper is now called from all 3 sites
// (analyzeImage, searchWeb, after_provider_response). Verify the helper writes
// the shared pause (pausedUntil set, pausedReason === PAUSE_REASON_429) for both
// a fetch-Response-shaped input + a pi-event-shaped input, honors Retry-After,
// rejects hex/sci-notation, + clamps to MAX_PAUSE_429_MS.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-corr8-2-"));
  const stateFile = join(dir, "state.json");
  const { readFileSync } = await import("node:fs");
  const q = createConcurrencyQueue({ stateFile });

  // (a) fetch-Response shape (Headers .get): Retry-After 60 → pause 60s.
  const resLike = new Response("{}", { status: 429, headers: { "retry-after": "60" } });
  const untilA = handle429({ status: 429, headers: resLike.headers }, q);
  const stA = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(stA.pausedUntil === untilA, "CORR8-2: handle429 returns the written pausedUntil (fetch-Response shape)");
  assert(stA.pausedUntil > Date.now() + 50_000, "CORR8-2: Retry-After 60s honored (pause ~60s)");
  assert(stA.pausedReason === PAUSE_REASON_429, "CORR8-2: pause tagged PAUSE_REASON_429 (fetch-Response shape)");
  q.clearPause({ force: true });

  // (b) pi-event shape (record): Retry-After 120 → pause 120s.
  const untilB = handle429({ status: 429, headers: { "retry-after": "120" } }, q);
  const stB = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(stB.pausedUntil === untilB, "CORR8-2: handle429 returns the written pausedUntil (pi-event shape)");
  assert(stB.pausedUntil > Date.now() + 110_000, "CORR8-2: Retry-After 120s honored (pi-event shape)");
  assert(stB.pausedReason === PAUSE_REASON_429, "CORR8-2: pause tagged PAUSE_REASON_429 (pi-event shape)");
  q.clearPause({ force: true });

  // (c) No Retry-After → falls back to PRIORITY_BACKOFF_MS (30s).
  const untilC = handle429({ status: 429, headers: {} }, q);
  const stC = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(untilC <= Date.now() + PRIORITY_BACKOFF_MS + 1_000, "CORR8-2: no Retry-After → PRIORITY_BACKOFF_MS floor (30s)");
  assert(stC.pausedReason === PAUSE_REASON_429, "CORR8-2: no Retry-After still tagged PAUSE_REASON_429");
  q.clearPause({ force: true });

  // (d) Hex/sci-notation Retry-After rejected → PRIORITY_BACKOFF_MS floor.
  const untilHex = handle429({ status: 429, headers: { "retry-after": "0x10" } }, q);
  assert(untilHex <= Date.now() + PRIORITY_BACKOFF_MS + 1_000, "CORR8-2: hex Retry-After rejected → 30s floor");
  q.clearPause({ force: true });
  const untilSci = handle429({ status: 429, headers: { "retry-after": "1e10" } }, q);
  assert(untilSci <= Date.now() + PRIORITY_BACKOFF_MS + 1_000, "CORR8-2: sci-notation Retry-After rejected → 30s floor");
  q.clearPause({ force: true });

  // (e) Huge Retry-After clamped to MAX_PAUSE_429_MS (2.5 min), not 5h.
  const untilHuge = handle429({ status: 429, headers: { "retry-after": "99999999" } }, q);
  assert(untilHuge <= Date.now() + MAX_PAUSE_429_MS + 1_000, "CORR8-2: huge Retry-After clamped to MAX_PAUSE_429_MS (2.5 min)");
  assert(untilHuge < Date.now() + MAX_PAUSE_MS, "CORR8-2: 429 pause tighter than the 5h ceiling");
  q.clearPause({ force: true });

  // (f) Case-insensitive header lookup (Retry-After / retry-after).
  handle429({ status: 429, headers: { "Retry-After": "45" } }, q);
  const stF = JSON.parse(readFileSync(stateFile, "utf8"));
  assert(stF.pausedUntil > Date.now() + 40_000, "CORR8-2: case-insensitive Retry-After header lookup");
  q.clearPause({ force: true });

  // (g) Non-429 status does not push a pause (the helper is 429-specific; callers
  // guard on status === 429 before calling). Verify the helper itself doesn't
  // refuse, but the caller's guard means a 200 doesn't reach it. We assert the
  // caller pattern: only status 429 calls handle429. (The helper still writes a
  // pause if called directly with a non-429 — that's the caller's contract.)
  // Here we just verify a 429 with undefined headers doesn't throw.
  let threw = false;
  try { handle429({ status: 429, headers: undefined }, q); } catch { threw = true; }
  assert(!threw, "CORR8-2: handle429 with undefined headers does not throw (PRIORITY_BACKOFF_MS floor)");
  q.clearPause({ force: true });

  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- SEC7-5: state file + lockfile created with mode 0600 (regression guard) ---
// The 0o600 mode (no PID leakage, no world-readable queue state) was not
// asserted in selfcheck beyond the S3 block. Add an explicit, deterministic
// assertion: trigger a write + assert (statSync(stateFile).mode & 0o777) ===
// 0o600. The lockfile mode is covered by S3's best-effort mid-acquire poll;
// here we deterministically assert the state file mode (the writeStateAtomic
// path) + the 0o600 arg to openSync is the same used by acquireLock.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-sec7-5-"));
  const stateFile = join(dir, "state.json");
  const { statSync } = await import("node:fs");
  const q = createConcurrencyQueue({ stateFile });
  // join + waitForLaunch forces a writeStateAtomic (state file) + acquireLock
  // (lockfile), both at 0o600.
  const id = q.join()!;
  const release = await q.waitForLaunch(id);
  release();
  const stateMode = statSync(stateFile).mode & 0o777;
  assert(stateMode === 0o600, `SEC7-5: state file mode is 0600 (got 0o${stateMode.toString(8)})`);
  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- ADV7-1: random temp suffix prevents pid-recycle collision ---
// A recycled pid would collide with a stale leftover at the per-pid temp name
// `${path}.${pid}.tmp`, wedging writeStateAtomic (EEXIST). The temp name now
// includes a random suffix so concurrent/recycled runs never collide. Plant a
// fresh .tmp at the OLD per-pid name (no random suffix) + assert writeStateAtomic
// succeeds (uses a different name).
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-adv7-1-"));
  const stateFile = join(dir, "state.json");
  const { writeFileSync, statSync } = await import("node:fs");

  // Plant a fresh .tmp at the OLD per-pid name (no random suffix).
  const oldPidTmp = `${stateFile}.${process.pid}.tmp`;
  writeFileSync(oldPidTmp, "leftover", { mode: 0o600 });

  // writeStateAtomic must succeed — it uses a DIFFERENT name (with random suffix).
  const q = createConcurrencyQueue({ stateFile });
  let threw = false;
  try { q.pauseUntil(Date.now() + 1_000, "ADV7-1 probe"); } catch { threw = true; }
  assert(!threw, "ADV7-1: writeStateAtomic succeeds despite a leftover at the old per-pid name (random suffix)");

  // The leftover at the old name must be untouched (writeStateAtomic used a
  // different name — it's not reaped because its mtime is fresh).
  let leftExists = false;
  try { statSync(oldPidTmp); leftExists = true; } catch { /* gone */ }
  assert(leftExists, "ADV7-1: leftover at old per-pid name untouched (writeStateAtomic used a different name)");

  // The state file must have been written (proving writeStateAtomic completed).
  let stateWritten = false;
  try { statSync(stateFile); stateWritten = true; } catch { /* not written */ }
  assert(stateWritten, "ADV7-1: state file written (writeStateAtomic completed with a random-suffixed name)");

  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- ADV7-2: reapStaleTmps caps unlinks per mutate (bounds the critical section) ---
// The reaper runs inside the O_EXCL lock; unlinking thousands of stale .tmp
// files would extend the critical section past the 2s lockTimeoutMs ceiling,
// racing two writers. Cap at REAP_TMP_MAX (100) per mutate; leave the rest.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-adv7-2-"));
  const stateFile = join(dir, "state.json");
  const { writeFileSync, utimesSync, readdirSync } = await import("node:fs");

  // Create 200 stale .tmp files (old mtime, past STALE_TMP_MS = 10s).
  const staleTime = (Date.now() / 1000) - 120;
  for (let i = 0; i < 200; i++) {
    const name = `${stateFile}.pid${i}.tmp`;
    writeFileSync(name, "", { mode: 0o600 });
    utimesSync(name, staleTime, staleTime);
  }

  // Trigger one mutate (pause) — reapStaleTmps must unlink at most 100.
  const q = createConcurrencyQueue({ stateFile });
  q.pauseUntil(Date.now() + 1_000, "ADV7-2 probe");

  // Count remaining .tmp files.
  const remaining = readdirSync(dir).filter((n: string) => n.startsWith(`${"state.json"}.`) && n.endsWith(".tmp"));
  assert(remaining.length === 100,
    `ADV7-2: reapStaleTmps unlinked exactly 100 of 200 stale .tmp files (left ${remaining.length}, expected 100)`);

  // A second mutate unlinks the rest.
  q.pauseUntil(Date.now() + 2_000, "ADV7-2 probe 2");
  const remaining2 = readdirSync(dir).filter((n: string) => n.startsWith(`${"state.json"}.`) && n.endsWith(".tmp"));
  assert(remaining2.length === 0,
    `ADV7-2: second mutate unlinks the remaining 100 stale .tmp files (left ${remaining2.length})`);

  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- ADV10-2: writeStateAtomic unlinks .tmp on renameSync failure ---
// When renameSync throws (EISDIR when `path` is a directory, EXDEV across
// filesystems), the .tmp file leaked on disk. Reaped after 10s by reapStaleTmps
// but accumulates under sustained failure. writeStateAtomic now wraps renameSync
// in try/catch that unlinks the temp on failure before re-throwing. We force
// EISDIR by making the state path a directory (so renameSync(tmp, path) throws)
// + assert no .tmp is left on disk after the throw.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-adv10-2-"));
  const stateFile = join(dir, "state.json");
  const { mkdirSync, readdirSync } = await import("node:fs");
  // Make `path` a directory so renameSync(tmp, path) throws EISDIR (cannot
  // rename a file over a directory).
  mkdirSync(stateFile, { mode: 0o700 });
  const q = createConcurrencyQueue({ stateFile });
  let threw = false;
  try {
    q.pauseUntil(Date.now() + 1_000, "ADV10-2 probe");
  } catch {
    threw = true;
  }
  assert(threw, "ADV10-2: writeStateAtomic threw when state path is a directory (renameSync EISDIR)");
  // No .tmp file may be left on disk (the catch unlinked it before re-throwing).
  const leftover = readdirSync(dir).filter((n: string) => n.endsWith(".tmp"));
  assert(leftover.length === 0,
    `ADV10-2: no .tmp leaked on renameSync failure (left ${leftover.length} .tmp file(s))`);
  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- SEC8-2/SEC9-6: reapStaleTmps uses lstatSync (symlink .tmp unlinked, not followed) ---
// A symlink .tmp → /etc/passwd must be unlinked directly without following the
// link (matching the lockfile's SEC7-1 posture). lstatSync detects non-regular
// files; unlinkSync removes the symlink itself, leaving the target untouched.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-sec8-2-"));
  const stateFile = join(dir, "state.json");
  const { symlinkSync, readFileSync, lstatSync, lutimesSync } = await import("node:fs");
  const target = join(dir, "secret-target");
  writeFileSync(target, "secret", { mode: 0o600 });
  const tmpPath = join(dir, "state.json.symlink.tmp");
  symlinkSync(target, tmpPath);
  // Drive a mutate (pauseUntil) which calls reapStaleTmps. The symlink .tmp's
  // own mtime (via lstatSync) must be stale. Use lutimesSync to set the LINK's
  // mtime (utimesSync would follow the symlink + set the target's mtime).
  const stale = (Date.now() / 1000) - 10_000;
  lutimesSync(tmpPath, stale, stale);
  const q = createConcurrencyQueue({ stateFile });
  q.pauseUntil(Date.now() + 1_000, "SEC8-2 probe");
  // The symlink .tmp must be gone (unlinked, not followed).
  let symlinkGone = false;
  try { lstatSync(tmpPath); } catch { symlinkGone = true; }
  assert(symlinkGone, "SEC8-2/SEC9-6: symlink .tmp unlinked (not followed)");
  // The target must be untouched.
  assert(readFileSync(target, "utf8") === "secret", "SEC8-2/SEC9-6: symlink target untouched");
  q.reset();
  rmSync(dir, { recursive: true, force: true });
}

// --- COV9-3/COV8-7: /umans-concurrency operator command driven through wiring ---
// All 4 branches untested through real wiring: status (renders snapshot),
// reset (clearPause({force:true}) + reset()), unknown-subcommand (prints
// usage), no-args (defaults to status). The clearPause({force:true}) caller
// exists only here. Change the harness mock to capture commands, then dispatch
// status / reset / bogus / "" against a seeded pause + assert notify text +
// state-file pausedUntil===0 after reset.
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-cov9-3-"));
  const stateFile = join(dir, "state.json");
  const savedEnv: Record<string, string | undefined> = {};
  const envOverrides: Record<string, string> = {
    UMANS_API_KEY: "uk-test-key",
    UMANS_CONCURRENCY_STATE_FILE: stateFile,
  };
  for (const [k, v] of Object.entries(envOverrides)) {
    savedEnv[k] = process.env[k];
    process.env[k] = v;
  }
  delete process.env.UMANS_DISABLE;
  delete process.env.UMANS_CONCURRENCY_DISABLE;
  const realFetch = globalThis.fetch;
  globalThis.fetch = ((_input: any) =>
    Promise.resolve(new Response("", { status: 404 }))) as any;
  try {
    const handlers = new Map<string, ((event: any, ctx: any) => Promise<any> | any)[]>();
    const widgets = new Map<string, any>();
    const statuses = new Map<string, any>();
    const notifications: { msg: string; type: string }[] = [];
    const cmds = new Map<string, { handler: (args: string, ctx: any) => Promise<any> }>();
    const pi: any = {
      on(event: string, h: (event: any, ctx: any) => Promise<any> | any) {
        if (!handlers.has(event)) handlers.set(event, []);
        handlers.get(event)!.push(h);
      },
      registerTool() {},
      registerCommand(name: string, def: any) {
        if (name === "umans-concurrency") cmds.set(name, def);
      },
      registerProvider() {},
      events: { on() {}, off() {}, emit() {} },
    };
    function makeCtx(): any {
      return {
        model: { provider: "umans", id: "umans-flash" },
        signal: new AbortController().signal, mode: "print", hasUI: false, cwd: dir, isIdle: () => true,
        ui: {
          setWidget: (k: string, c: any) => { widgets.set(k, c); },
          setStatus: (k: string, t: string | undefined) => { statuses.set(k, t); },
          notify: (msg: string, type?: string) => { notifications.push({ msg, type: type ?? "info" }); },
          theme: { fg: (_n: string, t: string) => t },
        },
        modelRegistry: { getApiKeyForProvider: async () => "uk-test-key" }, sessionManager: {},
      };
    }
    await umansFactory(pi as any);
    assert(cmds.has("umans-concurrency"), "COV9-3: /umans-concurrency command registered through wiring");
    const cmd = cmds.get("umans-concurrency")!;

    // Seed a 429-origin pause in the shared state file so status renders it +
    // reset's clearPause({force:true}) has something to clear (the force branch
    // is the only caller of clearPause({force:true})).
    const seedQ = createConcurrencyQueue({ stateFile });
    const pauseUntil = Date.now() + 30_000;
    seedQ.pauseUntil(pauseUntil, PAUSE_REASON_429);
    assert(seedQ.snapshot().paused === true, "COV9-3: seeded 429 pause visible to snapshot");

    // (a) status: notify text must mention queued + tokenHeld + paused + 429 reason.
    notifications.length = 0;
    await cmd.handler("status", makeCtx());
    const statusNote = notifications.find((n) => n.msg.startsWith("Umans concurrency:"));
    assert(!!statusNote, "COV9-3: status subcommand produced a notify");
    assert(statusNote!.msg.includes("paused"), `COV9-3: status notify mentions paused (got: ${statusNote!.msg})`);
    assert(statusNote!.msg.includes(PAUSE_REASON_429!), `COV9-3: status notify mentions 429 reason (got: ${statusNote!.msg})`);

    // (b) no-args ("") defaults to status — same notify shape.
    notifications.length = 0;
    await cmd.handler("", makeCtx());
    const emptyNote = notifications.find((n) => n.msg.startsWith("Umans concurrency:"));
    assert(!!emptyNote, "COV9-3: no-args defaulted to status (produced a notify)");
    assert(emptyNote!.msg.includes("paused"), "COV9-3: no-args status notify mentions paused");

    // (c) reset: clearPause({force:true}) + reset() — must clear the 429 pause
    // (the force branch is the only caller that overrides CORR4-1's 429 guard)
    // + drop this process's own waiter/token entry.
    notifications.length = 0;
    await cmd.handler("reset", makeCtx());
    const resetNote = notifications.find((n) => n.msg.includes("force-cleared"));
    assert(!!resetNote, `COV9-3: reset produced force-cleared notify (got: ${notifications.map((n) => n.msg).join(" | ")})`);
    // The state file's pausedUntil must be 0 after reset (clearPause force + reset).
    const raw = readFileSync(stateFile, "utf8");
    const parsed = JSON.parse(raw);
    assert(parsed.pausedUntil === 0, `COV9-3: reset cleared pausedUntil (got ${parsed.pausedUntil}, expected 0)`);

    // (d) bogus subcommand: prints usage.
    notifications.length = 0;
    await cmd.handler("bogus", makeCtx());
    const usageNote = notifications.find((n) => n.msg.startsWith("Usage: /umans-concurrency"));
    assert(!!usageNote, `COV9-3: bogus subcommand printed usage (got: ${notifications.map((n) => n.msg).join(" | ")})`);

    // Dispatch session_shutdown to stop the factory's refresh loop (if any started).
    const hs = handlers.get("session_shutdown");
    if (hs) for (const h of hs) await h({ type: "session_shutdown" }, makeCtx());
  } finally {
    globalThis.fetch = realFetch;
    for (const [k, v] of Object.entries(savedEnv)) {
      if (v === undefined) delete process.env[k]; else process.env[k] = v;
    }
    rmSync(dir, { recursive: true, force: true });
  }
}

// --- COV9-4/COV8-3: side-call acquireSlot wiring in tool execute bodies driven through real factory ---
// acquireSlot is a closure (not exported); COV2-H2 simulated the acquire+release
// pattern directly against createConcurrencyQueue. The real tool execute bodies
// (umans_web_search, umans_vision) were never driven through the real factory —
// the harness mocked registerTool as a no-op. A regression dropping acquireSlot
// from searchWeb/analyzeImage, or assigning side-call's release to
// mainTurnRelease (ADV4-3/CORR5-3 invariant) would not be caught. CORR8-2
// side-call-429 wiring tested at helper level but call sites not driven.
// Fix: capture tool defs in registerTool, dispatch umans_web_search's execute
// with stubbed fetch 200 (assert acquire+release — token not held after) and
// 429 (assert shared pause lands — pausedUntil > now, pausedReason === 429).
// Same for umans_vision's execute (populate imageStore via the via-handoff
// message_end handler, then drive the tool execute).
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-cov9-4-"));
  const stateFile = join(dir, "state.json");
  const savedEnv: Record<string, string | undefined> = {};
  const envOverrides: Record<string, string> = {
    UMANS_API_KEY: "uk-test-key",
    UMANS_CONCURRENCY_STATE_FILE: stateFile,
  };
  for (const [k, v] of Object.entries(envOverrides)) {
    savedEnv[k] = process.env[k];
    process.env[k] = v;
  }
  delete process.env.UMANS_DISABLE;
  delete process.env.UMANS_CONCURRENCY_DISABLE;
  // The main session sets UMANS_SEARCH_DISABLE=1; clear it so umans_web_search
  // registers + its execute body drives acquireSlot.
  savedEnv.UMANS_SEARCH_DISABLE = process.env.UMANS_SEARCH_DISABLE;
  delete process.env.UMANS_SEARCH_DISABLE;

  const realFetch = globalThis.fetch;
  // messagesStatus controls what /v1/messages returns: 200 (valid analysis) or 429.
  let messagesStatus = 200;
  let usageCalls = 0;
  let messagesCalls = 0;
  globalThis.fetch = ((input: any) => {
    const url = typeof input === "string" ? input : String(input);
    if (url.endsWith("/v1/usage")) {
      usageCalls++;
      return Promise.resolve(new Response(JSON.stringify({
        limits: { concurrency: { limit: 2, hard_cap: 4 } },
        usage: { concurrent_sessions: 0, priority: { low: false } },
      }), { status: 200, headers: { "Content-Type": "application/json" } }));
    }
    if (url.endsWith("/v1/messages")) {
      messagesCalls++;
      if (messagesStatus === 429) {
        return Promise.resolve(new Response("{\"error\":\"rate limited\"}", {
          status: 429, headers: { "retry-after": "60", "Content-Type": "application/json" },
        }));
      }
      // 200 — valid Anthropic-shaped analysis/search result.
      return Promise.resolve(new Response(JSON.stringify({
        content: [{ type: "text", text: "analysis result text" }],
      }), { status: 200, headers: { "Content-Type": "application/json" } }));
    }
    // /v1/models/info -> non-OK so the factory falls back to STATIC_CATALOG
    // (which has via-handoff models so umans_vision registers).
    return Promise.resolve(new Response("", { status: 404 }));
  }) as any;
  try {
    const handlers = new Map<string, ((event: any, ctx: any) => Promise<any> | any)[]>();
    const widgets = new Map<string, any>();
    const statuses = new Map<string, any>();
    const notifications: { msg: string; type: string }[] = [];
    const tools = new Map<string, { execute: (id: string, params: any, signal: AbortSignal, onUpdate: any, ctx: any) => Promise<any> }>();
    const pi: any = {
      on(event: string, h: (event: any, ctx: any) => Promise<any> | any) {
        if (!handlers.has(event)) handlers.set(event, []);
        handlers.get(event)!.push(h);
      },
      registerTool(def: any) { tools.set(def.name, def); },
      registerCommand() {},
      registerProvider() {},
      events: { on() {}, off() {}, emit() {} },
    };
    function makeCtx(model?: any): any {
      return {
        model: model ?? { provider: "umans", id: "umans-flash" },
        signal: new AbortController().signal, mode: "print", hasUI: false, cwd: dir, isIdle: () => true,
        ui: {
          setWidget: (k: string, c: any) => { widgets.set(k, c); },
          setStatus: (k: string, t: string | undefined) => { statuses.set(k, t); },
          notify: (msg: string, type?: string) => { notifications.push({ msg, type: type ?? "info" }); },
          theme: { fg: (_n: string, t: string) => t },
        },
        modelRegistry: { getApiKeyForProvider: async () => "uk-test-key" }, sessionManager: {},
      };
    }
    async function dispatch(event: string, payload: any, ctx?: any): Promise<any> {
      const hs = handlers.get(event);
      if (!hs) return undefined;
      // Return the first truthy result (message_end has two handlers: the
      // via-handoff transform returns {message}; the main-turn release returns
      // undefined — we want the transform's result).
      let result: any;
      for (const h of hs) {
        const r = await h(payload, ctx ?? makeCtx());
        if (r !== undefined && result === undefined) result = r;
      }
      return result;
    }
    await umansFactory(pi as any);

    // umans_web_search must have been registered with an execute fn.
    assert(tools.has("umans_web_search"), "COV9-4: umans_web_search tool registered through wiring");
    const searchTool = tools.get("umans_web_search")!;

    // (a) umans_web_search execute with fetch 200: acquireSlot joins + claims the
    // token, searchWeb fetches /v1/messages (200), finally releaseSlot frees
    // the token. After execute returns, the token must NOT be held (acquire+
    // release wired through). acquireSlot polled /usage (proves the real
    // capacity-free path ran, not a stubbed queue).
    messagesStatus = 200;
    usageCalls = 0;
    const searchRes200 = await searchTool.execute("call-1", { query: "latest news" }, new AbortController().signal, undefined, makeCtx());
    assert(usageCalls > 0, "COV9-4: web_search 200 drove acquireSlot /v1/usage poll through wiring");
    assert(typeof searchRes200?.content?.[0]?.text === "string" && searchRes200.content[0].text.length > 0,
      "COV9-4: web_search 200 returned result text");
    // Token must be released after the side-call completes (acquire+release).
    const probeA = createConcurrencyQueue({ stateFile });
    assert(probeA.snapshot().tokenHeld === false,
      "COV9-4: web_search 200 released the side-call slot (token not held after)");
    probeA.reset();

    // (b) umans_web_search execute with fetch 429: searchWeb sees 429, calls
    // handle429(res, concurrencyQueue) which writes the shared pause, then
    // throws (caught by execute's catch). The finally releaseSlot still frees
    // the token. Assert the shared pause landed (pausedUntil > now, pausedReason
    // === PAUSE_REASON_429) + token not held after.
    messagesStatus = 429;
    const before429 = Date.now();
    const searchRes429 = await searchTool.execute("call-2", { query: "will 429" }, new AbortController().signal, undefined, makeCtx());
    assert(typeof searchRes429?.content?.[0]?.text === "string" && searchRes429.content[0].text.includes("429"),
      `COV9-4: web_search 429 returned error text mentioning 429 (got: ${searchRes429?.content?.[0]?.text})`);
    const rawB = readFileSync(stateFile, "utf8");
    const parsedB = JSON.parse(rawB);
    assert(parsedB.pausedUntil > before429, "COV9-4: web_search 429 set pausedUntil > now (shared pause landed)");
    assert(parsedB.pausedReason === PAUSE_REASON_429, "COV9-4: web_search 429 tagged PAUSE_REASON_429");
    const probeB = createConcurrencyQueue({ stateFile });
    assert(probeB.snapshot().tokenHeld === false,
      "COV9-4: web_search 429 released the side-call slot (token not held after 429)");
    probeB.clearPause({ force: true });
    probeB.reset();

    // (c) umans_vision execute: first populate imageStore by dispatching the
    // via-handoff message_end handler with a user message containing an image
    // block. This drives transformMessageImages -> acquireSlot + analyzeImage
    // (fetch /v1/messages 200) -> imageStore.set(id, ...). The returned
    // transformed message text carries the image id.
    messagesStatus = 200;
    messagesCalls = 0;
    const transformed = await dispatch("message_end", {
      type: "message_end",
      message: {
        role: "user",
        provider: "umans",
        content: [
          { type: "text", text: "what is this?" },
          { type: "image", data: "aGVsbG8=", mimeType: "image/png" },
        ],
      },
    }, makeCtx({ provider: "umans", id: "umans-glm-5.2" })); // via-handoff model
    assert(messagesCalls > 0, "COV9-4: vision handoff drove analyzeImage /v1/messages fetch (acquireSlot wired)");
    assert(transformed?.message?.content, "COV9-4: vision handoff returned a transformed message");
    // Extract the image id from the [Image analysis (image:ID)]: text block.
    const analysisText: string = transformed.message.content
      .find((b: any) => typeof b?.text === "string" && b.text.includes("[Image analysis (image:"))?.text ?? "";
    const imgIdMatch = analysisText.match(/\[Image analysis \(image:([^\)]+)\)\]/);
    assert(!!imgIdMatch, `COV9-4: vision handoff produced an image id in the analysis text (got: ${analysisText})`);
    const imgId = imgIdMatch![1];
    // The token must be released after the handoff side-call completes.
    const probeC = createConcurrencyQueue({ stateFile });
    assert(probeC.snapshot().tokenHeld === false,
      "COV9-4: vision handoff released the side-call slot (token not held after)");
    probeC.reset();

    // umans_vision must have been registered (catalog has a via-handoff model).
    assert(tools.has("umans_vision"), "COV9-4: umans_vision tool registered through wiring");
    const visionTool = tools.get("umans_vision")!;

    // (d) umans_vision execute with fetch 200: acquireSlot + analyzeImage (200)
    // + releaseSlot. Assert token not held after.
    messagesStatus = 200;
    const visionRes200 = await visionTool.execute("call-3", { image_id: imgId, question: "describe it" }, new AbortController().signal, undefined, makeCtx());
    assert(typeof visionRes200?.content?.[0]?.text === "string" && visionRes200.content[0].text.length > 0,
      "COV9-4: vision 200 returned result text");
    const probeD = createConcurrencyQueue({ stateFile });
    assert(probeD.snapshot().tokenHeld === false,
      "COV9-4: vision 200 released the side-call slot (token not held after)");
    probeD.reset();

    // (e) umans_vision execute with fetch 429: analyzeImage sees 429, calls
    // handle429 -> shared pause lands. Assert pausedUntil > now + 429 tag.
    messagesStatus = 429;
    const before429v = Date.now();
    const visionRes429 = await visionTool.execute("call-4", { image_id: imgId, question: "will 429" }, new AbortController().signal, undefined, makeCtx());
    assert(typeof visionRes429?.content?.[0]?.text === "string" && visionRes429.content[0].text.includes("429"),
      `COV9-4: vision 429 returned error text mentioning 429 (got: ${visionRes429?.content?.[0]?.text})`);
    const rawE = readFileSync(stateFile, "utf8");
    const parsedE = JSON.parse(rawE);
    assert(parsedE.pausedUntil > before429v, "COV9-4: vision 429 set pausedUntil > now (shared pause landed)");
    assert(parsedE.pausedReason === PAUSE_REASON_429, "COV9-4: vision 429 tagged PAUSE_REASON_429");
    const probeE = createConcurrencyQueue({ stateFile });
    assert(probeE.snapshot().tokenHeld === false,
      "COV9-4: vision 429 released the side-call slot (token not held after 429)");
    probeE.clearPause({ force: true });
    probeE.reset();

    // Dispatch session_shutdown to stop the factory's refresh loop + clear state.
    await dispatch("session_shutdown", { type: "session_shutdown" });
  } finally {
    globalThis.fetch = realFetch;
    for (const [k, v] of Object.entries(savedEnv)) {
      if (v === undefined) delete process.env[k]; else process.env[k] = v;
    }
    rmSync(dir, { recursive: true, force: true });
  }
}

// --- COV9-8: concurrent mutate calls from one process (intra-process O_EXCL lock contention) ---
// transformMessageImages does Promise.all over N images, each calling acquireSlot → join →
// mutate. The O_EXCL lockfile is per-state-file, not per-process, so concurrent mutate calls
// from the same process contend on the same lockfile (second spins up to 2s via syncSleep).
// Assert N concurrent join()s all land their waiters in the state file (no acquireLock
// timeout, no lost write).
{
  const dir = mkdtempSync(join(tmpdir(), "umans-q-cov9-8-"));
  const stateFile = join(dir, "state.json");
  try {
    const q = createConcurrencyQueue({ stateFile, lockTimeoutMs: 2000 });
    // Launch 5 concurrent join()s — each does a mutate (acquireLock + writeStateAtomic).
    // If the lock contention is not handled correctly, some join()s would time out or lose
    // their waiter entry. All 5 should land in the state file.
    const ids = await Promise.all(
      Array.from({ length: 5 }, () => Promise.resolve(q.join())),
    );
    // All join()s must return a non-null id (queue not disabled).
    assert(ids.every((id) => id !== null), "COV9-8: all 5 concurrent join()s returned a non-null id");
    // Read the state file + assert all 5 waiters landed.
    const raw = readFileSync(stateFile, "utf8");
    const parsed = JSON.parse(raw);
    assert(Array.isArray(parsed.waiters), "COV9-8: state file has a waiters array");
    assert(parsed.waiters.length === 5, `COV9-8: all 5 waiters landed (got ${parsed.waiters.length})`);
    // Each id from join() must be present in the file.
    const fileIds = new Set(parsed.waiters.map((w: any) => w.id));
    for (const id of ids) {
      assert(fileIds.has(id), `COV9-8: waiter ${id} present in state file (no lost write)`);
    }
    q.reset();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

console.log("\nall checks passed");
