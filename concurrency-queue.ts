/**
 * Cross-process FIFO queue for outbound Umans requests, backed by a single
 * JSON file under ~/.pi/agent (umans-concurrency.json) guarded by an O_EXCL
 * lockfile. Lives in this repo as a standalone module so the queue logic is
 * unit-testable without spinning up pi.
 *
 * Design (see README → Concurrency & rate-limit safety):
 *
 * - The file is a PURE WAITER QUEUE — it holds no in-flight count and no
 *   capacity number. Whether a waiter may launch is decided solely by the
 *   live /v1/usage response (concurrent_sessions vs limits.concurrency.limit,
 *   and usage.priority.low). That authority is account-wide and shared across
 *   every machine using the key, so the file does not try to count slots that
 *   it cannot see. Multiple independent machines each run their own local
 *   queue and each serializes only its own launches; cross-machine coordination
 *   is left to the server + its ~2× headroom + the shared priority.low signal.
 *
 * - Launch token: the head waiter claims a token, polls /usage until a slot is
 *   free (and not deprioritized), then sends. The token stays held across the
 *   send until assistant message_end (the response stream has completed). The
 *   token is released there and the next head is allowed to poll. Releasing at
 *   message_end (not at after_provider_response headers, which fire ~1s in
 *   before the request is registered as in-flight, and not at turn_end, which
 *   fires after tool execution and would collapse throughput) frees the slot
 *   as soon as the stream completes AND during this turn's tool execution
 *   (tools don't consume a server concurrency slot). NOTE: message_end fires
 *   at CLIENT-side stream completion, which precedes the server's
 *   concurrent_sessions decrement by a network RTT + cleanup lag, so the next
 *   waiter's /usage poll can transiently see stale capacity and launch 1-2
 *   over `limit`. That overshoot stays within the documented burst headroom
 *   (hard_cap) -> no 429, no deprioritization (see isCapacityFree / CORR2-1).
 *   The launch token still serializes the /usage poll (no thundering-herd of
 *   polls all seeing stale capacity). The watchdog (reapStale, 120s token
 *   cap) reclaims a crashed/aborted holder; the AbortSignal plumbed through
 *   waitForLaunch/acquireSlot cancels an aborted turn's waiter entry so it
 *   doesn't block siblings for staleWaiterMs (5 min).
 *
 * - Watchdog: a crashed process would stall the queue at the token. The token
 *   entry carries a PID + birth timestamp; the next acquirer reclaims it if the
 *   PID is dead or the token is held beyond STALE_TOKEN_MS. Waiter entries are
 *   also PID+timestamp stamped so a dead waiter is skipped on promote.
 *
 * - Unlimited plans (limits.concurrency.limit === undefined, e.g. Code Max):
 *   the queue still serializes launches through the token + /usage poll so that
 *   priority.low backoff is honored, but the capacity check is skipped.
 *
 * Node has no flock builtin and macOS ships no `flock(1)`, so the critical
 * section (read-modify-write of the JSON) is guarded by an O_EXCL lockfile
 * with bounded spin-retry. The state file itself is written via atomic rename.
 */
import { mkdirSync, openSync, closeSync, unlinkSync, readFileSync, writeFileSync, renameSync, statSync, lstatSync, readdirSync, rmdirSync } from "node:fs";
import { dirname, basename } from "node:path";
import { homedir } from "node:os";

interface WaiterEntry {
  id: string;
  pid: number;
  ts: number;
}

interface TokenState {
  id: string;
  pid: number;
  ts: number;
}

export interface QueueState {
  /** FIFO of waiters; index 0 is the head (next to launch). */
  waiters: WaiterEntry[];
  /** The launch token: held by the process currently sending or polling /usage. null when free. */
  token: TokenState | null;
  /**
   * Shared deprioritization deadline (epoch-ms). Written by any process that
   * observes priority.low or a 429; read by every process before launching.
   * 0 = no active deprioritization. Account-wide source of truth is /usage, but
   * we cache it here so a 429 observed by process A pauses process B even before
   * B's next /usage poll lands.
   */
  pausedUntil: number;
  pausedReason: string | null;
  /**
   * Epoch-ms when the current pause was set. Used by reapStale to clear a
   * pause older than MAX_PAUSE_MS (defense-in-depth, in case the clamp in
   * pauseUntil is bypassed by a compromised sibling or a hand-edited file).
   * 0 when no pause has ever been set.
   */
  pausedTs: number;
}

export interface QueueConfig {
  /** Path to the state file. Defaults to ~/.pi/agent/umans-concurrency.json. */
  stateFile?: string;
  /** Max age of the launch token before it's considered stale (reapable). */
  staleTokenMs?: number;
  /** Max age of a waiter entry before it's considered stale (skippable). */
  staleWaiterMs?: number;
  /** Spin retry interval for the O_EXCL lock (ms). */
  lockRetryMs?: number;
  /** Max time to spin for the O_EXCL lock before failing (ms). */
  lockTimeoutMs?: number;
  /** Now() injection point for deterministic tests. */
  now?: () => number;
  /** PID injection point for deterministic tests. */
  pid?: () => number;
}

const DEFAULT_STATE_FILE = `${homedir()}/.pi/agent/umans-concurrency.json`;
// CORR2-3 / CMP-MED-4: 120s comfortably exceeds long streaming turns (xhigh/max
// thinking, long outputs, slow TTFT). The watchdog is a LAST RESORT (dead PID
// or truly hung process), not a tight bound on legitimate turns — the hard_cap
// burst headroom (CORR2-1) absorbs any transient over-limit from a reaped
// token. 30s was too tight and reaped tokens held by legitimately long streaming
// turns, racing a sibling launch the same way as the message_end release race.
const DEFAULT_STALE_TOKEN_MS = 120_000;
const DEFAULT_STALE_WAITER_MS = 5 * 60_000;
const DEFAULT_LOCK_RETRY_MS = 5;
const DEFAULT_LOCK_TIMEOUT_MS = 2_000;

function defaultNow(): number { return Date.now(); }
function defaultPid(): number { return process.pid; }

/** Conservative default backoff when /v1/usage reports priority.low with a null boxed_until. */
export const PRIORITY_BACKOFF_MS = 30_000;

/**
 * Maximum allowed pause duration (5h), matching the Umans 5h-account-pause
 * ceiling cited in design.md. pauseUntil() clamps any requested `until` to
 * now + MAX_PAUSE_MS, and reapStale() clears a pause whose pausedTs is older
 * than this — so a poisoned Retry-After header (e.g. 1e10) or a hand-edited
 * file cannot permanently wedge every local pi process sharing the file.
 */
export const MAX_PAUSE_MS = 5 * 60 * 60 * 1000; // 18,000,000 ms

/**
 * Tighter ceiling for a 429-sourced pause (5 × PRIORITY_BACKOFF_MS = 2.5 min).
 * ADV4-2: a server returning 429 forever (e.g. a misconfigured UMANS_BASE_URL)
 * writes a fresh pausedUntil on every turn, each extending the shared pause up
 * to the 5h MAX_PAUSE_MS ceiling — wedging all local pi processes for the real
 * Umans account-pause duration even though the 429 source is non-account-wide.
 * A 429 without a server-pushed boxed_until is capped tighter (2.5 min) so a
 * misconfigured base URL cannot wedge the account for hours; the server's
 * priority.low pause (a real account-wide deprio) still uses the 5h ceiling.
 * The 2.5 min cap is still >> the 30s PRIORITY_BACKOFF_MS floor, so a real 429
 * with a short Retry-After is honored, and repeated 429s extend up to 2.5 min
 * before the gate re-tries (better to re-try into a possibly-recovered account
 * than wedge for 5h on a misconfigured URL).
 */
export const MAX_PAUSE_429_MS = 5 * PRIORITY_BACKOFF_MS; // 150,000 ms (2.5 min)

/**
 * Reason tag written by the 429 handler (index.ts after_provider_response).
 * CORR4-1: /usage LAGS a 429 (the design acknowledges this at the capacityFree
 * doc-comment), so a stale 5s refreshUsage tick reporting priority.low===false
 * would wipe a sibling's freshly-written 429 pause within 1-5s — letting the
 * next waiter launch into the deprio the gate exists to prevent. clearPause
 * refuses to clear a pause tagged with this reason until it naturally elapses
 * OR /usage reports priority.low===true (confirming the server caught up).
 * Exported so the provider tags the 429 pause with the exact same string.
 */
export const PAUSE_REASON_429 = "HTTP 429 from gateway";

/**
 * Clamp a candidate pause deadline to now + MAX_PAUSE_MS (or a tighter ceiling
 * when passed — e.g. MAX_PAUSE_429_MS for a 429-sourced pause) so a poisoned or
 * over-large Retry-After/boxed_until cannot wedge the queue for centuries.
 * Exported so the provider (index.ts) can clamp its Retry-After parse to the
 * same ceiling.
 */
export function clampPauseUntil(until: number, now: number = Date.now(), ceilingMs: number = MAX_PAUSE_MS): number {
  const ceiling = now + ceilingMs;
  return until > ceiling ? ceiling : until;
}

/**
 * Cap + sanitize a pause reason before it is stored or rendered. SEC5-1 /
 * ADV5-5: a compromised or misconfigured gateway can push a crafted
 * `priority.reason` that flows unescaped into the status bar (PAUSED <Ns>
 * (<reason>)). Cap to ~64 chars and strip non-printable / control / ANSI-escape
 * characters so a crafted string cannot mangle the bar or inject control
 * sequences. The reason is operator-facing metadata only (the source is already
 * distinguishable via PAUSE_REASON_429); a 64-char printable-only cap removes
 * the injection surface without losing signal.
 */
const PAUSE_REASON_MAX_CHARS = 64;
// SEC9-1/SEC8-3: strip control chars (incl. DEL), ESC (0x1b, ANSI escape
// introducer), + Unicode bidi/RTL override chars (U+202A-E, U+2066-9, U+061C)
// and zero-width / BOM chars (U+200B-F, U+FEFF) that could spoof the displayed
// pause reason in the status bar. Keeps printable ASCII + common printable
// Unicode. Mirrored in sanitizeErrorBody (index.ts).
const SANITIZE_CTRL_RE = /[\x00-\x1f\x7f\u061c\u200b-\u200f\u202a-\u202e\u2066-\u2069\ufeff]/g;
export function sanitizeReason(reason: string | null | undefined): string | null {
  if (typeof reason !== "string") return null;
  const cleaned = reason.replace(SANITIZE_CTRL_RE, "").trim();
  if (!cleaned) return null;
  return cleaned.length > PAUSE_REASON_MAX_CHARS
    ? cleaned.slice(0, PAUSE_REASON_MAX_CHARS)
    : cleaned;
}

/** Normalized priority state derived from /v1/usage `usage.priority` (or a 429). */
export interface PriorityState {
  low: boolean;
  until: number; // epoch-ms; 0 when not low
  reason: string | null;
}

/**
 * Parse the `usage.priority` object from /v1/usage into a deadline. `boxed_until`
 * may be an ISO string, epoch seconds, or null; when low===true but boxed_until is
 * null/absent, we fall back to now + PRIORITY_BACKOFF_MS so callers always get a
 * concrete deadline to honor.
 * SEC7-2: clamp `until` to now + MAX_PAUSE_MS so the parse boundary matches the
 * 429 path's defense-in-depth (a poisoned boxed_until like 2099-12-31 cannot
 * propagate a centuries-long deadline even if a future caller bypasses
 * pauseUntil's own clamp). Low priority — the write boundary already clamps +
 * is only in-repo consumer, but this closes the parse-boundary gap.
 */
export function parsePriority(raw: unknown): PriorityState {
  const p = (raw ?? {}) as {
    low?: boolean | null;
    boxed_until?: string | number | null;
    reason?: string | null;
  };
  const low = p.low === true;
  let until = 0;
  if (low) {
    const b = p.boxed_until;
    let ms = 0;
    if (typeof b === "number" && b > 0) ms = b * 1000;
    else if (typeof b === "string" && b) {
      const t = Date.parse(b);
      if (!Number.isNaN(t)) ms = t;
    }
    until = ms > 0 ? ms : Date.now() + PRIORITY_BACKOFF_MS;
    // SEC7-2: clamp at the parse boundary too (defense-in-depth).
    until = clampPauseUntil(until, Date.now(), MAX_PAUSE_MS);
  }
  return { low, until, reason: sanitizeReason(p.reason) };
}

/**
 * A lightweight /v1/usage snapshot used for the capacity decision. Mirrors the
 * shape fetchUsageSnapshot returns (concurrentSessions + limit + hardCap +
 * priority).
 */
interface CapacitySnapshot {
  concurrentSessions: number | undefined;
  limit: number | undefined;
  /**
   * Account-wide hard burst cap (the threshold at which Umans actually returns
   * 429s / deprioritizes). When present, the gate compares against this instead
   * of `limit` so the documented burst headroom (burst_pct, e.g. limit=4 /
   * hard_cap=8 on Code Max) absorbs the message_end release race (CORR2-1) and
   * server-side concurrent_sessions accounting noise (ADV2-F3). Falls back to
   * `limit` when absent (e.g. unlimited plans, older API responses).
   */
  hardCap: number | undefined;
  priority: PriorityState;
}

/**
 * Inputs to the capacity decision: the effective concurrency cap (env override
 * or the live /usage value) and whether the shared pausedUntil is active.
 */
interface CapacityInputs {
  limit: number | undefined;
  queuePaused: boolean;
}

/**
 * Pure decision: may this process launch given a /usage snapshot + inputs?
 * Returns { free, repause? } where `repause` is set when priority.low is
 * observed and the caller should push the pause to the shared file.
 *
 * - If the shared pause is active → not free (C2: a 429 observed by any local
 *   process backs off all siblings before /usage propagates priority.low).
 * - If priority.low → not free + repause so siblings back off too (honored
 *   BEFORE the unlimited short-circuit so Code Max still pushes the pause).
 * - If /usage is unreachable (snap === null) → free (trust headroom rather
 *   than block forever; the queue still serializes launches via the token).
 * - If concurrent_sessions >= hardCap (or limit when hardCap absent, or
 *   inputs.limit when both snap caps are absent) → not free. CORR8-1 (HIGH):
 *   an unlimited plan (inputs.limit === undefined) is NO LONGER a short-
 *   circuit — when snap.hardCap is present the gate compares against it so a
 *   Code Max account can't trip the account-wide burst cap; only when ALL
 *   caps are undefined (true unlimited with no burst cap reported) is the
 *   gate free (no cap to exceed).
 * - Otherwise → free.
 *
 * CORR2-1: the gate's PURPOSE is to prevent 429s, which hit at `hard_cap`, not
 * `limit`. message_end releases at client-side stream completion, which
 * PRECEDES the server's concurrent_sessions decrement by a network RTT +
 * cleanup lag, so the next waiter's /usage poll can transiently see stale
 * (too-low) capacity and launch 1-2 over `limit`. That overshoot stays within
 * the documented burst headroom (hard_cap) → no 429, no deprioritization. The
 * launch token still serializes the /usage poll (no thundering-herd), and the
 * message_end release frees the slot for tool-execution parallelism. Gating to
 * hard_cap also absorbs server-side concurrent_sessions accounting noise
 * (ADV2-F3: the counter oscillates ±1 during a single serialized turn).
 */
export function isCapacityFree(
  snap: CapacitySnapshot | null,
  inputs: CapacityInputs,
): { free: boolean; repause?: { until: number; reason: string | null } } {
  if (inputs.queuePaused) return { free: false };
  // priority.low BEFORE the unlimited short-circuit so Code Max (limit absent)
  // still evaluates repause and pushes the shared pause to siblings (CORR2-2 /
  // COV2-unlimited+priority.low).
  if (snap?.priority.low) {
    return { free: false, repause: { until: snap.priority.until, reason: snap.priority.reason } };
  }
  // CORR8-1 (HIGH): the unlimited-plan short-circuit (inputs.limit ===
  // undefined) used to return { free: true } BEFORE evaluating
  // concurrent_sessions against hard_cap — contradicting D5 ("When hard_cap is
  // present, the gate compares against it; when absent, it falls back to
  // limit"). hard_cap is the actual 429 threshold, and an unlimited plan
  // (Code Max, limit undefined) still trips the account-wide burst cap. We
  // dropped the short-circuit; the cap check below handles all cases: when
  // hard_cap (or limit) is present, gate against it; when ALL of
  // snap.hardCap / snap.limit / inputs.limit are undefined (a true unlimited
  // plan with no burst cap reported), `cap` is undefined and we return free
  // (no cap to exceed — correct). The `!snap` (usage-unreachable) check stays
  // before the cap check so we trust headroom rather than block forever.
  if (!snap) return { free: true }; // /usage unreachable → trust headroom
  const cur = snap.concurrentSessions ?? 0;
  // CORR2-1: prefer hard_cap (the 429 threshold) over limit; fall back to
  // limit when the API did not report a hard_cap.
  const cap = snap.hardCap ?? snap.limit ?? inputs.limit;
  if (cap !== undefined && cur >= cap) return { free: false };
  return { free: true };
}

/**
 * Parse a UMANS_CONCURRENCY_LIMIT env value into a positive integer, falling
 * back to the live /v1/usage value when unset, empty, non-positive, or
 * fractional. Handles edge inputs: "2.5" (fractional → fallback, almost
 * certainly a typo — a float threshold on an integer counter silently floors
 * to 2 usable slots), " " (whitespace → 0 → fallback), "0" (non-positive →
 * fallback), "abc" (NaN → fallback), "" (empty → fallback). CLN4-3:
 * tightened from Number.isFinite to Number.isInteger so a fractional typo
 * falls back to the server value (a strict improvement for a slot-count knob).
 * CORR8-3 / ADV8-1: tightened further to a strict /^\d+$/ regex test BEFORE
 * Number(trimmed) so hex ("0x10" === 16) and scientific notation ("1e3" ===
 * 1000) — which Number() accepts + Number.isInteger() happily passes — are
 * rejected to fallback. A "0x10" typo would silently set the gate to 16,
 * defeating the cap. Mirrors the 429 Retry-After parse (which already used
 * /^\d+$/). The existing `n > 0` check still rejects 0.
 */
export function parseConcurrencyLimit(envValue: string | undefined, fallback: number | undefined): number | undefined {
  const trimmed = envValue?.trim();
  if (!trimmed || !/^\d+$/.test(trimmed)) return fallback;
  const n = Number(trimmed);
  return Number.isInteger(n) && n > 0 ? n : fallback;
}

/** Generate a unique waiter/token id. */
function newId(): string {
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

/**
 * Shape guard for a WaiterEntry. SEC5-2: a poisoned/hand-edited state file can
 * put arbitrary objects into `waiters` (e.g. { pid: "not-a-number" }). Without
 * validation, `isPidDead(w.pid)` would call `process.kill("not-a-number", 0)`
 * which throws a synchronous TypeError (not an errno-coded error) that the
 * catch in isPidDead does not filter — crashing the reader's mutate. Mirror
 * the defensive parsing already applied to the scalar fields: drop malformed
 * entries so reapStale/isPidDead operate on well-typed input.
 */
function isWaiterEntry(w: unknown): w is WaiterEntry {
  return typeof w === "object" && w !== null &&
    typeof (w as WaiterEntry).id === "string" &&
    typeof (w as WaiterEntry).pid === "number" &&
    typeof (w as WaiterEntry).ts === "number";
}

/** Shape guard for a TokenState (same rationale as isWaiterEntry). */
function isTokenState(t: unknown): t is TokenState {
  return typeof t === "object" && t !== null &&
    typeof (t as TokenState).id === "string" &&
    typeof (t as TokenState).pid === "number" &&
    typeof (t as TokenState).ts === "number";
}

/** Read the queue state, or return a fresh empty state if the file is absent/corrupt. */
export function readState(path: string): QueueState {
  try {
    const raw = readFileSync(path, "utf8");
    const parsed = JSON.parse(raw) as Partial<QueueState>;
    const waiters = Array.isArray(parsed.waiters) ? parsed.waiters.filter(isWaiterEntry) : [];
    const token = isTokenState(parsed.token) ? parsed.token : null;
    return {
      waiters,
      token,
      pausedUntil: typeof parsed.pausedUntil === "number" ? parsed.pausedUntil : 0,
      // COV6-1 / SEC6-1: sanitize pausedReason on the READ boundary too. SEC5-1
      // sanitizes at the write boundary (pauseUntil) + parse path (parsePriority),
      // but readState passed parsed.pausedReason straight through → snapshot() →
      // status bar/notify render raw. A hand-edited file, a compromised sibling
      // writing JSON directly, or a file poisoned by an earlier unfixed build
      // surfaces the raw string. The write-boundary sanitize is the primary
      // guard; this closes the hand-edited-file gap (defense-in-depth).
      pausedReason: sanitizeReason(parsed.pausedReason ?? null),
      pausedTs: typeof parsed.pausedTs === "number" ? parsed.pausedTs : 0,
    };
  } catch {
    return { waiters: [], token: null, pausedUntil: 0, pausedReason: null, pausedTs: 0 };
  }
}

/**
 * True if a PID is not currently alive. Never throws.
 *
 * CLN5-3: this is one of the module's pure-helper exports
 * (readState / reapStale / isPidDead / MAX_PAUSE_MS) — pure, side-effect-free
 * functions exposed so external consumers (and selfcheck) can build on the
 * queue's primitives. They are not used by index.ts (the provider goes through
 * the ConcurrencyQueue handle), but are a defensible small public API for a
 * standalone queue module.
 */
export function isPidDead(pid: number): boolean {
  // SEC7-3: defensive guard for non-numeric / non-finite input. The shape
  // guards (isWaiterEntry/isTokenState) already drop malformed entries, but a
  // future caller could bypass them; treat non-number / NaN / Infinity as
  // dead so reapStale reclaims rather than passing garbage to process.kill
  // (which would throw a synchronous TypeError not filtered by the catch).
  if (typeof pid !== "number" || !Number.isFinite(pid) || !pid || pid <= 0) return true;
  try {
    // process.kill(pid, 0) throws ESRCH (no such process) or EPERM (process
    // exists but caller lacks permission). ESRCH -> dead. EPERM -> the process
    // IS alive (just not ours); treat it as alive so we don't falsely reap a
    // live holder's token (CORR2-4 / CMP-LOW-1). In a single-user pi setup all
    // processes share the same UID, so EPERM is vanishingly rare; treating it
    // as alive is the fail-safe (the queue stalls briefly rather than yanking a
    // live holder's token).
    process.kill(pid, 0);
    return false; // exists and we have permission (or it's our own)
  } catch (e) {
    const code = (e as NodeJS.ErrnoException).code;
    if (code === "EPERM") return false; // exists, just no permission -> treat as alive
    return true; // ESRCH (no such process) or anything else -> dead
  }
}

/**
 * Reap a stale launch token and stale waiters from a state snapshot.
 * Also reaps a stale pause: if pausedTs is older than MAX_PAUSE_MS, OR the
 * pause DURATION (pausedUntil - now) itself exceeds MAX_PAUSE_MS, the pause is
 * cleared (defense-in-depth, in case the clamp in pauseUntil is bypassed by a
 * compromised sibling or a hand-edited file — SEC2-MED-1). Returns the cleaned
 * state; does not write to disk.
 *
 * CMP-MED-3: PID reuse is a known blind spot of the kill(pid, 0) probe — if a
 * holding pi process crashes, its PID can be recycled by an unrelated process
 * within the staleTokenMs window, and isPidDead would return false (alive).
 * The TIMESTAMP staleness check (now - token.ts > staleTokenMs) is the real
 * safety net that bounds this: a reused PID still ages out, so the worst case
 * is a bounded stall, not a permanent wedge. isPidDead is a fast-path
 * optimization; the timestamp is the authoritative bound.
 */
export function reapStale(state: QueueState, cfg: Required<QueueConfig>, now: number): QueueState {
  const staleToken = state.token && (
    isPidDead(state.token.pid) || (now - state.token.ts) > cfg.staleTokenMs
  );
  const token = staleToken ? null : state.token;
  const waiters = state.waiters.filter((w) =>
    !isPidDead(w.pid) && (now - w.ts) <= cfg.staleWaiterMs
  );
  // Reap a pause that violates the MAX_PAUSE_MS ceiling. Two conditions
  // (SEC2-MED-1): (1) pausedTs is older than the ceiling (the original
  // defense — a clamp-bypassed poisoned value ages out); (2) the pause
  // DURATION (pausedUntil - now) itself exceeds the ceiling from the current
  // vantage, regardless of pausedTs — this catches a forward-dated pausedTs
  // (a hand-edited file setting pausedTs to the future makes `now - pausedTs`
  // negative, bypassing condition 1) paired with an oversized pausedUntil.
  let { pausedUntil, pausedReason, pausedTs } = state;
  if (pausedUntil > 0) {
    const ageTooOld = pausedTs > 0 && (now - pausedTs) > MAX_PAUSE_MS;
    const durationTooLong = (pausedUntil - now) > MAX_PAUSE_MS;
    if (ageTooOld || durationTooLong) {
      pausedUntil = 0;
      pausedReason = null;
      pausedTs = 0;
    }
  }
  return { ...state, token, waiters, pausedUntil, pausedReason, pausedTs };
}

/**
 * C2/CMP6-1: feature-detect Atomics.wait for a non-CPU-burning sync sleep.
 * Exported (alongside decideLaunch / shouldReleaseOnMessageEnd) so the code
 * path is unit-testable. Returns true when Atomics.wait + SharedArrayBuffer are
 * available (the acquireLock spin uses it); false when the runtime lacks them
 * (older Node, or SAB disabled via --no-harmony-sharedarraybuffer) and the
 * caller must fall back to a busy-spin.
 */
export function canAtomicsWait(): boolean {
  return typeof Atomics !== "undefined" && typeof Atomics.wait === "function" &&
    typeof SharedArrayBuffer !== "undefined";
}

/**
 * C2/CMP6-1: synchronous sleep that does NOT spin the CPU. Under lock
 * contention the old busy-spin (while (cfg.now() < target) {}) burned CPU and
 * starved the lock holder's event loop on a single-core VM, racing the 2s
 * lock timeout. Atomics.wait blocks the calling thread (it yields the core)
 * without spinning — the holder can make progress. It blocks the event loop
 * (a sync sleep), which is acceptable here because mutate is already
 * synchronous + blocking. Falls back to the busy-spin when Atomics.wait /
 * SharedArrayBuffer are unavailable.
 */
function syncSleep(ms: number, cfg: Required<QueueConfig>): void {
  if (canAtomicsWait() && ms > 0) {
    // Atomics.wait returns 'timed-out' on timeout, 'ok' if notified, 'not-equal'
    // if the initial value mismatches. We pass 0 as the expected value (an
    // Int32Array starts zeroed) and never notify, so it always times out after
    // `ms`. The buffer is throwaway — allocated once per call; the cost is
    // negligible vs the lock retry cadence (5ms).
    const buf = new Int32Array(new SharedArrayBuffer(4));
    Atomics.wait(buf, 0, 0, ms);
    return;
  }
  // Fallback: busy-spin (the old behavior). Only used when Atomics is
  // unavailable; bounded by cfg.lockRetryMs (5ms default).
  const target = cfg.now() + ms;
  while (cfg.now() < target) { /* busy spin */ }
}

/**
 * Open an exclusive lockfile (O_EXCL), spinning until acquired or timeout.
 * Returns a release function. Used to guard the read-modify-write critical
 * section on the state file.
 *
 * Stale-lockfile recovery: if the lockfile exists but is older than the lock
 * timeout, a crashed holder left it behind — unlink it and retry. This keeps
 * a crashed process from permanently wedging the queue. (The critical section
 * is milliseconds, so an old lockfile is definitively stale.)
 *
 * CMP-MED-2: the 2s lockTimeoutMs is a hard CORRECTNESS ceiling on the
 * critical section, not just a liveness bound — no slow operation may be added
 * inside `mutate` (readState / reapStale / fn / writeStateAtomic). A
 * legitimately slow writer (disk pressure under a burst of `pi -p` jobs) that
 * holds the lock >2s will have its lockfile yanked mid-write, potentially
 * racing two writers (lost write). `proper-lockfile` decouples this with an
 * mtime-refresh while held; this implementation intentionally does not (the
 * critical section is sub-2s on local SSD). NFS caveat: `open(O_EXCL)` is
 * broken on NFS file systems (proper-lockfile switched to `mkdir` for this).
 * This is fine for the local home-directory path (~/.pi/agent on APFS) but
 * would break on an NFS-mounted home — do not deploy this onto NFS without
 * switching to `mkdir`-based locking.
 */
function acquireLock(lockFile: string, cfg: Required<QueueConfig>): () => void {
  const deadline = cfg.now() + cfg.lockTimeoutMs;
  let fd: number | undefined;
  while (fd === undefined) {
    try {
      fd = openSync(lockFile, "wx", 0o600);
      // CMP7-1: write the holder PID into the lockfile so the stale-lock
      // decision can distinguish "holder is dead" from "lockfile is merely
      // old". A slow-but-legitimate mutate (disk pressure) holding >2s used to
      // have its lockfile yanked mid-write; now a live holder PID keeps the
      // lock until the mtime ceiling (the 2s bound still applies as the
      // fallback). readFileSync is guarded by the SEC7-1 lstatSync fix above
      // (a symlink lockfile is reclaimed without being followed). Malformed/
      // empty content falls back to the mtime check.
      // CMP8-1 / SEC8-5: the nonce field was DROPPED — it was write-only (the
      // reclaim path below reads only parsed.pid, never the nonce), so it
      // provided no fencing. A nonce that is never read back is not a fencing
      // token. The PID-reuse bound is the documented 120s/2s timestamp
      // (staleTokenMs / lockTimeoutMs): a recycled PID still ages out, so the
      // worst case is a bounded stall, not a permanent wedge. Reading the
      // nonce back + comparing would add complexity for a single-user dev
      // tool; the drop is cleaner.
      writeFileSync(fd, JSON.stringify({ pid: cfg.pid() }), { encoding: "utf8" });
    } catch (e: any) {
      if (e.code !== "EEXIST") throw e;
      // Lock is held by another process — or stale from a crash. Reclaim if the
      // holder PID is dead OR the lockfile is older than the lock timeout.
      // SEC7-1: use lstatSync (not statSync) so the mtime check reads the
      // lockfile entry itself, NOT a symlink target. An attacker who can write
      // to ~/.pi/agent plants a symlink at ${stateFile}.lock -> any old file;
      // statSync follows it, reads the TARGET's old mtime, concludes stale,
      // and unlinkSync removes the SYMLINK — then O_EXCL succeeds, racing a
      // sibling mid-mutate (lost write). lstatSync never follows the link;
      // a symlink (or any non-regular file) is treated as stale + unlinked
      // without ever being followed.
      try {
        const st = lstatSync(lockFile);
        if (!st.isFile()) {
          unlinkSync(lockFile);
          continue; // retry the O_EXCL immediately
        }
        // CMP7-1: fast-path reclaim if the holder PID is dead. Read the
        // lockfile content (lstat already confirmed it's a regular file, so
        // readFileSync does not follow a symlink here). Malformed/empty content
        // falls back to the mtime check below.
        let holderPid: number | undefined;
        try {
          const raw = readFileSync(lockFile, "utf8");
          const parsed = JSON.parse(raw) as { pid?: unknown };
          if (typeof parsed.pid === "number" && Number.isFinite(parsed.pid)) {
            holderPid = parsed.pid;
          }
        } catch { /* malformed/empty — fall back to mtime */ }
        const holderDead = holderPid !== undefined && isPidDead(holderPid);
        if (holderDead || cfg.now() - st.mtimeMs > cfg.lockTimeoutMs) {
          unlinkSync(lockFile);
          continue; // retry the O_EXCL immediately
        }
      } catch {
        // stat failed (race: holder just released) — fall through to spin
      }
      // Bail if the deadline passed.
      if (cfg.now() >= deadline) {
        throw new Error(`concurrency-queue: timed out acquiring lock ${lockFile}`);
      }
      // Spin briefly. Use a synchronous sleep to avoid timers in the hot path.
      // C2/CMP6-1: prefer Atomics.wait over a CPU-burning busy-spin. Under
      // contention each process busy-spins 5ms per retry, burning CPU AND
      // blocking the event loop of the process that currently holds the lock —
      // the holder cannot make progress on writeStateAtomic/renameSync while a
      // spinner hogs the scheduler on a single-core VM, and the 2s lock
      // timeout can then fire on the holder's next mutate. Atomics.wait blocks
      // the calling thread WITHOUT spinning the CPU (it yields the core), so
      // the holder can run. It blocks the event loop (sync sleep), but that is
      // acceptable here — mutate is already synchronous + blocking. Guard with
      // a feature check (Atomics + SharedArrayBuffer available) and fall back
      // to the busy-spin if unavailable (older runtimes / disabled SAB).
      syncSleep(cfg.lockRetryMs, cfg);
    }
  }
  return () => {
    try { closeSync(fd); } catch { /* ignore */ }
    try { unlinkSync(lockFile); } catch { /* ignore: may have been removed */ }
  };
}

function withLock<T>(cfg: Required<QueueConfig>, lockFile: string, fn: (now: number) => T): T {
  const release = acquireLock(lockFile, cfg);
  try {
    return fn(cfg.now());
  } finally {
    release();
  }
}

function writeStateAtomic(path: string, state: QueueState): void {
  const dir = dirname(path);
  try { mkdirSync(dir, { recursive: true }); } catch { /* ignore EEXIST */ }
  // C4: stale-.tmp reaping moved out to mutate's withLock block so it uses the
  // injected cfg.now() (testability — a frozen clock now exercises the reaper).
  // ADV7-1: include a short random suffix in the temp name so a recycled pid
  // never collides with a stale leftover from a prior (crashed) run sharing
  // the same pid. The reaper's prefix match (`${basename(path)}.` + suffix
  // `.tmp`) still matches; the random middle distinguishes concurrent runs.
  const tmp = `${path}.${process.pid}.${Math.random().toString(36).slice(2, 8)}.tmp`;
  // SEC6-2: open with O_EXCL ("wx") + 0o600 so a planted symlink at the per-pid
  // temp name throws EEXIST instead of being followed. writeFileSync(symlink,
  // ...) follows the symlink and writes into its target (probe-confirmed);
  // openSync("wx") creates the file ONLY if it does not already exist (no
  // follow on creation), so a planted symlink/name is rejected. On EEXIST,
  // treat as a write conflict: the per-pid temp name is already in use (a
  // concurrent writer sharing our pid — impossible under the O_EXCL lockfile —
  // or a planted symlink/name). Throw so the caller's mutate surfaces it; the
  // lockfile + reapStaleTmps keep the temp namespace clean. Close the fd after
  // writing (writeFileSync(fd, ...) does not close it).
  const fd = openSync(tmp, "wx", 0o600);
  try {
    writeFileSync(fd, JSON.stringify(state), { encoding: "utf8" });
  } finally {
    try { closeSync(fd); } catch { /* best-effort */ }
  }
  renameSync(tmp, path); // atomic on POSIX & Windows; rename preserves the temp's 0600 mode
}

/**
 * Max age of a .tmp file before it's considered a crashed writer's leftover.
 * Matches DEFAULT_LOCK_TIMEOUT_MS (2s): no legitimate writer holds the lock
 * longer than the lock timeout (the lockfile is yanked at 2s), so a .tmp older
 * than 2s is by construction from a crashed/reaped writer. The reaper is
 * best-effort cleanup — the actual safety boundary is the atomic renameSync
 * (a crashed writer's .tmp never reaches `path`, and rename is atomic on
 * POSIX & Windows). 5x the lock timeout (10s) gives clock-resolution slack.
 */
const STALE_TMP_MS = 10_000;

/** Best-effort unlink of stale <path>.*.tmp files older than STALE_TMP_MS. */
// C4: now is threaded in from mutate's caller (cfg.now()) so a frozen clock
// exercises the reaper — a regression inverting the comparison would not be
// caught otherwise.
// ADV7-2: cap the number of .tmp files unlinked per mutate (REAP_TMP_MAX) to
// bound the critical section under pathological .tmp accumulation. The reaper
// runs inside the O_EXCL lock, so unlinking thousands of stale leftovers would
// extend the critical section past the 2s lockTimeoutMs ceiling (CMP-MED-2),
// racing two writers. Leave the rest for the next mutate.
const REAP_TMP_MAX = 100;
function reapStaleTmps(path: string, now: number): void {
  const dir = dirname(path);
  const prefix = `${basename(path)}.`;
  let entries: string[];
  try { entries = readdirSync(dir); } catch { return; /* dir missing/unreadable */ }
  let unlinked = 0;
  for (const name of entries) {
    if (unlinked >= REAP_TMP_MAX) break; // ADV7-2: bound the critical section
    if (!name.startsWith(prefix) || !name.endsWith(".tmp")) continue;
    const full = `${dir}/${name}`;
    try {
      const st = statSync(full);
      // CORR7-5: a planted .tmp DIRECTORY would make unlinkSync throw EISDIR
      // (swallowed) AND writeStateAtomic's openSync("wx") throw EEXIST (the
      // real wedge — the per-pid temp name is a directory). rmdir it if empty
      // (best-effort) + skip; a non-empty dir is left for the operator.
      if (st.isDirectory()) {
        try { rmdirSync(full); } catch { /* non-empty or gone — skip */ }
        unlinked++;
        continue;
      }
      if (now - st.mtimeMs > STALE_TMP_MS) {
        unlinkSync(full);
        unlinked++;
      }
    } catch { /* race: gone or unreadable — ignore */ }
  }
}

/**
 * A handle on the cross-process queue. Methods are safe to call concurrently
 * from one process; the O_EXCL lock serializes across processes.
 */
export interface ConcurrencyQueue {
  /**
   * Join the queue as a waiter. Returns our waiter id (or null if disabled).
   * Does not block. Use waitForLaunch() afterwards.
   */
  join(): string | null;
  /**
   * Block until this process is at the head of the queue AND has claimed the
   * launch token. Resolves with a release function that must be called when
   * the request completes (assistant message_end is the primary release path;
   * turn_end and agent_end are safety nets for turns that error before
   * message_end fires). Resolves with a no-op release function if the queue is
   * disabled (CLN7-3; unreachable from the provider path, which bails on
   * join() === null before reaching waitForLaunch).
   *
   * Capacity check is NOT performed here — the caller polls /usage itself
   * after claiming the token, so the decision uses the freshest server data.
   *
   * If `signal` aborts mid-wait, the poll loop stops, our waiter entry is
   * cancelled via `cancel(ourId)`, and the promise rejects with an
   * AbortError — so an aborted turn cannot wedge the local queue for
   * `staleWaiterMs` (5 min) or leak the token if it is later freed
   * (C4/ADV-2).
   */
  waitForLaunch(ourId: string, signal?: AbortSignal): Promise<() => void>;
  /**
   * Mark the account as deprioritized until `until` (epoch-ms). Shared across
   * all processes via the state file; idempotent (extends the deadline).
   */
  pauseUntil(until: number, reason?: string | null): void;
  /**
   * Clear deprioritization early (e.g. when /usage reports priority.low===false).
   * CORR4-1: by default this REFUSES to clear a 429-origin pause (tagged
   * PAUSE_REASON_429) — /usage lags a 429 by 1-5s, so a stale tick reporting
   * priority.low===false must not wipe a sibling's freshly-written 429 pause.
   * The 429 pause survives until it naturally elapses OR /usage reports
   * priority.low===true (confirming the server caught up). Pass {force:true}
   * to clear unconditionally (used by the /umans-concurrency reset operator
   * command to un-wedge a poisoned pause without editing the JSON by hand).
   */
  clearPause(opts?: { force?: boolean }): void;
  /**
   * Re-stamp the launch token's `ts` to `now` while we still hold it, so the
   * 120s watchdog (reapStale) does not reap a long capacity poll (a pause-
   * bounded wait can legitimately exceed 120s — see CORR4-3). Returns `true` if
   * `state.token?.id === ourId` (we still hold it) and the stamp was advanced;
   * returns `false` if the token was reaped by a sibling's reapStale (id
   * mismatch) or is absent. C1 (HIGH): a poller that holds the token across a
   * long /usage poll MUST call this on every iteration; if it returns false,
   * the poller must re-join the queue and wait its turn again (the sibling
   * that reaped + claimed is now sending — re-joining serializes us behind
   * it rather than racing a concurrent send that defeats the gate).
   */
  touchToken(ourId: string): boolean;
  /** Snapshot for status-bar display. */
  snapshot(): { queued: number; tokenHeld: boolean; paused: boolean; pausedUntil: number; pausedReason: string | null };
  /** Remove our waiter entry if still present (best-effort, used on abort). */
  cancel(ourId: string): void;
  /** Best-effort shutdown cleanup: clear this process's own waiter/token entry. Does NOT unlink the shared state file (siblings may still be queued). */
  reset(): void;
}

export function createConcurrencyQueue(opts?: QueueConfig & { disabled?: boolean }): ConcurrencyQueue {
  if (opts?.disabled) {
    return {
      join: () => null,
      waitForLaunch: () => Promise.resolve(() => {}),
      pauseUntil: () => {},
      clearPause: () => {},
      touchToken: () => true,
      snapshot: () => ({ queued: 0, tokenHeld: false, paused: false, pausedUntil: 0, pausedReason: null }),
      cancel: () => {},
      reset: () => {},
    };
  }

  const cfg: Required<QueueConfig> = {
    stateFile: opts?.stateFile ?? DEFAULT_STATE_FILE,
    staleTokenMs: opts?.staleTokenMs ?? DEFAULT_STALE_TOKEN_MS,
    staleWaiterMs: opts?.staleWaiterMs ?? DEFAULT_STALE_WAITER_MS,
    lockRetryMs: opts?.lockRetryMs ?? DEFAULT_LOCK_RETRY_MS,
    lockTimeoutMs: opts?.lockTimeoutMs ?? DEFAULT_LOCK_TIMEOUT_MS,
    now: opts?.now ?? defaultNow,
    pid: opts?.pid ?? defaultPid,
  };
  const lockFile = `${cfg.stateFile}.lock`;
  const ourPid = cfg.pid;

  // COV4-1: ensure the state file's parent dir exists before the first mutate.
  // The lockfile lives in the same dir as the state file, so acquireLock's
  // openSync(lockFile, "wx") throws ENOENT when the parent dir is missing —
  // BEFORE writeStateAtomic's own mkdirSync ever runs (it runs inside withLock,
  // i.e. after acquireLock already tried to open the lockfile). Bites a fresh
  // pi install or a test harness pointing stateFile at a nested path. Hoisting
  // mkdirSync here fixes both the lockfile and state-file paths. Best-effort:
  // EEXIST/EACCES are swallowed (the latter surfaces on the first mutate as a
  // more informative EACCES from openSync, matching the prior behavior).
  try { mkdirSync(dirname(cfg.stateFile), { recursive: true }); } catch { /* EEXIST or EACCES (surfaces on first mutate) */ }

  // Track whether THIS process currently holds the token, so we only release
  // our own and so the status bar can show "launching".
  let holdsToken = false;
  let ourTokenId: string | null = null;
  // COV5-5 / ADV5-4: track our waiter ids (set in join) alongside ourTokenId so
  // reset() can splice out a queued-but-not-launched waiter. Without this, a
  // process that join()ed but is still queued (ourTokenId === null) has reset()
  // as a no-op, leaking the waiter for staleWaiterMs (5 min) if the process
  // doesn't exit — blocking siblings behind a dead-PID entry.
  // COV6-2: a Set (not a single slot) so a second join() on one queue does NOT
  // overwrite the first id. Reachable from transformMessageImages (multi-image
  // handoff runs Promise.all → acquireSlot → join() per image). Probe (5×)
  // confirmed: two join() calls then reset() leaves exactly 1 waiter every
  // time with the single-slot design. The token single-slot (ourTokenId) is
  // safe — token release is closure-captured per waitForLaunch — but a Set is
  // used here because waiters accumulate.
  const ourWaiterIds: Set<string> = new Set();
  // CLN7-1: ourWaiterIds (Set) is the sole source of truth for this process's
  // waiter ids. The dead `let ourWaiterId` single-slot (leftover from the
  // COV6-2 Set refactor) was written in join() and cleared in release/cancel/
  // reset but NEVER read to drive any behavior — cancel takes ourId as a
  // parameter and consults the Set. Deleted. The ourTokenId single-slot is
  // safe (token release is closure-captured per waitForLaunch) — kept.
  // CORR7-2: a per-instance AbortController that reset() aborts to stop any
  // in-flight waitForLaunch poll loop on the same queue instance. Without it,
  // reset() splices our waiter id from the file, but a concurrent poll loop's
  // mutate sees stillQueued===false and RE-INSERTS the id at the tail (ADV-4's
  // re-insert path) every 50ms until the turn's AbortSignal aborts — leaking a
  // dead-PID waiter for staleWaiterMs (5 min) if the process exits before pi
  // aborts the signal. reset() aborts this controller so the poll stops + the
  // promise rejects (acquireSlot catches + returns undefined). A fresh
  // controller is created on each reset so subsequent waits are not pre-aborted.
  let resetAbort: AbortController = new AbortController();

  function mutate<T>(fn: (now: number, state: QueueState) => T): T {
    return withLock(cfg, lockFile, (now) => {
      const state = reapStale(readState(cfg.stateFile), cfg, now);
      const result = fn(now, state);
      // C4: reap stale .tmp files inside the lock, using the injected cfg.now()
      // (was Date.now() inside writeStateAtomic — a frozen clock never
      // exercised the reaper). ADV-1: best-effort cleanup of crashed writers'
      // leftovers; errors swallowed inside reapStaleTmps.
      reapStaleTmps(cfg.stateFile, now);
      writeStateAtomic(cfg.stateFile, state);
      return result;
    });
  }

  return {
    join(): string | null {
      const id = newId();
      mutate((now, state) => {
        state.waiters.push({ id, pid: ourPid(), ts: now });
      });
      ourWaiterIds.add(id);
      return id;
    },

    waitForLaunch(ourId: string, signal?: AbortSignal): Promise<() => void> {
      return new Promise((resolve, reject) => {
        // If the signal is already aborted, cancel + reject immediately.
        if (signal?.aborted || resetAbort.signal.aborted) {
          try { this.cancel(ourId); } catch { /* best-effort */ }
          reject(new Error("concurrency-queue: waitForLaunch aborted"));
          return;
        }
        let timer: ReturnType<typeof setTimeout> | undefined;
        const onAbort = () => {
          if (timer) clearTimeout(timer);
          try { this.cancel(ourId); } catch { /* best-effort */ }
          reject(new Error("concurrency-queue: waitForLaunch aborted"));
        };
        if (signal) signal.addEventListener("abort", onAbort, { once: true });
        // CORR7-2: compose the per-instance resetAbort controller with the
        // caller's signal so reset() can stop an in-flight poll loop. Use the
        // addEventListener bridge (not AbortSignal.any) for Node 18+ compat,
        // matching the existing abort plumbing in analyzeImage/fetchUsage.
        resetAbort.signal.addEventListener("abort", onAbort, { once: true });
        const cleanResetListener = () => resetAbort.signal.removeEventListener("abort", onAbort);
        const poll = () => {
          // Stop polling the moment the turn is aborted; otherwise the orphaned
          // promise would claim the token when freed and resolve a release fn
          // nobody holds (ADV-2 token leak).
          if (signal?.aborted || resetAbort.signal.aborted) return;
          // ADV4-1: the FIRST poll() runs synchronously inside the Promise
          // executor, so a throw here rejects the promise and acquireSlot's
          // finally cleans up — safe. But every SUBSEQUENT poll is a
          // setTimeout callback; a throw from mutate() there (acquireLock
          // timeout after 2s, readFileSync EACCES/EIO, writeStateAtomic
          // ENOSPC/EROFS) is not on any promise chain and surfaces as an
          // uncaughtException that terminates the Node process (and, with a
          // uncaughtException handler installed, leaves the promise forever
          // pending + the waiter leaked for staleWaiterMs = 5 min, stalling
          // siblings). Wrap the body so a throw on any re-entry clears the
          // timer, best-effort cancels our waiter entry, and rejects the
          // waitForLaunch promise — mirroring releaseSlot's drain-resilience
          // pattern (ADV3-1) so the poll loop is as resilient to lock/disk
          // errors as the drain loop already is.
          let got: boolean;
          try {
            got = mutate((now, state) => {
              // ADV-4: if our waiter entry was reaped by staleWaiterMs (5 min)
              // while we were still queued (e.g. a deep FIFO + slow models, or
              // a perpetually-full /usage per ADV-3), re-insert it at the tail
              // with a fresh timestamp so we don't poll forever with
              // head.id !== ourId permanently true. Only dead-PID waiters are
              // reaped for real; a live-PID waiter here means we were aged out
              // and must re-join.
              const stillQueued = state.waiters.some((w) => w.id === ourId);
              if (!stillQueued) {
                state.waiters.push({ id: ourId, pid: ourPid(), ts: now });
              }
              // Are we the head and is the token free?
              const head = state.waiters[0];
              if (!head || head.id !== ourId) return false; // not our turn
              if (state.token) return false; // token held by someone (reaped if stale)
              // Claim the token.
              state.token = { id: ourId, pid: ourPid(), ts: now };
              holdsToken = true;
              ourTokenId = ourId;
              return true;
            });
          } catch (err) {
            if (timer) clearTimeout(timer);
            cleanResetListener();
            try { this.cancel(ourId); } catch { /* best-effort */ }
            reject(new Error(`concurrency-queue: poll failed: ${err instanceof Error ? err.message : err}`));
            return;
          }
          if (got) {
            if (signal) signal.removeEventListener("abort", onAbort);
            cleanResetListener();
            resolve(() => {
              // Release: remove our token and our waiter entry. A throw here
              // (lock timeout, EACCES, ENOSPC) propagates to releaseSlot in
              // index.ts, which wraps release() in try/catch (ADV3-1) so the
              // drain continues and the watchdog reaps the stale entry.
              mutate((_now, state) => {
                if (state.token && state.token.id === ourId) {
                  state.token = null;
                }
                const idx = state.waiters.findIndex((w) => w.id === ourId);
                if (idx >= 0) state.waiters.splice(idx, 1);
                if (ourTokenId === ourId) {
                  holdsToken = false;
                  ourTokenId = null;
                }
                ourWaiterIds.delete(ourId);
              });
            });
            return;
          }
          // Not our turn yet; retry shortly. 50ms keeps the queue responsive
          // without hammering the lockfile or the disk.
          timer = setTimeout(poll, 50);
        };
        poll();
      });
    },

    pauseUntil(until: number, reason?: string | null): void {
      mutate((now, state) => {
        // ADV4-2: a 429-sourced pause (tagged PAUSE_REASON_429) is clamped to
        // the tighter MAX_PAUSE_429_MS (2.5 min) ceiling so a misconfigured
        // UMANS_BASE_URL returning 429 forever cannot wedge the account for the
        // full 5h MAX_PAUSE_MS. A server priority.low pause (the other caller)
        // keeps the 5h ceiling (a real account-wide deprio). The 2.5 min cap is
        // still >> the 30s PRIORITY_BACKOFF_MS floor, so a real 429 with a short
        // Retry-After is honored.
        const ceilingMs = reason === PAUSE_REASON_429 ? MAX_PAUSE_429_MS : MAX_PAUSE_MS;
        const clamped = clampPauseUntil(until, now, ceilingMs);
        // COV6-3: a past `clamped` is still > 0 when no pause is active, so the
        // write below would proceed — display is safe (pausedUntil > now is
        // false), but the on-disk pausedReason lingers as stale data. Early-
        // return when the pause is already elapsed (nothing to write).
        if (clamped <= now) {
          return;
        }
        if (clamped > state.pausedUntil) {
          state.pausedUntil = clamped;
          // SEC5-1 / ADV5-5: sanitize at the write boundary too (defense-in-
          // depth) so a poisoned reason never reaches the shared file,
          // regardless of caller. parsePriority already sanitizes the
          // server-sourced reason; this catches any future caller.
          // CORR7-1: do NOT overwrite a PAUSE_REASON_429 tag with a non-429
          // reason when extending. CORR4-1's clearPause guard keys on the
          // reason STRING, so a /usage priority.low tick with a longer deadline
          // + a non-null reason (e.g. "Account deprioritized") would wipe the
          // 429 tag, letting the next stale priority.low===false tick clear the
          // pause early — exactly the race CORR4-1 exists to prevent. The 429
          // tag stays authoritative; the longer deadline still extends
          // pausedUntil. When the 429 pause naturally elapses (pausedUntil <=
          // now), clearPause clears it normally.
          const newReason = sanitizeReason(reason);
          state.pausedReason = state.pausedReason === PAUSE_REASON_429
            ? PAUSE_REASON_429
            : (newReason ?? state.pausedReason ?? null);
          state.pausedTs = now;
        }
      });
    },

    clearPause(opts?: { force?: boolean }): void {
      mutate((now, state) => {
        // CORR4-1: /usage LAGS a 429 by 1-5s. A stale 5s refreshUsage tick
        // reporting priority.low===false would wipe a sibling's freshly-written
        // 429 pause, letting the next waiter launch into the deprio the gate
        // exists to prevent. Refuse to clear a 429-origin pause (tagged
        // PAUSE_REASON_429) unless forced — the 429 pause survives until it
        // naturally elapses (reapStale/pausedUntil<=now) OR /usage reports
        // priority.low===true (refreshUsage only calls clearPause on the
        // low===false branch, so a 429 pause set by a sibling survives the
        // stale tick; the 429 writer's OWN refreshUsage clears it only after its
        // own /usage catches up to priority.low===true then back to false).
        if (!opts?.force && state.pausedReason === PAUSE_REASON_429 && state.pausedUntil > now) {
          return; // keep the 429-origin pause
        }
        state.pausedUntil = 0;
        state.pausedReason = null;
        state.pausedTs = 0;
      });
    },

    touchToken(ourId: string): boolean {
      // C1 (HIGH): the capacity-poll loop in acquireSlot holds the launch token
      // across a long /usage poll (a pause-bounded wait can legitimately
      // exceed 120s — see CORR4-3). reapStale reaps any token whose now -
      // token.ts > staleTokenMs, regardless of liveness, so a poller that never
      // re-stamps its token gets reaped at 120s while it keeps polling — a
      // sibling claims and sends, the original poller breaks and sends too,
      // and two processes send concurrently, defeating the gate (the exact
      // 429 it exists to prevent). touchToken re-stamps state.token.ts to now
      // on every poll iteration so the watchdog only reaps a TRULY hung poller.
      // Returns false when the token was reaped (id mismatch) or absent — the
      // caller must then re-join the queue and wait its turn.
      try {
        return mutate((now, state) => {
          if (state.token && state.token.id === ourId) {
            state.token.ts = now;
            return true;
          }
          return false;
        });
      } catch {
        // mutate throws on lock timeout / disk error — treat as not-our-token
        // so the caller re-joins (safe default; the watchdog will clean up).
        return false;
      }
    },

    snapshot(): { queued: number; tokenHeld: boolean; paused: boolean; pausedUntil: number; pausedReason: string | null } {
      // Read without mutating; still reap for an accurate view.
      const now = cfg.now();
      const state = reapStale(readState(cfg.stateFile), cfg, now);
      // CORR7-3: reconcile holdsToken with the file. reapStale may have reaped
      // our token (id mismatch / absent) — e.g. the watchdog reaped it after
      // >120s while this process still believes it holds it. Without this,
      // snapshot().tokenHeld returns the stale local `holdsToken` (true) and the
      // status bar shows a stale `*`. If the file says the token is gone or
      // held by someone else, clear the local flag.
      if (holdsToken && state.token?.id !== ourTokenId) {
        holdsToken = false;
      }
      return {
        queued: state.waiters.length,
        tokenHeld: holdsToken,
        paused: state.pausedUntil > now,
        pausedUntil: state.pausedUntil,
        pausedReason: state.pausedUntil > now ? state.pausedReason : null,
      };
    },

    cancel(ourId: string): void {
      mutate((_now, state) => {
        const idx = state.waiters.findIndex((w) => w.id === ourId);
        if (idx >= 0) state.waiters.splice(idx, 1);
        if (state.token && state.token.id === ourId) {
          state.token = null;
          holdsToken = false;
          ourTokenId = null;
        }
        ourWaiterIds.delete(ourId);
      });
    },

    reset(): void {
      // CORR7-2: abort the per-instance resetAbort controller FIRST so any
      // in-flight waitForLaunch poll loop on this queue instance stops +
      // rejects (acquireSlot catches + returns undefined). Without this, reset()
      // splices our waiter id from the file, but a concurrent poll loop's mutate
      // sees stillQueued===false and RE-INSERTS the id at the tail (ADV-4's
      // re-insert path) every 50ms until the turn's AbortSignal aborts — leaking
      // a dead-PID waiter for staleWaiterMs (5 min) if the process exits before
      // pi aborts the signal. A fresh controller is created so subsequent waits
      // are not pre-aborted.
      resetAbort.abort();
      resetAbort = new AbortController();
      // Clear only OUR OWN entries: our waiter entry and the launch token if
      // we hold it. Do NOT unlink the shared state file or the lockfile — that
      // would race concurrent writers (breaking the O_EXCL invariant) and wipe
      // sibling pi processes' queue state. Leave both files for natural expiry
      // (the watchdog reaps stale token/waiter entries) and stale-lockfile
      // recovery. Matches the scope of `cancel`.
      // COV5-5 / ADV5-4: also splice out a queued-but-not-launched waiter
      // (ourTokenId === null, ourWaiterIds non-empty). Without this, a process that
      // join()ed but is still queued has reset() as a no-op, leaking the
      // waiter for staleWaiterMs (5 min) if the process doesn't exit —
      // blocking siblings behind a dead-PID entry.
      // COV6-2: splice EVERY id in ourWaiterIds (a Set, not a single slot) so a
      // second join() on one queue does not leak the first waiter. Reachable
      // from transformMessageImages (Promise.all → acquireSlot → join() per
      // image). The token single-slot (ourTokenId) is safe — token release is
      // closure-captured per waitForLaunch — but waiters accumulate, so we
      // splice the whole set.
      try {
        mutate((_now, state) => {
          for (const id of ourWaiterIds) {
            const idx = state.waiters.findIndex((w) => w.id === id);
            if (idx >= 0) state.waiters.splice(idx, 1);
          }
          const tokId = ourTokenId;
          if (tokId && state.token && state.token.id === tokId) {
            state.token = null;
          }
        });
      } catch { /* ignore: lock unavailable on shutdown; watchdog will clean up */ }
      holdsToken = false;
      ourTokenId = null;
      ourWaiterIds.clear();
    },
  };
}
