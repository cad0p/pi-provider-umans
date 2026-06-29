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
 *   over `limit`. The gate compares against `limit` (the soft cap), NOT
 *   `hard_cap`, so the burst headroom (hard_cap - limit) absorbs that
 *   overshoot → no 429, no deprioritization (see isCapacityFree).
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
import { mkdirSync, openSync, closeSync, unlinkSync, writeFileSync, renameSync, lstatSync, readdirSync, rmdirSync, fstatSync, readSync, constants as fsConstants } from "node:fs";
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

/**
 * An in-flight request: launched-but-not-completed, PID + timestamp tagged so
 * the same reapStale watchdog pattern that reaps stale waiters/tokens also
 * reaps a crashed/aborted in-flight entry (same 120s bound). The id reuses
 * the waiter id (addInFlight(ourId) is called after acquireSlot's capacity
 * check passes, reusing the same ourId), so cancel(ourId) cleans up both the
 * waiter + the in-flight entry in one pass.
 */
interface InFlightEntry {
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
   * In-flight requests launched by THIS machine but not yet completed. PID +
   * timestamp tagged (same reapStale watchdog pattern as waiters/token). The
   * gate checks max(localInFlight, concurrent_sessions_from_usage) < effectiveCap
   * — whichever is higher wins. Local in-flight catches within-machine bursts
   * (no /usage lag); /usage catches cross-machine + draindown. This does NOT
   * replace /usage — /usage is still the only cross-machine authority. Local
   * in-flight is an additional signal for the within-machine case, where the
   * /usage lag (300ms-2s) is fatal.
   */
  inflight: InFlightEntry[];
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
// 120s comfortably exceeds long streaming turns (xhigh/max
// thinking, long outputs, slow TTFT). The watchdog is a LAST RESORT (dead PID
// or truly hung process), not a tight bound on legitimate turns — the burst
// headroom (hard_cap - limit) absorbs any transient over-limit from a reaped
// token. 30s was too tight and reaped tokens held by legitimately long streaming
// turns, racing a sibling launch the same way as the message_end release race.
const DEFAULT_STALE_TOKEN_MS = 120_000;
const DEFAULT_STALE_WAITER_MS = 5 * 60 * 1000;
const DEFAULT_LOCK_RETRY_MS = 5;
const DEFAULT_LOCK_TIMEOUT_MS = 2_000;

/**
 * a lockfile whose mtime is in the future is treated as
 * stale + reclaimed. A future-dated mtime arises from a backwards clock jump
 * (NTP correction, resume from suspend, manual `date` change) or a
 * hand-edited/planted lockfile. Without this bound, `cfg.now() - st.mtimeMs`
 * is negative, the stale-lockfile condition is false, and the lock is never
 * reclaimed — wedging every `mutate` until the wall clock catches up.
 *
 * the prior 60s ceiling left a 1-60s gap where a near-future-dated
 * lockfile (small NTP skew) was NOT reclaimed, wedging the queue for up to 60s.
 * Lowered to 1s — catches any human-planted `touch -t` (always > 1s) and any
 * meaningful NTP skew (> 1s), while tolerating sub-ms floating-point jitter
 * from `utimesSync`/`Date.now()` round-trips (the child-holder test touches
 * mtime every 5ms; a zero tolerance would reclaim a live holder's freshly-
 * touched lockfile as "future-dated" due to sub-ms rounding). A lockfile < 1s
 * in the future ages out via the normal `lockTimeoutMs` ceiling within ~2s.
 */
export const MAX_LOCK_FUTURE_MS = 1_000;

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
 * a server returning 429 forever (e.g. a misconfigured UMANS_BASE_URL)
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
 * /usage LAGS a 429 (the design acknowledges this at the capacityFree
 * doc-comment), so a stale 5s refreshUsage tick reporting priority.low===false
 * would wipe a sibling's freshly-written 429 pause within 1-5s — letting the
 * next waiter launch into the deprio the gate exists to prevent. clearPause
 * refuses to clear a pause tagged with this reason until it naturally elapses
 * OR /usage reports priority.low===true (confirming the server caught up).
 * Exported so the provider tags the 429 pause with the exact same string.
 */
export const PAUSE_REASON_429 = "HTTP 429 from gateway";

/**
 * Reason tag written when the Umans server escalates from deprioritization
 * to a full account suspension (it returns HTTP 403 `account_suspended` /
 * `cap_abuse` / `cap_suspended` / `billing_error` instead of a 429). Both
 * the 403 response handler (raiseForUmansStatus + after_provider_response)
 * and the /v1/usage cap_abuse branch (isCapacityFree) push THIS SAME tag:
 * the 403 is the HTTP symptom of the same underlying cap_abuse suspension,
 * so a single tag eliminates the reason-flip fragility where a stale
 * /v1/usage tick could wipe a freshly-written pause whose reason changed
 * between the two observation channels. The cap_abuse pause uses the 5h
 * MAX_PAUSE_MS ceiling (the non-429 branch of pauseUntil).
 */
export const PAUSE_REASON_CAP_ABUSE = "account cap_abuse suspension";

/**
 * Reason tag written by the strike counter when the 24h 429 count approaches
 * the server's 5h-pause threshold (see refreshStrikes in index.ts). Sticky
 * so a stale /v1/usage tick does not wipe a freshly-written strike pause.
 */
export const PAUSE_REASON_STRIKES = "429 strike limit approached";

/**
 * Reason tag written by the main-turn after_provider_response 403 handler as
 * a SHORT non-sticky bridge. The pi after_provider_response event carries
 * status + headers but NO body (the body has not streamed yet at headers
 * time), so the boxed_until deadline carried in a 403 suspend-family body is
 * unreachable here. A non-sticky bridge backs siblings off immediately
 * without poisoning the gate for an unrelated 403 (an auth error, a proxy HTML
 * page): it is NOT in STICKY_PAUSE_REASONS, so a stale /v1/usage tick
 * reporting priority.low===false clears it, and the message_end handler
 * reconciles it against the real body once the stream completes — pushing
 * the sticky PAUSE_REASON_CAP_ABUSE pause if the body is a suspend family,
 * or clearing the bridge if it is not. The bridge is bounded by
 * PAUSE_403_BRIDGE_MS (5s): long enough for the body to stream + the
 * message_end reconciliation to run, short enough that an unrelated 403
 * does not serialize siblings beyond a brief blip.
 */
export const PAUSE_REASON_403_BRIDGE = "HTTP 403 bridge (awaiting body)";

/**
 * Duration of the main-turn 403 bridge pause (5s). See PAUSE_REASON_403_BRIDGE
 * for the bound rationale (body-stream lag for a 403 error response).
 */
export const PAUSE_403_BRIDGE_MS = 5_000;

/**
 * The set of pause reasons that a stale /v1/usage tick reporting
 * priority.low===false must NOT clear. /v1/usage LAGS a real suspension by
 * 1-5s (the same lag the sticky-pause guard defends for 429s), so a stale-low===false tick
 * arriving right after a 403/cap_abuse pause was written would wipe it —
 * letting the next waiter launch into a still-suspended account + re-trip
 * the cascade the pause exists to prevent. clearPause + the refreshUsage
 * call-site guard both check this set instead of the prior hardcoded
 * PAUSE_REASON_429 / PAUSE_REASON_STRIKES checks so the new cap_abuse
 * reason is covered symmetrically.
 */
export const STICKY_PAUSE_REASONS = new Set([PAUSE_REASON_429, PAUSE_REASON_CAP_ABUSE, PAUSE_REASON_STRIKES]);

/**
 * ISO-8601 timestamp regex used to extract `boxed_until` from a 403 error
 * body when the deadline is embedded in an error MESSAGE STRING rather than
 * a structured JSON field (the incident 2026-06-27 showed the server emits
 * the suspension deadline inside the error message text, not as a
 * `boxed_until` field). Matches `2026-06-28T03:09:24Z` and the
 * fractional-seconds variant `2026-06-28T03:09:24.123Z`.
 *
 * The `g` flag enables `matchAll` iteration so a body carrying a PAST
 * reference timestamp before the future deadline (e.g.
 * `account_suspended from <past> until <future>`) does not mask the real
 * deadline: the past match is skipped by the `t > now` guard + the loop
 * continues to the future match.
 */
const ISO_TIMESTAMP_RE = /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z/g;

/**
 * Tolerantly extract a suspension deadline (epoch-ms) from a 403 response
 * body. The Umans server emits `boxed_until` in three shapes observed across
 * the suspend family: (a) a structured JSON field (top-level or nested under
 * `error`), (b) an ISO-8601 timestamp embedded inside an error MESSAGE
 * STRING (the incident-2026-06-27 shape), or (c) absent entirely (an HTML
 * gateway page or an unrelated 403). This helper tries (a) then (b) then
 * returns `undefined` so the caller can apply a 30s floor fallback.
 *
 * A PAST `boxed_until` (the server thinks the suspension elapsed but is
 * still returning 403 — a stale deprio tick right as the suspension lifts, or
 * a crafted value) is treated as ABSENT → `undefined`. pauseUntil
 * early-returns on a past deadline, so without this guard a 403 carrying a
 * past `boxed_until` would silently disable the pause + re-arm the cascade.
 *
 * The LATEST future timestamp is returned (the maximum), not the first —
 * for BOTH paths (structured-JSON + regex fallback). A body can carry
 * multiple future timestamps where a NON-deadline future timestamp appears
 * BEFORE the real deadline; returning the first would yield a too-short
 * pause (siblings launch into the still-suspended account after the shorter
 * pause elapses). The maximum is fail-safe: it over-pauses on bodies with
 * multiple future timestamps (siblings wait longer, no cascade), but cannot
 * under-pause. The structured-JSON path is authoritative when present; the
 * regex is a tolerant secondary, and both share the fail-safe max-future
 * reduction.
 */
export function extractBoxedUntil(body: string): number | undefined {
  if (!body) return undefined;
  const now = Date.now();
  let parsed: unknown = undefined;
  try { parsed = JSON.parse(body); } catch { /* not JSON — fall through to regex */ }
  if (parsed && typeof parsed === "object") {
    const candidates = [
      (parsed as { boxed_until?: unknown }).boxed_until,
      (parsed as { error?: { boxed_until?: unknown } }).error?.boxed_until,
    ];
    // candidates = [top-level boxed_until, error.boxed_until]; see the JSDoc
    // for the max-future + past-as-absent rationale.
    let latest: number | undefined;
    for (const b of candidates) {
      const ms = toEpochMs(b);
      if (ms !== undefined && ms > now && (latest === undefined || ms > latest)) {
        latest = ms;
      }
    }
    if (latest !== undefined) return latest;
  }
  // Regex fallback for prose-only bodies. matchAll (not match) iterates every
  // ISO timestamp — requires the `g` flag on ISO_TIMESTAMP_RE. See the JSDoc
  // for the max-future + past-as-absent rationale.
  let latest: number | undefined;
  for (const m of body.matchAll(ISO_TIMESTAMP_RE)) {
    const t = Date.parse(m[0]);
    if (!Number.isNaN(t) && t > now && (latest === undefined || t > latest)) {
      latest = t;
    }
  }
  return latest;
}

function toEpochMs(b: unknown): number | undefined {
  if (typeof b === "number" && b > 0) return b > 1e12 ? b : b * 1000;
  if (typeof b === "string" && b) {
    const t = Date.parse(b);
    if (!Number.isNaN(t)) return t;
  }
  return undefined;
}

/**
 * Detect whether a response body indicates a suspend-family account state
 * (the Umans server returns HTTP 403 with one of these strings for a full
 * account suspension, as opposed to a per-request auth error or an HTML
 * gateway page). Matches `account_suspended`, `cap_abuse`, `cap_suspended`,
 * `billing_error` case-insensitively, either as a JSON `type`/`error.type`
 * field or anywhere in the raw body string (covers the error-message-text
 * shape). A 403 WITHOUT a suspend-family body (an auth error, a proxy HTML
 * page) does NOT push a pause — the turn still throws, but the shared gate
 * is not poisoned for siblings.
 */
const SUSPEND_REASON_RE = /account_suspended|cap_abuse|cap_suspended|billing_error/i;
export function isSuspendBody(body: string): boolean {
  if (!body) return false;
  return SUSPEND_REASON_RE.test(body);
}

/**
 * True when a /v1/usage `priority.reason` string indicates a full account
 * suspension (the gate must fully pause) vs. a transient deprioritization
 * (the gate lowers the cap by 1 + keeps working). Matches the same family
 * as isSuspendBody. `rate_limited` + absent/unknown keep the lower-cap-by-1
 * path.
 */
export function isSuspendReason(reason: string | null | undefined): boolean {
  if (typeof reason !== "string" || !reason) return false;
  return SUSPEND_REASON_RE.test(reason);
}

/**
 * upper bound on the state file size we'll read+parse.
 * Legitimate state is <2 KB even with hundreds of waiters; a poisoned or
 * runaway file (e.g. 1 GB) would OOM/stall the pi process. readState stats the
 * file first + bails to the empty-state catch when st.size exceeds this.
 */
export const MAX_STATE_BYTES = 1_000_000;

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
 * Cap + sanitize a pause reason before it is stored or rendered. A
 * compromised or misconfigured gateway can push a crafted
 * `priority.reason` that flows unescaped into the status bar (PAUSED <Ns>
 * (<reason>)). Cap to ~64 chars and strip non-printable / control / ANSI-escape
 * characters so a crafted string cannot mangle the bar or inject control
 * sequences. The reason is operator-facing metadata only (the source is already
 * distinguishable via PAUSE_REASON_429); a 64-char printable-only cap removes
 * the injection surface without losing signal.
 */
const PAUSE_REASON_MAX_CHARS = 64;
// strip control chars (incl. DEL), ESC (0x1b, ANSI escape
// introducer), + Unicode bidi/RTL override chars (U+202A-E, U+2066-9, U+061C)
// and zero-width / BOM chars (U+200B-F, U+FEFF) that could spoof the displayed
// pause reason in the status bar. Keeps printable ASCII + common printable
// Unicode. Mirrored in sanitizeErrorBody (index.ts) via the shared
// SANITIZE_CTRL_RE export so the character class stays in sync
// across both modules without manual duplication.
export const SANITIZE_CTRL_RE = /[\x00-\x1f\x7f\u061c\u200b-\u200f\u202a-\u202e\u2066-\u2069\ufeff]/g;
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
 * clamp `until` to now + MAX_PAUSE_MS so the parse boundary matches the
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
    // clamp at the parse boundary too (defense-in-depth).
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
   * 429s / deprioritizes). The gate compares against `limit` (the soft cap)
   * first, NOT `hard_cap` — the burst headroom (hard_cap - limit) exists to
   * absorb the message_end release race + server-side accounting noise, so
   * gating to `limit` keeps that headroom intact. `hard_cap` is the fallback
   * when the API reports only `hard_cap` (older API) or when a local env
   * override is absent.
   */
  hardCap: number | undefined;
  priority: PriorityState;
}

/**
 * Inputs to the capacity decision: the effective concurrency cap (env override
 * or the live /usage value), whether the shared pausedUntil is active, + the
 * local in-flight count (requests launched-but-not-completed on THIS machine).
 * localInFlight is passed IN (read via snapshot().inflightCount) so
 * isCapacityFree stays a pure decision with no I/O — selfcheck constructs a
 * CapacitySnapshot inline + asserts the decision without a state file.
 */
interface CapacityInputs {
  limit: number | undefined;
  queuePaused: boolean;
  /** Local in-flight count (passed IN, not read inside). */
  localInFlight?: number;
}

/**
 * Pure decision: may this process launch given a /usage snapshot + inputs?
 * Returns { free, repause? } where `repause` is set when priority.low is
 * observed and the caller should push the pause to the shared file.
 *
 * - If the shared pause is active → not free (C2: a 429 observed by any local
 *   process backs off all siblings before /usage propagates priority.low).
 * - If priority.low → free, but with the cap lowered by 1 (deprioritization
 *   means the server is under load + races are riskier; reduce parallelism
 *   by one slot rather than fully pausing — requests still go through, just
 *   slower). No repause is pushed: priority.low is a status signal, not a
 *   stop condition. Actual 429s + the strike counter handle the hard pause.
 * - If /usage is unreachable (snap === null) → free (trust headroom rather
 *   than block forever; the queue still serializes launches via the token).
 * - If concurrent_sessions >= limit (or hardCap when limit absent, or
 *   inputs.limit when both snap caps are absent, minus 1 when priority.low)
 *   → not free. An unlimited plan (inputs.limit === undefined)
 *   is NO LONGER a short-circuit — the cap check below runs so a Code Max
 *   account can't trip the account-wide burst cap; only when ALL caps are
 *   undefined (true unlimited with no burst cap reported) is the gate free
 *   (no cap to exceed).
 * - Otherwise → free.
 *
 * The gate compares against `limit` (the soft cap), NOT `hard_cap` (the 429
 * threshold). The message_end release race can transiently push
 * concurrent_sessions 1-2 over the gate before the server decrements;
 * gating to `limit` leaves the full burst headroom (hard_cap - limit) to
 * absorb that race + server-side concurrent_sessions accounting noise
 * (the counter oscillates ±1 during a single serialized turn).
 * Gating to `hard_cap` would leave zero headroom — the race would
 * immediately push past hard_cap → 429 → deprioritization.
 */
export function isCapacityFree(
  snap: CapacitySnapshot | null,
  inputs: CapacityInputs,
): { free: boolean; repause?: { until: number; reason: string | null } } {
  if (inputs.queuePaused) return { free: false };
  if (!snap) return { free: true }; // /usage unreachable → trust headroom
  // reason-aware pause: when priority.low AND the reason indicates a
  // suspend-family account state (cap_abuse / cap_suspended / account_suspended
  // / billing_error), the account is SUSPENDED (the server returns 403), not
  // just slow. Lowering the cap by 1 is wrong — no launches should happen
  // until boxed_until clears. Return { free: false, repause } so the caller
  // pushes a full PAUSE_REASON_CAP_ABUSE pause. The cap_abuse pause uses the
  // 5h MAX_PAUSE_MS ceiling (the non-429 branch of pauseUntil). A >5h real
  // suspension self-heals via overhang re-push (the 5h pause reaps, the next
  // poll re-observes cap_abuse + re-pushes with the remaining boxed_until).
  // return the repause, do NOT push it here — isCapacityFree is a pure
  // decision (no I/O); the caller in capacityFree pushes it. This mirrors how
  // a priority.low repause is already returned + pushed.
  if (snap.priority.low && isSuspendReason(snap.priority.reason)) {
    return { free: false, repause: { until: snap.priority.until, reason: PAUSE_REASON_CAP_ABUSE } };
  }
  // max(localInFlight, concurrentSessions): whichever is higher wins. Local
  // in-flight catches within-machine bursts (no /usage lag — the 300ms-2s
  // server accounting lag is what let 6 researchers all poll a stale-low
  // concurrent_sessions + launch simultaneously, hitting hard_cap → 19
  // concurrency 429s → cap_abuse 5h suspension). /usage catches cross-machine
  // + draindown. Using max (not sum) avoids double-counting the local
  // in-flight that /usage already includes once it catches up: if /usage is
  // fresh (reports 4), max(2, 4) = 4 >= cap → wait (correct); if /usage is
  // stale-low (reports 2, only seeing remote), max(2, 2) = 2 < cap → free
  // → 3rd local launches (true total 5, absorbed by hard_cap headroom).
  const cur = Math.max(inputs.localInFlight ?? 0, snap.concurrentSessions ?? 0);
  // Gate against `limit` (the soft cap), NOT `hard_cap` (the 429 threshold).
  // The message_end release race can transiently push concurrent_sessions
  // 1-2 over the gate before the server decrements. Gating to `limit` leaves
  // the full burst headroom (hard_cap - limit) to absorb that race; gating to
  // `hard_cap` leaves zero headroom, so the race immediately pushes past
  // hard_cap → 429 → deprioritization.
  //
  // Precedence: inputs.limit (the env override UMANS_CONCURRENCY_LIMIT, for
  // testing with a lower value) takes precedence over the server's reported
  // limit, which in turn takes precedence over the server's hard_cap (fallback
  // when the API reports only hard_cap, e.g. older API responses).
  const baseCap = inputs.limit ?? snap.limit ?? snap.hardCap;
  // Deprioritization (priority.low): lower the cap by 1 rather than fully
  // pausing. The server is under load + races are riskier, so reduce
  // parallelism by one slot. Requests still go through (just slower); this
  // is a status signal, not a stop condition. Actual 429s + the strike
  // counter handle the hard pause.
  //
  // Floor the post-deprio cap at 1 (not 0). The concurrencyLimit floor-of-1
  // (Math.max(1, Math.floor(serverLimit * multiplier))) guarantees at least 1
  // slot for a sub-1 multiplier; without this matching floor here, a sub-1
  // multiplier (e.g. 0.1 with limit 4 → floored to 1) under deprio would drop
  // the cap to 0 (max(0, 1-1)=0) → every poll reports not-free → a 60s stall
  // before fail-open on every launch while deprioritized. The two floors
  // together guarantee the gate keeps at least 1 slot live even under deprio
  // + a sub-1 multiplier, so a user who asked for "conservative" does not
  // get a 60s stall on every launch.
  const cap = snap.priority.low && baseCap !== undefined ? Math.max(1, baseCap - 1) : baseCap;
  if (cap !== undefined && cur >= cap) return { free: false };
  return { free: true };
}

/**
 * Parse a UMANS_CONCURRENCY_LIMIT env value into a positive integer, falling
 * back to the live /v1/usage value when unset, empty, non-positive, or
 * fractional. Handles edge inputs: "2.5" (fractional → fallback, almost
 * certainly a typo — a float threshold on an integer counter silently floors
 * to 2 usable slots), " " (whitespace → 0 → fallback), "0" (non-positive →
 * fallback), "abc" (NaN → fallback), "" (empty → fallback).
 * Tightened from Number.isFinite to Number.isInteger so a fractional typo
 * falls back to the server value (a strict improvement for a slot-count knob).
 * tightened further to a strict /^\d+$/ regex test BEFORE
 * Number(trimmed) so hex ("0x10" === 16) and scientific notation ("1e3" ===
 * 1000) — which Number() accepts + Number.isInteger() happily passes — are
 * rejected to fallback. A "0x10" typo would silently set the gate to 16,
 * defeating the cap. Mirrors the 429 Retry-After parse (which already used
 * /^\d+$/). The existing `n > 0` check still rejects 0.
 * Number.isInteger returns true for huge integers within
 * Number.MAX_VALUE (e.g. "999999999999999999999" → 1e21), silently disabling
 * the cap. Tightened to Number.isSafeInteger (rejects >2^53-1) + a practical
 * ceiling of 1024 so a long-digit typo falls back instead of effectively
 * unbounding the gate.
 */
const MAX_CONCURRENCY_LIMIT = 1024;
export function parseConcurrencyLimit(envValue: string | undefined, fallback: number | undefined): number | undefined {
  const trimmed = envValue?.trim();
  if (!trimmed || !/^\d+$/.test(trimmed)) return fallback;
  const n = Number(trimmed);
  return Number.isSafeInteger(n) && n > 0 && n <= MAX_CONCURRENCY_LIMIT ? n : fallback;
}

/** Generate a unique waiter/token id. */
function newId(): string {
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

/**
 * Shape guard for a WaiterEntry. A poisoned/hand-edited state file can
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

/** Shape guard for an InFlightEntry (same rationale as isWaiterEntry — Adv2). */
function isInFlightEntry(e: unknown): e is InFlightEntry {
  return typeof e === "object" && e !== null &&
    typeof (e as InFlightEntry).id === "string" &&
    typeof (e as InFlightEntry).pid === "number" &&
    typeof (e as InFlightEntry).ts === "number";
}

/** Read the queue state, or return a fresh empty state if the file is absent/corrupt. */
export function readState(path: string): QueueState {
  // open the fd FIRST + fstat it + read from the fd, so the
  // regular-file + size checks + the read are atomic wrt path swaps. The
  // prior lstatSync(path) + readFileSync(path) pair had a TOCTOU window: an
  // attacker could swap the file (e.g. to a symlink) between the lstat + the
  // read. Mirrors the write path's fd-based pattern (writeStateAtomic opens
  // with O_EXCL + writes to the fd). lstatSync is still used to detect a
  // symlink at the PATH (without following it) so a planted symlink state
  // file is rejected before opening; the fd read then cannot be swapped to a
  // different target. A missing file throws ENOENT from openSync, caught by
  // the outer try → empty state (matching the prior absent-file behavior).
  //
  // the open uses O_NOFOLLOW | O_NONBLOCK.
  // O_NOFOLLOW makes the open fail with ELOOP on a symlink (eliminating the
  // symlink-swap vector). O_NONBLOCK prevents a swapped FIFO from blocking
  // the open indefinitely (probe-confirmed: openSync(fifo, O_RDONLY|O_NOFOLLOW)
  // with no writer blocks past 8s; with O_NONBLOCK it returns immediately).
  // The post-open fstatSync re-check then rejects the non-regular fd (FIFO /
  // char device) before the read, closing the FIFO-wedge vector. The lstat
  // guard remains as a fast-path early return for the common symlink/FIFO case.
  //
  // The prior comment claimed O_NOFOLLOW alone prevented the FIFO block —
  // that was false (O_NOFOLLOW does not imply O_NONBLOCK).
  // Probe-confirmed the block + the fix.
  try {
    let fd: number | undefined;
    try {
      // lstatSync (not statSync) so a symlink state file is detected as
      // non-regular + treated as empty without following the link. A FIFO/pipe
      // or character device would otherwise block readFileSync indefinitely
      // (wedging the mutate call + the O_EXCL lock).
      const lstat = lstatSync(path);
      if (!lstat.isFile() || lstat.size > MAX_STATE_BYTES) {
        return { waiters: [], token: null, inflight: [], pausedUntil: 0, pausedReason: null, pausedTs: 0 };
      }
      // open the fd AFTER the lstat guard with
      // O_NOFOLLOW | O_NONBLOCK so a path swap between the lstat + the open
      // cannot redirect the open into a symlink (ELOOP) or block on a swapped
      // FIFO (O_NONBLOCK makes the open return immediately). fstat operates on
      // the fd we already hold; readSync reads from that fd. Re-check isFile
      // + size on the fd in case the file was swapped (the lstat was on the
      // path). A swapped FIFO opened with O_NOFOLLOW|O_NONBLOCK|O_RDONLY returns
      // immediately; the fstatSync re-check rejects the non-regular fd (closing
      // it) before the read.
      fd = openSync(path, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW | fsConstants.O_NONBLOCK);
      const st = fstatSync(fd);
      if (!st.isFile() || st.size > MAX_STATE_BYTES) {
        closeSync(fd);
        return { waiters: [], token: null, inflight: [], pausedUntil: 0, pausedReason: null, pausedTs: 0 };
      }
      const buf = Buffer.alloc(st.size);
      readSync(fd, buf, 0, st.size, 0);
      const raw = buf.toString("utf8");
      const parsed = JSON.parse(raw) as Partial<QueueState>;
      const waiters = Array.isArray(parsed.waiters) ? parsed.waiters.filter(isWaiterEntry) : [];
      const token = isTokenState(parsed.token) ? parsed.token : null;
      // shape-guard inflight (same pattern as waiters). A poisoned
      // non-array inflight (e.g. inflight: "garbage" or inflight: 42) would
      // make state.inflight.length undefined/throw → max(NaN, cap) = NaN →
      // NaN < cap is false → gate blocks forever (silent DoS). A non-array is
      // coerced to []; malformed entries are dropped by isInFlightEntry so
      // isPidDead operates on well-typed input (same rationale as isWaiterEntry).
      const inflight = Array.isArray(parsed.inflight) ? parsed.inflight.filter(isInFlightEntry) : [];
      return {
        waiters,
        token,
        inflight,
        pausedUntil: typeof parsed.pausedUntil === "number" ? parsed.pausedUntil : 0,
        // sanitize pausedReason on the READ boundary too.
        // The write-boundary sanitize (pauseUntil) + parse path (parsePriority)
        // cover the happy path, but readState passed parsed.pausedReason straight
        // through → snapshot() → status bar/notify render raw. A hand-edited
        // file, a compromised sibling writing JSON directly, or a file poisoned
        // by an earlier unfixed build surfaces the raw string. The
        // write-boundary sanitize is the primary guard; this closes the
        // hand-edited-file gap (defense-in-depth).
        pausedReason: sanitizeReason(parsed.pausedReason ?? null),
        pausedTs: typeof parsed.pausedTs === "number" ? parsed.pausedTs : 0,
      };
    } finally {
      if (fd !== undefined) { try { closeSync(fd); } catch { /* best-effort */ } }
    }
  } catch {
    return { waiters: [], token: null, inflight: [], pausedUntil: 0, pausedReason: null, pausedTs: 0 };
  }
}

/**
 * True if a PID is not currently alive. Never throws.
 *
 * this is one of the module's pure-helper exports — pure,
 * side-effect-free functions exposed so external consumers (and selfcheck)
 * can build on the queue's primitives. They are not used by index.ts (the
 * provider goes through the ConcurrencyQueue handle), but are a defensible
 * small public API for a standalone queue module. See the export list at
 * the top of this module for the full set (readState / reapStale / isPidDead
 * / parsePriority / parseConcurrencyLimit / isCapacityFree / clampPauseUntil
 * / sanitizeReason / MAX_PAUSE_MS / PAUSE_REASON_429 /
 * SANITIZE_CTRL_RE / MAX_STATE_BYTES).
 */
export function isPidDead(pid: number): boolean {
  // defensive guard for non-numeric / non-finite input. The shape
  // guards (isWaiterEntry/isTokenState) already drop malformed entries, but a
  // future caller could bypass them; treat non-number / NaN / Infinity as
  // dead so reapStale reclaims rather than passing garbage to process.kill
  // (which would throw a synchronous TypeError not filtered by the catch).
  if (typeof pid !== "number" || !Number.isFinite(pid) || !pid || pid <= 0) return true;
  try {
    // process.kill(pid, 0) throws ESRCH (no such process) or EPERM (process
    // exists but caller lacks permission). ESRCH -> dead. EPERM -> the process
    // IS alive (just not ours); treat it as alive so we don't falsely reap a
    // live holder's token. In a single-user pi setup all
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
 * compromised sibling or a hand-edited file). Returns the cleaned
 * state; does not write to disk.
 *
 * PID reuse is a known blind spot of the kill(pid, 0) probe — if a
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
  // Reap in-flight entries with the same watchdog pattern as waiters/token:
  // a dead-PID entry (crashed process) or one older than staleTokenMs (120s)
  // is reaped. The 120s bound matches the token (a legitimately long streaming
  // turn exceeds 120s; the in-flight entry for a completed turn is removed by
  // the release fn at message_end/turn_end/agent_end, so a live entry older
  // than 120s is by construction a crashed/aborted turn). A SIGKILL between
  // addInFlight + the HTTP send leaves a phantom entry that blocks one slot
  // for up to 120s (fail-closed, consistent with the token watchdog).
  const inflight = state.inflight.filter((e) =>
    !isPidDead(e.pid) && (now - e.ts) <= cfg.staleTokenMs
  );
  // Reap a pause that violates the MAX_PAUSE_MS ceiling. Three conditions:
  // (1) pausedTs is older than the ceiling (the original
  // defense — a clamp-bypassed poisoned value ages out); (2) the pause
  // DURATION (pausedUntil - now) itself exceeds the ceiling from the current
  // vantage, regardless of pausedTs — this catches a forward-dated pausedTs
  // (a hand-edited file setting pausedTs to the future makes `now - pausedTs`
  // negative, bypassing condition 1) paired with an oversized pausedUntil;
  // (3) the pause's OWN claimed duration (pausedUntil - pausedTs) exceeds the
  // ceiling, independent of now — closes a gap where a forward-dated
  // pausedTs (e.g. now+1h) paired with a sub-ceiling pausedUntil (e.g. now+4h)
  // bypasses both (1) (age is negative) and (2) (duration-from-now is 3h, under
  // the 5h ceiling), yet the pause's claimed 3h span exceeds MAX_PAUSE_MS from
  // pausedTs. Probe-confirmed: a hand-edited file with pausedTs=future+1h +
  // pausedUntil=future+4h survived both prior checks. Condition (3) keys on the
  // pause's own claimed span (pausedUntil - pausedTs) regardless of where
  // pausedTs sits relative to now, so a poisoned pause whose claimed duration
  // exceeds the ceiling is reaped no matter where pausedTs lands.
  let { pausedUntil, pausedReason, pausedTs } = state;
  if (pausedUntil > 0) {
    const ageTooOld = pausedTs > 0 && (now - pausedTs) > MAX_PAUSE_MS;
    const durationTooLong = (pausedUntil - now) > MAX_PAUSE_MS;
    const claimedDurationTooLong = pausedTs > 0 && (pausedUntil - pausedTs) > MAX_PAUSE_MS;
    if (ageTooOld || durationTooLong || claimedDurationTooLong) {
      pausedUntil = 0;
      pausedReason = null;
      pausedTs = 0;
    }
  }
  return { ...state, token, waiters, inflight, pausedUntil, pausedReason, pausedTs };
}

/**
 * feature-detect Atomics.wait for a non-CPU-burning sync sleep.
 * Exported (alongside decideLaunch / shouldReleaseOnMessageEnd) so the code
 * path is unit-testable. Returns true when Atomics.wait + SharedArrayBuffer are
 * available (the acquireLock spin uses it); false when the runtime lacks them
 * (older Node, or SAB disabled via --no-harmony-sharedarraybuffer) and the
 * caller must fall back to a busy-spin.
 */
function canAtomicsWait(): boolean {
  return typeof Atomics !== "undefined" && typeof Atomics.wait === "function" &&
    typeof SharedArrayBuffer !== "undefined";
}

/**
 * synchronous sleep that does NOT spin the CPU. Under lock
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
 * the 2s lockTimeoutMs is a hard CORRECTNESS ceiling on the
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
      // The lockfile is a zero-byte O_EXCL sentinel. The prior
      // design wrote the holder PID + read it back via readFileSync
      // for a PID-based fast-path, but the read was dropped (lstat→read
      // TOCTOU). The PID write was retained as dead code until it was dropped
      // — a write that is never read back is not a fencing token. The
      // mtime ceiling is the sole authoritative reclaim bound.
      // the only throwing call is openSync itself. If it
      // throws (EEXIST handled below; ENOSPC/EIO/EROFS re-thrown by the outer
      // catch), fd is still undefined (no leak possible — there is no
      // writeFileSync after openSync that could throw + leave fd open). The
      // prior fix wrapped a writeFileSync that no longer exists.
      // (EEXIST can't reach here — O_EXCL openSync only resolves when the
      // file did not exist.)
      // No PID write — the lockfile is a zero-byte O_EXCL sentinel.
      // The fd is closed by the release fn returned below.
    } catch (e: any) {
      if (e.code !== "EEXIST") throw e;
      // Lock is held by another process — or stale from a crash. Reclaim if the
      // holder PID is dead OR the lockfile is older than the lock timeout.
      // use lstatSync (not statSync) so the mtime check reads the
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
        // the mtime ceiling is the authoritative reclaim bound. The
        // PID-based fast-path was an optimization that read the
        // lockfile content via readFileSync — dropped to remove the TOCTOU
        // between lstatSync (confirms regular file) + readFileSync (follows
        // symlinks). The PID write was also dropped (dead code).
        // reclaim if the lockfile is older than lockTimeoutMs
        // (the original mtime ceiling) OR if its mtime is in the future beyond
        // MAX_LOCK_FUTURE_MS (1s). A future-dated mtime — from clock skew, NTP
        // correction, resume from suspend, or a `touch -t` attack — is not a
        // legitimate hold. The prior 60s ceiling left a 1-60s gap;
        // lowered to 1s to catch any human-planted touch + meaningful NTP skew
        // while tolerating sub-ms floating-point jitter from utimesSync/Date.now
        // round-trips. A lockfile < 1s in the future ages out via lockTimeoutMs.
        if (cfg.now() - st.mtimeMs > cfg.lockTimeoutMs || st.mtimeMs - cfg.now() > MAX_LOCK_FUTURE_MS) {
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
      // prefer Atomics.wait over a CPU-burning busy-spin. Under
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
  // stale-.tmp reaping moved out to mutate's withLock block so it uses the
  // injected cfg.now() (testability — a frozen clock now exercises the reaper).
  // include a short random suffix in the temp name so a recycled pid
  // never collides with a stale leftover from a prior (crashed) run sharing
  // the same pid. The reaper's prefix match (`${basename(path)}.` + suffix
  // `.tmp`) still matches; the random middle distinguishes concurrent runs.
  const tmp = `${path}.${process.pid}.${Math.random().toString(36).slice(2, 8)}.tmp`;
  // open with O_EXCL ("wx") + 0o600 so a planted symlink at the per-pid
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
  // wrap renameSync in try/catch so a throw (EISDIR, EXDEV) unlinks
  // the .tmp before re-throwing. Without this the temp file leaks on disk
  // (reaped after 10s by reapStaleTmps, but accumulates under sustained
  // failure — e.g. a planted directory at `path` makes every renameSync throw
  // EISDIR, leaking a .tmp per mutate). Probe-confirmed. unlinkSync errors are
  // swallowed (best-effort; the reaper is the safety net).
  try {
    renameSync(tmp, path); // atomic on POSIX & Windows; rename preserves the temp's 0600 mode
  } catch (err) {
    try { unlinkSync(tmp); } catch { /* best-effort: reaper will clean up */ }
    throw err;
  }
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
// now is threaded in from mutate's caller (cfg.now()) so a frozen clock
// exercises the reaper — a regression inverting the comparison would not be
// caught otherwise.
// cap the number of .tmp files unlinked per mutate (REAP_TMP_MAX) to
// bound the critical section under pathological .tmp accumulation. The reaper
// runs inside the O_EXCL lock, so unlinking thousands of stale leftovers would
// extend the critical section past the 2s lockTimeoutMs ceiling,
// racing two writers. Leave the rest for the next mutate.
const REAP_TMP_MAX = 100;
function reapStaleTmps(path: string, now: number): void {
  const dir = dirname(path);
  const prefix = `${basename(path)}.`;
  let entries: string[];
  try { entries = readdirSync(dir); } catch { return; /* dir missing/unreadable */ }
  let unlinked = 0;
  for (const name of entries) {
    if (unlinked >= REAP_TMP_MAX) break; // bound the critical section
    if (!name.startsWith(prefix) || !name.endsWith(".tmp")) continue;
    const full = `${dir}/${name}`;
    try {
      // use lstatSync (not statSync) so a symlink
      // .tmp → /etc/passwd is detected as non-regular + unlinked directly
      // without following the link (matching the lockfile's posture).
      // statSync would follow the symlink, read the TARGET's mtime, and
      // unlinkSync (which removes the symlink itself, safe) but the mtime
      // leak is inconsistent with the hardened posture.
      // a symlink .tmp is NEVER a legitimate temp file (the writer
      // uses openSync("wx") which creates regular files only). Unlink
      // unconditionally regardless of mtime — a freshly-planted symlink has
      // lstatSync().mtimeMs ≈ now, so the STALE_TMP_MS check below would skip
      // it, leaving it to block a future writeStateAtomic that generates the
      // same temp name (EEXIST). Matches the lockfile's non-regular → unlink
      // posture at acquireLock.
      const st = lstatSync(full);
      // a planted .tmp DIRECTORY would make unlinkSync throw EISDIR
      // (swallowed) AND writeStateAtomic's openSync("wx") throw EEXIST (the
      // real wedge — the per-pid temp name is a directory). rmdir it if empty
      // (best-effort) + skip; a non-empty dir is left for the operator.
      if (st.isDirectory()) {
        try { rmdirSync(full); } catch { /* non-empty or gone — skip */ }
        unlinked++;
        continue;
      }
      if (!st.isFile()) {
        // a symlink (or other non-regular non-directory) at a .tmp
        // name is never legitimate — unlink unconditionally regardless of
        // mtime. The writer uses openSync("wx") which creates regular files
        // only, so a non-regular .tmp was planted (attacker with write access
        // to ~/.pi/agent/) or left by a prior crash. Unlinking it prevents the
        // EEXIST wedge on a future writeStateAtomic.
        try { unlinkSync(full); } catch { /* race: gone — skip */ }
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
   * disabled (unreachable from the provider path, which bails on
   * join() === null before reaching waitForLaunch).
   *
   * Capacity check is NOT performed here — the caller polls /usage itself
   * after claiming the token, so the decision uses the freshest server data.
   *
   * If `signal` aborts mid-wait, the poll loop stops, our waiter entry is
   * cancelled via `cancel(ourId)`, and the promise rejects with an
   * AbortError — so an aborted turn cannot wedge the local queue for
   * `staleWaiterMs` (5 min) or leak the token if it is later freed.
   */
  waitForLaunch(ourId: string, signal?: AbortSignal): Promise<() => void>;
  /**
   * Mark the account as deprioritized until `until` (epoch-ms). Shared across
   * all processes via the state file; idempotent (extends the deadline).
   */
  pauseUntil(until: number, reason?: string | null): void;
  /**
   * Clear deprioritization early (e.g. when /usage reports priority.low===false).
   * by default this REFUSES to clear a 429-origin pause (tagged
   * PAUSE_REASON_429) — /usage lags a 429 by 1-5s, so a stale tick reporting
   * priority.low===false must not wipe a sibling's freshly-written 429 pause.
   * clearPause refuses purely on the PAUSE_REASON_429 + pausedUntil > now
   * condition; it does NOT itself inspect /usage. The priority.low===true
   * confirmation happens at the caller (refreshUsage routes low===true to
   * pauseUntil, not clearPause, so the 429 pause survives until it naturally
   * elapses). Pass {force:true} to clear unconditionally (used by the
   * /umans-concurrency reset operator command to un-wedge a poisoned pause
   * without editing the JSON by hand).
   */
  clearPause(opts?: { force?: boolean }): void;
  /**
   * Re-stamp the launch token's `ts` to `now` while we still hold it, so the
   * 120s watchdog (reapStale) does not reap a long capacity poll (a pause-
   * bounded wait can legitimately exceed 120s). Returns `true` if
   * `state.token?.id === ourId` (we still hold it) and the stamp was advanced;
   * returns `false` if the token was reaped by a sibling's reapStale (id
   * mismatch) or is absent. A poller that holds the token across a
   * long /usage poll MUST call this on every iteration; if it returns false,
   * the poller must re-join the queue and wait its turn again (the sibling
   * that reaped + claimed is now sending — re-joining serializes us behind
   * it rather than racing a concurrent send that defeats the gate).
   */
  touchToken(ourId: string): boolean;
  /**
   * Add an in-flight entry for `ourId` (called by acquireSlot AFTER the
   * capacity check passes + BEFORE releasing the token, so the next head's
   * readState sees our entry before it can claim the token — the order is
   * load-bearing). Throws on lock timeout / disk error (fail-closed: a
   * missing entry would deflate the gate for siblings, re-arming the
   * within-machine burst race this exists to prevent; the caller's finally
   * cancels the waiter + token). PID + timestamp tagged so the same reapStale
   * watchdog that reaps stale waiters/tokens also reaps a crashed/aborted
   * entry (same 120s bound).
   */
  addInFlight(ourId: string): void;
  /**
   * Remove the in-flight entry for `ourId` (best-effort, called by the release
   * fn at message_end/turn_end/agent_end + by cancel). Wrapped in try/catch by
   * the caller (mirrors releaseToken's best-effort posture — the watchdog reaps
   * a stale entry).
   */
  removeInFlight(ourId: string): void;
  /** Snapshot for status-bar display + the local in-flight count for the gate. */
  snapshot(): { queued: number; tokenHeld: boolean; paused: boolean; pausedUntil: number; pausedReason: string | null; inflightCount: number };
  /** Remove our waiter entry if still present (best-effort, used on abort). Also splices the matching in-flight entry (an abort-after-launch path that calls cancel must not leak the in-flight entry for 120s). */
  cancel(ourId: string): void;
  /** Best-effort shutdown cleanup: clear this process's own waiter/token/in-flight entries (PID-scoped — does NOT wipe siblings' in-flight). Does NOT unlink the shared state file (siblings may still be queued). */
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
      addInFlight: () => {},
      removeInFlight: () => {},
      snapshot: () => ({ queued: 0, tokenHeld: false, paused: false, pausedUntil: 0, pausedReason: null, inflightCount: 0 }),
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

  // ensure the state file's parent dir exists before the first mutate.
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
  // track our waiter ids (set in join) alongside ourTokenId so
  // reset() can splice out a queued-but-not-launched waiter. Without this, a
  // process that join()ed but is still queued (ourTokenId === null) has reset()
  // as a no-op, leaking the waiter for staleWaiterMs (5 min) if the process
  // doesn't exit — blocking siblings behind a dead-PID entry.
  // a Set (not a single slot) so a second join() on one queue does NOT
  // overwrite the first id. Reachable from transformMessageImages (multi-image
  // handoff runs Promise.all → acquireSlot → join() per image). Probe (5×)
  // confirmed: two join() calls then reset() leaves exactly 1 waiter every
  // time with the single-slot design. The token single-slot (ourTokenId) is
  // safe — token release is closure-captured per waitForLaunch — but a Set is
  // used here because waiters accumulate.
  const ourWaiterIds: Set<string> = new Set();
  // ourWaiterIds (Set) is the sole source of truth. Deleted the dead
  // let ourWaiterId single-slot.
  // a per-instance AbortController that reset() aborts to stop any
  // in-flight waitForLaunch poll loop on the same queue instance. Without it,
  // reset() splices our waiter id from the file, but a concurrent poll loop's
  // mutate sees stillQueued===false and RE-INSERTS the id at the tail
  // every 50ms until the turn's AbortSignal aborts — leaking a
  // dead-PID waiter for staleWaiterMs (5 min) if the process exits before pi
  // aborts the signal. reset() aborts this controller so the poll stops + the
  // promise rejects (acquireSlot catches + returns undefined). A fresh
  // controller is created on each reset so subsequent waits are not pre-aborted.
  let resetAbort: AbortController = new AbortController();

  function mutate<T>(fn: (now: number, state: QueueState) => T): T {
    return withLock(cfg, lockFile, (now) => {
      const state = reapStale(readState(cfg.stateFile), cfg, now);
      const result = fn(now, state);
      // reap stale .tmp files inside the lock, using the injected cfg.now()
      // (was Date.now() inside writeStateAtomic — a frozen clock never
      // exercised the reaper). Best-effort cleanup of crashed writers'
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
        // compose the caller's signal + the per-instance resetAbort
        // controller via AbortSignal.any (Node 20.3+; declared in package.json
        // engines). Replaces the manual addEventListener + finally
        // removeEventListener bridge (listener-leak footgun + boilerplate).
        // A single composed signal covers both abort sources; if the caller
        // passed no signal, resetAbort.signal is the sole source.
        const composed = signal ? AbortSignal.any([signal, resetAbort.signal]) : resetAbort.signal;
        // If already aborted, cancel + reject immediately.
        if (composed.aborted) {
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
        composed.addEventListener("abort", onAbort, { once: true });
        const cleanComposedListener = () => composed.removeEventListener("abort", onAbort);
        const poll = () => {
          // Stop polling the moment the turn is aborted; otherwise the orphaned
          // promise would claim the token when freed and resolve a release fn
          // nobody holds (token leak).
          if (signal?.aborted || resetAbort.signal.aborted) return;
          // the FIRST poll() runs synchronously inside the Promise
          // executor, so a throw here rejects the promise and acquireSlot's
          // finally cleans up — safe. But every SUBSEQUENT poll is a
          // setTimeout callback; a throw from mutate() there (acquireLock
          // timeout after 2s, readState open/fstatSync/readSync EACCES/EIO/ELOOP,
          // writeStateAtomic
          // ENOSPC/EROFS) is not on any promise chain and surfaces as an
          // uncaughtException that terminates the Node process (and, with a
          // uncaughtException handler installed, leaves the promise forever
          // pending + the waiter leaked for staleWaiterMs = 5 min, stalling
          // siblings). Wrap the body so a throw on any re-entry clears the
          // timer, best-effort cancels our waiter entry, and rejects the
          // waitForLaunch promise — mirroring releaseSlot's release-resilience
          // pattern so the poll loop is as resilient to lock/disk
          // errors as the release loop already is.
          let got: boolean;
          try {
            got = mutate((now, state) => {
              // if our waiter entry was reaped by staleWaiterMs (5 min)
              // while we were still queued (e.g. a deep FIFO + slow models, or
              // a perpetually-full /usage), re-insert it at the tail
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
            cleanComposedListener();
            try { this.cancel(ourId); } catch { /* best-effort */ }
            reject(new Error(`concurrency-queue: poll failed: ${err instanceof Error ? err.message : err}`));
            return;
          }
          if (got) {
            cleanComposedListener();
            resolve(() => {
              // Release: remove our token and our waiter entry. A throw here
              // (lock timeout, EACCES, ENOSPC) propagates to releaseSlot in
              // index.ts, which wraps release() in try/catch so the
              // release continues and the watchdog reaps the stale entry.
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
        // a 429-sourced pause (tagged PAUSE_REASON_429) is clamped to
        // the tighter MAX_PAUSE_429_MS (2.5 min) ceiling so a misconfigured
        // UMANS_BASE_URL returning 429 forever cannot wedge the account for the
        // full 5h MAX_PAUSE_MS. A server priority.low pause (the other caller)
        // keeps the 5h ceiling (a real account-wide deprio). The 2.5 min cap is
        // still >> the 30s PRIORITY_BACKOFF_MS floor, so a real 429 with a short
        // Retry-After is honored.
        const ceilingMs = reason === PAUSE_REASON_429 ? MAX_PAUSE_429_MS : MAX_PAUSE_MS;
        const clamped = clampPauseUntil(until, now, ceilingMs);
        // a past `clamped` is still > 0 when no pause is active, so the
        // write below would proceed — display is safe (pausedUntil > now is
        // false), but the on-disk pausedReason lingers as stale data. Early-
        // return when the pause is already elapsed (nothing to write).
        if (clamped <= now) {
          return;
        }
        if (clamped > state.pausedUntil) {
          state.pausedUntil = clamped;
          // sanitize at the write boundary too (defense-in-
          // depth) so a poisoned reason never reaches the shared file,
          // regardless of caller. parsePriority already sanitizes the
          // server-sourced reason; this catches any future caller.
          // do NOT overwrite a sticky reason tag with a different
          // reason when extending. clearPause's sticky guard keys on the
          // reason STRING, so a /usage priority.low tick with a longer deadline
          // + a non-null reason (e.g. "Account deprioritized") would wipe a
          // freshly-written sticky tag, letting the next stale
          // priority.low===false tick clear the pause early — exactly the
          // race this guard exists to prevent. The sticky tag stays
          // authoritative; the longer deadline still extends pausedUntil.
          // When the sticky pause naturally elapses (pausedUntil <= now),
          // clearPause clears it normally.
          const newReason = sanitizeReason(reason);
          state.pausedReason = state.pausedReason && STICKY_PAUSE_REASONS.has(state.pausedReason)
            ? state.pausedReason
            : (newReason ?? state.pausedReason ?? null);
          state.pausedTs = now;
        }
      });
    },

    clearPause(opts?: { force?: boolean }): void {
      mutate((now, state) => {
        // Refuse to clear a sticky-origin pause (see STICKY_PAUSE_REASONS)
        // unless forced. /v1/usage lags a real suspension by 1-5s, so a stale
        // low===false tick would wipe a freshly-written sticky pause. The
        // writer's OWN refreshUsage clears it only after its own /usage catches
        // up to priority.low===true then back to false.
        if (!opts?.force && state.pausedReason && STICKY_PAUSE_REASONS.has(state.pausedReason) && state.pausedUntil > now) {
          return; // keep the sticky-origin pause
        }
        state.pausedUntil = 0;
        state.pausedReason = null;
        state.pausedTs = 0;
      });
    },

    touchToken(ourId: string): boolean {
      // the capacity-poll loop in acquireSlot holds the launch token
      // across a long /usage poll (a pause-bounded wait can legitimately
      // exceed 120s). reapStale reaps any token whose now -
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

    addInFlight(ourId: string): void {
      // Fail-closed (Adv5): a throw (lock timeout, EACCES, ENOSPC) propagates
      // to acquireSlot's finally, which cancels the waiter + token + aborts
      // the turn. Do NOT swallow — a missing entry deflates the gate for
      // siblings (localInFlight under-counts → max(localInFlight, ...) does
      // not count us → the next waiter's poll sees stale-low + launches →
      // the within-machine burst race this exists to prevent). The caller's
      // finally cancels; releaseToken failure is recoverable (watchdog reaps
      // a stale token), but addInFlight failure is not (no watchdog for a
      // missing entry — the gate just under-counts).
      mutate((now, _state) => {
        _state.inflight.push({ id: ourId, pid: ourPid(), ts: now });
      });
    },

    removeInFlight(ourId: string): void {
      // Best-effort (mirrors releaseToken): the watchdog reaps a stale entry
      // at 120s. Wrapped in try/catch by the caller (the release fn).
      mutate((_now, state) => {
        const idx = state.inflight.findIndex((e) => e.id === ourId);
        if (idx >= 0) state.inflight.splice(idx, 1);
      });
    },

    snapshot(): { queued: number; tokenHeld: boolean; paused: boolean; pausedUntil: number; pausedReason: string | null; inflightCount: number } {
      // snapshot reads without the lock (atomic rename prevents torn
      // reads; value may be one mutate stale — capacity-poll compensates via
      // /usage priority.low). Correct for the status bar; the brief staleness
      // is acceptable for an operator-facing view.
      // Read without mutating; still reap for an accurate view.
      const now = cfg.now();
      const state = reapStale(readState(cfg.stateFile), cfg, now);
      // reconcile holdsToken with the file. reapStale may have reaped
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
        // Post-reap count (dead-PID + >120s entries removed). The gate reads
        // this via CapacityInputs.localInFlight so isCapacityFree stays pure.
        inflightCount: state.inflight.length,
      };
    },

    cancel(ourId: string): void {
      mutate((_now, state) => {
        const idx = state.waiters.findIndex((w) => w.id === ourId);
        if (idx >= 0) state.waiters.splice(idx, 1);
        // also splice the matching in-flight entry. An abort-after-launch
        // path that calls cancel (the token-reaped re-join path, the abort
        // signal, acquireSlot's finally) must not leak the in-flight entry for
        // 120s — localInFlight would be inflated by 1, needlessly blocking one
        // slot until reapStale catches up.
        const ifidx = state.inflight.findIndex((e) => e.id === ourId);
        if (ifidx >= 0) state.inflight.splice(ifidx, 1);
        if (state.token && state.token.id === ourId) {
          state.token = null;
          holdsToken = false;
          ourTokenId = null;
        }
        ourWaiterIds.delete(ourId);
      });
    },

    reset(): void {
      // abort the per-instance resetAbort controller FIRST so any
      // in-flight waitForLaunch poll loop on this queue instance stops +
      // rejects (acquireSlot catches + returns undefined). Without this, reset()
      // splices our waiter id from the file, but a concurrent poll loop's mutate
      // sees stillQueued===false and RE-INSERTS the id at the tail
      // every 50ms until the turn's AbortSignal aborts — leaking
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
      // also splice out a queued-but-not-launched waiter
      // (ourTokenId === null, ourWaiterIds non-empty). Without this, a process that
      // join()ed but is still queued has reset() as a no-op, leaking the
      // waiter for staleWaiterMs (5 min) if the process doesn't exit —
      // blocking siblings behind a dead-PID entry.
      // splice EVERY id in ourWaiterIds (a Set, not a single slot) so a
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
          // PID-scoped in-flight cleanup. Splice only in-flight entries
          // whose pid === ourPid() (or whose id is in ourWaiterIds —
          // addInFlight reuses the waiter id). Do NOT wipe siblings' in-flight
          // entries — a global wipe would re-arm the within-machine burst race
          // for siblings (their launches would vanish from the local count).
          const myPid = ourPid();
          state.inflight = state.inflight.filter((e) =>
            e.pid !== myPid && !ourWaiterIds.has(e.id)
          );
        });
      } catch { /* ignore: lock unavailable on shutdown; watchdog will clean up */ }
      holdsToken = false;
      ourTokenId = null;
      ourWaiterIds.clear();
    },
  };
}
