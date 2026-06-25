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
 *   send until turn_end (the full response is done = the server has
 *   decremented concurrent_sessions). Only then is the token released and the
 *   next head allowed to poll. This makes launches race-free within a single
 *   machine: the next poll sees a /usage that already reflects the completed
 *   request. (Releasing earlier — at after_provider_response headers — is too
 *   early: the request is still in-flight on the server until the body streams,
 *   so the next poll would see stale capacity.) The watchdog (reapStale, 30s
 *   token cap) reclaims a crashed/aborted holder; the AbortSignal plumbed
 *   through waitForLaunch/acquireSlot cancels an aborted turn's waiter entry
 *   so it doesn't block siblings for staleWaiterMs (5 min).
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
import { mkdirSync, openSync, closeSync, unlinkSync, readFileSync, writeFileSync, renameSync, statSync, readdirSync } from "node:fs";
import { dirname, basename } from "node:path";
import { homedir } from "node:os";

export interface WaiterEntry {
  id: string;
  pid: number;
  ts: number;
}

export interface TokenState {
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
const DEFAULT_STALE_TOKEN_MS = 30_000;
const DEFAULT_STALE_WAITER_MS = 5 * 60_000;
const DEFAULT_LOCK_RETRY_MS = 5;
const DEFAULT_LOCK_TIMEOUT_MS = 2_000;

function defaultNow(): number { return Date.now(); }
function defaultPid(): number { return process.pid; }

/** Conservative default backoff when /v1/usage reports priority.low with a null boxed_until. */
const PRIORITY_BACKOFF_MS = 30_000;

/**
 * Maximum allowed pause duration (5h), matching the Umans 5h-account-pause
 * ceiling cited in design.md. pauseUntil() clamps any requested `until` to
 * now + MAX_PAUSE_MS, and reapStale() clears a pause whose pausedTs is older
 * than this — so a poisoned Retry-After header (e.g. 1e10) or a hand-edited
 * file cannot permanently wedge every local pi process sharing the file.
 */
export const MAX_PAUSE_MS = 5 * 60 * 60 * 1000; // 18,000,000 ms

/**
 * Clamp a candidate pause deadline to now + MAX_PAUSE_MS so a poisoned or
 * over-large Retry-After/boxed_until cannot wedge the queue for centuries.
 * Exported so the provider (index.ts) can clamp its Retry-After parse to the
 * same ceiling.
 */
export function clampPauseUntil(until: number, now: number = Date.now()): number {
  const ceiling = now + MAX_PAUSE_MS;
  return until > ceiling ? ceiling : until;
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
  }
  return { low, until, reason: p.reason ?? null };
}



/** Generate a unique waiter/token id. */
export function newId(): string {
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

/** Read the queue state, or return a fresh empty state if the file is absent/corrupt. */
export function readState(path: string, now: number): QueueState {
  try {
    const raw = readFileSync(path, "utf8");
    const parsed = JSON.parse(raw) as Partial<QueueState>;
    return {
      waiters: Array.isArray(parsed.waiters) ? parsed.waiters as WaiterEntry[] : [],
      token: parsed.token ?? null,
      pausedUntil: typeof parsed.pausedUntil === "number" ? parsed.pausedUntil : 0,
      pausedReason: parsed.pausedReason ?? null,
      pausedTs: typeof parsed.pausedTs === "number" ? parsed.pausedTs : 0,
    };
  } catch {
    return { waiters: [], token: null, pausedUntil: 0, pausedReason: null, pausedTs: 0 };
  }
}

/** True if a PID is not currently alive. Never throws. */
export function isPidDead(pid: number): boolean {
  if (!pid || pid <= 0) return true;
  try {
    // process.kill(pid, 0) throws if the process doesn't exist (or we lack
    // permission); in either case we treat the token as reclaimable.
    process.kill(pid, 0);
    return false;
  } catch {
    return true;
  }
}

/**
 * Reap a stale launch token and stale waiters from a state snapshot.
 * Also reaps a stale pause: if pausedTs is older than MAX_PAUSE_MS, the pause
 * is cleared (defense-in-depth, in case the clamp in pauseUntil is bypassed).
 * Returns the cleaned state; does not write to disk.
 */
export function reapStale(state: QueueState, cfg: Required<QueueConfig>, now: number): QueueState {
  const staleToken = state.token && (
    isPidDead(state.token.pid) || (now - state.token.ts) > cfg.staleTokenMs
  );
  const token = staleToken ? null : state.token;
  const waiters = state.waiters.filter((w) =>
    !isPidDead(w.pid) && (now - w.ts) <= cfg.staleWaiterMs
  );
  // Reap a pause older than MAX_PAUSE_MS. We check pausedTs (when the pause
  // was set) rather than pausedUntil so a clamp-bypassed poisoned value still
  // gets reaped once it has aged past the ceiling.
  let { pausedUntil, pausedReason, pausedTs } = state;
  if (pausedTs > 0 && (now - pausedTs) > MAX_PAUSE_MS) {
    pausedUntil = 0;
    pausedReason = null;
    pausedTs = 0;
  }
  return { ...state, token, waiters, pausedUntil, pausedReason, pausedTs };
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
 */
function acquireLock(lockFile: string, cfg: Required<QueueConfig>): () => void {
  const deadline = cfg.now() + cfg.lockTimeoutMs;
  let fd: number | undefined;
  while (fd === undefined) {
    try {
      fd = openSync(lockFile, "wx", 0o600);
    } catch (e: any) {
      if (e.code !== "EEXIST") throw e;
      // Lock is held by another process — or stale from a crash. If the
      // lockfile is older than the lock timeout, reclaim it.
      try {
        const st = statSync(lockFile);
        if (cfg.now() - st.mtimeMs > cfg.lockTimeoutMs) {
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
      const target = cfg.now() + cfg.lockRetryMs;
      while (cfg.now() < target) { /* busy spin */ }
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
  // ADV-1: best-effort reap of stale .tmp files left by a crashed writer (a
  // process killed between writeFileSync and renameSync). We unlink any
  // <path>.*.tmp older than STALE_TMP_MS (60s). A fresh .tmp just written by
  // another live process has a current mtime and is left alone. Mirrors the
  // lockfile mtime-recovery pattern. Best-effort: errors are swallowed.
  reapStaleTmps(path);
  const tmp = `${path}.${process.pid}.tmp`;
  writeFileSync(tmp, JSON.stringify(state), { encoding: "utf8", mode: 0o600 });
  renameSync(tmp, path); // atomic on POSIX & Windows; rename preserves the temp's 0600 mode
}

/** Max age of a .tmp file before it's considered a crashed writer's leftover. */
const STALE_TMP_MS = 60_000;

/** Best-effort unlink of stale <path>.*.tmp files older than STALE_TMP_MS. */
function reapStaleTmps(path: string): void {
  const dir = dirname(path);
  const prefix = `${basename(path)}.`;
  let entries: string[];
  try { entries = readdirSync(dir); } catch { return; /* dir missing/unreadable */ }
  const now = Date.now();
  for (const name of entries) {
    if (!name.startsWith(prefix) || !name.endsWith(".tmp")) continue;
    const full = `${dir}/${name}`;
    try {
      const st = statSync(full);
      if (now - st.mtimeMs > STALE_TMP_MS) unlinkSync(full);
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
   * the request completes (turn_end is the primary release path; agent_end is
   * the drain safety net). Returns undefined if the queue is disabled.
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
  /** Clear deprioritization early (e.g. when /usage reports priority.low===false). */
  clearPause(): void;
  /** Snapshot for status-bar display. */
  snapshot(): { queued: number; tokenHeld: boolean; paused: boolean; pausedUntil: number };
  /** Remove our waiter entry if still present (best-effort, used on abort). */
  cancel(ourId: string): void;
  /** Hard-reset: clear the state file (used on session shutdown by the last owner). */
  reset(): void;
  /** True if this process currently holds the launch token. */
  holdsToken(): boolean;
}

export function createConcurrencyQueue(opts?: QueueConfig & { disabled?: boolean }): ConcurrencyQueue {
  if (opts?.disabled) {
    return {
      join: () => null,
      waitForLaunch: () => Promise.resolve(() => {}),
      pauseUntil: () => {},
      clearPause: () => {},
      snapshot: () => ({ queued: 0, tokenHeld: false, paused: false, pausedUntil: 0 }),
      cancel: () => {},
      reset: () => {},
      holdsToken: () => false,
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

  // Track whether THIS process currently holds the token, so we only release
  // our own and so the status bar can show "launching".
  let holdsToken = false;
  let ourTokenId: string | null = null;

  function mutate<T>(fn: (now: number, state: QueueState) => T): T {
    return withLock(cfg, lockFile, (now) => {
      const state = reapStale(readState(cfg.stateFile, now), cfg, now);
      const result = fn(now, state);
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
      return id;
    },

    waitForLaunch(ourId: string, signal?: AbortSignal): Promise<() => void> {
      return new Promise((resolve, reject) => {
        // If the signal is already aborted, cancel + reject immediately.
        if (signal?.aborted) {
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
        const poll = () => {
          // Stop polling the moment the turn is aborted; otherwise the orphaned
          // promise would claim the token when freed and resolve a release fn
          // nobody holds (ADV-2 token leak).
          if (signal?.aborted) return;
          const got = mutate((now, state) => {
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
          if (got) {
            if (signal) signal.removeEventListener("abort", onAbort);
            resolve(() => {
              // Release: remove our token and our waiter entry.
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
        const clamped = clampPauseUntil(until, now);
        if (clamped > state.pausedUntil) {
          state.pausedUntil = clamped;
          state.pausedReason = reason ?? state.pausedReason ?? null;
          state.pausedTs = now;
        }
      });
    },

    clearPause(): void {
      mutate((_now, state) => {
        state.pausedUntil = 0;
        state.pausedReason = null;
        state.pausedTs = 0;
      });
    },

    snapshot(): { queued: number; tokenHeld: boolean; paused: boolean; pausedUntil: number } {
      // Read without mutating; still reap for an accurate view.
      const now = cfg.now();
      const state = reapStale(readState(cfg.stateFile, now), cfg, now);
      return {
        queued: state.waiters.length,
        tokenHeld: holdsToken,
        paused: state.pausedUntil > now,
        pausedUntil: state.pausedUntil,
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
      });
    },

    reset(): void {
      // Clear only OUR OWN entries: our waiter entry and the launch token if
      // we hold it. Do NOT unlink the shared state file or the lockfile — that
      // would race concurrent writers (breaking the O_EXCL invariant) and wipe
      // sibling pi processes' queue state. Leave both files for natural expiry
      // (the watchdog reaps stale token/waiter entries) and stale-lockfile
      // recovery. Matches the scope of `cancel`.
      try {
        mutate((_now, state) => {
          if (ourTokenId) {
            const idx = state.waiters.findIndex((w) => w.id === ourTokenId);
            if (idx >= 0) state.waiters.splice(idx, 1);
            if (state.token && state.token.id === ourTokenId) {
              state.token = null;
            }
          }
        });
      } catch { /* ignore: lock unavailable on shutdown; watchdog will clean up */ }
      holdsToken = false;
      ourTokenId = null;
    },

    holdsToken(): boolean {
      return holdsToken;
    },
  };
}
