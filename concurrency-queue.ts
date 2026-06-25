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
 *   free (and not deprioritized), then sends. The token stays held until
 *   after_provider_response (headers arrived = the server has registered the
 *   request as in-flight). Only then is the token released and the next head
 *   allowed to poll. This makes launches race-free within a single machine:
 *   the next poll sees a /usage that already reflects the in-flight request.
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
import { existsSync, mkdirSync, openSync, closeSync, unlinkSync, readFileSync, writeFileSync, renameSync, statSync } from "node:fs";
import { dirname } from "node:path";
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

// Re-export under the alias index.ts imports as.
export { parsePriority as parsePriorityShared };

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
    };
  } catch {
    return { waiters: [], token: null, pausedUntil: 0, pausedReason: null };
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
  return { ...state, token, waiters };
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
      fd = openSync(lockFile, "wx");
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
  const tmp = `${path}.${process.pid}.tmp`;
  writeFileSync(tmp, JSON.stringify(state), "utf8");
  renameSync(tmp, path); // atomic on POSIX & Windows
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
   * the request completes (after_provider_response, or turn_end/agent_end as
   * safety nets). Returns undefined if the queue is disabled.
   *
   * Capacity check is NOT performed here — the caller polls /usage itself
   * after claiming the token, so the decision uses the freshest server data.
   */
  waitForLaunch(ourId: string): Promise<() => void>;
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

    waitForLaunch(ourId: string): Promise<() => void> {
      return new Promise((resolve) => {
        const poll = () => {
          const got = mutate((now, state) => {
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
          setTimeout(poll, 50);
        };
        poll();
      });
    },

    pauseUntil(until: number, reason?: string | null): void {
      mutate((now, state) => {
        if (until > state.pausedUntil) {
          state.pausedUntil = until;
          state.pausedReason = reason ?? state.pausedReason ?? null;
        }
      });
    },

    clearPause(): void {
      mutate((_now, state) => {
        state.pausedUntil = 0;
        state.pausedReason = null;
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
      try { unlinkSync(cfg.stateFile); } catch { /* ignore */ }
      try { unlinkSync(lockFile); } catch { /* ignore */ }
      holdsToken = false;
      ourTokenId = null;
    },

    holdsToken(): boolean {
      return holdsToken;
    },
  };
}
