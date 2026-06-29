/**
 * Pure helpers + constants shared across the Umans provider entry points
 * (index.ts, web-search.ts). No closure state, no pi runtime dependency —
 * everything here is unit-testable via selfcheck.ts without spinning up pi.
 *
 * Import graph (acyclic): utils.ts imports only from concurrency-queue.ts +
 * package.json. It never imports from index.ts.
 *
 * Re-exports: extractBoxedUntil + isSuspendBody are re-exported from
 * concurrency-queue.ts so callers that already import utils.ts can reach them
 * without a second import line, and so the 403-suspend detection used by
 * raiseForUmansStatus + the /v1/usage 403 path share a single source.
 */
import {
  clampPauseUntil,
  PRIORITY_BACKOFF_MS,
  PAUSE_REASON_429,
  PAUSE_REASON_CAP_ABUSE,
  PAUSE_REASON_STRIKES,
  STICKY_PAUSE_REASONS,
  extractBoxedUntil,
  isSuspendBody,
  isCapacityFree,
  parsePriority,
  parseConcurrencyLimit,
  MAX_PAUSE_429_MS,
  SANITIZE_CTRL_RE,
  type ConcurrencyQueue,
  type PriorityState,
} from "./concurrency-queue.ts";
// Derive USER_AGENT from package.json so the version doesn't drift on
// release. ESM JSON import attribute `with { type: "json" }` is stable in
// Node 20.10+ (the engines floor is >=20.10.0, matching this requirement).
import pkg from "./package.json" with { type: "json" };

export type ReasoningInfo = {
  supported: boolean;
  can_disable: boolean;
  levels: string[];
  default_level: string;
};

export type ModelCapabilities = {
  max_completion_tokens?: number;
  recommended_max_tokens?: number;
  context_window?: number;
  supports_vision?: boolean | "via-handoff";
  supports_tools?: boolean;
  reasoning?: ReasoningInfo;
};

export type UmansModelInfo = {
  name: string;
  display_name?: string;
  description?: string;
  deprecation?: unknown;
  capabilities: ModelCapabilities;
};

/** User-Agent sent on every gateway request. Derived from package.json version. */
export const USER_AGENT = `pi-umans-provider/${pkg.version}`;

// Web search side-call tuning. See searchWeb / the umans_web_search tool.
export const SEARCH_TIMEOUT_MS = 30_000;
export const SEARCH_MAX_TOKENS = 2048;

// Re-export the 403-suspend detection helpers so callers importing utils.ts
// can reach them without a second import line. Both raiseForUmansStatus (below)
// and the /v1/usage 403 path in index.ts share these for suspend detection.
export { extractBoxedUntil, isSuspendBody };

/**
 * Pick the model used to run the side-call web search. Defaults to umans-flash
 * (fastest); falls back to the first tool-capable model if flash is absent.
 */
export function pickSearchModel(catalog: Record<string, UmansModelInfo>): string {
  const defaultId = "umans-flash";
  if (catalog[defaultId] && !catalog[defaultId].deprecation) return defaultId;
  for (const [id, info] of Object.entries(catalog)) {
    if (!info.deprecation && info.capabilities?.supports_tools) return id;
  }
  return defaultId;
}

/**
 * cap + sanitize a gateway error body before echoing it into a tool
 * result / thrown error message. Gateway error bodies are attacker-controlled
 * (a compromised/misconfigured gateway can push crafted text) + flow into the
 * model's context (prompt-injection surface). Cap to 80 chars (down from 200)
 * + strip non-printable / control / ANSI-escape chars so a crafted body cannot
 * mangle the message or inject control sequences. Mirrors sanitizeReason's
 * approach. Exported so selfcheck can unit-test the cap + strip.
 */
const ERROR_BODY_MAX_CHARS = 80;
// mirror sanitizeReason's strip — control + ESC + Unicode
// bidi/RTL overrides + zero-width/BOM chars that could spoof displayed text.
// uses the shared SANITIZE_CTRL_RE export from concurrency-queue.ts
// so the character class stays in sync with sanitizeReason without manual
// duplication. The 80-char cap stays local (sanitizeReason caps at 64).
export function sanitizeErrorBody(body: string): string {
  const cleaned = body.replace(SANITIZE_CTRL_RE, "").trim();
  return cleaned.length > ERROR_BODY_MAX_CHARS ? cleaned.slice(0, ERROR_BODY_MAX_CHARS) : cleaned;
}

/** Duck-typed Retry-After header lookup (fetch Headers .get OR a plain record). */
export function readRetryAfter(headers: Headers | Record<string, string> | undefined | null): string | undefined {
  if (!headers) return undefined;
  // fetch Response.headers (Headers) exposes .get; pi's after_provider_response
  // event.headers is a plain record indexed by lowercased header name.
  if (typeof (headers as Headers).get === "function") {
    return (headers as Headers).get("retry-after") ?? undefined;
  }
  const rec = headers as Record<string, string>;
  // Try exact + case-insensitive (Retry-After / retry-after / RETRY-AFTER).
  for (const k of Object.keys(rec)) {
    if (k.toLowerCase() === "retry-after") return rec[k];
  }
  return undefined;
}

/**
 * Shared 429 handler: parse Retry-After (strict integer form only), clamp to
 * MAX_PAUSE_429_MS, push the shared pause (PAUSE_REASON_429) so sibling pi
 * processes back off, and return the resolved `until` deadline so the caller
 * can notify. pauseUntil can throw on disk failure (EACCES/ENOSPC/EROFS)
 * — the lost pause is bounded by the 120s watchdog + the 5s refreshUsage poll,
 * so warn + swallow so the caller's turn is not aborted.
 *
 * Extracted from after_provider_response so the side-call sites
 * (analyzeImage, searchWeb) push the SAME shared pause when they receive a
 * 429. Each side-call consumes a real account concurrency slot, and
 * per Umans docs each concurrency 429 deprioritizes the whole account ~30
 * min — so a side-call 429 must back off siblings (and the main turn on its
 * next launch), not merely throw.
 *
 * Accepts either a fetch Response (res.headers.get) or a pi after_provider_
 * response event (event.headers[...]) — the Retry-After header lookup is
 * duck-typed so the same helper drives all three sites.
 */
export function handle429(
  source: { status: number; headers?: Headers | Record<string, string> | undefined | null },
  concurrencyQueue: { pauseUntil(until: number, reason?: string | null): void },
): number {
  // readRetryAfter calls headers.get("retry-after") which could
  // throw on a malformed pi event (a buggy/Headers-like object whose .get
  // throws). The surrounding handle429 only wrapped pauseUntil in try/catch,
  // so a throwing .get propagated out as an unhandled extension error. Wrap
  // the header parse + fall back to the PRIORITY_BACKOFF_MS deadline on throw,
  // mirroring the pauseUntil guard below.
  let retryAfter: string | undefined;
  try {
    retryAfter = readRetryAfter(source.headers);
  } catch (err) {
    console.warn("umans: readRetryAfter threw in 429 handler (falling back to default backoff):", err instanceof Error ? err.message : err);
    retryAfter = undefined;
  }
  let until = Date.now() + PRIORITY_BACKOFF_MS;
  if (retryAfter) {
    // RFC 7231 Retry-After is delta-seconds (a non-negative integer) or an
    // HTTP-date. We only accept the integer form: Number() accepts hex
    // ("0x10"=16), scientific notation ("1e10"=1e10), and other misparses
    // that can wedge the queue. Parse strictly and cap the resulting
    // deadline at now + MAX_PAUSE_429_MS via clampPauseUntil (a
    // 429-sourced pause is clamped tighter than the 5h ceiling so a
    // misconfigured UMANS_BASE_URL returning 429 forever cannot wedge the
    // account for hours; pauseUntil also enforces this ceiling).
    const trimmed = String(retryAfter).trim();
    if (/^\d+$/.test(trimmed)) {
      const secs = parseInt(trimmed, 10);
      if (secs > 0) until = clampPauseUntil(Date.now() + secs * 1000, Date.now(), MAX_PAUSE_429_MS);
    }
  }
  try {
    concurrencyQueue.pauseUntil(until, PAUSE_REASON_429);
  } catch (err) {
    console.warn("umans: pauseUntil threw in 429 handler (continuing):", err instanceof Error ? err.message : err);
  }
  return until;
}

/**
 * shared !res.ok handler for the side-call sites (analyzeImage,
 * searchWeb). Both sites duplicated the same 429-push + read-body + sanitize +
 * throw block. This helper runs the 429 push (a side-call 429
 * deprioritizes the whole account — the side-call consumes a real
 * concurrency slot — so push the shared pause so sibling pi processes + the
 * main turn on its next launch back off, do NOT merely throw), reads + caps +
 * sanitizes the gateway error body (attacker-controlled body must not
 * inject control sequences or mangle the tool result), then throws an Error
 * carrying HTTP status + the sanitized body. The Promise<never> return type
 * preserves control-flow narrowing (no dangling code after the call).
 *
 * Accepts the same duck-typed { status, headers } shape as handle429 (a fetch
 * Response) + the optional concurrencyQueue (omitted when the caller has no
 * queue — then only the body sanitize + throw run, matching the prior inline
 * behavior).
 */
export async function raiseForUmansStatus(
  res: { status: number; headers?: Headers | Record<string, string> | undefined | null; text(): Promise<string> },
  concurrencyQueue?: { pauseUntil(until: number, reason?: string | null): void },
): Promise<never> {
  if (res.status === 429 && concurrencyQueue) {
    handle429(res, concurrencyQueue);
  }
  // read text() ONCE into a local. A fetch Response body can only be
  // consumed once; reading it again returns "". The 403 suspend-body check
  // + the sanitizeErrorBody call below both operate on this same string.
  const txt = await res.text().catch(() => "");
  // 403 suspend-family body (account_suspended / cap_abuse / cap_suspended /
  // billing_error) → treat like a 429: extractBoxedUntil + push
  // PAUSE_REASON_CAP_ABUSE so siblings back off. isSuspendBody gates; a
  // non-suspend 403 (auth error, proxy HTML) does NOT pause — the turn still
  // throws, but the gate is not poisoned for siblings. See extractBoxedUntil
  // for the tolerant-extraction + past-as-absent rationale.
  if (res.status === 403 && concurrencyQueue && isSuspendBody(txt)) {
    const now = Date.now();
    const extracted = extractBoxedUntil(txt);
    const until = extracted && extracted > now ? extracted : now + PRIORITY_BACKOFF_MS;
    try {
      concurrencyQueue.pauseUntil(until, PAUSE_REASON_CAP_ABUSE);
    } catch (err) {
      console.warn("umans: pauseUntil threw in 403 handler (continuing):", err instanceof Error ? err.message : err);
    }
  }
  // cap + sanitize the gateway error body before echoing it (cap 80,
  // strip non-printable / ANSI-escape) so a crafted body cannot inject
  // control sequences or mangle the tool result.
  const safe = sanitizeErrorBody(txt);
  throw new Error(`HTTP ${res.status}${safe ? `: ${safe}` : ""}`);
}

// ============================================================================
// Concurrency refresh / fetch / capacity-poll machinery
//
// Shared by index.ts (the Umans provider factory) + web-search.ts (the
// standalone web-search tool factory). Both factories create their OWN
// ConcurrencyQueue instance (per the file-split design: two queue instances
// in the same process coordinate through the shared state file), but the
// refresh + fetch + strike-pause logic that reads /v1/usage + /v1/usage/history
// + writes pauses is ONE copy here. A prior split duplicated this machinery
// byte-for-byte; the two copies diverged on the strike-pause gate (the
// deprioritized check was added to index.ts's copy but not web-search.ts's),
// re-introducing a false-positive 30-min pause. Keeping one copy makes that
// divergence structurally impossible.
//
// Each factory builds a ConcurrencyRuntime carrying its own queue instance +
// mutable capacity state, then calls these functions with that runtime. The
// factory keeps its own acquireSlot/releaseSlot closures (per-instance, capture
// the queue) + the session_start/session_shutdown wiring + the status bar
// (index.ts only) + the provider/tool registration.
// ============================================================================

// max time the head-waiter capacity poll will wait for a free slot before
// failing open (launching anyway). Bounds the queue against a hostile /
// misbehaving /usage that always reports full.
export const CAPACITY_POLL_TIMEOUT_MS = 60_000;

// 429 strike counter: the Umans account is paused for 5h after >20 concurrency
// 429s in 24h. The queue polls /v1/usage/history every STRIKE_POLL_INTERVAL_MS,
// sums rate_limit_concurrency buckets since the last cap_suspended (the server
// resets the counter on reactivation), and defensively pauses when the count
// reaches the dynamic threshold — better to self-pause briefly than risk the
// 5h ban. The threshold is dynamic: STRIKE_SERVER_LIMIT (20) minus the max
// in-flight (the concurrency limit, e.g. 4), so a burst of in-flight requests
// between the poll + the server's counter update can't tip us over before we
// react. With limit=4 → threshold=16.
const STRIKE_POLL_INTERVAL_MS = 5 * 60 * 1000; // 5 min
const STRIKE_WINDOW_MS = 24 * 60 * 60 * 1000; // 24h (rolling, matches the server)
const STRIKE_SERVER_LIMIT = 20; // server triggers 5h pause at >20 strikes/24h
const STRIKE_PAUSE_MS = 30 * 60 * 1000; // 30 min self-pause to let strikes age out

// /usage poll-interval backoff tuning (used by nextPollInterval + acquireSlotCore).
export const POLL_INTERVAL_BASE_MS = 300;
const POLL_INTERVAL_CAP_MS = 2_000;
const POLL_INTERVAL_GROWTH = 1.5;

/** Default gateway base URL when UMANS_BASE_URL is unset. */
const DEFAULT_BASE_URL = "https://api.code.umans.ai";

/**
 * Resolve the gateway base URL from UMANS_BASE_URL (trimmed, trailing slash
 * normalized) or fall back to DEFAULT_BASE_URL. Both factories call this once at
 * startup + assign to rt.baseUrl, so the literal + the normalization live in
 * one place.
 */
export function resolveBaseUrl(): string {
  return process.env.UMANS_BASE_URL?.trim().replace(/\/$/, "") || DEFAULT_BASE_URL;
}

const API_KEY_ENV = "UMANS_API_KEY";
const CONCURRENCY_LIMIT_ENV = "UMANS_CONCURRENCY_LIMIT";

/**
 * The /v1/usage response shape (the fields the refresh machinery reads).
 * afterRefreshUsage (the status-bar hook on ConcurrencyRuntime) receives it,
 * so the type stays in the same module as the runtime.
 */
type UsageData = {
  limits?: { concurrency?: { limit?: number; hard_cap?: number }; requests?: { limit?: number } };
  usage?: { requests_in_window?: number; concurrent_sessions?: number; priority?: unknown };
};

/**
 * Per-factory runtime carried by the shared refresh/fetch functions. The
 * factory owns this object (one instance per factory) + the shared functions
 * read + write its mutable fields directly so there is one copy of the
 * refresh logic. The factory keeps its own acquireSlot/releaseSlot closures
 * + status-bar-only locals (index.ts: currentConcurrency/requestLimit/
 * requestsUsed) outside this object.
 */
export interface ConcurrencyRuntime {
  baseUrl: string;
  queue: ConcurrencyQueue;
  // multiplier cached once at factory startup from readSettings(). The shared
  // concurrencyLimit scales the server soft cap by this, clamped to hardCap.
  multiplier: number;
  // mutable capacity state — the shared refresh/fetch functions read + write
  // these fields directly. `deprioritized` is the strike-pause gate: the
  // shared refreshStrikes only pushes a strike pause when the account is
  // deprioritized (priority.low === true), so a stale /history count on a
  // clear account cannot push a false-positive pause.
  hardCap: number | undefined;
  guaranteedConcurrency: number | undefined;
  strikes24h: number | undefined;
  deprioritized: boolean;
  priorityUntil: number | undefined;
  // timer state for the periodic refreshUsage (5s) + strike poll (5min) loops.
  refreshTimer: ReturnType<typeof setTimeout> | undefined;
  strikeTimer: ReturnType<typeof setTimeout> | undefined;
  refreshStopped: boolean;
  // status-bar hook: index.ts re-renders the status bar + updates its
  // status-only locals (currentConcurrency/requestLimit/requestsUsed) from the
  // raw usage data here. web-search.ts has no status bar + leaves this
  // undefined. Called once per refreshUsage with the fetched (non-null) data.
  afterRefreshUsage?(data: UsageData): void;
}

/**
 * Pure decision extracted from acquireSlot's capacity-poll loop so the branch
 * logic (free-first-poll, poll-then-free, timeout-fail-open, timeout-but-
 * paused-keeps-waiting, mid-poll-abort) is unit-testable without the full pi
 * runtime.
 *
 * - `launch`: capacity is free — proceed with the send.
 * - `abort`: the turn's AbortSignal fired mid-poll — cancel + reject.
 * - `failOpen`: the poll cap elapsed AND no known pause is active — launch
 *   ungated so a wedged /usage doesn't block forever. A known active pause
 *   keeps the gate waiting (bounded by the pause deadline + the 120s
 *   watchdog) — fail-open for a POSITIVE deprio signal would launch into a
 *   still-deprioritized account.
 * - `wait`: keep polling (300ms + jitter).
 */
type LaunchDecision = "launch" | "wait" | "failOpen" | "abort";
export function decideLaunch(opts: {
  isFree: boolean;
  elapsedMs: number;
  queuePaused: boolean;
  signalAborted: boolean;
}): LaunchDecision {
  // signalAborted takes precedence over isFree. When the turn's AbortSignal
  // fires mid-poll AND /usage is unreachable (fetchUsage returns null →
  // isCapacityFree(null) returns {free:true} via the trust-headroom stance),
  // the prior isFree-first ordering would return "launch" + hold the token
  // until a safety net fires. For a Ctrl-C'd turn that never sends, the token
  // leaks up to the 120s watchdog. Checking signalAborted first routes through
  // the abort branch immediately at the abort site.
  if (opts.signalAborted) return "abort";
  if (opts.isFree) return "launch";
  if (opts.elapsedMs >= CAPACITY_POLL_TIMEOUT_MS && !opts.queuePaused) return "failOpen";
  return "wait";
}

/**
 * Pure helper for the /usage poll interval under steady-full backoff. With N
 * local pi processes each running their own head waiter, a saturated queue
 * drives N×3.3 RPS to /usage continuously. Exponential backoff on the poll
 * interval when capacity is steadily full reduces RPS from ~3.3/s to ~0.5/s
 * during a sustained pause.
 *
 * - "wait": grow by GROWTH (1.5×), capped at CAP (2000ms). The ±100ms jitter
 *   is applied by the caller, not here (keeps this pure + deterministic).
 * - "launch" / "failOpen" / "abort": reset to BASE.
 */
export function nextPollInterval(currentMs: number, decision: LaunchDecision, opts?: { base?: number; cap?: number; growth?: number }): number {
  const base = opts?.base ?? POLL_INTERVAL_BASE_MS;
  const cap = opts?.cap ?? POLL_INTERVAL_CAP_MS;
  const growth = opts?.growth ?? POLL_INTERVAL_GROWTH;
  if (decision === "wait") {
    const next = Math.round(currentMs * growth);
    return Math.min(next > 0 ? next : base, cap);
  }
  return base;
}

/**
 * Resolve the Umans API key from the env var (preferred) or the pi
 * modelRegistry (ctx.modelRegistry.getApiKeyForProvider). The ctx param is
 * typed loosely (any) because the pi ExtensionAPI ctx shape is not narrowed
 * for this lookup; the key is never logged or leaked. Identical in both
 * factories pre-extraction; one copy here.
 */
export async function resolveApiKey(ctx?: any): Promise<string | undefined> {
  const envKey = process.env[API_KEY_ENV]?.trim();
  if (envKey) return envKey;
  try {
    return await ctx?.modelRegistry?.getApiKeyForProvider("umans");
  } catch {
    return undefined;
  }
}

/**
 * The effective concurrency limit for the runtime. The UMANS_CONCURRENCY_LIMIT
 * env var is the absolute override (testing knob): when set it wins outright,
 * bypassing the multiplier. Otherwise scale the server's soft cap
 * (guaranteedConcurrency) by the cached multiplier, then clamp to hardCap so a
 * high multiplier cannot push past the server's burst ceiling. floor() keeps
 * the slot count integral (0.5 of 4 = 2). Returns undefined before /v1/usage
 * populates guaranteedConcurrency.
 */
export function concurrencyLimit(rt: ConcurrencyRuntime): number | undefined {
  if (process.env[CONCURRENCY_LIMIT_ENV] !== undefined) {
    return parseConcurrencyLimit(process.env[CONCURRENCY_LIMIT_ENV], rt.guaranteedConcurrency);
  }
  const serverLimit = rt.guaranteedConcurrency;
  if (serverLimit === undefined) return undefined; // /v1/usage not yet populated
  // Floor the scaled value to at least 1 so a sub-1 multiplier (e.g. 0.1 with
  // limit 4 → floor(0.4)=0) gets at least 1 slot instead of wedging the gate.
  // A limit of 0 means "never free" → isCapacityFree reports not-free on every
  // poll → the head waiter polls until the 60s fail-open, a hostile UX for a
  // user who asked for "conservative". The hard_cap clamp still applies above.
  const scaled = Math.max(1, Math.floor(serverLimit * rt.multiplier));
  return rt.hardCap !== undefined ? Math.min(scaled, rt.hardCap) : scaled;
}

/**
 * Fetch /v1/usage. A 403 FROM /v1/usage is a POSITIVE suspension signal, not
 * absence — the server returns 403 for everything once the account is
 * suspended. Return a synthetic priority.low + reason=cap_abuse snapshot so
 * the cap_abuse branch in isCapacityFree fires + pushes the real pause. A
 * non-suspend 403 (auth error on /usage) keeps fail-open (return null).
 * Returns null on any other failure (caller leaves cached values).
 */
export async function fetchUsage(rt: ConcurrencyRuntime, apiKey: string, timeoutMs: number, parentSignal?: AbortSignal): Promise<UsageData | null> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  const composed = parentSignal ? AbortSignal.any([parentSignal, ctrl.signal]) : ctrl.signal;
  try {
    const res = await fetch(`${rt.baseUrl}/v1/usage`, {
      signal: composed,
      headers: {
        Authorization: `Bearer ${apiKey}`,
        Accept: "application/json",
        "User-Agent": USER_AGENT,
      },
    });
    if (!res.ok) {
      if (res.status === 403) {
        const txt = await res.text().catch(() => "");
        if (isSuspendBody(txt)) {
          const now = Date.now();
          const extracted = extractBoxedUntil(txt);
          const boxedUntilMs = extracted && extracted > now ? extracted : now + PRIORITY_BACKOFF_MS;
          return {
            usage: {
              concurrent_sessions: 0,
              priority: { low: true, boxed_until: new Date(boxedUntilMs).toISOString(), reason: "cap_abuse" },
            },
          };
        }
      }
      return null;
    }
    return await res.json() as UsageData;
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Lightweight one-shot /v1/usage fetch used by the head waiter (acquireSlot)
 * to decide whether to launch. Shorter timeout (3s vs 5s) so a slow /usage
 * doesn't stall the head-waiter poll; reads only the capacity-decision fields.
 * Returns null on any failure (caller retries).
 */
export async function fetchUsageSnapshot(rt: ConcurrencyRuntime, apiKey: string, parentSignal?: AbortSignal): Promise<{
  concurrentSessions: number | undefined;
  limit: number | undefined;
  hardCap: number | undefined;
  priority: PriorityState;
} | null> {
  const data = await fetchUsage(rt, apiKey, 3000, parentSignal);
  if (!data) return null;
  return {
    concurrentSessions: data.usage?.concurrent_sessions,
    limit: data.limits?.concurrency?.limit ?? undefined,
    hardCap: data.limits?.concurrency?.hard_cap ?? undefined,
    priority: parsePriority(data.usage?.priority),
  };
}

/**
 * Fetch the count of concurrency 429s since the last cap_suspended (5h pause)
 * from /v1/usage/history. The server pauses the account for 5h after >20
 * concurrency 429s in 24h AND resets the counter on reactivation (a
 * reactivation revokes + rotates API keys), so strikes before the most recent
 * cap_suspended bucket are excluded — matching the dashboard's behavior so the
 * count stays accurate after a reactivation. Returns a typed result:
 * suspend-403 (server returns 403 for /history too once suspended — caller
 * clears the cached count) vs transient failure (caller preserves the cached
 * count so a blip does not skip the self-pause).
 */
export async function fetch429Strikes(rt: ConcurrencyRuntime, apiKey: string): Promise<{ count: number | null; suspended: boolean }> {
  const now = Date.now();
  const from = new Date(now - STRIKE_WINDOW_MS).toISOString();
  const to = new Date(now).toISOString();
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 5000);
  try {
    const res = await fetch(
      `${rt.baseUrl}/v1/usage/history?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}&granularity=hour`,
      {
        signal: ctrl.signal,
        headers: {
          Authorization: `Bearer ${apiKey}`,
          Accept: "application/json",
          "User-Agent": USER_AGENT,
        },
      },
    );
    if (!res.ok) {
      // A 403 FROM /v1/usage/history is a POSITIVE suspension signal (the
      // server returns 403 for everything once suspended). Distinguish it from
      // a transient !res.ok so the caller clears the cache only on a real
      // suspension + preserves the cached count on a transient failure.
      if (res.status === 403) {
        const txt = await res.text().catch(() => "");
        if (isSuspendBody(txt)) return { count: null, suspended: true };
      }
      return { count: null, suspended: false };
    }
    const data = await res.json() as { buckets?: Array<{ bucket?: string; error_category?: string | null; requests?: number }> };
    if (!Array.isArray(data.buckets)) return { count: null, suspended: false };
    let lastPauseTs = 0;
    for (const b of data.buckets) {
      if (b.error_category === "cap_suspended" && b.bucket) {
        const ts = new Date(b.bucket).getTime();
        if (ts > lastPauseTs) lastPauseTs = ts;
      }
    }
    const strikes = data.buckets
      .filter((b) => b.error_category === "rate_limit_concurrency")
      .filter((b) => {
        // Exclude strikes before the most recent cap_suspended (counter reset).
        if (lastPauseTs === 0) return true;
        const ts = b.bucket ? new Date(b.bucket).getTime() : 0;
        return ts > lastPauseTs;
      })
      .reduce((sum, b) => sum + (typeof b.requests === "number" ? b.requests : 0), 0);
    return { count: strikes, suspended: false };
  } catch {
    return { count: null, suspended: false };
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Refresh the cached 24h 429 strike count + defensively pause if the count
 * reaches the dynamic threshold. The strike-pause push is GATED on
 * `deprioritized`: /v1/usage/history is eventually-consistent (lags
 * real-time), so a poll can return a stale count at/above the threshold that
 * the server no longer reports. /v1/usage is the capacity authority — its
 * priority.low flag (cached as `deprioritized` by refreshUsage) is the
 * server's own rate-limit signal. When the account is not deprioritized, a
 * stale history count must NOT push a strike pause (otherwise a clear account
 * gets a spurious 30-min pause). The strikes24h cache is still updated for
 * status-bar observability; only the pause push is gated. The cap_abuse
 * suspension path returns early above + is unaffected.
 *
 * This gate is the structural fix for the false-positive strike pause: one
 * copy of refreshStrikes, shared by both factories, so the gate cannot diverge.
 */
export async function refreshStrikes(rt: ConcurrencyRuntime, apiKey: string): Promise<void> {
  const result = await fetch429Strikes(rt, apiKey);
  if (result.suspended) {
    // /v1/usage/history may also return 403 during a suspension. Clear the
    // cached strikes value so the status bar does not show a stale "Strikes
    // 19/20" for the full 5h suspension. The threshold check below is skipped
    // (extend-never-shorten holds regardless because a cap_abuse pause is
    // sticky + longer than any strike pause).
    rt.strikes24h = undefined;
    return;
  }
  // Transient failure (network timeout, 5xx, JSON parse): preserve the cached
  // count so a single blip does not lose the strike count + skip the defensive
  // self-pause right when it matters most.
  if (result.count === null) return;
  const count = result.count;
  rt.strikes24h = count;
  const maxInFlight = concurrencyLimit(rt) ?? rt.guaranteedConcurrency ?? 0;
  // Skip the threshold check at startup before guaranteedConcurrency is
  // populated. With maxInFlight=0 the threshold would be 20 (the server limit)
  // — pausing at the server's own trigger point is too late + risks a
  // false-positive pause if the API transiently returns stale data. Wait for
  // the first refreshUsage to populate the real limit.
  if (maxInFlight <= 0) return;
  const strikeThreshold = Math.max(0, STRIKE_SERVER_LIMIT - maxInFlight);
  if (rt.deprioritized && count >= strikeThreshold) {
    const snap = rt.queue.snapshot();
    const now = Date.now();
    const strikeUntil = now + STRIKE_PAUSE_MS;
    // Only push if no active pause OR the active pause ends sooner than our
    // strike pause (extend, never shorten). A priority.low pause from the
    // server (boxed_until) or a 429 pause is left untouched if it's longer.
    if (!snap.paused || snap.pausedUntil < strikeUntil) {
      try {
        rt.queue.pauseUntil(strikeUntil, PAUSE_REASON_STRIKES);
      } catch (err) {
        console.warn("umans: pauseUntil threw in refreshStrikes (continuing):", err instanceof Error ? err.message : err);
      }
    }
  }
}

/**
 * Refresh guaranteedConcurrency/hardCap/deprioritized/priorityUntil from
 * /v1/usage. The synthetic cap_abuse return (fetchUsage on a /v1/usage 403
 * with a suspend body) carries no `limits` field — preserve the cached
 * guaranteedConcurrency/hardCap in that case (only update when data.limits is
 * present). Clears a non-sticky pause pushed by a PREVIOUS priority.low tick
 * when the account is no longer deprioritized (a stale /usage tick reporting
 * low===false must not wipe a freshly-written sticky 429/cap_abuse/strike
 * pause). Calls afterRefreshUsage (index.ts: status-bar re-render + status-
 * only locals) with the raw data.
 */
export async function refreshUsage(rt: ConcurrencyRuntime, apiKey: string): Promise<void> {
  const data = await fetchUsage(rt, apiKey, 5000);
  if (!data) return; // leave cached values; status bar will show "?"
  if (data.limits) {
    // null ?? undefined normalizes unlimited (null) limits so the display
    // guards hide them instead of rendering "x/null".
    rt.guaranteedConcurrency = data.limits.concurrency?.limit ?? undefined;
    rt.hardCap = data.limits.concurrency?.hard_cap ?? undefined;
  }
  // Track the deprioritization state for the status bar (DEPRIO banner) + the
  // strike-pause gate (refreshStrikes reads rt.deprioritized). priority.low is
  // a STATUS signal, not a stop condition: the gate lowers the cap by 1
  // (isCapacityFree) to reduce race risk, but does NOT push a full pause.
  const priority = parsePriority(data.usage?.priority);
  rt.deprioritized = priority.low;
  rt.priorityUntil = priority.until;
  if (!priority.low) {
    const snap = rt.queue.snapshot();
    if (snap.paused && !(snap.pausedReason && STICKY_PAUSE_REASONS.has(snap.pausedReason))) {
      rt.queue.clearPause();
    }
  }
  rt.afterRefreshUsage?.(data);
}

/** Stop the periodic refreshUsage + strike-poll loops + clear their timers. */
export function stopRefreshLoop(rt: ConcurrencyRuntime): void {
  rt.refreshStopped = true;
  if (rt.refreshTimer) {
    clearTimeout(rt.refreshTimer);
    rt.refreshTimer = undefined;
  }
  if (rt.strikeTimer) {
    clearTimeout(rt.strikeTimer);
    rt.strikeTimer = undefined;
  }
}

/**
 * (Re)start the periodic refreshUsage loop (5s) + the immediate strike poll.
 * The strike poll fires immediately (no delay) so the strike count is known at
 * startup, not 5 min in. Called from session_start (and model_select in
 * index.ts when switching to the Umans provider).
 */
export function restartRefreshLoop(rt: ConcurrencyRuntime, apiKey: string): void {
  stopRefreshLoop(rt);
  rt.refreshStopped = false;
  scheduleRefresh(rt, apiKey);
  scheduleStrikePoll(rt, apiKey, true);
}

/** Schedule the next periodic refreshUsage (5s). Re-schedules itself. */
function scheduleRefresh(rt: ConcurrencyRuntime, apiKey: string): void {
  if (rt.refreshStopped || !apiKey) return;
  rt.refreshTimer = setTimeout(async () => {
    await refreshUsage(rt, apiKey);
    scheduleRefresh(rt, apiKey);
  }, 5000);
}

/**
 * Schedule the next strike-count poll (STRIKE_POLL_INTERVAL_MS, 5min). The
 * first poll fires immediately (immediate=true) so the strike count is known
 * at startup. Re-schedules itself.
 */
export function scheduleStrikePoll(rt: ConcurrencyRuntime, apiKey: string, immediate = false): void {
  if (rt.refreshStopped || !apiKey) return;
  rt.strikeTimer = setTimeout(async () => {
    await refreshStrikes(rt, apiKey);
    scheduleStrikePoll(rt, apiKey);
  }, immediate ? 0 : STRIKE_POLL_INTERVAL_MS);
}

/**
 * Release a concurrency slot returned by acquireSlot. Best-effort: the release
 * closure + the in-flight/waiter removal each swallow throws (lock timeout,
 * EACCES/ENOSPC) so a repeatedly-throwing slot does not abort the caller's
 * turn; the 120s watchdog reaps the stale token/waiter regardless. index.ts
 * wraps this in its own releaseSlot (adding mainTurnRelease tracking + a
 * status-bar re-render); web-search.ts calls this directly.
 */
export function releaseSlotCore(release: (() => void) | undefined): void {
  if (!release) return;
  try {
    release();
  } catch (err) {
    console.warn("umans: concurrency release threw (release continues):", err instanceof Error ? err.message : err);
  }
}

/**
 * Acquire a concurrency slot for an outbound request (main turn or a
 * side-call). Joins the cross-process FIFO, waits until head + claims the
 * launch token, polls /v1/usage until the server reports a free slot, then
 * returns a release fn that drops the token + waiter entry. Returns undefined
 * when the queue is disabled or the turn's AbortSignal fires mid-poll (clean
 * cancellation, not a throw).
 *
 * This function BLOCKS until the slot is acquired — it is NOT a fast
 * non-blocking check. The wait is the FIFO queue wait (possibly minutes under
 * contention) + the /v1/usage capacity poll (up to CAPACITY_POLL_TIMEOUT_MS =
 * 60s fail-open, or longer while a known pause is active).
 *
 * The `apiKey` is used for the head-waiter poll. All captured state lives on
 * `rt` (the ConcurrencyRuntime), so this helper has no per-factory closure state
 * — both factories call it with their own rt. index.ts wraps the returned
 * release fn in its own releaseSlot (mainTurnRelease tracking + status-bar
 * re-render); web-search.ts uses it directly.
 */
export async function acquireSlotCore(rt: ConcurrencyRuntime, apiKey: string, signal?: AbortSignal): Promise<(() => void) | undefined> {
  const queue = rt.queue;
  const initialId = queue.join();
  if (!initialId) return undefined; // queue disabled
  let ourId: string = initialId;
  let released = false;
  const MAX_TOKEN_REJOINS = 3;
  let releaseToken: () => void = () => {};
  try {
    tokenAcquire: for (let rejoins = 0; rejoins <= MAX_TOKEN_REJOINS; rejoins++) {
      let releaseTokenThisIter: () => void;
      try {
        releaseTokenThisIter = await queue.waitForLaunch(ourId, signal);
      } catch (err) {
        // waitForLaunch rejects with "waitForLaunch aborted" when the signal is
        // already aborted (or aborts mid-wait). Return undefined (matching the
        // disabled-mode shape) so Ctrl-C surfaces as a clean cancellation, not an
        // uncaught extension error. waitForLaunch already cancelled our waiter.
        if (signal?.aborted) {
          released = true;
          return undefined;
        }
        throw err; // non-abort throw (lock timeout, EACCES) — propagate
      }
      releaseToken = releaseTokenThisIter;
      const limit = concurrencyLimit(rt);
      // Read queuePaused ONCE per poll iteration into a local const + pass the
      // same value to capacityFree + decideLaunch. Two unlocked snapshot reads
      // straddling an await could let capacityFree see queuePaused:true then
      // decideLaunch see queuePaused:false + elapsedMs >= 60s → failOpen into a
      // pause. Reading once makes the fail-open-during-pause guard structural.
      const capacityFree = async (queuePaused: boolean): Promise<boolean> => {
        const snap = await fetchUsageSnapshot(rt, apiKey, signal);
        const qSnap = queue.snapshot();
        const decision = isCapacityFree(snap, {
          limit,
          queuePaused,
          localInFlight: qSnap.inflightCount,
        });
        if (decision.repause) {
          // Write-amplification guard: skip the pauseUntil call when the
          // active pause already covers the requested deadline + reason. The
          // poll loop calls capacityFree every ~300ms; over a long suspension
          // that would be thousands of no-op lock acquisitions + file writes
          // (extend-never-shorten means the deadline does not move).
          const alreadyCovered = queuePaused &&
            qSnap.pausedUntil >= decision.repause.until &&
            qSnap.pausedReason === decision.repause.reason;
          if (!alreadyCovered) {
            // pauseUntil can throw on disk failure (EACCES, ENOSPC, EROFS). The
            // pause is a best-effort coordination signal (the server's
            // priority.low + the 120s watchdog bound it); warn + swallow so a
            // disk error does not abort a turn that already waited its FIFO
            // place.
            try {
              queue.pauseUntil(decision.repause.until, decision.repause.reason ?? undefined);
            } catch (err) {
              console.warn("umans: pauseUntil threw in capacityFree (continuing):", err instanceof Error ? err.message : err);
            }
          }
        }
        return decision.free;
      };
      const pollStart = Date.now();
      // Exponential backoff on the poll interval when capacity is steadily full
      // (300ms → 2000ms by 1.5×; reset to 300ms on launch/failOpen). The ±100ms
      // jitter breaks phase-locking across machines polling /usage on the same
      // tick. The branch logic lives in decideLaunch (pure, unit-tested).
      let pollIntervalMs = POLL_INTERVAL_BASE_MS;
      for (;;) {
        // Re-stamp our token's ts each iteration so the 120s watchdog does not
        // reap a legitimately long capacity poll. If the token was already
        // reaped by a sibling's reapStale (id mismatch or absent), touchToken
        // returns false — bail out of this poll, cancel our (now-stale) waiter,
        // re-join the queue, + wait our turn again. Guarded re-entry: bail
        // after MAX_TOKEN_REJOINS so a pathological state cannot wedge us here
        // forever (fall back to fail-open, bounded by the watchdog + hard_cap
        // headroom).
        if (!queue.touchToken(ourId)) {
          try { queue.cancel(ourId); } catch { /* best-effort */ }
          if (rejoins >= MAX_TOKEN_REJOINS) {
            // Pathological state: reaped + re-joined too many times. Fail open
            // rather than loop forever (bounded by the watchdog + the hard_cap
            // headroom). Clear the stale releaseToken closure before break —
            // it still points at the prior iteration's closure (a no-op since
            // the token was reaped, but confusing: the returned closure would
            // pretend to release a token we no longer hold). Point it at an
            // explicit no-op so fail-open proceeds without holding (or pretending
            // to release) a token.
            releaseToken = () => {};
            break;
          }
          ourId = queue.join()!;
          continue tokenAcquire;
        }
        const queuePaused = queue.snapshot().paused;
        const isFree = await capacityFree(queuePaused);
        const decision = decideLaunch({
          isFree,
          elapsedMs: Date.now() - pollStart,
          queuePaused,
          signalAborted: !!signal?.aborted,
        });
        if (decision === "launch") break; // capacity free — proceed to send
        if (decision === "abort") {
          // Ctrl-C mid-poll → return undefined (matching the disabled-mode
          // shape) instead of throwing. We already hold the token, so release it
          // before returning so the next head can poll immediately.
          try { releaseToken(); } catch { /* best-effort */ }
          try { queue.cancel(ourId); } catch { /* best-effort */ }
          released = true;
          return undefined;
        }
        if (decision === "failOpen") {
          // Only fail open when no known pause is active. A known pause means
          // the gate has a positive deprio signal; keep waiting (bounded by the
          // pause deadline + the 120s token watchdog). The decideLaunch guard
          // already checked queuePaused, so fail-open here is safe.
          break;
        }
        // decision === "wait"
        await new Promise((r) => setTimeout(r, pollIntervalMs + Math.floor(Math.random() * 100)));
        pollIntervalMs = nextPollInterval(pollIntervalMs, "wait");
      }
      // local in-flight tracking: add an in-flight entry BEFORE releasing the
      // token (the order is load-bearing — the next head's readState must see
      // our entry before it can claim the token, so max(localInFlight,
      // concurrent_sessions) counts us + blocks a sibling from launching into
      // our still-in-flight request). addInFlight is fail-closed: a throw
      // propagates to the finally, which cancels the waiter + token.
      queue.addInFlight(ourId);
      // Release the token IMMEDIATELY (not at message_end) so the next head can
      // poll + launch. The token serializes the /usage poll; holding it across
      // the send serializes to 1-at-a-time. Releasing here lets the next head
      // poll right away; the server's /usage lag means it sees a stale-low
      // concurrent_sessions + launches, achieving limit-concurrent saturation.
      try { releaseToken(); } catch { /* best-effort — release-resilience */ }
      releaseToken = () => {}; // no-op for the returned release fn
      released = true;
      return () => {
        releaseToken(); // no-op (token released above)
        try { queue.removeInFlight(ourId); } catch { /* best-effort: watchdog reaps at 120s */ }
        queue.cancel(ourId); // belt-and-suspenders: drop our waiter if still present
      };
    }
    return undefined;
  } finally {
    // If we exited without returning a release fn (throw, abort, or a path that
    // didn't set `released`), cancel our waiter entry so it doesn't pollute the
    // FIFO for staleWaiterMs. On the happy path the returned release fn owns the
    // cancellation, so `released` is true + this is a no-op.
    if (!released) {
      try { queue.cancel(ourId); } catch { /* best-effort */ }
    }
  }
}
