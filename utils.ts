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
  extractBoxedUntil,
  isSuspendBody,
  MAX_PAUSE_429_MS,
  SANITIZE_CTRL_RE,
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
