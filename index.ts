/**
 * Umans provider for pi.
 *
 * Registers the Umans Code gateway (https://api.code.umans.ai) as a first-class
 * pi provider using its Anthropic-compatible /v1/messages endpoint.
 *
 * Configuration is read from environment:
 *   UMANS_API_KEY          - required for inference (pi resolves $UMANS_API_KEY)
 *   UMANS_BASE_URL         - override gateway base URL (default: https://api.code.umans.ai)
 *   UMANS_BUDGET_THINKING  - "1" opts out of adaptive (effort-level) thinking into legacy budget-based thinking
 *   UMANS_DISABLE          - "1" disables the extension entirely
 *   UMANS_VISION_DISABLE   - "1" seeds vision handoff off (toggle live with /umans-vision)
 *   UMANS_VISION_MODEL     - seeds the vision model id (default: umans-kimi-k2.7, or first
 *                           native-vision model); change live with /umans-vision model <id>
 *   UMANS_SEARCH_DISABLE   - "1" disables the umans_web_search tool (e.g. when you use
 *                           your own MCP web-search tool). Vision handoff is unaffected.
 *   UMANS_CONCURRENCY_DISABLE - "1" disables client-side FIFO concurrency gating
 *                           (falls back to fire-and-forget; not recommended).
 *   UMANS_CONCURRENCY_LIMIT - override the capacity check value used by the queue
 *                           (default: live value from /v1/usage). Useful for testing.
 *                           The queue itself lives at ~/.pi/agent/umans-concurrency.json
 *                           and coordinates across all local pi processes.
 *
 * Client-side vision handoff: text-only ("via-handoff") Umans models can't see
 * images, so attached images are analyzed with a native-vision Umans model and
 * replaced in-message with `[Image analysis (image:ID)]: ...`. The analysis
 * persists in the conversation (KV-cache friendly: not re-analyzed each turn),
 * and the text model can call the `umans_vision` tool for targeted follow-ups.
 *
 * Models and capabilities are fetched live from /v1/models/info on extension
 * load. If the gateway is unreachable, a static fallback catalog is used so the
 * provider still registers.
 *
 * Usage:
 *   UMANS_API_KEY=uk-... pi -e ~/.pi/agent/extensions/umans-provider
 *   # then /model umans/umans-coder
 */
import { createHash } from "node:crypto";
import {
  createConcurrencyQueue,
  parsePriority,
  clampPauseUntil,
  isCapacityFree,
  parseConcurrencyLimit,
  PRIORITY_BACKOFF_MS,
  PAUSE_REASON_429,
  PAUSE_REASON_STRIKES,
  PAUSE_REASON_CAP_ABUSE,
  PAUSE_REASON_403_BRIDGE,
  PAUSE_403_BRIDGE_MS,
  STICKY_PAUSE_REASONS,
  extractBoxedUntil,
  isSuspendBody,
  MAX_PAUSE_429_MS,
  SANITIZE_CTRL_RE,
  type ConcurrencyQueue,
  type PriorityState,
} from "./concurrency-queue.ts";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { OAuthCredentials, OAuthLoginCallbacks } from "@earendil-works/pi-ai";
import { Type } from "typebox";
// Derive USER_AGENT from package.json so the version doesn't drift on
// release. ESM JSON import attribute `with { type: "json" }` is stable in
// Node 20.10+ (the engines floor is >=20.10.0, matching this requirement).
import pkg from "./package.json" with { type: "json" };

type ReasoningInfo = {
  supported: boolean;
  can_disable: boolean;
  levels: string[];
  default_level: string;
};

type ModelCapabilities = {
  max_completion_tokens?: number;
  recommended_max_tokens?: number;
  context_window?: number;
  supports_vision?: boolean | "via-handoff";
  supports_tools?: boolean;
  reasoning?: ReasoningInfo;
};

type UmansModelInfo = {
  name: string;
  display_name?: string;
  description?: string;
  deprecation?: unknown;
  capabilities: ModelCapabilities;
};

const DEFAULT_BASE_URL = "https://api.code.umans.ai";
const API_KEY_ENV = "UMANS_API_KEY";
const USER_AGENT = `pi-umans-provider/${pkg.version}`;
const STATUS_UPDATE_INTERVAL_MS = 1000;

// Client-side vision handoff env + tuning. See header doc for the design.
const VISION_DISABLE_ENV = "UMANS_VISION_DISABLE";
const VISION_MODEL_ENV = "UMANS_VISION_MODEL";
const SEARCH_DISABLE_ENV = "UMANS_SEARCH_DISABLE";
const CONCURRENCY_DISABLE_ENV = "UMANS_CONCURRENCY_DISABLE";
const CONCURRENCY_LIMIT_ENV = "UMANS_CONCURRENCY_LIMIT";
const CONCURRENCY_STATE_FILE_ENV = "UMANS_CONCURRENCY_STATE_FILE";
const VISION_MAX_TOKENS = 1024;
const VISION_TIMEOUT_MS = 60_000;
const VISION_ANALYSIS_PROMPT =
  "You are a vision assistant for a text-only coding model. Analyze the attached image thoroughly but concisely. " +
  "Capture: any visible text (verbatim), UI/layout, code/errors/stack traces, diagrams/charts, and other notable details. " +
  "Write a compact structured report. Do not speculate beyond what is visible.";

// Web search side-call tuning. See searchWeb / the umans_web_search tool.
const SEARCH_TIMEOUT_MS = 30_000;
const SEARCH_MAX_TOKENS = 2048;

// PRIORITY_BACKOFF_MS is imported from concurrency-queue.ts (the single
// source of truth — it's also the parsePriority fallback for a null boxed_until).
// max time the head-waiter capacity poll will wait for a free slot
// before failing open (launching anyway). Bounds the queue against a
// hostile/misbehaving /usage that always reports full.
const CAPACITY_POLL_TIMEOUT_MS = 60_000;

// 429 strike counter: the Umans account is paused for 5h after >20 concurrency
// 429s in 24h. The queue polls /v1/usage/history every STRIKE_POLL_INTERVAL_MS,
// sums rate_limit_concurrency buckets since the last cap_suspended (the server
// resets the counter on reactivation), and defensively pauses when the count
// reaches the dynamic threshold — better to self-pause briefly than risk the 5h ban.
//
// The threshold is dynamic: STRIKE_SERVER_LIMIT (20) minus the max in-flight
// requests (the concurrency limit, e.g. 4), so a burst of in-flight requests
// between the poll + the server's counter update can't tip us over before we
// react. With limit=4 → threshold=16; with limit=3 → threshold=17.
const STRIKE_POLL_INTERVAL_MS = 5 * 60 * 1000; // 5 min
const STRIKE_WINDOW_MS = 24 * 60 * 60 * 1000; // 24h (rolling, matches the server)
const STRIKE_SERVER_LIMIT = 20; // server triggers 5h pause at >20 strikes/24h
const STRIKE_PAUSE_MS = 30 * 60 * 1000; // 30 min self-pause to let strikes age out

/**
 * pure decision extracted from acquireSlot's capacity-poll loop so the
 * branch logic (free-first-poll, poll-then-free, timeout-fail-open, timeout-
 * but-paused-keeps-waiting, mid-poll-abort) is unit-testable without the full
 * pi runtime. The loop in acquireSlot drives capacityFree() (I/O) and applies
 * this decision each iteration.
 *
 * - `launch`: capacity is free — proceed with the send.
 * - `abort`: the turn's AbortSignal fired mid-poll — cancel + reject.
 * - `failOpen`: the poll cap elapsed AND no known pause is active — launch
 *   ungated (ADV-3) so a wedged /usage doesn't block forever. CORR4-3: a
 *   known active pause keeps the gate waiting (bounded by the pause deadline
 *   + the 120s watchdog) — fail-open for a POSITIVE deprio signal would launch
 *   into a still-deprioritized account.
 * - `wait`: keep polling (300ms + jitter).
 */
type LaunchDecision = "launch" | "wait" | "failOpen" | "abort";
export function decideLaunch(opts: {
  isFree: boolean;
  elapsedMs: number;
  queuePaused: boolean;
  signalAborted: boolean;
}): LaunchDecision {
  // signalAborted takes precedence over isFree. When the turn's
  // AbortSignal fires mid-poll AND /usage is unreachable (fetchUsage returns
  // null → isCapacityFree(null) returns {free:true} via the trust-headroom
  // stance), the prior isFree-first ordering would return "launch" + hold
  // the token until a safety net (turn_end/agent_end/session_shutdown) fires.
  // For a Ctrl-C'd turn that never sends, the token leaks up to the 120s
  // watchdog. Checking signalAborted first routes through the abort branch
  // (release token + cancel + return undefined) immediately at the abort site.
  if (opts.signalAborted) return "abort";
  if (opts.isFree) return "launch";
  if (opts.elapsedMs >= CAPACITY_POLL_TIMEOUT_MS && !opts.queuePaused) return "failOpen";
  return "wait";
}

/**
 * pure helper for the /usage poll interval under steady-full backoff.
 * With N local pi processes each running their own head waiter, a saturated
 * queue drives N×3.3 RPS to /usage continuously. Exponential backoff on the
 * poll interval when capacity is steadily full reduces RPS from ~3.3/s to
 * ~0.5/s during a sustained pause. Mirrors the decideLaunch /
 * shouldReleaseOnMessageEnd pattern: pure + exported so it is unit-testable
 * without the pi runtime.
 *
 * - "launch" / "failOpen": reset to BASE (the gate is about to release the
 *   token or fail open — no further polling, but if it does poll again it
 *   starts fresh at the fast cadence).
 * - "wait": grow by GROWTH (1.5×), capped at CAP (2000ms). The ±100ms jitter
 *   is applied by the caller, not here (keeps this pure + deterministic).
 */
export const POLL_INTERVAL_BASE_MS = 300;
export const POLL_INTERVAL_CAP_MS = 2_000;
export const POLL_INTERVAL_GROWTH = 1.5;
export function nextPollInterval(currentMs: number, decision: LaunchDecision, opts?: { base?: number; cap?: number; growth?: number }): number {
  const base = opts?.base ?? POLL_INTERVAL_BASE_MS;
  const cap = opts?.cap ?? POLL_INTERVAL_CAP_MS;
  const growth = opts?.growth ?? POLL_INTERVAL_GROWTH;
  if (decision === "wait") {
    const next = Math.round(currentMs * growth);
    return Math.min(next > 0 ? next : base, cap);
  }
  // launch / failOpen / abort: reset to base.
  return base;
}

/**
 * pure decision extracted from the message_end handler's release guard
 * so the "release only on an Umans assistant message" invariant is unit-
 * testable. The handler calls releaseMainTurn() only when this returns true;
 * user messages, tool results, and non-Umans providers are no-ops (the slot is
 * not held for them, or the turn_end/agent_end safety nets cover them).
 */
export function shouldReleaseOnMessageEnd(msg: { role?: string; provider?: string } | undefined, provider: string | undefined): boolean {
  return provider === "umans" && msg?.role === "assistant";
}

// Static fallback when /v1/models/info cannot be reached. Keep in sync with the
// public model list from https://api.code.umans.ai/v1/models
const STATIC_CATALOG: Record<string, UmansModelInfo> = {
  "umans-kimi-k2.6": {
    name: "umans-kimi-k2.6",
    display_name: "Umans Kimi K2.6",
    capabilities: {
      max_completion_tokens: 262144,
      recommended_max_tokens: 32768,
      context_window: 262144,
      supports_vision: true,
      supports_tools: true,
      reasoning: {
        supported: true,
        can_disable: true,
        levels: ["none", "minimal", "low", "medium", "high", "xhigh", "max"],
        default_level: "medium",
      },
    },
  },
  "umans-kimi-k2.7": {
    name: "umans-kimi-k2.7",
    display_name: "Umans Kimi K2.7 Code",
    capabilities: {
      max_completion_tokens: 262144,
      recommended_max_tokens: 32768,
      context_window: 262144,
      supports_vision: true,
      supports_tools: true,
      reasoning: {
        supported: true,
        can_disable: false,
        levels: ["minimal", "low", "medium", "high", "xhigh", "max"],
        default_level: "medium",
      },
    },
  },
  "umans-glm-5.1": {
    name: "umans-glm-5.1",
    display_name: "Umans GLM 5.1",
    capabilities: {
      max_completion_tokens: 131072,
      recommended_max_tokens: 131071,
      context_window: 202752,
      supports_vision: "via-handoff",
      supports_tools: true,
      reasoning: {
        supported: true,
        can_disable: true,
        levels: ["none", "minimal", "low", "medium", "high", "xhigh", "max"],
        default_level: "medium",
      },
    },
  },
  "umans-glm-5.2": {
    name: "umans-glm-5.2",
    display_name: "Umans GLM 5.2",
    capabilities: {
      max_completion_tokens: 131072,
      recommended_max_tokens: 131071,
      context_window: 405504,
      supports_vision: "via-handoff",
      supports_tools: true,
      reasoning: {
        supported: true,
        can_disable: true,
        levels: ["none", "minimal", "low", "medium", "high", "xhigh", "max"],
        default_level: "medium",
      },
    },
  },
  "umans-coder": {
    name: "umans-coder",
    display_name: "Umans Coder",
    capabilities: {
      max_completion_tokens: 262144,
      recommended_max_tokens: 32768,
      context_window: 262144,
      supports_vision: true,
      supports_tools: true,
      reasoning: {
        supported: true,
        can_disable: false,
        levels: ["minimal", "low", "medium", "high", "xhigh", "max"],
        default_level: "medium",
      },
    },
  },
  "umans-flash": {
    name: "umans-flash",
    display_name: "Umans Flash",
    capabilities: {
      max_completion_tokens: 262144,
      recommended_max_tokens: 32768,
      context_window: 262144,
      supports_vision: true,
      supports_tools: true,
      reasoning: {
        supported: true,
        can_disable: true,
        levels: ["none", "minimal", "low", "medium", "high", "xhigh", "max"],
        default_level: "medium",
      },
    },
  },
  "umans-qwen3.6-35b-a3b": {
    name: "umans-qwen3.6-35b-a3b",
    display_name: "Umans Qwen3.6 35B A3B",
    capabilities: {
      max_completion_tokens: 262144,
      recommended_max_tokens: 32768,
      context_window: 262144,
      supports_vision: true,
      supports_tools: true,
      reasoning: {
        supported: true,
        can_disable: true,
        levels: ["none", "minimal", "low", "medium", "high", "xhigh", "max"],
        default_level: "medium",
      },
    },
  },
};

/**
 * Resolve an output budget that never hits the gateway's hard cap.
 * The gateway rejects max_tokens >= max_completion_tokens with a 400.
 */
function safeMaxTokens(recommended?: number, cap?: number): number {
  const fallback = 32768;
  let value =
    typeof recommended === "number" && recommended > 0 ? recommended : fallback;
  if (typeof cap === "number" && cap > 0) {
    value = Math.min(value, cap - 1);
  }
  return Math.max(value, 1);
}

/**
 * Models that report any vision support (native or via-handoff) can accept
 * images through the Anthropic /v1/messages endpoint. The gateway handles the
 * handoff internally; from the client's perspective they are vision-capable.
 */
function toInputModalities(info: UmansModelInfo): ("text" | "image")[] {
  const v = info.capabilities?.supports_vision;
  return v === true || v === "via-handoff"
    ? ["text", "image"]
    : ["text"];
}

/**
 * Map pi thinking levels to Umans reasoning levels.
 *
 * Umans exposes levels: none, minimal, low, medium, high, xhigh, max.
 * Pi exposes levels: off, minimal, low, medium, high, xhigh.
 * Pi has no "max" level, so pi's xhigh is mapped to Umans's max when available,
 * giving users access to the deepest reasoning tier via pi's highest level.
 * When a model cannot disable reasoning (can_disable === false), mark the
 * "off" level as unsupported (null) so pi clamps to the minimum level instead
 * of sending a disabled-thinking parameter the model rejects.
 */
function toThinkingLevelMap(
  info: UmansModelInfo,
): Partial<Record<"off" | "minimal" | "low" | "medium" | "high" | "xhigh", string | null>> {
  const reasoning = info.capabilities?.reasoning;
  if (!reasoning?.supported) return {};

  const levels = new Set(reasoning.levels);
  const map: Partial<
    Record<"off" | "minimal" | "low" | "medium" | "high" | "xhigh", string | null>
  > = {};

  map.off = reasoning.can_disable && levels.has("none") ? "none" : null;
  map.minimal = levels.has("minimal") ? "minimal" : null;
  map.low = levels.has("low") ? "low" : null;
  map.medium = levels.has("medium") ? "medium" : null;
  map.high = levels.has("high") ? "high" : null;
  map.xhigh = levels.has("max") ? "max" : levels.has("xhigh") ? "xhigh" : null;

  return map;
}

async function fetchModelCatalog(
  baseUrl: string,
): Promise<Record<string, UmansModelInfo> | undefined> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 5000);
  try {
    const res = await fetch(`${baseUrl}/v1/models/info`, {
      headers: { "User-Agent": USER_AGENT, Accept: "application/json" },
      signal: ctrl.signal,
    });
    if (!res.ok) return undefined;
    const data = await res.json();
    // Expect a flat object keyed by model id, each value carrying capabilities.
    // Reject arrays or wrapper shapes ({ data: [...] }) so we fall back to static.
    if (!data || Array.isArray(data) ||
        !Object.values(data).every((m: unknown) => !!m && typeof m === "object" &&
          typeof (m as UmansModelInfo).capabilities === "object")) {
      return undefined;
    }
    return Object.keys(data).length > 0 ? (data as Record<string, UmansModelInfo>) : undefined;
  } catch {
    return undefined;
  } finally {
    clearTimeout(timer);
  }
}

export function isNativeVision(info: UmansModelInfo): boolean {
  return !info.deprecation && info.capabilities?.supports_vision === true;
}

/**
 * Pick the vision model used to analyze images for text-only (via-handoff)
 * models. Honors UMANS_VISION_MODEL when it points at a native-vision model;
 * otherwise defaults to umans-kimi-k2.7 (matching the gateway's "sends to
 * kimi" handoff), falling back to the first native-vision model in the catalog.
 */
export function pickVisionModel(catalog: Record<string, UmansModelInfo>): string | undefined {
  const configured = process.env[VISION_MODEL_ENV]?.trim();
  if (configured && catalog[configured] && isNativeVision(catalog[configured])) {
    return configured;
  }
  const defaultId = "umans-kimi-k2.7";
  if (catalog[defaultId] && isNativeVision(catalog[defaultId])) return defaultId;
  for (const [id, info] of Object.entries(catalog)) {
    if (isNativeVision(info)) return id;
  }
  return undefined;
}

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
 * Formats a human-readable countdown to a future deadline (e.g. " 3h12m",
 * " 45m", " 2s", " 0s"). Returns "" for past deadlines (already cleared).
 * Used by the DEPRIO + PAUSED status-bar banners so the user sees how long
 * until the state clears without mental arithmetic.
 */
export function countdown(untilMs: number | undefined, now?: number): string {
  if (untilMs === undefined) return "";
  const nowMs = now ?? Date.now();
  const remainingMs = untilMs - nowMs;
  if (remainingMs <= 0) return " 0s";
  const totalSec = Math.floor(remainingMs / 1000);
  const h = Math.floor(totalSec / 3600);
  const m = Math.floor((totalSec % 3600) / 60);
  const s = totalSec % 60;
  if (h > 0) return ` ${h}h${String(m).padStart(2, "0")}m`;
  if (m > 0) return ` ${m}m${String(s).padStart(2, "0")}s`;
  return ` ${s}s`;
}

/**
 * pure formatter for the status-bar text, extracted from the
 * `statusText` closure so the rendering (TTFT/TPS, Conc current/guaranteed,
 * Req, q N*, STRIKES X/20, DEPRIO +countdown, PAUSED +countdown (reason)) is
 * unit-testable without the pi runtime. The closure in index.ts builds the
 * inputs (effectiveLimit, currentConcurrency, requestLimit/Used, the queue
 * snapshot, concurrencyDisabled, strikes24h, deprioritized, priorityUntil)
 * + delegates to this helper.
 */
export function formatStatusText(opts: {
  metrics?: { ttft?: number; tps?: number };
  effectiveLimit?: number;
  currentConcurrency?: number;
  requestLimit?: number;
  requestsUsed?: number;
  queueSnap?: { queued: number; tokenHeld: boolean; paused: boolean; pausedUntil: number; pausedReason: string | null };
  concurrencyDisabled?: boolean;
  strikes24h?: number;
  deprioritized?: boolean;
  priorityUntil?: number;
  now?: number;
}): string {
  const parts: string[] = [];
  const { metrics, effectiveLimit, currentConcurrency, requestLimit, requestsUsed, queueSnap, concurrencyDisabled, strikes24h, deprioritized, priorityUntil, now } = opts;
  if (metrics?.ttft !== undefined) parts.push(`TTFT ${metrics.ttft}ms`);
  if (metrics?.tps !== undefined) parts.push(`TPS ${metrics.tps}`);
  const guaranteed = effectiveLimit !== undefined ? String(effectiveLimit) : "?";
  const current = currentConcurrency !== undefined ? String(currentConcurrency) : "?";
  parts.push(`Conc ${current}/${guaranteed}`);
  if (requestsUsed !== undefined && requestLimit !== undefined) {
    parts.push(`Req ${requestsUsed}/${requestLimit}`);
  }
  if (deprioritized) {
    // Show countdown until deprioritization clears (boxed_until).
    const remaining = countdown(priorityUntil, now);
    parts.push(`DEPRIO${remaining}`);
  }
  if (!concurrencyDisabled && queueSnap) {
    if (queueSnap.queued > 0 || queueSnap.tokenHeld) {
      parts.push(`q ${queueSnap.queued}${queueSnap.tokenHeld ? "*" : ""}`);
    }
    if (queueSnap.paused) {
      const remaining = countdown(queueSnap.pausedUntil, now);
      const reason = queueSnap.pausedReason ? ` (${queueSnap.pausedReason})` : "";
      parts.push(`PAUSED${remaining}${reason}`);
    }
  }
  if (strikes24h !== undefined) {
    parts.push(`Strikes ${strikes24h}/20`);
  }
  return `Umans ${parts.join(" │ ")}`;
}

export function hashImageId(data: string): string {
  return "img_" + createHash("sha256").update(data).digest("hex").slice(0, 8);
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

/**
 * Shared 429 handler: parse Retry-After (strict integer form only), clamp to
 * MAX_PAUSE_429_MS, push the shared pause (PAUSE_REASON_429) so sibling pi
 * processes back off, and return the resolved `until` deadline so the caller
 * can notify. COV4-2: pauseUntil can throw on disk failure (EACCES/ENOSPC/EROFS)
 * — the lost pause is bounded by the 120s watchdog + the 5s refreshUsage poll,
 * so warn + swallow so the caller's turn is not aborted.
 *
 * extracted from after_provider_response so the side-call sites
 * (analyzeImage, searchWeb) push the SAME shared pause when they receive a
 * 429. Per D6 each side-call consumes a real account concurrency slot, and
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
    // that can wedge the queue (S2/S4). Parse strictly and cap the resulting
    // deadline at now + MAX_PAUSE_429_MS via clampPauseUntil (ADV4-2: a
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
 * throw block. This helper runs the 429 push (CORR8-2: a side-call 429
 * deprioritizes the whole account — per D6 the side-call consumes a real
 * concurrency slot — so push the shared pause so sibling pi processes + the
 * main turn on its next launch back off, do NOT merely throw), reads + caps +
 * sanitizes the gateway error body (SEC7-4: attacker-controlled body must not
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

/** Duck-typed Retry-After header lookup (fetch Headers .get OR a plain record). */
function readRetryAfter(headers: Headers | Record<string, string> | undefined | null): string | undefined {
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

/*
 * Concurrency gating moved to ./concurrency-queue.ts (file-backed FIFO shared
 * across pi processes via ~/.pi/agent/umans-concurrency.json).
 */
// ConcurrencyQueue is imported directly and used at the
// factory call site. CLN4-1: WaiterEntry / TokenState / CapacitySnapshot /
// CapacityInputs are intentionally private (shape guards / internal
// decision inputs); QueueState + QueueConfig are the exported types (see
// concurrency-queue.ts). Local type alias for the release function returned
// by acquireSlot.
type Release = () => void;

// Session-scoped cache of image bytes keyed by a content hash. Lets the
// `umans_vision` tool re-query an image for targeted follow-ups without
// re-sending it to the text model each turn. Cleared on session start/shutdown.
// ponytail: in-memory only — lost on /reload or session switch; the persisted
// analysis text still stands, only fresh follow-ups on old images become
// unavailable until the image is re-attached.
const imageStore = new Map<string, { data: string; mimeType: string }>();

/**
 * Call a native-vision Umans model with one image + a text prompt and return
 * its text answer. Non-streaming, abort-aware (caller signal + hard timeout).
 */
async function analyzeImage(
  apiKey: string,
  model: string,
  baseUrl: string,
  image: { data: string; mimeType: string },
  prompt: string,
  signal?: AbortSignal,
  concurrencyQueue?: { pauseUntil(until: number, reason?: string | null): void },
): Promise<string> {
  // compose the caller's signal + a timer-driven controller via
  // AbortSignal.any (Node 20.3+; declared in package.json engines). Replaces
  // the manual addEventListener + finally removeEventListener bridge
  // (listener-leak footgun + boilerplate). The fetch aborts when EITHER the
  // parent signal aborts OR the timer fires.
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), VISION_TIMEOUT_MS);
  const composed = signal ? AbortSignal.any([signal, ctrl.signal]) : ctrl.signal;
  try {
    const res = await fetch(`${baseUrl}/v1/messages`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": apiKey,
        Authorization: `Bearer ${apiKey}`,
        "anthropic-version": "2023-06-01",
        "User-Agent": USER_AGENT,
      },
      body: JSON.stringify({
        model,
        max_tokens: VISION_MAX_TOKENS,
        messages: [
          {
            role: "user",
            content: [
              { type: "text", text: prompt },
              {
                type: "image",
                source: { type: "base64", media_type: image.mimeType, data: image.data },
              },
            ],
          },
        ],
      }),
      signal: composed,
    });
    if (!res.ok) {
      // delegated to raiseForUmansStatus (shared with searchWeb) —
      // runs the 429 push (CORR8-2), reads + sanitizes the body (SEC7-4), throws.
      await raiseForUmansStatus(res, concurrencyQueue);
    }
    const data = (await res.json()) as { content?: Array<{ type: string; text?: string }> };
    const text = (data.content ?? [])
      .filter((b) => b.type === "text" && typeof b.text === "string")
      .map((b) => b.text!)
      .join("\n")
      .trim();
    return text || "(no analysis returned)";
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Run a web search by making a sub-request to the Umans gateway with the
 * Anthropic `web_search_20250305` server tool declared. The gateway runs the
 * Exa search server-side and returns results; we surface the model's formatted
 * result text (titles, URLs, snippets) back to the calling model.
 *
 * Side-call because pi-ai only serializes client-side tools and cannot emit the
 * server-tool shape the gateway requires (see header doc). Costs one extra
 * round-trip per search; no pi-ai changes needed.
 */
async function searchWeb(
  apiKey: string,
  model: string,
  baseUrl: string,
  query: string,
  signal?: AbortSignal,
  concurrencyQueue?: { pauseUntil(until: number, reason?: string | null): void },
): Promise<string> {
  // compose the caller's signal + a timer-driven controller via
  // AbortSignal.any (Node 20.3+). See analyzeImage for the rationale.
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), SEARCH_TIMEOUT_MS);
  const composed = signal ? AbortSignal.any([signal, ctrl.signal]) : ctrl.signal;
  try {
    const res = await fetch(`${baseUrl}/v1/messages`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": apiKey,
        Authorization: `Bearer ${apiKey}`,
        "anthropic-version": "2023-06-01",
        "User-Agent": USER_AGENT,
      },
      body: JSON.stringify({
        model,
        max_tokens: SEARCH_MAX_TOKENS,
        tools: [{ type: "web_search_20250305", name: "web_search", max_uses: 3 }],
        messages: [
          {
            role: "user",
            content:
              "Search the web for the query below and return a concise list of the most relevant results. " +
              "For each result give: title, URL, and a short snippet of the key facts. " +
              "Do not answer beyond what the sources say.\n\nQuery: " +
              query,
          },
        ],
      }),
      signal: composed,
    });
    if (!res.ok) {
      // delegated to raiseForUmansStatus (shared with analyzeImage) —
      // runs the 429 push (CORR8-2), reads + sanitizes the body (SEC7-4), throws.
      await raiseForUmansStatus(res, concurrencyQueue);
    }
    const data = (await res.json()) as {
      content?: Array<{
        type: string;
        text?: string;
        content?: Array<{ url?: string; title?: string }>;
      }>;
    };
    const blocks = data.content ?? [];
    const text = blocks
      .filter((b) => b.type === "text" && typeof b.text === "string")
      .map((b) => b.text!)
      .join("\n")
      .trim();
    if (text) return text;
    // No synthesized text — fall back to the raw result list.
    const results =
      blocks.find((b) => b.type === "web_search_tool_result")?.content ?? [];
    if (results.length) {
      return results
        .map((r, i) => `${i + 1}. ${r.title ?? ""}\n   URL: ${r.url ?? ""}`)
        .join("\n");
    }
    return "(no search results returned)";
  } finally {
    clearTimeout(timer);
  }
}

export default async function (pi: ExtensionAPI) {
  if (process.env.UMANS_DISABLE === "1") return;

  const baseUrl =
    process.env.UMANS_BASE_URL?.trim().replace(/\/$/, "") || DEFAULT_BASE_URL;

  // The model-info endpoint is public, so this works even before the user has
  // configured an API key. It lets pi --list-models report accurate models.
  const catalog = (await fetchModelCatalog(baseUrl)) ?? STATIC_CATALOG;

  // Umans models expose reasoning as effort levels (low/medium/high/xhigh/max),
  // which is Anthropic's adaptive-thinking format (`thinking.type: "adaptive"` +
  // `output_config.effort`). Force adaptive by default so pi sends that format.
  // Set UMANS_BUDGET_THINKING=1 to fall back to legacy budget-based thinking.
  const useBudgetThinking = process.env.UMANS_BUDGET_THINKING === "1";

  const models = Object.entries(catalog)
    .filter(([, info]) => !info.deprecation)
    .map(([id, info]) => {
      const capabilities = info.capabilities ?? {};
      const reasoning = capabilities.reasoning;

      return {
        id,
        name: info.display_name || info.name || id,
        reasoning: reasoning?.supported ?? false,
        thinkingLevelMap: toThinkingLevelMap(info),
        input: toInputModalities(info),
        // ponytail: Umans gateway is currently unmetered; revisit when pricing appears in /v1/models/info.
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
        contextWindow: capabilities.context_window || 262144,
        maxTokens: safeMaxTokens(
          capabilities.recommended_max_tokens,
          capabilities.max_completion_tokens,
        ),
        compat: {
          // Umans models use effort levels = Anthropic adaptive thinking format.
          // Force adaptive by default; UMANS_BUDGET_THINKING=1 opts into legacy
          // budget-based thinking.
          forceAdaptiveThinking: reasoning?.supported && !useBudgetThinking,
          // Adaptive thinking returns thinking blocks with NO valid signature.
          // pi's default converts unsigned prior thinking to plain text on the next
          // turn, which corrupts context: the model echoes it as
          // `[Thinking from previous turn]`, the marker stacks each turn, and any
          // junk directive locks in (observed degrading a long helpdesk build until
          // thinking collapsed to just the marker). Preserve the thinking block with
          // an empty signature instead — Umans accepts empty-signature thinking.
          allowEmptySignature: true,
        },
      };
    });

  if (models.length === 0) {
    throw new Error("Umans provider: no models available from gateway or fallback");
  }

  async function loginUmans(
    callbacks: OAuthLoginCallbacks,
  ): Promise<OAuthCredentials> {
    const apiKey = await callbacks.onPrompt({
      message: "Enter your Umans API key:",
    });
    const key = apiKey.trim();
    if (!key) throw new Error("Umans API key is required");
    return {
      refresh: key,
      access: key,
      expires: Date.now() + 100 * 365 * 24 * 60 * 60 * 1000,
    };
  }

  function refreshUmansToken(
    credentials: OAuthCredentials,
  ): Promise<OAuthCredentials> {
    return Promise.resolve(credentials);
  }

  function getApiKey(credentials: OAuthCredentials): string {
    return credentials.access;
  }

  pi.registerProvider("umans", {
    name: "Umans",
    baseUrl,
    apiKey: `$${API_KEY_ENV}`,
    api: "anthropic-messages",
    authHeader: true,
    models,
    oauth: {
      name: "Umans",
      login: loginUmans,
      refreshToken: refreshUmansToken,
      getApiKey,
    },
  });

  // === Status bar: TTFT | TPS | Conc current/guaranteed ===
  const STATUS_KEY = "umans";
  let guaranteedConcurrency: number | undefined;
  let currentConcurrency: number | undefined;
  let requestLimit: number | undefined;
  let requestsUsed: number | undefined;
  let strikes24h: number | undefined;
  let deprioritized = false;
  let priorityUntil: number | undefined;

  // Cross-process FIFO queue over outbound Umans requests, backed by
  // ~/.pi/agent/umans-concurrency.json (O_EXCL lockfile + atomic rename). The
  // file is a PURE WAITER QUEUE + launch token; capacity is decided solely by
  // the live /v1/usage response polled by the head waiter, so multiple pi
  // processes (and multiple machines) coordinate through the server, not a
  // local count. UMANS_CONCURRENCY_DISABLE opts out (fire-and-forget).
  // UMANS_CONCURRENCY_LIMIT is now only a display/testing hint: when set, the
  // capacity check uses it instead of the server's limits.concurrency.limit.
  // UMANS_CONCURRENCY_STATE_FILE overrides the state file path so the
  // handler-wiring harness mock in selfcheck can point the real queue at a
  // tmpdir (isolating the test from the live ~/.pi/agent state file). Also
  // handy for local multi-process serialization experiments. No-op in normal
  // use (the default path is used when unset/empty).
  const concurrencyDisabled = process.env[CONCURRENCY_DISABLE_ENV] === "1";
  const concurrencyStateFile = process.env[CONCURRENCY_STATE_FILE_ENV]?.trim() || undefined;
  const concurrencyQueue: ConcurrencyQueue = createConcurrencyQueue({
    disabled: concurrencyDisabled,
    ...(concurrencyStateFile ? { stateFile: concurrencyStateFile } : {}),
  });
  function concurrencyLimit(): number | undefined {
    return parseConcurrencyLimit(process.env[CONCURRENCY_LIMIT_ENV], guaranteedConcurrency);
  }

  type LiveRequest = {
    startTime: number;
    firstTokenTime?: number;
    estimatedTokens: number;
    lastStatusUpdate: number;
  };
  let liveRequest: LiveRequest | undefined;
  let lastMetrics: { ttft?: number; tps?: number } = {};

  let refreshTimer: ReturnType<typeof setTimeout> | undefined;
  let strikeTimer: ReturnType<typeof setTimeout> | undefined;
  let refreshStopped = false;

  function stopRefreshLoop() {
    refreshStopped = true;
    if (refreshTimer) {
      clearTimeout(refreshTimer);
      refreshTimer = undefined;
    }
    if (strikeTimer) {
      clearTimeout(strikeTimer);
      strikeTimer = undefined;
    }
  }

  function restartRefreshLoop(apiKey: string) {
    stopRefreshLoop();
    refreshStopped = false;
    scheduleRefresh(apiKey);
    scheduleStrikePoll(apiKey, true); // immediate: know the strike count at startup
  }

  function scheduleRefresh(apiKey: string) {
    if (refreshStopped || !apiKey) return;
    refreshTimer = setTimeout(async () => {
      await refreshUsage(apiKey);
      scheduleRefresh(apiKey);
    }, 5000);
  }

  // Strike counter poll: fetch the 24h 429 count + defensively pause if we're
  // approaching the server's 5h-pause threshold. Runs on a longer interval than
  // refreshUsage (5 min vs 5s) because /history is heavier + the count moves
  // slowly. The first poll fires immediately (no delay) so we know the strike
  // count at startup, not 5 min in.
  function scheduleStrikePoll(apiKey: string, immediate = false) {
    if (refreshStopped || !apiKey) return;
    strikeTimer = setTimeout(async () => {
      await refreshStrikes(apiKey);
      scheduleStrikePoll(apiKey);
    }, immediate ? 0 : STRIKE_POLL_INTERVAL_MS);
  }

  async function refreshStrikes(apiKey: string) {
    const result = await fetch429Strikes(apiKey);
    if (result.suspended) {
      // /v1/usage/history may also return 403 during a suspension (the
      // server returns 403 for everything once suspended). The prior code
      // left the last cached strikes value, so the status bar showed a stale
      // "Strikes 19/20" for the full 5h suspension. Clear it so the bar
      // reflects that the count is unknown (the threshold check below is
      // skipped — extend-never-shorten holds regardless because a cap_abuse
      // pause is sticky + longer than any strike pause).
      strikes24h = undefined;
      return;
    }
    // Transient failure (network timeout, 5xx, JSON parse): preserve the
    // cached count so a single blip does not lose the strike count + skip
    // the defensive self-pause right when it matters most. The threshold
    // check is skipped (count === null), matching the prior behavior, but
    // the cached value survives until the next poll (5min).
    if (result.count === null) return;
    const count = result.count;
    strikes24h = count;
    // Dynamic threshold: server limit minus the max in-flight requests (the
    // concurrency limit). A burst of in-flight requests can all 429 before our
    // next poll (every 5 min) tips the server's counter past the limit; leaving
    // a margin equal to the max in-flight means we pause before that burst can
    // push us over. With limit=4 → threshold=16; with limit=3 → threshold=17.
    const maxInFlight = concurrencyLimit() ?? guaranteedConcurrency ?? 0;
    const strikeThreshold = Math.max(0, STRIKE_SERVER_LIMIT - maxInFlight);
    // Defensively self-pause when approaching the 5h-pause threshold. Better to
    // pause briefly + let strikes age out than risk the 5h account ban. The
    // pause is only pushed if no longer pause is already active (don't shorten a
    // real priority.low / 429 pause with a shorter strikes pause).
    if (count >= strikeThreshold) {
      const snap = concurrencyQueue.snapshot();
      const now = Date.now();
      const strikeUntil = now + STRIKE_PAUSE_MS;
      // Only push if no active pause OR the active pause ends sooner than our
      // strike pause (extend, never shorten). A priority.low pause from the
      // server (boxed_until) or a 429 pause is left untouched if it's longer.
      if (!snap.paused || snap.pausedUntil < strikeUntil) {
        try {
          concurrencyQueue.pauseUntil(strikeUntil, PAUSE_REASON_STRIKES);
        } catch (err) {
          console.warn("umans: pauseUntil threw in refreshStrikes (continuing):", err instanceof Error ? err.message : err);
        }
      }
    }
    // No updateStatus call here — refreshStrikes runs from a timer without an
    // event ctx. The strikes24h var is picked up on the next status render
    // (streaming event or the periodic refreshUsage path). The pause push
    // above is what matters operationally; the display follows on the next tick.
  }

  function computeCumulativeTps(req: LiveRequest, now: number): number {
    if (!req.firstTokenTime || req.estimatedTokens <= 0) return 0;
    const elapsedSec = (now - req.firstTokenTime) / 1000;
    // Wait a moment so a tiny first chunk does not create a wild initial value.
    if (elapsedSec < 0.5) return 0;
    return Math.round(req.estimatedTokens / elapsedSec);
  }

  function statusText(metrics?: { ttft?: number; tps?: number }) {
    // delegates to the pure formatStatusText helper so the rendering
    // is unit-testable. The closure supplies the live inputs.
    return formatStatusText({
      metrics,
      effectiveLimit: concurrencyLimit(),
      currentConcurrency,
      requestLimit,
      requestsUsed,
      queueSnap: concurrencyQueue.snapshot(),
      concurrencyDisabled,
      strikes24h,
      deprioritized,
      priorityUntil,
    });
  }

  function setWidget(ctx: any, text?: string) {
    try {
      ctx.ui.setWidget(
        STATUS_KEY,
        text ? [ctx.ui.theme.fg("dim", text)] : undefined,
        { placement: "belowEditor" },
      );
    } catch {
      // UI may not be available in all modes; ignore.
    }
  }

  function updateStatus(ctx: any, metrics?: { ttft?: number; tps?: number }) {
    if (metrics) {
      // TTFT is tied to the current response; update it when provided.
      if (metrics.ttft !== undefined) lastMetrics.ttft = metrics.ttft;
      // Keep the last non-zero TPS so the display does not flash 0 during
      // tool-call gaps or tiny response tails. It resets only when the user
      // switches away from Umans or the session shuts down.
      if (metrics.tps !== undefined && metrics.tps > 0) {
        lastMetrics.tps = metrics.tps;
      }
    }
    setWidget(ctx, statusText(lastMetrics));
  }

  // Shared /v1/usage fetch skeleton (CLN2-M2). refreshUsage and
  // fetchUsageSnapshot both build the identical AbortController + fetch +
  // JSON-parse skeleton; this helper dedupes ~15 lines. Returns the parsed
  // { limits, usage } on a 2xx, or null on any failure (caller decides how to
  // handle — refreshUsage leaves cached values, fetchUsageSnapshot returns null
  // and the caller fails-open).
  // accept an optional parentSignal (the turn's AbortSignal) + compose
  // it into the fetch signal so a Ctrl-C mid capacity-poll aborts the in-flight
  // /usage fetch immediately instead of waiting up to 3s for the timeout.
  // composition now uses AbortSignal.any (Node 20.3+) instead of the
  // manual addEventListener + finally removeEventListener bridge.
  async function fetchUsage(apiKey: string, timeoutMs: number, parentSignal?: AbortSignal): Promise<{
    limits?: { concurrency?: { limit?: number; hard_cap?: number }; requests?: { limit?: number } };
    usage?: { requests_in_window?: number; concurrent_sessions?: number; priority?: unknown };
  } | null> {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    const composed = parentSignal ? AbortSignal.any([parentSignal, ctrl.signal]) : ctrl.signal;
    try {
      const res = await fetch(`${baseUrl}/v1/usage`, {
        signal: composed,
        headers: {
          Authorization: `Bearer ${apiKey}`,
          Accept: "application/json",
          "User-Agent": USER_AGENT,
        },
      });
      if (!res.ok) {
        // a 403 FROM /v1/usage is a POSITIVE suspension signal, not absence —
        // the server returns 403 for everything once the account is suspended.
        // Return a synthetic priority.low + reason=cap_abuse snapshot so the
        // cap_abuse branch in isCapacityFree fires + pushes the real pause.
        // A non-suspend 403 (auth error on /usage) keeps fail-open (return null).
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
      return await res.json() as {
        limits?: { concurrency?: { limit?: number; hard_cap?: number }; requests?: { limit?: number } };
        usage?: { requests_in_window?: number; concurrent_sessions?: number; priority?: unknown };
      };
    } catch {
      return null;
    } finally {
      clearTimeout(timer);
    }
  }

  // Fetch the count of concurrency 429s since the last cap_suspended (5h pause)
  // from /v1/usage/history. This is the API-accessible proxy for the dashboard's
  // "X/20 limit hits today" counter (which is not available via API key — it
  // requires a NextAuth web session on app.umans.ai). The server pauses the
  // account for 5h after >20 concurrency 429s in 24h AND resets the counter on
  // reactivation (a reactivation revokes + rotates API keys), so we exclude
  // strikes from before the most recent cap_suspended bucket — matching the
  // dashboard's behavior so our count stays accurate after a reactivation.
  //
  // Returns a typed result: suspend-403 (server returns 403 for /history too
  // once suspended — clear the cached count so the bar shows no stale
  // "Strikes 19/20" for 5h) vs transient failure (preserve the cached count so
  // a blip does not skip the self-pause).
  async function fetch429Strikes(apiKey: string): Promise<{ count: number | null; suspended: boolean }> {
    const now = Date.now();
    const from = new Date(now - STRIKE_WINDOW_MS).toISOString();
    const to = new Date(now).toISOString();
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 5000);
    try {
      const res = await fetch(
        `${baseUrl}/v1/usage/history?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}&granularity=hour`,
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
        // a transient !res.ok (5xx, etc.) so the caller clears the cache only
        // on a real suspension + preserves the cached count on a transient
        // failure. A non-403 !res.ok is transient (count null, suspended false)
        // — the caller leaves the cached value.
        if (res.status === 403) {
          const txt = await res.text().catch(() => "");
          if (isSuspendBody(txt)) return { count: null, suspended: true };
        }
        return { count: null, suspended: false };
      }
      const data = await res.json() as { buckets?: Array<{ bucket?: string; error_category?: string | null; requests?: number }> };
      if (!Array.isArray(data.buckets)) return { count: null, suspended: false };
      // Find the most recent cap_suspended bucket timestamp. Strikes before it
      // are excluded (the counter resets on reactivation). If there's no
      // cap_suspended in the window, all strikes count.
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

  async function refreshUsage(apiKey: string) {
    const data = await fetchUsage(apiKey, 5000);
    if (!data) return; // leave cached values; status bar will show "?"
    // The synthetic cap_abuse return (fetchUsage on a /v1/usage 403 with a
    // suspend body) carries no `limits` field. Skip the limits assignments so
    // the cached guaranteedConcurrency / requestLimit are preserved for the
    // suspension window (otherwise they wipe to undefined).
    if (data.limits) {
      // null ?? undefined normalizes unlimited (null) limits so the display
      // guards below hide them instead of rendering "x/null".
      guaranteedConcurrency = data.limits.concurrency?.limit ?? undefined;
      currentConcurrency = data.usage?.concurrent_sessions;
      requestLimit = data.limits.requests?.limit ?? undefined;
      requestsUsed = data.usage?.requests_in_window;
    } else {
      // Synthetic cap_abuse object: only usage.concurrent_sessions is present
      // (0). Update the live concurrency display but preserve the cached limits.
      currentConcurrency = data.usage?.concurrent_sessions;
    }
    // Track the deprioritization state for the status bar (DEPRIO banner).
    // priority.low is a STATUS signal, not a stop condition: the gate lowers
    // the cap by 1 (isCapacityFree) to reduce race risk, but does NOT push a
    // full pause. Work continues — just slower. Actual 429s + the strike
    // counter handle the hard pause.
    const priority = parsePriority(data.usage?.priority);
    deprioritized = priority.low;
    priorityUntil = priority.until;
    // Only clear a pause pushed by a PREVIOUS priority.low tick — don't
    // clear a sticky-origin pause (see STICKY_PAUSE_REASONS); a stale /usage
    // tick reporting low===false before the server catches up must not wipe
    // a freshly-written 429 / cap_abuse / strike pause.
    if (!priority.low) {
      const snap = concurrencyQueue.snapshot();
      if (snap.paused && !(snap.pausedReason && STICKY_PAUSE_REASONS.has(snap.pausedReason))) {
        concurrencyQueue.clearPause();
      }
    }
  }

  // Lightweight one-shot /v1/usage fetch used by the head waiter to decide
  // whether to launch. Uses a shorter timeout (3s vs 5s) so a slow /usage
  // response doesn't stall the head-waiter poll; reads only the
  // capacity-decision fields (concurrent_sessions + limit + hard_cap +
  // priority). Returns null on any failure (caller retries).
  async function fetchUsageSnapshot(apiKey: string, parentSignal?: AbortSignal): Promise<{
    concurrentSessions: number | undefined;
    limit: number | undefined;
    hardCap: number | undefined;
    priority: PriorityState;
  } | null> {
    const data = await fetchUsage(apiKey, 3000, parentSignal);
    if (!data) return null;
    return {
      concurrentSessions: data.usage?.concurrent_sessions,
      limit: data.limits?.concurrency?.limit ?? undefined,
      hardCap: data.limits?.concurrency?.hard_cap ?? undefined,
      priority: parsePriority(data.usage?.priority),
    };
  }

  // Acquire a concurrency slot for an outbound Umans request (main turn or a
  // vision/search side-call). Joins the cross-process FIFO, waits until we are
  // head + have claimed the launch token, then polls /v1/usage until the server
  // reports a free slot (and no priority.low). Returns a release fn that drops
  // the token + our waiter entry; call it on assistant message_end (the
  // primary release path) or turn_end/agent_end as a safety net. Returns
  // undefined when the queue is disabled (fire-and-forget) or when the turn's
  // AbortSignal fires mid-poll (clean cancellation, not a throw — CLN7-2). The
  // `apiKey` is used for the head-waiter poll.
  //
  // this function BLOCKS until the slot is acquired — it is NOT a fast
  // non-blocking check. The wait is the FIFO queue wait (possibly minutes under
  // contention) + the /usage capacity poll (up to CAPACITY_POLL_TIMEOUT_MS =
  // 60s fail-open, or longer while a known pause is active per CORR4-3). All
  // callers (before_provider_request + the three side-call sites) await it inline
  // on the critical path of the turn — by design, the whole point is to
  // serialize launches so the account stays under its soft cap.
  //
  // Recovery for an aborted/stuck token holder is the watchdog (reapStale):
  // any token held >120s (or whose PID died) is reclaimed by the next acquirer,
  // so a crashed/aborted turn can stall the queue for at most 120s. For an
  // aborted-but-alive turn (user Ctrl-C mid-wait), the `signal` plumbed through
  // waitForLaunch cancels the waiter entry and rejects immediately (C4/ADV-2).
  async function acquireSlot(apiKey: string, signal?: AbortSignal): Promise<Release | undefined> {
    const initialId = concurrencyQueue.join();
    if (!initialId) return undefined; // queue disabled
    // ourId may be re-assigned below when touchToken returns false
    // (token reaped by a sibling's reapStale) and we re-join the queue. join()
    // returns null only when the queue is disabled, which is a creation-time
    // flag — it cannot flip mid-loop — so the re-assignment is non-null.
    let ourId: string = initialId;
    // Track whether we have already released our waiter entry so a throw
    // between join() and the return of a release fn cannot leak the waiter
    // for staleWaiterMs (5 min) (ADV-5). waitForLaunch itself cancels on
    // signal abort; this finally covers the non-abort throw paths (lock
    // timeout, EACCES, ENOSPC) and the C1 token-reaped re-join path.
    let released = false;
    // if our launch token is reaped by a sibling's reapStale while
    // we hold it across a long capacity poll (a pause-bounded wait can
    // legitimately exceed 120s — see CORR4-3), touchToken returns false and
    // we must re-join the queue + wait our turn again rather than race a
    // concurrent send. Guarded re-entry: bail after a bounded number of
    // re-joins so a pathological state (e.g. a sibling that reaps + crashes
    // in a tight loop) cannot wedge us here forever — fall back to fail-open
    // (the watchdog bounds the sibling's hold, and the hard_cap headroom
    // absorbs one extra send, matching the /usage-unreachable stance).
    const MAX_TOKEN_REJOINS = 3;
    let releaseToken: () => void = () => {};
    try {
    tokenAcquire: for (let rejoins = 0; rejoins <= MAX_TOKEN_REJOINS; rejoins++) {
      let releaseTokenThisIter: () => void;
      try {
        releaseTokenThisIter = await concurrencyQueue.waitForLaunch(ourId, signal);
      } catch (err) {
        // waitForLaunch rejects with "waitForLaunch aborted" when the
        // signal is already aborted (or aborts mid-wait). Return undefined
        // (matching the disabled-mode shape) instead of surfacing the throw
        // as an uncaught extension error on Ctrl-C. The handler's `if (release)`
        // guard becomes the abort path. waitForLaunch already cancelled our
        // waiter entry; the finally is a no-op (released stays false → cancel
        // is a belt-and-suspenders no-op since the id is already gone).
        if (signal?.aborted) {
          released = true;
          return undefined;
        }
        throw err; // non-abort throw (lock timeout, EACCES) — propagate
      }
      releaseToken = releaseTokenThisIter;
      // We are head + hold the launch token. Poll /usage until the server reports
      // a free slot (or the plan is unlimited) and the account isn't deprioritized.
      // The token serializes the /usage POLL (no thundering herd on the capacity
      // endpoint); it is released as soon as the capacity check passes (before
      // the send), NOT held across the send. Holding the token across the send
      // serializes to 1-at-a-time (over-serialization). Releasing immediately
      // lets the next head poll right away; the server's /usage lag means it
      // sees a stale-low concurrent_sessions + launches — achieving
      // limit-concurrent saturation (4/4). The hard_cap burst headroom
      // absorbs any overshoot from the lag.
      const limit = concurrencyLimit();
      // Unlimited plan: skip the capacity check (still honor priority.low).
      // read queuePaused ONCE per poll iteration into a local const
      // + pass the same value to capacityFree + decideLaunch. Previously
      // capacityFree read concurrencyQueue.snapshot().paused inside itself
      // + decideLaunch read it again after the await — two unlocked snapshot
      // reads straddling an await, so a sibling writing pausedUntil between
      // them could let capacityFree see queuePaused:true then decideLaunch see
      // queuePaused:false + elapsedMs >= 60s -> failOpen into a pause. Reading
      // once makes the fail-open-during-pause guard structural.
      const capacityFree = async (queuePaused: boolean): Promise<boolean> => {
        // consult the SHARED pause before launching. A 429 observed by any
        // local process writes pausedUntil to the shared file; reading it here
        // makes every sibling back off immediately, even before /usage
        // propagates priority.low (5s refresh lag, or a transient gateway-side
        // blip not yet reflected in /usage). Without this, process B would see
        // priority.low === false and launch right into the 429 that A just hit.
        const snap = await fetchUsageSnapshot(apiKey, signal);
        // pass localInFlight IN (see CapacityInputs) so isCapacityFree stays
        // pure. snapshot() calls reapStale, so inflightCount is the post-reap count.
        const qSnap = concurrencyQueue.snapshot();
        const decision = isCapacityFree(snap, {
          limit,
          queuePaused,
          localInFlight: qSnap.inflightCount,
        });
        if (decision.repause) {
          // C10 write-amplification guard: skip the pauseUntil call when the
          // active pause already covers the requested deadline + reason. The
          // capacity-poll loop calls capacityFree every ~300ms; with D12, each
          // iteration where priority.low && reason=cap_abuse returns a repause,
          // + the caller pushes pauseUntil — a mutate (O_EXCL lock + readState
          // + reapStale + writeStateAtomic + renameSync) every ~300ms. Over a
          // 5h suspension that is ~60,000 lock acquisitions + file writes,
          // all no-ops at the pause level (extend-never-shorten means the
          // deadline does not move). Skip when queuePaused &&
          // qSnap.pausedUntil >= decision.repause.until &&
          // qSnap.pausedReason === decision.repause.reason. qSnap was read
          // once per iteration (no straddle-await race).
          const alreadyCovered = queuePaused &&
            qSnap.pausedUntil >= decision.repause.until &&
            qSnap.pausedReason === decision.repause.reason;
          if (!alreadyCovered) {
            // pauseUntil runs mutate -> writeStateAtomic -> renameSync,
            // which can throw on disk failure (EACCES, ENOSPC, EROFS). The pause
            // is a best-effort coordination signal (the server's priority.low +
            // the 120s watchdog bound it); it must not abort a turn that already
            // waited its FIFO place. Warn + swallow, mirroring releaseSlot's
            // ADV3-1 release-resilience pattern.
            try {
              concurrencyQueue.pauseUntil(decision.repause.until, decision.repause.reason ?? undefined);
            } catch (err) {
              console.warn("umans: pauseUntil threw in capacityFree (continuing):", err instanceof Error ? err.message : err);
            }
          }
        }
        return decision.free;
      };
      // Poll at 300ms + up to 100ms jitter while full/deprioritized. If the turn
      // is aborted mid-poll, `signal` cancels the waiter; otherwise the watchdog
      // reaps the token after >120s and session_shutdown clears the waiter on exit.
      // the ±100ms jitter breaks phase-locking across machines —
      // D1 designs for multiple machines each running their own local queue and
      // polling /usage; without jitter, N machines' head waiters synchronize on
      // the same 300ms tick and amplify /usage load N× per cycle.
      // cap the total poll elapsed at CAPACITY_POLL_TIMEOUT_MS so a
      // hostile/misbehaving /usage (always reports full, or an account stuck
      // at the cap) cannot wedge the queue forever. After the cap, fail open
      // (launch anyway) — matching the /usage-unreachable fallback's stance
      // that the queue must not block indefinitely.
      // do NOT fail open during a KNOWN active pause (shared
      // pausedUntil, e.g. a 429 the gate observed). Fail-open for an
      // unreachable /usage is fine (no signal); fail-open for a POSITIVE
      // deprio signal launches into a still-deprioritized account, risking
      // another 429 and extending the account-wide deprioritization — exactly
      // what the gate exists to prevent. Keep waiting when a known pause is
      // active; the pause has a bounded deadline (clamped to MAX_PAUSE_MS)
      // and C1's touchToken keeps the watchdog from reaping a legitimately
      // long poll (the watchdog now only reaps a TRULY hung poller).
      const pollStart = Date.now();
      // exponential backoff on the poll interval when capacity is
      // steadily full. Start at 300ms, grow by 1.5× on each "wait" decision,
      // cap at 2000ms; reset to 300ms on "launch" / "failOpen". Reduces /usage
      // RPS from ~3.3/s to ~0.5/s during a sustained pause. The ±100ms jitter
      // breaks phase-locking across machines (CORR5-4 / ADV5-2).
      let pollIntervalMs = POLL_INTERVAL_BASE_MS;
      // the branch logic lives in decideLaunch (pure, unit-tested). The
      // loop here drives the /usage fetch (capacityFree, I/O) and applies the
      // decision each iteration.
      for (;;) {
        // re-stamp our token's ts each iteration so the 120s
        // watchdog does not reap a legitimately long capacity poll. If the
        // token was already reaped by a sibling's reapStale (id mismatch or
        // absent), touchToken returns false — bail out of this poll, cancel
        // our (now-stale) waiter entry, re-join the queue, and wait our turn
        // again. The sibling that reaped + claimed is now sending; re-joining
        // serializes us behind it rather than racing a concurrent send that
        // would defeat the gate.
        if (!concurrencyQueue.touchToken(ourId)) {
          try { concurrencyQueue.cancel(ourId); } catch { /* best-effort */ }
          if (rejoins >= MAX_TOKEN_REJOINS) {
            // Pathological state: reaped + re-joined too many times. Fail
            // open rather than loop forever (bounded by the watchdog + the
            // hard_cap headroom, same stance as /usage-unreachable).
            // clear the stale releaseToken closure before break.
            // releaseToken still points at the prior iteration's closure
            // (a no-op — the token was reaped — but confusing). The returned
            // closure below calls releaseToken(); point it at an explicit
            // no-op so fail-open proceeds without holding (or pretending to
            // release) a token.
            releaseToken = () => {};
            break;
          }
          ourId = concurrencyQueue.join()!;
          // join() returns null only when the queue is disabled, which is a
          // creation-time flag — it cannot flip mid-loop. The non-null
          // assertion mirrors the initial join() above.
          continue tokenAcquire; // re-wait our turn
        }
        // read queuePaused ONCE here, pass the same value to
        // capacityFree + decideLaunch (see capacityFree def for rationale).
        const queuePaused = concurrencyQueue.snapshot().paused;
        const isFree = await capacityFree(queuePaused);
        const decision = decideLaunch({
          isFree,
          elapsedMs: Date.now() - pollStart,
          queuePaused,
          signalAborted: !!signal?.aborted,
        });
        if (decision === "launch") break; // capacity free — proceed to send
        if (decision === "abort") {
          // return undefined on abort (matching the disabled-mode shape)
          // instead of throwing. The handler's `if (release)` guard becomes
          // the abort path, so a Ctrl-C mid-poll surfaces as a clean
          // cancellation rather than an uncaught extension error toast/log.
          // We already hold the token (touchToken returned true above), so
          // release it before returning — the watchdog would reap it
          // eventually, but releasing now frees the next head immediately.
          try { releaseToken(); } catch { /* best-effort */ }
          try { concurrencyQueue.cancel(ourId); } catch { /* best-effort: COV12-1 */ }
          released = true;
          return undefined;
        }
        if (decision === "failOpen") {
          // only fail open when no known pause is active. A known
          // pause means the gate has a positive deprio signal; keep waiting
          // (bounded by the pause deadline + the 120s token watchdog).
          break; // fail open below
        }
        // decision === "wait"
        await new Promise((r) => setTimeout(r, pollIntervalMs + Math.floor(Math.random() * 100)));
        // back off the next poll interval (grows 1.5×, caps at 2000ms).
        pollIntervalMs = nextPollInterval(pollIntervalMs, "wait");
      }
      // fail-open after the cap. The turn proceeds ungated; the watchdog still bounds
      // the token hold. We deliberately do not throw — a wedged /usage should
      // not break the user's turn, only the gate. The status bar's `q <queued>*`
      // already reflects the wait; the launch itself is silent so as not to
      // spam notifies on every poll.
      //
      // D11 local in-flight tracking: add an in-flight entry BEFORE releasing
      // the token (the order is load-bearing — the next head's readState
      // must see our entry before it can claim the token, so max(localInFlight,
      // concurrent_sessions) counts us + blocks a sibling from launching into
      // our still-in-flight request). addInFlight is fail-closed (Adv5): a throw
      // (lock timeout, EACCES, ENOSPC) propagates to the finally, which cancels
      // the waiter + token + aborts the turn — do NOT swallow, a missing entry
      // deflates the gate for siblings. The returned release fn calls
      // removeInFlight (best-effort, mirroring releaseToken) in addition to the
      // existing token + waiter cleanup.
      concurrencyQueue.addInFlight(ourId);
      // Throughput fix: release the token IMMEDIATELY (not at message_end) so
      // the next head can poll + launch. The token serializes the /usage poll;
      // holding it across the send serializes to 1-at-a-time. Releasing here
      // lets the next head poll right away — the server's /usage lag means it
      // sees a stale-low concurrent_sessions + launches, achieving
      // limit-concurrent saturation. The hard_cap absorbs overshoot.
      try { releaseToken(); } catch { /* best-effort — ADV3-1 release-resilience */ }
      releaseToken = () => {}; // no-op for the returned release fn (token already released)
      released = true;
      return () => {
        releaseToken(); // no-op (token released above)
        try { concurrencyQueue.removeInFlight(ourId); } catch { /* best-effort: watchdog reaps at 120s */ }
        concurrencyQueue.cancel(ourId); // belt-and-suspenders: drop our waiter if still present (also splices in-flight, C6)
      };
    } // end tokenAcquire
    // Unreachable: the loop above always either returns or throws. Defensive.
    return undefined;
    } finally {
      // If we exited without returning a release fn (throw, abort, or a path
      // that didn't set `released`), cancel our waiter entry so it doesn't
      // pollute the FIFO for staleWaiterMs (ADV-5). On the happy path the
      // returned release fn owns the cancellation, so `released` is true
      // and this is a no-op.
      if (!released) {
        try { concurrencyQueue.cancel(ourId); } catch { /* best-effort */ }
      }
    }
  }

  async function resolveApiKey(ctx?: any): Promise<string | undefined> {
    const envKey = process.env[API_KEY_ENV]?.trim();
    if (envKey) return envKey;
    try {
      return await ctx?.modelRegistry?.getApiKeyForProvider("umans");
    } catch {
      return undefined;
    }
  }

  // === Web search (reuses the gateway's built-in Exa via a side-call) ===
  // The Umans gateway runs web search through Exa, but only when the request
  // declares the Anthropic `web_search_20250305` server tool — which pi-ai
  // cannot send (it only serializes client-side tools). So we expose a normal
  // client-side tool: the main model calls it, we make a sub-request that does
  // declare the server tool, and return the results. One extra round-trip per
  // search; no pi-ai changes required.
  //
  // Set UMANS_SEARCH_DISABLE=1 to skip registering this tool (e.g. when you
  // already expose web search via your own MCP tool and want to avoid a
  // duplicate). Vision handoff is unaffected.
  const searchDisabled = process.env[SEARCH_DISABLE_ENV] === "1";
  const searchModelId = pickSearchModel(catalog);
  if (!searchDisabled) {
  pi.registerTool({
    name: "umans_web_search",
    label: "Umans Web Search",
    description:
      "Search the web (via the Umans gateway's built-in Exa) for current or real-time information " +
      "you do not already have: recent events, live prices, latest library/SDK versions, current docs, " +
      "or date-sensitive facts. Pass a focused search query.",
    promptSnippet: "Search the web for current information",
    promptGuidelines: [
      "Use umans_web_search for current or real-time information you do not already have: recent events, live prices, latest library versions, current docs, or date-sensitive facts. Pass a focused query.",
      "Do not use it for things you already know or can derive from the codebase.",
    ],
    parameters: Type.Object({
      query: Type.String({ description: "The web search query" }),
    }),
    async execute(_toolCallId, params, signal, _onUpdate, ctx) {
      const apiKey = await resolveApiKey(ctx);
      if (!apiKey) {
        return {
          content: [{ type: "text", text: "Umans API key unavailable; cannot run web search." }],
          details: {},
        };
      }
      // The side-call consumes a concurrency slot on the account; gate it
      // through the same cross-process FIFO so a burst of searches can't push
      // the main turn past the soft cap. ADV4-3 / CORR5-3: do NOT assign to
      // mainTurnRelease — side-calls manage their own release via releaseSlot
      // in the finally below. mainTurnRelease is main-turn-only (message_end
      // releases it); a side-call assigned there could be released instead of
      // the main turn's slot.
      const release = await acquireSlot(apiKey, signal);
      try {
        const results = await searchWeb(apiKey, searchModelId, baseUrl, params.query, signal, concurrencyQueue);
        return { content: [{ type: "text", text: results }], details: {} };
      } catch (err) {
        const m = err instanceof Error ? err.message : String(err);
        return { content: [{ type: "text", text: `Web search failed: ${m}` }], details: {} };
      } finally {
        releaseSlot(release);
      }
    },
  });
  }

  // === Client-side vision handoff (see module-level docs) ===
  // Mutable at runtime via the /umans-vision command; env vars only seed the
  // initial value (handy for headless/print mode). Read at call time by the
  // message_end handler and the umans_vision tool so command changes apply
  // immediately, without a /reload.
  let visionDisabled = process.env[VISION_DISABLE_ENV] === "1";
  let visionModelId = pickVisionModel(catalog);
  const hasViaHandoffModel = Object.values(catalog).some(
    (m) => !m.deprecation && m.capabilities?.supports_vision === "via-handoff",
  );

  function isViaHandoffUmans(modelId?: string): boolean {
    if (!modelId) return false;
    return catalog[modelId]?.capabilities?.supports_vision === "via-handoff";
  }

  function nativeVisionModelIds(): string[] {
    return Object.entries(catalog)
      .filter(([, info]) => isNativeVision(info))
      .map(([id]) => id);
  }

  function setVisionStatus(ctx: any, text: string | undefined) {
    try {
      ctx?.ui?.setStatus("umans-vision", text);
    } catch {
      // UI not available (print/json mode) — ignore.
    }
  }

  // Returns a copy of `message` with every image block replaced by an
  // `[Image analysis (image:ID)]: ...` text block. Returns undefined when there
  // are no images to transform. Image bytes are cached in `imageStore` keyed by
  // a content hash so the `umans_vision` tool can re-query them later.
  async function transformMessageImages(message: any, apiKey: string, ctx: any) {
    const content = Array.isArray(message.content) ? message.content : null;
    if (!content) return undefined;
    const imageIndices: number[] = [];
    for (let i = 0; i < content.length; i++) {
      if (content[i]?.type === "image") imageIndices.push(i);
    }
    if (imageIndices.length === 0) return undefined;
    if (!visionModelId) return undefined; // nothing to analyze with
    const model = visionModelId;

    setVisionStatus(
      ctx,
      `Umans vision: analyzing ${imageIndices.length} image${imageIndices.length > 1 ? "s" : ""}…`,
    );
    const replacements = new Map<number, { type: "text"; text: string }>();
    await Promise.all(
      imageIndices.map(async (i) => {
        const img = content[i];
        const id = hashImageId(img.data);
        imageStore.set(id, { data: img.data, mimeType: img.mimeType });
        let analysis: string;
        // Gate the vision side-call through the same cross-process FIFO so a
        // multi-image handoff can't push the main turn past the soft cap.
        // do NOT assign to mainTurnRelease — side-calls
        // manage their own release via releaseSlot in the finally below.
        const release = await acquireSlot(apiKey, ctx?.signal);
        try {
          // pass ctx?.signal so an aborted turn aborts the vision HTTP
          // fetch (the tool path at umans_vision already passes `signal`; only
          // the handoff path dropped it). The 60s VISION_TIMEOUT_MS still
          // bounds the worst case, but passing the signal makes the handoff
          // consistent with the tool + searchWeb.
          analysis = await analyzeImage(
            apiKey,
            model,
            baseUrl,
            { data: img.data, mimeType: img.mimeType },
            VISION_ANALYSIS_PROMPT,
            ctx?.signal,
            concurrencyQueue,
          );
        } catch (err) {
          const m = err instanceof Error ? err.message : String(err);
          analysis = `analysis unavailable (${m}); call the umans_vision tool with image id ${id} to retry`;
        } finally {
          releaseSlot(release);
        }
        replacements.set(i, {
          type: "text",
          text: `[Image analysis (image:${id})]: ${analysis}`,
        });
      }),
    );
    setVisionStatus(ctx, undefined);
    const newContent = content.map((b: any, i: number) => replacements.get(i) ?? b);
    return { ...message, content: newContent };
  }

  // The umans_vision follow-up tool + image interception register once (when
  // the catalog has any via-handoff model) and read the live visionDisabled /
  // visionModelId at call time, so /umans-vision can flip them without /reload.
  if (hasViaHandoffModel) {
    pi.registerTool({
      name: "umans_vision",
      label: "Umans Vision Follow-up",
      description:
        "Ask the Umans vision model a targeted question about an image that was summarized into an " +
        "`[Image analysis (image:ID)]` block. Use when the initial summary omits a specific detail you " +
        "need (text, region, color, layout). Pass the image ID from the block and your question.",
      promptSnippet: "Ask the vision model a targeted follow-up about an analyzed image",
      promptGuidelines: [
        "Use umans_vision to ask a targeted follow-up about any `[Image analysis (image:ID)]` block " +
          "when the initial summary lacks a specific detail you need (text, region, color, layout). " +
          "Pass the image ID and your question.",
      ],
      parameters: Type.Object({
        image_id: Type.String({
          description: "Image ID from the `[Image analysis (image:ID)]` block",
        }),
        question: Type.String({
          description: "The specific question to answer about the image",
        }),
      }),
      async execute(_toolCallId, params, signal, _onUpdate, ctx) {
        const image = imageStore.get(params.image_id);
        if (!image) {
          return {
            content: [
              {
                type: "text",
                text:
                  `Image ${params.image_id} is not available in this session ` +
                  "(it predates the session or the session was reloaded). " +
                  "Only the initial analysis in the conversation remains.",
              },
            ],
            details: {},
          };
        }
        const apiKey = await resolveApiKey(ctx);
        if (!apiKey) {
          return {
            content: [{ type: "text", text: "Umans API key unavailable; cannot query the vision model." }],
            details: {},
          };
        }
        if (!visionModelId) {
          return {
            content: [{ type: "text", text: "No vision model configured. Set one with /umans-vision model <id>." }],
            details: {},
          };
        }
        const model = visionModelId;
        // do NOT assign to mainTurnRelease — side-calls
        // manage their own release via releaseSlot in the finally below.
        const release = await acquireSlot(apiKey, signal);
        try {
          const answer = await analyzeImage(apiKey, model, baseUrl, image, params.question, signal, concurrencyQueue);
          return { content: [{ type: "text", text: answer }], details: {} };
        } catch (err) {
          const m = err instanceof Error ? err.message : String(err);
          return { content: [{ type: "text", text: `Vision follow-up failed: ${m}` }], details: {} };
        } finally {
          releaseSlot(release);
        }
      },
    });

    // Intercept images headed to a via-handoff (text-only) Umans model and
    // replace them with persisted analysis text. Runs on the finalized user /
    // toolResult message, before the first LLM `context` deep-copy, so the text
    // model never sees the raw image and the analysis sticks in history.
    pi.on("message_end", async (event, ctx) => {
      if (ctx.model?.provider !== "umans") return;
      if (!isViaHandoffUmans(ctx.model?.id)) return;
      const msg = event.message as any;
      if (msg.role !== "user" && msg.role !== "toolResult") return;
      const content = msg.content;
      if (!Array.isArray(content) || !content.some((b: any) => b?.type === "image")) return;
      if (visionDisabled) return; // opted out via /umans-vision off → gateway-side handoff
      if (!visionModelId) {
        ctx.ui?.notify?.(
          "Umans vision handoff skipped: no vision model. Run /umans-vision model <id>.",
          "warning",
        );
        return;
      }
      const apiKey = await resolveApiKey(ctx);
      // ponytail: no key — leave the image; the text-model call fails anyway.
      if (!apiKey) return;
      const imageCount = content.filter((b: any) => b?.type === "image").length;
      ctx.ui?.notify?.(
        `Umans vision handoff: analyzing ${imageCount} image${imageCount > 1 ? "s" : ""} with ${visionModelId}`,
        "info",
      );
      try {
        const transformed = await transformMessageImages(msg, apiKey, ctx);
        if (transformed) return { message: transformed };
      } catch (err) {
        const m = err instanceof Error ? err.message : String(err);
        ctx.ui?.notify?.(`Umans vision handoff failed: ${m}`, "error");
      }
    });

    // /umans-vision: live control of the client-side handoff (replaces env vars
    // for session-time use; env vars above still seed the initial value).
    pi.registerCommand("umans-vision", {
      description: "Umans vision handoff: show status, on/off, or pick the vision model",
      getArgumentCompletions(prefix: string) {
        const ids = nativeVisionModelIds();
        if (prefix.startsWith("model")) {
          const rest = prefix.slice("model".length).trimStart();
          return ids
            .filter((id) => id.startsWith(rest))
            .map((value) => ({ value, label: value }));
        }
        return ["on", "off", "model"]
          .filter((s) => s.startsWith(prefix.trimStart()))
          .map((value) => ({ value, label: value }));
      },
      handler: async (args: string, ctx) => {
        const parts = args.trim().split(/\s+/).filter(Boolean);
        if (parts.length === 0) {
          ctx.ui.notify(
            `Umans vision: ${visionDisabled ? "off" : "on"} | model: ${visionModelId ?? "(none)"} | available: ${nativeVisionModelIds().join(", ") || "none"}`,
            "info",
          );
          return;
        }
        const sub = parts[0];
        if (sub === "on") {
          visionDisabled = false;
          ctx.ui.notify("Umans vision handoff enabled", "info");
          return;
        }
        if (sub === "off") {
          visionDisabled = true;
          ctx.ui.notify("Umans vision handoff disabled (gateway-side fallback)", "info");
          return;
        }
        if (sub === "model") {
          const available = nativeVisionModelIds();
          const id = parts[1];
          if (!id) {
            ctx.ui.notify(
              `Vision model: ${visionModelId ?? "(none)"} | available: ${available.join(", ") || "none"}`,
              "info",
            );
            return;
          }
          if (!available.includes(id)) {
            ctx.ui.notify(
              `Unknown vision model: ${id} | available: ${available.join(", ") || "none"}`,
              "error",
            );
            return;
          }
          visionModelId = id;
          ctx.ui.notify(`Vision model set to ${id}`, "info");
          return;
        }
        ctx.ui.notify("Usage: /umans-vision [on|off|model [id]]", "info");
      },
    });
  }

  // /umans-concurrency: operator control of the cross-process FIFO gate.
  // wires clearPause({force:true}) + reset() to a real caller so the
  // `force` option is not a speculative-caller export. `reset` clears a
  // poisoned pause (e.g. a stale 429-origin pause wedging the queue) and this
  // process's own waiter/token entry — useful for un-wedging without editing
  // ~/.pi/agent/umans-concurrency.json by hand.
  pi.registerCommand("umans-concurrency", {
    description: "Umans concurrency queue: show status, or force-reset the pause/queue",
    getArgumentCompletions(prefix: string) {
      return ["status", "reset"]
        .filter((s) => s.startsWith(prefix.trimStart()))
        .map((value) => ({ value, label: value }));
    },
    handler: async (args: string, ctx) => {
      const parts = args.trim().split(/\s+/).filter(Boolean);
      const sub = parts[0] ?? "status";
      if (sub === "status") {
        if (concurrencyDisabled) {
          ctx.ui.notify("Umans concurrency queue: disabled (UMANS_CONCURRENCY_DISABLE=1)", "info");
          return;
        }
        const snap = concurrencyQueue.snapshot();
        const paused = snap.paused
          ? `paused ${Math.max(0, Math.round((snap.pausedUntil - Date.now()) / 1000))}s${snap.pausedReason ? ` (${snap.pausedReason})` : ""}`
          : "running";
        ctx.ui.notify(`Umans concurrency: queued=${snap.queued} tokenHeld=${snap.tokenHeld} ${paused}`, "info");
        return;
      }
      if (sub === "reset") {
        if (concurrencyDisabled) {
          ctx.ui.notify("Umans concurrency queue: disabled — nothing to reset", "info");
          return;
        }
        // Force-clear any pause (incl. a 429-origin pause that would otherwise
        // survive until it naturally elapses) + drop this process's own
        // waiter/token entry. Does NOT unlink the shared state file (siblings
        // may still be queued).
        try {
          concurrencyQueue.clearPause({ force: true });
        } catch (err) {
          ctx.ui.notify(`Umans concurrency: clearPause threw: ${err instanceof Error ? err.message : err}`, "error");
        }
        try {
          concurrencyQueue.reset();
        } catch (err) {
          ctx.ui.notify(`Umans concurrency: reset threw: ${err instanceof Error ? err.message : err}`, "error");
        }
        ctx.ui.notify("Umans concurrency: pause force-cleared + own waiter/token entry reset", "info");
        return;
      }
      ctx.ui.notify("Usage: /umans-concurrency [status|reset]", "info");
    },
  });

  // === Concurrency queue: hold the outbound request until a slot is free ===
  // before_provider_request fires after the payload is built, right before the
  // HTTP send, and is awaited by pi. We join the cross-process FIFO here so the
  // main turn blocks (queued) rather than hitting the server and risking a 429.
  //
  // Token-release contract (D2, revised for throughput): the token is
  // released IMMEDIATELY after the capacity check passes (before the send),
  // NOT at assistant message_end. The token's job is to serialize the
  // /usage poll (no thundering herd on the capacity endpoint); it must NOT
  // serialize the send itself. Holding the token across the send serialized
  // to 1-at-a-time (over-serialization, peak 1-2 instead of 4/4).
  //
  // Releasing immediately lets the next head poll right away. The server's
  // /usage concurrent_sessions lag means the next head sees a stale-low count
  // + launches — achieving limit-concurrent saturation (4/4). The hard_cap
  // burst headroom (hard_cap - limit = 4) absorbs any overshoot from the lag.
  //
  // The SLOT (the actual concurrency slot on the server) is tracked by the
  // request lifecycle: the server increments concurrent_sessions when the
  // request streams, decrements when it completes. We don't track a local
  // in-flight count (D1: /v1/usage is the only capacity authority); the
  // server lag + hard_cap headroom are the design's throughput mechanism.
  //
  // The message_end release is now a no-op (the token + waiter are already
  // released); it's kept as a safety net for turns that error before the
  // capacity check completes (the abort + rejoin-exhaustion paths).
  //
  // the main-turn release is tracked in a SINGLE slot
  // (mainTurnRelease), not a Set. The design guarantees at most one main-turn
  // slot is outstanding (side-calls manage their own release in a finally and
  // never register here), so a Set + FIFO-by-insertion release design was a
  // latent footgun: if a future change ever added a second entry, message_end
  // would release the oldest (possibly a side-call acquired before the main
  // turn) instead of the main turn's slot, leaking the token until the safety
  // nets. A single tracked slot makes the invariant structural — there is no
  // ordering to get wrong.
  let mainTurnRelease: Release | undefined;
  // release() calls mutate() -> withLock -> acquireLock, which can
  // throw (e.g. O_EXCL lock timeout after 2s per CMP-MED-2, EACCES, ENOSPC).
  // A throw propagating out of releaseSlot would abort the caller (message_end /
  // turn_end / agent_end / session_shutdown), leaking the token until the 120s
  // watchdog. Wrap release() in a try/catch: on throw, warn (the lock-timeout
  // is transient; the watchdog will reap the stale token/waiter) and swallow
  // so the single-slot release completes. Keep the slot clear + updateStatus in
  // a finally so the slot is released even on throw — otherwise a
  // repeatedly-throwing slot would loop forever.
  function releaseSlot(release: Release | undefined): void {
    if (!release) return;
    try {
      try {
        release();
      } catch (err) {
        // Transient (lock timeout) or environmental (EACCES/ENOSPC); the
        // 120s watchdog reaps the stale token/waiter entry regardless.
        console.warn("umans: concurrency release threw (release continues):", err instanceof Error ? err.message : err);
      }
    } finally {
      if (mainTurnRelease === release) mainTurnRelease = undefined;
      updateStatus(undefined as any);
    }
  }
  // Release the main-turn slot if held. Called at assistant message_end
  // (primary), turn_end / agent_end (safety nets), and session_shutdown
  // (cleanup). At most one main-turn slot is ever outstanding (CORR5-3), so a
  // single release is sufficient.
  function releaseMainTurn(): void {
    releaseSlot(mainTurnRelease);
  }

  pi.on("before_provider_request", async (_event, ctx) => {
    if (ctx.model?.provider !== "umans") return;
    if (concurrencyDisabled) return;
    const apiKey = await resolveApiKey(ctx);
    if (!apiKey) return; // no key — let the request fail naturally
    // acquireSlot joins the FIFO, waits for the token, polls /usage until the
    // server reports a free slot, then returns a release fn. Per D2 we hold the
    // token ACROSS the send (stored in mainTurnRelease) and release it at
    // assistant message_end (stream completed) — NOT inline before the send
    // (the prior release-token-immediately-after-launch design defeated serialization: siblings all polled
    // /usage, all saw capacity, all released, and all sent simultaneously —
    // empirically peak 4 vs limit 2 (C1)), and NOT at after_provider_response
    // headers (the server hasn't registered the request as in-flight until the
    // body streams). message_end frees the slot during tool execution too; the
    // release race (message_end precedes the server decrement) is absorbed by
    // the burst headroom (hard_cap - limit) since the gate compares against
    // `limit`, not `hard_cap`. turn_end and agent_end are safety
    // nets for turns that never reach message_end.
    // wrap acquireSlot in try/catch so a wedged lock (ADV12-1
    // future-dated mtime) or transient disk error (EACCES/ENOSPC/EROFS/ENOENT
    // on the lockfile or state file) does NOT break the user's turn as an
    // uncaught extension error. The queue must not break inference, only the
    // gate. Fail-open ungated (proceed without a release fn), matching the
    // /usage-unreachable stance at isCapacityFree + the ADV-3 poll-timeout
    // fail-open. The watchdog + hard_cap burst headroom absorb one ungated
    // send. This mirrors the COV4-2 hardening applied to pauseUntil/handle429.
    let release: Release | undefined;
    try {
      release = await acquireSlot(apiKey, ctx.signal);
    } catch (err) {
      ctx.ui?.notify?.(
        `Umans concurrency queue: gating unavailable (${err instanceof Error ? err.message : String(err)}); proceeding ungated.`,
        "warning",
      );
      release = undefined; // fail-open ungated
    }
    if (release) {
      // wrap acquire + register in a try/finally so a throw between
      // acquireSlot resolving and the safety-net registration (message_end /
      // turn_end / agent_end / session_shutdown) doesn't leak the token until
      // the 120s watchdog. On the happy path the release fn is owned by
      // mainTurnRelease and released at message_end; a throw here releases it
      // immediately. (updateStatus swallows internally, but this guards any
      // future throw in the registration path.)
      let registered = false;
      try {
        // guard against same-turn retry clobber. If pi fires
        // before_provider_request twice for the same turn without an
        // intervening message_end/turn_end (a retry), the second acquireSlot
        // would overwrite mainTurnRelease, orphaning the first release fn +
        // leaking its token until the 120s watchdog. Release the prior slot
        // before overwriting to keep the single-slot invariant structural.
        if (mainTurnRelease) {
          releaseSlot(mainTurnRelease);
          mainTurnRelease = undefined;
        }
        mainTurnRelease = release;
        registered = true;
        updateStatus(ctx);
      } finally {
        if (!registered) releaseSlot(release);
      }
    }
  });

  pi.on("after_provider_response", async (event, ctx) => {
    if (ctx.model?.provider !== "umans") return;
    if (concurrencyDisabled) return;
    // The launch token is NOT released here. after_provider_response fires at
    // HTTP headers (~1s in), but the request stays in-flight on the server
    // until the body stream completes — so the next waiter's /usage poll would
    // see stale capacity and launch too (peak 4 vs limit 2). The token is held
    // until assistant message_end (the primary release path) or turn_end /
    // agent_end (the safety nets). Here we only intercept 429s to extend
    // the shared pause window so sibling processes back off instead of
    // immediately re-launching. Per Umans docs, each 429 deprioritizes the
    // account for ~30 min (Retry-After overrides).
    if (event.status === 429) {
      // delegate to the shared handle429 helper (also used by
      // analyzeImage + searchWeb) so every 429 site parses Retry-After the
      // same way + pushes the shared pause with the same PAUSE_REASON_429 tag.
      const until = handle429(event, concurrencyQueue);
      ctx.ui?.notify?.(
        `Umans 429: pausing new turns ${Math.round((until - Date.now()) / 1000)}s to avoid account deprioritization.`,
        "warning",
      );
    }
    // 403 bridge: the body is unavailable at headers time, so push the
    // non-sticky PAUSE_REASON_403_BRIDGE (see the const) + reconcile at
    // message_end. The side-call path body-checks before pausing; this main-turn
    // path cannot, so the bridge narrows the false-positive blast radius.
    if (event.status === 403) {
      const until = Date.now() + PAUSE_403_BRIDGE_MS;
      try {
        concurrencyQueue.pauseUntil(until, PAUSE_REASON_403_BRIDGE);
      } catch (err) {
        console.warn("umans: pauseUntil threw in 403 main-turn handler (continuing):", err instanceof Error ? err.message : err);
      }
      ctx.ui?.notify?.(
        `Umans 403 (possible suspension, awaiting body confirmation): pausing new turns ${Math.round((until - Date.now()) / 1000)}s.`,
        "warning",
      );
    }
  });

  pi.on("session_start", async (_event, ctx) => {
    imageStore.clear();
    const apiKey = await resolveApiKey(ctx);
    if (ctx.model?.provider === "umans") {
      if (apiKey) await refreshUsage(apiKey);
      restartRefreshLoop(apiKey || "");
      updateStatus(ctx);
    }
  });

  pi.on("model_select", async (event, ctx) => {
    const provider = event.model.provider;
    if (provider !== "umans") {
      stopRefreshLoop();
      setWidget(ctx, undefined);
      liveRequest = undefined;
      lastMetrics = {};
      return;
    }
    updateStatus(ctx);
    const apiKey = await resolveApiKey(ctx);
    if (apiKey) await refreshUsage(apiKey);
    restartRefreshLoop(apiKey || "");
  });

  // turn_start opens the TTFT clock: it fires before API-key/HTTP/prefill, so TTFT
  // spans the full send→first-token gap, not just the stream body from message_start.
  pi.on("turn_start", async (event, ctx) => {
    if (ctx.model?.provider !== "umans") return;
    liveRequest = { startTime: event.timestamp, estimatedTokens: 0, lastStatusUpdate: 0 };
    updateStatus(ctx);
  });

  pi.on("turn_end", async (_event, ctx) => {
    if (ctx.model?.provider !== "umans") return;
    // Safety net: release any slot still held (e.g. a turn that errored before
    // the assistant message_end fired). The primary release is at assistant
    // message_end (below) so the slot frees as soon as the response stream
    // completes, letting siblings run during this turn's tool execution.
    releaseMainTurn();
    updateStatus(ctx);
  });
  pi.on("message_update", async (event, ctx) => {
    if (ctx.model?.provider !== "umans") return;
    const req = liveRequest;
    if (!req) return;
    const now = Date.now();
    const ev = event.assistantMessageEvent as any;
    let delta = "";
    if (ev?.type === "text_delta") delta = String(ev.delta ?? "");
    else if (ev?.type === "thinking_delta") delta = String(ev.delta ?? "");

    if (delta) {
      if (!req.firstTokenTime) req.firstTokenTime = now;
      req.estimatedTokens += Math.max(1, Math.round(delta.length / 4));
      const elapsedSec = req.firstTokenTime ? (now - req.firstTokenTime) / 1000 : 0;
      if (elapsedSec > 0 && now - req.lastStatusUpdate > STATUS_UPDATE_INTERVAL_MS) {
        const tps = computeCumulativeTps(req, now);
        updateStatus(ctx, { tps, ttft: req.firstTokenTime - req.startTime });
        req.lastStatusUpdate = now;
      }
    }
  });

  pi.on("message_end", async (event, ctx) => {
    const msg = event.message as any;
    // the release guard is a pure decision (shouldReleaseOnMessageEnd)
    // so the "release only on an Umans assistant message" invariant is unit-
    // testable. User messages, tool results, and non-Umans providers are no-ops.
    if (!shouldReleaseOnMessageEnd(msg, msg?.provider ?? ctx.model?.provider)) return;
    // 403 bridge reconciliation: clear the lingering non-sticky bridge now
    // that the body has streamed. The sticky guard inside clearPause's mutate
    // lock prevents wiping a sibling's PAUSE_REASON_CAP_ABUSE pause that may
    // have landed in the window. The snapshot pre-check (reason ===
    // PAUSE_REASON_403_BRIDGE) is a cheap avoid-the-write skip. This handler
    // does NOT re-derive suspension from the assistant message's errorMessage
    // — that prose is the SDK-parsed `error.message` with sibling fields (e.g.
    // boxed_until) dropped, so it is an unreliable signal for suspension.
    if (!concurrencyDisabled && msg?.stopReason === "error" && typeof msg?.errorMessage === "string") {
      const snap = concurrencyQueue.snapshot();
      if (snap.paused && snap.pausedReason === PAUSE_REASON_403_BRIDGE) {
        try {
          concurrencyQueue.clearPause();
        } catch (err) {
          console.warn("umans: clearPause threw in message_end 403 reconciliation (continuing):", err instanceof Error ? err.message : err);
        }
      }
    }
    // Primary release path (D2): the assistant response stream completed, freeing
    // the slot for this turn's tool execution (tools don't consume a server
    // concurrency slot). NOTE: message_end fires at CLIENT-side stream
    // completion, which PRECEDES the server's concurrent_sessions decrement by a
    // network RTT + cleanup lag, so the next waiter's /usage poll can
    // transiently see stale capacity and launch 1-2 over `limit`; the gate
    // compares against `limit` (not `hard_cap`) so the burst headroom
    // (hard_cap - limit) absorbs that overshoot → no 429, no
    // deprioritization (see isCapacityFree). turn_end and agent_end
    // remain as safety nets for turns that never reach here.
    releaseMainTurn();
    const req = liveRequest;
    let ttft: number | undefined;
    let tps: number | undefined;
    if (req) {
      ttft = req.firstTokenTime ? req.firstTokenTime - req.startTime : undefined;
      // Compute final TPS from the cumulative live count, excluding tool-call
      // JSON so a big tool argument dump does not spike TPS.
      tps = computeCumulativeTps(req, Date.now());
      liveRequest = undefined;
    }
    updateStatus(ctx, { ttft, tps });
  });

  // Safety nets: if anything aborts or finishes without firing message_end/turn_end,
  // reset counters so the status bar never stays inflated.
  pi.on("agent_end", async (_event, ctx) => {
    if (ctx.model?.provider !== "umans") return;
    liveRequest = undefined;
    // Release any slot still held (e.g. aborted turns that never reached
    // message_end / turn_end) so the gate never deadlocks. CORR5-3: at most one
    // main-turn slot is outstanding, so a single release is sufficient.
    releaseMainTurn();
    updateStatus(ctx);
  });

  pi.on("session_shutdown", async (_event, ctx) => {
    stopRefreshLoop();
    liveRequest = undefined;
    lastMetrics = {};
    imageStore.clear();
    // release the main-turn slot by invoking its release fn (not just
    // dropping the reference), so the token/waiter entry is cleaned up. CORR5-3:
    // at most one main-turn slot is outstanding (side-calls manage their own
    // release in a finally). reset() only clears ourTokenId's entry, so
    // releasing here ensures the held slot's token/waiter is released.
    releaseMainTurn();
    concurrencyQueue.reset();
    setWidget(ctx, undefined);
  });

}


