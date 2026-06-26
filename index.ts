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
  MAX_PAUSE_429_MS,
  type ConcurrencyQueue,
  type PriorityState,
} from "./concurrency-queue.ts";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { OAuthCredentials, OAuthLoginCallbacks } from "@earendil-works/pi-ai";
import { Type } from "typebox";

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
const USER_AGENT = "pi-umans-provider/1.4.0";
const STATUS_UPDATE_INTERVAL_MS = 1000;

// Client-side vision handoff env + tuning. See header doc for the design.
const VISION_DISABLE_ENV = "UMANS_VISION_DISABLE";
const VISION_MODEL_ENV = "UMANS_VISION_MODEL";
const SEARCH_DISABLE_ENV = "UMANS_SEARCH_DISABLE";
const CONCURRENCY_DISABLE_ENV = "UMANS_CONCURRENCY_DISABLE";
const CONCURRENCY_LIMIT_ENV = "UMANS_CONCURRENCY_LIMIT";
const VISION_MAX_TOKENS = 1024;
const VISION_TIMEOUT_MS = 60_000;
const VISION_ANALYSIS_PROMPT =
  "You are a vision assistant for a text-only coding model. Analyze the attached image thoroughly but concisely. " +
  "Capture: any visible text (verbatim), UI/layout, code/errors/stack traces, diagrams/charts, and other notable details. " +
  "Write a compact structured report. Do not speculate beyond what is visible.";

// Web search side-call tuning. See searchWeb / the umans_web_search tool.
const SEARCH_TIMEOUT_MS = 30_000;
const SEARCH_MAX_TOKENS = 2048;

// CLN2-L3: PRIORITY_BACKOFF_MS is imported from concurrency-queue.ts (the single
// source of truth — it's also the parsePriority fallback for a null boxed_until).
// ADV-3: max time the head-waiter capacity poll will wait for a free slot
// before failing open (launching anyway). Bounds the queue against a
// hostile/misbehaving /usage that always reports full.
const CAPACITY_POLL_TIMEOUT_MS = 60_000;

/**
 * COV5-1: pure decision extracted from acquireSlot's capacity-poll loop so the
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
export type LaunchDecision = "launch" | "wait" | "failOpen" | "abort";
export function decideLaunch(opts: {
  isFree: boolean;
  elapsedMs: number;
  queuePaused: boolean;
  signalAborted: boolean;
}): LaunchDecision {
  if (opts.isFree) return "launch";
  if (opts.signalAborted) return "abort";
  if (opts.elapsedMs >= CAPACITY_POLL_TIMEOUT_MS && !opts.queuePaused) return "failOpen";
  return "wait";
}

/**
 * COV5-2: pure decision extracted from the message_end handler's release guard
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

export function hashImageId(data: string): string {
  return "img_" + createHash("sha256").update(data).digest("hex").slice(0, 8);
}

/*
 * Concurrency gating moved to ./concurrency-queue.ts (file-backed FIFO shared
 * across pi processes via ~/.pi/agent/umans-concurrency.json).
 */
// CLN2-L2: ConcurrencyQueue is imported directly (line 45) and used at the
// factory call site. CLN4-1: the speculative type exports (QueueState /
// WaiterEntry / TokenState / QueueConfig / CapacitySnapshot / CapacityInputs)
// and newId were unexported — they had no in-repo consumer (not even tests).
// External consumers should import from concurrency-queue.ts directly.
// Local type alias for the release function returned by acquireSlot.
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
): Promise<string> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), VISION_TIMEOUT_MS);
  const onAbort = () => ctrl.abort();
  if (signal) {
    if (signal.aborted) ctrl.abort();
    else signal.addEventListener("abort", onAbort, { once: true });
  }
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
      signal: ctrl.signal,
    });
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      throw new Error(`HTTP ${res.status}${txt ? `: ${txt.slice(0, 200)}` : ""}`);
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
    if (signal) signal.removeEventListener("abort", onAbort);
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
): Promise<string> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), SEARCH_TIMEOUT_MS);
  const onAbort = () => ctrl.abort();
  if (signal) {
    if (signal.aborted) ctrl.abort();
    else signal.addEventListener("abort", onAbort, { once: true });
  }
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
      signal: ctrl.signal,
    });
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      throw new Error(`HTTP ${res.status}${txt ? `: ${txt.slice(0, 200)}` : ""}`);
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
    if (signal) signal.removeEventListener("abort", onAbort);
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

  // Cross-process FIFO queue over outbound Umans requests, backed by
  // ~/.pi/agent/umans-concurrency.json (O_EXCL lockfile + atomic rename). The
  // file is a PURE WAITER QUEUE + launch token; capacity is decided solely by
  // the live /v1/usage response polled by the head waiter, so multiple pi
  // processes (and multiple machines) coordinate through the server, not a
  // local count. UMANS_CONCURRENCY_DISABLE opts out (fire-and-forget).
  // UMANS_CONCURRENCY_LIMIT is now only a display/testing hint: when set, the
  // capacity check uses it instead of the server's limits.concurrency.limit.
  const concurrencyDisabled = process.env[CONCURRENCY_DISABLE_ENV] === "1";
  const concurrencyQueue: ConcurrencyQueue = createConcurrencyQueue({ disabled: concurrencyDisabled });
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
  let refreshStopped = false;

  function stopRefreshLoop() {
    refreshStopped = true;
    if (refreshTimer) {
      clearTimeout(refreshTimer);
      refreshTimer = undefined;
    }
  }

  function restartRefreshLoop(apiKey: string) {
    stopRefreshLoop();
    refreshStopped = false;
    scheduleRefresh(apiKey);
  }

  function scheduleRefresh(apiKey: string) {
    if (refreshStopped || !apiKey) return;
    refreshTimer = setTimeout(async () => {
      await refreshUsage(apiKey);
      scheduleRefresh(apiKey);
    }, 5000);
  }

  function computeCumulativeTps(req: LiveRequest, now: number): number {
    if (!req.firstTokenTime || req.estimatedTokens <= 0) return 0;
    const elapsedSec = (now - req.firstTokenTime) / 1000;
    // Wait a moment so a tiny first chunk does not create a wild initial value.
    if (elapsedSec < 0.5) return 0;
    return Math.round(req.estimatedTokens / elapsedSec);
  }

  function statusText(metrics?: { ttft?: number; tps?: number }) {
    const parts: string[] = [];
    if (metrics?.ttft !== undefined) parts.push(`TTFT ${metrics.ttft}ms`);
    if (metrics?.tps !== undefined) parts.push(`TPS ${metrics.tps}`);
    // The effective cap honors UMANS_CONCURRENCY_LIMIT over the live server
    // value, so the bar reflects what the queue capacity check uses.
    const effectiveLimit = concurrencyLimit();
    const guaranteed = effectiveLimit !== undefined ? String(effectiveLimit) : "?";
    // Real account-wide conc from /v1/usage (includes other clients/machines).
    // Refreshed only by the 5s poll; shows "?" until first poll lands.
    const current = currentConcurrency !== undefined ? String(currentConcurrency) : "?";
    parts.push(`Conc ${current}/${guaranteed}`);
    // Only show request usage when the plan has a hard limit (e.g. the $20 tier).
    if (requestsUsed !== undefined && requestLimit !== undefined) {
      parts.push(`Req ${requestsUsed}/${requestLimit}`);
    }
    // Cross-process queue: queued waiters across all local pi processes, plus
    // a launch/PAUSED indicator. `tokenHeld` means this process is polling
    // /usage or mid-send; `queued` is the FIFO depth in the shared file.
    if (!concurrencyDisabled) {
      const snap = concurrencyQueue.snapshot();
      if (snap.queued > 0 || snap.tokenHeld) {
        parts.push(`q ${snap.queued}${snap.tokenHeld ? "*" : ""}`);
      }
      if (snap.paused) {
        const secs = Math.max(0, Math.round((snap.pausedUntil - Date.now()) / 1000));
        // CMP-LOW-4: surface the pause reason (e.g. "HTTP 429 from gateway", or
        // the server's priority.reason) alongside the deadline so the user
        // knows WHY the account is backed off, not just how long.
        const reason = snap.pausedReason ? ` (${snap.pausedReason})` : "";
        parts.push(`PAUSED ${secs}s${reason}`);
      }
    }
    return `Umans ${parts.join(" │ ")}`;
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
  // handle — refreshUsage leaves cached values, fetchUsageSnapshot retries).
  async function fetchUsage(apiKey: string, timeoutMs: number): Promise<{
    limits?: { concurrency?: { limit?: number; hard_cap?: number }; requests?: { limit?: number } };
    usage?: { requests_in_window?: number; concurrent_sessions?: number; priority?: unknown };
  } | null> {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      const res = await fetch(`${baseUrl}/v1/usage`, {
        signal: ctrl.signal,
        headers: {
          Authorization: `Bearer ${apiKey}`,
          Accept: "application/json",
          "User-Agent": USER_AGENT,
        },
      });
      if (!res.ok) return null;
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

  async function refreshUsage(apiKey: string) {
    const data = await fetchUsage(apiKey, 5000);
    if (!data) return; // leave cached values; status bar will show "?"
    // null ?? undefined normalizes unlimited (null) limits so the display
    // guards below hide them instead of rendering "x/null".
    guaranteedConcurrency = data.limits?.concurrency?.limit ?? undefined;
    currentConcurrency = data.usage?.concurrent_sessions;
    requestLimit = data.limits?.requests?.limit ?? undefined;
    requestsUsed = data.usage?.requests_in_window;
    // Reconcile the shared pause window with the server's priority state.
    // priority.low is account-wide, so any pi process seeing it pauses all of
    // them via the shared queue file; clearing it when low===false lets the
    // queue drain as soon as the server says traffic is healthy again.
    // CLN2-L1: priorityState is a local const here (it was a module-level let
    // read only by this writer — no other handler or the status bar reads it;
    // the status bar reads concurrencyQueue.snapshot() for paused state).
    const priority = parsePriority(data.usage?.priority);
    if (priority.low) {
      // COV4-2: pauseUntil can throw on disk failure (EACCES/ENOSPC/EROFS).
      // The pause is a best-effort coordination signal; warn + swallow so the
      // 5s refreshUsage timer doesn't propagate a disk error.
      try {
        concurrencyQueue.pauseUntil(priority.until, priority.reason ?? undefined);
      } catch (err) {
        console.warn("umans: pauseUntil threw in refreshUsage (continuing):", err instanceof Error ? err.message : err);
      }
    } else {
      concurrencyQueue.clearPause();
    }
  }

  // Lightweight one-shot /v1/usage fetch used by the head waiter to decide
  // whether to launch. Uses a shorter timeout (3s vs 5s) so a slow /usage
  // response doesn't stall the head-waiter poll; reads only the
  // capacity-decision fields (concurrent_sessions + limit + hard_cap +
  // priority). Returns null on any failure (caller retries).
  async function fetchUsageSnapshot(apiKey: string): Promise<{
    concurrentSessions: number | undefined;
    limit: number | undefined;
    hardCap: number | undefined;
    priority: PriorityState;
  } | null> {
    const data = await fetchUsage(apiKey, 3000);
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
  // undefined when the queue is disabled (fire-and-forget). The `apiKey` is
  // used for the head-waiter poll.
  //
  // CLN4-4: this function BLOCKS until the slot is acquired — it is NOT a fast
  // non-blocking check. The wait is the FIFO queue wait (possibly minutes under
  // contention) + the /usage capacity poll (up to CAPACITY_POLL_TIMEOUT_MS =
  // 60s fail-open, or longer while a known pause is active per CORR4-3). All
  // callers (before_provider_request + the two side-call sites) await it inline
  // on the critical path of the turn — by design, the whole point is to
  // serialize launches so the account stays under its soft cap.
  //
  // Recovery for an aborted/stuck token holder is the watchdog (reapStale):
  // any token held >120s (or whose PID died) is reclaimed by the next acquirer,
  // so a crashed/aborted turn can stall the queue for at most 120s. For an
  // aborted-but-alive turn (user Ctrl-C mid-wait), the `signal` plumbed through
  // waitForLaunch cancels the waiter entry and rejects immediately (C4/ADV-2).
  async function acquireSlot(apiKey: string, signal?: AbortSignal): Promise<Release | undefined> {
    const ourId = concurrencyQueue.join();
    if (!ourId) return undefined; // queue disabled
    // Track whether we have already released our waiter entry so a throw
    // between join() and the return of a release fn cannot leak the waiter
    // for staleWaiterMs (5 min) (ADV-5). waitForLaunch itself cancels on
    // signal abort; this finally covers the non-abort throw paths (lock
    // timeout, EACCES, ENOSPC).
    let released = false;
    try {
      const releaseToken = await concurrencyQueue.waitForLaunch(ourId, signal);
      // We are head + hold the launch token. Poll /usage until the server reports
      // a free slot (or the plan is unlimited) and the account isn't deprioritized.
      // The token stays held during the poll + the subsequent send so the next
      // head polls a /usage that already reflects our in-flight request.
      const limit = concurrencyLimit();
      // Unlimited plan: skip the capacity check (still honor priority.low).
      const capacityFree = async (): Promise<boolean> => {
        // C2: consult the SHARED pause before launching. A 429 observed by any
        // local process writes pausedUntil to the shared file; reading it here
        // makes every sibling back off immediately, even before /usage
        // propagates priority.low (5s refresh lag, or a transient gateway-side
        // blip not yet reflected in /usage). Without this, process B would see
        // priority.low === false and launch right into the 429 that A just hit.
        const snap = await fetchUsageSnapshot(apiKey);
        const decision = isCapacityFree(snap, {
          limit,
          queuePaused: concurrencyQueue.snapshot().paused,
        });
        if (decision.repause) {
          // COV4-2: pauseUntil runs mutate -> writeStateAtomic -> renameSync,
          // which can throw on disk failure (EACCES, ENOSPC, EROFS). The pause
          // is a best-effort coordination signal (the server's priority.low +
          // the 120s watchdog bound it); it must not abort a turn that already
          // waited its FIFO place. Warn + swallow, mirroring releaseSlot's
          // ADV3-1 drain-resilience pattern.
          try {
            concurrencyQueue.pauseUntil(decision.repause.until, decision.repause.reason ?? undefined);
          } catch (err) {
            console.warn("umans: pauseUntil threw in capacityFree (continuing):", err instanceof Error ? err.message : err);
          }
        }
        return decision.free;
      };
      // Poll at 300ms + up to 100ms jitter while full/deprioritized. If the turn
      // is aborted mid-poll, `signal` cancels the waiter; otherwise the watchdog
      // reaps the token after >120s and session_shutdown clears the waiter on exit.
      // CORR5-4 / ADV5-2: the ±100ms jitter breaks phase-locking across machines —
      // D1 designs for multiple machines each running their own local queue and
      // polling /usage; without jitter, N machines' head waiters synchronize on
      // the same 300ms tick and amplify /usage load N× per cycle.
      // ADV-3: cap the total poll elapsed at CAPACITY_POLL_TIMEOUT_MS so a
      // hostile/misbehaving /usage (always reports full, or an account stuck
      // at the cap) cannot wedge the queue forever. After the cap, fail open
      // (launch anyway) — matching the /usage-unreachable fallback's stance
      // that the queue must not block indefinitely.
      // CORR4-3: do NOT fail open during a KNOWN active pause (shared
      // pausedUntil, e.g. a 429 the gate observed). Fail-open for an
      // unreachable /usage is fine (no signal); fail-open for a POSITIVE
      // deprio signal launches into a still-deprioritized account, risking
      // another 429 and extending the account-wide deprioritization — exactly
      // what the gate exists to prevent. Keep waiting when a known pause is
      // active; the pause has a bounded deadline (clamped to MAX_PAUSE_MS)
      // and the 120s watchdog reaps the token if this process hangs.
      const pollStart = Date.now();
      let stalled = false;
      // COV5-1: the branch logic lives in decideLaunch (pure, unit-tested). The
      // loop here drives the /usage fetch (capacityFree, I/O) and applies the
      // decision each iteration.
      for (;;) {
        const isFree = await capacityFree();
        const decision = decideLaunch({
          isFree,
          elapsedMs: Date.now() - pollStart,
          queuePaused: concurrencyQueue.snapshot().paused,
          signalAborted: !!signal?.aborted,
        });
        if (decision === "launch") break; // capacity free — proceed to send
        if (decision === "abort") {
          concurrencyQueue.cancel(ourId);
          released = true;
          throw new Error("concurrency-queue: acquireSlot aborted mid-poll");
        }
        if (decision === "failOpen") {
          // CORR4-3: only fail open when no known pause is active. A known
          // pause means the gate has a positive deprio signal; keep waiting
          // (bounded by the pause deadline + the 120s token watchdog).
          stalled = true;
          break; // fail open below
        }
        // decision === "wait"
        await new Promise((r) => setTimeout(r, 300 + Math.floor(Math.random() * 100)));
      }
      if (stalled) {
        // ADV-3: fail-open after the cap. The turn proceeds ungated; the
        // watchdog still bounds the token hold. We deliberately do not throw —
        // a wedged /usage should not break the user's turn, only the gate.
        // The status bar's `q <queued>*` already reflects the wait; the
        // launch itself is silent so as not to spam notifies on every poll.
      }
      released = true;
      return () => {
        releaseToken();
        concurrencyQueue.cancel(ourId); // belt-and-suspenders: drop our waiter if still present
      };
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
        const results = await searchWeb(apiKey, searchModelId, baseUrl, params.query, signal);
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
        // ADV4-3 / CORR5-3: do NOT assign to mainTurnRelease — side-calls
        // manage their own release via releaseSlot in the finally below.
        const release = await acquireSlot(apiKey, ctx?.signal);
        try {
          // ADV4-4: pass ctx?.signal so an aborted turn aborts the vision HTTP
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
        // ADV4-3 / CORR5-3: do NOT assign to mainTurnRelease — side-calls
        // manage their own release via releaseSlot in the finally below.
        const release = await acquireSlot(apiKey, signal);
        try {
          const answer = await analyzeImage(apiKey, model, baseUrl, image, params.question, signal);
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
  // CLN5-2: wires clearPause({force:true}) + reset() to a real caller so the
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
  // Token-release contract (D2): the token is held ACROSS the send and released
  // at assistant message_end (the response stream has completed). Releasing at
  // after_provider_response headers is too early (headers arrive ~1s in, but
  // the server only registers the request as in-flight once the body streams);
  // releasing at turn_end holds the token through tool execution, collapsing
  // throughput. message_end frees the slot as soon as the stream completes AND
  // during this turn's tool execution (tools don't consume a server concurrency
  // slot). NOTE: message_end fires at CLIENT-side stream completion, which
  // precedes the server's concurrent_sessions decrement by a network RTT +
  // cleanup lag, so the next waiter's /usage poll can transiently see stale
  // (too-low) capacity and launch 1-2 over `limit`. That overshoot stays within
  // the documented burst headroom (hard_cap) -> no 429, no deprioritization
  // (CORR2-1; see isCapacityFree). The launch token serializes the /usage poll
  // (no thundering-herd). The message_end release is the PRIMARY path;
  // turn_end and agent_end are safety nets for turns that error before
  // message_end fires.
  //
  // CORR5-3: the main-turn release is tracked in a SINGLE slot
  // (mainTurnRelease), not a Set. The design guarantees at most one main-turn
  // slot is outstanding (side-calls manage their own release in a finally and
  // never register here), so a Set + FIFO-by-insertion releaseOldest was a
  // latent footgun: if a future change ever added a second entry, message_end
  // would release the oldest (possibly a side-call acquired before the main
  // turn) instead of the main turn's slot, leaking the token until the safety
  // nets. A single tracked slot makes the invariant structural — there is no
  // ordering to get wrong.
  let mainTurnRelease: Release | undefined;
  // ADV3-1: release() calls mutate() -> withLock -> acquireLock, which can
  // throw (e.g. O_EXCL lock timeout after 2s per CMP-MED-2, EACCES, ENOSPC).
  // A throw propagating out of releaseSlot would abort the drains in
  // agent_end / session_shutdown mid-loop, leaking the token until the 120s
  // watchdog. Wrap release() in a try/catch: on throw, warn (the lock-timeout
  // is transient; the watchdog will reap the stale token/waiter) and swallow
  // so the drain continues. Keep the slot clear + updateStatus in a finally so
  // the slot is released even on throw — otherwise a repeatedly-throwing slot
  // would loop forever.
  function releaseSlot(release: Release | undefined): void {
    if (!release) return;
    try {
      try {
        release();
      } catch (err) {
        // Transient (lock timeout) or environmental (EACCES/ENOSPC); the
        // 120s watchdog reaps the stale token/waiter entry regardless.
        console.warn("umans: concurrency release threw (drain continues):", err instanceof Error ? err.message : err);
      }
    } finally {
      if (mainTurnRelease === release) mainTurnRelease = undefined;
      updateStatus(undefined as any);
    }
  }
  // Release the main-turn slot if held. Called at assistant message_end
  // (primary), turn_end / agent_end (safety nets), and session_shutdown
  // (drain). At most one main-turn slot is ever outstanding (CORR5-3), so a
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
    // (the prior ac4ad4b design defeated serialization: siblings all polled
    // /usage, all saw capacity, all released, and all sent simultaneously —
    // empirically peak 4 vs limit 2 (C1)), and NOT at after_provider_response
    // headers (the server hasn't registered the request as in-flight until the
    // body streams). message_end frees the slot during tool execution too; the
    // release race (message_end precedes the server decrement) is absorbed by
    // the hard_cap burst headroom (CORR2-1). turn_end and agent_end are safety
    // nets for turns that never reach message_end.
    const release = await acquireSlot(apiKey, ctx.signal);
    if (release) {
      // ADV2-F2: wrap acquire + register in a try/finally so a throw between
      // acquireSlot resolving and the safety-net registration (message_end /
      // turn_end / agent_end / session_shutdown) doesn't leak the token until
      // the 120s watchdog. On the happy path the release fn is owned by
      // mainTurnRelease and released at message_end; a throw here releases it
      // immediately. (updateStatus swallows internally, but this guards any
      // future throw in the registration path.)
      let registered = false;
      try {
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
    // agent_end (the drain safety nets). Here we only intercept 429s to extend
    // the shared pause window so sibling processes back off instead of
    // immediately re-launching. Per Umans docs, each 429 deprioritizes the
    // account for ~30 min (Retry-After overrides).
    if (event.status === 429) {
      const retryAfter = event.headers?.["retry-after"];
      let until = Date.now() + PRIORITY_BACKOFF_MS;
      if (retryAfter) {
        // RFC 7231 Retry-After is delta-seconds (a non-negative integer) or
        // an HTTP-date. We only accept the integer form: Number() accepts
        // hex ("0x10"=16), scientific notation ("1e10"=1e10), and other
        // misparses that can wedge the queue (S2/S4). Parse strictly and cap
        // the resulting deadline at now + MAX_PAUSE_429_MS via clampPauseUntil
        // (ADV4-2: a 429-sourced pause is clamped tighter than the 5h ceiling
        // so a misconfigured UMANS_BASE_URL returning 429 forever cannot wedge
        // the account for hours; pauseUntil also enforces this ceiling).
        const trimmed = String(retryAfter).trim();
        if (/^\d+$/.test(trimmed)) {
          const secs = parseInt(trimmed, 10);
          if (secs > 0) until = clampPauseUntil(Date.now() + secs * 1000, Date.now(), MAX_PAUSE_429_MS);
        }
      }
      // COV4-2: pauseUntil can throw on disk failure (EACCES/ENOSPC/EROFS).
      // The lost pause is bounded by the 120s watchdog + the 5s refreshUsage
      // poll, and the 429 notify still fires below. Warn + swallow so the
      // after_provider_response handler doesn't propagate a disk error.
      try {
        concurrencyQueue.pauseUntil(until, PAUSE_REASON_429);
      } catch (err) {
        console.warn("umans: pauseUntil threw in 429 handler (continuing):", err instanceof Error ? err.message : err);
      }
      ctx.ui?.notify?.(
        `Umans 429: pausing new turns ${Math.round((until - Date.now()) / 1000)}s to avoid account deprioritization.`,
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
    // COV5-2: the release guard is a pure decision (shouldReleaseOnMessageEnd)
    // so the "release only on an Umans assistant message" invariant is unit-
    // testable. User messages, tool results, and non-Umans providers are no-ops.
    if (!shouldReleaseOnMessageEnd(msg, msg?.provider ?? ctx.model?.provider)) return;
    // Primary release path (D2): the assistant response stream completed, freeing
    // the slot for this turn's tool execution (tools don't consume a server
    // concurrency slot). NOTE: message_end fires at CLIENT-side stream
    // completion, which PRECEDES the server's concurrent_sessions decrement by a
    // network RTT + cleanup lag, so the next waiter's /usage poll can
    // transiently see stale capacity and launch 1-2 over `limit`; that overshoot
    // stays within the documented burst headroom (hard_cap) -> no 429, no
    // deprioritization (CORR2-1; see isCapacityFree). turn_end and agent_end
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
    // Drain any slot still held (e.g. aborted turns that never reached
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
    // ADV2-F1: release the main-turn slot by invoking its release fn (not just
    // dropping the reference), so the token/waiter entry is cleaned up. CORR5-3:
    // at most one main-turn slot is outstanding (side-calls manage their own
    // release in a finally). reset() only clears ourTokenId's entry, so
    // releasing here ensures the held slot's token/waiter is released.
    releaseMainTurn();
    concurrencyQueue.reset();
    setWidget(ctx, undefined);
  });

}


