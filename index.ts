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
 *   UMANS_CONCURRENCY_LIMIT - override the concurrency soft cap used by the gate
 *                           (default: live value from /v1/usage). Useful for testing.
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
const USER_AGENT = "pi-umans-provider/1.2.5";
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

// Concurrency gate tuning. The gate keeps in-flight Umans requests at or below
// the plan's soft cap so the account never draws rate-limit 429s (which, per
// the Umans docs, deprioritize the whole account for ~30 min and can trigger a
// 5-hour pause after 10 in a day). When the server reports priority.low or a
// 429 is received, new acquisitions are paused until boxedUntil.
const CONCURRENCY_POLL_INTERVAL_MS = 5_000;
const PRIORITY_BACKOFF_MS = 30_000; // conservative default; server may report a boxed_until

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

/**
 * Normalized priority state derived from /v1/usage `usage.priority` (or a 429).
 * `until` is an epoch-ms deadline; 0 means "no active deprioritization".
 */
export interface PriorityState {
  low: boolean;
  until: number; // epoch-ms; 0 when not low
  reason: string | null;
}

export function parsePriority(raw: unknown): PriorityState {
  const p = (raw ?? {}) as {
    low?: boolean | null;
    boxed_until?: string | number | null;
    reason?: string | null;
  };
  const low = p.low === true;
  let until = 0;
  if (low) {
    // boxed_until may be an ISO string, epoch seconds, or null. When null but
    // low===true, fall back to the conservative PRIORITY_BACKOFF_MS window.
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

/**
 * Snapshot of the concurrency gate's state, surfaced in the status bar.
 * `inFlight` counts held slots; `queued` counts waiters in the FIFO.
 */
export interface ConcurrencySnapshot {
  inFlight: number;
  queued: number;
  limit: number | undefined;
  paused: boolean;
  pausedUntil: number; // epoch-ms; 0 when not paused
}

/**
 * A FIFO concurrency gate. acquire() returns a `Release` that must be called
 * exactly once when the protected request completes (success, error, or
 * abort). Waiters are served strictly in arrival order. When the account is
 * deprioritized (priority.low or a received 429), acquire() blocks until the
 * pause deadline passes, then re-enters the FIFO.
 *
 * Exported so the queue logic is unit-testable without a live provider.
 * createConcurrencyGate itself has no side effects.
 */
export interface ConcurrencyGate {
  acquire(): Promise<Release>;
  snapshot(): ConcurrencySnapshot;
  /** Mark the account as deprioritized until `until` (epoch-ms). Idempotent; extends the deadline. */
  pauseUntil(until: number, reason?: string | null): void;
  /** Clear deprioritization early (e.g. when /v1/usage reports priority.low===false again). */
  clearPause(): void;
  /** Replace the soft cap (push live /v1/usage values here). undefined = unlimited. */
  setLimit(limit: number | undefined): void;
  /** Hard-reset all state (used on session shutdown). */
  reset(): void;
}
export type Release = () => void;

export function createConcurrencyGate(opts?: {
  limit?: number;
  now?: () => number;
}): ConcurrencyGate {
  const now = opts?.now ?? Date.now;
  let limit = opts?.limit;
  let inFlight = 0;
  let paused = false;
  let pausedUntil = 0;
  let pausedReason: string | null = null;
  // FIFO of waiters. Each entry's `resolve` is called once a slot is free AND
  // any active pause has elapsed.
  const waiters: Array<() => void> = [];

  function isPausedAt(t: number): boolean {
    return paused && t < pausedUntil;
  }

  function pump(): void {
    // Grant slots to as many waiters as capacity + un-paused state allows.
    while (waiters.length > 0) {
      const t = now();
      if (isPausedAt(t)) break; // waiters stay queued until the pause elapses
      if (limit !== undefined && inFlight >= limit) break;
      const next = waiters.shift()!;
      inFlight++;
      next();
    }
  }

  function schedulePumpForPause(): void {
    // If paused with waiters, wake pump() when the pause should elapse.
    if (paused && pausedUntil > 0 && waiters.length > 0) {
      const delay = Math.max(100, pausedUntil - now());
      setTimeout(() => pump(), delay);
    }
  }

  return {
    acquire(): Promise<Release> {
      const t = now();
      // Fast path: a free slot, no pause, and (no limit OR under limit).
      if (!isPausedAt(t) && (limit === undefined || inFlight < limit)) {
        inFlight++;
        return Promise.resolve(() => {
          inFlight = Math.max(0, inFlight - 1);
          pump();
        });
      }
      // Slow path: enqueue and wait. pump() grants the slot later.
      return new Promise<Release>((resolve) => {
        waiters.push(() => resolve(() => {
          inFlight = Math.max(0, inFlight - 1);
          pump();
        }));
        // If paused, ensure pump() is rescheduled when the pause elapses;
        // otherwise pump() runs on the next release.
        if (isPausedAt(now())) schedulePumpForPause();
      });
    },
    snapshot(): ConcurrencySnapshot {
      const t = now();
      return {
        inFlight,
        queued: waiters.length,
        limit,
        paused: isPausedAt(t),
        pausedUntil: paused && pausedUntil > 0 ? pausedUntil : 0,
      };
    },
    pauseUntil(until: number, reason?: string | null): void {
      paused = true;
      pausedUntil = Math.max(pausedUntil, until);
      if (reason) pausedReason = reason;
      schedulePumpForPause();
    },
    clearPause(): void {
      paused = false;
      pausedUntil = 0;
      pausedReason = null;
      pump();
    },
    setLimit(newLimit: number | undefined): void {
      limit = newLimit;
      pump();
    },
    reset(): void {
      paused = false;
      pausedUntil = 0;
      pausedReason = null;
      // Drain waiters with no-ops so their promises never settle (process is
      // shutting down); inFlight is reset implicitly by the pending releases.
      while (waiters.length) waiters.shift()!();
      inFlight = 0;
    },
  };
}

/**
 * Set/replace a gate's soft cap after it is created. Exposed for the provider to
 * push live /v1/usage values into an existing gate without recreating it.
 */
export function setGateLimit(gate: ConcurrencyGate, limit: number | undefined): void {
  gate.setLimit(limit);
}

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
  let priorityState: PriorityState = { low: false, until: 0, reason: null };

  // Client-side FIFO gate over outbound Umans requests (main turns + vision /
  // search side-calls). Kept in sync with /v1/usage's concurrency.limit and
  // paused when the account is deprioritized. UMANS_CONCURRENCY_DISABLE opts
  // out entirely (fire-and-forget); UMANS_CONCURRENCY_LIMIT overrides the cap.
  const concurrencyDisabled = process.env[CONCURRENCY_DISABLE_ENV] === "1";
  const concurrencyGate = concurrencyDisabled ? undefined : createConcurrencyGate();
  function applyConcurrencyLimit(): void {
    if (!concurrencyGate) return;
    const envOverride = process.env[CONCURRENCY_LIMIT_ENV]?.trim();
    const n = envOverride ? Number(envOverride) : NaN;
    setGateLimit(concurrencyGate, Number.isFinite(n) && n > 0 ? n : guaranteedConcurrency);
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
    // The gate's effective cap honors UMANS_CONCURRENCY_LIMIT over the live
    // server value, so the bar reflects what the gate actually enforces.
    const gateSnap = concurrencyGate?.snapshot();
    const effectiveLimit = gateSnap?.limit ?? guaranteedConcurrency;
    const guaranteed = effectiveLimit !== undefined ? String(effectiveLimit) : "?";
    // Real account-wide conc from /v1/usage (includes other clients, not just
    // this pi instance). Refreshed only by the 5s poll; shows "?" until first
    // poll lands — never locally synthesized from sent-message turn counts.
    const current = currentConcurrency !== undefined ? String(currentConcurrency) : "?";
    parts.push(`Conc ${current}/${guaranteed}`);
    // Only show request usage when the plan has a hard limit (e.g. the $20 tier).
    if (requestsUsed !== undefined && requestLimit !== undefined) {
      parts.push(`Req ${requestsUsed}/${requestLimit}`);
    }
    // Client-side gate: in-flight/queued across this pi instance, plus a pause
    // indicator when the account is deprioritized (priority.low or a 429).
    if (concurrencyGate) {
      const snap = concurrencyGate.snapshot();
      if (snap.queued > 0 || snap.inFlight > 0) {
        parts.push(`gate ${snap.inFlight}${snap.queued > 0 ? `+${snap.queued}q` : ""}`);
      }
      if (snap.paused) {
        const secs = Math.max(0, Math.round((snap.pausedUntil - Date.now()) / 1000));
        parts.push(`PAUSED ${secs}s`);
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

  async function refreshUsage(apiKey: string) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 5000);
    try {
      const res = await fetch(`${baseUrl}/v1/usage`, {
        signal: ctrl.signal,
        headers: {
          Authorization: `Bearer ${apiKey}`,
          Accept: "application/json",
          "User-Agent": USER_AGENT,
        },
      });
      if (!res.ok) return;
      const data = await res.json() as {
        limits?: {
          concurrency?: { limit?: number };
          requests?: { limit?: number };
        };
        usage?: {
          requests_in_window?: number;
          concurrent_sessions?: number;
          priority?: unknown;
        };
      };
      // null ?? undefined normalizes unlimited (null) limits so the display
      // guards below hide them instead of rendering "x/null".
      guaranteedConcurrency = data.limits?.concurrency?.limit ?? undefined;
      currentConcurrency = data.usage?.concurrent_sessions;
      requestLimit = data.limits?.requests?.limit ?? undefined;
      requestsUsed = data.usage?.requests_in_window;
      // Push the live soft cap into the gate (unless overridden by env) and
      // reconcile the pause window with the server's priority state.
      applyConcurrencyLimit();
      priorityState = parsePriority(data.usage?.priority);
      if (concurrencyGate) {
        if (priorityState.low) {
          concurrencyGate.pauseUntil(priorityState.until, priorityState.reason ?? undefined);
        } else {
          concurrencyGate.clearPause();
        }
      }
    } catch {
      // Leave as undefined; status bar will show "?".
    } finally {
      clearTimeout(timer);
    }
  }

  // Acquire a concurrency slot for an outbound Umans request (main turn or a
  // vision/search side-call). Resolves once a slot is free AND the account is
  // not deprioritized. Returns a release fn (no-op when the gate is disabled).
  async function acquireSlot(): Promise<Release | undefined> {
    // If the server just reported priority.low, proactively extend the pause
    // window so a fresh burst of turns doesn't immediately re-trigger a 429.
    if (priorityState.low && priorityState.until > Date.now() && concurrencyGate) {
      concurrencyGate.pauseUntil(priorityState.until, priorityState.reason ?? undefined);
    }
    return concurrencyGate ? concurrencyGate.acquire() : undefined;
  }

  function isActiveUmans(ctx: any, msg?: any) {
    return (msg?.provider ?? ctx.model?.provider) === "umans";
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
      // through the same FIFO so a burst of searches can't push the main
      // turn past the soft cap.
      const release = await acquireSlot();
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
        // Gate the vision side-call through the same concurrency FIFO so a
        // multi-image handoff can't push the main turn past the soft cap.
        const release = await acquireSlot();
        try {
          analysis = await analyzeImage(
            apiKey,
            model,
            baseUrl,
            { data: img.data, mimeType: img.mimeType },
            VISION_ANALYSIS_PROMPT,
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
        const release = await acquireSlot();
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

  // === Concurrency gate: hold the outbound request until a slot is free ===
  // before_provider_request fires after the payload is built, right before the
  // HTTP send, and is awaited by pi. We acquire a FIFO slot here so the main
  // turn blocks (queued) rather than hitting the server and risking a 429. The
  // release is wired to after_provider_response + turn_end (safety net).
  const inflightSlots = new Set<Release>();
  function releaseSlot(release: Release | undefined): void {
    if (!release) return;
    inflightSlots.delete(release);
    release();
    updateStatus(undefined as any);
  }

  pi.on("before_provider_request", async (_event, ctx) => {
    if (ctx.model?.provider !== "umans") return;
    if (!concurrencyGate) return;
    const release = await acquireSlot();
    if (release) {
      inflightSlots.add(release);
      updateStatus(ctx);
    }
  });

  pi.on("after_provider_response", async (event, ctx) => {
    if (ctx.model?.provider !== "umans") return;
    if (!concurrencyGate) return;
    // Release the slot once the response headers arrive — the in-flight
    // request is no longer consuming a concurrency slot on the server at this
    // point (streaming drains the body afterwards).
    //
    // On a 429, extend the pause window so subsequent turns back off instead
    // of immediately re-queuing and re-triggering the limit. Per Umans docs,
    // each 429 deprioritizes the account for ~30 min, so we use that floor.
    if (event.status === 429) {
      const retryAfter = event.headers?.["retry-after"];
      let until = Date.now() + PRIORITY_BACKOFF_MS;
      if (retryAfter) {
        const secs = Number(retryAfter);
        if (Number.isFinite(secs) && secs > 0) until = Date.now() + secs * 1000;
      }
      concurrencyGate.pauseUntil(until, "HTTP 429 from gateway");
      priorityState = { low: true, until, reason: "HTTP 429 from gateway" };
      ctx.ui?.notify?.(
        `Umans 429: pausing new turns ${Math.round((until - Date.now()) / 1000)}s to avoid account deprioritization.`,
        "warning",
      );
    }
    // Release one slot per provider response. Because before_provider_request
    // acquires exactly one slot per turn and responses arrive one-per-turn,
    // draining the oldest held release is the correct FIFO release.
    const oldest = inflightSlots.values().next().value;
    releaseSlot(oldest);
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
    // Safety net: if after_provider_response never fired (e.g. the request
    // errored before headers), release any slot still held for this turn.
    // In normal flow the slot is already released and this is a no-op.
    const oldest = inflightSlots.values().next().value;
    releaseSlot(oldest);
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
    if (!isActiveUmans(ctx, msg)) return;
    if (msg?.role !== "assistant") return;
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
    // Drain any slots still held (e.g. aborted turns that never reached
    // turn_end / after_provider_response) so the gate never deadlocks.
    while (inflightSlots.size) {
      const oldest = inflightSlots.values().next().value;
      releaseSlot(oldest);
    }
    updateStatus(ctx);
  });

  pi.on("session_shutdown", async (_event, ctx) => {
    stopRefreshLoop();
    liveRequest = undefined;
    lastMetrics = {};
    imageStore.clear();
    inflightSlots.clear();
    concurrencyGate?.reset();
    setWidget(ctx, undefined);
  });

}


