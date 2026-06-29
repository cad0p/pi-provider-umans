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
  PAUSE_REASON_403_BRIDGE,
  PAUSE_403_BRIDGE_MS,
  type ConcurrencyQueue,
} from "./concurrency-queue.ts";
import {
  USER_AGENT,
  handle429,
  raiseForUmansStatus,
  resolveApiKey,
  concurrencyLimit as sharedConcurrencyLimit,
  acquireSlotCore,
  releaseSlotCore,
  resolveBaseUrl,
  stopRefreshLoop,
  restartRefreshLoop,
  refreshUsage as sharedRefreshUsage,
  type ConcurrencyRuntime,
  type ModelCapabilities,
  type ReasoningInfo,
  type UmansModelInfo,
} from "./utils.ts";
import { readSettings } from "./settings.ts";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { OAuthCredentials, OAuthLoginCallbacks } from "@earendil-works/pi-ai";
import { Type } from "typebox";

const API_KEY_ENV = "UMANS_API_KEY";
const STATUS_UPDATE_INTERVAL_MS = 1000;

// Client-side vision handoff env + tuning. See header doc for the design.
const VISION_DISABLE_ENV = "UMANS_VISION_DISABLE";
const VISION_MODEL_ENV = "UMANS_VISION_MODEL";
const CONCURRENCY_DISABLE_ENV = "UMANS_CONCURRENCY_DISABLE";
const CONCURRENCY_STATE_FILE_ENV = "UMANS_CONCURRENCY_STATE_FILE";
// Override for the global settings file (~/.pi/agent/umans.json). Mirrors
// UMANS_CONCURRENCY_STATE_FILE: lets selfcheck point readSettings at a temp
// file without monkey-patching homedir. No-op in normal use.
const SETTINGS_FILE_ENV = "UMANS_SETTINGS_FILE";
const VISION_MAX_TOKENS = 1024;
const VISION_TIMEOUT_MS = 60_000;
const VISION_ANALYSIS_PROMPT =
  "You are a vision assistant for a text-only coding model. Analyze the attached image thoroughly but concisely. " +
  "Capture: any visible text (verbatim), UI/layout, code/errors/stack traces, diagrams/charts, and other notable details. " +
  "Write a compact structured report. Do not speculate beyond what is visible.";

// The capacity-poll + strike-counter constants (CAPACITY_POLL_TIMEOUT_MS,
// STRIKE_*, POLL_INTERVAL_*), the pure decideLaunch / nextPollInterval
// helpers, and the resolveApiKey / resolveBaseUrl / concurrencyLimit /
// acquireSlotCore / refresh + fetch machinery all live in utils.ts now —
// shared with web-search.ts so the two factories cannot diverge on the
// strike-pause gate or the capacity-poll loop. Each factory builds a
// ConcurrencyRuntime (below) carrying its own queue instance + mutable
// capacity state + calls the shared functions with it.

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

/*
 * Concurrency gating moved to ./concurrency-queue.ts (file-backed FIFO shared
 * across pi processes via ~/.pi/agent/umans-concurrency.json).
 */
// ConcurrencyQueue is imported directly and used at the
// factory call site. WaiterEntry / TokenState / CapacitySnapshot /
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
      // delegated to raiseForUmansStatus (shared with searchWeb in web-search.ts) —
      // runs the 429 push, reads + sanitizes the body, throws.
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

export default async function (pi: ExtensionAPI) {
  if (process.env.UMANS_DISABLE === "1") return;

  const baseUrl = resolveBaseUrl();

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
  // Status-bar-only locals (not shared with web-search.ts). The shared
  // capacity state (guaranteedConcurrency, hardCap, strikes24h, deprioritized,
  // priorityUntil) lives on the ConcurrencyRuntime `rt` below so the one copy
  // of refreshStrikes/refreshUsage in utils.ts reads + writes it directly.
  let currentConcurrency: number | undefined;
  let requestLimit: number | undefined;
  let requestsUsed: number | undefined;
  // Stash the last seen pi ctx (from session_start) so the periodic
  // refreshUsage timer can re-render the status bar (countdown, strikes,
  // concurrency) even when the user is idle at the prompt — not just during
  // streaming events. Without this, the PAUSED/DEPRIO countdown freezes
  // because updateStatus is only called from event handlers.
  let lastCtx: any;

  // Cross-process FIFO queue over outbound Umans requests, backed by
  // ~/.pi/agent/umans-concurrency.json (O_EXCL lockfile + atomic rename). The
  // file is a PURE WAITER QUEUE + launch token; capacity is decided solely by
  // the live /v1/usage response polled by the head waiter, so multiple pi
  // processes (and multiple machines) coordinate through the server, not a
  // local count. UMANS_CONCURRENCY_DISABLE opts out (fire-and-forget).
  // UMANS_CONCURRENCY_LIMIT is a deprecated absolute override (testing knob):
  // when set it wins outright, bypassing the concurrency multiplier. The
  // user-facing knob is the concurrencyMultiplier setting (~/.pi/agent/
  // umans.json + .pi/umans.json); see concurrencyLimit + settings.ts.
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
  // Cache settings once at startup. concurrencyLimit() is on the hot path
  // (called per acquireSlot poll iteration ~300ms + per status-bar render),
  // so re-reading the file each call would hammer the filesystem. Restart
  // picks up changes; a live-reload watcher is not required.
  // UMANS_SETTINGS_FILE overrides the global path (~/.pi/agent/umans.json)
  // so selfcheck can point at a temp file; the project path (.pi/umans.json)
  // resolves against cwd as in production.
  const settingsGlobalPath = process.env[SETTINGS_FILE_ENV]?.trim() || undefined;
  const cachedSettings = readSettings(
    settingsGlobalPath ? { globalPath: settingsGlobalPath } : undefined,
  );
  // Shared concurrency runtime: carries this factory's queue instance + the
  // mutable capacity state read/written by the shared refresh/fetch machinery
  // in utils.ts (one copy of refreshStrikes/refreshUsage/fetchUsage/etc, shared
  // with web-search.ts so the strike-pause gate cannot diverge). The
  // status-bar-only locals above stay outside rt. afterRefreshUsage is the
  // status-bar hook: the shared refreshUsage calls it with the raw /v1/usage
  // data so index.ts can update currentConcurrency/requestLimit/requestsUsed
  // + re-render. web-search.ts builds an rt with no afterRefreshUsage hook.
  const rt: ConcurrencyRuntime = {
    baseUrl,
    queue: concurrencyQueue,
    multiplier: cachedSettings.concurrencyMultiplier,
    hardCap: undefined,
    guaranteedConcurrency: undefined,
    strikes24h: undefined,
    deprioritized: false,
    priorityUntil: undefined,
    refreshTimer: undefined,
    strikeTimer: undefined,
    refreshStopped: false,
    afterRefreshUsage(data) {
      if (data.limits) {
        currentConcurrency = data.usage?.concurrent_sessions;
        requestLimit = data.limits.requests?.limit ?? undefined;
        requestsUsed = data.usage?.requests_in_window;
      } else {
        // Synthetic cap_abuse object: only usage.concurrent_sessions is
        // present. Update the live concurrency display but preserve the
        // cached limits.
        currentConcurrency = data.usage?.concurrent_sessions;
      }
      // Re-render the status bar so the countdown (PAUSED/DEPRIO) +
      // concurrency + strikes display stays live while the user is idle.
      if (lastCtx) {
        try { updateStatus(lastCtx); } catch { /* UI may not be available */ }
      }
    },
  };

  type LiveRequest = {
    startTime: number;
    firstTokenTime?: number;
    estimatedTokens: number;
    lastStatusUpdate: number;
  };
  let liveRequest: LiveRequest | undefined;
  let lastMetrics: { ttft?: number; tps?: number } = {};

  function computeCumulativeTps(req: LiveRequest, now: number): number {
    if (!req.firstTokenTime || req.estimatedTokens <= 0) return 0;
    const elapsedSec = (now - req.firstTokenTime) / 1000;
    // Wait a moment so a tiny first chunk does not create a wild initial value.
    if (elapsedSec < 0.5) return 0;
    return Math.round(req.estimatedTokens / elapsedSec);
  }

  function statusText(metrics?: { ttft?: number; tps?: number }) {
    // delegates to the pure formatStatusText helper so the rendering
    // is unit-testable. The closure supplies the live inputs (shared capacity
    // state lives on rt; status-only locals stay as factory locals).
    return formatStatusText({
      metrics,
      effectiveLimit: sharedConcurrencyLimit(rt),
      currentConcurrency,
      requestLimit,
      requestsUsed,
      queueSnap: concurrencyQueue.snapshot(),
      concurrencyDisabled,
      strikes24h: rt.strikes24h,
      deprioritized: rt.deprioritized,
      priorityUntil: rt.priorityUntil,
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

  // Acquire a concurrency slot for an outbound Umans request (main turn or a
  // vision/search side-call). Delegates to the shared acquireSlotCore (utils.ts,
  // one copy shared with web-search.ts so the capacity-poll loop cannot diverge)
  // which joins the cross-process FIFO, waits until head + claims the launch
  // token, polls /v1/usage until the server reports a free slot (and no
  // priority.low), and returns a release fn. Returns undefined when the queue is
  // disabled (fire-and-forget) or when the turn's AbortSignal fires mid-poll
  // (clean cancellation, not a throw). The `apiKey` is used for the head-waiter
  // poll.
  //
  // this function BLOCKS until the slot is acquired — it is NOT a fast
  // non-blocking check. The wait is the FIFO queue wait (possibly minutes under
  // contention) + the /v1/usage capacity poll (up to CAPACITY_POLL_TIMEOUT_MS =
  // 60s fail-open, or longer while a known pause is active). All callers
  // (before_provider_request + the three side-call sites) await it inline on
  // the critical path of the turn — by design, the whole point is to serialize
  // launches so the account stays under its soft cap.
  //
  // Recovery for an aborted/stuck token holder is the watchdog (reapStale):
  // any token held >120s (or whose PID died) is reclaimed by the next acquirer,
  // so a crashed/aborted turn can stall the queue for at most 120s. For an
  // aborted-but-alive turn (user Ctrl-C mid-wait), the `signal` plumbed through
  // waitForLaunch cancels the waiter entry and rejects immediately.
  async function acquireSlot(apiKey: string, signal?: AbortSignal): Promise<Release | undefined> {
    return acquireSlotCore(rt, apiKey, signal) as Promise<Release | undefined>;
  }

  // resolveApiKey is imported from utils.ts (shared with web-search.ts) —
  // it reads UMANS_API_KEY then falls back to ctx.modelRegistry.

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
          // consistent with the tool + the web-search side-call (web-search.ts).
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
  // throw (e.g. O_EXCL lock timeout after 2s, EACCES, ENOSPC).
  // A throw propagating out of releaseSlot would abort the caller (message_end /
  // turn_end / agent_end / session_shutdown), leaking the token until the 120s
  // watchdog. Wrap release() in a try/catch: on throw, warn (the lock-timeout
  // is transient; the watchdog will reap the stale token/waiter) and swallow
  // so the single-slot release completes. Keep the slot clear + updateStatus in
  // a finally so the slot is released even on throw — otherwise a
  // repeatedly-throwing slot would loop forever.
  function releaseSlot(release: Release | undefined): void {
    // releaseSlotCore runs the release closure (swallowing throws so a
    // transient lock/disk error does not abort the caller's turn; the 120s
    // watchdog reaps the stale token/waiter regardless). The main-turn
    // tracking + status-bar re-render stay here (index.ts only).
    try {
      releaseSlotCore(release);
    } finally {
      if (mainTurnRelease === release) mainTurnRelease = undefined;
      updateStatus(undefined as any);
    }
  }
  // Release the main-turn slot if held. Called at assistant message_end
  // (primary), turn_end / agent_end (safety nets), and session_shutdown
  // (cleanup). At most one main-turn slot is ever outstanding, so a
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
    // server reports a free slot, then returns a release fn. Per D2 the token
    // serializes the /usage poll only — it is released IMMEDIATELY after the
    // capacity check passes (before the send), so the next head can poll +
    // launch right away. The server's /usage lag means the next head sees a
    // stale-low concurrent_sessions + launches, achieving limit-concurrent
    // saturation; the hard_cap burst headroom absorbs the overshoot. The
    // in-flight slot is tracked by the release fn (addInFlight at launch,
    // removeInFlight at message_end/turn_end/agent_end). turn_end + agent_end
    // are safety nets for turns that never reach message_end.
    // wrap acquireSlot in try/catch so a wedged lock (a lock-timeout
    // future-dated mtime) or transient disk error (EACCES/ENOSPC/EROFS/ENOENT
    // on the lockfile or state file) does NOT break the user's turn as an
    // uncaught extension error. The queue must not break inference, only the
    // gate. Fail-open ungated (proceed without a release fn), matching the
    // /usage-unreachable stance at isCapacityFree + the poll-timeout
    // fail-open. The watchdog + hard_cap burst headroom absorb one ungated
    // send. This mirrors the hardening applied to pauseUntil/handle429.
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
      // analyzeImage + searchWeb in web-search.ts) so every 429 site parses Retry-After the
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
    lastCtx = ctx; // stash for periodic updateStatus from the refreshUsage timer
    const apiKey = await resolveApiKey(ctx);
    if (ctx.model?.provider === "umans") {
      if (apiKey) await sharedRefreshUsage(rt, apiKey);
      restartRefreshLoop(rt, apiKey || "");
      updateStatus(ctx);
    }
  });

  pi.on("model_select", async (event, ctx) => {
    const provider = event.model.provider;
    if (provider !== "umans") {
      stopRefreshLoop(rt);
      setWidget(ctx, undefined);
      liveRequest = undefined;
      lastMetrics = {};
      return;
    }
    updateStatus(ctx);
    const apiKey = await resolveApiKey(ctx);
    if (apiKey) await sharedRefreshUsage(rt, apiKey);
    restartRefreshLoop(rt, apiKey || "");
  });

  // turn_start opens the TTFT clock: it fires before API-key/HTTP/prefill, so TTFT
  // spans the full send→first-token gap, not just the stream body from message_start.
  pi.on("turn_start", async (event, ctx) => {
    if (ctx.model?.provider !== "umans") return;
    lastCtx = ctx; // refresh in case of model switch / new session
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
    // message_end / turn_end) so the gate never deadlocks. At most one
    // main-turn slot is outstanding, so a single release is sufficient.
    releaseMainTurn();
    updateStatus(ctx);
  });

  pi.on("session_shutdown", async (_event, ctx) => {
    stopRefreshLoop(rt);
    liveRequest = undefined;
    lastMetrics = {};
    imageStore.clear();
    // release the main-turn slot by invoking its release fn (not just
    // dropping the reference), so the token/waiter entry is cleaned up.
    // At most one main-turn slot is outstanding (side-calls manage their own
    // release in a finally). reset() only clears ourTokenId's entry, so
    // releasing here ensures the held slot's token/waiter is released.
    releaseMainTurn();
    concurrencyQueue.reset();
    setWidget(ctx, undefined);
  });

}


