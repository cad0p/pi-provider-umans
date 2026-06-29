/**
 * Standalone web-search extension for the Umans provider.
 *
 * Splits the `umans_web_search` tool out of index.ts into its own file so
 * `pi config` (the `+`/`-` prefix mechanism on the `extensions` array in
 * settings.json) can toggle it on/off independently of the provider. Drop a
 * `-web-search.ts` entry to disable the built-in web search (e.g. when you
 * expose your own MCP web-search tool); `+web-search.ts` (or no entry — the
 * package's default extension list includes it) re-enables it.
 *
 * The tool runs a web search by making a sub-request to the Umans gateway with
 * the Anthropic `web_search_20250305` server tool declared. The gateway runs
 * the Exa search server-side and returns results; we surface the model's
 * formatted result text (titles, URLs, snippets) back to the calling model.
 * It's a side-call because pi-ai only serializes client-side tools and cannot
 * emit the server-tool shape the gateway requires. Costs one extra round-trip
 * per search; no pi-ai changes needed.
 *
 * Standalone factoring: this file creates its OWN concurrencyQueue instance via
 * createConcurrencyQueue() (same config as index.ts's instance). The two
 * instances in the same process coordinate through the shared
 * ~/.pi/agent/umans-concurrency.json state file — the file is a pure waiter
 * queue + launch token (capacity is decided by /v1/usage), so two queue
 * instances in the same process coordinate through the file the same way
 * cross-process coordination already does. The apiKey is resolved at tool
 * execute time via ctx.modelRegistry.getApiKeyForProvider("umans") (available
 * to any extension via ctx — no shared closure with index.ts needed), and
 * baseUrl resolves from UMANS_BASE_URL (re-resolved locally; trivial).
 *
 * Pure helpers (searchWeb, raiseForUmansStatus, sanitizeErrorBody, handle429,
 * pickSearchModel, the search constants) are imported from utils.ts — the
 * single source of truth shared with index.ts.
 */
import {
  createConcurrencyQueue,
  isCapacityFree,
  parsePriority,
  parseConcurrencyLimit,
  PAUSE_REASON_STRIKES,
  PAUSE_REASON_403_BRIDGE,
  PAUSE_403_BRIDGE_MS,
  STICKY_PAUSE_REASONS,
  PRIORITY_BACKOFF_MS,
  type ConcurrencyQueue,
  type PriorityState,
} from "./concurrency-queue.ts";
import {
  USER_AGENT,
  SEARCH_TIMEOUT_MS,
  SEARCH_MAX_TOKENS,
  pickSearchModel,
  sanitizeErrorBody,
  handle429,
  raiseForUmansStatus,
  readRetryAfter,
  extractBoxedUntil,
  isSuspendBody,
  type UmansModelInfo,
} from "./utils.ts";
import { readSettings } from "./settings.ts";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";

const DEFAULT_BASE_URL = "https://api.code.umans.ai";
const API_KEY_ENV = "UMANS_API_KEY";
const CONCURRENCY_DISABLE_ENV = "UMANS_CONCURRENCY_DISABLE";
const CONCURRENCY_LIMIT_ENV = "UMANS_CONCURRENCY_LIMIT";
const CONCURRENCY_STATE_FILE_ENV = "UMANS_CONCURRENCY_STATE_FILE";
// Override for the global settings file (~/.pi/agent/umans.json). Mirrors
// UMANS_CONCURRENCY_STATE_FILE: lets selfcheck point readSettings at a temp
// file without monkey-patching homedir. No-op in normal use.
const SETTINGS_FILE_ENV = "UMANS_SETTINGS_FILE";

// max time the head-waiter capacity poll will wait for a free slot
// before failing open (launching anyway). Bounds the queue against a
// hostile/misbehaving /usage that always reports full.
const CAPACITY_POLL_TIMEOUT_MS = 60_000;

// 429 strike counter bounds (mirrors index.ts — the side-call can't trip the
// 5h ban on its own, but the poll interval + window must match so a shared
// state file isn't wedged by mismatched constants).
const STRIKE_POLL_INTERVAL_MS = 5 * 60 * 1000; // 5 min
const STRIKE_WINDOW_MS = 24 * 60 * 60 * 1000; // 24h (rolling, matches the server)
const STRIKE_SERVER_LIMIT = 20; // server triggers 5h pause at >20 strikes/24h
const STRIKE_PAUSE_MS = 30 * 60 * 1000; // 30 min self-pause to let strikes age out

export const POLL_INTERVAL_BASE_MS = 300;
export const POLL_INTERVAL_CAP_MS = 2_000;
export const POLL_INTERVAL_GROWTH = 1.5;

type LaunchDecision = "launch" | "wait" | "failOpen" | "abort";
function decideLaunch(opts: {
  isFree: boolean;
  elapsedMs: number;
  queuePaused: boolean;
  signalAborted: boolean;
}): LaunchDecision {
  if (opts.signalAborted) return "abort";
  if (opts.isFree) return "launch";
  if (opts.elapsedMs >= CAPACITY_POLL_TIMEOUT_MS && !opts.queuePaused) return "failOpen";
  return "wait";
}

function nextPollInterval(currentMs: number, decision: LaunchDecision, opts?: { base?: number; cap?: number; growth?: number }): number {
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
 * Run a web search by making a sub-request to the Umans gateway with the
 * Anthropic `web_search_20250305` server tool declared. The gateway runs the
 * Exa search server-side and returns results; we surface the model's formatted
 * result text (titles, URLs, snippets) back to the calling model.
 *
 * Side-call because pi-ai only serializes client-side tools and cannot emit the
 * server-tool shape the gateway requires. Costs one extra round-trip per
 * search; no pi-ai changes needed.
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
  // AbortSignal.any (Node 20.3+). See index.ts analyzeImage for the rationale.
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
      // delegated to raiseForUmansStatus (shared with index.ts analyzeImage) —
      // runs the 429 push, reads + sanitizes the body, throws.
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

/**
 * Static fallback catalog lookup so web-search.ts can pick its search model
 * without depending on index.ts's runtime-fetched catalog. The search model
 * defaults to umans-flash (fastest); falls back to the first tool-capable
 * model in the passed catalog (when index.ts shares one) or the static
 * default. pickSearchModel (utils.ts) is the single source of truth for the
 * selection rule.
 */
function fallbackCatalogForSearch(): Record<string, UmansModelInfo> {
  return {
    "umans-flash": {
      name: "umans-flash",
      display_name: "Umans Flash",
      capabilities: {
        supports_tools: true,
        supports_vision: true,
      },
    },
  };
}

export default async function (pi: ExtensionAPI) {
  if (process.env.UMANS_DISABLE === "1") return;

  const baseUrl =
    process.env.UMANS_BASE_URL?.trim().replace(/\/$/, "") || DEFAULT_BASE_URL;

  // The search model is picked from the static fallback (web-search.ts does not
  // fetch /v1/models/info — that's index.ts's job). pickSearchModel defaults to
  // umans-flash, which is the fastest model + always tool-capable.
  const searchModelId = pickSearchModel(fallbackCatalogForSearch());

  // Cross-process FIFO queue — a SEPARATE instance from index.ts's. The two
  // instances coordinate through the shared ~/.pi/agent/umans-concurrency.json
  // state file (the file is a pure waiter queue + launch token; capacity is
  // decided by /v1/usage). UMANS_CONCURRENCY_DISABLE opts out
  // (fire-and-forget); UMANS_CONCURRENCY_STATE_FILE overrides the state file
  // path so the handler-wiring harness mock in selfcheck can point the real
  // queue at a tmpdir (isolating the test from the live state file).
  const concurrencyDisabled = process.env[CONCURRENCY_DISABLE_ENV] === "1";
  const concurrencyStateFile = process.env[CONCURRENCY_STATE_FILE_ENV]?.trim() || undefined;
  const concurrencyQueue: ConcurrencyQueue = createConcurrencyQueue({
    disabled: concurrencyDisabled,
    ...(concurrencyStateFile ? { stateFile: concurrencyStateFile } : {}),
  });

  // Cache settings once at startup. concurrencyLimit() is on the hot path
  // (per acquireSlot poll iteration + per strike poll), so re-reading the file
  // each call would hammer the filesystem. Restart picks up changes.
  // UMANS_SETTINGS_FILE overrides the global path so selfcheck can point at a
  // temp file; the project path resolves against cwd as in production.
  const settingsGlobalPath = process.env[SETTINGS_FILE_ENV]?.trim() || undefined;
  const cachedSettings = readSettings(
    settingsGlobalPath ? { globalPath: settingsGlobalPath } : undefined,
  );

  // Account-wide hard burst cap (the 429 threshold). The multiplier clamps the
  // effective limit to this so a high multiplier cannot push past the server's
  // burst ceiling. Populated from /v1/usage limits.concurrency.hard_cap.
  let hardCap: number | undefined;
  // Server soft cap (limits.concurrency.limit). The multiplier scales this
  // before the hard_cap clamp.
  let guaranteedConcurrency: number | undefined;
  let strikes24h: number | undefined;

  function concurrencyLimit(): number | undefined {
    // UMANS_CONCURRENCY_LIMIT env var is the absolute override (testing knob):
    // when set it wins outright, bypassing the multiplier. Takes precedence
    // over everything so existing CI/test scripts keep working.
    if (process.env[CONCURRENCY_LIMIT_ENV] !== undefined) {
      return parseConcurrencyLimit(process.env[CONCURRENCY_LIMIT_ENV], guaranteedConcurrency);
    }
    // No env override: scale the server's soft cap by the multiplier, then
    // clamp to hard_cap so a high multiplier cannot push past the server's
    // burst ceiling. multiplier 1.0 = full guaranteed concurrency (default);
    // 0.5 = half; 2.0 = burst into hard_cap headroom. floor() keeps the slot
    // count integral (0.5 of 4 = 2).
    const serverLimit = guaranteedConcurrency;
    if (serverLimit === undefined) return undefined; // /v1/usage not yet populated
    const multiplier = cachedSettings.concurrencyMultiplier;
    const scaled = Math.floor(serverLimit * multiplier);
    return hardCap !== undefined ? Math.min(scaled, hardCap) : scaled;
  }

  // Strike-counter refresh loop (mirrors index.ts). The side-call can't trip
  // the 5h ban on its own, but the poll keeps the shared strikes24h value
  // current so the threshold check runs against a fresh count. Bounded by
  // STRIKE_POLL_INTERVAL_MS (5 min) so /history RPS stays low.
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

  function scheduleStrikePoll(apiKey: string, immediate = false) {
    if (refreshStopped || !apiKey) return;
    strikeTimer = setTimeout(async () => {
      await refreshStrikes(apiKey);
      scheduleStrikePoll(apiKey);
    }, immediate ? 0 : STRIKE_POLL_INTERVAL_MS);
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

  async function refreshStrikes(apiKey: string) {
    const result = await fetch429Strikes(apiKey);
    if (result.suspended) {
      strikes24h = undefined;
      return;
    }
    if (result.count === null) return;
    const count = result.count;
    strikes24h = count;
    const maxInFlight = concurrencyLimit() ?? guaranteedConcurrency ?? 0;
    if (maxInFlight <= 0) return;
    const strikeThreshold = Math.max(0, STRIKE_SERVER_LIMIT - maxInFlight);
    if (count >= strikeThreshold) {
      const snap = concurrencyQueue.snapshot();
      const now = Date.now();
      const strikeUntil = now + STRIKE_PAUSE_MS;
      if (!snap.paused || snap.pausedUntil < strikeUntil) {
        try {
          concurrencyQueue.pauseUntil(strikeUntil, PAUSE_REASON_STRIKES);
        } catch (err) {
          console.warn("umans: pauseUntil threw in refreshStrikes (continuing):", err instanceof Error ? err.message : err);
        }
      }
    }
  }

  async function refreshUsage(apiKey: string) {
    const data = await fetchUsage(apiKey, 5000);
    if (!data) return;
    if (data.limits) {
      guaranteedConcurrency = data.limits.concurrency?.limit ?? undefined;
      hardCap = data.limits.concurrency?.hard_cap ?? undefined;
    }
    const priority = parsePriority(data.usage?.priority);
    if (!priority.low) {
      const snap = concurrencyQueue.snapshot();
      if (snap.paused && !(snap.pausedReason && STICKY_PAUSE_REASONS.has(snap.pausedReason))) {
        concurrencyQueue.clearPause();
      }
    }
  }

  // Acquire a concurrency slot for the web-search side-call. Mirrors index.ts's
  // acquireSlot: joins the cross-process FIFO, waits until head + claims the
  // launch token, polls /v1/usage until a free slot, returns a release fn. The
  // `apiKey` is used for the head-waiter poll. Returns undefined when the queue
  // is disabled or the turn's AbortSignal fires mid-poll.
  async function acquireSlot(apiKey: string, signal?: AbortSignal): Promise<(() => void) | undefined> {
    const initialId = concurrencyQueue.join();
    if (!initialId) return undefined; // queue disabled
    let ourId: string = initialId;
    let released = false;
    const MAX_TOKEN_REJOINS = 3;
    let releaseToken: () => void = () => {};
    try {
    tokenAcquire: for (let rejoins = 0; rejoins <= MAX_TOKEN_REJOINS; rejoins++) {
      let releaseTokenThisIter: () => void;
      try {
        releaseTokenThisIter = await concurrencyQueue.waitForLaunch(ourId, signal);
      } catch (err) {
        if (signal?.aborted) {
          released = true;
          return undefined;
        }
        throw err;
      }
      releaseToken = releaseTokenThisIter;
      const limit = concurrencyLimit();
      const capacityFree = async (queuePaused: boolean): Promise<boolean> => {
        const snap = await fetchUsageSnapshot(apiKey, signal);
        const qSnap = concurrencyQueue.snapshot();
        const decision = isCapacityFree(snap, {
          limit,
          queuePaused,
          localInFlight: qSnap.inflightCount,
        });
        if (decision.repause) {
          const alreadyCovered = queuePaused &&
            qSnap.pausedUntil >= decision.repause.until &&
            qSnap.pausedReason === decision.repause.reason;
          if (!alreadyCovered) {
            try {
              concurrencyQueue.pauseUntil(decision.repause.until, decision.repause.reason ?? undefined);
            } catch (err) {
              console.warn("umans: pauseUntil threw in capacityFree (continuing):", err instanceof Error ? err.message : err);
            }
          }
        }
        return decision.free;
      };
      const pollStart = Date.now();
      let pollIntervalMs = POLL_INTERVAL_BASE_MS;
      for (;;) {
        if (!concurrencyQueue.touchToken(ourId)) {
          try { concurrencyQueue.cancel(ourId); } catch { /* best-effort */ }
          if (rejoins >= MAX_TOKEN_REJOINS) {
            releaseToken = () => {};
            break;
          }
          ourId = concurrencyQueue.join()!;
          continue tokenAcquire;
        }
        const queuePaused = concurrencyQueue.snapshot().paused;
        const isFree = await capacityFree(queuePaused);
        const decision = decideLaunch({
          isFree,
          elapsedMs: Date.now() - pollStart,
          queuePaused,
          signalAborted: !!signal?.aborted,
        });
        if (decision === "launch") break;
        if (decision === "abort") {
          try { releaseToken(); } catch { /* best-effort */ }
          try { concurrencyQueue.cancel(ourId); } catch { /* best-effort */ }
          released = true;
          return undefined;
        }
        if (decision === "failOpen") {
          break;
        }
        await new Promise((r) => setTimeout(r, pollIntervalMs + Math.floor(Math.random() * 100)));
        pollIntervalMs = nextPollInterval(pollIntervalMs, "wait");
      }
      concurrencyQueue.addInFlight(ourId);
      try { releaseToken(); } catch { /* best-effort — release-resilience */ }
      releaseToken = () => {};
      released = true;
      return () => {
        releaseToken();
        try { concurrencyQueue.removeInFlight(ourId); } catch { /* best-effort */ }
        concurrencyQueue.cancel(ourId);
      };
    }
    return undefined;
    } finally {
      if (!released) {
        try { concurrencyQueue.cancel(ourId); } catch { /* best-effort */ }
      }
    }
  }

  function releaseSlot(release: (() => void) | undefined): void {
    if (!release) return;
    try {
      try {
        release();
      } catch (err) {
        console.warn("umans: concurrency release threw (release continues):", err instanceof Error ? err.message : err);
      }
    } finally {
      // no mainTurnRelease tracking here — web-search side-calls manage their
      // own release in the tool execute finally (mirrors index.ts's side-call
      // release pattern).
    }
  }

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
      // the main turn past the soft cap.
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

  // Seed the refresh loops on session_start so the multiplier + strikes values
  // are populated before the first side-call. session_shutdown stops the
  // loops + resets this process's queue entry (mirrors index.ts).
  pi.on("session_start", async (_event, ctx) => {
    const apiKey = await resolveApiKey(ctx);
    if (apiKey) await refreshUsage(apiKey);
    if (!refreshStopped) {
      scheduleStrikePoll(apiKey || "", true);
    }
  });

  pi.on("session_shutdown", async () => {
    stopRefreshLoop();
    concurrencyQueue.reset();
  });
}
