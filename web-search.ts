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
 * The refresh + fetch + capacity-poll machinery (refreshStrikes,
 * refreshUsage, fetchUsage, fetchUsageSnapshot, fetch429Strikes, decideLaunch,
 * nextPollInterval, resolveApiKey, concurrencyLimit, the capacity-poll +
 * strike constants) is imported from utils.ts — ONE copy shared with index.ts
 * so the two factories cannot diverge on the strike-pause gate (a prior
 * duplicated copy missed the deprioritized gate + never started the periodic
 * refreshUsage loop). Each factory builds a ConcurrencyRuntime carrying its
 * own queue instance + mutable capacity state + calls the shared functions
 * with it. searchWeb + raiseForUmansStatus + pickSearchModel + the search
 * constants are also imported from utils.ts.
 */
import {
  createConcurrencyQueue,
  isCapacityFree,
  type ConcurrencyQueue,
} from "./concurrency-queue.ts";
import {
  USER_AGENT,
  SEARCH_TIMEOUT_MS,
  SEARCH_MAX_TOKENS,
  pickSearchModel,
  raiseForUmansStatus,
  decideLaunch,
  nextPollInterval,
  POLL_INTERVAL_BASE_MS,
  fetchUsageSnapshot,
  resolveApiKey,
  concurrencyLimit as sharedConcurrencyLimit,
  releaseSlotCore,
  stopRefreshLoop,
  restartRefreshLoop,
  refreshUsage as sharedRefreshUsage,
  type ConcurrencyRuntime,
  type UmansModelInfo,
} from "./utils.ts";
import { readSettings } from "./settings.ts";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";

const DEFAULT_BASE_URL = "https://api.code.umans.ai";
const CONCURRENCY_DISABLE_ENV = "UMANS_CONCURRENCY_DISABLE";
const CONCURRENCY_STATE_FILE_ENV = "UMANS_CONCURRENCY_STATE_FILE";
// Override for the global settings file (~/.pi/agent/umans.json). Mirrors
// UMANS_CONCURRENCY_STATE_FILE: lets selfcheck point readSettings at a temp
// file without monkey-patching homedir. No-op in normal use.
const SETTINGS_FILE_ENV = "UMANS_SETTINGS_FILE";

// The capacity-poll + strike-counter constants (CAPACITY_POLL_TIMEOUT_MS,
// STRIKE_*, POLL_INTERVAL_*), the pure decideLaunch / nextPollInterval
// helpers, and the resolveApiKey / concurrencyLimit / refresh + fetch
// machinery all live in utils.ts — shared with index.ts so the two factories
// cannot diverge on the strike-pause gate. This factory builds a
// ConcurrencyRuntime carrying its own queue instance + mutable capacity state
// + calls the shared functions with it.

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

  // Shared concurrency runtime: carries this factory's queue instance + the
  // mutable capacity state read/written by the shared refresh/fetch machinery
  // in utils.ts (one copy of refreshStrikes/refreshUsage/fetchUsage/etc,
  // shared with index.ts so the strike-pause gate cannot diverge). web-search.ts
  // has no status bar, so afterRefreshUsage is left undefined — the shared
  // refreshUsage still updates rt.deprioritized (the strike-pause gate) + the
  // capacity state; it just has no status-bar hook to call.
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
  };

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
      const limit = sharedConcurrencyLimit(rt);
      const capacityFree = async (queuePaused: boolean): Promise<boolean> => {
        const snap = await fetchUsageSnapshot(rt, apiKey, signal);
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

  // releaseSlotCore (from utils.ts) runs the release closure swallowing
  // throws so a transient lock/disk error does not abort the caller's turn
  // (the 120s watchdog reaps the stale token/waiter regardless). web-search
  // side-calls manage their own release in the tool execute finally — no
  // mainTurnRelease tracking (index.ts-only).

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
        releaseSlotCore(release);
      }
    },
  });

  // Seed the refresh loops on session_start so the multiplier + strikes values
  // are populated before the first side-call. session_shutdown stops the
  // loops + resets this process's queue entry (mirrors index.ts).
  // Seed the refresh loops on session_start so the multiplier + strikes values
  // are populated before the first side-call. restartRefreshLoop starts BOTH
  // the periodic refreshUsage (5s) AND the immediate strike poll — mirroring
  // index.ts's session_start wiring so the multiplier + strike count stay
  // current mid-session (a prior copy only ran refreshUsage once at startup +
  // never started the periodic loop, leaving the limits stale mid-session).
  // session_shutdown stops the loops + resets this process's queue entry.
  pi.on("session_start", async (_event, ctx) => {
    const apiKey = await resolveApiKey(ctx);
    if (apiKey) await sharedRefreshUsage(rt, apiKey);
    restartRefreshLoop(rt, apiKey || "");
  });

  pi.on("session_shutdown", async () => {
    stopRefreshLoop(rt);
    concurrencyQueue.reset();
  });
}
