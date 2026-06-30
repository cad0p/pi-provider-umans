# pi-provider-umans

[Umans.ai](https://umans.ai) provider for [pi](https://github.com/earendil-works/pi) — speaks the **Anthropic Messages API** against the Umans Code gateway (`https://api.code.umans.ai`), with **dynamic model discovery** and **client-side vision handoff** for text-only models.

## Install

```bash
# From npm (once published)
pi install npm:pi-provider-umans

# From git
pi install git:github.com/umans-ai/pi-provider-umans

# From local path (for development)
pi install ./pi-provider-umans

# Or try without installing
pi -e ./pi-provider-umans
```

## Setup

### Option 1: `/login` (recommended — persists in auth.json)

In pi, run:

```
/login umans
```

Paste your API key when prompted. It's stored securely in `~/.pi/agent/auth.json` — no env vars needed.

### Option 2: Environment variable

```bash
export UMANS_API_KEY="sk-your-key-here"
```

## Anthropic Messages endpoint

This provider talks to Umans through its **Anthropic-compatible `/v1/messages` endpoint** and registers with pi as `api: "anthropic-messages"`. Requests use the standard Anthropic wire format — the `anthropic-version: 2023-06-01` header, message-block content, and Anthropic adaptive thinking (`thinking.type: "adaptive"`, surfaced as effort levels) — rather than the OpenAI Chat Completions format the extension used previously. That makes it a drop-in for pi's Anthropic provider stack.

The gateway base URL defaults to `https://api.code.umans.ai`; override it with `UMANS_BASE_URL` to point at a different environment.

## Dynamic model discovery

Models and capabilities are fetched live from `/v1/models/info` at load time, so new Umans models appear automatically — no extension update needed. If the gateway is unreachable, a built-in fallback catalog still lets the provider register.

Representative catalog (live list fetched from /v1/models/info):

| ID | Name | Vision | Reasoning | Context |
|---|---|---|---|---|
| `umans-kimi-k2.6` | Umans Kimi K2.6 | native | ✅ | 256K |
| `umans-kimi-k2.7` | Umans Kimi K2.7 Code | native | ✅ (always on) | 256K |
| `umans-glm-5.1` | Umans GLM 5.1 | via-handoff | ✅ | 202K |
| `umans-glm-5.2` | Umans GLM 5.2 | via-handoff | ✅ | 406K |
| `umans-coder` | Umans Coder | native | ✅ (always on) | 256K |
| `umans-flash` | Umans Flash | native | ✅ | 256K |
| `umans-qwen3.6-35b-a3b` | Umans Qwen3.6 35B A3B | native | ✅ | 256K |

New models added by Umans appear automatically — no extension update needed.

## Vision handoff

Text-only Umans models — currently the GLM 5.1 / 5.2 models, marked `supports_vision: "via-handoff"` — can't see images at the gateway. When you attach an image to a message headed for one of them, this extension intercepts it **client-side**: the image is analyzed by a native-vision Umans model and replaced in the message with an `[Image analysis (image:ID)]: …` text block *before* the text model is called.

- The analysis **persists in the conversation** (KV-cache friendly — it isn't re-analyzed on every turn).
- The text model can call the **`umans_vision`** tool to ask targeted follow-up questions about an analyzed image — pass the image ID from the `[Image analysis (image:ID)]` block.
- Native-vision models (Kimi K2.6 / K2.7, Coder, Flash, Qwen) see images directly — no handoff.

### Vision model & toggle

The handoff model defaults to `umans-kimi-k2.7` (or the first available native-vision model). Control it live with `/umans-vision`:

```
/umans-vision                  # show status
/umans-vision on | off         # enable / disable handoff (off falls back to the gateway)
/umans-vision model <id>       # pick the vision model
```

Or seed at startup:

```bash
export UMANS_VISION_MODEL="umans-kimi-k2.7"   # vision model id
export UMANS_VISION_DISABLE="1"               # start with handoff off
```

## Web search

The built-in `umans_web_search` tool searches the web via the Umans gateway's built-in Exa (useful for current information you don't already have: recent events, live prices, latest library versions, current docs). It lives in its own extension file (`web-search.ts`) so you can toggle it independently of the provider with `pi config`.

### Toggling web search on/off

Use `pi config` to edit the `extensions` array in `settings.json` (global `~/.pi/agent/settings.json` or project `.pi/settings.json`). The package's default extension list includes both `index.ts` (the provider) + `web-search.ts` (the search tool). To disable the built-in web search (e.g. when you expose your own MCP web-search tool):

```jsonc
// ~/.pi/agent/settings.json or .pi/settings.json
{
  "extensions": [
    "+index.ts",          // provider always on
    "-web-search.ts"      // disable the built-in umans_web_search tool
  ]
}
```

`+path` force-includes an exact path; `-path` force-excludes it. Omitting the prefix uses the package's default (both on). Changes take effect on the next pi restart (not mid-session).

### Migration from `UMANS_SEARCH_DISABLE`

The `UMANS_SEARCH_DISABLE=1` env var has been **removed**. It existed because web search was previously in `index.ts` (a single entry point) with no file-level toggle. Now that web search is its own file, `pi config` replaces it cleanly:

| Before | After |
|---|---|
| `UMANS_SEARCH_DISABLE=1` | `pi config` to disable `web-search.ts` (or edit `settings.json` extensions: `-web-search.ts`) |

`pi config` survives restarts, is discoverable via `/settings`, + works at both global + project scope — none of which the env var offered.

## Concurrency & rate-limit safety

Umans enforces a **concurrency soft cap** on in-flight requests per account (e.g. 3–4 on paid plans, with ~2× headroom before hard 429 enforcement). Per the [Umans docs](https://app.umans.ai/offers/code/docs), each 429 **deprioritizes the whole account for ~30 minutes** (requests still go through, just slower), and **>10 concurrency 429s in a day triggers a 5-hour pause**. Because all keys under an account share the counter, a burst of parallel subagents — or several `pi` processes on the same machine — can trip this easily.

Unlike the OpenAI/Anthropic SDKs (which retry 429s reactively with backoff), this provider gates proactively because Umans's 429 penalty is account-wide (~30min deprio) rather than per-request.

This provider ships a **cross-process FIFO queue** to keep you under the soft cap and avoid deprioritization:

- A single file at **`~/.pi/agent/umans-concurrency.json`** (guarded by an `O_EXCL` lockfile + atomic rename) holds a **pure waiter queue + launch token**. No in-flight count lives in the file.
- **Capacity is decided solely by the live `/v1/usage` response** (`concurrent_sessions` vs `limits.concurrency.limit`, and `usage.priority.low`), polled by the head waiter at ~300 ms. The file never tries to count slots it can't see — so multiple machines using the same key each run their own local queue and coordinate through the server + its headroom, not a local counter.
- The **launch token** serializes the `/v1/usage` poll (no thundering herd on the capacity endpoint). It is released **immediately after the capacity check passes** (before the send), not held across the send — holding it across the send serialized to 1-at-a-time (over-serialization). Releasing immediately lets the next head poll + launch right away; the server's `/v1/usage` lag means the next head sees a stale-low `concurrent_sessions` + launches, achieving `limit`-concurrent saturation (4/4). The gate compares against `limit` (the soft cap), so the `hard_cap` burst headroom (hard_cap - limit) absorbs the race overshoot → no 429s.
- When `/v1/usage` reports **`priority.low === true`** (deprioritization), the queue **lowers the effective cap by 1** (e.g. 4 → 3) rather than fully pausing — work continues (requests still go through, just slower), with one extra slot of burst headroom for races. When the gateway returns a **429**, the shared `pausedUntil` is written to the file — **all** local `pi` processes back off together until it elapses (Retry-After aware). `priority.low` is account-wide, so cross-machine processes also see it via `/usage`.
- A **watchdog** reclaims the token if the holding process dies (PID check) or holds it >120 s, so a crashed turn never stalls the queue.
- Vision handoff and web-search side-calls go through the **same queue**.
- The status bar shows live state: `Conc <cur>/<limit>`, `DEPRIO <countdown>` (deprioritized, time until clear), `q <queued>*` (the `*` marks this process polling), `PAUSED <countdown> (<reason>)` (backing off, time until clear), and `Strikes X/20`.
- **Unlimited plans** (Code Max, `limit === undefined`): the queue still serializes launches + honors `priority.low`, but skips the capacity check.
- **Operator reset**: `/umans-concurrency status` shows the queue depth + pause state; `/umans-concurrency reset` force-clears a poisoned pause (incl. a 429-origin pause) and drops this process's own waiter/token entry — useful for un-wedging without editing `~/.pi/agent/umans-concurrency.json` by hand.
- **Local filesystem required**: the queue state file lives at `~/.pi/agent/umans-concurrency.json` and must be on a local filesystem (not NFS / iCloud / Dropbox) — `O_EXCL` locking is broken on network filesystems, and a network-synced home can lose writes.
- **2s lockfile ceiling is a hard correctness bound**: the `O_EXCL` lockfile is created once at acquire time and never touched while held. A legitimately-slow writer (disk pressure, NFS) exceeding the 2s `lockTimeoutMs` will have its lockfile yanked mid-write, racing two writers (lost write). The current critical section (read-modify-write of the JSON) is sub-ms on local APFS, so the 2s ceiling has ~1000× headroom. If the queue is ever pointed at non-local storage, implement mtime-refresh-while-held (a `setInterval` that touches the lockfile mtime every 500ms while the lock is held, cleared on release) to decouple liveness from this correctness bound.

The concurrency env vars (`UMANS_CONCURRENCY_DISABLE`, `UMANS_CONCURRENCY_LIMIT`) are documented in the table below; the general Configuration table links back here to avoid duplicating them.

| Env var | Default | Effect |
|---|---|---|
| `UMANS_CONCURRENCY_DISABLE` | `0` | `1` disables the queue entirely (fire-and-forget; not recommended — you lose 429 protection). |
| `UMANS_CONCURRENCY_LIMIT` | (from `/v1/usage`) | **Deprecated** (testing-only absolute override). When set, wins outright over the concurrency multiplier + the live `/v1/usage` limit. Handy for testing the queue with a low number. The user-facing knob is the concurrency multiplier (see [Settings file](#settings-file)). |
| `UMANS_CONCURRENCY_STATE_FILE` | `~/.pi/agent/umans-concurrency.json` | Override the queue state file path (handy for multi-process isolation experiments). |

### 429 strike counter

The queue polls `/v1/usage/history` every 5 min to count concurrency 429s since the most recent `cap_suspended` bucket (the server resets the counter on reactivation, so pre-pause strikes are excluded to match the dashboard's behavior). When the count reaches the dynamic threshold — **20 minus the concurrency limit** (e.g. 20−4 = 16) — the queue **defensively self-pauses for 30 min** so strikes can age out of the rolling window rather than risk the 5h ban. The margin equals the max in-flight requests that could all 429 simultaneously before our next poll tips the server's counter over. The status bar shows `Strikes X/20` so you can see how close you are. The server's exact "limit hits today" counter lives on the dashboard (`app.umans.ai/api/account/cap-health`, NextAuth web-session only — not accessible via API key); the `/v1/usage/history` sum (excluding pre-pause strikes) matches the dashboard exactly.

### 403 account suspension

Beyond 429 deprioritization, the Umans server can **escalate to a full account suspension** by returning HTTP 403 with a body indicating `account_suspended` / `cap_abuse` / `cap_suspended` / `billing_error` (the 403 is the HTTP symptom of the same underlying `cap_abuse` suspension the `/v1/usage` `priority.reason=cap_abuse` branch detects). The queue handles this with two user-visible reason strings:

- **`account cap_abuse suspension`** (`PAUSED <countdown> (account cap_abuse suspension)`): the server has escalated to a 5h suspension. The queue pushes a full pause until the body's `boxed_until` deadline clears (capped at the 5h `MAX_PAUSE_MS` ceiling; a longer real suspension self-heals via overhang re-push). The `/v1/usage` `cap_abuse` branch, the 403 response handler, and the `/v1/usage`-403 synthetic-snapshot path all push this same tag (a single tag eliminates the reason-flip fragility where a stale `/usage` tick could wipe a freshly-written pause).
- **`HTTP 403 bridge (awaiting body)`** (`PAUSED <countdown> (HTTP 403 bridge (awaiting body))`): a 5s **non-sticky** bridge pushed at `after_provider_response` headers time (the body has not streamed yet at headers time, so the suspend body cannot be inspected). It backs siblings off immediately + is cleared at `message_end` once the body streams (or by a stale `/usage` low===false tick, since it is not in the sticky set). This is NOT a full pause — it is a brief bridge that reconciles to the real state within 5s.

During a suspension, `/v1/usage/history` itself returns 403 (the server returns 403 for everything once suspended), so the cached strike count is cleared rather than shown as a stale `Strikes 19/20` for the full 5h — the bar shows no strike count until `/history` is reachable again.

### Umans API endpoints used

| Endpoint | Method | Purpose |
|---|---|---|
| `GET /v1/usage` | GET | Live limits + concurrent_sessions + priority.low (polled every 5s + on every head-waiter launch) |
| `GET /v1/usage/history` | GET | 24h 429 strike count (polled every 5 min for the defensive pause) |
| `POST /v1/keys/validate` | POST | Plan + max_concurrency (not used by the queue; `/v1/usage` provides `limit`. Documented for future per-key limit support) |
| `GET /v1/models/info` | GET | Model catalog (public, no auth) |
| `GET /v1/status` | GET | Service health (uptime, TTFT, TPS) |

The dashboard endpoint `app.umans.ai/api/account/cap-health` (exact strike counter) is **not accessible via API key** — it requires a NextAuth web session (Google OIDC or email/password). The `/v1/usage/history` strike sum is the API-accessible proxy.

## Configuration

| Env var | Default | Effect |
|---|---|---|
| `UMANS_API_KEY` | — | API key for inference (or use `/login umans`). |
| `UMANS_BASE_URL` | `https://api.code.umans.ai` | Override the gateway base URL. |
| `UMANS_BUDGET_THINKING` | `0` | `1` opts out of adaptive (effort-level) thinking into legacy budget-based thinking. |
| `UMANS_DISABLE` | `0` | `1` disables the extension entirely. |
| `UMANS_VISION_DISABLE` | `0` | `1` starts vision handoff off (toggle live with `/umans-vision`). |
| `UMANS_VISION_MODEL` | `umans-kimi-k2.7` | Seed the vision model id. |
| `UMANS_CONCURRENCY_*` | — | See [Concurrency & rate-limit safety](#concurrency--rate-limit-safety) for `UMANS_CONCURRENCY_DISABLE`, `UMANS_CONCURRENCY_LIMIT` (deprecated), and `UMANS_CONCURRENCY_STATE_FILE`. |

### Settings file

A provider-specific settings file lets you tune behavior without env vars (env vars don't survive restarts, aren't discoverable via `/settings`, + can't be scoped per-project). The file is owned by this package; pi's `settings.json` schema is owned by the pi CLI + extensions don't add fields to it.

**Schema** (today only the concurrency multiplier lives here):

```json
{
  "concurrencyMultiplier": 1.0
}
```

**Paths** (mirrors pi's own `~/.pi/agent/settings.json` + `.pi/settings.json` layout):

| Path | Scope |
|---|---|
| `~/.pi/agent/umans.json` | Global (all projects) |
| `.pi/umans.json` | Project-local (deep-merged over global) |

**Precedence** (highest to lowest):

1. `UMANS_CONCURRENCY_LIMIT` env var (deprecated absolute override, testing only — takes precedence over everything, bypasses the multiplier)
2. `.pi/umans.json` (project settings, deep-merged over global)
3. `~/.pi/agent/umans.json` (global settings)
4. Default (`concurrencyMultiplier: 1.0`)

**Merge semantics** (mirror pi's `deepMergeSettings` so users get predictable, familiar config layering):

- Nested objects → deep merged (project fields override only their key; global siblings survive).
- Arrays → fully replaced (project array replaces global entirely, NOT concatenated). No arrays in the schema today, but documented so a future array field doesn't surprise.
- Primitives → override (project wins).

**`concurrencyMultiplier`** scales the effective concurrency limit:

- `1.0` (default) = full guaranteed concurrency (the server's `limits.concurrency.limit`).
- `0.5` = conservative (half the guaranteed concurrency; e.g. limit 4 → effective 2).
- `2.0` = burst into `hard_cap` headroom (e.g. limit 4 → effective 8, the 429 threshold).
- `3.0`+ = clamped to `hard_cap` (e.g. limit 4 × 3.0 = 12, clamped to `hard_cap` 8) so a high multiplier cannot push past the server's burst ceiling.

The deprioritization lowering (`priority.low` → cap − 1) applies AFTER the multiplier: `effectiveLimit = max(0, min(floor(serverLimit × multiplier), hardCap) − (deprioritized ? 1 : 0))`.

Malformed JSON → warn + defaults (don't crash the provider). Missing file → defaults. Invalid `concurrencyMultiplier` (0, negative, NaN, non-number) → warn + default.

## Development & testing

- `npm run check` — `tsc --noEmit`, must be green.
- `npm test` — runs `selfcheck.ts` (`node --experimental-strip-types selfcheck.ts`), the branchy pure-logic + queue integration checks. Must pass before merge.
- `npm run test:integration` — runs `./test-concurrency-gate.sh`, the **cross-process serialization proof** (spawns N `pi` processes against the live gateway + asserts peak concurrency stays under the cap). This requires a real `UMANS_API_KEY` and is **not run in CI** — run it locally before merging any change to the concurrency queue. Usage: `./test-concurrency-gate.sh [limit] [jobs] [--runs N] [--min-peak N]` (e.g. `./test-concurrency-gate.sh 2 5 --runs 3 --min-peak 2`).

## Getting an API Key

1. Log in to [app.umans.ai/billing](https://app.umans.ai/billing)
2. Go to Dashboard → API Keys
3. Generate a new key (shown only once — copy it immediately)
