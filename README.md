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

## Concurrency & rate-limit safety

Umans enforces a **concurrency soft cap** on in-flight requests per account (e.g. 3–4 on paid plans, with ~2× headroom before hard 429 enforcement). Per the [Umans docs](https://app.umans.ai/offers/code/docs), each 429 **deprioritizes the whole account for ~30 minutes** (requests still go through, just slower), and **>10 concurrency 429s in a day triggers a 5-hour pause**. Because all keys under an account share the counter, a burst of parallel subagents — or several `pi` processes on the same machine — can trip this easily.

Unlike the OpenAI/Anthropic SDKs (which retry 429s reactively with backoff), this provider gates proactively because Umans's 429 penalty is account-wide (~30min deprio) rather than per-request.

This provider ships a **cross-process FIFO queue** to keep you under the soft cap and avoid deprioritization:

- A single file at **`~/.pi/agent/umans-concurrency.json`** (guarded by an `O_EXCL` lockfile + atomic rename) holds a **pure waiter queue + launch token**. No in-flight count lives in the file.
- **Capacity is decided solely by the live `/v1/usage` response** (`concurrent_sessions` vs `limits.concurrency.limit`, and `usage.priority.low`), polled by the head waiter at ~300 ms. The file never tries to count slots it can't see — so multiple machines using the same key each run their own local queue and coordinate through the server + its headroom, not a local counter.
- The **launch token** is held from send until assistant `message_end` (the response stream has completed), so the next head's `/usage` poll sees the account's state and waits its turn. Launches are serialized within a machine; turns still stream concurrently and the slot frees for this turn's tool execution. The gate compares against `limit` (the soft cap) rather than `hard_cap` (the 429 threshold), so the burst headroom (`hard_cap - limit`) absorbs the small race between client-side stream completion and the server's `concurrent_sessions` decrement — the race overshoots `limit` by 1-2 but stays well below `hard_cap`, never tripping a 429.
- When `/v1/usage` reports **`priority.low === true`**, or the gateway returns a **429**, the shared `pausedUntil` is written to the file — **all** local `pi` processes back off together until it elapses (Retry-After aware). `priority.low` is account-wide, so cross-machine processes also see it via `/usage`.
- A **watchdog** reclaims the token if the holding process dies (PID check) or holds it >120 s, so a crashed turn never stalls the queue.
- Vision handoff and web-search side-calls go through the **same queue**.
- The status bar shows live state: `q <queued>*` (the `*` marks this process holding the token / launching) and `PAUSED <Ns>` while backing off.
- **Unlimited plans** (Code Max, `limit === undefined`): the queue still serializes launches + honors `priority.low`, but skips the capacity check.
- **Operator reset**: `/umans-concurrency status` shows the queue depth + pause state; `/umans-concurrency reset` force-clears a poisoned pause (incl. a 429-origin pause) and drops this process's own waiter/token entry — useful for un-wedging without editing `~/.pi/agent/umans-concurrency.json` by hand.
- **Local filesystem required**: the queue state file lives at `~/.pi/agent/umans-concurrency.json` and must be on a local filesystem (not NFS / iCloud / Dropbox) — `O_EXCL` locking is broken on network filesystems, and a network-synced home can lose writes.
- **2s lockfile ceiling is a hard correctness bound**: the `O_EXCL` lockfile is created once at acquire time and never touched while held. A legitimately-slow writer (disk pressure, NFS) exceeding the 2s `lockTimeoutMs` will have its lockfile yanked mid-write, racing two writers (lost write). The current critical section (read-modify-write of the JSON) is sub-ms on local APFS, so the 2s ceiling has ~1000× headroom. If the queue is ever pointed at non-local storage, implement mtime-refresh-while-held (a `setInterval` that touches the lockfile mtime every 500ms while the lock is held, cleared on release) to decouple liveness from this correctness bound.

The concurrency env vars (`UMANS_CONCURRENCY_DISABLE`, `UMANS_CONCURRENCY_LIMIT`) are documented in the table below; the general Configuration table links back here to avoid duplicating them.

| Env var | Default | Effect |
|---|---|---|
| `UMANS_CONCURRENCY_DISABLE` | `0` | `1` disables the queue entirely (fire-and-forget; not recommended — you lose 429 protection). |
| `UMANS_CONCURRENCY_LIMIT` | (from `/v1/usage`) | Override the capacity check value (handy for testing the queue with a low number). |
| `UMANS_CONCURRENCY_STATE_FILE` | `~/.pi/agent/umans-concurrency.json` | Override the queue state file path (handy for multi-process isolation experiments). |

### 429 strike counter

The queue polls `/v1/usage/history` every 5 min to count concurrency 429s since the most recent `cap_suspended` bucket (the server resets the counter on reactivation, so pre-pause strikes are excluded to match the dashboard's behavior). When the count reaches the dynamic threshold — **20 minus the concurrency limit** (e.g. 20−4 = 16) — the queue **defensively self-pauses for 30 min** so strikes can age out of the rolling window rather than risk the 5h ban. The margin equals the max in-flight requests that could all 429 simultaneously before our next poll tips the server's counter over. The status bar shows `Strikes X/20` so you can see how close you are. The server's exact "limit hits today" counter lives on the dashboard (`app.umans.ai/api/account/cap-health`, NextAuth web-session only — not accessible via API key); the `/v1/usage/history` sum (excluding pre-pause strikes) matches the dashboard exactly.

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
| `UMANS_SEARCH_DISABLE` | `0` | `1` disables the `umans_web_search` tool (e.g. when you use your own MCP web-search tool). Vision handoff is unaffected. |
| `UMANS_CONCURRENCY_*` | — | See [Concurrency & rate-limit safety](#concurrency--rate-limit-safety) for `UMANS_CONCURRENCY_DISABLE`, `UMANS_CONCURRENCY_LIMIT`, and `UMANS_CONCURRENCY_STATE_FILE`. |

## Development & testing

- `npm run check` — `tsc --noEmit`, must be green.
- `npm test` — runs `selfcheck.ts` (`node --experimental-strip-types selfcheck.ts`), the branchy pure-logic + queue integration checks. Must pass before merge.
- `npm run test:integration` — runs `./test-concurrency-gate.sh`, the **cross-process serialization proof** (spawns N `pi` processes against the live gateway + asserts peak concurrency stays under the cap). This requires a real `UMANS_API_KEY` and is **not run in CI** — run it locally before merging any change to the concurrency queue. Usage: `./test-concurrency-gate.sh [limit] [jobs] [--runs N] [--min-peak N]` (e.g. `./test-concurrency-gate.sh 2 5 --runs 3 --min-peak 2`).

## Getting an API Key

1. Log in to [app.umans.ai/billing](https://app.umans.ai/billing)
2. Go to Dashboard → API Keys
3. Generate a new key (shown only once — copy it immediately)
