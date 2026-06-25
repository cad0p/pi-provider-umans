# pi-provider-umans

[Umans.ai](https://umans.ai) provider for [pi](https://shittycodingagent.ai) — speaks the **Anthropic Messages API** against the Umans Code gateway (`https://api.code.umans.ai`), with **dynamic model discovery** and **client-side vision handoff** for text-only models.

## Install

```bash
# From npm (once published)
pi install npm:pi-provider-umans

# From git
pi install git:github.com/user/pi-provider-umans

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

Current catalog:

| ID | Name | Vision | Reasoning | Context |
|---|---|---|---|---|
| `umans-kimi-k2.6` | Umans Kimi K2.6 | native | ✅ | 256K |
| `umans-kimi-k2.7` | Umans Kimi K2.7 Code | native | ✅ (always on) | 256K |
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

Umans enforces a **concurrency soft cap** on in-flight requests per account (e.g. 3–4 on paid plans, with ~2× headroom before hard 429 enforcement). Per the [Umans docs](https://app.umans.ai/offers/code/docs), each 429 **deprioritizes the whole account for ~30 minutes** (requests still go through, just slower), and **>10 concurrency 429s in a day triggers a 5-hour pause**. Because all keys under an account share the counter, a burst of parallel subagents can trip this easily.

This provider ships a **client-side FIFO concurrency gate** to keep you under the soft cap and avoid deprioritization:

- **`before_provider_request`** holds each outbound turn in a FIFO queue until a slot is free, so requests are serialized at the soft cap rather than fired-and-retried.
- The gate's cap is pushed live from **`/v1/usage`** (`limits.concurrency.limit`) every 5 s.
- When `/v1/usage` reports **`usage.priority.low === true`**, or the gateway returns a **429**, new acquisitions are **paused** until `boxed_until` (or a 30 s floor) elapses — so you stop generating new 429s while the account recovers.
- Vision handoff and web-search side-calls go through the **same gate**, so a multi-image handoff or a search burst can't push the main turn over the cap.
- The status bar shows live state: `gate <inFlight>+<queued>q` when there's traffic, and `PAUSED <Ns>` while backing off.

| Env var | Default | Effect |
|---|---|---|
| `UMANS_CONCURRENCY_DISABLE` | `0` | `1` disables the gate entirely (fire-and-forget; not recommended — you lose 429 protection). |
| `UMANS_CONCURRENCY_LIMIT` | (from `/v1/usage`) | Override the soft cap (handy for testing the queue with a low number). |

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
| `UMANS_CONCURRENCY_DISABLE` | `0` | `1` disables the FIFO concurrency gate. |
| `UMANS_CONCURRENCY_LIMIT` | (from `/v1/usage`) | Override the concurrency soft cap. |

## Getting an API Key

1. Log in to [app.umans.ai/billing](https://app.umans.ai/billing)
2. Go to Dashboard → API Keys
3. Generate a new key (shown only once — copy it immediately)
