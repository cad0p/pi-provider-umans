// ponytail: one runnable check for the branchy pure logic of the vision handoff.
// Verifies model picking (env override / default / fallback / none) and image-id
// hashing. Does NOT cover the network path (analyzeImage) — that's integration.
//
// Run: bun selfcheck.ts
import { isNativeVision, pickVisionModel, hashImageId, createConcurrencyGate, parsePriority, setGateLimit } from "./index.ts";

function vision(
  name: string,
  v: boolean | "via-handoff" = true,
  deprecation?: unknown,
) {
  return { name, capabilities: { supports_vision: v }, ...(deprecation ? { deprecation } : {}) };
}

const CATALOG = {
  "umans-kimi-k2.6": vision("umans-kimi-k2.6", true),
  "umans-kimi-k2.7": vision("umans-kimi-k2.7", true),
  "umans-glm-5.2": vision("umans-glm-5.2", "via-handoff"),
  "umans-coder": vision("umans-coder", true),
};

function assert(cond: boolean, msg: string) {
  if (!cond) {
    console.error("FAIL:", msg);
    process.exit(1);
  }
  console.log("ok  ", msg);
}

// --- isNativeVision: true only for non-deprecated native-vision models ---
assert(isNativeVision(vision("a", true)) === true, "native vision is native");
assert(isNativeVision(vision("a", "via-handoff")) === false, "via-handoff is not native");
assert(isNativeVision(vision("a", true, "deprecated")) === false, "deprecated is not native");
assert(isNativeVision(vision("a", false)) === false, "non-vision is not native");

// --- pickVisionModel ---
delete process.env.UMANS_VISION_MODEL;
assert(pickVisionModel(CATALOG) === "umans-kimi-k2.7", "defaults to kimi-k2.7 (not insertion-order k2.6)");

process.env.UMANS_VISION_MODEL = "umans-coder";
assert(pickVisionModel(CATALOG) === "umans-coder", "env override honored when native-vision");

process.env.UMANS_VISION_MODEL = "umans-glm-5.2"; // via-handoff, not native
assert(pickVisionModel(CATALOG) === "umans-kimi-k2.7", "env override pointing at via-handoff ignored");

process.env.UMANS_VISION_MODEL = "umans-does-not-exist";
assert(pickVisionModel(CATALOG) === "umans-kimi-k2.7", "unknown env override ignored");

const NO_KIMI = {
  "umans-glm-5.2": vision("umans-glm-5.2", "via-handoff"),
  "umans-coder": vision("umans-coder", true),
};
delete process.env.UMANS_VISION_MODEL;
assert(pickVisionModel(NO_KIMI) === "umans-coder", "falls back to first native-vision model");

const TEXT_ONLY = { "umans-glm-5.2": vision("umans-glm-5.2", "via-handoff") };
assert(pickVisionModel(TEXT_ONLY) === undefined, "undefined when no native-vision model");

// --- hashImageId: deterministic, unique, well-formed ---
const a = hashImageId("data-a");
assert(a === hashImageId("data-a"), "hash is deterministic");
assert(a !== hashImageId("data-b"), "hash differs for different images");
assert(/^img_[0-9a-f]{8}$/.test(a), "hash format is img_<8 hex>");

// --- parsePriority ---
{
  const p = parsePriority(undefined);
  assert(p.low === false && p.until === 0, "parsePriority: undefined -> not low");

  const lowIso = parsePriority({ low: true, boxed_until: null, reason: "burst" });
  assert(lowIso.low === true && lowIso.reason === "burst", "parsePriority: low=true, reason captured");
  assert(lowIso.until > Date.now(), "parsePriority: null boxed_until falls back to now+backoff");

  const lowNull = parsePriority({ low: false });
  assert(lowNull.low === false && lowNull.until === 0, "parsePriority: low=false -> until 0");

  const epoch = Math.floor((Date.now() + 60000) / 1000);
  const pNum = parsePriority({ low: true, boxed_until: epoch });
  assert(pNum.until === epoch * 1000, "parsePriority: numeric boxed_until (seconds) -> ms");
}

// --- createConcurrencyGate: FIFO + soft cap ---
{
  // Use a controllable clock so pause math is deterministic.
  let t = 1_000_000;
  const now = () => t;
  const gate = createConcurrencyGate({ limit: 2, now });

  // Fast path: two slots acquired immediately.
  const r1 = await gate.acquire();
  const r2 = await gate.acquire();
  let snap = gate.snapshot();
  assert(snap.inFlight === 2 && snap.queued === 0, "gate: 2 slots in flight, none queued");

  // Third acquire blocks (cap=2). Start without awaiting.
  let got3 = false;
  const p3 = gate.acquire().then((r) => { got3 = true; return r; });
  // Yield once: the waiter should NOT have been granted (no slot free).
  await Promise.resolve();
  snap = gate.snapshot();
  assert(snap.inFlight === 2 && snap.queued === 1 && !got3, "gate: 3rd request queued, not granted");

  // Release one slot -> p3 should be granted (FIFO).
  r1();
  const r3 = await p3;
  snap = gate.snapshot();
  assert(snap.inFlight === 2 && snap.queued === 0 && got3, "gate: release grants queued FIFO");

  // Release all; no leaks.
  r2(); r3();
  snap = gate.snapshot();
  assert(snap.inFlight === 0 && snap.queued === 0, "gate: all released -> empty");
}

// --- createConcurrencyGate: pause blocks new acquisitions ---
{
  let t = 1_000_000;
  const now = () => t;
  const gate = createConcurrencyGate({ limit: 1, now });

  const r1 = await gate.acquire();
  // Pause until t+5000.
  gate.pauseUntil(t + 5000, "429");
  let snap = gate.snapshot();
  assert(snap.paused && snap.pausedUntil === t + 5000, "gate: pauseUntil sets pause window");

  // Release the held slot; a new acquire should still block (paused), even
  // though capacity is now 0/1.
  r1();
  let got2 = false;
  const p2 = gate.acquire().then((r) => { got2 = true; return r; });
  await Promise.resolve();
  assert(!got2, "gate: acquire blocks while paused even with free capacity");

  // Advance the clock past the pause deadline. The gate reschedules pump via
  // setTimeout; flush macrotasks by awaiting a real timeout.
  t = t + 5001;
  await new Promise((r) => setTimeout(r, 50));
  const r2 = await p2;
  assert(got2, "gate: acquire granted after pause elapses");
  snap = gate.snapshot();
  assert(!snap.paused, "gate: pause cleared after deadline");

  r2();
}

// --- setGateLimit: dynamic cap push ---
{
  const gate = createConcurrencyGate({ limit: 3 });
  setGateLimit(gate, 1);
  const r1 = await gate.acquire();
  let snap = gate.snapshot();
  assert(snap.limit === 1, "setGateLimit: updates cap");
  let got2 = false;
  const p2 = gate.acquire().then(() => { got2 = true; });
  await Promise.resolve();
  assert(!got2 && gate.snapshot().queued === 1, "setGateLimit: new cap respected (2nd queues)");
  r1();
  await p2;
  assert(got2, "setGateLimit: queued granted on release");
}

console.log("\nall checks passed");
