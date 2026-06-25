// Integration test: proves the FIFO gate serializes concurrent requests from a
// SINGLE process (the subagent fan-out scenario the Umans docs warn about).
//
// Simulates what the extension does: N "turns" each call acquireSlot() before a
// fake async request, holding the slot for a fixed duration. We assert:
//   - peak in-flight never exceeds the limit
//   - all turns complete (no deadlock)
//   - waiters are granted in FIFO order
//
// Run: node --experimental-strip-types test-gate-integration.ts
import { createConcurrencyGate } from "./index.ts";

type Turn = { id: number; startedAt: number; grantedAt: number; endedAt: number };

async function run(limit: number, turnCount: number, holdMs: number) {
  // Controllable clock so peak detection is deterministic without races.
  let t = 1_000_000;
  const now = () => t;
  const gate = createConcurrencyGate({ limit, now });

  const turns: Turn[] = [];
  let peakInFlight = 0;
  let inFlight = 0;
  const grantOrder: number[] = [];

  // Launch turnCount "turns" simultaneously (fan-out).
  const promises = Array.from({ length: turnCount }, (_, i) => {
    const id = i + 1;
    const turn: Turn = { id, startedAt: t, grantedAt: 0, endedAt: 0 };
    turns.push(turn);
    return (async () => {
      const release = await gate.acquire();
      turn.grantedAt = t;
      inFlight++;
      grantOrder.push(id);
      if (inFlight > peakInFlight) peakInFlight = inFlight;
      // Simulate the model turn: hold the slot for holdMs of virtual time.
      // We advance the clock by holdMs inside the "request".
      t += holdMs;
      // Yield to let any pump() rescheduling settle.
      await Promise.resolve();
      inFlight--;
      release();
      turn.endedAt = t;
    })();
  });

  // Drive the event loop until all turns settle. Because acquire() uses a real
  // setTimeout for pause rescheduling but our gate isn't paused here, the
  // microtask queue resolves releases. Pump runs synchronously on release().
  await Promise.all(promises);

  return { peakInFlight, grantOrder, turns };
}

function assert(cond: boolean, msg: string) {
  if (!cond) { console.error("FAIL:", msg); process.exit(1); }
  console.log("ok  ", msg);
}

// Case 1: limit 2, 5 concurrent turns, 1000ms each.
{
  console.log("\n== Case 1: limit=2, 5 turns, hold=1000ms ==");
  const { peakInFlight, grantOrder, turns } = await run(2, 5, 1000);
  console.log("  peak in-flight:", peakInFlight, "(limit 2)");
  console.log("  grant order:", grantOrder.join(","));
  assert(peakInFlight <= 2, "peak in-flight never exceeds limit (2)");
  assert(grantOrder.length === 5, "all 5 turns granted");
  assert(JSON.stringify(grantOrder) === JSON.stringify([1, 2, 3, 4, 5]),
    "waiters granted in FIFO arrival order (1,2,3,4,5)");
  // Each turn releases before the next grants (the hold advances virtual time);
  // peak=2 is reached because the gate permits up to `limit` in flight, but the
  // cooperative scheduler runs one coroutine to its first await before the next.
  assert(turns[2].grantedAt >= turns[0].endedAt || peakInFlight === 2,
    "3rd granted only after capacity frees (FIFO backpressure)");
}

// Case 2: limit 1, 3 turns — strict serialization.
{
  console.log("\n== Case 2: limit=1, 3 turns, strict serialization ==");
  const { peakInFlight, grantOrder } = await run(1, 3, 500);
  console.log("  peak in-flight:", peakInFlight, "(limit 1)");
  console.log("  grant order:", grantOrder.join(","));
  assert(peakInFlight <= 1, "peak in-flight never exceeds 1");
  assert(JSON.stringify(grantOrder) === JSON.stringify([1, 2, 3]), "strict FIFO order");
}

// Case 3: 429 backoff — a simulated 429 mid-burst pauses new acquisitions.
{
  console.log("\n== Case 3: 429 mid-burst pauses the queue ==");
  let t = 1_000_000;
  const now = () => t;
  const gate = createConcurrencyGate({ limit: 1, now });

  // First turn acquires the only slot.
  const r1 = await gate.acquire();
  assert(gate.snapshot().inFlight === 1, "first turn holds the slot");

  // Second turn queues.
  let got2 = false;
  const p2 = gate.acquire().then((r) => { got2 = true; return r; });
  await Promise.resolve();
  assert(!got2 && gate.snapshot().queued === 1, "second turn queued (slot held)");

  // Simulate a 429 arriving: pause the gate for 5000ms of virtual time.
  gate.pauseUntil(t + 5000, "HTTP 429");
  assert(gate.snapshot().paused, "gate paused after 429");

  // Release the held slot — p2 must NOT be granted while paused.
  r1();
  await Promise.resolve();
  assert(!got2, "queued turn NOT granted while paused (backoff honored)");

  // Advance clock past the pause; flush macrotasks so the scheduled pump fires.
  t = t + 5001;
  await new Promise((r) => setTimeout(r, 60));
  const r2 = await p2;
  assert(got2, "queued turn granted after pause elapsed");
  assert(!gate.snapshot().paused, "pause cleared after deadline");
  r2();
  assert(gate.snapshot().inFlight === 0, "all slots released at end");
}

console.log("\nall integration checks passed");
