import { describe, expect, it } from "vite-plus/test";

import { makeProviderReplayGate } from "./ProviderReplayGate.testkit.ts";

describe("ProviderReplayGate", () => {
  it("signals arrival before the held frame is released", async () => {
    const label = "held-frame";
    const gate = makeProviderReplayGate([label]);
    const reached = gate.waitForReached(label);
    let emitted = false;
    const emission = gate.beforeEmit(label).then(() => {
      emitted = true;
    });

    expect(await reached).toBe(true);
    expect(emitted).toBe(false);
    gate.release(label);
    await emission;
    expect(emitted).toBe(true);
    expect(await gate.waitForReached(label)).toBe(true);
    expect(await gate.waitForReached("unknown-frame")).toBe(false);
  });

  it("waits for opted-in frame processing after release", async () => {
    const gate = makeProviderReplayGate(["held-frame"]);
    const emission = gate.beforeEmit("held-frame", undefined, true);
    let processed = false;
    const receipt = gate.waitForProcessed("held-frame").then(() => {
      processed = true;
    });
    gate.release("held-frame");
    await emission;
    expect(processed).toBe(false);
    gate.afterEmit("held-frame");
    await receipt;
    expect(processed).toBe(true);
    await gate.waitForProcessed("untracked-frame");
  });

  it("stops waiting when the replay consumer is interrupted", async () => {
    const label = "held-frame";
    const gate = makeProviderReplayGate([label]);
    const controller = new AbortController();
    const waiting = gate.beforeEmit(label, controller.signal);

    expect(gate.hasReached(label)).toBe(true);
    controller.abort();
    await waiting;
    expect(gate.release(label)).toBe(true);
  });
});
