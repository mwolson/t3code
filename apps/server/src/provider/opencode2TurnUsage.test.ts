import { assert, describe, it } from "@effect/vitest";
import { makeOpenCode2TurnUsage, openCode2TurnTokenUsage } from "./opencode2TurnUsage.ts";

describe("OpenCode 2 turn usage normalization", () => {
  it("sums disjoint step counts and replaces duplicate step snapshots", () => {
    const usage = makeOpenCode2TurnUsage();
    usage.steps.set("first", {
      input: 10,
      output: 20,
      reasoning: 3,
      cache: { read: 4, write: 5 },
    });
    usage.steps.set("first", {
      input: 11,
      output: 21,
      reasoning: 4,
      cache: { read: 5, write: 6 },
    });
    usage.steps.set("second", {
      input: 1,
      output: 2,
      reasoning: 3,
      cache: { read: 4, write: 5 },
    });
    assert.deepEqual(openCode2TurnTokenUsage(usage, true), {
      usageScope: "main_agent",
      usageStatus: "complete",
      hasSubagents: false,
      inputTokens: 32,
      outputTokens: 30,
      cachedInputTokens: 9,
      cacheCreationTokens: 11,
      reasoningTokens: 7,
    });
  });

  it("distinguishes unavailable, complete zero, partial transport, and interrupted usage", () => {
    const usage = makeOpenCode2TurnUsage();
    assert.strictEqual(openCode2TurnTokenUsage(usage, true).usageStatus, "unavailable");
    const tokens = { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } };
    usage.steps.set("step", tokens);
    assert.strictEqual(openCode2TurnTokenUsage(usage, true).usageStatus, "complete");
    assert.strictEqual(openCode2TurnTokenUsage(usage, false).usageStatus, "partial");
    usage.complete = false;
    usage.hasSubagents = true;
    assert.deepEqual(openCode2TurnTokenUsage(usage, true), {
      usageScope: "main_agent",
      usageStatus: "partial",
      hasSubagents: true,
      inputTokens: 0,
      outputTokens: 0,
      cachedInputTokens: 0,
      cacheCreationTokens: 0,
      reasoningTokens: 0,
    });
  });
});
