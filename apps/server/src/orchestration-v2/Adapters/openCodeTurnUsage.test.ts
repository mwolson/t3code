import { assert, describe, it } from "@effect/vitest";
import { makeOpenCode2TurnUsage, openCode2TurnTokenUsage } from "./openCodeTurnUsage.ts";
import { openCode2TokenUsage } from "./OpenCodeAdapterV2.ts";

describe("OpenCode turn usage normalization", () => {
  it("distinguishes unavailable, complete zero, partial transport, and interrupted usage", () => {
    const usage = makeOpenCode2TurnUsage();
    assert.strictEqual(openCode2TurnTokenUsage(usage, true).usageStatus, "unavailable");
    const tokens = { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } };
    assert.isNotNull(openCode2TokenUsage({ tokens }, true));
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
