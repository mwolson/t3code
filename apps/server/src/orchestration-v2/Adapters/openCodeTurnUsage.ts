import type { TurnTokenUsage } from "@t3tools/contracts";
import type { TokenUsageInfo } from "@opencode-ai/client";

export function makeOpenCode2TurnUsage() {
  return {
    steps: new Map<string, TokenUsageInfo>(),
    complete: true,
    hasSubagents: false,
  };
}

export type OpenCode2TurnUsage = ReturnType<typeof makeOpenCode2TurnUsage>;

/** OC2 step tokens are disjoint counts, just like OC1 step-finish tokens. */
export function openCode2TurnTokenUsage(
  usage: OpenCode2TurnUsage,
  completed: boolean,
): TurnTokenUsage {
  const common = { usageScope: "main_agent" as const, hasSubagents: usage.hasSubagents };
  if (usage.steps.size === 0) return { ...common, usageStatus: "unavailable" };
  let inputTokens = 0;
  let outputTokens = 0;
  let cachedInputTokens = 0;
  let cacheCreationTokens = 0;
  let reasoningTokens = 0;
  for (const tokens of usage.steps.values()) {
    inputTokens += tokens.input + tokens.cache.read + tokens.cache.write;
    outputTokens += tokens.output + tokens.reasoning;
    cachedInputTokens += tokens.cache.read;
    cacheCreationTokens += tokens.cache.write;
    reasoningTokens += tokens.reasoning;
  }
  return {
    ...common,
    usageStatus: completed && usage.complete ? "complete" : "partial",
    inputTokens,
    outputTokens,
    cachedInputTokens,
    cacheCreationTokens,
    reasoningTokens,
  };
}
