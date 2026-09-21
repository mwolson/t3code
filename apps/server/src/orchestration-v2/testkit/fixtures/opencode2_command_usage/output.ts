import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";
import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import { assertBaseProjection, assertAssistantTextIncludes, projectionFor } from "../shared.ts";

export function assertOpenCode2CommandUsageOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({ result, transcript, runCount: 1, runStatuses: ["completed"] });
  const projection = projectionFor(result, transcript.scenario);
  assertAssistantTextIncludes(projection, "fixture simple ok");
  assert.deepEqual(projection.providerTurns.at(-1)?.turnTokenUsage, {
    usageScope: "main_agent",
    usageStatus: "complete",
    hasSubagents: false,
    inputTokens: 34,
    cachedInputTokens: 6,
    cacheCreationTokens: 8,
    outputTokens: 14,
    reasoningTokens: 4,
  });
}
