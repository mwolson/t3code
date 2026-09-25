import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertAssistantTextIncludes,
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  assertUserMessagesInclude,
  assertVisibleTurnItemsMirrorLocalTurnItems,
  OPENCODE2_RETRY_MISSING_FINISH_NO_START_PROMPT,
  projectionFor,
} from "../shared.ts";

export function assertOpenCode2RetryMissingFinishNoStartOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({ result, transcript, runCount: 1, runStatuses: ["completed"] });

  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertVisibleTurnItemsMirrorLocalTurnItems(projection);
  assertUserMessagesInclude(projection, [OPENCODE2_RETRY_MISSING_FINISH_NO_START_PROMPT]);
  assertAssistantTextIncludes(projection, "missing start retry complete");
  assert.isFalse(
    projection.turnItems.some((item) => item.type === "error"),
    "a retried model step must not fail a turn whose execution start was lost",
  );
}
