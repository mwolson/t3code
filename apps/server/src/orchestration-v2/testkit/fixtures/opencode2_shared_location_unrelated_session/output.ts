import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertAssistantTextIncludes,
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  assertUserMessagesInclude,
  assertVisibleTurnItemsMirrorLocalTurnItems,
  OPENCODE2_SHARED_LOCATION_PROMPT,
  projectionFor,
} from "../shared.ts";

export function assertOpenCode2SharedLocationUnrelatedSessionOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({ result, transcript, runCount: 1, runStatuses: ["completed"] });

  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertVisibleTurnItemsMirrorLocalTurnItems(projection);
  assertUserMessagesInclude(projection, [OPENCODE2_SHARED_LOCATION_PROMPT]);
  assertAssistantTextIncludes(projection, "owned session complete");
  assert.isFalse(
    projection.turnItems.some((item) => item.type === "error"),
    "the owned session's retried step must not fail the turn",
  );
  assert.isFalse(
    projection.turnItems.some(
      (item) =>
        item.type === "assistant_message" &&
        item.text.includes("orphan output must stay out of the owned turn"),
    ),
    "events from another session in the same location must not reach this turn",
  );
}
