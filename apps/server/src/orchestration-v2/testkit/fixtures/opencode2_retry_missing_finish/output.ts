import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertAssistantTextIncludes,
  assertBaseProjection,
  assertConversationMessageRoles,
  assertSemanticProjectionIntegrity,
  assertTurnItemTypes,
  assertUserMessagesInclude,
  assertVisibleTurnItemsMirrorLocalTurnItems,
  OPENCODE2_RETRY_MISSING_FINISH_PROMPT,
  projectionFor,
} from "../shared.ts";

export function assertOpenCode2RetryMissingFinishOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({ result, transcript, runCount: 1, runStatuses: ["completed"] });

  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertVisibleTurnItemsMirrorLocalTurnItems(projection);
  assertConversationMessageRoles(projection, ["user", "assistant"]);
  assertTurnItemTypes(projection, ["user_message", "reasoning", "assistant_message"]);
  assertUserMessagesInclude(projection, [OPENCODE2_RETRY_MISSING_FINISH_PROMPT]);
  assertAssistantTextIncludes(projection, "missing finish retry complete");
  assert.isFalse(
    projection.turnItems.some((item) => item.type === "error"),
    "a model step that OpenCode retries inside the same execution must not fail the T3 turn",
  );
  const terminalStatuses = new Set(["completed", "interrupted", "failed", "cancelled"]);
  const settled = result.domainEvents.flatMap((event) =>
    event.type === "run.updated" && terminalStatuses.has(event.payload.status)
      ? [event.payload.status]
      : [],
  );
  assert.deepEqual(
    Array.from(new Set(settled)),
    ["completed"],
    "the run must settle only as completed, on native execution success",
  );
}
