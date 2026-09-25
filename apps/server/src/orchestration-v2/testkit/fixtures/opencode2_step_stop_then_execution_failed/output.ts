import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  assertUserMessagesInclude,
  OPENCODE2_STEP_STOP_THEN_EXECUTION_FAILED_PROMPT,
  projectionFor,
} from "../shared.ts";

export function assertOpenCode2StepStopThenExecutionFailedOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({ result, transcript, runCount: 1, runStatuses: ["failed"] });

  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertUserMessagesInclude(projection, [OPENCODE2_STEP_STOP_THEN_EXECUTION_FAILED_PROMPT]);
  const errorItem = projection.turnItems.find((item) => item.type === "error");
  if (errorItem?.type !== "error") throw new Error("OpenCode 2 failure item is missing");
  assert.strictEqual(errorItem.failure.code, "provider.unavailable");
}
