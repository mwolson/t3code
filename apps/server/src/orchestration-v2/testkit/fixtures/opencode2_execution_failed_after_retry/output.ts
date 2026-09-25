import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  assertUserMessagesInclude,
  OPENCODE2_EXECUTION_FAILED_AFTER_RETRY_PROMPT,
  projectionFor,
} from "../shared.ts";

export function assertOpenCode2ExecutionFailedAfterRetryOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({ result, transcript, runCount: 1, runStatuses: ["failed"] });

  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertUserMessagesInclude(projection, [OPENCODE2_EXECUTION_FAILED_AFTER_RETRY_PROMPT]);
  const errors = projection.turnItems.filter((item) => item.type === "error");
  assert.lengthOf(errors, 1, "the turn fails once, on the execution failure");
  const errorItem = errors[0];
  if (errorItem?.type !== "error") throw new Error("OpenCode 2 failure item is missing");
  assert.strictEqual(errorItem.failure.code, "provider.invalid-output");
  assert.strictEqual(
    errorItem.failure.message,
    "OpenCode 2 ended a model step without a finish reason.",
  );
  assert.isUndefined(errorItem.retry, "the retried step had already started");
}
