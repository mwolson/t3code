import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertAssistantTextIncludes,
  assertBaseProjection,
  assertRunOrdinals,
  assertSemanticProjectionIntegrity,
  projectionFor,
} from "../shared.ts";

export function assertOpenCode2BackgroundWakeExecutionFailureOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({
    result,
    transcript,
    runCount: 2,
    runStatuses: ["completed", "failed"],
  });

  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertRunOrdinals(projection, [1, 2]);
  assertAssistantTextIncludes(projection, "PARENT_RELEASED");

  const continuation = projection.runs[1];
  assert.isDefined(continuation);
  const continuationItems = projection.turnItems.filter((item) => item.runId === continuation!.id);
  assert.isTrue(
    continuationItems.some(
      (item) => item.type === "assistant_message" && item.text.includes("CHILD_BACKGROUND_OK"),
    ),
    "the retried step's output belongs to the wake continuation",
  );
  const errors = continuationItems.filter((item) => item.type === "error");
  assert.lengthOf(errors, 1);
  const errorItem = errors[0];
  if (errorItem?.type !== "error") throw new Error("wake continuation failure item is missing");
  assert.strictEqual(errorItem.failure.code, "provider.unavailable");
}
