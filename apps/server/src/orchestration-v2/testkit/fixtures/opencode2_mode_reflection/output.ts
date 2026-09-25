import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";
import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import { assertQueuedTurnOutput } from "../queued_turn/codex_output.ts";
import { projectionFor } from "../shared.ts";

export function assertOpenCode2ModeReflectionOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertQueuedTurnOutput(result, transcript);
  const projection = projectionFor(result, transcript.scenario);
  assert.equal(projection.thread.interactionMode, "default");
  assert.equal(projection.thread.runtimeMode, "full-access");
  assert.equal(
    result.domainEvents.filter((event) => event.type === "thread.interaction-mode-updated").length,
    1,
  );
  for (const run of projection.runs)
    assert.deepEqual(run.modelSelection, projection.thread.modelSelection);
}
