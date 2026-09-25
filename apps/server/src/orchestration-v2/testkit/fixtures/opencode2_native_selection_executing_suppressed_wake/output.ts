import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import { assertMultiTurnOutput } from "../multi_turn/codex_output.ts";
import { OPENCODE2_MODEL_SELECTION, projectionFor } from "../shared.ts";

export function assertOpenCode2ExecutingSuppressedSelectionOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertMultiTurnOutput(result, transcript);
  const projection = projectionFor(result, transcript.scenario);
  assert.strictEqual(projection.thread.runtimeMode, "full-access");
  assert.strictEqual(projection.thread.interactionMode, "default");
  assert.deepEqual(projection.thread.modelSelection, OPENCODE2_MODEL_SELECTION);
  for (const run of projection.runs) {
    assert.deepEqual(run.modelSelection, OPENCODE2_MODEL_SELECTION);
  }
  assert.notInclude(JSON.stringify(projection), "CANCELLED_CHILD");
  assert.notInclude(JSON.stringify(projection), "CANCELLED_OUTPUT_MUST_NOT_APPEAR");
}
