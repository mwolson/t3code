import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import { assertMultiTurnOutput } from "../multi_turn/codex_output.ts";
import { projectionFor } from "../shared.ts";

export function assertOpenCode2NativeModelSelectionOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertMultiTurnOutput(result, transcript);
  const projection = projectionFor(result, transcript.scenario);
  const [first, second] = projection.runs;
  assert.isDefined(first);
  assert.isDefined(second);
  assert.deepStrictEqual(second!.modelSelection, first!.modelSelection);
  assert.strictEqual(projection.thread.runtimeMode, "full-access");
  assert.strictEqual(projection.thread.interactionMode, "default");
  const created = result.domainEvents.find(
    (event) => event.type === "run.created" && event.runId === second!.id,
  );
  assert.strictEqual(created?.type, "run.created");
  if (created?.type === "run.created") assert.strictEqual(created.payload.status, "queued");
}
