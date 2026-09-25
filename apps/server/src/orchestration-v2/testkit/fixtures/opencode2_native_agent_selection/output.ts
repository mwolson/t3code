import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";
import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import { assertMultiTurnOutput } from "../multi_turn/codex_output.ts";
import { projectionFor } from "../shared.ts";

export function assertOpenCode2NativeAgentSelectionOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertMultiTurnOutput(result, transcript);
  assert.equal(projectionFor(result, transcript.scenario).thread.interactionMode, "plan");
  assert.equal(
    result.domainEvents.filter((event) => event.type === "thread.interaction-mode-updated").length,
    1,
  );
}
