import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";
import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import { assertBaseProjection, projectionFor } from "../shared.ts";
export function assertOpenCode2ModeReflectionOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({ result, transcript, runCount: 1, runStatuses: ["completed"] });
  assert.equal(projectionFor(result, transcript.scenario).thread.interactionMode, "default");
  assert.equal(
    result.domainEvents.filter((event) => event.type === "thread.interaction-mode-updated").length,
    1,
  );
}
