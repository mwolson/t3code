import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";
import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import { assertOpenCode2BackgroundChildStopOutput } from "../opencode2_background_child_stop/output.ts";
export function assertOpenCode2SettledBackgroundStopOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertOpenCode2BackgroundChildStopOutput(result, transcript);
  assert.isTrue(
    result.domainEvents.some(
      (event) =>
        event.type === "turn-item.updated" && event.payload.type === "run_interrupt_request",
    ),
  );
  assert.isFalse(
    result.domainEvents.some((event) => event.type === "provider-turn.interrupt-requested"),
  );
  for (const projection of result.projections.values()) {
    assert.isFalse(
      projection.providerThreads.some((thread) => (thread.pendingBackgroundTasks?.length ?? 0) > 0),
    );
  }
}
