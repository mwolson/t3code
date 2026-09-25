import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import { assertQueuedTurnOutput } from "../queued_turn/codex_output.ts";
import { projectionFor } from "../shared.ts";

export function assertQueuedModeReflectionOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertQueuedTurnOutput(result, transcript);
  const projection = projectionFor(result, transcript.scenario);
  const queuedRun = projection.runs[1]!;
  const created = result.storedEvents.find(
    (stored) => stored.event.type === "run.created" && stored.event.payload.id === queuedRun.id,
  )!;
  const started = result.storedEvents.find(
    (stored) =>
      stored.event.type === "run.updated" &&
      stored.event.payload.id === queuedRun.id &&
      stored.event.payload.status === "starting",
  )!;
  const updates = result.storedEvents.filter(
    (stored) => stored.event.type === "thread.interaction-mode-updated",
  );
  const postStartUserChange = transcript.scenario.endsWith("user_aba");
  assert.equal(updates.length, postStartUserChange ? 2 : 3);
  const [out, back, reflected] = updates;
  assert.isAbove(out!.sequence, created.sequence);
  assert.isAbove(back!.sequence, out!.sequence);
  if (postStartUserChange) {
    assert.isAbove(out!.sequence, started.sequence);
    assert.isFalse(
      result.storedEvents.some((stored) =>
        String(stored.commandId).startsWith("command:provider-mode-reflection:"),
      ),
    );
  } else {
    assert.isAbove(started.sequence, back!.sequence);
    assert.isAbove(reflected!.sequence, started.sequence);
    assert.match(String(reflected!.commandId), /^command:provider-mode-reflection:/);
  }
  assert.equal(
    DateTime.toEpochMillis(out!.event.occurredAt),
    DateTime.toEpochMillis(back!.event.occurredAt),
  );
  assert.equal(projection.thread.interactionMode, postStartUserChange ? "plan" : "default");
  assert.equal(projection.thread.runtimeMode, "full-access");
  for (const run of projection.runs)
    assert.deepEqual(run.modelSelection, projection.thread.modelSelection);
}
