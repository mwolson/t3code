import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";
import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  backgroundNotifications,
  projectionFor,
} from "../shared.ts";

export function assertClaudeBackgroundTaskWakeAfterUserOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({
    result,
    transcript,
    runCount: 3,
    runStatuses: ["completed", "completed", "completed"],
  });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assert.deepEqual(
    projection.runs.map((run) =>
      projection.turnItems.flatMap((item) =>
        item.runId === run.id && item.type === "assistant_message" ? [item.text.trim()] : [],
      ),
    ),
    [["STARTED"], ["USER_REPLY"], ["WAKE_DONE"]],
  );
  assert.deepEqual(
    projection.runs.map((run) => {
      const message = projection.messages.find((candidate) => candidate.id === run.userMessageId);
      return `${message?.createdBy}:${message?.creationSource}`;
    }),
    ["user:web", "user:web", "agent:provider"],
  );
  assert.deepEqual(projection.runs[2]?.modelSelection, projection.runs[0]?.modelSelection);
  const pending = result.capturedShellSnapshots
    .get("intervening-user-complete")
    ?.threads.find((thread) => thread.id === projection.thread.id);
  assert.equal(pending?.status, "completed");
  assert.equal(pending?.pendingBackgroundTasks?.[0]?.taskId, "bdqirlcyw");
  const thinking = result.capturedShellSnapshots
    .get("wake-before-output")
    ?.threads.find((thread) => thread.id === projection.thread.id);
  assert.equal(thinking?.status, "running");
  assert.deepEqual(backgroundNotifications(projection), [
    {
      summary: 'Command "Background sleep test" finished',
      outcome: "completed",
      source: { kind: "command" },
    },
  ]);
  assert.deepEqual(projection.providerThreads[0]?.pendingBackgroundTasks ?? [], []);
  assert.lengthOf(projection.subagents, 0);
}
