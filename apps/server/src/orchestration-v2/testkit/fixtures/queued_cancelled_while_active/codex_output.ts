import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import { assertBaseProjection, projectionFor } from "../shared.ts";
import { ACTIVE_RUN_SHELL_SNAPSHOT_KEY } from "./input.ts";

export function assertQueuedCancelledWhileActiveOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({
    result,
    transcript,
    runCount: 2,
    runStatuses: ["completed", "cancelled"],
    providerTurnCountAtLeast: 1,
  });

  const projection = projectionFor(result, transcript.scenario);
  const activeRun = projection.runs[0];
  const cancelledRun = projection.runs[1];
  assert.isDefined(activeRun);
  assert.isDefined(cancelledRun);
  assert.isNull(cancelledRun.startedAt);
  const cancelledMessage = projection.messages.find(
    (message) => message.id === cancelledRun.userMessageId,
  );
  assert.isDefined(cancelledMessage);
  assert.equal(cancelledMessage.createdBy, "agent");
  assert.equal(cancelledMessage.creationSource, "server");

  const capturedShell = result.capturedShellSnapshots
    .get(ACTIVE_RUN_SHELL_SNAPSHOT_KEY)
    ?.threads.find((thread) => thread.id === projection.thread.id);
  assert.isDefined(capturedShell);
  assert.equal(capturedShell.latestRunId, cancelledRun.id);
  assert.equal(capturedShell.status, "cancelled");
  assert.equal(capturedShell.activeRunId, activeRun.id);
  // The replay gate can open before CTM's title-regeneration work emits the
  // run-start transition. Both statuses represent the active foreground run
  // and map to Working, while the cancelled latest run must not win.
  assert.include(["starting", "running"], capturedShell.activityRunStatus);
  assert.deepEqual(capturedShell.pendingBackgroundTasks, []);
}
