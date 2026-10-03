import { assert } from "@effect/vitest";
import { EnvironmentId, type ProviderReplayTranscript } from "@t3tools/contracts";

import { presentThreadShell } from "../../../../../../../packages/client-runtime/src/state/models.ts";
import { presentPendingBackgroundWork } from "../../../../../../../packages/client-runtime/src/state/threadExecution.ts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  assertUserMessagesInclude,
  projectionFor,
} from "../shared.ts";
import {
  CLAUDE_BACKGROUND_TASK_AFTER_ROOT_PROMPT,
  CLAUDE_BACKGROUND_TASK_USER_FOLLOW_UP_PROMPT,
} from "./input.ts";

const BACKGROUND_TASK_ID = "bc9gkn8ei";

export function assertClaudeBackgroundTaskAfterRootOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({
    result,
    transcript,
    runCount: 1,
    runStatuses: ["completed"],
  });

  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertUserMessagesInclude(projection, [CLAUDE_BACKGROUND_TASK_AFTER_ROOT_PROMPT]);

  const waitingShell = result.capturedShellSnapshots
    .get("background-bash-waiting")
    ?.threads.find((thread) => thread.id === projection.thread.id);
  assert.isDefined(waitingShell);
  assert.equal(waitingShell.status, "completed");
  assert.equal(waitingShell.activeRunId, null);
  assert.deepEqual(waitingShell.pendingBackgroundTasks, [
    {
      taskId: BACKGROUND_TASK_ID,
      description: "sleep 25 && echo L2_BG_DONE",
      kind: "command",
      wakesAgent: true,
    },
  ]);
  const presented = presentThreadShell(EnvironmentId.make("replay"), waitingShell);
  assert.equal(presented.latestRun?.status, "completed");
  assert.equal(presented.runtime?.status, "idle");
  assert.deepEqual(presented.pendingBackgroundTasks, waitingShell.pendingBackgroundTasks);
  assert.isTrue(presentPendingBackgroundWork(presented.pendingBackgroundTasks ?? [])?.waiting);

  const rootRun = projection.runs[0];
  assert.isDefined(rootRun);
  const pendingRosterIndex = result.domainEvents.findIndex(
    (event) =>
      event.type === "provider-thread.updated" &&
      event.payload.pendingBackgroundTasks?.some((task) => task.taskId === BACKGROUND_TASK_ID),
  );
  const waitingRunIndex = result.domainEvents.findIndex(
    (event) =>
      event.type === "run.updated" &&
      event.runId === rootRun.id &&
      event.payload.status === "waiting",
  );
  const waitingRootNodeIndex = result.domainEvents.findIndex(
    (event) =>
      event.type === "node.updated" &&
      event.payload.runId === rootRun.id &&
      event.payload.kind === "root_turn" &&
      event.payload.status === "waiting",
  );
  const idleRosterIndex = result.domainEvents.findIndex(
    (event, index) =>
      index > waitingRunIndex &&
      event.type === "provider-thread.updated" &&
      event.payload.status === "idle" &&
      (event.payload.pendingBackgroundTasks?.length ?? 0) === 0,
  );

  assert.isAtLeast(pendingRosterIndex, 0, "replay must project the live background task roster");
  assert.isAbove(
    waitingRunIndex,
    pendingRosterIndex,
    "checkpoint-waiting root run must retain the background roster",
  );
  assert.isAbove(
    waitingRootNodeIndex,
    pendingRosterIndex,
    "checkpoint-waiting root node must retain the background roster",
  );
  assert.isAbove(
    idleRosterIndex,
    waitingRunIndex,
    "late background completion must clear the roster and return the provider thread to idle",
  );
  assert.equal(rootRun.status, "completed");
  const rootNode = projection.nodes.find(
    (node) => node.runId === rootRun.id && node.kind === "root_turn",
  );
  assert.isDefined(rootNode);
  assert.equal(rootNode.status, "completed");

  assert.lengthOf(projection.providerThreads, 1);
  assert.equal(projection.providerThreads[0]?.status, "idle");
  assert.deepEqual(projection.providerThreads[0]?.pendingBackgroundTasks ?? [], []);
  assert.lengthOf(projection.subagents, 0);

  const assistantTexts = projection.turnItems.flatMap((item) =>
    item.type === "assistant_message" ? [item.text] : [],
  );
  assert.deepEqual(assistantTexts, ["L2_STARTED"]);

  const shell = result.shellSnapshot.threads.find((thread) => thread.id === projection.thread.id);
  assert.isDefined(shell);
  assert.equal(shell.activeRunId, null);
  assert.equal(shell.status, "completed");
  assert.deepEqual(shell.pendingBackgroundTasks ?? [], []);
}

export function assertClaudeBackgroundTaskWithUserRunOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({
    result,
    transcript,
    runCount: 2,
    runStatuses: ["completed", "completed"],
  });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertUserMessagesInclude(projection, [
    CLAUDE_BACKGROUND_TASK_AFTER_ROOT_PROMPT,
    CLAUDE_BACKGROUND_TASK_USER_FOLLOW_UP_PROMPT,
  ]);

  const before = capturedThread("before-user-run");
  assert.equal(before.activeRunId, null);
  assert.equal(before.status, "completed");
  assertWaiting(before);

  const during = capturedThread("during-user-run");
  assert.equal(during.activeRunId, projection.runs[1]?.id);
  assert.equal(during.status, "running");
  assert.deepEqual(during.pendingBackgroundTasks, []);
  const presentedDuring = presentThreadShell(EnvironmentId.make("replay"), during);
  assert.equal(presentedDuring.runtime?.status, "running");
  assert.isNull(presentPendingBackgroundWork(presentedDuring.pendingBackgroundTasks ?? []));

  const after = capturedThread("after-user-run");
  assert.equal(after.latestRunId, projection.runs[1]?.id);
  assert.equal(after.activeRunId, null);
  assert.equal(after.status, "completed");
  assertWaiting(after);
  assert.lengthOf(projection.providerThreads, 1);
  assert.equal(projection.providerThreads[0]?.status, "idle");
  assert.deepEqual(projection.providerThreads[0]?.pendingBackgroundTasks, []);
  assert.lengthOf(projection.subagents, 0);
  assert.deepEqual(
    projection.turnItems.flatMap((item) => (item.type === "assistant_message" ? [item.text] : [])),
    ["L2_STARTED", "USER_REPLY"],
  );

  const final = result.shellSnapshot.threads.find((thread) => thread.id === projection.thread.id);
  assert.isDefined(final);
  assert.equal(final.activeRunId, null);
  assert.deepEqual(final.pendingBackgroundTasks, []);
  const presentedFinal = presentThreadShell(EnvironmentId.make("replay"), final);
  assert.equal(presentedFinal.runtime?.status, "completed");
  assert.isNull(presentPendingBackgroundWork(presentedFinal.pendingBackgroundTasks ?? []));

  function capturedThread(key: string) {
    const shell = result.capturedShellSnapshots
      .get(key)
      ?.threads.find((thread) => thread.id === projection.thread.id);
    assert.isDefined(shell);
    return shell;
  }

  function assertWaiting(shell: ReturnType<typeof capturedThread>) {
    assert.deepEqual(shell.pendingBackgroundTasks, [
      {
        taskId: BACKGROUND_TASK_ID,
        description: "sleep 25 && echo L2_BG_DONE",
        kind: "command",
        wakesAgent: true,
      },
    ]);
    const presented = presentThreadShell(EnvironmentId.make("replay"), shell);
    assert.equal(presented.runtime?.status, "idle");
    assert.isTrue(presentPendingBackgroundWork(presented.pendingBackgroundTasks ?? [])?.waiting);
  }
}
