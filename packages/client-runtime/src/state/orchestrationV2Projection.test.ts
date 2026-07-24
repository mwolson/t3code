import { describe, expect, it } from "vite-plus/test";
import {
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2TurnItem,
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

import { applyOrchestrationV2ProjectionEvent } from "./orchestrationV2Projection.ts";

const now = DateTime.makeUnsafe("2026-06-20T00:00:00.000Z");
const threadId = ThreadId.make("thread-reducer");
const runId = RunId.make("run-reducer");
const run = {
  id: runId,
  threadId,
  ordinal: 1,
  providerInstanceId: ProviderInstanceId.make("codex"),
  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
  providerThreadId: null,
  userMessageId: MessageId.make("message-reducer"),
  rootNodeId: null,
  activeAttemptId: null,
  status: "completed",
  requestedAt: now,
  startedAt: now,
  completedAt: now,
  checkpointId: null,
  contextHandoffId: null,
} satisfies OrchestrationV2Run;

function commandItem(id: string, output = "done"): OrchestrationV2TurnItem {
  return {
    id: TurnItemId.make(id),
    threadId,
    runId,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 1,
    status: "completed",
    title: null,
    startedAt: now,
    completedAt: now,
    updatedAt: now,
    type: "command_execution",
    input: "pwd",
    output,
    exitCode: 0,
  };
}
const emptyProjection = {
  thread: {
    id: threadId,
    projectId: ProjectId.make("project-reducer"),
    title: "Reducer",
    providerInstanceId: ProviderInstanceId.make("codex"),
    modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { rootThreadId: threadId, parentThreadId: null, relationshipToParent: null },
    forkedFrom: null,
    createdBy: "user",
    creationSource: "web",
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    deletedAt: null,
    settledOverride: null,
    settledAt: null,
  },
  runs: [],
  attempts: [],
  nodes: [],
  subagents: [],
  providerSessions: [],
  providerThreads: [],
  providerTurns: [],
  runtimeRequests: [],
  messages: [],
  plans: [],
  turnItems: [],
  checkpointScopes: [],
  checkpoints: [],
  contextHandoffs: [],
  contextTransfers: [],
  visibleTurnItems: [],
  updatedAt: now,
} as OrchestrationV2ThreadProjection;

describe("applyOrchestrationV2ProjectionEvent", () => {
  it("applies thread lifecycle payloads instead of leaving stale metadata", () => {
    const archivedAt = DateTime.makeUnsafe("2026-06-20T01:00:00.000Z");
    const event = {
      id: "event-archive",
      type: "thread.archived",
      threadId,
      occurredAt: archivedAt,
      payload: { ...emptyProjection.thread, archivedAt, updatedAt: archivedAt },
    } as OrchestrationV2DomainEvent;

    const next = applyOrchestrationV2ProjectionEvent(emptyProjection, event);
    expect(next?.thread.archivedAt).toEqual(archivedAt);
    expect(next?.updatedAt).toEqual(archivedAt);
  });

  it("ignores events for another thread", () => {
    const event = {
      id: "event-other",
      type: "thread.deleted",
      threadId: ThreadId.make("thread-other"),
      occurredAt: now,
      payload: { ...emptyProjection.thread, id: ThreadId.make("thread-other"), deletedAt: now },
    } as OrchestrationV2DomainEvent;

    expect(applyOrchestrationV2ProjectionEvent(emptyProjection, event)).toBe(emptyProjection);
  });

  it("preserves visible row identity when run updates do not change membership", () => {
    const item = commandItem("item-stable");
    const visibleTurnItems = [
      {
        position: 0,
        visibility: "local" as const,
        sourceThreadId: threadId,
        sourceItemId: item.id,
        item,
      },
    ];
    const projection = {
      ...emptyProjection,
      runs: [run],
      turnItems: [item],
      visibleTurnItems,
    };
    const event = {
      id: "event-run-update",
      type: "run.updated",
      threadId,
      runId,
      occurredAt: now,
      payload: { ...run, status: "completed" },
    } as OrchestrationV2DomainEvent;

    const next = applyOrchestrationV2ProjectionEvent(projection, event);
    expect(next?.visibleTurnItems).toBe(visibleTurnItems);
    expect(next?.visibleTurnItems[0]).toBe(visibleTurnItems[0]);
  });

  it("replaces only the updated visible item when membership is unchanged", () => {
    const first = commandItem("item-first", "first");
    const second = commandItem("item-second", "second");
    const firstRow = {
      position: 0,
      visibility: "local" as const,
      sourceThreadId: threadId,
      sourceItemId: first.id,
      item: first,
    };
    const secondRow = {
      position: 1,
      visibility: "local" as const,
      sourceThreadId: threadId,
      sourceItemId: second.id,
      item: second,
    };
    const updated = commandItem("item-first", "streamed output");
    const projection = {
      ...emptyProjection,
      runs: [run],
      turnItems: [first, second],
      visibleTurnItems: [firstRow, secondRow],
    };
    const event = {
      id: "event-item-update",
      type: "turn-item.updated",
      threadId,
      runId,
      occurredAt: now,
      payload: updated,
    } as OrchestrationV2DomainEvent;

    const next = applyOrchestrationV2ProjectionEvent(projection, event);
    expect(next?.visibleTurnItems).not.toBe(projection.visibleTurnItems);
    expect(next?.visibleTurnItems[0]).not.toBe(firstRow);
    expect(next?.visibleTurnItems[0]?.item).toBe(updated);
    expect(next?.visibleTurnItems[1]).toBe(secondRow);
  });

  it("removes only hidden local items while preserving inherited rows", () => {
    const inherited = commandItem("item-inherited");
    const local = commandItem("item-local");
    const inheritedRow = {
      position: 0,
      visibility: "inherited" as const,
      sourceThreadId: ThreadId.make("thread-source"),
      sourceItemId: inherited.id,
      item: inherited,
    };
    const localRow = {
      position: 1,
      visibility: "local" as const,
      sourceThreadId: threadId,
      sourceItemId: local.id,
      item: local,
    };
    const projection = {
      ...emptyProjection,
      runs: [run],
      turnItems: [local],
      visibleTurnItems: [inheritedRow, localRow],
    };
    const event = {
      id: "event-run-rollback",
      type: "run.updated",
      threadId,
      runId,
      occurredAt: now,
      payload: { ...run, status: "rolled_back" },
    } as OrchestrationV2DomainEvent;

    const next = applyOrchestrationV2ProjectionEvent(projection, event);
    expect(next?.visibleTurnItems).toEqual([inheritedRow]);
    expect(next?.visibleTurnItems[0]).toBe(inheritedRow);
  });

  it("keeps prepended older history rows across live run and turn-item updates", () => {
    const olderRunId = RunId.make("run-older");
    const olderRun = { ...run, id: olderRunId, ordinal: 0 };
    const older = commandItem("item-older");
    const olderWithRun = { ...older, runId: olderRunId, ordinal: 0 };
    const recent = commandItem("item-recent", "recent");
    const olderRow = {
      position: 0,
      visibility: "local" as const,
      sourceThreadId: threadId,
      sourceItemId: olderWithRun.id,
      item: olderWithRun,
    };
    const recentRow = {
      position: 1,
      visibility: "local" as const,
      sourceThreadId: threadId,
      sourceItemId: recent.id,
      item: recent,
    };
    const projection = {
      ...emptyProjection,
      runs: [olderRun, run],
      turnItems: [olderWithRun, recent],
      visibleTurnItems: [olderRow, recentRow],
    };

    const runUpdated = applyOrchestrationV2ProjectionEvent(projection, {
      id: "event-run-live",
      type: "run.updated",
      threadId,
      runId,
      occurredAt: now,
      payload: { ...run, status: "running", completedAt: null },
    } as OrchestrationV2DomainEvent);
    expect(runUpdated?.visibleTurnItems.map((row) => row.sourceItemId)).toEqual([
      olderWithRun.id,
      recent.id,
    ]);
    expect(runUpdated?.visibleTurnItems[0]).toBe(olderRow);

    const streamed = commandItem("item-recent", "streamed");
    const itemUpdated = applyOrchestrationV2ProjectionEvent(runUpdated!, {
      id: "event-item-live",
      type: "turn-item.updated",
      threadId,
      runId,
      occurredAt: now,
      payload: streamed,
    } as OrchestrationV2DomainEvent);
    expect(itemUpdated?.visibleTurnItems.map((row) => row.sourceItemId)).toEqual([
      olderWithRun.id,
      recent.id,
    ]);
    expect(itemUpdated?.visibleTurnItems[0]?.item).toBe(olderWithRun);
    expect(itemUpdated?.visibleTurnItems[1]?.item).toBe(streamed);

    const attemptUpdated = applyOrchestrationV2ProjectionEvent(itemUpdated!, {
      id: "event-attempt-live",
      type: "run-attempt.updated",
      threadId,
      runId,
      occurredAt: now,
      payload: {
        id: "attempt-1",
        runId,
        attemptOrdinal: 1,
        rootNodeId: "node-root-1",
        providerInstanceId: ProviderInstanceId.make("codex"),
        providerThreadId: "provider-thread-1",
        providerTurnId: null,
        reason: "initial",
        status: "running",
        startedAt: now,
        completedAt: null,
      },
    } as OrchestrationV2DomainEvent);
    expect(attemptUpdated?.visibleTurnItems.map((row) => row.sourceItemId)).toEqual([
      olderWithRun.id,
      recent.id,
    ]);
    expect(attemptUpdated?.visibleTurnItems[0]?.sourceItemId).toBe(olderWithRun.id);
  });

  it("partialTimeline drops a missing older turn item instead of appending newest", () => {
    const recentLow = commandItem("item-window-low");
    const recentHigh = commandItem("item-window-high", "high");
    // Bounded window only retained ordinals 10+.
    const low = { ...recentLow, ordinal: 10 };
    const high = { ...recentHigh, ordinal: 11 };
    const lowRow = {
      position: 0,
      visibility: "local" as const,
      sourceThreadId: threadId,
      sourceItemId: low.id,
      item: low,
    };
    const highRow = {
      position: 1,
      visibility: "local" as const,
      sourceThreadId: threadId,
      sourceItemId: high.id,
      item: high,
    };
    const projection = {
      ...emptyProjection,
      runs: [run],
      turnItems: [low, high],
      visibleTurnItems: [lowRow, highRow],
    };
    const older = {
      ...commandItem("item-older-outside"),
      ordinal: 3,
      output: "should-not-append",
    };
    const next = applyOrchestrationV2ProjectionEvent(
      projection,
      {
        id: "event-old-item",
        type: "turn-item.updated",
        threadId,
        runId,
        occurredAt: now,
        payload: older,
      } as OrchestrationV2DomainEvent,
      { partialTimeline: true },
    );

    expect(next).toBe(projection);
    expect(next?.visibleTurnItems.map((row) => row.sourceItemId)).toEqual([low.id, high.id]);
    expect(next?.turnItems.map((item) => item.id)).toEqual([low.id, high.id]);
  });

  it("partialTimeline watermark drops missing old items when no local rows remain", () => {
    // Inherited-only visible window: oldestLocalTurnOrdinal is null without a watermark.
    const inherited = {
      ...commandItem("item-inherited"),
      ordinal: 1,
      threadId: ThreadId.make("parent-thread"),
    };
    const inheritedRow = {
      position: 0,
      visibility: "inherited" as const,
      sourceThreadId: ThreadId.make("parent-thread"),
      sourceItemId: inherited.id,
      item: inherited,
    };
    const projection = {
      ...emptyProjection,
      runs: [run],
      turnItems: [],
      visibleTurnItems: [inheritedRow],
    };
    const olderLocal = {
      ...commandItem("item-old-local"),
      ordinal: 7,
      output: "must-not-append",
    };
    const dropped = applyOrchestrationV2ProjectionEvent(
      projection,
      {
        id: "event-old-local-watermark",
        type: "turn-item.updated",
        threadId,
        runId,
        occurredAt: now,
        payload: olderLocal,
      } as OrchestrationV2DomainEvent,
      { partialTimeline: true, latestLocalTurnOrdinal: 20 },
    );
    expect(dropped).toBe(projection);

    const newerLocal = {
      ...commandItem("item-new-local"),
      ordinal: 21,
      output: "append-ok",
    };
    const appended = applyOrchestrationV2ProjectionEvent(
      projection,
      {
        id: "event-new-local-watermark",
        type: "turn-item.updated",
        threadId,
        runId,
        occurredAt: now,
        payload: newerLocal,
      } as OrchestrationV2DomainEvent,
      { partialTimeline: true, latestLocalTurnOrdinal: 20 },
    );
    expect(appended).not.toBe(projection);
    expect(appended?.visibleTurnItems.map((row) => row.sourceItemId)).toEqual([
      inherited.id,
      newerLocal.id,
    ]);
  });

  it("default full/web reducer still appends missing older turn items", () => {
    const recent = { ...commandItem("item-window"), ordinal: 10 };
    const recentRow = {
      position: 0,
      visibility: "local" as const,
      sourceThreadId: threadId,
      sourceItemId: recent.id,
      item: recent,
    };
    const projection = {
      ...emptyProjection,
      runs: [run],
      turnItems: [recent],
      visibleTurnItems: [recentRow],
    };
    const older = {
      ...commandItem("item-older-full"),
      ordinal: 3,
      output: "full-appends",
    };
    // No options: original append-on-miss behavior for full snapshots.
    const next = applyOrchestrationV2ProjectionEvent(projection, {
      id: "event-old-item-full",
      type: "turn-item.updated",
      threadId,
      runId,
      occurredAt: now,
      payload: older,
    } as OrchestrationV2DomainEvent);

    expect(next?.visibleTurnItems.map((row) => row.sourceItemId)).toEqual([recent.id, older.id]);
    expect(next?.turnItems.map((item) => item.id)).toEqual([recent.id, older.id]);
  });

  it("partialTimeline updates a retained out-of-window turn item without appending it", () => {
    const recent = { ...commandItem("item-window"), ordinal: 10 };
    const retained = {
      ...commandItem("item-retained-dep"),
      ordinal: 2,
      output: "stale",
    };
    const recentRow = {
      position: 0,
      visibility: "local" as const,
      sourceThreadId: threadId,
      sourceItemId: recent.id,
      item: recent,
    };
    const projection = {
      ...emptyProjection,
      runs: [run],
      turnItems: [retained, recent],
      visibleTurnItems: [recentRow],
    };
    const refreshed = { ...retained, output: "refreshed" };
    const next = applyOrchestrationV2ProjectionEvent(
      projection,
      {
        id: "event-retained-refresh",
        type: "turn-item.updated",
        threadId,
        runId,
        occurredAt: now,
        payload: refreshed,
      } as OrchestrationV2DomainEvent,
      { partialTimeline: true },
    );

    expect(next?.visibleTurnItems.map((row) => row.sourceItemId)).toEqual([recent.id]);
    expect(next?.turnItems.find((item) => item.id === retained.id)).toEqual(refreshed);
  });

  it("keeps a visible interrupt result when its request is only in turnItems outside the window", () => {
    const nodeId = NodeId.make("node-interrupt");
    const request = {
      id: TurnItemId.make("item-interrupt-request"),
      threadId,
      runId,
      nodeId,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: 0,
      status: "completed" as const,
      title: null,
      startedAt: now,
      completedAt: now,
      updatedAt: now,
      type: "run_interrupt_request" as const,
      message: "Stopping",
    } satisfies OrchestrationV2TurnItem;
    const result = {
      id: TurnItemId.make("item-interrupt-result"),
      threadId,
      runId,
      nodeId,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: 1,
      status: "completed" as const,
      title: null,
      startedAt: now,
      completedAt: now,
      updatedAt: now,
      type: "run_interrupt_result" as const,
      message: "Stopped",
    } satisfies OrchestrationV2TurnItem;
    const resultRow = {
      position: 0,
      visibility: "local" as const,
      sourceThreadId: threadId,
      sourceItemId: result.id,
      item: result,
    };
    const attemptId = RunAttemptId.make("attempt-superseded");
    // Bounded snapshot: request is retained in turnItems but not visibleTurnItems.
    const projection = {
      ...emptyProjection,
      runs: [{ ...run, status: "running" as const, completedAt: null }],
      attempts: [
        {
          id: attemptId,
          runId,
          attemptOrdinal: 1,
          rootNodeId: nodeId,
          providerInstanceId: ProviderInstanceId.make("codex"),
          providerThreadId: "provider-thread-1" as never,
          providerTurnId: null,
          reason: "initial" as const,
          status: "running" as const,
          startedAt: now,
          completedAt: null,
        },
      ],
      turnItems: [request, result],
      visibleTurnItems: [resultRow],
    };

    const attemptSuperseded = applyOrchestrationV2ProjectionEvent(projection, {
      id: "event-attempt-superseded",
      type: "run-attempt.updated",
      threadId,
      runId,
      occurredAt: now,
      payload: {
        id: attemptId,
        runId,
        attemptOrdinal: 1,
        rootNodeId: nodeId,
        providerInstanceId: ProviderInstanceId.make("codex"),
        providerThreadId: "provider-thread-1",
        providerTurnId: null,
        reason: "initial",
        status: "superseded",
        startedAt: now,
        completedAt: now,
      },
    } as OrchestrationV2DomainEvent);

    expect(attemptSuperseded?.visibleTurnItems.map((row) => row.sourceItemId)).toEqual([result.id]);
    expect(attemptSuperseded?.turnItems.map((item) => item.id)).toEqual([request.id, result.id]);

    const runUpdated = applyOrchestrationV2ProjectionEvent(attemptSuperseded!, {
      id: "event-run-live-after-interrupt",
      type: "run.updated",
      threadId,
      runId,
      occurredAt: now,
      payload: { ...run, status: "running", completedAt: null },
    } as OrchestrationV2DomainEvent);
    expect(runUpdated?.visibleTurnItems.map((row) => row.sourceItemId)).toEqual([result.id]);

    // Without the retained request, the same superseded attempt would hide the result.
    const missingRequest = {
      ...projection,
      turnItems: [result],
    };
    const hidden = applyOrchestrationV2ProjectionEvent(missingRequest, {
      id: "event-attempt-superseded-missing-request",
      type: "run-attempt.updated",
      threadId,
      runId,
      occurredAt: now,
      payload: {
        id: attemptId,
        runId,
        attemptOrdinal: 1,
        rootNodeId: nodeId,
        providerInstanceId: ProviderInstanceId.make("codex"),
        providerThreadId: "provider-thread-1",
        providerTurnId: null,
        reason: "initial",
        status: "superseded",
        startedAt: now,
        completedAt: now,
      },
    } as OrchestrationV2DomainEvent);
    expect(hidden?.visibleTurnItems).toEqual([]);
  });

  it("merges only settlement fields so concurrent metadata survives unsettle payloads", () => {
    const settledAt = DateTime.makeUnsafe("2026-06-20T00:30:00.000Z");
    const live = {
      ...emptyProjection,
      thread: {
        ...emptyProjection.thread,
        title: "Live renamed title",
        archivedAt: DateTime.makeUnsafe("2026-06-20T00:45:00.000Z"),
        settledOverride: "settled" as const,
        settledAt,
        updatedAt: settledAt,
      },
    };
    const stalePayload = {
      ...emptyProjection.thread,
      title: "Stale pre-rename title",
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      updatedAt: DateTime.makeUnsafe("2026-06-20T00:40:00.000Z"),
    };
    const event = {
      id: "event-activity-unsettle",
      type: "thread.unsettled",
      threadId,
      occurredAt: DateTime.makeUnsafe("2026-06-20T00:40:00.000Z"),
      payload: stalePayload,
    } as OrchestrationV2DomainEvent;

    // Activity at 00:40 is after settledAt 00:30, so pin clears, but live
    // non-settlement fields must not be restored from the stale payload.
    const next = applyOrchestrationV2ProjectionEvent(live, event);
    expect(next?.thread.settledOverride).toBeNull();
    expect(next?.thread.settledAt).toBeNull();
    expect(next?.thread.title).toBe("Live renamed title");
    expect(next?.thread.archivedAt).toEqual(live.thread.archivedAt);
  });

  it("does not clear a newer settled or active pin from delayed provider activity", () => {
    const pinAt = DateTime.makeUnsafe("2026-06-20T02:00:00.000Z");
    const delayedActivityAt = DateTime.makeUnsafe("2026-06-20T01:00:00.000Z");

    for (const override of ["settled", "active"] as const) {
      const projection = {
        ...emptyProjection,
        thread: {
          ...emptyProjection.thread,
          title: "Pinned",
          settledOverride: override,
          settledAt: override === "settled" ? pinAt : null,
          updatedAt: pinAt,
        },
      };
      const event = {
        id: `event-delayed-${override}`,
        type: "thread.unsettled",
        threadId,
        occurredAt: delayedActivityAt,
        payload: {
          ...projection.thread,
          title: "Stale",
          settledOverride: null,
          settledAt: null,
          updatedAt: delayedActivityAt,
        },
      } as OrchestrationV2DomainEvent;

      const next = applyOrchestrationV2ProjectionEvent(projection, event);
      expect(next).toBe(projection);
      expect(next?.thread.settledOverride).toBe(override);
      expect(next?.thread.title).toBe("Pinned");
    }
  });
});
