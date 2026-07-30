import {
  MessageId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2ProjectedTurnItem,
  type OrchestrationV2RunStatus,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import { formatPendingBackgroundWorkLabel } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";

import {
  threadRuntimeIsActive,
  type ThreadRuntimeSummary,
} from "@t3tools/client-runtime/state/models";
import { threadRuntimeHasInterruptibleRun } from "@t3tools/client-runtime/state/thread-execution";

import {
  buildThreadFeed,
  deriveThreadFeedPresentation,
  deriveThreadPendingBackgroundWork,
  deriveThreadWorkingStartedAt,
  resolvePresentedThreadExecution,
  threadFeedRunIsUnsettled,
  type ThreadFeedActivity,
  type ThreadFeedEntry,
} from "./threadActivity";

const threadId = ThreadId.make("thread-1");
const sourceThreadId = ThreadId.make("thread-source");
const runId = RunId.make("run-1");

function base(id: string, updatedAt: string, ordinal: number) {
  const timestamp = DateTime.makeUnsafe(updatedAt);
  return {
    id: TurnItemId.make(id),
    threadId,
    runId,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal,
    status: "completed" as const,
    title: null,
    startedAt: timestamp,
    completedAt: timestamp,
    updatedAt: timestamp,
  };
}

function projected(
  item: OrchestrationV2TurnItem,
  position: number,
  visibility: OrchestrationV2ProjectedTurnItem["visibility"] = "local",
): OrchestrationV2ProjectedTurnItem {
  return {
    position,
    visibility,
    sourceThreadId: visibility === "local" ? threadId : sourceThreadId,
    sourceItemId: item.id,
    item,
  };
}

function userMessage(updatedAt = "2026-06-20T00:00:01.000Z") {
  return {
    ...base("item-user", updatedAt, 0),
    type: "user_message" as const,
    messageId: MessageId.make("message-user"),
    createdBy: "user" as const,
    creationSource: "mobile" as const,
    inputIntent: "turn_start" as const,
    text: "Run checks",
    attachments: [],
  };
}

function command(updatedAt = "2026-06-20T00:00:02.000Z") {
  return {
    ...base("item-command", updatedAt, 1),
    type: "command_execution" as const,
    input: "vp check",
    output: "ok",
    exitCode: 0,
  };
}

function assistantMessage(updatedAt = "2026-06-20T00:00:03.000Z") {
  return {
    ...base("item-assistant", updatedAt, 2),
    type: "assistant_message" as const,
    messageId: MessageId.make("message-assistant"),
    text: "Done",
    streaming: false,
  };
}

describe("buildThreadFeed", () => {
  it("does not treat a queued-only run as live feed activity", () => {
    expect(
      threadFeedRunIsUnsettled({
        runId,
        status: "queued",
        startedAt: null,
        completedAt: null,
      }),
    ).toBe(false);
    expect(
      threadFeedRunIsUnsettled({
        runId,
        status: "running",
        startedAt: "2026-06-20T00:00:01.000Z",
        completedAt: null,
      }),
    ).toBe(true);
  });

  it("adds queued input only after dispatch creates its turn item", () => {
    const dispatchedRunId = RunId.make("run-dispatched-queued");
    const dispatchedMessageId = MessageId.make("message-dispatched-queued");
    expect(buildThreadFeed([])).toEqual([]);

    const promotedEntries = buildThreadFeed([
      projected(
        {
          ...userMessage(),
          id: TurnItemId.make("item-dispatched-queued"),
          runId: dispatchedRunId,
          messageId: dispatchedMessageId,
          inputIntent: "turn_start",
        },
        0,
      ),
    ]);
    expect(promotedEntries.map((entry) => entry.id)).toEqual([dispatchedMessageId]);
    expect(
      promotedEntries[0]?.type === "message" ? promotedEntries[0].message.inputIntent : undefined,
    ).toBe("turn_start");
  });

  it("preserves authoritative V2 order instead of sorting reconstructed collections", () => {
    const rows = [
      projected(userMessage("2026-06-20T00:00:03.000Z"), 0),
      projected(command("2026-06-20T00:00:01.000Z"), 1),
      projected(assistantMessage("2026-06-20T00:00:02.000Z"), 2),
    ];

    const feed = buildThreadFeed(rows);
    expect(feed.map((entry) => entry.type)).toEqual(["message", "activity-group", "message"]);
    expect(feed.map((entry) => entry.id)).toEqual([
      "message-user",
      "local:thread-1:item-command",
      "message-assistant",
    ]);
    const activity = feed.find((entry) => entry.type === "activity-group")?.activities[0];
    expect(activity?.projectedItem).toBe(rows[1]);
    expect(activity?.getFullDetail()).toContain('"input": "vp check"');
  });

  it("retains inherited and synthetic rows with their original projected identity", () => {
    const inherited = projected(command(), 0, "inherited");
    const { providerThreadId: _providerThreadId, ...forkBase } = base(
      "item-fork",
      "2026-06-20T00:00:03.000Z",
      2,
    );
    const synthetic = projected(
      {
        ...forkBase,
        type: "fork",
        source: { type: "run", threadId: sourceThreadId, runId },
        targetThreadId: threadId,
      },
      1,
      "synthetic",
    );

    const feed = buildThreadFeed([inherited, synthetic]);
    const activities = feed.flatMap((entry) =>
      entry.type === "activity-group" ? entry.activities : [],
    );
    expect(activities.map((activity) => activity.projectedItem)).toEqual([inherited, synthetic]);
    expect(activities.map((activity) => activity.projectedItem.visibility)).toEqual([
      "inherited",
      "synthetic",
    ]);
    expect(activities.at(-1)?.prominent).toBe(true);
  });

  it("keeps orchestration relationship cards visible when a completed run is folded", () => {
    const { providerThreadId: _providerThreadId, ...forkBase } = base(
      "item-fork",
      "2026-06-20T00:00:02.500Z",
      2,
    );
    const feed = buildThreadFeed([
      projected(userMessage(), 0),
      projected(command(), 1),
      projected(
        {
          ...forkBase,
          type: "fork",
          source: { type: "run", threadId, runId },
          targetThreadId: sourceThreadId,
        },
        2,
      ),
      projected(assistantMessage(), 3),
    ]);

    const collapsed = deriveThreadFeedPresentation(
      feed,
      {
        runId,
        status: "completed",
        startedAt: "2026-06-20T00:00:01.000Z",
        completedAt: "2026-06-20T00:00:03.000Z",
      },
      new Set(),
    );

    expect(
      collapsed.some(
        (entry) =>
          entry.type === "activity-group" &&
          entry.activities.some((activity) => activity.projectedItem.item.type === "fork"),
      ),
    ).toBe(true);
    expect(
      collapsed.some(
        (entry) =>
          entry.type === "activity-group" &&
          entry.activities.some(
            (activity) => activity.projectedItem.item.type === "command_execution",
          ),
      ),
    ).toBe(false);
  });

  it("folds settled V2 run work while keeping the terminal assistant message visible", () => {
    const feed = buildThreadFeed([
      projected(userMessage(), 0),
      projected(command(), 1),
      projected(assistantMessage(), 2),
    ]);
    const latestRun = {
      runId,
      status: "completed" as const,
      startedAt: "2026-06-20T00:00:01.000Z",
      completedAt: "2026-06-20T00:00:03.000Z",
    };

    const collapsed = deriveThreadFeedPresentation(feed, latestRun, new Set());
    expect(collapsed.map((entry) => entry.type)).toEqual(["message", "run-fold", "message"]);

    const expanded = deriveThreadFeedPresentation(feed, latestRun, new Set([runId]));
    expect(expanded.map((entry) => entry.type)).toEqual([
      "message",
      "run-fold",
      "activity-group",
      "message",
    ]);
  });

  it("keeps an active run expanded and marks failed tools as failures", () => {
    const failedCommand: OrchestrationV2TurnItem = {
      ...command(),
      status: "failed",
      completedAt: DateTime.makeUnsafe("2026-06-20T00:00:02.000Z"),
    };
    const feed = buildThreadFeed([projected(userMessage(), 0), projected(failedCommand, 1)]);
    const presented = deriveThreadFeedPresentation(
      feed,
      {
        runId,
        status: "running",
        startedAt: "2026-06-20T00:00:01.000Z",
        completedAt: null,
      },
      new Set(),
    );

    expect(presented.some((entry) => entry.type === "run-fold")).toBe(false);
    expect(presented.find((entry) => entry.type === "activity-group")?.activities[0]?.status).toBe(
      "failure",
    );
  });

  it("appends active work as a normal timeline row", () => {
    const startedAt = "2026-04-01T00:00:01.000Z";
    const presented = deriveThreadFeedPresentation([], null, new Set(), new Set(), startedAt);

    expect(presented).toEqual([
      {
        type: "working",
        id: "working-indicator-row",
        createdAt: startedAt,
      },
    ]);
    expect(deriveThreadFeedPresentation(presented, null, new Set())).toEqual([]);
  });

  it("appends waiting background work when no active work remains", () => {
    const pendingBackgroundTasks = [{ taskId: "background-1", description: "Run checks" }];
    const presented = deriveThreadFeedPresentation(
      [],
      null,
      new Set(),
      new Set(),
      null,
      pendingBackgroundTasks,
    );

    expect(presented).toEqual([
      {
        type: "waiting-background",
        id: "waiting-background-row",
        createdAt: null,
        label: formatPendingBackgroundWorkLabel(pendingBackgroundTasks),
      },
    ]);
  });

  it("labels a described task, an anonymous task, and a multi-task roster", () => {
    const label = (tasks: ReadonlyArray<{ taskId: string; description?: string }>) =>
      deriveThreadFeedPresentation([], null, new Set(), new Set(), null, tasks).find(
        (entry) => entry.type === "waiting-background",
      )?.label;

    expect(label([{ taskId: "background-1", description: "Run checks" }])).toBe(
      "Waiting on background task: Run checks",
    );
    expect(label([{ taskId: "background-1" }])).toBe("Waiting on a background task");
    expect(
      label([
        { taskId: "background-1", description: "Run checks" },
        { taskId: "background-2", description: "Run tests" },
      ]),
    ).toBe("Waiting on 2 background tasks: Run checks, …");
  });

  it("drops the waiting row once the roster drains", () => {
    const presented = deriveThreadFeedPresentation([], null, new Set(), new Set(), null, [
      { taskId: "background-1", description: "Run checks" },
    ]);
    expect(presented.map((entry) => entry.type)).toEqual(["waiting-background"]);

    expect(deriveThreadFeedPresentation(presented, null, new Set(), new Set(), null, [])).toEqual(
      [],
    );
  });

  it("appends the waiting row after real conversation content", () => {
    const feed = buildThreadFeed([projected(userMessage(), 0), projected(command(), 1)]);
    const presented = deriveThreadFeedPresentation(
      feed,
      { runId, status: "completed", startedAt: null, completedAt: null },
      new Set([runId]),
      new Set(),
      null,
      [{ taskId: "background-1", description: "Run checks" }],
    );

    expect(presented.at(-1)?.type).toBe("waiting-background");
    expect(presented.filter((entry) => entry.type === "waiting-background")).toHaveLength(1);
  });

  it("prefers active work over waiting background work", () => {
    const presented = deriveThreadFeedPresentation(
      [],
      null,
      new Set(),
      new Set(),
      "2026-04-01T00:00:01.000Z",
      [{ taskId: "background-1", description: "Run checks" }],
    );

    expect(presented.map((entry) => entry.type)).toEqual(["working"]);
  });

  it("does not append waiting background work for an empty roster", () => {
    expect(deriveThreadFeedPresentation([], null, new Set())).toEqual([]);
  });

  it("keeps waiting background work idempotent under re-derivation", () => {
    const pendingBackgroundTasks = [{ taskId: "background-1", description: "Run checks" }];
    const presented = deriveThreadFeedPresentation(
      [],
      null,
      new Set(),
      new Set(),
      null,
      pendingBackgroundTasks,
    );

    expect(
      deriveThreadFeedPresentation(
        presented,
        null,
        new Set(),
        new Set(),
        null,
        pendingBackgroundTasks,
      ),
    ).toEqual(presented);
  });

  it("models work-log overflow as list rows", () => {
    const activity = (
      id: string,
      createdAt: string,
      status: ThreadFeedActivity["status"] = "success",
    ): ThreadFeedActivity => ({
      id,
      createdAt,
      runId: null,
      summary: `Tool ${id}`,
      detail: null,
      canExpand: false,
      getFullDetail: () => null,
      getCopyText: () => id,
      icon: "command",
      logo: null,
      toolLike: true,
      prominent: false,
      status,
      projectedItem: projected(command(createdAt), 0),
    });
    const feed: ThreadFeedEntry[] = [
      {
        type: "activity-group",
        id: "work-group-1",
        createdAt: "2026-04-01T00:00:01.000Z",
        runId: null,
        activities: [
          activity("activity-1", "2026-04-01T00:00:01.000Z"),
          activity("activity-neutral", "2026-04-01T00:00:02.000Z", "neutral"),
          activity("activity-2", "2026-04-01T00:00:03.000Z"),
          activity("activity-3", "2026-04-01T00:00:04.000Z"),
        ],
      },
    ];

    const collapsed = deriveThreadFeedPresentation(feed, null, new Set());
    expect(collapsed.map((entry) => entry.id)).toEqual(["activity-3", "work-toggle:work-group-1"]);
    expect(collapsed[1]).toMatchObject({
      type: "work-toggle",
      groupId: "work-group-1",
      hiddenCount: 2,
      expanded: false,
    });

    const expanded = deriveThreadFeedPresentation(feed, null, new Set(), new Set(["work-group-1"]));
    expect(expanded.map((entry) => entry.id)).toEqual([
      "activity-1",
      "activity-2",
      "activity-3",
      "work-toggle:work-group-1",
    ]);
    expect(expanded.at(-1)).toMatchObject({
      type: "work-toggle",
      expanded: true,
    });
  });

  it("pretty prints T3 MCP dynamic tool activities and attaches the product logo", () => {
    const toolItem: OrchestrationV2TurnItem = {
      ...base("item-t3-tool", "2026-06-20T00:00:04.000Z", 3),
      type: "dynamic_tool",
      toolName: "mcp__t3-code__t3_thread_read",
      input: { threadId: "thread-child" },
      output: { messages: [] },
    };

    const feed = buildThreadFeed([projected(toolItem, 0)]);
    const activity = feed[0]?.type === "activity-group" ? feed[0].activities[0] : null;

    expect(activity?.summary).toBe("Read a T3 thread");
    expect(activity?.logo).toBe("t3-code");
    expect(activity?.getCopyText().split("\n")[0]).toBe("Read a T3 thread");
  });
});

describe("deriveThreadPendingBackgroundWork", () => {
  const backgroundRunId = RunId.make("run-background");
  const rolledBackRunId = RunId.make("run-rolled-back");

  function subagentItem(id: string, itemRunId: RunId) {
    return {
      id: TurnItemId.make(id),
      type: "subagent" as const,
      status: "running" as const,
      title: "Investigate the failure",
      runId: itemRunId,
    };
  }

  function projection(options: {
    readonly runs: ReadonlyArray<{
      readonly id: RunId;
      readonly ordinal: number;
      readonly status: OrchestrationV2RunStatus;
    }>;
    readonly turnItems: ReadonlyArray<ReturnType<typeof subagentItem>>;
  }) {
    return {
      providerThreads: [],
      runs: options.runs,
      turnItems: options.turnItems,
      thread: { activeProviderThreadId: null },
    };
  }

  it("surfaces a nonterminal background item once its run has settled", () => {
    const tasks = deriveThreadPendingBackgroundWork(
      projection({
        runs: [{ id: backgroundRunId, ordinal: 1, status: "completed" }],
        turnItems: [subagentItem("item-background", backgroundRunId)],
      }),
      "live",
    );

    expect(tasks).toHaveLength(1);
    expect(tasks[0]).toMatchObject({ taskId: "item-background", taskType: "subagent" });
  });

  it("abandons background items owned by a rolled back run", () => {
    const tasks = deriveThreadPendingBackgroundWork(
      projection({
        runs: [
          { id: rolledBackRunId, ordinal: 1, status: "rolled_back" },
          { id: backgroundRunId, ordinal: 2, status: "completed" },
        ],
        turnItems: [subagentItem("item-rolled-back", rolledBackRunId)],
      }),
      "live",
    );

    expect(tasks).toEqual([]);
  });

  it("stays quiet while the latest run is still active", () => {
    const tasks = deriveThreadPendingBackgroundWork(
      projection({
        runs: [{ id: backgroundRunId, ordinal: 1, status: "running" }],
        turnItems: [subagentItem("item-background", backgroundRunId)],
      }),
      "live",
    );

    expect(tasks).toEqual([]);
  });

  it("uses the shell roster instead of a cached projection", () => {
    const cached = projection({
      runs: [{ id: backgroundRunId, ordinal: 1, status: "completed" }],
      turnItems: [subagentItem("item-background", backgroundRunId)],
    });
    const shellTasks = [
      {
        taskId: "shell-task",
        description: "Fresh shell work",
        taskType: "command_execution",
      },
    ] as const;

    expect(deriveThreadPendingBackgroundWork(cached, "cached", shellTasks)).toEqual(shellTasks);
    expect(deriveThreadPendingBackgroundWork(undefined, "live")).toEqual([]);
  });
});

describe("deriveThreadWorkingStartedAt", () => {
  const startedAt = "2026-04-01T00:00:01.000Z";
  const waitingRun = { runId: String(runId), startedAt, completedAt: null };

  function runtime(status: ThreadRuntimeSummary["status"]): ThreadRuntimeSummary {
    return {
      status,
      activeRunId: null,
      providerInstanceId: ProviderInstanceId.make("claudeAgent"),
      providerName: null,
      lastError: null,
      updatedAt: startedAt,
    };
  }

  it("reports working while a turn is actually running", () => {
    expect(deriveThreadWorkingStartedAt(waitingRun, runtime("running"), null)).toBe(startedAt);
  });

  // The regression this pins: a successful run persists as `waiting` with a
  // null completedAt until checkpoint capture lands, so the raw timing helper
  // keeps reporting a start time. The roster parks the runtime at idle, and
  // without honouring that park the feed shows Working for the whole capture
  // window, hiding Waiting exactly when it should first appear.
  it("stays quiet once the roster has parked the runtime at idle", () => {
    const parkedRuntime = {
      ...runtime("idle"),
      activeRunId: RunId.make("run-stale"),
    };

    expect(deriveThreadWorkingStartedAt(waitingRun, parkedRuntime, null)).toBeNull();
    expect(threadRuntimeHasInterruptibleRun(parkedRuntime)).toBe(false);
  });

  it("still reports working for a waiting run with no roster behind it", () => {
    expect(deriveThreadWorkingStartedAt(waitingRun, runtime("waiting"), null)).toBe(startedAt);
  });

  it("uses one fresh shell execution when the terminal detail is stale", () => {
    const detailRun = {
      runId: RunId.make("run-stale-detail"),
      status: "completed" as const,
      requestedAt: "2026-04-01T00:00:00.000Z",
      startedAt: "2026-04-01T00:00:01.000Z",
      completedAt: "2026-04-01T00:00:02.000Z",
      assistantMessageId: null,
    };
    const shellRun = {
      runId: RunId.make("run-fresh-shell"),
      status: "running" as const,
      requestedAt: "2026-04-01T00:01:00.000Z",
      startedAt: "2026-04-01T00:01:01.000Z",
      completedAt: null,
      assistantMessageId: null,
    };
    const shellRuntime = {
      ...runtime("running"),
      activeRunId: shellRun.runId,
      updatedAt: "2026-04-01T00:01:01.000Z",
    };
    const presented = resolvePresentedThreadExecution({
      detailRun,
      detailRuntime: runtime("idle"),
      detailStatus: "cached",
      shellRun,
      shellRuntime,
    });

    expect(presented).toEqual({ run: shellRun, runtime: shellRuntime });
    expect(deriveThreadWorkingStartedAt(presented.run, presented.runtime, null)).toBe(
      shellRun.startedAt,
    );
    expect(threadRuntimeIsActive(presented.runtime)).toBe(true);
    expect(presented.runtime?.activeRunId).toBe(shellRun.runId);
  });

  it("keeps one live detail execution pair", () => {
    const detailRuntime = runtime("running");
    const detailRun = {
      runId: RunId.make("run-live-detail"),
      status: "running" as const,
      requestedAt: "2026-04-01T00:02:00.000Z",
      startedAt: "2026-04-01T00:02:01.000Z",
      completedAt: null,
      assistantMessageId: null,
    };
    const presented = resolvePresentedThreadExecution({
      detailRun,
      detailRuntime,
      detailStatus: "live",
      shellRun: null,
      shellRuntime: runtime("idle"),
    });

    expect(presented).toEqual({ run: detailRun, runtime: detailRuntime });
  });

  it("keeps fresher detail controls while synchronization is finishing", () => {
    const detailRun = {
      runId: RunId.make("run-fresh-detail"),
      status: "running" as const,
      requestedAt: "2026-04-01T00:02:00.000Z",
      startedAt: "2026-04-01T00:02:01.000Z",
      completedAt: null,
      assistantMessageId: null,
    };
    const detailRuntime = {
      ...runtime("running"),
      activeRunId: detailRun.runId,
      updatedAt: "2026-04-01T00:02:01.000Z",
    };
    const shellRuntime = {
      ...runtime("idle"),
      updatedAt: "2026-04-01T00:01:01.000Z",
    };
    const presented = resolvePresentedThreadExecution({
      detailRun,
      detailRuntime,
      detailStatus: "synchronizing",
      shellRun: null,
      shellRuntime,
    });

    expect(presented).toEqual({ run: detailRun, runtime: detailRuntime });
    expect(threadRuntimeIsActive(presented.runtime)).toBe(true);
    expect(presented.runtime?.activeRunId).toBe(detailRun.runId);
  });

  it("has no working row without a runtime", () => {
    expect(deriveThreadWorkingStartedAt(waitingRun, null, null)).toBeNull();
  });
});
