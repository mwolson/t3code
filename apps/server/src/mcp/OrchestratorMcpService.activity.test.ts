import {
  EnvironmentId,
  NodeId,
  type OrchestrationV2ThreadProjection,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestratorMcpFailure,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodePath from "@effect/platform-node/NodePath";
import * as ProjectService from "../project/ProjectService.ts";
import { expect, it } from "vite-plus/test";

import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderRegistry from "../provider/Services/ProviderRegistry.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import type * as McpInvocationContext from "./McpInvocationContext.ts";
import * as OrchestratorMcpService from "./OrchestratorMcpService.ts";

const orchestratorMcpServiceLayer = OrchestratorMcpService.layer.pipe(
  Layer.provide(NodePath.layer),
  Layer.provide(Layer.mock(ProjectService.ProjectService)({})),
);

const environmentId = EnvironmentId.make("environment-mcp-orchestrator-detail");
const projectId = ProjectId.make("project-mcp-orchestrator-detail");
const parentThreadId = ThreadId.make("thread-mcp-orchestrator-parent");
const childThreadId = ThreadId.make("thread-mcp-orchestrator-child");
const activeRunId = RunId.make("run-mcp-active");
const cancelledRunId = RunId.make("run-mcp-cancelled");
const childRunId = RunId.make("run-mcp-child");
const taskId = NodeId.make("node-mcp-task-1");
const now = DateTime.makeUnsafe("2026-08-04T12:00:00.000Z");
const codexDriver = ProviderDriverKind.make("codex");
// Distinct from driver kind so a regression that re-derives from driver fails.
const customCodexInstanceId = ProviderInstanceId.make("codex-custom-workspace");
const parentInstanceId = ProviderInstanceId.make("codex");

const makeScope = (): McpInvocationContext.McpInvocationScope => ({
  environmentId,
  threadId: parentThreadId,
  providerSessionId: "provider-session-mcp-orchestrator-detail",
  providerInstanceId: parentInstanceId,
  capabilities: new Set(["orchestration"]),
  issuedAt: 1,
});

function baseThread(input: {
  readonly threadId: ThreadId;
  readonly title: string;
  readonly instanceId: ProviderInstanceId;
  readonly model: string;
}) {
  return {
    id: input.threadId,
    projectId,
    title: input.title,
    createdBy: "user" as const,
    creationSource: "mcp" as const,
    modelSelection: {
      instanceId: input.instanceId,
      model: input.model,
    },
    runtimeMode: "full-access" as const,
    interactionMode: "default" as const,
    branch: null,
    worktreePath: null,
    lineage: {
      parentThreadId: null,
      relationshipToParent: null,
      rootThreadId: input.threadId,
    },
    archivedAt: null,
    deletedAt: null,
    providerInstanceId: input.instanceId,
    createdAt: now,
    updatedAt: now,
  };
}

function makeRun(input: {
  readonly id: RunId;
  readonly ordinal: number;
  readonly status: "running" | "waiting" | "cancelled" | "queued" | "completed";
  readonly instanceId?: ProviderInstanceId;
}) {
  return {
    id: input.id,
    ordinal: input.ordinal,
    status: input.status,
    modelSelection: {
      instanceId: input.instanceId ?? parentInstanceId,
      model: "gpt-5.4",
    },
    providerInstanceId: input.instanceId ?? parentInstanceId,
    requestedAt: now,
    startedAt: input.status === "cancelled" || input.status === "queued" ? null : now,
    completedAt: input.status === "cancelled" || input.status === "completed" ? now : null,
  };
}

it("readThread prefers activity-run status over a newer cancelled queued run", async () => {
  const projection = {
    thread: baseThread({
      threadId: parentThreadId,
      title: "Parent",
      instanceId: parentInstanceId,
      model: "gpt-5.4",
    }),
    runs: [
      makeRun({ id: activeRunId, ordinal: 1, status: "running" }),
      makeRun({ id: cancelledRunId, ordinal: 2, status: "cancelled" }),
    ],
    visibleTurnItems: [],
    runtimeRequests: [],
    messages: [],
    contextTransfers: [],
    subagents: [],
    updatedAt: now,
  } as unknown as OrchestrationV2ThreadProjection;

  const layer = orchestratorMcpServiceLayer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          getTimelinePage: () => Effect.succeed({ items: [], totalItems: 0, hasMore: false }),
          getThreadRecords: (threadId) =>
            threadId === parentThreadId
              ? Effect.succeed(projection)
              : Effect.die(`unexpected thread ${threadId}`),
        } satisfies Partial<ThreadManagementService.ThreadManagementService["Service"]>),
        Layer.mock(ProviderRegistry.ProviderRegistry)({
          getProviders: Effect.succeed([]),
        } satisfies Partial<ProviderRegistry.ProviderRegistry["Service"]>),
        Layer.mock(ScheduledTaskService.ScheduledTaskService)({
          list: () => Effect.succeed({ tasks: [] }),
        } satisfies Partial<ScheduledTaskService.ScheduledTaskService["Service"]>),
        Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({
          list: () => Effect.succeed([]),
        } satisfies Partial<ProviderAdapterRegistry.ProviderAdapterRegistryV2["Service"]>),
        NodeCrypto.layer,
      ),
    ),
  );

  await Effect.gen(function* () {
    const service = yield* OrchestratorMcpService.OrchestratorMcpService;
    const result = yield* service.readThread(makeScope(), { threadId: parentThreadId });
    expect(result.thread.status).toBe("running");
    expect(result.thread.latestRunId).toBe(cancelledRunId);
    expect(result.thread.activeRunId).toBe(activeRunId);
  }).pipe(Effect.provide(layer), Effect.runPromise);
});

it("readThread prefers waiting activity status over a newer cancelled queued run", async () => {
  const projection = {
    thread: baseThread({
      threadId: parentThreadId,
      title: "Parent waiting",
      instanceId: parentInstanceId,
      model: "gpt-5.4",
    }),
    runs: [
      makeRun({ id: activeRunId, ordinal: 1, status: "waiting" }),
      makeRun({ id: cancelledRunId, ordinal: 2, status: "cancelled" }),
    ],
    visibleTurnItems: [],
    runtimeRequests: [],
    messages: [],
    contextTransfers: [],
    subagents: [],
    updatedAt: now,
  } as unknown as OrchestrationV2ThreadProjection;

  const layer = orchestratorMcpServiceLayer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          getTimelinePage: () => Effect.succeed({ items: [], totalItems: 0, hasMore: false }),
          getThreadRecords: (threadId) =>
            threadId === parentThreadId
              ? Effect.succeed(projection)
              : Effect.die(`unexpected thread ${threadId}`),
        } satisfies Partial<ThreadManagementService.ThreadManagementService["Service"]>),
        Layer.mock(ProviderRegistry.ProviderRegistry)({
          getProviders: Effect.succeed([]),
        } satisfies Partial<ProviderRegistry.ProviderRegistry["Service"]>),
        Layer.mock(ScheduledTaskService.ScheduledTaskService)({
          list: () => Effect.succeed({ tasks: [] }),
        } satisfies Partial<ScheduledTaskService.ScheduledTaskService["Service"]>),
        Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({
          list: () => Effect.succeed([]),
        } satisfies Partial<ProviderAdapterRegistry.ProviderAdapterRegistryV2["Service"]>),
        NodeCrypto.layer,
      ),
    ),
  );

  await Effect.gen(function* () {
    const service = yield* OrchestratorMcpService.OrchestratorMcpService;
    const result = yield* service.readThread(makeScope(), { threadId: parentThreadId });
    expect(result.thread.status).toBe("waiting");
    expect(result.thread.activeRunId).toBe(activeRunId);
  }).pipe(Effect.provide(layer), Effect.runPromise);
});

it("taskStatus returns task.providerInstanceId rather than the driver kind", async () => {
  const parentProjection = {
    thread: baseThread({
      threadId: parentThreadId,
      title: "Parent",
      instanceId: parentInstanceId,
      model: "gpt-5.4",
    }),
    runs: [makeRun({ id: activeRunId, ordinal: 1, status: "running" })],
    visibleTurnItems: [],
    runtimeRequests: [],
    messages: [],
    contextTransfers: [],
    subagents: [
      {
        id: taskId,
        threadId: parentThreadId,
        runId: activeRunId,
        parentNodeId: NodeId.make("node-parent"),
        origin: "app_owned",
        createdBy: "agent",
        driver: codexDriver,
        providerInstanceId: customCodexInstanceId,
        providerThreadId: null,
        childThreadId,
        nativeTaskRef: null,
        prompt: "Inspect the custom instance.",
        title: null,
        model: "gpt-5.4",
        status: "running",
        result: null,
        startedAt: now,
        completedAt: null,
        updatedAt: now,
      },
    ],
    updatedAt: now,
  } as unknown as OrchestrationV2ThreadProjection;

  const childProjection = {
    thread: {
      ...baseThread({
        threadId: childThreadId,
        title: "Child",
        instanceId: customCodexInstanceId,
        model: "gpt-5.4",
      }),
      lineage: {
        parentThreadId,
        relationshipToParent: "subagent",
        rootThreadId: parentThreadId,
      },
      createdBy: "agent",
    },
    runs: [
      makeRun({
        id: childRunId,
        ordinal: 1,
        status: "running",
        instanceId: customCodexInstanceId,
      }),
    ],
    visibleTurnItems: [],
    runtimeRequests: [],
    messages: [],
    contextTransfers: [
      {
        type: "subagent_spawn",
        sourceThreadId: parentThreadId,
        targetThreadId: childThreadId,
        targetRunId: childRunId,
      },
    ],
    subagents: [],
    providerThreads: [],
    updatedAt: now,
  } as unknown as OrchestrationV2ThreadProjection;

  const layer = orchestratorMcpServiceLayer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          getTimelinePage: () => Effect.succeed({ items: [], totalItems: 0, hasMore: false }),
          getThreadRecords: (threadId) => {
            if (threadId === parentThreadId) return Effect.succeed(parentProjection);
            if (threadId === childThreadId) return Effect.succeed(childProjection);
            return Effect.die(`unexpected thread ${threadId}`);
          },
        } satisfies Partial<ThreadManagementService.ThreadManagementService["Service"]>),
        Layer.mock(ProviderRegistry.ProviderRegistry)({
          getProviders: Effect.succeed([]),
        } satisfies Partial<ProviderRegistry.ProviderRegistry["Service"]>),
        Layer.mock(ScheduledTaskService.ScheduledTaskService)({
          list: () => Effect.succeed({ tasks: [] }),
        } satisfies Partial<ScheduledTaskService.ScheduledTaskService["Service"]>),
        Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({
          list: () => Effect.succeed([]),
        } satisfies Partial<ProviderAdapterRegistry.ProviderAdapterRegistryV2["Service"]>),
        NodeCrypto.layer,
      ),
    ),
  );

  await Effect.gen(function* () {
    const service = yield* OrchestratorMcpService.OrchestratorMcpService;
    const result = yield* service.taskStatus(makeScope(), taskId);
    expect(result.providerInstanceId).toBe(customCodexInstanceId);
    expect(result.providerInstanceId).not.toBe(ProviderInstanceId.make(String(codexDriver)));
    expect(result.status).toBe("running");
    expect(result.taskId).toBe(taskId);
    expect(result.childThreadId).toBe(childThreadId);
  }).pipe(Effect.provide(layer), Effect.runPromise);
});

it("lists created cross-project threads in sorted pages from one active snapshot", async () => {
  const foreignProjectId = ProjectId.make("project-list-foreign");
  const shell = (id: string, project: ProjectId, minute: number) => ({
    ...baseThread({
      threadId: ThreadId.make(id),
      title: id,
      instanceId: parentInstanceId,
      model: "gpt-5.4",
    }),
    projectId: project,
    updatedAt: DateTime.makeUnsafe(`2026-08-04T12:0${minute}:00.000Z`),
    status: "idle" as const,
    activityRunStatus: null,
  });
  const older = shell("older", projectId, 1);
  const created = shell("created", foreignProjectId, 2);
  const newer = shell("newer", projectId, 3);
  const unrelated = shell("unrelated", foreignProjectId, 4);
  const subagent = {
    ...shell("subagent", foreignProjectId, 5),
    lineage: { ...created.lineage, relationshipToParent: "subagent" as const },
  };
  const shells = [older, newer, unrelated, created, subagent];
  let snapshots = 0;
  let shellReads = 0;
  const layer = orchestratorMcpServiceLayer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          getThreadRecords: () =>
            Effect.succeed({
              thread: baseThread({
                threadId: parentThreadId,
                title: "Parent",
                instanceId: parentInstanceId,
                model: "gpt-5.4",
              }),
              turnItems: [created, subagent].map((thread) => ({
                type: "thread_created",
                targetThreadId: thread.id,
              })),
            } as unknown as OrchestrationV2ThreadProjection),
          getShellSnapshot: () =>
            Effect.sync(() => {
              snapshots += 1;
              return { threads: shells } as never;
            }),
          getThreadShell: (threadId) =>
            Effect.sync(() => {
              shellReads += 1;
              return shells.find((thread) => thread.id === threadId) as never;
            }),
        }),
        Layer.mock(ProviderRegistry.ProviderRegistry)({ getProviders: Effect.succeed([]) }),
        Layer.mock(ScheduledTaskService.ScheduledTaskService)({
          list: () => Effect.succeed({ tasks: [] }),
        }),
        Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({
          list: () => Effect.succeed([]),
        }),
        NodeCrypto.layer,
      ),
    ),
  );
  await Effect.gen(function* () {
    const service = yield* OrchestratorMcpService.OrchestratorMcpService;
    const first = yield* service.listThreads(makeScope(), { limit: 2, includeSubagents: false });
    expect(first.threads.map((thread) => thread.threadId)).toEqual([newer.id, created.id]);
    expect(first.nextCursor).toBe(2);
    expect(first.total).toBe(3);
    expect(first.projectId).toBe(projectId);
    expect(first.threads.map((thread) => thread.projectId)).toEqual([projectId, foreignProjectId]);
    expect(snapshots).toBe(1);
    expect(shellReads).toBe(0);
    const second = yield* service.listThreads(makeScope(), {
      cursor: first.nextCursor!,
      limit: 2,
      includeSubagents: false,
    });
    expect(second.threads.map((thread) => thread.threadId)).toEqual([older.id]);
    expect(second.nextCursor).toBeNull();
    expect(second.total).toBe(3);
    const withSubagents = yield* service.listThreads(makeScope(), {});
    expect(withSubagents.threads.map((thread) => thread.threadId)).toEqual([
      subagent.id,
      newer.id,
      created.id,
      older.id,
    ]);
    expect(snapshots).toBe(3);
    expect(shellReads).toBe(0);
  }).pipe(Effect.provide(layer), Effect.runPromise);
});

it("readThread reaches a thread the user attached as context, but not one an agent attached", async () => {
  const foreignProjectId = ProjectId.make("project-mcp-orchestrator-foreign");
  const foreignThreadId = ThreadId.make("thread-mcp-orchestrator-foreign");
  const agentOnlyThreadId = ThreadId.make("thread-mcp-orchestrator-agent-only");
  const threadRecord = (threadId: ThreadId) => ({
    version: 1,
    kind: "thread",
    contextId: `thread_${threadId}`,
    label: "Attached",
    environmentId,
    threadId,
    title: "Attached",
  });
  const message = (input: { createdBy: "user" | "agent"; threadId: ThreadId }) => ({
    id: `message-${input.threadId}`,
    threadId: parentThreadId,
    runId: null,
    nodeId: null,
    role: "user",
    text: `[Attached](t3-context://v1/thread/thread_${input.threadId})`,
    context: { version: 1, records: [threadRecord(input.threadId)] },
    attachments: [],
    streaming: false,
    createdBy: input.createdBy,
    creationSource: input.createdBy === "user" ? "user" : "mcp",
    createdAt: now,
    updatedAt: now,
  });
  let createdRecord = false;
  let deleted = false;
  let parentDeleted = false;
  const parentProjection = {
    thread: baseThread({
      threadId: parentThreadId,
      title: "Parent",
      instanceId: parentInstanceId,
      model: "gpt-5.4",
    }),
    runs: [],
    turnItems: [],
    visibleTurnItems: [],
    runtimeRequests: [],
    messages: [
      message({ createdBy: "user", threadId: foreignThreadId }),
      message({ createdBy: "agent", threadId: agentOnlyThreadId }),
    ],
    contextTransfers: [],
    subagents: [],
    updatedAt: now,
  } as unknown as OrchestrationV2ThreadProjection;
  const foreignProjection = (threadId: ThreadId) =>
    ({
      thread: {
        ...baseThread({
          threadId,
          title: "Foreign",
          instanceId: parentInstanceId,
          model: "gpt-5.4",
        }),
        projectId: foreignProjectId,
        deletedAt: deleted ? now : null,
      },
      runs: [],
      visibleTurnItems: [
        {
          position: 0,
          visibility: "inherited",
          sourceThreadId: threadId,
          sourceItemId: "item-1",
          item: {
            id: "item-1",
            type: "assistant_message",
            messageId: "assistant-1",
            status: "completed",
            title: null,
            text: "Foreign thread said hello",
            createdAt: now,
            updatedAt: now,
          },
        },
      ],
      runtimeRequests: [],
      messages: [],
      contextTransfers: [],
      subagents: [],
      updatedAt: now,
    }) as unknown as OrchestrationV2ThreadProjection;

  const layer = orchestratorMcpServiceLayer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          getThreadRecords: (threadId, _collections, options) => {
            if (threadId === parentThreadId) {
              expect(options).toEqual({
                turnItemTypes: ["thread_created"],
                messageRoles: ["user"],
              });
              return Effect.succeed({
                ...parentProjection,
                thread: { ...parentProjection.thread, deletedAt: parentDeleted ? now : null },
                turnItems: createdRecord
                  ? [
                      {
                        type: "thread_created" as const,
                        targetThreadId: agentOnlyThreadId,
                        id: TurnItemId.make("created-record"),
                        threadId: parentThreadId,
                        runId: null,
                        nodeId: null,
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
                        targetRunId: null,
                        targetProviderInstanceId: parentInstanceId,
                        targetModel: "gpt-5.4",
                      },
                    ]
                  : [],
              });
            }
            if (threadId === foreignThreadId || threadId === agentOnlyThreadId) {
              return Effect.succeed(foreignProjection(threadId));
            }
            return Effect.die(`unexpected thread ${threadId}`);
          },
          getTimelinePage: (threadId) =>
            Effect.succeed({
              items: foreignProjection(threadId).visibleTurnItems,
              totalItems: 1,
              hasMore: false,
            }),
          getProjectThreadRecords: (input) =>
            Effect.fail(
              new ThreadManagementService.ThreadManagementThreadNotFoundError({
                projectId: input.projectId,
                threadId: input.threadId,
              }),
            ),
          getShellSnapshot: () =>
            Effect.succeed({
              threads: [foreignThreadId, agentOnlyThreadId].map((threadId) => ({
                ...foreignProjection(threadId).thread,
                status: "idle",
                activityRunStatus: null,
              })),
            } as never),
          getThreadShell: (threadId) =>
            Effect.succeed({
              ...foreignProjection(threadId).thread,
              status: "idle",
              activityRunStatus: null,
            } as never),
          // Access checks must reject before these succeed.
          sendToThread: () =>
            Effect.succeed({
              run: { id: RunId.make("run-unexpected-send"), status: "running" },
              delivery: "started",
            } as never),
          waitForThread: (input) =>
            Effect.succeed({ threadId: input.threadId, run: null, timedOut: false }),
          interruptThread: () => Effect.succeed({ type: "no_active_run" } as const),
        } satisfies Partial<ThreadManagementService.ThreadManagementService["Service"]>),
        Layer.mock(ProviderRegistry.ProviderRegistry)({
          getProviders: Effect.succeed([]),
        } satisfies Partial<ProviderRegistry.ProviderRegistry["Service"]>),
        Layer.mock(ScheduledTaskService.ScheduledTaskService)({
          list: () => Effect.succeed({ tasks: [] }),
        } satisfies Partial<ScheduledTaskService.ScheduledTaskService["Service"]>),
        Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({
          list: () => Effect.succeed([]),
        } satisfies Partial<ProviderAdapterRegistry.ProviderAdapterRegistryV2["Service"]>),
        NodeCrypto.layer,
      ),
    ),
  );

  await Effect.gen(function* () {
    const service = yield* OrchestratorMcpService.OrchestratorMcpService;
    const attached = yield* service.readThread(makeScope(), { threadId: foreignThreadId });
    expect(attached.thread.threadId).toBe(foreignThreadId);
    expect(attached.items.map((item) => item.text)).toEqual(["Foreign thread said hello"]);

    const denied = yield* service
      .readThread(makeScope(), { threadId: agentOnlyThreadId })
      .pipe(Effect.flip);
    expect(denied.code).toBe("thread_not_found");

    const write = yield* service
      .sendToThread(makeScope(), { threadId: foreignThreadId, message: "hi" })
      .pipe(Effect.flip);
    expect(write.code).toBe("thread_not_found");
    expect(
      (yield* service.waitForThread(makeScope(), { threadId: foreignThreadId }).pipe(Effect.flip))
        .code,
    ).toBe("thread_not_found");
    expect(
      (yield* service.interruptThread(makeScope(), { threadId: foreignThreadId }).pipe(Effect.flip))
        .code,
    ).toBe("thread_not_found");
    const unrelatedThreadId = ThreadId.make("thread-unrelated-cross-project");
    for (const operation of [
      service.readThread(makeScope(), { threadId: unrelatedThreadId }),
      service.sendToThread(makeScope(), { threadId: unrelatedThreadId, message: "hi" }),
      service.waitForThread(makeScope(), { threadId: unrelatedThreadId }),
      service.interruptThread(makeScope(), { threadId: unrelatedThreadId }),
    ]) {
      expect(
        (yield* Effect.flip(Effect.asVoid<unknown, OrchestratorMcpFailure, never>(operation))).code,
      ).toBe("thread_not_found");
    }
    createdRecord = true;
    const listed = yield* service.listThreads(makeScope(), {});
    expect(listed.threads.map((thread) => thread.threadId)).toEqual([agentOnlyThreadId]);
    expect(listed.total).toBe(1);
    expect(listed.nextCursor).toBeNull();
    expect((yield* service.listThreads(makeScope(), { cursor: 1 })).threads).toEqual([]);
    const unprivilegedScope = { ...makeScope(), capabilities: new Set<never>() };
    for (const operation of [
      service.readThread(unprivilegedScope, { threadId: agentOnlyThreadId }),
      service.sendToThread(unprivilegedScope, { threadId: agentOnlyThreadId, message: "hi" }),
      service.waitForThread(unprivilegedScope, { threadId: agentOnlyThreadId }),
      service.interruptThread(unprivilegedScope, { threadId: agentOnlyThreadId }),
      service.createThreads(unprivilegedScope, { threads: [{ projectDirectory: "/known" }] }),
    ]) {
      expect(
        (yield* Effect.flip(Effect.asVoid<unknown, OrchestratorMcpFailure, never>(operation))).code,
      ).toBe("capability_denied");
    }
    for (const operation of [
      service.readThread(makeScope(), { threadId: unrelatedThreadId }),
      service.sendToThread(makeScope(), { threadId: unrelatedThreadId, message: "hi" }),
      service.waitForThread(makeScope(), { threadId: unrelatedThreadId }),
      service.interruptThread(makeScope(), { threadId: unrelatedThreadId }),
    ]) {
      expect(
        (yield* Effect.flip(Effect.asVoid<unknown, OrchestratorMcpFailure, never>(operation))).code,
      ).toBe("thread_not_found");
    }
    expect(
      (yield* service.readThread(makeScope(), { threadId: agentOnlyThreadId })).thread.threadId,
    ).toBe(agentOnlyThreadId);
    parentDeleted = true;
    for (const operation of [
      service.listThreads(makeScope(), {}),
      service.readThread(makeScope(), { threadId: agentOnlyThreadId }),
      service.sendToThread(makeScope(), { threadId: agentOnlyThreadId, message: "hi" }),
      service.waitForThread(makeScope(), { threadId: agentOnlyThreadId }),
      service.interruptThread(makeScope(), { threadId: agentOnlyThreadId }),
    ]) {
      const result = yield* Effect.asVoid<unknown, OrchestratorMcpFailure, never>(operation).pipe(
        Effect.match({ onFailure: (error) => error.code, onSuccess: () => "allowed" }),
      );
      expect(result).toBe("thread_not_found");
    }
    parentDeleted = false;
    deleted = true;
    expect(
      (yield* service.readThread(makeScope(), { threadId: agentOnlyThreadId }).pipe(Effect.flip))
        .code,
    ).toBe("thread_not_found");
    expect(
      (yield* service
        .sendToThread(makeScope(), { threadId: agentOnlyThreadId, message: "hi" })
        .pipe(Effect.flip)).code,
    ).toBe("thread_not_found");
    expect(
      (yield* service.waitForThread(makeScope(), { threadId: agentOnlyThreadId }).pipe(Effect.flip))
        .code,
    ).toBe("thread_not_found");
    expect(
      (yield* service
        .interruptThread(makeScope(), { threadId: agentOnlyThreadId })
        .pipe(Effect.flip)).code,
    ).toBe("thread_not_found");
  }).pipe(Effect.provide(layer), Effect.runPromise);
  // Rebuild the service over persisted caller/target snapshots, retaining the grant row.
  parentDeleted = true;
  deleted = false;
  await Effect.gen(function* () {
    const service = yield* OrchestratorMcpService.OrchestratorMcpService;
    for (const operation of [
      service.readThread(makeScope(), { threadId: agentOnlyThreadId }),
      service.sendToThread(makeScope(), { threadId: agentOnlyThreadId, message: "after restart" }),
      service.waitForThread(makeScope(), { threadId: agentOnlyThreadId }),
      service.interruptThread(makeScope(), { threadId: agentOnlyThreadId }),
      service.listThreads(makeScope(), {}),
    ]) {
      expect(
        yield* Effect.asVoid<unknown, OrchestratorMcpFailure, never>(operation).pipe(
          Effect.match({ onFailure: (error) => error.code, onSuccess: () => "allowed" }),
        ),
      ).toBe("thread_not_found");
    }
  }).pipe(Effect.provide(layer), Effect.runPromise);
});
