import * as SourceControlProviderRegistry from "../sourceControl/SourceControlProviderRegistry.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  type ModelSelection,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import * as ServerConfig from "../config.ts";
import * as McpSessionRegistryTestkit from "../mcp/McpSessionRegistry.testkit.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as ProjectEnrichmentService from "../project/ProjectEnrichmentService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import type { ProviderInstance } from "../provider/ProviderDriver.ts";
import * as ProviderInstanceRegistry from "../provider/Services/ProviderInstanceRegistry.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import type { ProviderAdapterV2Shape } from "./ProviderAdapter.ts";
import {
  OrchestrationV2EventSinkLayerLive,
  OrchestrationV2LayerLive,
  ProjectServiceLayerLive,
} from "./runtimeLayer.ts";
import { worktreeRepairDependenciesTestLayer } from "./ProviderTurnStartService.testkit.ts";

const PlatformTestLayer = Layer.merge(
  NodeServices.layer,
  Layer.mock(SourceControlProviderRegistry.SourceControlProviderRegistry)({
    resolveLink: () => Effect.die("unused title link"),
  }),
);

const ServerConfigLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-orchestration-v2-delegated-completion-",
});

const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.4",
} satisfies ModelSelection;

const VcsDriverRegistryTestLayer = VcsDriverRegistry.layer.pipe(
  Layer.provide(VcsProcess.layer),
  Layer.provide(ServerConfigLayer),
  Layer.provide(PlatformTestLayer),
);

const CheckpointStoreTestLayer = CheckpointStore.layer.pipe(
  Layer.provide(VcsDriverRegistryTestLayer),
);

const driver = ProviderDriverKind.make("codex");
const orchestrationAdapter = {
  instanceId: modelSelection.instanceId,
  driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
  openSession: () => Effect.die("sessions are not used by delegated completion tests"),
} as ProviderAdapterV2Shape;
const providerInstance = {
  instanceId: modelSelection.instanceId,
  driverKind: driver,
  continuationIdentity: {
    driverKind: driver,
    continuationKey: "codex:test",
  },
  displayName: "Codex test",
  enabled: true,
  // No supportedRuntimeModes: every runtime mode runs as stored.
  snapshot: { getSnapshot: Effect.succeed({}) } as unknown as ProviderInstance["snapshot"],
  orchestrationAdapter,
  textGeneration: {} as ProviderInstance["textGeneration"],
} satisfies ProviderInstance;

const TestProviderInstanceRegistry = Layer.succeed(
  ProviderInstanceRegistry.ProviderInstanceRegistry,
  {
    getInstance: (instanceId) =>
      Effect.succeed(instanceId === providerInstance.instanceId ? providerInstance : undefined),
    listInstances: Effect.succeed([providerInstance]),
    listUnavailable: Effect.succeed([]),
    streamChanges: Stream.empty,
    subscribeChanges: Effect.never,
  },
);

const makeTestLayer = <E, R>(database: Layer.Layer<SqlClient.SqlClient, E, R>) =>
  Layer.mergeAll(
    OrchestrationV2LayerLive,
    OrchestrationV2EventSinkLayerLive,
    ProjectionStore.layer,
  ).pipe(
    Layer.provideMerge(ProjectServiceLayerLive),
    Layer.provide(
      Layer.mock(WorkspacePaths.WorkspacePaths)({
        normalizeWorkspaceRoot: (workspaceRoot) => Effect.succeed(workspaceRoot),
      }),
    ),
    Layer.provide(worktreeRepairDependenciesTestLayer),
    Layer.provide(
      Layer.succeed(ProjectEnrichmentService.ProjectEnrichmentService, {
        peek: () =>
          Effect.succeed({
            repositoryIdentity: null,
            faviconPath: null,
            repositoryIdentityResolved: false,
          }),
        request: () => Effect.void,
        getAvailable: () =>
          Effect.succeed({
            repositoryIdentity: null,
            faviconPath: null,
            repositoryIdentityResolved: false,
          }),
        invalidate: () => Effect.void,
        subscribeChanges: Effect.never,
      }),
    ),
    Layer.provide(McpSessionRegistryTestkit.layer),
    Layer.provideMerge(database),
    Layer.provide(CheckpointStoreTestLayer),
    Layer.provide(ServerConfigLayer),
    Layer.provide(ServerSettings.layerTest()),
    Layer.provide(TestProviderInstanceRegistry),
    Layer.provide(PlatformTestLayer),
  );

const TestLayer = makeTestLayer(SqlitePersistenceMemory);

it.effect("live upgrade reserves from the planned state", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sink = yield* EventSink.EventSinkV2;
    const now = yield* DateTime.now;
    const threadId = ThreadId.make("planned-state");
    const runId = RunId.make("planned-state:parent");
    const xId = NodeId.make("planned-state:x");
    const yId = NodeId.make("planned-state:y");
    yield* seedParentWithTerminalTask({
      threadId,
      runId,
      taskId: NodeId.make("planned-state:control"),
      now,
      projectId: ProjectId.make("planned-state:project"),
      rootNodeId: NodeId.make("planned-state:root"),
      completionWake: "always",
      deliveryState: "delivered",
      parentStatus: "running",
    });
    const seed = yield* orchestrator.getThreadProjection(threadId);
    const { completionDelivery: _omit, ...base } = seed.subagents[0]!;
    yield* sink.write({
      events: [
        {
          id: EventId.make("planned-state:x"),
          type: "subagent.updated",
          threadId,
          runId,
          occurredAt: now,
          payload: { ...base, id: xId, completionWake: "settled_only" },
        },
        {
          id: EventId.make("planned-state:y"),
          type: "subagent.updated",
          threadId,
          runId,
          occurredAt: now,
          payload: {
            ...base,
            id: yId,
            completionWake: "always",
            completionDelivery: { state: "pending", observedByRunId: null },
          },
        },
      ],
    });
    const upgrade = yield* orchestrator.dispatch({
      type: "delegated_task.wake-policy",
      commandId: CommandId.make("planned-state:upgrade"),
      parentThreadId: threadId,
      taskId: xId,
      completionWake: "always",
    });
    const reservations = upgrade.storedEvents.flatMap(({ event }) =>
      event.type === "run.updated" &&
      event.payload.id === runId &&
      event.payload.delegatedCompletion?.delivery != null
        ? [event.payload.delegatedCompletion.delivery]
        : [],
    );
    const after = yield* orchestrator.getThreadProjection(threadId);
    const cohort = after.runs.find((run) => run.id === runId)?.delegatedCompletion;
    assert.equal(reservations.length, 1, "one reservation per generation");
    assert.deepEqual(cohort?.delivery?.taskIds, [xId]);
    assert.equal(cohort?.delivery?.generation, 2);
    assert.equal(cohort?.nextGeneration, 3);
    assert.equal(
      after.subagents.find((task) => task.id === xId)?.completionDelivery?.state,
      "claimed",
    );
    assert.equal(
      after.subagents.find((task) => task.id === yId)?.completionDelivery?.state,
      "pending",
    );
  }).pipe(Effect.provide(Layer.fresh(TestLayer))),
);

it.effect("rearmed target keeps its own new policy", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const now = yield* DateTime.now;
    const threadId = ThreadId.make("target-policy");
    const runId = RunId.make("target-policy:parent");
    const taskId = NodeId.make("target-policy:task");
    yield* seedParentWithTerminalTask({
      threadId,
      runId,
      taskId,
      now,
      projectId: ProjectId.make("target-policy:project"),
      rootNodeId: NodeId.make("target-policy:root"),
      completionWake: "always",
      deliveryState: "pending",
      parentStatus: "completed",
    });
    yield* orchestrator.dispatch({
      type: "delegated_task.wake-policy",
      commandId: CommandId.make("target-policy:rearm"),
      parentThreadId: threadId,
      taskId,
      completionWake: "settled_only",
    });
    const after = yield* orchestrator.getThreadProjection(threadId);
    const task = after.subagents.find((candidate) => candidate.id === taskId);
    assert.equal(task?.completionDelivery?.state, "claimed");
    assert.equal(
      task?.completionWake,
      "settled_only",
      "the command's own policy change was clobbered",
    );
  }).pipe(Effect.provide(Layer.fresh(TestLayer))),
);

it.effect.each(["running", "completed"] as const)(
  "reservation planning failures retain the wake-policy command identity (%s)",
  (parentStatus) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make(`planning-error:${parentStatus}`);
      const taskId = NodeId.make("planning-error:task");
      yield* seedParentWithTerminalTask({
        threadId,
        runId: RunId.make("planning-error:parent"),
        taskId,
        now,
        projectId: ProjectId.make("planning-error:project"),
        rootNodeId: NodeId.make("planning-error:root"),
        completionWake: "settled_only",
        deliveryState: "pending",
        parentStatus,
      });
      const command = {
        type: "delegated_task.wake-policy" as const,
        commandId: CommandId.make("planning-error:upgrade"),
        parentThreadId: threadId,
        taskId,
        completionWake: "always" as const,
      };
      const original = store.getMessageCount;
      yield* Effect.acquireUseRelease(
        Effect.sync(() => {
          Object.assign(store, {
            getMessageCount: () =>
              Effect.fail(new ProjectionStore.ProjectionStoreThreadNotFoundError({ threadId })),
          });
        }),
        () =>
          Effect.gen(function* () {
            const error = yield* orchestrator.dispatch(command).pipe(Effect.flip);
            assert.equal(error._tag, "OrchestratorDispatchError");
            if (error._tag !== "OrchestratorDispatchError") return;
            assert.equal(error.commandId, command.commandId);
            assert.equal(error.commandType, command.type);
          }),
        () =>
          Effect.sync(() => {
            Object.assign(store, { getMessageCount: original });
          }),
      );
      const retry = yield* orchestrator.dispatch(command).pipe(Effect.flip);
      assert.equal(retry._tag, "OrchestratorCommandPreviouslyRejectedError");
    }).pipe(Effect.provide(Layer.fresh(TestLayer))),
);

const seedParentWithTerminalTask = (input: {
  readonly threadId: ThreadId;
  readonly projectId: ProjectId;
  readonly runId: RunId;
  readonly rootNodeId: NodeId;
  readonly taskId: NodeId;
  readonly deliveryState: "pending" | "delivered" | "claimed" | "acknowledged" | "disposed";
  readonly completionWake?: "always" | "settled_only";
  readonly parentStatus?: "running" | "completed";
  readonly deliveryTaskIds?: ReadonlyArray<NodeId>;
  readonly now: DateTime.Utc;
}) =>
  Effect.gen(function* () {
    const projects = yield* ProjectService.ProjectService;
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const eventSink = yield* EventSink.EventSinkV2;
    const providerThreadId = ProviderThreadId.make(
      `provider-thread:${String(input.threadId).replace("thread:", "")}`,
    );

    yield* projects.create({
      commandId: CommandId.make(`command:seed-project:${input.threadId}`),
      projectId: input.projectId,
      title: "Delegated completion delivery",
      workspaceRoot: `/workspace/${input.projectId}`,
    });

    yield* orchestrator.dispatch({
      type: "thread.create",
      createdBy: "user",
      creationSource: "web",
      commandId: CommandId.make(`command:seed-create:${input.threadId}`),
      threadId: input.threadId,
      projectId: input.projectId,
      title: "Delegated completion delivery",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
    });

    yield* eventSink.write({
      commandId: CommandId.make(`command:seed-projection:${input.threadId}`),
      events: [
        {
          id: EventId.make(`event:seed-provider-thread:${input.threadId}`),
          type: "provider-thread.updated",
          threadId: input.threadId,
          driver,
          providerInstanceId: modelSelection.instanceId,
          occurredAt: input.now,
          payload: {
            id: providerThreadId,
            driver,
            providerInstanceId: modelSelection.instanceId,
            providerSessionId: null,
            appThreadId: input.threadId,
            ownerNodeId: input.rootNodeId,
            nativeThreadRef: {
              driver,
              nativeId: `native:${input.threadId}`,
              strength: "strong",
            },
            nativeConversationHeadRef: null,
            status: "active",
            firstRunOrdinal: 1,
            lastRunOrdinal: 1,
            handoffIds: [],
            forkedFrom: null,
            createdAt: input.now,
            updatedAt: input.now,
          },
        },
        {
          id: EventId.make(`event:seed-run:${input.threadId}`),
          type: "run.updated",
          threadId: input.threadId,
          runId: input.runId,
          nodeId: input.rootNodeId,
          providerInstanceId: modelSelection.instanceId,
          occurredAt: input.now,
          payload: {
            id: input.runId,
            threadId: input.threadId,
            ordinal: 1,
            providerInstanceId: modelSelection.instanceId,
            modelSelection,
            providerThreadId,
            userMessageId: MessageId.make(`message:seed-user:${input.threadId}`),
            rootNodeId: input.rootNodeId,
            activeAttemptId: null,
            status: input.parentStatus ?? "running",
            requestedAt: input.now,
            startedAt: input.now,
            completedAt: input.parentStatus === "completed" ? input.now : null,
            checkpointId: null,
            contextHandoffId: null,
            delegatedCompletion: {
              disposition: "open",
              nextGeneration: 2,
              delivery:
                input.deliveryTaskIds === undefined
                  ? null
                  : {
                      generation: 1,
                      messageId: MessageId.make(`message:delegated-delivery:${input.threadId}`),
                      taskIds: input.deliveryTaskIds,
                    },
            },
          },
        },
        {
          id: EventId.make(`event:seed-task:${input.threadId}`),
          type: "subagent.updated",
          threadId: input.threadId,
          runId: input.runId,
          nodeId: input.taskId,
          driver,
          providerInstanceId: modelSelection.instanceId,
          occurredAt: input.now,
          payload: {
            id: input.taskId,
            threadId: input.threadId,
            runId: input.runId,
            parentNodeId: input.rootNodeId,
            origin: "app_owned",
            createdBy: "agent",
            driver,
            providerInstanceId: modelSelection.instanceId,
            providerThreadId: null,
            childThreadId: null,
            nativeTaskRef: null,
            prompt: "Inspect the delivered ownership edge.",
            title: null,
            model: null,
            completionWake: input.completionWake ?? "settled_only",
            completionDelivery: {
              state: input.deliveryState,
              observedByRunId: input.deliveryState === "acknowledged" ? input.runId : null,
            },
            status: "completed",
            result: "child finished",
            startedAt: input.now,
            completedAt: input.now,
            updatedAt: input.now,
          },
        },
      ],
    });
  });

it.layer(TestLayer)("delegated completion delivery repairs", (it) => {
  it.effect("acceptance batches pending siblings without acknowledging their results", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("mailbox-batch");
      const runId = RunId.make("mailbox-parent");
      const taskId = NodeId.make("mailbox-first");
      const messageId = MessageId.make(`message:delegated-delivery:${threadId}`);
      yield* seedParentWithTerminalTask({
        threadId,
        runId,
        projectId: ProjectId.make("mailbox-project"),
        rootNodeId: NodeId.make("mailbox-root"),
        taskId,
        deliveryState: "claimed",
        completionWake: "always",
        deliveryTaskIds: [taskId],
        now,
      });
      const projection = yield* orchestrator.getThreadProjection(threadId);
      const task = projection.subagents[0]!;
      const pendingIds = [NodeId.make("mailbox-second"), NodeId.make("mailbox-third")];
      yield* sink.write({
        events: [
          {
            id: EventId.make("mailbox-message"),
            type: "message.updated",
            threadId,
            runId,
            occurredAt: now,
            payload: {
              id: messageId,
              threadId,
              runId,
              nodeId: task.parentNodeId,
              role: "user",
              text: "Background task finished",
              attachments: [],
              streaming: false,
              createdBy: "agent",
              creationSource: "server",
              createdAt: now,
              updatedAt: now,
              delegatedCompletion: { parentRunId: runId, generation: 1, taskIds: [taskId] },
            },
          },
          ...pendingIds.map((id) => ({
            id: EventId.make(`event:${id}`),
            type: "subagent.updated" as const,
            threadId,
            runId,
            nodeId: id,
            occurredAt: now,
            payload: {
              ...task,
              id,
              completionDelivery: { state: "pending" as const, observedByRunId: null },
            },
          })),
        ],
      });
      yield* orchestrator.dispatch({
        type: "notification.delivery.accept",
        commandId: CommandId.make("accept-first"),
        threadId,
        messageId,
      });
      const accepted = yield* orchestrator.getThreadProjection(threadId);
      assert.notEqual(accepted.runs.find((run) => run.id === runId)?.userMessageId, messageId);
      assert.equal(
        accepted.subagents.find((row) => row.id === taskId)?.completionDelivery?.state,
        "delivered",
      );
      const cohort = accepted.runs.find((row) => row.id === runId)?.delegatedCompletion;
      assert.deepEqual(cohort?.delivery?.taskIds, pendingIds);
      assert.equal(cohort?.delivery?.generation, 2);
      for (const id of pendingIds) {
        assert.deepEqual(accepted.subagents.find((row) => row.id === id)?.completionDelivery, {
          state: "claimed",
          observedByRunId: null,
        });
      }
      yield* orchestrator.dispatch({
        type: "notification.delivery.accept",
        commandId: CommandId.make("repeat-old-acceptance"),
        threadId,
        messageId,
      });
      const duplicate = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(duplicate.runs.find((row) => row.id === runId)?.delegatedCompletion, cohort);
    }),
  );

  it.effect("builds completion text and metadata from the same live cohort", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread:delegated-delivery-live-cohort");
      const projectId = ProjectId.make("project:delegated-delivery-live-cohort");
      const runId = RunId.make("run:delegated-delivery-live-cohort");
      const rootNodeId = NodeId.make("node:delegated-delivery-live-cohort-root");
      const firstTaskId = NodeId.make("node:delegated-delivery-live-cohort-first");
      const secondTaskId = NodeId.make("node:delegated-delivery-live-cohort-second");
      const messageId = MessageId.make(`message:delegated-delivery:${threadId}`);

      yield* seedParentWithTerminalTask({
        threadId,
        projectId,
        runId,
        rootNodeId,
        taskId: firstTaskId,
        deliveryState: "claimed",
        completionWake: "always",
        deliveryTaskIds: [firstTaskId, secondTaskId],
        now,
      });

      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("command:delegated-delivery-live-cohort"),
        threadId,
        messageId,
        text: `Delegated task ${firstTaskId} reached a terminal state.`,
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "agent",
        creationSource: "server",
        delegatedCompletion: {
          parentRunId: runId,
          generation: 1,
          taskIds: [firstTaskId],
        },
      });

      const projection = yield* orchestrator.getThreadProjection(threadId);
      const message = projection.messages.find((candidate) => candidate.id === messageId);
      assert.deepEqual(message?.delegatedCompletion?.taskIds, [firstTaskId, secondTaskId]);
      assert.include(message?.text ?? "", String(firstTaskId));
      assert.include(message?.text ?? "", String(secondTaskId));
      assert.include(message?.text ?? "", "task_status");
    }),
  );

  it.effect("does not re-offer when wake-policy upgrades after delivered ownership settled", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread:delegated-delivery-a1");
      const projectId = ProjectId.make("project:delegated-delivery-a1");
      const runId = RunId.make("run:delegated-delivery-a1");
      const rootNodeId = NodeId.make("node:delegated-delivery-a1-root");
      const taskId = NodeId.make("node:delegated-delivery-a1-task");

      yield* seedParentWithTerminalTask({
        threadId,
        projectId,
        runId,
        rootNodeId,
        taskId,
        deliveryState: "delivered",
        completionWake: "settled_only",
        now,
      });

      const upgrade = yield* orchestrator.dispatch({
        type: "delegated_task.wake-policy",
        commandId: CommandId.make("command:delegated-delivery-a1:wake-policy"),
        parentThreadId: threadId,
        taskId,
        completionWake: "always",
      });

      const projection = yield* orchestrator.getThreadProjection(threadId);
      const task = projection.subagents.find((candidate) => candidate.id === taskId);
      const parentRun = projection.runs.find((candidate) => candidate.id === runId);

      assert.equal(task?.completionWake, "always");
      assert.deepEqual(task?.completionDelivery, {
        state: "delivered",
        observedByRunId: null,
      });
      assert.deepEqual(parentRun?.delegatedCompletion, {
        disposition: "open",
        nextGeneration: 2,
        delivery: null,
      });
      assert.isFalse(
        upgrade.storedEvents.some(
          (stored) =>
            stored.event.type === "subagent.updated" &&
            stored.event.payload.id === taskId &&
            stored.event.payload.completionDelivery?.state === "claimed",
        ),
      );
      assert.isFalse(
        upgrade.storedEvents.some(
          (stored) =>
            stored.event.type === "run.updated" &&
            stored.event.payload.id === runId &&
            stored.event.payload.delegatedCompletion?.delivery !== null &&
            stored.event.payload.delegatedCompletion?.delivery !== undefined,
        ),
      );
    }),
  );

  it.effect(
    "treats repeated acknowledge and dispose with distinct command IDs as successful no-ops",
    () =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("thread:delegated-delivery-a2");
        const projectId = ProjectId.make("project:delegated-delivery-a2");
        const runId = RunId.make("run:delegated-delivery-a2");
        const rootNodeId = NodeId.make("node:delegated-delivery-a2-root");
        const taskId = NodeId.make("node:delegated-delivery-a2-task");

        yield* seedParentWithTerminalTask({
          threadId,
          projectId,
          runId,
          rootNodeId,
          taskId,
          deliveryState: "delivered",
          completionWake: "always",
          now,
        });

        // Distinct command IDs mirror task_status vs t3_thread_read racing after
        // their shared read preflight saw delivered ownership.
        const firstAck = yield* orchestrator.dispatch({
          type: "delegated_task.completion-delivery.acknowledge",
          commandId: CommandId.make("command:delegated-delivery-a2:ack-task-status"),
          parentThreadId: threadId,
          taskId,
          observedByRunId: runId,
        });
        const secondAck = yield* orchestrator.dispatch({
          type: "delegated_task.completion-delivery.acknowledge",
          commandId: CommandId.make("command:delegated-delivery-a2:ack-thread-read"),
          parentThreadId: threadId,
          taskId,
          observedByRunId: runId,
        });

        const firstAckTask = firstAck.storedEvents.find(
          (stored) =>
            stored.event.type === "subagent.updated" && stored.event.payload.id === taskId,
        );
        const secondAckTask = secondAck.storedEvents.find(
          (stored) =>
            stored.event.type === "subagent.updated" && stored.event.payload.id === taskId,
        );
        assert.isDefined(firstAckTask);
        assert.isDefined(secondAckTask);
        if (
          firstAckTask?.event.type !== "subagent.updated" ||
          secondAckTask?.event.type !== "subagent.updated"
        ) {
          return yield* Effect.die(new Error("Acknowledge events missing."));
        }
        assert.equal(firstAckTask.event.payload.completionDelivery?.state, "acknowledged");
        assert.equal(secondAckTask.event.payload.completionDelivery?.state, "acknowledged");
        // Idempotent replay keeps the first observation's ownership and timestamp.
        assert.deepEqual(
          secondAckTask.event.payload.completionDelivery,
          firstAckTask.event.payload.completionDelivery,
        );
        assert.deepEqual(
          secondAckTask.event.payload.updatedAt,
          firstAckTask.event.payload.updatedAt,
        );
        assert.equal(secondAck.storedEvents.length, 1);

        const afterAck = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(
          afterAck.subagents.find((candidate) => candidate.id === taskId)?.completionDelivery,
          {
            state: "acknowledged",
            observedByRunId: runId,
          },
        );

        const firstDispose = yield* orchestrator.dispatch({
          type: "delegated_task.completion-delivery.dispose",
          commandId: CommandId.make("command:delegated-delivery-a2:dispose-task-status"),
          parentThreadId: threadId,
          taskId,
        });
        const secondDispose = yield* orchestrator.dispatch({
          type: "delegated_task.completion-delivery.dispose",
          commandId: CommandId.make("command:delegated-delivery-a2:dispose-thread-read"),
          parentThreadId: threadId,
          taskId,
        });

        const firstDisposeTask = firstDispose.storedEvents.find(
          (stored) =>
            stored.event.type === "subagent.updated" && stored.event.payload.id === taskId,
        );
        const secondDisposeTask = secondDispose.storedEvents.find(
          (stored) =>
            stored.event.type === "subagent.updated" && stored.event.payload.id === taskId,
        );
        assert.isDefined(firstDisposeTask);
        assert.isDefined(secondDisposeTask);
        if (
          firstDisposeTask?.event.type !== "subagent.updated" ||
          secondDisposeTask?.event.type !== "subagent.updated"
        ) {
          return yield* Effect.die(new Error("Dispose events missing."));
        }
        assert.equal(firstDisposeTask.event.payload.completionDelivery?.state, "disposed");
        assert.equal(secondDisposeTask.event.payload.completionDelivery?.state, "disposed");
        assert.deepEqual(
          secondDisposeTask.event.payload.completionDelivery,
          firstDisposeTask.event.payload.completionDelivery,
        );
        assert.equal(secondDispose.storedEvents.length, 1);

        const afterDispose = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(
          afterDispose.subagents.find((candidate) => candidate.id === taskId)?.completionDelivery,
          {
            state: "disposed",
            observedByRunId: null,
          },
        );

        const acknowledgeAfterDispose = yield* orchestrator.dispatch({
          type: "delegated_task.completion-delivery.acknowledge",
          commandId: CommandId.make("command:delegated-delivery-a2:ack-after-dispose"),
          parentThreadId: threadId,
          taskId,
          observedByRunId: runId,
        });
        const acknowledgedTask = acknowledgeAfterDispose.storedEvents.find(
          (stored) => stored.event.type === "subagent.updated",
        );
        if (acknowledgedTask?.event.type !== "subagent.updated") {
          return yield* Effect.die(new Error("Acknowledge-after-dispose event missing."));
        }
        assert.deepEqual(acknowledgedTask.event.payload.completionDelivery, {
          state: "disposed",
          observedByRunId: null,
        });
        assert.equal(acknowledgeAfterDispose.storedEvents.length, 1);

        const afterStaleAcknowledge = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(
          afterStaleAcknowledge.subagents.find((candidate) => candidate.id === taskId)
            ?.completionDelivery,
          {
            state: "disposed",
            observedByRunId: null,
          },
        );
      }),
  );

  it.effect("reserves concurrent explicit rearms atomically after a failed reservation write", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      const sql = yield* SqlClient.SqlClient;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("atomic-rearm");
      const runId = RunId.make("atomic-rearm:parent");
      const deliveredId = NodeId.make("atomic-rearm:delivered");
      const siblingIds = [NodeId.make("atomic-rearm:third"), NodeId.make("atomic-rearm:second")];
      yield* seedParentWithTerminalTask({
        threadId,
        runId,
        taskId: deliveredId,
        now,
        projectId: ProjectId.make("atomic-rearm:project"),
        rootNodeId: NodeId.make("atomic-rearm:root"),
        completionWake: "always",
        deliveryState: "delivered",
        parentStatus: "completed",
      });
      const seed = yield* orchestrator.getThreadProjection(threadId);
      yield* sink.write({
        events: [
          ...siblingIds,
          ...["one", "two", "three"].map((name) => NodeId.make(`atomic-rearm:control:${name}`)),
        ].map((id) => ({
          id: EventId.make(`seed:${id}`),
          type: "subagent.updated" as const,
          threadId,
          runId,
          occurredAt: now,
          payload: {
            ...seed.subagents[0]!,
            id,
            completionDelivery: {
              state: siblingIds.includes(id) ? ("pending" as const) : ("delivered" as const),
              observedByRunId: null,
            },
          },
        })),
      });
      const rearm = (id: string, taskId = deliveredId) =>
        orchestrator.dispatch({
          type: "delegated_task.wake-policy",
          commandId: CommandId.make(id),
          parentThreadId: threadId,
          taskId,
          completionWake: "settled_only",
        });
      yield* sql.unsafe(
        "CREATE TEMP TRIGGER fail_reservation BEFORE UPDATE ON orchestration_v2_projection_runs WHEN NEW.run_id = 'atomic-rearm:parent' BEGIN SELECT RAISE(ABORT, 'simulated reservation write failure'); END",
      );
      const beforeSequence = yield* sink.latestSequence();
      yield* rearm("atomic-rearm:failed").pipe(Effect.flip);
      assert.equal(yield* sink.latestSequence(), beforeSequence);
      const failed = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(
        failed.subagents.find((task) => task.id === deliveredId)?.completionWake,
        "always",
      );
      assert.equal(failed.runs[0]?.delegatedCompletion?.delivery, null);
      assert.equal(failed.runs[0]?.delegatedCompletion?.nextGeneration, 2);
      for (const id of siblingIds)
        assert.equal(
          failed.subagents.find((task) => task.id === id)?.completionDelivery?.state,
          "pending",
        );
      yield* sql.unsafe("DROP TRIGGER fail_reservation");
      const afterSequence = yield* sink.latestSequence();
      // Retry the same command after rollback, not an unrelated policy toggle.
      yield* rearm("atomic-rearm:failed");
      yield* Effect.all(
        ["one", "two", "three"].map((name) =>
          rearm(`atomic-rearm:${name}`, NodeId.make(`atomic-rearm:control:${name}`)),
        ),
        { concurrency: "unbounded" },
      );
      const reserved = yield* orchestrator.getThreadProjection(threadId);
      const cohort = reserved.runs[0]!.delegatedCompletion!;
      assert.equal(cohort.nextGeneration, 3);
      assert.deepEqual(cohort.delivery?.taskIds, siblingIds.toSorted());
      assert.equal(
        reserved.subagents.find((task) => task.id === deliveredId)?.completionDelivery?.state,
        "delivered",
      );
      for (const id of siblingIds)
        assert.equal(
          reserved.subagents.find((task) => task.id === id)?.completionDelivery?.state,
          "claimed",
        );
      const throughSequence = yield* sink.latestSequence();
      const events = yield* sink.stream({ afterSequence }).pipe(
        Stream.takeUntil((stored) => stored.sequence >= throughSequence),
        Stream.runCollect,
      );
      assert.equal(
        events.filter(
          (stored) =>
            stored.event.type === "run.updated" &&
            stored.event.payload.id === runId &&
            stored.event.payload.delegatedCompletion?.delivery != null,
        ).length,
        1,
      );
      const rebuilt = yield* Orchestrator.OrchestratorV2.pipe(
        Effect.provide(Layer.fresh(makeTestLayer(Layer.succeed(SqlClient.SqlClient, sql)))),
      );
      const recovered = yield* rebuilt.getThreadProjection(threadId);
      assert.deepEqual(recovered.runs[0]?.delegatedCompletion, cohort);
      for (const id of siblingIds)
        assert.equal(
          recovered.subagents.find((task) => task.id === id)?.completionDelivery?.state,
          "claimed",
        );
    }).pipe(Effect.provide(Layer.fresh(TestLayer))),
  );

  it.effect.each([
    "policy change",
    "recovery",
    "accept before finalization",
    "accept interrupted before finalization",
    "settlement",
    "user cancellation",
  ] as const)("handles cancelled delivery ownership via %s", (trigger) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make(`thread:delegated-delivery-cancel-replan:${trigger}`);
      const projectId = ProjectId.make("project:delegated-delivery-cancel-replan");
      const parentRunId = RunId.make("run:delegated-delivery-cancel-replan:parent");
      const deliveryRunId = RunId.make("run:delegated-delivery-cancel-replan:delivery");
      const parentRootNodeId = NodeId.make("node:delegated-delivery-cancel-replan:parent-root");
      const deliveryRootNodeId = NodeId.make("node:delegated-delivery-cancel-replan:delivery-root");
      const taskId = NodeId.make("node:delegated-delivery-cancel-replan:task");
      const messageId = MessageId.make(`message:delegated-delivery:${threadId}`);

      yield* seedParentWithTerminalTask({
        threadId,
        projectId,
        runId: parentRunId,
        rootNodeId: parentRootNodeId,
        taskId,
        deliveryState: "claimed",
        completionWake: "always",
        deliveryTaskIds: [taskId],
        parentStatus: "completed",
        now,
      });
      const finalizerEntered = yield* Deferred.make<void>();
      const releaseFinalizer = yield* Deferred.make<void>();
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const getThread = store.getThread;
      const acceptBeforeFinalization =
        trigger === "accept before finalization" ||
        trigger === "accept interrupted before finalization";
      const terminalStatus =
        trigger === "accept interrupted before finalization" ? "interrupted" : "cancelled";
      const queuedCancellation = trigger === "settlement" || trigger === "user cancellation";
      if (acceptBeforeFinalization) {
        let held = false;
        Object.assign(store, {
          getThread: (id: ThreadId) =>
            Effect.gen(function* () {
              const thread = yield* getThread(id);
              if (id === threadId && !held) {
                const state = yield* store.getThreadProjection(id);
                if (
                  state.runs.some(
                    (run) => run.id === deliveryRunId && run.status === terminalStatus,
                  )
                ) {
                  held = true;
                  yield* Deferred.succeed(finalizerEntered, undefined);
                  yield* Deferred.await(releaseFinalizer);
                }
              }
              return thread;
            }),
        });
      }
      const beforeCancel = yield* eventSink.latestSequence();
      yield* eventSink.write({
        commandId: CommandId.make("command:delegated-delivery-cancel-replan"),
        events: [
          {
            id: EventId.make("event:delegated-delivery-cancel-replan:message"),
            type: "message.updated",
            threadId,
            runId: deliveryRunId,
            nodeId: deliveryRootNodeId,
            providerInstanceId: modelSelection.instanceId,
            occurredAt: now,
            payload: {
              createdBy: "agent",
              creationSource: "server",
              id: messageId,
              threadId,
              runId: deliveryRunId,
              nodeId: deliveryRootNodeId,
              role: "user",
              text: `Delegated task ${taskId} reached a terminal state.`,
              attachments: [],
              streaming: false,
              createdAt: now,
              updatedAt: now,
              delegatedCompletion: {
                parentRunId,
                generation: 1,
                taskIds: [taskId],
              },
            },
          },
          {
            id: EventId.make("event:delegated-delivery-cancel-replan:run"),
            type: "run.updated",
            threadId,
            runId: deliveryRunId,
            nodeId: deliveryRootNodeId,
            providerInstanceId: modelSelection.instanceId,
            occurredAt: now,
            payload: {
              id: deliveryRunId,
              threadId,
              ordinal: 2,
              providerInstanceId: modelSelection.instanceId,
              modelSelection,
              providerThreadId: ProviderThreadId.make(
                `provider-thread:${String(threadId).replace("thread:", "")}`,
              ),
              userMessageId: messageId,
              rootNodeId: deliveryRootNodeId,
              activeAttemptId: null,
              status: queuedCancellation ? "queued" : terminalStatus,
              queueHeld: queuedCancellation,
              requestedAt: now,
              startedAt: queuedCancellation ? null : now,
              completedAt: queuedCancellation ? null : now,
              checkpointId: null,
              contextHandoffId: null,
            },
          },
        ],
      });

      if (queuedCancellation) {
        if (trigger === "user cancellation") {
          yield* orchestrator.dispatch({
            type: "queued-run.cancel",
            commandId: CommandId.make("cancel-replan:user-cancel"),
            threadId,
            runId: deliveryRunId,
          });
        } else {
          yield* orchestrator.dispatch({
            type: "thread.settle",
            commandId: CommandId.make("cancel-replan:settle"),
            threadId,
          });
        }
        const settled = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(settled.runs.find((run) => run.id === deliveryRunId)?.status, "cancelled");
        assert.equal(
          settled.subagents.find((task) => task.id === taskId)?.completionDelivery?.state,
          "disposed",
        );
        assert.equal(
          settled.runs.find((run) => run.id === parentRunId)?.delegatedCompletion?.disposition,
          "disposed",
        );
        yield* orchestrator.dispatch({
          type: "delegated_task.wake-policy",
          commandId: CommandId.make("cancel-replan:policy-after-settle"),
          parentThreadId: threadId,
          taskId,
          completionWake: "settled_only",
        });
        yield* orchestrator.dispatch({
          type: "delegated_task.wake-policy",
          commandId: CommandId.make("cancel-replan:automatic-upgrade"),
          parentThreadId: threadId,
          taskId,
          completionWake: "always",
        });
        const afterPolicy = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(
          afterPolicy.runs.find((run) => run.id === parentRunId)?.delegatedCompletion?.delivery,
          null,
        );
        assert.equal(
          afterPolicy.subagents.find((task) => task.id === taskId)?.completionDelivery?.state,
          "disposed",
        );
        return;
      }
      if (acceptBeforeFinalization) {
        yield* Effect.gen(function* () {
          yield* Deferred.await(finalizerEntered);
          const durable = yield* store.getThreadProjection(threadId);
          assert.equal(
            durable.runs.find((run) => run.id === deliveryRunId)?.status,
            terminalStatus,
          );
          assert.equal(
            durable.runs.find((run) => run.id === parentRunId)?.delegatedCompletion?.delivery
              ?.messageId,
            messageId,
          );
          const task = durable.subagents.find((row) => row.id === taskId)!;
          const siblingId = NodeId.make("cancel-pre-finalize:pending-sibling");
          yield* eventSink.write({
            events: [
              {
                id: EventId.make("cancel-pre-finalize:pending-sibling"),
                type: "subagent.updated",
                threadId,
                runId: parentRunId,
                occurredAt: now,
                payload: {
                  ...task,
                  id: siblingId,
                  completionDelivery: { state: "pending", observedByRunId: null },
                },
              },
            ],
          });
          yield* orchestrator.dispatch({
            type: "notification.delivery.accept",
            commandId: CommandId.make("cancel-pre-finalize:accept"),
            threadId,
            messageId,
          });
          const after = yield* store.getThreadProjection(threadId);
          assert.equal(
            after.subagents.find((row) => row.id === taskId)?.completionDelivery?.state,
            "claimed",
          );
          assert.equal(
            after.subagents.find((row) => row.id === siblingId)?.completionDelivery?.state,
            "pending",
          );
          assert.deepEqual(
            after.runs.find((run) => run.id === parentRunId)?.delegatedCompletion,
            durable.runs.find((run) => run.id === parentRunId)?.delegatedCompletion,
          );
        }).pipe(
          Effect.ensuring(Deferred.succeed(releaseFinalizer, undefined)),
          Effect.ensuring(Effect.sync(() => Object.assign(store, { getThread }))),
        );
      }
      if (acceptBeforeFinalization) return;
      // Wait on the durable cohort-finalization event, not scheduler timing.
      yield* eventSink.stream({ afterSequence: beforeCancel, eventType: "run.updated" }).pipe(
        Stream.filter(
          (stored) =>
            stored.event.type === "run.updated" && stored.event.payload.id === parentRunId,
        ),
        Stream.take(1),
        Stream.runDrain,
      );
      const afterCancel = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(
        afterCancel.subagents.find((candidate) => candidate.id === taskId)?.completionDelivery
          ?.state,
        "pending",
      );
      assert.equal(
        afterCancel.runs.find((candidate) => candidate.id === parentRunId)?.delegatedCompletion
          ?.delivery,
        null,
      );

      yield* orchestrator.dispatch({
        type: "notification.delivery.accept",
        commandId: CommandId.make("command:cancel-replan:late-acceptance"),
        threadId,
        messageId,
      });
      const afterLateAcceptance = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(
        afterLateAcceptance.runs.find((run) => run.id === parentRunId)?.delegatedCompletion
          ?.delivery,
        null,
      );
      assert.equal(
        afterLateAcceptance.subagents.find((task) => task.id === taskId)?.completionDelivery?.state,
        "pending",
      );
      assert.notInclude(yield* store.getRecoveryThreadIds("delegated-completions"), threadId);
      if (trigger === "policy change") {
        yield* orchestrator.dispatch({
          type: "delegated_task.wake-policy",
          commandId: CommandId.make("command:delegated-delivery-cancel-replan:wake-policy"),
          parentThreadId: threadId,
          taskId,
          completionWake: "settled_only",
        });
      } else {
        // Rebuild over the same database: cancellation must survive ordinary restart.
        const sql = yield* SqlClient.SqlClient;
        const otherRunId = RunId.make("cancel-replan:other-parent");
        const otherTaskId = NodeId.make("cancel-replan:other-task");
        const cancelledProjection = yield* orchestrator.getThreadProjection(threadId);
        yield* eventSink.write({
          events: [
            {
              id: EventId.make("cancel-replan:other-run"),
              type: "run.updated",
              threadId,
              runId: otherRunId,
              occurredAt: now,
              payload: {
                ...cancelledProjection.runs.find((run) => run.id === parentRunId)!,
                id: otherRunId,
                ordinal: 3,
                delegatedCompletion: {
                  disposition: "open",
                  nextGeneration: 2,
                  delivery: {
                    generation: 1,
                    messageId: MessageId.make("cancel-replan:other-message"),
                    taskIds: [otherTaskId],
                  },
                },
              },
            },
            {
              id: EventId.make("cancel-replan:other-task"),
              type: "subagent.updated",
              threadId,
              runId: otherRunId,
              occurredAt: now,
              payload: {
                ...cancelledProjection.subagents.find((task) => task.id === taskId)!,
                id: otherTaskId,
                runId: otherRunId,
                completionDelivery: { state: "claimed", observedByRunId: null },
              },
            },
          ],
        });
        assert.include(yield* store.getRecoveryThreadIds("delegated-completions"), threadId);
        yield* Orchestrator.OrchestratorV2.pipe(
          Effect.provide(Layer.fresh(makeTestLayer(Layer.succeed(SqlClient.SqlClient, sql)))),
        );
        const afterRestart = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(
          afterRestart.runs.find((run) => run.id === parentRunId)?.delegatedCompletion?.delivery,
          null,
        );
        assert.equal(
          afterRestart.subagents.find((task) => task.id === taskId)?.completionDelivery?.state,
          "pending",
        );
        yield* orchestrator.dispatch({
          type: "delegated_task.wake-policy",
          commandId: CommandId.make("cancel-replan:explicit-after-restart"),
          parentThreadId: threadId,
          taskId,
          completionWake: "settled_only",
        });
      }

      const afterReplan = yield* orchestrator.getThreadProjection(threadId);
      const parentRun = afterReplan.runs.find((candidate) => candidate.id === parentRunId);
      const task = afterReplan.subagents.find((candidate) => candidate.id === taskId);
      assert.equal(task?.completionDelivery?.state, "claimed");
      assert.isNotNull(parentRun?.delegatedCompletion?.delivery ?? null);
      assert.deepEqual(parentRun?.delegatedCompletion?.delivery?.taskIds, [taskId]);
    }).pipe(Effect.provide(Layer.fresh(TestLayer))),
  );
  it.effect("does not automatically re-reserve a cancelled delivery", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make(`thread:delegated-delivery-cancel-replan:standalone`);
      const projectId = ProjectId.make("project:delegated-delivery-cancel-replan");
      const parentRunId = RunId.make("run:delegated-delivery-cancel-replan:parent");
      const deliveryRunId = RunId.make("run:delegated-delivery-cancel-replan:delivery");
      const parentRootNodeId = NodeId.make("node:delegated-delivery-cancel-replan:parent-root");
      const deliveryRootNodeId = NodeId.make("node:delegated-delivery-cancel-replan:delivery-root");
      const taskId = NodeId.make("node:delegated-delivery-cancel-replan:task");
      const messageId = MessageId.make(`message:delegated-delivery:${threadId}`);

      yield* seedParentWithTerminalTask({
        threadId,
        projectId,
        runId: parentRunId,
        rootNodeId: parentRootNodeId,
        taskId,
        deliveryState: "claimed",
        completionWake: "always",
        deliveryTaskIds: [taskId],
        parentStatus: "completed",
        now,
      });
      const beforeCancel = yield* eventSink.latestSequence();
      yield* eventSink.write({
        commandId: CommandId.make("command:delegated-delivery-cancel-replan"),
        events: [
          {
            id: EventId.make("event:delegated-delivery-cancel-replan:message"),
            type: "message.updated",
            threadId,
            runId: deliveryRunId,
            nodeId: deliveryRootNodeId,
            providerInstanceId: modelSelection.instanceId,
            occurredAt: now,
            payload: {
              createdBy: "agent",
              creationSource: "server",
              id: messageId,
              threadId,
              runId: deliveryRunId,
              nodeId: deliveryRootNodeId,
              role: "user",
              text: `Delegated task ${taskId} reached a terminal state.`,
              attachments: [],
              streaming: false,
              createdAt: now,
              updatedAt: now,
              delegatedCompletion: {
                parentRunId,
                generation: 1,
                taskIds: [taskId],
              },
            },
          },
          {
            id: EventId.make("event:delegated-delivery-cancel-replan:run"),
            type: "run.updated",
            threadId,
            runId: deliveryRunId,
            nodeId: deliveryRootNodeId,
            providerInstanceId: modelSelection.instanceId,
            occurredAt: now,
            payload: {
              id: deliveryRunId,
              threadId,
              ordinal: 2,
              providerInstanceId: modelSelection.instanceId,
              modelSelection,
              providerThreadId: ProviderThreadId.make(
                `provider-thread:${String(threadId).replace("thread:", "")}`,
              ),
              userMessageId: messageId,
              rootNodeId: deliveryRootNodeId,
              activeAttemptId: null,
              status: "cancelled",
              queueHeld: false,
              requestedAt: now,
              startedAt: now,
              completedAt: now,
              checkpointId: null,
              contextHandoffId: null,
            },
          },
        ],
      });

      // Wait on the durable cohort-finalization event, not scheduler timing.
      yield* eventSink.stream({ afterSequence: beforeCancel, eventType: "run.updated" }).pipe(
        Stream.filter(
          (stored) =>
            stored.event.type === "run.updated" && stored.event.payload.id === parentRunId,
        ),
        Stream.take(1),
        Stream.runDrain,
      );
      const afterCancel = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(
        afterCancel.subagents.find((candidate) => candidate.id === taskId)?.completionDelivery
          ?.state,
        "pending",
      );
      assert.equal(
        afterCancel.runs.find((candidate) => candidate.id === parentRunId)?.delegatedCompletion
          ?.delivery,
        null,
      );
    }).pipe(Effect.provide(Layer.fresh(TestLayer))),
  );

  it.effect("overlapping explicit rearms keep policy and reservation ownership", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("review-lock-probe");
      const runId = RunId.make("review-lock-probe:parent");
      const deliveredId = NodeId.make("review-lock-probe:delivered");
      const siblingX = NodeId.make("review-lock-probe:x");
      const siblingY = NodeId.make("review-lock-probe:y");
      const controlId = NodeId.make("review-lock-probe:control");
      yield* seedParentWithTerminalTask({
        threadId,
        runId,
        taskId: deliveredId,
        now,
        projectId: ProjectId.make("review-lock-probe:project"),
        rootNodeId: NodeId.make("review-lock-probe:root"),
        completionWake: "always",
        deliveryState: "delivered",
        parentStatus: "completed",
      });
      const seed = yield* orchestrator.getThreadProjection(threadId);
      yield* sink.write({
        events: [siblingX, siblingY, controlId].map((id) => ({
          id: EventId.make(`review-lock-probe:seed:${id}`),
          type: "subagent.updated" as const,
          threadId,
          runId,
          occurredAt: now,
          payload: {
            ...seed.subagents[0]!,
            id,
            completionWake: "always" as const,
            completionDelivery: {
              state: id === controlId ? ("delivered" as const) : ("pending" as const),
              observedByRunId: null,
            },
          },
        })),
      });
      // Pause after the first snapshot, before reservation planning finishes.
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const getMessageCount = store.getMessageCount;
      let calls = 0;
      Object.assign(store, {
        getMessageCount: (id: ThreadId) =>
          Effect.gen(function* () {
            if (id === threadId && calls++ === 0) {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
            }
            return yield* getMessageCount(id);
          }),
      });
      const afterSequence = yield* sink.latestSequence();
      const first = yield* orchestrator
        .dispatch({
          type: "delegated_task.wake-policy",
          commandId: CommandId.make("review-lock-probe:first"),
          parentThreadId: threadId,
          taskId: controlId,
          completionWake: "settled_only",
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      const second = yield* orchestrator
        .dispatch({
          type: "delegated_task.wake-policy",
          commandId: CommandId.make("review-lock-probe:second"),
          parentThreadId: threadId,
          taskId: siblingX,
          completionWake: "settled_only",
        })
        .pipe(Effect.forkChild);
      yield* TestClock.adjust(0);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(first);
      yield* Fiber.join(second);
      Object.assign(store, { getMessageCount });
      const throughSequence = yield* sink.latestSequence();
      const events = yield* sink.stream({ afterSequence }).pipe(
        Stream.takeUntil((stored) => stored.sequence >= throughSequence),
        Stream.runCollect,
      );
      const reservations = Array.from(events).flatMap((stored) =>
        stored.event.type === "run.updated" &&
        stored.event.payload.id === runId &&
        stored.event.payload.delegatedCompletion?.delivery != null
          ? [stored.event.payload.delegatedCompletion.delivery]
          : [],
      );
      const after = yield* orchestrator.getThreadProjection(threadId);
      const x = after.subagents.find((task) => task.id === siblingX);
      const cohort = after.runs.find((run) => run.id === runId)?.delegatedCompletion;
      assert.equal(x?.completionWake, "settled_only", "second rearm's committed policy was lost");
      assert.equal(reservations.length, 1, "more than one reservation was written");
      assert.deepEqual(cohort?.delivery?.taskIds, [siblingX, siblingY].toSorted());
    }).pipe(Effect.provide(Layer.fresh(TestLayer))),
  );

  it.effect.each(["in-flight delivery", "live parent", "disposed cohort"] as const)(
    "explicit rearm preserves the %s",
    (guard) =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const sink = yield* EventSink.EventSinkV2;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make(`rearm-guard:${guard}`);
        const runId = RunId.make(`${threadId}:parent`);
        const taskId = NodeId.make(`${threadId}:claimed`);
        const siblingId = NodeId.make(`${threadId}:pending`);
        const controlId = NodeId.make(`${threadId}:control`);
        yield* seedParentWithTerminalTask({
          threadId,
          runId,
          taskId,
          now,
          projectId: ProjectId.make(`${threadId}:project`),
          rootNodeId: NodeId.make(`${threadId}:root`),
          parentStatus: guard === "live parent" ? "running" : "completed",
          completionWake: "always",
          deliveryState: guard === "in-flight delivery" ? "claimed" : "delivered",
          ...(guard === "in-flight delivery" ? { deliveryTaskIds: [taskId] } : {}),
        });
        const seed = yield* orchestrator.getThreadProjection(threadId);
        const deliveryRun = { ...seed.runs[0]! };
        delete deliveryRun.delegatedCompletion;
        yield* sink.write({
          events: [
            ...(guard === "in-flight delivery"
              ? [
                  {
                    id: EventId.make(`${threadId}:in-flight`),
                    type: "run.updated" as const,
                    threadId,
                    runId: RunId.make(`${threadId}:delivery`),
                    occurredAt: now,
                    payload: {
                      ...deliveryRun,
                      id: RunId.make(`${threadId}:delivery`),
                      ordinal: 2,
                      status: "running" as const,
                      userMessageId: seed.runs[0]!.delegatedCompletion!.delivery!.messageId,
                    },
                  },
                ]
              : []),
            ...[siblingId, controlId].map((id) => ({
              id: EventId.make(`seed:${id}`),
              type: "subagent.updated" as const,
              threadId,
              runId,
              occurredAt: now,
              payload: {
                ...seed.subagents[0]!,
                id,
                completionWake:
                  id === siblingId && guard === "live parent"
                    ? ("settled_only" as const)
                    : ("always" as const),
                completionDelivery: {
                  state: id === siblingId ? ("pending" as const) : ("delivered" as const),
                  observedByRunId: null,
                },
              },
            })),
            ...(guard === "disposed cohort"
              ? [
                  {
                    id: EventId.make(`${threadId}:dispose`),
                    type: "run.updated" as const,
                    threadId,
                    runId,
                    occurredAt: now,
                    payload: {
                      ...seed.runs[0]!,
                      delegatedCompletion: {
                        ...seed.runs[0]!.delegatedCompletion!,
                        disposition: "disposed" as const,
                      },
                    },
                  },
                ]
              : []),
          ],
        });
        const before = yield* orchestrator.getThreadProjection(threadId);
        yield* orchestrator.dispatch({
          type: "delegated_task.wake-policy",
          commandId: CommandId.make(`${threadId}:rearm`),
          parentThreadId: threadId,
          taskId: controlId,
          completionWake: "settled_only",
        });
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(
          after.runs.find((run) => run.id === runId)?.delegatedCompletion,
          before.runs.find((run) => run.id === runId)?.delegatedCompletion,
        );
        assert.equal(
          after.subagents.find((task) => task.id === siblingId)?.completionDelivery?.state,
          "pending",
        );
        assert.deepEqual(
          after.subagents.find((task) => task.id === taskId)?.completionDelivery,
          before.subagents.find((task) => task.id === taskId)?.completionDelivery,
        );
      }).pipe(Effect.provide(Layer.fresh(TestLayer))),
  );
});
