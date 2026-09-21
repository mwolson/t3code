import { SourceControlProviderRegistry } from "../sourceControl/SourceControlProviderRegistry.ts";
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
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import { ServerConfig } from "../config.ts";
import { layer as mcpSessionRegistryTestLayer } from "../mcp/McpSessionRegistry.testkit.ts";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import { OrchestrationLayerLive } from "../orchestration/runtimeLayer.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { ProjectEnrichmentService } from "../project/ProjectEnrichmentService.ts";
import type { ProviderInstance } from "../provider/ProviderDriver.ts";
import { ProviderInstanceRegistry } from "../provider/Services/ProviderInstanceRegistry.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import { EventSinkV2 } from "./EventSink.ts";
import { ProjectionStoreV2, layer as projectionStoreLayer } from "./ProjectionStore.ts";
import { OrchestratorV2 } from "./Orchestrator.ts";
import type { ProviderAdapterV2Shape } from "./ProviderAdapter.ts";
import { OrchestrationV2EventSinkLayerLive, OrchestrationV2LayerLive } from "./runtimeLayer.ts";
import { worktreeRepairDependenciesTestLayer } from "./ProviderTurnStartService.testkit.ts";

const PlatformTestLayer = Layer.merge(
  NodeServices.layer,
  Layer.mock(SourceControlProviderRegistry)({ resolveLink: () => Effect.die("unused title link") }),
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
  snapshot: {} as ProviderInstance["snapshot"],
  orchestrationAdapter,
  textGeneration: {} as ProviderInstance["textGeneration"],
} satisfies ProviderInstance;

const TestProviderInstanceRegistry = Layer.succeed(ProviderInstanceRegistry, {
  getInstance: (instanceId) =>
    Effect.succeed(instanceId === providerInstance.instanceId ? providerInstance : undefined),
  listInstances: Effect.succeed([providerInstance]),
  listUnavailable: Effect.succeed([]),
  streamChanges: Stream.empty,
  subscribeChanges: Effect.never,
});

const makeTestLayer = <E, R>(database: Layer.Layer<SqlClient.SqlClient, E, R>) =>
  Layer.mergeAll(
    OrchestrationLayerLive,
    OrchestrationV2LayerLive,
    OrchestrationV2EventSinkLayerLive,
    projectionStoreLayer,
  ).pipe(
    Layer.provide(worktreeRepairDependenciesTestLayer),
    Layer.provide(
      Layer.succeed(ProjectEnrichmentService, {
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
    Layer.provide(mcpSessionRegistryTestLayer),
    Layer.provideMerge(database),
    Layer.provide(CheckpointStoreTestLayer),
    Layer.provide(ServerConfigLayer),
    Layer.provide(ServerSettingsService.layerTest()),
    Layer.provide(TestProviderInstanceRegistry),
    Layer.provide(PlatformTestLayer),
  );

const TestLayer = makeTestLayer(SqlitePersistenceMemory);

const seedParentWithTerminalTask = (input: {
  readonly threadId: ThreadId;
  readonly projectId: ProjectId;
  readonly runId: RunId;
  readonly rootNodeId: NodeId;
  readonly taskId: NodeId;
  readonly deliveryState: "pending" | "delivered" | "claimed" | "acknowledged" | "disposed";
  readonly completionWake?: "always" | "settled_only";
  readonly settledDeliveryCount?: number;
  readonly parentStatus?: "running" | "completed";
  readonly deliveryTaskIds?: ReadonlyArray<NodeId>;
  readonly now: DateTime.Utc;
}) =>
  Effect.gen(function* () {
    const applicationEngine = yield* OrchestrationEngineService;
    const orchestrator = yield* OrchestratorV2;
    const eventSink = yield* EventSinkV2;
    const providerThreadId = ProviderThreadId.make(
      `provider-thread:${String(input.threadId).replace("thread:", "")}`,
    );

    yield* applicationEngine.dispatch({
      type: "project.create",
      commandId: CommandId.make(`command:seed-project:${input.threadId}`),
      projectId: input.projectId,
      title: "Delegated completion delivery",
      workspaceRoot: `/workspace/${input.projectId}`,
      defaultModelSelection: modelSelection,
      scripts: [],
      createdAt: DateTime.formatIso(input.now),
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
              settledDeliveryCount: input.settledDeliveryCount ?? 1,
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
  it.effect(
    "coalesces only queued Codex command notifications and preserves promotion boundaries",
    () =>
      Effect.gen(function* () {
        const orchestrator = yield* OrchestratorV2;
        const sink = yield* EventSinkV2;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("codex-command-coalescing");
        const parentRunId = RunId.make("codex-command-coalescing:parent");
        yield* seedParentWithTerminalTask({
          threadId,
          projectId: ProjectId.make("codex-command-coalescing:project"),
          runId: parentRunId,
          rootNodeId: NodeId.make("codex-command-coalescing:root"),
          taskId: NodeId.make("codex-command-coalescing:task"),
          deliveryState: "delivered",
          now,
        });
        const sendCommand = (label: string) =>
          orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(`command:coalesce:${label}`),
            threadId,
            messageId: MessageId.make(`message:coalesce:${label}`),
            text: `Background command completed: ${label}\nOutput tail: ${label}_OUTPUT`,
            notification: {
              source: { kind: "background_command" },
              outcome: "completed",
              summary: "Background command finished",
              detail: label,
            },
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            createdBy: "agent",
            creationSource: "provider",
          });
        yield* sendCommand("A");
        // Ordinary user/peer prompts and native task completions remain independent.
        for (const kind of ["user", "peer", "native-task"] as const) {
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(`command:coalesce:${kind}`),
            threadId,
            messageId: MessageId.make(`message:coalesce:${kind}`),
            text: kind,
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            createdBy: kind === "user" ? "user" : "agent",
            creationSource: kind === "user" ? "web" : "provider",
            ...(kind === "native-task"
              ? {
                  notification: {
                    source: { kind: "background_task" as const },
                    outcome: "completed" as const,
                    summary: "Background task finished",
                  },
                }
              : {}),
          });
        }
        yield* sendCommand("B");
        // Replaying the second command receipt must not append its output twice.
        yield* sendCommand("B");
        const queued = yield* orchestrator.getThreadProjection(threadId);
        const first = queued.messages.find((message) => message.id === "message:coalesce:A");
        assert.isDefined(first);
        assert.include(first?.text ?? "", "A_OUTPUT");
        assert.include(first?.text ?? "", "B_OUTPUT");
        assert.equal(first?.text.split("B_OUTPUT").length, 2);
        assert.equal(first?.notification?.detail, "A\n\nB");
        assert.equal(first?.notification?.summary, "Background commands finished");
        assert.lengthOf(
          queued.runs.filter((run) => run.status === "queued"),
          4,
        );
        assert.isFalse(queued.messages.some((message) => message.id === "message:coalesce:B"));
        for (const kind of ["user", "peer", "native-task"]) {
          assert.equal(
            queued.messages.find((message) => message.id === `message:coalesce:${kind}`)?.text,
            kind,
          );
        }
        const firstRun = queued.runs.find((run) => run.userMessageId === first?.id);
        if (firstRun === undefined) return yield* Effect.die("Missing queued command run");
        const edit = yield* Effect.result(
          orchestrator.dispatch({
            type: "queued-run.edit",
            commandId: CommandId.make("command:coalesce:public-edit"),
            threadId,
            runId: firstRun.id,
            text: "must not replace outputs",
          }),
        );
        assert.equal(edit._tag, "Failure");

        const parent = queued.runs.find((run) => run.id === parentRunId);
        if (parent === undefined) return yield* Effect.die("Missing parent run");
        const afterSequence = yield* sink.latestSequence();
        yield* sink.write({
          events: [
            {
              id: EventId.make("event:coalesce:parent-complete"),
              type: "run.updated",
              threadId,
              runId: parentRunId,
              occurredAt: now,
              payload: { ...parent, status: "completed", completedAt: now },
            },
          ],
        });
        yield* sink.stream({ afterSequence, eventType: "run.updated" }).pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "run.updated" &&
              stored.event.payload.id === firstRun.id &&
              stored.event.payload.status === "starting",
          ),
          Stream.take(1),
          Stream.runDrain,
        );
        // The first batch already promoted under the same lock used by dispatch.
        // This later completion must survive as a separate queued delivery.
        yield* sendCommand("C");
        const promoted = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(
          promoted.messages.find((message) => message.id === first?.id)?.text,
          first?.text,
        );
        assert.isTrue(
          promoted.runs.some(
            (run) => run.status === "queued" && run.userMessageId === "message:coalesce:C",
          ),
        );
        const activity = promoted.turnItems.find(
          (item) => item.runId === firstRun.id && item.type === "notification",
        );
        if (activity?.type !== "notification")
          return yield* Effect.die("Missing typed notification activity");
        assert.equal(activity.summary, "Background commands finished");
        assert.equal(activity.detail, "A\n\nB");
      }),
  );
  it.effect("plans an idle sibling after more than two settled deliveries", () =>
    Effect.gen(function* () {
      const orchestrator = yield* OrchestratorV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("idle-sibling-after-cap");
      const runId = RunId.make("idle-sibling-after-cap:parent");
      const taskId = NodeId.make("idle-sibling-after-cap:task");
      yield* seedParentWithTerminalTask({
        threadId,
        runId,
        taskId,
        now,
        projectId: ProjectId.make("idle-sibling-after-cap:project"),
        rootNodeId: NodeId.make("idle-sibling-after-cap:root"),
        deliveryState: "pending",
        settledDeliveryCount: 3,
        parentStatus: "completed",
      });
      yield* orchestrator.dispatch({
        type: "delegated_task.wake-policy",
        commandId: CommandId.make("idle-sibling-after-cap:policy"),
        parentThreadId: threadId,
        taskId,
        completionWake: "always",
      });
      const projection = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(
        projection.runs.find((run) => run.id === runId)?.delegatedCompletion?.delivery?.taskIds,
        [taskId],
      );
      assert.equal(
        projection.subagents.find((task) => task.id === taskId)?.completionDelivery?.state,
        "claimed",
      );
    }),
  );

  it.effect("acceptance batches pending siblings without acknowledging their results", () =>
    Effect.gen(function* () {
      const orchestrator = yield* OrchestratorV2;
      const sink = yield* EventSinkV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("mailbox-batch");
      const runId = RunId.make("mailbox-parent");
      const taskId = NodeId.make("mailbox-first");
      const messageId = MessageId.make(`message:delegated-delivery:${threadId}`);
      yield* seedParentWithTerminalTask({
        threadId,
        runId,
        projectId: ProjectId.make("mailbox-project"),
        settledDeliveryCount: 3,
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
      assert.equal(
        accepted.subagents.find((row) => row.id === taskId)?.completionDelivery?.state,
        "delivered",
      );
      const cohort = accepted.runs.find((row) => row.id === runId)?.delegatedCompletion;
      assert.deepEqual(cohort?.delivery?.taskIds, pendingIds);
      assert.equal(cohort?.delivery?.generation, 2);
      assert.equal(
        cohort?.settledDeliveryCount,
        projection.runs.find((row) => row.id === runId)?.delegatedCompletion?.settledDeliveryCount,
      );
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
      const orchestrator = yield* OrchestratorV2;
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
      const orchestrator = yield* OrchestratorV2;
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
        settledDeliveryCount: 1,
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
        const orchestrator = yield* OrchestratorV2;
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

  for (const trigger of ["policy change", "recovery"] as const) {
    it.effect(`re-plans a pending-only cohort after a cancelled wake via ${trigger}`, () =>
      Effect.gen(function* () {
        const orchestrator = yield* OrchestratorV2;
        const eventSink = yield* EventSinkV2;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make(`thread:delegated-delivery-cancel-replan:${trigger}`);
        const projectId = ProjectId.make("project:delegated-delivery-cancel-replan");
        const parentRunId = RunId.make("run:delegated-delivery-cancel-replan:parent");
        const deliveryRunId = RunId.make("run:delegated-delivery-cancel-replan:delivery");
        const parentRootNodeId = NodeId.make("node:delegated-delivery-cancel-replan:parent-root");
        const deliveryRootNodeId = NodeId.make(
          "node:delegated-delivery-cancel-replan:delivery-root",
        );
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
              stored.event.type === "run.updated" &&
              stored.event.payload.id === parentRunId &&
              stored.event.payload.delegatedCompletion?.delivery === null,
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
          afterLateAcceptance.subagents.find((task) => task.id === taskId)?.completionDelivery
            ?.state,
          "pending",
        );
        const store = yield* ProjectionStoreV2;
        assert.include(yield* store.getRecoveryThreadIds("delegated-completions"), threadId);
        if (trigger === "policy change") {
          yield* orchestrator.dispatch({
            type: "delegated_task.wake-policy",
            commandId: CommandId.make("command:delegated-delivery-cancel-replan:wake-policy"),
            parentThreadId: threadId,
            taskId,
            completionWake: "settled_only",
          });
        } else {
          // Rebuild the runtime over the same in-memory database. Its startup
          // selector and recovery loop must find the cohort with no delivery.
          const sql = yield* SqlClient.SqlClient;
          yield* OrchestratorV2.pipe(
            Effect.provide(Layer.fresh(makeTestLayer(Layer.succeed(SqlClient.SqlClient, sql)))),
          );
        }

        const afterReplan = yield* orchestrator.getThreadProjection(threadId);
        const parentRun = afterReplan.runs.find((candidate) => candidate.id === parentRunId);
        const task = afterReplan.subagents.find((candidate) => candidate.id === taskId);
        assert.equal(task?.completionDelivery?.state, "claimed");
        assert.isNotNull(parentRun?.delegatedCompletion?.delivery ?? null);
        assert.deepEqual(parentRun?.delegatedCompletion?.delivery?.taskIds, [taskId]);
      }).pipe(Effect.provide(Layer.fresh(TestLayer))),
    );
  }
});
