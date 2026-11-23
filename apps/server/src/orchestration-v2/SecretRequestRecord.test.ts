import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "gpt-5.1-codex" };
const adapter = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: (input: {
    readonly providerSessionId: ProviderSessionId;
    readonly runtimePolicy: { readonly cwd: string };
  }) =>
    Effect.succeed({
      id: input.providerSessionId,
      instanceId,
      driver: ProviderDriverKind.make("codex"),
      providerSession: {
        id: input.providerSessionId,
        driver: ProviderDriverKind.make("codex"),
        providerInstanceId: instanceId,
        status: "ready",
        cwd: input.runtimePolicy.cwd,
        model: modelSelection.model,
        capabilities: CodexProviderCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      },
      events: Stream.empty,
      close: Effect.void,
    } as never),
} as ProviderAdapter.ProviderAdapterV2["Service"];
const layerDatabase = SqlitePersistence.layerMemory;
const layerTest = Layer.mergeAll(
  layerDatabase,
  ProjectionStore.layer.pipe(Layer.provide(layerDatabase)),
  EffectOutbox.layer.pipe(Layer.provide(layerDatabase)),
  ProviderReplayHarness.layerWithRegistry(
    { name: "secret-request-record" },
    ProviderAdapterRegistry.layerFromAdapters([adapter]),
    { databaseLayer: layerDatabase, runEffectWorker: false },
  ),
);

const projectId = ProjectId.make("project:secret-request-record");
const threadId = ThreadId.make("thread:secret-request-record");
const sessionId = ProviderSessionId.make("session:secret-request-record");
const providerThreadId = ProviderThreadId.make("provider-thread:secret-request-record");
const runId = RunId.make("run:secret-request-record");
const firstAttemptId = RunAttemptId.make("attempt:secret-request-record:1");
const steeredAttemptId = RunAttemptId.make("attempt:secret-request-record:2");
const originalRoot = NodeId.make("node:secret-request-record:original");
const steeredRoot = NodeId.make("node:secret-request-record:steered");
const originalTurnId = ProviderTurnId.make("provider-turn:secret-request-record:original");
const messageId = MessageId.make("message:secret-request-record");
const cardId = TurnItemId.make("turn-item:secret-request:secret-request-record");
const staleCardId = TurnItemId.make("turn-item:secret-request:secret-request-record:stale");
const now = DateTime.makeUnsafe("2026-11-11T20:00:00.000Z");

/**
 * A pending secret card must stay closable after a steering restart replaces the
 * run's root node: the call that opened it only ever carries the card's own node,
 * so rejecting that node wedges the card open forever.
 */
it.effect("closes a secret card recorded under an earlier root after a steering restart", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("create-secret-request-record"),
      threadId,
      projectId,
      title: "Secret request record",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    yield* projections.apply({
      id: EventId.make("secret-request-record:session"),
      type: "provider-session.attached",
      threadId,
      occurredAt: now,
      payload: {
        id: sessionId,
        driver: adapter.driver,
        providerInstanceId: instanceId,
        status: "ready",
        cwd: "/repo",
        model: modelSelection.model,
        capabilities: CodexProviderCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      },
    });
    yield* projections.apply({
      id: EventId.make("secret-request-record:provider-thread"),
      type: "provider-thread.updated",
      threadId,
      occurredAt: now,
      payload: {
        id: providerThreadId,
        driver: adapter.driver,
        providerInstanceId: instanceId,
        providerSessionId: sessionId,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: { driver: adapter.driver, nativeId: "native-thread", strength: "strong" },
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      },
    });
    yield* projections.apply({
      id: EventId.make("secret-request-record:run"),
      type: "run.created",
      threadId,
      occurredAt: now,
      payload: {
        id: runId,
        threadId,
        ordinal: 1,
        providerInstanceId: instanceId,
        modelSelection,
        providerThreadId,
        userMessageId: messageId,
        rootNodeId: originalRoot,
        activeAttemptId: firstAttemptId,
        status: "running",
        requestedAt: now,
        startedAt: now,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
      },
    });
    yield* projections.apply({
      id: EventId.make("secret-request-record:attempt-1"),
      type: "run-attempt.created",
      threadId,
      occurredAt: now,
      payload: {
        id: firstAttemptId,
        runId,
        attemptOrdinal: 1,
        rootNodeId: originalRoot,
        providerInstanceId: instanceId,
        providerThreadId,
        providerTurnId: null,
        reason: "initial",
        status: "running",
        startedAt: now,
        completedAt: null,
      },
    });

    // The first attempt reached the provider; the card is asked on that turn.
    yield* projections.apply({
      id: EventId.make("secret-request-record:turn"),
      type: "provider-turn.updated",
      threadId,
      occurredAt: now,
      payload: {
        id: originalTurnId,
        providerThreadId,
        nodeId: originalRoot,
        runAttemptId: firstAttemptId,
        nativeTurnRef: null,
        ordinal: 1,
        status: "running",
        startedAt: now,
        completedAt: null,
      },
    });

    const record = (commandId: string, nodeId: NodeId, turnItemId: TurnItemId) =>
      orchestrator.dispatch({
        type: "secret_request.record",
        commandId: CommandId.make(commandId),
        threadId,
        runId,
        nodeId,
        turnItemId,
        label: "Booking details",
        reason: "To check the passenger in.",
        secretStatus: "pending" as const,
      });
    const readCard = (id: TurnItemId) =>
      projections
        .getThreadRecords(threadId, ["turnItems"], {
          turnItemTypes: ["secret_request"],
          messageRoles: [],
        })
        .pipe(
          Effect.map((records) => {
            const item = records.turnItems.find((candidate) => candidate.id === id);
            return item?.type === "secret_request" ? item : undefined;
          }),
        );

    yield* record("secret-request-record:open", originalRoot, cardId);
    const opened = yield* readCard(cardId);
    assert.equal(opened?.secretStatus, "pending");
    assert.equal(opened?.providerTurnId, originalTurnId);

    // The user steers while the card waits: the run's root moves to a new attempt.
    yield* projections.apply({
      id: EventId.make("secret-request-record:run-steered"),
      type: "run.updated",
      threadId,
      occurredAt: now,
      payload: {
        id: runId,
        threadId,
        ordinal: 1,
        providerInstanceId: instanceId,
        modelSelection,
        providerThreadId,
        userMessageId: messageId,
        rootNodeId: steeredRoot,
        activeAttemptId: steeredAttemptId,
        status: "starting",
        requestedAt: now,
        startedAt: now,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
      },
    });
    yield* projections.apply({
      id: EventId.make("secret-request-record:attempt-2"),
      type: "run-attempt.created",
      threadId,
      occurredAt: now,
      payload: {
        id: steeredAttemptId,
        runId,
        attemptOrdinal: 2,
        rootNodeId: steeredRoot,
        providerInstanceId: instanceId,
        providerThreadId,
        providerTurnId: null,
        reason: "steering_restart",
        status: "pending",
        startedAt: null,
        completedAt: null,
      },
    });

    // The waiting call only carries the card's own node; closing must still work.
    yield* orchestrator.dispatch({
      type: "secret_request.record",
      commandId: CommandId.make("secret-request-record:close"),
      threadId,
      runId,
      nodeId: originalRoot,
      turnItemId: cardId,
      label: "Booking details",
      reason: "To check the passenger in.",
      secretStatus: "cancelled" as const,
    });
    const cancelled = yield* readCard(cardId);
    assert.equal(cancelled?.secretStatus, "cancelled");
    assert.equal(cancelled?.status, "cancelled");
    // Closing through the card's own node keeps the identity it was asked with,
    // even though the run's active attempt has no provider turn of its own.
    assert.equal(cancelled?.nodeId, originalRoot);
    assert.equal(cancelled?.providerTurnId, originalTurnId);

    // A new card may not be opened under the stale root.
    const stale = yield* Effect.flip(
      record("secret-request-record:stale-open", originalRoot, staleCardId),
    );
    assert.match(String((stale as { cause?: unknown }).cause ?? ""), /is not the root of run/);
  }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "answering a secret card after its run completed dispatches its wake atomically, and archive cancels pending cards",
  () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const wakeThreadId = ThreadId.make("thread:secret-request-wake");
      const wakeSessionId = ProviderSessionId.make("session:secret-request-wake");
      const wakeProviderThreadId = ProviderThreadId.make("provider-thread:secret-request-wake");
      const wakeRunId = RunId.make("run:secret-request-wake");
      const wakeRoot = NodeId.make("node:secret-request-wake:root");
      const wakeAttemptId = RunAttemptId.make("attempt:secret-request-wake:1");
      const wakeCardId = TurnItemId.make("turn-item:secret-request:wake");
      const archiveCardId = TurnItemId.make("turn-item:secret-request:archive");

      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("create-secret-request-wake"),
        threadId: wakeThreadId,
        projectId,
        title: "Secret request wake",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
      yield* projections.apply({
        id: EventId.make("secret-request-wake:session"),
        type: "provider-session.attached",
        threadId: wakeThreadId,
        occurredAt: now,
        payload: {
          id: wakeSessionId,
          driver: adapter.driver,
          providerInstanceId: instanceId,
          status: "ready",
          cwd: "/repo",
          model: modelSelection.model,
          capabilities: CodexProviderCapabilitiesV2,
          createdAt: now,
          updatedAt: now,
          lastError: null,
        },
      });
      yield* projections.apply({
        id: EventId.make("secret-request-wake:provider-thread"),
        type: "provider-thread.updated",
        threadId: wakeThreadId,
        occurredAt: now,
        payload: {
          id: wakeProviderThreadId,
          driver: adapter.driver,
          providerInstanceId: instanceId,
          providerSessionId: wakeSessionId,
          appThreadId: wakeThreadId,
          ownerNodeId: null,
          nativeThreadRef: {
            driver: adapter.driver,
            nativeId: "native-thread-wake",
            strength: "strong",
          },
          nativeConversationHeadRef: null,
          status: "idle",
          firstRunOrdinal: 1,
          lastRunOrdinal: 1,
          handoffIds: [],
          forkedFrom: null,
          createdAt: now,
          updatedAt: now,
        },
      });
      yield* projections.apply({
        id: EventId.make("secret-request-wake:run"),
        type: "run.created",
        threadId: wakeThreadId,
        occurredAt: now,
        payload: {
          id: wakeRunId,
          threadId: wakeThreadId,
          ordinal: 1,
          providerInstanceId: instanceId,
          modelSelection,
          providerThreadId: wakeProviderThreadId,
          userMessageId: MessageId.make("message:secret-request-wake:initial"),
          rootNodeId: wakeRoot,
          activeAttemptId: wakeAttemptId,
          status: "running",
          requestedAt: now,
          startedAt: now,
          completedAt: null,
          checkpointId: null,
          contextHandoffId: null,
        },
      });

      // Open two secret cards while the run is active.
      for (const [cmd, id, label] of [
        ["secret-request-wake:open-1", wakeCardId, "API token"],
        ["secret-request-wake:open-2", archiveCardId, "Backup token"],
      ] as const) {
        yield* orchestrator.dispatch({
          type: "secret_request.record",
          commandId: CommandId.make(cmd),
          threadId: wakeThreadId,
          runId: wakeRunId,
          nodeId: wakeRoot,
          turnItemId: id,
          label,
          reason: "Needed for deploy.",
          secretStatus: "pending",
        });
      }

      // Complete the asking run; both cards remain pending.
      yield* projections.apply({
        id: EventId.make("secret-request-wake:run-done"),
        type: "run.updated",
        threadId: wakeThreadId,
        occurredAt: now,
        payload: {
          id: wakeRunId,
          threadId: wakeThreadId,
          ordinal: 1,
          providerInstanceId: instanceId,
          modelSelection,
          providerThreadId: wakeProviderThreadId,
          userMessageId: MessageId.make("message:secret-request-wake:initial"),
          rootNodeId: wakeRoot,
          activeAttemptId: wakeAttemptId,
          status: "completed",
          requestedAt: now,
          startedAt: now,
          completedAt: now,
          checkpointId: null,
          contextHandoffId: null,
        },
      });

      // Answering the first card after the run completed closes it and dispatches its wake,
      // routing back to the asking run's modelSelection even if the thread picker switched.
      yield* orchestrator.dispatch({
        type: "secret_request.record",
        commandId: CommandId.make("secret-request-wake:save-1"),
        threadId: wakeThreadId,
        runId: wakeRunId,
        nodeId: wakeRoot,
        turnItemId: wakeCardId,
        label: "API token",
        reason: "Needed for deploy.",
        secretStatus: "saved",
        wake: {
          messageId: MessageId.make(`message:secret-request:${wakeCardId}`),
          text: 'The user saved "API token".',
          notification: {
            source: { kind: "background_task" },
            outcome: "completed",
            summary: "Saved API token",
          },
          secretRequestTurnItemId: wakeCardId,
        },
      });

      const afterAnswer = yield* projections.getThreadProjection(wakeThreadId);
      const savedCard = afterAnswer.turnItems.find((item) => item.id === wakeCardId);
      assert.equal(savedCard?.type === "secret_request" ? savedCard.secretStatus : null, "saved");
      const wakeMessage = afterAnswer.messages.find(
        (message) => message.id === MessageId.make(`message:secret-request:${wakeCardId}`),
      );
      assert.isDefined(wakeMessage);
      assert.equal(wakeMessage?.text, 'The user saved "API token".');
      assert.equal(wakeMessage?.secretRequestTurnItemId, wakeCardId);
      const wakeRun = afterAnswer.runs.find((run) => run.userMessageId === wakeMessage?.id);
      assert.deepEqual(wakeRun?.modelSelection, modelSelection);

      // A new user-authored message durably cancels remaining pending secret cards.
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("secret-request-wake:user-followup"),
        threadId: wakeThreadId,
        messageId: MessageId.make("message:secret-request-wake:user-followup"),
        text: "Never mind, skip that step.",
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "user",
        creationSource: "web",
      });
      const afterUserMessage = yield* projections.getThreadProjection(wakeThreadId);
      const supersededCard = afterUserMessage.turnItems.find((item) => item.id === archiveCardId);
      assert.equal(
        supersededCard?.type === "secret_request" ? supersededCard.secretStatus : null,
        "cancelled",
      );
    }).pipe(Effect.provide(layerTest)),
);

it.effect("rejects answering a secret card on a rolled_back run or archived thread", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const guardThreadId = ThreadId.make("thread:secret-request-guards");
    const guardSessionId = ProviderSessionId.make("session:secret-request-guards");
    const guardProviderThreadId = ProviderThreadId.make("provider-thread:secret-request-guards");
    const guardRunId = RunId.make("run:secret-request-guards");
    const guardAttemptId = RunAttemptId.make("attempt:secret-request-guards:1");
    const guardRoot = NodeId.make("node:secret-request-guards:root");
    const guardCardId = TurnItemId.make("turn-item:secret-request:guards:1");

    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("create-secret-request-guards"),
      threadId: guardThreadId,
      projectId,
      title: "Secret request guards",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    yield* projections.apply({
      id: EventId.make("secret-request-guards:session"),
      type: "provider-session.attached",
      threadId: guardThreadId,
      occurredAt: now,
      payload: {
        id: guardSessionId,
        driver: adapter.driver,
        providerInstanceId: instanceId,
        status: "ready",
        cwd: "/repo",
        model: modelSelection.model,
        capabilities: CodexProviderCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      },
    });
    yield* projections.apply({
      id: EventId.make("secret-request-guards:provider-thread"),
      type: "provider-thread.updated",
      threadId: guardThreadId,
      occurredAt: now,
      payload: {
        id: guardProviderThreadId,
        driver: adapter.driver,
        providerInstanceId: instanceId,
        providerSessionId: guardSessionId,
        appThreadId: guardThreadId,
        ownerNodeId: null,
        nativeThreadRef: { driver: adapter.driver, nativeId: "native-guard", strength: "strong" },
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      },
    });
    yield* projections.apply({
      id: EventId.make("secret-request-guards:run"),
      type: "run.created",
      threadId: guardThreadId,
      occurredAt: now,
      payload: {
        id: guardRunId,
        threadId: guardThreadId,
        ordinal: 1,
        providerInstanceId: instanceId,
        modelSelection,
        providerThreadId: guardProviderThreadId,
        userMessageId: MessageId.make("message:secret-request-guards:initial"),
        rootNodeId: guardRoot,
        activeAttemptId: guardAttemptId,
        status: "running",
        requestedAt: now,
        startedAt: now,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
      },
    });
    yield* orchestrator.dispatch({
      type: "secret_request.record",
      commandId: CommandId.make("secret-request-guards:open"),
      threadId: guardThreadId,
      runId: guardRunId,
      nodeId: guardRoot,
      turnItemId: guardCardId,
      label: "Deploy key",
      reason: "Needed for deploy.",
      secretStatus: "pending",
    });
    // Mark run rolled_back and assert answering is rejected atomically.
    yield* projections.apply({
      id: EventId.make("secret-request-guards:rolled-back"),
      type: "run.updated",
      threadId: guardThreadId,
      occurredAt: now,
      payload: {
        id: guardRunId,
        threadId: guardThreadId,
        ordinal: 1,
        providerInstanceId: instanceId,
        modelSelection,
        providerThreadId: guardProviderThreadId,
        userMessageId: MessageId.make("message:secret-request-guards:initial"),
        rootNodeId: guardRoot,
        activeAttemptId: guardAttemptId,
        status: "rolled_back",
        requestedAt: now,
        startedAt: now,
        completedAt: now,
        checkpointId: null,
        contextHandoffId: null,
      },
    });
    const rejected = yield* orchestrator
      .dispatch({
        type: "secret_request.record",
        commandId: CommandId.make("secret-request-guards:answer-rolled-back"),
        threadId: guardThreadId,
        runId: guardRunId,
        nodeId: guardRoot,
        turnItemId: guardCardId,
        label: "Deploy key",
        reason: "Needed for deploy.",
        secretStatus: "saved",
        wake: {
          messageId: MessageId.make(`message:secret-request:${guardCardId}`),
          text: 'The user saved "Deploy key".',
          notification: {
            source: { kind: "background_task" },
            outcome: "completed",
            summary: "Saved Deploy key",
          },
          secretRequestTurnItemId: guardCardId,
        },
      })
      .pipe(Effect.flip);
    assert.equal(rejected._tag, "OrchestratorDispatchError");
  }).pipe(Effect.provide(layerTest)),
);

it.effect("binds a secret card wake on a reused delegated child thread to its open follow-up", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const parentThreadId = ThreadId.make("thread:secret-followup:parent");
    const childThreadId = ThreadId.make("thread:secret-followup:child");
    const parentRunId = RunId.make("run:secret-followup:parent:1");
    const parentRoot = NodeId.make("node:secret-followup:parent:root");
    const originalTaskId = NodeId.make("node:delegated-task:secret-followup:orig");
    const followupTaskId = NodeId.make("node:delegated-task:secret-followup:next");
    const childSessionId = ProviderSessionId.make("session:secret-followup:child");
    const childProviderThreadId = ProviderThreadId.make("provider-thread:secret-followup:child");
    const childRun1Id = RunId.make("run:secret-followup:child:1");
    const childRun2Id = RunId.make("run:secret-followup:child:2");
    const childRun2Root = NodeId.make("node:secret-followup:child:run2-root");
    const childRun2Attempt = RunAttemptId.make("attempt:secret-followup:child:2");
    const followupCardId = TurnItemId.make("turn-item:secret-request:followup:1");

    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("secret-followup:create-parent"),
      threadId: parentThreadId,
      projectId,
      title: "Parent",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    yield* projections.apply({
      id: EventId.make("secret-followup:parent-run"),
      type: "run.created",
      threadId: parentThreadId,
      occurredAt: now,
      payload: {
        id: parentRunId,
        threadId: parentThreadId,
        ordinal: 1,
        providerInstanceId: instanceId,
        modelSelection,
        providerThreadId: ProviderThreadId.make("provider-thread:secret-followup:parent"),
        userMessageId: MessageId.make("message:secret-followup:parent:1"),
        rootNodeId: parentRoot,
        activeAttemptId: RunAttemptId.make("attempt:secret-followup:parent:1"),
        status: "completed",
        requestedAt: now,
        startedAt: now,
        completedAt: now,
        checkpointId: null,
        contextHandoffId: null,
      },
    });
    yield* projections.apply({
      id: EventId.make("secret-followup:child-thread"),
      type: "thread.created",
      threadId: childThreadId,
      occurredAt: now,
      payload: {
        id: childThreadId,
        projectId,
        title: "Delegated child",
        modelSelection,
        providerInstanceId: instanceId,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        activeProviderThreadId: childProviderThreadId,
        lineage: {
          parentThreadId,
          rootThreadId: parentThreadId,
          relationshipToParent: "subagent",
        },
        forkedFrom: {
          type: "node",
          nodeId: originalTaskId,
        },
        createdBy: "agent",
        creationSource: "mcp",
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        deletedAt: null,
        lastVisitedAt: now,
        settledOverride: null,
        settledAt: null,
        unsettledAt: null,
        snoozedUntil: null,
        snoozedAt: null,
        pinnedAt: null,
        pinOrderKey: null,
        activeOrderKey: null,
        autoSettleDisabledAt: null,
        titleRegeneration: null,
        historyOrigin: "native",
      },
    });
    yield* projections.apply({
      id: EventId.make("secret-followup:orig-subagent"),
      type: "subagent.updated",
      threadId: parentThreadId,
      occurredAt: now,
      payload: {
        id: originalTaskId,
        threadId: parentThreadId,
        runId: parentRunId,
        parentNodeId: parentRoot,
        origin: "app_owned",
        driver: adapter.driver,
        providerInstanceId: instanceId,
        providerThreadId: childProviderThreadId,
        childThreadId,
        nativeTaskRef: null,
        model: modelSelection.model,
        modelSelection,
        prompt: "Initial task",
        title: "Delegated child",
        status: "completed",
        result: "Initial result",
        startedAt: now,
        completedAt: now,
        updatedAt: now,
        createdBy: "agent",
      },
    });
    yield* projections.apply({
      id: EventId.make("secret-followup:next-subagent"),
      type: "subagent.updated",
      threadId: parentThreadId,
      occurredAt: now,
      payload: {
        id: followupTaskId,
        threadId: parentThreadId,
        runId: parentRunId,
        parentNodeId: parentRoot,
        origin: "app_owned",
        driver: adapter.driver,
        providerInstanceId: instanceId,
        providerThreadId: childProviderThreadId,
        childThreadId,
        nativeTaskRef: null,
        model: modelSelection.model,
        modelSelection,
        prompt: "Follow-up task",
        title: "Delegated child",
        status: "running",
        result: null,
        startedAt: now,
        completedAt: null,
        updatedAt: now,
        createdBy: "agent",
      },
    });
    yield* projections.apply({
      id: EventId.make("secret-followup:child-session"),
      type: "provider-session.attached",
      threadId: childThreadId,
      occurredAt: now,
      payload: {
        id: childSessionId,
        driver: adapter.driver,
        providerInstanceId: instanceId,
        status: "ready",
        cwd: "/repo",
        model: modelSelection.model,
        capabilities: CodexProviderCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      },
    });
    yield* projections.apply({
      id: EventId.make("secret-followup:child-provider-thread"),
      type: "provider-thread.updated",
      threadId: childThreadId,
      occurredAt: now,
      payload: {
        id: childProviderThreadId,
        driver: adapter.driver,
        providerInstanceId: instanceId,
        providerSessionId: childSessionId,
        appThreadId: childThreadId,
        ownerNodeId: null,
        nativeThreadRef: { driver: adapter.driver, nativeId: "native-child", strength: "strong" },
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: 1,
        lastRunOrdinal: 2,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      },
    });
    yield* projections.apply({
      id: EventId.make("secret-followup:child-run-1"),
      type: "run.created",
      threadId: childThreadId,
      occurredAt: now,
      payload: {
        id: childRun1Id,
        threadId: childThreadId,
        ordinal: 1,
        providerInstanceId: instanceId,
        modelSelection,
        providerThreadId: childProviderThreadId,
        userMessageId: MessageId.make("message:secret-followup:child:1"),
        rootNodeId: NodeId.make("node:secret-followup:child:run1-root"),
        activeAttemptId: RunAttemptId.make("attempt:secret-followup:child:1"),
        status: "completed",
        requestedAt: now,
        startedAt: now,
        completedAt: now,
        checkpointId: null,
        contextHandoffId: null,
      },
    });
    yield* projections.apply({
      id: EventId.make("secret-followup:child-run-2"),
      type: "run.created",
      threadId: childThreadId,
      occurredAt: now,
      payload: {
        id: childRun2Id,
        threadId: childThreadId,
        ordinal: 2,
        providerInstanceId: instanceId,
        modelSelection,
        providerThreadId: childProviderThreadId,
        userMessageId: MessageId.make("message:secret-followup:child:2"),
        rootNodeId: childRun2Root,
        activeAttemptId: childRun2Attempt,
        delegatedTaskId: followupTaskId,
        status: "running",
        requestedAt: now,
        startedAt: now,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
      },
    });
    // Follow-up run opens a secret card and finishes its asking turn.
    yield* orchestrator.dispatch({
      type: "secret_request.record",
      commandId: CommandId.make("secret-followup:open"),
      threadId: childThreadId,
      runId: childRun2Id,
      nodeId: childRun2Root,
      turnItemId: followupCardId,
      label: "SSH passphrase",
      reason: "Needed to deploy.",
      secretStatus: "pending",
    });
    yield* projections.apply({
      id: EventId.make("secret-followup:child-run-2-done"),
      type: "run.updated",
      threadId: childThreadId,
      occurredAt: now,
      payload: {
        id: childRun2Id,
        threadId: childThreadId,
        ordinal: 2,
        providerInstanceId: instanceId,
        modelSelection,
        providerThreadId: childProviderThreadId,
        userMessageId: MessageId.make("message:secret-followup:child:2"),
        rootNodeId: childRun2Root,
        activeAttemptId: childRun2Attempt,
        delegatedTaskId: followupTaskId,
        status: "completed",
        requestedAt: now,
        startedAt: now,
        completedAt: now,
        checkpointId: null,
        contextHandoffId: null,
      },
    });

    // Answering the card starts a wake run bound to `followupTaskId` in the same commit.
    yield* orchestrator.dispatch({
      type: "secret_request.record",
      commandId: CommandId.make("secret-followup:save"),
      threadId: childThreadId,
      runId: childRun2Id,
      nodeId: childRun2Root,
      turnItemId: followupCardId,
      label: "SSH passphrase",
      reason: "Needed to deploy.",
      secretStatus: "saved",
      wake: {
        messageId: MessageId.make(`message:secret-request:${followupCardId}`),
        text: 'The user saved "SSH passphrase".',
        notification: {
          source: { kind: "background_task" },
          outcome: "completed",
          summary: "Saved SSH passphrase",
        },
        secretRequestTurnItemId: followupCardId,
      },
    });

    const childProjection = yield* projections.getThreadProjection(childThreadId);
    const wakeRun = childProjection.runs.find(
      (run) => run.userMessageId === MessageId.make(`message:secret-request:${followupCardId}`),
    );
    assert.isDefined(wakeRun);
    assert.equal(wakeRun?.delegatedTaskId, followupTaskId);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("promoting a queued user message to a steer cancels a later pending secret card", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const providerSessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
    const promoteThreadId = ThreadId.make("thread:secret-promote-steer");
    const promoteSessionId = ProviderSessionId.make("session:secret-promote-steer");
    const promoteProviderThreadId = ProviderThreadId.make("provider-thread:secret-promote-steer");
    const activeRunId = RunId.make("run:secret-promote-steer:active");
    const activeAttemptId = RunAttemptId.make("attempt:secret-promote-steer:active");
    const activeRoot = NodeId.make("node:secret-promote-steer:active");
    const activeTurnId = ProviderTurnId.make("provider-turn:secret-promote-steer:active");
    const promoteCardId = TurnItemId.make("turn-item:secret-request:promote");

    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("secret-promote:create"),
      threadId: promoteThreadId,
      projectId,
      title: "Promote steer cancels secret card",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    yield* providerSessions.open({
      threadId: promoteThreadId,
      providerSessionId: promoteSessionId,
      modelSelection,
      runtimePolicy: {
        cwd: process.cwd(),
        runtimeMode: "full-access",
        interactionMode: "default",
      },
    });
    yield* projections.apply({
      id: EventId.make("secret-promote:session"),
      type: "provider-session.attached",
      threadId: promoteThreadId,
      occurredAt: now,
      payload: {
        id: promoteSessionId,
        driver: adapter.driver,
        providerInstanceId: instanceId,
        status: "ready",
        cwd: "/repo",
        model: modelSelection.model,
        capabilities: CodexProviderCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      },
    });
    yield* projections.apply({
      id: EventId.make("secret-promote:provider-thread"),
      type: "provider-thread.updated",
      threadId: promoteThreadId,
      occurredAt: now,
      payload: {
        id: promoteProviderThreadId,
        driver: adapter.driver,
        providerInstanceId: instanceId,
        providerSessionId: promoteSessionId,
        appThreadId: promoteThreadId,
        ownerNodeId: null,
        nativeThreadRef: {
          driver: adapter.driver,
          nativeId: "native-thread-promote",
          strength: "strong",
        },
        nativeConversationHeadRef: null,
        status: "active",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      },
    });
    yield* projections.apply({
      id: EventId.make("secret-promote:run"),
      type: "run.created",
      threadId: promoteThreadId,
      occurredAt: now,
      payload: {
        id: activeRunId,
        threadId: promoteThreadId,
        ordinal: 1,
        providerInstanceId: instanceId,
        modelSelection,
        providerThreadId: promoteProviderThreadId,
        userMessageId: MessageId.make("message:secret-promote:initial"),
        rootNodeId: activeRoot,
        activeAttemptId,
        status: "running",
        requestedAt: now,
        startedAt: now,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
      },
    });
    yield* projections.apply({
      id: EventId.make("secret-promote:attempt"),
      type: "run-attempt.created",
      threadId: promoteThreadId,
      occurredAt: now,
      payload: {
        id: activeAttemptId,
        runId: activeRunId,
        attemptOrdinal: 1,
        rootNodeId: activeRoot,
        providerInstanceId: instanceId,
        providerThreadId: promoteProviderThreadId,
        providerTurnId: activeTurnId,
        reason: "initial",
        status: "running",
        startedAt: now,
        completedAt: null,
      },
    });
    yield* projections.apply({
      id: EventId.make("secret-promote:turn"),
      type: "provider-turn.updated",
      threadId: promoteThreadId,
      occurredAt: now,
      payload: {
        id: activeTurnId,
        providerThreadId: promoteProviderThreadId,
        nodeId: activeRoot,
        runAttemptId: activeAttemptId,
        nativeTurnRef: null,
        ordinal: 1,
        status: "running",
        startedAt: now,
        completedAt: null,
      },
    });

    // Queue a user follow-up BEFORE the active run opens a secret card.
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("secret-promote:queue"),
      threadId: promoteThreadId,
      messageId: MessageId.make("message:secret-promote:queued"),
      text: "Switch direction instead.",
      attachments: [],
      dispatchMode: { type: "queue_after_active" },
      createdBy: "user",
      creationSource: "web",
    });
    const afterQueue = yield* projections.getThreadProjection(promoteThreadId);
    const queuedRun = afterQueue.runs.find((run) => run.status === "queued");
    assert.isDefined(queuedRun);

    // The active run now opens a pending secret card.
    yield* orchestrator.dispatch({
      type: "secret_request.record",
      commandId: CommandId.make("secret-promote:open-card"),
      threadId: promoteThreadId,
      runId: activeRunId,
      nodeId: activeRoot,
      turnItemId: promoteCardId,
      label: "Deploy token",
      reason: "Needed for deploy.",
      secretStatus: "pending",
    });

    // Promote the queued user follow-up to steer the active run; the card must be cancelled.
    yield* orchestrator.dispatch({
      type: "queued-message.promote-to-steer",
      commandId: CommandId.make("secret-promote:promote"),
      threadId: promoteThreadId,
      queuedRunId: queuedRun!.id,
      targetRunId: activeRunId,
    });
    const afterPromote = yield* projections.getThreadProjection(promoteThreadId);
    const card = afterPromote.turnItems.find((item) => item.id === promoteCardId);
    assert.equal(card?.type === "secret_request" ? card.secretStatus : null, "cancelled");
  }).pipe(Effect.provide(layerTest)),
);

it.effect("thread.create with a user initialMessage succeeds before any projection exists", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const freshThreadId = ThreadId.make("thread:secret-fresh-launch");

    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("secret-fresh-launch:create"),
      threadId: freshThreadId,
      projectId,
      title: "Fresh launch",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "mobile",
      initialMessage: {
        messageId: MessageId.make("message:secret-fresh-launch:1"),
        text: "Hello from a brand new thread.",
        attachments: [],
        workspaceStrategy: { type: "root" },
      },
    });

    const projection = yield* projections.getThreadProjection(freshThreadId);
    assert.equal(projection.messages.length, 1);
    assert.equal(projection.runs.length, 1);
  }).pipe(Effect.provide(layerTest)),
);
