import { assert, it } from "@effect/vitest";
import * as TestClock from "effect/testing/TestClock";
import {
  ChatAttachmentId,
  COMPOSER_CONTEXT_TERMINAL_TEXT_MAX_CHARS,
  ComposerContextId,
  CommandId,
  EnvironmentId,
  MessageId,
  EventId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  PROVIDER_SEND_TURN_MAX_INPUT_CHARS,
  ScheduledTaskId,
  RunId,
  ThreadId,
  TurnItemId,
  type ModelSelection,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import {
  expandAssistantCitationsForProvider,
  serializeAssistantCitation,
} from "@t3tools/shared/assistantCitations";
import { projectComposerContextForProvider } from "@t3tools/shared/composerContextReferences";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import type * as Scope from "effect/Scope";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderContinuationService from "./ProviderContinuationService.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import { userFacingDispatchErrorMessage } from "./UserFacingErrors.ts";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";

const driver = ProviderDriverKind.make("codex");
const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "test-model" };

it.effect.each(
  (
    [
      [false, "during delivery"],
      [false, "before dispatch"],
      [true, "after delivery"],
      [true, "without native steering"],
      [true, "with interrupting native steering"],
      [true, "settled only"],
      [false, "during continuation"],
      ["question", "during delivery"],
    ] as const
  ).map(([kind, timing]) => ({
    mailbox: kind === true,
    question: kind === "question",
    timing,
    label:
      kind === "question" ? "delegated task question" : kind ? "mailbox notification" : "steering",
  })),
)("delivers $label when completion wins $timing", ({ mailbox, question, timing }) =>
  Effect.gen(function* () {
    const cwd = yield* checkpointWorkspace(`steering-completion-${timing.replaceAll(" ", "-")}`);
    const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
    const started: ProviderAdapter.ProviderAdapterV2TurnInput[] = [];
    const steerEntered = yield* Deferred.make<void>();
    const rejectSteer = yield* Deferred.make<void>();
    let steerCalls = 0;
    const capabilities = {
      ...CodexProviderCapabilitiesV2,
      turns: {
        ...CodexProviderCapabilitiesV2.turns,
        supportsActiveSteering: timing !== "without native steering",
        activeSteeringInterruptsTools: timing === "with interrupting native steering",
      },
    };
    const adapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
      instanceId,
      driver,
      getCapabilities: () => Effect.succeed(capabilities),
      planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
      openSession: (input) =>
        Effect.gen(function* () {
          const now = yield* DateTime.now;
          return {
            instanceId,
            driver,
            providerSessionId: input.providerSessionId,
            providerSession: {
              id: input.providerSessionId,
              driver,
              providerInstanceId: instanceId,
              status: "ready",
              cwd,
              model: modelSelection.model,
              capabilities,
              createdAt: now,
              updatedAt: now,
              lastError: null,
            },
            events: Stream.fromQueue(events),
            ensureThread: ({ threadId }) =>
              Effect.succeed({
                id: ProviderThreadId.make(`provider-thread:${threadId}`),
                driver,
                providerInstanceId: instanceId,
                providerSessionId: input.providerSessionId,
                appThreadId: threadId,
                ownerNodeId: null,
                nativeThreadRef: { driver, nativeId: "native-thread", strength: "strong" },
                nativeConversationHeadRef: null,
                status: "idle",
                firstRunOrdinal: null,
                lastRunOrdinal: null,
                handoffIds: [],
                forkedFrom: null,
                createdAt: now,
                updatedAt: now,
              }),
            resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
            startTurn: (turn) =>
              Effect.gen(function* () {
                started.push(turn);
                yield* Queue.offer(events, {
                  type: "provider_turn.updated",
                  driver,
                  providerTurn: {
                    id: ProviderTurnId.make(`provider-turn:${turn.attemptId}`),
                    providerThreadId: turn.providerThread.id,
                    nodeId: turn.rootNodeId,
                    runAttemptId: turn.attemptId,
                    nativeTurnRef: {
                      driver,
                      nativeId: `native:${turn.attemptId}`,
                      strength: "strong",
                    },
                    ordinal: turn.providerTurnOrdinal,
                    status: "running",
                    startedAt: now,
                    completedAt: null,
                  },
                });
              }),
            steerTurn: (turn) =>
              Effect.gen(function* () {
                steerCalls += 1;
                if (timing === "after delivery" || timing === "during continuation") return;
                yield* Deferred.succeed(steerEntered, undefined);
                yield* Deferred.await(rejectSteer);
                return yield* new ProviderAdapter.ProviderAdapterSteerRunError({
                  driver,
                  providerThreadId: turn.providerThread.id,
                  providerTurnId: turn.providerTurnId,
                  cause: "turn already completed",
                });
              }),
            interruptTurn: () => Effect.void,
            respondToRuntimeRequest: () => Effect.void,
            readThreadSnapshot: () => Effect.die("unused"),
            rollbackThread: () => Effect.die("unused"),
            forkThread: () => Effect.die("unused"),
          };
        }),
    };
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = ThreadId.make("thread:steering-completion");
      const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
        orchestrator.streamDomainEvents.pipe(
          Stream.filter(predicate),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("create"),
        threadId,
        projectId: ProjectId.make("project:steering-completion"),
        title: "Steering race",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: cwd,
        createdBy: "user",
        creationSource: "web",
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("first"),
        threadId,
        messageId: MessageId.make("message:first"),
        text: "first",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
      const running = yield* watch(
        (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
      );
      yield* worker.drain();
      yield* Fiber.join(running);
      const first = started[0]!;
      const messageId = MessageId.make("message:steering");
      const taskId = NodeId.make("task:mailbox");
      if (mailbox) {
        const sink = yield* EventSink.EventSinkV2;
        const current = yield* orchestrator.getThreadProjection(threadId);
        const parentRun = current.runs.find((run) => run.id === first.runId)!;
        const now = yield* DateTime.now;
        yield* sink.write({
          events: [
            {
              id: EventId.make("mailbox:cohort"),
              type: "run.updated",
              threadId,
              runId: first.runId,
              occurredAt: now,
              payload: {
                ...parentRun,
                delegatedCompletion: {
                  disposition: "open",
                  nextGeneration: 2,
                  delivery: { generation: 1, messageId, taskIds: [taskId] },
                },
              },
            },
            {
              id: EventId.make("mailbox:task"),
              type: "subagent.updated",
              threadId,
              runId: first.runId,
              nodeId: taskId,
              occurredAt: now,
              payload: {
                id: taskId,
                threadId,
                runId: first.runId,
                parentNodeId: first.rootNodeId,
                origin: "app_owned",
                createdBy: "agent",
                driver,
                providerInstanceId: instanceId,
                providerThreadId: null,
                childThreadId: null,
                nativeTaskRef: null,
                prompt: "Do background work",
                title: "Background test",
                model: null,
                completionWake: timing === "settled only" ? "settled_only" : "always",
                completionDelivery: { state: "claimed", observedByRunId: null },
                status: "completed",
                result: "done",
                startedAt: now,
                completedAt: now,
                updatedAt: now,
              },
            },
          ],
        });
      }
      if (question) {
        const { thread } = yield* orchestrator.getThreadProjection(threadId);
        yield* (yield* EventSink.EventSinkV2).write({
          events: [
            {
              id: EventId.make("event:composer-selection"),
              type: "thread.model-selection-updated",
              threadId,
              providerInstanceId: instanceId,
              occurredAt: yield* DateTime.now,
              payload: { ...thread, modelSelection: composerSelection },
            },
          ],
        });
      }
      const dispatchSteer = orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("steer"),
        threadId,
        messageId,
        text: "fix the popover",
        attachments: [
          {
            type: "image",
            id: "steering-screenshot",
            name: "image.png",
            mimeType: "image/png",
            sizeBytes: 10,
          },
        ],
        dispatchMode:
          mailbox || question
            ? { type: "queue_after_active" }
            : { type: "steer_active", targetRunId: first.runId },
        createdBy: mailbox || question ? "agent" : "user",
        creationSource: mailbox || question ? "server" : "web",
        ...(question
          ? {
              notification: {
                source: { kind: "delegated_task", taskIds: [taskId] },
                outcome: "updated",
                summary: "Delegated task has a question",
              },
            }
          : {}),
        ...(mailbox
          ? {
              delegatedCompletion: {
                parentRunId: first.runId,
                generation: 1,
                taskIds: [taskId],
              },
            }
          : {}),
      });
      if (timing !== "before dispatch") yield* dispatchSteer;
      if (timing === "during continuation") {
        const projection = yield* orchestrator.getThreadProjection(threadId);
        const turn = projection.providerTurns[0]!;
        const failed = yield* watch(
          (event) => event.type === "provider-turn.updated" && event.payload.status === "failed",
        );
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...turn, status: "failed", completedAt: yield* DateTime.now },
        });
        yield* Fiber.join(failed);
        yield* worker.drain();
        assert.equal(steerCalls, 1);
        assert.lengthOf(started, 1);
        const continued = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(continued.runs[0]?.status, "running");
        assert.equal(
          continued.messages.find((message) => message.id === messageId)?.runId,
          first.runId,
        );
        return;
      }
      if (timing === "after delivery") yield* worker.drain();
      const delivery =
        timing === "during delivery" ? yield* worker.runOnce().pipe(Effect.forkScoped) : null;
      if (delivery !== null) yield* Deferred.await(steerEntered);
      const completed = yield* watch(
        (event) =>
          event.type === "run.updated" &&
          event.payload.id === first.runId &&
          event.payload.status === "waiting",
      );
      const projection = yield* orchestrator.getThreadProjection(threadId);
      const turn = projection.providerTurns[0]!;
      yield* Queue.offer(events, {
        type: "provider_turn.updated",
        driver,
        providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
      });
      yield* Queue.offer(events, {
        type: "turn.terminal",
        driver,
        providerThreadId: turn.providerThreadId,
        providerTurnId: turn.id,
        runOrdinal: first.runOrdinal,
        status: "completed",
        failure: null,
        threadDisposition: "reusable",
      });
      yield* Fiber.join(completed);
      if (delivery !== null) {
        yield* Deferred.succeed(rejectSteer, undefined);
        yield* Fiber.join(delivery);
      }
      if (delivery !== null && !question) {
        const requeued = yield* orchestrator.getThreadProjection(threadId);
        assert.isTrue(requeued.runs.some((run) => run.status === "queued"));
        assert.isFalse(
          requeued.visibleTurnItems.some(
            ({ item }) => item.type === "user_message" && item.messageId === messageId,
          ),
          "a steer that fell back to the queue must not appear delivered",
        );
        const item = requeued.turnItems.find(
          (item) => item.type === "user_message" && item.messageId === messageId,
        );
        assert.equal(item?.status, "pending");
        assert.isNull(item?.startedAt);
        assert.isNull(item?.completedAt);
      }
      if (timing === "before dispatch") yield* dispatchSteer;
      yield* worker.drain();
      yield* orchestrator.resumeQueuedRuns;
      yield* worker.drain();
      if (timing === "after delivery") {
        assert.equal(steerCalls, 1);
        assert.equal(started.length, 1);
        if (mailbox) {
          const delivered = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(delivered.subagents[0]?.completionDelivery?.state, "delivered");
          assert.equal(delivered.subagents[0]?.completionDelivery?.observedByRunId, null);
          assert.equal(delivered.runs[0]?.delegatedCompletion?.delivery, null);
          assert.equal(
            delivered.turnItems.filter((item) => item.type === "notification").length,
            1,
          );
          yield* orchestrator.dispatch({
            type: "notification.delivery.accept",
            commandId: CommandId.make("duplicate-acceptance"),
            threadId,
            messageId,
          });
          yield* worker.drain();
          assert.equal(steerCalls, 1);
          yield* orchestrator.dispatch({
            type: "delegated_task.completion-delivery.acknowledge",
            commandId: CommandId.make("read-result"),
            parentThreadId: threadId,
            taskId,
            observedByRunId: first.runId,
          });
          const acknowledged = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(acknowledged.subagents[0]?.completionDelivery?.state, "acknowledged");
        }
        return;
      }
      assert.equal(started.length, 2);
      assert.equal(started[1]?.message.messageId, messageId);
      if (mailbox) assert.include(started[1]?.message.text ?? "", String(taskId));
      else assert.equal(started[1]?.message.text, "fix the popover");
      assert.deepEqual(started[1]?.message.attachments, [
        {
          type: "image",
          id: "steering-screenshot",
          name: "image.png",
          mimeType: "image/png",
          sizeBytes: 10,
        },
      ]);
      assert.equal(steerCalls, timing === "during delivery" ? 1 : 0);
      const final = yield* orchestrator.getThreadProjection(threadId);
      if (question) {
        assert.deepEqual(final.thread.modelSelection, composerSelection);
        assert.deepEqual(started[1]?.modelSelection, composerSelection);
      }
      if (!mailbox && !question) {
        assert.equal(
          final.turnItems.find(
            (item) => item.type === "user_message" && item.messageId === messageId,
          )?.status,
          "completed",
        );
      }
      assert.equal(final.messages.filter((message) => message.id === messageId).length, 1);
      assert.equal(
        final.messages.find((message) => message.id === messageId)?.runId,
        started[1]?.runId,
      );
      assert.equal(
        final.turnItems.filter((item) =>
          mailbox || question
            ? item.type === "notification"
            : item.type === "user_message" && item.messageId === messageId,
        ).length,
        1,
      );
      if (!mailbox && !question) {
        assert.equal(
          final.visibleTurnItems.filter(
            ({ item }) => item.type === "user_message" && item.messageId === messageId,
          ).length,
          1,
        );
      }
      yield* worker.drain();
      assert.equal(started.length, 2);
    }).pipe(
      Effect.provide(
        ProviderReplayHarness.layerWithRegistry(
          { name: `steering-completion-${timing}` },
          ProviderAdapterRegistry.layerSingle(adapter),
          { runEffectWorker: false },
        ),
      ),
    );
  }),
);

const runSelection = {
  instanceId,
  model: "test-model",
  options: [{ id: "effort", value: "xhigh" }],
};
const composerSelection = {
  ...runSelection,
  options: [...runSelection.options, { id: "fastMode", value: false }],
};

const nextTurnSelectionHarness = Effect.fn("nextTurnSelectionHarness")(function* (
  name: string,
  canRestart = false,
  canSteer = true,
  interruptTurn: Effect.Effect<void> = Effect.void,
  steeringAcceptsAttachments = true,
  nativeGoals?: boolean,
  onSteer: (
    turn: ProviderAdapter.ProviderAdapterV2SteerInput,
  ) => Effect.Effect<void, ProviderAdapter.ProviderAdapterV2Error> = () => Effect.void,
  extraAdapters: ReadonlyArray<ProviderAdapter.ProviderAdapterV2["Service"]> = [],
) {
  const driver = ProviderDriverKind.make(nativeGoals === undefined ? "codex" : "opencode");
  const cwd = yield* checkpointWorkspace(name);
  const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
  const started: ProviderAdapter.ProviderAdapterV2TurnInput[] = [];
  const steered: string[] = [];
  const steeredSelections: Array<ModelSelection | undefined> = [];
  const interrupted: ProviderTurnId[] = [];
  const capabilities = {
    ...CodexProviderCapabilitiesV2,
    turns: {
      ...CodexProviderCapabilitiesV2.turns,
      supportsActiveSteering: canSteer,
      supportsSteeringByInterruptRestart: canRestart,
      activeSteeringAcceptsAttachments: steeringAcceptsAttachments,
      ...(nativeGoals === undefined ? {} : { supportsNativeGoals: nativeGoals }),
    },
  };
  const adapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
    instanceId,
    driver,
    getCapabilities: () => Effect.succeed(capabilities),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: (input) =>
      Effect.gen(function* () {
        const now = yield* DateTime.now;
        const turnUpdated = (
          turn: ProviderAdapter.ProviderAdapterV2TurnInput,
          status: "running" | "interrupted",
        ) =>
          Queue.offer(events, {
            type: "provider_turn.updated",
            driver,
            providerTurn: {
              id: ProviderTurnId.make(`provider-turn:${turn.attemptId}`),
              providerThreadId: turn.providerThread.id,
              nodeId: turn.rootNodeId,
              runAttemptId: turn.attemptId,
              nativeTurnRef: {
                driver,
                nativeId: `native:${turn.attemptId}`,
                strength: "strong",
              },
              ordinal: turn.providerTurnOrdinal,
              status,
              startedAt: now,
              completedAt: status === "running" ? null : now,
            },
          });
        const startTurn = (turn: ProviderAdapter.ProviderAdapterV2TurnInput) =>
          Effect.gen(function* () {
            started.push(turn);
            yield* turnUpdated(turn, "running");
          });
        return {
          instanceId,
          driver,
          providerSessionId: input.providerSessionId,
          providerSession: {
            id: input.providerSessionId,
            driver,
            providerInstanceId: instanceId,
            status: "ready",
            cwd,
            model: runSelection.model,
            capabilities,
            createdAt: now,
            updatedAt: now,
            lastError: null,
          },
          events: Stream.fromQueue(events),
          ensureThread: ({ threadId }) =>
            Effect.succeed({
              id: ProviderThreadId.make(`provider-thread:${threadId}`),
              driver,
              providerInstanceId: instanceId,
              providerSessionId: input.providerSessionId,
              appThreadId: threadId,
              ownerNodeId: null,
              nativeThreadRef: { driver, nativeId: "native-thread", strength: "strong" },
              nativeConversationHeadRef: null,
              status: "idle",
              firstRunOrdinal: null,
              lastRunOrdinal: null,
              handoffIds: [],
              forkedFrom: null,
              createdAt: now,
              updatedAt: now,
            }),
          resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
          startTurn,
          compactThread: startTurn,
          steerTurn: (turn) =>
            Effect.sync(() => {
              steered.push(turn.message.text);
              steeredSelections.push(turn.modelSelection);
            }).pipe(Effect.andThen(onSteer(turn))),
          interruptTurn: ({ providerTurnId }) =>
            Effect.sync(() => {
              interrupted.push(providerTurnId);
            }).pipe(
              Effect.andThen(interruptTurn),
              Effect.andThen(
                Effect.forEach(
                  started.filter(
                    (turn) =>
                      ProviderTurnId.make(`provider-turn:${turn.attemptId}`) === providerTurnId,
                  ),
                  (turn) =>
                    turnUpdated(turn, "interrupted").pipe(
                      Effect.andThen(
                        Queue.offer(events, {
                          type: "turn.terminal",
                          driver,
                          providerThreadId: turn.providerThread.id,
                          providerTurnId,
                          runOrdinal: turn.runOrdinal,
                          status: "interrupted",
                          failure: null,
                          threadDisposition: "reusable",
                        }),
                      ),
                    ),
                  { discard: true },
                ),
              ),
            ),
          respondToRuntimeRequest: () => Effect.void,
          readThreadSnapshot: () => Effect.die("unused"),
          rollbackThread: () => Effect.die("unused"),
          forkThread: () => Effect.die("unused"),
        };
      }),
  };
  const layer = ProviderReplayHarness.layerWithRegistry(
    { name },
    ProviderAdapterRegistry.layerFromAdapters([adapter, ...extraAdapters]),
    { runEffectWorker: false },
  );
  const startFirstTurn = Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
    const threadId = ThreadId.make(`thread:${name}`);
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("create"),
      threadId,
      projectId: ProjectId.make(`project:${name}`),
      title: "Steer with changed options",
      modelSelection: runSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: cwd,
      createdBy: "user",
      creationSource: "web",
    });
    const running = yield* orchestrator.streamDomainEvents.pipe(
      Stream.filter(
        (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
      ),
      Stream.take(1),
      Stream.runDrain,
      Effect.forkScoped,
    );
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("first"),
      threadId,
      messageId: MessageId.make("message:first"),
      text: "first",
      attachments: [],
      modelSelection: runSelection,
      dispatchMode: { type: "start_immediately" },
      createdBy: "user",
      creationSource: "web",
    });
    yield* worker.drain();
    yield* Fiber.join(running);
    // Ingestion checks turn ownership under this lock after committing the running turn.
    yield* (yield* ThreadCommandExecutor.ThreadCommandExecutor).withLock(threadId, Effect.void);
    return threadId;
  });
  return { cwd, events, started, steered, steeredSelections, interrupted, layer, startFirstTurn };
});

it.effect.each([
  { notice: "delegated task question", goal: null, steers: true },
  { notice: "delegated task question", goal: null, steers: false, summary: "/review is ready" },
  { notice: "background command", goal: "active", steers: true },
  { notice: "background command", goal: null, steers: false },
] as const)(
  "a $notice notice steers into the running turn: $steers (goal $goal)",
  ({ notice, goal, steers, ...row }) =>
    Effect.gen(function* () {
      const { started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        `notice-steer-${notice.replaceAll(" ", "-")}-${goal ?? "none"}-${steers}`,
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const { thread } = yield* orchestrator.getThreadProjection(threadId);
        yield* (yield* EventSink.EventSinkV2).write({
          events: [
            {
              id: EventId.make("event:composer-selection"),
              type: "thread.model-selection-updated",
              threadId,
              providerInstanceId: instanceId,
              occurredAt: yield* DateTime.now,
              payload: { ...thread, modelSelection: composerSelection },
            },
          ],
        });
        if (goal !== null) {
          const providerThread = (yield* orchestrator.getThreadProjection(threadId))
            .providerThreads[0]!;
          yield* (yield* EventSink.EventSinkV2).write({
            events: [
              {
                id: EventId.make("event:goal-active"),
                type: "provider-thread.updated",
                threadId,
                occurredAt: yield* DateTime.now,
                payload: { ...providerThread, goal: { objective: "Ship it", status: goal } },
              },
            ],
          });
        }
        const messageId = MessageId.make("message:notice");
        const notification =
          notice === "delegated task question"
            ? {
                source: {
                  kind: "delegated_task" as const,
                  taskIds: [NodeId.make("task:question")],
                  childThreadId: ThreadId.make("thread:question-child"),
                },
                outcome: "updated" as const,
                summary: "summary" in row ? row.summary : "Delegated task has a question",
              }
            : {
                source: { kind: "command" as const },
                outcome: "completed" as const,
                summary: "Background command finished",
              };
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("notice-wake"),
          threadId,
          messageId,
          text: `${notification.summary}\nexit 0: built 12 files`,
          notification,
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "agent",
          creationSource: "server",
        });
        yield* worker.drain();
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(
          steered,
          steers ? [`${notification.summary}\nexit 0: built 12 files`] : [],
        );
        assert.lengthOf(started, 1);
        assert.equal(
          after.runs.some((run) => run.status === "queued"),
          !steers,
        );
        const message = after.messages.find((candidate) => candidate.id === messageId);
        assert.deepEqual(message?.notification, notification);
        if (steers) assert.equal(message?.runId, started[0]?.runId);
        assert.deepEqual(after.thread.modelSelection, composerSelection);
      }).pipe(Effect.provide(layer));
    }),
);

it.effect("sends a selected held message once and preserves the remaining queue order", () =>
  Effect.gen(function* () {
    const { started, steered, layer, startFirstTurn } =
      yield* nextTurnSelectionHarness("send-held-message");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      yield* Effect.forEach(["queued-first", "selected", "queued-last"], (text) =>
        orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(text),
          threadId,
          messageId: MessageId.make(`message:${text}`),
          text,
          attachments: [],
          modelSelection: runSelection,
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        }),
      );
      const before = yield* orchestrator.getThreadProjection(threadId);
      const selected = before.runs.find(
        (run) => run.userMessageId === MessageId.make("message:selected"),
      )!;
      const sink = yield* EventSink.EventSinkV2;
      const now = yield* DateTime.now;
      // Restart recovery cancels the active run and holds every queued message.
      yield* sink.write({
        events: [
          ...before.runs.map((run) => ({
            id: EventId.make(`restart:${run.id}`),
            type: "run.updated" as const,
            threadId,
            runId: run.id,
            occurredAt: now,
            payload:
              run.status === "queued"
                ? { ...run, queueHeld: true }
                : { ...run, status: "cancelled" as const, completedAt: now },
          })),
          ...before.providerTurns.map((turn) => ({
            id: EventId.make(`restart:${turn.id}`),
            type: "provider-turn.updated" as const,
            threadId,
            occurredAt: now,
            payload: { ...turn, status: "cancelled" as const, completedAt: now },
          })),
        ],
      });
      const command = {
        type: "queued-run.start" as const,
        commandId: CommandId.make("send-selected"),
        threadId,
        runId: selected.id,
      };
      yield* orchestrator.dispatch(command);
      yield* worker.drain();
      yield* orchestrator.dispatch(command);
      yield* worker.drain();
      const after = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(after.runs.find((run) => run.id === selected.id)?.status, "running");
      assert.deepEqual(
        started.map((input) => input.message.text),
        ["first", "selected"],
      );
      assert.equal(started[1]?.runId, selected.id);
      assert.equal(started[1]?.message.messageId, selected.userMessageId);
      assert.deepEqual(started[1]?.modelSelection, runSelection);
      assert.deepEqual(steered, []);
      assert.lengthOf(
        after.messages.filter((message) => message.id === selected.userMessageId),
        1,
      );
      assert.deepEqual(
        after.runs.filter((run) => run.status === "queued").map((run) => run.userMessageId),
        [MessageId.make("message:queued-first"), MessageId.make("message:queued-last")],
      );
      assert.isFalse(after.runs.some((run) => run.status === "queued" && run.queueHeld));
      const duplicate = yield* orchestrator
        .dispatch({
          ...command,
          commandId: CommandId.make("send-selected-again"),
        })
        .pipe(Effect.flip);
      assert.equal(duplicate._tag, "OrchestratorDispatchError");
    }).pipe(Effect.provide(layer));
  }),
);

it.effect.each(
  (["preparing", "starting", "running"] as const).flatMap((status) =>
    [false, true].map((direct) => ({ status, direct })),
  ),
)("delivers a deferred steer for a $status run (direct: $direct)", ({ status, direct }) =>
  Effect.gen(function* () {
    const restarting = status === "starting" && !direct;
    const { started, steered, layer, startFirstTurn, events } = yield* nextTurnSelectionHarness(
      `deferred-steer-${status}-${direct}`,
      restarting,
      !restarting,
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const before = yield* orchestrator.getThreadProjection(threadId);
      const run = before.runs[0]!;
      const turn = before.providerTurns[0]!;
      const now = yield* DateTime.now;
      yield* (yield* EventSink.EventSinkV2).write({
        events: [
          {
            id: EventId.make("setup-run"),
            type: "run.updated",
            threadId,
            occurredAt: now,
            payload: { ...run, status },
          },
          {
            id: EventId.make("setup-turn"),
            type: "provider-turn.updated",
            threadId,
            occurredAt: now,
            payload: { ...turn, status: "pending" },
          },
        ],
      });
      if (status === "preparing" && !direct) {
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("edited-steer"),
          threadId,
          messageId: MessageId.make("message:edited-steer"),
          text: "edit this steer",
          attachments: [],
          deliveryIntent: "steer",
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
        const edited = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (candidate) => candidate.userMessageId === MessageId.make("message:edited-steer"),
        )!;
        assert.equal(edited.steerWhenReadyRunId, run.id);
        yield* orchestrator.dispatch({
          type: "queued-run.edit",
          commandId: CommandId.make("edit-steer-to-compact"),
          threadId,
          runId: edited.id,
          text: "/compact",
        });
        const compact = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (candidate) => candidate.id === edited.id,
        )!;
        assert.isUndefined(compact.steerWhenReadyRunId);
        assert.equal(compact.queuePosition, edited.queuePosition);
      }
      if (direct) {
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("earlier-queue"),
          threadId,
          messageId: MessageId.make("message:earlier-queue"),
          text: "earlier queued selection",
          modelSelection: composerSelection,
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
      }
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("queue"),
        threadId,
        messageId: MessageId.make("message:steer-after-setup"),
        text: "steer after setup",
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        ...(direct ? { deliveryIntent: "steer" as const } : {}),
        createdBy: "user",
        creationSource: "web",
      });
      const queued = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
        (candidate) => candidate.userMessageId === MessageId.make("message:steer-after-setup"),
      )!;
      assert.deepEqual(queued.modelSelection, runSelection);
      for (const text of ["/compact", "/logout"]) {
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`queue-${text}`),
          threadId,
          messageId: MessageId.make(`message:${text}`),
          text,
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
        const maintenance = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (candidate) => candidate.userMessageId === MessageId.make(`message:${text}`),
        )!;
        yield* orchestrator.dispatch({
          type: "queued-message.promote-to-steer",
          commandId: CommandId.make(`promote-${text}`),
          threadId,
          queuedRunId: maintenance.id,
          targetRunId: run.id,
        });
        const promoted = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (candidate) => candidate.id === maintenance.id,
        )!;
        assert.equal(promoted.status, "queued");
        assert.equal(promoted.queuePosition, 1);
      }
      const command = {
        type: "queued-message.promote-to-steer" as const,
        commandId: CommandId.make("promote"),
        threadId,
        queuedRunId: queued.id,
        targetRunId: run.id,
      };
      if (!direct) {
        yield* orchestrator.dispatch(command);
        yield* orchestrator.dispatch(command);
      }
      let secondQueuedId: typeof queued.id | undefined;
      if (restarting) {
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("second"),
          threadId,
          messageId: MessageId.make("message:second"),
          text: "second steer",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
        secondQueuedId = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (candidate) => candidate.userMessageId === MessageId.make("message:second"),
        )!.id;
        yield* orchestrator.dispatch({
          ...command,
          commandId: CommandId.make("promote-second"),
          queuedRunId: secondQueuedId,
        });
      }
      const secondReDeferred = yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === secondQueuedId &&
            event.payload.status === "queued",
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      const secondPromoted = yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === secondQueuedId &&
            event.payload.status === "cancelled",
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      yield* worker.drain();
      assert.deepEqual(steered, []);
      const promoted = yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === queued.id &&
            event.payload.status === "cancelled",
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      yield* (yield* EventSink.EventSinkV2).write({
        events: [
          {
            id: EventId.make("ready-run"),
            type: "run.updated",
            threadId,
            occurredAt: now,
            payload: run,
          },
        ],
      });
      yield* Queue.offer(events, { type: "provider_turn.updated", driver, providerTurn: turn });
      yield* Fiber.join(promoted);
      if (restarting) yield* Fiber.join(secondReDeferred);
      yield* worker.drain();
      if (restarting) {
        yield* Fiber.join(secondPromoted);
        yield* worker.drain();
        assert.deepEqual(
          started.map((turn) => turn.message.text),
          ["first", "steer after setup", "second steer"],
        );
      } else {
        assert.deepEqual(steered, ["steer after setup"]);
        assert.lengthOf(started, 1);
      }
      const after = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(
        after.messages.find((message) => message.id === queued.userMessageId)?.runId,
        run.id,
      );
      assert.equal(after.runs.find((candidate) => candidate.id === run.id)?.status, "running");
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("force sends multiple queued messages and can force send a requeued message again", () =>
  Effect.gen(function* () {
    const { started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
      "multiple-promoted-messages",
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const queue = (text: string, commandId: string) =>
        orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(commandId),
          threadId,
          messageId: MessageId.make(`message:${text}`),
          text,
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
      for (const text of ["one", "two", "remaining"]) yield* queue(text, `queue-${text}`);
      const promote = Effect.fn("test.promote")(function* (text: string, commandId: string) {
        const projection = yield* orchestrator.getThreadProjection(threadId);
        const queued = projection.runs.find(
          (run) =>
            run.status === "queued" && run.userMessageId === MessageId.make(`message:${text}`),
        )!;
        yield* orchestrator.dispatch({
          type: "queued-message.promote-to-steer",
          commandId: CommandId.make(commandId),
          threadId,
          queuedRunId: queued.id,
          targetRunId: started[0]!.runId,
        });
      });
      yield* promote("one", "promote-one");
      yield* promote("two", "promote-two");
      yield* worker.drain();
      assert.deepEqual(steered, ["one", "two"]);

      // The completion-race fallback redispatches the same message into the queue.
      yield* queue("one", "requeue-one");
      const requeued = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(
        requeued.turnItems.find(
          (item) => item.type === "user_message" && item.messageId === "message:one",
        )?.status,
        "pending",
      );
      yield* promote("one", "promote-one-again");
      yield* worker.drain();
      assert.deepEqual(steered, ["one", "two", "one"]);
      assert.lengthOf(started, 1);
      const final = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(
        final.runs.filter((run) => run.status === "queued").map((run) => run.userMessageId),
        [MessageId.make("message:remaining")],
      );
      for (const text of ["one", "two"]) {
        const items = final.turnItems.filter(
          (item) => item.type === "user_message" && item.messageId === `message:${text}`,
        );
        assert.lengthOf(items, 1);
        assert.equal(items[0]?.status, "completed");
      }
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("keeps a message queued when send now races with a running turn", () =>
  Effect.gen(function* () {
    const { started, layer, startFirstTurn } = yield* nextTurnSelectionHarness("send-queued-race");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("queue"),
        threadId,
        messageId: MessageId.make("message:queued"),
        text: "queued",
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "user",
        creationSource: "web",
      });
      const before = yield* orchestrator.getThreadProjection(threadId);
      const queued = before.runs.find((run) => run.status === "queued")!;
      const error = yield* orchestrator
        .dispatch({
          type: "queued-run.start",
          commandId: CommandId.make("send-now"),
          threadId,
          runId: queued.id,
        })
        .pipe(Effect.flip);
      yield* worker.drain();
      assert.equal(error._tag, "OrchestratorDispatchError");
      assert.equal(started.length, 1);
      const after = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(after.runs, before.runs);
    }).pipe(Effect.provide(layer));
  }),
);

// regression: #19210 a provider that can restart the turn must still steer, since restarting ends its native subagents
it.effect(
  "steers a changed turn-scoped selection into a running turn the provider could restart",
  () =>
    Effect.gen(function* () {
      const { started, steered, steeredSelections, layer, startFirstTurn } =
        yield* nextTurnSelectionHarness("steering-selection-change", true);
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const steer = (id: string, selection: ModelSelection) =>
          orchestrator
            .dispatch({
              type: "message.dispatch",
              commandId: CommandId.make(id),
              threadId,
              messageId: MessageId.make(`message:${id}`),
              text: id,
              attachments: [],
              modelSelection: selection,
              dispatchMode: { type: "steer_active", targetRunId: started[0]!.runId },
              createdBy: "user",
              creationSource: "web",
            })
            .pipe(Effect.andThen(worker.drain()));

        yield* steer("steer-changed", composerSelection);
        const changed = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(steered, ["steer-changed"]);
        assert.equal(started.length, 1);
        assert.lengthOf(changed.attempts, 1);
        assert.equal(changed.runs[0]?.status, "running");
        assert.deepEqual(changed.runs[0]?.modelSelection, runSelection);
        assert.deepEqual(changed.thread.modelSelection, composerSelection);

        yield* steer("steer-reverted", runSelection);
        const reverted = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(steered, ["steer-changed", "steer-reverted"]);
        assert.lengthOf(reverted.attempts, 1);
        assert.deepEqual(reverted.thread.modelSelection, runSelection);

        const otherSelection = {
          instanceId: ProviderInstanceId.make("codex-work"),
          model: "other",
        };
        const sink = yield* EventSink.EventSinkV2;
        yield* sink.write({
          events: [
            {
              id: EventId.make("switched-away"),
              type: "thread.provider-switched",
              threadId,
              providerInstanceId: otherSelection.instanceId,
              occurredAt: yield* DateTime.now,
              payload: {
                ...reverted.thread,
                providerInstanceId: otherSelection.instanceId,
                modelSelection: otherSelection,
              },
            },
          ],
        });
        yield* steer("steer-back", runSelection);
        const back = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(steered, ["steer-changed", "steer-reverted", "steer-back"]);
        // The adapter gets each steer's selection so it can switch the live turn, and back.
        assert.deepEqual(steeredSelections, [composerSelection, runSelection, runSelection]);
        assert.equal(back.thread.providerInstanceId, instanceId);
        assert.deepEqual(back.thread.modelSelection, runSelection);
      }).pipe(Effect.provide(layer));
    }),
);

it.effect("restarts the turn for an image when native steering takes text only", () =>
  Effect.gen(function* () {
    const { started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
      "steering-text-only-attachment",
      true,
      true,
      Effect.void,
      false,
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const before = yield* orchestrator.getThreadProjection(threadId);
      const originalStart = before.runs[0]!.startedAt;
      assert.isNotNull(originalStart);
      // A feedback card the run showed before the restart, still waiting for the user.
      const card = {
        type: "feedback_request.record" as const,
        threadId,
        runId: before.runs[0]!.id,
        nodeId: before.runs[0]!.rootNodeId!,
        turnItemId: TurnItemId.make("turn-item:feedback-request:steer"),
        preview: {
          kind: "bug" as const,
          subject: "general",
          expected: "a",
          actual: "b",
          workaround: "",
          trace: null,
        },
      };
      yield* orchestrator.dispatch({
        ...card,
        commandId: CommandId.make("feedback-pending"),
        feedbackStatus: "pending",
      });
      yield* TestClock.adjust("10 seconds");
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("steer-image"),
        threadId,
        messageId: MessageId.make("message:steer-image"),
        text: "look at this",
        attachments: [
          {
            type: "image",
            id: ChatAttachmentId.make("steer-image"),
            name: "screenshot.png",
            mimeType: "image/png",
            sizeBytes: 4,
          },
        ],
        modelSelection: runSelection,
        dispatchMode: { type: "steer_active", targetRunId: started[0]!.runId },
        createdBy: "user",
        creationSource: "web",
      });
      yield* worker.drain();
      const after = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(after.runs[0]?.id, before.runs[0]?.id);
      assert.deepEqual(after.runs[0]?.startedAt, originalStart);
      assert.equal(after.attempts[0]?.status, "superseded");
      const restartedAt = yield* DateTime.now;
      assert.deepEqual(after.attempts[1]?.startedAt, restartedAt);
      assert.deepEqual(
        after.nodes.find((node) => node.id === after.runs[0]?.rootNodeId)?.startedAt,
        restartedAt,
      );
      assert.deepEqual(steered, []);
      assert.deepEqual(
        started.map((turn) => [turn.message.text, turn.message.attachments.length]),
        [
          ["first", 0],
          ["look at this", 1],
        ],
      );
      // The restart replaced the root, and the user's answer can still close the card.
      assert.notEqual(after.runs[0]?.rootNodeId, card.nodeId);
      yield* orchestrator.dispatch({
        ...card,
        commandId: CommandId.make("feedback-sent"),
        feedbackStatus: "sent",
      });
      const closed = (yield* orchestrator.getThreadProjection(threadId)).turnItems.find(
        (item) => item.id === card.turnItemId,
      );
      assert.deepInclude(closed, {
        status: "completed",
        feedbackStatus: "sent",
        nodeId: card.nodeId,
      });
    }).pipe(Effect.provide(layer));
  }),
);

it.effect.each(["complete", "stop-before-end"] as const)(
  "starts a steer that missed the turn in its place on the saved next-turn selection: %s",
  (timing) =>
    Effect.gen(function* () {
      const { events, started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        `steering-selection-follow-up-${timing}`,
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const first = started[0]!;
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("steer"),
          threadId,
          messageId: MessageId.make("message:steer"),
          text: "late steer",
          attachments: [],
          modelSelection: composerSelection,
          dispatchMode: { type: "steer_active", targetRunId: first.runId },
          createdBy: "user",
          creationSource: "web",
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("queued-later"),
          threadId,
          messageId: MessageId.make("message:queued-later"),
          text: "queued later",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
        if (timing === "stop-before-end") {
          yield* orchestrator.dispatch({
            type: "run.interrupt",
            commandId: CommandId.make("stop"),
            threadId,
            runId: first.runId,
            holdQueue: true,
          });
        }
        const settled = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === first.runId &&
              event.payload.status === "waiting",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
        });
        yield* Queue.offer(events, {
          type: "turn.terminal",
          driver,
          providerThreadId: turn.providerThreadId,
          providerTurnId: turn.id,
          runOrdinal: first.runOrdinal,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* Fiber.join(settled);
        yield* worker.drain();
        if (timing === "stop-before-end") {
          const queued = (yield* orchestrator.getThreadProjection(threadId)).runs
            .filter((run) => run.status === "queued")
            .toSorted((left, right) => (left.queuePosition ?? 0) - (right.queuePosition ?? 0));
          assert.deepEqual(
            queued.map((run) => [run.userMessageId, run.queueHeld === true]),
            [
              [MessageId.make("message:steer"), false],
              [MessageId.make("message:queued-later"), false],
            ],
          );
          return;
        }
        yield* orchestrator.resumeQueuedRuns;
        yield* worker.drain();

        const projection = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(steered, []);
        assert.equal(started.length, 2);
        assert.equal(started[1]?.message.messageId, MessageId.make("message:steer"));
        assert.deepEqual(started[1]?.modelSelection, composerSelection);
        assert.deepEqual(projection.thread.modelSelection, composerSelection);
        const message = projection.turnItems.find(
          (item) =>
            item.type === "user_message" && item.messageId === MessageId.make("message:steer"),
        );
        assert.equal(message?.runId, started[1]!.runId);
        assert.isAbove(
          message!.ordinal,
          Math.max(
            ...projection.turnItems
              .filter((item) => item.runId === first.runId)
              .map((item) => item.ordinal),
          ),
        );
      }).pipe(Effect.provide(layer));
    }),
);

const declinedSteerTimings = [
  "complete",
  "stop-before-decline",
  "stop-after-decline",
  "queued-model",
  "two-steers",
] as const;

it.effect.each(declinedSteerTimings)(
  "persists a declined steer without blocking the worker: %s",
  (timing) =>
    Effect.gen(function* () {
      let beforeDecline: Effect.Effect<void> = Effect.void;
      const { events, started, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        `steering-declined-${timing}`,
        false,
        true,
        Effect.void,
        true,
        undefined,
        (turn) =>
          Effect.suspend(() => beforeDecline).pipe(
            Effect.andThen(
              Effect.fail(
                new ProviderAdapter.ProviderAdapterSteerDeclinedError({
                  driver,
                  providerThreadId: turn.providerThread.id,
                  providerTurnId: turn.providerTurnId,
                }),
              ),
            ),
          ),
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const first = started[0]!;
        const stop = orchestrator
          .dispatch({
            type: "run.interrupt",
            commandId: CommandId.make("stop"),
            threadId,
            runId: first.runId,
            holdQueue: true,
          })
          .pipe(Effect.asVoid, Effect.orDie);
        if (timing === "stop-before-decline") beforeDecline = stop;
        const steerSelection = { ...runSelection, model: "steer-model" };
        if (timing === "queued-model") {
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("queued-model"),
            threadId,
            messageId: MessageId.make("message:queued-model"),
            text: "already queued",
            attachments: [],
            modelSelection: composerSelection,
            dispatchMode: { type: "queue_after_active" },
            createdBy: "user",
            creationSource: "web",
          });
          beforeDecline = Effect.forEach(["queued-later", "steer-later", "queued-last"], (id) =>
            orchestrator.dispatch({
              type: "message.dispatch",
              commandId: CommandId.make(id),
              threadId,
              messageId: MessageId.make(`message:${id}`),
              text: id,
              attachments: [],
              dispatchMode:
                id === "steer-later"
                  ? { type: "steer_active", targetRunId: first.runId }
                  : { type: "queue_after_active" },
              createdBy: "user",
              creationSource: "web",
            }),
          ).pipe(Effect.asVoid, Effect.orDie);
        }
        if (timing === "two-steers") {
          // The second steer arrives before the first is declined.
          beforeDecline = orchestrator
            .dispatch({
              type: "message.dispatch",
              commandId: CommandId.make("steer-second"),
              threadId,
              messageId: MessageId.make("message:steer-second"),
              text: "second",
              attachments: [],
              dispatchMode: { type: "steer_active", targetRunId: first.runId },
              createdBy: "user",
              creationSource: "web",
            })
            .pipe(
              Effect.andThen(Effect.sync(() => (beforeDecline = Effect.void))),
              Effect.asVoid,
              Effect.orDie,
            );
        }
        const messageId = MessageId.make("message:declined");
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("steer"),
          threadId,
          messageId,
          text: "follow up",
          attachments: [],
          ...(timing === "queued-model" ? { modelSelection: steerSelection } : {}),
          dispatchMode: { type: "steer_active", targetRunId: first.runId },
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();
        const pending = yield* orchestrator.getThreadProjection(threadId);
        const queued = pending.runs.find(
          (run) => run.userMessageId === messageId && run.id !== first.runId,
        );
        const effects = yield* (yield* EffectOutbox.EffectOutboxV2).listByCommandId(
          CommandId.make("steer"),
        );
        assert.deepEqual(
          effects.map((effect) => [effect.request.type, effect.status]),
          [["provider-turn.steer", "succeeded"]],
        );
        assert.equal(started.length, 1);
        if (timing === "stop-before-decline") {
          assert.isUndefined(queued);
          return;
        }
        assert.equal(queued?.status, "queued");
        assert.equal(queued?.userMessageId, messageId);
        if (timing === "queued-model") {
          assert.deepEqual(queued?.modelSelection, steerSelection);
          assert.deepEqual(
            pending.runs
              .filter((run) => run.status === "queued")
              .toSorted(
                (left, right) =>
                  (left.queuePosition ?? left.ordinal) - (right.queuePosition ?? right.ordinal),
              )
              .map((run) => run.userMessageId),
            [
              MessageId.make("message:queued-model"),
              messageId,
              MessageId.make("message:queued-later"),
              MessageId.make("message:steer-later"),
              MessageId.make("message:queued-last"),
            ],
          );
          return;
        }
        if (timing === "two-steers") {
          assert.deepEqual(
            pending.runs
              .filter((run) => run.status === "queued")
              .toSorted(
                (left, right) =>
                  (left.queuePosition ?? left.ordinal) - (right.queuePosition ?? right.ordinal),
              )
              .map((run) => run.userMessageId),
            [messageId, MessageId.make("message:steer-second")],
          );
          return;
        }
        if (timing === "stop-after-decline") {
          yield* stop;
          yield* worker.drain();
          yield* orchestrator.resumeQueuedRuns;
          yield* worker.drain();
          const held = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(held.runs.find((run) => run.id === queued?.id)?.queueHeld, true);
          assert.equal(started.length, 1);
          return;
        }
        const settled = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === first.runId &&
              event.payload.status === "waiting",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        const turn = pending.providerTurns[0]!;
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
        });
        yield* Queue.offer(events, {
          type: "turn.terminal",
          driver,
          providerThreadId: turn.providerThreadId,
          providerTurnId: turn.id,
          runOrdinal: first.runOrdinal,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* Fiber.join(settled);
        yield* worker.drain();
        yield* orchestrator.resumeQueuedRuns;
        yield* worker.drain();
        assert.equal(started.length, 2);
        assert.equal(started[1]?.message.messageId, messageId);
        assert.equal(started[1]?.runId, queued?.id);
      }).pipe(Effect.provide(layer));
    }),
);

// regression: #41509 a declined steer of a delegated completion wake must still queue the wake
it.effect("queues a delegated completion wake whose steer the provider declined", () =>
  Effect.gen(function* () {
    const { events, started, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
      "steering-declined-delegated-completion",
      false,
      true,
      Effect.void,
      true,
      undefined,
      (turn) =>
        Effect.fail(
          new ProviderAdapter.ProviderAdapterSteerDeclinedError({
            driver,
            providerThreadId: turn.providerThread.id,
            providerTurnId: turn.providerTurnId,
          }),
        ),
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const first = started[0]!;
      const messageId = MessageId.make("message:declined-wake");
      const taskId = NodeId.make("task:declined-wake");
      const parentRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
        (run) => run.id === first.runId,
      )!;
      const now = yield* DateTime.now;
      yield* (yield* EventSink.EventSinkV2).write({
        events: [
          {
            id: EventId.make("event:declined-wake:cohort"),
            type: "run.updated",
            threadId,
            runId: first.runId,
            occurredAt: now,
            payload: {
              ...parentRun,
              delegatedCompletion: {
                disposition: "open",
                nextGeneration: 2,
                delivery: { generation: 1, messageId, taskIds: [taskId] },
              },
            },
          },
          {
            id: EventId.make("event:declined-wake:task"),
            type: "subagent.updated",
            threadId,
            runId: first.runId,
            nodeId: taskId,
            occurredAt: now,
            payload: {
              id: taskId,
              threadId,
              runId: first.runId,
              parentNodeId: first.rootNodeId,
              origin: "app_owned",
              createdBy: "agent",
              driver,
              providerInstanceId: instanceId,
              providerThreadId: null,
              childThreadId: null,
              nativeTaskRef: null,
              prompt: "Do background work",
              title: "Background test",
              model: null,
              completionWake: "always",
              completionDelivery: { state: "claimed", observedByRunId: null },
              status: "completed",
              result: "done",
              startedAt: now,
              completedAt: now,
              updatedAt: now,
            },
          },
        ],
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("wake"),
        threadId,
        messageId,
        text: "Delegated task completed",
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        delegatedCompletion: { parentRunId: first.runId, generation: 1, taskIds: [taskId] },
        createdBy: "agent",
        creationSource: "server",
      });
      yield* worker.drain();
      const effects = yield* (yield* EffectOutbox.EffectOutboxV2).listByCommandId(
        CommandId.make("wake"),
      );
      assert.deepEqual(
        effects.map((effect) => [effect.request.type, effect.status]),
        [["provider-turn.steer", "succeeded"]],
        "the declined steer's queue fallback was rejected",
      );
      const pending = yield* orchestrator.getThreadProjection(threadId);
      const queued = pending.runs.find(
        (run) => run.userMessageId === messageId && run.id !== first.runId,
      );
      assert.equal(queued?.status, "queued");

      const turn = pending.providerTurns[0]!;
      const settled = yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === first.runId &&
            event.payload.status === "waiting",
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      yield* Queue.offer(events, {
        type: "provider_turn.updated",
        driver,
        providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
      });
      yield* Queue.offer(events, {
        type: "turn.terminal",
        driver,
        providerThreadId: turn.providerThreadId,
        providerTurnId: turn.id,
        runOrdinal: first.runOrdinal,
        status: "completed",
        failure: null,
        threadDisposition: "reusable",
      });
      yield* Fiber.join(settled);
      yield* worker.drain();
      yield* orchestrator.resumeQueuedRuns;
      yield* worker.drain();
      assert.equal(started[1]?.message.messageId, messageId, "the parent was never woken");
    }).pipe(Effect.provide(layer));
  }),
);

// regression: #26695 cancelling a Copilot prompt to steer it kills its background agents
it.effect("queues a steer until the turn ends when the provider cannot steer it", () =>
  Effect.gen(function* () {
    const { events, started, steered, interrupted, layer, startFirstTurn } =
      yield* nextTurnSelectionHarness("steering-unsupported", false, false);
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const first = started[0]!;
      const messageId = MessageId.make("message:steer");
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("steer"),
        threadId,
        messageId,
        text: "follow up",
        attachments: [],
        modelSelection: runSelection,
        dispatchMode: { type: "start_immediately" },
        deliveryIntent: "steer",
        createdBy: "user",
        creationSource: "web",
      });
      yield* worker.drain();

      const held = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(steered, []);
      assert.deepEqual(interrupted, []);
      assert.equal(started.length, 1);
      assert.equal(held.runs.find((run) => run.id === first.runId)?.status, "running");
      const queued = held.runs.find((run) => run.userMessageId === messageId);
      assert.equal(queued?.status, "queued");

      const settled = yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === first.runId &&
            event.payload.status === "waiting",
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      const turn = held.providerTurns[0]!;
      yield* Queue.offer(events, {
        type: "provider_turn.updated",
        driver,
        providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
      });
      yield* Queue.offer(events, {
        type: "turn.terminal",
        driver,
        providerThreadId: turn.providerThreadId,
        providerTurnId: turn.id,
        runOrdinal: first.runOrdinal,
        status: "completed",
        failure: null,
        threadDisposition: "reusable",
      });
      yield* Fiber.join(settled);
      yield* worker.drain();
      yield* orchestrator.resumeQueuedRuns;
      yield* worker.drain();

      assert.deepEqual(interrupted, []);
      assert.equal(started.length, 2);
      assert.equal(started[1]?.runId, queued?.id);
      assert.equal(started[1]?.message.text, "follow up");
    }).pipe(Effect.provide(layer));
  }),
);

// regression: #39404 a later agent message steers ahead of one queued behind /compact
it.effect("delivers agent messages queued behind /compact before a later one steers", () =>
  Effect.gen(function* () {
    const { events, started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
      "agent-order-after-compact",
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const nextRunning = Effect.fn("nextRunning")(function* () {
        return yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
      });
      const lock = (yield* ThreadCommandExecutor.ThreadCommandExecutor).withLock(
        threadId,
        Effect.void,
      );
      const finishTurn = Effect.fn("finishTurn")(function* (index: number) {
        const turnInput = started[index]!;
        const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns.find(
          (candidate) => candidate.runAttemptId === turnInput.attemptId,
        )!;
        const settled = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === turnInput.runId &&
              (event.payload.status === "waiting" || event.payload.status === "completed"),
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
        });
        yield* Queue.offer(events, {
          type: "turn.terminal",
          driver,
          providerThreadId: turn.providerThreadId,
          providerTurnId: turn.id,
          runOrdinal: turnInput.runOrdinal,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* Fiber.join(settled);
        yield* worker.drain();
        yield* orchestrator.resumeQueuedRuns;
        yield* worker.drain();
      });
      yield* finishTurn(0);
      const compactRunning = yield* nextRunning();
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("compact"),
        threadId,
        messageId: MessageId.make("message:compact"),
        text: "/compact",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
      yield* worker.drain();
      yield* Fiber.join(compactRunning);
      yield* lock;
      const sendFromAgent = (codeword: string) =>
        Effect.gen(function* () {
          const active = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
            (run) => run.status === "running",
          )!;
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(`send-${codeword}`),
            threadId,
            messageId: MessageId.make(`message:${codeword}`),
            text: codeword,
            attachments: [],
            dispatchMode: { type: "steer_active", targetRunId: active.id },
            deliveryIntent: "auto",
            createdBy: "agent",
            creationSource: "mcp",
            senderThreadId: ThreadId.make("thread:orchestrator"),
          });
          yield* worker.drain();
        });
      yield* sendFromAgent("ALPHA");
      yield* sendFromAgent("BRAVO");
      const queued = (yield* orchestrator.getThreadProjection(threadId)).runs.filter(
        (run) => run.status === "queued",
      );
      assert.lengthOf(queued, 2);
      assert.deepEqual(steered, []);

      const alphaRunning = yield* nextRunning();
      yield* finishTurn(1);
      yield* Fiber.join(alphaRunning);
      yield* lock;
      assert.deepEqual(
        started.map((turn) => turn.message.text.split("\n").at(-1)),
        ["first", "/compact", "ALPHA"],
      );

      yield* sendFromAgent("CHARLIE");
      const delivered = [...steered, ...started.slice(3).map((turn) => turn.message.text)]
        .map((text) => text.split("\n").at(-1))
        .filter((text) => text === "BRAVO" || text === "CHARLIE");
      // Whatever reaches the agent so far does so in send order: CHARLIE never before BRAVO.
      assert.deepEqual(delivered, ["BRAVO", "CHARLIE"].slice(0, delivered.length));
    }).pipe(Effect.provide(layer));
  }),
);

// regression: #41455 one sender's queued message holds back another sender's automatic steer
it.effect("an agent's automatic send steers past another sender's queued message", () =>
  Effect.gen(function* () {
    const { started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
      "agent-steer-past-other-sender",
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const active = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
        (run) => run.status === "running",
      )!;
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("send-first"),
        threadId,
        messageId: MessageId.make("message:queued-first"),
        text: "queued first",
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "agent",
        creationSource: "mcp",
        senderThreadId: ThreadId.make("thread:sender-a"),
      });
      yield* worker.drain();
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("send-urgent"),
        threadId,
        messageId: MessageId.make("message:urgent"),
        text: "urgent",
        attachments: [],
        dispatchMode: { type: "steer_active", targetRunId: active.id },
        deliveryIntent: "auto",
        createdBy: "agent",
        creationSource: "mcp",
        senderThreadId: ThreadId.make("thread:sender-c"),
      });
      yield* worker.drain();

      assert.lengthOf(started, 1);
      assert.deepEqual(
        steered.map((text) => text.split("\n").at(-1)),
        ["urgent"],
      );
    }).pipe(Effect.provide(layer));
  }),
);

// regression: #33454 a steer that restarts a completion wake run left its cohort claimed forever
const restartCompletionWake = Effect.fn("restartCompletionWake")(function* (
  harness: Pick<
    Effect.Success<ReturnType<typeof nextTurnSelectionHarness>>,
    "events" | "started" | "startFirstTurn"
  >,
  siblingFinishesFirst: boolean,
  userRunBeforeWake = false,
  intentNamesTarget = false,
) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
  const sink = yield* EventSink.EventSinkV2;
  const threadId = yield* harness.startFirstTurn;
  const watchRun = (runId: string, status: string) =>
    orchestrator.streamDomainEvents.pipe(
      Stream.filter(
        (event) =>
          event.type === "run.updated" &&
          event.payload.id === runId &&
          event.payload.status === status,
      ),
      Stream.take(1),
      Stream.runDrain,
      Effect.forkScoped,
    );
  const watchTurnRunning = (ordinal: number) =>
    orchestrator.streamDomainEvents.pipe(
      Stream.filter(
        (event) =>
          event.type === "provider-turn.updated" &&
          event.payload.status === "running" &&
          event.payload.ordinal === ordinal,
      ),
      Stream.take(1),
      Stream.runDrain,
      Effect.forkScoped,
    );
  const finishTurn = (turn: ProviderAdapter.ProviderAdapterV2TurnInput) =>
    Effect.gen(function* () {
      const providerTurn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns.find(
        (candidate) => candidate.runAttemptId === turn.attemptId,
      )!;
      const settled = yield* watchRun(turn.runId, "waiting");
      yield* Queue.offer(harness.events, {
        type: "provider_turn.updated",
        driver,
        providerTurn: {
          ...providerTurn,
          status: "completed",
          completedAt: yield* DateTime.now,
        },
      });
      yield* Queue.offer(harness.events, {
        type: "turn.terminal",
        driver,
        providerThreadId: providerTurn.providerThreadId,
        providerTurnId: providerTurn.id,
        runOrdinal: turn.runOrdinal,
        status: "completed",
        failure: null,
        threadDisposition: "reusable",
      });
      yield* Fiber.join(settled);
      yield* worker.drain();
    });

  const origin = harness.started[0]!;
  const firstTaskId = NodeId.make("task:steered-wake-first");
  const siblingTaskId = NodeId.make("task:steered-wake-sibling");
  const now = yield* DateTime.now;
  yield* sink.write({
    events: [firstTaskId, siblingTaskId].map((taskId) => ({
      id: EventId.make(`event:seed:${taskId}`),
      type: "subagent.updated" as const,
      threadId,
      runId: origin.runId,
      nodeId: taskId,
      occurredAt: now,
      payload: {
        id: taskId,
        threadId,
        runId: origin.runId,
        parentNodeId: origin.rootNodeId,
        origin: "app_owned" as const,
        createdBy: "agent" as const,
        driver,
        providerInstanceId: instanceId,
        providerThreadId: null,
        childThreadId: null,
        remoteChild: {
          environmentId: EnvironmentId.make("environment:box"),
          threadId: ThreadId.make(`thread:remote:${taskId}`),
          label: "box",
        },
        nativeTaskRef: null,
        prompt: "Do background work",
        title: null,
        model: null,
        completionWake: "always" as const,
        completionDelivery: { state: "pending" as const, observedByRunId: null },
        status: "running" as const,
        result: null,
        startedAt: now,
        completedAt: null,
        updatedAt: now,
      },
    })),
  });
  yield* finishTurn(origin);
  // The wake queues behind a user run that started after the origin run finished.
  if (userRunBeforeWake) {
    const userRunning = yield* watchTurnRunning(2);
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("command:user-run"),
      threadId,
      messageId: MessageId.make("message:user-run"),
      text: "unrelated question",
      attachments: [],
      modelSelection: runSelection,
      dispatchMode: { type: "start_immediately" },
      createdBy: "user",
      creationSource: "web",
    });
    yield* worker.drain();
    yield* Fiber.join(userRunning);
  }

  const completeTask = (taskId: NodeId) =>
    orchestrator.dispatch({
      type: "delegated_task.remote.complete",
      commandId: CommandId.make(`command:remote-complete:${taskId}`),
      parentThreadId: threadId,
      taskId,
      status: "completed",
      result: `${taskId} finished`,
      stoppedThere: "ran-out",
    });
  yield* completeTask(firstTaskId);
  const firstDelivery = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
    (run) => run.id === origin.runId,
  )!.delegatedCompletion!.delivery!;
  assert.deepEqual(firstDelivery.taskIds, [firstTaskId]);
  yield* orchestrator.dispatch({
    type: "message.dispatch",
    commandId: CommandId.make("command:first-wake"),
    threadId,
    messageId: firstDelivery.messageId,
    text: "Delegated task completed",
    attachments: [],
    modelSelection: runSelection,
    dispatchMode: { type: "queue_after_active" },
    delegatedCompletion: {
      parentRunId: origin.runId,
      generation: firstDelivery.generation,
      taskIds: firstDelivery.taskIds,
    },
    createdBy: "agent",
    creationSource: "server",
  });
  const wakeOrdinal = userRunBeforeWake ? 3 : 2;
  const wakeRunning = yield* watchTurnRunning(wakeOrdinal);
  if (userRunBeforeWake) yield* finishTurn(harness.started[1]!);
  yield* orchestrator.resumeQueuedRuns;
  yield* worker.drain();
  yield* Fiber.join(wakeRunning);
  const wake = harness.started[wakeOrdinal - 1]!;
  assert.equal(wake.message.messageId, firstDelivery.messageId);
  if (userRunBeforeWake) {
    // The wake's run holds a compaction, so the origin run's records are older than the window.
    const compactedAt = yield* DateTime.now;
    yield* sink.write({
      events: [
        {
          id: EventId.make("event:wake-compaction"),
          type: "turn-item.updated" as const,
          threadId,
          runId: wake.runId,
          nodeId: wake.rootNodeId,
          occurredAt: compactedAt,
          payload: {
            id: TurnItemId.make("item:wake-compaction"),
            threadId,
            runId: wake.runId,
            nodeId: wake.rootNodeId,
            providerThreadId: null,
            providerTurnId: null,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: 1_000,
            status: "completed" as const,
            title: null,
            startedAt: compactedAt,
            completedAt: compactedAt,
            updatedAt: compactedAt,
            type: "compaction" as const,
            driver,
          },
        },
      ],
    });
  }

  if (siblingFinishesFirst) {
    yield* completeTask(siblingTaskId);
  }

  // The provider cannot steer natively, so the steer restarts the wake run.
  const steeredRunning = yield* watchTurnRunning(wakeOrdinal + 1);
  yield* orchestrator.dispatch({
    type: "message.dispatch",
    commandId: CommandId.make("command:steer-wake"),
    threadId,
    messageId: MessageId.make("message:steer-wake"),
    text: "also check the logs",
    attachments: [],
    modelSelection: runSelection,
    ...(intentNamesTarget
      ? { dispatchMode: { type: "start_immediately" as const }, deliveryIntent: "restart" as const }
      : { dispatchMode: { type: "steer_active" as const, targetRunId: wake.runId } }),
    createdBy: "user",
    creationSource: "web",
  });
  yield* worker.drain();
  yield* Fiber.join(steeredRunning);
  const steered = harness.started[wakeOrdinal]!;
  assert.equal(steered.runId, wake.runId);
  assert.equal(
    (yield* orchestrator.getThreadProjection(threadId)).attempts.find(
      (attempt) => attempt.id === steered.attemptId,
    )?.reason,
    "steering_restart",
  );
  return {
    threadId,
    origin,
    wake,
    steered,
    firstTaskId,
    siblingTaskId,
    watchRun,
    finishTurn,
    completeTask,
  };
});

it.effect("wakes the parent for a sibling result after a steer restarts the completion wake", () =>
  Effect.gen(function* () {
    const harness = yield* nextTurnSelectionHarness("steered-completion-wake", true, false);
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const {
        threadId,
        origin,
        wake,
        steered,
        firstTaskId,
        siblingTaskId,
        watchRun,
        finishTurn,
        completeTask,
      } = yield* restartCompletionWake(harness, false);

      // A follow-up queued behind the wake starts once the terminal-run listener has settled it.
      const followUp = MessageId.make("message:follow-up");
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("command:follow-up"),
        threadId,
        messageId: followUp,
        text: "follow up",
        attachments: [],
        modelSelection: runSelection,
        dispatchMode: { type: "queue_after_active" },
        createdBy: "user",
        creationSource: "web",
      });
      const followUpRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
        (run) => run.userMessageId === followUp,
      )!;
      const followUpStarted = yield* watchRun(followUpRun.id, "starting");
      yield* finishTurn(steered);
      yield* Fiber.join(followUpStarted);
      assert.equal(
        (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.id === wake.runId,
        )?.status,
        "completed",
      );

      yield* completeTask(siblingTaskId);
      const final = yield* orchestrator.getThreadProjection(threadId);
      const tasks = new Map(
        final.subagents.map((task) => [task.id, task.completionDelivery?.state]),
      );
      assert.deepInclude(
        final.runs.find((run) => run.id === origin.runId)?.delegatedCompletion?.delivery ?? {},
        { generation: 2, taskIds: [siblingTaskId] },
        "the sibling finished after the steered wake but no new completion wake was reserved",
      );
      assert.equal(tasks.get(siblingTaskId), "claimed");
      assert.equal(
        tasks.get(firstTaskId),
        "delivered",
        "the steered wake run's own task stays claimed after that run completed",
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect.each([false, true])(
  "settles a steered completion wake whose parent run is before the last compaction (restart by intent: %s)",
  (intentNamesTarget) =>
    Effect.gen(function* () {
      const harness = yield* nextTurnSelectionHarness(
        `steered-wake-after-compaction-${intentNamesTarget}`,
        true,
        false,
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const { threadId, steered, firstTaskId, finishTurn } = yield* restartCompletionWake(
          harness,
          false,
          true,
          intentNamesTarget,
        );
        yield* finishTurn(steered);
        const tasks = (yield* orchestrator.getThreadProjection(threadId)).subagents;
        assert.equal(
          tasks.find((task) => task.id === firstTaskId)?.completionDelivery?.state,
          "delivered",
        );
      }).pipe(Effect.provide(harness.layer));
    }),
);

it.effect(
  "offers the next completion wake when a steer restarts a wake a sibling finished behind",
  () =>
    Effect.gen(function* () {
      const harness = yield* nextTurnSelectionHarness(
        "steered-completion-wake-behind",
        true,
        false,
      );
      const offers: ProviderContinuationRequests.ProviderContinuationRequest[] = [];
      const requests = Layer.succeed(ProviderContinuationRequests.ProviderContinuationRequests, {
        offer: (request) => Effect.sync(() => void offers.push(request)),
        take: Effect.never,
      });
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const { threadId, origin, firstTaskId, siblingTaskId } = yield* restartCompletionWake(
          harness,
          true,
        );
        const projection = yield* orchestrator.getThreadProjection(threadId);
        const delivery = projection.runs.find((run) => run.id === origin.runId)?.delegatedCompletion
          ?.delivery;
        assert.deepInclude(delivery ?? {}, { generation: 2, taskIds: [siblingTaskId] });
        const tasks = new Map(
          projection.subagents.map((task) => [task.id, task.completionDelivery?.state]),
        );
        assert.equal(tasks.get(firstTaskId), "delivered");
        assert.equal(tasks.get(siblingTaskId), "claimed");
        assert.deepEqual(offers.at(-1)?.delegatedCompletion, {
          parentRunId: origin.runId,
          generation: 2,
          messageId: delivery!.messageId,
        });
      }).pipe(Effect.provide(harness.layer.pipe(Layer.provide(requests))));
    }),
);

// Codex flushes held drafts after the final answer; reserve their pre-steer position before turn end.
it.effect("keeps a held commentary draft before the steer when turn end flushes it", () =>
  Effect.gen(function* () {
    const { events, started, layer, startFirstTurn } =
      yield* nextTurnSelectionHarness("steering-held-draft");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const first = started[0]!;
      const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
      const now = yield* DateTime.now;
      const assistantItem = (id: string, text: string, streaming: boolean) =>
        ({
          type: "turn_item.updated",
          driver,
          turnItem: {
            id: TurnItemId.make(id),
            threadId,
            runId: first.runId,
            nodeId: first.rootNodeId,
            providerThreadId: turn.providerThreadId,
            providerTurnId: turn.id,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: 0,
            status: streaming ? "running" : "completed",
            title: null,
            startedAt: now,
            completedAt: streaming ? null : now,
            updatedAt: now,
            type: "assistant_message",
            messageId: MessageId.make(`message:${id}`),
            text,
            streaming,
          },
        }) satisfies ProviderAdapter.ProviderAdapterV2Event;
      const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
        orchestrator.streamDomainEvents.pipe(
          Stream.filter(predicate),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );

      yield* Queue.offer(events, assistantItem("draft", "Let me look at the", true));
      const commentaryWritten = yield* watch(
        (event) => event.type === "turn-item.updated" && event.payload.id === "commentary",
      );
      yield* Queue.offer(events, assistantItem("commentary", "Checking the popover.", false));
      yield* Fiber.join(commentaryWritten);

      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("steer"),
        threadId,
        messageId: MessageId.make("message:steer"),
        text: "fix the popover",
        attachments: [],
        dispatchMode: { type: "steer_active", targetRunId: first.runId },
        createdBy: "user",
        creationSource: "web",
      });
      yield* worker.drain();

      const settled = yield* watch(
        (event) =>
          event.type === "run.updated" &&
          event.payload.id === first.runId &&
          event.payload.status === "waiting",
      );
      yield* Queue.offer(events, assistantItem("answer", "All fixed.", false));
      yield* Queue.offer(events, assistantItem("draft", "Let me look at the", false));
      yield* Queue.offer(events, {
        type: "provider_turn.updated",
        driver,
        providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
      });
      yield* Queue.offer(events, {
        type: "turn.terminal",
        driver,
        providerThreadId: turn.providerThreadId,
        providerTurnId: turn.id,
        runOrdinal: first.runOrdinal,
        status: "completed",
        failure: null,
        threadDisposition: "reusable",
      });
      yield* Fiber.join(settled);
      yield* worker.drain();

      const items = (yield* orchestrator.getThreadProjection(threadId)).turnItems
        .toSorted((left, right) => left.ordinal - right.ordinal)
        .flatMap((item) =>
          item.type === "user_message"
            ? [String(item.messageId)]
            : item.type === "assistant_message"
              ? [String(item.id)]
              : [],
        );
      assert.deepEqual(items, ["message:first", "draft", "commentary", "message:steer", "answer"]);
    }).pipe(Effect.provide(layer));
  }),
);

it.effect.each(["auto", "deferred", "promote-edited"] as const)(
  "rejects expanded steering input before commit: %s",
  (delivery) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { started, steered, interrupted, layer, startFirstTurn } =
          yield* nextTurnSelectionHarness(`oversized-steer-${delivery}`, true);
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const outbox = yield* EffectOutbox.EffectOutboxV2;
          const sink = yield* EventSink.EventSinkV2;
          const threadId = yield* startFirstTurn;
          const first = started[0]!;
          const citation = serializeAssistantCitation({
            version: 1,
            environmentId: EnvironmentId.make("env"),
            threadId,
            messageId: MessageId.make("assistant"),
            text: "\u0001".repeat(8_000),
            start: 0,
            end: 8_000,
            prefix: "",
            suffix: "",
          });
          const text = `${"x".repeat(80_000)}${citation}`;
          // Context excerpts are not part of the limit, matching the web composer's check.
          const largeContextText =
            "Read [one](t3-context://v1/terminal/one) and [two](t3-context://v1/terminal/two)";
          const context = {
            version: 1 as const,
            records: (["one", "two"] as const).map((id) => ({
              version: 1 as const,
              kind: "terminal" as const,
              contextId: ComposerContextId.make(id),
              label: id,
              terminalId: "terminal",
              terminalLabel: "Terminal",
              lineStart: 1,
              lineEnd: 1,
              text: "y".repeat(COMPOSER_CONTEXT_TERMINAL_TEXT_MAX_CHARS),
            })),
          };
          assert.isBelow(text.length, PROVIDER_SEND_TURN_MAX_INPUT_CHARS);
          assert.isAbove(
            expandAssistantCitationsForProvider(text).length,
            PROVIDER_SEND_TURN_MAX_INPUT_CHARS,
          );
          assert.isAbove(
            projectComposerContextForProvider({
              text: largeContextText,
              records: context.records,
            }).length,
            PROVIDER_SEND_TURN_MAX_INPUT_CHARS,
          );
          if (delivery === "deferred") {
            const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
            yield* sink.write({
              events: [
                {
                  id: EventId.make("turn-not-ready"),
                  type: "provider-turn.updated",
                  threadId,
                  occurredAt: yield* DateTime.now,
                  payload: { ...turn, status: "pending" },
                },
              ],
            });
          }
          const commandId = CommandId.make("oversized-steer");
          let command: Parameters<typeof orchestrator.dispatch>[0] = {
            type: "message.dispatch",
            commandId,
            threadId,
            messageId: MessageId.make("oversized"),
            text,
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            deliveryIntent: delivery === "deferred" ? "steer" : "auto",
            createdBy: "user",
            creationSource: "mobile",
          };
          if (delivery === "promote-edited") {
            yield* orchestrator.dispatch({
              ...command,
              commandId: CommandId.make("queue"),
              text: "Initially valid queued message",
              dispatchMode: { type: "queue_after_active" },
              deliveryIntent: undefined,
            });
            const queued = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
              (run) => run.status === "queued",
            )!;
            yield* orchestrator.dispatch({
              type: "queued-run.edit",
              commandId: CommandId.make("edit"),
              threadId,
              runId: queued.id,
              text,
            });
            command = {
              type: "queued-message.promote-to-steer",
              commandId,
              threadId,
              queuedRunId: queued.id,
              targetRunId: first.runId,
            };
          }
          const before = yield* orchestrator.getThreadProjection(threadId);
          const sequence = yield* orchestrator.getThreadEventSequence(threadId);
          const error = yield* orchestrator.dispatch(command).pipe(Effect.flip);
          assert.instanceOf(error, Orchestrator.OrchestratorCommandRejectedError);
          assert.equal(
            userFacingDispatchErrorMessage(error),
            "Message exceeds 120,000 characters after expanding quotes. Shorten it and try again.",
          );
          assert.deepEqual(yield* outbox.listByCommandId(commandId), []);
          assert.deepEqual(yield* sink.readByCommandId({ commandId }).pipe(Stream.runCollect), []);
          yield* worker.drain();
          assert.equal(yield* orchestrator.getThreadEventSequence(threadId), sequence);
          assert.deepEqual(yield* orchestrator.getThreadProjection(threadId), before);
          assert.deepEqual(steered, []);
          assert.deepEqual(interrupted, []);
          assert.lengthOf(started, 1);
          if (delivery === "auto") {
            yield* orchestrator.dispatch({
              type: "message.dispatch",
              commandId: CommandId.make("large-context-steer"),
              threadId,
              messageId: MessageId.make("large-context"),
              text: largeContextText,
              context,
              attachments: [],
              dispatchMode: { type: "start_immediately" },
              deliveryIntent: "auto",
              createdBy: "user",
              creationSource: "mobile",
            });
            yield* worker.drain();
            assert.lengthOf(steered, 1);
          }
        }).pipe(Effect.provide(layer));
      }),
    ),
);

it.effect("steers a promoted queued message with the selection it was queued with", () =>
  Effect.gen(function* () {
    const { started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
      "promote-queued-selection",
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("queued"),
        threadId,
        messageId: MessageId.make("message:queued"),
        text: "queued",
        attachments: [],
        modelSelection: composerSelection,
        dispatchMode: { type: "queue_after_active" },
        createdBy: "user",
        creationSource: "web",
      });
      const queuedRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
        (run) => run.status === "queued",
      )!;
      yield* orchestrator.dispatch({
        type: "queued-message.promote-to-steer",
        commandId: CommandId.make("promote"),
        threadId,
        queuedRunId: queuedRun.id,
        targetRunId: started[0]!.runId,
      });
      yield* worker.drain();

      const projection = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(steered, ["queued"]);
      assert.deepEqual(projection.thread.modelSelection, composerSelection);
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("steers a promoted scheduled delivery with the thread's current selection", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        "promote-scheduled-selection",
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        // Bound scheduled tasks send like ScheduledTaskService: no model of their own.
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("scheduled"),
          threadId,
          messageId: MessageId.make("message:scheduled"),
          scheduledTaskId: ScheduledTaskId.make("task:scheduled"),
          text: "scheduled",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
        const queuedRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.status === "queued",
        )!;
        assert.deepEqual(queuedRun.modelSelection, runSelection);
        yield* orchestrator.dispatch({
          type: "thread.model-selection.set",
          commandId: CommandId.make("select-composer"),
          threadId,
          modelSelection: composerSelection,
        });
        yield* orchestrator.dispatch({
          type: "queued-message.promote-to-steer",
          commandId: CommandId.make("promote"),
          threadId,
          queuedRunId: queuedRun.id,
          targetRunId: started[0]!.runId,
        });
        yield* worker.drain();

        const projection = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(steered, [
          '[T3 Code: scheduled task "task:scheduled" sent this prompt, not the user typing in this chat.]\n\nscheduled',
        ]);
        assert.deepEqual(projection.thread.modelSelection, composerSelection);
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect("steers a deferred agent message on a pick made while it waited", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { started, steered, steeredSelections, layer, startFirstTurn, events } =
        yield* nextTurnSelectionHarness("deferred-agent-steer-selection");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
        yield* (yield* EventSink.EventSinkV2).write({
          events: [
            {
              id: EventId.make("turn-not-ready"),
              type: "provider-turn.updated",
              threadId,
              occurredAt: yield* DateTime.now,
              payload: { ...turn, status: "pending" },
            },
          ],
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("agent-steer"),
          threadId,
          messageId: MessageId.make("message:agent-steer"),
          text: "agent steer",
          attachments: [],
          dispatchMode: { type: "steer_active", targetRunId: started[0]!.runId },
          createdBy: "agent",
          creationSource: "mcp",
        });
        const deferred = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.status === "queued",
        )!;
        assert.equal(deferred.steerWhenReadyRunId, started[0]!.runId);
        yield* orchestrator.dispatch({
          type: "thread.model-selection.set",
          commandId: CommandId.make("select-composer"),
          threadId,
          modelSelection: composerSelection,
        });
        const promoted = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === deferred.id &&
              event.payload.status === "cancelled",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* Queue.offer(events, { type: "provider_turn.updated", driver, providerTurn: turn });
        yield* Fiber.join(promoted);
        yield* worker.drain();

        assert.lengthOf(steered, 1);
        assert.deepEqual(steeredSelections, [composerSelection]);
        assert.deepEqual(
          (yield* orchestrator.getThreadProjection(threadId)).thread.modelSelection,
          composerSelection,
        );
      }).pipe(Effect.provide(layer));
    }),
  ),
);

const claudeSelection = {
  instanceId: ProviderInstanceId.make("claudeAgent"),
  model: "claude-test",
};
const claudeAdapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
  instanceId: claudeSelection.instanceId,
  driver: ProviderDriverKind.make("claudeAgent"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
  openSession: () => Effect.die("the Claude instance never starts a turn here"),
};

// Queues a scheduled delivery behind the Codex turn, stops that turn (Stop holds the queue), then
// switches the thread to Claude: a thread changes instance only while no run is active.
const scheduledDeliveryAfterClaudeSwitch = (name: string, text: string) =>
  Effect.gen(function* () {
    const harness = yield* nextTurnSelectionHarness(
      name,
      false,
      true,
      Effect.void,
      true,
      undefined,
      undefined,
      [claudeAdapter],
    );
    const queueAndSwitch = Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = yield* harness.startFirstTurn;
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("scheduled"),
        threadId,
        messageId: MessageId.make("message:scheduled"),
        scheduledTaskId: ScheduledTaskId.make("task:scheduled"),
        text,
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "user",
        creationSource: "web",
      });
      const queuedRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
        (run) => run.status === "queued",
      )!;
      assert.equal(queuedRun.providerInstanceId, instanceId);
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const first = harness.started[0]!;
      yield* orchestrator.dispatch({
        type: "run.interrupt",
        commandId: CommandId.make("stop"),
        threadId,
        runId: first.runId,
        holdQueue: true,
      });
      yield* worker.drain();
      const stopped = (yield* orchestrator.getThreadProjection(threadId)).runs;
      assert.equal(stopped.find((run) => run.id === first.runId)?.status, "interrupted");
      const held = stopped.find((run) => run.id === queuedRun.id);
      assert.equal(held?.status, "queued");
      assert.isTrue(held?.queueHeld);
      yield* orchestrator.dispatch({
        type: "thread.model-selection.set",
        commandId: CommandId.make("select-claude"),
        threadId,
        modelSelection: claudeSelection,
      });
      return { threadId, queuedRun };
    });
    return { ...harness, queueAndSwitch };
  });

it.effect("checks an edited scheduled /goal against the thread's current provider", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { layer, queueAndSwitch } = yield* scheduledDeliveryAfterClaudeSwitch(
        "edit-scheduled-goal",
        "scheduled",
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const { threadId, queuedRun } = yield* queueAndSwitch;
        const editError = yield* orchestrator
          .dispatch({
            type: "queued-run.edit",
            commandId: CommandId.make("edit"),
            threadId,
            runId: queuedRun.id,
            text: "/goal pause",
          })
          .pipe(Effect.flip);

        assert.match(String(editError.cause), /Claude goals cannot be paused or resumed/);
        assert.equal(
          (yield* orchestrator.getThreadProjection(threadId)).messages.find(
            (message) => message.id === queuedRun.userMessageId,
          )?.text,
          "scheduled",
        );
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect("checks a queued scheduled /goal against the thread's current provider at start", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { started, layer, queueAndSwitch } = yield* scheduledDeliveryAfterClaudeSwitch(
        "start-scheduled-goal",
        "/goal pause",
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const { threadId, queuedRun } = yield* queueAndSwitch;
        yield* orchestrator.dispatch({
          type: "queue.resume",
          commandId: CommandId.make("resume"),
          threadId,
        });
        yield* orchestrator.resumeQueuedRuns;
        yield* worker.drain();

        const projection = yield* orchestrator.getThreadProjection(threadId);
        const failure = projection.turnItems.find(
          (item) => item.type === "error" && item.runId === queuedRun.id,
        );
        assert.equal(started.length, 1);
        assert.equal(projection.runs.find((run) => run.id === queuedRun.id)?.status, "failed");
        assert.equal(
          failure?.type === "error" ? failure.failure.code : null,
          "queued_start_failed",
        );
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect("marks the model switch on a promoted message that restarts the turn", () =>
  Effect.gen(function* () {
    const { started, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
      "promote-restart-switch",
      true,
      false,
    );
    const otherSelection = { ...runSelection, model: "other-model" };
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("queued"),
        threadId,
        messageId: MessageId.make("message:queued"),
        text: "queued",
        attachments: [],
        modelSelection: otherSelection,
        dispatchMode: { type: "queue_after_active" },
        createdBy: "user",
        creationSource: "web",
      });
      const queuedRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
        (run) => run.status === "queued",
      )!;
      yield* orchestrator.dispatch({
        type: "queued-message.promote-to-steer",
        commandId: CommandId.make("promote"),
        threadId,
        queuedRunId: queuedRun.id,
        targetRunId: started[0]!.runId,
      });
      yield* worker.drain();

      const promoted = (yield* orchestrator.getThreadProjection(threadId)).turnItems.find(
        (item) => item.type === "user_message" && item.messageId === "message:queued",
      );
      assert.deepEqual(promoted?.type === "user_message" ? promoted.modelSwitch : undefined, {
        from: runSelection,
        to: otherSelection,
      });
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("keeps a queued message queued when promoted into a run that Stop reached", () =>
  Effect.gen(function* () {
    const { started, steered, layer, startFirstTurn } =
      yield* nextTurnSelectionHarness("steering-after-stop");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const first = started[0]!;
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("queued"),
        threadId,
        messageId: MessageId.make("message:queued"),
        text: "queued",
        attachments: [],
        modelSelection: runSelection,
        dispatchMode: { type: "queue_after_active" },
        createdBy: "user",
        creationSource: "web",
      });
      const queuedRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
        (run) => run.status === "queued",
      )!;
      // The worker has not delivered the interrupt yet, so the run still reads as running.
      yield* orchestrator.dispatch({
        type: "run.interrupt",
        commandId: CommandId.make("stop"),
        threadId,
        runId: first.runId,
        holdQueue: true,
      });

      const promoteError = yield* orchestrator
        .dispatch({
          type: "queued-message.promote-to-steer",
          commandId: CommandId.make("promote"),
          threadId,
          queuedRunId: queuedRun.id,
          targetRunId: first.runId,
        })
        .pipe(Effect.flip);
      yield* worker.drain();

      const projection = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(promoteError._tag, "OrchestratorDispatchError");
      assert.deepEqual(steered, []);
      const queued = projection.runs.find((run) => run.id === queuedRun.id);
      assert.equal(queued?.status, "queued");
      assert.isTrue(queued?.queueHeld);
    }).pipe(Effect.provide(layer));
  }),
);

// regression: #32891 a restart that Stop already reached is lost
it.effect.each([false, true])(
  "refuses a steer after Stop (provider turn ready: %s)",
  (providerTurnReady) =>
    Effect.gen(function* () {
      const { started, steered, layer, startFirstTurn } =
        yield* nextTurnSelectionHarness("restart-after-stop");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const first = started[0]!;
        // The worker has not delivered the interrupt yet, so the run still reads as running.
        yield* orchestrator.dispatch({
          type: "run.interrupt",
          commandId: CommandId.make("stop"),
          threadId,
          runId: first.runId,
        });
        const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
        if (!providerTurnReady) {
          yield* (yield* EventSink.EventSinkV2).write({
            events: [
              {
                id: EventId.make("turn-not-ready"),
                type: "provider-turn.updated",
                threadId,
                occurredAt: yield* DateTime.now,
                payload: { ...turn, status: "pending" },
              },
            ],
          });
        }
        const steerError = yield* orchestrator
          .dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("steer"),
            threadId,
            messageId: MessageId.make("message:steer"),
            text: "steer",
            attachments: [],
            modelSelection: runSelection,
            dispatchMode: { type: "steer_active", targetRunId: first.runId },
            createdBy: "user",
            creationSource: "web",
          })
          .pipe(Effect.flip);
        assert.equal(steerError._tag, "OrchestratorDispatchError");
        assert.include(String(steerError.cause), "is stopping and cannot be steered");
        if (!providerTurnReady) {
          yield* (yield* EventSink.EventSinkV2).write({
            events: [
              {
                id: EventId.make("restore-ready-turn"),
                type: "provider-turn.updated",
                threadId,
                occurredAt: yield* DateTime.now,
                payload: turn,
              },
            ],
          });
        }
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("restart"),
          threadId,
          messageId: MessageId.make("message:restart"),
          text: "restart",
          attachments: [],
          modelSelection: runSelection,
          dispatchMode: { type: "restart_active", targetRunId: first.runId },
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();

        assert.deepEqual(steered, []);
        assert.deepEqual(
          started.map((turn) => turn.message.text),
          ["first", "restart"],
        );
      }).pipe(Effect.provide(layer));
    }),
);

// regression: #32891 a restart that arrives after the turn ended is lost
it.effect.each([
  { status: "completed", settled: true },
  { status: "completed", settled: false },
  { status: "interrupted", settled: true },
  { status: "interrupted", settled: false },
  { status: "cancelled", settled: false },
] as const)(
  "starts a restart that missed the turn ($status, run settled: $settled) as a new turn",
  ({ status, settled: runSettled }) =>
    Effect.gen(function* () {
      const { events, started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        `restart-after-${status}-${runSettled ? "settled" : "unsettled"}`,
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const first = started[0]!;
        const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
        const turnEnded = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.id === turn.id &&
              event.payload.status === status,
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...turn, status, completedAt: yield* DateTime.now },
        });
        yield* Fiber.join(turnEnded);
        const settleRun = Effect.gen(function* () {
          const settled = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.payload.id === first.runId &&
                event.payload.status !== "running",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* Queue.offer(events, {
            type: "turn.terminal",
            driver,
            providerThreadId: turn.providerThreadId,
            providerTurnId: turn.id,
            runOrdinal: first.runOrdinal,
            status,
            failure: null,
            threadDisposition: "reusable",
          });
          yield* Fiber.join(settled);
          yield* worker.drain();
        });
        if (runSettled) yield* settleRun;

        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("restart"),
          threadId,
          messageId: MessageId.make("message:restart"),
          text: "restart",
          attachments: [],
          modelSelection: runSelection,
          dispatchMode: { type: "restart_active", targetRunId: first.runId },
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();
        if (!runSettled) {
          const restartLeftQueue = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.payload.userMessageId === "message:restart" &&
                event.payload.status !== "queued",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* settleRun;
          yield* Fiber.join(restartLeftQueue);
          yield* worker.drain();
        }

        assert.deepEqual(steered, []);
        assert.deepEqual(
          started.map((turn) => turn.message.text),
          ["first", "restart"],
        );
      }).pipe(Effect.provide(layer));
    }),
);

// regression: #32891 a restart into a failed provider turn is lost
it.effect("queues a restart sent into a provider turn that failed before its run settled", () =>
  Effect.gen(function* () {
    const { events, started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
      "restart-after-failed-unsettled",
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const first = started[0]!;
      const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
      const turnFailed = yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "provider-turn.updated" &&
            event.payload.id === turn.id &&
            event.payload.status === "failed",
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      yield* Queue.offer(events, {
        type: "provider_turn.updated",
        driver,
        providerTurn: { ...turn, status: "failed", completedAt: yield* DateTime.now },
      });
      yield* Fiber.join(turnFailed);

      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("restart"),
        threadId,
        messageId: MessageId.make("message:restart"),
        text: "restart",
        attachments: [],
        modelSelection: runSelection,
        dispatchMode: { type: "restart_active", targetRunId: first.runId },
        createdBy: "user",
        creationSource: "web",
      });
      yield* worker.drain();

      assert.deepEqual(steered, []);
      const projection = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(
        projection.runs.find((run) => run.userMessageId === "message:restart")?.status,
        "queued",
      );
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("queues a plain send behind the stopped queue and releases its hold", () =>
  Effect.gen(function* () {
    const interruptReached = yield* Deferred.make<void>();
    const releaseInterrupt = yield* Deferred.make<void>();
    const { started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
      "plain-send-after-stop",
      false,
      true,
      Deferred.succeed(interruptReached, undefined).pipe(
        Effect.andThen(Deferred.await(releaseInterrupt)),
      ),
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const send = (id: string, delivery: "queue" | "auto") =>
        orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(id),
          threadId,
          messageId: MessageId.make(`message:${id}`),
          text: id,
          attachments: [],
          modelSelection: runSelection,
          ...(delivery === "queue"
            ? { dispatchMode: { type: "queue_after_active" } }
            : { dispatchMode: { type: "start_immediately" }, deliveryIntent: "auto" }),
          createdBy: "user",
          creationSource: "web",
        });
      yield* send("queued", "queue");
      yield* orchestrator.dispatch({
        type: "run.interrupt",
        commandId: CommandId.make("stop"),
        threadId,
        runId: started[0]!.runId,
        holdQueue: true,
      });
      // The provider has not stopped yet, so a plain send still resolves to a steer.
      yield* send("sent", "auto");
      // Once the stop settles, the run-update reactor promotes the queue on
      // its own fiber, so read the queue while the provider is still stopping.
      const draining = yield* worker.drain().pipe(Effect.forkScoped);
      yield* Deferred.await(interruptReached);

      const projection = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(started.length, 1);
      const queued = projection.runs
        .filter((run) => run.status === "queued")
        .sort((left, right) => (left.queuePosition ?? 0) - (right.queuePosition ?? 0));
      assert.deepEqual(
        queued.map(
          (run) => projection.messages.find((message) => message.id === run.userMessageId)?.text,
        ),
        ["queued", "sent"],
      );
      // The user sent it, so the queue they stopped runs again once the provider stops.
      assert.isFalse(queued.some((run) => run.queueHeld === true));
      yield* Deferred.succeed(releaseInterrupt, undefined);
      yield* Fiber.join(draining);
      assert.deepEqual(steered, []);
    }).pipe(Effect.provide(layer));
  }),
);

const goalHarness = Effect.fn("goalHarness")(function* (name: string, nativeGoals?: boolean) {
  const harness = yield* nextTurnSelectionHarness(
    name,
    false,
    true,
    Effect.void,
    true,
    nativeGoals,
  );
  const send = (id: string, text: string, delivery: "queue" | "auto", freshContext = false) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(id),
        threadId: ThreadId.make(`thread:${name}`),
        messageId: MessageId.make(`message:${id}`),
        text,
        attachments: [],
        modelSelection: runSelection,
        ...(freshContext ? { freshContext } : {}),
        ...(delivery === "queue"
          ? { dispatchMode: { type: "queue_after_active" } }
          : { dispatchMode: { type: "start_immediately" }, deliveryIntent: "auto" }),
        createdBy: "user",
        creationSource: "web",
      });
    });
  const read = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const projection = yield* (yield* Orchestrator.OrchestratorV2).getThreadProjection(threadId);
      const textOf = (run: (typeof projection.runs)[number]) =>
        projection.messages.find((message) => message.id === run.userMessageId)?.text;
      return {
        queue: projection.runs
          .filter((run) => run.status === "queued")
          .toSorted((left, right) => (left.queuePosition ?? 0) - (right.queuePosition ?? 0))
          .map(textOf),
        started: harness.started.map((turn) => turn.message.text),
        stopRequested: projection.turnItems.some(
          (item) =>
            item.type === "run_interrupt_request" && item.runId === harness.started[0]?.runId,
        ),
      };
    });
  return { ...harness, send, read };
});

it.effect.each([undefined, false, true])(
  "goal dispatch stops the active turn only with native support: %s",
  (nativeGoals) =>
    Effect.gen(function* () {
      const { steered, layer, startFirstTurn, send, read } = yield* goalHarness(
        "goal-steer",
        nativeGoals,
      );
      yield* Effect.gen(function* () {
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        yield* send("queued", "queued", "queue");
        yield* send("goal", "/goal all tests pass", "auto");
        yield* worker.drain();

        assert.deepEqual(steered, []);
        assert.deepEqual(yield* read(threadId), {
          queue: nativeGoals === false ? ["queued", "/goal all tests pass"] : ["queued"],
          started: nativeGoals === false ? ["first"] : ["first", "/goal all tests pass"],
          stopRequested: nativeGoals !== false,
        });
      }).pipe(Effect.provide(layer));
    }),
);

it.effect("starts a goal that names no model on the thread's model ahead of a queued pick", () =>
  Effect.gen(function* () {
    const { started, layer, startFirstTurn, read } = yield* goalHarness("goal-ahead-of-pick");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      for (const [id, text, modelSelection] of [
        ["picked", "picked", composerSelection],
        ["goal", "/goal all tests pass", undefined],
      ] as const) {
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(id),
          threadId,
          messageId: MessageId.make(`message:${id}`),
          text,
          attachments: [],
          ...(modelSelection === undefined ? {} : { modelSelection }),
          dispatchMode:
            id === "picked" ? { type: "queue_after_active" } : { type: "start_immediately" },
          ...(id === "picked" ? {} : { deliveryIntent: "auto" as const }),
          createdBy: "user",
          creationSource: "web",
        });
      }
      yield* worker.drain();

      assert.deepEqual(yield* read(threadId), {
        queue: ["picked"],
        started: ["first", "/goal all tests pass"],
        stopRequested: true,
      });
      assert.deepEqual(started[1]?.modelSelection, runSelection);
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("restarts by stopping the running turn when the provider cannot interrupt-restart", () =>
  Effect.gen(function* () {
    const { started, steered, layer, startFirstTurn, send, read } = yield* goalHarness(
      "restart-without-interrupt-restart",
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      yield* send("queued", "queued", "queue");
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("restart"),
        threadId,
        messageId: MessageId.make("message:restart"),
        text: "do this instead",
        attachments: [],
        modelSelection: runSelection,
        dispatchMode: { type: "restart_active", targetRunId: started[0]!.runId },
        createdBy: "agent",
        creationSource: "server",
      });
      yield* worker.drain();

      assert.deepEqual(steered, []);
      assert.deepEqual(yield* read(threadId), {
        queue: ["queued"],
        started: ["first", "do this instead"],
        stopRequested: true,
      });
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("starts a restart that stops the running turn ahead of an older held queue", () =>
  Effect.gen(function* () {
    const { started, layer, startFirstTurn, send, read } =
      yield* goalHarness("restart-past-held-queue");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      yield* send("queued", "queued", "queue");
      yield* orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make("stop"),
        threadId,
      });
      yield* worker.drain();
      const secondRunning = yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("second"),
        threadId,
        messageId: MessageId.make("message:second"),
        text: "second",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "agent",
        creationSource: "mcp",
      });
      yield* worker.drain();
      yield* Fiber.join(secondRunning);
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("restart"),
        threadId,
        messageId: MessageId.make("message:restart"),
        text: "do this instead",
        attachments: [],
        dispatchMode: { type: "restart_active", targetRunId: started[1]!.runId },
        createdBy: "agent",
        creationSource: "mcp",
      });
      yield* worker.drain();

      const { queue, started: startedTexts } = yield* read(threadId);
      assert.deepEqual(
        { queue, started: startedTexts },
        {
          queue: ["queued"],
          started: [
            "first",
            ...["second", "do this instead"].map(
              (text) =>
                `[T3 Code: this message came through T3 Code's tools, not from the user typing in this chat.]\n\n${text}`,
            ),
          ],
        },
      );
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("reports a restart that stops the running turn as restarted", () =>
  Effect.gen(function* () {
    const { started, layer, startFirstTurn } = yield* goalHarness("restart-send-delivery");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadManagement = yield* ThreadManagementService.ThreadManagementService;
      const threadId = yield* startFirstTurn;
      const input = {
        projectId: ProjectId.make("project:restart-send-delivery"),
        commandId: CommandId.make("restart"),
        threadId,
        messageId: MessageId.make("message:restart"),
        text: "do this instead",
        attachments: [],
        mode: "restart",
        createdBy: "agent",
        creationSource: "mcp",
      } as const;
      const sent = yield* threadManagement.sendToThread(input);
      // Stop holds the replacement before it starts; a replay must still report the restart.
      yield* orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make("stop-after-restart"),
        threadId,
      });
      const replayed = yield* threadManagement.sendToThread(input);

      assert.equal(started.length, 1);
      assert.deepEqual([sent.delivery, replayed.delivery], ["restarted", "restarted"]);
    }).pipe(
      Effect.provide(
        ThreadManagementService.layer.pipe(
          Layer.provideMerge(layer),
          Layer.provide(ServerSettings.layerTest()),
          Layer.provide(
            CommandReceiptStore.layer.pipe(Layer.provide(SqlitePersistence.layerMemory)),
          ),
        ),
      ),
    );
  }),
);

it.effect("replays an accepted steer after its target turn completes", () =>
  Effect.gen(function* () {
    const name = "steer-retry-after-completion";
    const { events, started, steered, layer, startFirstTurn } =
      yield* nextTurnSelectionHarness(name);
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadManagement = yield* ThreadManagementService.ThreadManagementService;
      const threadId = yield* startFirstTurn;
      const input = {
        projectId: ProjectId.make(`project:${name}`),
        commandId: CommandId.make("steer"),
        threadId,
        messageId: MessageId.make("message:steer"),
        text: "Please report the test result",
        attachments: [],
        mode: "steer",
        createdBy: "agent",
        creationSource: "mcp",
      } as const;
      const accepted = yield* threadManagement.sendToThread(input);
      assert.equal(accepted.delivery, "steered");
      yield* worker.drain();

      const waiting = yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === accepted.run.id &&
            event.payload.status === "waiting",
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped({ startImmediately: true }),
      );
      const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
      yield* Queue.offer(events, {
        type: "provider_turn.updated",
        driver,
        providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
      });
      yield* Queue.offer(events, {
        type: "turn.terminal",
        driver,
        providerThreadId: turn.providerThreadId,
        providerTurnId: turn.id,
        runOrdinal: accepted.run.ordinal,
        status: "completed",
        failure: null,
        threadDisposition: "reusable",
      });
      yield* Fiber.join(waiting);
      yield* worker.drain();

      const replayed = yield* threadManagement.sendToThread(input);
      assert.equal(replayed.dispatch.replayed, true);
      assert.equal(replayed.delivery, accepted.delivery);
      assert.equal(replayed.message.id, accepted.message.id);
      assert.equal(replayed.run.id, accepted.run.id);
      assert.equal(replayed.run.status, "completed");

      const fresh = yield* threadManagement
        .sendToThread({
          ...input,
          commandId: CommandId.make("fresh-steer"),
          messageId: MessageId.make("message:fresh-steer"),
        })
        .pipe(Effect.flip);
      assert.equal(fresh._tag, "ThreadManagementNoSteerableRunError");
      yield* worker.drain();
      assert.deepEqual(steered, [
        `[T3 Code: this message came through T3 Code's tools, not from the user typing in this chat.]\n\n${input.text}`,
      ]);
      assert.lengthOf(started, 1);
    }).pipe(
      Effect.provide(
        ThreadManagementService.layer.pipe(
          Layer.provideMerge(layer),
          Layer.provide(ServerSettings.layerTest()),
          Layer.provide(
            CommandReceiptStore.layer.pipe(Layer.provide(SqlitePersistence.layerMemory)),
          ),
        ),
      ),
    );
  }),
);

it.effect("queues a goal clear without stopping work when no goal drives it", () =>
  Effect.gen(function* () {
    const { steered, layer, startFirstTurn, send, read } = yield* goalHarness("goal-clear");
    yield* Effect.gen(function* () {
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      yield* send("queued", "queued", "queue");
      yield* send("clear", "/goal clear", "auto");
      yield* worker.drain();

      assert.deepEqual(steered, []);
      assert.deepEqual(yield* read(threadId), {
        queue: ["queued", "/goal clear"],
        started: ["first"],
        stopRequested: false,
      });
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("promoting a queued /context moves it first and does not steer", () =>
  Effect.gen(function* () {
    const { started, steered, layer, startFirstTurn, send, read } =
      yield* goalHarness("slash-promote");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      yield* send("queued", "queued", "queue");
      yield* send("context", "/context", "queue");
      const queuedRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
        (run) => run.userMessageId === MessageId.make("message:context"),
      )!;
      yield* orchestrator.dispatch({
        type: "queued-message.promote-to-steer",
        commandId: CommandId.make("promote"),
        threadId,
        queuedRunId: queuedRun.id,
        targetRunId: started[0]!.runId,
      });
      yield* worker.drain();

      assert.deepEqual(steered, []);
      assert.deepEqual(yield* read(threadId), {
        queue: ["/context", "queued"],
        started: ["first"],
        stopRequested: false,
      });
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("restarts the turn for a slash command sent as an explicit restart", () =>
  Effect.gen(function* () {
    const { started, steered, interrupted, layer, startFirstTurn } =
      yield* nextTurnSelectionHarness("slash-restart", true);
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("restart-context"),
        threadId,
        messageId: MessageId.make("message:restart-context"),
        text: "/context",
        attachments: [],
        modelSelection: runSelection,
        dispatchMode: { type: "restart_active", targetRunId: started[0]!.runId },
        createdBy: "user",
        creationSource: "web",
      });
      yield* worker.drain();

      assert.deepEqual(steered, []);
      assert.lengthOf(interrupted, 1);
      assert.deepEqual(
        started.map((turn) => turn.message.text),
        ["first", "/context"],
      );
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("promotes a queued goal by stopping the running turn", () =>
  Effect.gen(function* () {
    const { started, steered, layer, startFirstTurn, send, read } =
      yield* goalHarness("goal-promote");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      yield* send("queued", "queued", "queue");
      yield* send("goal", "/goal ship it", "queue");
      const goalRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
        (run) => run.userMessageId === MessageId.make("message:goal"),
      )!;
      yield* orchestrator.dispatch({
        type: "queued-message.promote-to-steer",
        commandId: CommandId.make("promote"),
        threadId,
        queuedRunId: goalRun.id,
        targetRunId: started[0]!.runId,
      });
      yield* worker.drain();

      assert.deepEqual(steered, []);
      assert.deepEqual(yield* read(threadId), {
        queue: ["queued"],
        started: ["first", "/goal ship it"],
        stopRequested: true,
      });
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("stops the running turn for a goal an agent steers into it", () =>
  Effect.gen(function* () {
    const { started, steered, layer, startFirstTurn, read } = yield* goalHarness("goal-raw-steer");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      // Agent sends carry a raw steer target and no delivery intent.
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("goal"),
        threadId,
        messageId: MessageId.make("message:goal"),
        text: "/goal ship it",
        attachments: [],
        modelSelection: runSelection,
        dispatchMode: { type: "steer_active", targetRunId: started[0]!.runId },
        createdBy: "agent",
        creationSource: "server",
      });
      yield* worker.drain();

      assert.deepEqual(steered, []);
      assert.deepEqual(yield* read(threadId), {
        queue: [],
        started: ["first", "/goal ship it"],
        stopRequested: true,
      });
    }).pipe(Effect.provide(layer));
  }),
);

// A fresh message skips compaction as its own turn, which steering a running turn would not.
it.effect.each(["next", "/goal ship it", "fresh next"])(
  "promoting %s into a compaction stops it and sends it next",
  (promoted) =>
    Effect.gen(function* () {
      const { events, layer, send, read, started, steered, startFirstTurn } = yield* goalHarness(
        `compaction-promote-${promoted.length}`,
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        yield* send("compact", "/compact", "queue");
        yield* send("queued", "queued", "queue");
        yield* send("next", promoted, "queue", promoted.startsWith("fresh"));
        const settled = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === started[0]!.runId &&
              event.payload.status === "waiting",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        const compacting = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.status === "running" &&
              event.payload.runAttemptId !== started[0]!.attemptId,
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
        });
        yield* Queue.offer(events, {
          type: "turn.terminal",
          driver,
          providerThreadId: turn.providerThreadId,
          providerTurnId: turn.id,
          runOrdinal: started[0]!.runOrdinal,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* Fiber.join(settled);
        yield* worker.drain();
        yield* orchestrator.resumeQueuedRuns;
        yield* worker.drain();
        yield* Fiber.join(compacting);
        const compactRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.userMessageId === MessageId.make("message:compact"),
        )!;
        const nextRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.userMessageId === MessageId.make("message:next"),
        )!;
        yield* orchestrator.dispatch({
          type: "queued-message.promote-to-steer",
          commandId: CommandId.make("promote"),
          threadId,
          queuedRunId: nextRun.id,
          targetRunId: compactRun.id,
        });
        yield* worker.drain();

        const projection = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(steered, []);
        assert.isTrue(
          projection.turnItems.some(
            (item) => item.type === "run_interrupt_request" && item.runId === compactRun.id,
          ),
        );
        assert.deepEqual((yield* read(threadId)).queue, ["queued"]);
        assert.deepEqual(
          started.map((turn) => turn.message.text),
          ["first", "/compact", promoted],
        );
      }).pipe(Effect.provide(layer));
    }),
);

it.effect("stops a turn that is still starting when a goal is sent into it", () =>
  Effect.gen(function* () {
    const { cwd, layer, send, started } = yield* goalHarness("goal-during-start");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = ThreadId.make("thread:goal-during-start");
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("create"),
        threadId,
        projectId: ProjectId.make("project:goal-during-start"),
        title: "Goal during start",
        modelSelection: runSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: cwd,
        createdBy: "user",
        creationSource: "web",
      });
      // The stopped run's end starts the queue, which then needs one more drain.
      const goalStarting = yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event) => event.type === "run.updated" && event.payload.status === "starting",
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped({ startImmediately: true }),
      );
      yield* send("first", "first", "auto");
      yield* send("goal", "/goal all tests pass", "auto");
      yield* worker.drain();
      yield* Fiber.join(goalStarting);
      yield* worker.drain();

      const projection = yield* orchestrator.getThreadProjection(threadId);
      const statusOf = (text: string) =>
        projection.runs.find(
          (run) =>
            projection.messages.find((message) => message.id === run.userMessageId)?.text === text,
        )?.status;
      assert.equal(statusOf("first"), "interrupted");
      assert.deepEqual(
        started.map((turn) => turn.message.text),
        ["/goal all tests pass"],
      );
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("stops the running turn when a goal clear ends the goal driving it", () =>
  Effect.gen(function* () {
    const { events, layer, startFirstTurn, send, read, steered } =
      yield* goalHarness("goal-clear-active");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const providerThread = (yield* orchestrator.getThreadProjection(threadId))
        .providerThreads[0]!;
      const goalSeen = yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "provider-thread.updated" && event.payload.goal?.status === "active",
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped({ startImmediately: true }),
      );
      yield* Queue.offer(events, {
        type: "provider_thread.updated",
        driver,
        providerThread: {
          ...providerThread,
          goal: { objective: "all tests pass", status: "active" },
        },
      });
      yield* Fiber.join(goalSeen);
      yield* send("clear", "/goal clear", "auto");
      yield* worker.drain();

      assert.deepEqual(steered, []);
      assert.deepEqual(yield* read(threadId), {
        queue: [],
        started: ["first", "/goal clear"],
        stopRequested: true,
      });
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("a side chat that opens with /goal gets its context on the next question", () =>
  Effect.gen(function* () {
    const { events, layer, started, startFirstTurn } = yield* goalHarness("side-chat-goal");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const parentId = yield* startFirstTurn;
      const sideChatId = ThreadId.make("thread:side-chat-goal-child");
      yield* orchestrator.dispatch({
        type: "thread.fork",
        commandId: CommandId.make("side-chat"),
        sourceThreadId: parentId,
        targetThreadId: sideChatId,
        sourcePoint: { type: "latest_stable" },
        sideChat: true,
        createdBy: "user",
        creationSource: "web",
      });
      const ask = (id: string, text: string) =>
        orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(id),
          threadId: sideChatId,
          messageId: MessageId.make(`message:${id}`),
          text,
          attachments: [],
          modelSelection: runSelection,
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
      const sideChatEvent = (matches: (event: OrchestrationV2DomainEvent) => boolean) =>
        orchestrator.streamDomainEvents.pipe(
          Stream.filter((event) => event.threadId === sideChatId && matches(event)),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped({ startImmediately: true }),
        );
      const running = yield* sideChatEvent(
        (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
      );
      const settled = yield* sideChatEvent(
        (event) => event.type === "run.updated" && event.payload.status === "waiting",
      );
      yield* ask("goal", "/goal clear");
      yield* worker.drain();
      yield* Fiber.join(running);
      const turn = (yield* orchestrator.getThreadProjection(sideChatId)).providerTurns[0]!;
      yield* Queue.offer(events, {
        type: "provider_turn.updated",
        driver,
        providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
      });
      yield* Queue.offer(events, {
        type: "turn.terminal",
        driver,
        providerThreadId: turn.providerThreadId,
        providerTurnId: turn.id,
        runOrdinal: started[1]!.runOrdinal,
        status: "completed",
        failure: null,
        threadDisposition: "reusable",
      });
      yield* Fiber.join(settled);
      yield* worker.drain();
      yield* ask("question", "What is the main thread doing?");
      yield* worker.drain();

      const [first, goal, question] = started.map((turn) => turn.message.text);
      assert.deepEqual([first, goal], ["first", "/goal clear"]);
      assert.include(question, "You are in a side chat beside the main thread.");
      assert.include(question, "- User: first");
      assert.isTrue(question?.endsWith("What is the main thread doing?"));
    }).pipe(Effect.provide(layer));
  }),
);

// Transcripts cannot reject steering while the turn still projects as running, so these
// settlement cases keep the real orchestrator, projection, outbox and continuation worker
// and control only the adapter boundary. Retry time is virtual; ordering uses barriers.

const runningSelection = { instanceId, model: "running-model" };
const desiredSelection = {
  instanceId,
  model: "next-turn-model",
  options: [{ id: "effort", value: "high" }],
};
const desiredSelectionMessageId = MessageId.make("message:desired-selection");
const settlementThreadId = ThreadId.make("thread:completion-settlement");
const markerRunId = RunId.make("run:completion-settlement-marker");

interface SettlementAdapterControls {
  readonly events: Queue.Queue<ProviderAdapter.ProviderAdapterV2Event>;
  readonly started: Array<ProviderAdapter.ProviderAdapterV2TurnInput>;
  readonly steered: Array<MessageId>;
  sessionOpens: number;
  interrupts: number;
}

it.effect("recovers a completion whose steering retries exhausted before the parent settled", () =>
  runSettlementScenario("exhausted-before-terminal", (h) =>
    Effect.gen(function* () {
      const parent = yield* h.startRun("message:parent");
      yield* h.saveDesiredSelection(parent.runId);
      const { messageId, taskId } = yield* h.reserveSteer(parent.runId);
      yield* h.exhaustSteering();
      assert.deepEqual(h.controls.steered, [messageId, messageId, messageId, messageId, messageId]);
      const stalled = yield* h.projection();
      assert.equal(stalled.runs.length, 1);
      assert.equal(h.task(stalled, taskId)?.completionDelivery?.state, "claimed");
      const siblingIds = yield* h.addPendingSiblings(parent.runId, 3);
      const userMessages = stalled.messages.filter((message) => message.createdBy === "user");

      yield* h.finishTurn(parent, "completed");
      yield* h.settled();
      assert.deepEqual(
        h.offers.map((offer) => offer.delegatedCompletion),
        [{ parentRunId: parent.runId, generation: 1, messageId }],
        "the terminal boundary reoffers the exhausted reservation",
      );

      yield* h.startContinuationWorker();
      const wake = yield* h.awaitDispatchedTurn(2);
      assert.equal(wake.message.messageId, messageId);
      assert.include(wake.message.text, String(taskId));
      for (const siblingId of siblingIds) assert.notInclude(wake.message.text, String(siblingId));
      assert.deepEqual(wake.modelSelection, desiredSelection);
      const recovered = yield* h.projection();
      assert.deepEqual(recovered.thread.modelSelection, desiredSelection);
      assert.deepEqual(h.run(recovered, parent.runId)?.modelSelection, runningSelection);
      assert.equal(recovered.messages.filter((message) => message.id === messageId).length, 1);
      assert.equal(
        recovered.messages.find((message) => message.id === messageId)?.runId,
        wake.runId,
      );
      assert.equal(recovered.turnItems.filter((item) => item.type === "notification").length, 1);

      // A duplicate terminal event and a duplicate request reuse the same identity.
      yield* h.repeatRunEvent(parent.runId);
      yield* h.settled();
      assert.equal(h.offers.length, 1);
      yield* Queue.offer(h.requests, h.offers[0]!);

      yield* h.finishTurn(wake, "completed");
      const siblingsWake = yield* h.awaitDispatchedTurn(3);
      assert.equal(h.controls.started.length, 3);
      assert.equal(h.offers[1]?.delegatedCompletion?.generation, 2);
      assert.notEqual(siblingsWake.message.messageId, messageId);
      for (const siblingId of siblingIds)
        assert.include(siblingsWake.message.text, String(siblingId));
      assert.notInclude(siblingsWake.message.text, String(taskId));
      assert.deepEqual(siblingsWake.modelSelection, desiredSelection);

      yield* h.finishTurn(siblingsWake, "completed");
      yield* h.settled();
      const delivered = yield* h.projection();
      const cohort = h.run(delivered, parent.runId)?.delegatedCompletion;
      assert.equal(cohort?.delivery, null);
      assert.deepEqual(
        delivered.subagents.map((task) => task.completionDelivery?.state),
        ["delivered", "delivered", "delivered", "delivered"],
      );
      assert.deepEqual(
        delivered.messages.filter((message) => message.createdBy === "user"),
        userMessages,
        "recovery needs no new user input",
      );
      assert.deepEqual(delivered.thread.modelSelection, desiredSelection);
      assert.equal(h.controls.sessionOpens, 1);
      assert.equal(h.controls.interrupts, 0);

      // Late or repeated acknowledgments of the recovered delivery change nothing.
      yield* h.orchestrator.dispatch({
        type: "notification.delivery.accept",
        commandId: CommandId.make("late-acceptance"),
        threadId: settlementThreadId,
        messageId,
      });
      const afterLateAcceptance = yield* h.projection();
      assert.deepEqual(afterLateAcceptance.subagents, delivered.subagents);
      assert.deepEqual(afterLateAcceptance.runs, delivered.runs);
      for (const task of delivered.subagents) {
        yield* h.orchestrator.dispatch({
          type: "delegated_task.completion-delivery.acknowledge",
          commandId: CommandId.make(`read-result:${task.id}`),
          parentThreadId: settlementThreadId,
          taskId: task.id,
          observedByRunId: siblingsWake.runId,
        });
      }
      yield* h.repeatRunEvent(parent.runId);
      yield* h.settled();
      assert.equal(h.controls.started.length, 3);
      assert.isTrue(
        (yield* h.projection()).subagents.every(
          (task) => task.completionDelivery?.state === "acknowledged",
        ),
      );
    }),
  ),
);

it.effect("drops a queued recovery offer whose parent is rolled back before dispatch", () =>
  runSettlementScenario("rollback-after-offer", (h) =>
    Effect.gen(function* () {
      const parent = yield* h.startRun("message:parent");
      const { messageId, taskId } = yield* h.reserveSteer(parent.runId);
      yield* h.exhaustSteering();
      yield* h.finishTurn(parent, "completed");
      yield* h.settled();
      assert.deepEqual(
        h.offers.map((offer) => offer.delegatedCompletion),
        [{ parentRunId: parent.runId, generation: 1, messageId }],
      );
      const completed = yield* h.projection();

      yield* h.rollBack(parent.runId);
      yield* h.settled();
      assert.equal(h.offers.length, 1, "the rollback event does not offer again");
      assert.equal(
        yield* h.dispatchStaleCompletion(parent.runId, messageId, [taskId]),
        "Delegated completion delivery is no longer dispatchable.",
      );

      // The worker handles requests serially, so reading a repeated request
      // proves the queued offer was handled.
      yield* h.startContinuationWorker();
      yield* Queue.offer(h.requests, h.offers[0]!);
      yield* h.awaitContinuationReads(2);
      assert.deepEqual(h.dispatchAttempts, [], "the queued offer is dropped, not dispatched");
      assert.equal(h.offers.length, 1, "the dropped offer is not retried");
      assert.equal(h.controls.started.length, 1);
      const rolledBack = yield* h.projection();
      assert.deepEqual(
        rolledBack.runs.map((run) => run.id),
        completed.runs.map((run) => run.id),
      );
      assert.equal(h.run(rolledBack, parent.runId)?.status, "rolled_back");
      assert.deepEqual(
        h.run(rolledBack, parent.runId)?.delegatedCompletion,
        h.run(completed, parent.runId)?.delegatedCompletion,
      );
      assert.equal(h.task(rolledBack, taskId)?.completionDelivery?.state, "claimed");
      assert.equal(
        rolledBack.messages.find((message) => message.id === messageId)?.runId,
        parent.runId,
      );

      // Reading the result later does not release the discarded cohort to a sibling.
      const [siblingId] = yield* h.addPendingSiblings(parent.runId, 1);
      yield* h.orchestrator.dispatch({
        type: "delegated_task.completion-delivery.acknowledge",
        commandId: CommandId.make("read-after-rollback"),
        parentThreadId: settlementThreadId,
        taskId,
        observedByRunId: parent.runId,
      });
      const read = yield* h.projection();
      assert.deepEqual(h.run(read, parent.runId)?.delegatedCompletion?.delivery, {
        generation: 1,
        messageId,
        taskIds: [],
      });
      assert.equal(h.task(read, siblingId!)?.completionDelivery?.state, "pending");
      assert.equal(h.offers.length, 1);
      assert.equal(h.controls.started.length, 1);
    }),
  ),
);

it.effect("reserves waiting siblings when the settled steer's result is read", () =>
  runSettlementScenario("release-settled", (h) =>
    Effect.gen(function* () {
      const parent = yield* h.startRun("message:parent");
      const { messageId, taskId } = yield* h.reserveSteer(parent.runId);
      const siblingIds = yield* h.addPendingSiblings(parent.runId, 2);
      yield* h.exhaustSteering();
      yield* h.finishTurn(parent, "completed");
      yield* h.settled();

      yield* h.resolveResult("acknowledge", taskId, parent.runId);
      const released = yield* h.projection();
      const delivery = h.run(released, parent.runId)?.delegatedCompletion?.delivery;
      assert.equal(delivery?.generation, 2);
      assert.notEqual(delivery?.messageId, messageId);
      assert.deepEqual(delivery?.taskIds, siblingIds);
      for (const siblingId of siblingIds)
        assert.equal(h.task(released, siblingId)?.completionDelivery?.state, "claimed");
      assert.deepEqual(
        h.offers.map((offer) => offer.delegatedCompletion),
        [
          { parentRunId: parent.runId, generation: 1, messageId },
          { parentRunId: parent.runId, generation: 2, messageId: delivery!.messageId },
        ],
      );

      yield* h.startContinuationWorker();
      const wake = yield* h.awaitDispatchedTurn(2);
      assert.equal(wake.message.messageId, delivery?.messageId);
      for (const siblingId of siblingIds) assert.include(wake.message.text, String(siblingId));
      assert.notInclude(wake.message.text, String(taskId));
      yield* h.finishTurn(wake, "completed");
      yield* h.settled();
      const delivered = yield* h.projection();
      for (const siblingId of siblingIds)
        assert.equal(h.task(delivered, siblingId)?.completionDelivery?.state, "delivered");
      assert.equal(h.run(delivered, parent.runId)?.delegatedCompletion?.delivery, null);
      assert.equal(h.dispatches.length, 1);
      assert.equal(h.controls.started.length, 2);
    }),
  ),
);

it.effect("delivers a waiting sibling once after a steer read while live settles", () =>
  runSettlementScenario("live-read-then-settle", (h) =>
    Effect.gen(function* () {
      const parent = yield* h.startRun("message:parent");
      const { messageId, taskId } = yield* h.reserveSteer(parent.runId);
      const [siblingId] = yield* h.addPendingSiblings(parent.runId, 1);
      yield* h.exhaustSteering();
      yield* h.resolveResult("acknowledge", taskId, parent.runId);
      const userMessages = (yield* h.projection()).messages.filter(
        (message) => message.createdBy === "user",
      );
      assert.deepEqual(
        h.run(yield* h.projection(), parent.runId)?.delegatedCompletion?.delivery,
        { generation: 1, messageId, taskIds: [] },
        "the live steer keeps its emptied reservation",
      );
      assert.equal(
        yield* h.dispatchStaleCompletion(parent.runId, messageId, [taskId]),
        "Delegated completion delivery is no longer dispatchable.",
        "an emptied reservation starts no wake",
      );

      yield* h.finishTurn(parent, "completed");
      yield* h.settled();
      const released = yield* h.projection();
      const delivery = h.run(released, parent.runId)?.delegatedCompletion?.delivery;
      assert.equal(delivery?.generation, 2);
      assert.notEqual(delivery?.messageId, messageId);
      assert.deepEqual(delivery?.taskIds, [siblingId!]);
      assert.equal(h.task(released, siblingId!)?.completionDelivery?.state, "claimed");
      assert.deepEqual(
        h.offers.map((offer) => offer.delegatedCompletion),
        [{ parentRunId: parent.runId, generation: 2, messageId: delivery!.messageId }],
      );

      yield* h.startContinuationWorker();
      const wake = yield* h.awaitDispatchedTurn(2);
      assert.equal(wake.message.messageId, delivery?.messageId);
      assert.include(wake.message.text, String(siblingId));
      assert.notInclude(wake.message.text, String(taskId));
      yield* h.finishTurn(wake, "completed");
      yield* h.settled();
      const delivered = yield* h.projection();
      assert.equal(h.task(delivered, siblingId!)?.completionDelivery?.state, "delivered");
      assert.equal(h.task(delivered, taskId)?.completionDelivery?.state, "acknowledged");
      const cohort = h.run(delivered, parent.runId)?.delegatedCompletion;
      assert.equal(cohort?.delivery, null);

      // A repeated settlement leaves the delivered cohort alone.
      yield* h.repeatRunEvent(parent.runId);
      yield* h.settled();
      const repeated = yield* h.projection();
      assert.deepEqual(h.run(repeated, parent.runId)?.delegatedCompletion, cohort);
      assert.deepEqual(repeated.subagents, delivered.subagents);
      assert.equal(h.offers.length, 1);
      assert.equal(h.dispatches.length, 1);
      assert.equal(h.controls.started.length, 2, "one wake, and no empty wake");
      assert.deepEqual(
        repeated.messages.filter((message) => message.createdBy === "user"),
        userMessages,
        "recovery needs no new user input",
      );
    }),
  ),
);

const runSettlementScenario = <E>(
  name: string,
  body: (harness: SettlementHarness) => Effect.Effect<void, E, Scope.Scope>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace(`completion-settlement-${name}`);
      const controls: SettlementAdapterControls = {
        events: yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>(),
        started: [],
        steered: [],
        sessionOpens: 0,
        interrupts: 0,
      };
      const requests =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      const offers: Array<ProviderContinuationRequests.ProviderContinuationRequest> = [];
      const markerChecks = yield* Queue.unbounded<void>();
      yield* Effect.gen(function* () {
        const harness = yield* makeSettlementHarness(controls, requests, offers, markerChecks, cwd);
        yield* body(harness);
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry(
            { name: `completion-settlement-${name}` },
            ProviderAdapterRegistry.layerSingle(makeSettlementAdapter(cwd, controls)),
            {
              runEffectWorker: false,
              wrapProjectionStore: (store) => ({
                ...store,
                getRunMessage: (threadId, runId) =>
                  store
                    .getRunMessage(threadId, runId)
                    .pipe(
                      Effect.tap(() =>
                        runId === markerRunId ? Queue.offer(markerChecks, undefined) : Effect.void,
                      ),
                    ),
              }),
            },
          ),
        ),
        Effect.provideService(ProviderContinuationRequests.ProviderContinuationRequests, {
          offer: (request) =>
            Effect.sync(() => offers.push(request)).pipe(
              Effect.andThen(Queue.offer(requests, request)),
            ),
          take: Queue.take(requests),
        }),
      );
    }),
  );

type SettlementHarness = Effect.Success<ReturnType<typeof makeSettlementHarness>>;

const makeSettlementHarness = Effect.fnUntraced(function* (
  controls: SettlementAdapterControls,
  requests: Queue.Queue<ProviderContinuationRequests.ProviderContinuationRequest>,
  offers: ReadonlyArray<ProviderContinuationRequests.ProviderContinuationRequest>,
  markerChecks: Queue.Queue<void>,
  cwd: string,
) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
  const sink = yield* EventSink.EventSinkV2;
  const dispatches: Array<CommandId> = [];
  const dispatchAttempts: Array<CommandId> = [];
  const dispatched = yield* Queue.unbounded<void>();
  const continuationReads = yield* Queue.unbounded<void>();

  let markers = 0;
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make(`create:${settlementThreadId}`),
    threadId: settlementThreadId,
    projectId: ProjectId.make("project:completion-settlement"),
    title: "Completion settlement",
    modelSelection: runningSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: cwd,
    createdBy: "user",
    creationSource: "web",
  });

  const projection = () =>
    orchestrator.getThreadProjection(settlementThreadId).pipe(
      Effect.map((current) => ({
        ...current,
        runs: current.runs.filter((run) => run.id !== markerRunId),
      })),
    );
  const run = (
    current: { readonly runs: ReadonlyArray<OrchestrationV2Run> },
    runId: RunId,
  ): OrchestrationV2Run | undefined => current.runs.find((candidate) => candidate.id === runId);
  const task = (current: Effect.Success<ReturnType<typeof projection>>, taskId: NodeId) =>
    current.subagents.find((candidate) => candidate.id === taskId);

  const watch = Effect.fnUntraced(function* (
    predicate: (event: OrchestrationV2DomainEvent) => boolean,
  ) {
    const afterSequence = yield* sink.latestSequence();
    return yield* sink.stream({ afterSequence }).pipe(
      Stream.map((stored) => stored.event),
      Stream.filter(predicate),
      Stream.take(1),
      Stream.runDrain,
      Effect.forkScoped,
    );
  });

  // The run reactor serializes updates within one thread. Reading the rolled-back
  // marker's input proves all earlier reactions for this thread finished.
  const settled = Effect.fnUntraced(function* () {
    markers += 1;
    const current = yield* orchestrator.getThreadProjection(settlementThreadId);
    const marker = current.runs.find((run) => run.id === markerRunId);
    const { delegatedCompletion: _cohort, ...base } = marker ?? current.runs[0]!;
    const now = yield* DateTime.now;
    yield* sink.write({
      events: [
        {
          id: EventId.make(`event:marker:${markers}`),
          type: "run.updated",
          threadId: settlementThreadId,
          runId: markerRunId,
          occurredAt: now,
          payload: {
            ...base,
            id: markerRunId,
            userMessageId: MessageId.make("message:completion-settlement-marker"),
            ordinal: marker?.ordinal ?? Math.max(...current.runs.map((run) => run.ordinal)) + 1,
            status: "rolled_back",
            completedAt: now,
          },
        },
      ],
    });
    yield* Queue.take(markerChecks);
  });

  // Acknowledges (reads) or disposes one delegated result.
  const resolveResult = (
    release: "acknowledge" | "dispose",
    taskId: NodeId,
    observedByRunId: RunId,
  ) =>
    orchestrator.dispatch(
      release === "acknowledge"
        ? {
            type: "delegated_task.completion-delivery.acknowledge",
            commandId: CommandId.make(`acknowledge:${taskId}`),
            parentThreadId: settlementThreadId,
            taskId,
            observedByRunId,
          }
        : {
            type: "delegated_task.completion-delivery.dispose",
            commandId: CommandId.make(`dispose:${taskId}`),
            parentThreadId: settlementThreadId,
            taskId,
          },
    );

  const startRun = Effect.fnUntraced(function* (id: string) {
    const running = yield* watch(
      (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
    );
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(`dispatch:${id}`),
      threadId: settlementThreadId,
      messageId: MessageId.make(id),
      text: id,
      attachments: [],
      dispatchMode: { type: "start_immediately" },
      createdBy: "user",
      creationSource: "web",
    });
    yield* worker.drain();
    yield* Fiber.join(running);
    return controls.started.at(-1)!;
  });

  const finishTurn = Effect.fnUntraced(function* (
    turnInput: ProviderAdapter.ProviderAdapterV2TurnInput,
    status: "completed" | "interrupted",
  ) {
    const current = yield* orchestrator.getThreadProjection(turnInput.threadId);
    const providerTurn = current.providerTurns.find(
      (candidate) => candidate.runAttemptId === turnInput.attemptId,
    )!;
    const reached = yield* watch(
      (event) =>
        event.type === "run.updated" &&
        event.payload.id === turnInput.runId &&
        event.payload.status === (status === "completed" ? "waiting" : status),
    );
    yield* Queue.offer(controls.events, {
      type: "provider_turn.updated",
      driver,
      providerTurn: { ...providerTurn, status, completedAt: yield* DateTime.now },
    });
    yield* Queue.offer(controls.events, {
      type: "turn.terminal",
      driver,
      providerThreadId: providerTurn.providerThreadId,
      providerTurnId: providerTurn.id,
      runOrdinal: turnInput.runOrdinal,
      status,
      failure: null,
      threadDisposition: "reusable",
    });
    yield* Fiber.join(reached);
    yield* worker.drain();
    assert.equal(
      run(yield* orchestrator.getThreadProjection(turnInput.threadId), turnInput.runId)?.status,
      status,
    );
  });

  const saveDesiredSelection = Effect.fnUntraced(function* (runId: RunId) {
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("desired-selection"),
      threadId: settlementThreadId,
      messageId: desiredSelectionMessageId,
      text: "use the new model next turn",
      attachments: [],
      modelSelection: desiredSelection,
      dispatchMode: { type: "steer_active", targetRunId: runId },
      createdBy: "user",
      creationSource: "web",
    });
    yield* worker.drain();
    const selected = yield* projection();
    assert.deepEqual(selected.thread.modelSelection, desiredSelection);
    assert.deepEqual(run(selected, runId)?.modelSelection, runningSelection);
  });

  // Seeds the claimed first result of an open cohort, then routes its durable
  // mailbox message through the orchestrator, which steers the active run.
  const reserveSteer = Effect.fnUntraced(function* (ownerRunId: RunId) {
    const messageId = MessageId.make("message:completion-steer");
    const taskId = NodeId.make("task:first");
    const current = yield* projection();
    const owner = run(current, ownerRunId)!;
    const ownerTurn = controls.started.find((turn) => turn.runId === ownerRunId)!;
    const now = yield* DateTime.now;
    yield* sink.write({
      events: [
        {
          id: EventId.make("event:cohort"),
          type: "run.updated",
          threadId: settlementThreadId,
          runId: ownerRunId,
          occurredAt: now,
          payload: {
            ...owner,
            delegatedCompletion: {
              disposition: "open",
              nextGeneration: 2,
              delivery: { generation: 1, messageId, taskIds: [taskId] },
            },
          },
        },
        {
          id: EventId.make("event:first-task"),
          type: "subagent.updated",
          threadId: settlementThreadId,
          runId: ownerRunId,
          nodeId: taskId,
          occurredAt: now,
          payload: {
            id: taskId,
            threadId: settlementThreadId,
            runId: ownerRunId,
            parentNodeId: ownerTurn.rootNodeId,
            origin: "app_owned",
            createdBy: "agent",
            driver,
            providerInstanceId: instanceId,
            providerThreadId: null,
            childThreadId: null,
            nativeTaskRef: null,
            prompt: "Do background work",
            title: "First result",
            model: null,
            completionWake: "always",
            completionDelivery: { state: "claimed", observedByRunId: null },
            status: "completed",
            result: "done",
            startedAt: now,
            completedAt: now,
            updatedAt: now,
          },
        },
      ],
    });
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("completion-delivery"),
      threadId: settlementThreadId,
      messageId,
      text: "Delegated task completed.",
      attachments: [],
      dispatchMode: { type: "queue_after_active" },
      createdBy: "agent",
      creationSource: "server",
      delegatedCompletion: { parentRunId: ownerRunId, generation: 1, taskIds: [taskId] },
    });
    return { messageId, taskId };
  });

  // Siblings that finished while the first delivery was in flight wait as pending.
  const addPendingSiblings = Effect.fnUntraced(function* (ownerRunId: RunId, count: number) {
    const first = (yield* projection()).subagents.find(
      (candidate) => candidate.runId === ownerRunId,
    )!;
    const now = yield* DateTime.now;
    const ids = Array.from({ length: count }, (_, index) =>
      NodeId.make(`task:sibling-${index + 1}`),
    );
    yield* sink.write({
      events: ids.map((id) => ({
        id: EventId.make(`event:${id}`),
        type: "subagent.updated" as const,
        threadId: settlementThreadId,
        runId: ownerRunId,
        nodeId: id,
        occurredAt: now,
        payload: {
          ...first,
          id,
          title: `Sibling ${id}`,
          completionDelivery: { state: "pending" as const, observedByRunId: null },
        },
      })),
    });
    return ids;
  });

  // Drives every real outbox retry, moving virtual time only to scheduled boundaries.
  const exhaustSteering = Effect.fnUntraced(function* () {
    for (let attempt = 0; attempt < 5; attempt += 1) {
      yield* worker.drain();
      const next = yield* worker.nextClaimableAt();
      if (Option.isSome(next)) yield* TestClock.setTime(DateTime.toEpochMillis(next.value));
    }
    assert.isTrue(Option.isNone(yield* worker.nextClaimableAt()));
  });

  const repeatRunEvent = Effect.fnUntraced(function* (runId: RunId) {
    const current = run(yield* projection(), runId)!;
    yield* sink.write({
      events: [
        {
          id: EventId.make(`event:repeat:${runId}:${yield* sink.latestSequence()}`),
          type: "run.updated",
          threadId: settlementThreadId,
          runId,
          occurredAt: yield* DateTime.now,
          payload: current,
        },
      ],
    });
  });

  // CheckpointRollbackService spreads the discarded run and keeps its cohort.
  const rollBack = Effect.fnUntraced(function* (runId: RunId) {
    const current = run(yield* projection(), runId)!;
    const now = yield* DateTime.now;
    yield* sink.write({
      events: [
        {
          id: EventId.make(`event:rollback:${runId}`),
          type: "run.updated",
          threadId: settlementThreadId,
          runId,
          occurredAt: now,
          payload: { ...current, status: "rolled_back", completedAt: now },
        },
      ],
    });
  });

  // Sends what the continuation worker builds from a read taken before the
  // reservation changed, and returns why the orchestrator refused it.
  const dispatchStaleCompletion = (
    parentRunId: RunId,
    messageId: MessageId,
    taskIds: ReadonlyArray<NodeId>,
  ) =>
    orchestrator
      .dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`stale-completion:${messageId}`),
        threadId: settlementThreadId,
        messageId,
        text: "Delegated task completed.",
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "agent",
        creationSource: "server",
        delegatedCompletion: { parentRunId, generation: 1, taskIds },
      })
      .pipe(
        Effect.as("dispatched"),
        Effect.catchTags({
          OrchestratorDispatchError: (error) => Effect.succeed(String(error.cause)),
        }),
      );

  const startContinuationWorker = () =>
    Layer.build(
      ProviderContinuationService.layer.pipe(
        Layer.provide(IdAllocator.layer),
        Layer.provide(
          Layer.mock(ThreadManagementService.ThreadManagementService)({
            getThreadRecords: (id, fields, filter) =>
              orchestrator
                .getThreadRecords(id, fields, filter)
                .pipe(Effect.tap(() => Queue.offer(continuationReads, undefined))),
            dispatch: (command) =>
              Effect.sync(() => dispatchAttempts.push(command.commandId)).pipe(
                Effect.andThen(orchestrator.dispatch(command)),
                Effect.tap(() =>
                  Effect.sync(() => dispatches.push(command.commandId)).pipe(
                    Effect.andThen(Queue.offer(dispatched, undefined)),
                  ),
                ),
              ),
          }),
        ),
      ),
    );

  // Waits for the next continuation dispatch to start provider turn `count`.
  // An earlier drain may already have started it, so check before joining.
  const awaitDispatchedTurn = Effect.fnUntraced(function* (count: number) {
    yield* Queue.take(dispatched);
    const isRunning = (event: OrchestrationV2DomainEvent) =>
      event.type === "provider-turn.updated" &&
      event.payload.status === "running" &&
      event.payload.runAttemptId === controls.started[count - 1]?.attemptId;
    const running = yield* watch(isRunning);
    yield* worker.drain();
    const turn = controls.started[count - 1];
    const alreadyRunning =
      turn !== undefined &&
      (yield* projection()).providerTurns.some(
        (candidate) => candidate.runAttemptId === turn.attemptId && candidate.status === "running",
      );
    yield* alreadyRunning ? Fiber.interrupt(running) : Fiber.join(running);
    assert.equal(controls.started.length, count);
    return controls.started[count - 1]!;
  });

  const awaitContinuationReads = Effect.fnUntraced(function* (count: number) {
    for (let read = 0; read < count; read += 1) yield* Queue.take(continuationReads);
  });

  return {
    controls,
    requests,
    offers,
    dispatches,
    dispatchAttempts,
    orchestrator,
    projection,
    run,
    task,
    settled,
    resolveResult,
    startRun,
    finishTurn,
    saveDesiredSelection,
    reserveSteer,
    addPendingSiblings,
    exhaustSteering,
    repeatRunEvent,
    rollBack,
    dispatchStaleCompletion,
    startContinuationWorker,
    awaitDispatchedTurn,
    awaitContinuationReads,
  };
});

function makeSettlementAdapter(
  cwd: string,
  controls: SettlementAdapterControls,
): ProviderAdapter.ProviderAdapterV2["Service"] {
  return {
    instanceId,
    driver,
    getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: (input) =>
      Effect.gen(function* () {
        controls.sessionOpens += 1;
        const now = yield* DateTime.now;
        return {
          instanceId,
          driver,
          providerSessionId: input.providerSessionId,
          providerSession: {
            id: input.providerSessionId,
            driver,
            providerInstanceId: instanceId,
            status: "ready",
            cwd,
            model: runningSelection.model,
            capabilities: CodexProviderCapabilitiesV2,
            createdAt: now,
            updatedAt: now,
            lastError: null,
          },
          events: Stream.fromQueue(controls.events),
          ensureThread: ({ threadId: appThreadId }) =>
            Effect.succeed({
              id: ProviderThreadId.make(`provider-thread:${appThreadId}`),
              driver,
              providerInstanceId: instanceId,
              providerSessionId: input.providerSessionId,
              appThreadId,
              ownerNodeId: null,
              nativeThreadRef: { driver, nativeId: "native-thread", strength: "strong" },
              nativeConversationHeadRef: null,
              status: "idle",
              firstRunOrdinal: null,
              lastRunOrdinal: null,
              handoffIds: [],
              forkedFrom: null,
              createdAt: now,
              updatedAt: now,
            }),
          resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
          startTurn: (turn) =>
            Effect.gen(function* () {
              controls.started.push(turn);
              yield* Queue.offer(controls.events, {
                type: "provider_turn.updated",
                driver,
                providerTurn: {
                  id: ProviderTurnId.make(`provider-turn:${turn.attemptId}`),
                  providerThreadId: turn.providerThread.id,
                  nodeId: turn.rootNodeId,
                  runAttemptId: turn.attemptId,
                  nativeTurnRef: {
                    driver,
                    nativeId: `native:${turn.attemptId}`,
                    strength: "strong",
                  },
                  ordinal: turn.providerTurnOrdinal,
                  status: "running",
                  startedAt: yield* DateTime.now,
                  completedAt: null,
                },
              });
            }),
          steerTurn: (turn) =>
            Effect.gen(function* () {
              if (turn.message.messageId === desiredSelectionMessageId) return;
              controls.steered.push(turn.message.messageId);
              return yield* new ProviderAdapter.ProviderAdapterSteerRunError({
                driver,
                providerThreadId: turn.providerThread.id,
                providerTurnId: turn.providerTurnId,
                cause: "Pi turn is not active.",
              });
            }),
          interruptTurn: () =>
            Effect.sync(() => {
              controls.interrupts += 1;
            }),
          respondToRuntimeRequest: () => Effect.void,
          readThreadSnapshot: () => Effect.die("unused"),
          rollbackThread: () => Effect.die("unused"),
          forkThread: () => Effect.die("unused"),
        };
      }),
  };
}
