import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  EventId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunId,
  ThreadId,
  TurnItemId,
  type ModelSelection,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import { ProjectionStoreV2 } from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderContinuationService from "./ProviderContinuationService.ts";
import { ThreadManagementService } from "./ThreadManagementService.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";

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
    ] as const
  ).map(([mailbox, timing]) => ({
    mailbox,
    timing,
    label: mailbox ? "mailbox notification" : "steering",
  })),
)("delivers $label when completion wins $timing", ({ mailbox, timing }) =>
  Effect.scoped(
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
                  if (timing === "after delivery") return;
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
          dispatchMode: mailbox
            ? { type: "queue_after_active" }
            : { type: "steer_active", targetRunId: first.runId },
          createdBy: mailbox ? "agent" : "user",
          creationSource: mailbox ? "server" : "web",
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
        if (timing === "after delivery") yield* worker.drain();
        const delivery =
          timing === "during delivery" ? yield* worker.runOnce.pipe(Effect.forkScoped) : null;
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
        assert.equal(final.messages.filter((message) => message.id === messageId).length, 1);
        assert.equal(
          final.messages.find((message) => message.id === messageId)?.runId,
          started[1]?.runId,
        );
        assert.equal(
          final.turnItems.filter((item) =>
            mailbox
              ? item.type === "notification"
              : item.type === "user_message" && item.messageId === messageId,
          ).length,
          1,
        );
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
  ),
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
) {
  const cwd = yield* checkpointWorkspace(name);
  const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
  const started: ProviderAdapter.ProviderAdapterV2TurnInput[] = [];
  const steered: string[] = [];
  const capabilities = {
    ...CodexProviderCapabilitiesV2,
    turns: {
      ...CodexProviderCapabilitiesV2.turns,
      supportsActiveSteering: true,
      supportsSteeringByInterruptRestart: canRestart,
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
            Effect.sync(() => {
              steered.push(turn.message.text);
            }),
          interruptTurn: () => Effect.void,
          respondToRuntimeRequest: () => Effect.void,
          readThreadSnapshot: () => Effect.die("unused"),
          rollbackThread: () => Effect.die("unused"),
          forkThread: () => Effect.die("unused"),
        };
      }),
  };
  const layer = ProviderReplayHarness.layerWithRegistry(
    { name },
    ProviderAdapterRegistry.layerSingle(adapter),
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
    return threadId;
  });
  return { events, started, steered, layer, startFirstTurn };
});

it.effect("sends a selected held message once and preserves the remaining queue order", () =>
  Effect.scoped(
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
  ),
);

it.effect("keeps a message queued when send now races with a running turn", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { started, layer, startFirstTurn } =
        yield* nextTurnSelectionHarness("send-queued-race");
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
  ),
);

// regression: #19210 a provider that can restart the turn must still steer, since restarting ends its native subagents
it.effect(
  "steers a changed turn-scoped selection into a running turn the provider could restart",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
          "steering-selection-change",
          true,
        );
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
          assert.equal(back.thread.providerInstanceId, instanceId);
          assert.deepEqual(back.thread.modelSelection, runSelection);
        }).pipe(Effect.provide(layer));
      }),
    ),
);

it.effect("starts a steer that missed the turn on the saved next-turn selection", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { events, started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        "steering-selection-follow-up",
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
        yield* orchestrator.resumeQueuedRuns;
        yield* worker.drain();

        const projection = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(steered, []);
        assert.equal(started.length, 2);
        assert.equal(started[1]?.message.messageId, MessageId.make("message:steer"));
        assert.deepEqual(started[1]?.modelSelection, composerSelection);
        assert.deepEqual(projection.thread.modelSelection, composerSelection);
      }).pipe(Effect.provide(layer));
    }),
  ),
);

// A draft that never reaches a paragraph boundary is held back while it
// streams. Codex only flushes it at turn end, after the final answer is written,
// so the draft must already own its place from before the steer.
it.effect("keeps a held commentary draft before the steer when turn end flushes it", () =>
  Effect.scoped(
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

        // The cut-off draft starts first; the commentary after it is written,
        // so the draft has been seen by the time the steer arrives.
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
        assert.deepEqual(items, [
          "message:first",
          "draft",
          "commentary",
          "message:steer",
          "answer",
        ]);
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect("keeps a queued message queued when promoted into a run that Stop reached", () =>
  Effect.scoped(
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
  ),
);

it.effect("queues a plain send behind the stopped queue and releases its hold", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { started, steered, layer, startFirstTurn } =
        yield* nextTurnSelectionHarness("plain-send-after-stop");
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
        yield* worker.drain();

        const projection = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(steered, []);
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
      }).pipe(Effect.provide(layer));
    }),
  ),
);

// A provider transcript cannot reject steering while the orchestrator still
// projects the turn as running. These delegated-completion settlement cases keep
// the real orchestrator, SQLite projection, outbox retries and continuation
// worker, and control only that adapter boundary. Retry time is virtual;
// ordering uses event barriers.

const runningSelection = { instanceId, model: "running-model" };
const desiredSelection = {
  instanceId,
  model: "next-turn-model",
  options: [{ id: "effort", value: "high" }],
};
const desiredSelectionMessageId = MessageId.make("message:desired-selection");
const settlementThreadId = ThreadId.make("thread:completion-settlement");
const markerThreadId = ThreadId.make("thread:completion-settlement-marker");
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
      assert.equal(h.offers.length, 2);
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
      assert.isAtLeast(yield* h.settled(), 1, "the terminal handler ran");
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
      yield* Effect.gen(function* () {
        const harness = yield* makeSettlementHarness(controls, requests, offers, cwd);
        yield* body(harness);
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry(
            { name: `completion-settlement-${name}` },
            ProviderAdapterRegistry.layerSingle(makeSettlementAdapter(cwd, controls)),
            { runEffectWorker: false },
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
  cwd: string,
) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
  const sink = yield* EventSink.EventSinkV2;
  const store = yield* ProjectionStoreV2;
  const dispatches: Array<CommandId> = [];
  const dispatchAttempts: Array<CommandId> = [];
  const dispatched = yield* Queue.unbounded<void>();
  const continuationReads = yield* Queue.unbounded<void>();

  // The terminal-run listener handles run updates one at a time, and each
  // makes a queue check. A marker run's check on another thread therefore
  // proves every earlier terminal event was handled, without adding input to
  // this thread. Counting this thread's checks shows its handler ran.
  const markerChecks = yield* Queue.unbounded<void>();
  let parentChecks = 0;
  let parentChecksAtMarker = 0;
  const canStartQueuedRun = store.canStartQueuedRun;
  Object.assign(store, {
    canStartQueuedRun: (id: ThreadId) =>
      canStartQueuedRun(id).pipe(
        Effect.tap(() => {
          if (id === settlementThreadId) parentChecks += 1;
          return id === markerThreadId ? Queue.offer(markerChecks, undefined) : Effect.void;
        }),
      ),
  });
  yield* Effect.addFinalizer(() => Effect.sync(() => Object.assign(store, { canStartQueuedRun })));
  let markers = 0;

  for (const [id, title] of [
    [settlementThreadId, "Completion settlement"],
    [markerThreadId, "Terminal listener marker"],
  ] as const) {
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make(`create:${id}`),
      threadId: id,
      projectId: ProjectId.make("project:completion-settlement"),
      title,
      modelSelection: runningSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: cwd,
      createdBy: "user",
      creationSource: "web",
    });
  }

  const projection = () => orchestrator.getThreadProjection(settlementThreadId);
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

  const settled = Effect.fnUntraced(function* () {
    markers += 1;
    const current = yield* orchestrator.getThreadProjection(markerThreadId);
    const now = yield* DateTime.now;
    const template = current.runs[0] ?? (yield* projection()).runs[0]!;
    const { delegatedCompletion: _cohort, ...base } = template;
    yield* sink.write({
      events: [
        {
          id: EventId.make(`event:marker:${markers}`),
          type: "run.updated",
          threadId: markerThreadId,
          runId: markerRunId,
          occurredAt: now,
          payload: {
            ...base,
            id: markerRunId,
            threadId: markerThreadId,
            ordinal: 1,
            status: "completed",
            completedAt: now,
          },
        },
      ],
    });
    yield* Queue.take(markerChecks);
    const handled = parentChecks - parentChecksAtMarker;
    parentChecksAtMarker = parentChecks;
    return handled;
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
      const next = yield* worker.nextClaimableAt;
      if (Option.isSome(next)) yield* TestClock.setTime(DateTime.toEpochMillis(next.value));
    }
    assert.isTrue(Option.isNone(yield* worker.nextClaimableAt));
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
          Layer.mock(ThreadManagementService)({
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
