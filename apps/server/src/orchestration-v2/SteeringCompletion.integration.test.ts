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
  ThreadId,
  type OrchestrationV2DomainEvent,
  type ModelSelection,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import { OrchestrationEffectWorkerV2 } from "./EffectWorker.ts";
import { EventSinkV2 } from "./EventSink.ts";
import { OrchestratorV2 } from "./Orchestrator.ts";
import {
  ProviderAdapterSteerRunError,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2Shape,
  type ProviderAdapterV2TurnInput,
} from "./ProviderAdapter.ts";
import { makeSingleLayer } from "./ProviderAdapterRegistry.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "./testkit/ReplayFixtureWorkspace.ts";

const driver = ProviderDriverKind.make("codex");
const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "test-model" };

for (const mailbox of [false, true]) {
  for (const timing of [
    "before delivery",
    "during delivery",
    "before dispatch",
    "after delivery",
    "without native steering",
    "settled only",
  ] as const) {
    if (!mailbox && (timing === "without native steering" || timing === "settled only")) continue;
    it.effect(
      `delivers ${mailbox ? "mailbox notification" : "steering"} when completion wins ${timing}`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const cwd = yield* checkpointWorkspace(
              `steering-completion-${timing.replaceAll(" ", "-")}`,
            );
            const events = yield* Queue.unbounded<ProviderAdapterV2Event>();
            const started: ProviderAdapterV2TurnInput[] = [];
            const steerEntered = yield* Deferred.make<void>();
            const rejectSteer = yield* Deferred.make<void>();
            let steerCalls = 0;
            let interrupts = 0;
            let sessionOpens = 0;
            let requireRestart = false;
            const deliveredSteers: Array<{ messageId: MessageId; providerTurnId: ProviderTurnId }> =
              [];
            const capabilities = {
              ...CodexProviderCapabilitiesV2,
              turns: {
                ...CodexProviderCapabilitiesV2.turns,
                supportsActiveSteering: timing !== "without native steering",
              },
            };
            const adapter: ProviderAdapterV2Shape = {
              instanceId,
              driver,
              getCapabilities: () => Effect.succeed(capabilities),
              planSelectionTransition: () =>
                Effect.succeed({ type: requireRestart ? "restart_session" : "apply_on_next_turn" }),
              openSession: (input) =>
                Effect.gen(function* () {
                  sessionOpens += 1;
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
                        deliveredSteers.push({
                          messageId: turn.message.messageId,
                          providerTurnId: turn.providerTurnId,
                        });
                        if (
                          timing === "after delivery" ||
                          turn.message.messageId === "message:desired-selection"
                        )
                          return;
                        yield* Deferred.succeed(steerEntered, undefined);
                        yield* Deferred.await(rejectSteer);
                        return yield* new ProviderAdapterSteerRunError({
                          driver,
                          providerThreadId: turn.providerThread.id,
                          providerTurnId: turn.providerTurnId,
                          cause: "turn already completed",
                        });
                      }),
                    interruptTurn: () =>
                      Effect.sync(() => {
                        interrupts += 1;
                      }),
                    respondToRuntimeRequest: () => Effect.void,
                    readThreadSnapshot: () => Effect.die("unused"),
                    rollbackThread: () => Effect.die("unused"),
                    forkThread: () => Effect.die("unused"),
                  };
                }),
            };
            yield* Effect.gen(function* () {
              const orchestrator = yield* OrchestratorV2;
              const worker = yield* OrchestrationEffectWorkerV2;
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
                (event) =>
                  event.type === "provider-turn.updated" && event.payload.status === "running",
              );
              yield* worker.drain();
              yield* Fiber.join(running);
              const first = started[0]!;
              const messageId = MessageId.make("message:steering");
              const taskId = NodeId.make("task:mailbox");
              const desiredSelection = {
                ...modelSelection,
                model: "next-turn-model",
                options: [{ id: "effort", value: "high" }],
              };
              if ((mailbox && timing === "after delivery") || timing === "during delivery") {
                yield* orchestrator.dispatch({
                  type: "message.dispatch",
                  commandId: CommandId.make("desired-selection"),
                  threadId,
                  messageId: MessageId.make("message:desired-selection"),
                  text: "use the new model next turn",
                  attachments: [],
                  modelSelection: desiredSelection,
                  dispatchMode: { type: "steer_active", targetRunId: first.runId },
                  createdBy: "user",
                  creationSource: "web",
                });
                yield* worker.drain();
                const selected = yield* orchestrator.getThreadProjection(threadId);
                assert.deepEqual(selected.thread.modelSelection, desiredSelection);
                assert.deepEqual(selected.runs[0]!.modelSelection, modelSelection);
                requireRestart = mailbox && timing === "after delivery";
              }
              if (mailbox) {
                const sink = yield* EventSinkV2;
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
                          settledDeliveryCount: 0,
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
              const beforeCompletion = yield* orchestrator.getThreadProjection(threadId);
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
              if (timing === "after delivery") {
                yield* worker.drain();
                if (mailbox) {
                  const delivered = yield* orchestrator.getThreadProjection(threadId);
                  const sink = yield* EventSinkV2;
                  const stored = yield* sink
                    .readByCommandId({ commandId: CommandId.make("steer") })
                    .pipe(Stream.runCollect);
                  assert.isEmpty(
                    stored.filter(({ event }) => event.type === "thread.model-selection-updated"),
                  );
                  assert.deepEqual(delivered.thread.modelSelection, desiredSelection);
                  assert.deepEqual(delivered.runs[0]!.modelSelection, modelSelection);
                  assert.deepEqual(delivered.attempts, beforeCompletion.attempts);
                  assert.deepEqual(delivered.providerSessions, beforeCompletion.providerSessions);
                  assert.deepEqual(delivered.providerTurns, beforeCompletion.providerTurns);
                  assert.equal(delivered.runs.length, 1);
                  assert.equal(interrupts, 0);
                  assert.equal(sessionOpens, 1);
                  assert.deepEqual(
                    deliveredSteers.filter((steer) => steer.messageId === messageId),
                    [
                      {
                        messageId,
                        providerTurnId: beforeCompletion.providerTurns[0]!.id,
                      },
                    ],
                  );
                  assert.equal(
                    delivered.messages.find((message) => message.id === messageId)?.runId,
                    first.runId,
                  );
                }
              }
              requireRestart = false;
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
                assert.equal(steerCalls, mailbox ? 2 : 1);
                assert.equal(started.length, 1);
                if (mailbox) {
                  const delivered = yield* orchestrator.getThreadProjection(threadId);
                  assert.equal(delivered.subagents[0]?.completionDelivery?.state, "delivered");
                  assert.equal(delivered.subagents[0]?.completionDelivery?.observedByRunId, null);
                  assert.equal(delivered.runs[0]?.delegatedCompletion?.delivery, null);
                  assert.equal(delivered.runs[0]?.delegatedCompletion?.settledDeliveryCount, 0);
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
                  assert.equal(steerCalls, 2);
                  assert.equal(
                    deliveredSteers.filter((steer) => steer.messageId === messageId).length,
                    1,
                  );
                  yield* orchestrator.dispatch({
                    type: "delegated_task.completion-delivery.acknowledge",
                    commandId: CommandId.make("read-result"),
                    parentThreadId: threadId,
                    taskId,
                    observedByRunId: first.runId,
                  });
                  const acknowledged = yield* orchestrator.getThreadProjection(threadId);
                  assert.equal(
                    acknowledged.subagents[0]?.completionDelivery?.state,
                    "acknowledged",
                  );
                  const followingRunning = yield* watch(
                    (event) =>
                      event.type === "provider-turn.updated" &&
                      event.payload.status === "running" &&
                      event.payload.id !== turn.id,
                  );
                  yield* orchestrator.dispatch({
                    type: "message.dispatch",
                    commandId: CommandId.make("following"),
                    threadId,
                    messageId: MessageId.make("message:following"),
                    text: "continue without an explicit selection",
                    attachments: [],
                    dispatchMode: { type: "start_immediately" },
                    createdBy: "agent",
                    creationSource: "server",
                  });
                  yield* worker.drain();
                  yield* Fiber.join(followingRunning);
                  assert.equal(started.length, 2);
                  assert.deepEqual(started[1]!.modelSelection, desiredSelection);
                  assert.deepEqual(started[0]!.modelSelection, modelSelection);
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
              assert.equal(steerCalls, timing === "during delivery" ? 2 : 0);
              const final = yield* orchestrator.getThreadProjection(threadId);
              if (timing === "during delivery") {
                assert.deepEqual(started[1]!.modelSelection, desiredSelection);
                assert.deepEqual(final.thread.modelSelection, desiredSelection);
                assert.deepEqual(
                  final.runs.find((run) => run.id === first.runId)!.modelSelection,
                  modelSelection,
                );
                assert.deepEqual(
                  final.runs.find((run) => run.id === started[1]!.runId)!.modelSelection,
                  desiredSelection,
                );
                assert.equal(final.runs.length, 2);
                assert.equal(interrupts, 0);
              }
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
                makeOrchestratorV2ReplayLayerWithRegistry(
                  { name: `steering-completion-${timing}` },
                  makeSingleLayer(adapter),
                  { runEffectWorker: false },
                ),
              ),
            );
          }),
        ),
    );
  }
}

it.effect("steers a Pi-like turn when composer selection differs but stays turn-scoped", () =>
  Effect.scoped(
    Effect.gen(function* () {
      // Replay cannot express this: the failure happens in command policy
      // before a provider transcript exists. Pi advertises active steering
      // without interrupt-restart; a composer option diff must not force restart.
      const cwd = yield* checkpointWorkspace("steer-turn-scoped-selection");
      const events = yield* Queue.unbounded<ProviderAdapterV2Event>();
      const started: ProviderAdapterV2TurnInput[] = [];
      const steered: string[] = [];
      let interrupts = 0;
      const piInstanceId = ProviderInstanceId.make("pi");
      const runningSelection = {
        instanceId: piInstanceId,
        model: "grok-4.6",
        options: [{ id: "thinking", value: "high" }],
      } satisfies ModelSelection;
      const composerSelection = {
        instanceId: piInstanceId,
        model: "grok-4.6",
        options: [{ id: "thinking", value: "low" }],
      } satisfies ModelSelection;
      const now = yield* DateTime.now;
      const piCapabilities = {
        ...CodexProviderCapabilitiesV2,
        turns: {
          ...CodexProviderCapabilitiesV2.turns,
          supportsActiveSteering: true,
          supportsSteeringByInterruptRestart: false,
        },
      };
      let transition: "apply_on_next_turn" | "restart_session" | "create_with_handoff" | "reject" =
        "apply_on_next_turn";
      const adapter: ProviderAdapterV2Shape = {
        instanceId: piInstanceId,
        driver: ProviderDriverKind.make("pi"),
        getCapabilities: () => Effect.succeed(piCapabilities),
        planSelectionTransition: () =>
          Effect.succeed(
            transition === "reject"
              ? { type: "reject", reason: "Unsupported selection" }
              : { type: transition },
          ),
        openSession: (input) =>
          Effect.succeed({
            instanceId: piInstanceId,
            driver: ProviderDriverKind.make("pi"),
            providerSessionId: input.providerSessionId,
            providerSession: {
              id: input.providerSessionId,
              driver: ProviderDriverKind.make("pi"),
              providerInstanceId: piInstanceId,
              status: "ready",
              cwd,
              model: runningSelection.model,
              capabilities: piCapabilities,
              createdAt: now,
              updatedAt: now,
              lastError: null,
            },
            events: Stream.fromQueue(events),
            ensureThread: ({ threadId }) =>
              Effect.succeed({
                id: ProviderThreadId.make(`provider-thread:${threadId}`),
                driver: ProviderDriverKind.make("pi"),
                providerInstanceId: piInstanceId,
                providerSessionId: input.providerSessionId,
                appThreadId: threadId,
                ownerNodeId: null,
                nativeThreadRef: {
                  driver: ProviderDriverKind.make("pi"),
                  nativeId: "native-thread",
                  strength: "strong",
                },
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
                const startedAt = yield* DateTime.now;
                yield* Queue.offer(events, {
                  type: "provider_turn.updated",
                  driver: ProviderDriverKind.make("pi"),
                  providerTurn: {
                    id: ProviderTurnId.make(`provider-turn:${turn.attemptId}`),
                    providerThreadId: turn.providerThread.id,
                    nodeId: turn.rootNodeId,
                    runAttemptId: turn.attemptId,
                    nativeTurnRef: {
                      driver: ProviderDriverKind.make("pi"),
                      nativeId: `native:${turn.attemptId}`,
                      strength: "strong",
                    },
                    ordinal: turn.providerTurnOrdinal,
                    status: "running",
                    startedAt,
                    completedAt: null,
                  },
                });
              }),
            steerTurn: (turn) =>
              Effect.sync(() => {
                steered.push(turn.message.text);
              }),
            interruptTurn: () =>
              Effect.sync(() => {
                interrupts += 1;
              }),
            respondToRuntimeRequest: () => Effect.void,
            readThreadSnapshot: () => Effect.die("unused"),
            rollbackThread: () => Effect.die("unused"),
            forkThread: () => Effect.die("unused"),
          }),
      };
      yield* Effect.gen(function* () {
        const orchestrator = yield* OrchestratorV2;
        const worker = yield* OrchestrationEffectWorkerV2;
        const threadId = ThreadId.make("thread:steer-turn-scoped-selection");
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
          projectId: ProjectId.make("project:steer-turn-scoped-selection"),
          title: "Steer turn-scoped selection",
          modelSelection: runningSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
          createdBy: "user",
          creationSource: "web",
        });
        const running = yield* watch(
          (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
        );
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("first"),
          threadId,
          messageId: MessageId.make("message:first"),
          text: "first",
          attachments: [],
          modelSelection: runningSelection,
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();
        yield* Fiber.join(running);
        const first = started[0];
        assert.isDefined(first);
        const active = yield* orchestrator.getThreadProjection(threadId);
        const sink = yield* EventSinkV2;
        const assertDesiredSelection = Effect.fnUntraced(function* (
          selection: ModelSelection,
          command: string,
          changed: boolean,
        ) {
          const projection = yield* orchestrator.getThreadProjection(threadId);
          assert.deepEqual(projection.thread.modelSelection, selection);
          assert.deepEqual(projection.runs, active.runs);
          assert.deepEqual(projection.attempts, active.attempts);
          assert.deepEqual(projection.providerSessions, active.providerSessions);
          assert.deepEqual(projection.providerThreads, active.providerThreads);
          assert.deepEqual(projection.providerTurns, active.providerTurns);
          assert.equal(started.length, 1);
          assert.equal(interrupts, 0);
          const stored = yield* sink
            .readByCommandId({ commandId: CommandId.make(command) })
            .pipe(Stream.runCollect);
          const updates = stored.filter(
            ({ event }) => event.type === "thread.model-selection-updated",
          );
          assert.equal(updates.length, changed ? 1 : 0);
          if (changed) {
            const event = updates[0]!.event;
            assert.equal(event.type, "thread.model-selection-updated");
            if (event.type === "thread.model-selection-updated")
              assert.deepEqual(event.payload.modelSelection, selection);
          }
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("steer"),
          threadId,
          messageId: MessageId.make("message:steer"),
          text: "keep going",
          attachments: [],
          modelSelection: composerSelection,
          dispatchMode: { type: "steer_active", targetRunId: first.runId },
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();
        assert.deepEqual(steered, ["keep going"]);
        yield* assertDesiredSelection(composerSelection, "steer", true);
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("steer-model"),
          threadId,
          messageId: MessageId.make("message:steer-model"),
          text: "change model next turn",
          attachments: [],
          modelSelection: { ...composerSelection, model: "another-model" },
          dispatchMode: { type: "steer_active", targetRunId: first.runId },
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();
        assert.deepEqual(steered, ["keep going", "change model next turn"]);
        const futureSelection = { ...composerSelection, model: "another-model" };
        yield* assertDesiredSelection(futureSelection, "steer-model", true);
        for (const [step, selection, changed] of [
          ["back-to-running", runningSelection, true],
          ["repeat-running", runningSelection, false],
          ["future-again", futureSelection, true],
          ["repeat", futureSelection, false],
        ] as const) {
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(step),
            threadId,
            messageId: MessageId.make(`message:${step}`),
            text: step,
            attachments: [],
            modelSelection: selection,
            dispatchMode: { type: "steer_active", targetRunId: first.runId },
            createdBy: "user",
            creationSource: "web",
          });
          yield* worker.drain();
          yield* assertDesiredSelection(selection, step, changed);
        }
        const beforeRejection = yield* orchestrator.getThreadProjection(threadId);
        const acceptedSteers = [...steered];
        for (const policy of [
          "reject",
          "restart_session",
          "create_with_handoff",
          "cross_instance",
          "explicit_restart",
        ] as const) {
          transition =
            policy === "restart_session" || policy === "create_with_handoff" || policy === "reject"
              ? policy
              : "apply_on_next_turn";
          const result = yield* orchestrator
            .dispatch({
              type: "message.dispatch",
              commandId: CommandId.make(`blocked:${policy}`),
              threadId,
              messageId: MessageId.make(`message:blocked:${policy}`),
              text: "must not steer",
              attachments: [],
              modelSelection:
                policy === "cross_instance"
                  ? { ...composerSelection, instanceId: ProviderInstanceId.make("other") }
                  : composerSelection,
              dispatchMode: {
                type: policy === "explicit_restart" ? "restart_active" : "steer_active",
                targetRunId: first.runId,
              },
              createdBy: "user",
              creationSource: "web",
            })
            .pipe(Effect.result);
          assert.equal(result._tag, "Failure", policy);
          assert.deepEqual(yield* orchestrator.getThreadProjection(threadId), beforeRejection);
          const stored = yield* sink
            .readByCommandId({ commandId: CommandId.make(`blocked:${policy}`) })
            .pipe(Stream.runCollect);
          assert.isEmpty(stored);
        }
        yield* worker.drain();
        assert.deepEqual(steered, acceptedSteers);
        yield* assertDesiredSelection(futureSelection, "future-again", true);
        transition = "apply_on_next_turn";
        for (const [creationSource, selection] of [
          ["server", runningSelection],
          ["mcp", futureSelection],
        ] as const) {
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(`agent:${creationSource}`),
            threadId,
            messageId: MessageId.make(`message:agent:${creationSource}`),
            text: "explicit agent selection",
            attachments: [],
            modelSelection: selection,
            dispatchMode: { type: "steer_active", targetRunId: first.runId },
            createdBy: "agent",
            creationSource,
          });
          yield* worker.drain();
          yield* assertDesiredSelection(selection, `agent:${creationSource}`, true);
        }
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("queued"),
          threadId,
          messageId: MessageId.make("message:queued"),
          text: "promote this queued message",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
        const queued = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.status === "queued",
        )!;
        assert.deepEqual(queued.modelSelection, futureSelection);
        const promotionSelection = { ...futureSelection, model: "promotion-model" };
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("select-before-promotion"),
          threadId,
          messageId: MessageId.make("message:select-before-promotion"),
          text: "select before promotion",
          attachments: [],
          modelSelection: promotionSelection,
          dispatchMode: { type: "steer_active", targetRunId: first.runId },
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();
        yield* orchestrator.dispatch({
          type: "queued-message.promote-to-steer",
          commandId: CommandId.make("promote"),
          threadId,
          queuedRunId: queued.id,
          targetRunId: first.runId,
        });
        yield* worker.drain();
        const promoted = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(promoted.thread.modelSelection, promotionSelection);
        assert.deepEqual(
          promoted.runs.find((run) => run.id === first.runId),
          active.runs[0],
        );
        assert.deepEqual(
          promoted.attempts.filter((attempt) => attempt.runId === first.runId),
          active.attempts,
        );
        assert.deepEqual(promoted.providerSessions, active.providerSessions);
        assert.deepEqual(promoted.providerTurns, active.providerTurns);
        assert.equal(promoted.runs.find((run) => run.id === queued.id)?.status, "cancelled");
        assert.equal(
          promoted.messages.find((message) => message.id === queued.userMessageId)?.runId,
          first.runId,
        );
        assert.equal(steered.filter((text) => text === "promote this queued message").length, 1);
        assert.equal(started.length, 1);
        assert.equal(interrupts, 0);
        const promotionEvents = yield* sink
          .readByCommandId({ commandId: CommandId.make("promote") })
          .pipe(Stream.runCollect);
        assert.isEmpty(
          promotionEvents.filter(({ event }) => event.type === "thread.model-selection-updated"),
        );
        const completed = yield* watch(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === first.runId &&
            event.payload.status === "waiting",
        );
        const turn = active.providerTurns[0]!;
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver: adapter.driver,
          providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
        });
        yield* Queue.offer(events, {
          type: "turn.terminal",
          driver: adapter.driver,
          providerThreadId: turn.providerThreadId,
          providerTurnId: turn.id,
          runOrdinal: first.runOrdinal,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* Fiber.join(completed);
        yield* worker.drain();
        const followingRunning = yield* watch(
          (event) =>
            event.type === "provider-turn.updated" &&
            event.payload.status === "running" &&
            event.payload.id !== turn.id,
        );
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("following"),
          threadId,
          messageId: MessageId.make("message:following"),
          text: "follow up without selection",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "server",
        });
        yield* worker.drain();
        yield* Fiber.join(followingRunning);
        assert.equal(started.length, 2);
        assert.deepEqual(started[1]!.modelSelection, promotionSelection);
        assert.deepEqual(started[0]!.modelSelection, runningSelection);
        const final = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(
          final.runs.find((run) => run.id === first.runId)!.modelSelection,
          runningSelection,
        );
        assert.deepEqual(
          final.runs.find((run) => run.id === started[1]!.runId)!.modelSelection,
          promotionSelection,
        );
      }).pipe(
        Effect.provide(
          makeOrchestratorV2ReplayLayerWithRegistry(
            { name: "steer-turn-scoped-selection" },
            makeSingleLayer(adapter),
            { runEffectWorker: false },
          ),
        ),
      );
    }),
  ),
);
