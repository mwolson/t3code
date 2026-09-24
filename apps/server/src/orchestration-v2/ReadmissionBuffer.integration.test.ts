import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  ThreadId,
  TurnItemId,
  type ModelSelection,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import { OrchestrationEffectWorkerV2 } from "./EffectWorker.ts";
import { OrchestratorV2 } from "./Orchestrator.ts";
import { EventSinkV2 } from "./EventSink.ts";
import { ProviderSessionManagerV2 } from "./ProviderSessionManager.ts";
import {
  ProviderAdapterTurnStartError,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2Shape,
  type ProviderAdapterV2TurnInput,
} from "./ProviderAdapter.ts";
import { makeSingleLayer } from "./ProviderAdapterRegistry.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "./testkit/ReplayFixtureWorkspace.ts";

const driver = ProviderDriverKind.make("codex");
const instanceId = ProviderInstanceId.make("codex");
const selection = (effort: string): ModelSelection => ({
  instanceId,
  model: "same-model",
  options: [{ id: "effort", value: effort }],
});
const A = selection("low");
const B = selection("medium");
const C = selection("high");
const capabilities = CodexProviderCapabilitiesV2;

for (const failure of ["none", "before", "after", "race", "imported"] as const) {
  const uncertain = failure === "before" || failure === "after";
  it.effect(
    failure === "imported"
      ? "freshly admits an imported native binding without run history"
      : `preserves shared buffered output through ${failure} partial failure`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const cwd = yield* checkpointWorkspace(`readmission-buffer-${failure}`);
          const queues = new Map<string, Queue.Queue<ProviderAdapterV2Event>>();
          const buffers = new Map<string, { text: string; selection: ModelSelection }>();
          const calls: ProviderAdapterV2TurnInput[] = [];
          const opens: string[] = [];
          const closes: string[] = [];
          const consumed: Array<{ text: string; selection: ModelSelection; delivery: string }> = [];
          const loaded = new Map<string, ModelSelection>();
          let inspectStarting:
            | ((input: ProviderAdapterV2TurnInput) => Effect.Effect<void>)
            | undefined;
          const adapter: ProviderAdapterV2Shape = {
            driver,
            instanceId,
            getCapabilities: () => Effect.succeed(capabilities),
            planSelectionTransition: ({ current, target }) =>
              Effect.succeed({
                type:
                  current.options?.[0]?.value === "low" && target.options?.[0]?.value === "high"
                    ? "restart_session"
                    : "apply_on_next_turn",
              }),
            openSession: (input) =>
              Effect.gen(function* () {
                const events = yield* Queue.unbounded<ProviderAdapterV2Event>();
                queues.set(input.providerSessionId, events);
                opens.push(input.providerSessionId);
                yield* Effect.addFinalizer(() =>
                  Effect.sync(() => {
                    closes.push(input.providerSessionId);
                  }),
                );
                const now = yield* DateTime.now;
                const bindings = new Set<string>();
                const bind = (thread: OrchestrationV2ProviderThread) =>
                  Effect.sync(() => {
                    bindings.add(thread.id);
                    if (!loaded.has(thread.id)) loaded.set(thread.id, input.modelSelection);
                    return thread;
                  });
                return {
                  driver,
                  instanceId,
                  providerSessionId: input.providerSessionId,
                  providerSession: {
                    id: input.providerSessionId,
                    driver,
                    providerInstanceId: instanceId,
                    status: "ready",
                    cwd,
                    model: input.modelSelection.model,
                    capabilities,
                    createdAt: now,
                    updatedAt: now,
                    lastError: null,
                  },
                  events: Stream.fromQueue(events),
                  hasPendingBackgroundWork: Effect.sync(() =>
                    [...bindings].some((id) => buffers.has(id)),
                  ),
                  hasPendingBackgroundWorkForThread: () => Effect.succeed(false),
                  hasBufferedOutputForThread: (thread) => Effect.sync(() => buffers.has(thread.id)),
                  ensureThread: ({ threadId, existingProviderThread }) =>
                    bind({
                      ...existingProviderThread!,
                      id:
                        existingProviderThread?.id ??
                        ProviderThreadId.make(`provider-thread:${threadId}`),
                      driver,
                      providerInstanceId: instanceId,
                      providerSessionId: input.providerSessionId,
                      appThreadId: threadId,
                      ownerNodeId: null,
                      nativeThreadRef: {
                        driver,
                        nativeId: `native:${threadId}`,
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
                  resumeThread: ({ providerThread }) => bind(providerThread),
                  startTurn: (turn) =>
                    Effect.gen(function* () {
                      assert.isDefined(inspectStarting);
                      yield* inspectStarting!(turn);
                      calls.push(turn);
                      if (turn.message.messageId === "message:reject") {
                        if (failure === "after")
                          loaded.set(turn.providerThread.id, turn.modelSelection);
                        return yield* new ProviderAdapterTurnStartError({
                          driver,
                          threadId: turn.threadId,
                          providerThreadId: turn.providerThread.id,
                          runId: turn.runId,
                          cause: "Controlled partial selection failure.",
                        });
                      }
                      const providerTurnId = ProviderTurnId.make(`provider-turn:${turn.attemptId}`);
                      yield* Queue.offer(events, {
                        type: "provider_turn.updated",
                        driver,
                        providerTurn: {
                          id: providerTurnId,
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
                      if (turn.message.creationSource === "provider") {
                        const buffer = buffers.get(turn.providerThread.id)!;
                        assert.isDefined(buffer, "the original runtime owns the output");
                        consumed.push({ ...buffer, delivery: turn.message.messageId });
                        buffers.delete(turn.providerThread.id);
                        const at = yield* DateTime.now;
                        yield* Queue.offer(events, {
                          type: "turn_item.updated",
                          driver,
                          turnItem: {
                            id: TurnItemId.make(`item:buffer:${turn.message.messageId}`),
                            threadId: turn.threadId,
                            runId: turn.runId,
                            nodeId: turn.rootNodeId,
                            providerThreadId: turn.providerThread.id,
                            providerTurnId,
                            nativeItemRef: null,
                            parentItemId: null,
                            ordinal: turn.runOrdinal * 100 + 1,
                            status: "completed",
                            title: null,
                            startedAt: at,
                            completedAt: at,
                            updatedAt: at,
                            type: "assistant_message",
                            text: buffer.text,
                            messageId: MessageId.make(`assistant:${turn.message.messageId}`),
                            streaming: false,
                          },
                        });
                      } else {
                        loaded.set(turn.providerThread.id, turn.modelSelection);
                      }
                    }),
                  steerTurn: () => Effect.void,
                  interruptTurn: () => Effect.void,
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
            const manager = yield* ProviderSessionManagerV2;
            inspectStarting = (input) =>
              Effect.gen(function* () {
                const runtime = Option.getOrThrow(
                  yield* manager.get(input.providerThread.providerSessionId!).pipe(Effect.orDie),
                );
                assert.isNull(
                  yield* runtime.executionSelection!(input.providerThread),
                  "a requested start has no acknowledgement yet",
                );
              });
            const threadId = ThreadId.make("thread:buffer-owner");
            const siblingId = ThreadId.make("thread:buffer-sibling");
            const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
              orchestrator.streamDomainEvents.pipe(
                Stream.filter(predicate),
                Stream.take(1),
                Stream.runDrain,
                Effect.forkScoped,
              );
            const send = (id: ThreadId, name: string, modelSelection?: ModelSelection) =>
              orchestrator.dispatch({
                type: "message.dispatch",
                commandId: CommandId.make(name),
                threadId: id,
                messageId: MessageId.make(`message:${name}`),
                text: name,
                attachments: [],
                ...(modelSelection ? { modelSelection } : {}),
                dispatchMode: { type: "start_immediately" },
                createdBy: "user",
                creationSource: "web",
              });
            const start = Effect.fnUntraced(function* (id: ThreadId, name: string) {
              const running = yield* watch(
                (event) =>
                  event.threadId === id &&
                  event.type === "provider-turn.updated" &&
                  event.payload.status === "running",
              );
              yield* send(id, name);
              yield* worker.drain();
              yield* Fiber.join(running);
            });
            const finish = Effect.fnUntraced(function* (id: ThreadId) {
              const projection = yield* orchestrator.getThreadProjection(id);
              const run = projection.runs.at(-1)!;
              const turn = projection.providerTurns.find(
                (candidate) => candidate.runAttemptId === run.activeAttemptId,
              )!;
              const binding = projection.providerThreads.find(
                (thread) => thread.id === run.providerThreadId,
              )!;
              const waiting = yield* watch(
                (event) =>
                  event.type === "run.updated" &&
                  event.payload.id === run.id &&
                  event.payload.status === "waiting",
              );
              const events = queues.get(binding.providerSessionId!)!;
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
                runOrdinal: run.ordinal,
                status: "completed",
                failure: null,
                threadDisposition: "reusable",
              });
              yield* Fiber.join(waiting);
              yield* worker.drain();
            });
            for (const id of [threadId, siblingId]) {
              yield* orchestrator.dispatch({
                type: "thread.create",
                commandId: CommandId.make(`create:${id}`),
                threadId: id,
                projectId: ProjectId.make("project:buffer"),
                title: "Buffer",
                modelSelection: A,
                runtimeMode: "full-access",
                interactionMode: "default",
                branch: null,
                worktreePath: cwd,
                createdBy: "user",
                creationSource: "web",
              });
            }
            if (failure === "imported") {
              const sink = yield* EventSinkV2;
              const now = yield* DateTime.now;
              yield* sink.write({
                events: [
                  {
                    id: EventId.make("imported-binding"),
                    type: "provider-thread.updated",
                    threadId,
                    providerInstanceId: instanceId,
                    occurredAt: now,
                    payload: {
                      id: ProviderThreadId.make("imported-native-binding"),
                      driver,
                      providerInstanceId: instanceId,
                      providerSessionId: null,
                      appThreadId: threadId,
                      ownerNodeId: null,
                      nativeThreadRef: {
                        driver,
                        nativeId: "imported-native-with-no-runs",
                        strength: "strong",
                      },
                      nativeConversationHeadRef: null,
                      status: "not_loaded",
                      firstRunOrdinal: null,
                      lastRunOrdinal: null,
                      handoffIds: [],
                      forkedFrom: null,
                      createdAt: now,
                      updatedAt: now,
                    },
                  },
                ],
              });
            }
            yield* start(threadId, "first");
            if (failure === "imported")
              assert.equal(
                calls[0]!.providerThread.nativeThreadRef?.nativeId,
                "imported-native-with-no-runs",
              );
            yield* finish(threadId);
            if (failure === "imported") {
              const imported = calls[0]!.providerThread;
              const resident = Option.getOrThrow(yield* manager.get(imported.providerSessionId!));
              assert.deepEqual(yield* resident.executionSelection!(imported), A);
              assert.equal(opens.length, 1);
              return;
            }
            yield* start(siblingId, "sibling");
            assert.equal(opens.length, 1, "both threads genuinely share residency");
            const owner = (yield* orchestrator.getThreadProjection(threadId)).providerThreads[0]!;
            const oldSession = owner.providerSessionId!;
            const oldRuntime = Option.getOrThrow(yield* manager.get(oldSession));
            assert.deepEqual(yield* oldRuntime.executionSelection!(owner), A);
            assert.isNull(
              yield* oldRuntime.executionSelection!({
                ...owner,
                id: ProviderThreadId.make("another-binding"),
              }),
            );
            assert.isNull(
              yield* oldRuntime.executionSelection!({
                ...owner,
                nativeThreadRef: { driver, nativeId: "another-native-thread", strength: "strong" },
              }),
            );
            if (uncertain) {
              yield* send(threadId, "reject", B);
              yield* worker.drain();
              const failed = (yield* orchestrator.getThreadProjection(threadId)).runs.at(-1)!;
              assert.equal(failed.status, "failed");
              assert.isNull(yield* oldRuntime.executionSelection!(owner));
              buffers.set(owner.id, {
                text: "Owned output after ambiguous application",
                selection: loaded.get(owner.id)!,
              });
            } else {
              if (failure === "race") yield* send(threadId, "admitted-before-buffer", B);
              buffers.set(owner.id, { text: "Owned original output", selection: A });
              if (failure === "race") {
                yield* worker.drain();
                assert.equal(
                  (yield* orchestrator.getThreadProjection(threadId)).runs.at(-1)!.status,
                  "failed",
                );
                assert.deepEqual(yield* oldRuntime.executionSelection!(owner), A);
                assert.isFalse(
                  calls.some((call) => call.message.messageId === "message:admitted-before-buffer"),
                );
              }
            }
            yield* orchestrator.dispatch({
              type: "thread.model-selection.set",
              commandId: CommandId.make("set-c"),
              threadId,
              modelSelection: C,
            });
            yield* worker.drain();
            assert.isTrue(
              Option.isSome(yield* manager.get(oldSession)),
              "selection detach retains the owner runtime",
            );
            assert.isTrue(buffers.has(owner.id));
            const attemptedNewExecution = yield* Effect.exit(send(threadId, "blocked-following"));
            assert.equal(attemptedNewExecution._tag, "Failure");
            const notification = {
              type: "message.dispatch" as const,
              commandId: CommandId.make("deliver"),
              threadId,
              messageId: MessageId.make("message:deliver"),
              text: "Do not execute this placeholder",
              attachments: [],
              dispatchMode: { type: "queue_after_active" as const },
              createdBy: "agent" as const,
              creationSource: "provider" as const,
              notification: {
                source: { kind: "background_task" as const },
                outcome: "updated" as const,
                summary: "Owned output",
              },
            };
            if (uncertain) {
              assert.equal(
                (yield* Effect.exit(orchestrator.dispatch(notification)))._tag,
                "Failure",
              );
              assert.isTrue(buffers.has(owner.id));
              assert.deepEqual(consumed, []);
              assert.equal(
                calls.filter((call) => call.message.messageId === "message:reject").length,
                1,
              );
            } else {
              const item = yield* watch(
                (event) =>
                  event.type === "turn-item.updated" &&
                  event.payload.type === "assistant_message" &&
                  event.payload.text === "Owned original output",
              );
              yield* orchestrator.dispatch(notification);
              yield* worker.drain();
              yield* Fiber.join(item);
              assert.deepEqual(consumed, [
                { text: "Owned original output", selection: A, delivery: "message:deliver" },
              ]);
              assert.equal(calls.at(-1)!.providerThread.providerSessionId, oldSession);
              assert.deepEqual(calls.at(-1)!.modelSelection, A);
              assert.deepEqual(
                (yield* orchestrator.getThreadProjection(threadId)).thread.modelSelection,
                C,
              );
              yield* finish(threadId);
              yield* start(threadId, "following");
              assert.equal(opens.length, 2);
              assert.deepEqual(calls.at(-1)!.modelSelection, C);
              const replacement = calls.at(-1)!.providerThread;
              const replacementRuntime = Option.getOrThrow(
                yield* manager.get(replacement.providerSessionId!),
              );
              assert.deepEqual(yield* replacementRuntime.executionSelection!(replacement), C);
              assert.isNull(yield* oldRuntime.executionSelection!(owner));
              const delivered = yield* oldRuntime.events.pipe(
                Stream.filter((event) => event.type === "turn.terminal" && event.runOrdinal === 1),
                Stream.take(1),
                Stream.runDrain,
                Effect.forkScoped,
              );
              const firstTurn = (yield* orchestrator.getThreadProjection(threadId))
                .providerTurns[0]!;
              yield* Queue.offer(queues.get(oldSession)!, {
                type: "turn.terminal",
                driver,
                providerThreadId: owner.id,
                providerTurnId: firstTurn.id,
                runOrdinal: 1,
                status: "completed",
                failure: null,
                threadDisposition: "reusable",
              });
              yield* Fiber.join(delivered);
              assert.deepEqual(
                yield* replacementRuntime.executionSelection!(replacement),
                C,
                "late output from old residency cannot certify or relabel its replacement",
              );
              assert.isNull(yield* oldRuntime.executionSelection!(owner));
            }
            yield* finish(siblingId);
            yield* start(siblingId, "sibling-following");
            assert.equal(calls.at(-1)!.providerThread.providerSessionId, oldSession);
            assert.isFalse(
              closes.includes(oldSession),
              "the unrelated shared thread remains alive",
            );
            const sibling = calls.at(-1)!.providerThread;
            yield* finish(siblingId);
            assert.deepEqual(yield* oldRuntime.executionSelection!(sibling), A);
            yield* oldRuntime.resumeThread({
              providerThread: sibling,
              threadId: siblingId,
              modelSelection: B,
              runtimePolicy: calls.at(-1)!.runtimePolicy,
            });
            assert.isNull(
              yield* oldRuntime.executionSelection!(sibling),
              "a resume request is not an execution acknowledgement",
            );
            yield* orchestrator.dispatch({
              type: "provider-session.detach",
              commandId: CommandId.make("stop-sibling"),
              threadId: siblingId,
              providerSessionId: oldSession,
              reason: "Explicit Stop",
            });
            yield* worker.drain();
            assert.isNull(yield* oldRuntime.executionSelection!(sibling));
            if (uncertain) {
              yield* TestClock.adjust("5 hours");
              assert.isTrue(
                Option.isSome(yield* manager.get(oldSession)),
                "blocked owned output survives the normal idle-pin cap",
              );
              assert.isTrue(buffers.has(owner.id));
            }
          }).pipe(
            Effect.provide(
              makeOrchestratorV2ReplayLayerWithRegistry(
                { name: `readmission-buffer-${failure}` },
                makeSingleLayer(adapter),
                { runEffectWorker: false },
              ),
            ),
          );
        }),
      ),
  );
}
