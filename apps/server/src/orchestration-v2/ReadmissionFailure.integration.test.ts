import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  ThreadId,
  type ModelSelection,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import { OrchestrationEffectWorkerV2 } from "./EffectWorker.ts";
import { OrchestratorV2 } from "./Orchestrator.ts";
import {
  ProviderAdapterTurnStartError,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2Shape,
  type ProviderAdapterV2TurnInput,
} from "./ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
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
const capabilities = {
  ...CodexProviderCapabilitiesV2,
  sessions: {
    ...CodexProviderCapabilitiesV2.sessions,
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: false,
  },
};

it.effect(
  "requires fresh admission after either partial-start outcome without replaying the failed prompt",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const cwd = yield* checkpointWorkspace("admission-execution-evidence");
        const results = [];
        for (const applyBeforeFailure of [false, true]) {
          const eventQueues = new Map<string, Queue.Queue<ProviderAdapterV2Event>>();
          const closed = new Set<string>();
          let loaded = A;
          let pending = false;
          const calls: ProviderAdapterV2TurnInput[] = [];
          const providerTurns: string[] = [];
          const plans: Array<{ current: ModelSelection; target: ModelSelection }> = [];
          let opens = 0;
          const adapter: ProviderAdapterV2Shape = {
            driver,
            instanceId,
            getCapabilities: () => Effect.succeed(capabilities),
            planSelectionTransition: ({ current, target }) =>
              Effect.sync(() => {
                plans.push({ current, target });
                return {
                  type:
                    current.options?.[0]?.value === "low" && target.options?.[0]?.value === "high"
                      ? "restart_session"
                      : "apply_on_next_turn",
                };
              }),
            openSession: (input) =>
              Effect.gen(function* () {
                opens += 1;
                loaded = input.modelSelection;
                const events = yield* Queue.unbounded<ProviderAdapterV2Event>();
                yield* Effect.addFinalizer(() =>
                  Effect.sync(() => {
                    closed.add(input.providerSessionId);
                  }),
                );
                const now = yield* DateTime.now;
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
                  hasPendingBackgroundWork: Effect.sync(() => pending),
                  ensureThread: ({ threadId, existingProviderThread }) =>
                    Effect.succeed({
                      ...existingProviderThread!,
                      id:
                        existingProviderThread?.id ??
                        ProviderThreadId.make(`provider-thread:${threadId}`),
                      driver,
                      providerInstanceId: instanceId,
                      providerSessionId: input.providerSessionId,
                      appThreadId: threadId,
                      ownerNodeId: null,
                      nativeThreadRef: { driver, nativeId: "native:evidence", strength: "strong" },
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
                      calls.push(turn);
                      eventQueues.set(turn.attemptId, events);
                      if (turn.message.text === "reject") {
                        // Both outcomes obey the current error type: it does not state whether
                        // a selection-changing operation completed before the request failed.
                        if (applyBeforeFailure) loaded = turn.modelSelection;
                        return yield* new ProviderAdapterTurnStartError({
                          driver,
                          threadId: turn.threadId,
                          providerThreadId: turn.providerThread.id,
                          runId: turn.runId,
                          cause: "controlled request rejection",
                        });
                      }
                      loaded = turn.modelSelection;
                      const id = ProviderTurnId.make(`provider-turn:${turn.attemptId}`);
                      providerTurns.push(id);
                      yield* Queue.offer(events, {
                        type: "provider_turn.updated",
                        driver,
                        providerTurn: {
                          id,
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
                  steerTurn: () => Effect.void,
                  interruptTurn: () => Effect.void,
                  respondToRuntimeRequest: () => Effect.void,
                  readThreadSnapshot: () => Effect.die("unused"),
                  rollbackThread: () => Effect.die("unused"),
                  forkThread: () => Effect.die("unused"),
                };
              }),
          };
          const result = yield* Effect.gen(function* () {
            const orchestrator = yield* OrchestratorV2;
            const worker = yield* OrchestrationEffectWorkerV2;
            const threadId = ThreadId.make("thread:execution-evidence");
            const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
              orchestrator.streamDomainEvents.pipe(
                Stream.filter(predicate),
                Stream.take(1),
                Stream.runDrain,
                Effect.forkScoped,
              );
            const send = (name: string, modelSelection?: ModelSelection) =>
              orchestrator.dispatch({
                type: "message.dispatch",
                commandId: CommandId.make(name),
                threadId,
                messageId: MessageId.make(`message:${name}`),
                text: name,
                attachments: [],
                ...(modelSelection ? { modelSelection } : {}),
                dispatchMode: { type: "start_immediately" },
                createdBy: "user",
                creationSource: "web",
              });
            yield* orchestrator.dispatch({
              type: "thread.create",
              commandId: CommandId.make("create"),
              threadId,
              projectId: ProjectId.make("project:execution-evidence"),
              title: "Evidence",
              modelSelection: A,
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: null,
              worktreePath: cwd,
              createdBy: "user",
              creationSource: "web",
            });
            const running = yield* watch(
              (event) =>
                event.type === "provider-turn.updated" && event.payload.status === "running",
            );
            yield* send("first");
            yield* worker.drain();
            yield* Fiber.join(running);
            const initial = yield* orchestrator.getThreadProjection(threadId);
            const first = initial.runs[0]!;
            const turn = initial.providerTurns.find(
              (candidate) => candidate.runAttemptId === first.activeAttemptId,
            )!;
            const waiting = yield* watch(
              (event) =>
                event.type === "run.updated" &&
                event.payload.id === first.id &&
                event.payload.status === "waiting",
            );
            const events = eventQueues.get(first.activeAttemptId!)!;
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
              runOrdinal: first.ordinal,
              status: "completed",
              failure: null,
              threadDisposition: "reusable",
            });
            yield* Fiber.join(waiting);
            yield* worker.drain();
            yield* TestClock.adjust("1 second");
            yield* send("reject", B);
            yield* worker.drain();
            const failed = yield* orchestrator.getThreadProjection(threadId);
            const rejected = failed.runs.at(-1)!;
            assert.equal(rejected.status, "failed");
            assert.isNotNull(rejected.startedAt);
            const rejectedAttempt = failed.attempts.find(
              (candidate) => candidate.id === rejected.activeAttemptId,
            )!;
            assert.isNotNull(rejectedAttempt.startedAt);
            assert.equal(
              providerTurns.length,
              1,
              "the adapter emitted no provider turn for the rejected request",
            );
            pending = true;
            const blocked = yield* Effect.exit(send("blocked-replacement", C));
            assert.equal(
              blocked._tag,
              "Failure",
              "unknown selection cannot replace a runtime with pending work",
            );
            assert.equal(opens, 1);
            assert.equal(closed.size, 0);
            assert.equal(calls.length, 2);
            pending = false;
            const actualBeforeSet = loaded;
            const terminalLooking = failed.runs.findLast(
              (run) =>
                run.providerThreadId === failed.thread.activeProviderThreadId &&
                run.startedAt !== null,
            );
            assert.deepEqual(terminalLooking?.modelSelection, B);
            const ownedTurns = failed.providerTurns.filter(
              (candidate) => candidate.runAttemptId === rejectedAttempt.id,
            );
            const evidence = {
              desired: failed.thread.modelSelection,
              runs: failed.runs.map((run) => ({
                ordinal: run.ordinal,
                selection: run.modelSelection,
                status: run.status,
                started: run.startedAt !== null,
                activeAttemptOrdinal: failed.attempts.find(
                  (attempt) => attempt.id === run.activeAttemptId,
                )?.attemptOrdinal,
              })),
              attempts: failed.attempts.map((attempt) => ({
                ordinal: attempt.attemptOrdinal,
                status: attempt.status,
                started: attempt.startedAt !== null,
                nativeThreadId: attempt.nativeThreadId,
              })),
              providerTurns: failed.providerTurns.map((candidate) => ({
                ordinal: candidate.ordinal,
                status: candidate.status,
                native: candidate.nativeTurnRef,
              })),
              sessions: failed.providerSessions.map((session) => ({
                model: session.model,
                status: session.status,
                instanceId: session.providerInstanceId,
              })),
              bindings: failed.providerThreads.map((thread) => ({
                native: thread.nativeThreadRef,
                status: thread.status,
                instanceId: thread.providerInstanceId,
                hasSession: thread.providerSessionId !== null,
              })),
            };
            yield* orchestrator.dispatch({
              type: "thread.model-selection.set",
              commandId: CommandId.make("set-c"),
              threadId,
              modelSelection: C,
            });
            yield* worker.drain();
            const resumed = yield* watch(
              (event) =>
                event.type === "provider-turn.updated" &&
                event.payload.status === "running" &&
                event.payload.runAttemptId !== first.activeAttemptId,
            );
            yield* send("following");
            yield* worker.drain();
            yield* Fiber.join(resumed);
            const admitted = yield* orchestrator.getThreadProjection(threadId);
            assert.equal(opens, 2, "uncertain residency must not be reused");
            assert.deepEqual(
              calls.map((call) => call.message.messageId),
              ["message:first", "message:reject", "message:following"],
            );
            assert.isTrue(calls.at(-1)!.message.text.endsWith("following"));
            assert.deepEqual(calls.at(-1)!.modelSelection, C);
            assert.equal(admitted.runs.find((run) => run.id === rejected.id)!.status, "failed");
            assert.equal(admitted.runs.length, 3);
            const oldSessionId = calls[0]!.providerThread.providerSessionId!;
            const newSessionId = calls.at(-1)!.providerThread.providerSessionId!;
            assert.notEqual(oldSessionId, newSessionId);
            assert.isTrue(closed.has(oldSessionId));
            assert.isFalse(closed.has(newSessionId));
            return {
              applyBeforeFailure,
              actualBeforeSet,
              inferred: terminalLooking!.modelSelection,
              adapterEmittedProviderTurns: providerTurns.length,
              syntheticRejectedTurns: ownedTurns,
              opens,
              calls: calls.map((call) => call.modelSelection),
              plans,
              evidence,
            };
          }).pipe(
            Effect.provide(
              ProviderReplayHarness.layerWithRegistry(
                { name: "admission-execution-evidence" },
                ProviderAdapterRegistry.layerSingle(adapter),
                { runEffectWorker: false },
              ),
            ),
          );
          results.push(result);
        }
        // Native failed-turn ids include run-derived identifiers; they are identical in
        // these deterministic scenarios. No sleeps or absence inferred from timeouts.
        assert.deepEqual(results[0]!.evidence, results[1]!.evidence);
        assert.deepEqual(results[0]!.actualBeforeSet, A);
        assert.deepEqual(results[1]!.actualBeforeSet, B);
        for (const result of results) {
          assert.isFalse(
            result.plans.some(
              (plan) =>
                plan.current.options?.[0]?.value === "medium" &&
                plan.target.options?.[0]?.value === "high",
            ),
            "failed B is not trustworthy execution evidence",
          );
        }
      }),
    ),
);
