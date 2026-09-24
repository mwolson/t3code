// Pooled runtime ownership fixture; Codex transport is covered separately.
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
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import { OrchestrationEffectWorkerV2 } from "./EffectWorker.ts";
import { OrchestratorV2 } from "./Orchestrator.ts";
import { ProviderSessionManagerV2 } from "./ProviderSessionManager.ts";
import { ProviderAdapterEnsureThreadError } from "./ProviderAdapter.ts";
import type {
  ProviderAdapterV2Event,
  ProviderAdapterV2Shape,
  ProviderAdapterV2TurnInput,
} from "./ProviderAdapter.ts";
import { makeSingleLayer } from "./ProviderAdapterRegistry.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "./testkit/ReplayFixtureWorkspace.ts";

const driver = ProviderDriverKind.make("codex");
const instanceId = ProviderInstanceId.make("codex");
const A: ModelSelection = {
  instanceId,
  model: "gpt-review",
  options: [{ id: "effort", value: "low" }],
};

for (const residency of ["idle-release", "resident", "failed-ensure", "server-release"] as const) {
  it.effect(`pooled binding after ${residency}`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const cwd = yield* checkpointWorkspace(`review-pooled-${residency}`);
        const queues = new Map<string, Queue.Queue<ProviderAdapterV2Event>>();
        const opens: string[] = [];
        const closes: string[] = [];
        const calls: ProviderAdapterV2TurnInput[] = [];
        let failEnsure = residency === "failed-ensure";
        const adapter: ProviderAdapterV2Shape = {
          driver,
          instanceId,
          getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
          planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
          openSession: (input) =>
            Effect.gen(function* () {
              const events = yield* Queue.unbounded<ProviderAdapterV2Event>();
              queues.set(input.providerSessionId, events);
              opens.push(input.providerSessionId);
              yield* Effect.addFinalizer(() =>
                Effect.sync(() => void closes.push(input.providerSessionId)),
              );
              const now = yield* DateTime.now;
              const bind = (thread: OrchestrationV2ProviderThread) => Effect.succeed(thread);
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
                  capabilities: CodexProviderCapabilitiesV2,
                  createdAt: now,
                  updatedAt: now,
                  lastError: null,
                },
                events: Stream.fromQueue(events),
                ensureThread: ({ threadId, existingProviderThread }) =>
                  Effect.suspend(() => {
                    if (failEnsure) {
                      failEnsure = false;
                      return Effect.fail(
                        new ProviderAdapterEnsureThreadError({
                          driver,
                          threadId,
                          cause: "first ensure failed without a native reference",
                        }),
                      );
                    }
                    return bind({
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
                    });
                  }),
                resumeThread: ({ providerThread }) =>
                  bind({ ...providerThread, providerSessionId: input.providerSessionId }),
                startTurn: (turn) =>
                  Effect.gen(function* () {
                    calls.push(turn);
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
        yield* Effect.gen(function* () {
          const orchestrator = yield* OrchestratorV2;
          const worker = yield* OrchestrationEffectWorkerV2;
          const manager = yield* ProviderSessionManagerV2;
          const send = (threadId: ThreadId, name: string) =>
            Effect.gen(function* () {
              const before = calls.length;
              const started = yield* orchestrator.streamDomainEvents.pipe(
                Stream.filter(
                  (event) =>
                    event.threadId === threadId &&
                    event.type === "provider-turn.updated" &&
                    event.payload.status === "running",
                ),
                Stream.runHead,
                Effect.forkChild({ startImmediately: true }),
              );
              yield* orchestrator.dispatch({
                type: "message.dispatch",
                commandId: CommandId.make(name),
                threadId,
                messageId: MessageId.make(`message:${name}`),
                text: name,
                attachments: [],
                dispatchMode: { type: "start_immediately" },
                createdBy: "user",
                creationSource: "web",
              });
              yield* worker.drain();
              assert.equal(calls.length, before + 1);
              yield* Fiber.join(started);
            });
          const finish = (threadId: ThreadId) =>
            Effect.gen(function* () {
              const completed = yield* orchestrator.streamDomainEvents.pipe(
                Stream.filter(
                  (event) =>
                    event.threadId === threadId &&
                    event.type === "run.updated" &&
                    event.payload.status === "waiting",
                ),
                Stream.runHead,
                Effect.forkChild({ startImmediately: true }),
              );
              const projection = yield* orchestrator.getThreadProjection(threadId);
              const run = projection.runs.at(-1)!;
              const turn = projection.providerTurns.find(
                (t) => t.runAttemptId === run.activeAttemptId,
              )!;
              const binding = projection.providerThreads.find(
                (t) => t.id === run.providerThreadId,
              )!;
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
              yield* Fiber.join(completed);
              yield* worker.drain();
            });
          const t1 = ThreadId.make("thread:pooled-one");
          const t2 = ThreadId.make("thread:pooled-two");
          for (const id of [t1, t2]) {
            yield* orchestrator.dispatch({
              type: "thread.create",
              commandId: CommandId.make(`create:${id}`),
              threadId: id,
              projectId: ProjectId.make("project:pooled"),
              title: "Pooled",
              modelSelection: A,
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: null,
              worktreePath: cwd,
              createdBy: "user",
              creationSource: "web",
            });
          }
          if (residency === "failed-ensure") {
            yield* orchestrator.dispatch({
              type: "message.dispatch",
              commandId: CommandId.make("failed-ensure"),
              threadId: t1,
              messageId: MessageId.make("failed-ensure"),
              text: "Failed prompt must not replay",
              attachments: [],
              dispatchMode: { type: "start_immediately" },
              createdBy: "user",
              creationSource: "web",
            });
            yield* worker.drain();
            const failed = yield* orchestrator.getThreadProjection(t1);
            assert.equal(failed.runs[0]?.status, "starting");
            assert.isNull(failed.providerThreads[0]?.nativeThreadRef);
            assert.lengthOf(calls, 0);
            yield* orchestrator.dispatch({
              type: "run.interrupt",
              commandId: CommandId.make("cancel-failed-ensure"),
              threadId: t1,
              runId: failed.runs[0]!.id,
            });
            yield* worker.drain();
            yield* send(t1, "explicit-ensure-retry");
            const recovered = yield* orchestrator.getThreadProjection(t1);
            assert.lengthOf(calls, 1);
            assert.lengthOf(recovered.runs, 2);
            assert.equal(
              calls[0]?.providerThread.id,
              failed.providerThreads[0]?.id,
              "retry retains the app binding rather than creating a selection handoff",
            );
            assert.isTrue(calls[0]!.message.text.endsWith("User message:\nexplicit-ensure-retry"));
            assert.isTrue(
              recovered.contextHandoffs.every(
                (handoff) => handoff.strategy === "delta_since_target_last_seen",
              ),
              "ordinary history catch-up remains available",
            );
            assert.equal(recovered.runs[0]?.status, "interrupted");
            return;
          }
          yield* send(t1, "one-first");
          yield* finish(t1);
          const shared = calls[0]!.providerThread.providerSessionId!;
          if (residency === "idle-release" || residency === "server-release") {
            yield* manager.release({
              providerSessionId: shared,
              reason: residency === "idle-release" ? "idle_timeout" : "server_shutdown",
            });
            yield* worker.drain();
          }
          yield* send(t2, "two-first");
          yield* send(t1, "one-second");
          assert.equal(opens.length, residency === "resident" ? 1 : 2);
          assert.equal(closes.length, residency === "resident" ? 0 : 1);
          // Pooled contract: both threads share one resident provider process.
          assert.equal(
            calls.at(-1)!.providerThread.providerSessionId,
            calls.at(-2)!.providerThread.providerSessionId,
          );
        }).pipe(
          Effect.provide(
            makeOrchestratorV2ReplayLayerWithRegistry(
              { name: `review-pooled-${residency}` },
              makeSingleLayer(adapter),
              { runEffectWorker: false },
            ),
          ),
        );
      }),
    ),
  );
}
