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
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import { OrchestrationEffectWorkerV2 } from "./EffectWorker.ts";
import { OrchestratorV2 } from "./Orchestrator.ts";
import {
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
  options: [
    { id: "effort", value: effort },
    { id: "enabled", value: true },
  ],
});
const A = selection("low");
const B = selection("medium");
const C = selection("high");
const effort = (value: ModelSelection) =>
  value.options?.find((option) => option.id === "effort")?.value;

it.effect.each(
  (() => {
    const cases = [];
    for (const origin of ["web", "server", "mcp"] as const) {
      for (const pooled of [false, true]) {
        for (const transition of [
          "apply_on_next_turn",
          "restart_session",
          "handoff",
          "reject",
        ] as const) {
          for (const path of ["ordinary", "queued", "selection-set"] as const) {
            if (transition === "reject" && path === "queued") continue;
            const name = `${origin}-${path}-${transition}-${pooled ? "pooled" : "exclusive"}`;
            cases.push({
              origin,
              pooled,
              transition,
              path,
              name,
              label: `admits from full execution evidence: ${name}`,
            });
          }
        }
      }
    }
    return cases;
  })(),
)("admits from full execution evidence: $name", ({ origin, pooled, transition, path, name }) =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace(`execution-admission-${name}`);
      const capabilities = {
        ...CodexProviderCapabilitiesV2,
        turns: { ...CodexProviderCapabilitiesV2.turns, supportsSteeringByInterruptRestart: false },
        sessions: {
          ...CodexProviderCapabilitiesV2.sessions,
          supportsMultipleProviderThreadsPerSession: pooled,
        },
      };
      const calls: ProviderAdapterV2TurnInput[] = [];
      const opens: string[] = [];
      const closes: string[] = [];
      const queues = new Map<string, Queue.Queue<ProviderAdapterV2Event>>();
      const planned: ModelSelection[] = [];
      const adapter: ProviderAdapterV2Shape = {
        driver,
        instanceId,
        getCapabilities: () => Effect.succeed(capabilities),
        planSelectionTransition: ({ current, target }) =>
          Effect.sync(() => {
            planned.push(current);
            if (effort(current) !== "low" || effort(target) !== "high")
              return { type: "apply_on_next_turn" as const };
            if (transition === "reject")
              return { type: "reject" as const, reason: "Unsupported target." };
            return {
              type: transition === "handoff" ? ("create_with_handoff" as const) : transition,
            };
          }),
        openSession: (input) =>
          Effect.gen(function* () {
            const now = yield* DateTime.now;
            const events = yield* Queue.unbounded<ProviderAdapterV2Event>();
            let loaded = input.modelSelection;
            opens.push(input.providerSessionId);
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                closes.push(input.providerSessionId);
              }),
            );
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
              ensureThread: ({ threadId, existingProviderThread }) =>
                Effect.succeed({
                  id:
                    existingProviderThread?.id ??
                    ProviderThreadId.make(`binding:${input.providerSessionId}:${threadId}`),
                  driver,
                  providerInstanceId: instanceId,
                  providerSessionId: input.providerSessionId,
                  appThreadId: threadId,
                  ownerNodeId: null,
                  nativeThreadRef: {
                    driver,
                    nativeId: `native:${input.providerSessionId}`,
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
                  assert.isFalse(
                    effort(loaded) === "low" &&
                      effort(turn.modelSelection) === "high" &&
                      transition !== "apply_on_next_turn",
                    "an incompatible native operation must not reach the adapter",
                  );
                  calls.push(turn);
                  loaded = turn.modelSelection;
                  queues.set(turn.attemptId, events);
                  yield* Queue.offer(events, {
                    type: "provider_turn.updated",
                    driver,
                    providerTurn: {
                      id: ProviderTurnId.make(`turn:${turn.attemptId}`),
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
        const threadId = ThreadId.make(`thread:${name}`);
        const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
          orchestrator.streamDomainEvents.pipe(
            Stream.filter(predicate),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
        const dispatch = (command: {
          name: string;
          modelSelection?: ModelSelection;
          queued?: boolean;
        }) =>
          orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(command.name),
            threadId,
            messageId: MessageId.make(`message:${command.name}`),
            text: command.name,
            attachments: [],
            ...(command.modelSelection === undefined
              ? {}
              : { modelSelection: command.modelSelection }),
            dispatchMode: {
              type: command.queued ? "queue_after_active" : "start_immediately",
            },
            createdBy: origin === "web" ? "user" : "agent",
            creationSource: origin,
          });
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make("create"),
          threadId,
          projectId: ProjectId.make("project:admission"),
          title: "Admission",
          modelSelection: A,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
          createdBy: "user",
          creationSource: "web",
        });
        const firstRunning = yield* watch(
          (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
        );
        yield* dispatch({ name: "first" });
        yield* worker.drain();
        yield* Fiber.join(firstRunning);
        const first = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("desired"),
          threadId,
          messageId: MessageId.make("message:desired"),
          text: "B next turn",
          attachments: [],
          modelSelection: B,
          dispatchMode: { type: "steer_active", targetRunId: first.id },
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();
        if (path === "queued")
          yield* dispatch({ name: "following", modelSelection: C, queued: true });
        const waiting = yield* watch(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === first.id &&
            event.payload.status === "waiting",
        );
        const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
        yield* Queue.offer(queues.get(first.activeAttemptId!)!, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
        });
        yield* Queue.offer(queues.get(first.activeAttemptId!)!, {
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
        if (path !== "queued") yield* worker.drain();
        const before = yield* orchestrator.getThreadProjection(threadId);
        if (path === "selection-set") {
          const result = yield* Effect.exit(
            orchestrator.dispatch({
              type: "thread.model-selection.set",
              commandId: CommandId.make("set"),
              threadId,
              modelSelection: C,
            }),
          );
          assert.equal(result._tag, transition === "reject" ? "Failure" : "Success");
          if (transition !== "reject") {
            yield* worker.drain();
            yield* dispatch({ name: "following" });
          }
        } else if (path === "ordinary") {
          const result = yield* Effect.exit(dispatch({ name: "following", modelSelection: C }));
          assert.equal(result._tag, transition === "reject" ? "Failure" : "Success");
        }
        yield* worker.drain();
        if (transition === "reject") {
          const after = yield* orchestrator.getThreadProjection(threadId);
          assert.deepEqual(after.thread.modelSelection, B);
          assert.deepEqual(after.runs, before.runs);
          assert.deepEqual(after.messages, before.messages);
          assert.equal(calls.length, 1);
          assert.equal(opens.length, 1);
          return;
        }
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(calls.length, 2);
        assert.deepEqual(calls[1]!.modelSelection, C);
        assert.deepEqual(after.thread.modelSelection, C);
        assert.isTrue(
          planned.some((current) => effort(current) === "low"),
          "desired B never erases loaded A evidence",
        );
        assert.equal(opens.length, transition === "apply_on_next_turn" ? 1 : 2);
        if (transition === "handoff") {
          assert.notEqual(calls[1]!.providerThread.id, calls[0]!.providerThread.id);
          assert.notEqual(
            calls[1]!.providerThread.nativeThreadRef?.nativeId,
            calls[0]!.providerThread.nativeThreadRef?.nativeId,
          );
          assert.isAbove(after.contextTransfers.length, 0);
        }
        if (transition === "restart_session" && path !== "selection-set") {
          assert.equal(
            calls[1]!.providerThread.nativeThreadRef?.nativeId,
            calls[0]!.providerThread.nativeThreadRef?.nativeId,
          );
        }
        if (!pooled && transition !== "apply_on_next_turn") assert.include(closes, opens[0]!);
        yield* orchestrator.dispatch({
          type: "provider-session.detach",
          commandId: CommandId.make("stop"),
          threadId,
          providerSessionId: calls[1]!.providerThread.providerSessionId!,
          reason: "Explicit Stop",
        });
        yield* worker.drain();
        if (!pooled) assert.include(closes, opens.at(-1)!);
      }).pipe(
        Effect.provide(
          makeOrchestratorV2ReplayLayerWithRegistry(
            { name: `execution-admission-${name}` },
            makeSingleLayer(adapter),
            { runEffectWorker: false },
          ),
        ),
      );
    }),
  ),
);
