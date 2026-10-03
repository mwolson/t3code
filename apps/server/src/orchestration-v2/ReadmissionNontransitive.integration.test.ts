import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  ThreadId,
  type ModelSelection,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderCapabilities,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import { OrchestrationEffectWorkerV2 } from "./EffectWorker.ts";
import { EventSinkV2 } from "./EventSink.ts";
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
const A: ModelSelection = {
  instanceId,
  model: "model-a",
  options: [{ id: "effort", value: "low" }],
};
const B: ModelSelection = {
  instanceId,
  model: "model-b",
  options: [{ id: "effort", value: "high" }],
};
const C: ModelSelection = {
  instanceId,
  model: "model-c",
  options: [{ id: "effort", value: "high" }],
};
const pooled: OrchestrationV2ProviderCapabilities = CodexProviderCapabilitiesV2;
// Same shape SelectionRestart.integration.test.ts uses for its restart_session cases.
const exclusive: OrchestrationV2ProviderCapabilities = {
  ...CodexProviderCapabilitiesV2,
  sessions: {
    ...CodexProviderCapabilitiesV2.sessions,
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: false,
  },
};
const caps = { pooled, exclusive } as const;
type Caps = keyof typeof caps;
// Non-transitive but contract-valid: A->B and B->C apply in session, A->C needs restart.
const applyOnNextTurn = (current: ModelSelection, target: ModelSelection) =>
  current.model === target.model ||
  (current.model === "model-a" && target.model === "model-b") ||
  (current.model === "model-b" && target.model === "model-c");

interface Log {
  readonly opens: Array<{ sessionId: string; model: string }>;
  readonly attempts: Array<{
    sessionId: string;
    opened: string;
    requested: string;
    requestedEffort: unknown;
    compatible: boolean;
  }>;
  interrupts: number;
  closes: number;
  readonly closed: Set<string>;
}

const makeAdapter = (
  capsName: Caps,
  log: Log,
  cwd: string,
  queues: Map<string, Queue.Queue<ProviderAdapterV2Event>>,
): ProviderAdapterV2Shape => ({
  instanceId,
  driver,
  getCapabilities: () => Effect.succeed(caps[capsName]),
  planSelectionTransition: ({ current, target }) =>
    Effect.succeed({
      type: applyOnNextTurn(current, target) ? "apply_on_next_turn" : "restart_session",
    }),
  openSession: (input) =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const events = yield* Queue.unbounded<ProviderAdapterV2Event>();
      log.opens.push({ sessionId: input.providerSessionId, model: input.modelSelection.model });
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          log.closes += 1;
          log.closed.add(input.providerSessionId);
        }),
      );
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
          model: input.modelSelection.model,
          capabilities: caps[capsName],
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
            log.attempts.push({
              sessionId: input.providerSessionId,
              opened: input.modelSelection.model,
              requested: turn.modelSelection.model,
              requestedEffort: turn.modelSelection.options,
              compatible: applyOnNextTurn(input.modelSelection, turn.modelSelection),
            });
            queues.set(turn.attemptId, events);
            yield* Queue.offer(events, {
              type: "provider_turn.updated",
              driver,
              providerTurn: {
                id: ProviderTurnId.make(`provider-turn:${turn.attemptId}`),
                providerThreadId: turn.providerThread.id,
                nodeId: turn.rootNodeId,
                runAttemptId: turn.attemptId,
                nativeTurnRef: { driver, nativeId: `native:${turn.attemptId}`, strength: "strong" },
                ordinal: turn.providerTurnOrdinal,
                status: "running",
                startedAt: yield* DateTime.now,
                completedAt: null,
              },
            });
          }),
        steerTurn: () => Effect.void,
        interruptTurn: () =>
          Effect.sync(() => {
            log.interrupts += 1;
          }),
        respondToRuntimeRequest: () => Effect.void,
        readThreadSnapshot: () => Effect.die("unused"),
        rollbackThread: () => Effect.die("unused"),
        forkThread: () => Effect.die("unused"),
      };
    }),
});

type Scenario =
  | { readonly name: "steer-set-send"; readonly caps: Caps }
  | {
      readonly name: "queued";
      readonly caps: Caps;
      readonly kind: "notification" | "completion" | "user";
      readonly order: "terminal-first" | "detach-first";
    };

const scenarios: Scenario[] = [
  { name: "steer-set-send", caps: "exclusive" },
  { name: "steer-set-send", caps: "pooled" },
  { name: "queued", caps: "exclusive", kind: "notification", order: "terminal-first" },
  { name: "queued", caps: "pooled", kind: "notification", order: "terminal-first" },
  { name: "queued", caps: "exclusive", kind: "completion", order: "terminal-first" },
  { name: "queued", caps: "pooled", kind: "completion", order: "terminal-first" },
  { name: "queued", caps: "exclusive", kind: "user", order: "terminal-first" },
  { name: "queued", caps: "pooled", kind: "user", order: "terminal-first" },
];

it.effect.each(
  (() => {
    const cases = [];
    for (const scenario of scenarios.flatMap((entry) => [
      { ...entry, equalTime: false },
      { ...entry, equalTime: true },
    ])) {
      const kind = scenario.name === "queued" ? `${scenario.name}-${scenario.kind}` : scenario.name;
      const title = `${kind}-${scenario.caps}-${scenario.equalTime ? "equal-time" : "ordered"}`;
      cases.push({ scenario, title, kind });
    }
    return cases;
  })(),
)("preserves queued selection and residency: $title", ({ scenario, title, kind }) =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace(`queued-admission-${title}`);
      const log: Log = { opens: [], attempts: [], interrupts: 0, closes: 0, closed: new Set() };
      const queues = new Map<string, Queue.Queue<ProviderAdapterV2Event>>();
      const adapter = makeAdapter(scenario.caps, log, cwd, queues);
      yield* Effect.gen(function* () {
        const orchestrator = yield* OrchestratorV2;
        const worker = yield* OrchestrationEffectWorkerV2;
        const sink = yield* EventSinkV2;
        const threadId = ThreadId.make("thread:queued-admission");
        const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
          orchestrator.streamDomainEvents.pipe(
            Stream.filter(predicate),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
        const runningFor = (attemptId: string) =>
          watch(
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.status === "running" &&
              event.payload.runAttemptId === attemptId,
          );
        const finish = Effect.fnUntraced(function* (
          runId: string,
          attemptId: string,
          runOrdinal: number,
        ) {
          if (!scenario.equalTime) yield* TestClock.adjust("1 second");
          const projection = yield* orchestrator.getThreadProjection(threadId);
          const turn = projection.providerTurns.find(
            (candidate) => candidate.runAttemptId === attemptId,
          )!;
          const waiting = yield* watch(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === runId &&
              event.payload.status === "waiting",
          );
          const events = queues.get(attemptId)!;
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
            runOrdinal,
            status: "completed",
            failure: null,
            threadDisposition: "reusable",
          });
          yield* Fiber.join(waiting);
          yield* worker.drain();
          yield* orchestrator.resumeQueuedRuns;
          yield* worker.drain();
        });
        const tick = scenario.equalTime ? Effect.void : TestClock.adjust("1 second");

        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make("create"),
          threadId,
          projectId: ProjectId.make("project:queued-admission"),
          title: "Queued admission",
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
        yield* worker.drain();
        yield* Fiber.join(firstRunning);
        const first = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
        assert.equal(log.attempts.length, 1);

        if (scenario.name === "steer-set-send") {
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("desired"),
            threadId,
            messageId: MessageId.make("message:desired"),
            text: "use B next turn",
            attachments: [],
            modelSelection: B,
            dispatchMode: { type: "steer_active", targetRunId: first.id },
            createdBy: "user",
            creationSource: "web",
          });
          yield* worker.drain();
          yield* tick;
          yield* orchestrator.dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("set-c"),
            threadId,
            modelSelection: C,
          });
          // Deliver the already-running turn's terminal before draining the
          // selection command's valid detach, which closes exclusive residency.
          yield* tick;
          yield* finish(first.id, first.activeAttemptId!, first.ordinal);
          yield* tick;
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("following"),
            threadId,
            messageId: MessageId.make("message:following"),
            text: "following",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* worker.drain();
        } else {
          const messageId = MessageId.make("message:queued");
          const taskId = NodeId.make("task:queued");
          const firstInput = { runId: first.id, rootNodeId: first.rootNodeId! };
          if (scenario.kind === "completion") {
            const projection = yield* orchestrator.getThreadProjection(threadId);
            const parentRun = projection.runs.find((run) => run.id === first.id)!;
            const now = yield* DateTime.now;
            yield* sink.write({
              events: [
                {
                  id: EventId.make("queued:cohort"),
                  type: "run.updated",
                  threadId,
                  runId: first.id,
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
                  id: EventId.make("queued:task"),
                  type: "subagent.updated",
                  threadId,
                  runId: first.id,
                  nodeId: taskId,
                  occurredAt: now,
                  payload: {
                    id: taskId,
                    threadId,
                    runId: first.id,
                    parentNodeId: firstInput.rootNodeId,
                    origin: "app_owned",
                    createdBy: "agent",
                    driver,
                    providerInstanceId: instanceId,
                    providerThreadId: null,
                    childThreadId: null,
                    nativeTaskRef: null,
                    prompt: "Background work",
                    title: "Queued completion",
                    model: null,
                    completionWake: "settled_only",
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
          const automatic = scenario.kind !== "user";
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("queued"),
            threadId,
            messageId,
            text: "queued",
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            createdBy: automatic ? "agent" : "user",
            creationSource: automatic ? "server" : "web",
            ...(automatic ? {} : { modelSelection: A }),
            ...(scenario.kind === "notification"
              ? {
                  notification: {
                    source: { kind: "background_task" as const },
                    outcome: "updated" as const,
                    summary: "Background activity updated",
                  },
                }
              : {}),
            ...(scenario.kind === "completion"
              ? {
                  delegatedCompletion: {
                    parentRunId: first.id,
                    generation: 1,
                    taskIds: [taskId],
                  },
                }
              : {}),
          });
          yield* worker.drain();
          const queued = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
            (run) => run.status === "queued",
          )!;
          assert.deepEqual(queued.modelSelection, A);
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("desired"),
            threadId,
            messageId: MessageId.make("message:desired"),
            text: "use B next turn",
            attachments: [],
            modelSelection: B,
            dispatchMode: { type: "steer_active", targetRunId: first.id },
            createdBy: "user",
            creationSource: "web",
          });
          yield* worker.drain();
          yield* tick;
          yield* orchestrator.dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("set-c"),
            threadId,
            modelSelection: C,
          });
          yield* tick;
          if (scenario.order === "detach-first") yield* worker.drain();

          const queuedRunning = yield* runningFor(queued.activeAttemptId!);
          yield* finish(first.id, first.activeAttemptId!, first.ordinal);
          assert.equal(
            log.attempts.length,
            2,
            "both starts reached the adapter before the drained outbox boundary",
          );
          assert.isFalse(
            log.closed.has(log.attempts[1]!.sessionId),
            "the current runtime survives the old detach",
          );
          yield* Fiber.join(queuedRunning);
          assert.deepEqual(
            (yield* orchestrator.getThreadProjection(threadId)).thread.modelSelection,
            automatic ? C : A,
          );
          assert.isFalse(
            log.closed.has(log.attempts[1]!.sessionId),
            "queued residency survives old detach, including equal timestamps",
          );
          if (scenario.caps === "exclusive")
            assert.isTrue(
              log.closed.has(log.attempts[0]!.sessionId),
              "the old effect still releases its own runtime",
            );
          assert.equal(log.attempts[1]!.requested, A.model);
          assert.deepEqual(log.attempts[1]!.requestedEffort, A.options);
          yield* finish(queued.id, queued.activeAttemptId!, queued.ordinal);
          yield* tick;
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("following"),
            threadId,
            messageId: MessageId.make("message:following"),
            text: "implicit following",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "agent",
            creationSource: "server",
          });
          yield* worker.drain();
          assert.equal(log.attempts.at(-1)!.requested, automatic ? C.model : A.model);
          assert.deepEqual(log.attempts.at(-1)!.requestedEffort, automatic ? C.options : A.options);
        }
        const incompatible = log.attempts.filter((attempt) => !attempt.compatible);
        assert.deepEqual(
          incompatible,
          [],
          "every turn must run on a session whose loaded selection permits it",
        );
      }).pipe(
        Effect.provide(
          makeOrchestratorV2ReplayLayerWithRegistry(
            { name: `queued-admission-${title}` },
            makeSingleLayer(adapter),
            { runEffectWorker: false },
          ),
        ),
      );
    }),
  ),
);
