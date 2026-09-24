import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  type ModelSelection,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  type RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

const driver = ProviderDriverKind.make("codex");
const providerInstanceId = ProviderInstanceId.make("codex-restart-test");
const initialSelection = {
  instanceId: providerInstanceId,
  model: "restart-model-a",
} satisfies ModelSelection;
const replacementSelection = {
  instanceId: providerInstanceId,
  model: "restart-model-b",
} satisfies ModelSelection;
const seedSelection = {
  instanceId: providerInstanceId,
  model: "seed-model",
} satisfies ModelSelection;
const pooledCapabilities: OrchestrationV2ProviderCapabilities = CodexProviderCapabilitiesV2;
const exclusiveCapabilities: OrchestrationV2ProviderCapabilities = {
  ...CodexProviderCapabilitiesV2,
  sessions: {
    ...CodexProviderCapabilitiesV2.sessions,
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: false,
  },
};

interface ActiveTurn {
  readonly input: ProviderAdapter.ProviderAdapterV2TurnInput;
  readonly providerTurnId: ProviderTurnId;
}

interface RestartAdapterState {
  readonly activeTurn: ActiveTurn | null;
  readonly opened: ReadonlyArray<{
    readonly model: string | null;
    readonly cwd: string | null;
  }>;
  readonly started: ReadonlyArray<{
    readonly model: string;
    readonly cwd: string | null;
    readonly attemptId: string;
  }>;
  readonly closedSessionCount: number;
  readonly failedReplacementOpen: boolean;
}

function makeRestartAdapter(
  state: Ref.Ref<RestartAdapterState>,
  sessionCapabilities: OrchestrationV2ProviderCapabilities = pooledCapabilities,
  providerInstanceId = initialSelection.instanceId,
): ProviderAdapter.ProviderAdapterV2["Service"] {
  return {
    instanceId: providerInstanceId,
    driver,
    getCapabilities: () => Effect.succeed(sessionCapabilities),
    planSelectionTransition: ({ current, target }) =>
      Effect.succeed(
        current.model === target.model
          ? ({ type: "apply_on_next_turn" } as const)
          : ({ type: "restart_session" } as const),
      ),
    openSession: (sessionInput) =>
      Effect.gen(function* () {
        const failThisOpen = yield* Ref.modify(state, (current) => {
          const shouldFail =
            sessionInput.modelSelection.model === replacementSelection.model &&
            !current.failedReplacementOpen;
          return [
            shouldFail,
            {
              ...current,
              failedReplacementOpen: current.failedReplacementOpen || shouldFail,
              opened: [
                ...current.opened,
                {
                  model: sessionInput.modelSelection.model,
                  cwd: sessionInput.runtimePolicy.cwd,
                },
              ],
            },
          ] as const;
        });
        if (failThisOpen) {
          return yield* new ProviderAdapter.ProviderAdapterOpenSessionError({
            driver,
            providerSessionId: sessionInput.providerSessionId,
            cause: "simulated replacement open failure",
          });
        }

        const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
        const now = yield* DateTime.now;
        const providerSession: OrchestrationV2ProviderSession = {
          id: sessionInput.providerSessionId,
          driver,
          providerInstanceId,
          status: "ready",
          cwd: sessionInput.runtimePolicy.cwd ?? "/fallback",
          model: sessionInput.modelSelection.model,
          capabilities: sessionCapabilities,
          createdAt: now,
          updatedAt: now,
          lastError: null,
        };
        yield* Effect.addFinalizer(() =>
          Ref.update(state, (current) => ({
            ...current,
            closedSessionCount: current.closedSessionCount + 1,
          })),
        );

        const publishTerminal = (active: ActiveTurn, status: "completed" | "interrupted") =>
          Effect.gen(function* () {
            const occurredAt = yield* DateTime.now;
            yield* Queue.offer(events, {
              type: "provider_turn.updated",
              driver,
              providerTurn: {
                id: active.providerTurnId,
                providerThreadId: active.input.providerThread.id,
                nodeId: active.input.rootNodeId,
                runAttemptId: active.input.attemptId,
                nativeTurnRef: {
                  driver,
                  nativeId: `native:${active.providerTurnId}`,
                  strength: "strong",
                },
                ordinal: active.input.providerTurnOrdinal,
                status,
                startedAt: occurredAt,
                completedAt: occurredAt,
              },
            });
            yield* Queue.offer(events, {
              type: "turn.terminal",
              driver,
              providerThreadId: active.input.providerThread.id,
              providerTurnId: active.providerTurnId,
              runOrdinal: active.input.runOrdinal,
              status,
              failure: null,
              threadDisposition: "reusable",
            });
          });

        return {
          instanceId: providerInstanceId,
          driver,
          providerSessionId: sessionInput.providerSessionId,
          providerSession,
          events: Stream.fromQueue(events),
          ensureThread: (threadInput) =>
            Effect.gen(function* () {
              const createdAt = yield* DateTime.now;
              return {
                id: ProviderThreadId.make(`provider-thread:${threadInput.threadId}`),
                driver,
                providerInstanceId,
                providerSessionId: sessionInput.providerSessionId,
                appThreadId: threadInput.threadId,
                ownerNodeId: null,
                nativeThreadRef: {
                  driver,
                  nativeId: `native-thread:${threadInput.threadId}`,
                  strength: "strong",
                },
                nativeConversationHeadRef: null,
                status: "idle",
                firstRunOrdinal: null,
                lastRunOrdinal: null,
                handoffIds: [],
                forkedFrom: null,
                createdAt,
                updatedAt: createdAt,
              } satisfies OrchestrationV2ProviderThread;
            }),
          resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
          startTurn: (input) =>
            Effect.gen(function* () {
              yield* Ref.update(state, (current) => ({
                ...current,
                started: [
                  ...current.started,
                  {
                    model: input.modelSelection.model,
                    cwd: input.runtimePolicy.cwd,
                    attemptId: input.attemptId,
                  },
                ],
              }));
              const active = {
                input,
                providerTurnId: ProviderTurnId.make(`provider-turn:${input.attemptId}`),
              } satisfies ActiveTurn;
              if (input.modelSelection.model === initialSelection.model) {
                const occurredAt = yield* DateTime.now;
                yield* Ref.update(state, (current) => ({ ...current, activeTurn: active }));
                yield* Queue.offer(events, {
                  type: "provider_turn.updated",
                  driver,
                  providerTurn: {
                    id: active.providerTurnId,
                    providerThreadId: input.providerThread.id,
                    nodeId: input.rootNodeId,
                    runAttemptId: input.attemptId,
                    nativeTurnRef: {
                      driver,
                      nativeId: `native:${active.providerTurnId}`,
                      strength: "strong",
                    },
                    ordinal: input.providerTurnOrdinal,
                    status: "running",
                    startedAt: occurredAt,
                    completedAt: null,
                  },
                });
                return;
              }
              yield* publishTerminal(active, "completed");
            }),
          steerTurn: () => Effect.void,
          interruptTurn: () =>
            Effect.gen(function* () {
              const active = (yield* Ref.get(state)).activeTurn;
              if (active !== null) {
                const updatedAt = yield* DateTime.now;
                yield* Queue.offer(events, {
                  type: "provider_thread.updated",
                  driver,
                  providerThread: {
                    ...active.input.providerThread,
                    status: "idle",
                    updatedAt,
                  },
                });
                yield* publishTerminal(active, "interrupted");
                yield* Ref.update(state, (current) => ({ ...current, activeTurn: null }));
              }
            }),
          respondToRuntimeRequest: () => Effect.void,
          readThreadSnapshot: () => Effect.die("unused readThreadSnapshot"),
          rollbackThread: () => Effect.die("unused rollbackThread"),
          forkThread: () => Effect.die("unused forkThread"),
        };
      }),
  };
}

it.live("retries a failed selection restart session open before completing", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("selection-restart-lifecycle");
      const threadId = ThreadId.make("thread:selection-restart-lifecycle");
      const state = yield* Ref.make<RestartAdapterState>({
        activeTurn: null,
        opened: [],
        started: [],
        closedSessionCount: 0,
        failedReplacementOpen: false,
      });
      const layerRegistry = ProviderAdapterRegistry.layerSingle(makeRestartAdapter(state));

      const result = yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const eventSink = yield* EventSink.EventSinkV2;

        yield* orchestrator.dispatch({
          type: "thread.create",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make("command:selection-restart:create"),
          threadId,
          projectId: ProjectId.make("project:selection-restart"),
          title: "Selection restart",
          modelSelection: initialSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
        });
        const running = yield* eventSink
          .stream({
            threadId,
            afterSequence: yield* eventSink.latestSequence({ threadId }),
          })
          .pipe(
            Stream.map((stored) => stored.event),
            Stream.filter(
              (event) =>
                event.type === "provider-turn.updated" && event.payload.status === "running",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make("command:selection-restart:first"),
          threadId,
          messageId: MessageId.make("message:selection-restart:first"),
          text: "first",
          attachments: [],
          modelSelection: initialSelection,
          dispatchMode: { type: "start_immediately" },
        });
        yield* Fiber.join(running);
        yield* worker.drain();
        const activeProjection = yield* orchestrator.getThreadProjection(threadId);
        assert.isTrue(activeProjection.providerTurns.some((turn) => turn.status === "running"));
        const activeRunId = activeProjection.runs[0]?.id;
        if (activeRunId === undefined) {
          return yield* Effect.die("active restart test run is missing");
        }

        const completed = yield* eventSink
          .stream({
            threadId,
            afterSequence: yield* eventSink.latestSequence({ threadId }),
          })
          .pipe(
            Stream.map((stored) => stored.event),
            Stream.filter(
              (event) => event.type === "run.updated" && event.payload.status === "completed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make("command:selection-restart:second"),
          threadId,
          messageId: MessageId.make("message:selection-restart:second"),
          text: "second",
          attachments: [],
          modelSelection: replacementSelection,
          dispatchMode: { type: "restart_active", targetRunId: activeRunId },
        });
        yield* Fiber.join(completed);
        yield* worker.drain();
        return {
          projection: yield* orchestrator.getThreadProjection(threadId),
          captured: yield* Ref.get(state),
        };
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry(
            { name: "selection-restart-lifecycle" },
            layerRegistry,
          ),
        ),
      );
      const { projection, captured } = result;

      assert.lengthOf(projection.runs, 1);
      assert.lengthOf(projection.attempts, 2);
      assert.deepEqual(
        projection.attempts.map((attempt) => attempt.status),
        ["superseded", "completed"],
      );
      assert.deepEqual(
        projection.providerTurns.map((turn) => turn.status),
        ["interrupted", "completed"],
      );
      assert.isFalse(
        projection.turnItems.some(
          (item) => item.type === "run_interrupt_request" || item.type === "run_interrupt_result",
        ),
        "selection restart supersede must not project hard-Stop interrupt items",
      );
      assert.equal(projection.runs.at(-1)?.modelSelection.model, replacementSelection.model);
      assert.equal(projection.runs.at(-1)?.status, "completed");
      assert.isTrue(captured.failedReplacementOpen);
      assert.equal(captured.closedSessionCount, 0);
      assert.deepEqual(
        captured.opened.map((open) => [open.model, open.cwd]),
        [
          [initialSelection.model, cwd],
          [replacementSelection.model, cwd],
          [replacementSelection.model, cwd],
        ],
      );
      assert.deepEqual(
        captured.started.map((turn) => [turn.model, turn.cwd]),
        [
          [initialSelection.model, cwd],
          [replacementSelection.model, cwd],
        ],
      );
      assert.notEqual(
        projection.providerSessions[0]?.id,
        projection.providerThreads[0]?.providerSessionId,
      );
      assert.equal(
        projection.providerThreads[0]?.providerSessionId,
        projection.providerSessions.find((session) => session.model === replacementSelection.model)
          ?.id,
      );
    }),
  ),
);

it.live(
  "restarts the live session on a model change when a newer stopped session record exists",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const name = "selection-restart-dead-stopped";
        const cwd = yield* checkpointWorkspace(name);
        const threadId = ThreadId.make(`thread:${name}`);
        const state = yield* Ref.make<RestartAdapterState>({
          activeTurn: null,
          opened: [],
          started: [],
          closedSessionCount: 0,
          failedReplacementOpen: true,
        });
        const layerRegistry = ProviderAdapterRegistry.layerSingle(
          makeRestartAdapter(state, exclusiveCapabilities),
        );

        const result = yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const eventSink = yield* EventSink.EventSinkV2;
          const dispatch = (step: string, modelSelection: ModelSelection) =>
            Effect.gen(function* () {
              const terminal = yield* orchestrator.streamDomainEvents.pipe(
                Stream.filter(
                  (event) => event.type === "run.updated" && event.payload.status === "completed",
                ),
                Stream.take(1),
                Stream.runDrain,
                Effect.forkScoped,
              );
              yield* orchestrator.dispatch({
                type: "message.dispatch",
                createdBy: "user",
                creationSource: "web",
                commandId: CommandId.make(`${name}:${step}`),
                threadId,
                messageId: MessageId.make(`${name}:${step}`),
                text: step,
                attachments: [],
                modelSelection,
                dispatchMode: { type: "start_immediately" },
              });
              yield* worker.drain();
              yield* Fiber.join(terminal);
              yield* worker.drain();
              return yield* orchestrator.getThreadProjection(threadId);
            });
          yield* orchestrator.dispatch({
            type: "thread.create",
            createdBy: "user",
            creationSource: "web",
            commandId: CommandId.make(`${name}:create`),
            threadId,
            projectId: ProjectId.make(`project:${name}`),
            title: name,
            modelSelection: seedSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
          });
          const first = yield* dispatch("first", seedSelection);
          const liveSession = first.providerSessions.find(
            (session) => session.status !== "stopped" && session.status !== "error",
          );
          assert.isDefined(liveSession);

          const deadAt = yield* DateTime.now;
          const deadSession: OrchestrationV2ProviderSession = {
            id: ProviderSessionId.make(`session:${name}:dead`),
            driver,
            providerInstanceId,
            status: "ready",
            cwd,
            model: seedSelection.model,
            capabilities: exclusiveCapabilities,
            createdAt: deadAt,
            updatedAt: deadAt,
            lastError: null,
          };
          yield* eventSink.write({
            events: [
              {
                id: EventId.make(`event:${name}:dead-attached`),
                type: "provider-session.attached",
                threadId,
                driver,
                providerInstanceId,
                occurredAt: deadAt,
                payload: deadSession,
              },
              {
                id: EventId.make(`event:${name}:dead-updated`),
                type: "provider-session.updated",
                threadId,
                driver,
                providerInstanceId,
                occurredAt: deadAt,
                payload: {
                  ...deadSession,
                  status: "stopped",
                  updatedAt: deadAt,
                  lastError: null,
                },
              },
            ],
          });

          const switchCommandId = CommandId.make(`${name}:switch`);
          yield* orchestrator.dispatch({
            type: "thread.model-selection.set",
            commandId: switchCommandId,
            threadId,
            modelSelection: replacementSelection,
          });
          yield* worker.drain();
          const storedSwitchEvents = yield* eventSink
            .readByCommandId({ commandId: switchCommandId })
            .pipe(Stream.runCollect);
          const detachedSessionIds = [...storedSwitchEvents].flatMap((stored) =>
            stored.event.type === "provider-session.detached"
              ? [stored.event.payload.providerSessionId]
              : [],
          );

          const second = yield* dispatch("second", replacementSelection);
          return {
            projection: second,
            captured: yield* Ref.get(state),
            liveSessionId: liveSession.id,
            detachedSessionIds,
          };
        }).pipe(Effect.provide(ProviderReplayHarness.layerWithRegistry({ name }, layerRegistry)));

        const { projection, captured } = result;
        assert.lengthOf(projection.runs, 2);
        assert.equal(projection.runs[1]?.modelSelection.model, replacementSelection.model);
        assert.deepEqual(result.detachedSessionIds, [result.liveSessionId]);
        assert.equal(captured.closedSessionCount, 1);
        assert.deepEqual(
          captured.opened.map((open) => open.model),
          [seedSelection.model, replacementSelection.model],
        );
        assert.deepEqual(
          captured.started.map((turn) => turn.model),
          [seedSelection.model, replacementSelection.model],
        );
        const servingSession = projection.providerSessions.find(
          (session) =>
            session.id ===
            projection.providerThreads.find(
              (providerThread) => providerThread.id === projection.thread.activeProviderThreadId,
            )?.providerSessionId,
        );
        assert.equal(servingSession?.model, replacementSelection.model);
      }),
    ),
);

it.live.each(["active", "idle", "selection-command", "pooled", "separate-home"] as const)(
  "preserves native history only for compatible account switches (%s)",
  (mode) =>
    Effect.scoped(
      Effect.gen(function* () {
        const name = `shared-home-${mode}`;
        const cwd = yield* checkpointWorkspace(name);
        const threadId = ThreadId.make(`thread:${name}`);
        const targetId = ProviderInstanceId.make("codex-shadow-account");
        const state = yield* Ref.make<RestartAdapterState>({
          activeTurn: null,
          opened: [],
          started: [],
          closedSessionCount: 0,
          failedReplacementOpen: true,
        });
        const resumes: Array<{
          instanceId: ProviderInstanceId;
          nativeId: string | null | undefined;
        }> = [];
        const messages: string[] = [];
        const capabilities = mode === "pooled" ? pooledCapabilities : exclusiveCapabilities;
        const adapters = [providerInstanceId, targetId].map((instanceId) => {
          const base = makeRestartAdapter(state, capabilities, instanceId);
          return {
            ...base,
            openSession: (input) =>
              base.openSession(input).pipe(
                Effect.map((session) => ({
                  ...session,
                  resumeThread: (input) =>
                    Effect.gen(function* () {
                      resumes.push({
                        instanceId,
                        nativeId: input.providerThread.nativeThreadRef?.nativeId,
                      });
                      return yield* session.resumeThread(input);
                    }),
                  startTurn: (input) =>
                    Effect.gen(function* () {
                      messages.push(input.message.text);
                      yield* session.startTurn(input);
                    }),
                })),
              ),
          } satisfies ProviderAdapter.ProviderAdapterV2["Service"];
        });
        const layerRegistry = Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistryV2, {
          get: (instanceId) =>
            Effect.succeed(adapters.find((adapter) => adapter.instanceId === instanceId)!),
          list: () => Effect.succeed([providerInstanceId, targetId]),
          getMetadata: (instanceId) =>
            Effect.succeed({
              driver,
              continuationKey:
                mode === "separate-home" ? `codex:home:/${instanceId}` : "codex:home:/shared",
              enabled: true,
              capabilities,
            }),
        });
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const selection = {
            ...initialSelection,
            model: mode === "active" ? initialSelection.model : "complete",
          };
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make(`${name}:create`),
            threadId,
            projectId: ProjectId.make(`project:${name}`),
            title: name,
            modelSelection: selection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
          const dispatch = (step: string, modelSelection: ModelSelection, targetRunId?: RunId) =>
            Effect.gen(function* () {
              const terminal = yield* orchestrator.streamDomainEvents.pipe(
                Stream.filter((event) =>
                  mode === "active" && step === "first"
                    ? event.type === "provider-turn.updated" && event.payload.status === "running"
                    : event.type === "run.updated" && event.payload.status === "completed",
                ),
                Stream.take(1),
                Stream.runDrain,
                Effect.forkScoped,
              );
              yield* orchestrator.dispatch({
                type: "message.dispatch",
                commandId: CommandId.make(`${name}:${step}`),
                threadId,
                messageId: MessageId.make(`${name}:${step}`),
                text: step,
                attachments: [],
                modelSelection,
                dispatchMode:
                  targetRunId === undefined
                    ? { type: "start_immediately" }
                    : { type: "restart_active", targetRunId },
                createdBy: "user",
                creationSource: "web",
              });
              yield* worker.drain();
              yield* Fiber.join(terminal);
              yield* worker.drain();
              return yield* orchestrator.getThreadProjection(threadId);
            });
          const first = yield* dispatch("first", selection);
          const originalThread = first.providerThreads.find(
            (thread) => thread.id === first.thread.activeProviderThreadId,
          )!;
          const targetSelection = { instanceId: targetId, model: "complete" };
          if (mode === "selection-command") {
            yield* orchestrator.dispatch({
              type: "provider.switch",
              commandId: CommandId.make(`${name}:switch`),
              threadId,
              modelSelection: targetSelection,
            });
            yield* worker.drain();
          }
          const second = yield* dispatch(
            "second",
            targetSelection,
            mode === "active" ? first.runs[0]!.id : undefined,
          );
          const targetThread = second.providerThreads.find(
            (thread) => thread.id === second.thread.activeProviderThreadId,
          )!;
          if (mode === "separate-home") {
            assert.notEqual(targetThread.id, originalThread.id);
            assert.lengthOf(second.contextHandoffs, 1);
            assert.lengthOf(second.contextTransfers, 1);
            assert.isEmpty(resumes);
            assert.notEqual(messages[1], "second");
            return;
          }
          assert.equal(targetThread.id, originalThread.id);
          assert.deepEqual(targetThread.nativeThreadRef, originalThread.nativeThreadRef);
          assert.equal(targetThread.providerInstanceId, targetId);
          assert.notEqual(targetThread.providerSessionId, originalThread.providerSessionId);
          assert.isEmpty(second.contextHandoffs);
          assert.isEmpty(second.contextTransfers);
          assert.deepEqual(resumes, [
            { instanceId: targetId, nativeId: originalThread.nativeThreadRef?.nativeId },
          ]);
          assert.deepEqual(messages, ["first", "second"]);
          assert.equal((yield* Ref.get(state)).closedSessionCount, mode === "pooled" ? 0 : 1);
          if (mode === "pooled") {
            assert.isFalse(
              second.providerSessions.some(
                (session) => session.id === originalThread.providerSessionId,
              ),
            );
          }
          const third = yield* dispatch("third", { ...selection, model: "complete" });
          const returnedThread = third.providerThreads.find(
            (thread) => thread.id === third.thread.activeProviderThreadId,
          )!;
          assert.equal(returnedThread.id, originalThread.id);
          assert.deepEqual(returnedThread.nativeThreadRef, originalThread.nativeThreadRef);
          assert.equal(returnedThread.providerInstanceId, providerInstanceId);
          assert.isEmpty(third.contextHandoffs);
          assert.deepEqual(messages, ["first", "second", "third"]);
        }).pipe(Effect.provide(ProviderReplayHarness.layerWithRegistry({ name }, layerRegistry)));
      }),
    ),
);

const admissionInstanceId = ProviderInstanceId.make("codex");
const effortSelection = (effort: string): ModelSelection => ({
  instanceId: admissionInstanceId,
  model: "same-model",
  options: [{ id: "effort", value: effort }],
});
const effortOf = (selection: ModelSelection) =>
  selection.options?.find((option) => option.id === "effort")?.value;
const admissionCapabilities: OrchestrationV2ProviderCapabilities = {
  ...CodexProviderCapabilitiesV2,
  turns: { ...CodexProviderCapabilitiesV2.turns, supportsSteeringByInterruptRestart: false },
  sessions: {
    ...CodexProviderCapabilitiesV2.sessions,
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: false,
  },
};
const lowToHighRestarts = ({
  current,
  target,
}: {
  readonly current: ModelSelection;
  readonly target: ModelSelection;
}) =>
  Effect.succeed({
    type:
      effortOf(current) === "low" && effortOf(target) === "high"
        ? ("restart_session" as const)
        : ("apply_on_next_turn" as const),
  });
const runningTurn = (
  events: Queue.Queue<ProviderAdapter.ProviderAdapterV2Event>,
  turn: ProviderAdapter.ProviderAdapterV2TurnInput,
) =>
  Effect.gen(function* () {
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
  });
const admissionProviderThread = (
  providerSessionId: ProviderSessionId,
  threadId: ThreadId,
  existingProviderThread: OrchestrationV2ProviderThread | undefined,
  nativeId: string,
) =>
  Effect.map(DateTime.now, (now): OrchestrationV2ProviderThread => ({
    id: existingProviderThread?.id ?? ProviderThreadId.make(`provider-thread:${threadId}`),
    driver,
    providerInstanceId: admissionInstanceId,
    providerSessionId,
    appThreadId: threadId,
    ownerNodeId: null,
    nativeThreadRef: { driver, nativeId, strength: "strong" },
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: null,
    lastRunOrdinal: null,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  }));
const admissionSession = (
  providerSessionId: ProviderSessionId,
  cwd: string,
  model: string,
  capabilities: OrchestrationV2ProviderCapabilities,
) =>
  Effect.map(DateTime.now, (now): OrchestrationV2ProviderSession => ({
    id: providerSessionId,
    driver,
    providerInstanceId: admissionInstanceId,
    status: "ready",
    cwd,
    model,
    capabilities,
    createdAt: now,
    updatedAt: now,
    lastError: null,
  }));
const admissionThread = (threadId: ThreadId, modelSelection: ModelSelection, cwd: string) => ({
  type: "thread.create" as const,
  commandId: CommandId.make(`create:${threadId}`),
  threadId,
  projectId: ProjectId.make("project:admission"),
  title: "Admission",
  modelSelection,
  runtimeMode: "full-access" as const,
  interactionMode: "default" as const,
  branch: null,
  worktreePath: cwd,
  createdBy: "user" as const,
  creationSource: "web" as const,
});
const userMessage = (threadId: ThreadId, name: string, modelSelection?: ModelSelection) => ({
  type: "message.dispatch" as const,
  commandId: CommandId.make(name),
  threadId,
  messageId: MessageId.make(`message:${name}`),
  text: name,
  attachments: [],
  ...(modelSelection === undefined ? {} : { modelSelection }),
  dispatchMode: { type: "start_immediately" as const },
  createdBy: "user" as const,
  creationSource: "web" as const,
});
/** Starts the thread's next turn and waits until the provider reports it running. */
const startAdmissionTurn = (threadId: ThreadId, name: string, modelSelection?: ModelSelection) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
    const running = yield* orchestrator.streamDomainEvents.pipe(
      Stream.filter(
        (event) =>
          event.threadId === threadId &&
          event.type === "provider-turn.updated" &&
          event.payload.status === "running",
      ),
      Stream.take(1),
      Stream.runDrain,
      Effect.forkScoped,
    );
    yield* orchestrator.dispatch(userMessage(threadId, name, modelSelection));
    yield* worker.drain();
    yield* Fiber.join(running);
  });
/** Completes the thread's latest (or given) run through the event stream its attempt started on. */
const completeAdmissionTurn = (
  threadId: ThreadId,
  eventsFor: (attemptId: string) => Queue.Queue<ProviderAdapter.ProviderAdapterV2Event>,
  runId?: RunId,
) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
    const projection = yield* orchestrator.getThreadProjection(threadId);
    const run =
      runId === undefined ? projection.runs.at(-1)! : projection.runs.find((r) => r.id === runId)!;
    const turn = projection.providerTurns.find(
      (candidate) => candidate.runAttemptId === run.activeAttemptId,
    )!;
    const waiting = yield* orchestrator.streamDomainEvents.pipe(
      Stream.filter(
        (event) =>
          event.type === "run.updated" &&
          event.payload.id === run.id &&
          event.payload.status === "waiting",
      ),
      Stream.take(1),
      Stream.runDrain,
      Effect.forkScoped,
    );
    const events = eventsFor(run.activeAttemptId!);
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

it.effect("plans a selection change from the selection that last ran, not a steered one", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("execution-admission");
      const threadId = ThreadId.make("thread:execution-admission");
      const A = effortSelection("low");
      const B = effortSelection("medium");
      const C = effortSelection("high");
      const queues = new Map<string, Queue.Queue<ProviderAdapter.ProviderAdapterV2Event>>();
      const calls: ProviderAdapter.ProviderAdapterV2TurnInput[] = [];
      const opens: string[] = [];
      const closes: string[] = [];
      const adapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
        driver,
        instanceId: admissionInstanceId,
        getCapabilities: () => Effect.succeed(admissionCapabilities),
        planSelectionTransition: lowToHighRestarts,
        openSession: (input) =>
          Effect.gen(function* () {
            const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
            let loaded = input.modelSelection;
            opens.push(input.providerSessionId);
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => void closes.push(input.providerSessionId)),
            );
            return {
              driver,
              instanceId: admissionInstanceId,
              providerSessionId: input.providerSessionId,
              providerSession: yield* admissionSession(
                input.providerSessionId,
                cwd,
                input.modelSelection.model,
                admissionCapabilities,
              ),
              events: Stream.fromQueue(events),
              ensureThread: ({ threadId, existingProviderThread }) =>
                admissionProviderThread(
                  input.providerSessionId,
                  threadId,
                  existingProviderThread,
                  `native:${input.providerSessionId}`,
                ),
              resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
              startTurn: (turn) =>
                Effect.gen(function* () {
                  assert.isFalse(
                    effortOf(loaded) === "low" && effortOf(turn.modelSelection) === "high",
                    "an incompatible native operation must not reach the adapter",
                  );
                  calls.push(turn);
                  loaded = turn.modelSelection;
                  queues.set(turn.attemptId, events);
                  yield* runningTurn(events, turn);
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
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        yield* orchestrator.dispatch(admissionThread(threadId, A, cwd));
        yield* startAdmissionTurn(threadId, "first");
        const first = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
        // The steer saves B for the next turn; no turn ever runs on B.
        yield* orchestrator.dispatch({
          ...userMessage(threadId, "desired", B),
          dispatchMode: { type: "steer_active", targetRunId: first.id },
        });
        yield* worker.drain();
        yield* completeAdmissionTurn(threadId, (attemptId) => queues.get(attemptId)!);
        yield* orchestrator.dispatch(userMessage(threadId, "following", C));
        yield* worker.drain();
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(calls.length, 2);
        assert.deepEqual(calls[1]!.modelSelection, C);
        assert.deepEqual(after.thread.modelSelection, C);
        assert.equal(opens.length, 2, "A to C restarts even though B to C would not");
        assert.equal(
          calls[1]!.providerThread.nativeThreadRef?.nativeId,
          calls[0]!.providerThread.nativeThreadRef?.nativeId,
        );
        assert.include(closes, opens[0]!);
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry(
            { name: "execution-admission" },
            ProviderAdapterRegistry.layerSingle(adapter),
            { runEffectWorker: false },
          ),
        ),
      );
    }),
  ),
);

it.effect("opens a fresh session after a start that failed partway, without resending it", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("admission-failed-start");
      const threadId = ThreadId.make("thread:admission-failed-start");
      const A = effortSelection("low");
      const B = effortSelection("medium");
      const C = effortSelection("high");
      const queues = new Map<string, Queue.Queue<ProviderAdapter.ProviderAdapterV2Event>>();
      const closed = new Set<string>();
      let pending = false;
      let opens = 0;
      const calls: ProviderAdapter.ProviderAdapterV2TurnInput[] = [];
      const plans: Array<{ readonly current: ModelSelection; readonly target: ModelSelection }> =
        [];
      const adapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
        driver,
        instanceId: admissionInstanceId,
        getCapabilities: () => Effect.succeed(admissionCapabilities),
        planSelectionTransition: (input) =>
          Effect.suspend(() => {
            plans.push(input);
            return lowToHighRestarts(input);
          }),
        openSession: (input) =>
          Effect.gen(function* () {
            opens += 1;
            const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => void closed.add(input.providerSessionId)),
            );
            return {
              driver,
              instanceId: admissionInstanceId,
              providerSessionId: input.providerSessionId,
              providerSession: yield* admissionSession(
                input.providerSessionId,
                cwd,
                input.modelSelection.model,
                admissionCapabilities,
              ),
              events: Stream.fromQueue(events),
              hasPendingBackgroundWork: Effect.sync(() => pending),
              ensureThread: ({ threadId, existingProviderThread }) =>
                admissionProviderThread(
                  input.providerSessionId,
                  threadId,
                  existingProviderThread,
                  "native:failed-start",
                ),
              resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
              startTurn: (turn) =>
                Effect.gen(function* () {
                  calls.push(turn);
                  queues.set(turn.attemptId, events);
                  // The error does not say whether the native selection changed first.
                  if (turn.message.text === "reject")
                    return yield* new ProviderAdapter.ProviderAdapterTurnStartError({
                      driver,
                      threadId: turn.threadId,
                      providerThreadId: turn.providerThread.id,
                      runId: turn.runId,
                      cause: "controlled request rejection",
                    });
                  yield* runningTurn(events, turn);
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
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        yield* orchestrator.dispatch(admissionThread(threadId, A, cwd));
        yield* startAdmissionTurn(threadId, "first");
        yield* completeAdmissionTurn(threadId, (attemptId) => queues.get(attemptId)!);
        yield* TestClock.adjust("1 second");
        yield* orchestrator.dispatch(userMessage(threadId, "reject", B));
        yield* worker.drain();
        const failed = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(failed.runs.at(-1)!.status, "failed");
        pending = true;
        const blocked = yield* Effect.exit(
          orchestrator.dispatch(userMessage(threadId, "blocked-replacement", C)),
        );
        assert.equal(
          blocked._tag,
          "Failure",
          "an unknown selection cannot replace a runtime with pending work",
        );
        assert.equal(opens, 1);
        assert.equal(closed.size, 0);
        pending = false;
        yield* orchestrator.dispatch({
          type: "thread.model-selection.set",
          commandId: CommandId.make("set-c"),
          threadId,
          modelSelection: C,
        });
        yield* worker.drain();
        yield* startAdmissionTurn(threadId, "following");
        assert.equal(opens, 2, "the uncertain resident session is not reused");
        assert.deepEqual(
          calls.map((call) => call.message.messageId),
          ["message:first", "message:reject", "message:following"],
        );
        assert.deepEqual(calls.at(-1)!.modelSelection, C);
        const oldSessionId = calls[0]!.providerThread.providerSessionId!;
        const newSessionId = calls.at(-1)!.providerThread.providerSessionId!;
        assert.notEqual(oldSessionId, newSessionId);
        assert.isTrue(closed.has(oldSessionId));
        assert.isFalse(closed.has(newSessionId));
        assert.isFalse(
          plans.some(
            (plan) => effortOf(plan.current) === "medium" && effortOf(plan.target) === "high",
          ),
          "the failed B start is not execution evidence",
        );
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry(
            { name: "admission-failed-start" },
            ProviderAdapterRegistry.layerSingle(adapter),
            { runEffectWorker: false },
          ),
        ),
      );
    }),
  ),
);

it.effect("keeps a queued wake's selection and runtime through an equal-time stale detach", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("queued-admission");
      const threadId = ThreadId.make("thread:queued-admission");
      const A: ModelSelection = {
        instanceId: admissionInstanceId,
        model: "model-a",
        options: [{ id: "effort", value: "low" }],
      };
      const B: ModelSelection = {
        ...A,
        model: "model-b",
        options: [{ id: "effort", value: "high" }],
      };
      const C: ModelSelection = { ...B, model: "model-c" };
      // Contract-valid but not transitive: A to B and B to C apply in session, A to C restarts.
      const applyOnNextTurn = (current: ModelSelection, target: ModelSelection) =>
        current.model === target.model ||
        (current.model === "model-a" && target.model === "model-b") ||
        (current.model === "model-b" && target.model === "model-c");
      const queues = new Map<string, Queue.Queue<ProviderAdapter.ProviderAdapterV2Event>>();
      const closed = new Set<string>();
      const attempts: Array<{
        readonly sessionId: string;
        readonly requested: ModelSelection;
        readonly compatible: boolean;
      }> = [];
      const adapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
        driver,
        instanceId: admissionInstanceId,
        getCapabilities: () => Effect.succeed(exclusiveCapabilities),
        planSelectionTransition: ({ current, target }) =>
          Effect.succeed({
            type: applyOnNextTurn(current, target) ? "apply_on_next_turn" : "restart_session",
          }),
        openSession: (input) =>
          Effect.gen(function* () {
            const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => void closed.add(input.providerSessionId)),
            );
            return {
              driver,
              instanceId: admissionInstanceId,
              providerSessionId: input.providerSessionId,
              providerSession: yield* admissionSession(
                input.providerSessionId,
                cwd,
                input.modelSelection.model,
                exclusiveCapabilities,
              ),
              events: Stream.fromQueue(events),
              ensureThread: ({ threadId }) =>
                admissionProviderThread(input.providerSessionId, threadId, undefined, "native"),
              resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
              startTurn: (turn) =>
                Effect.gen(function* () {
                  attempts.push({
                    sessionId: input.providerSessionId,
                    requested: turn.modelSelection,
                    compatible: applyOnNextTurn(input.modelSelection, turn.modelSelection),
                  });
                  queues.set(turn.attemptId, events);
                  yield* runningTurn(events, turn);
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
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const finish = (runId: RunId) =>
          Effect.gen(function* () {
            yield* completeAdmissionTurn(threadId, (attemptId) => queues.get(attemptId)!, runId);
            yield* orchestrator.resumeQueuedRuns;
            yield* worker.drain();
          });
        yield* orchestrator.dispatch(admissionThread(threadId, A, cwd));
        yield* startAdmissionTurn(threadId, "first");
        const first = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("queued"),
          threadId,
          messageId: MessageId.make("message:queued"),
          text: "queued",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "agent",
          creationSource: "server",
          notification: {
            source: { kind: "background_task" },
            outcome: "updated",
            summary: "Background activity updated",
          },
        });
        yield* worker.drain();
        const queued = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.status === "queued",
        )!;
        assert.deepEqual(queued.modelSelection, A);
        yield* orchestrator.dispatch({
          ...userMessage(threadId, "desired", B),
          dispatchMode: { type: "steer_active", targetRunId: first.id },
        });
        yield* worker.drain();
        // No clock tick: the selection change's detach shares the queued start's timestamp.
        yield* orchestrator.dispatch({
          type: "thread.model-selection.set",
          commandId: CommandId.make("set-c"),
          threadId,
          modelSelection: C,
        });
        const queuedRunning = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.status === "running" &&
              event.payload.runAttemptId === queued.activeAttemptId,
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* finish(first.id);
        yield* Fiber.join(queuedRunning);
        assert.lengthOf(attempts, 2);
        assert.deepEqual(attempts[1]!.requested, A);
        assert.isFalse(closed.has(attempts[1]!.sessionId), "the old detach spares the new runtime");
        assert.isTrue(closed.has(attempts[0]!.sessionId), "the old detach still closes its own");
        assert.deepEqual(
          (yield* orchestrator.getThreadProjection(threadId)).thread.modelSelection,
          C,
        );
        yield* finish(queued.id);
        yield* orchestrator.dispatch({
          ...userMessage(threadId, "following"),
          createdBy: "agent",
          creationSource: "server",
        });
        yield* worker.drain();
        assert.deepEqual(attempts.at(-1)!.requested, C);
        assert.deepEqual(
          attempts.filter((attempt) => !attempt.compatible),
          [],
          "every turn runs on a session whose loaded selection permits it",
        );
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry(
            { name: "queued-admission" },
            ProviderAdapterRegistry.layerSingle(adapter),
            { runEffectWorker: false },
          ),
        ),
      );
    }),
  ),
);

it.effect("delivers buffered output on its own selection before a selection change applies", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("buffered-output-admission");
      const ownerId = ThreadId.make("thread:buffer-owner");
      const siblingId = ThreadId.make("thread:buffer-sibling");
      const A = effortSelection("low");
      const C = effortSelection("high");
      const queues = new Map<string, Queue.Queue<ProviderAdapter.ProviderAdapterV2Event>>();
      const buffers = new Map<string, string>();
      const calls: ProviderAdapter.ProviderAdapterV2TurnInput[] = [];
      const opens: string[] = [];
      const closes: string[] = [];
      const adapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
        driver,
        instanceId: admissionInstanceId,
        getCapabilities: () => Effect.succeed(pooledCapabilities),
        planSelectionTransition: lowToHighRestarts,
        openSession: (input) =>
          Effect.gen(function* () {
            const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
            opens.push(input.providerSessionId);
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => void closes.push(input.providerSessionId)),
            );
            const bindings = new Set<string>();
            const bind = (thread: OrchestrationV2ProviderThread) =>
              Effect.sync(() => {
                bindings.add(thread.id);
                return thread;
              });
            return {
              driver,
              instanceId: admissionInstanceId,
              providerSessionId: input.providerSessionId,
              providerSession: yield* admissionSession(
                input.providerSessionId,
                cwd,
                input.modelSelection.model,
                pooledCapabilities,
              ),
              events: Stream.fromQueue(events),
              hasPendingBackgroundWork: Effect.sync(() =>
                [...bindings].some((id) => buffers.has(id)),
              ),
              hasPendingBackgroundWorkForThread: () => Effect.succeed(false),
              hasBufferedOutputForThread: (thread) => Effect.sync(() => buffers.has(thread.id)),
              ensureThread: ({ threadId, existingProviderThread }) =>
                admissionProviderThread(
                  input.providerSessionId,
                  threadId,
                  existingProviderThread,
                  `native:${threadId}`,
                ).pipe(Effect.flatMap(bind)),
              resumeThread: ({ providerThread }) => bind(providerThread),
              startTurn: (turn) =>
                Effect.gen(function* () {
                  calls.push(turn);
                  queues.set(turn.attemptId, events);
                  yield* runningTurn(events, turn);
                  if (turn.message.creationSource !== "provider") return;
                  const text = buffers.get(turn.providerThread.id);
                  assert.isDefined(text, "the original runtime owns the output");
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
                      providerTurnId: ProviderTurnId.make(`provider-turn:${turn.attemptId}`),
                      nativeItemRef: null,
                      parentItemId: null,
                      ordinal: turn.runOrdinal * 100 + 1,
                      status: "completed",
                      title: null,
                      startedAt: at,
                      completedAt: at,
                      updatedAt: at,
                      type: "assistant_message",
                      text: text!,
                      messageId: MessageId.make(`assistant:${turn.message.messageId}`),
                      streaming: false,
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
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const eventsFor = (attemptId: string) => queues.get(attemptId)!;
        for (const id of [ownerId, siblingId])
          yield* orchestrator.dispatch(admissionThread(id, A, cwd));
        yield* startAdmissionTurn(ownerId, "first");
        yield* completeAdmissionTurn(ownerId, eventsFor);
        yield* startAdmissionTurn(siblingId, "sibling");
        assert.lengthOf(opens, 1, "both threads share one provider process");
        const owner = (yield* orchestrator.getThreadProjection(ownerId)).providerThreads[0]!;
        const oldSession = owner.providerSessionId!;
        buffers.set(owner.id, "Owned original output");
        yield* orchestrator.dispatch({
          type: "thread.model-selection.set",
          commandId: CommandId.make("set-c"),
          threadId: ownerId,
          modelSelection: C,
        });
        yield* worker.drain();
        assert.notInclude(closes, oldSession, "the process holding the output stays up");
        const blocked = yield* Effect.exit(
          orchestrator.dispatch(userMessage(ownerId, "blocked-following")),
        );
        assert.equal(blocked._tag, "Failure", "no new execution before the output is delivered");
        const delivered = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "turn-item.updated" &&
              event.payload.type === "assistant_message" &&
              event.payload.text === "Owned original output",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("deliver"),
          threadId: ownerId,
          messageId: MessageId.make("message:deliver"),
          text: "Do not execute this placeholder",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "agent",
          creationSource: "provider",
          notification: {
            source: { kind: "background_task" },
            outcome: "updated",
            summary: "Owned output",
          },
        });
        yield* worker.drain();
        yield* Fiber.join(delivered);
        assert.equal(calls.at(-1)!.providerThread.providerSessionId, oldSession);
        assert.deepEqual(calls.at(-1)!.modelSelection, A);
        assert.deepEqual(
          (yield* orchestrator.getThreadProjection(ownerId)).thread.modelSelection,
          C,
        );
        yield* completeAdmissionTurn(ownerId, eventsFor);
        yield* startAdmissionTurn(ownerId, "following");
        assert.lengthOf(opens, 2);
        assert.deepEqual(calls.at(-1)!.modelSelection, C);
        yield* completeAdmissionTurn(siblingId, eventsFor);
        yield* startAdmissionTurn(siblingId, "sibling-following");
        assert.equal(calls.at(-1)!.providerThread.providerSessionId, oldSession);
        assert.notInclude(closes, oldSession, "the other thread on the process keeps running");
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry(
            { name: "buffered-output-admission" },
            ProviderAdapterRegistry.layerSingle(adapter),
            { runEffectWorker: false },
          ),
        ),
      );
    }),
  ),
);
