import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderTurnId,
  ThreadId,
  type ModelSelection,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import { resolveSelfInvocation } from "@t3tools/shared/nodeRuntime";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as AcpErrors from "effect-acp/errors";
import * as AcpSessionRuntime from "../provider/acp/AcpSessionRuntime.ts";
import { makeAcpAdapterV2, AcpProviderCapabilitiesV2 } from "./Adapters/AcpAdapterV2.ts";
import { makeReplayServerConfig } from "./Adapters/CodexAdapterV2.testkit.ts";
import { OrchestrationEffectWorkerV2 } from "./EffectWorker.ts";
import { layer as idAllocatorLayer, IdAllocatorV2 } from "./IdAllocator.ts";
import { OrchestratorV2 } from "./Orchestrator.ts";
import { makeSingleLayer } from "./ProviderAdapterRegistry.ts";
import { type ProviderContinuationRequest } from "./ProviderContinuationRequests.ts";
import { ProviderSessionManagerV2 } from "./ProviderSessionManager.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "./testkit/ReplayFixtureWorkspace.ts";

type Runtime = AcpSessionRuntime.AcpSessionRuntime["Service"];

const harness = (input: {
  readonly name: string;
  readonly terminate: boolean;
  readonly rejectModeWhenArmed: boolean;
  readonly rejectConfig?: boolean;
  readonly holdSecondPrompt?: boolean;
}) =>
  Effect.gen(function* () {
    const cwd = yield* checkpointWorkspace(input.name);
    const instanceId = ProviderInstanceId.make("acp-selection-settled");
    const driver = ProviderDriverKind.make("acp-selection-settled");
    const offers = yield* Queue.unbounded<ProviderContinuationRequest>();
    const stopped = yield* Deferred.make<void>();
    const stopEntered = yield* Deferred.make<void>();
    const promptGate = {
      reached: yield* Deferred.make<void>(),
      release: yield* Deferred.make<void>(),
    };
    const state = {
      update: undefined as Parameters<Runtime["handleSessionUpdate"]>[0] | undefined,
      armed: false,
      opens: 0,
      prompts: 0,
      terminations: 0,
      modelCalls: [] as Array<{ model: string; force: boolean | undefined }>,
      configCalls: [] as Array<{ id: string; value: unknown; force: boolean | undefined }>,
    };
    const adapter = yield* Effect.gen(function* () {
      const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      return makeAcpAdapterV2({
        instanceId,
        crypto: yield* Crypto.Crypto,
        fileSystem: yield* FileSystem.FileSystem,
        idAllocator: yield* IdAllocatorV2,
        selfInvocation: yield* resolveSelfInvocation(),
        serverConfig: yield* makeReplayServerConfig(input.name),
        continuationRequests: {
          offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
        },
        flavor: {
          driver,
          capabilities: AcpProviderCapabilitiesV2,
          deferFinalizeForBackgroundWork: true,
          enablePostSettleContinuation: true,
          ...(input.terminate ? { terminateRuntimeProcessGroupOnInterrupt: true } : {}),
          sessionModeForPolicy: () => (input.rejectConfig ? undefined : "code"),
          makeRuntime: (runtimeInput) =>
            Effect.gen(function* () {
              state.opens++;
              const context = yield* Layer.build(
                AcpSessionRuntime.layer({
                  ...runtimeInput,
                  spawn: {
                    command: process.execPath,
                    args: [new URL("../../scripts/acp-mock-agent.ts", import.meta.url).pathname],
                    cwd: runtimeInput.cwd,
                    env: {
                      T3_ACP_SESSION_LIFECYCLE: "1",
                      ...(input.rejectConfig ? { T3_ACP_FAIL_SET_CONFIG_OPTION: "1" } : {}),
                    },
                  },
                  authMethodId: "test",
                  ...(input.terminate ? { ownDetachedProcessGroup: true } : {}),
                }).pipe(
                  Layer.provide(
                    Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, childProcessSpawner),
                  ),
                ),
              );
              const runtime = yield* AcpSessionRuntime.AcpSessionRuntime.pipe(
                Effect.provide(context),
              );
              return {
                ...runtime,
                handleSessionUpdate: (handler) =>
                  Effect.sync(() => {
                    state.update = handler;
                  }).pipe(Effect.andThen(runtime.handleSessionUpdate(handler))),
                prompt: (promptInput) =>
                  Effect.gen(function* () {
                    state.prompts++;
                    if (input.holdSecondPrompt && state.prompts === 2) {
                      yield* Deferred.succeed(promptGate.reached, undefined);
                      yield* Deferred.await(promptGate.release);
                    }
                    return yield* runtime.prompt(promptInput);
                  }),
                setModel: (model: string, force?: boolean) =>
                  Effect.sync(() => {
                    state.modelCalls.push({ model, force });
                  }).pipe(Effect.andThen(runtime.setModel(model, force))),
                setConfigOption: (id: string, value: string | boolean, force?: boolean) =>
                  Effect.sync(() => {
                    state.configCalls.push({ id, value, force });
                  }).pipe(Effect.andThen(runtime.setConfigOption(id, value, force))),
                setMode: (mode: string, force?: boolean) =>
                  state.armed && input.rejectModeWhenArmed
                    ? Effect.fail(
                        new AcpErrors.AcpRequestError({
                          code: -32603,
                          errorMessage: "selection-settled mode rejected",
                          operation: "receive-response",
                        }),
                      )
                    : runtime.setMode(mode, force),
                ...(input.terminate && runtime.terminateProcessGroup !== undefined
                  ? {
                      terminateProcessGroup: Effect.sync(() => {
                        state.terminations++;
                      }).pipe(Effect.andThen(runtime.terminateProcessGroup)),
                    }
                  : {}),
              };
            }),
        },
      });
    }).pipe(Effect.provide(Layer.merge(NodeServices.layer, idAllocatorLayer)));
    const observedAdapter = {
      ...adapter,
      openSession: (input: Parameters<typeof adapter.openSession>[0]) =>
        adapter.openSession(input).pipe(
          Effect.map((runtime) => ({
            ...runtime,
            interruptTurn: (input: Parameters<typeof runtime.interruptTurn>[0]) =>
              Deferred.succeed(stopEntered, undefined).pipe(
                Effect.andThen(runtime.interruptTurn(input)),
                Effect.ensuring(Deferred.succeed(stopped, undefined)),
              ),
          })),
        ),
    };
    return {
      adapter: observedAdapter,
      instanceId,
      offers,
      state,
      cwd,
      stopped,
      stopEntered,
      promptGate,
    };
  });

const buffer = (
  state: { update: Parameters<Runtime["handleSessionUpdate"]>[0] | undefined },
  text: string,
) =>
  state.update!({
    sessionId: "mock-session-1",
    update: { sessionUpdate: "agent_message_chunk", content: { type: "text", text } },
  });

for (const terminate of [false, true]) {
  it.live(
    `selection-settled ACP Stop on failed run with retained output: terminate=${terminate}`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const name = `selection-settled-acp-stop-${terminate}`;
          const h = yield* harness({ name, terminate, rejectModeWhenArmed: true });
          yield* Effect.gen(function* () {
            const orchestrator = yield* OrchestratorV2;
            const worker = yield* OrchestrationEffectWorkerV2;
            const manager = yield* ProviderSessionManagerV2;
            const threadId = ThreadId.make(`thread:${name}`);
            const projection = orchestrator.getThreadProjection(threadId);
            const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
              orchestrator.streamDomainEvents.pipe(
                Stream.filter(predicate),
                Stream.runHead,
                Effect.forkChild({ startImmediately: true }),
              );
            const send = (msg: string, model: string) =>
              orchestrator.dispatch({
                type: "message.dispatch",
                commandId: CommandId.make(msg),
                threadId,
                messageId: MessageId.make(msg),
                text: msg,
                attachments: [],
                dispatchMode: { type: "start_immediately" },
                createdBy: "user",
                creationSource: "web",
                modelSelection: { instanceId: h.instanceId, model },
              });
            yield* orchestrator.dispatch({
              type: "thread.create",
              commandId: CommandId.make("create"),
              threadId,
              projectId: ProjectId.make("project:selection-settled-acp-stop"),
              title: "ACP stop",
              modelSelection: { instanceId: h.instanceId, model: "default" },
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: null,
              worktreePath: h.cwd,
              createdBy: "user",
              creationSource: "web",
            });
            const settled = yield* watch(
              (event) => event.type === "run.updated" && event.payload.status === "completed",
            );
            yield* send("first", "default");
            yield* Fiber.join(settled);
            yield* worker.drain();
            const binding = (yield* projection).providerThreads[0]!;
            const runtime = Option.getOrThrow(yield* manager.get(binding.providerSessionId!));
            h.state.armed = true;
            const failed = yield* watch(
              (event) => event.type === "run.updated" && event.payload.status === "failed",
            );
            yield* send("change", "composer-2");
            yield* Fiber.join(failed);
            yield* worker.drain();
            yield* buffer(h.state, "Retained stop output");
            const before = yield* runtime.hasBufferedOutputForThread!(binding);
            assert.isTrue(before);
            const latest = (yield* projection).runs.at(-1)!;
            const stopExit = yield* Effect.exit(
              orchestrator.dispatch({
                type: "run.interrupt",
                commandId: CommandId.make("explicit-stop"),
                threadId,
                runId: latest.id,
              }),
            );
            yield* worker.drain();
            yield* Deferred.await(h.stopped);
            const resident = yield* manager.get(binding.providerSessionId!);
            const after = Option.isSome(resident)
              ? yield* resident.value.hasBufferedOutputForThread!(binding)
              : false;
            assert.equal(stopExit._tag, "Success");
            assert.equal(h.state.terminations, terminate ? 1 : 0);
            assert.isFalse(after, "explicit Stop discards retained ACP output");
            if (!terminate) {
              const request = yield* Queue.take(h.offers);
              let dispatched = false;
              const stale = yield* request.dispatchIfCurrent!(
                Effect.sync(() => {
                  dispatched = true;
                }),
              );
              assert.isTrue(Option.isNone(stale));
              assert.isFalse(dispatched, "Stop invalidates its queued continuation");
              yield* buffer(h.state, "Late output after Stop");
              assert.isFalse(yield* runtime.hasBufferedOutputForThread!(binding));
              assert.equal(
                yield* Queue.size(h.offers),
                0,
                "stopped frames cannot re-offer a continuation",
              );
            }
          }).pipe(
            Effect.provide(
              makeOrchestratorV2ReplayLayerWithRegistry({ name }, makeSingleLayer(h.adapter)),
            ),
          );
        }),
      ),
  );
}

const unexposedCases: ReadonlyArray<{
  readonly label: string;
  readonly model: string;
  readonly options: NonNullable<ModelSelection["options"]>;
  readonly rejectConfig?: boolean;
}> = [
  {
    label: "default-agent-rejected-option",
    model: "default",
    options: [{ id: "mode", value: "code" }],
    rejectConfig: true,
  },
  {
    label: "default-unexposed-effort",
    model: "default",
    options: [{ id: "effort", value: "high" }],
  },
  {
    label: "concrete-unexposed-effort",
    model: "composer-2",
    options: [{ id: "effort", value: "high" }],
  },
  {
    label: "default-unadvertised-mode",
    model: "default",
    options: [{ id: "mode", value: "no-such-mode" }],
  },
];

for (const scenario of unexposedCases) {
  it.live(
    `selection-settled ACP unchanged selection with a skipped option: ${scenario.label}`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const name = `selection-settled-acp-skip-${scenario.label}`;
          const h = yield* harness({
            name,
            terminate: false,
            rejectModeWhenArmed: false,
            rejectConfig: scenario.rejectConfig ?? false,
          });
          yield* Effect.gen(function* () {
            const orchestrator = yield* OrchestratorV2;
            const worker = yield* OrchestrationEffectWorkerV2;
            const manager = yield* ProviderSessionManagerV2;
            const threadId = ThreadId.make(`thread:${name}`);
            const projection = orchestrator.getThreadProjection(threadId);
            const selection: ModelSelection = {
              instanceId: h.instanceId,
              model: scenario.model,
              options: scenario.options,
            };
            const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
              orchestrator.streamDomainEvents.pipe(
                Stream.filter(predicate),
                Stream.runHead,
                Effect.forkChild({ startImmediately: true }),
              );
            const sendAndSettle = (msg: string) =>
              Effect.gen(function* () {
                const settled = yield* watch(
                  (event) =>
                    event.type === "run.updated" &&
                    (event.payload.status === "completed" || event.payload.status === "failed"),
                );
                const exit = yield* Effect.exit(
                  orchestrator.dispatch({
                    type: "message.dispatch",
                    commandId: CommandId.make(msg),
                    threadId,
                    messageId: MessageId.make(msg),
                    text: msg,
                    attachments: [],
                    dispatchMode: { type: "start_immediately" },
                    createdBy: "user",
                    creationSource: "web",
                    modelSelection: selection,
                  }),
                );
                if (exit._tag === "Success") yield* Fiber.join(settled);
                yield* worker.drain();
                return exit._tag === "Success" ? "Success" : String(exit.cause);
              });
            yield* orchestrator.dispatch({
              type: "thread.create",
              commandId: CommandId.make("create"),
              threadId,
              projectId: ProjectId.make("project:selection-settled-acp-skip"),
              title: "ACP skipped option",
              modelSelection: selection,
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: null,
              worktreePath: h.cwd,
              createdBy: "user",
              creationSource: "web",
            });
            const first = yield* sendAndSettle("first");
            const binding = (yield* projection).providerThreads[0]!;
            const runtime = Option.getOrThrow(yield* manager.get(binding.providerSessionId!));
            const evidenceAfterFirst = yield* runtime.executionSelection!(binding);
            const callsAfterFirst = {
              model: h.state.modelCalls.length,
              config: h.state.configCalls.length,
            };
            const second = yield* sendAndSettle("second");
            const third = yield* sendAndSettle("third");
            assert.equal(second, "Success");
            assert.equal(third, "Success");
            assert.equal(
              h.state.opens,
              1,
              "an unchanged selection must not replace the ACP process",
            );
            assert.equal(first, "Success");
            assert.equal(h.state.prompts, 3);
            assert.deepEqual(evidenceAfterFirst, selection);
            assert.deepEqual(yield* runtime.executionSelection!(binding), selection);
            assert.equal(h.state.modelCalls.length, callsAfterFirst.model);
            assert.equal(h.state.configCalls.length, callsAfterFirst.config);
            assert.isFalse(h.state.modelCalls.some((call) => call.force === true));
          }).pipe(
            Effect.provide(
              makeOrchestratorV2ReplayLayerWithRegistry({ name }, makeSingleLayer(h.adapter)),
            ),
          );
        }),
      ),
  );
}

for (const scenario of ["stale-stop-soft", "stale-stop-hard", "dispatch-racing-stop"] as const) {
  it.live(`selection-settled ACP continuation ownership: ${scenario}`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const name = `selection-settled-${scenario}`;
        const staleStop = scenario !== "dispatch-racing-stop";
        const h = yield* harness({
          name,
          terminate: scenario === "stale-stop-hard",
          rejectModeWhenArmed: false,
          holdSecondPrompt: staleStop,
        });
        yield* Effect.gen(function* () {
          const orchestrator = yield* OrchestratorV2;
          const worker = yield* OrchestrationEffectWorkerV2;
          const manager = yield* ProviderSessionManagerV2;
          const threadId = ThreadId.make(`thread:${name}`);
          const projection = orchestrator.getThreadProjection(threadId);
          const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
            orchestrator.streamDomainEvents.pipe(
              Stream.filter(predicate),
              Stream.runHead,
              Effect.forkChild({ startImmediately: true }),
            );
          const send = (msg: string) =>
            orchestrator.dispatch({
              type: "message.dispatch",
              commandId: CommandId.make(msg),
              threadId,
              messageId: MessageId.make(msg),
              text: msg,
              attachments: [],
              dispatchMode: { type: "start_immediately" },
              createdBy: "user",
              creationSource: "web",
              modelSelection: { instanceId: h.instanceId, model: "default" },
            });
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create"),
            threadId,
            projectId: ProjectId.make("project:selection-stop-race"),
            title: "ACP Stop ownership",
            modelSelection: { instanceId: h.instanceId, model: "default" },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: h.cwd,
            createdBy: "user",
            creationSource: "web",
          });
          const settled = yield* watch(
            (event) => event.type === "run.updated" && event.payload.status === "completed",
          );
          yield* send("first");
          yield* Fiber.join(settled);
          yield* worker.drain();
          const binding = (yield* projection).providerThreads[0]!;
          const runtime = Option.getOrThrow(yield* manager.get(binding.providerSessionId!));
          yield* buffer(h.state, "Retained output before Stop");
          assert.isTrue(yield* runtime.hasBufferedOutputForThread!(binding));
          const stop = runtime.interruptTurn({
            providerThread: binding,
            providerTurnId: ProviderTurnId.make("stale-turn"),
            requestRuntimeRestart: true,
          });
          if (staleStop) {
            const dispatch = yield* send("second").pipe(
              Effect.forkChild({ startImmediately: true }),
            );
            const drain = yield* worker.drain().pipe(Effect.forkChild({ startImmediately: true }));
            yield* Deferred.await(h.promptGate.reached);
            const before = yield* runtime.hasBufferedOutputForThread!(binding);
            const stopped = yield* Effect.exit(stop);
            const after = yield* runtime.hasBufferedOutputForThread!(binding);
            yield* Deferred.succeed(h.promptGate.release, undefined);
            yield* Fiber.join(dispatch);
            yield* Fiber.join(drain);
            yield* worker.drain();
            assert.equal(stopped._tag, "Success");
            assert.isTrue(before);
            assert.isTrue(
              after,
              "a stale Stop cannot discard output while a different turn is active",
            );
            assert.equal(h.state.terminations, 0);
            return;
          }
          const request = yield* Queue.take(h.offers);
          const dispatchEntered = yield* Deferred.make<void>();
          const releaseDispatch = yield* Deferred.make<void>();
          const dispatch = yield* request.dispatchIfCurrent!(
            Effect.gen(function* () {
              yield* Deferred.succeed(dispatchEntered, undefined);
              yield* Deferred.await(releaseDispatch);
              return yield* runtime.hasBufferedOutputForThread!(binding);
            }),
          ).pipe(Effect.forkChild({ startImmediately: true }));
          yield* Deferred.await(dispatchEntered);
          const stopFiber = yield* stop.pipe(Effect.forkChild({ startImmediately: true }));
          yield* Deferred.await(h.stopEntered);
          yield* Effect.yieldNow;
          const stopFinishedDuringDispatch = yield* Deferred.isDone(h.stopped);
          yield* Deferred.succeed(releaseDispatch, undefined);
          const dispatched = yield* Fiber.join(dispatch);
          yield* Fiber.join(stopFiber);
          assert.isFalse(
            stopFinishedDuringDispatch,
            "Stop waits for the admitted dispatch before discarding",
          );
          assert.deepEqual(
            dispatched,
            Option.some(true),
            "an admitted continuation never sees an emptied buffer during dispatch",
          );
          assert.isFalse(yield* runtime.hasBufferedOutputForThread!(binding));
          assert.isTrue(Option.isNone(yield* request.dispatchIfCurrent!(Effect.succeed("stale"))));
          yield* buffer(h.state, "Late frame after racing Stop");
          assert.isFalse(yield* runtime.hasBufferedOutputForThread!(binding));
          assert.equal(yield* Queue.size(h.offers), 0);
        }).pipe(
          Effect.provide(
            makeOrchestratorV2ReplayLayerWithRegistry({ name }, makeSingleLayer(h.adapter)),
          ),
        );
      }),
    ),
  );
}
