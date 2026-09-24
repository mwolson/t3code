import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import { resolveSelfInvocation } from "@t3tools/shared/nodeRuntime";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/process";
import * as AcpErrors from "effect-acp/errors";
import * as AcpSessionRuntime from "../provider/acp/AcpSessionRuntime.ts";
import { makeAcpAdapterV2, AcpProviderCapabilitiesV2 } from "./Adapters/AcpAdapterV2.ts";
import { makeReplayServerConfig } from "./Adapters/CodexAdapterV2.testkit.ts";
import { OrchestrationEffectWorkerV2 } from "./EffectWorker.ts";
import { layer as idAllocatorLayer, IdAllocatorV2 } from "./IdAllocator.ts";
import { OrchestratorV2 } from "./Orchestrator.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import {
  ProviderContinuationRequests,
  type ProviderContinuationRequest,
} from "./ProviderContinuationRequests.ts";
import * as ProviderContinuationService from "./ProviderContinuationService.ts";
import { ProviderSessionManagerV2 } from "./ProviderSessionManager.ts";
import { ThreadManagementService } from "./ThreadManagementService.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "./testkit/ReplayFixtureWorkspace.ts";

type Runtime = AcpSessionRuntime.AcpSessionRuntime["Service"];
const encodeLog = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
it.live.each(
  (() => {
    const cases = [];
    for (const scenario of [
      "before-application",
      "partial-model",
      "error-response",
      "indeterminate",
      "config-response",
      "mixed-prefix",
    ] as const) {
      cases.push({ scenario });
    }
    return cases;
  })(),
)("ACP $scenario output retains producing attribution without certifying a drain", ({ scenario }) =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace(`acp-selection-${scenario}`);
      const fileSystem = yield* FileSystem.FileSystem.pipe(Effect.provide(NodeServices.layer));
      const requestLogPath = `${cwd}/acp-requests.jsonl`;
      const modelRequests = () =>
        fileSystem
          .readFileString(requestLogPath)
          .pipe(
            Effect.map(
              (text) =>
                text.split("\n").filter((line) => line.includes('"configId":"model"')).length,
            ),
          );
      const instanceId = ProviderInstanceId.make("acp-test");
      const driver = ProviderDriverKind.make("acp-test");
      const offers = yield* Queue.unbounded<ProviderContinuationRequest>();
      const logs: Array<unknown> = [];
      const logger = Logger.make(({ message }) => {
        logs.push(message);
      });
      let update: Parameters<Runtime["handleSessionUpdate"]>[0] | undefined;
      let armed = false;
      let opens = 0;
      let prompts = 0;
      const modelCalls: Array<{ model: string; force: boolean | undefined }> = [];
      let modeCalls = 0;
      const reject = () =>
        new AcpErrors.AcpRequestError({
          code: -32603,
          errorMessage: "fixture configuration rejected",
          operation: "receive-response",
        });
      const buffer = () =>
        update!({
          sessionId: "mock-session-1",
          update: {
            sessionUpdate: "agent_message_chunk",
            content: { type: "text", text: `Retained ${scenario} output` },
          },
        });
      const adapter = yield* Effect.gen(function* () {
        const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        return makeAcpAdapterV2({
          instanceId,
          crypto: yield* Crypto.Crypto,
          fileSystem: yield* FileSystem.FileSystem,
          idAllocator: yield* IdAllocatorV2,
          selfInvocation: yield* resolveSelfInvocation(),
          serverConfig: yield* makeReplayServerConfig(`acp-selection-${scenario}`),
          continuationRequests: {
            offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
          },
          flavor: {
            driver,
            capabilities: AcpProviderCapabilitiesV2,
            deferFinalizeForBackgroundWork: true,
            enablePostSettleContinuation: true,
            sessionModeForPolicy: () => (scenario === "config-response" ? undefined : "code"),
            makeRuntime: (input) =>
              Effect.gen(function* () {
                opens++;
                const context = yield* Layer.build(
                  AcpSessionRuntime.layer({
                    ...input,
                    spawn: {
                      command: process.execPath,
                      args: [new URL("../../scripts/acp-mock-agent.ts", import.meta.url).pathname],
                      cwd: input.cwd,
                      env: {
                        T3_ACP_SESSION_LIFECYCLE: "1",
                        T3_ACP_REQUEST_LOG_PATH: requestLogPath,
                      },
                    },
                    authMethodId: "test",
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
                      update = handler;
                    }).pipe(Effect.andThen(runtime.handleSessionUpdate(handler))),
                  prompt: (input) =>
                    Effect.sync(() => {
                      prompts++;
                    }).pipe(Effect.andThen(runtime.prompt(input))),
                  setModel: (model, force) =>
                    Effect.gen(function* () {
                      modelCalls.push({ model, force });
                      if (
                        armed &&
                        (scenario === "before-application" || scenario === "mixed-prefix")
                      )
                        yield* buffer();
                      if (armed && scenario === "error-response") return yield* reject();
                      yield* runtime.setModel(model, force);
                      if (armed && scenario === "indeterminate")
                        return yield* new AcpErrors.AcpTransportError({
                          operation: "call-rpc",
                          method: "session/set_config_option",
                          cause: "fixture lost response",
                        });
                    }),
                  setConfigOption: (id, value, force) =>
                    armed && scenario === "config-response"
                      ? Effect.fail(reject())
                      : runtime.setConfigOption(id, value, force),
                  setMode: (mode, force) =>
                    Effect.gen(function* () {
                      modeCalls++;
                      if (armed) {
                        return yield* reject();
                      }
                      return yield* runtime.setMode(mode, force);
                    }),
                };
              }),
          },
        });
      }).pipe(Effect.provide(Layer.merge(NodeServices.layer, idAllocatorLayer)));
      yield* Effect.gen(function* () {
        const orchestrator = yield* OrchestratorV2;
        const worker = yield* OrchestrationEffectWorkerV2;
        const manager = yield* ProviderSessionManagerV2;
        const threadId = ThreadId.make(`thread:acp-selection-${scenario}`);
        const projection = orchestrator.getThreadProjection(threadId);
        const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
          orchestrator.streamDomainEvents.pipe(
            Stream.filter(predicate),
            Stream.runHead,
            Effect.forkChild({ startImmediately: true }),
          );
        const send = (name: string, model: string) =>
          orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(name),
            threadId,
            messageId: MessageId.make(name),
            text: name,
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
            modelSelection: {
              instanceId,
              model,
              ...(scenario === "config-response"
                ? { options: [{ id: "mode", value: name === "first" ? "code" : "architect" }] }
                : {}),
            },
          });
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make("create"),
          threadId,
          projectId: ProjectId.make("project:acp-selection"),
          title: "ACP attribution",
          modelSelection: { instanceId, model: "default" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
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
        assert.isFalse(yield* runtime.hasBufferedOutputForThread!(binding));
        armed = true;
        const failed = yield* watch(
          (event) =>
            event.type === "run.updated" &&
            event.payload.status === (scenario === "config-response" ? "completed" : "failed"),
        );
        yield* send("change", "composer-2");
        yield* Fiber.join(failed);
        yield* worker.drain();
        if (scenario !== "before-application") yield* buffer();
        assert.isTrue(yield* runtime.hasBufferedOutputForThread!(binding));
        const evidenceBeforeDrain = yield* runtime.executionSelection!(binding);
        if (scenario === "config-response") {
          assert.deepEqual(evidenceBeforeDrain, {
            instanceId,
            model: "composer-2",
            options: [{ id: "mode", value: "architect" }],
          });
        } else {
          assert.isNull(evidenceBeforeDrain);
        }
        const warnings = logs.filter((message) =>
          encodeLog(message).includes("acp-selection-step-indeterminate"),
        );
        assert.lengthOf(warnings, scenario === "indeterminate" ? 1 : 0);
        if (scenario === "indeterminate") {
          assert.include(encodeLog(warnings), '"optionId":"model"');
          assert.include(encodeLog(warnings), '"sessionId":"mock-session-1"');
        }
        const offered = yield* Queue.take(offers);
        const requests = yield* Queue.unbounded<ProviderContinuationRequest>();
        const dispatched = yield* Queue.unbounded<boolean>();
        yield* Layer.build(
          ProviderContinuationService.layer.pipe(
            Layer.provide(
              Layer.mergeAll(
                idAllocatorLayer,
                Layer.succeed(ProviderContinuationRequests, {
                  offer: (request) => Queue.offer(requests, request).pipe(Effect.asVoid),
                  take: Queue.take(requests),
                }),
                Layer.mock(ThreadManagementService)({
                  getThreadRecords: orchestrator.getThreadRecords,
                  dispatch: (command) =>
                    orchestrator
                      .dispatch(command)
                      .pipe(
                        Effect.onExit((exit) =>
                          Queue.offer(dispatched, Exit.isSuccess(exit)).pipe(Effect.asVoid),
                        ),
                      ),
                }),
              ),
            ),
          ),
        );
        const beforeDrain = { modelCalls: modelCalls.length, modeCalls, prompts };
        const delivered = yield* watch(
          (event) =>
            event.type === "run.updated" && ["completed", "failed"].includes(event.payload.status),
        );
        yield* Queue.offer(requests, offered);
        assert.isTrue(
          yield* Queue.take(dispatched),
          "owned producer-attributed output must be admitted",
        );
        yield* Fiber.join(delivered);
        yield* worker.drain();
        assert.deepEqual(
          { modelCalls: modelCalls.length, modeCalls, prompts },
          beforeDrain,
          "drain must neither configure nor prompt",
        );
        assert.deepEqual(
          yield* runtime.executionSelection!(binding),
          evidenceBeforeDrain,
          "drain cannot change configuration evidence",
        );
        const current = yield* projection;
        const expectedModel =
          scenario === "partial-model" || scenario === "config-response" ? "composer-2" : "default";
        assert.equal(current.runs.at(-1)?.modelSelection.model, expectedModel);
        if (scenario === "config-response")
          assert.deepEqual(current.runs.at(-1)?.modelSelection.options, [
            { id: "mode", value: "code" },
          ]);
        assert.isTrue(
          current.messages.some((message) => message.text.includes(`Retained ${scenario} output`)),
        );
        if (scenario === "mixed-prefix") {
          assert.isTrue(
            yield* runtime.hasBufferedOutputForThread!(binding),
            "different producing label remains after prefix drain",
          );
          assert.equal((yield* runtime.bufferedExecutionSelection!(binding))?.model, "composer-2");
          assert.equal(yield* Queue.size(offers), 1, "prefix completion re-offers its remainder");
          const next = yield* Queue.take(offers);
          const completed = yield* watch(
            (event) =>
              event.type === "run.updated" &&
              event.payload.ordinal === 4 &&
              event.payload.status === "completed",
          );
          yield* Queue.offer(requests, next);
          assert.isTrue(
            yield* Queue.take(dispatched),
            "remainder is re-offered after prefix completion",
          );
          yield* Fiber.join(completed);
          yield* worker.drain();
          assert.equal((yield* projection).runs.at(-1)?.modelSelection.model, "composer-2");
          assert.deepEqual({ modelCalls: modelCalls.length, modeCalls, prompts }, beforeDrain);
          assert.isNull(yield* runtime.executionSelection!(binding));
        }
        assert.isFalse(yield* runtime.hasBufferedOutputForThread!(binding));
        assert.equal(prompts, scenario === "config-response" ? 2 : 1, "no prompt is replayed");
        armed = false;
        const requestsBeforeRetry = yield* modelRequests();
        const recovered = yield* watch(
          (event) => event.type === "run.updated" && event.payload.status === "completed",
        );
        yield* send("explicit-retry", "composer-2");
        yield* Fiber.join(recovered);
        yield* worker.drain();
        assert.equal(opens, 1, "full reapplication must reuse the native session");
        assert.isAbove(modelCalls.length, beforeDrain.modelCalls);
        assert.isTrue(modelCalls.at(-1)?.force);
        assert.isAbove(
          yield* modelRequests(),
          requestsBeforeRetry,
          "reapplication must reach native wire even when the runtime cache matches",
        );
        assert.deepEqual(yield* runtime.executionSelection!(binding), {
          instanceId,
          model: "composer-2",
          ...(scenario === "config-response"
            ? { options: [{ id: "mode", value: "architect" }] }
            : {}),
        });
        assert.equal(prompts, scenario === "config-response" ? 3 : 2);
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry(
            { name: `acp-selection-${scenario}` },
            ProviderAdapterRegistry.layerSingle(adapter),
          ).pipe(Layer.provideMerge(Logger.layer([logger], { mergeWithExisting: false }))),
        ),
      );
    }),
  ),
);
