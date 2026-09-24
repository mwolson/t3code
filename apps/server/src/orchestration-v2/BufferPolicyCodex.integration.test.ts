import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CodexSettings,
  CommandId,
  MessageId,
  ProjectId,
  ThreadId,
  type ModelSelection,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as CodexClient from "effect-codex-app-server/client";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stdio from "effect/Stdio";
import * as Stream from "effect/Stream";
import { CODEX_DEFAULT_INSTANCE_ID, makeCodexAdapterV2 } from "./Adapters/CodexAdapterV2.ts";
import { makeReplayServerConfig } from "./Adapters/CodexAdapterV2.testkit.ts";
import { OrchestrationEffectWorkerV2 } from "./EffectWorker.ts";
import { layer as idAllocatorLayer, IdAllocatorV2 } from "./IdAllocator.ts";
import { OrchestratorV2 } from "./Orchestrator.ts";
import { makeSingleLayer } from "./ProviderAdapterRegistry.ts";
import {
  ProviderContinuationRequests,
  type ProviderContinuationRequest,
} from "./ProviderContinuationRequests.ts";
import { workerLive } from "./ProviderContinuationService.ts";
import { ProviderSessionManagerV2 } from "./ProviderSessionManager.ts";
import { ThreadManagementService } from "./ThreadManagementService.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "./testkit/ReplayFixtureWorkspace.ts";

const A: ModelSelection = {
  instanceId: CODEX_DEFAULT_INSTANCE_ID,
  model: "gpt-5.4",
  options: [{ id: "reasoningEffort", value: "high" }],
};
const B: ModelSelection = { ...A, options: [{ id: "reasoningEffort", value: "low" }] };
const NATIVE = "native-buffer-codex";
const settings = Schema.decodeSync(CodexSettings)({});
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeRequest = Schema.decodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      id: Schema.optional(Schema.Number),
      method: Schema.String,
      params: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
    }),
  ),
);
const turn = (id: string, status = "inProgress") => ({
  id,
  status,
  items: [],
  itemsView: "notLoaded",
  error: null,
  startedAt: 1782622440,
  completedAt: status === "completed" ? 1782622450 : null,
  durationMs: null,
});
const commandItem = (status: string) => ({
  type: "commandExecution",
  id: "command-background",
  command: "sleep 60",
  cwd: "/workspace",
  processId: "4242",
  source: "unifiedExecStartup",
  status,
  commandActions: [{ type: "unknown", command: "sleep 60" }],
  aggregatedOutput: "finished",
  exitCode: status === "completed" ? 0 : null,
  durationMs: null,
});

for (const queued of [false, true]) {
  it.effect(`real Codex transport and continuation worker: ${queued ? "queued" : "idle"}`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const cwd = yield* checkpointWorkspace("codex-buffer-policy");
        const inbound = yield* Queue.unbounded<Uint8Array>();
        const offered = yield* Queue.unbounded<ProviderContinuationRequest>();
        const acceptedStarts: Array<Record<string, unknown>> = [];
        const opens: Array<string> = [];
        const encoder = new TextEncoder();
        const emit = (frame: unknown) =>
          Queue.offer(inbound, encoder.encode(`${encodeJson(frame)}\n`)).pipe(Effect.asVoid);
        const notify = (method: string, params: unknown) => emit({ method, params });
        let remainder = "";
        const adapter = yield* Effect.gen(function* () {
          const client = yield* CodexClient.make(
            Stdio.make({
              args: Effect.succeed([]),
              stdin: Stream.fromQueue(inbound),
              stderr: () => Sink.drain,
              stdout: () =>
                Sink.forEach((chunk) =>
                  Effect.gen(function* () {
                    remainder +=
                      typeof chunk === "string" ? chunk : new TextDecoder().decode(chunk);
                    const lines = remainder.split("\n");
                    remainder = lines.pop()!;
                    for (const line of lines.filter(Boolean)) {
                      const request = yield* decodeRequest(line).pipe(Effect.orDie);
                      if (request.method === "initialized") continue;
                      let result: unknown;
                      if (request.method === "initialize")
                        result = {
                          userAgent: "test",
                          codexHome: "/tmp/codex",
                          platformFamily: "unix",
                          platformOs: "linux",
                        };
                      else if (
                        request.method === "thread/start" ||
                        request.method === "thread/resume"
                      )
                        result = {
                          thread: {
                            id: NATIVE,
                            sessionId: NATIVE,
                            forkedFromId: null,
                            preview: "",
                            projectId: null,
                            ephemeral: false,
                            modelProvider: "openai",
                            createdAt: 1782622440,
                            updatedAt: 1782622440,
                            status: { type: "idle" },
                            path: `/tmp/${NATIVE}.jsonl`,
                            cwd,
                            cliVersion: "0.144.0",
                            source: "vscode",
                            threadSource: null,
                            agentNickname: null,
                            agentRole: null,
                            gitInfo: null,
                            name: null,
                            turns: [],
                          },
                          model: A.model,
                          modelProvider: "openai",
                          serviceTier: null,
                          cwd,
                          instructionSources: [],
                          approvalPolicy: "never",
                          approvalsReviewer: "user",
                          sandbox: { type: "dangerFullAccess" },
                          reasoningEffort: "high",
                        };
                      else if (request.method === "turn/start") {
                        acceptedStarts.push(request.params ?? {});
                        result = { turn: turn(`turn-${acceptedStarts.length}`) };
                      } else {
                        yield* emit({
                          id: request.id,
                          error: {
                            code: -32601,
                            message: `Unexpected Codex request: ${request.method}`,
                          },
                        });
                        continue;
                      }
                      yield* emit({ id: request.id, result });
                    }
                  }),
                ),
            }),
          );
          return makeCodexAdapterV2({
            instanceId: CODEX_DEFAULT_INSTANCE_ID,
            settings,
            environment: {},
            fileSystem: yield* FileSystem.FileSystem,
            idAllocator: yield* IdAllocatorV2,
            serverConfig: yield* makeReplayServerConfig("buffer-policy-codex"),
            clientFactory: {
              open: (input) =>
                Effect.sync(() => {
                  opens.push(input.providerSessionId);
                  return client;
                }),
            },
            continuationRequests: {
              offer: (request) => Queue.offer(offered, request).pipe(Effect.asVoid),
            },
          });
        }).pipe(Effect.provide(Layer.merge(idAllocatorLayer, NodeServices.layer)));
        yield* Effect.gen(function* () {
          const orchestrator = yield* OrchestratorV2;
          const worker = yield* OrchestrationEffectWorkerV2;
          const manager = yield* ProviderSessionManagerV2;
          const threadId = ThreadId.make(`thread:codex-buffer:${queued}`);
          const projection = orchestrator.getThreadProjection(threadId);
          const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
            orchestrator.streamDomainEvents.pipe(
              Stream.filter(predicate),
              Stream.runHead,
              Effect.forkChild({ startImmediately: true }),
            );
          const send = (name: string, modelSelection?: ModelSelection) =>
            orchestrator.dispatch({
              type: "message.dispatch",
              commandId: CommandId.make(name),
              threadId,
              messageId: MessageId.make(`message:${name}`),
              text: name,
              attachments: [],
              dispatchMode: { type: "start_immediately" },
              createdBy: "user",
              creationSource: "web",
              ...(modelSelection ? { modelSelection } : {}),
            });
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create"),
            threadId,
            projectId: ProjectId.make("project:codex-buffer"),
            title: "Codex buffer",
            modelSelection: A,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
          const started = yield* watch(
            (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
          );
          yield* send("first");
          yield* worker.drain();
          yield* Fiber.join(started);
          const settled = yield* watch(
            (event) => event.type === "run.updated" && event.payload.status === "waiting",
          );
          yield* notify("item/started", {
            threadId: NATIVE,
            turnId: "turn-1",
            item: commandItem("inProgress"),
            startedAtMs: 1782622440000,
          });
          yield* notify("turn/completed", { threadId: NATIVE, turn: turn("turn-1", "completed") });
          yield* Fiber.join(settled);
          yield* worker.drain();
          const binding = (yield* projection).providerThreads[0]!;
          const runtime = Option.getOrThrow(yield* manager.get(binding.providerSessionId!));
          assert.isTrue(yield* runtime.hasPendingBackgroundWorkForThread!(binding));
          assert.deepEqual(yield* runtime.executionSelection!(binding), A);
          // Unlike Claude, a Codex option change does not replace the process.
          const second = yield* watch(
            (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
          );
          yield* send("changed-options", B);
          yield* worker.drain();
          yield* Fiber.join(second);
          assert.lengthOf(opens, 1);
          assert.deepEqual(yield* runtime.executionSelection!(binding), B);
          assert.equal(acceptedStarts[0]?.effort, "high");
          assert.equal(acceptedStarts[1]?.effort, "low");
          assert.isTrue(yield* runtime.hasPendingBackgroundWorkForThread!(binding));
          const finishSecond = Effect.gen(function* () {
            const ended = yield* watch(
              (event) =>
                event.type === "run.updated" &&
                event.payload.status === "waiting" &&
                event.payload.ordinal === 2,
            );
            yield* notify("turn/completed", {
              threadId: NATIVE,
              turn: turn("turn-2", "completed"),
            });
            yield* Fiber.join(ended);
            yield* worker.drain();
          });
          if (!queued) yield* finishSecond;
          yield* notify("item/completed", {
            threadId: NATIVE,
            turnId: "turn-1",
            item: commandItem("completed"),
            completedAtMs: 1782622450000,
          });
          const request = yield* Queue.take(offered);
          assert.equal(request.delivery, "message_text");
          assert.isFalse(yield* runtime.hasPendingBackgroundWorkForThread!(binding));
          yield* orchestrator.dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("save-future"),
            threadId,
            modelSelection: A,
          });
          const requests = yield* Queue.unbounded<ProviderContinuationRequest>();
          const dispatched = yield* Queue.unbounded<boolean>();
          yield* Layer.build(
            workerLive.pipe(
              Layer.provide(
                Layer.mergeAll(
                  idAllocatorLayer,
                  Layer.succeed(ProviderContinuationRequests, {
                    offer: (value) => Queue.offer(requests, value).pipe(Effect.asVoid),
                    take: Queue.take(requests),
                  }),
                  Layer.mock(ThreadManagementService)({
                    getThreadRecords: orchestrator.getThreadRecords,
                    dispatch: (value) =>
                      orchestrator
                        .dispatch(value)
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
          yield* Queue.offer(requests, request);
          assert.isTrue(yield* Queue.take(dispatched));
          if (queued) {
            assert.equal((yield* projection).runs.at(-1)?.status, "queued");
            yield* orchestrator.dispatch({
              type: "thread.model-selection.set",
              commandId: CommandId.make("save-future-after-queue"),
              threadId,
              modelSelection: B,
            });
            yield* finishSecond;
          }
          yield* worker.drain();
          assert.lengthOf(acceptedStarts, 3, "completed-command wake starts a new native prompt");
          const current = yield* projection;
          assert.deepEqual(current.thread.modelSelection, queued ? B : A);
          assert.deepEqual(current.runs.at(-1)?.modelSelection, A);
          assert.equal(acceptedStarts[2]?.effort, "high");
          assert.isTrue(encodeJson(acceptedStarts[2]?.input).includes("sleep 60"));
          assert.lengthOf(opens, 1);
          const completed = yield* watch(
            (event) =>
              event.type === "run.updated" &&
              event.payload.status === "waiting" &&
              event.payload.ordinal === 3,
          );
          yield* notify("item/completed", {
            threadId: NATIVE,
            turnId: "turn-1",
            item: commandItem("completed"),
            completedAtMs: 1782622450000,
          });
          yield* notify("turn/completed", { threadId: NATIVE, turn: turn("turn-3", "completed") });
          yield* Fiber.join(completed);
          yield* worker.drain();
          assert.equal(
            yield* Queue.size(offered),
            0,
            "duplicate native completion has no second owner",
          );
          assert.lengthOf(acceptedStarts, 3);
          assert.deepEqual((yield* projection).thread.modelSelection, queued ? B : A);
        }).pipe(
          Effect.provide(
            makeOrchestratorV2ReplayLayerWithRegistry(
              { name: "codex-buffer-policy" },
              makeSingleLayer(adapter),
              { runEffectWorker: false },
            ),
          ),
        );
      }),
    ),
  );
}
