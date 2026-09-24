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
const B: ModelSelection = {
  ...A,
  options: [
    { id: "reasoningEffort", value: "low" },
    { id: "serviceTier", value: "flex" },
  ],
};
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

for (const background of [true, false])
  for (const changed of [false, true, "ownership"] as const) {
    it.effect(`Codex failed start recovery: background=${background}, changed=${changed}`, () =>
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
          let failNextStart = false;
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
                        else if (request.method === "turn/start" && failNextStart) {
                          failNextStart = false;
                          yield* emit({
                            id: request.id,
                            error: { code: -32000, message: "review transient turn/start failure" },
                          });
                          continue;
                        } else if (request.method === "turn/start") {
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
            const threadId = ThreadId.make(`thread:review-codex-failed:${background}`);
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
              (event) =>
                event.type === "provider-turn.updated" && event.payload.status === "running",
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
            yield* notify("turn/completed", {
              threadId: NATIVE,
              turn: turn("turn-1", "completed"),
            });
            yield* Fiber.join(settled);
            yield* worker.drain();
            const binding = (yield* projection).providerThreads[0]!;
            const runtime = Option.getOrThrow(yield* manager.get(binding.providerSessionId!));
            if (changed === "ownership") {
              const staleWake = yield* Effect.exit(
                orchestrator.dispatch({
                  type: "message.dispatch",
                  commandId: CommandId.make("stale-buffered-wake"),
                  threadId,
                  messageId: MessageId.make("stale-buffered-wake"),
                  text: "Stale buffered signal",
                  attachments: [],
                  dispatchMode: { type: "start_immediately" },
                  createdBy: "agent",
                  creationSource: "provider",
                }),
              );
              assert.isTrue(
                Exit.isFailure(staleWake),
                "matching selection without owned output cannot admit a buffered wake",
              );
              assert.lengthOf((yield* projection).runs, 1);
              return;
            }
            const log = (label: string, value: unknown) => Effect.logDebug(label, value);
            if (!background) {
              yield* notify("item/completed", {
                threadId: NATIVE,
                turnId: "turn-1",
                item: commandItem("completed"),
                completedAtMs: 1782622450000,
              });
              yield* Queue.take(offered);
            }
            yield* log(
              "pendingForThread",
              yield* runtime.hasPendingBackgroundWorkForThread!(binding),
            );
            yield* log("executionBeforeFailure", yield* runtime.executionSelection!(binding));
            failNextStart = true;
            const failed = yield* watch(
              (event) => event.type === "run.updated" && event.payload.status === "failed",
            );
            const second = yield* Effect.exit(send("second", changed ? B : A));
            yield* log("second.exit", second._tag);
            yield* worker.drain();
            yield* Fiber.join(failed);
            yield* log("afterFailure.execution", yield* runtime.executionSelection!(binding));
            yield* log(
              "afterFailure.runs",
              (yield* projection).runs.map((run) => [run.ordinal, run.status]),
            );
            if (changed) assert.isNull(yield* runtime.executionSelection!(binding));
            const third = yield* Effect.exit(send("third", changed ? B : A));
            yield* log("third.exit", third._tag === "Failure" ? String(third.cause) : "Success");
            yield* worker.drain();
            const after = yield* projection;
            yield* log(
              "third.runs",
              after.runs.map((run) => [run.ordinal, run.status, run.providerThreadId]),
            );
            yield* log("opens", opens);
            yield* log(
              "providerSessions",
              after.providerSessions.map((session) => [session.id, session.status]),
            );
            yield* log("acceptedStarts", acceptedStarts.length);
            assert.equal(
              third._tag,
              "Success",
              "same full selection retry after a transient Codex turn/start failure is non-destructive",
            );
            assert.lengthOf(
              opens,
              1,
              "no extra app-server process for a non-destructive Codex retry",
            );
            assert.deepEqual(yield* runtime.executionSelection!(binding), changed ? B : A);
            assert.isTrue(yield* runtime.reappliesFullSelection!(B));
            for (const omitted of ["reasoningEffort", "serviceTier"]) {
              assert.isFalse(
                yield* runtime.reappliesFullSelection!({
                  ...B,
                  options: B.options!.filter((option) => option.id !== omitted),
                }),
              );
            }
            if (changed) {
              assert.equal(acceptedStarts.at(-1)?.effort, "low");
              assert.equal(acceptedStarts.at(-1)?.serviceTier, "flex");
            }
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
