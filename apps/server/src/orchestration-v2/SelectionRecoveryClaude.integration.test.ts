import type { SDKMessage } from "@anthropic-ai/claude-agent-sdk";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  ClaudeSettings,
  CommandId,
  MessageId,
  ProjectId,
  ThreadId,
  type ModelSelection,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import type * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import {
  CLAUDE_DEFAULT_INSTANCE_ID,
  ClaudeAgentSdkQueryRunnerError,
  makeClaudeAdapterV2,
} from "./Adapters/ClaudeAdapterV2.ts";
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

const NATIVE = "native-selection-recovery";
const FLUSH = "00000000-0000-4000-8000-000000000099";
const SETTINGS = Schema.decodeSync(ClaudeSettings)({});
const A: ModelSelection = {
  instanceId: CLAUDE_DEFAULT_INSTANCE_ID,
  model: "claude-sonnet-4-6",
  options: [{ id: "effort", value: "high" }],
};
const B: ModelSelection = { ...A, options: [{ id: "effort", value: "low" }] };
const ULTRA: ModelSelection = { ...A, options: [{ id: "effort", value: "ultrathink" }] };
const frame = (value: unknown) => value as SDKMessage;
const result = (id: number, text: string, wake = false) =>
  frame({
    type: "result",
    subtype: "success",
    duration_ms: 10,
    duration_api_ms: 10,
    is_error: false,
    num_turns: 1,
    result: text,
    stop_reason: "end_turn",
    total_cost_usd: 0,
    usage: {
      input_tokens: 1,
      output_tokens: 1,
      cache_creation_input_tokens: 0,
      cache_read_input_tokens: 0,
    },
    modelUsage: {},
    permission_denials: [],
    uuid: `00000000-0000-4000-8000-${String(id).padStart(12, "0")}`,
    session_id: NATIVE,
    ...(wake ? { origin: { kind: "task-notification" } } : {}),
  });
const started = frame({
  type: "system",
  subtype: "task_started",
  task_id: "build",
  description: "build",
  task_type: "local_bash",
  uuid: "started",
  session_id: NATIVE,
});
const finished = frame({
  type: "system",
  subtype: "task_notification",
  task_id: "build",
  status: "completed",
  output_file: "/tmp/build.log",
  summary: "Build finished",
  uuid: "finished",
  session_id: NATIVE,
});
const assistant = frame({
  type: "assistant",
  message: { role: "assistant", content: [{ type: "text", text: "The build has finished." }] },
  parent_tool_use_id: null,
  uuid: "assistant",
  session_id: NATIVE,
});

it.effect.each(
  (() => {
    const cases = [];
    for (const scenario of [
      "revert",
      "refused-wake",
      "in-flight",
      "failed-offer",
      "idle-exit",
      "notification-only",
      "saved-selection",
    ] as const) {
      cases.push({ scenario });
    }
    return cases;
  })(),
)("Claude selection recovery: $scenario", ({ scenario }) =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace(`selection-recovery-${scenario}`);
      const sdk = yield* Queue.unbounded<SDKMessage, Cause.Done>();
      const flushed = yield* Queue.unbounded<void>();
      const offers = yield* Queue.unbounded<ProviderContinuationRequest>();
      const reached = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const streamExit = yield* Deferred.make<void>();
      const threadId = ThreadId.make(`thread:selection-recovery:${scenario}`);
      const producingSelection =
        scenario === "failed-offer" || scenario === "idle-exit" ? ULTRA : A;
      let opens = 0;
      let closes = 0;
      const prompts: string[] = [];
      const adapter = yield* Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        return makeClaudeAdapterV2({
          instanceId: CLAUDE_DEFAULT_INSTANCE_ID,
          settings: SETTINGS,
          environment: {},
          fileSystem,
          attachmentsDir: yield* fileSystem.makeTempDirectoryScoped({
            prefix: "selection-recovery-",
          }),
          path: yield* Path.Path,
          idAllocator: yield* IdAllocatorV2,
          continuationRequests: {
            offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
          },
          queryRunner: {
            allocateSessionId: Effect.succeed(NATIVE),
            open: () =>
              Effect.sync(() => {
                opens++;
                return {
                  messages: Stream.fromQueue(sdk).pipe(
                    Stream.tap((message) =>
                      Reflect.get(message, "uuid") === FLUSH
                        ? Queue.offer(flushed, undefined).pipe(Effect.asVoid)
                        : Effect.void,
                    ),
                    Stream.ensuring(Deferred.succeed(streamExit, undefined)),
                  ),
                  offer: (message) =>
                    Effect.gen(function* () {
                      prompts.push(String(message.message.content));
                      if (
                        prompts.length === 2 &&
                        (scenario === "in-flight" || scenario === "failed-offer")
                      ) {
                        yield* Deferred.succeed(reached, undefined);
                        yield* Deferred.await(release);
                        if (scenario === "failed-offer")
                          return yield* new ClaudeAgentSdkQueryRunnerError({
                            method: "offer",
                            cause: new Error("offer rejected"),
                          });
                      }
                    }),
                  setModel: () => Effect.die("live query model must remain fixed"),
                  setPermissionMode: () => Effect.void,
                  interrupt: Effect.void,
                  close: Effect.sync(() => {
                    closes++;
                  }).pipe(Effect.andThen(Queue.shutdown(sdk))),
                };
              }),
            forkSession: () => Effect.die("unused fork"),
            subagentLaunchToolUseId: () => Effect.succeed(null),
            assertComplete: Effect.void,
          },
        });
      }).pipe(Effect.provide(Layer.merge(idAllocatorLayer, NodeServices.layer)));
      yield* Effect.gen(function* () {
        const orchestrator = yield* OrchestratorV2;
        const worker = yield* OrchestrationEffectWorkerV2;
        const manager = yield* ProviderSessionManagerV2;
        const projection = orchestrator.getThreadProjection(threadId);
        const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
          orchestrator.streamDomainEvents.pipe(
            Stream.filter(predicate),
            Stream.runHead,
            Effect.forkChild({ startImmediately: true }),
          );
        const emit = (...messages: SDKMessage[]) =>
          Queue.offerAll(sdk, [
            ...messages,
            frame({
              type: "system",
              subtype: "status",
              status: null,
              uuid: FLUSH,
              session_id: NATIVE,
            }),
          ]).pipe(Effect.andThen(Queue.take(flushed)));
        const send = (name: string, selection = producingSelection) =>
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
            modelSelection: selection,
          });
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make("create"),
          threadId,
          projectId: ProjectId.make("project:selection-recovery"),
          title: "Selection recovery",
          modelSelection: producingSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
          createdBy: "user",
          creationSource: "web",
        });
        yield* send("first");
        yield* worker.drain();
        const settled = yield* watch(
          (event) =>
            event.type === "run.updated" &&
            (event.payload.status === "waiting" || event.payload.status === "completed"),
        );
        yield* emit(started, result(2, "Kicked off."));
        yield* Fiber.join(settled);
        yield* worker.drain();
        const binding = (yield* projection).providerThreads[0]!;
        const runtime = Option.getOrThrow(yield* manager.get(binding.providerSessionId!));
        if (scenario === "revert" || scenario === "refused-wake") {
          const failed = yield* watch(
            (event) => event.type === "run.updated" && event.payload.status === "failed",
          );
          yield* send("change", B);
          yield* worker.drain();
          yield* Fiber.join(failed);
          assert.deepEqual(
            yield* runtime.executionSelection!(binding),
            A,
            "clean refusal preserves the intact live query selection",
          );
          assert.equal(closes, 0);
          if (scenario === "revert") {
            assert.isTrue(Exit.isSuccess(yield* Effect.exit(send("revert", A))));
            yield* worker.drain();
            assert.lengthOf(prompts, 2);
            assert.equal(opens, 1);
            return;
          }
        }
        yield* emit(finished);
        assert.isFalse(yield* runtime.hasPendingBackgroundWorkForThread!(binding));
        if (scenario === "notification-only") {
          assert.isFalse(
            yield* runtime.hasBufferedOutputForThread!(binding),
            "opaque notification-only traffic has no native output to protect",
          );
          assert.isTrue(Exit.isSuccess(yield* Effect.exit(send("replace", B))));
          yield* worker.drain();
          assert.equal(opens, 2);
          assert.equal(closes, 1);
          assert.lengthOf(prompts, 2);
          return;
        }
        yield* emit(assistant, result(5, "Wake complete", true));
        const offered = yield* Queue.take(offers);
        if (scenario === "idle-exit") {
          yield* Queue.end(sdk);
          yield* Deferred.await(streamExit);
          yield* Effect.yieldNow;
          assert.isNull(yield* runtime.liveExecutionSelection?.(binding) ?? Effect.succeed(null));
          assert.deepEqual(
            yield* runtime.bufferedExecutionSelection?.(binding) ?? Effect.succeed(null),
            producingSelection,
          );
        }
        if (scenario === "saved-selection") {
          yield* orchestrator.dispatch({
            type: "thread.model-selection.set",
            commandId: CommandId.make("save"),
            threadId,
            modelSelection: B,
          });
        }
        const requests = yield* Queue.unbounded<ProviderContinuationRequest>();
        const dispatched = yield* Queue.unbounded<boolean>();
        yield* Layer.build(
          workerLive.pipe(
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
        const delivered = yield* watch(
          (event) =>
            event.type === "turn-item.updated" &&
            event.payload.type === "assistant_message" &&
            event.payload.text === "The build has finished.",
        );
        if (scenario === "in-flight" || scenario === "failed-offer") {
          yield* send("second");
          const drain = yield* worker.drain().pipe(Effect.forkChild({ startImmediately: true }));
          yield* Deferred.await(reached);
          yield* Queue.offer(requests, offered);
          const admitted = yield* Queue.take(dispatched);
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(drain);
          assert.isTrue(admitted, "a buffered wake queues behind pending acknowledgment");
          if (scenario === "failed-offer") {
            // A provider failure holds the queue until the user resumes it.
            assert.isTrue((yield* projection).runs.at(-1)?.queueHeld);
            yield* orchestrator.dispatch({
              type: "queue.resume",
              commandId: CommandId.make("resume"),
              threadId,
            });
          }
          if (scenario === "in-flight") {
            const done = yield* watch(
              (event) =>
                event.type === "run.updated" &&
                event.payload.ordinal === 2 &&
                (event.payload.status === "waiting" || event.payload.status === "completed"),
            );
            yield* emit(result(8, "Second complete"));
            yield* Fiber.join(done);
          }
        } else {
          yield* Queue.offer(requests, offered);
          assert.isTrue(yield* Queue.take(dispatched));
        }
        yield* worker.drain();
        assert.notEqual((yield* projection).runs.at(-1)?.status, "failed");
        yield* Fiber.join(delivered);
        yield* worker.drain();
        const after = yield* projection;
        const wakeRun = after.runs.at(-1)!;
        assert.deepEqual(wakeRun.modelSelection, producingSelection);
        assert.isFalse(yield* runtime.hasBufferedOutputForThread!(binding));
        assert.equal(
          opens,
          1,
          "draining never replaces the producing query, even after its idle exit",
        );
        assert.equal(closes, 0);
        assert.lengthOf(prompts, scenario === "in-flight" || scenario === "failed-offer" ? 2 : 1);
        if (scenario === "failed-offer") {
          assert.equal(after.runs[1]?.status, "failed");
          assert.deepEqual(yield* runtime.executionSelection!(binding), ULTRA);
          assert.isTrue(prompts.every((prompt) => prompt.startsWith("Ultrathink:")));
        }
        if (scenario === "saved-selection") assert.deepEqual(after.thread.modelSelection, B);
      }).pipe(
        Effect.provide(
          makeOrchestratorV2ReplayLayerWithRegistry(
            { name: `selection-recovery-${scenario}` },
            makeSingleLayer(adapter),
            { runEffectWorker: false },
          ),
        ),
      );
    }),
  ),
);
