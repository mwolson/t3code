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
import { CLAUDE_DEFAULT_INSTANCE_ID, makeClaudeAdapterV2 } from "./Adapters/ClaudeAdapterV2.ts";
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

const NATIVE = "native-buffer-policy-claude";
const A: ModelSelection = {
  instanceId: CLAUDE_DEFAULT_INSTANCE_ID,
  model: "claude-sonnet-4-6",
  options: [{ id: "effort", value: "high" }],
};
const B: ModelSelection = { ...A, options: [{ id: "effort", value: "low" }] };
const settings = Schema.decodeSync(ClaudeSettings)({});
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
  description: "npm run build",
  task_type: "local_bash",
  uuid: "00000000-0000-4000-8000-000000000001",
  session_id: NATIVE,
});
const finished = frame({
  type: "system",
  subtype: "task_notification",
  task_id: "build",
  status: "completed",
  output_file: "/tmp/build.log",
  summary: "Build finished",
  uuid: "00000000-0000-4000-8000-000000000003",
  session_id: NATIVE,
});
const assistant = frame({
  type: "assistant",
  message: { role: "assistant", content: [{ type: "text", text: "The build has finished." }] },
  parent_tool_use_id: null,
  uuid: "00000000-0000-4000-8000-000000000004",
  session_id: NATIVE,
});

it.effect.each(
  (() => {
    const cases = [];
    for (const scenario of [
      "idle-wake",
      "queued-wake",
      "running-replacement",
      "finish-recovery",
      "stop-recovery",
      "subagent-stop",
      "uncertain-buffer",
    ] as const) {
      cases.push({ scenario });
    }
    return cases;
  })(),
)("real Claude buffer policy: $scenario", ({ scenario }) =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace(`buffer-policy-${scenario}`);
      const sdk = yield* Queue.unbounded<SDKMessage>();
      const offers = yield* Queue.unbounded<ProviderContinuationRequest>();
      const threadId = ThreadId.make(`thread:buffer-policy:${scenario}`);
      let opens = 0;
      let closes = 0;
      let prompts = 0;
      const adapter = yield* Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        return makeClaudeAdapterV2({
          instanceId: CLAUDE_DEFAULT_INSTANCE_ID,
          settings,
          environment: {},
          fileSystem,
          attachmentsDir: yield* fileSystem.makeTempDirectoryScoped({
            prefix: "buffer-policy-claude-",
          }),
          path: yield* Path.Path,
          idAllocator: yield* IdAllocatorV2,
          continuationRequests: {
            offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
          },
          queryRunner: {
            allocateSessionId: Effect.succeed(NATIVE),
            open: () =>
              Effect.gen(function* () {
                const messages = opens === 0 ? sdk : yield* Queue.unbounded<SDKMessage>();
                opens++;
                return {
                  messages: Stream.fromQueue(messages),
                  offer: () =>
                    Effect.sync(() => {
                      prompts++;
                    }),
                  setModel: () => Effect.void,
                  setPermissionMode: () => Effect.void,
                  interrupt: Effect.void,
                  close: Queue.shutdown(messages).pipe(
                    Effect.andThen(
                      Effect.sync(() => {
                        closes++;
                      }),
                    ),
                  ),
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
            ...(modelSelection === undefined ? {} : { modelSelection }),
          });
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make("create"),
          threadId,
          projectId: ProjectId.make("project:buffer-policy"),
          title: "Buffer policy",
          modelSelection: A,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
          createdBy: "user",
          creationSource: "web",
        });
        const firstStarted = yield* watch(
          (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
        );
        yield* send("first");
        yield* worker.drain();
        yield* Fiber.join(firstStarted);
        const firstTerminal = yield* watch(
          (event) =>
            event.type === "run.updated" &&
            (event.payload.status === "waiting" || event.payload.status === "completed"),
        );
        yield* Queue.offer(
          sdk,
          scenario === "subagent-stop" ? frame({ ...started, task_type: "local_agent" }) : started,
        );
        yield* Queue.offer(sdk, result(2, "Kicked off."));
        yield* Fiber.join(firstTerminal);
        yield* worker.drain();
        const binding = (yield* projection).providerThreads[0]!;
        const runtime = Option.getOrThrow(yield* manager.get(binding.providerSessionId!));
        assert.isTrue(yield* runtime.hasRunningBackgroundWorkForThread!(binding));
        assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
        if (scenario !== "idle-wake" && scenario !== "queued-wake") {
          const failed = yield* watch(
            (event) => event.type === "run.updated" && event.payload.status === "failed",
          );
          const dispatch = yield* Effect.exit(send("change", B));
          // The command may save future settings; the actual query boundary owns the veto.
          assert.equal(dispatch._tag, "Success");
          yield* worker.drain();
          yield* Fiber.join(failed);
          assert.equal(opens, 1);
          assert.equal(closes, 0);
          assert.equal(prompts, 1);
          assert.isTrue(yield* runtime.hasRunningBackgroundWorkForThread!(binding));
          assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
          const current = yield* projection;
          assert.equal(current.runs[1]?.status, "failed");
          assert.deepEqual(current.thread.modelSelection, B);
          const errors = current.turnItems.filter((item) => item.type === "error");
          assert.isTrue(
            errors.some((item) => item.failure?.message.includes("running background") === true),
          );
          assert.deepEqual(yield* runtime.executionSelection!(binding), A);
          assert.equal(
            (yield* projection).runs.length,
            2,
            "no implicit retry of the failed prompt",
          );
          if (scenario === "running-replacement") return;
          if (scenario === "uncertain-buffer") {
            yield* Queue.offer(sdk, finished);
            yield* Queue.offer(sdk, assistant);
            yield* Queue.offer(sdk, result(7, "Finished after refusal", true));
            const offered = yield* Queue.take(offers);
            const requests = yield* Queue.unbounded<ProviderContinuationRequest>();
            const dispatched = yield* Queue.unbounded<boolean>();
            yield* Layer.build(
              ProviderContinuationService.layer.pipe(
                EffectLayer(orchestrator, requests, dispatched),
              ),
            );
            const delivered = yield* watch(
              (event) =>
                event.type === "turn-item.updated" &&
                event.payload.type === "assistant_message" &&
                event.payload.text === "The build has finished.",
            );
            yield* Queue.offer(requests, offered);
            assert.isTrue(
              yield* Queue.take(dispatched),
              "a refused replacement leaves the producing query known",
            );
            yield* worker.drain();
            assert.notEqual((yield* projection).runs.at(-1)?.status, "failed");
            yield* Fiber.join(delivered);
            assert.isFalse(yield* runtime.hasBufferedOutputForThread!(binding));
            assert.equal(closes, 0);
            assert.equal(prompts, 1);
            return;
          }
          if (scenario === "stop-recovery" || scenario === "subagent-stop") {
            const stoppedSubagent =
              scenario === "subagent-stop"
                ? yield* watch(
                    (event) =>
                      event.type === "subagent.updated" && event.payload.status === "cancelled",
                  )
                : undefined;
            yield* orchestrator.dispatch({
              type: "run.interrupt",
              commandId: CommandId.make("explicit-stop"),
              threadId,
              runId: current.runs[1]!.id,
            });
            yield* worker.drain();
            assert.equal(closes, 1, "explicit Stop closes the blocked owner");
            assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
            if (stoppedSubagent !== undefined) yield* Fiber.join(stoppedSubagent);
            if (scenario === "subagent-stop") {
              const tasks = (yield* projection).subagents;
              assert.lengthOf(tasks, 1);
              assert.equal(tasks[0]?.status, "cancelled");
              assert.equal(tasks[0]?.id, current.subagents[0]?.id);
              assert.equal(tasks[0]?.runId, current.subagents[0]?.runId);
            }
          } else {
            const subscription = yield* runtime.subscribeEvents!;
            const cleared = yield* subscription.events.pipe(
              Stream.filter(
                (event) =>
                  event.type === "provider_thread.updated" &&
                  event.providerThread.pendingBackgroundTasks?.length === 0,
              ),
              Stream.runHead,
              Effect.forkChild({ startImmediately: true }),
            );
            yield* Queue.offer(
              sdk,
              frame({
                type: "system",
                subtype: "background_tasks_changed",
                tasks: [],
                uuid: "00000000-0000-4000-8000-000000000010",
                session_id: NATIVE,
              }),
            );
            yield* Fiber.join(cleared);
            assert.isFalse(yield* runtime.hasRunningBackgroundWorkForThread!(binding));
            assert.equal(closes, 0, "task finishes in the original process");
          }
          const recovered = yield* watch(
            (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
          );
          yield* send("explicit-recovery", B);
          yield* worker.drain();
          yield* Fiber.join(recovered);
          assert.equal(opens, 2);
          assert.equal(prompts, 2, "only the explicit retry is sent");
          const after = yield* projection;
          assert.lengthOf(after.runs, 3);
          assert.equal(after.runs[1]?.status, "failed");
          assert.equal(after.messages.filter((message) => message.text === "change").length, 1);
          assert.equal(after.runs.at(-1)?.status, "running");
          return;
        }
        yield* Queue.offer(sdk, finished);
        yield* Queue.offer(sdk, assistant);
        yield* Queue.offer(sdk, result(5, "Wake complete", true));
        const offered = yield* Queue.take(offers);
        assert.isFalse(yield* runtime.hasRunningBackgroundWorkForThread!(binding));
        assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
        if (scenario === "queued-wake") {
          const active = yield* watch(
            (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
          );
          yield* send("intervening");
          yield* worker.drain();
          yield* Fiber.join(active);
        }
        const requests = yield* Queue.unbounded<ProviderContinuationRequest>();
        const dispatched = yield* Queue.unbounded<boolean>();
        yield* Layer.build(
          ProviderContinuationService.layer.pipe(EffectLayer(orchestrator, requests, dispatched)),
        );
        yield* Queue.offer(requests, offered);
        assert.isTrue(
          yield* Queue.take(dispatched),
          "real continuation worker admits completed wake",
        );
        const delivered = yield* watch(
          (event) =>
            event.type === "turn-item.updated" &&
            event.payload.type === "assistant_message" &&
            event.payload.text === "The build has finished.",
        );
        if (scenario === "queued-wake") {
          assert.equal((yield* projection).runs.at(-1)?.status, "queued");
          const terminal = yield* watch(
            (event) =>
              event.type === "run.updated" &&
              event.payload.status === "waiting" &&
              event.payload.ordinal === 2,
          );
          yield* Queue.offer(sdk, result(6, "Intervening complete"));
          yield* Fiber.join(terminal);
        }
        yield* worker.drain();
        assert.notEqual(
          (yield* projection).runs.at(-1)?.status,
          "failed",
          "completed wake starts successfully",
        );
        yield* Fiber.join(delivered);
        assert.equal(opens, 1);
        assert.equal(closes, 0);
        assert.equal(
          prompts,
          scenario === "queued-wake" ? 2 : 1,
          "drain never resends the continuation prompt",
        );
        assert.isFalse(
          yield* runtime.hasBufferedOutputForThread?.(binding) ?? Effect.succeed(true),
        );
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry(
            { name: `buffer-policy-${scenario}` },
            ProviderAdapterRegistry.layerSingle(adapter),
            { runEffectWorker: false },
          ),
        ),
      );
    }),
  ),
);

function EffectLayer(
  orchestrator: OrchestratorV2["Service"],
  requests: Queue.Queue<ProviderContinuationRequest>,
  dispatched: Queue.Queue<boolean>,
) {
  return Layer.provide(
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
  );
}
