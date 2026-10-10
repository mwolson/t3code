import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as Crypto from "effect/Crypto";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
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

import { OrchestratorV2 } from "./Orchestrator.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import type { ProviderContinuationRequest } from "@t3tools/provider-core/server/ProviderContinuationRequests";
import { ProviderSessionManagerV2 } from "./ProviderSessionManager.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";

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
type NotificationScenario =
  | "opaque-notification-only"
  | "failed-later-turn"
  | "known-subagent-blocks"
  | "known-subagent-later-turn"
  | "replacement-query-later-turn";

const selectionNotification = (scenario: NotificationScenario) =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace(`selection-notification-buffer-${scenario}`);
      const sdk = yield* Queue.unbounded<SDKMessage>();
      let currentSdk = sdk;
      const offers = yield* Queue.unbounded<ProviderContinuationRequest>();
      const threadId = ThreadId.make(`thread:selection-notification-buffer:${scenario}`);
      let opens = 0;
      let closes = 0;
      let prompts = 0;
      const adapter = yield* Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        return yield* makeClaudeAdapterV2({
          crypto: yield* Crypto.Crypto,
          instanceId: CLAUDE_DEFAULT_INSTANCE_ID,
          settings,
          environment: {},
          fileSystem,
          attachmentsDir: yield* fileSystem.makeTempDirectoryScoped({
            prefix: "selection-notification-buffer-",
          }),
          path: yield* Path.Path,
          idAllocator: yield* IdAllocator.IdAllocatorV2,
          continuationRequests: {
            offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
          },
          queryRunner: {
            allocateSessionId: Effect.succeed(NATIVE),
            open: () =>
              Effect.gen(function* () {
                const messages = opens === 0 ? sdk : yield* Queue.unbounded<SDKMessage>();
                opens++;
                currentSdk = messages;
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
      }).pipe(
        Effect.provide(
          Layer.mergeAll(IdAllocator.layer, McpProviderSessions.layer, NodeServices.layer),
        ),
      );
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
          projectId: ProjectId.make("project:selection-notification-buffer"),
          title: "Review2 buffer",
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
        const knownSubagent = scenario.startsWith("known-subagent");
        yield* Queue.offer(
          sdk,
          knownSubagent ? frame({ ...started, task_type: "local_agent" }) : started,
        );
        yield* Queue.offer(sdk, result(2, "Kicked off."));
        yield* Fiber.join(firstTerminal);
        yield* worker.drain();
        const binding = (yield* projection).providerThreads[0]!;
        const runtime = Option.getOrThrow(yield* manager.get(binding.providerSessionId!));
        // The task finishes; Claude clears the roster but never dequeues the
        // notification into a native wake turn (no assistant/user/result).
        yield* Queue.offer(sdk, finished);
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
        yield* worker.drain();
        assert.equal(yield* runtime.hasBufferedOutputForThread!(binding), knownSubagent);
        assert.equal(yield* Queue.size(offers), knownSubagent ? 1 : 0);
        assert.isNotNull(yield* runtime.bufferedExecutionSelection!(binding));
        if (scenario === "known-subagent-blocks") {
          const change = yield* Effect.exit(send("change", B));
          yield* worker.drain();
          assert.isTrue(
            change._tag === "Failure" || (yield* projection).runs.at(-1)?.status === "failed",
          );
          assert.equal(opens, 1, "known-subagent output prevents query replacement");
          assert.equal(closes, 0);
          assert.isTrue(yield* runtime.hasBufferedOutputForThread!(binding));
          return;
        }
        const same = yield* Effect.exit(
          send("same", scenario === "replacement-query-later-turn" ? B : A),
        );
        yield* worker.drain();
        yield* Queue.offer(
          currentSdk,
          frame({
            type: "assistant",
            message: {
              role: "assistant",
              content: [{ type: "text", text: "Answering the user." }],
            },
            parent_tool_use_id: null,
            uuid: "00000000-0000-4000-8000-000000000020",
            session_id: NATIVE,
          }),
        );
        const sameCompleted = yield* watch(
          (event) =>
            event.type === "run.updated" &&
            event.payload.ordinal === 2 &&
            ["completed", "failed", "waiting"].includes(event.payload.status),
        );
        const completion = result(8, "Answered with the build result.");
        yield* Queue.offer(
          currentSdk,
          scenario === "failed-later-turn"
            ? frame({
                ...completion,
                subtype: "error_during_execution",
                is_error: true,
                errors: ["turn failed"],
              })
            : completion,
        );
        yield* Fiber.join(sameCompleted);
        yield* worker.drain();
        assert.equal(same._tag, "Success");
        if (scenario !== "opaque-notification-only") {
          if (scenario === "failed-later-turn") {
            assert.equal((yield* projection).runs.at(-1)?.status, "failed");
          }
          const bufferedSelection = yield* runtime.bufferedExecutionSelection!(binding);
          if (scenario === "replacement-query-later-turn") {
            assert.isNull(
              bufferedSelection,
              "query replacement discards old notification evidence",
            );
          } else {
            assert.isNotNull(
              bufferedSelection,
              "a failed turn or undelivered subagent output preserves its producing buffer",
            );
          }
          assert.equal(opens, scenario === "replacement-query-later-turn" ? 2 : 1);
          assert.equal(yield* runtime.hasBufferedOutputForThread!(binding), knownSubagent);
          assert.equal(yield* Queue.size(offers), knownSubagent ? 1 : 0);
          return;
        }
        assert.isNull(
          yield* runtime.bufferedExecutionSelection!(binding),
          "later same-query completion releases notification replay state",
        );
        assert.equal(yield* Queue.size(offers), 0);
        const changed = yield* watch(
          (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
        );
        const change = yield* Effect.exit(send("change", B));
        assert.equal(change._tag, "Success");
        yield* worker.drain();
        yield* Fiber.join(changed);
        assert.equal(opens, 2);
        assert.equal(closes, 1);
        assert.equal(prompts, 3);
        assert.equal(
          change._tag,
          "Success",
          "a later selection change is not blocked forever by a notification-only buffer",
        );
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry(
            { name: `selection-notification-buffer-${scenario}` },
            ProviderAdapterRegistry.layerSingle(adapter),
          ),
        ),
      );
    }),
  );

it.effect("real Claude selection notification ownership: opaque-notification-only", () =>
  selectionNotification("opaque-notification-only"),
);
it.effect("real Claude selection notification ownership: failed-later-turn", () =>
  selectionNotification("failed-later-turn"),
);
it.effect("real Claude selection notification ownership: known-subagent-blocks", () =>
  selectionNotification("known-subagent-blocks"),
);
it.effect("real Claude selection notification ownership: known-subagent-later-turn", () =>
  selectionNotification("known-subagent-later-turn"),
);
it.effect("real Claude selection notification ownership: replacement-query-later-turn", () =>
  selectionNotification("replacement-query-later-turn"),
);
