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
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import {
  ProviderContinuationRequests,
  type ProviderContinuationRequest,
} from "./ProviderContinuationRequests.ts";
import * as ProviderContinuationService from "./ProviderContinuationService.ts";
import { ProviderSessionManagerV2 } from "./ProviderSessionManager.ts";
import { ProviderSwitchPlanError } from "./ProviderSwitchService.ts";
import { ThreadManagementService } from "./ThreadManagementService.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
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

type RecoveryScenario =
  | "revert"
  | "refused-wake"
  | "in-flight"
  | "failed-offer"
  | "idle-exit"
  | "notification-only"
  | "saved-selection"
  | "init-wake"
  | "active-init-wake"
  | "init-first-wake"
  | "recovered-wake"
  | "continuation-init-wake"
  | "buffered-subagent"
  | "persistent-failure"
  | "archived-wake"
  | "archived-unarchive"
  | "dead-producer"
  | "active-stop-wake"
  | "replaced-producer"
  | "double-init-wake";

const selectionRecovery = (scenario: RecoveryScenario) =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace(`selection-recovery-${scenario}`);
      let sdk = yield* Queue.unbounded<SDKMessage, Cause.Done>();
      const flushed = yield* Queue.unbounded<void>();
      const offers = yield* Queue.unbounded<ProviderContinuationRequest>();
      const reached = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const streamExit = yield* Deferred.make<void>();
      const threadId = ThreadId.make(`thread:selection-recovery:${scenario}`);
      const producingSelection =
        scenario === "failed-offer" || scenario === "idle-exit" ? ULTRA : A;
      const thinkingWake =
        scenario === "init-wake" ||
        scenario === "active-init-wake" ||
        scenario === "init-first-wake" ||
        scenario === "recovered-wake" ||
        scenario === "continuation-init-wake" ||
        scenario === "persistent-failure" ||
        scenario === "archived-wake" ||
        scenario === "archived-unarchive" ||
        scenario === "dead-producer" ||
        scenario === "active-stop-wake" ||
        scenario === "double-init-wake";
      let withholdBufferedSelection = false;
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
                const processSdk = sdk;
                return {
                  messages: Stream.fromQueue(processSdk).pipe(
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
                  }).pipe(Effect.andThen(Queue.shutdown(processSdk))),
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
        yield* emit(
          ...(scenario === "buffered-subagent" ? [] : [started]),
          ...(scenario === "continuation-init-wake"
            ? [frame({ ...started, task_id: "second-build", uuid: "second-started" })]
            : []),
          result(2, "Kicked off."),
        );
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
        if (scenario === "active-init-wake" || scenario === "replaced-producer") {
          yield* send("intervening-user");
          yield* worker.drain();
          yield* emit(finished);
          assert.equal(yield* Queue.size(offers), 0, "live completion does not offer a wake");
          assert.isFalse(yield* runtime.hasBufferedOutputForThread!(binding));
          const interveningDone = yield* watch(
            (event) =>
              event.type === "run.updated" &&
              event.payload.ordinal === 2 &&
              (event.payload.status === "waiting" || event.payload.status === "completed"),
          );
          yield* emit(result(3, "Intervening user complete"));
          yield* Fiber.join(interveningDone);
          yield* worker.drain();
          assert.lengthOf((yield* projection).runs, 2);
          if (scenario === "replaced-producer") {
            yield* emit(
              frame({
                type: "rate_limit_event",
                rate_limit_info: { status: "allowed", rateLimitType: "five_hour" },
                session_id: NATIVE,
                uuid: "old-query-rate-limit",
              }),
            );
            assert.isFalse(yield* runtime.hasBufferedOutputForThread!(binding));
            sdk = yield* Queue.unbounded<SDKMessage, Cause.Done>();
            yield* send("replace", B);
            yield* worker.drain();
            assert.equal(opens, 2);
            assert.equal(closes, 1);
            const replacementDone = yield* watch(
              (event) =>
                event.type === "run.updated" &&
                event.payload.ordinal === 3 &&
                (event.payload.status === "waiting" || event.payload.status === "completed"),
            );
            yield* emit(result(4, "Replacement user complete"));
            yield* Fiber.join(replacementDone);
            yield* worker.drain();
            yield* emit(
              frame({
                type: "system",
                subtype: "init",
                session_id: NATIVE,
                uuid: "replacement-init",
              }),
            );
            assert.isFalse(
              yield* runtime.hasBufferedOutputForThread!(binding),
              "old producer evidence cannot own output from its replacement",
            );
            assert.equal(yield* Queue.size(offers), 0);
            assert.deepEqual(yield* runtime.liveExecutionSelection!(binding), B);
            return;
          }
        } else {
          if (scenario === "buffered-subagent") {
            yield* emit(
              frame({
                ...started,
                task_id: "late-subagent",
                task_type: "local_agent",
                is_backgrounded: true,
                uuid: "late-started",
              }),
            );
            assert.equal(yield* Queue.size(offers), 0);
            yield* emit(frame({ ...finished, task_id: "late-subagent", uuid: "late-finished" }));
            assert.isTrue(
              yield* runtime.hasBufferedOutputForThread!(binding),
              "buffered subagent start owns its immediate completion offer",
            );
            assert.equal(yield* Queue.size(offers), 1);
          } else if (scenario === "init-first-wake") {
            yield* emit(
              frame({ type: "system", subtype: "init", session_id: NATIVE, uuid: "early-init" }),
            );
            assert.equal(
              yield* Queue.size(offers),
              0,
              "init waits for tracked completion evidence",
            );
            assert.isFalse(yield* runtime.hasBufferedOutputForThread!(binding));
          }
          if (scenario !== "buffered-subagent") yield* emit(finished);
        }
        assert.equal(
          yield* runtime.hasPendingBackgroundWorkForThread!(binding),
          scenario === "continuation-init-wake",
        );
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
        if (thinkingWake) {
          if (scenario !== "init-first-wake") {
            assert.isFalse(yield* runtime.hasBufferedOutputForThread!(binding));
            assert.equal(
              yield* Queue.size(offers),
              0,
              "a notification alone cannot request a turn",
            );
            assert.lengthOf(
              (yield* projection).runs,
              scenario === "active-init-wake" ? 2 : 1,
              "completion alone cannot create a turn",
            );
            yield* emit(
              frame({ type: "system", subtype: "init", session_id: NATIVE, uuid: "wake-init" }),
            );
          }
          assert.isTrue(
            yield* runtime.hasBufferedOutputForThread!(binding),
            "tracked completion plus init owns the wake before model output",
          );
          if (scenario === "init-wake") {
            const sequence = yield* orchestrator.getThreadEventSequence(threadId);
            withholdBufferedSelection = true;
            const mismatch = yield* orchestrator
              .dispatch({
                type: "message.dispatch",
                commandId: CommandId.make("mismatched-wake"),
                threadId,
                messageId: MessageId.make("message:mismatched-wake"),
                text: "Build finished",
                notification: {
                  source: { kind: "command" },
                  outcome: "completed",
                  summary: "Build finished",
                },
                attachments: [],
                dispatchMode: { type: "start_immediately" },
                createdBy: "agent",
                creationSource: "provider",
                modelSelection: B,
              })
              .pipe(Effect.flip);
            assert.equal(mismatch._tag, "OrchestratorDispatchError");
            withholdBufferedSelection = false;
            assert.equal(
              mismatch.cause,
              "Buffered provider output has no trustworthy execution selection.",
            );
            assert.equal(yield* orchestrator.getThreadEventSequence(threadId), sequence);
            assert.isTrue(yield* runtime.hasBufferedOutputForThread!(binding));
            const replacement = yield* Effect.exit(send("replace-thinking-query", B));
            assert.isTrue(
              Exit.isFailure(replacement),
              "a proven wake protects its producing query",
            );
            assert.equal(opens, 1);
            assert.equal(closes, 0);
          }
        } else if (scenario !== "buffered-subagent") {
          yield* emit(assistant, result(5, "Wake complete", true));
        }
        if (scenario === "double-init-wake") {
          yield* emit(
            frame({
              type: "system",
              subtype: "init",
              session_id: NATIVE,
              uuid: "unrelated-buffered-init",
            }),
          );
          assert.equal(yield* Queue.size(offers), 1, "a second root init shares the pending offer");
        }
        if (scenario === "dead-producer") {
          yield* emit(
            frame({
              ...assistant,
              message: { role: "assistant", content: [{ type: "text", text: "Dropped wake" }] },
              uuid: "dead-producer-output",
            }),
          );
        }
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
                      .dispatch(
                        scenario === "init-wake" && command.type === "message.dispatch"
                          ? { ...command, modelSelection: B }
                          : command,
                      )
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
        const recoveryDispatchDone = yield* Deferred.make<void>();
        const recoveryRequest = {
          ...offered,
          dispatchIfCurrent: <Value, Error, Requirements>(
            effect: Effect.Effect<Value, Error, Requirements>,
          ) =>
            (offered.dispatchIfCurrent === undefined
              ? effect.pipe(Effect.map(Option.some))
              : offered.dispatchIfCurrent(effect)
            ).pipe(
              Effect.ensuring(
                offered.clearIfCurrent === undefined
                  ? Deferred.succeed(recoveryDispatchDone, undefined)
                  : Effect.void,
              ),
            ),
          clearIfCurrent: (options?: { readonly suppressReofferWhile?: Effect.Effect<boolean> }) =>
            (offered.clearIfCurrent?.(options) ?? Effect.void).pipe(
              Effect.ensuring(Deferred.succeed(recoveryDispatchDone, undefined)),
            ),
        };
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
        } else if (scenario === "active-stop-wake") {
          yield* send("intervening-before-stop");
          yield* worker.drain();
          yield* Queue.offer(requests, offered);
          assert.isTrue(yield* Queue.take(dispatched));
          const resumedDone = yield* watch(
            (event) =>
              event.type === "run.updated" &&
              event.payload.ordinal === 3 &&
              (event.payload.status === "completed" || event.payload.status === "failed"),
          );
          const userRun = (yield* projection).runs[1]!;
          yield* orchestrator.dispatch({
            type: "run.interrupt",
            commandId: CommandId.make("active-stop"),
            threadId,
            runId: userRun.id,
          });
          yield* worker.drain();
          assert.equal(closes, 1);
          assert.isFalse(
            yield* runtime.hasBufferedOutputForThread!(binding),
            "mid-turn Stop discards pending wake state",
          );
          assert.isNull(yield* runtime.bufferedExecutionSelection!(binding));
          let staleRan = false;
          assert.isTrue(
            Option.isNone(
              yield* offered.dispatchIfCurrent!(
                Effect.sync(() => {
                  staleRan = true;
                }),
              ),
            ),
          );
          assert.isFalse(staleRan);
          yield* orchestrator.dispatch({
            type: "queue.resume",
            commandId: CommandId.make("resume-stopped-wake"),
            threadId,
          });
          yield* worker.drain();
          yield* Fiber.join(resumedDone);
          yield* worker.drain();
          assert.isFalse(
            (yield* projection).runs.some((run) => run.status === "running"),
            "resuming cannot attach to the stopped producer",
          );
          return;
        } else if (scenario === "dead-producer") {
          yield* Queue.offer(requests, offered);
          assert.isTrue(yield* Queue.take(dispatched));
          yield* Queue.end(sdk);
          yield* Deferred.await(streamExit);
          yield* Effect.yieldNow;
          assert.isNull(yield* runtime.liveExecutionSelection!(binding));
          const refused = yield* watch(
            (event) =>
              event.type === "run.updated" &&
              event.payload.ordinal === 2 &&
              event.payload.status === "failed",
          );
          yield* worker.drain();
          yield* Fiber.join(refused);
          yield* worker.drain();
          assert.isFalse(
            yield* runtime.hasPendingBackgroundWork!,
            "dead-producer refusal drops the result-less wake buffer",
          );
          assert.isNull(yield* runtime.bufferedExecutionSelection!(binding));
          assert.isTrue(
            Option.isNone(yield* offered.dispatchIfCurrent!(Effect.void)),
            "dead-producer refusal releases its offer slot",
          );
          sdk = yield* Queue.unbounded<SDKMessage, Cause.Done>();
          yield* send("recover-live-producer");
          yield* worker.drain();
          assert.equal(opens, 2);
          const recovered = yield* watch(
            (event) =>
              event.type === "run.updated" &&
              event.payload.ordinal === 3 &&
              (event.payload.status === "waiting" || event.payload.status === "completed"),
          );
          yield* emit(
            frame({ ...started, task_id: "recovered-build", uuid: "recovered-started" }),
            result(9, "Recovered producer complete"),
          );
          yield* Fiber.join(recovered);
          yield* worker.drain();
          yield* emit(
            frame({ ...finished, task_id: "recovered-build", uuid: "recovered-finished" }),
            frame({ type: "system", subtype: "init", session_id: NATIVE, uuid: "recovered-init" }),
          );
          assert.equal(yield* Queue.size(offers), 1, "a live producer can offer a later wake");
          const later = yield* Queue.take(offers);
          yield* Queue.offer(requests, later);
          assert.isTrue(yield* Queue.take(dispatched));
          yield* worker.drain();
          const laterDone = yield* watch(
            (event) =>
              event.type === "run.updated" &&
              event.payload.ordinal === 4 &&
              (event.payload.status === "waiting" || event.payload.status === "completed"),
          );
          yield* emit(assistant, result(10, "Recovered wake complete", true));
          yield* Fiber.join(laterDone);
          yield* worker.drain();
          const after = yield* projection;
          assert.equal(after.runs.at(-1)?.status, "completed");
          assert.isFalse(
            after.turnItems.some(
              (item) => item.type === "assistant_message" && item.text === "Dropped wake",
            ),
            "the dropped wake cannot be delivered under the new producer",
          );
          assert.lengthOf(prompts, 2);
          assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
          return;
        } else if (scenario === "archived-wake" || scenario === "archived-unarchive") {
          yield* orchestrator.dispatch({
            type: "thread.archive",
            commandId: CommandId.make("archive-wake"),
            threadId,
          });
          yield* Queue.offer(requests, recoveryRequest);
          yield* Deferred.await(recoveryDispatchDone);
          yield* emit(
            assistant,
            frame({
              type: "system",
              subtype: "init",
              session_id: NATIVE,
              uuid: "archived-new-init",
            }),
          );
          assert.equal(
            yield* Queue.size(offers),
            0,
            "archived threads never re-offer, even on new init",
          );
          assert.equal(yield* Queue.size(dispatched), 0);
          if (scenario === "archived-wake") return;
          yield* orchestrator.dispatch({
            type: "thread.unarchive",
            commandId: CommandId.make("unarchive-wake"),
            threadId,
          });
          yield* emit(
            frame({ type: "system", subtype: "init", session_id: NATIVE, uuid: "unarchived-init" }),
          );
          assert.equal(yield* Queue.size(offers), 1, "unarchiving permits later wake offers");
          const unarchivedOffer = yield* Queue.take(offers);
          assert.isTrue(Option.isSome(yield* unarchivedOffer.dispatchIfCurrent!(Effect.void)));
          return;
        } else if (scenario === "persistent-failure") {
          withholdBufferedSelection = true;
          yield* Queue.offer(requests, recoveryRequest);
          assert.isFalse(yield* Queue.take(dispatched));
          yield* Deferred.await(recoveryDispatchDone);
          yield* emit(assistant);
          assert.equal(yield* Queue.size(offers), 1, "one frame-driven retry is allowed");
          const retry = yield* Queue.take(offers);
          const retryDone = yield* Deferred.make<void>();
          yield* Queue.offer(requests, {
            ...retry,
            clearIfCurrent: () =>
              retry.clearIfCurrent!().pipe(Effect.ensuring(Deferred.succeed(retryDone, undefined))),
          });
          assert.isFalse(yield* Queue.take(dispatched));
          yield* Deferred.await(retryDone);
          yield* emit(assistant, result(5, "Still unavailable", true));
          assert.equal(yield* Queue.size(offers), 0, "further frames cannot repeat a failed retry");
          yield* emit(
            frame({
              type: "system",
              subtype: "init",
              session_id: NATIVE,
              uuid: "genuinely-new-init",
            }),
          );
          assert.equal(
            yield* Queue.size(offers),
            1,
            "a new native init renews wake retry evidence",
          );
          return;
        } else if (scenario === "recovered-wake" || scenario === "buffered-subagent") {
          const sequence = yield* orchestrator.getThreadEventSequence(threadId);
          withholdBufferedSelection = true;
          yield* Queue.offer(requests, recoveryRequest);
          assert.isFalse(yield* Queue.take(dispatched), "the real notified dispatch is refused");
          yield* Deferred.await(recoveryDispatchDone);
          assert.equal(yield* orchestrator.getThreadEventSequence(threadId), sequence);
          assert.isTrue(yield* runtime.hasBufferedOutputForThread!(binding));
          assert.equal(yield* Queue.size(offers), 0, "refusal alone does not spin a retry");
          withholdBufferedSelection = false;
          yield* emit(
            scenario === "buffered-subagent"
              ? frame({ ...finished, task_id: "late-subagent", uuid: "late-finished" })
              : assistant,
          );
          assert.equal(yield* Queue.size(offers), 1, "owned wake evidence permits one re-offer");
          const recovered = yield* Queue.take(offers);
          assert.deepEqual(
            recovered.notification,
            offered.notification,
            "recovery preserves the report",
          );
          let staleDispatchRan = false;
          const staleDispatch = yield* offered.dispatchIfCurrent!(
            Effect.sync(() => {
              staleDispatchRan = true;
            }),
          );
          assert.isTrue(
            Option.isNone(staleDispatch),
            "stale dispatch cannot act for a newer offer",
          );
          assert.isFalse(staleDispatchRan);
          yield* offered.clearIfCurrent?.() ?? Effect.void;
          yield* emit(assistant);
          assert.equal(
            yield* Queue.size(offers),
            0,
            "a stale callback cannot unlock the new offer",
          );
          yield* Queue.offer(requests, recovered);
          assert.isTrue(yield* Queue.take(dispatched));
        } else {
          yield* Queue.offer(requests, offered);
          assert.isTrue(yield* Queue.take(dispatched));
        }
        yield* worker.drain();
        if (scenario === "buffered-subagent") {
          const done = yield* watch(
            (event) =>
              event.type === "run.updated" &&
              event.payload.ordinal === 2 &&
              (event.payload.status === "waiting" || event.payload.status === "completed"),
          );
          yield* emit(assistant, result(5, "Subagent wake complete", true));
          yield* Fiber.join(done);
          yield* worker.drain();
          assert.equal(
            (yield* projection).subagents.find(
              (task) => task.nativeTaskRef?.nativeId === "late-subagent",
            )?.status,
            "completed",
          );
        }
        if (thinkingWake) {
          const wake = (yield* projection).runs.at(-1)!;
          assert.equal(wake.status, "running");
          assert.deepEqual(wake.modelSelection, A);
          const settled = yield* watch(
            (event) =>
              event.type === "run.updated" &&
              event.payload.ordinal === wake.ordinal &&
              (event.payload.status === "waiting" || event.payload.status === "completed"),
          );
          if (scenario === "continuation-init-wake") {
            yield* emit(frame({ ...finished, task_id: "second-build", uuid: "second-finished" }));
            assert.equal(
              yield* Queue.size(offers),
              0,
              "a live continuation completion waits for its next native wake",
            );
          }
          yield* emit(assistant, result(5, "Wake complete", true));
          yield* Fiber.join(settled);
        }
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
        assert.lengthOf(
          prompts,
          scenario === "in-flight" || scenario === "failed-offer" || scenario === "active-init-wake"
            ? 2
            : 1,
        );
        assert.equal(
          after.runs.filter((run) => {
            const message = after.messages.find((candidate) => candidate.id === run.userMessageId);
            return message?.createdBy === "agent" && message.creationSource === "provider";
          }).length,
          1,
          "exactly one native continuation is persisted",
        );
        if (scenario === "failed-offer") {
          assert.equal(after.runs[1]?.status, "failed");
          assert.deepEqual(yield* runtime.executionSelection!(binding), ULTRA);
          assert.isTrue(prompts.every((prompt) => prompt.startsWith("Ultrathink:")));
        }
        if (scenario === "saved-selection") assert.deepEqual(after.thread.modelSelection, B);
        if (scenario === "continuation-init-wake") {
          yield* emit(
            frame({
              type: "system",
              subtype: "init",
              session_id: NATIVE,
              uuid: "second-wake-init",
            }),
          );
          assert.isTrue(
            yield* runtime.hasBufferedOutputForThread!(binding),
            "a task consumed live in a continuation owns its next thinking wake",
          );
          assert.equal(yield* Queue.size(offers), 1);
          const second = yield* Queue.take(offers);
          assert.equal(second.notification?.summary, 'Command "build" finished');
          yield* Queue.offer(requests, second);
          assert.isTrue(yield* Queue.take(dispatched));
          yield* worker.drain();
          assert.deepEqual((yield* projection).runs.at(-1)?.modelSelection, A);
          const done = yield* watch(
            (event) =>
              event.type === "run.updated" &&
              event.payload.ordinal === 3 &&
              (event.payload.status === "waiting" || event.payload.status === "completed"),
          );
          yield* emit(
            frame({ ...assistant, uuid: "second-wake-output" }),
            result(6, "Second wake complete", true),
          );
          yield* Fiber.join(done);
          yield* worker.drain();
          assert.equal((yield* projection).runs.at(-1)?.status, "completed");
          assert.isFalse(yield* runtime.hasBufferedOutputForThread!(binding));
          assert.lengthOf(prompts, 1);
          assert.equal(opens, 1);
          return;
        }
        if (thinkingWake) {
          assert.equal(wakeRun.status, "completed");
          yield* emit(
            frame({ type: "system", subtype: "init", session_id: NATIVE, uuid: "unrelated-init" }),
          );
          assert.isFalse(yield* runtime.hasBufferedOutputForThread!(binding));
          const sequence = yield* orchestrator.getThreadEventSequence(threadId);
          const unowned = yield* orchestrator
            .dispatch({
              type: "message.dispatch",
              commandId: CommandId.make("unowned-wake"),
              threadId,
              messageId: MessageId.make("message:unowned-wake"),
              text: "Unrelated init",
              attachments: [],
              dispatchMode: { type: "start_immediately" },
              createdBy: "agent",
              creationSource: "provider",
              modelSelection: A,
            })
            .pipe(Effect.flip);
          assert.equal(unowned._tag, "OrchestratorDispatchError");
          const planError = yield* Schema.decodeUnknownEffect(ProviderSwitchPlanError)(
            unowned.cause,
          );
          assert.equal(
            planError.cause,
            "Buffered output cannot be safely admitted on this binding.",
          );
          assert.equal(yield* orchestrator.getThreadEventSequence(threadId), sequence);
          assert.lengthOf((yield* projection).runs, scenario === "active-init-wake" ? 3 : 2);
          assert.equal(yield* Queue.size(offers), 0, "unrelated init cannot consume an offer slot");
        }
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry(
            { name: `selection-recovery-${scenario}` },
            ProviderAdapterRegistry.layerSingle({
              ...adapter,
              openSession: (input) =>
                adapter.openSession(input).pipe(
                  Effect.map((runtime) => ({
                    ...runtime,
                    bufferedExecutionSelection: (binding) =>
                      withholdBufferedSelection
                        ? Effect.succeed(null)
                        : runtime.bufferedExecutionSelection!(binding),
                  })),
                ),
            }),
            { runEffectWorker: false },
          ),
        ),
      );
    }),
  );

it.effect("Claude selection recovery: revert", () => selectionRecovery("revert"));
it.effect("Claude selection recovery: refused-wake", () => selectionRecovery("refused-wake"));
it.effect("Claude selection recovery: in-flight", () => selectionRecovery("in-flight"));
it.effect("Claude selection recovery: failed-offer", () => selectionRecovery("failed-offer"));
it.effect("Claude selection recovery: idle-exit", () => selectionRecovery("idle-exit"));
it.effect("Claude selection recovery: notification-only", () =>
  selectionRecovery("notification-only"),
);
it.effect("Claude selection recovery: saved-selection", () => selectionRecovery("saved-selection"));
it.effect("Claude selection recovery: init-wake", () => selectionRecovery("init-wake"));

it.effect("Claude selection recovery: active-init-wake", () =>
  selectionRecovery("active-init-wake"),
);
it.effect("Claude selection recovery: init-first-wake", () => selectionRecovery("init-first-wake"));
it.effect("Claude selection recovery: recovered-wake", () => selectionRecovery("recovered-wake"));

it.effect("Claude selection recovery: continuation-init-wake", () =>
  selectionRecovery("continuation-init-wake"),
);
it.effect("Claude selection recovery: buffered-subagent", () =>
  selectionRecovery("buffered-subagent"),
);
it.effect("Claude selection recovery: persistent-failure", () =>
  selectionRecovery("persistent-failure"),
);
it.effect("Claude selection recovery: archived-wake", () => selectionRecovery("archived-wake"));
it.effect("Claude selection recovery: archived-unarchive", () =>
  selectionRecovery("archived-unarchive"),
);
it.effect("Claude selection recovery: dead-producer", () => selectionRecovery("dead-producer"));
it.effect("Claude selection recovery: active-stop-wake", () =>
  selectionRecovery("active-stop-wake"),
);
it.effect("Claude selection recovery: replaced-producer", () =>
  selectionRecovery("replaced-producer"),
);
it.effect("Claude selection recovery: double-init-wake", () =>
  selectionRecovery("double-init-wake"),
);
