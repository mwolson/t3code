import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  ThreadId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import {
  readProviderReplayTranscript,
  materializeReplayTranscriptWorkspace,
} from "@t3tools/provider-testing/replayTranscript";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { MuseOrchestratorReplayHarness } from "../Adapters/MuseAdapterV2.testkit.ts";
import { EventSinkV2 } from "../EventSink.ts";
import { OrchestrationEffectWorkerV2 } from "../EffectWorker.ts";
import { OrchestratorV2 } from "../Orchestrator.ts";
import { ProviderSessionManagerV2 } from "../ProviderSessionManager.ts";
import * as ProviderReplayHarness from "./ProviderReplayHarness.ts";

it.effect.each(["continuation", "user"] as const)(
  "admits a held Muse report before the first acknowledged prompt: %s",
  (mode) =>
    Effect.scoped(
      Effect.gen(function* () {
        const cwd = yield* checkpointWorkspace(`muse-unacknowledged-${mode}`);
        const recorded = yield* readProviderReplayTranscript(
          new URL(
            `./fixtures/muse_unacknowledged_report/${mode}_transcript.ndjson`,
            import.meta.url,
          ),
        ).pipe(Effect.provide(NodeServices.layer));
        const transcript = yield* MuseOrchestratorReplayHarness.decodeTranscript(
          materializeReplayTranscriptWorkspace(recorded, cwd),
        );
        const offers =
          yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
        const registry = MuseOrchestratorReplayHarness.makeProviderAdapterRegistryLayer(
          transcript,
        ).pipe(
          Layer.provide(
            Layer.succeed(ProviderContinuationRequests.ProviderContinuationRequests, {
              offer: (request) => Queue.offer(offers, request).pipe(Effect.asVoid),
              take: Queue.take(offers),
            }),
          ),
        );
        yield* Effect.gen(function* () {
          const orchestrator = yield* OrchestratorV2;
          const worker = yield* OrchestrationEffectWorkerV2;
          const manager = yield* ProviderSessionManagerV2;
          const sink = yield* EventSinkV2;
          const threadId = ThreadId.make(`thread:muse-unacknowledged:${mode}`);
          const selection = {
            instanceId: ProviderInstanceId.make("muse"),
            model: "muse-spark-1.3-contributor",
            options: [{ id: "reasoningEffort", value: "max" }],
          };
          const policy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
            runtimeMode: "full-access",
            interactionMode: "default",
            cwd,
          });
          const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
            orchestrator.streamDomainEvents.pipe(
              Stream.filter(predicate),
              Stream.runHead,
              Effect.forkChild({ startImmediately: true }),
            );
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create"),
            threadId,
            projectId: ProjectId.make("project:muse-unacknowledged"),
            title: "Held report",
            modelSelection: selection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
          // Attach the native session, as on resume, without sending a T3 prompt.
          const runtime = yield* manager.open({
            threadId,
            providerSessionId: ProviderSessionId.make(`session:muse:${mode}`),
            modelSelection: selection,
            runtimePolicy: policy,
          });
          const binding = yield* runtime.ensureThread({
            threadId,
            modelSelection: selection,
            runtimePolicy: policy,
          });
          const now = yield* DateTime.now;
          const before = yield* orchestrator.getThreadProjection(threadId);
          yield* sink.write({
            events: [
              {
                id: EventId.make("attached-binding"),
                type: "provider-thread.updated",
                threadId,
                providerInstanceId: selection.instanceId,
                occurredAt: now,
                payload: binding,
              },
              {
                id: EventId.make("active-binding"),
                type: "thread.metadata-updated",
                threadId,
                providerInstanceId: selection.instanceId,
                occurredAt: now,
                payload: { ...before.thread, activeProviderThreadId: binding.id },
              },
            ],
          });
          const offered = yield* Queue.take(offers);
          assert.isTrue(yield* runtime.hasBufferedOutputForThread!(binding));
          assert.isNull(yield* runtime.bufferedExecutionSelection!(binding));
          assert.isNull(yield* runtime.executionSelection!(binding));
          const sequence = yield* orchestrator.getThreadEventSequence(threadId);
          const incompatible = yield* orchestrator
            .dispatch({
              type: "message.dispatch",
              commandId: CommandId.make("replace-held-report"),
              threadId,
              messageId: MessageId.make("message:replace-held-report"),
              text: "Change model",
              attachments: [],
              dispatchMode: { type: "start_immediately" },
              createdBy: "user",
              creationSource: "web",
              modelSelection: { ...selection, model: "different-model" },
            })
            .pipe(Effect.flip);
          assert.equal(incompatible._tag, "OrchestratorDispatchError");
          assert.equal(yield* orchestrator.getThreadEventSequence(threadId), sequence);
          assert.isTrue(yield* runtime.hasBufferedOutputForThread!(binding));
          assert.isNull(yield* runtime.executionSelection!(binding));
          const asked = yield* watch(
            (event) =>
              event.type === "runtime-request.updated" && event.payload.status === "pending",
          );
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("adopt"),
            threadId,
            messageId: MessageId.make("message:adopt"),
            text: mode === "user" ? "Next task" : "Deliver report",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: mode === "user" ? "user" : "agent",
            creationSource: mode === "user" ? "web" : "provider",
            modelSelection: selection,
          });
          yield* worker.drain();
          const pending = Option.getOrThrow(yield* Fiber.join(asked));
          assert.equal(pending.type, "runtime-request.updated");
          if (pending.type !== "runtime-request.updated") return;
          assert.deepEqual(
            yield* runtime.executionSelection!(binding),
            mode === "user" ? selection : null,
            "a drain cannot acknowledge the composer's selection",
          );
          const done = yield* watch(
            (event) =>
              event.type === "run.updated" &&
              (event.payload.status === "completed" || event.payload.status === "waiting"),
          );
          yield* orchestrator.dispatch({
            type: "runtime-request.respond",
            commandId: CommandId.make("approve"),
            threadId,
            requestId: pending.payload.id,
            decision: "accept",
          });
          yield* worker.drain();
          yield* Fiber.join(done);
          yield* worker.drain();
          const after = yield* orchestrator.getThreadProjection(threadId);
          assert.lengthOf(after.runs, 1);
          assert.equal(after.runs[0]?.status, "completed");
          assert.isTrue(
            after.messages.some(
              (message) =>
                message.text === "Held workflow report delivered" &&
                message.runId === after.runs[0]?.id,
            ),
          );
          assert.equal(
            after.runtimeRequests.find((request) => request.id === pending.payload.id)?.status,
            "resolved",
          );
          assert.isFalse(yield* runtime.hasBufferedOutputForThread!(binding));
          assert.isTrue(
            Option.isNone(yield* offered.dispatchIfCurrent!(Effect.void)),
            "adoption invalidates the held report's offer",
          );
        }).pipe(
          Effect.provide(
            ProviderReplayHarness.layerWithRegistry(
              { name: `muse-unacknowledged-${mode}` },
              registry,
              { runEffectWorker: false },
            ),
          ),
        );
      }),
    ),
);
