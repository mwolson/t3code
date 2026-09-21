import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { ProviderDriverKind } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import { CodexOrchestratorReplayHarness } from "../Adapters/CodexAdapterV2.testkit.ts";
import { EventSinkV2 } from "../EventSink.ts";
import { layer as idAllocatorLayer } from "../IdAllocator.ts";
import { OrchestratorV2 } from "../Orchestrator.ts";
import {
  ProviderContinuationRequests,
  type ProviderContinuationRequest,
} from "../ProviderContinuationRequests.ts";
import { workerLive as continuationWorker } from "../ProviderContinuationService.ts";
import { ThreadManagementService } from "../ThreadManagementService.ts";
import { provideDeterministicTestRuntime } from "./DeterministicRuntime.ts";
import { CODEX_MODEL_SELECTION, materializeFixtureInput } from "./fixtures/shared.ts";
import { runOrchestratorV2Scenario } from "./OrchestratorScenario.ts";
import { makeProviderReplayGate } from "./ProviderReplayGate.testkit.ts";
import { makeOrchestratorV2ProviderReplayLayer } from "./ProviderReplayHarness.ts";
import { checkpointWorkspace } from "./ReplayFixtureWorkspace.ts";
import {
  materializeReplayTranscriptWorkspace,
  readProviderReplayTranscript,
} from "./ReplayTranscriptNdjson.ts";

it.effect("coalesces two real Codex command completions behind an active turn", () =>
  Effect.gen(function* () {
    const name = "codex_background_command_coalescing";
    const workspace = yield* checkpointWorkspace(name);
    const raw = yield* readProviderReplayTranscript(
      new URL(
        "./fixtures/codex_background_command_coalescing/codex_transcript.ndjson",
        import.meta.url,
      ),
    );
    const transcript = yield* CodexOrchestratorReplayHarness.decodeTranscript(
      materializeReplayTranscriptWorkspace(raw, workspace),
    );
    const materialized = yield* materializeFixtureInput({
      scenario: name,
      fixtureInput: {
        steps: [
          { type: "message", text: "Start two background commands." },
          { type: "message", text: "Keep working while commands finish." },
        ],
      },
      driver: ProviderDriverKind.make("codex"),
      modelSelection: CODEX_MODEL_SELECTION,
    });
    const scenario = {
      ...materialized,
      name,
      transcript,
      runtimePolicyOverride: { cwd: workspace },
    };
    const gate = makeProviderReplayGate(["after-coalescing"]);
    yield* Effect.addFinalizer(() => Effect.sync(() => gate.releaseAll()));

    // The generic replay harness intentionally omits continuation delivery.
    // Share a real mailbox between the adapter and the production worker here;
    // forwarding the two thread-service operations retains real dispatch,
    // locking, persistence, promotion, provider startup and checkpointing.
    const requests = yield* Queue.unbounded<ProviderContinuationRequest>();
    const requestLayer = Layer.succeed(ProviderContinuationRequests, {
      offer: (request) => Queue.offer(requests, request).pipe(Effect.asVoid),
      take: Queue.take(requests),
    });
    const replayLayer = makeOrchestratorV2ProviderReplayLayer(
      scenario,
      {
        ...CodexOrchestratorReplayHarness,
        makeProviderAdapterRegistryLayer: (value, options) =>
          CodexOrchestratorReplayHarness.makeProviderAdapterRegistryLayer(value, options).pipe(
            Layer.provide(requestLayer),
          ),
      },
      { replayGate: gate },
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* OrchestratorV2;
      const sink = yield* EventSinkV2;
      yield* Layer.build(
        continuationWorker.pipe(
          Layer.provide(requestLayer),
          Layer.provide(idAllocatorLayer),
          Layer.provide(
            Layer.mock(ThreadManagementService)({
              dispatch: orchestrator.dispatch,
              getThreadProjection: orchestrator.getThreadProjection,
            }),
          ),
        ),
      );
      const scenarioFiber = yield* runOrchestratorV2Scenario(scenario).pipe(Effect.forkScoped);
      yield* sink.stream({ afterSequence: 0, eventType: "message.updated" }).pipe(
        Stream.filter(
          ({ event }) =>
            event.type === "message.updated" &&
            event.payload.notification?.summary === "Background commands finished",
        ),
        Stream.take(1),
        Stream.runDrain,
      );
      const threadId = materialized.projectionThreadIds[0]!;
      const queued = yield* orchestrator.getThreadProjection(threadId);
      const notifications = queued.messages.filter((message) => message.notification !== undefined);
      assert.lengthOf(notifications, 1);
      const notification = notifications[0]!;
      assert.include(notification.text, "ALPHA_OUTPUT");
      assert.include(notification.text, "BRAVO_OUTPUT");
      assert.equal(notification.text.split("BRAVO_OUTPUT").length, 2);
      assert.deepEqual(notification.notification, {
        source: { kind: "background_command" },
        outcome: "failed",
        summary: "Background commands finished",
        detail: "echo ALPHA\n\necho BRAVO",
      });
      assert.equal(
        queued.runs.find((run) => run.userMessageId === notification.id)?.status,
        "queued",
      );
      assert.isTrue(queued.runs.some((run) => run.status === "running"));
      assert.isTrue(gate.release("after-coalescing"));
      const result = yield* Fiber.join(scenarioFiber);
      const projection = result.projections.get(threadId)!;
      assert.deepEqual(
        projection.runs.map((run) => run.status),
        ["completed", "completed", "completed"],
      );
      const items = projection.turnItems.filter((item) => item.type === "notification");
      assert.lengthOf(items, 1);
      assert.equal(items[0]?.summary, "Background commands finished");
      assert.equal(items[0]?.detail, "echo ALPHA\n\necho BRAVO");
      // Batching a notification does not collapse the command output rows.
      const commands = projection.turnItems.filter((item) => item.type === "command_execution");
      assert.lengthOf(commands, 2);
      assert.sameMembers(
        commands.map((item) => item.input),
        ["echo ALPHA", "echo BRAVO"],
      );
      assert.sameMembers(
        commands.map((item) => item.output),
        ["ALPHA_OUTPUT\n", "BRAVO_OUTPUT\n"],
      );
      assert.notEqual(commands[0]?.id, commands[1]?.id);
      assert.isTrue(
        projection.messages.some((message) => message.text === "Both command results received."),
      );
    }).pipe(Effect.provide(replayLayer));
  }).pipe(
    Effect.provide(Layer.mergeAll(idAllocatorLayer, NodeServices.layer)),
    provideDeterministicTestRuntime,
    Effect.scoped,
  ),
);
