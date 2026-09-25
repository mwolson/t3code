import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { TestClock } from "effect/testing";

import { CodexOrchestratorReplayHarness } from "../../../Adapters/CodexAdapterV2.testkit.ts";
import { OrchestrationEffectWorkerV2, runDaemon } from "../../../EffectWorker.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { OrchestratorV2 } from "../../../Orchestrator.ts";

import { ProviderAdapterRegistryV2 } from "../../../ProviderAdapterRegistry.ts";
import { provideDeterministicTestRuntime } from "../../DeterministicRuntime.ts";
import { runOrchestratorV2Scenario } from "../../OrchestratorScenario.ts";
import * as ProviderReplayHarness from "../../ProviderReplayHarness.ts";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import {
  materializeReplayTranscriptWorkspace,
  readProviderReplayTranscript,
} from "@t3tools/provider-testing/replayTranscript";
import { simpleInput } from "../simple/input.ts";
import { CODEX_MODEL_SELECTION, materializeFixtureInput } from "../shared.ts";

it.effect("retries a failed open before consuming the successful provider transcript", () =>
  Effect.gen(function* () {
    const workspace = yield* checkpointWorkspace("retry-open");
    const raw = yield* readProviderReplayTranscript(
      new URL("../simple/codex_transcript.ndjson", import.meta.url),
    ).pipe(Effect.provide(NodeServices.layer));
    const transcript = yield* CodexOrchestratorReplayHarness.decodeTranscript(
      materializeReplayTranscriptWorkspace(raw, workspace),
    );
    const input = yield* materializeFixtureInput({
      scenario: "retry-open",
      fixtureInput: simpleInput(),
      driver: CodexOrchestratorReplayHarness.driver,
      modelSelection: CODEX_MODEL_SELECTION,
    }).pipe(Effect.provide(IdAllocator.layer));
    let opens = 0;
    const registry = Layer.effect(
      ProviderAdapterRegistryV2,
      Effect.gen(function* () {
        const delegate = yield* ProviderAdapterRegistryV2;
        return ProviderAdapterRegistryV2.of({
          ...delegate,
          get: (id) =>
            delegate.get(id).pipe(
              Effect.map((adapter) => ({
                ...adapter,
                openSession: (request) =>
                  Effect.gen(function* () {
                    opens += 1;
                    if (opens === 1)
                      return yield* new ProviderAdapter.ProviderAdapterOpenSessionError({
                        driver: adapter.driver,
                        providerSessionId: request.providerSessionId,
                        cause: "transient open failure",
                      });
                    return yield* adapter.openSession(request);
                  }),
              })),
            ),
        });
      }),
    ).pipe(
      Layer.provide(CodexOrchestratorReplayHarness.makeProviderAdapterRegistryLayer(transcript)),
    );
    const layer = ProviderReplayHarness.layerWithRegistry(
      { name: "retry-open", runtimePolicyOverride: { cwd: workspace } },
      registry,
      { runEffectWorker: false },
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* OrchestratorV2;
      const worker = yield* OrchestrationEffectWorkerV2;
      for (const command of input.commands) yield* orchestrator.dispatch(command);
      yield* worker.drain();
      assert.equal(opens, 1);
      const threadId = input.projectionThreadIds[0]!;
      const before = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(before.runs[0]?.status, "starting");
      assert.isFalse(before.turnItems.some((item) => item.type === "error"));
      yield* TestClock.adjust("100 millis");
      yield* runDaemon.pipe(Effect.forkScoped);
      const result = yield* runOrchestratorV2Scenario({
        name: "retry-open",
        commands: [],
        steps: [{ type: "await_thread_idle", threadId }],
        projectionThreadIds: input.projectionThreadIds,
      });
      const after = result.projections.get(threadId)!;
      assert.equal(opens, 2);
      assert.equal(after.runs[0]?.status, "completed");
      assert.isTrue(
        after.messages.some((message) => message.role === "assistant" && message.text.length > 0),
      );
      assert.isFalse(after.turnItems.some((item) => item.type === "error"));
    }).pipe(Effect.provide(layer));
  }).pipe(provideDeterministicTestRuntime, Effect.scoped),
);
