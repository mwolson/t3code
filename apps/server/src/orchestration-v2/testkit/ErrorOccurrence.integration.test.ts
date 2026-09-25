import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as TestClock from "effect/testing/TestClock";

import { ClaudeOrchestratorReplayHarness } from "../Adapters/ClaudeAdapterV2.testkit.ts";
import { layer as idAllocatorLayer } from "../IdAllocator.ts";
import { ProjectionStoreV2, threadShellFromProjection } from "../ProjectionStore.ts";
import { ProviderSessionManagerV2 } from "../ProviderSessionManager.ts";
import { provideDeterministicTestRuntime } from "./DeterministicRuntime.ts";
import { ORCHESTRATOR_REPLAY_FIXTURES } from "./fixtures/index.ts";
import { CLAUDE_RESULT_IS_ERROR_PROMPT, materializeFixtureInput } from "./fixtures/shared.ts";
import { runOrchestratorV2Scenario } from "./OrchestratorScenario.ts";
import { makeOrchestratorV2ProviderReplayLayer } from "./ProviderReplayHarness.ts";
import { checkpointWorkspace } from "./ReplayFixtureWorkspace.ts";
import { readProviderReplayTranscript } from "./ReplayTranscriptNdjson.ts";

it.effect("preserves a replayed root failure occurrence through session release", () =>
  Effect.gen(function* () {
    const fixture = ORCHESTRATOR_REPLAY_FIXTURES.find(
      (entry) => entry.name === "claude_result_is_error",
    )!;
    const provider = fixture.providers.find((entry) => entry.driver === "claudeAgent")!;
    const raw = yield* readProviderReplayTranscript(provider.transcriptFile).pipe(
      Effect.provide(NodeServices.layer),
    );
    const end = raw.entries.findIndex(
      (entry) => entry.type === "emit_inbound" && entry.label === "result:1_is_error",
    );
    assert.isAtLeast(end, 0);
    const transcript = yield* ClaudeOrchestratorReplayHarness.decodeTranscript({
      ...raw,
      entries: raw.entries.slice(0, end + 1),
    });
    const materialized = yield* materializeFixtureInput({
      scenario: "error-occurrence",
      fixtureInput: { steps: [{ type: "message", text: CLAUDE_RESULT_IS_ERROR_PROMPT }] },
      driver: provider.driver,
      modelSelection: provider.modelSelection,
    }).pipe(Effect.provide(idAllocatorLayer), provideDeterministicTestRuntime);
    const scenario = {
      name: "error-occurrence",
      transcript,
      ...materialized,
      runtimePolicyOverride: {
        ...provider.runtimePolicyOverride,
        cwd: yield* checkpointWorkspace("error-occurrence"),
      },
    };
    yield* Effect.gen(function* () {
      const result = yield* runOrchestratorV2Scenario(scenario);
      const threadId = materialized.projectionThreadIds[0]!;
      const projection = result.projections.get(threadId)!;
      assert.equal(projection.runs.at(-1)?.status, "failed");
      const item = projection.turnItems.find((item) => item.type === "error");
      assert.isDefined(item);
      if (item?.type !== "error") throw new Error("Missing replayed error");
      const occurrenceAt = item.completedAt ?? item.startedAt;
      assert.isNotNull(occurrenceAt);
      if (occurrenceAt === null) throw new Error("Missing failure occurrence");
      const expectedAt = DateTime.formatIso(occurrenceAt);
      const before = threadShellFromProjection(projection);
      assert.equal(before.lastError, item.failure.message);
      assert.equal(before.lastErrorAt, expectedAt);
      const manager = yield* ProviderSessionManagerV2;
      const store = yield* ProjectionStoreV2;
      const session = projection.providerSessions.at(-1)!;
      assert.isDefined(session);
      yield* TestClock.adjust("1 second");
      yield* manager.release({ providerSessionId: session.id, reason: "idle_timeout" });
      const after = yield* store.getThreadProjection(threadId);
      assert.equal(after.providerSessions.at(-1)?.status, "stopped");
      const sqlShell = (yield* store.getShellSnapshot()).threads.find(
        (thread) => thread.id === threadId,
      )!;
      for (const shell of [threadShellFromProjection(after), sqlShell]) {
        assert.equal(shell.lastError, before.lastError);
        assert.equal(shell.lastErrorAt, expectedAt);
      }
    }).pipe(
      Effect.provide(
        makeOrchestratorV2ProviderReplayLayer(scenario, ClaudeOrchestratorReplayHarness),
      ),
      provideDeterministicTestRuntime,
    );
  }).pipe(Effect.scoped),
);
