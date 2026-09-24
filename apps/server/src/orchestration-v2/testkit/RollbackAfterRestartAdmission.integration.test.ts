/**
 * A rollback after the provider session's idle release opens a fresh session
 * and loads the native thread there before any turn has run on it. The next
 * message must admit that binding as a fresh attachment rather than as one
 * whose selection became uncertain: replacing it would detach the native
 * thread the rollback loaded, mid-rollback or just after it.
 */
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import type { OrchestrationV2DomainEvent } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import { CodexOrchestratorReplayHarness } from "../Adapters/CodexAdapterV2.testkit.ts";
import * as IdAllocator from "../IdAllocator.ts";
import { provideDeterministicTestRuntime } from "./DeterministicRuntime.ts";
import { ORCHESTRATOR_REPLAY_FIXTURES } from "./fixtures/index.ts";
import { materializeFixtureInput, type OrchestratorFixtureInput } from "./fixtures/shared.ts";
import type { OrchestratorV2ScenarioStep } from "./OrchestratorScenario.ts";
import { runOrchestratorV2ProviderReplayScenario } from "./ProviderReplayHarness.ts";
import { checkpointWorkspace } from "./ReplayFixtureWorkspace.ts";
import {
  materializeReplayTranscriptRuntimeInstructions,
  materializeReplayTranscriptWorkspace,
  readProviderReplayTranscript,
} from "./ReplayTranscriptNdjson.ts";

const FIXTURE = "thread_rollback_after_restart";
const fixture = ORCHESTRATOR_REPLAY_FIXTURES.find((candidate) => candidate.name === FIXTURE);
const codex = fixture?.providers.find((provider) => provider.driver === "codex");

/** The recorded rollback-after-restart run, with its steps reordered by `adjust`. */
const runRollbackAfterRestart = (input: {
  readonly buildInput: () => OrchestratorFixtureInput;
  readonly adjust?: (
    steps: ReadonlyArray<OrchestratorV2ScenarioStep>,
  ) => ReadonlyArray<OrchestratorV2ScenarioStep>;
  readonly check?: (domainEvents: ReadonlyArray<OrchestrationV2DomainEvent>) => void;
}) =>
  Effect.gen(function* () {
    if (codex === undefined) return yield* Effect.die(`The ${FIXTURE} fixture has no Codex run.`);
    const recorded = yield* readProviderReplayTranscript(codex.transcriptFile).pipe(
      Effect.provide(NodeServices.layer),
    );
    const fixtureInput = input.buildInput();
    const workspace = yield* checkpointWorkspace(FIXTURE, fixtureInput.workspaceFiles);
    const transcript = yield* CodexOrchestratorReplayHarness.decodeTranscript(
      materializeReplayTranscriptWorkspace(
        materializeReplayTranscriptRuntimeInstructions(recorded, {
          driver: codex.driver,
          model: codex.modelSelection.model,
        }),
        workspace,
      ),
    );
    const materialized = yield* materializeFixtureInput({
      scenario: FIXTURE,
      fixtureInput,
      driver: codex.driver,
      modelSelection: codex.modelSelection,
    }).pipe(Effect.provide(IdAllocator.layer), provideDeterministicTestRuntime);
    const result = yield* runOrchestratorV2ProviderReplayScenario(
      {
        name: `${FIXTURE}/${codex.driver}`,
        transcript,
        commands: materialized.commands,
        steps: input.adjust?.(materialized.steps) ?? materialized.steps,
        projectionThreadIds: materialized.projectionThreadIds,
        runtimePolicyOverride: { ...codex.runtimePolicyOverride, cwd: workspace },
      },
      CodexOrchestratorReplayHarness,
    ).pipe(provideDeterministicTestRuntime);
    codex.assertOutput(result, transcript);
    const projection = result.projections.get(materialized.projectionThreadIds[0]!);
    assert.deepEqual(
      projection?.runs.map((run) => run.status),
      ["completed", "rolled_back", "completed"],
    );
    input.check?.(result.domainEvents);
  }).pipe(Effect.scoped);

describe("admission after a rollback that reopened the provider session", () => {
  if (fixture === undefined) return;

  it.effect("admits the next message on the rollback's binding once the rollback ended", () =>
    runRollbackAfterRestart({
      buildInput: () => {
        const input = fixture.buildInput();
        const rollback = input.steps.findIndex((step) => step.type === "rollback");
        return {
          ...input,
          steps: [
            ...input.steps.slice(0, rollback + 1),
            { type: "await_run_status", targetRunIndex: 2, status: "rolled_back" },
            ...input.steps.slice(rollback + 1),
          ],
        };
      },
    }),
  );

  it.effect("admits the next message on the rollback's binding while the rollback runs", () =>
    runRollbackAfterRestart({
      buildInput: fixture.buildInput,
      // The rollback holds on its native revert's answer while the next message
      // is dispatched and admitted; only then does the rollback finish.
      adjust: (steps) => {
        const HELD = "thread/revert";
        const message = steps.findLastIndex(
          (step) => step.type === "dispatch" && step.command.type === "message.dispatch",
        );
        const dispatch = steps[message];
        if (dispatch?.type !== "dispatch") return steps;
        return [
          ...steps.slice(0, message),
          { type: "await_replay_gate", label: HELD },
          { ...dispatch, await: true },
          { type: "release_replay_gate", label: HELD },
          ...steps.slice(message + 1),
        ];
      },
      // Run 3 was admitted before the rollback projected run 2 as rolled back.
      check: (events) => {
        const admitted = events.findIndex(
          (event) => event.type === "run.created" && event.payload.ordinal === 3,
        );
        const rolledBack = events.findIndex(
          (event) =>
            event.type === "run.updated" &&
            event.payload.ordinal === 2 &&
            event.payload.status === "rolled_back",
        );
        assert.isAtLeast(admitted, 0);
        assert.isAbove(rolledBack, admitted);
      },
    }),
  );
});
