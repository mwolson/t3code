/**
 * A thread whose session forgot its binding (a plain detach, here from
 * settling the thread) is attached again by a rollback, which loads nothing
 * the session manager sees; it has no record of the binding's selection.
 * OpenCode then starts a follow-up on its own, which the adapter holds for a
 * continuation. A user message that changes the selection, or moves the
 * thread to another provider instance, must be refused at admission while that
 * output is held: before a run or a selection change is committed, and
 * without touching the held output.
 */
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  ProviderInstanceId,
  type ModelSelection,
  type OrchestrationV2Command,
  type ProviderReplayEntry,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { OpenCode2OrchestratorReplayHarness } from "../Adapters/OpenCode2AdapterV2.testkit.ts";
import * as IdAllocator from "../IdAllocator.ts";
import * as ProviderAdapter from "../ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "../ProviderAdapterRegistry.ts";
import { provideDeterministicTestRuntime } from "./DeterministicRuntime.ts";
import { ORCHESTRATOR_REPLAY_FIXTURES } from "./fixtures/index.ts";
import { materializeFixtureInput } from "./fixtures/shared.ts";
import { runOrchestratorV2ProviderReplayScenario } from "./ProviderReplayHarness.ts";
import {
  materializeReplayTranscriptRuntimeInstructions,
  readProviderReplayTranscript,
} from "./ReplayTranscriptNdjson.ts";
import { checkpointWorkspace } from "./ReplayFixtureWorkspace.ts";

const FIXTURE = "opencode2_revert";
const fixture = ORCHESTRATOR_REPLAY_FIXTURES.find((candidate) => candidate.name === FIXTURE);
const provider = fixture?.providers[0];
/** Held until both messages were refused: the last frame after the follow-up. */
const WAKE_HELD = "wake-held";
/** Another OpenCode instance the thread could move to. */
const OTHER = ProviderInstanceId.make("opencode-other");

const labelOf = (entry: ProviderReplayEntry) =>
  entry.type === "runtime_exit" ? undefined : entry.label;
/** A recorded session event again, later in the stream, with its own id and sequence. */
const again = (entry: ProviderReplayEntry, seq: number, label?: string): ProviderReplayEntry => {
  if (entry.type !== "emit_inbound") return entry;
  const frame = entry.frame as { readonly event: Record<string, unknown> };
  const durable = frame.event["durable"] as Record<string, unknown>;
  return {
    ...entry,
    ...(label === undefined ? {} : { label }),
    frame: {
      ...frame,
      event: { ...frame.event, id: `evt_wake_${seq}`, durable: { ...durable, seq } },
    },
  };
};

/** An error's message and its causes' messages, for asserting on the refusal reason. */
const causeText = (error: unknown): string => {
  const parts: Array<string> = [];
  let current: unknown = error;
  for (let depth = 0; depth < 6 && current !== undefined && current !== null; depth++) {
    if (typeof current === "string") {
      parts.push(current);
      break;
    }
    if (current instanceof Error) parts.push(current.message);
    current = (current as { readonly cause?: unknown }).cause;
  }
  return parts.join(" | ");
};

describe("admission while a rollback-attached OpenCode 2 binding holds a follow-up", () => {
  if (fixture === undefined || provider === undefined) return;

  it.effect("refuses a changed selection and a replacement before committing either", () =>
    Effect.gen(function* () {
      const recorded = yield* readProviderReplayTranscript(provider.transcriptFile).pipe(
        Effect.provide(NodeServices.layer),
      );
      const at = (label: string) => {
        const index = recorded.entries.findIndex((entry) => labelOf(entry) === label);
        assert.isAtLeast(index, 0, label);
        return index;
      };
      const entry = (label: string) => recorded.entries[at(label)]!;
      const turns = recorded.entries.slice(0, at("session.execution.succeeded.2") + 1);
      const revert = recorded.entries.slice(
        at("message.list.users.1"),
        at("message.list.history.response.2") + 1,
      );
      const sessionId = "ses_f1484bef1ffeGfSJ1e2FXLCaus";
      const out = (type: string, input?: unknown): ProviderReplayEntry => ({
        type: "expect_outbound",
        frame: input === undefined ? { type } : { type, input },
      });
      const reply = (operation: string, data: unknown): ProviderReplayEntry => ({
        type: "emit_inbound",
        frame: { type: "sdk.response", operation, data },
      });
      const created = entry("session.create.response");
      const sessionData =
        created.type === "emit_inbound"
          ? (created.frame as { readonly data: { readonly data: unknown } }).data.data
          : null;
      const entries: ReadonlyArray<ProviderReplayEntry> = [
        ...turns,
        // Settling detached the thread, and the runtime unloaded its session.
        // The rollback loads it again on its own (no load the session manager
        // sees) before rolling it back.
        // It first asks the server whether anything still runs there.
        out("session.active"),
        reply("session.active", { data: {} }),
        out("session.get", { sessionID: sessionId }),
        reply("session.get", { data: sessionData }),
        out("permission.list", { sessionID: sessionId }),
        reply("permission.list", { data: [] }),
        out("session.form.list", { sessionID: sessionId }),
        reply("session.form.list", { data: [] }),
        // It gives the session T3's rules for the thread's mode again.
        out("session.update", "<any>"),
        reply("session.update", null),
        ...revert,
        // OpenCode then runs a follow-up on its own, with no turn of T3's.
        again(entry("session.execution.started"), 40),
        again(entry("session.text.ended"), 41),
        again(entry("session.execution.succeeded"), 42),
        again(entry("session.usage.updated"), 43, WAKE_HELD),
        { type: "runtime_exit", status: "success" },
      ];
      const fixtureInput = {
        steps: fixture.buildInput().steps.filter((step) => step.type === "message"),
      };
      const workspace = yield* checkpointWorkspace(FIXTURE);
      const transcript = yield* OpenCode2OrchestratorReplayHarness.decodeTranscript(
        materializeReplayTranscriptRuntimeInstructions(
          { ...recorded, scenario: `${FIXTURE}-wake-admission`, entries },
          { driver: provider.driver, model: provider.modelSelection.model },
        ),
      );
      const materialized = yield* materializeFixtureInput({
        scenario: FIXTURE,
        fixtureInput,
        driver: provider.driver,
        modelSelection: provider.modelSelection,
      }).pipe(Effect.provide(IdAllocator.layer), provideDeterministicTestRuntime);
      const threadId = materialized.projectionThreadIds[0]!;
      const message = (key: string, modelSelection: ModelSelection): OrchestrationV2Command => ({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make(`command:wake-admission:${key}`),
        threadId,
        messageId: MessageId.make(`message:wake-admission:${key}`),
        text: `Reply with exactly: ${key}`,
        attachments: [],
        modelSelection,
        dispatchMode: { type: "start_immediately" },
      });
      const ids = yield* IdAllocator.IdAllocatorV2;
      // Settling detaches the thread from its session runtime, which forgets
      // what the manager knew about the binding; the runtime stays up.
      const detach: OrchestrationV2Command = {
        type: "thread.settle",
        commandId: CommandId.make("command:wake-admission:settle"),
        threadId,
      };
      const scopeId = yield* ids.allocate.checkpointScope({ threadId, name: "root" });
      const rollback: OrchestrationV2Command = {
        type: "checkpoint.rollback",
        restoreFiles: false,
        commandId: CommandId.make("command:wake-admission:rollback"),
        threadId,
        scopeId,
        checkpointId: yield* ids.allocate.checkpoint({ checkpointScopeId: scopeId, name: "1" }),
      };
      const changed = message("changed", {
        ...provider.modelSelection,
        model: "opencode/mimo-v2.6-flash-free",
      });
      const replaced = message("replaced", { ...provider.modelSelection, instanceId: OTHER });
      // The thread's session runtime, and another instance to move to.
      const sessions: Array<ProviderAdapter.ProviderAdapterV2SessionRuntime> = [];
      const harness: typeof OpenCode2OrchestratorReplayHarness = {
        ...OpenCode2OrchestratorReplayHarness,
        makeProviderAdapterRegistryLayer: (decoded, options) =>
          Layer.effect(
            ProviderAdapterRegistry.ProviderAdapterRegistryV2,
            Effect.gen(function* () {
              const registry = yield* ProviderAdapterRegistry.ProviderAdapterRegistryV2;
              const recorder = (
                adapter: ProviderAdapter.ProviderAdapterV2Shape,
              ): ProviderAdapter.ProviderAdapterV2Shape => ({
                ...adapter,
                openSession: (input) =>
                  adapter
                    .openSession(input)
                    .pipe(Effect.tap((session) => Effect.sync(() => void sessions.push(session)))),
              });
              return ProviderAdapterRegistry.ProviderAdapterRegistryV2.of({
                ...registry,
                get: (id) =>
                  id === OTHER
                    ? registry.get(provider.modelSelection.instanceId).pipe(
                        Effect.map((adapter) => ({
                          ...adapter,
                          instanceId: OTHER,
                          openSession: () => Effect.die("The other instance never opens."),
                        })),
                      )
                    : registry.get(id).pipe(Effect.map(recorder)),
                list: () => registry.list().pipe(Effect.map((instances) => [...instances, OTHER])),
              });
            }),
          ).pipe(
            Layer.provide(
              OpenCode2OrchestratorReplayHarness.makeProviderAdapterRegistryLayer(decoded, options),
            ),
          ),
      };
      const result = yield* runOrchestratorV2ProviderReplayScenario(
        {
          name: `${FIXTURE}/wake-admission`,
          transcript,
          commands: [...materialized.commands, detach, rollback, changed, replaced],
          steps: [
            ...materialized.steps,
            { type: "dispatch", command: detach },
            { type: "advance_clock", duration: "1 millis" },
            // The rollback attaches it again and OpenCode loads nothing new.
            { type: "dispatch", command: rollback },
            { type: "advance_clock", duration: "1 millis" },
            {
              type: "await_run_status",
              threadId,
              runId: ids.derive.run({ threadId, ordinal: 2 }),
              status: "rolled_back",
            },
            { type: "await_replay_gate", label: WAKE_HELD },
            { type: "dispatch_refused", command: changed, key: "changed" },
            { type: "dispatch_refused", command: replaced, key: "replaced" },
            { type: "release_replay_gate", label: WAKE_HELD },
          ],
          projectionThreadIds: materialized.projectionThreadIds,
          runtimePolicyOverride: { ...provider.runtimePolicyOverride, cwd: workspace },
        },
        harness,
      ).pipe(provideDeterministicTestRuntime);
      const projection = result.projections.get(threadId);
      assert.isDefined(projection);
      // Neither message made a run or changed the thread's selection.
      assert.deepEqual(
        projection.runs.map((run) => run.status),
        ["completed", "rolled_back"],
      );
      assert.deepEqual(projection.thread.modelSelection, provider.modelSelection);
      for (const key of ["changed", "replaced"]) {
        assert.include(
          causeText(result.refusedDispatches.get(key)),
          ProviderAdapter.PROVIDER_BUFFERED_OUTPUT_MESSAGE,
          key,
        );
      }
      // The follow-up is still held for its continuation.
      assert.lengthOf(sessions, 1);
      const binding = projection.providerThreads[0]!;
      assert.isTrue(yield* sessions[0]!.hasBufferedOutputForThread!(binding));
    }).pipe(Effect.scoped, Effect.provide(Layer.merge(NodeServices.layer, IdAllocator.layer))),
  );
});
