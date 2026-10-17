import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type ModelSelection,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { PiAdapterV2Driver } from "@t3tools/provider-pi/server";
import { makeFakePi, type FakePi } from "@t3tools/provider-pi/testing";
import * as Orchestrator from "../Orchestrator.ts";
import * as EffectWorker from "../EffectWorker.ts";
import * as ProviderSessionManager from "../ProviderSessionManager.ts";
import * as ProjectionStore from "../ProjectionStore.ts";
import * as ProviderAdapterRegistry from "../ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./ProviderReplayHarness.ts";
const layerTest = Layer.mergeAll(
  NodeServices.layer,
  IdAllocator.layer,
  McpProviderSessions.layer,
  TestProviderHost.layer().pipe(Layer.provide(NodeServices.layer)),
);
const PI_INSTANCE_ID = ProviderInstanceId.make("pi");
const THREAD_ID = ThreadId.make("thread-pi-test");
const modelSelection = (model: string): ModelSelection => ({ instanceId: PI_INSTANCE_ID, model });
const makeAdapter = (fake: FakePi) =>
  PiAdapterV2Driver.create({
    instanceId: PI_INSTANCE_ID,
    displayName: undefined,
    enabled: true,
    environment: [],
    config: { enabled: true, binaryPath: "pi", launchArgs: "", customModels: [] },
  }).pipe(
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, fake.spawner),
    Effect.provideService(HostProcess.Environment, {}),
  );
const abortReplayCase = Schema.Struct({
  name: Schema.String,
  events: Schema.Array(Schema.Record(Schema.String, Schema.Unknown)),
  status: Schema.Literals(["failed", "completed"]),
  error: Schema.NullOr(Schema.String),
});
const decodeAbortReplayCase = Schema.decodeSync(Schema.fromJsonString(abortReplayCase));
const abortReplayCaseNames = [
  "final-abort",
  "recovered-stop",
  "recovered-toolUse",
  "recovered-length",
  "recovered-then-second-abort",
  "compaction-no-assistant",
  "compaction-will-retry-no-assistant",
  "cancelled-compaction",
  "failed-compaction",
  "custom-and-agent-start",
  "compaction-and-assistant",
  "noop-compaction-and-assistant",
  "retry-abort-success-flag",
  "retry-abort-specific-failure",
  "specific-provider-failure",
  "ordinary-retry",
  "overflow-recovery",
  "retry-exhaustion-compaction",
  "normal-stop",
  "normal-tool-use",
  "pending-does-not-clear",
  "deferred-does-not-clear",
  "notifications",
  "subordinate-abort",
  "review-subordinate-after-last-main",
  "review-notify-error-after-last-main",
];

// Synthetic protocol scenarios, not a wire capture. Only Pi's process is replaced;
// commands, ingestion, terminalization, checkpoints and projections are real.
describe("Pi abort outcome orchestrator replay", () => {
  it.effect.each(abortReplayCaseNames)("%s", (name) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const fixturePath = yield* path.fromFileUrl(
        new URL("./fixtures/pi_abort_outcome/scenarios.ndjson", import.meta.url),
      );
      const transcript = yield* fs.readFileString(fixturePath);
      const cases = transcript
        .trim()
        .split("\n")
        .map((line) => decodeAbortReplayCase(line));
      assert.sameMembers(
        cases.map((entry) => entry.name),
        abortReplayCaseNames,
      );
      const scenario = cases.find((entry) => entry.name === name)!;
      const fake = yield* makeFakePi;
      const adapter = yield* makeAdapter(fake);
      const observed: Array<ProviderAdapter.ProviderAdapterV2Event> = [];
      const registry = ProviderAdapterRegistry.layerSingle({
        ...adapter,
        openSession: (input) =>
          adapter.openSession(input).pipe(
            Effect.map((runtime) => ({
              ...runtime,
              events: runtime.events.pipe(
                Stream.tap((event) => Effect.sync(() => observed.push(event))),
              ),
            })),
          ),
      });
      const cwd = yield* checkpointWorkspace(`pi-abort-${scenario.name}`);
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const store = yield* ProjectionStore.ProjectionStoreV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make("pi-abort:create"),
          threadId: THREAD_ID,
          projectId: ProjectId.make("pi-abort:project"),
          createdBy: "user",
          creationSource: "web",
          title: "Pi abort replay",
          modelSelection: modelSelection("default"),
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("pi-abort:message"),
          threadId: THREAD_ID,
          messageId: MessageId.make("pi-abort:message"),
          createdBy: "user",
          creationSource: "web",
          text: "Respond",
          attachments: [],
          modelSelection: modelSelection("default"),
          dispatchMode: { type: "start_immediately" },
        });
        yield* fake.takeRequest("prompt");
        for (const event of scenario.events) yield* fake.emit(event);
        yield* fake.emit({ type: "agent_settled" });
        yield* orchestrator.streamStoredEvents.pipe(
          Stream.filter(
            ({ event }) =>
              event.type === "run.updated" &&
              (event.payload.status === "failed" || event.payload.status === "completed"),
          ),
          Stream.runHead,
        );
        yield* worker.drain();
        const projection = yield* orchestrator.getThreadProjection(THREAD_ID);
        assert.lengthOf(projection.runs, 1);
        assert.equal(projection.runs[0]!.status, scenario.status);
        assert.equal(projection.providerTurns.at(-1)?.status, scenario.status);
        const terminals = observed.filter((event) => event.type === "turn.terminal");
        assert.lengthOf(terminals, 1);
        assert.equal(terminals[0]?.threadDisposition, "reusable");
        const errors = observed.filter(
          (event) =>
            event.type === "provider_session.updated" && event.providerSession.lastError !== null,
        );
        assert.lengthOf(errors, scenario.error === null ? 0 : 1);
        const session = projection.providerSessions.at(-1)!;
        assert.equal(session.lastError, scenario.error);
        if (scenario.error !== null) {
          assert.isDefined(session.lastErrorAt);
          assert.isNotNull(session.lastErrorAt);
          const shell = (yield* store.getShellSnapshot()).threads.find(
            (thread) => thread.id === THREAD_ID,
          )!;
          assert.equal(shell.lastError, scenario.error);
          assert.equal(shell.lastErrorAt, DateTime.formatIso(session.lastErrorAt!));
          yield* TestClock.adjust("1 second");
          yield* manager.release({ providerSessionId: session.id, reason: "idle_timeout" });
          const after = (yield* store.getThreadProviderContext(THREAD_ID)).providerSessions.at(-1)!;
          assert.equal(after.lastError, scenario.error);
          assert.deepEqual(after.lastErrorAt, session.lastErrorAt);
        }
        assert.lengthOf(
          fake.allRequests().filter((request) => request["type"] === "prompt"),
          1,
        );
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry(
            {
              name: `pi-abort-${scenario.name}`,
              runtimePolicyOverride: {
                cwd,
                approvalPolicy: "never",
                sandboxPolicy: { type: "readOnly" },
              },
            },
            registry,
          ),
        ),
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});
