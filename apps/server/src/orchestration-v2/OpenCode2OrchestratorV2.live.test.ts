/**
 * Runs OpenCode 2 through the whole orchestrator with the real driver: the
 * driver probes the binary, spawns `opencode serve`, and routes to the 2.x
 * adapter. One thread reads a file, runs a shell command, and stops another;
 * a Supervised thread approves one command, declines another, and answers a
 * question; a plan-mode thread may write only its plan.
 *
 *   OPENCODE2_BIN=/path/to/opencode OPENCODE2_LIVE_ROOT=/scratch/dir \
 *     vp test run src/orchestration-v2/OpenCode2OrchestratorV2.live.test.ts
 *
 * The server runs with isolated HOME and XDG directories on the free
 * `opencode/big-pickle` model; `OPENCODE2_MODEL` picks another (its provider's
 * key comes from the test's environment, which the spawned server inherits).
 */
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as SourceControlProviderRegistry from "../sourceControl/SourceControlProviderRegistry.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqlite from "node:sqlite";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  MessageId,
  type ModelSelection,
  type OrchestrationV2ThreadProjection,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { FetchHttpClient } from "effect/unstable/http";
import { describe } from "vite-plus/test";

import * as ResetCreditCoordinator from "../provider/Layers/resetCreditCoordinator.ts";
import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import * as BackgroundPolicy from "../background/BackgroundPolicy.ts";
import * as HostPowerMonitor from "../background/HostPowerMonitor.ts";
import * as ServerConfig from "../config.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as AntigravityInstallation from "../provider/AntigravityInstallation.ts";
import * as CodexInstallation from "../provider/CodexInstallation.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ModelManifest from "../provider/ModelManifest.ts";
import { ProviderInstanceRegistryHydrationLive } from "../provider/Layers/ProviderInstanceRegistryHydration.ts";
import * as ProviderEventLoggers from "../provider/Layers/ProviderEventLoggers.ts";
import * as OpenCodeRuntime from "../provider/opencodeRuntime.ts";
import * as OpenCodeServerLedger from "../provider/OpenCodeServerLedger.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProviderInstanceRegistry from "../provider/Services/ProviderInstanceRegistry.ts";
import { worktreeRepairDependenciesTestLayer } from "./ProviderTurnStartService.testkit.ts";
import { OrchestrationV2LayerLive } from "./runtimeLayer.ts";
import * as McpSessionRegistryTestkit from "../mcp/McpSessionRegistry.testkit.ts";

const binaryPath = process.env.OPENCODE2_BIN;
const ROOT = process.env.OPENCODE2_LIVE_ROOT ?? "";
const INSTANCE = ProviderInstanceId.make("opencode");
const MODEL: ModelSelection = {
  instanceId: INSTANCE,
  model: process.env.OPENCODE2_MODEL ?? "opencode/big-pickle",
};
// The free model the thread switches to mid-conversation.
const SWITCHED_MODEL = "opencode/mimo-v2.6-flash-free";

const PlatformTestLayer = Layer.merge(
  NodeServices.layer,
  Layer.mock(SourceControlProviderRegistry.SourceControlProviderRegistry)({
    resolveLink: () => Effect.die("unused title link"),
  }),
);
const serverConfigLayer = ServerConfig.layerTest(`${ROOT}/work`, { prefix: "t3-opencode2-live-" });
const vcsDriverRegistryLayer = VcsDriverRegistry.layer.pipe(
  Layer.provide(VcsProcess.layer),
  Layer.provide(serverConfigLayer),
  Layer.provide(PlatformTestLayer),
);
// Isolated OpenCode state: the server never touches the developer's own data.
const serverSettingsLayer = ServerSettings.layerTest({
  providerInstances: {
    [INSTANCE]: {
      driver: ProviderDriverKind.make("opencode"),
      enabled: true,
      environment: [
        { name: "HOME", value: ROOT },
        { name: "XDG_CONFIG_HOME", value: `${ROOT}/config` },
        { name: "XDG_DATA_HOME", value: `${ROOT}/data` },
        { name: "XDG_STATE_HOME", value: `${ROOT}/state` },
        { name: "XDG_CACHE_HOME", value: `${ROOT}/cache` },
      ],
      config: { enabled: true, binaryPath },
    },
  },
});
const backgroundPolicyLayer = BackgroundPolicy.layer.pipe(
  Layer.provide(Layer.effect(HostPowerMonitor.HostPowerMonitor, HostPowerMonitor.make())),
  Layer.provide(serverSettingsLayer),
);
const providerInstanceRegistryLayer = ProviderInstanceRegistryHydrationLive.pipe(
  Layer.provide(
    Layer.mergeAll(
      serverConfigLayer.pipe(Layer.provide(PlatformTestLayer)),
      serverSettingsLayer,
      ServerSecretStore.layer.pipe(
        Layer.provide(serverConfigLayer),
        Layer.provide(PlatformTestLayer),
      ),
      NodeServices.layer,
      FetchHttpClient.layer,
      OpenCodeRuntime.OpenCodeRuntimeLive.pipe(
        Layer.provide(OpenCodeServerLedger.layerTest),
        Layer.provide(PlatformTestLayer),
      ),
      Layer.succeed(
        ProviderEventLoggers.ProviderEventLoggers,
        ProviderEventLoggers.NoOpProviderEventLoggers,
      ),
      ModelManifest.layerTest,
      AntigravityInstallation.AntigravityInstallation.layer.pipe(
        Layer.provide(serverConfigLayer.pipe(Layer.provide(PlatformTestLayer))),
        Layer.provide(FetchHttpClient.layer),
        Layer.provide(PlatformTestLayer),
      ),
      // The Codex driver now resolves managed ChatGPT installs; these runs never launch Codex.
      Layer.mock(CodexInstallation.CodexInstallation)({
        managedDirectory: "unused-managed-installation",
      }),
      Layer.succeed(ServerEnvironment.ServerEnvironmentIdentity, {
        getEnvironmentId: Effect.succeed(
          EnvironmentId.make("00000000-0000-4000-8000-000000000001"),
        ),
      }),
    ),
  ),
);
const liveLayer = OrchestrationV2LayerLive.pipe(
  Layer.provide(worktreeRepairDependenciesTestLayer),
  Layer.provide(McpSessionRegistryTestkit.layer),
  Layer.provide(SqlitePersistenceMemory),
  Layer.provide(CheckpointStore.layer.pipe(Layer.provide(vcsDriverRegistryLayer))),
  Layer.provide(serverConfigLayer),
  Layer.provide(serverSettingsLayer),
  // Merged, not only provided: the test reads the same instance the orchestrator uses.
  Layer.provideMerge(providerInstanceRegistryLayer),
  Layer.provide(ResetCreditCoordinator.layer),
  Layer.provide(backgroundPolicyLayer),
  Layer.provide(PlatformTestLayer),
);

const settled = (projection: OrchestrationV2ThreadProjection) =>
  projection.runs.length > 0 &&
  projection.runs.every(
    (run) => !["queued", "starting", "running", "waiting"].includes(run.status),
  );

const waitFor = Effect.fn("OpenCode2Live.waitFor")(function* (
  threadId: ThreadId,
  done: (projection: OrchestrationV2ThreadProjection) => boolean,
) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  for (let attempt = 0; attempt < 240; attempt += 1) {
    const projection = yield* orchestrator.getThreadProjection(threadId);
    if (done(projection)) return projection;
    yield* Effect.sleep("500 millis");
  }
  const last = yield* orchestrator.getThreadProjection(threadId);
  const items = last.turnItems.map((item) =>
    item.type === "error" ? `error:${item.failure.message}` : `${item.type}:${item.status}`,
  );
  return yield* Effect.die(
    new Error(
      `Timed out waiting on OpenCode 2 thread ${threadId}: runs ${last.runs.map((run) => run.status).join(",")}; items ${items.join(",")}`,
    ),
  );
});

const send = Effect.fn("OpenCode2Live.send")(function* (
  threadId: ThreadId,
  key: string,
  text: string,
  modelSelection: ModelSelection = MODEL,
) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  yield* orchestrator.dispatch({
    type: "message.dispatch",
    createdBy: "user",
    creationSource: "web",
    commandId: CommandId.make(`command:opencode2-live:${key}`),
    threadId,
    messageId: MessageId.make(`message:opencode2-live:${key}`),
    text,
    attachments: [],
    modelSelection,
    dispatchMode: { type: "start_immediately" },
  });
});

const AssistantModel = Schema.fromJsonString(
  Schema.Struct({ model: Schema.Struct({ providerID: Schema.String, id: Schema.String }) }),
);
const decodeAssistantModel = Schema.decodeUnknownSync(AssistantModel);

/**
 * The `provider/model` of each assistant message in a native session, oldest
 * first, from the spawned server's own database under the isolated XDG root.
 */
const assistantModels = (nativeSessionId: string) =>
  Effect.sync(() => {
    const db = new NodeSqlite.DatabaseSync(`${ROOT}/data/opencode/opencode.db`, { readOnly: true });
    try {
      return db
        .prepare(
          "SELECT data FROM session_message WHERE session_id = ? AND type = 'assistant' ORDER BY seq",
        )
        .all(nativeSessionId)
        .map((row) => decodeAssistantModel(row.data).model)
        .map((model) => `${model.providerID}/${model.id}`);
    } finally {
      db.close();
    }
  });

describe.runIf(binaryPath !== undefined && ROOT !== "")("OpenCode 2 live orchestrator", () => {
  it.live(
    "runs a tool turn and stops a running shell command through the real driver",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        yield* fs.writeFileString(path.join(ROOT, "work", "hello.txt"), "hello from t3 live\n");
        yield* EffectWorker.runDaemonWithOptions({ concurrency: 2 }).pipe(Effect.forkScoped);

        // A status check starts a fresh server, which lists no models for its
        // first few hundred milliseconds; the picker must still get them.
        const instance =
          yield* (yield* ProviderInstanceRegistry.ProviderInstanceRegistry).getInstance(INSTANCE);
        assert.isDefined(instance);
        const status = yield* instance!.snapshot.refresh;
        assert.equal(status.status, "ready");
        assert.include(
          status.models.map((model) => model.slug),
          "opencode/big-pickle",
        );
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const threadId = ThreadId.make("thread:opencode2-live");
        yield* orchestrator.dispatch({
          type: "thread.create",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make("command:opencode2-live:create"),
          threadId,
          projectId: ProjectId.make("project:opencode2-live"),
          title: "OpenCode 2 live",
          modelSelection: MODEL,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: `${ROOT}/work`,
        });

        yield* send(
          threadId,
          "tools",
          // The shell call outlasts the spawned server's 30 second idle timeout.
          "Use the read tool to read hello.txt, then run the shell command `sleep 35 && echo TOOL_OK` with the shell tool in the foreground (not in the background) and wait for it to finish, then reply DONE.",
        );
        const first = yield* waitFor(threadId, settled);
        assert.deepEqual(
          first.runs.map((run) => run.status),
          ["completed"],
        );
        const shell = first.turnItems.find((item) => item.type === "command_execution");
        assert.deepInclude(shell, { status: "completed", exitCode: 0 });
        assert.include(shell?.type === "command_execution" ? shell.output : "", "TOOL_OK");
        assert.isDefined(
          first.turnItems.find((item) => item.type === "dynamic_tool" && item.toolName === "read"),
        );
        assert.isAbove(first.providerTurns[0]?.tokenUsage?.maxTokens ?? 0, 0);

        yield* send(
          threadId,
          "stop",
          "Run the shell command `sleep 60 && echo LATE` with the shell tool in the foreground (not in the background) and wait for it to finish, then reply DONE.",
        );
        const running = yield* waitFor(threadId, (projection) =>
          projection.turnItems.some(
            (item) =>
              item.type === "command_execution" &&
              item.status === "running" &&
              item.input.includes("sleep 60"),
          ),
        );
        const secondRun = running.runs.at(-1)!;
        yield* orchestrator.dispatch({
          type: "run.interrupt",
          commandId: CommandId.make("command:opencode2-live:interrupt"),
          threadId,
          runId: secondRun.id,
        });
        const stopped = yield* waitFor(threadId, settled);
        assert.deepEqual(
          stopped.runs.map((run) => run.status),
          ["completed", "interrupted"],
        );
        const sleep = stopped.turnItems.find(
          (item) => item.type === "command_execution" && item.input.includes("sleep 60"),
        );
        assert.equal(sleep?.status, "interrupted");

        // A model change applies to the same native session on the next turn.
        const switched: ModelSelection = { instanceId: INSTANCE, model: SWITCHED_MODEL };
        yield* send(threadId, "switch", "Reply with exactly: SWITCHED", switched);
        const third = yield* waitFor(
          threadId,
          (projection) => projection.runs.length === 3 && settled(projection),
        );
        assert.equal(third.runs.at(-1)?.status, "completed");
        const sessionId = third.providerThreads[0]?.nativeThreadRef?.nativeId;
        assert.isDefined(sessionId);
        const models = yield* assistantModels(sessionId!);
        assert.equal(models.at(-1), SWITCHED_MODEL);
        assert.notEqual(models[0], SWITCHED_MODEL);
      }).pipe(Effect.provide(Layer.merge(liveLayer, NodeServices.layer)), Effect.scoped),
    360_000,
  );

  it.live(
    "asks before each shell command in Supervised, runs the approved one, skips the declined one, and answers a question",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        yield* EffectWorker.runDaemonWithOptions({ concurrency: 2 }).pipe(Effect.forkScoped);
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const threadId = ThreadId.make("thread:opencode2-live-supervised");
        yield* orchestrator.dispatch({
          type: "thread.create",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make("command:opencode2-live:supervised-create"),
          threadId,
          projectId: ProjectId.make("project:opencode2-live"),
          title: "OpenCode 2 live supervised",
          modelSelection: MODEL,
          runtimeMode: "approval-required",
          interactionMode: "default",
          branch: null,
          worktreePath: `${ROOT}/work`,
        });
        const pendingAsk = (projection: OrchestrationV2ThreadProjection) =>
          projection.runtimeRequests.find((request) => request.status === "pending");
        const answer = (
          key: string,
          request: OrchestrationV2ThreadProjection["runtimeRequests"][number],
          response: { decision: "accept" | "decline" } | { answers: Record<string, string> },
        ) =>
          orchestrator.dispatch({
            type: "runtime-request.respond",
            commandId: CommandId.make(`command:opencode2-live:${key}`),
            threadId,
            requestId: request.id,
            ...response,
          });

        yield* send(
          threadId,
          "approve",
          "Run the shell command `touch approved.txt` with the shell tool, then reply DONE.",
        );
        const asked = yield* waitFor(
          threadId,
          (projection) => pendingAsk(projection) !== undefined,
        );
        assert.equal(pendingAsk(asked)?.kind, "command");
        yield* answer("approve-answer", pendingAsk(asked)!, { decision: "accept" });
        const approved = yield* waitFor(threadId, settled);
        assert.equal(approved.runs.at(-1)?.status, "completed");
        assert.isTrue(yield* fs.exists(path.join(ROOT, "work", "approved.txt")));

        yield* send(
          threadId,
          "decline",
          "Run the shell command `touch declined.txt` with the shell tool, then reply DONE.",
        );
        const second = yield* waitFor(
          threadId,
          (projection) => projection.runs.length === 2 && pendingAsk(projection) !== undefined,
        );
        yield* answer("decline-answer", pendingAsk(second)!, { decision: "decline" });
        // The model may try again; every retry is declined too.
        let declined = yield* waitFor(
          threadId,
          (projection) =>
            (projection.runs.length === 2 && settled(projection)) ||
            pendingAsk(projection) !== undefined,
        );
        for (let retry = 0; pendingAsk(declined) !== undefined && retry < 3; retry += 1) {
          yield* answer(`decline-retry-${retry}`, pendingAsk(declined)!, { decision: "decline" });
          declined = yield* waitFor(
            threadId,
            (projection) =>
              (projection.runs.length === 2 && settled(projection)) ||
              pendingAsk(projection) !== undefined,
          );
        }
        assert.equal(declined.runs.at(-1)?.status, "completed");
        assert.isFalse(yield* fs.exists(path.join(ROOT, "work", "declined.txt")));

        yield* send(
          threadId,
          "question",
          "Before doing anything, use the question tool to ask me which color I prefer, offering the options red and blue. After I answer, reply with only the chosen color.",
        );
        const questioned = yield* waitFor(
          threadId,
          (projection) => projection.runs.length === 3 && pendingAsk(projection) !== undefined,
        );
        const question = pendingAsk(questioned)!;
        assert.equal(question.kind, "user_input");
        const form = questioned.turnItems.find(
          (item) => item.type === "user_input_request" && item.requestId === question.id,
        );
        const firstQuestion = form?.type === "user_input_request" ? form.questions[0] : undefined;
        assert.isDefined(firstQuestion);
        yield* answer("question-answer", question, { answers: { [firstQuestion!.id]: "Blue" } });
        const answered = yield* waitFor(
          threadId,
          (projection) => projection.runs.length === 3 && settled(projection),
        );
        assert.equal(answered.runs.at(-1)?.status, "completed");
        const reply = answered.messages.findLast((message) => message.role === "assistant");
        assert.match(reply?.text ?? "", /blue/i);
      }).pipe(Effect.provide(Layer.merge(liveLayer, NodeServices.layer)), Effect.scoped),
    600_000,
  );

  it.live(
    "lets plan mode write only the plan agent's plan directory under Full access",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        yield* EffectWorker.runDaemonWithOptions({ concurrency: 2 }).pipe(Effect.forkScoped);
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const threadId = ThreadId.make("thread:opencode2-live-plan");
        yield* orchestrator.dispatch({
          type: "thread.create",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make("command:opencode2-live:plan-create"),
          threadId,
          projectId: ProjectId.make("project:opencode2-live"),
          title: "OpenCode 2 live plan",
          modelSelection: MODEL,
          runtimeMode: "full-access",
          interactionMode: "plan",
          branch: null,
          worktreePath: `${ROOT}/work`,
        });
        // The plan agent's directory: `$HOME/.opencode/plan` under the isolated HOME.
        const planDir = path.join(ROOT, ".opencode", "plan");
        // No agent switch yet (a later layer adds it), so the build agent runs
        // under the session's plan rules: the edit deny holds on its own.
        yield* send(
          threadId,
          "plan",
          `This is a permission test. Do exactly these two tool calls and nothing else: 1) use the write tool to create ${planDir}/probe-plan.md containing PLAN_OK; 2) use the write tool to create plan_write_probe.txt in the current directory containing NO. Then reply with which calls succeeded.`,
        );
        const planned = yield* waitFor(threadId, settled);
        assert.equal(planned.runs.at(-1)?.status, "completed");
        assert.isFalse(yield* fs.exists(path.join(ROOT, "work", "plan_write_probe.txt")));
        assert.isTrue(yield* fs.exists(path.join(planDir, "probe-plan.md")));
      }).pipe(Effect.provide(Layer.merge(liveLayer, NodeServices.layer)), Effect.scoped),
    360_000,
  );
});
