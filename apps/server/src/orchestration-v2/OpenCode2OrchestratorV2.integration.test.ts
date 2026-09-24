import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  type ModelSelection,
  type OrchestrationV2Command,
  type OrchestrationV2Run,
  ProjectId,
  type ProviderInteractionMode,
  ProviderInstanceId,
  type ProviderReplayEntry,
  type RuntimeMode,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import {
  OPENCODE2_HTTP_PROTOCOL,
  OpenCode2OrchestratorReplayHarness,
} from "./Adapters/OpenCode2AdapterV2.testkit.ts";
import { OPENCODE_PROVIDER } from "@t3tools/provider-opencode/testing";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import { ORCHESTRATOR_REPLAY_FIXTURES } from "./testkit/fixtures/index.ts";
import { materializeFixtureInput } from "./testkit/fixtures/shared.ts";
import { materializeReplayTranscriptRuntimeInstructions } from "./testkit/ReplayRuntimeInstructions.ts";
import { provideDeterministicTestRuntime } from "./testkit/DeterministicRuntime.ts";
import type { OrchestratorV2ScenarioStep } from "./testkit/OrchestratorScenario.ts";
import { runOrchestratorV2ProviderReplayScenario } from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import { readProviderReplayTranscript } from "@t3tools/provider-testing/replayTranscript";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";

const SESSION = "ses_f148ca2deffeJcwCnRQtb0YFNX";

const FIRST_TURN_END = "first-turn-end";

const BACKGROUND_HOLD = "background-hold";

const BACKGROUND_PARENT = "ses_f1485cda4ffeXuc6GjAU9vDiRb";

const BACKGROUND_CHILD = "ses_f1485c529ffea4URrYruwEg0Ja";
const BACKGROUND_PROMPT =
  "Use the subagent tool with background enabled to delegate to the general subagent with the prompt: 'Run the shell command `sleep 20` with the bash tool and then reply exactly CHILD_OK.' As soon as it is launched, reply exactly PARENT_OK and end your turn without waiting for it.";
const instanceId = ProviderInstanceId.make("opencode");
const bigPickle: ModelSelection = { instanceId, model: "opencode/big-pickle" };
const mimo: ModelSelection = { instanceId, model: "opencode/mimo-v2.6-flash-free" };

const out = (type: string, input?: unknown): ProviderReplayEntry => ({
  type: "expect_outbound",
  frame: input === undefined ? { type } : { type, input },
});
const reply = (operation: string, data: unknown): ProviderReplayEntry => ({
  type: "emit_inbound",
  frame: { type: "sdk.response", operation, data },
});
const event = (type: string, data: Record<string, unknown>): ProviderReplayEntry => ({
  type: "emit_inbound",
  frame: {
    type: "sdk.event",
    event: { id: `evt_${type.replaceAll(".", "")}`, created: 1, type, data },
  },
});
const labelled = (entry: ProviderReplayEntry, label: string): ProviderReplayEntry =>
  entry.type === "runtime_exit" ? entry : { ...entry, label };
/** MCP rules are last-match-wins; allow this thread after denying all T3 servers. */
const mcpRules = (name: string) => [
  { action: "t3-code-*", resource: "*", effect: "deny" },
  { action: `t3-code-thread_${name}_*`, resource: "*", effect: "allow" },
];
const FULL_ACCESS = [{ action: "*", resource: "*", effect: "allow" }];

const t3Rules = (name: string) => [...FULL_ACCESS, ...mcpRules(name)];

const BUILD_PATHS = [
  {
    action: "external_directory",
    resource: "/home/.local/share/opencode/tool-output/*",
    effect: "allow",
  },
];
const PLAN_PATHS = [
  ...BUILD_PATHS,
  { action: "edit", resource: "/home/.opencode/plan/*", effect: "allow" },
  { action: "external_directory", resource: "/home/.opencode/plan/*", effect: "allow" },
];
const agentInfo = (id: string, description: string, permissions: ReadonlyArray<unknown>) => ({
  id,
  name: id === "plan" ? "Plan" : "Build",
  request: { settings: {}, headers: {}, body: {} },
  description,
  mode: "primary",
  hidden: false,
  permissions,
});

const agentList = (directory: string) => ({
  location: { directory },
  data: [
    agentInfo("build", "The default agent.", [
      { action: "*", resource: "*", effect: "allow" },
      { action: "external_directory", resource: "*", effect: "ask" },
      ...BUILD_PATHS,
    ]),
    agentInfo("plan", "Read-only agent for planning.", [
      { action: "*", resource: "*", effect: "allow" },
      { action: "external_directory", resource: "*", effect: "ask" },
      ...BUILD_PATHS,
      { action: "edit", resource: "*", effect: "deny" },
      ...PLAN_PATHS.slice(BUILD_PATHS.length),
    ]),
  ],
});

const autoEditRules = (name: string) => [
  { action: "shell", resource: "*", effect: "ask" },
  { action: "edit", resource: "*", effect: "allow" },
  { action: "external_directory", resource: "*", effect: "ask" },
  ...BUILD_PATHS,
  ...mcpRules(name),
];

const planRules = (name: string) => [
  { action: "*", resource: "*", effect: "allow" },
  { action: "edit", resource: "*", effect: "deny" },
  ...PLAN_PATHS,
  ...mcpRules(name),
];

const sessionInfo = (directory: string, permissions: ReadonlyArray<unknown>, title: string) => ({
  data: {
    id: SESSION,
    title,
    projectID: "global",
    model: { id: "big-pickle", providerID: "opencode", variant: "default" },
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    time: { created: 1790656601394, updated: 1790656601394 },
    location: { directory },
    permissions,
  },
});

const instructionsWritten: ReadonlyArray<ProviderReplayEntry> = [
  out("session.instructions.entry.put", { sessionID: SESSION, key: "t3-code", value: "<any>" }),
  reply("session.instructions.entry.put", null),
];

const answeredPrompt = (text: string): ReadonlyArray<ProviderReplayEntry> => [
  out("session.prompt", { sessionID: SESSION, text: "<any>" }),
  reply("session.prompt", {
    data: {
      id: `msg_user_${text}`,
      sessionID: SESSION,
      time: { created: 1790656601410 },
      type: "user",
      payload: { text: "<prompt>" },
      delivery: "steer",
    },
  }),
  event("session.text.ended", {
    sessionID: SESSION,
    assistantMessageID: `msg_assistant_${text}`,
    ordinal: 0,
    text,
  }),
  event("session.execution.succeeded", { sessionID: SESSION }),
];

/** A `/api/model` entry as 2.0.18 lists it; a known window means no re-read after a turn. */
const catalogModel = (id: string, name: string) => ({
  id,
  modelID: id,
  providerID: "opencode",
  family: id,
  name,
  compatibility: { reasoningField: "reasoning_content" },
  package: "@opencode/ai/providers/openai-compatible",
  settings: { apiKey: "public", baseURL: "https://opencode.ai/zen/v1", provider: "opencode" },
  capabilities: { tools: true, input: ["text"], output: ["text"] },
  variants: [],
  time: { released: 1760659200000 },
  cost: [{ input: 0, output: 0, cache: { read: 0, write: 0 } }],
  status: "active",
  enabled: true,
  limit: { context: 200000, input: 160000, output: 32000 },
});
const createdSession = (
  directory: string,
  name: string,
  permissions: ReadonlyArray<unknown> = t3Rules(name),

  narrows = false,
): ReadonlyArray<ProviderReplayEntry> => [
  out("event.subscribe"),
  out("model.list", "<any>"),
  reply("model.list", {
    location: { directory },
    data: [
      catalogModel("big-pickle", "Big Pickle"),
      catalogModel("mimo-v2.6-flash-free", "MiMo V2.6 Flash Free"),
    ],
  }),
  ...(narrows ? [out("agent.list", "<any>"), reply("agent.list", agentList(directory))] : []),

  out("session.create", { location: { directory }, model: "<any>", permissions, title: name }),
  reply("session.create", sessionInfo(directory, permissions, name)),
];

const withDirectory = <T>(value: T, directory: string): T => {
  const replace = (entry: unknown): unknown =>
    entry === "<work>"
      ? directory
      : Array.isArray(entry)
        ? entry.map(replace)
        : typeof entry === "object" && entry !== null
          ? Object.fromEntries(Object.entries(entry).map(([key, inner]) => [key, replace(inner)]))
          : entry;
  return replace(value) as T;
};

const threadCommands = (input: {
  readonly name: string;
  readonly worktreePath: string;
  readonly runtimeMode?: RuntimeMode;
  readonly interactionMode?: ProviderInteractionMode;
}) => {
  const threadId = ThreadId.make(`thread:${input.name}`);
  const command = (key: string) => CommandId.make(`command:${input.name}:${key}`);
  return {
    threadId,
    create: {
      type: "thread.create",
      createdBy: "user",
      creationSource: "web",
      commandId: command("create"),
      threadId,
      projectId: ProjectId.make(`project:${input.name}`),
      title: input.name,
      modelSelection: bigPickle,
      runtimeMode: input.runtimeMode ?? "full-access",
      interactionMode: input.interactionMode ?? "default",
      branch: null,
      worktreePath: input.worktreePath,
    } satisfies OrchestrationV2Command,
    message: (key: string, modelSelection: ModelSelection = bigPickle, text?: string) =>
      ({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: command(key),
        threadId,
        messageId: MessageId.make(`message:${input.name}:${key}`),
        text: text ?? `Reply with exactly: ${key}`,
        attachments: [],
        modelSelection,
        dispatchMode: { type: "start_immediately" },
      }) satisfies OrchestrationV2Command,
    interactionMode: (key: string, interactionMode: "default" | "plan") =>
      ({
        type: "thread.interaction-mode.set",
        commandId: command(key),
        threadId,
        interactionMode,
      }) satisfies OrchestrationV2Command,
    command,
  };
};

const runScenario = (input: {
  readonly name: string;
  readonly threadId: ThreadId;
  readonly entries: ReadonlyArray<ProviderReplayEntry>;
  readonly commands: ReadonlyArray<OrchestrationV2Command>;
}) =>
  Effect.gen(function* () {
    const transcript = yield* OpenCode2OrchestratorReplayHarness.decodeTranscript({
      provider: OPENCODE_PROVIDER,
      protocol: OPENCODE2_HTTP_PROTOCOL,
      version: "2.0.18",
      scenario: input.name,
      entries: input.entries,
    });
    const steps = input.commands.flatMap((command): Array<OrchestratorV2ScenarioStep> => [
      { type: "dispatch", command },
      { type: "advance_clock", duration: "1 millis" },
      ...(command.type === "message.dispatch"
        ? [{ type: "await_thread_idle" as const, threadId: input.threadId }]
        : []),
    ]);
    const result = yield* runOrchestratorV2ProviderReplayScenario(
      { name: input.name, transcript, commands: input.commands, steps },
      OpenCode2OrchestratorReplayHarness,
    ).pipe(provideDeterministicTestRuntime);
    const projection = result.projections.get(input.threadId);
    assert.isDefined(projection);
    return projection;
  });

const FIXTURE_REVERT = "opencode2_revert";
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

describe("OpenCode 2 through the orchestrator", () => {
  it.effect.each(["message", "thread settings"] as const)(
    "switches the session's model before the next prompt when changed from the %s",
    (via) =>
      Effect.gen(function* () {
        const name = `opencode2-model-switch-${via.replace(" ", "-")}`;
        const cwd = yield* checkpointWorkspace(name);
        const thread = threadCommands({ name, worktreePath: cwd });
        const projection = yield* runScenario({
          name,
          threadId: thread.threadId,
          entries: [
            ...createdSession(cwd, name),
            ...instructionsWritten,
            ...answeredPrompt("FIRST"),

            out("session.get", { sessionID: SESSION }),
            reply("session.get", sessionInfo(cwd, t3Rules(name), name)),
            out("session.switchModel", {
              sessionID: SESSION,
              model: { providerID: "opencode", id: "mimo-v2.6-flash-free" },
            }),
            reply("session.switchModel", null),

            ...instructionsWritten,
            ...answeredPrompt("SECOND"),
          ],
          commands: [
            thread.create,
            thread.message("first"),
            ...(via === "thread settings"
              ? [
                  {
                    type: "thread.model-selection.set",
                    commandId: thread.command("model"),
                    threadId: thread.threadId,
                    modelSelection: mimo,
                  } satisfies OrchestrationV2Command,
                ]
              : []),
            thread.message("second", mimo),
          ],
        });
        assert.deepEqual(
          projection.runs.map((run) => [run.status, run.modelSelection.model]),
          [
            ["completed", bigPickle.model],
            ["completed", mimo.model],
          ],
        );

        assert.lengthOf(projection.providerThreads, 1);
      }).pipe(Effect.scoped),
  );

  it.effect("rewrites the session's rules when the thread's mode changes between turns", () =>
    Effect.gen(function* () {
      const name = "opencode2-mode-change";
      const cwd = yield* checkpointWorkspace(name);
      const thread = threadCommands({ name, worktreePath: cwd });
      const setMode = (key: string, runtimeMode: RuntimeMode) =>
        ({
          type: "thread.runtime-mode.set",
          commandId: thread.command(key),
          threadId: thread.threadId,
          runtimeMode,
        }) satisfies OrchestrationV2Command;
      const projection = yield* runScenario({
        name,
        threadId: thread.threadId,
        entries: [
          ...createdSession(cwd, name),
          ...instructionsWritten,
          ...answeredPrompt("FIRST"),

          out("session.get", { sessionID: SESSION }),
          reply("session.get", sessionInfo(cwd, t3Rules(name), name)),
          out("agent.list", "<any>"),
          reply("agent.list", agentList(cwd)),
          out("session.update", { sessionID: SESSION, permissions: autoEditRules(name) }),
          reply("session.update", null),
          ...answeredPrompt("SECOND"),

          out("session.get", { sessionID: SESSION }),
          reply("session.get", sessionInfo(cwd, autoEditRules(name), name)),
          out("session.update", { sessionID: SESSION, permissions: t3Rules(name) }),
          reply("session.update", null),
          ...answeredPrompt("THIRD"),
        ],
        commands: [
          thread.create,
          thread.message("first"),
          setMode("auto-edit", "auto-accept-edits"),
          thread.message("second"),
          setMode("full", "full-access"),
          thread.message("third"),
        ],
      });
      assert.deepEqual(
        projection.runs.map((run) => run.status),
        ["completed", "completed", "completed"],
      );
      assert.lengthOf(projection.providerThreads, 1);
    }).pipe(Effect.scoped),
  );

  it.effect("denies edits outside the plan directory in plan mode and lifts it after", () =>
    Effect.gen(function* () {
      const name = "opencode2-plan-rules";
      const cwd = yield* checkpointWorkspace(name);
      const thread = threadCommands({ name, worktreePath: cwd, interactionMode: "plan" });
      const projection = yield* runScenario({
        name,
        threadId: thread.threadId,
        entries: [
          ...createdSession(cwd, name, planRules(name), true),

          out("session.switchAgent", { sessionID: SESSION, agent: "plan" }),
          reply("session.switchAgent", null),
          ...instructionsWritten,
          ...answeredPrompt("PLANNED"),
          out("session.get", { sessionID: SESSION }),
          reply("session.get", sessionInfo(cwd, planRules(name), name)),
          out("session.update", { sessionID: SESSION, permissions: t3Rules(name) }),
          reply("session.update", null),
          out("session.switchAgent", { sessionID: SESSION, agent: "build" }),
          reply("session.switchAgent", null),
          ...answeredPrompt("BUILT"),
        ],
        commands: [
          thread.create,
          thread.message("plan"),
          {
            type: "thread.interaction-mode.set",
            commandId: thread.command("default"),
            threadId: thread.threadId,
            interactionMode: "default",
          },
          thread.message("build"),
        ],
      });
      assert.deepEqual(
        projection.runs.map((run) => run.status),
        ["completed", "completed"],
      );
    }).pipe(Effect.scoped),
  );

  it.effect("forks from an earlier turn before the next turn's user message", () =>
    Effect.gen(function* () {
      const name = "opencode2-fork";
      const cwd = yield* checkpointWorkspace(name);
      const recorded = yield* readProviderReplayTranscript(
        new URL("./testkit/fixtures/opencode2_fork/opencode_transcript.ndjson", import.meta.url),
      );

      const transcript = yield* OpenCode2OrchestratorReplayHarness.decodeTranscript(
        withDirectory(recorded, cwd),
      );
      const source = threadCommands({ name, worktreePath: cwd });
      const target = ThreadId.make(`thread:${name}:target`);
      const [one, two] = [source.message("one"), source.message("two")];
      const commands: ReadonlyArray<OrchestrationV2Command> = [
        source.create,
        one,
        two,
        {
          type: "thread.fork",
          createdBy: "user",
          creationSource: "web",
          commandId: source.command("fork"),
          sourceThreadId: source.threadId,
          targetThreadId: target,
          sourcePoint: {
            type: "run",
            runId: (yield* IdAllocator.IdAllocatorV2).derive.run({
              threadId: source.threadId,
              ordinal: 1,
            }),
          },
        },
        {
          ...source.message("repeat"),
          threadId: target,
          messageId: MessageId.make(`message:${name}:repeat`),
        },
      ];
      const steps: Array<OrchestratorV2ScenarioStep> = commands.flatMap((command) => [
        { type: "dispatch" as const, command },
        { type: "advance_clock" as const, duration: "1 millis" as const },
        ...(command.type === "message.dispatch"
          ? [{ type: "await_thread_idle" as const, threadId: command.threadId }]
          : []),
      ]);
      const result = yield* runOrchestratorV2ProviderReplayScenario(
        { name, transcript, commands, steps, projectionThreadIds: [source.threadId, target] },
        OpenCode2OrchestratorReplayHarness,
      ).pipe(provideDeterministicTestRuntime);
      const forked = result.projections.get(target);
      assert.isDefined(forked);
      assert.equal(forked.contextTransfers[0]?.resolution?.strategy, "native_fork");
      assert.equal(
        forked.providerThreads[0]?.nativeThreadRef?.nativeId,
        recorded.metadata?.["forkedNativeSessionId"],
      );

      assert.deepEqual(
        forked.runs.map((run) => run.status),
        ["completed"],
      );
      const visible = forked.visibleTurnItems.flatMap((row) =>
        row.item.type === "assistant_message" ? [row.item.text] : [],
      );
      assert.deepEqual(visible, ["ONE", "ONE"]);
    }).pipe(Effect.scoped, Effect.provide(Layer.merge(NodeServices.layer, IdAllocator.layer))),
  );

  it.effect("never sends OpenCode a queued message that was cancelled", () =>
    Effect.gen(function* () {
      const name = "opencode2-queued-cancel";
      const cwd = yield* checkpointWorkspace(name);
      const thread = threadCommands({ name, worktreePath: cwd });
      const queued = (key: string): OrchestrationV2Command => ({
        ...thread.message(key),
        dispatchMode: { type: "queue_after_active" },
      });
      const ids = yield* IdAllocator.IdAllocatorV2;
      const run = (ordinal: number) => ids.derive.run({ threadId: thread.threadId, ordinal });

      const [first, second, third] = [thread.message("first"), queued("second"), queued("third")];
      const cancel: OrchestrationV2Command = {
        type: "queued-run.cancel",
        commandId: thread.command("cancel"),
        threadId: thread.threadId,
        runId: run(2),
      };
      const transcript = yield* OpenCode2OrchestratorReplayHarness.decodeTranscript({
        provider: OPENCODE_PROVIDER,
        protocol: OPENCODE2_HTTP_PROTOCOL,
        version: "2.0.18",
        scenario: name,
        entries: [
          ...createdSession(cwd, name),
          ...instructionsWritten,
          out("session.prompt", { sessionID: SESSION, id: "<any>", text: "<any>" }),
          reply("session.prompt", {
            data: {
              id: "msg_user_FIRST",
              sessionID: SESSION,
              time: { created: 1 },
              type: "user",
              payload: { text: "<prompt>" },
              delivery: "steer",
            },
          }),

          ...answeredPrompt("THIRD"),
        ],
      });
      const steps: Array<OrchestratorV2ScenarioStep> = [
        { type: "dispatch", command: thread.create },
        { type: "advance_clock", duration: "1 millis" },
        { type: "dispatch", command: first, await: false, key: "first" },
        { type: "await_run_steerable", threadId: thread.threadId, runId: run(1) },
        { type: "dispatch", command: second },
        { type: "dispatch", command: third },
        { type: "dispatch", command: cancel },
        { type: "await_run_status", threadId: thread.threadId, runId: run(2), status: "cancelled" },
        { type: "release_replay_gate", label: FIRST_TURN_END },
        { type: "await", key: "first" },
        { type: "await_thread_idle", threadId: thread.threadId },
      ];
      const result = yield* runOrchestratorV2ProviderReplayScenario(
        {
          name,
          transcript: {
            ...transcript,
            entries: [
              ...transcript.entries.slice(0, -4),
              labelled(
                event("session.execution.succeeded", { sessionID: SESSION }),
                FIRST_TURN_END,
              ),
              ...transcript.entries.slice(-4),
            ],
          },
          commands: [thread.create, first, second, third, cancel],
          steps,
        },
        OpenCode2OrchestratorReplayHarness,
      ).pipe(provideDeterministicTestRuntime);
      const projection = result.projections.get(thread.threadId);
      assert.isDefined(projection);
      assert.deepEqual(
        projection.runs.map((candidate) => candidate.status),
        ["completed", "cancelled", "completed"],
      );
    }).pipe(Effect.scoped, Effect.provide(Layer.merge(NodeServices.layer, IdAllocator.layer))),
  );

  const backgroundReconnect = Effect.fn("backgroundReconnect")(function* (input: {
    readonly name: string;
    readonly cut: string;
    readonly hold?: string;
    readonly reconnect: (parent: string) => ReadonlyArray<ProviderReplayEntry>;
    readonly steps: (
      thread: ReturnType<typeof threadCommands>,
      run: (ordinal: number) => OrchestrationV2Run["id"],
    ) => ReadonlyArray<OrchestratorV2ScenarioStep>;
    readonly commands: (
      thread: ReturnType<typeof threadCommands>,
    ) => ReadonlyArray<OrchestrationV2Command>;
  }) {
    const cwd = yield* checkpointWorkspace(input.name);
    const recorded = yield* readProviderReplayTranscript(
      new URL(
        "./testkit/fixtures/opencode2_background/opencode_transcript.ndjson",
        import.meta.url,
      ),
    );
    const cut = recorded.entries.findIndex(
      (entry) => entry.type !== "runtime_exit" && entry.label === input.cut,
    );
    assert.isAtLeast(cut, 0);
    const kept = recorded.entries
      .slice(0, cut)
      .map((entry) =>
        entry.type !== "runtime_exit" && entry.label === input.hold
          ? labelled(entry, BACKGROUND_HOLD)
          : entry,
      );
    const transcript = yield* OpenCode2OrchestratorReplayHarness.decodeTranscript({
      ...recorded,
      scenario: input.name,
      entries: [
        ...kept,
        { type: "runtime_exit", status: "success" },
        ...input.reconnect(BACKGROUND_PARENT),
      ],
    });
    const thread = threadCommands({ name: input.name, worktreePath: cwd });
    const ids = yield* IdAllocator.IdAllocatorV2;
    const run = (ordinal: number) => ids.derive.run({ threadId: thread.threadId, ordinal });
    const result = yield* runOrchestratorV2ProviderReplayScenario(
      {
        name: input.name,
        transcript,
        commands: input.commands(thread),
        steps: input.steps(thread, run),
      },
      OpenCode2OrchestratorReplayHarness,

      { runContinuationWorker: true },
    ).pipe(provideDeterministicTestRuntime);
    const projection = result.projections.get(thread.threadId);
    assert.isDefined(projection);
    return { result, projection };
  });

  it.effect("backfills a follow-up whose stream dropped mid-run and ends it completed", () =>
    Effect.gen(function* () {
      const { result, projection } = yield* backgroundReconnect({
        name: "opencode2-follow-up-reconnect",

        cut: "session.step.streamed.5",
        hold: "session.text.ended.3",
        reconnect: (parent) => [
          out("event.subscribe"),
          out("session.active"),
          reply("session.active", { data: {} }),

          out("message.list", { sessionID: parent, order: "desc", limit: "50" }),
          reply("message.list", {
            data: [
              {
                id: "msg_0eb7a99c0001idleFollowUp00",
                time: { created: 5 },
                type: "idle",
                outcome: "succeeded",
              },
              {
                id: "msg_0eb7a96bb00170K1f1HJUeBNwF",
                time: { created: 4 },
                type: "assistant",
                agent: "build",
                model: { id: "big-pickle", providerID: "opencode", variant: "default" },
                content: [
                  {
                    type: "text",
                    text: "The background subagent finished and returned `CHILD_OK`.",
                  },
                ],
                finish: "stop",
              },
              {
                id: "msg_0eb7a3aee0015KmTB8XkMDW8iy",
                time: { created: 3 },
                type: "synthetic",
                text: "<subagent>CHILD_OK</subagent>",
              },
              {
                id: "msg_0eb7a3e3c001idleParentTurn0",
                time: { created: 2 },
                type: "idle",
                outcome: "succeeded",
              },
            ],
            cursor: {},
          }),
        ],
        commands: (thread) => [
          thread.create,
          thread.message("start", bigPickle, BACKGROUND_PROMPT),
        ],
        steps: (thread, run) => [
          { type: "dispatch", command: thread.create },
          { type: "advance_clock", duration: "1 millis" },

          {
            type: "dispatch",
            command: thread.message("start", bigPickle, BACKGROUND_PROMPT),
            await: false,
            key: "start",
          },
          { type: "advance_clock", duration: "1 millis" },

          { type: "await_run_steerable", threadId: thread.threadId, runId: run(2) },
          { type: "release_replay_gate", label: BACKGROUND_HOLD },
          {
            type: "await_run_status",
            threadId: thread.threadId,
            runId: run(2),
            status: "completed",
          },
          { type: "await", key: "start" },
          { type: "await_thread_idle", threadId: thread.threadId },
        ],
      });
      assert.deepEqual(
        projection.runs.map((candidate) => candidate.status),
        ["completed", "completed"],
      );
      const followUp = projection.runs[1];
      assert.deepEqual(
        projection.turnItems.flatMap((item) =>
          item.runId === followUp?.id && item.type === "assistant_message" ? [item.text] : [],
        ),
        ["The background subagent finished and returned `CHILD_OK`."],
      );
      const shell = result.shellSnapshot.threads.find((row) => row.id === projection.thread.id);
      assert.deepEqual(shell?.pendingBackgroundTasks ?? [], []);
    }).pipe(Effect.scoped, Effect.provide(Layer.merge(NodeServices.layer, IdAllocator.layer))),
  );

  it.effect(
    "settles a background subagent whose end was lost with the stream, and takes the next turn",
    () =>
      Effect.gen(function* () {
        const { result, projection } = yield* backgroundReconnect({
          name: "opencode2-background-lost",

          cut: "session.step.started.3",
          reconnect: (parent) => [
            out("event.subscribe"),

            out("session.active"),
            reply("session.active", { data: {} }),

            out("message.list", { sessionID: BACKGROUND_CHILD, order: "desc", limit: "50" }),
            reply("message.list", {
              data: [
                {
                  id: "msg_0eb7a99b0001idleChildTurn0",
                  time: { created: 4 },
                  type: "idle",
                  outcome: "succeeded",
                },
                {
                  id: "msg_0eb7a916c001NRJQJ5gwIT0QnU",
                  time: { created: 3 },
                  type: "assistant",
                  agent: "general",
                  model: { id: "big-pickle", providerID: "opencode", variant: "default" },
                  content: [{ type: "text", text: "CHILD_OK" }],
                  finish: "stop",
                },
                {
                  id: "msg_0eb7a3aea001Lw1Pav5d4akfEg",
                  time: { created: 2 },
                  type: "user",
                  text: "Run the shell command `sleep 20` with the bash tool and then reply exactly CHILD_OK.",
                },
              ],
              cursor: {},
            }),
            out("session.prompt", { sessionID: parent, id: "<any>", text: "<any>" }),
            reply("session.prompt", {
              data: {
                id: "msg_next",
                sessionID: parent,
                time: { created: 9 },
                type: "user",
                payload: { text: "<prompt>" },
                delivery: "steer",
              },
            }),
            event("session.execution.started", { sessionID: parent }),
            event("session.text.ended", {
              sessionID: parent,
              assistantMessageID: "msg_next_reply",
              ordinal: 0,
              text: "NEXT",
            }),
            event("session.execution.succeeded", { sessionID: parent }),
          ],
          commands: (thread) => [
            thread.create,
            thread.message("start", bigPickle, BACKGROUND_PROMPT),
            thread.message("next"),
          ],
          steps: (thread, run) => [
            { type: "dispatch", command: thread.create },
            { type: "advance_clock", duration: "1 millis" },
            { type: "dispatch", command: thread.message("start", bigPickle, BACKGROUND_PROMPT) },
            { type: "advance_clock", duration: "1 millis" },
            { type: "await_thread_idle", threadId: thread.threadId },
            { type: "dispatch", command: thread.message("next") },
            { type: "advance_clock", duration: "1 millis" },
            {
              type: "await_run_status",
              threadId: thread.threadId,
              runId: run(2),
              status: "completed",
            },
            { type: "await_thread_idle", threadId: thread.threadId },
          ],
        });

        assert.deepEqual(
          projection.runs.map((candidate) => candidate.status),
          ["completed", "completed"],
        );
        assert.lengthOf(projection.subagents, 1);
        assert.deepInclude(projection.subagents[0], { status: "interrupted" });
        assert.include(projection.subagents[0]?.result ?? "", "lost its connection to OpenCode");
        assert.isFalse(
          projection.turnItems.some(
            (item) => item.status === "running" || item.status === "waiting",
          ),
        );
        const shell = result.shellSnapshot.threads.find((row) => row.id === projection.thread.id);
        assert.deepEqual(shell?.pendingBackgroundTasks ?? [], []);
      }).pipe(Effect.scoped, Effect.provide(Layer.merge(NodeServices.layer, IdAllocator.layer))),
  );

  it.effect(
    "settles a background subagent whose report OpenCode delivers into the launching turn",
    () =>
      Effect.gen(function* () {
        // OpenCode 2.0.18 can deliver a child report into the parent's still-running execution, which then answers it.

        const cwd = yield* checkpointWorkspace("opencode2-background-in-turn");
        const recorded = yield* readProviderReplayTranscript(
          new URL(
            "./testkit/fixtures/opencode2_background/opencode_transcript.ndjson",
            import.meta.url,
          ),
        );
        const byLabel = (label: string) => {
          const entry = recorded.entries.find(
            (candidate) => candidate.type !== "runtime_exit" && candidate.label === label,
          );
          assert.isDefined(entry);
          return entry!;
        };
        const parentEnd = recorded.entries.findIndex(
          (entry) => entry.type !== "runtime_exit" && entry.label === "session.execution.succeeded",
        );
        const childStart = recorded.entries.findIndex(
          (entry) => entry.type !== "runtime_exit" && entry.label === "session.step.started.3",
        );
        const transcript = yield* OpenCode2OrchestratorReplayHarness.decodeTranscript({
          ...recorded,
          scenario: "opencode2-background-in-turn",
          entries: [
            ...recorded.entries.slice(0, parentEnd),

            ...recorded.entries.slice(
              childStart,
              recorded.entries.indexOf(byLabel("session.inbox.enqueued.3")),
            ),

            byLabel("session.inbox.enqueued.3"),
            byLabel("session.inbox.delivered.3"),

            byLabel("session.text.ended.3"),
            byLabel("session.execution.succeeded.3"),
          ],
        });
        const thread = threadCommands({ name: "opencode2-background-in-turn", worktreePath: cwd });
        const ids = yield* IdAllocator.IdAllocatorV2;
        const result = yield* runOrchestratorV2ProviderReplayScenario(
          {
            name: "opencode2-background-in-turn",
            transcript,
            commands: [thread.create, thread.message("start", bigPickle, BACKGROUND_PROMPT)],
            steps: [
              { type: "dispatch", command: thread.create },
              { type: "advance_clock", duration: "1 millis" },
              { type: "dispatch", command: thread.message("start", bigPickle, BACKGROUND_PROMPT) },
              { type: "advance_clock", duration: "1 millis" },
              {
                type: "await_run_status",
                threadId: thread.threadId,
                runId: ids.derive.run({ threadId: thread.threadId, ordinal: 1 }),
                status: "completed",
              },
              { type: "await_thread_idle", threadId: thread.threadId },
            ],
          },
          OpenCode2OrchestratorReplayHarness,
          { runContinuationWorker: true },
        ).pipe(provideDeterministicTestRuntime);
        const projection = result.projections.get(thread.threadId);
        assert.isDefined(projection);

        assert.deepEqual(
          projection!.runs.map((candidate) => candidate.status),
          ["completed"],
        );
        assert.lengthOf(projection!.subagents, 1);
        assert.deepInclude(projection!.subagents[0], { status: "completed" });
        assert.include(projection!.subagents[0]?.result ?? "", "CHILD_OK");
        assert.isTrue(
          projection!.turnItems.some(
            (item) =>
              item.type === "assistant_message" && item.text.includes("finished and returned"),
          ),
        );
        assert.isFalse(
          projection!.turnItems.some(
            (item) => item.status === "running" || item.status === "waiting",
          ),
        );
        const shell = result.shellSnapshot.threads.find((row) => row.id === projection!.thread.id);
        assert.deepEqual(shell?.pendingBackgroundTasks ?? [], []);
      }).pipe(Effect.scoped, Effect.provide(Layer.merge(NodeServices.layer, IdAllocator.layer))),
  );

  // A rollback re-attaches a binding the session manager has no selection record
  // for, and OpenCode then holds a follow-up for a continuation. Changing the
  // selection or moving the thread must be refused before either is committed.
  it.effect("refuses a selection change while a rollback-attached binding holds a follow-up", () =>
    Effect.gen(function* () {
      const fixture = ORCHESTRATOR_REPLAY_FIXTURES.find(
        (candidate) => candidate.name === FIXTURE_REVERT,
      );
      const provider = fixture?.providers[0];
      if (fixture === undefined || provider === undefined)
        return yield* Effect.die("The opencode2_revert fixture has no OpenCode 2 run.");
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
        ...fixture.buildInput(),
        steps: fixture.buildInput().steps.filter((step) => step.type === "message"),
      };
      const workspace = yield* checkpointWorkspace(FIXTURE_REVERT);
      const transcript = yield* OpenCode2OrchestratorReplayHarness.decodeTranscript(
        materializeReplayTranscriptRuntimeInstructions(
          { ...recorded, scenario: `${FIXTURE_REVERT}-wake-admission`, entries },
          { driver: provider.driver, model: provider.modelSelection.model },
        ),
      );
      const materialized = yield* materializeFixtureInput({
        scenario: FIXTURE_REVERT,
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
                adapter: ProviderAdapter.ProviderAdapterV2["Service"],
              ): ProviderAdapter.ProviderAdapterV2["Service"] => ({
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
          name: `${FIXTURE_REVERT}/wake-admission`,
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
