/**
 * OpenCode 2 through the whole orchestrator, against a replayed HTTP server:
 * the transcript fixes the order of every request the adapter sends, so a
 * request the orchestrator never lets it make fails the run.
 */
import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  type ModelSelection,
  type OrchestrationV2Command,
  ProjectId,
  type ProviderInteractionMode,
  ProviderInstanceId,
  type ProviderReplayEntry,
  type RuntimeMode,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import {
  OPENCODE2_HTTP_PROTOCOL,
  OpenCode2OrchestratorReplayHarness,
} from "./Adapters/OpenCode2AdapterV2.testkit.ts";
import { OPENCODE_PROVIDER } from "./Adapters/OpenCodeAdapterV2.ts";
import { provideDeterministicTestRuntime } from "./testkit/DeterministicRuntime.ts";
import type { OrchestratorV2ScenarioStep } from "./testkit/OrchestratorScenario.ts";
import { runOrchestratorV2ProviderReplayScenario } from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "./testkit/ReplayFixtureWorkspace.ts";

const SESSION = "ses_f148ca2deffeJcwCnRQtb0YFNX";
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
const T3_RULES = [
  { action: "*", resource: "*", effect: "allow" },
  { action: "subagent", resource: "*", effect: "deny" },
];
/** Paths the build and plan agents allow for themselves, as 2.0.18 lists them. */
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
/** `/api/agent` trimmed to the two agents a T3 session runs. */
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
/** A session this runtime loads again (after a detach) waits on nothing. */
const noOpenRequests: ReadonlyArray<ProviderReplayEntry> = [
  out("permission.list", { sessionID: SESSION }),
  reply("permission.list", { data: [] }),
  out("session.form.list", { sessionID: SESSION }),
  reply("session.form.list", { data: [] }),
];
const SUPERVISED_RULES = [
  { action: "shell", resource: "*", effect: "ask" },
  { action: "edit", resource: "*", effect: "ask" },
  { action: "external_directory", resource: "*", effect: "ask" },
  ...BUILD_PATHS,
  { action: "subagent", resource: "*", effect: "deny" },
];
const AUTO_EDIT_RULES = [
  { action: "shell", resource: "*", effect: "ask" },
  { action: "edit", resource: "*", effect: "allow" },
  { action: "external_directory", resource: "*", effect: "ask" },
  ...BUILD_PATHS,
  { action: "subagent", resource: "*", effect: "deny" },
];
/** Plan mode on Full access: edits are denied except the plan agent's own plan files. */
const PLAN_RULES = [
  { action: "*", resource: "*", effect: "allow" },
  { action: "edit", resource: "*", effect: "deny" },
  ...PLAN_PATHS,
  { action: "subagent", resource: "*", effect: "deny" },
];

const sessionInfo = (directory: string, permissions: ReadonlyArray<unknown> = T3_RULES) => ({
  data: {
    id: SESSION,
    projectID: "global",
    model: { id: "big-pickle", providerID: "opencode", variant: "default" },
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    time: { created: 1790656601394, updated: 1790656601394 },
    location: { directory },
    permissions,
  },
});
/** One prompt the server accepts and answers with `text`. */
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
  permissions: ReadonlyArray<unknown> = T3_RULES,
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
  // Only a mode that narrows Full access reads the agents' own path rules.
  ...(permissions === T3_RULES
    ? []
    : [out("agent.list", "<any>"), reply("agent.list", agentList(directory))]),
  out("session.create", { location: { directory }, model: "<any>", permissions }),
  reply("session.create", sessionInfo(directory, permissions)),
];

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
    message: (key: string, modelSelection: ModelSelection = bigPickle) =>
      ({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: command(key),
        threadId,
        messageId: MessageId.make(`message:${input.name}:${key}`),
        text: `Reply with exactly: ${key}`,
        attachments: [],
        modelSelection,
        dispatchMode: { type: "start_immediately" },
      }) satisfies OrchestrationV2Command,
    command,
  };
};

/** Runs `commands` in order, letting the thread go idle after each message. */
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

describe("OpenCode 2 through the orchestrator", () => {
  for (const via of ["message", "thread settings"] as const) {
    it.effect(
      `switches the session's model before the next prompt when changed from the ${via}`,
      () =>
        Effect.gen(function* () {
          const name = `opencode2-model-switch-${via.replace(" ", "-")}`;
          const cwd = yield* checkpointWorkspace(name);
          const thread = threadCommands({ name, worktreePath: cwd });
          const projection = yield* runScenario({
            name,
            threadId: thread.threadId,
            entries: [
              ...createdSession(cwd),
              ...answeredPrompt("FIRST"),
              // The next turn resumes the session at its new selection.
              out("session.get", { sessionID: SESSION }),
              reply("session.get", sessionInfo(cwd)),
              out("session.switchModel", {
                sessionID: SESSION,
                model: { providerID: "opencode", id: "mimo-v2.6-flash-free" },
              }),
              reply("session.switchModel", null),
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
          // One native session carried both turns.
          assert.lengthOf(projection.providerThreads, 1);
        }).pipe(Effect.scoped),
    );
  }

  it.effect("moves the session to the thread's new worktree before the next prompt", () =>
    Effect.gen(function* () {
      const name = "opencode2-worktree-move";
      const before = yield* checkpointWorkspace(`${name}-before`);
      const after = yield* checkpointWorkspace(`${name}-after`);
      const thread = threadCommands({ name, worktreePath: before });
      const projection = yield* runScenario({
        name,
        threadId: thread.threadId,
        entries: [
          ...createdSession(before),
          ...answeredPrompt("FIRST"),
          out("session.get", { sessionID: SESSION }),
          reply("session.get", sessionInfo(before)),
          // The worktree change detached the thread, so its session is loaded afresh.
          ...noOpenRequests,
          out("session.move", { sessionID: SESSION, directory: after }),
          reply("session.move", null),
          ...answeredPrompt("SECOND"),
        ],
        commands: [
          thread.create,
          thread.message("first"),
          {
            type: "thread.metadata.update",
            commandId: thread.command("worktree"),
            threadId: thread.threadId,
            worktreePath: after,
          },
          thread.message("second"),
        ],
      });
      assert.deepEqual(
        projection.runs.map((run) => run.status),
        ["completed", "completed"],
      );
      assert.lengthOf(projection.providerThreads, 1);
    }).pipe(Effect.scoped),
  );

  it.effect("gives a session made with older rules T3's rules before its next prompt", () =>
    Effect.gen(function* () {
      const name = "opencode2-resume-rules";
      const before = yield* checkpointWorkspace(`${name}-before`);
      const after = yield* checkpointWorkspace(`${name}-after`);
      const thread = threadCommands({ name, worktreePath: before });
      const projection = yield* runScenario({
        name,
        threadId: thread.threadId,
        entries: [
          ...createdSession(before),
          ...answeredPrompt("FIRST"),
          // Reopened after a worktree change, the session reports the rules an
          // older build gave it; they are replaced before anything runs.
          out("session.get", { sessionID: SESSION }),
          reply(
            "session.get",
            sessionInfo(before, [{ action: "*", resource: "*", effect: "allow" }]),
          ),
          ...noOpenRequests,
          out("session.update", { sessionID: SESSION, permissions: T3_RULES }),
          reply("session.update", null),
          out("session.move", { sessionID: SESSION, directory: after }),
          reply("session.move", null),
          ...answeredPrompt("SECOND"),
        ],
        commands: [
          thread.create,
          thread.message("first"),
          {
            type: "thread.metadata.update",
            commandId: thread.command("worktree"),
            threadId: thread.threadId,
            worktreePath: after,
          },
          thread.message("second"),
        ],
      });
      assert.deepEqual(
        projection.runs.map((run) => run.status),
        ["completed", "completed"],
      );
    }).pipe(Effect.scoped),
  );

  it.effect(
    "creates a Supervised thread's session with rules that ask before shell and edits",
    () =>
      Effect.gen(function* () {
        const name = "opencode2-supervised-rules";
        const cwd = yield* checkpointWorkspace(name);
        const thread = threadCommands({
          name,
          worktreePath: cwd,
          runtimeMode: "approval-required",
        });
        const projection = yield* runScenario({
          name,
          threadId: thread.threadId,
          entries: [...createdSession(cwd, SUPERVISED_RULES), ...answeredPrompt("FIRST")],
          commands: [thread.create, thread.message("first")],
        });
        assert.deepEqual(
          projection.runs.map((run) => run.status),
          ["completed"],
        );
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
          ...createdSession(cwd),
          ...answeredPrompt("FIRST"),
          // A mode change detaches nothing: the same session is resumed with the new rules.
          out("session.get", { sessionID: SESSION }),
          reply("session.get", sessionInfo(cwd)),
          out("agent.list", "<any>"),
          reply("agent.list", agentList(cwd)),
          out("session.update", { sessionID: SESSION, permissions: AUTO_EDIT_RULES }),
          reply("session.update", null),
          ...answeredPrompt("SECOND"),
          // Back to Full access: the narrowing rules go.
          out("session.get", { sessionID: SESSION }),
          reply("session.get", sessionInfo(cwd, AUTO_EDIT_RULES)),
          out("session.update", { sessionID: SESSION, permissions: T3_RULES }),
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
          ...createdSession(cwd, PLAN_RULES),
          ...answeredPrompt("PLANNED"),
          out("session.get", { sessionID: SESSION }),
          reply("session.get", sessionInfo(cwd, PLAN_RULES)),
          out("session.update", { sessionID: SESSION, permissions: T3_RULES }),
          reply("session.update", null),
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
});
