/**
 * Failures and settings a live OpenCode 2 server cannot produce on demand,
 * driven through the real adapter and `@opencode/client` against a replayed
 * HTTP server. Frames reuse the shapes recorded against 2.0.18.
 */
import { assert, it } from "@effect/vitest";
import {
  CheckpointId,
  EnvironmentId,
  MessageId,
  NodeId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
  type ProviderReplayEntry,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as Exit from "effect/Exit";
import { TestClock } from "effect/testing";
import { describe } from "vite-plus/test";

import type {
  ProviderAdapterV2Event,
  ProviderAdapterV2SessionRuntime,
} from "../ProviderAdapter.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import * as IdAllocator from "../IdAllocator.ts";
import type { ProviderContinuationRequest } from "../ProviderContinuationRequests.ts";
import * as ProviderContinuationRequests from "../ProviderContinuationRequests.ts";
import { OPENCODE_PROVIDER } from "./OpenCodeAdapterV2.ts";
import { OPENCODE_2_STILL_STOPPING } from "./OpenCode2AdapterV2.ts";
import { openCode2ReplayRuntime } from "./OpenCode2AdapterV2.testkit.ts";

const SESSION = "ses_f148ca2deffeJcwCnRQtb0YFNX";
const WORK = "/work/opencode2";
const instanceId = ProviderInstanceId.make("opencode");
const threadId = ThreadId.make("thread:opencode2-adapter");

const out = (type: string, input?: unknown): ProviderReplayEntry => ({
  type: "expect_outbound",
  frame: input === undefined ? { type } : { type, input },
});
/** A recorded response body; `{ data }` is the server's envelope, `null` an empty 204. */
const reply = (operation: string, data: unknown): ProviderReplayEntry => ({
  type: "emit_inbound",
  frame: { type: "sdk.response", operation, data },
});
const replyData = (operation: string, data: unknown) => reply(operation, { data });
const event = (type: string, data: Record<string, unknown>): ProviderReplayEntry => ({
  type: "emit_inbound",
  frame: {
    type: "sdk.event",
    event: { id: `evt_${type.replaceAll(".", "")}0000`, created: 1, type, data, ...durable },
  },
});
const durable = { durable: { aggregateID: SESSION, seq: 1, version: 1 } };

/** The rules T3 gives every session it runs, with only this thread's own T3 MCP server allowed. */
const mcpRules = [
  { action: "t3-code-*", resource: "*", effect: "deny" },
  { action: "t3-code-thread_opencode2-adapter_*", resource: "*", effect: "allow" },
];
const t3Rules = [{ action: "*", resource: "*", effect: "allow" }, ...mcpRules];
const sessionInfo = (overrides: Record<string, unknown> = {}) => ({
  id: SESSION,
  permissions: t3Rules,
  projectID: "global",
  model: { id: "big-pickle", providerID: "opencode", variant: "default" },
  cost: 0,
  tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
  time: { created: 1790656601394, updated: 1790656601394 },
  location: { directory: WORK },
  ...overrides,
});
// `/api/model` as 2.0.18 lists big-pickle: its 160k input limit is the usable window.
const modelCatalog = {
  location: { directory: WORK },
  data: [
    {
      id: "big-pickle",
      modelID: "big-pickle",
      providerID: "opencode",
      family: "big-pickle",
      name: "Big Pickle",
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
    },
  ],
};

/** The prompt id the recorded answer carries; a replay maps it to the one T3 chose. */
const PROMPT_ID = "msg_0eb735d41001NJee1EvVePJAK5";
const promptAccepted = replyData("session.prompt", {
  id: PROMPT_ID,
  sessionID: SESSION,
  time: { created: 1790656601410 },
  type: "user",
  payload: { text: "hi" },
  delivery: "steer",
});

/** A resumed session that waits on nothing. */
const noOpenRequests: ReadonlyArray<ProviderReplayEntry> = [
  out("permission.list", { sessionID: SESSION }),
  replyData("permission.list", []),
  out("session.form.list", { sessionID: SESSION }),
  replyData("session.form.list", []),
];

/**
 * A thread's first turn writes T3's instructions entry before it starts; the
 * adapter only rewrites it when it changes, so later turns do not.
 */
const withInstructions = (
  entries: ReadonlyArray<ProviderReplayEntry>,
): ReadonlyArray<ProviderReplayEntry> => {
  const first = entries.findIndex(
    (entry) =>
      entry.type === "expect_outbound" &&
      typeof entry.frame === "object" &&
      entry.frame !== null &&
      "type" in entry.frame &&
      [
        "session.prompt",
        "session.command",
        "session.compact",
        "command.list",
        "skill.list",
      ].includes(String(entry.frame.type)),
  );
  // T3's MCP server is added before the entry that describes it.
  const after = entries.findIndex(
    (entry, index) =>
      index < first &&
      entry.type === "emit_inbound" &&
      typeof entry.frame === "object" &&
      entry.frame !== null &&
      "operation" in entry.frame &&
      entry.frame.operation === "mcp.add",
  );
  const at = after < 0 ? first : after + 1;
  return first < 0
    ? entries
    : [
        ...entries.slice(0, at),
        out("session.instructions.entry.put", {
          sessionID: SESSION,
          key: "t3-code",
          value: "<any>",
        }),
        reply("session.instructions.entry.put", null),
        ...entries.slice(at),
      ];
};

const openCode2ReplayRuntimeWithInstructions = (
  entries: ReadonlyArray<ProviderReplayEntry>,
  options?: { readonly external?: boolean },
) => openCode2ReplayRuntime(withInstructions(entries), options);

/** What every session sends when it opens: the event stream, then the model list. */
const opening: ReadonlyArray<ProviderReplayEntry> = [
  out("event.subscribe"),
  out("model.list", "<any>"),
  reply("model.list", modelCatalog),
];

const bigPickle: ModelSelection = { instanceId, model: "opencode/big-pickle" };
const policy = (runtimeMode: "full-access" | "approval-required" = "full-access") => ({
  runtimeMode,
  interactionMode: "default" as const,
  cwd: WORK,
});

const providerThread = (now: DateTime.Utc): OrchestrationV2ProviderThread => ({
  id: ProviderThreadId.make("provider-thread:opencode2-adapter"),
  driver: OPENCODE_PROVIDER,
  providerInstanceId: instanceId,
  providerSessionId: ProviderSessionId.make("provider-session:opencode2-adapter"),
  appThreadId: threadId,
  ownerNodeId: null,
  nativeThreadRef: { driver: OPENCODE_PROVIDER, nativeId: SESSION, strength: "strong" },
  nativeConversationHeadRef: null,
  status: "idle",
  firstRunOrdinal: null,
  lastRunOrdinal: null,
  handoffIds: [],
  forkedFrom: null,
  createdAt: now,
  updatedAt: now,
});

const turnInput = (
  thread: OrchestrationV2ProviderThread,
  modelSelection: ModelSelection = bigPickle,
  runtimeMode: "full-access" | "approval-required" = "full-access",
) => ({
  appThread: {} as OrchestrationV2AppThread,
  threadId,
  runId: RunId.make("run:opencode2-adapter"),
  runOrdinal: 1,
  providerTurnOrdinal: 1,
  attemptId: RunAttemptId.make("attempt:opencode2-adapter"),
  rootNodeId: NodeId.make("node:opencode2-adapter"),
  providerThread: thread,
  message: {
    messageId: MessageId.make("message:opencode2-adapter"),
    text: "hi",
    attachments: [],
    createdBy: "user" as const,
    creationSource: "web" as const,
    scheduledTaskId: undefined,
    senderThreadId: undefined,
  },
  modelSelection,
  runtimePolicy: policy(runtimeMode),
});

/** The agents' own path rules, as `/api/agent` lists them for build. */
const buildPaths = [
  {
    action: "external_directory",
    resource: "/home/.local/share/opencode/tool-output/*",
    effect: "allow",
  },
];
const agentList = {
  location: { directory: WORK },
  data: [
    {
      id: "build",
      name: "Build",
      request: { settings: {}, headers: {}, body: {} },
      description: "The default agent.",
      mode: "primary",
      hidden: false,
      permissions: [{ action: "*", resource: "*", effect: "allow" }, ...buildPaths],
    },
  ],
};
/** Supervised: shell, edits and other directories ask. */
const supervisedRules = [
  { action: "shell", resource: "*", effect: "ask" },
  { action: "edit", resource: "*", effect: "ask" },
  { action: "external_directory", resource: "*", effect: "ask" },
  ...buildPaths,
  ...mcpRules,
];

// The first ask and question form the spike recorded (recordings/permission, question).
const shellAsk = {
  data: {
    id: "per_0eb7c4d7e001Pyt8o50Vi4KrOO",
    sessionID: SESSION,
    action: "shell",
    resources: ["echo FIRST"],
    save: ["echo *"],
    source: { type: "tool", messageID: "msg_0eb7c4330001dYFQKuTpfD780v", id: "call_1" },
  },
};
const shellAskEvent = event("permission.asked", shellAsk.data);
const colorForm = {
  id: "frm_0eb79ab35001fkvFECSh3wYNVD",
  sessionID: SESSION,
  title: "Questions",
  metadata: { kind: "question" },
  fields: [
    {
      key: "q0",
      title: "Color preference",
      description: "Which color do you prefer?",
      type: "string",
      options: [{ value: "Red", label: "Red" }],
      custom: true,
    },
  ],
};

/**
 * Resumes the recorded session and returns the runtime and the thread. A
 * supervised resume gives the session Supervised rules first.
 */
const resumed = (
  entries: ReadonlyArray<ProviderReplayEntry>,
  options?: { readonly external?: boolean; readonly supervised?: boolean },
) =>
  Effect.gen(function* () {
    const runtime = yield* openCode2ReplayRuntime(
      withInstructions([
        ...opening,
        out("session.get", { sessionID: SESSION }),
        replyData("session.get", sessionInfo()),
        ...noOpenRequests,
        ...(options?.supervised === true
          ? [
              out("agent.list", "<any>"),
              reply("agent.list", agentList),
              out("session.update", { sessionID: SESSION, permissions: supervisedRules }),
              reply("session.update", null),
            ]
          : []),
        ...entries,
      ]),
      options?.external === undefined ? undefined : { external: options.external },
    );
    const thread = yield* runtime.resumeThread({
      providerThread: providerThread(yield* DateTime.now),
      threadId,
      modelSelection: bigPickle,
      runtimePolicy: policy(options?.supervised === true ? "approval-required" : "full-access"),
    });
    return { runtime, thread };
  });

const requestOf = (runtime: ProviderAdapterV2SessionRuntime) =>
  runtime.events.pipe(
    Stream.filter(
      (event): event is Extract<ProviderAdapterV2Event, { type: "runtime_request.updated" }> =>
        event.type === "runtime_request.updated",
    ),
    Stream.map((event) => event.runtimeRequest),
    Stream.runHead,
    Effect.map(Option.getOrUndefined),
  );

const terminalOf = (runtime: ProviderAdapterV2SessionRuntime) =>
  runtime.events.pipe(
    Stream.filter(
      (event): event is Extract<ProviderAdapterV2Event, { type: "turn.terminal" }> =>
        event.type === "turn.terminal",
    ),
    Stream.runHead,
    Effect.map(Option.getOrUndefined),
  );

// The history the spike read back after its `simple` turn (recordings/simple.ndjson).
const history = {
  data: [
    {
      id: "msg_0eb732081001RntUJfRtTXOAjd",
      time: { created: 1790656585885 },
      text: "Think carefully step by step about whether 391 is prime, showing your reasoning, then answer in one short sentence.",
      type: "user",
    },
    {
      id: "msg_0eb7320a9001vve3OV5uNi2HRT",
      time: { created: 1790656585925, streamed: 1790656590719, completed: 1790656590736 },
      type: "assistant",
      agent: "build",
      model: { id: "space-bunny-free", providerID: "opencode", variant: "high" },
      content: [
        { type: "reasoning", text: "Check divisibility up to sqrt(391)." },
        { type: "text", text: "391 is not prime: it's the product 17 × 23." },
      ],
      finish: "stop",
      cost: 0,
      tokens: { input: 8701, output: 113, reasoning: 147, cache: { read: 489, write: 0 } },
    },
    {
      id: "msg_0eb733399001NhwTrB32UU6d6H",
      time: { created: 1790656590745 },
      type: "idle",
      outcome: "succeeded",
    },
  ],
  cursor: {},
};

describe("OpenCode2 adapter", () => {
  it.effect("switches the session's model and variant before a turn that changed them", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.switchModel", {
          sessionID: SESSION,
          model: { providerID: "openrouter", id: "deepseek/deepseek-v4-flash", variant: "high" },
        }),
        reply("session.switchModel", null),
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        event("session.execution.succeeded", { sessionID: SESSION }),
      ]);
      const terminal = yield* terminalOf(runtime).pipe(Effect.forkScoped);
      yield* runtime.startTurn(
        turnInput(thread, {
          instanceId,
          model: "openrouter/deepseek/deepseek-v4-flash",
          options: [{ id: "variant", value: "high" }],
        }),
      );
      assert.equal((yield* Fiber.join(terminal))?.status, "completed");
    }).pipe(Effect.scoped),
  );

  it.effect("ends the turn when its terminal event is one this build cannot decode", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        // A reason added after 2.0.18: the full schema rejects the frame.
        event("session.execution.interrupted", { sessionID: SESSION, reason: "budget" }),
      ]);
      const terminal = yield* terminalOf(runtime).pipe(Effect.forkScoped);
      yield* runtime.startTurn(turnInput(thread));
      assert.equal((yield* Fiber.join(terminal))?.status, "interrupted");
    }).pipe(Effect.scoped),
  );

  it.effect("keeps a turn running through a start event this build cannot decode", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        {
          type: "emit_inbound",
          frame: {
            type: "sdk.event",
            event: {
              id: "evt_executionstartedx",
              created: 1,
              type: "session.execution.started",
              data: { sessionID: SESSION },
              durable: "not-an-envelope",
            },
          },
        },
        event("session.text.started", {
          sessionID: SESSION,
          assistantMessageID: "msg_0eb735d5b001oAFVeY5jz3WD4Z",
          ordinal: 0,
        }),
        event("session.text.ended", {
          sessionID: SESSION,
          assistantMessageID: "msg_0eb735d5b001oAFVeY5jz3WD4Z",
          ordinal: 0,
          text: "DONE",
        }),
        event("session.execution.succeeded", { sessionID: SESSION }),
      ]);
      const collected = yield* runtime.events.pipe(
        Stream.takeUntil((event) => event.type === "turn.terminal"),
        Stream.runCollect,
        Effect.forkScoped,
      );
      yield* runtime.startTurn(turnInput(thread));
      const seen = yield* Fiber.join(collected);
      const terminals = seen.filter((event) => event.type === "turn.terminal");
      assert.deepEqual(
        terminals.map((event) => event.type === "turn.terminal" && event.status),
        ["completed"],
      );
      // The reply after the malformed start still reached the turn.
      assert.isTrue(
        seen.some(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "assistant_message" &&
            event.turnItem.text === "DONE",
        ),
      );
    }).pipe(Effect.scoped),
  );

  it.effect("stops running turns on an external server when the session closes", () =>
    Effect.gen(function* () {
      const scope = yield* Scope.make();
      const { runtime, thread } = yield* resumed(
        [
          out("session.prompt", { sessionID: SESSION, text: "<any>" }),
          promptAccepted,
          out("session.interrupt", { sessionID: SESSION }),
          reply("session.interrupt", { interrupted: true }),
        ],
        { external: true },
      ).pipe(Scope.provide(scope));
      yield* runtime.startTurn(turnInput(thread));
      yield* Scope.close(scope, Exit.void);
    }),
  );

  it.effect("ends a turn locally when a stuck server never answers Stop", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        out("session.interrupt", { sessionID: SESSION }),
        reply("session.interrupt", "<hang>"),
      ]);
      const terminal = yield* terminalOf(runtime).pipe(Effect.forkScoped);
      yield* runtime.startTurn(turnInput(thread));
      const interrupt = yield* runtime
        .interruptTurn({
          providerThread: thread,
          providerTurnId: yield* providerTurnId,
        })
        .pipe(Effect.forkScoped);
      yield* TestClock.adjust("11 seconds");
      yield* Fiber.join(interrupt);
      assert.equal((yield* Fiber.join(terminal))?.status, "interrupted");
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

  /** A subagent's session as 2.0.18 announced it in the spike's `background` run. */
  const childCreated = (child: string) => ({
    sessionID: child,
    slug: "proud-canyon",
    version: "2.0.18",
    projectID: "global",
    parentID: SESSION,
    location: { directory: WORK },
    subpath: "",
    title: "Sleep",
    agent: "general",
    model: { id: "big-pickle", providerID: "opencode", variant: "default" },
  });
  /** A turn whose model launched a background subagent that still runs. */
  const backgroundLaunch = (child: string): ReadonlyArray<ProviderReplayEntry> => {
    const call = "call-background";
    const tool = { sessionID: SESSION, assistantMessageID: "msg_assistant", id: call };
    return [
      out("session.prompt", { sessionID: SESSION, text: "<any>" }),
      promptAccepted,
      event("session.execution.started", { sessionID: SESSION }),
      event("session.tool.input.started", { ...tool, name: "subagent" }),
      event("session.tool.called", {
        ...tool,
        name: "subagent",
        input: { description: "Sleep", prompt: "sleep", background: true },
        executed: false,
      }),
      event("session.created", childCreated(child)),
      event("session.tool.progress", {
        ...tool,
        metadata: { sessionID: child, status: "running" },
      }),
    ];
  };
  // The subagent's child thread hangs off the app thread's lineage.
  const withLineage = (thread: OrchestrationV2ProviderThread) => ({
    ...turnInput(thread),
    appThread: {
      id: threadId,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    } as OrchestrationV2AppThread,
  });
  const CHILD = "ses_f1485c529ffea4URrYruwEg0Ja";
  /**
   * The single reader of the runtime's events: resolves `attached` once the
   * running background child has its thread, then returns the turn's terminal.
   */
  const watchBackgroundTurn = (runtime: ProviderAdapterV2SessionRuntime) =>
    Effect.gen(function* () {
      const attached = yield* Deferred.make<void>();
      const terminal = yield* runtime.events.pipe(
        Stream.tap((event) =>
          event.type === "subagent.updated" && event.subagent.childThreadId !== null
            ? Deferred.succeed(attached, undefined)
            : Effect.void,
        ),
        Stream.filter(
          (event): event is Extract<ProviderAdapterV2Event, { type: "turn.terminal" }> =>
            event.type === "turn.terminal",
        ),
        Stream.runHead,
        Effect.map(Option.getOrUndefined),
        Effect.forkScoped,
      );
      return { attached: Deferred.await(attached), terminal: Fiber.join(terminal) };
    });
  it.effect("keeps background subagents running when a turn is interrupted to restart it", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        ...backgroundLaunch(CHILD),
        // Only the parent's execution is stopped.
        out("session.interrupt", { sessionID: SESSION }),
        reply("session.interrupt", { interrupted: true }),
        event("session.execution.interrupted", { sessionID: SESSION }),
      ]);
      const watch = yield* watchBackgroundTurn(runtime);
      yield* runtime.startTurn(withLineage(thread));
      yield* watch.attached;
      // The orchestrator's restart interrupt: no `requestRuntimeRestart`.
      yield* runtime.interruptTurn({
        providerThread: thread,
        providerTurnId: yield* providerTurnId,
      });
      assert.equal((yield* watch.terminal)?.status, "interrupted");
      assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
    }).pipe(Effect.scoped),
  );

  it.effect("stops background subagents on a user Stop", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        ...backgroundLaunch(CHILD),
        out("session.interrupt", { sessionID: CHILD }),
        reply("session.interrupt", { interrupted: true }),
        out("session.interrupt", { sessionID: SESSION }),
        reply("session.interrupt", { interrupted: true }),
        event("session.execution.interrupted", { sessionID: SESSION }),
      ]);
      const watch = yield* watchBackgroundTurn(runtime);
      yield* runtime.startTurn(withLineage(thread));
      yield* watch.attached;
      yield* runtime.interruptTurn({
        providerThread: thread,
        providerTurnId: yield* providerTurnId,
        requestRuntimeRestart: true,
      });
      assert.equal((yield* watch.terminal)?.status, "interrupted");
      assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
    }).pipe(Effect.scoped),
  );

  it.effect("stops a background subagent announced before its call named it", () =>
    Effect.gen(function* () {
      const call = "call-background";
      const tool = { sessionID: SESSION, assistantMessageID: "msg_assistant", id: call };
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        event("session.execution.started", { sessionID: SESSION }),
        event("session.tool.input.started", { ...tool, name: "subagent" }),
        event("session.tool.called", {
          ...tool,
          name: "subagent",
          input: { description: "Sleep", prompt: "sleep", background: true },
          executed: false,
        }),
        // OpenCode announces the child before the call's progress names it.
        event("session.created", childCreated(CHILD)),
        // Emitted after the announcement, so the Stop comes after it too.
        event("session.text.ended", {
          sessionID: SESSION,
          assistantMessageID: "msg_assistant",
          ordinal: 0,
          text: "Launched.",
        }),
        out("session.interrupt", { sessionID: CHILD }),
        reply("session.interrupt", { interrupted: true }),
        out("session.interrupt", { sessionID: SESSION }),
        reply("session.interrupt", { interrupted: true }),
        event("session.execution.interrupted", { sessionID: SESSION }),
      ]);
      const announced = yield* Deferred.make<void>();
      const terminal = yield* runtime.events.pipe(
        Stream.tap((event) =>
          event.type === "turn_item.updated" && event.turnItem.type === "assistant_message"
            ? Deferred.succeed(announced, undefined)
            : Effect.void,
        ),
        Stream.filter(
          (event): event is Extract<ProviderAdapterV2Event, { type: "turn.terminal" }> =>
            event.type === "turn.terminal",
        ),
        Stream.runHead,
        Effect.map(Option.getOrUndefined),
        Effect.forkScoped,
      );
      yield* runtime.startTurn(withLineage(thread));
      yield* Deferred.await(announced);
      yield* runtime.interruptTurn({
        providerThread: thread,
        providerTurnId: yield* providerTurnId,
        requestRuntimeRestart: true,
      });
      assert.equal((yield* Fiber.join(terminal))?.status, "interrupted");
      assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
    }).pipe(Effect.scoped),
  );

  it.effect("keeps a finished background subagent's queued report as pending work", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        ...backgroundLaunch(CHILD),
        event("session.execution.succeeded", { sessionID: SESSION }),
        // The child ends; OpenCode queues its report for the parent, and only
        // then starts the parent's follow-up execution.
        event("session.execution.succeeded", { sessionID: CHILD }),
        event("session.inbox.enqueued", {
          inboxID: "msg_report",
          sessionID: SESSION,
          item: {
            type: "synthetic",
            payload: {
              text: `<subagent sessionID="${CHILD}" state="completed" description="Sleep">\nCHILD_OK\n</subagent>`,
              description: "Sleep",
              metadata: {
                source: "subagent",
                childID: CHILD,
                agent: "General",
                state: "completed",
              },
            },
            delivery: "steer",
          },
        }),
        // Emitted once the report is in, so the check below runs in the gap.
        event("session.usage.updated", { sessionID: SESSION }),
      ]);
      const reported = yield* Deferred.make<void>();
      yield* runtime.events.pipe(
        Stream.tap((event) =>
          event.type === "subagent.updated" && event.subagent.status === "completed"
            ? Deferred.succeed(reported, undefined)
            : Effect.void,
        ),
        Stream.runDrain,
        Effect.forkScoped,
      );
      yield* runtime.startTurn(withLineage(thread));
      yield* Deferred.await(reported);
      // The follow-up execution OpenCode will start for the report is still to come.
      assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
      assert.isTrue(yield* runtime.hasPendingBackgroundWorkForThread!(thread));
    }).pipe(Effect.scoped),
  );

  it.effect("leaves no background work pending once a reconnect finds its subagent gone", () =>
    Effect.gen(function* () {
      // The subagent ends while the stream is down: its end, its report and
      // the follow-up it starts are never seen. `session.active` lists nothing.
      const { runtime, thread } = yield* resumed([
        ...backgroundLaunch(CHILD),
        event("session.execution.started", { sessionID: CHILD }),
        event("session.execution.succeeded", { sessionID: SESSION }),
        { type: "runtime_exit", status: "success" } as const,
        out("event.subscribe"),
        out("session.active"),
        replyData("session.active", {}),
      ]);
      const settled = yield* Deferred.make<string | null>();
      yield* runtime.events.pipe(
        Stream.tap((event) =>
          event.type === "subagent.updated" && event.subagent.status === "interrupted"
            ? Deferred.succeed(settled, event.subagent.result)
            : Effect.void,
        ),
        Stream.runDrain,
        Effect.forkScoped,
      );
      yield* runtime.startTurn(withLineage(thread));
      assert.include((yield* Deferred.await(settled)) ?? "", "lost its connection to OpenCode");
      assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
      assert.isFalse(yield* runtime.hasPendingBackgroundWorkForThread!(thread));
    }).pipe(Effect.scoped),
  );

  it.effect("stops a nested background subagent's report on its own parent's session", () =>
    Effect.gen(function* () {
      const MIDDLE = "ses_middle0000000000000000000";
      const GRANDCHILD = "ses_grandchild00000000000000";
      const tool = (session: string, id: string) => ({
        sessionID: session,
        assistantMessageID: `msg_assistant_${id}`,
        id,
      });
      const report = {
        inboxID: "msg_nested_report",
        sessionID: MIDDLE,
        item: {
          type: "synthetic",
          payload: {
            text: `<subagent sessionID="${GRANDCHILD}" state="cancelled" description="Deep">\n</subagent>`,
            description: "Deep",
            metadata: {
              source: "subagent",
              childID: GRANDCHILD,
              agent: "General",
              state: "cancelled",
            },
          },
          delivery: "steer",
        },
      };
      const offered = yield* Deferred.make<void>();
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        event("session.execution.started", { sessionID: SESSION }),
        // The thread's model starts a background subagent...
        event("session.tool.input.started", { ...tool(SESSION, "call-middle"), name: "subagent" }),
        event("session.tool.called", {
          ...tool(SESSION, "call-middle"),
          name: "subagent",
          input: { description: "Middle", prompt: "delegate", background: true },
          executed: false,
        }),
        event("session.created", { ...childCreated(MIDDLE), title: "Middle" }),
        event("session.tool.progress", {
          ...tool(SESSION, "call-middle"),
          metadata: { sessionID: MIDDLE, status: "running" },
        }),
        event("session.execution.started", { sessionID: MIDDLE }),
        // ...which starts one of its own in the background.
        event("session.tool.input.started", { ...tool(MIDDLE, "call-deep"), name: "subagent" }),
        event("session.tool.called", {
          ...tool(MIDDLE, "call-deep"),
          name: "subagent",
          input: { description: "Deep", prompt: "sleep", background: true },
          executed: false,
        }),
        event("session.created", {
          ...childCreated(GRANDCHILD),
          parentID: MIDDLE,
          title: "Deep",
        }),
        event("session.tool.progress", {
          ...tool(MIDDLE, "call-deep"),
          metadata: { sessionID: GRANDCHILD, status: "running" },
        }),
        event("session.text.ended", {
          sessionID: SESSION,
          assistantMessageID: "msg_assistant_root",
          ordinal: 0,
          text: "Launched.",
        }),
        // A Stop on the thread interrupts both background subagents.
        out("session.interrupt", { sessionID: MIDDLE }),
        reply("session.interrupt", { interrupted: true }),
        out("session.interrupt", { sessionID: GRANDCHILD }),
        reply("session.interrupt", { interrupted: true }),
        out("session.interrupt", { sessionID: SESSION }),
        reply("session.interrupt", { interrupted: true }),
        event("session.execution.interrupted", { sessionID: SESSION }),
        event("session.execution.interrupted", { sessionID: MIDDLE }),
        // OpenCode still reports the stopped grandchild to the middle session
        // and wakes it; that execution is stopped, not run as a subagent turn.
        event("session.inbox.enqueued", report),
        event("session.execution.started", { sessionID: MIDDLE }),
        out("session.interrupt", { sessionID: MIDDLE }),
        reply("session.interrupt", { interrupted: true }),
        event("session.execution.interrupted", { sessionID: MIDDLE }),
        // Later OpenCode runs the thread's own session by itself: a follow-up
        // T3 offers a turn for, which marks that everything above was handled.
        event("session.execution.started", { sessionID: SESSION }),
      ]).pipe(
        Effect.provideService(ProviderContinuationRequests.ProviderContinuationRequests, {
          offer: () => Deferred.succeed(offered, undefined).pipe(Effect.asVoid),
          take: Effect.never,
        }),
      );
      const launched = yield* Deferred.make<void>();
      const middleTurns: Array<string> = [];
      yield* runtime.events.pipe(
        Stream.tap((event) =>
          Effect.gen(function* () {
            if (
              event.type === "provider_turn.updated" &&
              event.providerTurn.nativeTurnRef?.nativeId?.startsWith(`${MIDDLE}:turn:`) === true
            ) {
              middleTurns.push(event.providerTurn.nativeTurnRef.nativeId);
            }
            if (event.type === "turn_item.updated" && event.turnItem.type === "assistant_message") {
              yield* Deferred.succeed(launched, undefined);
            }
          }),
        ),
        Stream.runDrain,
        Effect.forkScoped,
      );
      yield* runtime.startTurn(withLineage(thread));
      yield* Deferred.await(launched);
      yield* runtime.interruptTurn({
        providerThread: thread,
        providerTurnId: yield* providerTurnId,
        requestRuntimeRestart: true,
      });
      // The report wakes the middle session; the adapter stops that execution
      // (the replay fails on any other request) and opens no turn for it.
      yield* Deferred.await(offered);
      assert.deepEqual([...new Set(middleTurns)], [`${MIDDLE}:turn:1`]);
    }).pipe(Effect.scoped),
  );

  /** A prompt accepted, then a Stop the server never answers, advanced past its timeout. */
  const stopTimedOut: ReadonlyArray<ProviderReplayEntry> = [
    out("session.prompt", { sessionID: SESSION, text: "<any>" }),
    promptAccepted,
    out("session.interrupt", { sessionID: SESSION }),
    reply("session.interrupt", "<hang>"),
  ];
  const secondTurn = (thread: OrchestrationV2ProviderThread) => ({
    ...turnInput(thread),
    runId: RunId.make("run:opencode2-adapter:2"),
    runOrdinal: 2,
    providerTurnOrdinal: 2,
    attemptId: RunAttemptId.make("attempt:opencode2-adapter:2"),
  });
  const stopFirstTurn = (
    runtime: ProviderAdapterV2SessionRuntime,
    thread: OrchestrationV2ProviderThread,
  ) =>
    Effect.gen(function* () {
      yield* runtime.startTurn(turnInput(thread));
      const interrupt = yield* runtime
        .interruptTurn({ providerThread: thread, providerTurnId: yield* providerTurnId })
        .pipe(Effect.forkScoped);
      yield* TestClock.adjust("11 seconds");
      yield* Fiber.join(interrupt);
    });
  const terminals = (runtime: ProviderAdapterV2SessionRuntime, count: number) =>
    runtime.events.pipe(
      Stream.filter(
        (event): event is Extract<ProviderAdapterV2Event, { type: "turn.terminal" }> =>
          event.type === "turn.terminal",
      ),
      Stream.take(count),
      Stream.runCollect,
      Effect.forkScoped,
    );

  it.effect("never lets a timed-out Stop's late end finish the next turn", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        ...stopTimedOut,
        // The server no longer runs the stopped execution, so the next turn goes ahead.
        out("session.active"),
        reply("session.active", { data: {} }),
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        // The stopped execution's end arrives late, then the new turn's own.
        event("session.execution.succeeded", { sessionID: SESSION }),
        event("session.execution.started", { sessionID: SESSION }),
        event("session.execution.failed", {
          sessionID: SESSION,
          error: { type: "provider", message: "second turn failed" },
        }),
      ]);
      const ended = yield* terminals(runtime, 2);
      yield* stopFirstTurn(runtime, thread);
      yield* runtime.startTurn(secondTurn(thread));
      const [first, second] = yield* Fiber.join(ended);
      assert.equal(first?.status, "interrupted");
      // Only the second turn's own end finishes it.
      assert.equal(second?.status, "failed");
      assert.equal(second?.failure?.message, "second turn failed");
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

  it.effect(
    "starts the next turn once the server no longer runs a timed-out Stop's execution",
    () =>
      Effect.gen(function* () {
        const { runtime, thread } = yield* resumed([
          ...stopTimedOut,
          out("session.active"),
          reply("session.active", { data: {} }),
          out("session.prompt", { sessionID: SESSION, text: "<any>" }),
          promptAccepted,
          event("session.execution.started", { sessionID: SESSION }),
          event("session.execution.succeeded", { sessionID: SESSION }),
        ]);
        const ended = yield* terminals(runtime, 2);
        yield* stopFirstTurn(runtime, thread);
        yield* runtime.startTurn(secondTurn(thread));
        const [, second] = yield* Fiber.join(ended);
        assert.equal(second?.status, "completed");
      }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

  it.effect("reads a timed-out Stop's next turn from an execution start it cannot decode", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        ...stopTimedOut,
        out("session.active"),
        reply("session.active", { data: {} }),
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        // A newer server's start: the full schema rejects it, but it still opens the turn.
        {
          type: "emit_inbound",
          frame: {
            type: "sdk.event",
            event: {
              id: "evt_executionstartednewer",
              created: 1,
              type: "session.execution.started",
              data: { sessionID: SESSION },
              durable: "not-an-envelope",
            },
          },
        },
        event("session.text.ended", {
          sessionID: SESSION,
          assistantMessageID: "msg_0eb735d5b001oAFVeY5jz3WD4Z",
          ordinal: 0,
          text: "DONE",
        }),
        event("session.execution.succeeded", { sessionID: SESSION }),
      ]);
      const ended = yield* terminals(runtime, 2);
      yield* stopFirstTurn(runtime, thread);
      yield* runtime.startTurn(secondTurn(thread));
      const [, second] = yield* Fiber.join(ended);
      assert.equal(second?.status, "completed");
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

  it.effect("checks the server before prompting again after a prompt request failed", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        // The request failed, but the server may have taken the prompt.
        reply("session.prompt", {
          status: 502,
          body: { _tag: "UnknownError", message: "bad gateway" },
        }),
        out("session.active"),
        reply("session.active", { data: { [SESSION]: { type: "running" } } }),
        out("session.interrupt", { sessionID: SESSION }),
        reply("session.interrupt", { interrupted: true }),
      ]);
      const ended = yield* terminals(runtime, 2);
      yield* runtime.startTurn(turnInput(thread)).pipe(Effect.ignore);
      yield* runtime.startTurn(secondTurn(thread));
      const [first, second] = yield* Fiber.join(ended);
      assert.equal(first?.status, "failed");
      assert.equal(second?.failure?.message, OPENCODE_2_STILL_STOPPING);
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

  it.effect("prompts again without a check after the server refused a prompt", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        reply("session.prompt", {
          status: 400,
          body: { _tag: "InvalidRequestError", message: "bad prompt" },
        }),
        // A clear refusal: nothing runs, so the next turn prompts directly.
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        event("session.execution.succeeded", { sessionID: SESSION }),
      ]);
      const ended = yield* terminals(runtime, 2);
      yield* runtime.startTurn(turnInput(thread)).pipe(Effect.ignore);
      yield* runtime.startTurn(secondTurn(thread));
      const [first, second] = yield* Fiber.join(ended);
      assert.deepEqual([first?.status, second?.status], ["failed", "completed"]);
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

  it.effect("stops a timed-out Stop's execution again and fails the turn while it runs", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        ...stopTimedOut,
        out("session.active"),
        reply("session.active", { data: { [SESSION]: { type: "running" } } }),
        // Stopped again, and the turn fails without a prompt.
        out("session.interrupt", { sessionID: SESSION }),
        reply("session.interrupt", { interrupted: true }),
      ]);
      const ended = yield* terminals(runtime, 2);
      yield* stopFirstTurn(runtime, thread);
      yield* runtime.startTurn(secondTurn(thread));
      const [, second] = yield* Fiber.join(ended);
      assert.equal(second?.status, "failed");
      assert.equal(second?.failure?.message, OPENCODE_2_STILL_STOPPING);
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

  it.effect("does not report a Stop the server says did nothing", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        out("session.interrupt", { sessionID: SESSION }),
        reply("session.interrupt", { interrupted: false }),
      ]);
      yield* runtime.startTurn(turnInput(thread));
      const failed = yield* runtime
        .interruptTurn({
          providerThread: thread,
          providerTurnId: yield* providerTurnId,
        })
        .pipe(Effect.flip);
      assert.equal(failed._tag, "ProviderAdapterProtocolError");
    }).pipe(Effect.scoped),
  );

  it.effect("reports a turn as completed when its Stop request failed", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        out("session.interrupt", { sessionID: SESSION }),
        reply("session.interrupt", {
          status: 500,
          body: { _tag: "UnknownError", message: "interrupt failed" },
        }),
        event("session.execution.succeeded", { sessionID: SESSION }),
      ]);
      const terminal = yield* terminalOf(runtime).pipe(Effect.forkScoped);
      yield* runtime.startTurn(turnInput(thread));
      const failed = yield* runtime
        .interruptTurn({ providerThread: thread, providerTurnId: yield* providerTurnId })
        .pipe(Effect.flip);
      assert.equal(failed._tag, "ProviderAdapterInterruptError");
      assert.equal((yield* Fiber.join(terminal))?.status, "completed");
    }).pipe(Effect.scoped),
  );

  it.effect("refuses to resume a thread without an OpenCode session as a protocol error", () =>
    Effect.gen(function* () {
      const runtime = yield* openCode2ReplayRuntime(opening);
      const failed = yield* runtime
        .resumeThread({
          providerThread: { ...providerThread(yield* DateTime.now), nativeThreadRef: null },
        })
        .pipe(Effect.flip);
      assert.equal(failed._tag, "ProviderAdapterProtocolError");
    }).pipe(Effect.scoped),
  );

  it.effect("gives a resumed session T3's rules when it was made with others", () =>
    Effect.gen(function* () {
      const runtime = yield* openCode2ReplayRuntimeWithInstructions([
        ...opening,
        out("session.get", { sessionID: SESSION }),
        // Made by an earlier build that denied subagents; resuming drops the deny.
        replyData(
          "session.get",
          sessionInfo({
            permissions: [
              { action: "*", resource: "*", effect: "allow" },
              { action: "subagent", resource: "*", effect: "deny" },
            ],
          }),
        ),
        ...noOpenRequests,
        out("session.update", { sessionID: SESSION, permissions: t3Rules }),
        reply("session.update", null),
      ]);
      yield* runtime.resumeThread({
        providerThread: providerThread(yield* DateTime.now),
        threadId,
        modelSelection: bigPickle,
        runtimePolicy: policy(),
      });
    }).pipe(Effect.scoped),
  );

  it.effect("moves the session when the thread's worktree changed", () =>
    Effect.gen(function* () {
      const runtime = yield* openCode2ReplayRuntimeWithInstructions([
        ...opening,
        out("session.get", { sessionID: SESSION }),
        replyData("session.get", sessionInfo()),
        ...noOpenRequests,
        out("session.move", { sessionID: SESSION, directory: "/work/opencode2-feature" }),
        reply("session.move", null),
      ]);
      yield* runtime.resumeThread({
        providerThread: providerThread(yield* DateTime.now),
        threadId,
        modelSelection: bigPickle,
        runtimePolicy: { ...policy(), cwd: "/work/opencode2-feature" },
      });
    }).pipe(Effect.scoped),
  );

  it.effect(
    "moves the session when a thread it resumes through ensureThread changed worktree",
    () =>
      Effect.gen(function* () {
        const runtime = yield* openCode2ReplayRuntimeWithInstructions([
          ...opening,
          out("session.get", { sessionID: SESSION }),
          replyData("session.get", sessionInfo()),
          ...noOpenRequests,
          out("session.move", { sessionID: SESSION, directory: "/work/opencode2-feature" }),
          reply("session.move", null),
        ]);
        yield* runtime.ensureThread({
          threadId,
          modelSelection: bigPickle,
          runtimePolicy: { ...policy(), cwd: "/work/opencode2-feature" },
          existingProviderThread: providerThread(yield* DateTime.now),
        });
      }).pipe(Effect.scoped),
  );

  it.effect("breaks the thread and forgets it when the session was deleted outside T3", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        reply("session.prompt", {
          status: 404,
          body: {
            _tag: "SessionNotFoundError",
            sessionID: SESSION,
            message: `Session not found: ${SESSION}`,
          },
        }),
      ]);
      const terminal = yield* terminalOf(runtime).pipe(Effect.forkScoped);
      yield* runtime.startTurn(turnInput(thread)).pipe(Effect.ignore);
      const ended = yield* Fiber.join(terminal);
      assert.equal(ended?.status, "failed");
      assert.equal(ended?.threadDisposition, "broken");
      // The next turn must resume (and fail into a handoff), not reuse the dead session.
      const again = yield* runtime.startTurn(turnInput(thread)).pipe(Effect.flip);
      assert.equal(again._tag, "ProviderAdapterProtocolError");
      assert.include(again.message, "not registered");
    }).pipe(Effect.scoped),
  );

  it.effect("refuses a model slug that is not provider/model before creating a session", () =>
    Effect.gen(function* () {
      // Nothing but the session's opening is expected: no create, no prompt.
      const runtime = yield* openCode2ReplayRuntime([...opening]);
      const created = yield* runtime
        .ensureThread({
          threadId,
          modelSelection: { instanceId, model: "big-pickle" },
          runtimePolicy: policy(),
        })
        .pipe(Effect.flip);
      assert.equal(created._tag, "ProviderAdapterProtocolError");
      assert.include(created.message, "OpenCode model 'big-pickle' must use provider/model format");
    }).pipe(Effect.scoped),
  );

  it.effect("refuses a turn whose model slug is not provider/model before prompting", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([]);
      const terminal = yield* terminalOf(runtime).pipe(Effect.forkScoped);
      yield* runtime.startTurn(turnInput(thread, { instanceId, model: "big-pickle" }));
      const refused = yield* Fiber.join(terminal);
      assert.equal(refused?.failure?.class, "validation_error");
      assert.include(refused?.failure?.message, "must use provider/model format");
    }).pipe(Effect.scoped),
  );

  it.effect("stops the requests a session still waits on when a restarted T3 loads it", () =>
    Effect.gen(function* () {
      // T3 restarted while the server kept waiting on an ask T3 no longer shows.
      const runtime = yield* openCode2ReplayRuntimeWithInstructions(
        [
          ...opening,
          out("session.get", { sessionID: SESSION }),
          replyData("session.get", sessionInfo()),
          out("permission.list", { sessionID: SESSION }),
          replyData("permission.list", [shellAsk.data]),
          out("session.form.list", { sessionID: SESSION }),
          replyData("session.form.list", [colorForm]),
          out("permission.reply", {
            sessionID: SESSION,
            requestID: shellAsk.data.id,
            decision: "reject",
          }),
          reply("permission.reply", null),
          out("session.form.cancel", { sessionID: SESSION, formID: colorForm.id }),
          reply("session.form.cancel", null),
          // The next turn checks that the stopped run is gone. Its end arrives
          // late and is its own; the turn ends on its own execution's end.
          out("session.active"),
          reply("session.active", { data: {} }),
          event("session.execution.interrupted", { sessionID: SESSION, reason: "user" }),
          out("session.prompt", { sessionID: SESSION, text: "<any>" }),
          promptAccepted,
          event("session.execution.started", { sessionID: SESSION }),
          event("session.execution.succeeded", { sessionID: SESSION }),
        ],
        { external: true },
      );
      const thread = yield* runtime.resumeThread({
        providerThread: providerThread(yield* DateTime.now),
        threadId,
        modelSelection: bigPickle,
        runtimePolicy: policy(),
      });
      const terminal = yield* terminalOf(runtime).pipe(Effect.forkScoped);
      yield* runtime.startTurn(turnInput(thread));
      assert.equal((yield* Fiber.join(terminal))?.status, "completed");
    }).pipe(Effect.scoped),
  );

  it.effect("ends the turn when an answer to its request cannot be delivered", () =>
    Effect.gen(function* () {
      const failedReply = reply("permission.reply", {
        status: 500,
        body: { _tag: "UnknownError", message: "reply failed" },
      });
      const replyOut = out("permission.reply", {
        sessionID: SESSION,
        requestID: shellAsk.data.id,
        decision: "once",
      });
      const { runtime, thread } = yield* resumed(
        [
          out("session.prompt", { sessionID: SESSION, text: "<any>" }),
          promptAccepted,
          shellAskEvent,
          // One try and one retry, then the turn ends and the session is stopped.
          replyOut,
          failedReply,
          replyOut,
          failedReply,
          out("session.interrupt", { sessionID: SESSION }),
          reply("session.interrupt", { interrupted: true }),
        ],
        { supervised: true },
      );
      const requested = yield* requestOf(runtime).pipe(Effect.forkScoped);
      const terminal = yield* terminalOf(runtime).pipe(Effect.forkScoped);
      yield* runtime.startTurn(turnInput(thread, bigPickle, "approval-required"));
      const request = yield* Fiber.join(requested);
      yield* runtime.respondToRuntimeRequest({ requestId: request!.id, decision: "accept" });
      const ended = yield* Fiber.join(terminal);
      assert.equal(ended?.status, "failed");
      assert.equal(
        ended?.status === "failed" ? ended.failure.message : undefined,
        "OpenCode is waiting on a request T3 Code couldn't answer.",
      );
    }).pipe(Effect.scoped),
  );

  it.effect("counts an answer to a request OpenCode already dropped as delivered", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed(
        [
          out("session.prompt", { sessionID: SESSION, text: "<any>" }),
          promptAccepted,
          shellAskEvent,
          // The execution ended first, so OpenCode no longer knows the request.
          out("permission.reply", {
            sessionID: SESSION,
            requestID: shellAsk.data.id,
            decision: "once",
          }),
          reply("permission.reply", {
            status: 404,
            body: {
              _tag: "PermissionNotFoundError",
              requestID: shellAsk.data.id,
              message: "Permission request not found",
            },
          }),
          event("session.execution.succeeded", { sessionID: SESSION }),
        ],
        { supervised: true },
      );
      const requested = yield* requestOf(runtime).pipe(Effect.forkScoped);
      const terminal = yield* terminalOf(runtime).pipe(Effect.forkScoped);
      yield* runtime.startTurn(turnInput(thread, bigPickle, "approval-required"));
      const request = yield* Fiber.join(requested);
      yield* runtime.respondToRuntimeRequest({ requestId: request!.id, decision: "accept" });
      // Not "waiting on a request T3 Code couldn't answer": nothing waits on it.
      assert.equal((yield* Fiber.join(terminal))?.status, "completed");
    }).pipe(Effect.scoped),
  );

  it.effect("declines a form T3 cannot show with the reason, instead of leaving it open", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        event("form.created", {
          form: {
            id: "frm_0eb79ab35001fkvFECSh3wYNVD",
            sessionID: SESSION,
            title: "MCP authorization",
            metadata: { kind: "mcp" },
            fields: [
              {
                key: "authorization",
                type: "external",
                url: "https://example.com/authorize",
                title: "Authorize",
              },
            ],
          },
        }),
        out("session.form.cancel", {
          sessionID: SESSION,
          formID: "frm_0eb79ab35001fkvFECSh3wYNVD",
        }),
        reply("session.form.cancel", null),
      ]);
      const terminal = yield* terminalOf(runtime).pipe(Effect.forkScoped);
      yield* runtime.startTurn(turnInput(thread));
      const ended = yield* Fiber.join(terminal);
      assert.equal(ended?.status, "failed");
      assert.include(
        ended?.status === "failed" ? ended.failure.message : "",
        "asked for a link to open",
      );
    }).pipe(Effect.scoped),
  );

  it.effect("asks a subagent's permission request on the parent thread's turn", () =>
    Effect.gen(function* () {
      const child = "ses_f0e5aa64cffelFnoQRL0DAA9BH";
      const { runtime, thread } = yield* resumed(
        [
          out("session.prompt", { sessionID: SESSION, text: "<any>" }),
          promptAccepted,
          // As 2.0.18 announced the child the live probe's parent started.
          event("session.created", {
            sessionID: child,
            slug: "stellar-garden",
            version: "2.0.18",
            projectID: "global",
            parentID: SESSION,
            location: { directory: WORK },
            subpath: "",
            title: "Echo test command",
            agent: "general",
            permissions: supervisedRules,
            model: { id: "big-pickle", providerID: "opencode", variant: "default" },
          }),
          event("permission.asked", { ...shellAsk.data, sessionID: child }),
          out("permission.reply", {
            sessionID: child,
            requestID: shellAsk.data.id,
            decision: "once",
          }),
          reply("permission.reply", null),
          event("permission.replied", {
            sessionID: child,
            requestID: shellAsk.data.id,
            reply: "once",
          }),
          event("session.execution.succeeded", { sessionID: SESSION }),
        ],
        { supervised: true },
      );
      const requested = yield* requestOf(runtime).pipe(Effect.forkScoped);
      const terminal = yield* terminalOf(runtime).pipe(Effect.forkScoped);
      yield* runtime.startTurn(turnInput(thread, bigPickle, "approval-required"));
      const request = yield* Fiber.join(requested);
      assert.equal(request?.nativeRequestRef?.nativeId, shellAsk.data.id);
      yield* runtime.respondToRuntimeRequest({ requestId: request!.id, decision: "accept" });
      assert.equal((yield* Fiber.join(terminal))?.status, "completed");
    }).pipe(Effect.scoped),
  );

  /**
   * A supervised foreground subagent that asks to run `echo FIRST`: the
   * child is announced and named by the parent's call, then asks.
   */
  const subagentAsks = (child: string): ReadonlyArray<ProviderReplayEntry> => {
    const call = "call-subagent";
    const tool = { sessionID: SESSION, assistantMessageID: "msg_assistant", id: call };
    return [
      out("session.prompt", { sessionID: SESSION, text: "<any>" }),
      promptAccepted,
      event("session.execution.started", { sessionID: SESSION }),
      event("session.tool.input.started", { ...tool, name: "subagent" }),
      event("session.tool.called", {
        ...tool,
        name: "subagent",
        input: { description: "Echo", prompt: "echo" },
        executed: false,
      }),
      event("session.created", { ...childCreated(child), title: "Echo" }),
      event("session.tool.progress", {
        ...tool,
        metadata: { sessionID: child, status: "running" },
      }),
      event("session.execution.started", { sessionID: child }),
      event("permission.asked", { ...shellAsk.data, sessionID: child }),
    ];
  };

  it.effect("keeps a subagent's 'allow this session' in the subagent's own rules", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed(
        [
          ...subagentAsks(CHILD),
          // The subagent's session asked, so the grant is its rule, not the parent's.
          // Supervised rules for the subagent's own agent (`general`, which lists
          // no path rules here) with the grant.
          out("session.update", {
            sessionID: CHILD,
            permissions: [
              ...supervisedRules.slice(0, 3),
              { action: "shell", resource: "echo *", effect: "allow" },
              // A subagent's session may use its thread's T3 MCP server.
              ...mcpRules,
            ],
          }),
          reply("session.update", null),
          out("permission.reply", {
            sessionID: CHILD,
            requestID: shellAsk.data.id,
            decision: "once",
          }),
          reply("permission.reply", null),
          event("session.execution.succeeded", { sessionID: CHILD }),
          event("session.execution.succeeded", { sessionID: SESSION }),
        ],
        { supervised: true },
      );
      const requested = yield* requestOf(runtime).pipe(Effect.forkScoped);
      const terminal = yield* terminalOf(runtime).pipe(Effect.forkScoped);
      yield* runtime.startTurn({
        ...withLineage(thread),
        runtimePolicy: policy("approval-required"),
      });
      const request = yield* Fiber.join(requested);
      yield* runtime.respondToRuntimeRequest({
        requestId: request!.id,
        decision: "acceptForSession",
      });
      assert.equal((yield* Fiber.join(terminal))?.status, "completed");
    }).pipe(Effect.scoped),
  );

  it.effect("stops the subagent whose request's answer cannot be delivered", () =>
    Effect.gen(function* () {
      const replyOut = out("permission.reply", {
        sessionID: CHILD,
        requestID: shellAsk.data.id,
        decision: "once",
      });
      const failedReply = reply("permission.reply", {
        status: 500,
        body: { _tag: "UnknownError", message: "reply failed" },
      });
      const { runtime, thread } = yield* resumed(
        [
          ...subagentAsks(CHILD),
          replyOut,
          failedReply,
          replyOut,
          failedReply,
          // The subagent's session is the one waiting on the answer.
          out("session.interrupt", { sessionID: CHILD }),
          reply("session.interrupt", { interrupted: true }),
        ],
        { supervised: true },
      );
      const requested = yield* requestOf(runtime).pipe(Effect.forkScoped);
      yield* runtime.startTurn({
        ...withLineage(thread),
        runtimePolicy: policy("approval-required"),
      });
      const request = yield* Fiber.join(requested);
      yield* runtime.respondToRuntimeRequest({ requestId: request!.id, decision: "accept" });
    }).pipe(Effect.scoped),
  );

  it.effect("keeps 'allow this session' in the session's rules, not OpenCode's saved grants", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed(
        [
          out("session.prompt", { sessionID: SESSION, text: "<any>" }),
          promptAccepted,
          shellAskEvent,
          // The grant goes into this session's rules; the reply is only `once`.
          out("session.update", {
            sessionID: SESSION,
            permissions: [
              ...supervisedRules.slice(0, 3),
              { action: "shell", resource: "echo *", effect: "allow" },
              ...supervisedRules.slice(3),
            ],
          }),
          reply("session.update", null),
          out("permission.reply", {
            sessionID: SESSION,
            requestID: shellAsk.data.id,
            decision: "once",
          }),
          reply("permission.reply", null),
          event("session.execution.succeeded", { sessionID: SESSION }),
        ],
        { supervised: true },
      );
      const requested = yield* requestOf(runtime).pipe(Effect.forkScoped);
      const terminal = yield* terminalOf(runtime).pipe(Effect.forkScoped);
      yield* runtime.startTurn(turnInput(thread, bigPickle, "approval-required"));
      const request = yield* Fiber.join(requested);
      yield* runtime.respondToRuntimeRequest({
        requestId: request!.id,
        decision: "acceptForSession",
      });
      assert.equal((yield* Fiber.join(terminal))?.status, "completed");
    }).pipe(Effect.scoped),
  );

  it.effect("reads the agents again while a fresh server still lists none", () =>
    Effect.gen(function* () {
      // 2.0.18 answers `/api/agent` with `[]` for a moment after it starts;
      // rules written from that would drop the agents' own paths.
      const runtime = yield* openCode2ReplayRuntime([
        ...opening,
        out("agent.list", "<any>"),
        reply("agent.list", { location: { directory: WORK }, data: [] }),
        out("agent.list", "<any>"),
        reply("agent.list", agentList),
        out("session.create", {
          location: { directory: WORK },
          model: { providerID: "opencode", id: "big-pickle" },
          permissions: supervisedRules,
        }),
        replyData("session.create", sessionInfo({ permissions: supervisedRules })),
      ]);
      const created = yield* runtime
        .ensureThread({
          threadId,
          modelSelection: bigPickle,
          runtimePolicy: policy("approval-required"),
        })
        .pipe(Effect.forkScoped);
      yield* TestClock.adjust("250 millis");
      assert.equal((yield* Fiber.join(created)).nativeThreadRef?.nativeId, SESSION);
    }).pipe(Effect.scoped),
  );

  it.effect("refuses a turn once reconnecting to a lost event stream has given up", () =>
    Effect.gen(function* () {
      // The server goes away with no turn running, and never comes back.
      const { runtime, thread } = yield* resumed([{ type: "runtime_exit", status: "success" }]);
      // Reconnecting retries a few times, seconds apart, then ends the runtime's events.
      const drained = yield* runtime.events.pipe(Stream.runDrain, Effect.forkScoped);
      yield* TestClock.adjust("1 minute");
      yield* Fiber.join(drained);
      const refused = yield* runtime.startTurn(turnInput(thread)).pipe(Effect.flip);
      assert.equal(refused._tag, "ProviderAdapterEventStreamError");
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

  it.effect("fails the session once reconnecting to a lost event stream has given up", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        { type: "runtime_exit", status: "success" },
      ]);
      // The runtime's event stream has one consumer.
      const events = yield* runtime.events.pipe(Stream.runCollect, Effect.forkScoped);
      yield* runtime.startTurn(turnInput(thread));
      yield* TestClock.adjust("1 minute");
      const collected = yield* Fiber.join(events);
      const terminal = collected.find((event) => event.type === "turn.terminal");
      assert.deepInclude(terminal, { status: "failed", threadDisposition: "broken" });
      const last = collected.findLast((event) => event.type === "provider_session.updated");
      assert.equal(
        last?.type === "provider_session.updated" ? last.providerSession.status : undefined,
        "error",
      );
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );
  // A stream that drops and comes back: what the gap lost is read from the
  // session's history, and the turn ends from the server's own state. Real
  // servers only do this on a network blip, so these are driven here.
  const reconnected = (active: Record<string, unknown>, ended?: "succeeded") => [
    { type: "runtime_exit", status: "success" } as const,
    out("event.subscribe"),
    out("session.active"),
    replyData("session.active", active),
    out("message.list", { sessionID: SESSION, order: "desc", limit: "50" }),
    reply("message.list", {
      data: [
        // OpenCode appends an `idle` item after each execution it ends.
        ...(ended === undefined
          ? []
          : [{ id: "msg_idle_gap", time: { created: 3 }, type: "idle", outcome: ended }]),
        {
          id: "msg_assistant_gap",
          time: { created: 2 },
          type: "assistant",
          agent: "build",
          model: { id: "big-pickle", providerID: "opencode", variant: "default" },
          content: [
            { type: "text", text: "Sent while the stream was down." },
            {
              type: "tool",
              id: "call_gap",
              name: "shell",
              executed: true,
              state: {
                status: "completed",
                input: { command: "echo GAP" },
                content: [{ type: "text", text: "GAP" }],
                metadata: {},
              },
              time: { created: 2, ran: 2, completed: 2 },
            },
          ],
          finish: "stop",
        },
        {
          id: "msg_0eb735d41001NJee1EvVePJAK5",
          time: { created: 1 },
          text: "hi",
          type: "user",
        },
      ],
      cursor: {},
    }),
  ];
  const turnItems = (collected: ReadonlyArray<ProviderAdapterV2Event>) =>
    collected.flatMap((event) =>
      event.type === "turn_item.updated" ? [`${event.turnItem.type}:${event.turnItem.status}`] : [],
    );

  it.effect("does not end a turn from an earlier turn's history when its prompt is not there", () =>
    Effect.gen(function* () {
      // The stream drops before the turn's prompt reached the history: what the
      // history holds is the previous turn's, and its `idle` is not this turn's end.
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        { type: "runtime_exit", status: "success" } as const,
        out("event.subscribe"),
        out("session.active"),
        replyData("session.active", {}),
        out("message.list", { sessionID: SESSION, order: "desc", limit: "50" }),
        reply("message.list", {
          data: [
            { id: "msg_idle_earlier", time: { created: 3 }, type: "idle", outcome: "succeeded" },
            {
              id: "msg_assistant_earlier",
              time: { created: 2 },
              type: "assistant",
              agent: "build",
              model: { id: "big-pickle", providerID: "opencode", variant: "default" },
              content: [{ type: "text", text: "An earlier turn's answer." }],
              finish: "stop",
            },
            { id: "msg_user_earlier", time: { created: 1 }, text: "earlier", type: "user" },
          ],
          cursor: {},
        }),
      ]);
      const events = yield* runtime.events.pipe(
        Stream.takeUntil((event) => event.type === "turn.terminal"),
        Stream.runCollect,
        Effect.forkScoped,
      );
      yield* runtime.startTurn(turnInput(thread));
      const collected = yield* Fiber.join(events);
      assert.notInclude(
        collected.flatMap((event) =>
          event.type === "turn_item.updated" && event.turnItem.type === "assistant_message"
            ? [event.turnItem.text]
            : [],
        ),
        "An earlier turn's answer.",
      );
      assert.deepInclude(collected.at(-1), { type: "turn.terminal", status: "interrupted" });
    }).pipe(Effect.scoped),
  );

  it.effect("ends a turn that finished while the stream was down with the server's outcome", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, id: PROMPT_ID, text: "<any>" }),
        promptAccepted,
        ...reconnected({}, "succeeded"),
      ]);
      const events = yield* runtime.events.pipe(
        Stream.takeUntil((event) => event.type === "turn.terminal"),
        Stream.runCollect,
        Effect.forkScoped,
      );
      yield* runtime.startTurn(turnInput(thread));
      const collected = yield* Fiber.join(events);
      // The reply and the tool call it ran are shown before the turn ends.
      assert.includeMembers(turnItems(collected), [
        "assistant_message:completed",
        "command_execution:completed",
      ]);
      assert.deepInclude(collected.at(-1), { type: "turn.terminal", status: "completed" });
    }).pipe(Effect.scoped),
  );

  it.effect("finishes a workspace command whose stream dropped before its inbox event", () =>
    Effect.gen(function* () {
      // `session.command` answers 204 without the inbox item's id, and the
      // stream drops before `session.inbox.enqueued` would have named it.
      const { runtime, thread } = yield* resumed([
        out("command.list", "<any>"),
        reply("command.list", { location: { directory: WORK }, data: [{ name: "hello" }] }),
        // Where the history stood before the command.
        out("message.list", { sessionID: SESSION, order: "desc", limit: "1" }),
        reply("message.list", {
          data: [
            { id: "msg_idle_before", time: { created: 1 }, type: "idle", outcome: "succeeded" },
          ],
          cursor: {},
        }),
        out("session.command", { sessionID: SESSION, name: "hello", text: "WORLD" }),
        reply("session.command", null),
        { type: "runtime_exit", status: "success" } as const,
        out("event.subscribe"),
        out("session.active"),
        replyData("session.active", {}),
        out("message.list", { sessionID: SESSION, order: "desc", limit: "50" }),
        reply("message.list", {
          data: [
            { id: "msg_idle_cmd", time: { created: 4 }, type: "idle", outcome: "succeeded" },
            {
              id: "msg_assistant_cmd",
              time: { created: 3 },
              type: "assistant",
              agent: "build",
              model: { id: "big-pickle", providerID: "opencode", variant: "default" },
              content: [{ type: "text", text: "HELLO WORLD" }],
              finish: "stop",
            },
            // The command's expanded template, the turn's own user item.
            {
              id: "msg_user_cmd",
              time: { created: 2 },
              text: "Reply with exactly: HELLO WORLD",
              type: "user",
            },
            // The previous turn, which already ended before this one started.
            { id: "msg_idle_before", time: { created: 1 }, type: "idle", outcome: "succeeded" },
          ],
          cursor: {},
        }),
      ]);
      const events = yield* runtime.events.pipe(
        Stream.takeUntil((event) => event.type === "turn.terminal"),
        Stream.runCollect,
        Effect.forkScoped,
      );
      yield* runtime.startTurn({
        ...turnInput(thread),
        message: { ...turnInput(thread).message, text: "/hello WORLD" },
      });
      const collected = yield* Fiber.join(events);
      const texts = collected.flatMap((event) =>
        event.type === "turn_item.updated" && event.turnItem.type === "assistant_message"
          ? [event.turnItem.text]
          : [],
      );
      assert.include(texts, "HELLO WORLD");
      assert.deepInclude(collected.at(-1), { type: "turn.terminal", status: "completed" });
    }).pipe(Effect.scoped),
  );

  it.live("returns the server it reconnected to once the session closes", () =>
    Effect.gen(function* () {
      // A spawned server stops after it has no borrowers for a while, so a
      // session must not keep holding the connection it reconnected with.
      const borrowers = { current: 0 };
      yield* Effect.gen(function* () {
        yield* openCode2ReplayRuntime(
          [
            ...opening,
            { type: "runtime_exit", status: "success" },
            out("event.subscribe"),
            event("server.connected", {}),
          ],
          { borrowers },
        );
        // Reconnected: the dropped connection is returned and the new one is held.
        yield* Effect.sleep("200 millis");
        assert.equal(borrowers.current, 1);
      }).pipe(Effect.scoped);
      assert.equal(borrowers.current, 0);
    }),
  );

  it.effect("keeps a turn still running after a reconnect open for its next events", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, id: PROMPT_ID, text: "<any>" }),
        promptAccepted,
        ...reconnected({ [SESSION]: { type: "running" } }),
        event("session.text.ended", {
          sessionID: SESSION,
          assistantMessageID: "msg_assistant_after",
          ordinal: 0,
          text: "Arrived on the new stream.",
        }),
        event("session.execution.succeeded", { sessionID: SESSION }),
      ]);
      const events = yield* runtime.events.pipe(
        Stream.takeUntil((event) => event.type === "turn.terminal"),
        Stream.runCollect,
        Effect.forkScoped,
      );
      yield* runtime.startTurn(turnInput(thread));
      const collected = yield* Fiber.join(events);
      const texts = collected.flatMap((event) =>
        event.type === "turn_item.updated" && event.turnItem.type === "assistant_message"
          ? [event.turnItem.text]
          : [],
      );
      assert.includeMembers(texts, [
        "Sent while the stream was down.",
        "Arrived on the new stream.",
      ]);
      assert.deepInclude(collected.at(-1), { type: "turn.terminal", status: "completed" });
    }).pipe(Effect.scoped),
  );

  it.effect(
    "registers T3's MCP server for the thread alone and removes it when the thread unloads",
    () =>
      Effect.gen(function* () {
        McpProviderSession.setMcpProviderSession({
          environmentId: EnvironmentId.make("environment:opencode2-adapter"),
          threadId,
          providerSessionId: "mcp:opencode2-adapter",
          providerInstanceId: instanceId,
          endpoint: "http://127.0.0.1:3773/mcp",
          authorizationHeader: "Bearer thread-credential",
          browserToolsAvailable: false,
        });
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => McpProviderSession.clearMcpProviderSession(threadId)),
        );
        const server = "t3-code-thread_opencode2-adapter";
        const { runtime, thread } = yield* resumed([
          // Registered for the session's directory under the thread's own name;
          // the session's rules allow only this name's tools (see `t3Rules`).
          out("mcp.add", {
            server,
            "location[directory]": WORK,
            config: {
              type: "remote",
              url: "http://127.0.0.1:3773/mcp",
              headers: { Authorization: "Bearer thread-credential" },
              oauth: false,
            },
          }),
          reply("mcp.add", null),
          out("session.prompt", { sessionID: SESSION, text: "<any>" }),
          promptAccepted,
          event("session.execution.succeeded", { sessionID: SESSION }),
          out("mcp.remove", { server, "location[directory]": WORK }),
          reply("mcp.remove", null),
        ]);
        const terminal = yield* terminalOf(runtime).pipe(Effect.forkScoped);
        yield* runtime.startTurn(turnInput(thread));
        assert.equal((yield* Fiber.join(terminal))?.status, "completed");
        yield* runtime.unloadThread!({ providerThread: thread });
      }).pipe(Effect.scoped),
  );

  it.effect("reads user and assistant text from the session's message list", () =>
    Effect.gen(function* () {
      const runtime = yield* openCode2ReplayRuntimeWithInstructions([
        ...opening,
        out("message.list", { sessionID: SESSION, order: "asc", limit: "100" }),
        reply("message.list", history),
      ]);
      const snapshot = yield* runtime.readThreadSnapshot({
        providerThread: providerThread(yield* DateTime.now),
      });
      assert.deepEqual(
        snapshot.messages.map((message) => [message.role, message.text]),
        [
          ["user", history.data[0]!.text],
          ["assistant", "391 is not prime: it's the product 17 × 23."],
        ],
      );
      assert.equal(
        snapshot.providerThread.nativeConversationHeadRef?.nativeId,
        history.data[0]!.id,
      );
    }).pipe(Effect.scoped),
  );

  it.effect("ends a turn before re-reading a model list that never answers", () =>
    Effect.gen(function* () {
      // The session opened before the catalog loaded, so the turn's model has no window.
      const runtime = yield* openCode2ReplayRuntimeWithInstructions([
        out("event.subscribe"),
        out("model.list", "<any>"),
        reply("model.list", { location: { directory: WORK }, data: [] }),
        out("session.get", { sessionID: SESSION }),
        replyData("session.get", sessionInfo()),
        ...noOpenRequests,
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        event("session.execution.succeeded", { sessionID: SESSION }),
        out("model.list", "<any>"),
        reply("model.list", "<hang>"),
      ]);
      const thread = yield* runtime.resumeThread({
        providerThread: providerThread(yield* DateTime.now),
        threadId,
        modelSelection: bigPickle,
        runtimePolicy: policy(),
      });
      const terminal = yield* terminalOf(runtime).pipe(Effect.forkScoped);
      yield* runtime.startTurn(turnInput(thread));
      assert.equal((yield* Fiber.join(terminal))?.status, "completed");
      // The terminal did not wait on the re-read, which is still in flight.
      for (let i = 0; i < 20; i++) yield* Effect.yieldNow;
    }).pipe(Effect.scoped),
  );

  it.effect("reports cache writes as cache creation, not only as input", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        // A step from a provider that reports prompt-cache writes.
        event("session.step.ended", {
          sessionID: SESSION,
          assistantMessageID: "msg_0eb735d5b001oAFVeY5jz3WD4Z",
          finish: "stop",
          cost: 0,
          tokens: { input: 1200, output: 40, reasoning: 0, cache: { read: 300, write: 2500 } },
        }),
        event("session.execution.succeeded", { sessionID: SESSION }),
      ]);
      const turn = yield* runtime.events.pipe(
        Stream.filter(
          (event) =>
            event.type === "provider_turn.updated" && event.providerTurn.completedAt !== null,
        ),
        Stream.runHead,
        Effect.forkScoped,
      );
      yield* runtime.startTurn(turnInput(thread));
      const settled = Option.getOrUndefined(yield* Fiber.join(turn));
      assert.deepInclude(
        settled?.type === "provider_turn.updated" ? settled.providerTurn.turnTokenUsage : undefined,
        {
          inputTokens: 1200 + 300 + 2500,
          cachedInputTokens: 300,
          cacheCreationTokens: 2500,
          outputTokens: 40,
        },
      );
    }).pipe(Effect.scoped),
  );

  it.effect("reports a model's input limit as its context window", () =>
    Effect.gen(function* () {
      const runtime = yield* openCode2ReplayRuntime([...opening]);
      assert.equal(runtime.getModelContextWindow?.(bigPickle), 160000);
    }).pipe(Effect.scoped),
  );

  it.effect("clears a staged revert whose commit failed, before the next prompt too", () =>
    Effect.gen(function* () {
      const kept = "msg_t3_turn_run-attempt:kept:1";
      const dropped = "msg_t3_turn_run-attempt:dropped:1";
      const { runtime, thread } = yield* resumed([
        out("message.list", "<any>"),
        reply("message.list", {
          data: [
            { id: dropped, time: { created: 2 }, text: "second", type: "user" },
            { id: kept, time: { created: 1 }, text: "first", type: "user" },
          ],
          cursor: {},
        }),
        out("session.revert.stage", { sessionID: SESSION, messageID: dropped, files: false }),
        replyData("session.revert.stage", { messageID: dropped, files: [] }),
        out("session.revert.commit", { sessionID: SESSION }),
        reply("session.revert.commit", {
          status: 500,
          body: { _tag: "UnknownError", message: "disk I/O error" },
        }),
        // Uncleared, OpenCode would commit the stage on the next prompt.
        out("session.revert.clear", { sessionID: SESSION }),
        reply("session.revert.clear", {
          status: 500,
          body: { _tag: "UnknownError", message: "busy" },
        }),
        // The next turn clears it first, then waits out the empty execution `clear` runs.
        out("session.revert.clear", { sessionID: SESSION }),
        reply("session.revert.clear", null),
        event("session.execution.started", { sessionID: SESSION }),
        event("session.execution.succeeded", { sessionID: SESSION }),
        out("session.prompt", { sessionID: SESSION, id: "<any>", text: "<any>" }),
        promptAccepted,
        event("session.execution.started", { sessionID: SESSION }),
        event("session.execution.succeeded", { sessionID: SESSION }),
      ]);
      const now = yield* DateTime.now;
      const turn = (attempt: string, ordinal: number) => ({
        id: ProviderTurnId.make(`provider-turn:${attempt}`),
        providerThreadId: thread.id,
        nodeId: NodeId.make(`node:${attempt}`),
        runAttemptId: RunAttemptId.make(`run-attempt:${attempt}:1`),
        nativeTurnRef: {
          driver: OPENCODE_PROVIDER,
          nativeId: `msg_t3_turn_run-attempt:${attempt}:1`,
          strength: "weak" as const,
        },
        ordinal,
        status: "completed" as const,
        startedAt: now,
        completedAt: now,
      });
      const rollback = yield* runtime
        .rollbackThread({
          providerThread: thread,
          target: {
            type: "provider_turn",
            checkpointId: CheckpointId.make("checkpoint:kept"),
            appRunOrdinal: 1,
            providerTurn: turn("kept", 1),
          },
          providerThreadTurns: [turn("kept", 1), turn("dropped", 2)],
        })
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(rollback));
      yield* runtime.startTurn(turnInput(thread));
      assert.equal((yield* terminalOf(runtime))?.status, "completed");
    }).pipe(Effect.scoped),
  );

  it.effect("offers no follow-up for the execution a cleared revert starts", () =>
    Effect.gen(function* () {
      const kept = "msg_t3_turn_run-attempt:kept:1";
      const dropped = "msg_t3_turn_run-attempt:dropped:1";
      const offers: Array<ProviderContinuationRequest> = [];
      const { runtime, thread } = yield* resumed([
        out("message.list", "<any>"),
        reply("message.list", {
          data: [
            { id: dropped, time: { created: 2 }, text: "second", type: "user" },
            { id: kept, time: { created: 1 }, text: "first", type: "user" },
          ],
          cursor: {},
        }),
        out("session.revert.stage", { sessionID: SESSION, messageID: dropped, files: false }),
        replyData("session.revert.stage", { messageID: dropped, files: [] }),
        out("session.revert.commit", { sessionID: SESSION }),
        reply("session.revert.commit", {
          status: 500,
          body: { _tag: "UnknownError", message: "disk I/O error" },
        }),
        // Uncleared, OpenCode would commit the stage on the next prompt. The
        // clear wakes the session into an empty execution of its own, with no
        // turn of T3's running: it is no subagent's follow-up.
        out("session.revert.clear", { sessionID: SESSION }),
        reply("session.revert.clear", null),
        event("session.revert.cleared", { sessionID: SESSION }),
        event("session.execution.started", { sessionID: SESSION }),
        event("session.execution.succeeded", { sessionID: SESSION }),
      ]).pipe(
        Effect.provideService(ProviderContinuationRequests.ProviderContinuationRequests, {
          offer: (request) => Effect.sync(() => void offers.push(request)),
          take: Effect.never,
        }),
      );
      const now = yield* DateTime.now;
      const turn = (attempt: string, ordinal: number) => ({
        id: ProviderTurnId.make(`provider-turn:${attempt}`),
        providerThreadId: thread.id,
        nodeId: NodeId.make(`node:${attempt}`),
        runAttemptId: RunAttemptId.make(`run-attempt:${attempt}:1`),
        nativeTurnRef: {
          driver: OPENCODE_PROVIDER,
          nativeId: `msg_t3_turn_run-attempt:${attempt}:1`,
          strength: "weak" as const,
        },
        ordinal,
        status: "completed" as const,
        startedAt: now,
        completedAt: now,
      });
      const rollback = yield* runtime
        .rollbackThread({
          providerThread: thread,
          target: {
            type: "provider_turn",
            checkpointId: CheckpointId.make("checkpoint:kept"),
            appRunOrdinal: 1,
            providerTurn: turn("kept", 1),
          },
          providerThreadTurns: [turn("kept", 1), turn("dropped", 2)],
        })
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(rollback));
      assert.lengthOf(offers, 0);
      assert.isFalse(yield* runtime.hasPendingBackgroundWorkForThread!(thread));
    }).pipe(Effect.scoped),
  );

  it.effect("ends a follow-up turn a steer joined exactly once", () =>
    Effect.gen(function* () {
      const reportText = `<subagent sessionID="${CHILD}" state="completed" description="Sleep">\nCHILD_OK\n</subagent>`;
      const steerId = "msg_t3_steer_message:opencode2-adapter:steer";
      const offers: Array<ProviderContinuationRequest> = [];
      const { runtime, thread } = yield* resumed([
        ...backgroundLaunch(CHILD),
        event("session.execution.succeeded", { sessionID: SESSION }),
        event("session.execution.succeeded", { sessionID: CHILD }),
        event("session.inbox.enqueued", {
          inboxID: "msg_report",
          sessionID: SESSION,
          item: {
            type: "synthetic",
            payload: {
              text: reportText,
              description: "Sleep",
              metadata: {
                source: "subagent",
                childID: CHILD,
                agent: "General",
                state: "completed",
              },
            },
            delivery: "steer",
          },
        }),
        // OpenCode starts the follow-up on its own; T3 holds it for its turn.
        event("session.execution.started", { sessionID: SESSION }),
        event("session.inbox.delivered", { sessionID: SESSION, inboxID: "msg_report" }),
        // The user steers into the follow-up turn; its execution reads the steer.
        out("session.prompt", {
          sessionID: SESSION,
          id: steerId,
          text: "<any>",
          delivery: "steer",
        }),
        replyData("session.prompt", {
          id: steerId,
          sessionID: SESSION,
          time: { created: 1790656601500 },
          type: "user",
          payload: { text: "Also say STEERED." },
          delivery: "steer",
        }),
        event("session.inbox.delivered", { sessionID: SESSION, inboxID: steerId }),
        event("session.text.ended", {
          sessionID: SESSION,
          assistantMessageID: "msg_followup",
          ordinal: 0,
          text: "CHILD_OK STEERED",
        }),
        event("session.execution.succeeded", { sessionID: SESSION }),
      ]).pipe(
        Effect.provideService(ProviderContinuationRequests.ProviderContinuationRequests, {
          offer: (request) => Effect.sync(() => void offers.push(request)),
          take: Effect.never,
        }),
      );
      const terminals: Array<string> = [];
      const followUpEnded = yield* Deferred.make<void>();
      yield* runtime.events.pipe(
        Stream.tap((event) =>
          Effect.gen(function* () {
            if (event.type !== "turn.terminal") return;
            terminals.push(event.status);
            if (terminals.length === 2) yield* Deferred.succeed(followUpEnded, undefined);
          }),
        ),
        Stream.runDrain,
        Effect.forkScoped,
      );
      yield* runtime.startTurn(withLineage(thread));
      // The launching turn ends; then the held follow-up is offered and taken.
      yield* Effect.gen(function* () {
        while (offers.length === 0) yield* Effect.yieldNow;
      }).pipe(Effect.timeout("2 seconds"), Effect.orDie);
      const followUp = {
        ...withLineage(thread),
        runId: RunId.make("run:opencode2-adapter:wake"),
        runOrdinal: 2,
        providerTurnOrdinal: 2,
        attemptId: RunAttemptId.make("attempt:opencode2-adapter:wake"),
        message: {
          ...turnInput(thread).message,
          messageId: MessageId.make("message:opencode2-adapter:wake"),
          createdBy: "agent" as const,
          creationSource: "provider" as const,
        },
      };
      yield* runtime.startTurn(followUp);
      const followUpTurnId = (yield* IdAllocator.IdAllocatorV2).derive.providerTurn({
        driver: OPENCODE_PROVIDER,
        nativeTurnId: `${SESSION}:attempt:attempt:opencode2-adapter:wake`,
      });
      yield* runtime.steerTurn({
        threadId,
        runId: followUp.runId,
        providerThread: thread,
        providerTurnId: followUpTurnId,
        message: {
          ...turnInput(thread).message,
          messageId: MessageId.make("message:opencode2-adapter:steer"),
          text: "Also say STEERED.",
        },
      });
      yield* Deferred.await(followUpEnded);
      // The launching turn and the steered follow-up end once each. The
      // replay has no frame after the follow-up's end, so a second end of the
      // follow-up (or another continuation) would show here or fail the replay.
      yield* Effect.yieldNow;
      assert.deepEqual(terminals, ["completed", "completed"]);
      assert.lengthOf(offers, 1);
    }).pipe(Effect.scoped, Effect.provide(IdAllocator.layer)),
  );
});

/** The provider turn the adapter derives for `turnInput`'s attempt. */
const providerTurnId = Effect.gen(function* () {
  const ids = yield* IdAllocator.IdAllocatorV2;
  return ids.derive.providerTurn({
    driver: OPENCODE_PROVIDER,
    nativeTurnId: `${SESSION}:attempt:attempt:opencode2-adapter`,
  });
}).pipe(Effect.provide(IdAllocator.layer));
