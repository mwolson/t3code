/**
 * Failures and settings a live OpenCode 2 server cannot produce on demand,
 * driven through the real adapter and `@opencode/client` against a replayed
 * HTTP server. Frames reuse the shapes recorded against 2.0.18.
 */
import { assert, it } from "@effect/vitest";
import {
  MessageId,
  NodeId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
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
import * as IdAllocator from "../IdAllocator.ts";
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

/** The rules T3 gives every session it runs. */
const t3Rules = [{ action: "*", resource: "*", effect: "allow" }];
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

const promptAccepted = replyData("session.prompt", {
  id: "msg_0eb735d41001NJee1EvVePJAK5",
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
      [
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
      ],
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
      const runtime = yield* openCode2ReplayRuntime([
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
      const runtime = yield* openCode2ReplayRuntime([
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
        const runtime = yield* openCode2ReplayRuntime([
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
      const runtime = yield* openCode2ReplayRuntime(
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

  it.effect("refuses a turn after the event stream is gone instead of prompting", () =>
    Effect.gen(function* () {
      // The stream ends with no turn running; no prompt is expected after it.
      const { runtime, thread } = yield* resumed([{ type: "runtime_exit", status: "success" }]);
      // The runtime ends its own events once the lost stream is settled.
      yield* runtime.events.pipe(Stream.runDrain);
      const refused = yield* runtime
        .startTurn(turnInput(thread))
        .pipe(Effect.flip, Effect.timeout("5 seconds"));
      assert.equal(refused._tag, "ProviderAdapterEventStreamError");
    }).pipe(Effect.scoped),
  );

  it.effect("reports the session as failed after a lost stream settles its turns", () =>
    Effect.gen(function* () {
      const { runtime, thread } = yield* resumed([
        out("session.prompt", { sessionID: SESSION, text: "<any>" }),
        promptAccepted,
        { type: "runtime_exit", status: "success" },
      ]);
      // The runtime's event stream has one consumer.
      const events = yield* runtime.events.pipe(Stream.runCollect, Effect.forkScoped);
      yield* runtime.startTurn(turnInput(thread));
      const collected = yield* Fiber.join(events);
      const last = collected.findLast((event) => event.type === "provider_session.updated");
      assert.equal(
        last?.type === "provider_session.updated" ? last.providerSession.status : undefined,
        "error",
      );
    }).pipe(Effect.scoped),
  );
  it.effect("reads user and assistant text from the session's message list", () =>
    Effect.gen(function* () {
      const runtime = yield* openCode2ReplayRuntime([
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
      const runtime = yield* openCode2ReplayRuntime([
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
});

/** The provider turn the adapter derives for `turnInput`'s attempt. */
const providerTurnId = Effect.gen(function* () {
  const ids = yield* IdAllocator.IdAllocatorV2;
  return ids.derive.providerTurn({
    driver: OPENCODE_PROVIDER,
    nativeTurnId: `${SESSION}:attempt:attempt:opencode2-adapter`,
  });
}).pipe(Effect.provide(IdAllocator.layer));
