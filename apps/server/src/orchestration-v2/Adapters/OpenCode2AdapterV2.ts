/**
 * The OpenCode 2 runtime behind the `opencode` driver. It talks to the
 * instance's `opencode serve` process through the HTTP client and reads that
 * server's `/api/event` stream, routed here by session id.
 *
 * A turn is one `session.prompt`; the session's next `session.execution.*`
 * terminal ends it. Each runtime mode is a set of session permission rules,
 * and OpenCode's permission asks and question forms become runtime requests
 * on the asking session's thread. Subagents, steering, fork, rollback and
 * compaction arrive in later layers: the `subagent` tool is denied and the
 * capabilities below say no.
 *
 * @module orchestration-v2/Adapters/OpenCode2AdapterV2
 */
import {
  AbsolutePath,
  Form,
  Location,
  Model,
  Permission,
  Provider,
  Session,
  type OpenCodeEvent,
} from "@opencode/client/effect";
import type {
  OrchestrationV2ConversationMessage,
  OrchestrationV2ExecutionNode,
  OrchestrationV2ProviderCapabilities,
  OrchestrationV2ProviderSession,
  OrchestrationV2ProviderThread,
  OrchestrationV2ProviderTurn,
  OrchestrationV2RuntimeRequest,
  OrchestrationV2TurnItem,
  OrchestrationV2UserInputQuestion,
  ProviderApprovalDecision,
  ProviderInstanceId,
  RuntimeRequestId,
} from "@t3tools/contracts";
import type * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Queue from "effect/Queue";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as ServerConfig from "../../config.ts";
import { paginate, type OpenCode2StreamEvent } from "../../provider/opencode2/OpenCode2Client.ts";
import * as OpenCode2Server from "../../provider/opencode2/OpenCode2Server.ts";
import {
  parseOpenCodeModelSlug,
  type OpenCodeRuntimeError,
} from "../../provider/opencodeRuntime.ts";
import { buildRuntimeInstructions } from "../../provider/RuntimeInstructions.ts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import { causeErrorTag } from "@t3tools/shared/observability";

import { providerMessageTextWithAttachmentPaths } from "../AttachmentPrompt.ts";
import * as IdAllocator from "../IdAllocator.ts";
import { makeProviderFailure } from "../ProviderFailure.ts";
import * as ProviderAdapter from "../ProviderAdapter.ts";
import { turnScopedSelectionTransition } from "../ProviderSelectionTransition.ts";
import { OPENCODE_PROVIDER, openCodePermissionRequestKind } from "./OpenCodeAdapterV2.ts";
import { openCodeToolTurnItem } from "./OpenCodeToolItems.ts";

const OpenCode2ProviderCapabilities = {
  sessions: {
    // One server serves every location, so one session runtime owns them all.
    supportsMultipleProviderThreadsPerSession: true,
    supportsModelSwitchInSession: true,
    supportsProviderSwitchingViaHandoff: true,
    // A mode change rewrites the session's rules when its next turn resumes it.
    supportsRuntimeModeSwitchInSession: true,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: true,
    canReadThreadSnapshot: true,
    canRollbackThread: false,
    canForkThread: false,
    canForkFromTurn: false,
    canForkFromSubagentThread: false,
    exposesNativeThreadId: true,
  },
  turns: {
    exposesNativeTurnId: false,
    emitsTurnStarted: true,
    emitsTurnCompleted: true,
    supportsInterrupt: true,
    supportsActiveSteering: false,
    supportsSteeringByInterruptRestart: true,
    supportsQueuedMessages: true,
    terminalStatusQuality: "strong",
  },
  streaming: {
    streamsAssistantText: true,
    streamsReasoning: true,
    streamsToolOutput: false,
    streamsPlanText: false,
    emitsMessageCompleted: true,
  },
  tools: {
    exposesToolItemIds: true,
    emitsToolStarted: true,
    emitsToolCompleted: true,
    emitsToolOutput: true,
    supportsMcpTools: false,
    supportsDynamicToolCallbacks: false,
  },
  approvals: {
    supportsCommandApproval: true,
    supportsFileReadApproval: true,
    supportsFileChangeApproval: true,
    supportsApplyPatchApproval: true,
    approvalsHaveNativeRequestIds: true,
    approvalCallbacksAreLiveOnly: true,
    approvalsCanOriginateFromSubagents: false,
  },
  planning: {
    emitsPlanUpdated: false,
    emitsTodoList: false,
    emitsProposedPlan: false,
    supportsStructuredQuestions: true,
    planDeltasHaveItemIds: false,
  },
  subagents: {
    supportsSubagents: false,
    exposesSubagentThreadIds: false,
    emitsSubagentLifecycle: false,
    canWaitForSubagents: false,
    canCloseSubagents: false,
    canForkSubagentThread: false,
  },
  context: {
    acceptsSystemContext: false,
    acceptsDeveloperContext: false,
    acceptsSyntheticUserContext: true,
    canGenerateSummaries: false,
    canConsumeHandoffSummaries: true,
    supportsDeltaHandoff: true,
    supportsFullThreadHandoff: true,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: true,
    supportsNestedCheckpointScopes: true,
    providerCanRollbackConversation: false,
    providerRollbackReturnsSnapshot: false,
    providerCanReadConversationSnapshot: true,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "weak",
    nativeItemIds: "strong",
    nativeRequestIds: "strong",
  },
  // OpenCode enforces each runtime mode through the session's permission rules.
  runtimePolicy: { enforcement: "native" },
} satisfies OrchestrationV2ProviderCapabilities;

type EventOf<T extends OpenCodeEvent["type"]> = Extract<OpenCodeEvent, { readonly type: T }>;
type Tokens = EventOf<"session.step.ended">["data"]["tokens"];

interface ActiveTurn {
  readonly input: ProviderAdapter.ProviderAdapterV2TurnInput;
  readonly providerTurn: OrchestrationV2ProviderTurn;
  /** Open text and reasoning blocks, keyed `<assistantMessageID>:<kind>:<ordinal>`. */
  readonly texts: Map<string, OpenBlock>;
  readonly tools: Map<string, { readonly name: string; input: Record<string, unknown> }>;
  readonly startedAt: Map<string, DateTime.Utc>;
  readonly ordinals: Map<string, number>;
  nextOrdinal: number;
  /** Input includes cache reads and writes, as 1.x reports it; the parts are also kept apart. */
  readonly usage: {
    input: number;
    cached: number;
    cacheWrite: number;
    output: number;
    reasoning: number;
  };
  steps: number;
  lastStep: Tokens | undefined;
  interrupted: boolean;
  /**
   * Set on a turn started after a timed-out Stop's run left the server: that
   * run's tail can still be on the stream, and everything before this turn's
   * own `session.execution.started` belongs to it.
   */
  awaitingStart: boolean;
}

interface OpenBlock {
  readonly block: { readonly assistantMessageID: string; readonly ordinal: number };
  readonly kind: "text" | "reasoning";
  readonly startedAt: DateTime.Utc;
  text: string;
}

interface ThreadState {
  readonly sessionId: string;
  providerThread: OrchestrationV2ProviderThread;
  readonly providerTurns: Map<string, OrchestrationV2ProviderTurn>;
  active: ActiveTurn | undefined;
  /** What the native session runs now, so a changed selection is switched before prompting. */
  model: ModelRef | undefined;
  /**
   * Set when a turn ended here while OpenCode may still be running it: a Stop
   * that timed out, a prompt whose request failed without a clear answer, or a
   * request T3 could not answer. Execution events carry only the session id,
   * so the next execution end belongs to that run; it clears this and ends no turn.
   */
  unsettled: boolean;
  /** The session's location, where its agents' path rules are read. */
  directory: string;
  /** The agent the session runs; its own path rules stay in force. */
  agent: string;
  /** The native session's rules as T3 last read or wrote them, and the policy they are for. */
  rules: ReadonlyArray<Rule> | undefined;
  policy: RulesPolicy;
  /** "Allow … this session" answers, kept in the session's rules while T3 has it open. */
  readonly grants: Array<Rule>;
}

type Rule = Permission.Rule;
type RulesPolicy = Pick<
  ProviderAdapter.ProviderAdapterV2RuntimePolicy,
  "runtimeMode" | "interactionMode"
>;
type NativeForm = EventOf<"form.created">["data"]["form"];

/** A permission ask or question form shown to the user and not answered yet. */
interface PendingRequest {
  readonly request: OrchestrationV2RuntimeRequest;
  readonly item: OrchestrationV2TurnItem;
  readonly node: OrchestrationV2ExecutionNode;
  readonly state: ThreadState;
  readonly turn: ActiveTurn;
  /** The session that asked: the thread's own, or one of its subagents'. */
  readonly sessionId: string;
  /** Set once T3 sends its answer; the orchestrator has already recorded it. */
  answering: boolean;
  readonly native:
    | {
        readonly type: "permission";
        readonly id: string;
        readonly action: string;
        readonly resources: ReadonlyArray<string>;
        readonly save: ReadonlyArray<string>;
      }
    | { readonly type: "form"; readonly id: string; readonly form: NativeForm };
}

const rule = (action: string, effect: Rule["effect"]): Rule => ({ action, resource: "*", effect });

/**
 * A session's permission rules. OpenCode checks the agent's rules and then
 * these, and the last rule that matches decides, so these override the
 * agent's. `paths` are the agent's own allows for its directories (saved tool
 * output, the plan agent's plan directory), which the blanket rules here would
 * otherwise override; `grants` are "Always allow this session" answers.
 */
const sessionRules = (
  policy: RulesPolicy,
  paths: ReadonlyArray<Rule>,
  grants: ReadonlyArray<Rule>,
): ReadonlyArray<Rule> => [
  ...(policy.runtimeMode === "full-access"
    ? [rule("*", "allow")]
    : [
        rule("shell", "ask"),
        rule("edit", policy.runtimeMode === "auto-accept-edits" ? "allow" : "ask"),
        rule("external_directory", "ask"),
      ]),
  ...grants,
  // Plan mode writes only its plan, which `paths` allows again. Shell and read
  // are never denied: the free tier refuses sessions whose rules deny them.
  ...(policy.interactionMode === "plan" ? [rule("edit", "deny")] : []),
  ...paths,
  // A background child wakes its parent in a turn T3 would not see.
  rule("subagent", "deny"),
];

const sameRules = (left: ReadonlyArray<Rule> | undefined, right: ReadonlyArray<Rule>) =>
  left?.length === right.length &&
  left.every(
    (entry, index) =>
      entry.action === right[index]?.action &&
      entry.resource === right[index]?.resource &&
      entry.effect === right[index]?.effect,
  );

/**
 * Sent with a declined permission. A reject without a message is OpenCode's
 * "stop": it ends the whole execution, which is Cancel.
 */
const DECLINED = "The user declined this request.";

/**
 * Steered into the session before a decline: a declined shell call reaches the
 * model only as "Unable to execute command", which it retries.
 */
const declinedNote = (action: string, resources: ReadonlyArray<string>) =>
  `The user declined the ${action} request${resources.length === 0 ? "" : ` (${resources.join(", ")})`}. Do not retry it; continue without it or ask the user how to proceed.`;

/** The session-wide choice for a request whose `save` patterns OpenCode would remember. */
const sessionGrantLabel = (action: string, save: ReadonlyArray<string>) =>
  save.every((pattern) => pattern === "*")
    ? `Allow every ${action} request this session`
    : `Allow ${save.join(", ")} this session`;

const text = (value: string | undefined, fallback: string) => value?.trim() || fallback;

/**
 * A form's fields as T3 questions, or why T3 cannot ask them: a link to open,
 * a field shown only for another answer, a hidden field, or a number or yes/no
 * value. OpenCode's question tool only asks text and multi-select fields.
 */
const formQuestions = (
  form: NativeForm,
):
  | { readonly questions: ReadonlyArray<OrchestrationV2UserInputQuestion> }
  | { readonly unsupported: string } => {
  const questions: Array<OrchestrationV2UserInputQuestion> = [];
  for (const [index, field] of form.fields.entries()) {
    if (field.type === "external") return { unsupported: "a link to open" };
    if (field.type !== "string" && field.type !== "multiselect") {
      return { unsupported: `a ${field.type} value` };
    }
    if (field.hidden === true) return { unsupported: "a hidden field" };
    if ((field.when?.length ?? 0) > 0) return { unsupported: "a field that depends on another" };
    const header = text(field.title, `Question ${index + 1}`);
    const options = (field.options ?? []).map((option) => {
      const label = text(option.label, text(option.value, "Option"));
      return { label, description: text(option.description, label), value: option.value };
    });
    questions.push({
      id: field.key,
      header,
      question: text(field.description, header),
      options,
      multiSelect: field.type === "multiselect",
      allowCustomAnswer: field.custom === true || options.length === 0,
    });
  }
  return { questions };
};

/** T3's answers in OpenCode's shape: a list for a multi-select, text otherwise. */
const formAnswer = (form: NativeForm, answers: Readonly<Record<string, unknown>>) => {
  const answer: Record<string, string | ReadonlyArray<string>> = {};
  for (const field of form.fields) {
    const raw = answers[field.key];
    const values = (Array.isArray(raw) ? raw : [raw]).filter(
      (value): value is string => typeof value === "string" && value.trim().length > 0,
    );
    if (values.length > 0) {
      answer[field.key] = field.type === "multiselect" ? values : values.join(", ");
    }
  }
  return answer;
};

const approves = (decision: ProviderApprovalDecision) =>
  decision === "accept" || decision === "acceptForSession" || decision === "acceptAlways";

/**
 * An answer to a request OpenCode already dropped (its execution ended, or
 * its session is gone): nothing waits on it, so it counts as delivered.
 */
const permissionGone = {
  PermissionNotFoundError: () => Effect.void,
  SessionNotFoundError: () => Effect.void,
};
const formGone = {
  FormNotFoundError: () => Effect.void,
  FormAlreadySettledError: () => Effect.void,
  SessionNotFoundError: () => Effect.void,
};

/** The session rules an agent keeps for its own directories, which T3's blanket rules would override. */
const agentPaths = (rules: ReadonlyArray<Rule>) =>
  rules.filter(
    (entry) =>
      entry.effect === "allow" &&
      entry.resource !== "*" &&
      (entry.action === "edit" || entry.action === "external_directory"),
  );

/** One wording for every capability later layers add. */
const notYet = (feature: string) =>
  new ProviderAdapter.ProviderAdapterProtocolError({
    driver: OPENCODE_PROVIDER,
    detail: `OpenCode 2 ${feature} is not supported yet`,
  });

const ref = (nativeId: string, strength: "strong" | "weak" = "strong") => ({
  driver: OPENCODE_PROVIDER,
  nativeId,
  strength,
});

const sessionIdOf = (providerThread: OrchestrationV2ProviderThread) => {
  const nativeId = providerThread.nativeThreadRef?.nativeId;
  return nativeId === undefined || nativeId === null
    ? Effect.fail(
        new ProviderAdapter.ProviderAdapterProtocolError({
          driver: OPENCODE_PROVIDER,
          detail: `Provider thread ${providerThread.id} has no OpenCode session`,
        }),
      )
    : Effect.succeed(nativeId);
};

const textOf = (content: ReadonlyArray<{ readonly type: string; readonly text?: string }>) =>
  content.flatMap((part) => (part.type === "text" && part.text ? [part.text] : [])).join("\n");

const INTERRUPT_TIMEOUT = "10 seconds";
const ACTIVE_CHECK_TIMEOUT = "5 seconds";
/** Answers that mean the server refused a prompt; any other failure may have been accepted. */
const CLEAR_PROMPT_REJECTIONS: ReadonlySet<string> = new Set([
  "InvalidRequestError",
  "ConflictError",
  "UnauthorizedError",
]);

export const OPENCODE_2_STILL_STOPPING =
  "OpenCode is still stopping the previous turn. Send the message again in a moment.";
const REQUEST_REPLY_TIMEOUT = "10 seconds";

/** Whether an answer to a paused request reached the server, trying twice. */
const deliver = <E>(answer: Effect.Effect<void, E>) =>
  answer.pipe(
    Effect.retry({ times: 1 }),
    Effect.timeout(REQUEST_REPLY_TIMEOUT),
    Effect.exit,
    Effect.map(Exit.isSuccess),
  );

type ModelRef = ReturnType<typeof Model.Ref.make>;

// Errors already in the adapter channel keep their tag; only lower-level ones are wrapped.
const isProviderAdapterError = Schema.is(ProviderAdapter.ProviderAdapterV2Error);

/**
 * The model OpenCode should run for a `provider/model` slug and its reasoning
 * variant, or undefined for any other slug: sending none would run OpenCode's
 * default while T3 records the requested model.
 */
const modelRef = (selection: ProviderAdapter.ProviderAdapterV2TurnInput["modelSelection"]) => {
  const parsed = parseOpenCodeModelSlug(selection.model);
  if (parsed === null) return undefined;
  const variant = getModelSelectionStringOptionValue(selection, "variant");
  return Model.Ref.make({
    providerID: Provider.ID.make(parsed.providerID),
    id: Model.ID.make(parsed.modelID),
    ...(variant === undefined ? {} : { variant: Model.VariantID.make(variant) }),
  });
};
const malformedModel = (model: string) =>
  `OpenCode model '${model}' must use provider/model format`;
const sameModel = (left: ModelRef, right: ModelRef | undefined) =>
  left.providerID === right?.providerID &&
  left.id === right?.id &&
  (left.variant ?? "default") === (right?.variant ?? "default");

/** The turn's own tokens: steps add up, and the last step's input is the live context size. */
const turnTokenUsage = (turn: ActiveTurn, status: OrchestrationV2ProviderTurn["status"]) =>
  turn.steps === 0
    ? {
        usageScope: "main_agent" as const,
        usageStatus: "unavailable" as const,
        hasSubagents: false,
      }
    : {
        usageScope: "main_agent" as const,
        usageStatus: status === "completed" ? ("complete" as const) : ("partial" as const),
        inputTokens: turn.usage.input,
        cachedInputTokens: turn.usage.cached,
        cacheCreationTokens: turn.usage.cacheWrite,
        outputTokens: turn.usage.output,
        reasoningTokens: turn.usage.reasoning,
        hasSubagents: false,
      };

/**
 * The adapter for one provider instance. It talks to the instance's
 * {@link OpenCode2Server.OpenCode2Server}, which the driver builds from the instance's settings.
 */
export const make = Effect.fn("OpenCode2Adapter.make")(function* (instanceId: ProviderInstanceId) {
  const server = yield* OpenCode2Server.OpenCode2Server;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const serverConfig = yield* ServerConfig.ServerConfig;
  const driver = OPENCODE_PROVIDER;
  // Context windows by `provider/model`, from the latest `/api/model` read.
  const contextWindows = new Map<string, number>();

  const openSession = Effect.fn("OpenCode2Adapter.openSession")(function* (
    input: Parameters<ProviderAdapter.ProviderAdapterV2Shape["openSession"]>[0],
    connection: OpenCode2Server.OpenCode2Connection,
  ) {
    const { client } = connection;
    const sessionScope = yield* Effect.scope;
    const now = yield* DateTime.now;
    let session: OrchestrationV2ProviderSession = {
      id: input.providerSessionId,
      driver,
      providerInstanceId: instanceId,
      status: "ready",
      cwd: input.runtimePolicy.cwd ?? serverConfig.cwd,
      model: input.modelSelection.model,
      capabilities: OpenCode2ProviderCapabilities,
      createdAt: now,
      updatedAt: now,
      lastError: null,
    };
    const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event, Cause.Done>();
    const threads = new Map<string, ThreadState>();
    // A subagent's session, by its id, to the thread whose session started it.
    const childOwners = new Map<string, ThreadState>();
    const pending = new Map<RuntimeRequestId, PendingRequest>();
    const emit = (event: ProviderAdapter.ProviderAdapterV2Event) =>
      Queue.offer(events, event).pipe(Effect.asVoid);
    const ownerOf = (sessionId: string) => threads.get(sessionId) ?? childOwners.get(sessionId);

    const setSessionStatus = (
      status: OrchestrationV2ProviderSession["status"],
      lastError: string | null,
    ) =>
      Effect.gen(function* () {
        session = { ...session, status, lastError, updatedAt: yield* DateTime.now };
        yield* emit({ type: "provider_session.updated", driver, providerSession: session });
      });

    const ordinalOf = (turn: ActiveTurn, nativeId: string) => {
      const known = turn.ordinals.get(nativeId);
      if (known !== undefined) return known;
      const next = turn.nextOrdinal++;
      turn.ordinals.set(nativeId, next);
      return next;
    };

    const itemBase = (
      state: ThreadState,
      turn: ActiveTurn,
      nativeId: string,
      status: OrchestrationV2TurnItem["status"],
      startedAt: DateTime.Utc,
      completedAt: DateTime.Utc | null,
      updatedAt: DateTime.Utc,
    ) => ({
      id: idAllocator.derive.turnItemFromProviderItem({ driver, nativeItemId: nativeId }),
      threadId: turn.input.threadId,
      runId: turn.input.runId,
      nodeId: idAllocator.derive.nodeFromProviderItem({ driver, nativeItemId: nativeId }),
      providerThreadId: state.providerThread.id,
      providerTurnId: turn.providerTurn.id,
      nativeItemRef: ref(nativeId),
      parentItemId: null,
      ordinal: ordinalOf(turn, nativeId),
      status,
      title: null,
      startedAt,
      completedAt,
      updatedAt,
    });

    const emitNode = (
      state: ThreadState,
      turn: ActiveTurn,
      nativeId: string,
      kind: OrchestrationV2ExecutionNode["kind"],
      status: OrchestrationV2ExecutionNode["status"],
      startedAt: DateTime.Utc,
      completedAt: DateTime.Utc | null,
    ) =>
      emit({
        type: "node.updated",
        driver,
        node: {
          id: idAllocator.derive.nodeFromProviderItem({ driver, nativeItemId: nativeId }),
          threadId: turn.input.threadId,
          runId: turn.input.runId,
          parentNodeId: turn.input.rootNodeId,
          rootNodeId: turn.input.rootNodeId,
          kind,
          status,
          countsForRun: false,
          providerThreadId: state.providerThread.id,
          providerTurnId: turn.providerTurn.id,
          nativeItemRef: ref(nativeId),
          runtimeRequestId: null,
          checkpointScopeId: null,
          startedAt,
          completedAt,
        },
      });

    /** One text or reasoning block, re-emitted with its accumulated text on every change. */
    const emitText = Effect.fnUntraced(function* (
      state: ThreadState,
      turn: ActiveTurn,
      data: {
        readonly assistantMessageID: string;
        readonly ordinal: number;
        readonly text?: string;
      },
      kind: "text" | "reasoning",
      update: (current: string) => string,
      completed = "text" in data,
    ) {
      const nativeId = `${data.assistantMessageID}:${kind}:${data.ordinal}`;
      const updatedAt = yield* DateTime.now;
      const entry = turn.texts.get(nativeId) ?? {
        block: data,
        kind,
        startedAt: updatedAt,
        text: "",
      };
      entry.text = update(entry.text);
      if (completed) turn.texts.delete(nativeId);
      else turn.texts.set(nativeId, entry);
      if (entry.text.length === 0) return;
      const status = completed ? "completed" : "running";
      const completedAt = completed ? updatedAt : null;
      const nodeKind = kind === "text" ? "assistant_message" : "reasoning";
      yield* emitNode(state, turn, nativeId, nodeKind, status, entry.startedAt, completedAt);
      const base = itemBase(state, turn, nativeId, status, entry.startedAt, completedAt, updatedAt);
      if (kind === "reasoning") {
        yield* emit({
          type: "turn_item.updated",
          driver,
          turnItem: { ...base, type: "reasoning", text: entry.text, streaming: !completed },
        });
        return;
      }
      const messageId = idAllocator.derive.messageFromProviderItem({
        driver,
        nativeItemId: nativeId,
      });
      const message: OrchestrationV2ConversationMessage = {
        createdBy: "agent",
        creationSource: "provider",
        id: messageId,
        threadId: turn.input.threadId,
        runId: turn.input.runId,
        nodeId: base.nodeId,
        role: "assistant",
        text: entry.text,
        attachments: [],
        streaming: !completed,
        createdAt: entry.startedAt,
        updatedAt,
      };
      yield* emit({ type: "message.updated", driver, message });
      yield* emit({
        type: "turn_item.updated",
        driver,
        turnItem: {
          ...base,
          type: "assistant_message",
          messageId,
          text: entry.text,
          streaming: !completed,
        },
      });
    });

    const emitTool = Effect.fnUntraced(function* (
      state: ThreadState,
      turn: ActiveTurn,
      id: string,
      status: "running" | "completed" | "failed" | "interrupted",
      result?: { readonly output: string | undefined; readonly metadata: unknown },
    ) {
      const tool = turn.tools.get(id);
      if (tool === undefined) return;
      const updatedAt = yield* DateTime.now;
      const startedAt = turn.startedAt.get(id) ?? updatedAt;
      const completedAt = status === "running" ? null : updatedAt;
      yield* emitNode(state, turn, id, "tool_call", status, startedAt, completedAt);
      yield* emit({
        type: "turn_item.updated",
        driver,
        turnItem: openCodeToolTurnItem(
          itemBase(state, turn, id, status, startedAt, completedAt, updatedAt),
          {
            name: tool.name,
            input: tool.input,
            output: result?.output,
            completedMetadata: status === "completed" ? result?.metadata : undefined,
          },
        ),
      });
    });

    const emitProviderTurn = (
      state: ThreadState,
      turn: ActiveTurn,
      providerTurn: OrchestrationV2ProviderTurn,
    ) => {
      state.providerTurns.set(String(providerTurn.id), providerTurn);
      return emit({
        type: "provider_turn.updated",
        driver,
        threadId: turn.input.threadId,
        providerTurn,
      });
    };

    const finishTurn = Effect.fnUntraced(function* (
      state: ThreadState,
      terminal:
        | { readonly status: "completed" | "interrupted" }
        | { readonly status: "failed"; readonly failure: ReturnType<typeof makeProviderFailure> },
      threadDisposition: "reusable" | "broken" = "reusable",
    ) {
      const turn = state.active;
      if (turn === undefined) return;
      state.active = undefined;
      // OpenCode drops a request when its execution ends; so does the turn.
      for (const entry of pending.values()) {
        if (entry.turn === turn) yield* settleRequest(entry, "cancelled");
      }
      const completedAt = yield* DateTime.now;
      // Blocks still open when the execution ends are final as they stand.
      for (const open of turn.texts.values()) {
        yield* emitText(state, turn, open.block, open.kind, (text) => text, true);
      }
      for (const id of turn.tools.keys()) {
        yield* emitTool(
          state,
          turn,
          id,
          terminal.status === "completed" ? "completed" : "interrupted",
        );
      }
      const window = contextWindows.get(turn.input.modelSelection.model);
      const lastStep = turn.lastStep;
      yield* emitProviderTurn(state, turn, {
        ...turn.providerTurn,
        status: terminal.status,
        completedAt,
        turnTokenUsage: turnTokenUsage(turn, terminal.status),
        ...(lastStep === undefined
          ? {}
          : {
              tokenUsage: {
                usedTokens:
                  lastStep.input + lastStep.cache.read + lastStep.cache.write + lastStep.output,
                maxTokens: window ?? null,
                inputTokens: lastStep.input + lastStep.cache.read + lastStep.cache.write,
                cachedInputTokens: lastStep.cache.read,
                outputTokens: lastStep.output,
                reasoningOutputTokens: lastStep.reasoning,
                updatedAt: DateTime.formatIso(completedAt),
              },
            }),
      });
      state.providerThread = {
        ...state.providerThread,
        status: threadDisposition === "broken" ? "error" : "idle",
        updatedAt: completedAt,
      };
      yield* emit({
        type: "provider_thread.updated",
        driver,
        providerThread: state.providerThread,
      });
      const anyActive = [...threads.values()].some((candidate) => candidate.active !== undefined);
      yield* setSessionStatus(pending.size > 0 ? "waiting" : anyActive ? "running" : "ready", null);
      const base = {
        type: "turn.terminal" as const,
        driver,
        providerThreadId: state.providerThread.id,
        providerTurnId: turn.providerTurn.id,
        runOrdinal: turn.input.runOrdinal,
        threadDisposition,
      };
      yield* emit(
        terminal.status === "failed"
          ? {
              ...base,
              status: "failed",
              failure: terminal.failure,
              failureItemOrdinal: ordinalOf(turn, `terminal-failure:${turn.providerTurn.id}`),
            }
          : { ...base, status: terminal.status, failure: null },
      );
      // A spawned server lists its models lazily, so a window still unknown is
      // read again for the next turn, off this stream so it never delays one.
      if (window === undefined) yield* Effect.forkIn(readModels, sessionScope);
    });

    /**
     * Ends the turn a request T3 could not answer would block, and stops the
     * session: OpenCode waits on an unanswered request forever. The stopped
     * run's end is its own, not the next turn's.
     */
    const abandonRequest = Effect.fnUntraced(function* (state: ThreadState, reason: string) {
      yield* Effect.logWarning("Could not answer an OpenCode request; ending its turn.", {
        reason,
      });
      if (state.active === undefined) return;
      state.unsettled = true;
      yield* finishTurn(state, {
        status: "failed",
        failure: makeProviderFailure({
          message: "OpenCode is waiting on a request T3 Code couldn't answer.",
          class: "provider_error",
        }),
      });
      yield* client.session
        .interrupt({ sessionID: Session.ID.make(state.sessionId) })
        .pipe(Effect.timeout("2 seconds"), Effect.ignore({ log: true }));
    });

    /** Stops tracking a request; the session is waiting only while any remain. */
    const forgetRequest = Effect.fnUntraced(function* (entry: PendingRequest) {
      if (!pending.delete(entry.request.id)) return false;
      if (pending.size === 0 && session.status === "waiting") {
        yield* setSessionStatus("running", null);
      }
      return true;
    });

    /**
     * Settles a request OpenCode answered or dropped on its own. T3's own
     * answers are only forgotten: the orchestrator already recorded them.
     */
    const settleRequest = Effect.fnUntraced(function* (
      entry: PendingRequest,
      status: "resolved" | "cancelled",
    ) {
      if (!(yield* forgetRequest(entry)) || entry.answering) return;
      const now = yield* DateTime.now;
      const itemStatus = status === "resolved" ? "completed" : "cancelled";
      yield* emit({
        type: "runtime_request.updated",
        driver,
        threadId: entry.turn.input.threadId,
        runtimeRequest: { ...entry.request, status, resolvedAt: now },
      });
      yield* emit({
        type: "node.updated",
        driver,
        node: { ...entry.node, status: itemStatus, completedAt: now },
      });
      yield* emit({
        type: "turn_item.updated",
        driver,
        turnItem: { ...entry.item, status: itemStatus, completedAt: now, updatedAt: now },
      });
    });

    /**
     * Stops a run no turn of T3's waits on (one a Stop left running, or one
     * found asking after a reconnect): a reject without a message and a
     * cancelled form both end OpenCode's execution. Its end is not a turn's.
     */
    const stopStaleRequest = (
      sessionId: string,
      native: { readonly type: "permission" | "form"; readonly id: string },
    ): Effect.Effect<void> =>
      native.type === "permission"
        ? client.permission
            .reply({
              sessionID: Session.ID.make(sessionId),
              requestID: Permission.ID.make(native.id),
              decision: "reject",
            })
            .pipe(
              Effect.catchTags(permissionGone),
              Effect.timeout(REQUEST_REPLY_TIMEOUT),
              Effect.ignore({ log: true }),
            )
        : client.session.form
            .cancel({ sessionID: sessionId, formID: Form.ID.make(native.id) })
            .pipe(
              Effect.catchTags(formGone),
              Effect.timeout(REQUEST_REPLY_TIMEOUT),
              Effect.ignore({ log: true }),
            );

    /**
     * Stops what a session still waits on when this runtime first loads it:
     * T3 shows none of those requests (a restart or a closed session expired
     * them), and OpenCode would wait on them forever.
     */
    const stopLeftoverRequests = Effect.fnUntraced(function* (state: ThreadState) {
      const sessionID = Session.ID.make(state.sessionId);
      const listed = yield* Effect.all([
        client.permission.list({ sessionID }),
        client.session.form.list({ sessionID }),
      ]).pipe(Effect.timeout(ACTIVE_CHECK_TIMEOUT), Effect.option);
      if (listed._tag === "None") {
        return yield* Effect.logWarning("Could not list an OpenCode session's open requests.");
      }
      const [permissions, forms] = listed.value;
      if (permissions.length === 0 && forms.length === 0) return;
      // The stopped run's end is not the next turn's.
      state.unsettled = true;
      for (const request of permissions) {
        yield* stopStaleRequest(state.sessionId, { type: "permission", id: request.id });
      }
      for (const form of forms) {
        yield* stopStaleRequest(state.sessionId, { type: "form", id: form.id });
      }
    });

    /**
     * Shows a permission ask or question form on the thread whose session (or
     * subagent session) asked, under that thread's running turn. A request no
     * turn of T3's is waiting on is left for OpenCode's own clients.
     */
    const showRequest = Effect.fnUntraced(function* (
      sessionId: string,
      native: PendingRequest["native"],
      body:
        | {
            readonly type: "approval_request";
            readonly requestKind: Extract<
              OrchestrationV2TurnItem,
              { type: "approval_request" }
            >["requestKind"];
            readonly prompt: string;
            readonly options: Extract<
              OrchestrationV2TurnItem,
              { type: "approval_request" }
            >["options"];
          }
        | {
            readonly type: "user_input_request";
            readonly questions: ReadonlyArray<OrchestrationV2UserInputQuestion>;
          },
    ) {
      const state = ownerOf(sessionId);
      const turn = state?.active;
      if (state === undefined || turn === undefined) return;
      if ([...pending.values()].some((entry) => entry.native.id === native.id)) return;
      const now = yield* DateTime.now;
      const requestId = yield* idAllocator.allocate.runtimeRequest({
        driver,
        providerTurnId: turn.providerTurn.id,
        nativeRequestId: native.id,
      });
      const nodeId = idAllocator.derive.approvalNode({ requestId });
      const request: OrchestrationV2RuntimeRequest = {
        id: requestId,
        nodeId,
        providerTurnId: turn.providerTurn.id,
        nativeRequestRef: ref(native.id),
        kind: body.type === "approval_request" ? body.requestKind : "user_input",
        status: "pending",
        responseCapability: { type: "live", providerSessionId: input.providerSessionId },
        createdAt: now,
        resolvedAt: null,
      };
      const node: OrchestrationV2ExecutionNode = {
        id: nodeId,
        threadId: turn.input.threadId,
        runId: turn.input.runId,
        parentNodeId: turn.input.rootNodeId,
        rootNodeId: turn.input.rootNodeId,
        kind: body.type,
        status: "waiting",
        countsForRun: false,
        providerThreadId: state.providerThread.id,
        providerTurnId: turn.providerTurn.id,
        nativeItemRef: ref(native.id),
        runtimeRequestId: requestId,
        checkpointScopeId: null,
        startedAt: now,
        completedAt: null,
      };
      const base = {
        id: idAllocator.derive.approvalTurnItem({ requestId }),
        threadId: turn.input.threadId,
        runId: turn.input.runId,
        nodeId,
        providerThreadId: state.providerThread.id,
        providerTurnId: turn.providerTurn.id,
        nativeItemRef: ref(native.id),
        parentItemId: null,
        ordinal: ordinalOf(turn, native.id),
        status: "waiting" as const,
        startedAt: now,
        completedAt: null,
        updatedAt: now,
      };
      const item: OrchestrationV2TurnItem =
        body.type === "approval_request"
          ? {
              ...base,
              title: native.type === "permission" ? native.action : null,
              type: "approval_request",
              requestId,
              requestKind: body.requestKind,
              prompt: body.prompt,
              ...(body.options === undefined ? {} : { options: body.options }),
            }
          : {
              ...base,
              title: "User input",
              type: "user_input_request",
              requestId,
              questions: body.questions,
            };
      pending.set(requestId, {
        request,
        item,
        node,
        state,
        turn,
        sessionId,
        answering: false,
        native,
      });
      yield* emit({ type: "node.updated", driver, node });
      yield* emit({
        type: "runtime_request.updated",
        driver,
        threadId: turn.input.threadId,
        runtimeRequest: request,
      });
      yield* emit({ type: "turn_item.updated", driver, turnItem: item });
      yield* setSessionStatus("waiting", null);
    });

    const onPermissionAsked = Effect.fnUntraced(function* (event: EventOf<"permission.asked">) {
      const { data } = event;
      const turn = ownerOf(data.sessionID)?.active;
      const toolName =
        data.source === undefined ? undefined : turn?.tools.get(data.source.id)?.name;
      const save = data.save ?? [];
      yield* showRequest(
        data.sessionID,
        { type: "permission", id: data.id, action: data.action, resources: data.resources, save },
        {
          type: "approval_request",
          requestKind: openCodePermissionRequestKind(data.action, toolName),
          prompt: data.resources.length === 0 ? data.action : data.resources.join("\n"),
          // "Always" in OpenCode saves a grant for the whole project, so the
          // session-wide choice is T3's own rule on this session instead.
          options: [
            { decision: "cancel", label: "Cancel" },
            { decision: "decline", label: "Decline" },
            ...(save.length > 0
              ? [
                  {
                    decision: "acceptForSession" as const,
                    label: sessionGrantLabel(data.action, save),
                  },
                ]
              : []),
            { decision: "accept", label: "Approve" },
          ],
        },
      );
    });

    const onFormCreated = Effect.fnUntraced(function* (event: EventOf<"form.created">) {
      const { form } = event.data;
      const state = ownerOf(form.sessionID);
      if (state?.active === undefined) return;
      const mapped = formQuestions(form);
      if ("questions" in mapped) {
        return yield* showRequest(
          form.sessionID,
          { type: "form", id: form.id, form },
          { type: "user_input_request", questions: mapped.questions },
        );
      }
      // Cancelling ends OpenCode's execution as a user stop, so the turn is
      // failed here with the reason and that stop's end is skipped.
      yield* Effect.logWarning("Declined an OpenCode form T3 Code cannot show.", {
        reason: mapped.unsupported,
      });
      const cancelled = yield* deliver(
        client.session.form.cancel({ sessionID: form.sessionID, formID: form.id }),
      );
      if (!cancelled) return yield* abandonRequest(state, "form cancel failed");
      state.unsettled = true;
      yield* finishTurn(state, {
        status: "failed",
        failure: makeProviderFailure({
          message: `OpenCode asked for ${mapped.unsupported}, which T3 Code can't show. The question was declined.`,
          class: "provider_error",
        }),
      });
    });

    const handleEvent = Effect.fnUntraced(function* (event: OpenCode2StreamEvent) {
      // The end of the run a timed-out Stop left behind; no turn is its own.
      const endedSession =
        event.type === "unreadable.execution.ended"
          ? event.sessionID
          : event.type === "session.execution.succeeded" ||
              event.type === "session.execution.failed" ||
              event.type === "session.execution.interrupted"
            ? event.data.sessionID
            : undefined;
      const ended = endedSession === undefined ? undefined : threads.get(endedSession);
      if (ended?.unsettled === true) {
        ended.unsettled = false;
        return;
      }
      // Only marks where a turn's own execution begins; it never ends one.
      if (event.type === "unreadable.execution.started") {
        const turn = threads.get(event.sessionID)?.active;
        if (turn !== undefined) turn.awaitingStart = false;
        return;
      }
      if (event.type === "unreadable.execution.ended") {
        const state = threads.get(event.sessionID);
        if (state === undefined || state.active?.awaitingStart === true) return;
        return yield* finishTurn(
          state,
          event.executionType === "session.execution.succeeded"
            ? { status: state.active?.interrupted === true ? "interrupted" : "completed" }
            : event.executionType === "session.execution.interrupted"
              ? { status: "interrupted" }
              : {
                  status: "failed",
                  failure: makeProviderFailure({
                    message: "OpenCode ended the turn with an error this version cannot read.",
                    class: "provider_error",
                  }),
                },
        );
      }
      // A subagent's requests are asked on the thread whose session started it.
      if (event.type === "session.created" && event.data.parentID !== undefined) {
        const owner = ownerOf(event.data.parentID);
        if (owner !== undefined) childOwners.set(event.data.sessionID, owner);
        return;
      }
      if (event.type === "permission.asked" || event.type === "form.created") {
        const asking =
          event.type === "permission.asked" ? event.data.sessionID : event.data.form.sessionID;
        const state = ownerOf(asking);
        if (state === undefined) return;
        const turn = state.active;
        if (turn !== undefined && !turn.awaitingStart) {
          if (event.type === "permission.asked") return yield* onPermissionAsked(event);
          return yield* onFormCreated(event);
        }
        // Asked by the run a Stop left behind, which nothing answers.
        if (state.unsettled || turn?.awaitingStart === true) {
          yield* stopStaleRequest(
            asking,
            event.type === "permission.asked"
              ? { type: "permission", id: event.data.id }
              : { type: "form", id: event.data.form.id },
          );
        }
        return;
      }
      // Answered in another OpenCode client, or dropped by OpenCode: a reject
      // it sends on its own (a Stop, or another reject in the same session)
      // cancels the request. T3's own answers are settled where they are sent.
      if (
        event.type === "permission.replied" ||
        event.type === "form.replied" ||
        event.type === "form.cancelled"
      ) {
        const nativeId = event.type === "permission.replied" ? event.data.requestID : event.data.id;
        const entry = [...pending.values()].find((candidate) => candidate.native.id === nativeId);
        if (entry === undefined || entry.answering) return;
        const answered =
          event.type === "form.replied" ||
          (event.type === "permission.replied" && event.data.reply !== "reject");
        return yield* settleRequest(entry, answered ? "resolved" : "cancelled");
      }
      if (!("sessionID" in event.data) || typeof event.data.sessionID !== "string") return;
      const state = threads.get(event.data.sessionID);
      const turn = state?.active;
      if (state === undefined || turn === undefined) return;
      // A session runs one execution at a time, and each opens with `started`
      // on this ordered stream, so what comes before it is the stopped run's.
      if (turn.awaitingStart) {
        if (event.type === "session.execution.started") turn.awaitingStart = false;
        return;
      }
      switch (event.type) {
        case "session.text.started":
        case "session.reasoning.started":
        case "session.text.delta":
        case "session.reasoning.delta":
        case "session.text.ended":
        case "session.reasoning.ended": {
          const data = event.data;
          const kind = event.type.startsWith("session.text.") ? "text" : "reasoning";
          return yield* emitText(state, turn, data, kind, (text) =>
            "delta" in data ? text + data.delta : "text" in data ? data.text : text,
          );
        }
        case "session.tool.input.started":
          // Its form is the item the user answers; the tool call would repeat it.
          if (event.data.name === "question") return;
          turn.tools.set(event.data.id, { name: event.data.name, input: {} });
          turn.startedAt.set(event.data.id, yield* DateTime.now);
          return yield* emitTool(state, turn, event.data.id, "running");
        case "session.tool.called": {
          const tool = turn.tools.get(event.data.id);
          if (tool !== undefined) tool.input = event.data.input;
          return yield* emitTool(state, turn, event.data.id, "running");
        }
        case "session.tool.success": {
          const output = textOf(event.data.content);
          yield* emitTool(state, turn, event.data.id, "completed", {
            output,
            metadata: event.data.metadata,
          });
          turn.tools.delete(event.data.id);
          return;
        }
        case "session.tool.failed": {
          const aborted = event.data.error.type === "aborted";
          yield* emitTool(state, turn, event.data.id, aborted ? "interrupted" : "failed", {
            output: event.data.error.message,
            metadata: event.data.metadata,
          });
          turn.tools.delete(event.data.id);
          return;
        }
        case "session.step.ended":
        case "session.step.failed": {
          const tokens = event.data.tokens;
          if (tokens === undefined) return;
          turn.steps += 1;
          turn.lastStep = tokens;
          turn.usage.input += tokens.input + tokens.cache.read + tokens.cache.write;
          turn.usage.cached += tokens.cache.read;
          turn.usage.cacheWrite += tokens.cache.write;
          turn.usage.output += tokens.output + tokens.reasoning;
          turn.usage.reasoning += tokens.reasoning;
          return;
        }
        case "session.execution.succeeded":
          return yield* finishTurn(state, {
            status: turn.interrupted ? "interrupted" : "completed",
          });
        case "session.execution.interrupted":
          return yield* finishTurn(state, { status: "interrupted" });
        case "session.execution.failed":
          return yield* finishTurn(state, {
            status: "failed",
            failure: makeProviderFailure({
              message: event.data.error.message,
              code: event.data.error.type,
              class: "provider_error",
            }),
          });
        default:
          return;
      }
    });

    // The stream is the only terminal signal, so a lost stream settles every
    // running turn and breaks the session: T3 reopens it for the next turn.
    // Set before the turns are settled, so a turn starting meanwhile sees it.
    let streamFailure: string | undefined;
    const failAll = Effect.fnUntraced(function* (message: string) {
      streamFailure = message;
      for (const state of threads.values()) {
        const failure = makeProviderFailure({ message, class: "transport_error" });
        yield* finishTurn(state, { status: "failed", failure }, "broken");
      }
      yield* setSessionStatus("error", message);
      yield* Queue.end(events);
    });
    // Subscribed before any session or prompt call, so no event of theirs is missed.
    const stream = yield* connection.events;
    yield* stream.pipe(
      Stream.runForEach(handleEvent),
      Effect.matchCauseEffect({
        onSuccess: () => failAll("The OpenCode event stream ended."),
        onFailure: () => failAll("The OpenCode event stream failed."),
      }),
      Effect.forkScoped,
    );

    // A server T3 did not start keeps running after T3 stops, so stop the turns
    // it would otherwise finish unseen. A spawned server stops with its owner.
    if (connection.external) {
      yield* Effect.addFinalizer(() =>
        Effect.forEach(
          [...threads].filter(([, state]) => state.active !== undefined),
          ([sessionId]) =>
            client.session
              .interrupt({ sessionID: Session.ID.make(sessionId) })
              .pipe(Effect.timeout("1 second"), Effect.ignore({ log: true })),
          { concurrency: 8, discard: true },
        ),
      );
    }

    // Context windows come from the server's model list, read when the session
    // opens and again after a turn whose model had none yet.
    const readModels = client.model
      .list({ location: { directory: session.cwd ?? serverConfig.cwd } })
      .pipe(
        Effect.timeout("5 seconds"),
        Effect.tap((models) =>
          Effect.sync(() => {
            for (const model of models.data) {
              // A model's input limit, when it has one, is its real headroom.
              contextWindows.set(
                `${model.providerID}/${model.id}`,
                model.limit.input ?? model.limit.context,
              );
            }
          }),
        ),
        Effect.ignore({ log: true }),
      );
    yield* readModels;

    // Each agent's own path allows, by the directory they were listed for.
    const agentRules = new Map<string, ReadonlyMap<string, ReadonlyArray<Rule>>>();
    const pathsFor = Effect.fnUntraced(function* (
      directory: string,
      agents: ReadonlyArray<string>,
    ) {
      let known = agentRules.get(directory);
      if (known === undefined) {
        // A fresh server lists no agents for its first moments, like its models.
        const listed = yield* client.agent.list({ location: { directory } }).pipe(
          Effect.repeat({
            until: (list) => list.data.length > 0,
            schedule: Schedule.spaced("250 millis"),
          }),
          Effect.timeout("5 seconds"),
          // The failure can carry the server's URL or response text, so only its
          // tag is annotated; the full failure stays in the log's cause.
          Effect.tapCause((cause) =>
            Effect.logWarning(
              "Could not list OpenCode agents; their path rules are skipped.",
              cause,
            ).pipe(Effect.annotateLogs({ errorTag: causeErrorTag(cause) })),
          ),
          Effect.option,
        );
        if (listed._tag === "None") return [];
        known = new Map(
          listed.value.data.map((agent) => [agent.id, agentPaths(agent.permissions)]),
        );
        agentRules.set(directory, known);
      }
      const paths = new Map<string, Rule>();
      for (const agent of agents) {
        for (const entry of known.get(agent) ?? []) {
          paths.set(`${entry.action}\u0000${entry.resource}`, entry);
        }
      }
      return [...paths.values()];
    });

    /**
     * The rules a thread's session runs `policy` with. Full access allows
     * every path already, so the agent's own path rules are read only for the
     * modes that narrow it.
     */
    const rulesFor = Effect.fnUntraced(function* (
      thread: Pick<ThreadState, "directory" | "agent" | "grants">,
      policy: RulesPolicy,
    ) {
      const plan = policy.interactionMode === "plan";
      const paths =
        policy.runtimeMode === "full-access" && !plan
          ? []
          : yield* pathsFor(thread.directory, plan ? [thread.agent, "plan"] : [thread.agent]);
      return sessionRules(policy, paths, policy.runtimeMode === "full-access" ? [] : thread.grants);
    });

    /** Writes the session's rules for `policy` when they differ from what it has. */
    const writeRules = Effect.fnUntraced(function* (state: ThreadState, policy: RulesPolicy) {
      const rules = yield* rulesFor(state, policy);
      if (!sameRules(state.rules, rules)) {
        yield* client.session.update({
          sessionID: Session.ID.make(state.sessionId),
          permissions: rules,
        });
        state.rules = rules;
      }
      state.policy = policy;
    });

    const register = (
      providerThread: OrchestrationV2ProviderThread,
      native: {
        readonly id: string;
        readonly model?: ModelRef | undefined;
        readonly agent?: string | undefined;
        readonly permissions?: ReadonlyArray<Rule> | undefined;
      },
      directory: string,
    ) => {
      const existing = threads.get(native.id);
      if (existing !== undefined) {
        existing.providerThread = providerThread;
        existing.model = native.model;
        existing.directory = directory;
        existing.agent = native.agent ?? existing.agent;
        existing.rules = native.permissions;
        return existing;
      }
      const state: ThreadState = {
        sessionId: native.id,
        providerThread,
        providerTurns: new Map(),
        active: undefined,
        model: native.model,
        unsettled: false,
        directory,
        agent: native.agent ?? "build",
        rules: native.permissions,
        policy: input.runtimePolicy,
        grants: [],
      };
      threads.set(native.id, state);
      return state;
    };

    const prompt = (turnInput: ProviderAdapter.ProviderAdapterV2TurnInput) => {
      const text = providerMessageTextWithAttachmentPaths({
        text: turnInput.message.text,
        attachments: turnInput.message.attachments,
        attachmentsDir: serverConfig.attachmentsDir,
      }).trim();
      const instructions = buildRuntimeInstructions({
        harness: "OpenCode",
        model: turnInput.modelSelection.model,
      });
      return `${text}\n\n${instructions}`;
    };

    const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
      instanceId,
      driver,
      providerSessionId: input.providerSessionId,
      get providerSession() {
        return session;
      },
      events: Stream.fromQueue(events),
      getModelContextWindow: (selection) =>
        selection.instanceId === instanceId ? contextWindows.get(selection.model) : undefined,
      ensureThread: (threadInput) =>
        Effect.gen(function* () {
          if (threadInput.existingProviderThread?.nativeThreadRef != null) {
            return yield* runtime.resumeThread({
              providerThread: threadInput.existingProviderThread,
              threadId: threadInput.threadId,
              modelSelection: threadInput.modelSelection,
              runtimePolicy: threadInput.runtimePolicy,
            });
          }
          const model = modelRef(threadInput.modelSelection);
          if (model === undefined) {
            return yield* new ProviderAdapter.ProviderAdapterProtocolError({
              driver: OPENCODE_PROVIDER,
              detail: malformedModel(threadInput.modelSelection.model),
            });
          }
          const directory = threadInput.runtimePolicy.cwd ?? serverConfig.cwd;
          const policy = threadInput.runtimePolicy;
          // A new session runs OpenCode's default agent.
          const permissions = yield* rulesFor({ directory, agent: "build", grants: [] }, policy);
          const created = yield* client.session.create({
            location: Location.PublicRef.make({ directory: AbsolutePath.make(directory) }),
            model,
            permissions,
          });
          const createdAt = yield* DateTime.now;
          const providerThread: OrchestrationV2ProviderThread = {
            ...(threadInput.existingProviderThread ?? {
              id: idAllocator.derive.providerThread({ driver, nativeThreadId: created.id }),
              driver,
              providerInstanceId: instanceId,
              appThreadId: threadInput.threadId,
              ownerNodeId: null,
              firstRunOrdinal: null,
              lastRunOrdinal: null,
              handoffIds: [],
              forkedFrom: null,
              createdAt,
            }),
            providerSessionId: input.providerSessionId,
            nativeThreadRef: ref(created.id),
            nativeConversationHeadRef: null,
            status: "idle",
            updatedAt: createdAt,
          };
          const state = register(
            providerThread,
            { id: created.id, model: created.model, agent: created.agent, permissions },
            directory,
          );
          state.policy = policy;
          return providerThread;
        }).pipe(
          Effect.mapError((cause) =>
            isProviderAdapterError(cause)
              ? cause
              : new ProviderAdapter.ProviderAdapterEnsureThreadError({
                  driver,
                  threadId: threadInput.threadId,
                  cause,
                }),
          ),
        ),
      resumeThread: (threadInput) =>
        Effect.gen(function* () {
          const sessionId = yield* sessionIdOf(threadInput.providerThread);
          // 1.x session ids survive the upgrade; a server without this session
          // fails the resume, so T3 recreates the thread with a handoff.
          const native = yield* client.session.get({ sessionID: Session.ID.make(sessionId) });
          const providerThread: OrchestrationV2ProviderThread = {
            ...threadInput.providerThread,
            providerSessionId: input.providerSessionId,
            status: "idle",
            updatedAt: yield* DateTime.now,
          };
          const cwd = threadInput.runtimePolicy?.cwd;
          const loaded = threads.has(sessionId);
          const state = register(providerThread, native, cwd ?? native.location.directory);
          // OpenCode keeps no request across its own restart, but a server that
          // outlived T3 may still wait on one T3 no longer shows.
          if (!loaded) yield* stopLeftoverRequests(state);
          // The session gets the rules for this thread's mode: it may have run
          // another mode, or been made by 1.x or an earlier build.
          yield* writeRules(state, threadInput.runtimePolicy ?? state.policy);
          // A thread moved to another worktree takes its session with it.
          if (cwd != null && native.location.directory !== cwd) {
            yield* client.session.move({
              sessionID: Session.ID.make(sessionId),
              directory: AbsolutePath.make(cwd),
            });
          }
          return providerThread;
        }).pipe(
          Effect.mapError((cause) =>
            isProviderAdapterError(cause)
              ? cause
              : new ProviderAdapter.ProviderAdapterResumeThreadError({
                  driver,
                  providerSessionId: input.providerSessionId,
                  providerThreadId: threadInput.providerThread.id,
                  cause,
                }),
          ),
        ),
      startTurn: (turnInput) =>
        Effect.gen(function* () {
          const sessionId = yield* sessionIdOf(turnInput.providerThread);
          const state = threads.get(sessionId);
          if (state === undefined) {
            return yield* new ProviderAdapter.ProviderAdapterProtocolError({
              driver: OPENCODE_PROVIDER,
              detail: `OpenCode session ${sessionId} is not registered`,
            });
          }
          if (state.active !== undefined) {
            return yield* new ProviderAdapter.ProviderAdapterProtocolError({
              driver: OPENCODE_PROVIDER,
              detail: `OpenCode session ${sessionId} already has an active turn`,
            });
          }
          // After a timed-out Stop the server says whether that run is gone. A
          // run still going is stopped again and this turn fails so it can be
          // sent again; a run that is gone may still have its end on the
          // stream, which the turn skips.
          const afterUnsettled = state.unsettled;
          let stillStopping = false;
          if (state.unsettled) {
            const active = yield* client.session
              .active()
              .pipe(Effect.timeout(ACTIVE_CHECK_TIMEOUT));
            stillStopping = sessionId in active;
            if (stillStopping) {
              yield* client.session
                .interrupt({ sessionID: Session.ID.make(sessionId) })
                .pipe(Effect.timeout(INTERRUPT_TIMEOUT), Effect.ignore({ log: true }));
            } else {
              state.unsettled = false;
            }
          }
          // Installs the turn; every path after it ends the turn with a terminal.
          const begin = Effect.gen(function* () {
            const startedAt = yield* DateTime.now;
            const nativeTurnId = `${sessionId}:attempt:${turnInput.attemptId}`;
            const providerTurn: OrchestrationV2ProviderTurn = {
              id: idAllocator.derive.providerTurn({ driver, nativeTurnId }),
              providerThreadId: turnInput.providerThread.id,
              nodeId: turnInput.rootNodeId,
              runAttemptId: turnInput.attemptId,
              nativeTurnRef: ref(nativeTurnId, "weak"),
              ordinal: turnInput.providerTurnOrdinal,
              status: "running",
              startedAt,
              completedAt: null,
            };
            const turn: ActiveTurn = {
              input: turnInput,
              providerTurn,
              texts: new Map(),
              tools: new Map(),
              startedAt: new Map(),
              ordinals: new Map(),
              nextOrdinal: turnInput.providerTurnOrdinal * 100 + 1,
              usage: { input: 0, cached: 0, cacheWrite: 0, output: 0, reasoning: 0 },
              steps: 0,
              lastStep: undefined,
              interrupted: false,
              awaitingStart: afterUnsettled,
            };
            // No stream is left to end this turn, so it must not start.
            if (streamFailure !== undefined) {
              return yield* new ProviderAdapter.ProviderAdapterEventStreamError({
                driver,
                providerSessionId: input.providerSessionId,
                cause: streamFailure,
              });
            }
            state.active = turn;
            yield* emitProviderTurn(state, turn, providerTurn);
            state.providerThread = {
              ...state.providerThread,
              status: "active",
              firstRunOrdinal: state.providerThread.firstRunOrdinal ?? turnInput.runOrdinal,
              lastRunOrdinal: turnInput.runOrdinal,
              updatedAt: startedAt,
            };
            yield* emit({
              type: "provider_thread.updated",
              driver,
              providerThread: state.providerThread,
            });
            yield* setSessionStatus("running", null);
            return turn;
          });
          if (stillStopping) {
            yield* begin;
            return yield* finishTurn(state, {
              status: "failed",
              failure: makeProviderFailure({
                message: OPENCODE_2_STILL_STOPPING,
                class: "provider_error",
              }),
            });
          }
          // A turn T3 will not run still starts and fails, so the refusal is what
          // the user reads.
          const model = modelRef(turnInput.modelSelection);
          if (model === undefined) {
            yield* begin;
            return yield* finishTurn(state, {
              status: "failed",
              failure: makeProviderFailure({
                message: malformedModel(turnInput.modelSelection.model),
                class: "validation_error",
              }),
            });
          }
          // The thread's mode may have changed since the session was loaded.
          yield* writeRules(state, turnInput.runtimePolicy);
          // A selection changed since the last turn applies now; OpenCode keeps
          // the session's model otherwise.
          if (!sameModel(model, state.model)) {
            yield* client.session.switchModel({ sessionID: Session.ID.make(sessionId), model });
            state.model = model;
          }
          const turn = yield* begin;
          yield* client.session
            .prompt({ sessionID: Session.ID.make(sessionId), text: prompt(turnInput) })
            .pipe(
              // Deleted outside T3: the thread is broken, and forgetting it makes
              // the next turn resume, fail, and recreate it with a handoff.
              Effect.catchTags({
                SessionNotFoundError: () =>
                  finishTurn(
                    state,
                    {
                      status: "failed",
                      failure: makeProviderFailure({
                        message:
                          "The OpenCode session no longer exists. Send the message again to continue in a new session.",
                        class: "provider_error",
                      }),
                    },
                    "broken",
                  ).pipe(Effect.andThen(Effect.sync(() => threads.delete(sessionId)))),
              }),
              Effect.tapError((cause) =>
                state.active === turn
                  ? Effect.gen(function* () {
                      // Without a clear rejection the server may have taken the
                      // prompt, so the next turn checks before it prompts again.
                      if (!CLEAR_PROMPT_REJECTIONS.has(cause._tag)) state.unsettled = true;
                      yield* finishTurn(state, {
                        status: "failed",
                        failure: makeProviderFailure({ cause, class: "provider_error" }),
                      });
                    })
                  : Effect.void,
              ),
            );
        }).pipe(
          Effect.mapError((cause) =>
            isProviderAdapterError(cause)
              ? cause
              : new ProviderAdapter.ProviderAdapterTurnStartError({
                  driver,
                  threadId: turnInput.threadId,
                  providerThreadId: turnInput.providerThread.id,
                  runId: turnInput.runId,
                  cause,
                }),
          ),
        ),
      steerTurn: (steerInput) =>
        Effect.fail(
          new ProviderAdapter.ProviderAdapterSteerRunUnsupportedError({
            driver,
            providerThreadId: steerInput.providerThread.id,
          }),
        ),
      interruptTurn: (interruptInput) =>
        Effect.gen(function* () {
          const sessionId = yield* sessionIdOf(interruptInput.providerThread);
          const state = threads.get(sessionId);
          const turn = state?.active;
          if (
            state === undefined ||
            turn === undefined ||
            turn.providerTurn.id !== interruptInput.providerTurnId
          ) {
            return;
          }
          // The session answers with `session.execution.interrupted`, which ends
          // the turn. A server that does not answer in time is stuck, so the turn
          // ends here instead of waiting on it.
          turn.interrupted = true;
          const reply = yield* client.session
            .interrupt({ sessionID: Session.ID.make(sessionId) })
            .pipe(
              Effect.timeoutOption(INTERRUPT_TIMEOUT),
              // A Stop that never reached the server stopped nothing.
              Effect.tapError(() =>
                Effect.sync(() => {
                  turn.interrupted = false;
                }),
              ),
            );
          if (reply._tag === "None") {
            state.unsettled = true;
            return yield* finishTurn(state, { status: "interrupted" });
          }
          // Nothing was running. Unless the execution already ended (its event
          // is on the way), the turn is still open and nothing stopped.
          if (!reply.value.interrupted && state.active === turn) {
            turn.interrupted = false;
            return yield* new ProviderAdapter.ProviderAdapterProtocolError({
              driver: OPENCODE_PROVIDER,
              detail: `OpenCode session ${sessionId} had nothing running to stop`,
            });
          }
        }).pipe(
          Effect.mapError((cause) =>
            isProviderAdapterError(cause)
              ? cause
              : new ProviderAdapter.ProviderAdapterInterruptError({
                  driver,
                  providerThreadId: interruptInput.providerThread.id,
                  providerTurnId: interruptInput.providerTurnId,
                  cause,
                }),
          ),
        ),
      unloadThread: ({ providerThread }) =>
        Effect.sync(() => {
          const nativeId = providerThread.nativeThreadRef?.nativeId;
          const state = nativeId == null ? undefined : threads.get(nativeId);
          if (nativeId == null || state === undefined || state.active !== undefined) return;
          threads.delete(nativeId);
          for (const [child, owner] of childOwners) {
            if (owner === state) childOwners.delete(child);
          }
        }),
      respondToRuntimeRequest: (requestInput) =>
        Effect.gen(function* () {
          const entry = pending.get(requestInput.requestId);
          if (entry === undefined || entry.answering) {
            return yield* new ProviderAdapter.ProviderAdapterProtocolError({
              driver,
              detail: `No pending OpenCode request ${requestInput.requestId}`,
            });
          }
          const { decision, answers } = requestInput;
          const { native } = entry;
          if (native.type === "permission" && decision === undefined) {
            return yield* new ProviderAdapter.ProviderAdapterProtocolError({
              driver,
              detail: `OpenCode approval request ${requestInput.requestId} requires a decision`,
            });
          }
          entry.answering = true;
          // OpenCode's own "always" saves a grant for the whole project, so a
          // session-wide answer is a rule on this session instead. The grant
          // is best effort: this request is answered either way.
          if (
            native.type === "permission" &&
            (decision === "acceptForSession" || decision === "acceptAlways")
          ) {
            const { state } = entry;
            for (const resource of native.save) {
              if (
                !state.grants.some(
                  (grant) => grant.action === native.action && grant.resource === resource,
                )
              ) {
                state.grants.push({ action: native.action, resource, effect: "allow" });
              }
            }
            yield* writeRules(state, state.policy).pipe(Effect.ignore({ log: true }));
          }
          const sessionID = Session.ID.make(entry.sessionId);
          // Best effort: the decline stands without the note.
          if (native.type === "permission" && decision === "decline") {
            yield* client.session
              .synthetic({
                sessionID,
                text: declinedNote(native.action, native.resources),
                delivery: "steer",
                resume: false,
              })
              .pipe(Effect.timeout(REQUEST_REPLY_TIMEOUT), Effect.ignore({ log: true }));
          }
          const delivered = yield* native.type === "permission"
            ? deliver(
                client.permission
                  .reply({
                    sessionID,
                    requestID: Permission.ID.make(native.id),
                    decision: decision !== undefined && approves(decision) ? "once" : "reject",
                    // Without a message OpenCode ends the whole run, which is Cancel.
                    ...(decision === "decline" ? { message: DECLINED } : {}),
                  })
                  .pipe(Effect.catchTags(permissionGone)),
              )
            : deliver(
                (answers === undefined || decision === "decline" || decision === "cancel"
                  ? client.session.form.cancel({
                      sessionID: entry.sessionId,
                      formID: Form.ID.make(native.id),
                    })
                  : client.session.form.reply({
                      sessionID: entry.sessionId,
                      formID: Form.ID.make(native.id),
                      answer: formAnswer(native.form, answers),
                    })
                ).pipe(Effect.catchTags(formGone)),
              );
          yield* forgetRequest(entry);
          if (!delivered) yield* abandonRequest(entry.state, "answer not delivered");
        }).pipe(
          Effect.mapError((cause) =>
            isProviderAdapterError(cause)
              ? cause
              : new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
                  driver,
                  requestId: requestInput.requestId,
                  cause,
                }),
          ),
        ),
      readThreadSnapshot: ({ providerThread }) =>
        Effect.gen(function* () {
          const sessionId = yield* sessionIdOf(providerThread);
          const history = yield* paginate(
            { sessionID: Session.ID.make(sessionId), order: "asc" as const, limit: 100 },
            client.message.list,
          ).pipe(Stream.runCollect);
          const snapshotAt = yield* DateTime.now;
          const messages = history.flatMap((message): Array<OrchestrationV2ConversationMessage> => {
            const text =
              message.type === "user"
                ? message.text
                : message.type === "assistant"
                  ? textOf(message.content)
                  : "";
            if (text.length === 0 || (message.type !== "user" && message.type !== "assistant")) {
              return [];
            }
            const createdAt = message.time.created;
            return [
              {
                createdBy: message.type === "user" ? "user" : "agent",
                creationSource: "provider",
                id: idAllocator.derive.messageFromProviderItem({
                  driver,
                  nativeItemId: message.id,
                }),
                threadId: providerThread.appThreadId ?? input.threadId,
                runId: null,
                nodeId: null,
                role: message.type,
                text,
                attachments: [],
                streaming: false,
                createdAt,
                updatedAt: createdAt,
              },
            ];
          });
          const lastUser = history.findLast((message) => message.type === "user")?.id;
          const state = threads.get(sessionId);
          return {
            providerThread: {
              ...providerThread,
              providerSessionId: input.providerSessionId,
              nativeConversationHeadRef: lastUser === undefined ? null : ref(lastUser, "weak"),
              status: "idle" as const,
              updatedAt: snapshotAt,
            },
            providerTurns: state === undefined ? [] : [...state.providerTurns.values()],
            messages,
            runtimeRequests: [],
          };
        }).pipe(
          Effect.mapError((cause) =>
            isProviderAdapterError(cause)
              ? cause
              : new ProviderAdapter.ProviderAdapterReadThreadSnapshotError({
                  driver,
                  providerThreadId: providerThread.id,
                  cause,
                }),
          ),
        ),
      rollbackThread: (rollbackInput) =>
        Effect.fail(
          new ProviderAdapter.ProviderAdapterRollbackThreadError({
            driver,
            providerThreadId: rollbackInput.providerThread.id,
            checkpointId: rollbackInput.target.checkpointId,
            cause: notYet("rollback"),
          }),
        ),
      forkThread: (forkInput) =>
        Effect.fail(
          new ProviderAdapter.ProviderAdapterForkThreadError({
            driver,
            providerThreadId: forkInput.sourceProviderThread.id,
            cause: notYet("fork"),
          }),
        ),
    };
    return runtime;
  });

  return ProviderAdapter.ProviderAdapterV2.of({
    instanceId,
    driver,
    getCapabilities: () => Effect.succeed(OpenCode2ProviderCapabilities),
    planSelectionTransition: () => Effect.succeed(turnScopedSelectionTransition()),
    // The session borrows the instance's server for as long as it is open, so a
    // spawned server is not idle-stopped under a long tool call.
    openSession: (input) =>
      Effect.gen(function* () {
        const lent = yield* Deferred.make<
          OpenCode2Server.OpenCode2Connection,
          OpenCodeRuntimeError
        >();
        yield* server
          .withConnection((connection) =>
            Deferred.succeed(lent, connection).pipe(Effect.andThen(Effect.never)),
          )
          .pipe(
            Effect.catch((error) => Deferred.fail(lent, error)),
            Effect.forkScoped,
          );
        return yield* openSession(input, yield* Deferred.await(lent));
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapter.ProviderAdapterOpenSessionError({
              driver,
              providerSessionId: input.providerSessionId,
              cause,
            }),
        ),
      ),
  });
});
