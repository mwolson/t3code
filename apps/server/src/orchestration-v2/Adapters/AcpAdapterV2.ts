import {
  type ChatAttachment,
  type ModelSelection,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2PlanArtifact,
  type OrchestrationV2PlanStep,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderFailure,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2Subagent,
  type OrchestrationV2TurnItem,
  type OrchestrationV2UserInputQuestion,
  type ProviderApprovalDecision,
  type ProviderInstanceId,
  type ProviderDriverKind,
  type ProviderRequestKind,
  type ProviderThreadId,
  type ProviderUserInputAnswers,
  type RuntimeRequestId,
  type ThreadId,
} from "@t3tools/contracts";
import { modelSelectionsEqual } from "@t3tools/shared/model";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/schema";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import {
  mergeToolCallState,
  parsePermissionRequest,
  parseSessionUpdateEvent,
  type AcpPlanUpdate,
  type AcpToolCallState,
} from "../../provider/acp/AcpRuntimeModel.ts";
import type {
  AcpSessionRuntimeOptions,
  AcpSessionRuntimeStartResult,
} from "../../provider/acp/AcpSessionRuntime.ts";
import * as AcpSessionRuntime from "../../provider/acp/AcpSessionRuntime.ts";
import { IdAllocatorV2, type IdAllocatorV2Shape } from "../IdAllocator.ts";
import { type ProviderContinuationRequest } from "../ProviderContinuationRequests.ts";
import { makeProviderFailure } from "../ProviderFailure.ts";
import { acpSelectionTransition } from "../ProviderSelectionTransition.ts";
import {
  makeSubagentChildThread,
  makeSubagentConversationArtifacts,
  subagentThreadTitle,
} from "../SubagentProjection.ts";
import {
  ProviderAdapterEnsureThreadError,
  ProviderAdapterForkThreadError,
  ProviderAdapterInterruptError,
  ProviderAdapterOpenSessionError,
  ProviderAdapterProtocolError,
  ProviderAdapterReadThreadSnapshotError,
  ProviderAdapterResumeThreadError,
  ProviderAdapterRollbackThreadError,
  ProviderAdapterRuntimeRequestResponseError,
  ProviderAdapterSteerRunUnsupportedError,
  ProviderAdapterTurnStartError,
  ProviderAdapterV2,
  type ProviderAdapterV2EnsureThreadInput,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2InterruptInput,
  type ProviderAdapterV2OpenSessionInput,
  type ProviderAdapterV2RuntimePolicy,
  type ProviderAdapterV2SessionRuntime,
  type ProviderAdapterV2Shape,
  type ProviderAdapterV2TurnInput,
} from "../ProviderAdapter.ts";

export const ACP_PROTOCOL = "acp.ndjson-jsonrpc" as const;

export interface AcpAdapterV2RuntimeInput {
  readonly cwd: string;
  readonly mcpServers: ReadonlyArray<EffectAcpSchema.McpServer>;
  readonly interruptPromptOnCancel?: boolean;
  readonly clientCapabilities: EffectAcpSchema.InitializeRequest["clientCapabilities"];
  readonly clientInfo: AcpSessionRuntimeOptions["clientInfo"];
  readonly requestLogger?: NonNullable<AcpSessionRuntimeOptions["requestLogger"]>;
  readonly protocolLogging: NonNullable<AcpSessionRuntimeOptions["protocolLogging"]>;
}

export type AcpAdapterV2NativeLogging = Pick<
  AcpSessionRuntimeOptions,
  "requestLogger" | "protocolLogging"
>;

export interface AcpAdapterV2UserInputRequest {
  readonly nativeItemId: string;
  readonly nativeRequestId: string;
  readonly questions: ReadonlyArray<OrchestrationV2UserInputQuestion>;
}

export interface AcpAdapterV2ExtensionContext {
  readonly runtime: AcpSessionRuntime.AcpSessionRuntime["Service"];
  readonly requestUserInput: (
    input: AcpAdapterV2UserInputRequest,
  ) => Effect.Effect<ProviderUserInputAnswers | null, EffectAcpErrors.AcpError>;
}

export interface AcpRootTurnIdleSnapshot {
  readonly finalized: boolean;
  readonly interrupted: boolean;
  readonly assistantStreamOpen: boolean;
  readonly reasoningStreamOpen: boolean;
  readonly hasRunningTool: boolean;
  readonly hasPendingRuntimeRequest: boolean;
  readonly hasToolHistory: boolean;
  readonly hasRunningSubagent: boolean;
  readonly hasOutput: boolean;
}

/**
 * Debounce used if a flavor re-enables speculative idle settlement.
 * Kept for tests and future root-matched recovery; Grok no longer idle-settles.
 */
export const acpRootTurnSettleDebounceMs = 2_000;

/** Let trailing root session chunks land before terminalizing a settled turn. */
export const acpRootTurnCompletionDrainMs = 100;

/**
 * True when root-session streaming is quiescent enough for speculative settle.
 *
 * Always false today: settling on "assistant text then quiet" over-settles Grok
 * preamble-before-tools turns, and settling after tools drops later tool waves
 * while `session/prompt` is still open. Terminalize from the prompt RPC (or a
 * future root-matched completion signal), not from local silence.
 */
export function acpRootTurnIsIdle(snapshot: AcpRootTurnIdleSnapshot): boolean {
  if (snapshot.finalized || snapshot.interrupted) return false;
  if (snapshot.assistantStreamOpen || snapshot.reasoningStreamOpen) return false;
  if (snapshot.hasRunningTool || snapshot.hasPendingRuntimeRequest) return false;
  if (snapshot.hasRunningSubagent) return false;
  if (!snapshot.hasOutput) return false;
  // Structural gates above stay for unit tests / future re-enable. Speculative
  // idle completion is intentionally disabled.
  return false;
}

/** True when idle settle should be (re-)scheduled after pending runtime work clears. */
export function acpRootTurnShouldRearmRecoveryTimers(context: {
  readonly finalized: boolean;
  readonly interrupted: boolean;
}): boolean {
  return !context.finalized && !context.interrupted;
}

export interface AcpAdapterV2Flavor {
  readonly driver: ProviderDriverKind;
  readonly capabilities: OrchestrationV2ProviderCapabilities;
  readonly makeRuntime: (
    input: AcpAdapterV2RuntimeInput,
  ) => Effect.Effect<
    AcpSessionRuntime.AcpSessionRuntime["Service"],
    EffectAcpErrors.AcpError,
    Scope.Scope
  >;
  readonly resolveModelId?: (selection: ModelSelection) => string | undefined;
  readonly registerExtensions?: (
    context: AcpAdapterV2ExtensionContext,
  ) => Effect.Effect<void, EffectAcpErrors.AcpError>;
  readonly extractSubagentUpdate?: (
    toolCall: AcpToolCallState,
  ) => AcpAdapterV2SubagentUpdate | undefined;
  /**
   * Optional Grok-style rewrite before tool projection (e.g. keep monitor start
   * ACKs in the running state until stream end).
   */
  readonly normalizeToolCall?: (toolCall: AcpToolCallState) => AcpToolCallState;
  /**
   * Optional mapping from a long-lived background tool start ACK to a task id
   * (e.g. monitor task uuid) so later synthetic text events can update it.
   */
  readonly extractBackgroundTaskId?: (toolCall: AcpToolCallState) => string | undefined;
  /**
   * Optional parse of root-session synthetic text (monitor-event lines, monitor
   * ended reminders). Returns a task mutation when the text should update a
   * previously registered background tool rather than be treated as user input.
   */
  readonly extractBackgroundToolMutation?: (text: string) =>
    | {
        readonly taskId: string;
        readonly status: "running" | "completed" | "failed";
        readonly appendOutput: string;
      }
    | undefined;
  /**
   * Optional hydration when a later tool (e.g. get_command TaskOutput) completes
   * a previously registered background task id.
   */
  readonly extractBackgroundTaskCompletion?: (toolCall: AcpToolCallState) =>
    | {
        readonly taskId: string;
        readonly status: "running" | "completed" | "failed";
        readonly appendOutput: string;
      }
    | undefined;
  /**
   * When true, keep the active turn open after session/prompt returns while
   * background tools/subagents are still running so later monitor/wake traffic
   * can project (Grok monitors finish after the root prompt settles).
   */
  readonly deferFinalizeForBackgroundWork?: boolean;
  readonly assertComplete?: Effect.Effect<void, EffectAcpErrors.AcpError>;
  /**
   * When true, schedule speculative local settlement after root session
   * quiet. Disabled for Grok: short idle windows over-settle preamble-before-
   * tools turns and `session/cancel` from that path freezes projection while
   * the agent keeps working. Prefer `session/prompt` return (or a future
   * root-matched terminal signal).
   */
  readonly settleRootTurnWhenIdle?: boolean;
  /** Interrupt the local prompt fiber before `session/cancel` (Grok wedged prompts). */
  readonly interruptPromptOnCancel?: boolean;
  /**
   * Kill and respawn the ACP child process before the next `session/prompt` after a
   * user interrupt. Grok can keep `task_already_running` state until the process exits.
   */
  readonly restartRuntimeAfterInterrupt?: boolean;
  /**
   * When true (with continuationRequests), post-settle root session/update traffic
   * buffers and requests a provider continuation run instead of being dropped or
   * only appended to loaded history.
   */
  readonly enablePostSettleContinuation?: boolean;
  /**
   * When true, send image attachment content blocks even if the ACP agent
   * advertises `promptCapabilities.image: false`. Grok CLI currently accepts
   * and vision-processes image blocks while still reporting the capability as
   * false; without this override, screenshot turns fail before `session/prompt`.
   */
  readonly supportsImagePrompts?: boolean;
}

/** Whether image attachment blocks may be included in session/prompt. */
export function acpSupportsImagePrompts(input: {
  readonly flavorSupportsImagePrompts?: boolean | undefined;
  readonly negotiatedImage?: boolean | undefined;
}): boolean {
  return input.flavorSupportsImagePrompts === true || input.negotiatedImage === true;
}

export interface AcpAdapterV2SubagentUpdate {
  readonly nativeTaskId: string;
  readonly prompt: string;
  readonly title: string | null;
  readonly model: string | null;
  readonly status: "running" | "completed" | "failed";
  readonly childSessionId: string | null;
  readonly result: string | null;
  /**
   * When false, still project a normal tool turn item after the subagent update
   * (hydration tools like get_command_or_subagent_output). Defaults to true.
   */
  readonly suppressNormalTool?: boolean;
}

export interface AcpAdapterV2Options {
  readonly instanceId: ProviderInstanceId;
  readonly flavor: AcpAdapterV2Flavor;
  readonly fileSystem: FileSystem.FileSystem;
  readonly idAllocator: IdAllocatorV2Shape;
  readonly serverConfig: ServerConfig["Service"];
  readonly nativeLogging?: (threadId: ThreadId) => AcpAdapterV2NativeLogging;
  /**
   * Shared with ProviderContinuationService so post-settle wake traffic can start
   * a continuation run. Optional: adapters that omit it keep pre-continuation drop
   * / history-only behavior for null-activeTurn updates.
   */
  readonly continuationRequests?: {
    readonly offer: (request: ProviderContinuationRequest) => Effect.Effect<void>;
  };
}

export const AcpProviderCapabilitiesV2 = {
  sessions: {
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: false,
    supportsProviderSwitchingViaHandoff: true,
    supportsRuntimeModeSwitchInSession: false,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: true,
    canReadThreadSnapshot: false,
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
    streamsToolOutput: true,
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
    supportsApplyPatchApproval: false,
    approvalsHaveNativeRequestIds: false,
    approvalCallbacksAreLiveOnly: true,
    approvalsCanOriginateFromSubagents: false,
  },
  planning: {
    emitsPlanUpdated: true,
    emitsTodoList: true,
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
    canGenerateSummaries: true,
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
    providerCanReadConversationSnapshot: false,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "weak",
    nativeItemIds: "weak",
    nativeRequestIds: "weak",
  },
} satisfies OrchestrationV2ProviderCapabilities;

function negotiatedCapabilities(
  base: OrchestrationV2ProviderCapabilities,
  started: AcpSessionRuntimeStartResult,
): OrchestrationV2ProviderCapabilities {
  const agent = started.initializeResult.agentCapabilities ?? {};
  const session = agent.sessionCapabilities;
  const setup = started.sessionSetupResult;
  const hasModelConfig =
    setup.configOptions?.some((option) => option.category === "model") === true;
  const hasModeConfig = setup.configOptions?.some((option) => option.category === "mode") === true;
  const supportsMcp = agent.mcpCapabilities?.http === true || agent.mcpCapabilities?.sse === true;
  const canLoad = agent.loadSession === true;
  const canFork = session?.fork != null;
  return {
    ...base,
    sessions: {
      ...base.sessions,
      supportsModelSwitchInSession: setup.models != null || hasModelConfig,
      supportsRuntimeModeSwitchInSession: setup.modes != null || hasModeConfig,
    },
    threads: {
      ...base.threads,
      canReadThreadSnapshot: canLoad,
      canForkThread: canFork,
      canForkFromTurn: false,
    },
    tools: {
      ...base.tools,
      supportsMcpTools: supportsMcp,
    },
    checkpointing: {
      ...base.checkpointing,
      providerCanReadConversationSnapshot: canLoad,
    },
  };
}

function acpMcpServers(threadId: ThreadId): ReadonlyArray<EffectAcpSchema.McpServer> {
  const session = McpProviderSession.readMcpProviderSession(threadId);
  if (session === undefined) {
    return [];
  }
  return [
    {
      type: "http",
      name: "t3-code",
      url: session.endpoint,
      headers: [
        {
          name: "Authorization",
          value: session.authorizationHeader,
        },
      ],
    },
  ];
}

function nativeThreadId(driver: ProviderDriverKind, thread: OrchestrationV2ProviderThread): string {
  const id = thread.nativeThreadRef?.nativeId;
  if (id === null || id === undefined || id.trim().length === 0) {
    throw new ProviderAdapterProtocolError({
      driver,
      detail: `Provider thread ${thread.id} is missing its ACP session id`,
    });
  }
  return id;
}

function makeProviderThread(input: {
  readonly driver: ProviderDriverKind;
  readonly providerInstanceId: ProviderInstanceId;
  readonly idAllocator: IdAllocatorV2Shape;
  readonly appThreadId: OrchestrationV2ProviderThread["appThreadId"];
  readonly providerSessionId: OrchestrationV2ProviderThread["providerSessionId"];
  readonly nativeThreadId: string;
  readonly ownerNodeId?: OrchestrationV2ProviderThread["ownerNodeId"];
  readonly forkedFrom?: OrchestrationV2ProviderThread["forkedFrom"];
  readonly now: DateTime.Utc;
}): OrchestrationV2ProviderThread {
  return {
    id: input.idAllocator.derive.providerThread({
      driver: input.driver,
      nativeThreadId: input.nativeThreadId,
    }),
    driver: input.driver,
    providerInstanceId: input.providerInstanceId,
    providerSessionId: input.providerSessionId,
    appThreadId: input.appThreadId,
    ownerNodeId: input.ownerNodeId ?? null,
    nativeThreadRef: {
      driver: input.driver,
      nativeId: input.nativeThreadId,
      strength: "strong",
    },
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: null,
    lastRunOrdinal: null,
    handoffIds: [],
    forkedFrom: input.forkedFrom ?? null,
    createdAt: input.now,
    updatedAt: input.now,
  };
}

function unknownRecord(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function nonEmptyText(value: unknown, fallback: string): string {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : fallback;
}

function decodeByteText(value: unknown): string | undefined {
  if (!Array.isArray(value) || value.length === 0) return undefined;
  if (!value.every((entry) => typeof entry === "number" && Number.isInteger(entry))) {
    return undefined;
  }
  try {
    const text = new TextDecoder().decode(Uint8Array.from(value as number[])).trim();
    return text.length > 0 ? text : undefined;
  } catch {
    return undefined;
  }
}

function textFromUnknown(value: unknown): string | undefined {
  if (typeof value === "string") {
    return value;
  }
  const fromBytes = decodeByteText(value);
  if (fromBytes !== undefined) {
    return fromBytes;
  }
  if (Array.isArray(value)) {
    const parts = value.flatMap((entry) => {
      const text = textFromUnknown(entry);
      return text === undefined || text.length === 0 ? [] : [text];
    });
    return parts.length === 0 ? undefined : parts.join("\n");
  }
  const record = unknownRecord(value);
  if (record === undefined) {
    return undefined;
  }
  // Prefer prompt-facing Grok fields before nested envelopes.
  for (const key of [
    "output_for_prompt",
    "stdout",
    "stderr",
    "output",
    "content",
    "text",
    "message",
  ]) {
    const direct = record[key];
    if (typeof direct === "string" && direct.length > 0) {
      return direct;
    }
    const decoded = decodeByteText(direct);
    if (decoded !== undefined) {
      return decoded;
    }
    const text = textFromUnknown(direct);
    if (text !== undefined && text.length > 0) {
      return text;
    }
  }
  const result = unknownRecord(record.Result) ?? unknownRecord(record.result);
  if (result !== undefined) {
    return textFromUnknown(result);
  }
  return undefined;
}

function commandExitCode(value: unknown): number | undefined {
  const record = unknownRecord(value);
  for (const key of ["exitCode", "exit_code", "code"]) {
    const candidate = record?.[key];
    if (typeof candidate === "number" && Number.isInteger(candidate)) {
      return candidate;
    }
  }
  return undefined;
}

function pathFromToolCall(toolCall: AcpToolCallState): string | undefined {
  const locations = toolCall.data.locations;
  if (Array.isArray(locations)) {
    for (const location of locations) {
      const path = unknownRecord(location)?.path;
      if (typeof path === "string" && path.trim().length > 0) {
        return path.trim();
      }
    }
  }
  const rawInput = unknownRecord(toolCall.data.rawInput);
  for (const key of ["path", "filePath", "file_path", "url", "query", "pattern"]) {
    const candidate = rawInput?.[key];
    if (typeof candidate === "string" && candidate.trim().length > 0) {
      return candidate.trim();
    }
  }
  return undefined;
}

function providerRequestKind(kind: string | "unknown"): ProviderRequestKind {
  switch (kind) {
    case "execute":
      return "command";
    case "read":
    case "search":
    case "fetch":
      return "file-read";
    case "edit":
    case "delete":
    case "move":
      return "file-change";
    default:
      return "command";
  }
}

function toolStatus(
  status: AcpToolCallState["status"],
): "pending" | "running" | "completed" | "failed" {
  switch (status) {
    case "completed":
      return "completed";
    case "failed":
      return "failed";
    case "pending":
      return "pending";
    default:
      return "running";
  }
}

function nodeStatus(status: ReturnType<typeof toolStatus>): OrchestrationV2ExecutionNode["status"] {
  return status === "pending" ? "running" : status;
}

function completedAtForStatus(
  status: ReturnType<typeof toolStatus>,
  now: DateTime.Utc,
): DateTime.Utc | null {
  return status === "completed" || status === "failed" ? now : null;
}

function selectPermissionOptionId(
  request: EffectAcpSchema.RequestPermissionRequest,
  decision: Exclude<ProviderApprovalDecision, "cancel">,
): string | undefined {
  const kind =
    decision === "acceptForSession"
      ? "allow_always"
      : decision === "accept"
        ? "allow_once"
        : "reject_once";
  return request.options.find((option) => option.kind === kind)?.optionId.trim() || undefined;
}

function selectAutoApprovedPermissionOption(
  request: EffectAcpSchema.RequestPermissionRequest,
): string | undefined {
  return (
    selectPermissionOptionId(request, "acceptForSession") ??
    selectPermissionOptionId(request, "accept")
  );
}

export type AcpPermissionDisposition = "allow" | "ask" | "deny";

export function acpPermissionDisposition(
  runtimePolicy: ProviderAdapterV2RuntimePolicy,
  request: EffectAcpSchema.RequestPermissionRequest,
): AcpPermissionDisposition {
  const approvalPolicy = runtimePolicy.approvalPolicy;
  const requiresApproval =
    approvalPolicy === undefined
      ? runtimePolicy.runtimeMode === "approval-required"
      : approvalPolicy !== "never";
  if (requiresApproval) {
    return "ask";
  }

  const sandboxPolicy = unknownRecord(runtimePolicy.sandboxPolicy);
  const sandboxType = sandboxPolicy?.type;
  const toolKind = request.toolCall.kind ?? "other";
  switch (sandboxType) {
    case "readOnly":
      return toolKind === "read" || toolKind === "search" || toolKind === "think"
        ? "allow"
        : "deny";
    case "workspaceWrite":
      return toolKind === "read" ||
        toolKind === "search" ||
        toolKind === "think" ||
        toolKind === "edit" ||
        toolKind === "delete" ||
        toolKind === "move"
        ? "allow"
        : "deny";
    case "dangerFullAccess":
    case "externalSandbox":
      return "allow";
    case undefined:
      return runtimePolicy.runtimeMode === "approval-required" ? "deny" : "allow";
    default:
      return "deny";
  }
}

function elicitationContent(
  answers: ProviderUserInputAnswers,
  allowedKeys: ReadonlySet<string>,
): Record<string, EffectAcpSchema.ElicitationContentValue> {
  const content: Record<string, EffectAcpSchema.ElicitationContentValue> = {};
  for (const [key, value] of Object.entries(answers)) {
    if (!allowedKeys.has(key)) continue;
    if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      content[key] = value;
    } else if (Array.isArray(value)) {
      content[key] = value.filter((entry): entry is string => typeof entry === "string");
    }
  }
  return content;
}

interface ActiveTextSegment {
  readonly nativeItemId: string;
  readonly startedAt: DateTime.Utc;
  text: string;
}

interface ActiveTextStream {
  current: ActiveTextSegment | null;
  nextSegment: number;
}

interface ActiveAcpTurn {
  readonly input: ProviderAdapterV2TurnInput;
  readonly providerTurnId: OrchestrationV2ProviderTurn["id"];
  readonly nativeTurnId: string;
  readonly startedAt: DateTime.Utc;
  readonly completed: Deferred.Deferred<void, never>;
  readonly assistant: ActiveTextStream;
  readonly reasoning: ActiveTextStream;
  readonly tools: Map<string, AcpToolCallState>;
  readonly toolStartedAt: Map<string, DateTime.Utc>;
  readonly subagents: Map<string, ActiveAcpSubagent>;
  readonly subagentsBySessionId: Map<string, ActiveAcpSubagent>;
  readonly pendingSubagentNotifications: Map<string, Array<EffectAcpSchema.SessionNotification>>;
  /** Background monitor/task id → toolCallId for synthetic root text updates. */
  readonly toolCallIdsByBackgroundTaskId: Map<string, string>;
  /**
   * Monitor end events often only say "use get_command…"; keep the turn open
   * until TaskOutput hydration arrives (or the safety timeout elapses).
   */
  readonly awaitingBackgroundHydration: Set<string>;
  plan: {
    readonly id: OrchestrationV2PlanArtifact["id"];
    readonly startedAt: DateTime.Utc;
  } | null;
  interrupted: boolean;
  finalized: boolean;
  settleScheduleGeneration: number;
  /** session/prompt already returned; finalize deferred for background work. */
  promptSettled: boolean;
  promptSettledStatus: "completed" | "interrupted" | "failed" | "cancelled" | null;
  backgroundFinalizeGeneration: number;
}

export function acpRootTurnHasIngestedOutput(context: {
  readonly assistant: ActiveTextStream;
  readonly reasoning: ActiveTextStream;
  readonly tools: ReadonlyMap<string, AcpToolCallState>;
  readonly plan: unknown;
}): boolean {
  return (
    context.assistant.nextSegment > 0 ||
    context.reasoning.nextSegment > 0 ||
    context.tools.size > 0 ||
    context.plan !== null
  );
}

/** True when a root session/update carries ingestible turn output, not keepalive noise. */
export function acpRootSessionUpdateIngestsOutput(
  notification: EffectAcpSchema.SessionNotification,
): boolean {
  const update = notification.update;
  switch (update.sessionUpdate) {
    case "agent_message_chunk":
    case "agent_thought_chunk":
      return update.content.type === "text" && update.content.text.length > 0;
    case "tool_call":
    case "tool_call_update":
    case "plan":
      return parseSessionUpdateEvent(notification).events.some(
        (event) => event._tag === "ToolCallUpdated" || event._tag === "PlanUpdated",
      );
    default:
      return false;
  }
}

/**
 * Post-settle traffic that should start a continuation run. Excludes monitor
 * end/event chatter that must not become ghost history or empty wake runs.
 */
export function acpPostSettleWakeEvidence(
  notification: EffectAcpSchema.SessionNotification,
  flavor: Pick<AcpAdapterV2Flavor, "extractBackgroundToolMutation"> = {},
): boolean {
  if (!acpRootSessionUpdateIngestsOutput(notification)) return false;
  const update = notification.update;
  if (
    (update.sessionUpdate === "user_message_chunk" ||
      update.sessionUpdate === "agent_message_chunk") &&
    update.content.type === "text"
  ) {
    const text = update.content.text;
    if (flavor.extractBackgroundToolMutation?.(text) !== undefined) return false;
    if (/<monitor-event\b/i.test(text) || /Monitor\s+["']?[0-9a-f-]{8,}["']?\s+ended/i.test(text)) {
      return false;
    }
  }
  return true;
}

interface ActiveAcpSubagent {
  task: OrchestrationV2Subagent;
  readonly childThreadId: ThreadId;
  readonly childRootNodeId: OrchestrationV2ExecutionNode["id"];
  readonly turnItemId: OrchestrationV2TurnItem["id"];
  readonly turnItemOrdinal: number;
  childSessionId: string | null;
  assistantText: string;
  nextChildOrdinal: number;
}

function acpTurnHasPendingRuntimeRequest(
  providerTurnId: OrchestrationV2ProviderTurn["id"],
  pending: ReadonlyMap<string, PendingRuntimeRequest>,
): boolean {
  return [...pending.values()].some(
    (request) =>
      request.runtimeRequest.providerTurnId === providerTurnId &&
      request.runtimeRequest.status === "pending",
  );
}

type PendingRuntimeRequest = {
  readonly requestId: RuntimeRequestId;
  readonly runtimeRequest: OrchestrationV2RuntimeRequest;
  readonly node: OrchestrationV2ExecutionNode;
  readonly turnItem: OrchestrationV2TurnItem;
} & (
  | {
      readonly type: "approval";
      readonly decision: Deferred.Deferred<ProviderApprovalDecision>;
    }
  | {
      readonly type: "user_input";
      readonly answers: Deferred.Deferred<ProviderUserInputAnswers | null>;
    }
);

interface SnapshotMessageState {
  readonly order: Array<string>;
  readonly messages: Map<string, OrchestrationV2ConversationMessage>;
  loadingRole: "user" | "assistant" | null;
  loadingIndex: number;
}

export function makeAcpAdapterV2(options: AcpAdapterV2Options): ProviderAdapterV2Shape {
  const { flavor, fileSystem, idAllocator, serverConfig } = options;
  const driver = flavor.driver;
  const continuationRequests = options.continuationRequests;
  const postSettleContinuationEnabled =
    flavor.enablePostSettleContinuation === true && continuationRequests !== undefined;

  return ProviderAdapterV2.of({
    instanceId: options.instanceId,
    driver,
    getCapabilities: () => Effect.succeed(flavor.capabilities),
    planSelectionTransition: (input) => Effect.succeed(acpSelectionTransition(input)),
    openSession: Effect.fn("AcpAdapterV2.openSession")(
      function* (input: ProviderAdapterV2OpenSessionInput) {
        const sessionScope = yield* Effect.scope;
        const events = yield* Queue.unbounded<ProviderAdapterV2Event>();
        const activeTurn = yield* Ref.make<ActiveAcpTurn | null>(null);
        const activeSessionId = yield* Ref.make<string | null>(null);
        const activeSessionSetup = yield* Ref.make<AcpSessionRuntimeStartResult | null>(null);
        const activeSelection = yield* Ref.make<ModelSelection | null>(null);
        const runtimeRestartRequired = yield* Ref.make(false);
        const pendingRuntimeRequests = yield* Ref.make(new Map<string, PendingRuntimeRequest>());
        const nextElicitationOrdinal = yield* Ref.make(0);
        const itemOrdinals = yield* Ref.make(new Map<string, number>());
        const nextItemOrdinalsByTurn = yield* Ref.make(new Map<string, number>());
        const providerTurns = yield* Ref.make(new Map<string, OrchestrationV2ProviderTurn>());
        const snapshot = yield* Ref.make<SnapshotMessageState>({
          order: [],
          messages: new Map(),
          loadingRole: null,
          loadingIndex: 0,
        });
        // Post-settle wake support (Grok async subagent/monitor follow-up). After
        // the root turn finalizes, later root session/update traffic buffers here
        // until a provider continuation run attaches and drains it.
        const lastTurnRoute = yield* Ref.make<{
          readonly threadId: ThreadId;
          readonly providerThreadId: ProviderThreadId;
        } | null>(null);
        const wakeBuffer = yield* Ref.make<Array<EffectAcpSchema.SessionNotification>>([]);
        const continuationRequested = yield* Ref.make(false);

        const emitProviderEvent = (event: ProviderAdapterV2Event) =>
          Queue.offer(events, event).pipe(Effect.asVoid);
        let scheduleSettleRootTurnWhenIdle = (_context: ActiveAcpTurn) => Effect.void;
        let rearmRootTurnRecoveryTimers = (_context: ActiveAcpTurn) => Effect.void;
        let scheduleDeferredFinalize: (context: ActiveAcpTurn) => Effect.Effect<void> = () =>
          Effect.void;

        const nativeLogging = options.nativeLogging?.(input.threadId);
        const runtimeInput: AcpAdapterV2RuntimeInput = {
          cwd: input.runtimePolicy.cwd ?? process.cwd(),
          mcpServers: acpMcpServers(input.threadId),
          interruptPromptOnCancel: flavor.interruptPromptOnCancel ?? false,
          clientCapabilities: {
            fs: { readTextFile: false, writeTextFile: false },
            terminal: false,
            elicitation: { form: {} },
          },
          clientInfo: { name: "t3-code", version: "0.0.0" },
          ...(nativeLogging?.requestLogger === undefined
            ? {}
            : { requestLogger: nativeLogging.requestLogger }),
          protocolLogging: nativeLogging?.protocolLogging ?? {
            logIncoming: true,
            logOutgoing: true,
            logger: () => Effect.void,
          },
        };
        let runtimeScope: Scope.Closeable | undefined;
        let runtime!: AcpSessionRuntime.AcpSessionRuntime["Service"];

        const resolveItemOrdinal = Effect.fnUntraced(function* (
          context: ActiveAcpTurn,
          nativeItemId: string,
        ) {
          const existing = (yield* Ref.get(itemOrdinals)).get(nativeItemId);
          if (existing !== undefined) return existing;
          const nextWithinTurn = yield* Ref.modify(nextItemOrdinalsByTurn, (current) => {
            const next = (current.get(context.nativeTurnId) ?? 0) + 1;
            const updated = new Map(current);
            updated.set(context.nativeTurnId, next);
            return [next, updated] as const;
          });
          const ordinal = context.input.providerTurnOrdinal * 100 + nextWithinTurn;
          yield* Ref.update(itemOrdinals, (current) => {
            const updated = new Map(current);
            updated.set(nativeItemId, ordinal);
            return updated;
          });
          return ordinal;
        });

        const rememberSnapshotMessage = (message: OrchestrationV2ConversationMessage) =>
          Ref.update(snapshot, (current) => {
            const key = String(message.id);
            const exists = current.messages.has(key);
            const messages = new Map(current.messages);
            messages.set(key, message);
            return {
              ...current,
              order: exists ? current.order : [...current.order, key],
              messages,
            };
          });

        const emitTextSegment = Effect.fnUntraced(function* (
          context: ActiveAcpTurn,
          kind: "assistant" | "reasoning",
          completed: boolean,
        ) {
          const stream = kind === "assistant" ? context.assistant : context.reasoning;
          const segment = stream.current;
          if (segment === null || segment.text.length === 0) return;
          const now = yield* DateTime.now;
          const ordinal = yield* resolveItemOrdinal(context, segment.nativeItemId);
          const nodeId = idAllocator.derive.nodeFromProviderItem({
            driver,
            nativeItemId: segment.nativeItemId,
          });
          const turnItemId = idAllocator.derive.turnItemFromProviderItem({
            driver,
            nativeItemId: segment.nativeItemId,
          });
          const nativeItemRef = {
            driver,
            nativeId: segment.nativeItemId,
            strength: "weak" as const,
          };
          yield* emitProviderEvent({
            type: "node.updated",
            driver,
            node: {
              id: nodeId,
              threadId: context.input.threadId,
              runId: context.input.runId,
              parentNodeId: context.input.rootNodeId,
              rootNodeId: context.input.rootNodeId,
              kind: kind === "assistant" ? "assistant_message" : "reasoning",
              status: completed ? "completed" : "running",
              countsForRun: false,
              providerThreadId: context.input.providerThread.id,
              providerTurnId: context.providerTurnId,
              nativeItemRef,
              runtimeRequestId: null,
              checkpointScopeId: null,
              startedAt: segment.startedAt,
              completedAt: completed ? now : null,
            },
          });
          if (kind === "assistant") {
            const messageId = idAllocator.derive.messageFromProviderItem({
              driver,
              nativeItemId: segment.nativeItemId,
            });
            const message: OrchestrationV2ConversationMessage = {
              createdBy: "agent",
              creationSource: "provider",
              id: messageId,
              threadId: context.input.threadId,
              runId: context.input.runId,
              nodeId,
              role: "assistant",
              text: segment.text,
              attachments: [],
              streaming: !completed,
              createdAt: segment.startedAt,
              updatedAt: now,
            };
            yield* emitProviderEvent({ type: "message.updated", driver, message });
            yield* emitProviderEvent({
              type: "turn_item.updated",
              driver,
              turnItem: {
                id: turnItemId,
                threadId: context.input.threadId,
                runId: context.input.runId,
                nodeId,
                providerThreadId: context.input.providerThread.id,
                providerTurnId: context.providerTurnId,
                nativeItemRef,
                parentItemId: null,
                ordinal,
                status: completed ? "completed" : "running",
                title: null,
                startedAt: segment.startedAt,
                completedAt: completed ? now : null,
                updatedAt: now,
                type: "assistant_message",
                messageId,
                text: segment.text,
                streaming: !completed,
              },
            });
            if (completed) yield* rememberSnapshotMessage(message);
            return;
          }
          yield* emitProviderEvent({
            type: "turn_item.updated",
            driver,
            turnItem: {
              id: turnItemId,
              threadId: context.input.threadId,
              runId: context.input.runId,
              nodeId,
              providerThreadId: context.input.providerThread.id,
              providerTurnId: context.providerTurnId,
              nativeItemRef,
              parentItemId: null,
              ordinal,
              status: completed ? "completed" : "running",
              title: null,
              startedAt: segment.startedAt,
              completedAt: completed ? now : null,
              updatedAt: now,
              type: "reasoning",
              text: segment.text,
              streaming: !completed,
            },
          });
        });

        const closeTextStream = Effect.fnUntraced(function* (
          context: ActiveAcpTurn,
          kind: "assistant" | "reasoning",
        ) {
          const stream = kind === "assistant" ? context.assistant : context.reasoning;
          if (stream.current === null) return;
          yield* emitTextSegment(context, kind, true);
          stream.current = null;
          if (kind === "assistant") {
            yield* scheduleSettleRootTurnWhenIdle(context);
          }
        });

        const closeTextStreams = Effect.fnUntraced(function* (context: ActiveAcpTurn) {
          yield* closeTextStream(context, "reasoning");
          yield* closeTextStream(context, "assistant");
        });

        const appendText = Effect.fnUntraced(function* (
          context: ActiveAcpTurn,
          kind: "assistant" | "reasoning",
          text: string,
        ) {
          if (text.length === 0) return;
          const other = kind === "assistant" ? "reasoning" : "assistant";
          yield* closeTextStream(context, other);
          const stream = kind === "assistant" ? context.assistant : context.reasoning;
          if (stream.current === null) {
            const now = yield* DateTime.now;
            stream.current = {
              nativeItemId: `${context.nativeTurnId}:${kind}:${stream.nextSegment}`,
              startedAt: now,
              text: "",
            };
            stream.nextSegment += 1;
          }
          stream.current.text += text;
          yield* emitTextSegment(context, kind, false);
        });

        const emitSubagentAssistant = Effect.fnUntraced(function* (
          subagent: ActiveAcpSubagent,
          text: string,
        ) {
          if (text.length === 0) return;
          subagent.assistantText += text;
          const now = yield* DateTime.now;
          const nativeItemId = `${subagent.task.nativeTaskRef?.nativeId ?? subagent.task.id}:result`;
          const artifacts = makeSubagentConversationArtifacts({
            messageId: idAllocator.derive.messageFromProviderItem({ driver, nativeItemId }),
            turnItemId: idAllocator.derive.turnItemFromProviderItem({ driver, nativeItemId }),
            threadId: subagent.childThreadId,
            rootNodeId: subagent.childRootNodeId,
            providerThreadId: subagent.task.providerThreadId,
            providerTurnId: null,
            nativeItemRef: { driver, nativeId: nativeItemId, strength: "weak" },
            role: "assistant",
            text: subagent.assistantText,
            ordinal: subagent.nextChildOrdinal,
            now,
          });
          yield* emitProviderEvent({ type: "message.updated", driver, message: artifacts.message });
          yield* emitProviderEvent({
            type: "turn_item.updated",
            driver,
            turnItem: artifacts.turnItem,
          });
        });

        const projectSubagentNotification = Effect.fnUntraced(function* (
          subagent: ActiveAcpSubagent,
          notification: EffectAcpSchema.SessionNotification,
        ) {
          const update = notification.update;
          if (update.sessionUpdate === "agent_message_chunk" && update.content.type === "text") {
            yield* emitSubagentAssistant(subagent, update.content.text);
          }
        });

        const emitSubagent = Effect.fnUntraced(function* (
          context: ActiveAcpTurn,
          update: AcpAdapterV2SubagentUpdate,
        ) {
          // Hydration tools (get_command_or_subagent_output) use a new toolCallId
          // but reference the child via subagent_id / task id.
          const existing =
            context.subagents.get(update.nativeTaskId) ??
            (update.childSessionId !== null
              ? context.subagentsBySessionId.get(update.childSessionId)
              : undefined);
          // get_command_or_subagent_output may target monitors/bash tasks. Only
          // hydrate when we already have a matching subagent lineage. Spawn ACKs
          // (non-empty prompt) may create a new lineage; empty-prompt hydration
          // with suppressNormalTool must not invent a phantom subagent.
          if (existing === undefined) {
            const isHydrationOnly = update.prompt === "" && update.title === null;
            if (isHydrationOnly || update.suppressNormalTool === false) {
              return;
            }
          }
          const now = yield* DateTime.now;
          const nativeTaskId = existing?.task.nativeTaskRef?.nativeId ?? update.nativeTaskId;
          const nativeItemRef = {
            driver,
            nativeId: nativeTaskId,
            strength: "strong" as const,
          };
          const nodeId =
            existing?.task.id ??
            idAllocator.derive.nodeFromProviderItem({
              driver,
              nativeItemId: nativeTaskId,
            });
          const childThreadId =
            existing?.childThreadId ??
            idAllocator.derive.threadFromProviderThread({
              driver,
              nativeThreadId: `${nativeThreadId(driver, context.input.providerThread)}:task:${nativeTaskId}`,
            });
          const childRootNodeId =
            existing?.childRootNodeId ??
            idAllocator.derive.nodeFromProviderItem({
              driver,
              nativeItemId: `${nativeTaskId}:child-root`,
            });
          const turnItemId =
            existing?.turnItemId ??
            idAllocator.derive.turnItemFromProviderItem({
              driver,
              nativeItemId: nativeTaskId,
            });
          const turnItemOrdinal =
            existing?.turnItemOrdinal ?? (yield* resolveItemOrdinal(context, nativeTaskId));
          const taskStatus = update.status;
          const task: OrchestrationV2Subagent = {
            ...(existing?.task ?? {
              id: nodeId,
              threadId: context.input.threadId,
              runId: context.input.runId,
              parentNodeId: context.input.rootNodeId,
              origin: "provider_native" as const,
              createdBy: "agent" as const,
              driver,
              providerInstanceId: context.input.modelSelection.instanceId,
              providerThreadId: null,
              childThreadId,
              nativeTaskRef: nativeItemRef,
              prompt: update.prompt,
              title: update.title,
              model: update.model,
              result: null,
              startedAt: now,
            }),
            status: taskStatus,
            result: existing?.assistantText || update.result,
            completedAt: taskStatus === "running" ? null : now,
            updatedAt: now,
          };
          const subagent: ActiveAcpSubagent = existing ?? {
            task,
            childThreadId,
            childRootNodeId,
            turnItemId,
            turnItemOrdinal,
            childSessionId: null,
            assistantText: "",
            nextChildOrdinal: 101,
          };
          subagent.task = task;
          context.subagents.set(nativeTaskId, subagent);

          if (existing === undefined) {
            yield* emitProviderEvent({
              type: "app_thread.created",
              driver,
              appThread: makeSubagentChildThread({
                parentThread: context.input.appThread,
                childThreadId,
                parentNodeId: nodeId,
                activeProviderThreadId: null,
                providerInstanceId: context.input.modelSelection.instanceId,
                modelSelection: {
                  ...context.input.modelSelection,
                  model: update.model ?? context.input.modelSelection.model,
                },
                title: subagentThreadTitle({
                  parentTitle: context.input.appThread.title,
                  title: update.title,
                  prompt: update.prompt,
                  ordinal: context.subagents.size,
                }),
                now,
                createdBy: "agent",
                creationSource: "provider",
              }),
            });
            const promptNativeItemId = `${nativeTaskId}:prompt`;
            const promptArtifacts = makeSubagentConversationArtifacts({
              messageId: idAllocator.derive.messageFromProviderItem({
                driver,
                nativeItemId: promptNativeItemId,
              }),
              turnItemId: idAllocator.derive.turnItemFromProviderItem({
                driver,
                nativeItemId: promptNativeItemId,
              }),
              threadId: childThreadId,
              rootNodeId: childRootNodeId,
              providerThreadId: null,
              providerTurnId: null,
              nativeItemRef: { driver, nativeId: promptNativeItemId, strength: "weak" },
              role: "user",
              text: update.prompt,
              ordinal: 100,
              now,
            });
            yield* emitProviderEvent({
              type: "message.updated",
              driver,
              message: promptArtifacts.message,
            });
            yield* emitProviderEvent({
              type: "turn_item.updated",
              driver,
              turnItem: promptArtifacts.turnItem,
            });
          }

          if (update.childSessionId !== null && subagent.childSessionId === null) {
            subagent.childSessionId = update.childSessionId;
            context.subagentsBySessionId.set(update.childSessionId, subagent);
            const providerThread = makeProviderThread({
              driver,
              providerInstanceId: context.input.modelSelection.instanceId,
              idAllocator,
              appThreadId: childThreadId,
              providerSessionId: input.providerSessionId,
              nativeThreadId: update.childSessionId,
              forkedFrom: {
                providerThreadId: context.input.providerThread.id,
                providerTurnId: context.providerTurnId,
              },
              now,
            });
            subagent.task = { ...subagent.task, providerThreadId: providerThread.id };
            yield* emitProviderEvent({
              type: "provider_thread.updated",
              driver,
              providerThread: { ...providerThread, status: "idle" },
            });
            const buffered = context.pendingSubagentNotifications.get(update.childSessionId) ?? [];
            context.pendingSubagentNotifications.delete(update.childSessionId);
            yield* Effect.forEach(
              buffered,
              (notification) => projectSubagentNotification(subagent, notification),
              { concurrency: 1, discard: true },
            );
          }

          if (
            taskStatus !== "running" &&
            subagent.assistantText.length === 0 &&
            update.result !== null
          ) {
            yield* emitSubagentAssistant(subagent, update.result);
          }
          const result = subagent.assistantText || update.result;
          subagent.task = {
            ...subagent.task,
            status: taskStatus,
            result,
            completedAt: taskStatus === "running" ? null : now,
            updatedAt: now,
          };
          const providerThreadId = subagent.task.providerThreadId;
          yield* emitProviderEvent({
            type: "node.updated",
            driver,
            node: {
              id: nodeId,
              threadId: context.input.threadId,
              runId: context.input.runId,
              parentNodeId: context.input.rootNodeId,
              rootNodeId: context.input.rootNodeId,
              kind: "subagent",
              status: taskStatus,
              countsForRun: false,
              providerThreadId: context.input.providerThread.id,
              providerTurnId: context.providerTurnId,
              nativeItemRef,
              runtimeRequestId: null,
              checkpointScopeId: null,
              startedAt: subagent.task.startedAt,
              completedAt: subagent.task.completedAt,
            },
          });
          yield* emitProviderEvent({
            type: "node.updated",
            driver,
            node: {
              id: childRootNodeId,
              threadId: childThreadId,
              runId: null,
              parentNodeId: null,
              rootNodeId: childRootNodeId,
              kind: "root_turn",
              status: taskStatus,
              countsForRun: false,
              providerThreadId,
              providerTurnId: null,
              nativeItemRef,
              runtimeRequestId: null,
              checkpointScopeId: null,
              startedAt: subagent.task.startedAt,
              completedAt: subagent.task.completedAt,
            },
          });
          yield* emitProviderEvent({ type: "subagent.updated", driver, subagent: subagent.task });
          yield* emitProviderEvent({
            type: "turn_item.updated",
            driver,
            turnItem: {
              id: turnItemId,
              threadId: context.input.threadId,
              runId: context.input.runId,
              nodeId,
              providerThreadId: context.input.providerThread.id,
              providerTurnId: context.providerTurnId,
              nativeItemRef,
              parentItemId: null,
              ordinal: turnItemOrdinal,
              status: taskStatus,
              title: subagent.task.title,
              startedAt: subagent.task.startedAt,
              completedAt: subagent.task.completedAt,
              updatedAt: now,
              type: "subagent",
              subagentId: subagent.task.id,
              origin: "provider_native",
              driver,
              providerInstanceId: context.input.modelSelection.instanceId,
              childThreadId,
              prompt: subagent.task.prompt,
              result,
            },
          });
        });

        const toolOutputText = (toolCall: AcpToolCallState): string => {
          if (typeof toolCall.data.rawOutput === "string") {
            return toolCall.data.rawOutput;
          }
          if (
            typeof (toolCall.data.rawOutput as { text?: unknown } | undefined)?.text === "string"
          ) {
            return String((toolCall.data.rawOutput as { text: string }).text);
          }
          return textFromUnknown(toolCall.data.rawOutput) ?? "";
        };

        const setToolOutputText = (toolCall: AcpToolCallState, text: string): AcpToolCallState => ({
          ...toolCall,
          data: {
            ...toolCall.data,
            rawOutput: {
              type: "Text",
              text,
            },
          },
        });

        const appendToolOutputText = (
          toolCall: AcpToolCallState,
          appendOutput: string,
        ): AcpToolCallState => {
          if (appendOutput.length === 0) return toolCall;
          return setToolOutputText(toolCall, `${toolOutputText(toolCall)}${appendOutput}`);
        };

        const isMonitorEndNoticeText = (text: string): boolean =>
          /Monitor\s+["']?[0-9a-f-]{8,}/i.test(text) && /ended/i.test(text);

        const hasDeferredBackgroundWork = (context: ActiveAcpTurn): boolean => {
          if (context.awaitingBackgroundHydration.size > 0) return true;
          for (const toolCallId of context.toolCallIdsByBackgroundTaskId.values()) {
            const tool = context.tools.get(toolCallId);
            if (tool === undefined) continue;
            const status = toolStatus(tool.status);
            if (status === "pending" || status === "running") return true;
          }
          for (const subagent of context.subagents.values()) {
            if (subagent.task.status === "running" || subagent.task.status === "pending") {
              return true;
            }
          }
          return false;
        };

        // scheduleDeferredFinalize is declared above openSession body start and
        // assigned after finalizeTurn so forked finalize Effect typing stays clean.
        const rearmDeferredFinalize = (context: ActiveAcpTurn) =>
          Effect.gen(function* () {
            if (!flavor.deferFinalizeForBackgroundWork) return;
            if (!context.promptSettled || context.finalized) return;
            if (hasDeferredBackgroundWork(context)) return;
            yield* scheduleDeferredFinalize(context);
          });

        // `let` breaks circular inference from monitor hydration re-entry.
        let emitTool: (
          context: ActiveAcpTurn,
          incoming: AcpToolCallState,
        ) => Effect.Effect<void> = () => Effect.void;

        const markAwaitingBackgroundHydration = (context: ActiveAcpTurn, taskId: string) =>
          Effect.gen(function* () {
            if (context.awaitingBackgroundHydration.has(taskId)) return;
            context.awaitingBackgroundHydration.add(taskId);
            // Safety: do not hold the root turn forever if the agent never hydrates.
            yield* Effect.gen(function* () {
              yield* Effect.sleep("60000 millis");
              if (context.finalized || !context.awaitingBackgroundHydration.has(taskId)) return;
              context.awaitingBackgroundHydration.delete(taskId);
              // End notices keep the tool running until hydration; force-complete so
              // deferred finalize can proceed when get_command never arrives.
              const toolCallId = context.toolCallIdsByBackgroundTaskId.get(taskId);
              const tool = toolCallId !== undefined ? context.tools.get(toolCallId) : undefined;
              if (tool !== undefined) {
                const status = toolStatus(tool.status);
                if (status === "pending" || status === "running") {
                  yield* emitTool(context, { ...tool, status: "completed" });
                }
              }
              yield* rearmDeferredFinalize(context);
            }).pipe(Effect.forkIn(sessionScope), Effect.asVoid);
          });

        emitTool = Effect.fnUntraced(function* (
          context: ActiveAcpTurn,
          incoming: AcpToolCallState,
        ) {
          yield* closeTextStreams(context);
          const previous = context.tools.get(incoming.toolCallId);
          const merged = mergeToolCallState(previous, incoming);
          const toolCall = flavor.normalizeToolCall?.(merged) ?? merged;
          context.tools.set(toolCall.toolCallId, toolCall);
          const backgroundTaskId = flavor.extractBackgroundTaskId?.(toolCall);
          if (backgroundTaskId !== undefined) {
            context.toolCallIdsByBackgroundTaskId.set(backgroundTaskId, toolCall.toolCallId);
          }

          // get_command TaskOutput for a registered monitor: hydrate that tool.
          const backgroundCompletion = flavor.extractBackgroundTaskCompletion?.(toolCall);
          let hydratedRegisteredMonitor = false;
          if (backgroundCompletion !== undefined) {
            context.awaitingBackgroundHydration.delete(backgroundCompletion.taskId);
            const targetToolCallId = context.toolCallIdsByBackgroundTaskId.get(
              backgroundCompletion.taskId,
            );
            const target =
              targetToolCallId !== undefined ? context.tools.get(targetToolCallId) : undefined;
            if (target !== undefined && target.toolCallId !== toolCall.toolCallId) {
              hydratedRegisteredMonitor = true;
              const nextStatus =
                backgroundCompletion.status === "running"
                  ? ("inProgress" as const)
                  : backgroundCompletion.status === "failed"
                    ? ("failed" as const)
                    : ("completed" as const);
              const hydrated =
                backgroundCompletion.appendOutput.length > 0
                  ? // TaskOutput is the real stdout; replace end-notice boilerplate
                    // so the timeline shows the listing, not only "Monitor ended…".
                    isMonitorEndNoticeText(toolOutputText(target)) ||
                    toolOutputText(target).trim().length === 0
                    ? setToolOutputText(
                        { ...target, status: nextStatus },
                        backgroundCompletion.appendOutput,
                      )
                    : appendToolOutputText(
                        { ...target, status: nextStatus },
                        backgroundCompletion.appendOutput,
                      )
                  : { ...target, status: nextStatus };
              yield* emitTool(context, hydrated);
            }
          }

          // Monitor TaskOutput shares the get_command tool shape with subagent
          // hydration; do not spawn a phantom subagent for a registered monitor.
          const subagentUpdate = hydratedRegisteredMonitor
            ? undefined
            : flavor.extractSubagentUpdate?.(toolCall);
          if (subagentUpdate !== undefined) {
            yield* emitSubagent(context, subagentUpdate);
            if (subagentUpdate.suppressNormalTool !== false) {
              yield* rearmDeferredFinalize(context);
              return;
            }
          }
          const status = toolStatus(toolCall.status);
          const now = yield* DateTime.now;
          const nativeItemId = `${nativeThreadId(driver, context.input.providerThread)}:tool:${toolCall.toolCallId}`;
          const ordinal = yield* resolveItemOrdinal(context, nativeItemId);
          const nodeId = idAllocator.derive.nodeFromProviderItem({ driver, nativeItemId });
          const turnItemId = idAllocator.derive.turnItemFromProviderItem({
            driver,
            nativeItemId,
          });
          const nativeItemRef = {
            driver,
            nativeId: toolCall.toolCallId,
            strength: "strong" as const,
          };
          const startedAt = context.toolStartedAt.get(toolCall.toolCallId) ?? now;
          context.toolStartedAt.set(toolCall.toolCallId, startedAt);
          const completedAt = completedAtForStatus(status, now);
          const title = toolCall.title ?? null;
          yield* emitProviderEvent({
            type: "node.updated",
            driver,
            node: {
              id: nodeId,
              threadId: context.input.threadId,
              runId: context.input.runId,
              parentNodeId: context.input.rootNodeId,
              rootNodeId: context.input.rootNodeId,
              kind: "tool_call",
              status: nodeStatus(status),
              countsForRun: true,
              providerThreadId: context.input.providerThread.id,
              providerTurnId: context.providerTurnId,
              nativeItemRef,
              runtimeRequestId: null,
              checkpointScopeId: null,
              startedAt,
              completedAt,
            },
          });

          const base = {
            id: turnItemId,
            threadId: context.input.threadId,
            runId: context.input.runId,
            nodeId,
            providerThreadId: context.input.providerThread.id,
            providerTurnId: context.providerTurnId,
            nativeItemRef,
            parentItemId: null,
            ordinal,
            status,
            title,
            startedAt,
            completedAt,
            updatedAt: now,
          } as const;
          const rawInput = toolCall.data.rawInput;
          const rawOutput = toolCall.data.rawOutput ?? toolCall.data.content;
          const path = pathFromToolCall(toolCall);
          const rawInputRecord = unknownRecord(rawInput);
          const inputVariant =
            typeof rawInputRecord?.variant === "string"
              ? rawInputRecord.variant.trim().toLowerCase()
              : "";
          const monitorCommand =
            typeof rawInputRecord?.command === "string" && rawInputRecord.command.trim().length > 0
              ? rawInputRecord.command.trim()
              : undefined;
          // Grok Monitor tools arrive as generic kind + variant; project like shell
          // so stdout is plain text in the timeline (not JSON {type:Text,text:...}).
          const projectAsCommandExecution = inputVariant === "monitor";
          let turnItem: OrchestrationV2TurnItem;
          switch (toolCall.kind) {
            case "read":
            case "search":
              turnItem = {
                ...base,
                type: "file_search",
                ...(path === undefined ? {} : { pattern: path }),
                ...(path === undefined
                  ? {}
                  : {
                      results: [
                        {
                          fileName: path,
                          ...(textFromUnknown(rawOutput) === undefined
                            ? {}
                            : { preview: textFromUnknown(rawOutput) }),
                        },
                      ],
                    }),
              };
              break;
            case "execute":
              turnItem = {
                ...base,
                type: "command_execution",
                input: toolCall.command ?? monitorCommand ?? toolCall.title ?? "Command",
                ...(textFromUnknown(rawOutput) === undefined
                  ? {}
                  : { output: textFromUnknown(rawOutput) }),
                ...(commandExitCode(rawOutput) === undefined
                  ? {}
                  : { exitCode: commandExitCode(rawOutput) }),
              };
              break;
            case "edit":
            case "delete":
            case "move":
              turnItem = {
                ...base,
                type: "file_change",
                fileName: path ?? toolCall.title ?? "File change",
                ...(textFromUnknown(rawOutput) === undefined
                  ? {}
                  : { diffStr: textFromUnknown(rawOutput) }),
              };
              break;
            case "fetch":
              turnItem = {
                ...base,
                type: "web_search",
                ...(path === undefined ? {} : { patterns: [path] }),
                ...(path === undefined
                  ? {}
                  : {
                      results: [
                        {
                          url: path,
                          ...(textFromUnknown(rawOutput) === undefined
                            ? {}
                            : { snippet: textFromUnknown(rawOutput) }),
                        },
                      ],
                    }),
              };
              break;
            default:
              if (projectAsCommandExecution) {
                turnItem = {
                  ...base,
                  type: "command_execution",
                  input:
                    toolCall.command ??
                    monitorCommand ??
                    toolCall.title ??
                    (inputVariant === "monitor" ? "Monitor" : "Command"),
                  ...(textFromUnknown(rawOutput) === undefined
                    ? {}
                    : { output: textFromUnknown(rawOutput) }),
                  ...(commandExitCode(rawOutput) === undefined
                    ? {}
                    : { exitCode: commandExitCode(rawOutput) }),
                };
              } else {
                turnItem = {
                  ...base,
                  type: "dynamic_tool",
                  toolName: toolCall.title ?? toolCall.kind ?? null,
                  input: rawInput ?? {},
                  ...(rawOutput === undefined ? {} : { output: rawOutput }),
                };
              }
          }
          yield* emitProviderEvent({ type: "turn_item.updated", driver, turnItem });
          yield* rearmDeferredFinalize(context);
        });

        const emitPlan = Effect.fnUntraced(function* (
          context: ActiveAcpTurn,
          update: AcpPlanUpdate,
        ) {
          yield* closeTextStreams(context);
          const nativeItemId = `${context.nativeTurnId}:plan`;
          const ordinal = yield* resolveItemOrdinal(context, nativeItemId);
          const now = yield* DateTime.now;
          const nodeId = idAllocator.derive.nodeFromProviderItem({ driver, nativeItemId });
          const turnItemId = idAllocator.derive.turnItemFromProviderItem({
            driver,
            nativeItemId,
          });
          if (context.plan === null) {
            context.plan = {
              id: yield* idAllocator.allocate.plan({
                threadId: context.input.threadId,
                runId: context.input.runId,
                driver,
              }),
              startedAt: now,
            };
          }
          const planId = context.plan.id;
          const steps: ReadonlyArray<OrchestrationV2PlanStep> = update.plan.map((step, index) => ({
            id: `acp-step-${index + 1}`,
            text: nonEmptyText(step.step, `Step ${index + 1}`),
            status:
              step.status === "inProgress"
                ? "running"
                : step.status === "completed"
                  ? "completed"
                  : "pending",
          }));
          const completed = steps.length > 0 && steps.every((step) => step.status === "completed");
          const nativeItemRef = { driver, nativeId: nativeItemId, strength: "weak" as const };
          const plan: OrchestrationV2PlanArtifact = {
            id: planId,
            threadId: context.input.threadId,
            runId: context.input.runId,
            nodeId,
            status: completed ? "completed" : "active",
            kind: "todo_list",
            steps,
            ...(update.explanation == null ? {} : { explanation: update.explanation }),
          };
          yield* emitProviderEvent({
            type: "node.updated",
            driver,
            node: {
              id: nodeId,
              threadId: context.input.threadId,
              runId: context.input.runId,
              parentNodeId: context.input.rootNodeId,
              rootNodeId: context.input.rootNodeId,
              kind: "todo_list",
              status: completed ? "completed" : "running",
              countsForRun: false,
              providerThreadId: context.input.providerThread.id,
              providerTurnId: context.providerTurnId,
              nativeItemRef,
              runtimeRequestId: null,
              checkpointScopeId: null,
              startedAt: context.plan.startedAt,
              completedAt: completed ? now : null,
            },
          });
          yield* emitProviderEvent({ type: "plan.updated", driver, plan });
          yield* emitProviderEvent({
            type: "turn_item.updated",
            driver,
            turnItem: {
              id: turnItemId,
              threadId: context.input.threadId,
              runId: context.input.runId,
              nodeId,
              providerThreadId: context.input.providerThread.id,
              providerTurnId: context.providerTurnId,
              nativeItemRef,
              parentItemId: null,
              ordinal,
              status: completed ? "completed" : "running",
              title: null,
              startedAt: context.plan.startedAt,
              completedAt: completed ? now : null,
              updatedAt: now,
              type: "todo_list",
              planId,
              steps,
              ...(update.explanation == null ? {} : { explanation: update.explanation }),
            },
          });
        });

        const appendLoadedHistory = (
          notification: EffectAcpSchema.SessionNotification,
          role: "user" | "assistant",
          text: string,
        ) =>
          Effect.gen(function* () {
            if (text.length === 0) return;
            const now = yield* DateTime.now;
            yield* Ref.update(snapshot, (current) => {
              const startsNew = current.loadingRole !== role;
              const loadingIndex = startsNew ? current.loadingIndex + 1 : current.loadingIndex;
              const nativeItemId = `${notification.sessionId}:history:${role}:${loadingIndex}`;
              const messageId = idAllocator.derive.messageFromProviderItem({
                driver,
                nativeItemId,
              });
              const key = String(messageId);
              const previous = current.messages.get(key);
              const messages = new Map(current.messages);
              messages.set(key, {
                createdBy: previous?.createdBy ?? (role === "user" ? "user" : "agent"),
                creationSource: previous?.creationSource ?? "provider",
                id: messageId,
                threadId: input.threadId,
                runId: null,
                nodeId: null,
                role,
                text: `${previous?.text ?? ""}${text}`,
                attachments: [],
                streaming: false,
                createdAt: previous?.createdAt ?? now,
                updatedAt: now,
              });
              return {
                order: current.order.includes(key) ? current.order : [...current.order, key],
                messages,
                loadingRole: role,
                loadingIndex,
              };
            });
          });

        const bufferPostSettleWake = Effect.fnUntraced(function* (
          notification: EffectAcpSchema.SessionNotification,
        ) {
          if (!postSettleContinuationEnabled || continuationRequests === undefined) {
            return false;
          }
          const rootSessionId = yield* Ref.get(activeSessionId);
          if (rootSessionId === null || notification.sessionId !== rootSessionId) {
            return false;
          }
          if (!acpPostSettleWakeEvidence(notification, flavor)) {
            return false;
          }
          yield* Ref.update(wakeBuffer, (current) => [...current, notification]);
          const alreadyRequested = yield* Ref.get(continuationRequested);
          if (alreadyRequested) {
            return true;
          }
          const route = yield* Ref.get(lastTurnRoute);
          if (route === null) {
            yield* Effect.logWarning("orchestration-v2.acp-wake-turn-unroutable", {
              driver,
              providerSessionId: input.providerSessionId,
              sessionId: notification.sessionId,
            });
            return true;
          }
          yield* Ref.set(continuationRequested, true);
          yield* Effect.logInfo("orchestration-v2.acp-wake-turn-detected", {
            driver,
            providerSessionId: input.providerSessionId,
            threadId: route.threadId,
            providerThreadId: route.providerThreadId,
          });
          yield* continuationRequests.offer({
            threadId: route.threadId,
            providerThreadId: route.providerThreadId,
            driver,
            detail: null,
          });
          return true;
        });

        const handleSessionUpdate = Effect.fnUntraced(function* (
          notification: EffectAcpSchema.SessionNotification,
        ) {
          const context = yield* Ref.get(activeTurn);
          const update = notification.update;
          if (context === null) {
            // Prefer continuation buffering over history append so the same
            // frames are not double-counted once a continuation run attaches.
            if (yield* bufferPostSettleWake(notification)) {
              return;
            }
            if (
              (update.sessionUpdate === "user_message_chunk" ||
                update.sessionUpdate === "agent_message_chunk") &&
              update.content.type === "text"
            ) {
              // Late monitor end/event reminders must not become ghost user/assistant
              // history (or OS-facing chatter) after the root turn already finalized.
              const text = update.content.text;
              const lateBackgroundMutation =
                flavor.extractBackgroundToolMutation?.(text) !== undefined;
              const lateMonitorChatter =
                /<monitor-event\b/i.test(text) ||
                /Monitor\s+["']?[0-9a-f-]{8,}["']?\s+ended/i.test(text);
              if (!lateBackgroundMutation && !lateMonitorChatter) {
                yield* appendLoadedHistory(
                  notification,
                  update.sessionUpdate === "user_message_chunk" ? "user" : "assistant",
                  text,
                );
              }
            } else if (
              update.sessionUpdate === "tool_call" ||
              update.sessionUpdate === "tool_call_update" ||
              update.sessionUpdate === "plan"
            ) {
              yield* Ref.update(snapshot, (current) => ({ ...current, loadingRole: null }));
            }
            return;
          }
          if (context.finalized) return;
          if (notification.sessionId !== (yield* Ref.get(activeSessionId))) {
            if (flavor.extractSubagentUpdate === undefined) return;
            if (update.sessionUpdate !== "agent_message_chunk" || update.content.type !== "text") {
              return;
            }
            const subagent = context.subagentsBySessionId.get(notification.sessionId);
            if (subagent !== undefined) {
              yield* projectSubagentNotification(subagent, notification);
              return;
            }
            const buffered = context.pendingSubagentNotifications.get(notification.sessionId) ?? [];
            buffered.push(notification);
            context.pendingSubagentNotifications.set(notification.sessionId, buffered);
            return;
          }
          switch (update.sessionUpdate) {
            case "agent_message_chunk":
              if (update.content.type === "text") {
                yield* appendText(context, "assistant", update.content.text);
              }
              break;
            case "agent_thought_chunk":
              if (update.content.type === "text") {
                yield* appendText(context, "reasoning", update.content.text);
              }
              break;
            case "user_message_chunk":
              if (update.content.type === "text" && flavor.extractBackgroundToolMutation) {
                const mutation = flavor.extractBackgroundToolMutation(update.content.text);
                if (mutation !== undefined) {
                  const toolCallId = context.toolCallIdsByBackgroundTaskId.get(mutation.taskId);
                  const previous =
                    toolCallId !== undefined ? context.tools.get(toolCallId) : undefined;
                  if (previous !== undefined) {
                    let nextStatus =
                      mutation.status === "running"
                        ? ("inProgress" as const)
                        : mutation.status === "failed"
                          ? ("failed" as const)
                          : ("completed" as const);
                    // End notices typically omit full stdout (no get_command mention
                    // either). Keep the tool running and hold finalize until
                    // TaskOutput hydrates, or the safety timer force-completes.
                    if (nextStatus !== "inProgress") {
                      yield* markAwaitingBackgroundHydration(context, mutation.taskId);
                      nextStatus = "inProgress";
                    }
                    yield* emitTool(
                      context,
                      appendToolOutputText(
                        { ...previous, status: nextStatus },
                        mutation.appendOutput,
                      ),
                    );
                  }
                }
              }
              break;
            default: {
              const parsed = parseSessionUpdateEvent(notification);
              for (const event of parsed.events) {
                if (event._tag === "ToolCallUpdated") {
                  yield* emitTool(context, event.toolCall);
                } else if (event._tag === "PlanUpdated") {
                  yield* emitPlan(context, event.payload);
                }
              }
            }
          }
          if (acpRootSessionUpdateIngestsOutput(notification)) {
            yield* scheduleSettleRootTurnWhenIdle(context);
          }
          // Keep deferred finalize quiet-window fresh while wake traffic lands.
          yield* rearmDeferredFinalize(context);
        });

        const activeContext = Effect.gen(function* () {
          const context = yield* Ref.get(activeTurn);
          if (context === null) {
            return yield* new EffectAcpErrors.AcpTransportError({
              detail: "ACP agent requested input without an active turn",
              cause: "No active ACP turn",
            });
          }
          return context;
        });

        const emitApprovalRequest = Effect.fnUntraced(function* (
          context: ActiveAcpTurn,
          params: EffectAcpSchema.RequestPermissionRequest,
        ) {
          yield* closeTextStreams(context);
          const parsed = parsePermissionRequest(params);
          const nativeRequestId = params.toolCall.toolCallId;
          const requestId = yield* idAllocator.allocate.runtimeRequest({
            driver,
            providerTurnId: context.providerTurnId,
            nativeRequestId,
          });
          const decision = yield* Deferred.make<ProviderApprovalDecision>();
          const now = yield* DateTime.now;
          const nodeId = idAllocator.derive.approvalNode({ requestId });
          const requestKind = providerRequestKind(parsed.kind);
          const nativeItemRef = { driver, nativeId: nativeRequestId, strength: "weak" as const };
          const ordinal = yield* resolveItemOrdinal(
            context,
            `${context.nativeTurnId}:approval:${nativeRequestId}`,
          );
          const runtimeRequest: OrchestrationV2RuntimeRequest = {
            id: requestId,
            nodeId,
            providerTurnId: context.providerTurnId,
            nativeRequestRef: nativeItemRef,
            kind: requestKind,
            status: "pending",
            responseCapability: {
              type: "live",
              providerSessionId: input.providerSessionId,
            },
            createdAt: now,
            resolvedAt: null,
          };
          const node: OrchestrationV2ExecutionNode = {
            id: nodeId,
            threadId: context.input.threadId,
            runId: context.input.runId,
            parentNodeId: context.input.rootNodeId,
            rootNodeId: context.input.rootNodeId,
            kind: "approval_request",
            status: "waiting",
            countsForRun: false,
            providerThreadId: context.input.providerThread.id,
            providerTurnId: context.providerTurnId,
            nativeItemRef,
            runtimeRequestId: requestId,
            checkpointScopeId: null,
            startedAt: now,
            completedAt: null,
          };
          const turnItem: OrchestrationV2TurnItem = {
            id: idAllocator.derive.approvalTurnItem({ requestId }),
            threadId: context.input.threadId,
            runId: context.input.runId,
            nodeId,
            providerThreadId: context.input.providerThread.id,
            providerTurnId: context.providerTurnId,
            nativeItemRef,
            parentItemId: null,
            ordinal,
            status: "waiting",
            title: null,
            startedAt: now,
            completedAt: null,
            updatedAt: now,
            type: "approval_request",
            requestId,
            requestKind,
            ...(parsed.detail === undefined ? {} : { prompt: parsed.detail }),
          };
          yield* Ref.update(pendingRuntimeRequests, (current) => {
            const updated = new Map(current);
            updated.set(String(requestId), {
              type: "approval",
              requestId,
              decision,
              runtimeRequest,
              node,
              turnItem,
            });
            return updated;
          });
          yield* emitProviderEvent({
            type: "node.updated",
            driver,
            node,
          });
          yield* emitProviderEvent({
            type: "runtime_request.updated",
            driver,
            threadId: context.input.threadId,
            runtimeRequest,
          });
          yield* emitProviderEvent({
            type: "turn_item.updated",
            driver,
            turnItem,
          });
          const resolved = yield* Deferred.await(decision).pipe(
            Effect.ensuring(
              Effect.gen(function* () {
                yield* Ref.update(pendingRuntimeRequests, (current) => {
                  const updated = new Map(current);
                  updated.delete(String(requestId));
                  return updated;
                });
                yield* rearmRootTurnRecoveryTimers(context);
              }),
            ),
          );
          return resolved;
        });

        const requestUserInputInternal = Effect.fnUntraced(function* (
          request: AcpAdapterV2UserInputRequest,
        ) {
          const context = yield* activeContext;
          yield* closeTextStreams(context);
          const requestId = yield* idAllocator.allocate.runtimeRequest({
            driver,
            providerTurnId: context.providerTurnId,
            nativeRequestId: request.nativeRequestId,
          });
          const answers = yield* Deferred.make<ProviderUserInputAnswers | null>();
          const now = yield* DateTime.now;
          const nodeId = idAllocator.derive.nodeFromProviderItem({
            driver,
            nativeItemId: request.nativeItemId,
          });
          const turnItemId = idAllocator.derive.turnItemFromProviderItem({
            driver,
            nativeItemId: request.nativeItemId,
          });
          const nativeItemRef = {
            driver,
            nativeId: request.nativeItemId,
            strength: "weak" as const,
          };
          const ordinal = yield* resolveItemOrdinal(context, request.nativeItemId);
          const runtimeRequest: OrchestrationV2RuntimeRequest = {
            id: requestId,
            nodeId,
            providerTurnId: context.providerTurnId,
            nativeRequestRef: {
              driver,
              nativeId: request.nativeRequestId,
              strength: "weak",
            },
            kind: "user_input",
            status: "pending",
            responseCapability: {
              type: "live",
              providerSessionId: input.providerSessionId,
            },
            createdAt: now,
            resolvedAt: null,
          };
          const node: OrchestrationV2ExecutionNode = {
            id: nodeId,
            threadId: context.input.threadId,
            runId: context.input.runId,
            parentNodeId: context.input.rootNodeId,
            rootNodeId: context.input.rootNodeId,
            kind: "user_input_request",
            status: "waiting",
            countsForRun: false,
            providerThreadId: context.input.providerThread.id,
            providerTurnId: context.providerTurnId,
            nativeItemRef,
            runtimeRequestId: requestId,
            checkpointScopeId: null,
            startedAt: now,
            completedAt: null,
          };
          const turnItem: OrchestrationV2TurnItem = {
            id: turnItemId,
            threadId: context.input.threadId,
            runId: context.input.runId,
            nodeId,
            providerThreadId: context.input.providerThread.id,
            providerTurnId: context.providerTurnId,
            nativeItemRef,
            parentItemId: null,
            ordinal,
            status: "waiting",
            title: null,
            startedAt: now,
            completedAt: null,
            updatedAt: now,
            type: "user_input_request",
            requestId,
            questions: [...request.questions],
          };
          yield* Ref.update(pendingRuntimeRequests, (current) => {
            const updated = new Map(current);
            updated.set(String(requestId), {
              type: "user_input",
              requestId,
              answers,
              runtimeRequest,
              node,
              turnItem,
            });
            return updated;
          });
          yield* emitProviderEvent({
            type: "node.updated",
            driver,
            node,
          });
          yield* emitProviderEvent({
            type: "runtime_request.updated",
            driver,
            threadId: context.input.threadId,
            runtimeRequest,
          });
          yield* emitProviderEvent({
            type: "turn_item.updated",
            driver,
            turnItem,
          });
          return yield* Deferred.await(answers).pipe(
            Effect.ensuring(
              Effect.gen(function* () {
                yield* Ref.update(pendingRuntimeRequests, (current) => {
                  const updated = new Map(current);
                  updated.delete(String(requestId));
                  return updated;
                });
                yield* rearmRootTurnRecoveryTimers(context);
              }),
            ),
          );
        });

        const requestUserInput = (request: AcpAdapterV2UserInputRequest) =>
          requestUserInputInternal(request).pipe(
            Effect.mapError(
              (cause) =>
                new EffectAcpErrors.AcpTransportError({
                  detail: "Failed to handle ACP user input request",
                  cause,
                }),
            ),
          );

        const cancelPendingRuntimeRequests = Effect.fnUntraced(function* () {
          const requests = [...(yield* Ref.get(pendingRuntimeRequests)).values()];
          if (requests.length === 0) return;

          const now = yield* DateTime.now;
          yield* Effect.forEach(
            requests,
            (request) =>
              Effect.gen(function* () {
                const cancelled = yield* request.type === "approval"
                  ? Deferred.succeed(request.decision, "cancel")
                  : Deferred.succeed(request.answers, null);
                if (!cancelled) return;

                yield* emitProviderEvent({
                  type: "runtime_request.updated",
                  driver,
                  threadId: request.node.threadId,
                  runtimeRequest: {
                    ...request.runtimeRequest,
                    status: "cancelled",
                    resolvedAt: now,
                  },
                });
                yield* emitProviderEvent({
                  type: "node.updated",
                  driver,
                  node: {
                    ...request.node,
                    status: "cancelled",
                    completedAt: now,
                  },
                });
                yield* emitProviderEvent({
                  type: "turn_item.updated",
                  driver,
                  turnItem: {
                    ...request.turnItem,
                    status: "cancelled",
                    completedAt: now,
                    updatedAt: now,
                  },
                });
              }),
            { concurrency: 1, discard: true },
          );
        });

        const wireAcpRuntimeHandlers = Effect.fnUntraced(function* () {
          yield* runtime.handleSessionUpdate((notification) =>
            handleSessionUpdate(notification).pipe(
              Effect.mapError(
                (cause) =>
                  new EffectAcpErrors.AcpTransportError({
                    detail: "Failed to project an ACP session update",
                    cause,
                  }),
              ),
            ),
          );
          yield* runtime.handleRequestPermission((params) =>
            Effect.gen(function* () {
              const context = yield* activeContext;
              const disposition = acpPermissionDisposition(context.input.runtimePolicy, params);
              if (disposition === "allow") {
                const optionId = selectAutoApprovedPermissionOption(params);
                return optionId === undefined
                  ? ({ outcome: { outcome: "cancelled" } } as const)
                  : ({ outcome: { outcome: "selected", optionId } } as const);
              }
              if (disposition === "deny") {
                const optionId = selectPermissionOptionId(params, "decline");
                return optionId === undefined
                  ? ({ outcome: { outcome: "cancelled" } } as const)
                  : ({ outcome: { outcome: "selected", optionId } } as const);
              }
              const decision = yield* emitApprovalRequest(context, params);
              if (decision === "cancel") {
                return { outcome: { outcome: "cancelled" } } as const;
              }
              const optionId = selectPermissionOptionId(params, decision);
              return optionId === undefined
                ? ({ outcome: { outcome: "cancelled" } } as const)
                : ({ outcome: { outcome: "selected", optionId } } as const);
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new EffectAcpErrors.AcpTransportError({
                    detail: "Failed to handle an ACP permission request",
                    cause,
                  }),
              ),
            ),
          );
          yield* runtime.handleElicitation((params) =>
            Effect.gen(function* () {
              if (params.mode === "url") {
                return { action: { action: "decline" } } as const;
              }
              const questions = Object.entries(params.requestedSchema.properties ?? {}).map(
                ([id, property], index): OrchestrationV2UserInputQuestion => {
                  const record = unknownRecord(property);
                  const enumValues = Array.isArray(record?.enum)
                    ? record.enum.filter((value): value is string => typeof value === "string")
                    : [];
                  const options =
                    enumValues.length > 0
                      ? enumValues.map((value) => ({ label: value, description: value }))
                      : record?.type === "boolean"
                        ? [
                            { label: "true", description: "Yes" },
                            { label: "false", description: "No" },
                          ]
                        : [];
                  return {
                    id,
                    header: nonEmptyText(record?.title, `Question ${index + 1}`),
                    question: nonEmptyText(record?.description, params.message),
                    options,
                  };
                },
              );
              const ordinal = yield* Ref.getAndUpdate(
                nextElicitationOrdinal,
                (current) => current + 1,
              );
              const nativeRequestId = `${params.sessionId}:elicitation:${ordinal}`;
              const answers = yield* requestUserInput({
                nativeItemId: nativeRequestId,
                nativeRequestId,
                questions,
              });
              return answers === null
                ? ({ action: { action: "cancel" } } as const)
                : ({
                    action: {
                      action: "accept",
                      content: elicitationContent(
                        answers,
                        new Set(Object.keys(params.requestedSchema.properties ?? {})),
                      ),
                    },
                  } as const);
            }),
          );
          if (flavor.registerExtensions !== undefined) {
            yield* flavor.registerExtensions({ runtime, requestUserInput });
          }
        });

        const spawnAcpRuntime = Effect.fnUntraced(function* () {
          if (runtimeScope !== undefined) {
            yield* Scope.close(runtimeScope, Exit.void);
          }
          runtimeScope = yield* Scope.make();
          runtime = yield* flavor
            .makeRuntime(runtimeInput)
            .pipe(Effect.provideService(Scope.Scope, runtimeScope));
        });

        const restartAcpRuntime = Effect.fnUntraced(function* () {
          yield* spawnAcpRuntime();
          yield* wireAcpRuntimeHandlers();
        });

        yield* spawnAcpRuntime();
        yield* wireAcpRuntimeHandlers();

        const started = yield* runtime.start();
        yield* Ref.set(activeSessionId, started.sessionId);
        yield* Ref.set(activeSessionSetup, started);
        const capabilities = negotiatedCapabilities(flavor.capabilities, started);
        const canLoadSession = started.initializeResult.agentCapabilities?.loadSession === true;
        const canResumeSession =
          started.initializeResult.agentCapabilities?.sessionCapabilities?.resume != null;
        const supportsImagePrompts = acpSupportsImagePrompts({
          flavorSupportsImagePrompts: flavor.supportsImagePrompts,
          negotiatedImage:
            started.initializeResult.agentCapabilities?.promptCapabilities?.image === true,
        });

        const activateSession = Effect.fnUntraced(function* (sessionId: string) {
          if (canLoadSession) {
            return yield* runtime.loadSession(sessionId);
          }
          if (canResumeSession) {
            return yield* runtime.resumeSession(sessionId);
          }
          return yield* new ProviderAdapterProtocolError({
            driver,
            detail: `ACP driver cannot load or resume session ${sessionId}`,
          });
        });

        const configureSession = Effect.fnUntraced(function* (
          startResult: AcpSessionRuntimeStartResult,
          modelSelection: ModelSelection,
          runtimePolicy: ProviderAdapterV2RuntimePolicy,
        ) {
          const requestedModel = flavor.resolveModelId?.(modelSelection) ?? modelSelection.model;
          if (
            requestedModel.length > 0 &&
            requestedModel !== "auto" &&
            requestedModel !== "default"
          ) {
            const currentModel = startResult.sessionSetupResult.models?.currentModelId;
            if (currentModel !== requestedModel) {
              if (startResult.sessionSetupResult.models != null) {
                yield* runtime.setSessionModel(requestedModel);
              } else if (
                startResult.sessionSetupResult.configOptions?.some(
                  (option) => option.category === "model",
                ) === true
              ) {
                yield* runtime.setModel(requestedModel);
              }
            }
          }
          const configOptions = yield* runtime.getConfigOptions;
          const availableConfigIds = new Set(configOptions.map((option) => option.id));
          const unsupportedConfigIds = (modelSelection.options ?? [])
            .map((selection) => selection.id)
            .filter((id) => !availableConfigIds.has(id));
          if (unsupportedConfigIds.length > 0) {
            return yield* new ProviderAdapterProtocolError({
              driver,
              detail: `ACP session ${startResult.sessionId} does not expose requested configuration option(s): ${unsupportedConfigIds.join(", ")}`,
            });
          }
          for (const selection of modelSelection.options ?? []) {
            yield* runtime.setConfigOption(selection.id, selection.value);
          }
          const modeState = yield* runtime.getModeState;
          if (runtimePolicy.interactionMode === "plan" && modeState !== undefined) {
            const planMode = modeState.availableModes.find(
              (mode) => mode.id === "plan" || mode.id === "architect",
            );
            if (planMode !== undefined) yield* runtime.setMode(planMode.id);
          }
        });

        yield* configureSession(started, input.modelSelection, input.runtimePolicy);
        yield* Ref.set(activeSelection, input.modelSelection);
        const createdAt = yield* DateTime.now;
        const providerSession: OrchestrationV2ProviderSession = {
          id: input.providerSessionId,
          driver,
          providerInstanceId: options.instanceId,
          status: "ready",
          cwd: input.runtimePolicy.cwd ?? process.cwd(),
          model: input.modelSelection.model,
          capabilities,
          createdAt,
          updatedAt: createdAt,
          lastError: null,
        };

        const providerTurnPayload = (
          context: ActiveAcpTurn,
          status: OrchestrationV2ProviderTurn["status"],
          completedAt: DateTime.Utc | null,
        ): OrchestrationV2ProviderTurn => ({
          id: context.providerTurnId,
          providerThreadId: context.input.providerThread.id,
          nodeId: context.input.rootNodeId,
          runAttemptId: context.input.attemptId,
          nativeTurnRef: {
            driver,
            nativeId: context.nativeTurnId,
            strength: "weak",
          },
          ordinal: context.input.providerTurnOrdinal,
          status,
          startedAt: context.startedAt,
          completedAt,
        });

        const drainTrailingRootTurnChunks = Effect.fnUntraced(function* () {
          if (!flavor.settleRootTurnWhenIdle) return;
          // Projected via handleSessionUpdate, not getEvents(). Cooperative yield
          // only — replay uses TestClock; Effect.sleep here would stall settlement.
          yield* Effect.yieldNow;
          yield* Effect.yieldNow;
        });

        const finalizeTurn = Effect.fnUntraced(function* (
          context: ActiveAcpTurn,
          status: "completed" | "interrupted" | "failed" | "cancelled",
          failure?: OrchestrationV2ProviderFailure,
          options?: { readonly drainTrailingChunks?: boolean },
        ) {
          if (context.finalized) return;
          context.finalized = true;
          if (options?.drainTrailingChunks === true) {
            yield* drainTrailingRootTurnChunks();
          }
          yield* closeTextStreams(context);
          const now = yield* DateTime.now;
          const turn = providerTurnPayload(context, status, now);
          yield* Ref.update(providerTurns, (current) => {
            const updated = new Map(current);
            updated.set(String(turn.id), turn);
            return updated;
          });
          yield* emitProviderEvent({
            type: "provider_turn.updated",
            driver,
            threadId: context.input.threadId,
            providerTurn: turn,
          });
          yield* emitProviderEvent({
            type: "provider_thread.updated",
            driver,
            providerThread: {
              ...context.input.providerThread,
              providerSessionId: input.providerSessionId,
              status: "active",
              lastRunOrdinal: context.input.runOrdinal,
              firstRunOrdinal:
                context.input.providerThread.firstRunOrdinal ?? context.input.runOrdinal,
              updatedAt: now,
            },
          });
          yield* emitProviderEvent(
            status === "failed"
              ? {
                  type: "turn.terminal",
                  driver,
                  providerThreadId: context.input.providerThread.id,
                  providerTurnId: context.providerTurnId,
                  runOrdinal: context.input.runOrdinal,
                  failureItemOrdinal: yield* resolveItemOrdinal(
                    context,
                    `terminal-failure:${context.providerTurnId}`,
                  ),
                  status,
                  failure: failure ?? makeProviderFailure({ class: "provider_error" }),
                  threadDisposition: "reusable",
                }
              : {
                  type: "turn.terminal",
                  driver,
                  providerThreadId: context.input.providerThread.id,
                  providerTurnId: context.providerTurnId,
                  runOrdinal: context.input.runOrdinal,
                  status,
                  failure: null,
                  threadDisposition: "reusable",
                },
          );
          yield* Ref.set(activeTurn, null);
          yield* Deferred.succeed(context.completed, undefined).pipe(Effect.ignore);
        });

        const trySettleRootTurnWhenIdle = Effect.fnUntraced(function* (context: ActiveAcpTurn) {
          const pending = yield* Ref.get(pendingRuntimeRequests);
          const hasPendingRuntimeRequest = acpTurnHasPendingRuntimeRequest(
            context.providerTurnId,
            pending,
          );
          const hasRunningTool = [...context.tools.values()].some((tool) => {
            const status = toolStatus(tool.status);
            return status === "pending" || status === "running";
          });
          // Debounce already proved root-session quiescence; open segment handles
          // without an explicit close should not block settlement.
          const hasRunningSubagent = [...context.subagents.values()].some(
            (subagent) => subagent.task.status === "running",
          );
          if (
            !acpRootTurnIsIdle({
              finalized: context.finalized,
              interrupted: context.interrupted,
              assistantStreamOpen: false,
              reasoningStreamOpen: false,
              hasRunningTool,
              hasPendingRuntimeRequest,
              hasToolHistory: context.tools.size > 0,
              hasRunningSubagent,
              hasOutput: context.assistant.nextSegment > 0,
            })
          ) {
            return;
          }
          // Never session/cancel here. Speculative settle must not kill in-flight
          // Grok work; late tools would arrive with activeTurn null and drop.
          yield* finalizeTurn(context, "completed", undefined, { drainTrailingChunks: true });
        });

        scheduleDeferredFinalize = (context) =>
          Effect.gen(function* () {
            if (!flavor.deferFinalizeForBackgroundWork) return;
            if (!context.promptSettled || context.finalized || context.interrupted) return;
            if (hasDeferredBackgroundWork(context)) return;
            context.backgroundFinalizeGeneration += 1;
            const generation = context.backgroundFinalizeGeneration;
            // Minimal quiet for all models (no per-model carveouts). Defer +
            // awaitingBackgroundHydration hold the turn through monitors; this
            // is only a short debounce after the last rearm so a slightly late
            // first post-hydration assistant chunk is not dropped. Multi-second
            // floors (4–20s) only prolonged Working on grok-4.5.
            yield* Effect.gen(function* () {
              yield* Effect.sleep("2000 millis");
              if (
                context.finalized ||
                context.interrupted ||
                context.backgroundFinalizeGeneration !== generation
              ) {
                return;
              }
              if (hasDeferredBackgroundWork(context)) return;
              const status = context.promptSettledStatus ?? "completed";
              yield* finalizeTurn(context, status, undefined, { drainTrailingChunks: true });
            }).pipe(Effect.forkIn(sessionScope), Effect.asVoid);
          });

        scheduleSettleRootTurnWhenIdle = (context) =>
          Effect.gen(function* () {
            if (!flavor.settleRootTurnWhenIdle) return;
            context.settleScheduleGeneration += 1;
            const generation = context.settleScheduleGeneration;
            yield* Effect.gen(function* () {
              yield* Effect.sleep(`${acpRootTurnSettleDebounceMs} millis`);
              if (context.finalized || context.interrupted) return;
              if (context.settleScheduleGeneration !== generation) return;
              const active = yield* Ref.get(activeTurn);
              if (active !== context) return;
              yield* trySettleRootTurnWhenIdle(context);
            }).pipe(Effect.forkIn(sessionScope), Effect.asVoid);
          });

        rearmRootTurnRecoveryTimers = (context) =>
          Effect.gen(function* () {
            if (!acpRootTurnShouldRearmRecoveryTimers(context)) return;
            yield* scheduleSettleRootTurnWhenIdle(context);
          });

        const resolvePromptParts = Effect.fnUntraced(function* (
          turnInput: ProviderAdapterV2TurnInput,
        ) {
          const prompt: Array<EffectAcpSchema.ContentBlock> = [];
          if (turnInput.message.text.length > 0) {
            prompt.push({ type: "text", text: turnInput.message.text });
          }
          if (turnInput.message.attachments.length > 0 && !supportsImagePrompts) {
            return yield* new ProviderAdapterProtocolError({
              driver,
              detail: "ACP driver did not negotiate image prompt support",
            });
          }
          for (const attachment of turnInput.message.attachments) {
            const path = resolveAttachmentPath({
              attachmentsDir: serverConfig.attachmentsDir,
              attachment: attachment as ChatAttachment,
            });
            if (path === null) {
              return yield* new ProviderAdapterProtocolError({
                driver,
                detail: `Invalid attachment id '${attachment.id}'`,
              });
            }
            const bytes = yield* fileSystem.readFile(path).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterProtocolError({
                    driver,
                    detail: `Failed to read attachment '${attachment.id}'`,
                    payload: cause,
                  }),
              ),
            );
            prompt.push({
              type: "image",
              data: Buffer.from(bytes).toString("base64"),
              mimeType: attachment.mimeType,
            });
          }
          if (prompt.length === 0) {
            return yield* new ProviderAdapterProtocolError({
              driver,
              detail: "ACP turn requires non-empty text or attachments",
            });
          }
          return prompt;
        });

        const startTurn = Effect.fn("AcpAdapterV2.startTurn")(
          function* (turnInput: ProviderAdapterV2TurnInput) {
            const existing = yield* Ref.get(activeTurn);
            if (existing !== null) {
              return yield* new ProviderAdapterProtocolError({
                driver,
                detail: `ACP provider turn ${existing.providerTurnId} is still active`,
              });
            }
            const requestedSessionId = nativeThreadId(driver, turnInput.providerThread);
            const restartAfterInterrupt =
              flavor.restartRuntimeAfterInterrupt === true &&
              (yield* Ref.get(runtimeRestartRequired));
            if (restartAfterInterrupt) {
              yield* restartAcpRuntime();
              yield* Ref.set(runtimeRestartRequired, false);
              yield* Ref.set(activeSessionId, null);
              yield* Ref.set(activeSessionSetup, null);
              yield* Ref.set(activeSelection, null);
              yield* Ref.set(snapshot, {
                order: [],
                messages: new Map(),
                loadingRole: null,
                loadingIndex: 0,
              });
            }
            const needsSessionActivation =
              (yield* Ref.get(activeSessionId)) !== requestedSessionId || restartAfterInterrupt;
            if (needsSessionActivation) {
              const activated = yield* activateSession(requestedSessionId);
              yield* Ref.set(activeSessionId, activated.sessionId);
              yield* Ref.set(activeSessionSetup, activated);
              yield* configureSession(activated, turnInput.modelSelection, turnInput.runtimePolicy);
              yield* Ref.set(activeSelection, turnInput.modelSelection);
            } else {
              const configuredSelection = yield* Ref.get(activeSelection);
              if (
                configuredSelection === null ||
                !modelSelectionsEqual(configuredSelection, turnInput.modelSelection)
              ) {
                const currentSessionSetup = yield* Ref.get(activeSessionSetup);
                if (currentSessionSetup === null) {
                  return yield* new ProviderAdapterProtocolError({
                    driver,
                    detail: `ACP session ${requestedSessionId} has no active setup metadata`,
                  });
                }
                yield* configureSession(
                  currentSessionSetup,
                  turnInput.modelSelection,
                  turnInput.runtimePolicy,
                );
                yield* Ref.set(activeSelection, turnInput.modelSelection);
              }
            }
            yield* Ref.set(lastTurnRoute, {
              threadId: turnInput.threadId,
              providerThreadId: turnInput.providerThread.id,
            });
            // Continuation turns attach to wake traffic the agent already produced
            // after the prior root turn settled; do not re-prompt the ACP session.
            const isContinuationTurn =
              postSettleContinuationEnabled &&
              turnInput.message.createdBy === "agent" &&
              turnInput.message.creationSource === "provider";
            const prompt = isContinuationTurn ? null : yield* resolvePromptParts(turnInput);
            const startedAt = yield* DateTime.now;
            const nativeTurnId = `${requestedSessionId}:turn:${turnInput.providerTurnOrdinal}`;
            const providerTurnId = idAllocator.derive.providerTurn({ driver, nativeTurnId });
            const completed = yield* Deferred.make<void, never>();
            const context: ActiveAcpTurn = {
              input: turnInput,
              providerTurnId,
              nativeTurnId,
              startedAt,
              completed,
              assistant: { current: null, nextSegment: 0 },
              reasoning: { current: null, nextSegment: 0 },
              tools: new Map(),
              toolStartedAt: new Map(),
              subagents: new Map(),
              subagentsBySessionId: new Map(),
              pendingSubagentNotifications: new Map(),
              toolCallIdsByBackgroundTaskId: new Map(),
              awaitingBackgroundHydration: new Set(),
              plan: null,
              interrupted: false,
              finalized: false,
              settleScheduleGeneration: 0,
              promptSettled: false,
              promptSettledStatus: null,
              backgroundFinalizeGeneration: 0,
            };
            yield* Ref.set(activeTurn, context);
            const runningTurn = providerTurnPayload(context, "running", null);
            yield* Ref.update(providerTurns, (current) => {
              const updated = new Map(current);
              updated.set(String(runningTurn.id), runningTurn);
              return updated;
            });
            yield* emitProviderEvent({
              type: "provider_turn.updated",
              driver,
              threadId: turnInput.threadId,
              providerTurn: runningTurn,
            });
            yield* emitProviderEvent({
              type: "provider_thread.updated",
              driver,
              providerThread: {
                ...turnInput.providerThread,
                providerSessionId: input.providerSessionId,
                status: "active",
                updatedAt: startedAt,
              },
            });
            yield* rememberSnapshotMessage({
              createdBy: turnInput.message.createdBy,
              creationSource: turnInput.message.creationSource,
              id: turnInput.message.messageId,
              threadId: turnInput.threadId,
              runId: turnInput.runId,
              nodeId: turnInput.rootNodeId,
              role: "user",
              text: turnInput.message.text,
              attachments: [...turnInput.message.attachments],
              streaming: false,
              createdAt: startedAt,
              updatedAt: startedAt,
            });
            if (isContinuationTurn) {
              const drained = yield* Ref.modify(wakeBuffer, (current) => {
                const next: Array<EffectAcpSchema.SessionNotification> = [];
                return [current.slice(), next] as const;
              });
              yield* Ref.set(continuationRequested, false);
              // Treat attach mode as prompt-settled so deferred finalize / quiet
              // windows can complete the continuation after wake traffic drains.
              context.promptSettled = true;
              context.promptSettledStatus = "completed";
              if (drained.length === 0) {
                yield* finalizeTurn(context, "completed", undefined, {
                  drainTrailingChunks: true,
                });
                return;
              }
              for (const notification of drained) {
                yield* handleSessionUpdate(notification);
              }
              if (!context.finalized) {
                if (hasDeferredBackgroundWork(context)) {
                  yield* rearmDeferredFinalize(context);
                } else {
                  yield* scheduleDeferredFinalize(context);
                }
              }
              return;
            }
            yield* runtime.prompt({ prompt: prompt! }).pipe(
              Effect.flatMap((result) => {
                if (context.finalized) return Effect.void;
                const status =
                  result.stopReason === "cancelled"
                    ? context.interrupted
                      ? "interrupted"
                      : "cancelled"
                    : "completed";
                // Grok monitors (and async subagents) keep working after the root
                // prompt RPC returns. Defer finalize so their later updates and
                // wake-turn traffic still project onto this run.
                if (
                  flavor.deferFinalizeForBackgroundWork === true &&
                  hasDeferredBackgroundWork(context)
                ) {
                  context.promptSettled = true;
                  context.promptSettledStatus = status;
                  return Effect.void;
                }
                return finalizeTurn(context, status, undefined, { drainTrailingChunks: true });
              }),
              Effect.catchCause((cause) =>
                context.finalized
                  ? Effect.void
                  : finalizeTurn(
                      context,
                      context.interrupted ? "interrupted" : "failed",
                      makeProviderFailure({
                        cause: Cause.squash(cause),
                        class: "provider_error",
                      }),
                    ).pipe(
                      Effect.andThen(
                        Effect.logWarning("orchestration-v2.acp-prompt-failed", {
                          driver,
                          providerSessionId: input.providerSessionId,
                          providerThreadId: turnInput.providerThread.id,
                          providerTurnId,
                          cause,
                        }),
                      ),
                    ),
              ),
              Effect.forkIn(sessionScope),
            );
          },
          (effect, turnInput) =>
            effect.pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterTurnStartError({
                    driver,
                    threadId: turnInput.threadId,
                    providerThreadId: turnInput.providerThread.id,
                    runId: turnInput.runId,
                    cause,
                  }),
              ),
            ),
        );

        yield* Effect.addFinalizer(() =>
          Effect.gen(function* () {
            const requests = [...(yield* Ref.get(pendingRuntimeRequests)).values()];
            yield* Effect.forEach(
              requests,
              (request) =>
                request.type === "approval"
                  ? Deferred.succeed(request.decision, "cancel").pipe(Effect.ignore)
                  : Deferred.succeed(request.answers, null).pipe(Effect.ignore),
              { discard: true },
            );
            const sessionCapabilities =
              started.initializeResult.agentCapabilities?.sessionCapabilities;
            if (sessionCapabilities?.close != null) {
              yield* runtime.closeSession().pipe(Effect.ignore);
            }
            if (flavor.assertComplete !== undefined) {
              yield* flavor.assertComplete.pipe(Effect.orDie);
            }
            if (runtimeScope !== undefined) {
              yield* Scope.close(runtimeScope, Exit.void).pipe(Effect.ignore);
            }
          }),
        );

        const sessionRuntime: ProviderAdapterV2SessionRuntime = {
          instanceId: options.instanceId,
          driver,
          providerSessionId: input.providerSessionId,
          providerSession,
          events: Stream.fromEffectRepeat(Queue.take(events)),
          ...(postSettleContinuationEnabled
            ? {
                hasPendingBackgroundWork: Effect.gen(function* () {
                  if ((yield* Ref.get(wakeBuffer)).length > 0) return true;
                  if (yield* Ref.get(continuationRequested)) return true;
                  return false;
                }),
              }
            : {}),
          ensureThread: Effect.fn("AcpAdapterV2.ensureThread")(
            function* (threadInput: ProviderAdapterV2EnsureThreadInput) {
              const now = yield* DateTime.now;
              const sessionId = yield* Ref.get(activeSessionId);
              if (sessionId === null) {
                return yield* new ProviderAdapterProtocolError({
                  driver,
                  detail: "ACP runtime did not produce a session id",
                });
              }
              return makeProviderThread({
                driver,
                providerInstanceId: options.instanceId,
                idAllocator,
                appThreadId: threadInput.threadId,
                providerSessionId: input.providerSessionId,
                nativeThreadId: sessionId,
                now,
              });
            },
            (effect, threadInput) =>
              effect.pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapterEnsureThreadError({
                      driver,
                      threadId: threadInput.threadId,
                      cause,
                    }),
                ),
              ),
          ),
          resumeThread: Effect.fn("AcpAdapterV2.resumeThread")(
            function* (threadInput: {
              readonly providerThread: OrchestrationV2ProviderThread;
              readonly modelSelection?: ModelSelection;
              readonly runtimePolicy?: ProviderAdapterV2RuntimePolicy;
            }) {
              const sessionId = nativeThreadId(driver, threadInput.providerThread);
              if ((yield* Ref.get(activeSessionId)) !== sessionId) {
                yield* Ref.set(snapshot, {
                  order: [],
                  messages: new Map(),
                  loadingRole: null,
                  loadingIndex: 0,
                });
                const activated = yield* activateSession(sessionId);
                yield* Ref.set(activeSessionId, activated.sessionId);
                yield* Ref.set(activeSessionSetup, activated);
                const nextSelection = threadInput.modelSelection ?? input.modelSelection;
                yield* configureSession(
                  activated,
                  nextSelection,
                  threadInput.runtimePolicy ?? input.runtimePolicy,
                );
                yield* Ref.set(activeSelection, nextSelection);
              }
              const now = yield* DateTime.now;
              return {
                ...threadInput.providerThread,
                providerSessionId: input.providerSessionId,
                status: "idle" as const,
                updatedAt: now,
              };
            },
            (effect, threadInput) =>
              effect.pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapterResumeThreadError({
                      driver,
                      providerSessionId: input.providerSessionId,
                      providerThreadId: threadInput.providerThread.id,
                      cause,
                    }),
                ),
              ),
          ),
          startTurn,
          steerTurn: (turnInput) =>
            Effect.fail(
              new ProviderAdapterSteerRunUnsupportedError({
                driver,
                providerThreadId: turnInput.providerThread.id,
              }),
            ),
          interruptTurn: Effect.fn("AcpAdapterV2.interruptTurn")(
            function* (turnInput: ProviderAdapterV2InterruptInput) {
              const context = yield* Ref.get(activeTurn);
              if (context?.providerTurnId !== turnInput.providerTurnId) {
                return yield* new ProviderAdapterProtocolError({
                  driver,
                  detail: `ACP provider turn ${turnInput.providerTurnId} is not active`,
                });
              }
              context.interrupted = true;
              yield* runtime.cancel.pipe(Effect.ensuring(cancelPendingRuntimeRequests()));
              const stopped = yield* Deferred.await(context.completed).pipe(
                Effect.timeoutOption("10 seconds"),
              );
              if (Option.isNone(stopped)) {
                return yield* new ProviderAdapterProtocolError({
                  driver,
                  detail: `ACP provider turn ${turnInput.providerTurnId} did not acknowledge cancellation before the interrupt timeout`,
                });
              }
              if (
                flavor.restartRuntimeAfterInterrupt === true &&
                turnInput.requestRuntimeRestart === true
              ) {
                yield* Ref.set(runtimeRestartRequired, true);
              }
            },
            (effect, turnInput) =>
              effect.pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapterInterruptError({
                      driver,
                      providerThreadId: turnInput.providerThread.id,
                      providerTurnId: turnInput.providerTurnId,
                      cause,
                    }),
                ),
              ),
          ),
          respondToRuntimeRequest: (requestInput) =>
            Effect.gen(function* () {
              const pending = (yield* Ref.get(pendingRuntimeRequests)).get(
                String(requestInput.requestId),
              );
              if (pending === undefined) {
                return yield* new ProviderAdapterProtocolError({
                  driver,
                  detail: `No pending ACP runtime request ${requestInput.requestId}`,
                });
              }
              if (pending.type === "user_input") {
                yield* Deferred.succeed(pending.answers, requestInput.answers ?? null);
                return;
              }
              if (requestInput.decision === undefined) {
                return yield* new ProviderAdapterProtocolError({
                  driver,
                  detail: `ACP approval request ${requestInput.requestId} requires a decision`,
                });
              }
              yield* Deferred.succeed(pending.decision, requestInput.decision);
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterRuntimeRequestResponseError({
                    driver,
                    requestId: requestInput.requestId,
                    cause,
                  }),
              ),
            ),
          readThreadSnapshot: Effect.fn("AcpAdapterV2.readThreadSnapshot")(
            function* (snapshotInput) {
              const sessionId = nativeThreadId(driver, snapshotInput.providerThread);
              if ((yield* Ref.get(activeSessionId)) !== sessionId) {
                if (!capabilities.threads.canReadThreadSnapshot) {
                  return yield* new ProviderAdapterProtocolError({
                    driver,
                    detail: "ACP driver does not support session/load snapshots",
                  });
                }
                yield* Ref.set(snapshot, {
                  order: [],
                  messages: new Map(),
                  loadingRole: null,
                  loadingIndex: 0,
                });
                const activated = yield* runtime.loadSession(sessionId);
                yield* Ref.set(activeSessionId, activated.sessionId);
                yield* Ref.set(activeSessionSetup, activated);
                yield* Ref.set(activeSelection, null);
              }
              const state = yield* Ref.get(snapshot);
              const now = yield* DateTime.now;
              return {
                providerThread: {
                  ...snapshotInput.providerThread,
                  providerSessionId: input.providerSessionId,
                  status: "idle" as const,
                  updatedAt: now,
                },
                providerTurns: [...(yield* Ref.get(providerTurns)).values()],
                messages: state.order.flatMap((key) => {
                  const message = state.messages.get(key);
                  return message === undefined ? [] : [message];
                }),
                runtimeRequests: [],
                providerPayload: { protocol: ACP_PROTOCOL, sessionId },
              };
            },
            (effect, snapshotInput) =>
              effect.pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapterReadThreadSnapshotError({
                      driver,
                      providerThreadId: snapshotInput.providerThread.id,
                      cause,
                    }),
                ),
              ),
          ),
          rollbackThread: (rollbackInput) =>
            Effect.fail(
              new ProviderAdapterRollbackThreadError({
                driver,
                providerThreadId: rollbackInput.providerThread.id,
                checkpointId: rollbackInput.target.checkpointId,
                cause: "ACP does not define conversation rollback.",
              }),
            ),
          forkThread: Effect.fn("AcpAdapterV2.forkThread")(
            function* (forkInput) {
              if (!capabilities.threads.canForkThread) {
                return yield* new ProviderAdapterProtocolError({
                  driver,
                  detail: "ACP driver did not negotiate session/fork",
                });
              }
              if (forkInput.providerTurnId !== undefined) {
                return yield* new ProviderAdapterProtocolError({
                  driver,
                  detail: "ACP session/fork can only fork the current session head",
                });
              }
              const sourceSessionId = nativeThreadId(driver, forkInput.sourceProviderThread);
              const forked = yield* runtime.forkSession(sourceSessionId);
              yield* Ref.set(activeSessionId, forked.sessionId);
              yield* Ref.set(activeSessionSetup, forked);
              yield* Ref.set(activeSelection, null);
              const now = yield* DateTime.now;
              return makeProviderThread({
                driver,
                providerInstanceId: options.instanceId,
                idAllocator,
                appThreadId: forkInput.targetThreadId,
                providerSessionId: input.providerSessionId,
                nativeThreadId: forked.sessionId,
                ...(forkInput.ownerNodeId === undefined
                  ? {}
                  : { ownerNodeId: forkInput.ownerNodeId }),
                forkedFrom: {
                  providerThreadId: forkInput.sourceProviderThread.id,
                  ...(forkInput.providerTurnId === undefined
                    ? {}
                    : { providerTurnId: forkInput.providerTurnId }),
                },
                now,
              });
            },
            (effect, forkInput) =>
              effect.pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapterForkThreadError({
                      driver,
                      providerThreadId: forkInput.sourceProviderThread.id,
                      cause,
                    }),
                ),
              ),
          ),
        };
        return sessionRuntime;
      },
      (effect, input) =>
        effect.pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapterOpenSessionError({
                driver,
                providerSessionId: input.providerSessionId,
                cause,
              }),
          ),
        ),
    ),
  });
}

export type AcpAdapterV2Env = FileSystem.FileSystem | IdAllocatorV2;
