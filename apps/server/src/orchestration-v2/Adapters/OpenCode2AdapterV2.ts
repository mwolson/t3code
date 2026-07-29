/**
 * OpenCode 2.x ("OpenCode 2.0") orchestration adapter.
 *
 * A separate adapter rather than a mode of `OpenCodeAdapterV2`: 2.x shares the
 * vendor name and the tool vocabulary with 1.x and nothing else. Concretely,
 *
 *   - the wire surface is `/api/*` only, reached through `client.v2.*`;
 *   - every response is double-wrapped, `{ data: { data: … } }`, because the
 *     SDK's own `.data` is the parsed body and the body carries its own
 *     envelope;
 *   - the event vocabulary is a flat stream of typed lifecycle events
 *     (`session.text.started` / `.delta` / `.ended`, `session.tool.*`,
 *     `session.execution.*`) rather than 1.x's `message.part.updated` carrying
 *     a whole part object;
 *   - the model binds at session create via `ModelRef`, not per prompt;
 *   - permissions have no session-scoped ruleset. `session.create` takes none,
 *     so a non-interactive policy is expressed by answering
 *     `permission.v2.asked` rather than by installing rules up front.
 *
 * `live-scenarios/tests/opencode2-drive-probe.mjs` in the parent workspace is
 * the executable statement of this contract against a real binary.
 *
 * @module orchestration-v2/Adapters/OpenCode2AdapterV2
 */
import type {
  PromptInputFileAttachment,
  QuestionV2Info,
  SessionInfoV2,
  SessionMessageInfo,
  SessionPendingInfo,
  ShellInfoV2,
  V2Event,
} from "@opencode-ai/sdk-next/v2";
import { HostProcessEnvironment } from "@t3tools/shared/hostProcess";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import { causeErrorTag } from "@t3tools/shared/observability";
import {
  type ChatAttachment,
  type ModelSelection,
  type OpenCode2Settings,
  OpenCode2Settings as OpenCode2SettingsSchema,
  type OrchestrationV2AppThread,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderFailure,
  type OrchestrationV2ProviderRef,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2TurnItem,
  ProviderDriverKind,
  type ProviderInstanceId,
  type ProviderRequestKind,
  type ProviderSessionId,
  type RuntimeRequestId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as NodeURL from "node:url";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import type { EventNdjsonLogger } from "../../provider/Layers/EventNdjsonLogger.ts";
import { ProviderEventLoggers } from "../../provider/Layers/ProviderEventLoggers.ts";
import {
  structuralProtocolMethod,
  summarizeNativeProtocolPayload,
} from "../../provider/NativeProtocolLogging.ts";
import {
  OpenCode2Runtime,
  OpenCode2RuntimeError,
  runOpenCode2Sdk,
  type OpenCode2RuntimeShape,
} from "../../provider/opencode2Runtime.ts";
import {
  openCodeRuntimeErrorDetail,
  parseOpenCodeModelSlug,
} from "../../provider/opencodeRuntime.ts";
import { mergeProviderInstanceEnvironment } from "../../provider/ProviderInstanceEnvironment.ts";
import { IdAllocatorV2, type IdAllocatorV2Shape } from "../IdAllocator.ts";
import { makeProviderFailure } from "../ProviderFailure.ts";
import { turnScopedSelectionTransition } from "../ProviderSelectionTransition.ts";
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
  ProviderAdapterSteerRunError,
  ProviderAdapterTurnStartError,
  ProviderAdapterV2,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2OpenSessionInput,
  type ProviderAdapterV2SessionRuntime,
  type ProviderAdapterV2Shape,
  type ProviderAdapterV2TurnInput,
} from "../ProviderAdapter.ts";
import {
  ProviderAdapterDriverCreateError,
  type ProviderAdapterDriver,
  type ProviderAdapterDriverCreateInput,
} from "../ProviderAdapterDriver.ts";
// Tool names, permission actions, and the terminal-status mapping are the one
// thing 1.x and 2.x genuinely share, so these classifiers stay in one place
// rather than drifting between two copies.
import {
  openCodeBoundaryAfterProviderTurn,
  openCodePermissionRequestKind,
  openCodeToolProjectionKind,
  terminalToolStatus,
} from "./OpenCodeAdapterV2.ts";

export const OPENCODE2_PROVIDER = ProviderDriverKind.make("opencode2");
export const OPENCODE2_DRIVER_KIND = OPENCODE2_PROVIDER;
export const OPENCODE2_SDK_PROTOCOL = "opencode2-sdk.sse" as const;
const DEFAULT_OPENCODE2_SETTINGS = Schema.decodeSync(OpenCode2SettingsSchema)({});

/**
 * 2.x keeps 1.x's durable session/message identifiers and adds a durable
 * execution, but it still exposes no first-class turn object: the admitted
 * session input is the closest native correlation point, and the
 * `session.execution.*` terminal events are the authoritative settle signal.
 *
 * Subagents are declared unsupported rather than merely unimplemented. 2.x
 * reports tool calls by `callID` with no child-session linkage in the event
 * payload, so there is nothing to project a subagent thread from.
 */
export const OpenCode2ProviderCapabilitiesV2 = {
  sessions: {
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: true,
    supportsProviderSwitchingViaHandoff: true,
    supportsRuntimeModeSwitchInSession: false,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: true,
    canReadThreadSnapshot: true,
    canRollbackThread: true,
    canForkThread: true,
    canForkFromTurn: true,
    canForkFromSubagentThread: false,
    exposesNativeThreadId: true,
  },
  turns: {
    exposesNativeTurnId: false,
    emitsTurnStarted: true,
    emitsTurnCompleted: true,
    supportsInterrupt: true,
    supportsActiveSteering: true,
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
    supportsMcpTools: true,
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
    canGenerateSummaries: true,
    canConsumeHandoffSummaries: true,
    supportsDeltaHandoff: true,
    supportsFullThreadHandoff: true,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: true,
    supportsNestedCheckpointScopes: true,
    providerCanRollbackConversation: true,
    providerRollbackReturnsSnapshot: true,
    providerCanReadConversationSnapshot: true,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "weak",
    nativeItemIds: "strong",
    nativeRequestIds: "strong",
  },
} satisfies OrchestrationV2ProviderCapabilities;

type TerminalTurnStatus = Extract<
  OrchestrationV2ProviderTurn["status"],
  "completed" | "interrupted" | "failed" | "cancelled"
>;

type OpenCode2ToolStatus = "pending" | "running" | "completed" | "error";

interface OpenCode2TextPart {
  readonly kind: "text" | "reasoning";
  readonly id: string;
  readonly startedAt: DateTime.Utc;
  text: string;
  completed: boolean;
}

interface OpenCode2ToolPart {
  readonly kind: "tool";
  readonly id: string;
  readonly callId: string;
  readonly startedAt: DateTime.Utc;
  name: string;
  input: Record<string, unknown>;
  inputText: string;
  output: string | undefined;
  structured: Record<string, unknown> | undefined;
  status: OpenCode2ToolStatus;
  errorMessage: string | undefined;
  completedAt: DateTime.Utc | null;
}

interface OpenCode2ShellProjection {
  readonly shellId: string;
  readonly state: OpenCode2ThreadState;
  readonly turn: ActiveOpenCode2Turn;
  readonly part: OpenCode2ToolPart;
  readonly location: SessionInfoV2["location"];
  status: ShellInfoV2["status"];
}

type OpenCode2Part = OpenCode2TextPart | OpenCode2ToolPart;

interface ActiveOpenCode2Turn {
  readonly threadId: ThreadId;
  readonly runId: ProviderAdapterV2TurnInput["runId"];
  readonly rootNodeId: ProviderAdapterV2TurnInput["rootNodeId"];
  readonly appThread: OrchestrationV2AppThread;
  readonly modelSelection: ModelSelection;
  readonly providerTurnId: OrchestrationV2ProviderTurn["id"];
  readonly runOrdinal: number;
  readonly startedAt: DateTime.Utc;
  readonly itemOrdinals: Map<string, number>;
  readonly parts: Map<string, OpenCode2Part>;
  readonly toolIdsByCallId: Map<string, string>;
  readonly providerTurn: OrchestrationV2ProviderTurn;
  nextItemOrdinal: number;
  nativeInputId: string | null;
  executionStarted: boolean;
  interrupted: boolean;
  finalized: boolean;
}

interface OpenCode2ThreadState {
  readonly nativeSessionId: string;
  location: SessionInfoV2["location"];
  providerThread: OrchestrationV2ProviderThread;
  appThread: OrchestrationV2AppThread | null;
  activeTurn: ActiveOpenCode2Turn | null;
  boundModel: string | null;
  boundAgent: string | null;
  readonly providerTurns: Map<string, OrchestrationV2ProviderTurn>;
  readonly messages: Map<string, OrchestrationV2ConversationMessage>;
  readonly runtimeRequests: Map<string, OrchestrationV2RuntimeRequest>;
}

interface PendingOpenCode2Request {
  readonly requestId: RuntimeRequestId;
  readonly nativeRequestId: string;
  readonly turn: ActiveOpenCode2Turn;
  readonly state: OpenCode2ThreadState;
  readonly nodeId: OrchestrationV2ExecutionNode["id"];
  readonly turnItemId: OrchestrationV2TurnItem["id"];
  readonly requestKind: OrchestrationV2RuntimeRequest["kind"];
  readonly createdAt: DateTime.Utc;
  readonly permission?: { readonly action: string; readonly resources: ReadonlyArray<string> };
  readonly questions?: ReadonlyArray<QuestionV2Info>;
}

export interface OpenCode2AdapterV2Options {
  readonly instanceId: ProviderInstanceId;
  readonly settings: OpenCode2Settings;
  readonly environment: NodeJS.ProcessEnv;
  readonly runtime: OpenCode2RuntimeShape;
  readonly idAllocator: IdAllocatorV2Shape;
  readonly serverConfig: ServerConfig["Service"];
  readonly nativeEventLogger?: EventNdjsonLogger;
}

export const openCode2PendingWorkForSession = Effect.fnUntraced(function* (input: {
  readonly sessionID: string;
  readonly pending: Effect.Effect<ReadonlyArray<SessionPendingInfo>, OpenCode2RuntimeError>;
  readonly shells: Effect.Effect<ReadonlyArray<ShellInfoV2>, OpenCode2RuntimeError>;
}) {
  const pending = yield* input.pending;
  if (pending.some((item) => item.sessionID === input.sessionID)) {
    return true;
  }
  const shells = yield* input.shells;
  return shells.some(
    (shell) => shell.status === "running" && shell.metadata.sessionID === input.sessionID,
  );
});

export function openCode2ToolNeedsTerminalOverride(
  part: Pick<OpenCode2ToolPart, "status" | "errorMessage">,
  terminal: TerminalTurnStatus,
): boolean {
  if (part.status === "pending" || part.status === "running") return true;
  return (
    terminal === "interrupted" &&
    part.status === "error" &&
    part.errorMessage === "Tool execution interrupted"
  );
}

type OpenCode2SessionErrorData = Extract<V2Event, { type: "session.error" }>["data"];

export function openCode2SessionErrorMessage(data: OpenCode2SessionErrorData): string {
  const error = data.error;
  if (error === undefined) return "OpenCode 2 reported a session error.";
  return recordString(error.data, "message") ?? error.name;
}

export function openCode2SessionErrorStatus(
  data: OpenCode2SessionErrorData,
  interrupted: boolean,
): TerminalTurnStatus {
  return interrupted || data.error?.name === "MessageAbortedError" ? "interrupted" : "failed";
}

export function openCode2SessionErrorTargetSessionIds(
  sessionID: string | undefined,
  activeSessionIDs: ReadonlyArray<string>,
): ReadonlyArray<string> {
  if (sessionID === undefined) return activeSessionIDs;
  return activeSessionIDs.includes(sessionID) ? [sessionID] : [];
}

export function openCode2InterruptedThreadDisposition(
  reason: Extract<V2Event, { type: "session.execution.interrupted" }>["data"]["reason"],
): "reusable" | "broken" {
  return reason === "shutdown" ? "broken" : "reusable";
}

export function openCode2ShouldSettleTurn(
  source: "execution-terminal" | "execution-interrupted" | "idle",
  executionStarted: boolean,
  interruptRequested = false,
): boolean {
  if (source === "idle") return !executionStarted;
  if (source === "execution-interrupted") return executionStarted || interruptRequested;
  return executionStarted;
}

export interface OpenCode2ProtocolLogEvent {
  readonly direction: "incoming" | "outgoing";
  readonly messageKind: "request" | "response" | "notification" | "error";
  readonly method: string;
  readonly payload: unknown;
}

export function makeOpenCode2ProtocolLogger(input: {
  readonly nativeEventLogger: EventNdjsonLogger | undefined;
  readonly idAllocator: IdAllocatorV2Shape;
  readonly providerInstanceId: ProviderInstanceId;
  readonly providerSessionId: ProviderSessionId;
  readonly threadId: ThreadId;
}): (event: OpenCode2ProtocolLogEvent) => Effect.Effect<void, never> {
  return (event) =>
    Effect.gen(function* () {
      if (!input.nativeEventLogger) return;
      const observedAt = DateTime.formatIso(yield* DateTime.now);
      const method = structuralProtocolMethod(event.method);
      yield* input.nativeEventLogger.write(
        {
          observedAt,
          event: {
            id: yield* input.idAllocator.allocate.rawEvent({
              providerSessionId: input.providerSessionId,
              method,
            }),
            kind: "protocol",
            protocol: OPENCODE2_SDK_PROTOCOL,
            provider: OPENCODE2_PROVIDER,
            providerInstanceId: input.providerInstanceId,
            providerSessionId: input.providerSessionId,
            createdAt: observedAt,
            threadId: input.threadId,
            payload: {
              direction: event.direction,
              messageKind: event.messageKind,
              method,
              payload: summarizeNativeProtocolPayload(event.payload),
            },
          },
        },
        input.threadId,
      );
    }).pipe(
      Effect.catchCause((cause) =>
        Cause.hasInterrupts(cause)
          ? Effect.interrupt
          : Effect.logWarning("Failed to write native OpenCode 2 event log.", {
              errorTag: causeErrorTag(cause),
              reasonCount: cause.reasons.length,
              provider: OPENCODE2_PROVIDER,
              threadId: input.threadId,
            }),
      ),
    );
}

function protocolError(detail: string, payload?: unknown): ProviderAdapterProtocolError {
  return new ProviderAdapterProtocolError({
    driver: OPENCODE2_PROVIDER,
    detail,
    ...(payload === undefined ? {} : { payload }),
  });
}

/**
 * Builds the native selection fragment shared by session creation and
 * subsequent model or agent switches.
 *
 * @internal exported for tests
 */
export function openCode2SessionSelectionParameters(modelSelection: ModelSelection) {
  const parsed = parseOpenCodeModelSlug(modelSelection.model);
  if (parsed === null) {
    throw protocolError(
      `OpenCode 2 model '${modelSelection.model}' must use provider/model format`,
    );
  }
  const variant = getModelSelectionStringOptionValue(modelSelection, "variant");
  const agent = getModelSelectionStringOptionValue(modelSelection, "agent");
  return {
    model: {
      id: parsed.modelID,
      providerID: parsed.providerID,
      ...(variant === undefined ? {} : { variant }),
    },
    ...(agent === undefined ? {} : { agent }),
  };
}

function nativeThreadId(providerThread: OrchestrationV2ProviderThread): string {
  const nativeId = providerThread.nativeThreadRef?.nativeId;
  if (nativeId === null || nativeId === undefined) {
    throw protocolError(`Provider thread ${providerThread.id} has no OpenCode 2 session id`);
  }
  return nativeId;
}

function providerRef(nativeId: string, strength: "strong" | "weak" = "strong") {
  return {
    driver: OPENCODE2_PROVIDER,
    nativeId,
    strength,
  } satisfies OrchestrationV2ProviderRef;
}

function dateTimeFromEpoch(value: number | undefined, fallback: DateTime.Utc): DateTime.Utc {
  if (value === undefined) return fallback;
  return Option.getOrElse(DateTime.make(value), () => fallback);
}

function nonEmptyString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value : undefined;
}

function recordValue(input: unknown, key: string): unknown {
  return typeof input === "object" && input !== null && key in input
    ? (input as Record<string, unknown>)[key]
    : undefined;
}

function recordString(input: unknown, ...keys: ReadonlyArray<string>): string | undefined {
  for (const key of keys) {
    const value = nonEmptyString(recordValue(input, key));
    if (value !== undefined) return value;
  }
  return undefined;
}

function recordNumber(input: unknown, ...keys: ReadonlyArray<string>): number | undefined {
  for (const key of keys) {
    const value = recordValue(input, key);
    if (typeof value === "number" && Number.isFinite(value)) return value;
  }
  return undefined;
}

function stableJson(value: unknown): string {
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function sdkResponseForRawLog(value: unknown): unknown {
  if (typeof value !== "object" || value === null) return value;
  if ("data" in value) return { data: (value as { readonly data?: unknown }).data ?? null };
  if ("stream" in value) return { subscribed: true };
  return value;
}

/**
 * 2.x payloads are double-wrapped: the SDK's `.data` is the parsed body, and
 * every body carries its own `data` envelope. Reading one layer yields the
 * envelope rather than the value, which fails far from here.
 *
 * @internal exported for tests
 */
export function unwrapOpenCode2Data<A>(
  operation: string,
  result: { readonly data?: { readonly data?: A } | undefined },
): NonNullable<A> {
  const payload = result.data?.data;
  if (payload === undefined || payload === null) {
    throw new OpenCode2RuntimeError({
      operation,
      detail: `OpenCode 2 ${operation} returned no response payload.`,
    });
  }
  return payload as NonNullable<A>;
}

/**
 * Stable per-question id so an answer map keyed by header, question text, or
 * generated id all resolve. Mirrors `openCodeQuestionId` for the 2.x question
 * shape, which carries a header but none of 1.x's other fields.
 *
 * @internal exported for tests
 */
export function openCode2QuestionId(index: number, header: string): string {
  const slug = header
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return slug.length > 0 ? `question-${index}-${slug}` : `question-${index}`;
}

/**
 * 2.x has no session-scoped permission ruleset — `session.create` accepts none
 * and saved permissions are project-wide — so a non-interactive policy has to
 * be expressed by answering requests as they arrive. `always` rather than
 * `once` keeps a long run from re-asking for every repeat of the same action.
 *
 * @internal exported for tests
 */
export function openCode2AutoPermissionReply(
  runtimePolicy: ProviderAdapterV2TurnInput["runtimePolicy"],
): "always" | null {
  const approvalPolicy = nonEmptyString(runtimePolicy.approvalPolicy);
  if (approvalPolicy !== undefined) return approvalPolicy === "never" ? "always" : null;
  if (typeof runtimePolicy.approvalPolicy === "object" && runtimePolicy.approvalPolicy !== null) {
    return null;
  }
  return runtimePolicy.runtimeMode === "full-access" ? "always" : null;
}

function toOpenCode2FileAttachments(input: {
  readonly attachments: ReadonlyArray<ChatAttachment> | undefined;
  readonly resolveAttachmentPath: (attachment: ChatAttachment) => string | null;
}): Array<PromptInputFileAttachment> {
  const files: Array<PromptInputFileAttachment> = [];
  for (const attachment of input.attachments ?? []) {
    const attachmentPath = input.resolveAttachmentPath(attachment);
    if (!attachmentPath) continue;
    files.push({
      uri: NodeURL.pathToFileURL(attachmentPath).href,
      ...(attachment.name ? { name: attachment.name } : {}),
    });
  }
  return files;
}

function toolNodeStatus(status: OpenCode2ToolStatus): {
  readonly node: OrchestrationV2ExecutionNode["status"];
  readonly item: OrchestrationV2TurnItem["status"];
} {
  switch (status) {
    case "pending":
      return { node: "pending", item: "pending" };
    case "running":
      return { node: "running", item: "running" };
    case "completed":
      return { node: "completed", item: "completed" };
    case "error":
      return { node: "failed", item: "failed" };
  }
}

function toolContentText(content: unknown): string | undefined {
  if (!Array.isArray(content)) return undefined;
  const chunks = content
    .filter((entry) => recordString(entry, "type") === "text")
    .map((entry) => recordString(entry, "text"))
    .filter((text): text is string => text !== undefined);
  return chunks.length === 0 ? undefined : chunks.join("\n");
}

function openCode2PermissionRequestKind(action: string): ProviderRequestKind {
  return openCodePermissionRequestKind(action);
}

function makeProviderThread(input: {
  readonly idAllocator: IdAllocatorV2Shape;
  readonly providerInstanceId: ProviderInstanceId;
  readonly providerSessionId: OrchestrationV2ProviderThread["providerSessionId"];
  readonly appThreadId: OrchestrationV2ProviderThread["appThreadId"];
  readonly ownerNodeId?: OrchestrationV2ProviderThread["ownerNodeId"];
  readonly nativeSession: SessionInfoV2;
  readonly forkedFrom?: OrchestrationV2ProviderThread["forkedFrom"];
  readonly now: DateTime.Utc;
}): OrchestrationV2ProviderThread {
  return {
    id: input.idAllocator.derive.providerThread({
      driver: OPENCODE2_PROVIDER,
      nativeThreadId: input.nativeSession.id,
    }),
    driver: OPENCODE2_PROVIDER,
    providerInstanceId: input.providerInstanceId,
    providerSessionId: input.providerSessionId,
    appThreadId: input.appThreadId,
    ownerNodeId: input.ownerNodeId ?? null,
    nativeThreadRef: {
      driver: OPENCODE2_PROVIDER,
      nativeId: input.nativeSession.id,
      strength: "strong",
    },
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: null,
    lastRunOrdinal: null,
    handoffIds: [],
    forkedFrom: input.forkedFrom ?? null,
    createdAt: dateTimeFromEpoch(input.nativeSession.time.created, input.now),
    updatedAt: dateTimeFromEpoch(input.nativeSession.time.updated, input.now),
  };
}

export function makeOpenCode2AdapterV2(options: OpenCode2AdapterV2Options): ProviderAdapterV2Shape {
  const { idAllocator, runtime, serverConfig } = options;

  return ProviderAdapterV2.of({
    instanceId: options.instanceId,
    driver: OPENCODE2_PROVIDER,
    getCapabilities: () => Effect.succeed(OpenCode2ProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed(turnScopedSelectionTransition()),
    openSession: Effect.fn("OpenCode2AdapterV2.openSession")(
      function* (input: ProviderAdapterV2OpenSessionInput) {
        const scope = yield* Effect.scope;
        const cwd = input.runtimePolicy.cwd ?? serverConfig.cwd;
        const connection = yield* runtime.connectToOpenCode2Server({
          binaryPath: options.settings.binaryPath,
          serverUrl: options.settings.serverUrl,
          serverPassword: options.settings.serverPassword,
          environment: options.environment,
        });
        const client = runtime.createOpenCode2SdkClient({
          baseUrl: connection.url,
          directory: cwd,
          serverPassword: connection.password,
        });

        const now = yield* DateTime.now;
        let sessionEntity: OrchestrationV2ProviderSession = {
          id: input.providerSessionId,
          driver: OPENCODE2_PROVIDER,
          providerInstanceId: options.instanceId,
          status: "ready",
          cwd,
          model: input.modelSelection.model,
          capabilities: OpenCode2ProviderCapabilitiesV2,
          createdAt: now,
          updatedAt: now,
          lastError: null,
        };
        const events = yield* Queue.unbounded<ProviderAdapterV2Event>();
        const threads = new Map<string, OpenCode2ThreadState>();
        const shellProjections = new Map<string, OpenCode2ShellProjection>();
        const shellSessionIds = new Map<string, string>();
        const pendingRequests = new Map<string, PendingOpenCode2Request>();
        const pendingRequestsByNativeId = new Map<string, PendingOpenCode2Request>();
        const abortController = new AbortController();

        const emitProviderEvent = (event: ProviderAdapterV2Event) =>
          Queue.offer(events, event).pipe(Effect.asVoid);

        const logProtocolEvent = makeOpenCode2ProtocolLogger({
          nativeEventLogger: options.nativeEventLogger,
          idAllocator,
          providerInstanceId: options.instanceId,
          providerSessionId: input.providerSessionId,
          threadId: input.threadId,
        });

        const sdkCall = <A>(
          method: string,
          payload: unknown,
          call: () => Promise<A>,
        ): Effect.Effect<A, OpenCode2RuntimeError> =>
          logProtocolEvent({
            direction: "outgoing",
            messageKind: "request",
            method,
            payload,
          }).pipe(
            Effect.andThen(runOpenCode2Sdk(method, call)),
            Effect.tap((response) =>
              logProtocolEvent({
                direction: "incoming",
                messageKind: "response",
                method,
                payload: sdkResponseForRawLog(response),
              }),
            ),
          );

        const updateProviderSession = (
          status: OrchestrationV2ProviderSession["status"],
          lastError: string | null = sessionEntity.lastError,
        ) =>
          Effect.gen(function* () {
            const updatedAt = yield* DateTime.now;
            sessionEntity = { ...sessionEntity, status, lastError, updatedAt };
            yield* emitProviderEvent({
              type: "provider_session.updated",
              driver: OPENCODE2_PROVIDER,
              providerSession: sessionEntity,
            });
          });

        const updateProviderThread = (
          state: OpenCode2ThreadState,
          patch: Partial<OrchestrationV2ProviderThread>,
        ) =>
          Effect.gen(function* () {
            const updatedAt = yield* DateTime.now;
            state.providerThread = { ...state.providerThread, ...patch, updatedAt };
            yield* emitProviderEvent({
              type: "provider_thread.updated",
              driver: OPENCODE2_PROVIDER,
              providerThread: state.providerThread,
            });
          });

        const itemOrdinal = (turn: ActiveOpenCode2Turn, nativeItemId: string): number => {
          const existing = turn.itemOrdinals.get(nativeItemId);
          if (existing !== undefined) return existing;
          const ordinal = turn.nextItemOrdinal++;
          turn.itemOrdinals.set(nativeItemId, ordinal);
          return ordinal;
        };

        const emitProviderTurn = (
          state: OpenCode2ThreadState,
          turn: ActiveOpenCode2Turn,
          status: OrchestrationV2ProviderTurn["status"],
          completedAt: DateTime.Utc | null,
        ) => {
          const providerTurn: OrchestrationV2ProviderTurn = {
            ...turn.providerTurn,
            nativeTurnRef:
              turn.nativeInputId === null
                ? turn.providerTurn.nativeTurnRef
                : providerRef(turn.nativeInputId, "weak"),
            status,
            completedAt,
          };
          Object.assign(turn.providerTurn, providerTurn);
          state.providerTurns.set(String(providerTurn.id), providerTurn);
          return emitProviderEvent({
            type: "provider_turn.updated",
            driver: OPENCODE2_PROVIDER,
            threadId: turn.threadId,
            providerTurn,
          });
        };

        const emitTextPart = Effect.fnUntraced(function* (
          state: OpenCode2ThreadState,
          turn: ActiveOpenCode2Turn,
          part: OpenCode2TextPart,
          forceCompleted = false,
        ) {
          if (part.text.length === 0) return;
          const emittedAt = yield* DateTime.now;
          const isCompleted = forceCompleted || part.completed;
          const completedAt = isCompleted ? emittedAt : null;
          const nativeItemRef = providerRef(part.id);
          const nodeId = idAllocator.derive.nodeFromProviderItem({
            driver: OPENCODE2_PROVIDER,
            nativeItemId: part.id,
          });
          const turnItemId = idAllocator.derive.turnItemFromProviderItem({
            driver: OPENCODE2_PROVIDER,
            nativeItemId: part.id,
          });
          const ordinal = itemOrdinal(turn, part.id);
          yield* emitProviderEvent({
            type: "node.updated",
            driver: OPENCODE2_PROVIDER,
            node: {
              id: nodeId,
              threadId: turn.threadId,
              runId: turn.runId,
              parentNodeId: turn.rootNodeId,
              rootNodeId: turn.rootNodeId,
              kind: part.kind === "text" ? "assistant_message" : "reasoning",
              status: isCompleted ? "completed" : "running",
              countsForRun: false,
              providerThreadId: state.providerThread.id,
              providerTurnId: turn.providerTurnId,
              nativeItemRef,
              runtimeRequestId: null,
              checkpointScopeId: null,
              startedAt: part.startedAt,
              completedAt,
            },
          });
          if (part.kind === "text") {
            const messageId = idAllocator.derive.messageFromProviderItem({
              driver: OPENCODE2_PROVIDER,
              nativeItemId: part.id,
            });
            const message: OrchestrationV2ConversationMessage = {
              createdBy: "agent",
              creationSource: "provider",
              id: messageId,
              threadId: turn.threadId,
              runId: turn.runId,
              nodeId,
              role: "assistant",
              text: part.text,
              attachments: [],
              streaming: !isCompleted,
              createdAt: part.startedAt,
              updatedAt: emittedAt,
            };
            state.messages.set(String(message.id), message);
            yield* emitProviderEvent({
              type: "message.updated",
              driver: OPENCODE2_PROVIDER,
              message,
            });
            yield* emitProviderEvent({
              type: "turn_item.updated",
              driver: OPENCODE2_PROVIDER,
              turnItem: {
                id: turnItemId,
                threadId: turn.threadId,
                runId: turn.runId,
                nodeId,
                providerThreadId: state.providerThread.id,
                providerTurnId: turn.providerTurnId,
                nativeItemRef,
                parentItemId: null,
                ordinal,
                status: isCompleted ? "completed" : "running",
                title: null,
                startedAt: part.startedAt,
                completedAt,
                updatedAt: emittedAt,
                type: "assistant_message",
                messageId,
                text: part.text,
                streaming: !isCompleted,
              },
            });
            return;
          }
          yield* emitProviderEvent({
            type: "turn_item.updated",
            driver: OPENCODE2_PROVIDER,
            turnItem: {
              id: turnItemId,
              threadId: turn.threadId,
              runId: turn.runId,
              nodeId,
              providerThreadId: state.providerThread.id,
              providerTurnId: turn.providerTurnId,
              nativeItemRef,
              parentItemId: null,
              ordinal,
              status: isCompleted ? "completed" : "running",
              title: null,
              startedAt: part.startedAt,
              completedAt,
              updatedAt: emittedAt,
              type: "reasoning",
              text: part.text,
              streaming: !isCompleted,
            },
          });
        });

        const emitToolPart = Effect.fnUntraced(function* (
          state: OpenCode2ThreadState,
          turn: ActiveOpenCode2Turn,
          part: OpenCode2ToolPart,
          /**
           * Force a terminal status for a tool the turn ended underneath.
           * `session.interrupt` stops the execution without reporting a final
           * state for whatever tool was mid-flight, so the last observed
           * status stays `running` and the row would spin forever.
           */
          terminal?: TerminalTurnStatus,
        ) {
          const emittedAt = yield* DateTime.now;
          const status =
            terminal === undefined ? toolNodeStatus(part.status) : terminalToolStatus(terminal);
          const completedAt =
            terminal === undefined ? part.completedAt : (part.completedAt ?? emittedAt);
          const nativeItemRef = providerRef(part.id);
          const nodeId = idAllocator.derive.nodeFromProviderItem({
            driver: OPENCODE2_PROVIDER,
            nativeItemId: part.id,
          });
          const turnItemId = idAllocator.derive.turnItemFromProviderItem({
            driver: OPENCODE2_PROVIDER,
            nativeItemId: part.id,
          });
          const base = {
            id: turnItemId,
            threadId: turn.threadId,
            runId: turn.runId,
            nodeId,
            providerThreadId: state.providerThread.id,
            providerTurnId: turn.providerTurnId,
            nativeItemRef,
            parentItemId: null,
            ordinal: itemOrdinal(turn, part.id),
            status: status.item,
            title: part.name,
            startedAt: part.startedAt,
            completedAt,
            updatedAt: emittedAt,
          } satisfies Pick<
            OrchestrationV2TurnItem,
            | "id"
            | "threadId"
            | "runId"
            | "nodeId"
            | "providerThreadId"
            | "providerTurnId"
            | "nativeItemRef"
            | "parentItemId"
            | "ordinal"
            | "status"
            | "title"
            | "startedAt"
            | "completedAt"
            | "updatedAt"
          >;
          const projectionKind = openCodeToolProjectionKind(part.name);
          const exitCode = recordNumber(part.structured, "exit", "exitCode");
          let turnItem: OrchestrationV2TurnItem;
          if (projectionKind === "command_execution") {
            turnItem = {
              ...base,
              type: "command_execution",
              input: recordString(part.input, "command", "cmd") ?? stableJson(part.input),
              ...(part.output === undefined ? {} : { output: part.output }),
              ...(exitCode === undefined ? {} : { exitCode }),
            };
          } else if (projectionKind === "file_change") {
            turnItem = {
              ...base,
              type: "file_change",
              fileName: recordString(part.input, "filePath", "path", "file") ?? part.name,
              ...(recordString(part.input, "oldString", "oldText") === undefined
                ? {}
                : { oldStr: recordString(part.input, "oldString", "oldText")! }),
              ...(recordString(part.input, "newString", "content", "newText") === undefined
                ? {}
                : { newStr: recordString(part.input, "newString", "content", "newText")! }),
              ...(recordString(part.structured, "diff", "patch") === undefined
                ? {}
                : { diffStr: recordString(part.structured, "diff", "patch")! }),
            };
          } else if (projectionKind === "file_search") {
            turnItem = {
              ...base,
              type: "file_search",
              ...(recordString(part.input, "pattern", "query", "path", "filePath") === undefined
                ? {}
                : { pattern: recordString(part.input, "pattern", "query", "path", "filePath")! }),
            };
          } else if (projectionKind === "web_search") {
            const pattern = recordString(part.input, "query", "url", "pattern");
            turnItem = {
              ...base,
              type: "web_search",
              ...(pattern === undefined ? {} : { patterns: [pattern] }),
            };
          } else {
            turnItem = {
              ...base,
              type: "dynamic_tool",
              toolName: part.name,
              input: part.input,
              ...(part.output === undefined ? {} : { output: part.output }),
            };
          }
          yield* emitProviderEvent({
            type: "node.updated",
            driver: OPENCODE2_PROVIDER,
            node: {
              id: nodeId,
              threadId: turn.threadId,
              runId: turn.runId,
              parentNodeId: turn.rootNodeId,
              rootNodeId: turn.rootNodeId,
              kind: "tool_call",
              status: status.node,
              countsForRun: false,
              providerThreadId: state.providerThread.id,
              providerTurnId: turn.providerTurnId,
              nativeItemRef,
              runtimeRequestId: null,
              checkpointScopeId: null,
              startedAt: part.startedAt,
              completedAt,
            },
          });
          yield* emitProviderEvent({
            type: "turn_item.updated",
            driver: OPENCODE2_PROVIDER,
            turnItem,
          });
        });

        const runningShellForPart = (
          turn: ActiveOpenCode2Turn,
          part: OpenCode2ToolPart,
        ): OpenCode2ShellProjection | undefined =>
          Array.from(shellProjections.values()).find(
            (shell) => shell.turn === turn && shell.part === part && shell.status === "running",
          );

        const runtimeRequestTurnItem = (
          pending: PendingOpenCode2Request,
          status: OrchestrationV2TurnItem["status"],
          completedAt: DateTime.Utc | null,
          updatedAt: DateTime.Utc,
        ): OrchestrationV2TurnItem => {
          const base = {
            id: pending.turnItemId,
            threadId: pending.turn.threadId,
            runId: pending.turn.runId,
            nodeId: pending.nodeId,
            providerThreadId: pending.state.providerThread.id,
            providerTurnId: pending.turn.providerTurnId,
            nativeItemRef: providerRef(pending.nativeRequestId),
            parentItemId: null,
            ordinal: itemOrdinal(pending.turn, pending.nativeRequestId),
            status,
            startedAt: pending.createdAt,
            completedAt,
            updatedAt,
          };
          if (pending.questions !== undefined) {
            return {
              ...base,
              title: "User input",
              type: "user_input_request",
              requestId: pending.requestId,
              questions: pending.questions.map((question, index) => ({
                id: openCode2QuestionId(index, question.header),
                header: question.header.trim() || `Question ${index + 1}`,
                question:
                  question.question.trim() || question.header.trim() || `Question ${index + 1}`,
                options: question.options.map((option) => ({
                  label: option.label.trim() || "Option",
                  description: option.description.trim() || option.label.trim() || "Option",
                })),
              })),
            };
          }
          const permission = pending.permission;
          if (permission === undefined) {
            throw protocolError(`OpenCode 2 request ${pending.requestId} has no native payload`);
          }
          return {
            ...base,
            title: permission.action,
            type: "approval_request",
            requestId: pending.requestId,
            requestKind:
              pending.requestKind === "user_input"
                ? "command"
                : (pending.requestKind as Exclude<ProviderRequestKind, "user_input">),
            prompt:
              permission.resources.length === 0
                ? permission.action
                : permission.resources.join("\n"),
          };
        };

        const emitRuntimeRequest = Effect.fnUntraced(function* (
          state: OpenCode2ThreadState,
          turn: ActiveOpenCode2Turn,
          nativeRequestId: string,
          request:
            | {
                readonly type: "permission";
                readonly action: string;
                readonly resources: ReadonlyArray<string>;
              }
            | { readonly type: "question"; readonly questions: ReadonlyArray<QuestionV2Info> },
        ) {
          if (pendingRequestsByNativeId.has(nativeRequestId)) return;
          const createdAt = yield* DateTime.now;
          const requestId = yield* idAllocator.allocate.runtimeRequest({
            driver: OPENCODE2_PROVIDER,
            providerTurnId: turn.providerTurnId,
            nativeRequestId,
          });
          const nodeId = idAllocator.derive.approvalNode({ requestId });
          const turnItemId = idAllocator.derive.approvalTurnItem({ requestId });
          const requestKind: OrchestrationV2RuntimeRequest["kind"] =
            request.type === "permission"
              ? openCode2PermissionRequestKind(request.action)
              : "user_input";
          const pending: PendingOpenCode2Request = {
            requestId,
            nativeRequestId,
            turn,
            state,
            nodeId,
            turnItemId,
            requestKind,
            createdAt,
            ...(request.type === "permission"
              ? { permission: { action: request.action, resources: request.resources } }
              : { questions: request.questions }),
          };
          pendingRequests.set(String(requestId), pending);
          pendingRequestsByNativeId.set(nativeRequestId, pending);
          const runtimeRequest: OrchestrationV2RuntimeRequest = {
            id: requestId,
            nodeId,
            providerTurnId: turn.providerTurnId,
            nativeRequestRef: providerRef(nativeRequestId),
            kind: requestKind,
            status: "pending",
            responseCapability: {
              type: "live",
              providerSessionId: input.providerSessionId,
            },
            createdAt,
            resolvedAt: null,
          };
          state.runtimeRequests.set(String(requestId), runtimeRequest);
          yield* emitProviderEvent({
            type: "node.updated",
            driver: OPENCODE2_PROVIDER,
            node: {
              id: nodeId,
              threadId: turn.threadId,
              runId: turn.runId,
              parentNodeId: turn.rootNodeId,
              rootNodeId: turn.rootNodeId,
              kind: request.type === "question" ? "user_input_request" : "approval_request",
              status: "waiting",
              countsForRun: false,
              providerThreadId: state.providerThread.id,
              providerTurnId: turn.providerTurnId,
              nativeItemRef: providerRef(nativeRequestId),
              runtimeRequestId: requestId,
              checkpointScopeId: null,
              startedAt: createdAt,
              completedAt: null,
            },
          });
          yield* emitProviderEvent({
            type: "runtime_request.updated",
            driver: OPENCODE2_PROVIDER,
            threadId: turn.threadId,
            runtimeRequest,
          });
          yield* emitProviderEvent({
            type: "turn_item.updated",
            driver: OPENCODE2_PROVIDER,
            turnItem: runtimeRequestTurnItem(pending, "waiting", null, createdAt),
          });
          yield* updateProviderSession("waiting", null);
        });

        const resolveRuntimeRequest = Effect.fnUntraced(function* (
          nativeRequestId: string,
          status: "resolved" | "cancelled",
        ) {
          const pending = pendingRequestsByNativeId.get(nativeRequestId);
          if (pending === undefined) return;
          const resolvedAt = yield* DateTime.now;
          const current = pending.state.runtimeRequests.get(String(pending.requestId));
          if (current !== undefined) {
            const resolved: OrchestrationV2RuntimeRequest = { ...current, status, resolvedAt };
            pending.state.runtimeRequests.set(String(pending.requestId), resolved);
            yield* emitProviderEvent({
              type: "runtime_request.updated",
              driver: OPENCODE2_PROVIDER,
              threadId: pending.turn.threadId,
              runtimeRequest: resolved,
            });
          }
          yield* emitProviderEvent({
            type: "node.updated",
            driver: OPENCODE2_PROVIDER,
            node: {
              id: pending.nodeId,
              threadId: pending.turn.threadId,
              runId: pending.turn.runId,
              parentNodeId: pending.turn.rootNodeId,
              rootNodeId: pending.turn.rootNodeId,
              kind: pending.questions === undefined ? "approval_request" : "user_input_request",
              status: status === "resolved" ? "completed" : "cancelled",
              countsForRun: false,
              providerThreadId: pending.state.providerThread.id,
              providerTurnId: pending.turn.providerTurnId,
              nativeItemRef: providerRef(nativeRequestId),
              runtimeRequestId: pending.requestId,
              checkpointScopeId: null,
              startedAt: pending.createdAt,
              completedAt: resolvedAt,
            },
          });
          yield* emitProviderEvent({
            type: "turn_item.updated",
            driver: OPENCODE2_PROVIDER,
            turnItem: runtimeRequestTurnItem(
              pending,
              status === "resolved" ? "completed" : "cancelled",
              resolvedAt,
              resolvedAt,
            ),
          });
          pendingRequests.delete(String(pending.requestId));
          pendingRequestsByNativeId.delete(nativeRequestId);
          if (pendingRequests.size === 0) yield* updateProviderSession("running", null);
        });

        const finalizeTurn = Effect.fnUntraced(function* (
          state: OpenCode2ThreadState,
          turn: ActiveOpenCode2Turn,
          status: TerminalTurnStatus,
          terminal?: {
            readonly failure?: OrchestrationV2ProviderFailure;
            readonly threadDisposition?: "reusable" | "broken";
          },
        ) {
          if (turn.finalized) return;
          turn.finalized = true;
          const completedAt = yield* DateTime.now;
          for (const part of turn.parts.values()) {
            if (part.kind === "tool") {
              if (status === "completed" && runningShellForPart(turn, part) !== undefined) {
                continue;
              }
              if (openCode2ToolNeedsTerminalOverride(part, status)) {
                yield* emitToolPart(state, turn, part, status);
              }
              continue;
            }
            yield* emitTextPart(state, turn, part, true);
          }
          for (const pending of Array.from(pendingRequests.values())) {
            if (pending.turn.providerTurnId === turn.providerTurnId) {
              yield* resolveRuntimeRequest(pending.nativeRequestId, "cancelled");
            }
          }
          yield* emitProviderTurn(state, turn, status, completedAt);
          const threadDisposition = terminal?.threadDisposition ?? "reusable";
          yield* updateProviderThread(state, {
            status: "active",
            nativeConversationHeadRef:
              turn.nativeInputId === null
                ? state.providerThread.nativeConversationHeadRef
                : providerRef(turn.nativeInputId, "weak"),
          });
          state.activeTurn = null;
          const anotherTurnIsActive = Array.from(threads.values()).some(
            (candidate) => candidate.activeTurn !== null,
          );
          yield* updateProviderSession(
            anotherTurnIsActive ? "running" : status === "failed" ? "error" : "ready",
            status === "failed" ? sessionEntity.lastError : null,
          );
          yield* emitProviderEvent(
            status === "failed"
              ? {
                  type: "turn.terminal",
                  driver: OPENCODE2_PROVIDER,
                  providerThreadId: state.providerThread.id,
                  providerTurnId: turn.providerTurnId,
                  runOrdinal: turn.runOrdinal,
                  failureItemOrdinal: itemOrdinal(turn, `terminal-failure:${turn.providerTurnId}`),
                  status,
                  failure:
                    terminal?.failure ??
                    makeProviderFailure({
                      message: sessionEntity.lastError ?? undefined,
                      class: "provider_error",
                    }),
                  threadDisposition,
                }
              : {
                  type: "turn.terminal",
                  driver: OPENCODE2_PROVIDER,
                  providerThreadId: state.providerThread.id,
                  providerTurnId: turn.providerTurnId,
                  runOrdinal: turn.runOrdinal,
                  status,
                  failure: null,
                  threadDisposition,
                },
          );
        });

        /** Resolve the active turn for a session id, or nothing if it settled. */
        const activeFor = (
          sessionID: string | undefined,
        ): { state: OpenCode2ThreadState; turn: ActiveOpenCode2Turn } | null => {
          if (sessionID === undefined) return null;
          const state = threads.get(sessionID);
          const turn = state?.activeTurn;
          if (state === undefined || turn === null || turn === undefined || turn.finalized) {
            return null;
          }
          return { state, turn };
        };

        const textPartId = (kind: "text" | "reasoning", messageId: string, ordinal: number) =>
          `${messageId}:${kind}:${ordinal}`;

        const upsertTextPart = Effect.fnUntraced(function* (
          state: OpenCode2ThreadState,
          turn: ActiveOpenCode2Turn,
          kind: "text" | "reasoning",
          data: { readonly assistantMessageID: string; readonly ordinal: number },
          update: { readonly delta?: string; readonly text?: string; readonly completed?: boolean },
        ) {
          const id = textPartId(kind, data.assistantMessageID, data.ordinal);
          const startedAt = yield* DateTime.now;
          const existing = turn.parts.get(id);
          const part: OpenCode2TextPart =
            existing !== undefined && existing.kind !== "tool"
              ? existing
              : { kind, id, startedAt, text: "", completed: false };
          if (update.text !== undefined) part.text = update.text;
          else if (update.delta !== undefined) part.text += update.delta;
          if (update.completed === true) part.completed = true;
          turn.parts.set(id, part);
          yield* emitTextPart(state, turn, part);
        });

        const upsertToolPart = Effect.fnUntraced(function* (
          state: OpenCode2ThreadState,
          turn: ActiveOpenCode2Turn,
          callId: string,
          update: {
            readonly name?: string;
            readonly input?: Record<string, unknown>;
            readonly inputDelta?: string;
            readonly output?: string;
            readonly structured?: Record<string, unknown>;
            readonly status?: OpenCode2ToolStatus;
            readonly errorMessage?: string;
          },
        ) {
          const now = yield* DateTime.now;
          const id = turn.toolIdsByCallId.get(callId) ?? `tool:${callId}`;
          turn.toolIdsByCallId.set(callId, id);
          const existing = turn.parts.get(id);
          const part: OpenCode2ToolPart =
            existing !== undefined && existing.kind === "tool"
              ? existing
              : {
                  kind: "tool",
                  id,
                  callId,
                  startedAt: now,
                  // The name arrives on `session.tool.input.started`, ahead of
                  // every other event for this call, so this placeholder only
                  // shows if 2.x ever reorders them.
                  name: update.name ?? "tool",
                  input: {},
                  inputText: "",
                  output: undefined,
                  structured: undefined,
                  status: "pending",
                  errorMessage: undefined,
                  completedAt: null,
                };
          if (update.name !== undefined) part.name = update.name;
          if (update.inputDelta !== undefined) part.inputText += update.inputDelta;
          if (update.input !== undefined) part.input = update.input;
          if (update.output !== undefined) part.output = update.output;
          if (update.structured !== undefined) part.structured = update.structured;
          if (update.errorMessage !== undefined) part.errorMessage = update.errorMessage;
          const preserveRunningShell =
            update.status !== undefined &&
            (update.status === "completed" || update.status === "error") &&
            runningShellForPart(turn, part) !== undefined;
          if (update.status !== undefined && !preserveRunningShell) {
            part.status = update.status;
            if (update.status === "completed" || update.status === "error") part.completedAt = now;
          }
          turn.parts.set(id, part);
          yield* emitToolPart(state, turn, part);
        });

        const shellToolStatus = (shell: ShellInfoV2): OpenCode2ToolStatus => {
          if (shell.status === "running") return "running";
          if (shell.status === "exited" && shell.exit === 0) return "completed";
          return "error";
        };

        const registerShellProjection = Effect.fnUntraced(function* (
          state: OpenCode2ThreadState,
          shell: ShellInfoV2,
        ) {
          const existing = shellProjections.get(shell.id);
          if (existing !== undefined) {
            existing.status = shell.status;
            existing.part.status = shellToolStatus(shell);
            existing.part.structured = {
              ...existing.part.structured,
              ...(shell.exit === undefined ? {} : { exit: shell.exit }),
            };
            if (existing.part.status !== "running") {
              existing.part.completedAt = yield* DateTime.now;
            }
            yield* emitToolPart(existing.state, existing.turn, existing.part);
            return existing;
          }

          const turn = state.activeTurn;
          if (turn === null) return null;
          const associated = Array.from(turn.parts.values()).find(
            (part): part is OpenCode2ToolPart =>
              part.kind === "tool" &&
              openCodeToolProjectionKind(part.name) === "command_execution" &&
              recordString(part.input, "command", "cmd") === shell.command &&
              Array.from(shellProjections.values()).every((projection) => projection.part !== part),
          );
          const now = yield* DateTime.now;
          const part: OpenCode2ToolPart =
            associated ??
            ({
              kind: "tool",
              id: `shell:${shell.id}`,
              callId: shell.id,
              startedAt: dateTimeFromEpoch(shell.time.started, now),
              name: "bash",
              input: { command: shell.command },
              inputText: "",
              output: undefined,
              structured: shell.exit === undefined ? undefined : { exit: shell.exit },
              status: shellToolStatus(shell),
              errorMessage: undefined,
              completedAt:
                shell.status === "running" ? null : dateTimeFromEpoch(shell.time.completed, now),
            } satisfies OpenCode2ToolPart);
          part.status = shellToolStatus(shell);
          if (shell.exit !== undefined) {
            part.structured = { ...part.structured, exit: shell.exit };
          }
          if (part.status !== "running") {
            part.completedAt = dateTimeFromEpoch(shell.time.completed, now);
          }
          turn.parts.set(part.id, part);
          const projection: OpenCode2ShellProjection = {
            shellId: shell.id,
            state,
            turn,
            part,
            location: state.location,
            status: shell.status,
          };
          shellProjections.set(shell.id, projection);
          shellSessionIds.set(shell.id, state.nativeSessionId);
          yield* emitToolPart(state, turn, part);
          return projection;
        });

        const readShellOutput = Effect.fnUntraced(function* (
          shellId: string,
          location: SessionInfoV2["location"],
          initial?: {
            readonly output: string;
            readonly cursor: number;
            readonly truncated: boolean;
          },
        ) {
          let output = initial?.output ?? "";
          let cursor = initial?.cursor ?? 0;
          let truncated = initial?.truncated ?? true;
          while (truncated) {
            const parameters = {
              id: shellId,
              location,
              cursor: String(cursor),
              limit: String(64 * 1024),
            };
            const response = yield* sdkCall("shell.output", parameters, () =>
              client.v2.shell.output(parameters),
            );
            const page = unwrapOpenCode2Data<{
              readonly output: string;
              readonly cursor: number;
              readonly size: number;
              readonly truncated: boolean;
            }>("shell.output", response);
            output += page.output;
            if (!page.truncated) return output;
            if (page.cursor <= cursor) {
              return yield* protocolError(
                `OpenCode 2 shell ${shellId} output cursor did not advance`,
              );
            }
            cursor = page.cursor;
            truncated = page.truncated;
          }
          return output;
        });

        const completeShellProjection = Effect.fnUntraced(function* (
          shellId: string,
          patch: {
            readonly exit?: number;
            readonly status: ShellInfoV2["status"];
            readonly output?: {
              readonly output: string;
              readonly cursor: number;
              readonly truncated: boolean;
            };
          },
        ) {
          const projection = shellProjections.get(shellId);
          if (projection === undefined) return;
          projection.status = patch.status;
          if (
            projection.turn.providerTurn.status !== "running" &&
            projection.turn.providerTurn.status !== "completed"
          ) {
            return;
          }
          const output =
            patch.status === "killed" && patch.output === undefined
              ? projection.part.output
              : yield* readShellOutput(shellId, projection.location, patch.output).pipe(
                  Effect.catchCause((cause) =>
                    Effect.logWarning("Failed to read OpenCode 2 shell output.", {
                      errorTag: causeErrorTag(cause),
                      provider: OPENCODE2_PROVIDER,
                      shellId,
                    }).pipe(Effect.as(projection.part.output)),
                  ),
                );
          const completedAt = yield* DateTime.now;
          projection.part.status =
            patch.status === "exited" && patch.exit === 0 ? "completed" : "error";
          projection.part.completedAt = completedAt;
          if (output !== undefined) projection.part.output = output;
          projection.part.structured = {
            ...projection.part.structured,
            ...(patch.exit === undefined ? {} : { exit: patch.exit }),
          };
          yield* emitToolPart(projection.state, projection.turn, projection.part);
        });

        const autoReplyPermission = Effect.fnUntraced(function* (
          sessionID: string,
          requestID: string,
          reply: "always",
        ) {
          yield* sdkCall("session.permission.reply", { sessionID, requestID, reply }, () =>
            client.v2.session.permission.reply({ sessionID, requestID, reply }),
          ).pipe(
            Effect.catch((cause: OpenCode2RuntimeError) =>
              Effect.logWarning("Failed to auto-approve an OpenCode 2 permission request.", {
                provider: OPENCODE2_PROVIDER,
                detail: openCodeRuntimeErrorDetail(cause),
              }),
            ),
          );
        });

        const failActiveTurns = Effect.fnUntraced(function* (
          detail: string,
          failureClass: "transport_error" | "provider_error",
        ) {
          yield* updateProviderSession("error", detail);
          for (const state of threads.values()) {
            if (state.activeTurn !== null) {
              yield* finalizeTurn(state, state.activeTurn, "failed", {
                failure: makeProviderFailure({ message: detail, class: failureClass }),
                threadDisposition: "broken",
              });
            }
          }
        });

        const handleEvent = Effect.fnUntraced(function* (event: V2Event) {
          yield* logProtocolEvent({
            direction: "incoming",
            messageKind: "notification",
            method: event.type,
            payload: event,
          });
          switch (event.type) {
            case "session.input.promoted": {
              const state = threads.get(event.data.sessionID);
              if (state !== undefined && state.activeTurn === null) {
                yield* updateProviderThread(state, {});
              }
              return;
            }
            case "session.shell.started": {
              const state = threads.get(event.data.sessionID);
              if (state === undefined) return;
              yield* registerShellProjection(state, event.data.shell);
              yield* updateProviderThread(state, {});
              return;
            }
            case "session.shell.ended": {
              yield* completeShellProjection(event.data.shell.id, {
                status: event.data.shell.status,
                ...(event.data.shell.exit === undefined ? {} : { exit: event.data.shell.exit }),
                output: event.data.output,
              });
              shellProjections.delete(event.data.shell.id);
              shellSessionIds.delete(event.data.shell.id);
              const state = threads.get(event.data.sessionID);
              if (state !== undefined) yield* updateProviderThread(state, {});
              return;
            }
            case "shell.created": {
              const sessionID = recordString(event.data.info.metadata, "sessionID");
              if (sessionID === undefined) return;
              shellSessionIds.set(event.data.info.id, sessionID);
              const state = threads.get(sessionID);
              if (state !== undefined) {
                yield* registerShellProjection(state, event.data.info);
                yield* updateProviderThread(state, {});
              }
              return;
            }
            case "shell.exited": {
              yield* completeShellProjection(event.data.id, {
                status: event.data.status,
                ...(event.data.exit === undefined ? {} : { exit: event.data.exit }),
              });
              const sessionID = shellSessionIds.get(event.data.id);
              shellProjections.delete(event.data.id);
              shellSessionIds.delete(event.data.id);
              if (sessionID === undefined) return;
              const state = threads.get(sessionID);
              if (state !== undefined) yield* updateProviderThread(state, {});
              return;
            }
            case "shell.deleted": {
              const sessionID = shellSessionIds.get(event.data.id);
              if (sessionID === undefined) return;
              const projection = shellProjections.get(event.data.id);
              if (projection !== undefined && projection.turn.finalized) {
                yield* completeShellProjection(event.data.id, { status: "killed" });
              }
              shellProjections.delete(event.data.id);
              shellSessionIds.delete(event.data.id);
              const state = threads.get(sessionID);
              if (state !== undefined) yield* updateProviderThread(state, {});
              return;
            }
            case "session.input.admitted": {
              const active = activeFor(event.data.sessionID);
              if (active === null) return;
              if (active.turn.nativeInputId === null) {
                active.turn.nativeInputId = event.data.inputID;
                yield* emitProviderTurn(active.state, active.turn, "running", null);
              }
              return;
            }
            case "session.text.started":
            case "session.text.delta":
            case "session.text.ended": {
              const active = activeFor(event.data.sessionID);
              if (active === null) return;
              yield* upsertTextPart(active.state, active.turn, "text", event.data, {
                ...("delta" in event.data ? { delta: event.data.delta } : {}),
                ...("text" in event.data ? { text: event.data.text } : {}),
                ...(event.type === "session.text.ended" ? { completed: true } : {}),
              });
              return;
            }
            case "session.reasoning.started":
            case "session.reasoning.delta":
            case "session.reasoning.ended": {
              const active = activeFor(event.data.sessionID);
              if (active === null) return;
              yield* upsertTextPart(active.state, active.turn, "reasoning", event.data, {
                ...("delta" in event.data ? { delta: event.data.delta } : {}),
                ...("text" in event.data ? { text: event.data.text } : {}),
                ...(event.type === "session.reasoning.ended" ? { completed: true } : {}),
              });
              return;
            }
            case "session.tool.input.started": {
              const active = activeFor(event.data.sessionID);
              if (active === null) return;
              yield* upsertToolPart(active.state, active.turn, event.data.callID, {
                name: event.data.name,
                status: "pending",
              });
              return;
            }
            case "session.tool.input.delta": {
              const active = activeFor(event.data.sessionID);
              if (active === null) return;
              yield* upsertToolPart(active.state, active.turn, event.data.callID, {
                inputDelta: event.data.delta,
              });
              return;
            }
            case "session.tool.called": {
              const active = activeFor(event.data.sessionID);
              if (active === null) return;
              yield* upsertToolPart(active.state, active.turn, event.data.callID, {
                input: event.data.input,
                status: "running",
              });
              return;
            }
            case "session.tool.progress": {
              const active = activeFor(event.data.sessionID);
              if (active === null) return;
              const output = toolContentText(event.data.content);
              yield* upsertToolPart(active.state, active.turn, event.data.callID, {
                ...(output === undefined ? {} : { output }),
                structured: event.data.structured,
                status: "running",
              });
              return;
            }
            case "session.tool.success": {
              const active = activeFor(event.data.sessionID);
              if (active === null) return;
              const output = toolContentText(event.data.content);
              yield* upsertToolPart(active.state, active.turn, event.data.callID, {
                ...(output === undefined ? {} : { output }),
                structured: event.data.structured,
                status: "completed",
              });
              return;
            }
            case "session.tool.failed": {
              const active = activeFor(event.data.sessionID);
              if (active === null) return;
              yield* upsertToolPart(active.state, active.turn, event.data.callID, {
                output: event.data.error.message,
                errorMessage: event.data.error.message,
                status: "error",
              });
              return;
            }
            case "permission.v2.asked": {
              const active = activeFor(event.data.sessionID);
              if (active === null) return;
              const autoReply = openCode2AutoPermissionReply(input.runtimePolicy);
              if (autoReply !== null) {
                yield* autoReplyPermission(event.data.sessionID, event.data.id, autoReply);
                return;
              }
              yield* emitRuntimeRequest(active.state, active.turn, event.data.id, {
                type: "permission",
                action: event.data.action,
                resources: event.data.resources,
              });
              return;
            }
            case "permission.v2.replied":
              yield* resolveRuntimeRequest(event.data.requestID, "resolved");
              return;
            case "question.v2.asked": {
              const active = activeFor(event.data.sessionID);
              if (active === null) return;
              yield* emitRuntimeRequest(active.state, active.turn, event.data.id, {
                type: "question",
                questions: event.data.questions,
              });
              return;
            }
            case "question.v2.replied":
              yield* resolveRuntimeRequest(event.data.requestID, "resolved");
              return;
            case "question.v2.rejected":
              yield* resolveRuntimeRequest(event.data.requestID, "cancelled");
              return;
            case "session.execution.started": {
              const active = activeFor(event.data.sessionID);
              if (active === null) return;
              // Execution terminals carry only a session id. This start event
              // is the correlation barrier that keeps a late prior terminal
              // from settling the new active turn.
              active.turn.executionStarted = true;
              return;
            }
            case "session.execution.succeeded": {
              const active = activeFor(event.data.sessionID);
              if (active === null) return;
              if (
                !openCode2ShouldSettleTurn(
                  "execution-interrupted",
                  active.turn.executionStarted,
                  active.turn.interrupted,
                )
              ) {
                return;
              }
              yield* finalizeTurn(
                active.state,
                active.turn,
                active.turn.interrupted ? "interrupted" : "completed",
              );
              return;
            }
            case "session.execution.interrupted": {
              const active = activeFor(event.data.sessionID);
              if (active === null) return;
              if (!openCode2ShouldSettleTurn("execution-terminal", active.turn.executionStarted)) {
                return;
              }
              active.turn.interrupted = true;
              yield* finalizeTurn(active.state, active.turn, "interrupted", {
                threadDisposition: openCode2InterruptedThreadDisposition(event.data.reason),
              });
              return;
            }
            case "session.execution.failed": {
              const active = activeFor(event.data.sessionID);
              if (active === null) return;
              if (!openCode2ShouldSettleTurn("execution-terminal", active.turn.executionStarted)) {
                return;
              }
              const message = event.data.error.message;
              yield* updateProviderSession("error", message);
              yield* finalizeTurn(active.state, active.turn, "failed", {
                failure: makeProviderFailure({
                  message,
                  code: event.data.error.type,
                  class: "provider_error",
                }),
              });
              return;
            }
            // 2.x settles on `session.execution.*`; `session.idle` is only a
            // backstop for builds that never enter the authoritative lifecycle.
            case "session.idle": {
              const active = activeFor(event.data.sessionID);
              if (active === null) return;
              if (!openCode2ShouldSettleTurn("idle", active.turn.executionStarted)) return;
              yield* finalizeTurn(
                active.state,
                active.turn,
                active.turn.interrupted ? "interrupted" : "completed",
              );
              return;
            }
            case "session.error": {
              const activeSessionIDs = Array.from(threads.values())
                .filter((state) => state.activeTurn !== null && !state.activeTurn.finalized)
                .map((state) => state.nativeSessionId);
              const targetSessionIDs = openCode2SessionErrorTargetSessionIds(
                event.data.sessionID,
                activeSessionIDs,
              );
              const message = openCode2SessionErrorMessage(event.data);
              const isAbort = event.data.error?.name === "MessageAbortedError";
              if (!isAbort) yield* updateProviderSession("error", message);
              for (const sessionID of targetSessionIDs) {
                const active = activeFor(sessionID);
                if (active === null) continue;
                yield* finalizeTurn(
                  active.state,
                  active.turn,
                  openCode2SessionErrorStatus(event.data, active.turn.interrupted),
                  {
                    failure: makeProviderFailure({
                      message,
                      code: event.data.error?.name ?? null,
                      class: "provider_error",
                    }),
                    threadDisposition: event.data.sessionID === undefined ? "broken" : "reusable",
                  },
                );
              }
              // Finalizing one of several active turns temporarily marks the
              // shared provider session as running. Restore the unscoped
              // provider failure after every affected turn has closed.
              if (!isAbort && targetSessionIDs.length > 1) {
                yield* updateProviderSession("error", message);
              }
              return;
            }
            default:
              return;
          }
        });

        const subscription = yield* sdkCall("event.subscribe", {}, () =>
          client.v2.event.subscribe({ signal: abortController.signal }),
        );
        yield* Scope.addFinalizer(
          scope,
          Effect.sync(() => abortController.abort()),
        );
        yield* Stream.fromAsyncIterable(
          subscription.stream,
          (cause) =>
            new OpenCode2RuntimeError({
              operation: "event.subscribe",
              detail: openCodeRuntimeErrorDetail(cause),
              cause,
            }),
        ).pipe(
          Stream.runForEach(handleEvent),
          Effect.exit,
          Effect.flatMap((exit) =>
            abortController.signal.aborted || Exit.isSuccess(exit)
              ? Effect.void
              : failActiveTurns(
                  openCodeRuntimeErrorDetail(Cause.squash(exit.cause)),
                  "transport_error",
                ),
          ),
          Effect.forkIn(scope),
        );

        if (!connection.external && connection.exitCode !== null) {
          yield* connection.exitCode.pipe(
            Effect.flatMap((code) =>
              abortController.signal.aborted
                ? Effect.void
                : failActiveTurns(
                    `OpenCode 2 server exited unexpectedly (${code}).`,
                    "transport_error",
                  ),
            ),
            Effect.forkIn(scope),
          );
        }

        const registerThread = (
          nativeSession: SessionInfoV2,
          providerThread: OrchestrationV2ProviderThread,
        ): OpenCode2ThreadState => {
          const existing = threads.get(nativeSession.id);
          if (existing !== undefined) {
            existing.location = nativeSession.location;
            existing.providerThread = providerThread;
            return existing;
          }
          const state: OpenCode2ThreadState = {
            nativeSessionId: nativeSession.id,
            location: nativeSession.location,
            providerThread,
            appThread: null,
            activeTurn: null,
            boundModel:
              nativeSession.model === undefined
                ? null
                : `${nativeSession.model.providerID}/${nativeSession.model.id}`,
            boundAgent: nativeSession.agent ?? null,
            providerTurns: new Map(),
            messages: new Map(),
            runtimeRequests: new Map(),
          };
          threads.set(nativeSession.id, state);
          return state;
        };

        /**
         * 2.x binds the model and agent to the session, not to the prompt, so a
         * selection change between turns has to be pushed before the prompt.
         */
        const alignSessionSelection = Effect.fnUntraced(function* (
          state: OpenCode2ThreadState,
          modelSelection: ModelSelection,
        ) {
          const sessionID = state.nativeSessionId;
          const selection = openCode2SessionSelectionParameters(modelSelection);
          if (state.boundModel !== modelSelection.model) {
            const model = selection.model;
            yield* sdkCall("session.switchModel", { sessionID, model }, () =>
              client.v2.session.switchModel({ sessionID, model }),
            );
            state.boundModel = modelSelection.model;
          }
          const agent = selection.agent;
          if (agent !== undefined && state.boundAgent !== agent) {
            yield* sdkCall("session.switchAgent", { sessionID, agent }, () =>
              client.v2.session.switchAgent({ sessionID, agent }),
            );
            state.boundAgent = agent;
          }
        });

        const promptPayload = (message: ProviderAdapterV2TurnInput["message"]) => {
          const text = message.text.trim();
          const files = toOpenCode2FileAttachments({
            attachments: message.attachments,
            resolveAttachmentPath: (attachment) =>
              resolveAttachmentPath({ attachmentsDir: serverConfig.attachmentsDir, attachment }),
          });
          if (text.length === 0 && files.length === 0) {
            throw protocolError("OpenCode 2 turns require text or at least one valid attachment");
          }
          return {
            ...(text.length === 0 ? {} : { text }),
            ...(files.length === 0 ? {} : { files }),
          };
        };

        const readSnapshot = Effect.fnUntraced(function* (
          providerThread: OrchestrationV2ProviderThread,
        ) {
          const sessionID = nativeThreadId(providerThread);
          const response = yield* sdkCall("message.list", { sessionID }, () =>
            client.v2.message.list({ sessionID }),
          );
          const nativeMessages = unwrapOpenCode2Data<Array<SessionMessageInfo>>(
            "message.list",
            response,
          );
          const state = threads.get(sessionID);
          const snapshotNow = yield* DateTime.now;
          const messages: Array<OrchestrationV2ConversationMessage> = nativeMessages.flatMap(
            (info) => {
              const text =
                info.type === "user"
                  ? info.text
                  : info.type === "assistant"
                    ? info.content
                        .filter((entry) => entry.type === "text")
                        .map((entry) => entry.text)
                        .join("\n")
                    : "";
              if (text.trim().length === 0) return [];
              const createdAt = dateTimeFromEpoch(info.time.created, snapshotNow);
              return [
                {
                  createdBy: info.type === "user" ? ("user" as const) : ("agent" as const),
                  creationSource: "provider" as const,
                  id: idAllocator.derive.messageFromProviderItem({
                    driver: OPENCODE2_PROVIDER,
                    nativeItemId: info.id,
                  }),
                  threadId: providerThread.appThreadId ?? input.threadId,
                  runId: null,
                  nodeId: null,
                  role: info.type === "user" ? ("user" as const) : ("assistant" as const),
                  text,
                  attachments: [],
                  streaming: false,
                  createdAt,
                  updatedAt: createdAt,
                },
              ];
            },
          );
          const lastUser = nativeMessages.findLast((info) => info.type === "user")?.id;
          return {
            providerThread: {
              ...providerThread,
              providerSessionId: input.providerSessionId,
              nativeConversationHeadRef:
                lastUser === undefined ? null : providerRef(lastUser, "weak"),
              status: "idle" as const,
              updatedAt: snapshotNow,
            },
            providerTurns: state === undefined ? [] : [...state.providerTurns.values()],
            messages,
            runtimeRequests: state === undefined ? [] : [...state.runtimeRequests.values()],
            providerPayload: nativeMessages,
          };
        });

        const inspectPendingBackgroundWork = Effect.fnUntraced(function* (
          state: OpenCode2ThreadState,
        ) {
          const sessionID = state.nativeSessionId;
          return yield* openCode2PendingWorkForSession({
            sessionID,
            pending: sdkCall("session.pending.list", { sessionID }, () =>
              client.v2.session.pending.list({ sessionID }),
            ).pipe(
              Effect.map((response) =>
                unwrapOpenCode2Data<Array<SessionPendingInfo>>("session.pending.list", response),
              ),
            ),
            shells: sdkCall("shell.list", { location: state.location }, () =>
              client.v2.shell.list({ location: state.location }),
            ).pipe(
              Effect.map((response) =>
                unwrapOpenCode2Data<Array<ShellInfoV2>>("shell.list", response),
              ),
              Effect.tap((shells) =>
                Effect.sync(() => {
                  for (const shell of shells) {
                    if (shell.metadata.sessionID === sessionID) {
                      shellSessionIds.set(shell.id, sessionID);
                    }
                  }
                }),
              ),
            ),
          });
        });

        const hasPendingBackgroundWorkForState = (state: OpenCode2ThreadState) =>
          inspectPendingBackgroundWork(state).pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("Failed to inspect OpenCode 2 pending background work.", {
                errorTag: causeErrorTag(cause),
                provider: OPENCODE2_PROVIDER,
                providerThreadId: state.providerThread.id,
              }).pipe(Effect.as(false)),
            ),
          );

        const runtimeSession: ProviderAdapterV2SessionRuntime = {
          instanceId: options.instanceId,
          driver: OPENCODE2_PROVIDER,
          providerSessionId: input.providerSessionId,
          providerSession: sessionEntity,
          events: Stream.fromEffectRepeat(Queue.take(events)),
          hasPendingBackgroundWork: Effect.gen(function* () {
            for (const state of threads.values()) {
              if (yield* hasPendingBackgroundWorkForState(state)) return true;
            }
            return false;
          }),
          hasPendingBackgroundWorkForThread: (providerThread) =>
            Effect.gen(function* () {
              const sessionID = nativeThreadId(providerThread);
              const state = threads.get(sessionID);
              if (state === undefined) {
                return yield* protocolError(
                  `OpenCode 2 session ${sessionID} is not registered for pending-work inspection`,
                );
              }
              return yield* inspectPendingBackgroundWork(state);
            }).pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("Failed to inspect OpenCode 2 pending background work.", {
                  errorTag: causeErrorTag(cause),
                  provider: OPENCODE2_PROVIDER,
                  providerThreadId: providerThread.id,
                }).pipe(Effect.as(false)),
              ),
            ),
          ensureThread: (threadInput) =>
            Effect.gen(function* () {
              if (threadInput.existingProviderThread !== undefined) {
                return yield* runtimeSession.resumeThread({
                  providerThread: threadInput.existingProviderThread,
                });
              }
              const selection = openCode2SessionSelectionParameters(threadInput.modelSelection);
              const agent = selection.agent;
              const parameters = {
                ...selection,
                location: { directory: threadInput.runtimePolicy.cwd ?? cwd },
              };
              const response = yield* sdkCall("session.create", parameters, () =>
                client.v2.session.create(parameters),
              );
              const nativeSession = unwrapOpenCode2Data<SessionInfoV2>("session.create", response);
              const createdAt = yield* DateTime.now;
              const providerThread = makeProviderThread({
                idAllocator,
                providerInstanceId: options.instanceId,
                providerSessionId: input.providerSessionId,
                appThreadId: threadInput.threadId,
                nativeSession,
                now: createdAt,
              });
              const state = registerThread(nativeSession, providerThread);
              state.boundModel = threadInput.modelSelection.model;
              if (agent !== undefined) state.boundAgent = agent;
              return providerThread;
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterEnsureThreadError({
                    driver: OPENCODE2_PROVIDER,
                    threadId: threadInput.threadId,
                    cause,
                  }),
              ),
            ),
          resumeThread: (threadInput) =>
            Effect.gen(function* () {
              const sessionID = nativeThreadId(threadInput.providerThread);
              const response = yield* sdkCall("session.get", { sessionID }, () =>
                client.v2.session.get({ sessionID }),
              );
              const nativeSession = unwrapOpenCode2Data<SessionInfoV2>("session.get", response);
              const resumedAt = yield* DateTime.now;
              const providerThread = {
                ...threadInput.providerThread,
                providerSessionId: input.providerSessionId,
                status: "idle" as const,
                updatedAt: dateTimeFromEpoch(nativeSession.time.updated, resumedAt),
              };
              registerThread(nativeSession, providerThread);
              return providerThread;
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterResumeThreadError({
                    driver: OPENCODE2_PROVIDER,
                    providerSessionId: input.providerSessionId,
                    providerThreadId: threadInput.providerThread.id,
                    cause,
                  }),
              ),
            ),
          startTurn: (turnInput) =>
            Effect.gen(function* () {
              const sessionID = nativeThreadId(turnInput.providerThread);
              const state = threads.get(sessionID);
              if (state === undefined) {
                return yield* protocolError(`OpenCode 2 session ${sessionID} is not registered`);
              }
              if (state.activeTurn !== null) {
                return yield* protocolError(
                  `OpenCode 2 provider thread ${turnInput.providerThread.id} already has an active turn`,
                );
              }
              const payload = promptPayload(turnInput.message);
              const startedAt = yield* DateTime.now;
              const syntheticNativeTurnId = `${sessionID}:attempt:${turnInput.attemptId}`;
              const providerTurnId = idAllocator.derive.providerTurn({
                driver: OPENCODE2_PROVIDER,
                nativeTurnId: syntheticNativeTurnId,
              });
              const providerTurn: OrchestrationV2ProviderTurn = {
                id: providerTurnId,
                providerThreadId: turnInput.providerThread.id,
                nodeId: turnInput.rootNodeId,
                runAttemptId: turnInput.attemptId,
                nativeTurnRef: providerRef(syntheticNativeTurnId, "weak"),
                ordinal: turnInput.providerTurnOrdinal,
                status: "running",
                startedAt,
                completedAt: null,
              };
              const turn: ActiveOpenCode2Turn = {
                threadId: turnInput.threadId,
                runId: turnInput.runId,
                rootNodeId: turnInput.rootNodeId,
                appThread: turnInput.appThread,
                modelSelection: turnInput.modelSelection,
                providerTurnId,
                runOrdinal: turnInput.runOrdinal,
                startedAt,
                itemOrdinals: new Map(),
                parts: new Map(),
                toolIdsByCallId: new Map(),
                providerTurn,
                nextItemOrdinal: turnInput.providerTurnOrdinal * 100 + 1,
                nativeInputId: null,
                executionStarted: false,
                interrupted: false,
                finalized: false,
              };
              state.appThread = turnInput.appThread;
              state.activeTurn = turn;
              state.providerTurns.set(String(providerTurnId), providerTurn);
              yield* emitProviderTurn(state, turn, "running", null);
              yield* updateProviderThread(state, {
                status: "active",
                firstRunOrdinal: state.providerThread.firstRunOrdinal ?? turnInput.runOrdinal,
                lastRunOrdinal: turnInput.runOrdinal,
              });
              yield* updateProviderSession("running", null);
              yield* alignSessionSelection(state, turnInput.modelSelection);
              const prompted = yield* sdkCall("session.prompt", { sessionID, ...payload }, () =>
                client.v2.session.prompt({ sessionID, ...payload }),
              ).pipe(
                Effect.tapError((cause) =>
                  finalizeTurn(state, turn, "failed", {
                    failure: makeProviderFailure({ cause, class: "provider_error" }),
                  }),
                ),
              );
              // The admitted input id is the closest native turn correlation
              // point 2.x offers, and it arrives on the prompt response before
              // `session.input.admitted` reaches the event stream.
              const admittedId = recordString(prompted.data?.data, "id");
              if (admittedId !== undefined && turn.nativeInputId === null) {
                turn.nativeInputId = admittedId;
                yield* emitProviderTurn(state, turn, "running", null);
              }
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterTurnStartError({
                    driver: OPENCODE2_PROVIDER,
                    threadId: turnInput.threadId,
                    providerThreadId: turnInput.providerThread.id,
                    runId: turnInput.runId,
                    cause,
                  }),
              ),
            ),
          steerTurn: (steerInput) =>
            Effect.gen(function* () {
              const sessionID = nativeThreadId(steerInput.providerThread);
              const state = threads.get(sessionID);
              const turn = state?.activeTurn;
              if (
                state === undefined ||
                turn === undefined ||
                turn === null ||
                turn.providerTurnId !== steerInput.providerTurnId
              ) {
                return yield* protocolError(
                  `OpenCode 2 turn ${steerInput.providerTurnId} is not active`,
                );
              }
              const payload = promptPayload(steerInput.message);
              yield* sdkCall("session.prompt", { sessionID, ...payload, delivery: "steer" }, () =>
                client.v2.session.prompt({ sessionID, ...payload, delivery: "steer" }),
              );
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterSteerRunError({
                    driver: OPENCODE2_PROVIDER,
                    providerThreadId: steerInput.providerThread.id,
                    providerTurnId: steerInput.providerTurnId,
                    cause,
                  }),
              ),
            ),
          interruptTurn: (interruptInput) =>
            Effect.gen(function* () {
              const sessionID = nativeThreadId(interruptInput.providerThread);
              const state = threads.get(sessionID);
              const turn = state?.activeTurn;
              if (
                turn === undefined ||
                turn === null ||
                turn.providerTurnId !== interruptInput.providerTurnId
              ) {
                return yield* protocolError(
                  `OpenCode 2 turn ${interruptInput.providerTurnId} is not active`,
                );
              }
              turn.interrupted = true;
              yield* sdkCall("session.interrupt", { sessionID }, () =>
                client.v2.session.interrupt({ sessionID }),
              ).pipe(Effect.tapError(() => Effect.sync(() => (turn.interrupted = false))));
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterInterruptError({
                    driver: OPENCODE2_PROVIDER,
                    providerThreadId: interruptInput.providerThread.id,
                    providerTurnId: interruptInput.providerTurnId,
                    cause,
                  }),
              ),
            ),
          respondToRuntimeRequest: (requestInput) =>
            Effect.gen(function* () {
              const pending = pendingRequests.get(String(requestInput.requestId));
              if (pending === undefined) {
                return yield* protocolError(
                  `No pending OpenCode 2 request ${requestInput.requestId}`,
                );
              }
              const sessionID = pending.state.nativeSessionId;
              const requestID = pending.nativeRequestId;
              if (pending.questions !== undefined) {
                if (requestInput.answers === undefined) {
                  return yield* protocolError(
                    `OpenCode 2 question request ${requestInput.requestId} requires answers`,
                  );
                }
                const answers = pending.questions.map((question, index) => {
                  const raw =
                    requestInput.answers?.[openCode2QuestionId(index, question.header)] ??
                    requestInput.answers?.[question.header] ??
                    requestInput.answers?.[question.question];
                  if (Array.isArray(raw)) {
                    return raw.filter((value): value is string => typeof value === "string");
                  }
                  if (typeof raw === "string") return raw.trim().length > 0 ? [raw] : [];
                  return [];
                });
                yield* sdkCall("session.question.reply", { sessionID, requestID, answers }, () =>
                  client.v2.session.question.reply({
                    sessionID,
                    requestID,
                    questionV2Reply: { answers },
                  }),
                );
                return;
              }
              if (requestInput.decision === undefined) {
                return yield* protocolError(
                  `OpenCode 2 approval request ${requestInput.requestId} requires a decision`,
                );
              }
              const reply =
                requestInput.decision === "accept"
                  ? ("once" as const)
                  : requestInput.decision === "acceptForSession"
                    ? ("always" as const)
                    : ("reject" as const);
              yield* sdkCall("session.permission.reply", { sessionID, requestID, reply }, () =>
                client.v2.session.permission.reply({ sessionID, requestID, reply }),
              );
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterRuntimeRequestResponseError({
                    driver: OPENCODE2_PROVIDER,
                    requestId: requestInput.requestId,
                    cause,
                  }),
              ),
            ),
          readThreadSnapshot: (snapshotInput) =>
            readSnapshot(snapshotInput.providerThread).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterReadThreadSnapshotError({
                    driver: OPENCODE2_PROVIDER,
                    providerThreadId: snapshotInput.providerThread.id,
                    cause,
                  }),
              ),
            ),
          rollbackThread: (rollbackInput) =>
            Effect.gen(function* () {
              const sessionID = nativeThreadId(rollbackInput.providerThread);
              const state = threads.get(sessionID);
              if (state?.activeTurn !== null && state?.activeTurn !== undefined) {
                return yield* protocolError(
                  `Cannot roll back OpenCode 2 thread ${rollbackInput.providerThread.id} while a turn is active`,
                );
              }
              const response = yield* sdkCall("message.list", { sessionID }, () =>
                client.v2.message.list({ sessionID }),
              );
              const nativeMessages = unwrapOpenCode2Data<Array<SessionMessageInfo>>(
                "message.list",
                response,
              );
              let boundaryMessageId: string | undefined;
              if (rollbackInput.target.type === "thread_start") {
                boundaryMessageId = nativeMessages.find((info) => info.type === "user")?.id;
              } else {
                boundaryMessageId = openCodeBoundaryAfterProviderTurn(
                  rollbackInput.providerThreadTurns,
                  rollbackInput.target.providerTurn.id,
                );
              }
              if (boundaryMessageId !== undefined) {
                // Stage then commit: 2.x split 1.x's single `session.revert`
                // into a reversible boundary plus an explicit commit.
                yield* sdkCall(
                  "session.revert.stage",
                  { sessionID, messageID: boundaryMessageId, files: true },
                  () =>
                    client.v2.session.revert.stage({
                      sessionID,
                      messageID: boundaryMessageId!,
                      files: true,
                    }),
                );
                yield* sdkCall("session.revert.commit", { sessionID }, () =>
                  client.v2.session.revert.commit({ sessionID }),
                );
              }
              const snapshot = yield* readSnapshot(rollbackInput.providerThread);
              return {
                ...snapshot,
                providerThread: {
                  ...snapshot.providerThread,
                  nativeConversationHeadRef:
                    rollbackInput.target.type === "provider_turn"
                      ? rollbackInput.target.providerTurn.nativeTurnRef
                      : null,
                },
              };
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterRollbackThreadError({
                    driver: OPENCODE2_PROVIDER,
                    providerThreadId: rollbackInput.providerThread.id,
                    checkpointId: rollbackInput.target.checkpointId,
                    cause,
                  }),
              ),
            ),
          forkThread: (forkInput) =>
            Effect.gen(function* () {
              const sessionID = nativeThreadId(forkInput.sourceProviderThread);
              const sourceState = threads.get(sessionID);
              if (sourceState?.activeTurn !== null && sourceState?.activeTurn !== undefined) {
                return yield* protocolError(
                  `Cannot fork OpenCode 2 thread ${forkInput.sourceProviderThread.id} while a turn is active`,
                );
              }
              let boundaryMessageId: string | undefined;
              if (forkInput.providerTurnId !== undefined) {
                const sourceTurns = forkInput.sourceProviderTurns ?? [];
                const selected = sourceTurns.find((turn) => turn.id === forkInput.providerTurnId);
                if (selected === undefined) {
                  return yield* protocolError(
                    `OpenCode 2 fork boundary turn ${forkInput.providerTurnId} was not found`,
                  );
                }
                boundaryMessageId = openCodeBoundaryAfterProviderTurn(sourceTurns, selected.id);
              }
              const parameters = {
                sessionID,
                ...(boundaryMessageId === undefined ? {} : { messageID: boundaryMessageId }),
              };
              const response = yield* sdkCall("session.fork", parameters, () =>
                client.v2.session.fork(parameters),
              );
              const nativeSession = unwrapOpenCode2Data<SessionInfoV2>("session.fork", response);
              const forkedAt = yield* DateTime.now;
              const providerThread = makeProviderThread({
                idAllocator,
                providerInstanceId: options.instanceId,
                providerSessionId: input.providerSessionId,
                appThreadId: forkInput.targetThreadId,
                ...(forkInput.ownerNodeId === undefined
                  ? {}
                  : { ownerNodeId: forkInput.ownerNodeId }),
                nativeSession,
                forkedFrom: {
                  providerThreadId: forkInput.sourceProviderThread.id,
                  ...(forkInput.providerTurnId === undefined
                    ? {}
                    : { providerTurnId: forkInput.providerTurnId }),
                },
                now: forkedAt,
              });
              registerThread(nativeSession, providerThread);
              return providerThread;
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterForkThreadError({
                    driver: OPENCODE2_PROVIDER,
                    providerThreadId: forkInput.sourceProviderThread.id,
                    cause,
                  }),
              ),
            ),
        };

        return runtimeSession;
      },
      (effect, input) =>
        effect.pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapterOpenSessionError({
                driver: OPENCODE2_PROVIDER,
                providerSessionId: input.providerSessionId,
                cause,
              }),
          ),
        ),
    ),
  });
}

export type OpenCode2AdapterV2DriverEnv =
  | OpenCode2Runtime
  | IdAllocatorV2
  | ProviderEventLoggers
  | ServerConfig;

export const OpenCode2AdapterV2Driver: ProviderAdapterDriver<
  OpenCode2Settings,
  OpenCode2AdapterV2DriverEnv
> = {
  driverKind: OPENCODE2_DRIVER_KIND,
  configSchema: OpenCode2SettingsSchema,
  defaultConfig: (): OpenCode2Settings => DEFAULT_OPENCODE2_SETTINGS,
  create: Effect.fn("OpenCode2AdapterV2Driver.create")(
    function* (input: ProviderAdapterDriverCreateInput<OpenCode2Settings>) {
      const hostEnvironment = yield* HostProcessEnvironment;
      const openCode2Runtime = yield* OpenCode2Runtime;
      const idAllocator = yield* IdAllocatorV2;
      const providerEventLoggers = yield* ProviderEventLoggers;
      const serverConfig = yield* ServerConfig;
      return makeOpenCode2AdapterV2({
        instanceId: input.instanceId,
        settings: { ...input.config, enabled: input.enabled },
        environment: mergeProviderInstanceEnvironment(input.environment, hostEnvironment),
        runtime: openCode2Runtime,
        idAllocator,
        serverConfig,
        ...(providerEventLoggers.native === undefined
          ? {}
          : { nativeEventLogger: providerEventLoggers.native }),
      });
    },
    (effect, input) =>
      effect.pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterDriverCreateError({
              driver: OPENCODE2_DRIVER_KIND,
              instanceId: input.instanceId,
              detail: "Failed to create OpenCode 2 v2 adapter.",
              cause,
            }),
        ),
      ),
  ),
};
