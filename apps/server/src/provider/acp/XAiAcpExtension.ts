import type { ProviderUserInputAnswers, UserInputQuestion } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import type * as EffectAcpSchema from "effect-acp/schema";

import type * as AcpSessionRuntime from "./AcpSessionRuntime.ts";
import type { AcpToolCallState } from "./AcpRuntimeModel.ts";

const xAiStopReasonMissingMetaKey = "xAiStopReasonMissing";
const completedXAiPromptIdLimit = 128;

const XAiPromptCompleteNotification = Schema.Struct({
  sessionId: Schema.String,
  promptId: Schema.optional(Schema.String),
  stopReason: Schema.optional(Schema.String),
  agentResult: Schema.optional(Schema.NullOr(Schema.Unknown)),
});

type XAiPromptCompleteNotification = typeof XAiPromptCompleteNotification.Type;

/**
 * Live Grok CLI (0.2.x) emits root turn completion as an extension session
 * update, not as top-level `_x.ai/session/prompt_complete`:
 * `{ method: "_x.ai/session/update", params: { sessionId, update: {
 *   sessionUpdate: "turn_completed", prompt_id, stop_reason } } }`.
 */
const XAiSessionUpdateNotification = Schema.Struct({
  sessionId: Schema.String,
  update: Schema.Struct({
    sessionUpdate: Schema.String,
    prompt_id: Schema.optional(Schema.String),
    promptId: Schema.optional(Schema.String),
    stop_reason: Schema.optional(Schema.String),
    stopReason: Schema.optional(Schema.String),
  }),
  _meta: Schema.optional(Schema.Unknown),
});

type XAiSessionUpdateNotification = typeof XAiSessionUpdateNotification.Type;

const XAI_TASK_COMPLETED_PROMPT_ID_PREFIX = "task-completed-";

/**
 * Map a Grok `_x.ai/session/update` payload to a prompt-complete shape when it
 * is a root `turn_completed` for a real prompt id. Returns null for hooks,
 * background task completions, and other update kinds.
 */
export function xAiPromptCompleteFromSessionUpdate(
  notification: XAiSessionUpdateNotification,
): XAiPromptCompleteNotification | null {
  const update = notification.update;
  if (update.sessionUpdate !== "turn_completed") {
    return null;
  }
  const promptId = nonEmptyString(update.prompt_id) ?? nonEmptyString(update.promptId);
  // Require prompt id so we never session-fallback match a background task
  // completion (`task-completed-*`) or an unrelated CLI turn.
  if (promptId === undefined || promptId.startsWith(XAI_TASK_COMPLETED_PROMPT_ID_PREFIX)) {
    return null;
  }
  const stopReason = nonEmptyString(update.stop_reason) ?? nonEmptyString(update.stopReason);
  return {
    sessionId: notification.sessionId,
    promptId,
    ...(stopReason === undefined ? {} : { stopReason }),
  };
}

interface PendingXAiPromptCompletion {
  readonly sessionId: string;
  readonly promptId: string;
  readonly deferred: Deferred.Deferred<EffectAcpSchema.PromptResponse>;
}

export interface XAiAcpSubagentUpdate {
  readonly nativeTaskId: string;
  readonly prompt: string;
  readonly title: string | null;
  readonly model: string | null;
  readonly status: "running" | "completed" | "failed";
  readonly childSessionId: string | null;
  readonly result: string | null;
  /**
   * When false, the ACP adapter also projects a normal tool turn item for this
   * tool call (used for get_command_or_subagent_output hydration). Default true.
   */
  readonly suppressNormalTool?: boolean;
}

export interface XAiAcpBackgroundToolMutation {
  readonly taskId: string;
  readonly status: "running" | "completed" | "failed";
  readonly appendOutput: string;
}

const XAI_UUID_RE = "[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}";

function unknownRecord(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function nonEmptyString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;
}

function titleKey(toolCall: AcpToolCallState): string {
  return (toolCall.title ?? "").trim().toLowerCase();
}

function decodeByteText(value: unknown): string | undefined {
  if (typeof value === "string") return nonEmptyString(value);
  if (!Array.isArray(value)) return undefined;
  if (value.length === 0) return undefined;
  if (!value.every((entry) => typeof entry === "number")) return undefined;
  try {
    return nonEmptyString(new TextDecoder().decode(Uint8Array.from(value as number[])));
  } catch {
    return undefined;
  }
}

function xAiToolOutputText(toolCall: AcpToolCallState): string | undefined {
  const rawOutput = unknownRecord(toolCall.data.rawOutput);
  if (rawOutput !== undefined) {
    const direct =
      nonEmptyString(rawOutput.text) ??
      nonEmptyString(rawOutput.output_for_prompt) ??
      decodeByteText(rawOutput.output);
    if (direct !== undefined) return direct;
    const result = unknownRecord(rawOutput.Result) ?? unknownRecord(rawOutput.result);
    if (result !== undefined) {
      const nested =
        nonEmptyString(result.output) ??
        nonEmptyString(result.text) ??
        decodeByteText(result.output);
      if (nested !== undefined) return nested;
    }
  }
  if (typeof toolCall.data.rawOutput === "string") {
    return nonEmptyString(toolCall.data.rawOutput);
  }
  const content = toolCall.data.content;
  if (!Array.isArray(content)) return undefined;
  const text = content
    .flatMap((entry) => {
      const record = unknownRecord(entry);
      const nested = unknownRecord(record?.content);
      return nonEmptyString(nested?.text) ?? nonEmptyString(record?.text) ?? [];
    })
    .join("\n")
    .trim();
  return text.length > 0 ? text : undefined;
}

function firstUuidMatch(text: string, pattern: RegExp): string | undefined {
  const match = pattern.exec(text);
  return match?.[1];
}

function extractXAiChildSessionId(output: string | undefined): string | null {
  if (output === undefined) return null;
  return (
    firstUuidMatch(output, new RegExp(`(?:^|\\n)\\s*Agent ID:\\s*(${XAI_UUID_RE})\\b`, "i")) ??
    firstUuidMatch(output, new RegExp(`(?:^|\\n)\\s*subagent_id:\\s*(${XAI_UUID_RE})\\b`, "i")) ??
    firstUuidMatch(output, new RegExp(`===\\s*Task\\s+(${XAI_UUID_RE})\\s*===`, "i")) ??
    null
  );
}

function isXAiAsyncSpawnAck(output: string | undefined): boolean {
  if (output === undefined) return false;
  if (/subagent started in background/i.test(output)) return true;
  return (
    new RegExp(`subagent_id:\\s*${XAI_UUID_RE}`, "i").test(output) &&
    /get_command_or_subagent_output/i.test(output)
  );
}

function isXAiMonitorStartAck(output: string | undefined): boolean {
  if (output === undefined) return false;
  return /monitor started\s*\(\s*task\s+/i.test(output);
}

function isXAiSpawnOrTaskTool(
  toolCall: AcpToolCallState,
  rawInput: Record<string, unknown> | undefined,
): boolean {
  const title = titleKey(toolCall);
  if (title === "task" || title === "spawn_subagent" || title.includes("spawn subagent")) {
    return true;
  }
  const variant = nonEmptyString(rawInput?.variant)?.toLowerCase();
  if (variant === "cursortask" || variant === "task" || variant === "spawn_subagent") {
    return true;
  }
  if (nonEmptyString(rawInput?.subagent_type) !== undefined) return true;
  if (nonEmptyString(rawInput?.subagentType) !== undefined) return true;
  return false;
}

function isXAiGetSubagentOutputTool(
  toolCall: AcpToolCallState,
  rawInput: Record<string, unknown> | undefined,
): boolean {
  const title = titleKey(toolCall);
  if (
    title === "get_command_or_subagent_output" ||
    title.includes("get_command_or_subagent_output") ||
    title === "wait_commands_or_subagents"
  ) {
    return true;
  }
  const variant = nonEmptyString(rawInput?.variant)?.toLowerCase();
  if (variant === "taskoutput" || variant === "task_output") return true;
  const rawOutput = unknownRecord(toolCall.data.rawOutput);
  const outputType = nonEmptyString(rawOutput?.type)?.toLowerCase();
  if (outputType === "taskoutput" || outputType === "task_output") return true;
  // ACP often titles this tool with the finished subagent label.
  if (title.startsWith("[subagent:") && Array.isArray(rawInput?.task_ids)) return true;
  return false;
}

export function isXAiMonitorTool(toolCall: AcpToolCallState): boolean {
  const title = titleKey(toolCall);
  if (title === "monitor" || title.startsWith("monitor ")) return true;
  const rawInput = unknownRecord(toolCall.data.rawInput);
  const variant = nonEmptyString(rawInput?.variant)?.toLowerCase();
  return variant === "monitor";
}

export function extractXAiMonitorTaskId(toolCall: AcpToolCallState): string | undefined {
  if (!isXAiMonitorTool(toolCall)) return undefined;
  const rawOutput = unknownRecord(toolCall.data.rawOutput);
  // Live ACP start ACK: { type: "Monitor", taskId, timeoutMs, persistent }
  if (rawOutput !== undefined && nonEmptyString(rawOutput.type)?.toLowerCase() === "monitor") {
    const structured = nonEmptyString(rawOutput.taskId) ?? nonEmptyString(rawOutput.task_id);
    if (structured !== undefined) return structured;
  }
  const output = xAiToolOutputText(toolCall);
  if (output !== undefined) {
    const fromAck = firstUuidMatch(
      output,
      new RegExp(`monitor started\\s*\\(\\s*task\\s+(${XAI_UUID_RE})\\b`, "i"),
    );
    if (fromAck !== undefined) return fromAck;
  }
  const rawInput = unknownRecord(toolCall.data.rawInput);
  return (
    nonEmptyString(rawInput?.task_id) ??
    nonEmptyString(rawInput?.taskId) ??
    (Array.isArray(rawInput?.task_ids) ? nonEmptyString(rawInput.task_ids[0]) : undefined)
  );
}

/**
 * When get_command_or_subagent_output / TaskOutput completes a registered monitor
 * task, return hydration for that background tool id.
 */
export function extractXAiBackgroundTaskCompletion(toolCall: AcpToolCallState):
  | {
      readonly taskId: string;
      readonly status: "running" | "completed" | "failed";
      readonly appendOutput: string;
    }
  | undefined {
  const rawInput = unknownRecord(toolCall.data.rawInput);
  if (!isXAiGetSubagentOutputTool(toolCall, rawInput)) return undefined;
  const rawOutput = unknownRecord(toolCall.data.rawOutput);
  const result = unknownRecord(rawOutput?.Result) ?? unknownRecord(rawOutput?.result);
  const taskId =
    nonEmptyString(result?.task_id) ??
    nonEmptyString(result?.taskId) ??
    (Array.isArray(rawInput?.task_ids) ? nonEmptyString(rawInput.task_ids[0]) : undefined) ??
    nonEmptyString(rawInput?.task_id);
  if (taskId === undefined) return undefined;
  // Caller matches taskId against registered background tools (monitors).
  // Subagent hydration stays on extractXAiAcpSubagentUpdate.
  const text = xAiToolOutputText(toolCall);
  const status = statusFromGetOutputTool(toolCall, text);
  const appendOutput = resultFromGetOutputTool(toolCall, text) ?? "";
  return { taskId, status, appendOutput };
}

/**
 * Grok ACP often sends `title: "Tool"` even when rawInput has a useful
 * description or variant (especially Monitor). Prefer description/variant so
 * the timeline matches Claude-style tool names rather than a generic label.
 */
export function isGenericAcpToolTitle(title: string | undefined): boolean {
  const key = (title ?? "").trim().toLowerCase();
  return key.length === 0 || key === "tool" || key === "tool call" || key === "terminal";
}

export function resolveXAiAcpToolTitle(toolCall: AcpToolCallState): string | undefined {
  if (!isGenericAcpToolTitle(toolCall.title)) {
    return nonEmptyString(toolCall.title);
  }
  const rawInput = unknownRecord(toolCall.data.rawInput);
  const description = nonEmptyString(rawInput?.description);
  const variant = nonEmptyString(rawInput?.variant)?.toLowerCase();
  if (description !== undefined) {
    if (variant === "monitor" && !description.toLowerCase().startsWith("monitor")) {
      return `Monitor: ${description}`;
    }
    return description;
  }
  if (variant === "monitor") return "Monitor";
  if (variant === "task" || variant === "cursortask" || variant === "spawn_subagent") {
    return "Task";
  }
  if (variant === "taskoutput" || variant === "task_output") return "Task output";
  if (variant !== undefined) {
    return variant;
  }
  const command = nonEmptyString(rawInput?.command);
  if (command !== undefined) {
    return command.length > 80 ? `${command.slice(0, 77)}...` : command;
  }
  return nonEmptyString(toolCall.title);
}

/**
 * Normalize Grok ACP tool presentation and monitor lifecycle:
 * - replace generic titles ("Tool") with description / variant labels
 * - structured Monitor start ACK stays running
 * - text start ACK stays running
 * - structured Bash results with exit_code are terminal
 */
export function normalizeXAiAcpToolCallState(toolCall: AcpToolCallState): AcpToolCallState {
  const resolvedTitle = resolveXAiAcpToolTitle(toolCall);
  const withTitle =
    resolvedTitle !== undefined && resolvedTitle !== toolCall.title
      ? { ...toolCall, title: resolvedTitle }
      : toolCall;

  if (!isXAiMonitorTool(withTitle)) {
    return withTitle;
  }
  const rawOutput = unknownRecord(withTitle.data.rawOutput);
  if (rawOutput !== undefined) {
    const outputType = nonEmptyString(rawOutput.type)?.toLowerCase();
    if (outputType === "monitor") {
      // Start registration only; process still running in the background.
      return { ...withTitle, status: "inProgress" };
    }
    if (outputType === "bash" && typeof rawOutput.exit_code === "number") {
      return {
        ...withTitle,
        status: rawOutput.exit_code === 0 ? "completed" : "failed",
      };
    }
  }
  const output = xAiToolOutputText(withTitle);
  if (withTitle.status === "completed" && isXAiMonitorStartAck(output)) {
    return { ...withTitle, status: "inProgress" };
  }
  return withTitle;
}

/**
 * Parse Grok synthetic monitor traffic that arrives as root text chunks
 * (`<monitor-event>` lines and "Monitor ... ended" reminders).
 */
export function extractXAiAcpBackgroundToolMutation(
  text: string,
): XAiAcpBackgroundToolMutation | undefined {
  const eventMatch = text.match(
    new RegExp(
      `<monitor-event\\s+task_id=["']?(${XAI_UUID_RE})["']?\\s*>\\s*([\\s\\S]*?)\\s*</monitor-event>`,
      "i",
    ),
  );
  if (eventMatch?.[1] !== undefined) {
    const line = (eventMatch[2] ?? "").trim();
    return {
      taskId: eventMatch[1],
      status: "running",
      appendOutput: line.length > 0 ? `${line}\n` : "",
    };
  }

  const endedMatch = text.match(new RegExp(`Monitor\\s+["']?(${XAI_UUID_RE})["']?\\s+ended`, "i"));
  if (endedMatch?.[1] !== undefined) {
    const summary = text.replace(/<\/?system-reminder>/gi, "").trim();
    return {
      taskId: endedMatch[1],
      status:
        /exited\s*\(\s*code\s*0\s*\)/i.test(text) || /ended cleanly/i.test(text)
          ? "completed"
          : /exited|failed|error|signal/i.test(text)
            ? "failed"
            : "completed",
      appendOutput: summary.length > 0 ? `${summary}\n` : "Monitor ended.\n",
    };
  }

  return undefined;
}

function taskIdsFromGetOutputTool(
  toolCall: AcpToolCallState,
  rawInput: Record<string, unknown> | undefined,
): ReadonlyArray<string> {
  const ids: string[] = [];
  const push = (value: unknown) => {
    const text = nonEmptyString(value);
    if (text !== undefined && new RegExp(`^${XAI_UUID_RE}$`, "i").test(text)) {
      ids.push(text);
    }
  };
  if (Array.isArray(rawInput?.task_ids)) {
    for (const entry of rawInput.task_ids) push(entry);
  }
  push(rawInput?.task_id);
  push(rawInput?.taskId);
  const rawOutput = unknownRecord(toolCall.data.rawOutput);
  const result = unknownRecord(rawOutput?.Result) ?? unknownRecord(rawOutput?.result);
  push(result?.task_id);
  push(result?.taskId);
  const output = xAiToolOutputText(toolCall);
  if (output !== undefined) {
    const taskHeader = firstUuidMatch(
      output,
      new RegExp(`===\\s*Task\\s+(${XAI_UUID_RE})\\s*===`, "i"),
    );
    if (taskHeader !== undefined) ids.push(taskHeader);
    const metaId = firstUuidMatch(output, new RegExp(`subagent_id:\\s*(${XAI_UUID_RE})\\b`, "i"));
    if (metaId !== undefined) ids.push(metaId);
  }
  return [...new Set(ids)];
}

function resultFromGetOutputTool(
  toolCall: AcpToolCallState,
  output: string | undefined,
): string | null {
  const rawOutput = unknownRecord(toolCall.data.rawOutput);
  const result = unknownRecord(rawOutput?.Result) ?? unknownRecord(rawOutput?.result);
  const structured =
    nonEmptyString(result?.output) ??
    nonEmptyString(result?.text) ??
    decodeByteText(result?.output);
  if (structured !== undefined) {
    // Drop trailing machine meta blocks when present.
    const cleaned = structured
      .replace(/<subagent_meta>[\s\S]*?<\/subagent_meta>/gi, "")
      .replace(/<subagent_result>[\s\S]*?<\/subagent_result>/gi, "")
      .trim();
    if (cleaned.length > 0) return cleaned;
    return structured;
  }
  if (output === undefined) return null;
  const marker = output.match(/=== Output ===\s*([\s\S]*)$/i);
  const body = (marker?.[1] ?? output).trim();
  return body.length > 0 ? body : null;
}

function statusFromGetOutputTool(
  toolCall: AcpToolCallState,
  output: string | undefined,
): "running" | "completed" | "failed" {
  if (toolCall.status === "failed") return "failed";
  const rawOutput = unknownRecord(toolCall.data.rawOutput);
  const result = unknownRecord(rawOutput?.Result) ?? unknownRecord(rawOutput?.result);
  const structuredStatus = nonEmptyString(result?.status)?.toLowerCase();
  if (structuredStatus === "completed" || structuredStatus === "success") return "completed";
  if (structuredStatus === "failed" || structuredStatus === "error") return "failed";
  if (structuredStatus === "running" || structuredStatus === "pending") return "running";
  if (typeof result?.exit_code === "number") {
    return result.exit_code === 0 ? "completed" : "failed";
  }
  if (output !== undefined) {
    if (/Status:\s*failed/i.test(output) || /Exit Code:\s*(?!0)\d+/i.test(output)) {
      return "failed";
    }
    if (/Status:\s*completed/i.test(output) || /Status:\s*success/i.test(output)) {
      return "completed";
    }
    if (/Status:\s*running/i.test(output) || /Status:\s*pending/i.test(output)) {
      return "running";
    }
  }
  return toolCall.status === "completed" ? "completed" : "running";
}

/**
 * Recognizes Grok Task / spawn_subagent envelopes and get_command hydration
 * without teaching the generic ACP adapter about xAI tool names.
 *
 * Current Grok CLI returns spawn_subagent immediately with a background ACK
 * (`subagent_id: ...`). That must stay `running` until child work finishes or
 * get_command_or_subagent_output reports a terminal status.
 */
export function extractXAiAcpSubagentUpdate(
  toolCall: AcpToolCallState,
): XAiAcpSubagentUpdate | undefined {
  const rawInput = unknownRecord(toolCall.data.rawInput);
  const output = xAiToolOutputText(toolCall);

  if (isXAiGetSubagentOutputTool(toolCall, rawInput)) {
    const taskIds = taskIdsFromGetOutputTool(toolCall, rawInput);
    const childSessionId = taskIds[0] ?? extractXAiChildSessionId(output);
    if (childSessionId === null) return undefined;
    // Prefer the durable subagent row as the completion surface (Claude/Codex
    // style). Suppress the noisy TaskOutput tool card; monitor hydration is
    // handled separately via extractBackgroundTaskCompletion.
    return {
      nativeTaskId: toolCall.toolCallId,
      prompt: "",
      title: null,
      model: null,
      status: statusFromGetOutputTool(toolCall, output),
      childSessionId,
      result: resultFromGetOutputTool(toolCall, output),
      suppressNormalTool: true,
    };
  }

  if (!isXAiSpawnOrTaskTool(toolCall, rawInput)) return undefined;

  const childSessionId = extractXAiChildSessionId(output);
  const asyncSpawnAck = isXAiAsyncSpawnAck(output);
  const legacyResult =
    output
      ?.replace(new RegExp(`(?:^|\\n)\\s*Agent ID:\\s*${XAI_UUID_RE}[^\\n]*(?:\\n|$)`, "gi"), "\n")
      .replace(
        new RegExp(`(?:^|\\n)\\s*subagent_id:\\s*${XAI_UUID_RE}[^\\n]*(?:\\n|$)`, "gi"),
        "\n",
      )
      .trim() || null;

  let status: "running" | "completed" | "failed";
  if (toolCall.status === "failed") {
    status = "failed";
  } else if (asyncSpawnAck) {
    // Spawn RPC completed, but the child is still running.
    status = "running";
  } else if (toolCall.status === "completed") {
    status = "completed";
  } else {
    status = "running";
  }

  return {
    nativeTaskId: toolCall.toolCallId,
    prompt: nonEmptyString(rawInput?.prompt) ?? "",
    title: nonEmptyString(rawInput?.description) ?? null,
    model: nonEmptyString(rawInput?.model) ?? null,
    status,
    childSessionId,
    result: asyncSpawnAck ? null : legacyResult,
    suppressNormalTool: true,
  };
}

const XAiAskUserQuestionOption = Schema.Struct({
  label: Schema.String,
  description: Schema.optional(Schema.String),
  preview: Schema.optional(Schema.String),
  id: Schema.optional(Schema.String),
});

const XAiAskUserQuestion = Schema.Struct({
  id: Schema.optional(Schema.String),
  question: Schema.String,
  options: Schema.Array(XAiAskUserQuestionOption),
  multiSelect: Schema.optional(Schema.NullOr(Schema.Boolean)),
});

const XAiAskUserQuestionParams = Schema.Struct({
  sessionId: Schema.String,
  toolCallId: Schema.String,
  questions: Schema.Array(XAiAskUserQuestion),
  mode: Schema.Literals(["default", "plan"]),
});

const XAiWrappedAskUserQuestionParams = Schema.Struct({
  method: Schema.Literals(["x.ai/ask_user_question", "_x.ai/ask_user_question"]),
  params: XAiAskUserQuestionParams,
});

export const XAiAskUserQuestionRequest = Schema.Union([
  XAiAskUserQuestionParams,
  XAiWrappedAskUserQuestionParams,
]);

type XAiAskUserQuestionRequestParams = typeof XAiAskUserQuestionParams.Type;
type XAiAskUserQuestionRequest = typeof XAiAskUserQuestionRequest.Type;

function trimmed(value: string | undefined): string | undefined {
  const text = value?.trim();
  return text && text.length > 0 ? text : undefined;
}

function unwrapAskUserQuestionParams(
  params: XAiAskUserQuestionRequest,
): XAiAskUserQuestionRequestParams {
  return "params" in params ? params.params : params;
}

export function extractXAiAskUserQuestions(
  params: XAiAskUserQuestionRequest,
): ReadonlyArray<UserInputQuestion> {
  return unwrapAskUserQuestionParams(params).questions.map((question) => ({
    id: question.id ?? question.question,
    header: "Question",
    question: question.question,
    multiSelect: question.multiSelect === true,
    options:
      question.options.length > 0
        ? question.options.map((option) => ({
            label: option.label,
            description: option.description ?? option.label,
          }))
        : [{ label: "OK", description: "Continue" }],
  }));
}

export function extractXAiAskUserQuestionIdentity(params: XAiAskUserQuestionRequest): {
  readonly sessionId: string;
  readonly toolCallId: string;
} {
  const unwrapped = unwrapAskUserQuestionParams(params);
  return {
    sessionId: unwrapped.sessionId,
    toolCallId: unwrapped.toolCallId,
  };
}

interface XAiAskUserQuestionAnnotation {
  readonly preview?: string;
  readonly notes?: string;
}

interface XAiAskUserQuestionAcceptedResponse {
  readonly outcome: "accepted";
  readonly answers: Record<string, ReadonlyArray<string>>;
  readonly annotations?: Record<string, XAiAskUserQuestionAnnotation>;
}

interface XAiAskUserQuestionCancelledResponse {
  readonly outcome: "cancelled";
}

export type XAiAskUserQuestionResponse =
  | XAiAskUserQuestionAcceptedResponse
  | XAiAskUserQuestionCancelledResponse;

interface NormalizedXAiAnswer {
  readonly questionText: string;
  readonly selectedLabels: ReadonlyArray<string>;
  readonly annotation?: XAiAskUserQuestionAnnotation;
}

function answerValues(answer: unknown): ReadonlyArray<string> {
  if (Array.isArray(answer)) {
    return answer.flatMap((entry) => {
      const text = typeof entry === "string" ? trimmed(entry) : undefined;
      return text ? [text] : [];
    });
  }
  const text = typeof answer === "string" ? trimmed(answer) : undefined;
  return text ? [text] : [];
}

function normalizeAnswerForXAi(
  question: XAiAskUserQuestionRequestParams["questions"][number],
  answer: unknown,
): NormalizedXAiAnswer | undefined {
  const values = answerValues(answer);
  if (values.length === 0) {
    return undefined;
  }

  const optionByLabel = new Map(question.options.map((option) => [option.label, option]));
  const resolvedValues = values.map((value) => ({
    value,
    option: optionByLabel.get(value),
  }));
  const selectedLabels = resolvedValues.flatMap(({ option }) => (option ? [option.label] : []));
  const notes = resolvedValues.flatMap(({ option, value }) => (option ? [] : [value]));
  const preview =
    question.multiSelect === true
      ? undefined
      : resolvedValues.map(({ option }) => trimmed(option?.preview)).find((value) => value);

  const annotation =
    preview || notes.length > 0
      ? {
          ...(preview ? { preview } : {}),
          ...(notes.length > 0 ? { notes: notes.join("\n") } : {}),
        }
      : undefined;

  return {
    questionText: question.question,
    selectedLabels: selectedLabels.length > 0 ? selectedLabels : ["Other"],
    ...(annotation ? { annotation } : {}),
  };
}

function findQuestionAnswer(
  answers: ProviderUserInputAnswers,
  question: XAiAskUserQuestionRequestParams["questions"][number],
): unknown {
  const key = question.id ?? question.question;
  return answers[key] ?? answers[question.question];
}

export function makeXAiAskUserQuestionResponse(
  params: XAiAskUserQuestionRequest,
  answers: ProviderUserInputAnswers,
): XAiAskUserQuestionAcceptedResponse {
  const questions = unwrapAskUserQuestionParams(params).questions;
  const normalized = questions.flatMap((question) => {
    const entry = normalizeAnswerForXAi(question, findQuestionAnswer(answers, question));
    return entry ? [entry] : [];
  });
  const annotations = Object.fromEntries(
    normalized.flatMap((entry) =>
      entry.annotation ? [[entry.questionText, entry.annotation] as const] : [],
    ),
  );

  return {
    outcome: "accepted",
    answers: Object.fromEntries(
      normalized.map((entry) => [entry.questionText, entry.selectedLabels]),
    ),
    ...(Object.keys(annotations).length > 0 ? { annotations } : {}),
  };
}

export function makeXAiAskUserQuestionCancelledResponse(): XAiAskUserQuestionCancelledResponse {
  return { outcome: "cancelled" };
}

function promptIdFromResponse(response: EffectAcpSchema.PromptResponse): string | undefined {
  const meta = response._meta;
  if (meta === null || typeof meta !== "object") {
    return undefined;
  }
  const promptId = meta.promptId ?? meta.requestId;
  return typeof promptId === "string" && promptId.length > 0 ? promptId : undefined;
}

function normalizeXAiStopReason(value: string | undefined): EffectAcpSchema.StopReason {
  switch (value) {
    case "cancelled":
    case "end_turn":
    case "max_tokens":
    case "max_turn_requests":
    case "refusal":
      return value;
    default:
      return "end_turn";
  }
}

function promptResponseFromXAi(
  notification: XAiPromptCompleteNotification,
): EffectAcpSchema.PromptResponse {
  const stopReason = normalizeXAiStopReason(notification.stopReason);
  const meta: Record<string, unknown> = {
    sessionId: notification.sessionId,
  };
  if (notification.stopReason === undefined) {
    meta[xAiStopReasonMissingMetaKey] = true;
  }
  if (notification.promptId !== undefined) {
    meta.promptId = notification.promptId;
    meta.requestId = notification.promptId;
  }
  if (notification.agentResult !== undefined) {
    meta.agentResult = notification.agentResult;
  }
  return {
    stopReason,
    _meta: meta,
  };
}

const registerXAiPromptCompletionFallback = (
  pendingRef: Ref.Ref<ReadonlyArray<PendingXAiPromptCompletion>>,
  sessionId: string,
  promptId: string,
) =>
  Deferred.make<EffectAcpSchema.PromptResponse>().pipe(
    Effect.tap((deferred) =>
      Ref.update(pendingRef, (pending) => [...pending, { sessionId, promptId, deferred }]),
    ),
    Effect.map((deferred) => ({ deferred, promptId })),
  );

const unregisterXAiPromptCompletionFallback = (
  pendingRef: Ref.Ref<ReadonlyArray<PendingXAiPromptCompletion>>,
  deferred: Deferred.Deferred<EffectAcpSchema.PromptResponse>,
) => Ref.update(pendingRef, (pending) => pending.filter((entry) => entry.deferred !== deferred));

const abortPendingPromptCompletions = (
  pendingRef: Ref.Ref<ReadonlyArray<PendingXAiPromptCompletion>>,
  sessionId: string,
) =>
  Ref.modify(pendingRef, (pending) => {
    const [toAbort, remaining] = pending.reduce<
      [ReadonlyArray<PendingXAiPromptCompletion>, ReadonlyArray<PendingXAiPromptCompletion>]
    >(
      ([aborting, kept], entry) =>
        entry.sessionId === sessionId ? [[...aborting, entry], kept] : [aborting, [...kept, entry]],
      [[], []],
    );
    if (toAbort.length === 0) {
      return [Effect.void, pending] as const;
    }
    return [
      Effect.forEach(
        toAbort,
        (entry) =>
          Deferred.succeed(
            entry.deferred,
            promptResponseFromXAi({
              sessionId: entry.sessionId,
              promptId: entry.promptId,
              stopReason: "cancelled",
              agentResult: null,
            }),
          ),
        { concurrency: "unbounded" },
      ).pipe(Effect.asVoid),
      remaining,
    ] as const;
  }).pipe(Effect.flatten);

const resolveXAiPromptCompletionFallback = ({
  pendingRef,
  completedPromptIdsRef,
  notification,
}: {
  readonly pendingRef: Ref.Ref<ReadonlyArray<PendingXAiPromptCompletion>>;
  readonly completedPromptIdsRef: Ref.Ref<ReadonlyArray<string>>;
  readonly notification: XAiPromptCompleteNotification;
}) =>
  Ref.get(completedPromptIdsRef).pipe(
    Effect.flatMap((completedPromptIds) => {
      if (
        notification.promptId !== undefined &&
        completedPromptIds.includes(notification.promptId)
      ) {
        return Effect.void;
      }
      return Ref.modify(pendingRef, (pending) => {
        const index =
          notification.promptId !== undefined
            ? pending.findIndex(
                (entry) =>
                  entry.sessionId === notification.sessionId &&
                  entry.promptId === notification.promptId,
              )
            : (() => {
                const sessionPendingIndexes = pending.flatMap((entry, entryIndex) =>
                  entry.sessionId === notification.sessionId ? [entryIndex] : [],
                );
                if (sessionPendingIndexes.length === 0) {
                  return -1;
                }
                return sessionPendingIndexes[0] ?? -1;
              })();
        if (index < 0) {
          return [Effect.void, pending] as const;
        }
        const entry = pending[index];
        if (!entry) {
          return [Effect.void, pending] as const;
        }
        return [
          Deferred.succeed(entry.deferred, promptResponseFromXAi(notification)).pipe(Effect.asVoid),
          [...pending.slice(0, index), ...pending.slice(index + 1)],
        ] as const;
      }).pipe(Effect.flatten);
    }),
  );

const rememberCompletedXAiPromptId = (
  completedPromptIdsRef: Ref.Ref<ReadonlyArray<string>>,
  response: EffectAcpSchema.PromptResponse,
  fallbackPromptId: string,
) => {
  const promptId = promptIdFromResponse(response) ?? fallbackPromptId;
  if (promptId.length === 0) {
    return Effect.void;
  }
  return Ref.update(completedPromptIdsRef, (completedPromptIds) => {
    if (completedPromptIds.includes(promptId)) {
      return completedPromptIds;
    }
    return [...completedPromptIds, promptId].slice(-completedXAiPromptIdLimit);
  });
};

/**
 * Grok-specific ACP runtime wrapper. Races `session/prompt` against root-matched
 * terminal notifications:
 * - `_x.ai/session/update` + `turn_completed` + matching `prompt_id` (current CLI)
 * - `_x.ai/session/prompt_complete` (legacy / alternate builds)
 *
 * Pending entries are keyed by root sessionId + T3-injected promptId, so
 * foreign/child sessions and `task-completed-*` ids do not settle the root turn.
 */
export const makeXAiPromptCompletionRuntime = Effect.fn("makeXAiPromptCompletionRuntime")(
  function* (runtime: AcpSessionRuntime.AcpSessionRuntime["Service"]) {
    let nextPromptFallbackId = 0;
    const allocatePromptFallbackId = Effect.sync(() => {
      nextPromptFallbackId += 1;
      return `t3-xai-prompt-${nextPromptFallbackId}`;
    });
    const pendingXAiPromptCompletionsRef = yield* Ref.make<
      ReadonlyArray<PendingXAiPromptCompletion>
    >([]);
    const completedXAiPromptIdsRef = yield* Ref.make<ReadonlyArray<string>>([]);

    const settleFromPromptComplete = (notification: XAiPromptCompleteNotification) =>
      resolveXAiPromptCompletionFallback({
        pendingRef: pendingXAiPromptCompletionsRef,
        completedPromptIdsRef: completedXAiPromptIdsRef,
        notification,
      }).pipe(Effect.catch(() => Effect.void));

    yield* runtime.handleExtNotification(
      "_x.ai/session/prompt_complete",
      XAiPromptCompleteNotification,
      settleFromPromptComplete,
    );

    yield* runtime.handleExtNotification(
      "_x.ai/session/update",
      XAiSessionUpdateNotification,
      (notification) => {
        const complete = xAiPromptCompleteFromSessionUpdate(notification);
        if (complete === null) {
          return Effect.void;
        }
        return settleFromPromptComplete(complete);
      },
    );

    return {
      ...runtime,
      prompt: (payload) =>
        Effect.gen(function* () {
          const started = yield* runtime.start();
          const promptId = yield* allocatePromptFallbackId;
          const fallback = yield* registerXAiPromptCompletionFallback(
            pendingXAiPromptCompletionsRef,
            started.sessionId,
            promptId,
          );
          const cancelledResponse = promptResponseFromXAi({
            sessionId: started.sessionId,
            promptId: fallback.promptId,
            stopReason: "cancelled",
            agentResult: null,
          });
          const promptRpcFiber = yield* runtime
            .prompt({
              ...payload,
              _meta: {
                ...payload._meta,
                promptId: fallback.promptId,
                requestId: fallback.promptId,
              },
            })
            .pipe(Effect.forkChild);
          return yield* Effect.raceFirst(
            Fiber.join(promptRpcFiber).pipe(
              Effect.catchCause((cause) =>
                Cause.hasInterruptsOnly(cause)
                  ? Effect.succeed(cancelledResponse)
                  : Effect.failCause(cause),
              ),
            ),
            Deferred.await(fallback.deferred),
          ).pipe(
            Effect.tap((response) =>
              rememberCompletedXAiPromptId(completedXAiPromptIdsRef, response, fallback.promptId),
            ),
            Effect.ensuring(
              Effect.gen(function* () {
                yield* Fiber.interrupt(promptRpcFiber).pipe(Effect.ignore);
                yield* unregisterXAiPromptCompletionFallback(
                  pendingXAiPromptCompletionsRef,
                  fallback.deferred,
                );
              }),
            ),
          );
        }),
      cancel: Effect.gen(function* () {
        const started = yield* runtime.start();
        yield* abortPendingPromptCompletions(pendingXAiPromptCompletionsRef, started.sessionId);
        yield* runtime.cancel;
      }),
    } satisfies AcpSessionRuntime.AcpSessionRuntime["Service"];
  },
);

export function promptResponseHasMissingXAiStopReason(
  response: EffectAcpSchema.PromptResponse,
): boolean {
  const meta = response._meta;
  return meta !== null && typeof meta === "object" && meta[xAiStopReasonMissingMetaKey] === true;
}
