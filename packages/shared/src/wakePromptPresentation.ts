export const PROVIDER_BUFFERED_CONTINUATION_TEXT = "Background task completed.";

export type WakePromptKind = "background" | "delegated";

export type ProviderWakeKind = "background_command" | "background_task";

export interface ProviderWake {
  readonly kind: ProviderWakeKind;
  readonly count: number;
}

export interface WakePromptMessage {
  readonly text: string;
  readonly createdBy?: string | undefined;
  readonly creationSource?: string | undefined;
  readonly delegatedCompletion?: unknown;
  readonly providerWake?: unknown;
}

export interface WakePromptPresentation {
  readonly kind: WakePromptKind;
  readonly heading: string;
  readonly preview: string | null;
}

const DELEGATED_WAKE_SENTENCE = /Delegated tasks? (.+?) reached (?:a )?terminal states?\./gu;
const BACKGROUND_TASK_COMPLETED = /Background task completed\./gu;
const BACKGROUND_COMMAND_COMPLETED = /Background command completed(?: \(exit -?\d+\))?:/gu;

export function isWakePromptMessage(message: WakePromptMessage): boolean {
  return resolveWakePromptPresentation(message) !== null;
}

export function isBackgroundCommandWakeMessage(message: WakePromptMessage): boolean {
  if (message.createdBy !== "agent" || message.creationSource !== "provider") {
    return false;
  }
  const wake = providerWakeFromMetadata(message.providerWake);
  if (wake !== undefined) {
    return wake.kind === "background_command";
  }
  return countMatches(BACKGROUND_COMMAND_COMPLETED, message.text) > 0;
}

export function backgroundCommandWakeCount(message: WakePromptMessage): number {
  const wake = providerWakeFromMetadata(message.providerWake);
  if (wake?.kind === "background_command") {
    return wake.count;
  }
  if (wake?.kind === "background_task") {
    return 0;
  }
  return countMatches(BACKGROUND_COMMAND_COMPLETED, message.text);
}

export function mergedBackgroundCommandWake(
  messages: ReadonlyArray<WakePromptMessage>,
): ProviderWake | undefined {
  let count = 0;
  for (const message of messages) {
    count += backgroundCommandWakeCount(message);
  }
  if (count < 1) {
    return undefined;
  }
  return { kind: "background_command", count };
}

export function joinWakePromptTexts(texts: ReadonlyArray<string>): string {
  const parts: string[] = [];
  for (const text of texts) {
    const trimmed = text.trimEnd();
    if (trimmed.length === 0) {
      continue;
    }
    parts.push(trimmed);
  }
  return parts.join("\n\n");
}

export function resolveWakePromptPresentation(
  message: WakePromptMessage,
): WakePromptPresentation | null {
  const hasDelegatedMetadata =
    message.delegatedCompletion !== undefined && message.delegatedCompletion !== null;
  const providerWake = providerWakeFromMetadata(message.providerWake);
  const canBeDelegatedText = message.createdBy === "agent" && message.creationSource === "server";
  const canBeBackgroundText =
    message.createdBy === "agent" && message.creationSource === "provider";
  if (
    !hasDelegatedMetadata &&
    providerWake === undefined &&
    !canBeDelegatedText &&
    !canBeBackgroundText
  ) {
    return null;
  }

  const metadataTaskIds = taskIdsFromMetadata(message.delegatedCompletion);
  const parsedTaskIds =
    hasDelegatedMetadata || canBeDelegatedText ? collectDelegatedTaskIds(message.text) : [];
  const taskIds = uniqueStrings([...metadataTaskIds, ...parsedTaskIds]);
  const backgroundCounts = backgroundWakeCounts(message.text, providerWake, canBeBackgroundText);
  const backgroundCount = backgroundCounts.task + backgroundCounts.command;
  const hasDelegated = hasDelegatedMetadata || (canBeDelegatedText && taskIds.length > 0);
  const hasBackground = canBeBackgroundText && backgroundCount > 0;

  if (hasDelegated) {
    return {
      kind: "delegated",
      heading:
        taskIds.length > 1
          ? `${taskIds.length} delegated tasks finished`
          : "Delegated task finished",
      preview: formatDelegatedWakePreview(taskIds),
    };
  }

  if (hasBackground) {
    return {
      kind: "background",
      heading:
        backgroundCount > 1
          ? `${backgroundCount} background tasks finished`
          : "Background task finished",
      preview: null,
    };
  }
  return null;
}

function collectDelegatedTaskIds(text: string): string[] {
  const ids: string[] = [];
  for (const match of text.matchAll(DELEGATED_WAKE_SENTENCE)) {
    const taskList = match[1]?.trim();
    if (taskList === undefined || taskList.length === 0) {
      continue;
    }
    for (const taskId of taskList.split(", ")) {
      const trimmed = taskId.trim();
      if (trimmed.length > 0) {
        ids.push(trimmed);
      }
    }
  }
  return uniqueStrings(ids);
}

function taskIdsFromMetadata(delegatedCompletion: unknown): string[] {
  if (delegatedCompletion === null || typeof delegatedCompletion !== "object") {
    return [];
  }
  const taskIds = Reflect.get(delegatedCompletion, "taskIds");
  if (!Array.isArray(taskIds)) {
    return [];
  }
  return uniqueStrings(
    taskIds.filter(
      (taskId): taskId is string => typeof taskId === "string" && taskId.trim().length > 0,
    ),
  );
}

function backgroundWakeCounts(
  text: string,
  providerWake: ProviderWake | undefined,
  canBeBackgroundText: boolean,
): { readonly task: number; readonly command: number } {
  if (providerWake?.kind === "background_task") {
    return { task: providerWake.count, command: 0 };
  }
  if (providerWake?.kind === "background_command") {
    return { task: 0, command: providerWake.count };
  }
  if (!canBeBackgroundText) {
    return { task: 0, command: 0 };
  }
  return {
    task: countMatches(BACKGROUND_TASK_COMPLETED, text),
    command: countMatches(BACKGROUND_COMMAND_COMPLETED, text),
  };
}

function providerWakeFromMetadata(value: unknown): ProviderWake | undefined {
  if (value === null || typeof value !== "object") {
    return undefined;
  }
  const kind = Reflect.get(value, "kind");
  const count = Reflect.get(value, "count");
  if (kind !== "background_command" && kind !== "background_task") {
    return undefined;
  }
  if (typeof count !== "number" || !Number.isInteger(count) || count < 1) {
    return undefined;
  }
  return { kind, count };
}

function countMatches(pattern: RegExp, text: string): number {
  return [...text.matchAll(pattern)].length;
}

function uniqueStrings(values: ReadonlyArray<string>): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const value of values) {
    if (seen.has(value)) {
      continue;
    }
    seen.add(value);
    result.push(value);
  }
  return result;
}

function formatDelegatedWakePreview(taskIds: ReadonlyArray<string>): string | null {
  if (taskIds.length === 0) {
    return null;
  }
  return taskIds.map(delegatedTaskPreviewLabel).join(", ");
}

export function delegatedTaskPreviewLabel(taskId: string): string {
  let decoded = taskId;
  try {
    decoded = decodeURIComponent(taskId);
  } catch {
    decoded = taskId;
  }
  const segments = decoded.split(":").filter((segment) => segment.length > 0);
  const last = segments.at(-1)?.trim();
  if (last !== undefined && last.length > 0 && last !== "delegated-task" && last !== "node") {
    return last;
  }
  return decoded;
}
