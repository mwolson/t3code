export const PROVIDER_BUFFERED_CONTINUATION_TEXT = "Background task completed.";

export type WakePromptKind = "background" | "delegated";

export interface WakePromptMessage {
  readonly text: string;
  readonly createdBy?: string | undefined;
  readonly creationSource?: string | undefined;
  readonly delegatedCompletion?: unknown;
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

export function resolveWakePromptPresentation(
  message: WakePromptMessage,
): WakePromptPresentation | null {
  const metadataTaskIds = taskIdsFromMetadata(message.delegatedCompletion);
  const parsedTaskIds = collectDelegatedTaskIds(message.text);
  const taskIds = uniqueStrings([...metadataTaskIds, ...parsedTaskIds]);
  const backgroundTaskCount = countMatches(BACKGROUND_TASK_COMPLETED, message.text);
  const backgroundCommandCount = countMatches(BACKGROUND_COMMAND_COMPLETED, message.text);
  const backgroundCount = backgroundTaskCount + backgroundCommandCount;
  const hasDelegatedMetadata =
    message.delegatedCompletion !== undefined && message.delegatedCompletion !== null;
  const hasDelegated =
    hasDelegatedMetadata ||
    (message.createdBy === "agent" && message.creationSource === "server" && taskIds.length > 0);
  const hasBackground =
    message.createdBy === "agent" && message.creationSource === "provider" && backgroundCount > 0;

  if (!hasDelegated && !hasBackground) {
    return null;
  }

  if (hasDelegated && backgroundCount === 0) {
    return {
      kind: "delegated",
      heading:
        taskIds.length > 1
          ? `${taskIds.length} delegated tasks finished`
          : "Delegated task finished",
      preview: formatDelegatedWakePreview(taskIds),
    };
  }

  if (!hasDelegated && hasBackground) {
    return {
      kind: "background",
      heading:
        backgroundCount > 1
          ? `${backgroundCount} background tasks finished`
          : "Background task finished",
      preview: null,
    };
  }

  return {
    kind: "delegated",
    heading: `${taskIds.length + backgroundCount} tasks finished`,
    preview: formatDelegatedWakePreview(taskIds),
  };
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
