import type { OrchestrationV2Run, OrchestrationV2ThreadProjection } from "@t3tools/contracts";
import { isBackgroundCommandWakeMessage } from "@t3tools/shared/wakePromptPresentation";

export function isAutomaticCompletionRun(
  projection: OrchestrationV2ThreadProjection,
  run: OrchestrationV2Run,
): boolean {
  return projection.messages.some(
    (message) => message.id === run.userMessageId && message.delegatedCompletion !== undefined,
  );
}

export function queuedRunsInDeliveryOrder(
  projection: OrchestrationV2ThreadProjection,
): ReadonlyArray<OrchestrationV2Run> {
  const automaticCompletionMessageIds = new Set(
    projection.messages
      .filter((message) => message.delegatedCompletion !== undefined)
      .map((message) => message.id),
  );
  return projection.runs
    .filter((run) => run.status === "queued")
    .toSorted((left, right) => {
      const deliveryPriority =
        Number(automaticCompletionMessageIds.has(right.userMessageId)) -
        Number(automaticCompletionMessageIds.has(left.userMessageId));
      return (
        deliveryPriority ||
        (left.queuePosition ?? left.ordinal) - (right.queuePosition ?? right.ordinal) ||
        left.ordinal - right.ordinal
      );
    });
}

export function queuedBackgroundCommandWakeRuns(
  projection: OrchestrationV2ThreadProjection,
): ReadonlyArray<OrchestrationV2Run> {
  return queuedRunsInDeliveryOrder(projection).filter((run) => {
    const message = projection.messages.find((candidate) => candidate.id === run.userMessageId);
    return message !== undefined && isBackgroundCommandWakeMessage(message);
  });
}
