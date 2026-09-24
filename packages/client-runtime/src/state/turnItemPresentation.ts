import {
  ORCHESTRATION_V2_WORKSPACE_PREPARATION_FAILURE_CODE,
  type OrchestrationV2Run,
  NodeId,
  type OrchestrationV2Notification,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";

const WORKSPACE_PREPARATION_INPUT = "Preparing workspace";

/** Present old trial wakes through the notification UI, without changing durable data. */
export function presentLegacyWakeItem(item: OrchestrationV2TurnItem): OrchestrationV2TurnItem {
  if (item.type !== "user_message" || item.createdBy !== "agent") return item;
  if (item.senderThreadId !== undefined) return item;

  let notification: OrchestrationV2Notification | undefined;
  if (item.creationSource === "provider") {
    const commandCount = [
      ...item.text.matchAll(/^Background command completed(?: \(exit -?\d+\))?:/gmu),
    ].length;
    const taskCount = [...item.text.matchAll(/^Background task completed\./gmu)].length;
    const legacyKind =
      item.providerWake?.kind ?? (commandCount > 0 ? "background_command" : "background_task");
    const count = item.providerWake?.count ?? commandCount + taskCount;
    if (count > 0) {
      notification = {
        source: { kind: legacyKind === "background_command" ? "command" : "background_task" },
        outcome: "unknown",
        summary: count > 1 ? `${count} background tasks finished` : "Background task finished",
        detail: item.text,
      };
    }
  } else if (item.creationSource === "server") {
    const taskIds = [
      ...new Set(
        [...item.text.matchAll(/^Delegated tasks? (.+?) reached (?:a )?terminal states?\./gmu)]
          .flatMap((match) => (match[1] ?? "").split(", "))
          .map((id) => id.trim())
          .filter((id) => id.length > 0),
      ),
    ].map((id) => NodeId.make(id));
    if (taskIds.length > 0) {
      notification = {
        source: { kind: "delegated_task", taskIds },
        outcome: "unknown",
        summary:
          taskIds.length > 1
            ? `${taskIds.length} delegated tasks finished`
            : "Delegated task finished",
        detail: item.text,
      };
    }
  }
  return notification === undefined ? item : { ...item, type: "notification", ...notification };
}

/**
 * Workspace setup is client bookkeeping; preparation failures have their own
 * error item. A retry cancels that item, which then has nothing left to say.
 */
export function turnItemIsWorkspacePreparation(item: OrchestrationV2TurnItem): boolean {
  return (
    (item.type === "command_execution" && item.input === WORKSPACE_PREPARATION_INPUT) ||
    (item.type === "error" &&
      item.status === "cancelled" &&
      item.failure.code === ORCHESTRATION_V2_WORKSPACE_PREPARATION_FAILURE_CODE)
  );
}

/**
 * Runs a Retry can prepare again: their workspace preparation failed and the
 * run still ended there. Older servers record no preparation on the run, so
 * they never offer it.
 */
export function workspacePreparationRetryRunIds(
  runs: ReadonlyArray<Pick<OrchestrationV2Run, "id" | "status" | "workspacePreparation">>,
  items: ReadonlyArray<OrchestrationV2TurnItem>,
): ReadonlySet<OrchestrationV2Run["id"]> {
  const failedRuns = new Set(
    runs.flatMap((run) =>
      run.status === "failed" && run.workspacePreparation !== undefined ? [run.id] : [],
    ),
  );
  const retryable = new Set<OrchestrationV2Run["id"]>();
  if (failedRuns.size === 0) return retryable;
  for (const item of items) {
    if (
      item.type === "error" &&
      item.status === "failed" &&
      item.failure.code === ORCHESTRATION_V2_WORKSPACE_PREPARATION_FAILURE_CODE &&
      item.runId !== null &&
      failedRuns.has(item.runId)
    )
      retryable.add(item.runId);
  }
  return retryable;
}
