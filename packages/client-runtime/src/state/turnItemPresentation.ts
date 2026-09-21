import {
  NodeId,
  type OrchestrationV2Notification,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";

const WORKSPACE_PREPARATION_INPUT = "Preparing workspace";

/** Present old trial wakes through the notification UI, without changing durable data. */
export function presentLegacyWakeItem(item: OrchestrationV2TurnItem): OrchestrationV2TurnItem {
  if (item.type !== "user_message" || item.createdBy !== "agent") return item;

  let notification: OrchestrationV2Notification | undefined;
  if (item.creationSource === "provider") {
    const commandCount = [
      ...item.text.matchAll(/^Background command completed(?: \(exit -?\d+\))?:/gmu),
    ].length;
    const taskCount = [...item.text.matchAll(/^Background task completed\./gmu)].length;
    const kind =
      item.providerWake?.kind ?? (commandCount > 0 ? "background_command" : "background_task");
    const count = item.providerWake?.count ?? commandCount + taskCount;
    if (count > 0) {
      notification = {
        source: { kind },
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

/** Workspace setup is client bookkeeping; preparation failures have their own error item. */
export function turnItemIsWorkspacePreparation(item: OrchestrationV2TurnItem): boolean {
  return item.type === "command_execution" && item.input === WORKSPACE_PREPARATION_INPUT;
}
