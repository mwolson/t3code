import type { OrchestrationV2ThreadProjection, OrchestrationV2TurnItem } from "@t3tools/contracts";

/**
 * Mobile long-thread open: keep only the newest N projected rows in memory.
 * Full history still lives on the server; load-more can be layered later via
 * the V1-style window contracts once V2 HTTP exposes them.
 *
 * null = disabled (default for web and unconfigured clients).
 */
let threadOpenTailLimit: number | null = null;

export function setThreadOpenTailLimit(limit: number | null): void {
  if (limit === null) {
    threadOpenTailLimit = null;
    return;
  }
  threadOpenTailLimit = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : null;
}

export function getThreadOpenTailLimit(): number | null {
  return threadOpenTailLimit;
}

function renumberVisibleItems(
  rows: OrchestrationV2ThreadProjection["visibleTurnItems"],
): OrchestrationV2ThreadProjection["visibleTurnItems"] {
  return rows.map((row, position) => (row.position === position ? row : { ...row, position }));
}

/**
 * Returns a projection whose `visibleTurnItems` (and matching `turnItems` /
 * `nodes`) are capped to the newest `limit` rows. No-op when already within
 * the limit.
 */
export function tailThreadProjection(
  projection: OrchestrationV2ThreadProjection,
  limit: number,
): OrchestrationV2ThreadProjection {
  if (!Number.isFinite(limit) || limit <= 0) {
    return projection;
  }
  const maxItems = Math.floor(limit);
  const visible = projection.visibleTurnItems;
  if (visible.length <= maxItems) {
    return projection;
  }

  const tailedVisible = renumberVisibleItems(visible.slice(-maxItems));
  const keepItemIds = new Set(tailedVisible.map((row) => row.sourceItemId));
  const tailedTurnItems = projection.turnItems.filter((item) => keepItemIds.has(item.id));

  const keepNodeIds = new Set<string>();
  for (const item of tailedTurnItems as ReadonlyArray<OrchestrationV2TurnItem>) {
    if (item.nodeId !== null && item.nodeId !== undefined) {
      keepNodeIds.add(String(item.nodeId));
    }
  }
  const tailedNodes =
    keepNodeIds.size === 0
      ? projection.nodes
      : projection.nodes.filter((node) => keepNodeIds.has(String(node.id)));

  return {
    ...projection,
    visibleTurnItems: tailedVisible,
    turnItems: tailedTurnItems,
    nodes: tailedNodes,
  };
}

export function maybeTailThreadProjection(
  projection: OrchestrationV2ThreadProjection,
): OrchestrationV2ThreadProjection {
  const limit = getThreadOpenTailLimit();
  return limit === null ? projection : tailThreadProjection(projection, limit);
}
