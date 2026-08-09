import { describe, expect, it } from "vite-plus/test";
import { TurnItemId, type OrchestrationV2TurnItem } from "@t3tools/contracts";

import {
  getThreadOpenTailLimit,
  maybeTailThreadProjection,
  setThreadOpenTailLimit,
  tailThreadProjection,
} from "./threadOpenTail.ts";
import { v2Projection, v2ThreadId } from "./orchestrationV2TestFixtures.ts";

function makeTurnItem(index: number): OrchestrationV2TurnItem {
  return {
    id: TurnItemId.make(`turn-item-${index}`),
    threadId: v2ThreadId,
    runId: null,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: index,
    status: "completed",
    title: null,
    startedAt: v2Projection.updatedAt,
    completedAt: v2Projection.updatedAt,
    updatedAt: v2Projection.updatedAt,
    type: "command_execution",
    input: `cmd-${index}`,
    output: `out-${index}`,
    exitCode: 0,
  };
}

describe("threadOpenTail", () => {
  it("is disabled by default and via null", () => {
    setThreadOpenTailLimit(null);
    expect(getThreadOpenTailLimit()).toBeNull();
    expect(maybeTailThreadProjection(v2Projection)).toBe(v2Projection);
  });

  it("keeps the newest N visible rows and matching turnItems", () => {
    setThreadOpenTailLimit(null);
    const items = Array.from({ length: 5 }, (_, index) => {
      const item = makeTurnItem(index);
      return {
        position: index,
        visibility: "local" as const,
        sourceThreadId: v2ThreadId,
        sourceItemId: item.id,
        item,
      };
    });
    const projection = {
      ...v2Projection,
      turnItems: items.map((row) => row.item),
      visibleTurnItems: items,
    };

    const tailed = tailThreadProjection(projection, 2);
    expect(tailed.visibleTurnItems.map((row) => String(row.sourceItemId))).toEqual([
      "turn-item-3",
      "turn-item-4",
    ]);
    expect(tailed.visibleTurnItems.map((row) => row.position)).toEqual([0, 1]);
    expect(tailed.turnItems.map((item) => String(item.id))).toEqual(["turn-item-3", "turn-item-4"]);
    expect(maybeTailThreadProjection(projection)).toBe(projection);

    setThreadOpenTailLimit(2);
    expect(maybeTailThreadProjection(projection).visibleTurnItems).toHaveLength(2);
    setThreadOpenTailLimit(null);
  });
});
