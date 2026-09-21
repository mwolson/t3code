import {
  MessageId,
  OrchestrationV2TurnItem,
  OrchestrationV2TurnItemJson,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";

import { presentLegacyWakeItem, turnItemIsWorkspacePreparation } from "./turnItemPresentation.ts";

const encodeTurnItemJson = Schema.encodeSync(OrchestrationV2TurnItemJson);
const decodeDurableTurnItem = Schema.decodeSync(Schema.fromJsonString(OrchestrationV2TurnItemJson));
const decodeWireTurnItem = Schema.decodeSync(OrchestrationV2TurnItem);

function command(input: string): Extract<OrchestrationV2TurnItem, { type: "command_execution" }> {
  const now = DateTime.makeUnsafe("2026-08-03T00:00:00.000Z");
  return {
    id: TurnItemId.make("item-command"),
    threadId: ThreadId.make("thread-1"),
    runId: RunId.make("run-1"),
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 1,
    status: "completed",
    title: "Workspace ready",
    startedAt: now,
    completedAt: now,
    updatedAt: now,
    type: "command_execution",
    input,
    output: "Workspace preparation completed.",
    exitCode: 0,
  };
}

function wake(
  overrides: Partial<Extract<OrchestrationV2TurnItem, { type: "user_message" }>> = {},
): Extract<OrchestrationV2TurnItem, { type: "user_message" }> {
  return {
    ...command("ignored"),
    type: "user_message",
    messageId: MessageId.make("old-wake"),
    createdBy: "agent",
    creationSource: "provider",
    inputIntent: "turn_start",
    text: "Background task completed.",
    attachments: [],
    ...overrides,
  };
}

describe("legacy trial wake presentation", () => {
  it("preserves authoritative metadata through durable JSON and live wire decoding", () => {
    const original = wake({
      text: "Provider-specific payload without the sentinel",
      providerWake: { kind: "background_command", count: 2 },
    });
    const encoded = encodeTurnItemJson(original);
    const durable = decodeDurableTurnItem(JSON.stringify(encoded));
    const wire = decodeWireTurnItem(durable);
    expect(wire).toMatchObject({
      providerWake: original.providerWake,
      createdBy: "agent",
      creationSource: "provider",
    });
    expect(presentLegacyWakeItem(wire)).toMatchObject({
      type: "notification",
      source: { kind: "background_command" },
      summary: "2 background tasks finished",
      detail: original.text,
    });
    expect(wire.type).toBe("user_message");
  });

  it.each([
    ["Background task completed.", "provider", "background_task", "Background task finished"],
    [
      "Background command completed (exit -1): command",
      "provider",
      "background_command",
      "Background task finished",
    ],
    [
      "Delegated task node:task-1 reached a terminal state. Read the result.",
      "server",
      "delegated_task",
      "Delegated task finished",
    ],
    [
      "Delegated tasks node:task-1, node:task-2 reached terminal states.",
      "server",
      "delegated_task",
      "2 delegated tasks finished",
    ],
  ] as const)(
    "recognizes the old %s sentinel only with authoritative provenance",
    (text, creationSource, kind, summary) => {
      expect(presentLegacyWakeItem(wake({ text, creationSource }))).toMatchObject({
        type: "notification",
        source: { kind },
        summary,
      });
      const user = wake({ text, creationSource, createdBy: "user" });
      expect(presentLegacyWakeItem(user)).toBe(user);
      const web = wake({ text, creationSource: "web" });
      expect(presentLegacyWakeItem(web)).toBe(web);
    },
  );

  it("does not trust wake-shaped metadata on an ordinary user or unrelated agent message", () => {
    for (const item of [
      wake({ createdBy: "user", providerWake: { kind: "background_task", count: 1 } }),
      wake({ text: "Explain this example: Background task completed." }),
      wake({ creationSource: "server" }),
    ])
      expect(presentLegacyWakeItem(item)).toBe(item);
  });

  it("leaves upstream typed notifications unchanged", () => {
    const item: OrchestrationV2TurnItem = {
      ...command("ignored"),
      type: "notification",
      source: { kind: "monitor" },
      outcome: "failed",
      summary: "Build failed",
    };
    expect(presentLegacyWakeItem(item)).toBe(item);
  });
});

describe("turnItemIsWorkspacePreparation", () => {
  it("identifies the synthetic workspace preparation command", () => {
    expect(turnItemIsWorkspacePreparation(command("Preparing workspace"))).toBe(true);
    expect(turnItemIsWorkspacePreparation(command("prepare workspace"))).toBe(false);
  });
});
