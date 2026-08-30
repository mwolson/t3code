import { describe, expect, it } from "vite-plus/test";

import { queuedBackgroundCommandWakeRuns, queuedRunsInDeliveryOrder } from "./QueuedRunOrder.ts";

describe("queued run delivery order", () => {
  it("keeps automatic completion delivery ahead of visible queued messages", () => {
    const projection = {
      messages: [
        { id: "message:visible-first" },
        {
          id: "message:automatic",
          delegatedCompletion: {
            generation: 1,
            parentRunId: "run:parent",
            taskIds: ["task:child"],
          },
        },
        { id: "message:visible-second" },
      ],
      runs: [
        {
          id: "run:visible-first",
          ordinal: 2,
          queuePosition: 1,
          status: "queued",
          userMessageId: "message:visible-first",
        },
        {
          id: "run:automatic",
          ordinal: 4,
          queuePosition: 3,
          status: "queued",
          userMessageId: "message:automatic",
        },
        {
          id: "run:visible-second",
          ordinal: 3,
          queuePosition: 2,
          status: "queued",
          userMessageId: "message:visible-second",
        },
      ],
    } as never;

    expect(queuedRunsInDeliveryOrder(projection).map((run) => run.id)).toEqual([
      "run:automatic",
      "run:visible-first",
      "run:visible-second",
    ]);
  });

  it("collects queued Codex background-command wakes in delivery order", () => {
    const projection = {
      messages: [
        { id: "message:visible", text: "Visible queued message", createdBy: "user" },
        {
          id: "message:codex-first",
          text: "Background command completed (exit 1): sleep 12",
          createdBy: "agent",
          creationSource: "provider",
        },
        {
          id: "message:claude",
          text: "Background task completed.",
          createdBy: "agent",
          creationSource: "provider",
        },
        {
          id: "message:codex-second",
          text: "Background command completed (exit 0): sleep 18",
          createdBy: "agent",
          creationSource: "provider",
        },
        {
          id: "message:codex-metadata",
          text: "custom wake",
          createdBy: "agent",
          creationSource: "provider",
          providerWake: { kind: "background_command", count: 1 },
        },
      ],
      runs: [
        {
          id: "run:visible",
          ordinal: 2,
          queuePosition: 1,
          status: "queued",
          userMessageId: "message:visible",
        },
        {
          id: "run:codex-first",
          ordinal: 3,
          queuePosition: 2,
          status: "queued",
          userMessageId: "message:codex-first",
        },
        {
          id: "run:claude",
          ordinal: 4,
          queuePosition: 3,
          status: "queued",
          userMessageId: "message:claude",
        },
        {
          id: "run:codex-second",
          ordinal: 5,
          queuePosition: 4,
          status: "queued",
          userMessageId: "message:codex-second",
        },
        {
          id: "run:codex-metadata",
          ordinal: 6,
          queuePosition: 5,
          status: "queued",
          userMessageId: "message:codex-metadata",
        },
      ],
    } as never;

    expect(queuedBackgroundCommandWakeRuns(projection).map((run) => run.id)).toEqual([
      "run:codex-first",
      "run:codex-second",
      "run:codex-metadata",
    ]);
  });
});
