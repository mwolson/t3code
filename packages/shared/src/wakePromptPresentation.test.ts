import { describe, expect, it } from "vite-plus/test";

import {
  delegatedTaskPreviewLabel,
  isWakePromptMessage,
  PROVIDER_BUFFERED_CONTINUATION_TEXT,
  resolveWakePromptPresentation,
} from "./wakePromptPresentation.ts";

const DELEGATED_TASK_ID =
  "node:delegated-task:command%3Amcp%3A60ee323f-f008-4b52-ae3f-7fdf19dbab11%3Adelegate-task%3Aretry-progress-precedent-20260824";

describe("wakePromptPresentation", () => {
  it("treats a server delegated-completion prompt as a work-log row", () => {
    expect(
      resolveWakePromptPresentation({
        createdBy: "agent",
        creationSource: "server",
        text: `Delegated task ${DELEGATED_TASK_ID} reached a terminal state. Use task_status with taskId ${DELEGATED_TASK_ID} to read the result.`,
      }),
    ).toEqual({
      kind: "delegated",
      heading: "Delegated task finished",
      preview: "retry-progress-precedent-20260824",
    });
  });

  it("counts several delegated task ids in the heading", () => {
    expect(
      resolveWakePromptPresentation({
        createdBy: "agent",
        creationSource: "server",
        text: "Delegated tasks task-a, task-b reached terminal states. Use task_status with each taskId to read the results.",
      }),
    ).toEqual({
      kind: "delegated",
      heading: "2 delegated tasks finished",
      preview: "task-a, task-b",
    });
  });

  it("treats Claude's buffered continuation text as a work-log row", () => {
    expect(
      resolveWakePromptPresentation({
        createdBy: "agent",
        creationSource: "provider",
        text: PROVIDER_BUFFERED_CONTINUATION_TEXT,
      }),
    ).toEqual({
      kind: "background",
      heading: "Background task finished",
      preview: null,
    });
  });

  it("leaves ordinary agent-to-agent user bubbles alone", () => {
    expect(
      isWakePromptMessage({
        createdBy: "agent",
        creationSource: "provider",
        text: "Review this area",
      }),
    ).toBe(false);
    expect(
      isWakePromptMessage({
        createdBy: "user",
        creationSource: "web",
        text: PROVIDER_BUFFERED_CONTINUATION_TEXT,
      }),
    ).toBe(false);
  });

  it("accepts delegatedCompletion metadata without the canned prompt text", () => {
    expect(
      isWakePromptMessage({
        createdBy: "agent",
        creationSource: "server",
        text: "custom wake",
        delegatedCompletion: { parentRunId: "run-1" },
      }),
    ).toBe(true);
  });

  it("uses the last decoded task-id segment as the preview label", () => {
    expect(delegatedTaskPreviewLabel(DELEGATED_TASK_ID)).toBe("retry-progress-precedent-20260824");
  });

  it("counts two delegated wake sentences glued into one message", () => {
    expect(
      resolveWakePromptPresentation({
        createdBy: "agent",
        creationSource: "server",
        text: `Delegated task task-a reached a terminal state. Use task_status with taskId task-a to read the result.Delegated task task-b reached a terminal state. Use task_status with taskId task-b to read the result.`,
      }),
    ).toEqual({
      kind: "delegated",
      heading: "2 delegated tasks finished",
      preview: "task-a, task-b",
    });
  });

  it("counts two canned background completions glued into one message", () => {
    expect(
      resolveWakePromptPresentation({
        createdBy: "agent",
        creationSource: "provider",
        text: `${PROVIDER_BUFFERED_CONTINUATION_TEXT}${PROVIDER_BUFFERED_CONTINUATION_TEXT}`,
      }),
    ).toEqual({
      kind: "background",
      heading: "2 background tasks finished",
      preview: null,
    });
  });

  it("treats a Codex background-command continuation as a work-log row", () => {
    expect(
      resolveWakePromptPresentation({
        createdBy: "agent",
        creationSource: "provider",
        text: "Background command completed (exit 0): sleep 4",
      }),
    ).toEqual({
      kind: "background",
      heading: "Background task finished",
      preview: null,
    });
  });
});
