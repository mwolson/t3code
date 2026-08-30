import { describe, expect, it } from "vite-plus/test";

import {
  backgroundCommandWakeCount,
  delegatedTaskPreviewLabel,
  isBackgroundCommandWakeMessage,
  isWakePromptMessage,
  joinWakePromptTexts,
  mergedBackgroundCommandWake,
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

  it("does not treat header-shaped text as a wake without agent plus provider attribution", () => {
    expect(
      isWakePromptMessage({
        text: "Background command completed (exit 0): sleep 1",
      }),
    ).toBe(false);
    expect(
      isWakePromptMessage({
        createdBy: "agent",
        creationSource: "mcp",
        text: "Background command completed (exit 0): sleep 1",
      }),
    ).toBe(false);
    expect(
      isBackgroundCommandWakeMessage({
        createdBy: "agent",
        creationSource: "mcp",
        text: "Background command completed (exit 0): sleep 1",
      }),
    ).toBe(false);
  });

  it("accepts providerWake metadata without the T3-authored header", () => {
    expect(
      isBackgroundCommandWakeMessage({
        createdBy: "agent",
        creationSource: "provider",
        text: "custom wake",
        providerWake: { kind: "background_command", count: 2 },
      }),
    ).toBe(true);
    expect(
      isWakePromptMessage({
        createdBy: "agent",
        creationSource: "provider",
        text: "custom wake",
        providerWake: { kind: "background_task", count: 1 },
      }),
    ).toBe(true);
    expect(
      isBackgroundCommandWakeMessage({
        createdBy: "agent",
        creationSource: "provider",
        text: "custom wake",
        providerWake: { kind: "background_task", count: 1 },
      }),
    ).toBe(false);
    expect(
      resolveWakePromptPresentation({
        createdBy: "agent",
        creationSource: "provider",
        text: "custom wake",
        providerWake: { kind: "background_command", count: 2 },
      }),
    ).toEqual({
      kind: "background",
      heading: "2 background tasks finished",
      preview: null,
    });
    expect(
      backgroundCommandWakeCount({
        createdBy: "agent",
        creationSource: "provider",
        text: "Background command completed (exit 0): sleep 1",
        providerWake: { kind: "background_command", count: 2 },
      }),
    ).toBe(2);
    expect(
      mergedBackgroundCommandWake([
        {
          createdBy: "agent",
          creationSource: "provider",
          text: "custom",
          providerWake: { kind: "background_command", count: 1 },
        },
        {
          createdBy: "agent",
          creationSource: "provider",
          text: "Background command completed (exit 0): sleep 1",
        },
      ]),
    ).toEqual({ kind: "background_command", count: 2 });
  });

  it("prefers delegated metadata when both wake kinds are present", () => {
    expect(
      resolveWakePromptPresentation({
        createdBy: "agent",
        creationSource: "provider",
        text: "custom wake",
        delegatedCompletion: { parentRunId: "run-1" },
        providerWake: { kind: "background_task", count: 1 },
      }),
    ).toEqual({
      kind: "delegated",
      heading: "Delegated task finished",
      preview: null,
    });
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
    expect(
      isWakePromptMessage({
        createdBy: "agent",
        creationSource: "provider",
        text: "Background command completed (exit 1): sleep 15; echo CODEX_BG_FAIL; exit 1\n\nOutput tail:\nCODEX_BG_FAIL",
      }),
    ).toBe(true);
  });

  it("distinguishes Codex command wakes from Claude canned background text", () => {
    expect(
      isBackgroundCommandWakeMessage({
        createdBy: "agent",
        creationSource: "provider",
        text: "Background command completed (exit 1): sleep 15; echo CODEX_BG_A; exit 1",
      }),
    ).toBe(true);
    expect(
      isBackgroundCommandWakeMessage({
        createdBy: "agent",
        creationSource: "provider",
        text: PROVIDER_BUFFERED_CONTINUATION_TEXT,
      }),
    ).toBe(false);
    expect(
      isBackgroundCommandWakeMessage({
        createdBy: "user",
        creationSource: "web",
        text: "Background command completed: sleep 20",
      }),
    ).toBe(false);
  });

  it("joins Codex command wakes without inventing a Claude canned line", () => {
    expect(
      joinWakePromptTexts([
        "Background command completed (exit 1): sleep 12; echo CODEX_BG_A; exit 1\n\nOutput tail:\nCODEX_BG_A",
        "Background command completed (exit 0): sleep 18; echo CODEX_BG_B\n\nOutput tail:\nCODEX_BG_B",
      ]),
    ).toBe(
      "Background command completed (exit 1): sleep 12; echo CODEX_BG_A; exit 1\n\nOutput tail:\nCODEX_BG_A\n\nBackground command completed (exit 0): sleep 18; echo CODEX_BG_B\n\nOutput tail:\nCODEX_BG_B",
    );
    expect(
      resolveWakePromptPresentation({
        createdBy: "agent",
        creationSource: "provider",
        text: joinWakePromptTexts([
          "Background command completed (exit 1): sleep 12",
          "Background command completed (exit 0): sleep 18",
        ]),
      }),
    ).toEqual({
      kind: "background",
      heading: "2 background tasks finished",
      preview: null,
    });
  });
});
