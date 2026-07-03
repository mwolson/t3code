import { it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import { describe, expect } from "vite-plus/test";

import {
  extractXAiAcpBackgroundToolMutation,
  extractXAiAcpSubagentUpdate,
  extractXAiAskUserQuestions,
  extractXAiBackgroundTaskCompletion,
  extractXAiMonitorTaskId,
  isGenericAcpToolTitle,
  isXAiMonitorTool,
  makeXAiAskUserQuestionCancelledResponse,
  makeXAiAskUserQuestionResponse,
  makeXAiPromptCompletionRuntime,
  normalizeXAiAcpToolCallState,
  resolveXAiAcpToolTitle,
  xAiPromptCompleteFromSessionUpdate,
  XAiAskUserQuestionRequest,
} from "./XAiAcpExtension.ts";
import * as AcpSessionRuntime from "./AcpSessionRuntime.ts";

const decodeXAiAskUserQuestionRequest = Schema.decodeUnknownSync(XAiAskUserQuestionRequest);

describe("xAiPromptCompleteFromSessionUpdate", () => {
  it("maps live turn_completed snake_case payloads", () => {
    expect(
      xAiPromptCompleteFromSessionUpdate({
        sessionId: "019f4428-4bf1-7e52-b7c6-c29b506543b1",
        update: {
          sessionUpdate: "turn_completed",
          prompt_id: "t3-xai-prompt-1",
          stop_reason: "end_turn",
        },
      }),
    ).toEqual({
      sessionId: "019f4428-4bf1-7e52-b7c6-c29b506543b1",
      promptId: "t3-xai-prompt-1",
      stopReason: "end_turn",
    });
  });

  it("ignores non-turn updates and task-completed prompt ids", () => {
    expect(
      xAiPromptCompleteFromSessionUpdate({
        sessionId: "root",
        update: { sessionUpdate: "hook_execution", prompt_id: "t3-xai-prompt-1" },
      }),
    ).toBeNull();
    expect(
      xAiPromptCompleteFromSessionUpdate({
        sessionId: "root",
        update: {
          sessionUpdate: "turn_completed",
          prompt_id: "task-completed-call-abc",
          stop_reason: "end_turn",
        },
      }),
    ).toBeNull();
    expect(
      xAiPromptCompleteFromSessionUpdate({
        sessionId: "root",
        update: { sessionUpdate: "turn_completed", stop_reason: "end_turn" },
      }),
    ).toBeNull();
  });
});

describe("XAiAcpExtension", () => {
  it("recognizes Grok Task starts as native subagents", () => {
    expect(
      extractXAiAcpSubagentUpdate({
        toolCallId: "task-1",
        title: "Task",
        status: "inProgress",
        data: {
          rawInput: {
            description: "Explore server architecture",
            prompt: "Audit apps/server.",
            subagent_type: "generalPurpose",
            model: "composer-2.5-fast",
          },
        },
      }),
    ).toEqual({
      nativeTaskId: "task-1",
      prompt: "Audit apps/server.",
      title: "Explore server architecture",
      model: "composer-2.5-fast",
      status: "running",
      childSessionId: null,
      result: null,
      suppressNormalTool: true,
    });
  });

  it("extracts Grok child session lineage from completed Task output", () => {
    expect(
      extractXAiAcpSubagentUpdate({
        toolCallId: "task-1",
        title: "Task",
        status: "completed",
        data: {
          rawInput: {
            description: "Explore server architecture",
            prompt: "Audit apps/server.",
            subagent_type: "generalPurpose",
          },
          rawOutput: {
            type: "Text",
            text: [
              "Server audit complete.",
              "",
              "Agent ID: 019f0220-e192-7c41-9e9d-b406bc3459c8 (resume supported)",
            ].join("\n"),
          },
        },
      }),
    ).toEqual({
      nativeTaskId: "task-1",
      prompt: "Audit apps/server.",
      title: "Explore server architecture",
      model: null,
      status: "completed",
      childSessionId: "019f0220-e192-7c41-9e9d-b406bc3459c8",
      result: "Server audit complete.",
      suppressNormalTool: true,
    });
  });

  it("keeps async spawn_subagent ACKs running and binds subagent_id", () => {
    expect(
      extractXAiAcpSubagentUpdate({
        toolCallId: "call-spawn-1",
        title: "spawn_subagent",
        status: "completed",
        data: {
          rawInput: {
            description: "Demo subagent wait and ls",
            prompt: "Wait then ls.",
            subagent_type: "general-purpose",
          },
          rawOutput: {
            type: "Text",
            text: [
              "Subagent started in background.",
              "subagent_id: 019f44a6-4820-7402-925d-bc862ee711dd",
              "type: general-purpose",
              "description: Demo subagent wait and ls",
              "",
              'Use get_command_or_subagent_output with task_ids=["019f44a6-4820-7402-925d-bc862ee711dd"] and timeout_ms to wait for results.',
            ].join("\n"),
          },
        },
      }),
    ).toEqual({
      nativeTaskId: "call-spawn-1",
      prompt: "Wait then ls.",
      title: "Demo subagent wait and ls",
      model: null,
      status: "running",
      childSessionId: "019f44a6-4820-7402-925d-bc862ee711dd",
      result: null,
      suppressNormalTool: true,
    });
  });

  it("hydrates subagent completion from get_command_or_subagent_output", () => {
    expect(
      extractXAiAcpSubagentUpdate({
        toolCallId: "call-get-1",
        title: "get_command_or_subagent_output",
        status: "completed",
        data: {
          rawInput: {
            task_ids: ["019f44a6-4820-7402-925d-bc862ee711dd"],
            timeout_ms: 30000,
          },
          rawOutput: {
            type: "Text",
            text: [
              "=== Task 019f44a6-4820-7402-925d-bc862ee711dd ===",
              "Status: completed",
              "Exit Code: 0",
              "",
              "=== Output ===",
              "SUBAGENT_MARKER: 35 entries",
            ].join("\n"),
          },
        },
      }),
    ).toEqual({
      nativeTaskId: "call-get-1",
      prompt: "",
      title: null,
      model: null,
      status: "completed",
      childSessionId: "019f44a6-4820-7402-925d-bc862ee711dd",
      result: "SUBAGENT_MARKER: 35 entries",
      suppressNormalTool: true,
    });
  });

  it("hydrates structured ACP TaskOutput tool envelopes", () => {
    expect(
      extractXAiAcpSubagentUpdate({
        toolCallId: "call-get-2",
        title: "[subagent:general-purpose] Sleep then return SUBAGENT_DONE (019f44b0)",
        status: "completed",
        data: {
          rawInput: {
            variant: "TaskOutput",
            task_ids: ["019f44b0-1b73-7f32-bb1b-1ff696f536e3"],
            timeout_ms: 20000,
          },
          rawOutput: {
            type: "TaskOutput",
            Result: {
              task_id: "019f44b0-1b73-7f32-bb1b-1ff696f536e3",
              status: "completed",
              exit_code: 0,
              output:
                "SUBAGENT_DONE\n\n<subagent_meta>id=019f44b0-1b73-7f32-bb1b-1ff696f536e3</subagent_meta>\n",
            },
          },
        },
      }),
    ).toEqual({
      nativeTaskId: "call-get-2",
      prompt: "",
      title: null,
      model: null,
      status: "completed",
      childSessionId: "019f44b0-1b73-7f32-bb1b-1ff696f536e3",
      result: "SUBAGENT_DONE",
      suppressNormalTool: true,
    });
  });

  it("keeps monitor start ACKs running and extracts task ids", () => {
    const toolCall = {
      toolCallId: "call-mon-1",
      title: "monitor",
      status: "completed" as const,
      data: {
        rawInput: {
          command: "echo mon_line_1",
          description: "Demo monitor",
        },
        rawOutput: {
          type: "Text",
          text: "Monitor started (task 019f44a5-87d1-7640-8e35-6a4667ffc873, timeout 36000000ms).\nYou will be notified on each event.",
        },
      },
    };
    expect(normalizeXAiAcpToolCallState(toolCall).status).toBe("inProgress");
    expect(extractXAiMonitorTaskId(toolCall)).toBe("019f44a5-87d1-7640-8e35-6a4667ffc873");
  });

  it("completes Monitor variant tools from structured Bash exit codes", () => {
    const toolCall = {
      toolCallId: "call-mon-2",
      title: "Tool",
      status: "inProgress" as const,
      data: {
        rawInput: {
          variant: "Monitor",
          command: "echo MON_DONE",
          description: "Stream mon lines",
        },
        rawOutput: {
          type: "Bash",
          output: Array.from(new TextEncoder().encode("mon_line_1\nMON_DONE\n")),
          output_for_prompt: "mon_line_1\nMON_DONE\n",
          exit_code: 0,
        },
      },
    };
    expect(isXAiMonitorTool(toolCall)).toBe(true);
    expect(normalizeXAiAcpToolCallState(toolCall).status).toBe("completed");
  });

  it("keeps structured Monitor start ACKs running and extracts taskId", () => {
    const toolCall = {
      toolCallId: "call-mon-3",
      title: "Start monitor: Wait 30s then list directory",
      status: "completed" as const,
      data: {
        rawInput: {
          variant: "Monitor",
          command: "sleep 30 && ls",
          description: "Wait 30s then list directory",
        },
        rawOutput: {
          type: "Monitor",
          taskId: "019f44b8-8e98-7c80-a40e-df1e26a5f9e3",
          timeoutMs: 36000000,
          persistent: false,
        },
      },
    };
    expect(normalizeXAiAcpToolCallState(toolCall).status).toBe("inProgress");
    expect(extractXAiMonitorTaskId(toolCall)).toBe("019f44b8-8e98-7c80-a40e-df1e26a5f9e3");
  });

  it("replaces generic ACP titles with description / Monitor labels", () => {
    expect(isGenericAcpToolTitle("Tool")).toBe(true);
    expect(isGenericAcpToolTitle("Read package.json")).toBe(false);
    expect(
      resolveXAiAcpToolTitle({
        toolCallId: "call-mon-title",
        title: "Tool",
        status: "completed",
        data: {
          rawInput: {
            variant: "Monitor",
            command: "sleep 30 && ls",
            description: "Wait 30s then list directory",
          },
        },
      }),
    ).toBe("Monitor: Wait 30s then list directory");
    const normalized = normalizeXAiAcpToolCallState({
      toolCallId: "call-mon-title",
      title: "Tool",
      status: "completed",
      data: {
        rawInput: {
          variant: "Monitor",
          command: "sleep 30 && ls",
          description: "Wait 30s then list directory",
        },
        rawOutput: {
          type: "Monitor",
          taskId: "019f44b8-8e98-7c80-a40e-df1e26a5f9e3",
          timeoutMs: 36000000,
          persistent: false,
        },
      },
    });
    expect(normalized.title).toBe("Monitor: Wait 30s then list directory");
    expect(normalized.status).toBe("inProgress");
    // Non-generic titles from the CLI are preserved.
    expect(
      resolveXAiAcpToolTitle({
        toolCallId: "call-read",
        title: "Read package.json",
        status: "inProgress",
        data: { rawInput: { path: "package.json" } },
      }),
    ).toBe("Read package.json");
  });

  it("hydrates monitor completion from TaskOutput get_command envelopes", () => {
    expect(
      extractXAiBackgroundTaskCompletion({
        toolCallId: "call-get-mon",
        title: "Wait 30s then list directory",
        status: "completed",
        data: {
          rawInput: {
            variant: "TaskOutput",
            task_ids: ["019f44b8-8e98-7c80-a40e-df1e26a5f9e3"],
          },
          rawOutput: {
            type: "TaskOutput",
            Result: {
              task_id: "019f44b8-8e98-7c80-a40e-df1e26a5f9e3",
              command: "[monitor] Wait 30s then list directory",
              status: "completed",
              exit_code: 0,
              output: "agents\nAGENTS.md\nnotes\n",
            },
          },
        },
      }),
    ).toEqual({
      taskId: "019f44b8-8e98-7c80-a40e-df1e26a5f9e3",
      status: "completed",
      appendOutput: "agents\nAGENTS.md\nnotes",
    });
  });

  it("parses monitor event lines and end reminders", () => {
    expect(
      extractXAiAcpBackgroundToolMutation(
        '<monitor-event task_id="019f44a5-87d1-7640-8e35-6a4667ffc873">\n[Demo] mon_line_1\n</monitor-event>',
      ),
    ).toEqual({
      taskId: "019f44a5-87d1-7640-8e35-6a4667ffc873",
      status: "running",
      appendOutput: "[Demo] mon_line_1\n",
    });
    expect(
      extractXAiAcpBackgroundToolMutation(
        [
          "<system-reminder>",
          'Monitor "019f44a5-87d1-7640-8e35-6a4667ffc873" ended: [monitor ended: exited (code 0)].',
          "Description: Demo monitor",
          "</system-reminder>",
        ].join("\n"),
      ),
    ).toMatchObject({
      taskId: "019f44a5-87d1-7640-8e35-6a4667ffc873",
      status: "completed",
    });
  });

  it("extracts questions from the real xAI ask_user_question payload shape", () => {
    const questions = extractXAiAskUserQuestions({
      sessionId: "session-1",
      toolCallId: "tool-call-1",
      mode: "default",
      questions: [
        {
          id: "scope",
          question: "Which scope should Grok use?",
          options: [
            { label: "Workspace", description: "Use the current workspace" },
            { label: "Session", description: "Only use this session" },
          ],
        },
      ],
    });

    expect(questions).toEqual([
      {
        id: "scope",
        header: "Question",
        question: "Which scope should Grok use?",
        multiSelect: false,
        options: [
          { label: "Workspace", description: "Use the current workspace" },
          { label: "Session", description: "Only use this session" },
        ],
      },
    ]);
  });

  it("extracts questions from wrapped _x.ai extension payloads", () => {
    const payload = {
      method: "_x.ai/ask_user_question",
      params: {
        sessionId: "session-1",
        toolCallId: "tool-call-1",
        mode: "plan",
        questions: [
          {
            question: "Which changes should be included?",
            multiSelect: true,
            options: [{ label: "Tests" }, { label: "Docs" }],
          },
        ],
      },
    };
    const decoded = decodeXAiAskUserQuestionRequest(payload);
    const questions = extractXAiAskUserQuestions(decoded);

    expect(questions).toEqual([
      {
        id: "Which changes should be included?",
        header: "Question",
        question: "Which changes should be included?",
        multiSelect: true,
        options: [
          { label: "Tests", description: "Tests" },
          { label: "Docs", description: "Docs" },
        ],
      },
    ]);
  });

  it("treats nullable multiSelect from Grok as single-select", () => {
    const questions = extractXAiAskUserQuestions({
      sessionId: "session-1",
      toolCallId: "tool-call-1",
      mode: "default",
      questions: [
        {
          question: "Which label should Grok use?",
          multiSelect: null,
          options: [
            { label: "Alpha", description: "Use the Alpha label" },
            { label: "Beta", description: "Use the Beta label" },
            { label: "Other", description: "Use the Other label" },
          ],
        },
      ],
    });

    expect(questions).toEqual([
      {
        id: "Which label should Grok use?",
        header: "Question",
        question: "Which label should Grok use?",
        multiSelect: false,
        options: [
          { label: "Alpha", description: "Use the Alpha label" },
          { label: "Beta", description: "Use the Beta label" },
          { label: "Other", description: "Use the Other label" },
        ],
      },
    ]);
  });

  it("maps UI question ids back to xAI question text in accepted responses", () => {
    const response = makeXAiAskUserQuestionResponse(
      {
        sessionId: "session-1",
        toolCallId: "tool-call-1",
        mode: "default",
        questions: [
          {
            id: "scope",
            question: "Which scope should Grok use?",
            options: [
              { label: "workspace", description: "Use the current workspace" },
              { label: "session", description: "Only use this session" },
            ],
          },
        ],
      },
      { scope: "workspace" },
    );

    expect(response).toEqual({
      outcome: "accepted",
      answers: {
        "Which scope should Grok use?": ["workspace"],
      },
    });
  });

  it("orders accepted answers by the original xAI question order", () => {
    const response = makeXAiAskUserQuestionResponse(
      {
        sessionId: "session-1",
        toolCallId: "tool-call-1",
        mode: "default",
        questions: [
          {
            id: "first",
            question: "First question?",
            options: [{ label: "A", description: "A" }],
          },
          {
            id: "second",
            question: "Second question?",
            options: [{ label: "B", description: "B" }],
          },
        ],
      },
      {
        second: "B",
        first: "A",
      },
    );

    expect(Object.keys(response.answers)).toEqual(["First question?", "Second question?"]);
    expect(response).toMatchObject({
      outcome: "accepted",
      answers: {
        "First question?": ["A"],
        "Second question?": ["B"],
      },
    });
  });

  it("encodes typed custom answers as xAI Other annotations", () => {
    const response = makeXAiAskUserQuestionResponse(
      {
        method: "x.ai/ask_user_question",
        params: {
          sessionId: "session-1",
          toolCallId: "tool-call-1",
          mode: "default",
          questions: [
            {
              question: "Which ice cream flavor?",
              options: [
                { label: "vanilla", description: "Vanilla flavor" },
                { label: "chocolate", description: "Chocolate flavor" },
              ],
            },
          ],
        },
      },
      { "Which ice cream flavor?": "pistachio" },
    );

    expect(response).toEqual({
      outcome: "accepted",
      answers: {
        "Which ice cream flavor?": ["Other"],
      },
      annotations: {
        "Which ice cream flavor?": {
          notes: "pistachio",
        },
      },
    });
  });

  it("encodes interrupted dialogs as xAI cancelled responses", () => {
    expect(makeXAiAskUserQuestionCancelledResponse()).toEqual({
      outcome: "cancelled",
    });
  });

  it("does not echo preview annotations for multi-select answers", () => {
    const response = makeXAiAskUserQuestionResponse(
      {
        sessionId: "session-1",
        toolCallId: "tool-call-1",
        mode: "default",
        questions: [
          {
            question: "Which files should Grok touch?",
            multiSelect: true,
            options: [
              {
                label: "Tests",
                description: "Update tests",
                preview: "test preview",
              },
              {
                label: "Docs",
                description: "Update docs",
                preview: "docs preview",
              },
            ],
          },
        ],
      },
      { "Which files should Grok touch?": ["Tests", "Docs"] },
    );

    expect(response).toEqual({
      outcome: "accepted",
      answers: {
        "Which files should Grok touch?": ["Tests", "Docs"],
      },
    });
  });

  it.effect("settles a hung prompt from a root-session prompt_complete notification", () =>
    Effect.gen(function* () {
      const handlers = new Map<string, (notification: unknown) => Effect.Effect<void>>();
      const hungPrompt = yield* Deferred.make<never>();
      const baseRuntime = {
        start: () =>
          Effect.succeed({
            sessionId: "root-session",
            initializeResult: {},
            sessionSetupResult: {},
            modelConfigId: undefined,
          }),
        prompt: () => Deferred.await(hungPrompt),
        cancel: Effect.void,
        handleExtNotification: (
          method: string,
          _schema: unknown,
          handler: (notification: unknown) => Effect.Effect<void>,
        ) => {
          handlers.set(method, handler);
          return Effect.void;
        },
        handleExtRequest: () => Effect.void,
      } as unknown as AcpSessionRuntime.AcpSessionRuntime["Service"];

      const runtime = yield* makeXAiPromptCompletionRuntime(baseRuntime);
      const promptFiber = yield* runtime
        .prompt({ prompt: [{ type: "text", text: "hi" }] })
        .pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      const promptCompleteHandler = handlers.get("_x.ai/session/prompt_complete");
      expect(promptCompleteHandler).toBeDefined();
      yield* promptCompleteHandler!({
        sessionId: "root-session",
        stopReason: "end_turn",
      });
      const response = yield* Fiber.join(promptFiber);
      expect(response.stopReason).toBe("end_turn");
    }),
  );

  it.effect("settles a hung prompt from _x.ai/session/update turn_completed", () =>
    Effect.gen(function* () {
      const handlers = new Map<string, (notification: unknown) => Effect.Effect<void>>();
      let capturedMeta: Record<string, unknown> | null | undefined;
      const hungPrompt = yield* Deferred.make<never>();
      const baseRuntime = {
        start: () =>
          Effect.succeed({
            sessionId: "root-session",
            initializeResult: {},
            sessionSetupResult: {},
            modelConfigId: undefined,
          }),
        prompt: (payload: { readonly _meta?: Record<string, unknown> | null }) => {
          capturedMeta = payload._meta ?? null;
          return Deferred.await(hungPrompt);
        },
        cancel: Effect.void,
        handleExtNotification: (
          method: string,
          _schema: unknown,
          handler: (notification: unknown) => Effect.Effect<void>,
        ) => {
          handlers.set(method, handler);
          return Effect.void;
        },
        handleExtRequest: () => Effect.void,
      } as unknown as AcpSessionRuntime.AcpSessionRuntime["Service"];

      const runtime = yield* makeXAiPromptCompletionRuntime(baseRuntime);
      const promptFiber = yield* runtime
        .prompt({ prompt: [{ type: "text", text: "hi" }] })
        .pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      const promptId = capturedMeta?.promptId;
      expect(typeof promptId).toBe("string");
      const sessionUpdateHandler = handlers.get("_x.ai/session/update");
      expect(sessionUpdateHandler).toBeDefined();
      yield* sessionUpdateHandler!({
        sessionId: "root-session",
        update: {
          sessionUpdate: "turn_completed",
          prompt_id: promptId,
          stop_reason: "end_turn",
        },
      });
      const response = yield* Fiber.join(promptFiber);
      expect(response.stopReason).toBe("end_turn");
    }),
  );

  it.effect("ignores turn_completed for non-pending prompt ids and task completions", () =>
    Effect.gen(function* () {
      const handlers = new Map<string, (notification: unknown) => Effect.Effect<void>>();
      const hungPrompt = yield* Deferred.make<never>();
      const baseRuntime = {
        start: () =>
          Effect.succeed({
            sessionId: "root-session",
            initializeResult: {},
            sessionSetupResult: {},
            modelConfigId: undefined,
          }),
        prompt: () => Deferred.await(hungPrompt),
        cancel: Effect.void,
        handleExtNotification: (
          method: string,
          _schema: unknown,
          handler: (notification: unknown) => Effect.Effect<void>,
        ) => {
          handlers.set(method, handler);
          return Effect.void;
        },
        handleExtRequest: () => Effect.void,
      } as unknown as AcpSessionRuntime.AcpSessionRuntime["Service"];

      const runtime = yield* makeXAiPromptCompletionRuntime(baseRuntime);
      const promptFiber = yield* runtime
        .prompt({ prompt: [{ type: "text", text: "hi" }] })
        .pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      const sessionUpdateHandler = handlers.get("_x.ai/session/update");
      expect(sessionUpdateHandler).toBeDefined();
      yield* sessionUpdateHandler!({
        sessionId: "root-session",
        update: {
          sessionUpdate: "turn_completed",
          prompt_id: "task-completed-call-abc",
          stop_reason: "end_turn",
        },
      });
      yield* sessionUpdateHandler!({
        sessionId: "root-session",
        update: {
          sessionUpdate: "turn_completed",
          prompt_id: "some-other-cli-turn",
          stop_reason: "end_turn",
        },
      });
      yield* Effect.yieldNow;
      expect(promptFiber.pollUnsafe()).toBeUndefined();
      yield* Fiber.interrupt(promptFiber);
    }),
  );

  it.effect("ignores prompt_complete notifications for foreign session ids", () =>
    Effect.gen(function* () {
      const handlers = new Map<string, (notification: unknown) => Effect.Effect<void>>();
      const hungPrompt = yield* Deferred.make<never>();
      const baseRuntime = {
        start: () =>
          Effect.succeed({
            sessionId: "root-session",
            initializeResult: {},
            sessionSetupResult: {},
            modelConfigId: undefined,
          }),
        prompt: () => Deferred.await(hungPrompt),
        cancel: Effect.void,
        handleExtNotification: (
          method: string,
          _schema: unknown,
          handler: (notification: unknown) => Effect.Effect<void>,
        ) => {
          handlers.set(method, handler);
          return Effect.void;
        },
        handleExtRequest: () => Effect.void,
      } as unknown as AcpSessionRuntime.AcpSessionRuntime["Service"];

      const runtime = yield* makeXAiPromptCompletionRuntime(baseRuntime);
      const promptFiber = yield* runtime
        .prompt({ prompt: [{ type: "text", text: "hi" }] })
        .pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      const promptCompleteHandler = handlers.get("_x.ai/session/prompt_complete");
      expect(promptCompleteHandler).toBeDefined();
      yield* promptCompleteHandler!({
        sessionId: "child-session",
      });
      yield* Effect.yieldNow;
      expect(promptFiber.pollUnsafe()).toBeUndefined();
      yield* Fiber.interrupt(promptFiber);
    }),
  );

  it.effect("injects promptId and requestId into prompt _meta", () =>
    Effect.gen(function* () {
      let capturedMeta: Record<string, unknown> | null | undefined;
      const baseRuntime = {
        start: () => Effect.succeed({ sessionId: "session-1" }),
        prompt: (payload: { readonly _meta?: Record<string, unknown> | null }) => {
          capturedMeta = payload._meta ?? null;
          return Effect.succeed({ stopReason: "end_turn" as const });
        },
        cancel: Effect.void,
        handleExtNotification: () => Effect.void,
        handleExtRequest: () => Effect.void,
      } as unknown as AcpSessionRuntime.AcpSessionRuntime["Service"];

      const runtime = yield* makeXAiPromptCompletionRuntime(baseRuntime);
      yield* runtime.prompt({ prompt: [{ type: "text", text: "hi" }] });

      expect(typeof capturedMeta?.promptId).toBe("string");
      expect(capturedMeta).toMatchObject({
        promptId: capturedMeta?.promptId,
        requestId: capturedMeta?.promptId,
      });
    }),
  );
});
