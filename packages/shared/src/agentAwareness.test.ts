import { describe, expect, it } from "@effect/vitest";

import type {
  EnvironmentId,
  OrchestrationV2ThreadShell,
  Project,
  ThreadId,
} from "@t3tools/contracts";
import { ProviderInstanceId, RuntimeRequestId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

import { projectThreadAwarenessV2 } from "./agentAwareness.ts";

const NOW = "2026-05-22T12:00:00.000Z";

const project = {
  title: "t3code",
} satisfies Pick<Project, "title">;

describe("projectThreadAwarenessV2", () => {
  const updatedAt = DateTime.makeUnsafe(NOW);
  const v2Thread = (
    overrides: Partial<
      Pick<
        OrchestrationV2ThreadShell,
        | "delegatedFrom"
        | "activityRunStatus"
        | "latestRunStopRequested"
        | "status"
        | "pendingBackgroundTasks"
        | "pendingRuntimeRequest"
        | "lineage"
        | "settledOverride"
      >
    > = {},
  ) => ({
    createdBy: "agent" as const,
    creationSource: "mcp" as const,
    id: "thread-2" as ThreadId,
    lineage: {
      rootThreadId: "thread-2" as ThreadId,
      parentThreadId: null,
      relationshipToParent: null,
    },
    title: "Integrate orchestration",
    modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
    status: "running" as const,
    pendingRuntimeRequest: null,
    settledOverride: null,
    updatedAt,
    ...overrides,
  });

  it("announces completion only for an unstopped top-level thread", () => {
    const input = {
      environmentId: "env-1" as EnvironmentId,
      project,
      thread: v2Thread({ status: "completed" }),
    };
    expect(projectThreadAwarenessV2(input)?.phase).toBe("completed");
    const delegated = {
      ...input,
      thread: {
        ...input.thread,
        delegatedFrom: {
          environmentId: "parent-env" as EnvironmentId,
          threadId: "parent-thread" as ThreadId,
          title: "Parent",
        },
      },
    };
    expect(projectThreadAwarenessV2(delegated)).toBeNull();
    expect(
      projectThreadAwarenessV2({
        ...delegated,
        thread: { ...delegated.thread, status: "failed" },
      })?.phase,
    ).toBe("failed");
    expect(
      projectThreadAwarenessV2({
        ...input,
        thread: { ...input.thread, latestRunStopRequested: true },
      }),
    ).toBeNull();
  });

  it.each([
    ["running", "subagent"],
    ["completed", "subagent"],
    ["failed", "subagent"],
    ["running", "side chat"],
  ] as const)("does not publish %s %s activity", (status, kind) => {
    expect(
      projectThreadAwarenessV2({
        environmentId: "env-1" as EnvironmentId,
        project,
        thread: v2Thread({
          status,
          lineage: {
            rootThreadId: "parent" as ThreadId,
            parentThreadId: "parent" as ThreadId,
            ...(kind === "subagent"
              ? { relationshipToParent: "subagent" }
              : { relationshipToParent: "fork", sideChat: true }),
          },
        }),
      }),
    ).toBeNull();
  });

  it("keeps a delegated question quiet while agent-created top-level questions notify", () => {
    const request = {
      id: RuntimeRequestId.make("question"),
      kind: "user_input" as const,
      createdAt: updatedAt,
    };
    const input = {
      environmentId: "env-1" as EnvironmentId,
      project,
      thread: v2Thread({ pendingRuntimeRequest: request }),
    };
    expect(projectThreadAwarenessV2(input)?.phase).toBe("waiting_for_input");
    const delegated = {
      ...input.thread,
      delegatedFrom: {
        environmentId: "elsewhere" as EnvironmentId,
        threadId: "parent" as ThreadId,
        title: "Parent",
      },
    };
    expect(projectThreadAwarenessV2({ ...input, thread: delegated })?.phase).toBe("running");
    expect(
      projectThreadAwarenessV2({ ...input, thread: { ...delegated, status: "completed" } }),
    ).toBeNull();
    expect(
      projectThreadAwarenessV2({ ...input, thread: { ...delegated, status: "failed" } })?.phase,
    ).toBe("failed");
    expect(
      projectThreadAwarenessV2({
        ...input,
        thread: { ...delegated, pendingRuntimeRequest: { ...request, kind: "command" } },
      })?.phase,
    ).toBe("waiting_for_approval");
  });

  it("keeps an older activity run visible over a newer cancelled run", () => {
    expect(
      projectThreadAwarenessV2({
        environmentId: "env-1" as EnvironmentId,
        project,
        thread: v2Thread({ status: "cancelled", activityRunStatus: "running" }),
      }),
    ).toMatchObject({ phase: "running", headline: "Agent is working" });
  });

  it.each(["completed", "failed"] as const)("removes settled %s work", (status) => {
    expect(
      projectThreadAwarenessV2({
        environmentId: "env-1" as EnvironmentId,
        project,
        thread: v2Thread({ status, settledOverride: "settled" }),
      }),
    ).toBeNull();
  });

  it.each(["starting", "running", "waiting"] as const)(
    "keeps settled %s work visible",
    (status) => {
      expect(
        projectThreadAwarenessV2({
          environmentId: "env-1" as EnvironmentId,
          project,
          thread: v2Thread({ status, settledOverride: "settled" }),
        }),
      ).toMatchObject({ phase: status === "starting" ? "starting" : "running" });
    },
  );

  it.each(["user_input", "command"] as const)(
    "keeps a settled terminal thread with a pending %s request",
    (kind) => {
      expect(
        projectThreadAwarenessV2({
          environmentId: "env-1" as EnvironmentId,
          project,
          thread: v2Thread({
            status: "completed",
            settledOverride: "settled",
            pendingRuntimeRequest: {
              id: RuntimeRequestId.make("request-settled"),
              kind,
              createdAt: updatedAt,
            },
          }),
        }),
      ).toMatchObject({
        phase: kind === "user_input" ? "waiting_for_input" : "waiting_for_approval",
      });
    },
  );

  it("keeps settled work held open by an awaited monitor", () => {
    expect(
      projectThreadAwarenessV2({
        environmentId: "env-1" as EnvironmentId,
        project,
        thread: v2Thread({
          status: "completed",
          settledOverride: "settled",
          pendingBackgroundTasks: [{ taskId: "watch", kind: "monitor", awaited: true }],
        }),
      }),
    ).toMatchObject({ phase: "running" });
  });

  it.each([
    ["only a dev server", "completed", [{ taskId: "dev", kind: "command" }]],
    ["a dev server's watchdog monitor", "completed", [{ taskId: "watch", kind: "monitor" }]],
    [
      "a command the agent waits on",
      "running",
      [{ taskId: "tests", kind: "command", awaited: true }],
    ],
    [
      "a dev server and a subagent",
      "running",
      [
        { taskId: "dev", kind: "command" },
        { taskId: "review", kind: "subagent" },
      ],
    ],
  ] as const)("reports a completed run waiting on %s as %s", (_case, phase, tasks) => {
    expect(
      projectThreadAwarenessV2({
        environmentId: "env-1" as EnvironmentId,
        project,
        thread: v2Thread({ status: "completed", pendingBackgroundTasks: tasks }),
      }),
    ).toMatchObject({ phase });
  });

  it("reports an async question as input while the run keeps working", () => {
    const input = {
      environmentId: "env-1" as EnvironmentId,
      project,
      thread: v2Thread({
        pendingRuntimeRequest: {
          id: RuntimeRequestId.make("request-async"),
          kind: "user_input",
          createdAt: updatedAt,
          blocking: false,
        },
      }),
    };
    expect(projectThreadAwarenessV2(input)).toMatchObject({
      phase: "waiting_for_input",
      headline: "Waiting for input",
    });
  });

  it("does not present authentication refreshes as user approvals", () => {
    expect(
      projectThreadAwarenessV2({
        environmentId: "env-1" as EnvironmentId,
        project,
        thread: v2Thread({
          pendingRuntimeRequest: {
            id: RuntimeRequestId.make("request-auth-refresh"),
            kind: "auth_refresh",
            createdAt: updatedAt,
          },
        }),
      }),
    ).toMatchObject({ phase: "running", headline: "Agent is working" });
  });
});
