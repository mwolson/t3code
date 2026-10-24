import type { OrchestrationV2ThreadShell } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import type { AgentAwarenessState } from "./agentAwareness.ts";
import { agentAwarenessTransition } from "./personalRelay.ts";

const at = DateTime.makeUnsafe("2026-10-17T12:00:00.000Z");
const thread: Parameters<typeof agentAwarenessTransition>[0]["thread"] = {
  latestRunId: "run-1" as OrchestrationV2ThreadShell["latestRunId"],
  latestRunCompletedAt: at,
  createdAt: DateTime.makeUnsafe("2026-10-17T11:00:00.000Z"),
  pendingRuntimeRequest: null,
};
const state: AgentAwarenessState = {
  environmentId: "env" as AgentAwarenessState["environmentId"],
  threadId: "thread" as AgentAwarenessState["threadId"],
  phase: "completed",
  projectTitle: "Project",
  threadTitle: "Thread",
  headline: "Finished",
  modelTitle: "Model",
  updatedAt: DateTime.formatIso(at),
  deepLink: "/threads/env/thread",
};

describe("personal publish claims", () => {
  it("keeps run identity across title and projection changes, and separates the next completion", () => {
    const first = agentAwarenessTransition({ thread, state });
    expect(
      agentAwarenessTransition({
        thread,
        state: { ...state, threadTitle: "Renamed", updatedAt: "2026-10-17T13:00:00.000Z" },
      }),
    ).toEqual(first);
    expect(
      agentAwarenessTransition({
        thread: { ...thread, latestRunId: "run-2" as typeof thread.latestRunId },
        state,
      })?.id,
    ).not.toBe(first?.id);
    expect(agentAwarenessTransition({ thread: { ...thread, latestRunId: null }, state })?.id).toBe(
      `completed-at:${state.updatedAt}:completed`,
    );
  });
});
