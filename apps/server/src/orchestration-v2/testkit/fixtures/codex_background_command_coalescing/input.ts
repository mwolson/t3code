import type { OrchestratorFixtureInput } from "../shared.ts";

export function codexBackgroundCommandCoalescingInput(): OrchestratorFixtureInput {
  return {
    steps: [
      { type: "message", text: "Start two background commands." },
      { type: "message", text: "Keep working while commands finish." },
      { type: "await_command_notice_updates", count: 2, distinctMessages: 1 },
      { type: "release_replay_gate", label: "after-coalescing" },
      { type: "await_run_status", targetRunIndex: 3, status: "completed" },
    ],
  };
}
