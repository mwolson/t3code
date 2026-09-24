import { claudeBackgroundWakeResultLabel } from "../../../Adapters/ClaudeAdapterV2.testkit.ts";
import {
  CLAUDE_BACKGROUND_TASK_WAKE_FOLLOW_UP_PROMPT,
  CLAUDE_BACKGROUND_TASK_WAKE_PROMPT,
} from "../claude_background_task_wake/input.ts";
import type { OrchestratorFixtureInput } from "../shared.ts";

// Reorder the recorded user reply before the wake, as in the incident.
export function claudeBackgroundTaskWakeAfterUserInput(): OrchestratorFixtureInput {
  return {
    steps: [
      { type: "message", text: CLAUDE_BACKGROUND_TASK_WAKE_PROMPT },
      { type: "await_run_status", targetRunIndex: 1, status: "completed" },
      { type: "message", text: CLAUDE_BACKGROUND_TASK_WAKE_FOLLOW_UP_PROMPT },
      { type: "await_run_status", targetRunIndex: 2, status: "completed" },
      { type: "capture_shell_snapshot", key: "intervening-user-complete" },
      { type: "release_replay_gate", label: "notification:background-wake:1" },
      { type: "await_run_status", targetRunIndex: 3, status: "running" },
      { type: "capture_shell_snapshot", key: "wake-before-output" },
      { type: "release_replay_gate", label: "output:background-wake:1" },
      {
        type: "await_run_status",
        targetRunIndex: 3,
        status: "running",
        waitForTurnItemType: "assistant_message",
      },
      { type: "release_replay_gate", label: claudeBackgroundWakeResultLabel(1) },
      { type: "await_run_status", targetRunIndex: 3, status: "completed" },
    ],
  };
}
