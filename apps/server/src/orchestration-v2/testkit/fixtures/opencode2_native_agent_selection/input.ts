import {
  MULTI_TURN_FIRST_PROMPT,
  MULTI_TURN_SECOND_PROMPT,
  type OrchestratorFixtureInput,
} from "../shared.ts";

export function nativeAgentSelectionInput(): OrchestratorFixtureInput {
  return {
    steps: [
      { type: "message", text: MULTI_TURN_FIRST_PROMPT },
      {
        type: "await_mode_reflection",
        nativeThreadId: "ses_opencode2_multi_turn",
        nativeSequence: 21,
      },
      { type: "release_replay_gate", label: "session.execution.succeeded.first" },
      { type: "await_run_status", targetRunIndex: 1, status: "completed" },
      { type: "message", text: MULTI_TURN_SECOND_PROMPT },
    ],
  };
}
