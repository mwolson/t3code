import {
  MULTI_TURN_FIRST_PROMPT,
  MULTI_TURN_SECOND_PROMPT,
  type OrchestratorFixtureInput,
} from "../shared.ts";

export function queuedModeReflectionUserAbaInput(): OrchestratorFixtureInput {
  return {
    interactionMode: "plan",
    steps: [
      { type: "message", text: MULTI_TURN_FIRST_PROMPT },
      { type: "queue_message", text: MULTI_TURN_SECOND_PROMPT },
      { type: "await_run_status", targetRunIndex: 2, status: "running" },
      { type: "interaction_mode", interactionMode: "default" },
      { type: "interaction_mode", interactionMode: "plan" },
      { type: "release_replay_gate", label: "native.build" },
    ],
  };
}

export function queuedModeReflectionInput(): OrchestratorFixtureInput {
  return {
    interactionMode: "plan",
    steps: [
      { type: "message", text: MULTI_TURN_FIRST_PROMPT },
      { type: "queue_message", text: MULTI_TURN_SECOND_PROMPT },
      { type: "interaction_mode", interactionMode: "default" },
      { type: "interaction_mode", interactionMode: "plan" },
      { type: "release_replay_gate", label: "session.execution.succeeded.first" },
      {
        type: "await_mode_reflection",
        nativeThreadId: "ses_opencode2_queued_turn",
        nativeSequence: 20,
      },
      { type: "release_replay_gate", label: "session.execution.succeeded.second" },
    ],
  };
}
