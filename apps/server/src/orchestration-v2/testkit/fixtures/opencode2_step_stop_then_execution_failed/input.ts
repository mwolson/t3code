import {
  OPENCODE2_STEP_STOP_THEN_EXECUTION_FAILED_PROMPT,
  type OrchestratorFixtureInput,
} from "../shared.ts";

export function openCode2StepStopThenExecutionFailedInput(): OrchestratorFixtureInput {
  return {
    steps: [{ type: "message", text: OPENCODE2_STEP_STOP_THEN_EXECUTION_FAILED_PROMPT }],
  };
}
