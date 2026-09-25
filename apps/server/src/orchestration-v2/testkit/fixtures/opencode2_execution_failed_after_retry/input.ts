import {
  OPENCODE2_EXECUTION_FAILED_AFTER_RETRY_PROMPT,
  type OrchestratorFixtureInput,
} from "../shared.ts";

export function openCode2ExecutionFailedAfterRetryInput(): OrchestratorFixtureInput {
  return {
    steps: [{ type: "message", text: OPENCODE2_EXECUTION_FAILED_AFTER_RETRY_PROMPT }],
  };
}
