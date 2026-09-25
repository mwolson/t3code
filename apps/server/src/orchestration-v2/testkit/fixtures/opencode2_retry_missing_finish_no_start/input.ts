import {
  OPENCODE2_RETRY_MISSING_FINISH_NO_START_PROMPT,
  type OrchestratorFixtureInput,
} from "../shared.ts";

export function openCode2RetryMissingFinishNoStartInput(): OrchestratorFixtureInput {
  return {
    steps: [{ type: "message", text: OPENCODE2_RETRY_MISSING_FINISH_NO_START_PROMPT }],
  };
}
