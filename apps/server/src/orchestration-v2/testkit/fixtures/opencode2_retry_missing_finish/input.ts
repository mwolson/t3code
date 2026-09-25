import { OPENCODE2_RETRY_MISSING_FINISH_PROMPT, type OrchestratorFixtureInput } from "../shared.ts";

export function openCode2RetryMissingFinishInput(): OrchestratorFixtureInput {
  return {
    steps: [{ type: "message", text: OPENCODE2_RETRY_MISSING_FINISH_PROMPT }],
  };
}
