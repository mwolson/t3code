import { SUBAGENT_TEXT_PROMPT, type OrchestratorFixtureInput } from "../shared.ts";

export function subagentTextInput(): OrchestratorFixtureInput {
  return {
    steps: [{ type: "message", text: SUBAGENT_TEXT_PROMPT }],
  };
}
