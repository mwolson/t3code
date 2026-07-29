import { OPENCODE2_THREAD_DELETE_PROMPT, type OrchestratorFixtureInput } from "../shared.ts";

export function openCode2ThreadDeleteInput(): OrchestratorFixtureInput {
  return {
    steps: [{ type: "message", text: OPENCODE2_THREAD_DELETE_PROMPT }, { type: "delete" }],
  };
}
