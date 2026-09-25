import { OPENCODE2_SHARED_LOCATION_PROMPT, type OrchestratorFixtureInput } from "../shared.ts";

export function openCode2SharedLocationUnrelatedSessionInput(): OrchestratorFixtureInput {
  return {
    steps: [{ type: "message", text: OPENCODE2_SHARED_LOCATION_PROMPT }],
  };
}
