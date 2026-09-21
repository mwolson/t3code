import type { OrchestratorFixtureInput } from "../shared.ts";
export function openCode2ModeReflectionInput(): OrchestratorFixtureInput {
  return {
    interactionMode: "plan",
    steps: [
      { type: "message", text: "Respond with exactly: first fixture turn complete" },
      { type: "await_interaction_mode", interactionMode: "default" },
    ],
  };
}
