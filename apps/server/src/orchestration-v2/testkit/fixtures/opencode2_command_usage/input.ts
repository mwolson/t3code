import type { OrchestratorFixtureInput } from "../shared.ts";

export function openCode2CommandUsageInput(): OrchestratorFixtureInput {
  return { steps: [{ type: "message", text: "/review staged changes" }] };
}
