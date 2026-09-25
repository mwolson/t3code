import {
  OPENCODE2_DEFERRED_CHILD_OVERFLOW_PROMPT,
  type OrchestratorFixtureInput,
} from "../shared.ts";

export function openCode2DeferredChildOverflowInput(): OrchestratorFixtureInput {
  return {
    steps: [{ type: "message", text: OPENCODE2_DEFERRED_CHILD_OVERFLOW_PROMPT }],
  };
}
