import {
  OPENCODE2_DESCENDANT_CHILD_SHELL_STOP_PROMPT,
  type OrchestratorFixtureInput,
} from "../shared.ts";

export function openCode2DescendantChildShellStopInput(): OrchestratorFixtureInput {
  return {
    steps: [
      { type: "message", text: OPENCODE2_DESCENDANT_CHILD_SHELL_STOP_PROMPT },
      { type: "interrupt", targetRunIndex: 1, waitForTurnItemType: "subagent" },
    ],
  };
}
