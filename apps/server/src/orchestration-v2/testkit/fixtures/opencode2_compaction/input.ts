import {
  OPENCODE2_COMPACTION_INTERRUPT_PROMPT,
  OPENCODE2_COMPACTION_PROMPT,
  type OrchestratorFixtureInput,
} from "../shared.ts";

export function openCode2CompactionInput(): OrchestratorFixtureInput {
  return {
    steps: [
      { type: "message", text: OPENCODE2_COMPACTION_PROMPT },
      { type: "advance_clock", duration: "31 minutes" },
      { type: "await_provider_session_status", status: "stopped" },
      { type: "compact" },
      { type: "message", text: OPENCODE2_COMPACTION_INTERRUPT_PROMPT },
      { type: "interrupt", targetRunIndex: 2, waitForTurnItemType: "compaction" },
    ],
  };
}
