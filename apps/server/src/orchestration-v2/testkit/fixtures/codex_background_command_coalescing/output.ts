import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import { assertBaseProjection, projectionFor } from "../shared.ts";

export function assertCodexBackgroundCommandCoalescingOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({
    result,
    transcript,
    runCount: 3,
    runStatuses: ["completed", "completed", "completed"],
  });
  const projection = projectionFor(result, transcript.scenario);
  const [rootRun, , wakeRun] = projection.runs;
  const notices = projection.messages.filter(
    (message) => message.notification?.source.kind === "command",
  );
  assert.lengthOf(notices, 1);
  assert.equal(notices[0]?.id, wakeRun?.userMessageId);
  assert.include(notices[0]?.text, "ALPHA_OUTPUT");
  assert.include(notices[0]?.text, "BRAVO_OUTPUT");
  assert.deepEqual(notices[0]?.notification, {
    source: { kind: "command" },
    outcome: "failed",
    summary: "Background commands finished",
    detail: "echo ALPHA\n\necho BRAVO",
  });
  const outputs = projection.turnItems.filter((item) => item.type === "command_execution");
  assert.deepEqual(
    outputs.map((item) => [item.runId, item.output]),
    [
      [rootRun?.id, "ALPHA_OUTPUT\n"],
      [rootRun?.id, "BRAVO_OUTPUT\n"],
    ],
  );
}
