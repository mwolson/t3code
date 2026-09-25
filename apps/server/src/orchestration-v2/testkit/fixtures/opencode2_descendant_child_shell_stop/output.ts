import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  projectionFor,
} from "../shared.ts";

export function assertOpenCode2DescendantChildShellStopOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  // The strict transcript requires the child's shell removal after its interrupt.
  assertBaseProjection({ result, transcript, runCount: 1, runStatuses: ["interrupted"] });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  const item = projection.turnItems.find((candidate) => candidate.type === "subagent");
  assert.strictEqual(item?.type, "subagent");
  if (item?.type !== "subagent") throw new Error("OpenCode 2 subagent item is missing");
  assert.isNotNull(item.childThreadId);
  const child = result.projections.get(item.childThreadId!);
  assert.isDefined(child);
  const commands = child!.turnItems.filter((candidate) => candidate.type === "command_execution");
  assert.lengthOf(commands, 1);
  assert.notStrictEqual(commands[0]?.status, "running");
}
