import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertAssistantTextIncludes,
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  projectionFor,
} from "../shared.ts";

export function assertOpenCode2DeferredChildOverflowOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({ result, transcript, runCount: 1, runStatuses: ["completed"] });

  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertAssistantTextIncludes(projection, "PARENT_AFTER_OVERFLOW");
  const item = projection.turnItems.find((candidate) => candidate.type === "subagent");
  assert.strictEqual(item?.type, "subagent");
  if (item?.type !== "subagent") throw new Error("OpenCode 2 subagent item is missing");
  // The stop step inside the open execution must not complete the child; the
  // synthetic overflow failure settles it.
  assert.strictEqual(item.status, "failed");
  assert.strictEqual(projection.subagents[0]?.status, "failed");
  assert.isNotNull(item.childThreadId);
  const child = result.projections.get(item.childThreadId!);
  assert.isDefined(child);
  const failures = child!.turnItems.filter((candidate) => candidate.type === "error");
  assert.lengthOf(failures, 1);
  const failure = failures[0];
  if (failure?.type !== "error") throw new Error("OpenCode 2 child failure item is missing");
  assert.strictEqual(failure.status, "failed");
  assert.strictEqual(failure.failure.code, "provider.error");
  assert.isTrue(
    child!.turnItems.some((candidate) => candidate.type === "reasoning"),
    "the retained prefix replays into the child",
  );
}
