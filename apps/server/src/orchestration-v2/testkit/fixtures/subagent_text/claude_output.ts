import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertExecutionNodeKinds,
  assertNoExtraAppRunsForProviderChildren,
  assertRunProviderTurnCardinality,
  assertSemanticProjectionIntegrity,
  assertTurnItemTypes,
  assertUserMessagesInclude,
  projectionFor,
  SUBAGENT_TEXT_FINAL_MARKER,
  SUBAGENT_TEXT_NARRATION_MARKER,
  SUBAGENT_TEXT_PARENT_MARKER,
  SUBAGENT_TEXT_PROMPT,
} from "../shared.ts";

export function assertClaudeSubagentTextOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({
    result,
    transcript,
    runCount: 1,
    runStatuses: ["completed"],
  });

  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertExecutionNodeKinds(projection, ["root_turn", "subagent"]);
  assertTurnItemTypes(projection, ["user_message", "subagent", "assistant_message"]);
  assertRunProviderTurnCardinality({ projection, rootRunCount: 1 });
  assertNoExtraAppRunsForProviderChildren({ projection, expectedAppRuns: 1 });
  assertUserMessagesInclude(projection, [SUBAGENT_TEXT_PROMPT]);

  // The regression this fixture guards: the SDK forwards the subagent's
  // narration and final answer into the parent query stream tagged with
  // parent_tool_use_id, and none of it may render as parent assistant prose.
  const parentAssistantItems = projection.turnItems.filter(
    (item) => item.type === "assistant_message",
  );
  assert.lengthOf(
    parentAssistantItems,
    1,
    "the parent thread must contain only the parent's own assistant message",
  );
  assert.equal(parentAssistantItems[0]?.text, SUBAGENT_TEXT_PARENT_MARKER);

  assert.lengthOf(projection.subagents, 1);
  assert.lengthOf(result.shellSnapshot.threads, 2);
  const subagent = projection.subagents[0]!;
  assert.equal(subagent.origin, "provider_native");
  assert.equal(subagent.createdBy, "agent");
  assert.equal(subagent.driver, "claudeAgent");
  assert.equal(subagent.status, "completed");
  assert.equal(subagent.result, SUBAGENT_TEXT_FINAL_MARKER);
  assert.isNotNull(subagent.childThreadId);
  assert.isNotNull(subagent.completedAt);
  if (subagent.childThreadId === null) {
    throw new Error(`Subagent ${subagent.id} is missing its child thread`);
  }

  const childProjection = result.projections.get(subagent.childThreadId);
  assert.isDefined(childProjection);
  assert.equal(childProjection.thread.lineage.parentThreadId, projection.thread.id);
  assert.equal(childProjection.thread.lineage.relationshipToParent, "subagent");
  assertUserMessagesInclude(childProjection, [subagent.prompt]);
  assertTurnItemTypes(childProjection, ["user_message", "dynamic_tool", "assistant_message"]);

  // The streamed narration and the streamed final answer both land in the
  // child thread, in stream order around the tool call.
  const childAssistantItems = childProjection.turnItems
    .filter((item) => item.type === "assistant_message")
    .sort((left, right) => left.ordinal - right.ordinal);
  assert.deepEqual(
    childAssistantItems.map((item) => item.text),
    [SUBAGENT_TEXT_NARRATION_MARKER, SUBAGENT_TEXT_FINAL_MARKER],
    "child thread must contain exactly the streamed narration and final answer; " +
      "the task_notification summary must not append a duplicate result message",
  );
  const readItem = childProjection.turnItems.find((item) => item.type === "dynamic_tool");
  assert.isDefined(readItem, "the subagent's Read tool call must project into the child thread");
  const narrationItem = childAssistantItems[0]!;
  const finalItem = childAssistantItems[1]!;
  assert.isBelow(narrationItem.ordinal, readItem.ordinal);
  assert.isBelow(readItem.ordinal, finalItem.ordinal);

  const parentItem = projection.turnItems.find(
    (item) => item.type === "subagent" && item.subagentId === subagent.id,
  );
  assert.isDefined(parentItem, "parent thread must render the subagent lifecycle item");
}
