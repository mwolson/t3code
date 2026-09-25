import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  projectionFor,
} from "../shared.ts";

const EXPECTED_AUTHORIZATION_MESSAGE =
  "OpenCode 2 provider authorization failed (HTTP 401). Reconnect the provider in OpenCode, then retry.";

export function assertOpenCode2AuthorizationFailureOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({ result, transcript, runCount: 1, runStatuses: ["failed"] });

  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  const errorItem = projection.turnItems.find((item) => item.type === "error");
  assert.strictEqual(errorItem?.type, "error");
  if (errorItem?.type !== "error") throw new Error("OpenCode 2 failure item is missing");
  assert.strictEqual(errorItem.failure.message, EXPECTED_AUTHORIZATION_MESSAGE);
  assert.strictEqual(errorItem.failure.code, "Integration.Authorization");
  assert.strictEqual(errorItem.failure.class, "provider_error");
  assert.strictEqual(errorItem.failure.retryable, false);

  const errors = result.domainEvents.flatMap((event) =>
    event.type === "provider-session.updated" &&
    event.payload.lastError === EXPECTED_AUTHORIZATION_MESSAGE
      ? [event.payload]
      : [],
  );
  const first = errors[0];
  assert.isDefined(first, "the adapter must emit the session-selected error");
  if (first?.lastErrorAt == null) throw new Error("OpenCode session error occurrence is missing");
  assert.strictEqual(
    DateTime.toEpochMillis(first.lastErrorAt),
    DateTime.toEpochMillis(first.updatedAt),
  );
  for (const refreshed of errors) {
    assert.deepEqual(
      refreshed.lastErrorAt,
      first.lastErrorAt,
      "status refresh must preserve the occurrence",
    );
  }
  const retained = projection.providerSessions.find((session) => session.id === first.id);
  assert.deepEqual(
    retained?.lastErrorAt,
    first.lastErrorAt,
    "the occurrence must survive the JSON projection codec",
  );
}
