import type { RelayAgentActivityState } from "@t3tools/contracts/relay";
import { stableStringify } from "@t3tools/shared/relaySigning";
import {
  makeAggregateState,
  TERMINAL_AGENT_ACTIVITY_DISPLAY_TTL_MS,
} from "t3code-relay/agentActivity/agentActivityAggregate";
import {
  agentActivityExpiresAt,
  isTerminalPhase,
} from "t3code-relay/agentActivity/agentActivityPayloads";
import type { TargetRow } from "t3code-relay/agentActivity/LiveActivities";
import * as Schema from "effect/Schema";

const decodeJson = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Unknown));
export function next(input: {
  readonly nowMs: number;
  readonly rows: ReadonlyArray<RelayAgentActivityState>;
  readonly targets: ReadonlyArray<TargetRow>;
  readonly retryAts: ReadonlyArray<string>;
  readonly alertExpiries: ReadonlyArray<string>;
}): number | null {
  const candidates: number[] = [];
  const aggregate = makeAggregateState({
    activeStates: input.rows,
    terminalState: null,
    nowMs: input.nowMs,
  });
  for (const row of input.rows)
    candidates.push(
      isTerminalPhase(row)
        ? Date.parse(row.updatedAt) + TERMINAL_AGENT_ACTIVITY_DISPLAY_TTL_MS
        : agentActivityExpiresAt(row),
    );
  for (const target of input.targets) {
    if (!target.activity_push_token) continue;
    if (target.remote_started_at) candidates.push(Date.parse(target.remote_started_at) + 120_000);
    const previous =
      target.last_aggregate_json === null ? null : decodeJson(target.last_aggregate_json);
    const previousJson =
      previous && previous._tag === "Some" ? stableStringify(previous.value) : null;
    if (target.last_live_activity_delivery_at && stableStringify(aggregate) !== previousJson)
      candidates.push(Date.parse(target.last_live_activity_delivery_at) + 15_000);
  }
  candidates.push(...input.retryAts.map(Date.parse), ...input.alertExpiries.map(Date.parse));
  const future = candidates.filter((time) => Number.isFinite(time) && time > input.nowMs);
  return future.length === 0 ? null : Math.min(...future);
}
