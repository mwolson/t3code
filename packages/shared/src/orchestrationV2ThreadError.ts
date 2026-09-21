import type {
  OrchestrationV2ProviderFailure,
  OrchestrationV2Run,
  OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

/** Only a failed root turn of the current run owns the thread's failure state. */
export function latestRootProviderFailure(
  run: OrchestrationV2Run | null,
  turnItems: ReadonlyArray<OrchestrationV2TurnItem>,
): OrchestrationV2ProviderFailure | null {
  return latestRootProviderFailureItem(run, turnItems)?.failure ?? null;
}

export function latestRootProviderFailureItem(
  run: OrchestrationV2Run | null,
  turnItems: ReadonlyArray<OrchestrationV2TurnItem>,
): Extract<OrchestrationV2TurnItem, { type: "error" }> | null {
  if (run?.status !== "failed") return null;
  let latest: Extract<OrchestrationV2TurnItem, { type: "error" }> | null = null;
  for (const item of turnItems) {
    if (
      item.type !== "error" ||
      item.status !== "failed" ||
      item.runId !== run.id ||
      item.nodeId !== run.rootNodeId
    )
      continue;
    if (
      latest === null ||
      DateTime.toEpochMillis(item.updatedAt) > DateTime.toEpochMillis(latest.updatedAt) ||
      (DateTime.toEpochMillis(item.updatedAt) === DateTime.toEpochMillis(latest.updatedAt) &&
        (item.ordinal > latest.ordinal || (item.ordinal === latest.ordinal && item.id > latest.id)))
    ) {
      latest = item;
    }
  }
  return latest;
}

export function providerFailureOccurredAt(
  item: Extract<OrchestrationV2TurnItem, { type: "error" }> | null,
): string | null {
  const at = item?.completedAt ?? item?.startedAt;
  return at == null ? null : DateTime.formatIso(at);
}

/** A distinct session failure supersedes the turn's classification. */
export function threadErrorSummary(
  failure: OrchestrationV2ProviderFailure | null,
  sessionError: string | null,
  occurrence?: { readonly sessionErrorAt?: string | null; readonly failureAt?: string | null },
) {
  const currentFailure =
    sessionError !== null && sessionError !== failure?.message ? null : failure;
  return {
    usageLimitResetAt:
      currentFailure?.class === "usage_limit" ? (currentFailure.resetAt ?? null) : null,
    lastError: sessionError ?? failure?.message ?? null,
    // Identity follows the source that supplied the text, not an unrelated refresh.
    lastErrorAt:
      sessionError !== null
        ? (occurrence?.sessionErrorAt ?? null)
        : failure !== null
          ? (occurrence?.failureAt ?? null)
          : null,
    lastErrorClass:
      sessionError !== null && sessionError !== failure?.message ? null : (failure?.class ?? null),
  };
}
