import type { OrchestrationV2ThreadProjection } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

import type { ThreadRunSummary, ThreadRuntimeSummary } from "./models.ts";

const ACTIVE_RUN_STATUSES = new Set(["preparing", "queued", "starting", "running", "waiting"]);

function findLatestAssistantMessageIdForRun(
  projection: OrchestrationV2ThreadProjection,
  runId: OrchestrationV2ThreadProjection["runs"][number]["id"],
): OrchestrationV2ThreadProjection["messages"][number]["id"] | null {
  for (let index = projection.messages.length - 1; index >= 0; index -= 1) {
    const message = projection.messages[index];
    if (message?.runId === runId && message.role === "assistant") return message.id;
  }
  return null;
}

export function deriveLatestThreadRun(
  projection: OrchestrationV2ThreadProjection,
): ThreadRunSummary | null {
  const run = projection.runs.reduce<(typeof projection.runs)[number] | null>(
    (latest, candidate) =>
      latest === null || candidate.ordinal > latest.ordinal ? candidate : latest,
    null,
  );
  if (run === null) return null;
  return {
    runId: run.id,
    status: run.status,
    requestedAt: DateTime.formatIso(run.requestedAt),
    startedAt: run.startedAt === null ? null : DateTime.formatIso(run.startedAt),
    completedAt: run.completedAt === null ? null : DateTime.formatIso(run.completedAt),
    assistantMessageId: findLatestAssistantMessageIdForRun(projection, run.id),
    ...(run.sourcePlanRef === undefined ? {} : { sourcePlanRef: run.sourcePlanRef }),
  };
}

export function deriveThreadRuntime(
  projection: OrchestrationV2ThreadProjection,
): ThreadRuntimeSummary | null {
  const latestRun = deriveLatestThreadRun(projection);
  let providerSession: (typeof projection.providerSessions)[number] | undefined;
  for (let index = projection.providerSessions.length - 1; index >= 0; index -= 1) {
    const session = projection.providerSessions[index];
    if (session?.providerInstanceId === projection.thread.providerInstanceId) {
      providerSession = session;
      break;
    }
  }
  if (latestRun === null && projection.thread.activeProviderThreadId === null) return null;
  let activeRunId: (typeof projection.runs)[number]["id"] | null = null;
  for (let index = projection.runs.length - 1; index >= 0; index -= 1) {
    const run = projection.runs[index];
    if (run && ACTIVE_RUN_STATUSES.has(run.status)) {
      activeRunId = run.id;
      break;
    }
  }
  return {
    status: latestRun?.status ?? "idle",
    activeRunId,
    providerInstanceId: projection.thread.providerInstanceId,
    providerName: providerSession?.driver ?? null,
    lastError: providerSession?.lastError ?? null,
    updatedAt: DateTime.formatIso(projection.updatedAt),
  };
}
