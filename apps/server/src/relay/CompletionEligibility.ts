import type {
  OrchestrationV2DomainEvent,
  OrchestrationV2PendingBackgroundTask,
  OrchestrationV2ThreadProjection,
  OrchestrationV2ThreadShell,
  ThreadPullRequestLink,
} from "@t3tools/contracts";
import { isOrchestrationV2WorkActive } from "@t3tools/contracts";
import { backgroundWorkHoldsCompletion } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import {
  threadPullRequestKeyOf,
  threadPullRequestKeysEqual,
} from "../sourceControl/threadPullRequests.ts";
import {
  applyToProjection,
  emptyProjection,
  threadShellFromProjection,
} from "../orchestration-v2/ProjectionStore.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";

export class CompletionEligibilityChanged extends Schema.TaggedError<CompletionEligibilityChanged>()(
  "CompletionEligibilityChanged",
  {},
) {
  override get message(): string {
    return "The completion snapshot changed while reading its eligibility history.";
  }
}

// Reuse the projection's hold rules, retaining no message bodies or unrelated tool output.
function affectsCompletion(event: OrchestrationV2DomainEvent): boolean {
  if (event.type.startsWith("thread.")) return true;
  switch (event.type) {
    case "run.created":
    case "run.updated":
    case "run.background-work-cancelled":
    case "provider-thread.updated":
      return true;
    case "turn-item.updated":
      return ["command_execution", "dynamic_tool", "subagent"].includes(event.payload.type);
    default:
      return false;
  }
}

export const make = Effect.gen(function* () {
  const events = yield* EventStore.EventStoreV2;
  const cached = new Map<string, { readonly revision: string; readonly at: DateTime.Utc }>();
  return Effect.fn("CompletionEligibility.resolve")(function* (
    thread: OrchestrationV2ThreadShell,
    observedSequence?: number,
  ) {
    const completedAt = thread.latestRunCompletedAt;
    if (thread.latestRunId === null || completedAt == null) return completedAt ?? null;
    const runId = thread.latestRunId;
    const revision = `${runId}:${DateTime.toEpochMillis(completedAt)}`;
    const previous = cached.get(thread.id);
    if (previous?.revision === revision) return previous.at;
    let projection: OrchestrationV2ThreadProjection | undefined;
    let eligibleAt = completedAt;
    let held = false;
    let replayRunId: string | null = null;
    let lastRelevantAt = completedAt;
    let replayInputs: string | null = null;
    const rawInputs = new Map<string, string>();
    const rawPullRequests = new Map<string, ReadonlyArray<ThreadPullRequestLink>>();
    const throughSequence = yield* events.latestSequence({ threadId: thread.id });
    yield* events.read({ threadId: thread.id, throughSequence }).pipe(
      Stream.filter((stored) => affectsCompletion(stored.event)),
      Stream.runForEach(({ event }) =>
        Effect.sync(() => {
          if (event.type === "thread.created") projection = emptyProjection(event);
          else if (projection !== undefined) projection = applyToProjection(projection, event);
          // Raw releases remain visible even when replay projection rules disagree with the shell.
          const raw = rawHoldInput(
            event,
            runId,
            rawPullRequests,
            projection?.thread.activeProviderThreadId,
          );
          if (raw !== null) {
            const previous = rawInputs.get(raw.key);
            if (
              previous !== undefined &&
              previous !== raw.value &&
              (event.type !== "turn-item.updated" || previous === "active")
            ) {
              lastRelevantAt = DateTime.max(lastRelevantAt, event.occurredAt);
            }
            rawInputs.set(raw.key, raw.value);
          }
          if (projection === undefined) return;
          const shell = threadShellFromProjection(projection);
          const inputs = holdInputs(shell, runId);
          if (inputs !== null && replayInputs !== inputs) {
            lastRelevantAt = DateTime.max(lastRelevantAt, event.occurredAt);
          }
          replayInputs = inputs;
          if (shell.latestRunId !== replayRunId) {
            replayRunId = shell.latestRunId;
            held = false;
            eligibleAt = shell.latestRunCompletedAt ?? completedAt;
          }
          if (shell.latestRunId !== runId) return;
          const holding = backgroundWorkHoldsCompletion(shell.pendingBackgroundTasks ?? []);
          if (held && !holding) {
            eligibleAt = DateTime.max(completedAt, event.occurredAt);
          } else if (!held && !holding && shell.latestRunCompletedAt != null) {
            eligibleAt = DateTime.max(eligibleAt, shell.latestRunCompletedAt);
          }
          held = holding;
        }),
      ),
    );
    if (projection !== undefined && (replayRunId !== runId || held)) {
      if (observedSequence !== undefined && throughSequence > observedSequence) {
        return yield* new CompletionEligibilityChanged();
      }
      eligibleAt = DateTime.max(completedAt, lastRelevantAt);
      yield* Effect.logWarning(
        "Completion eligibility replay disagreed with the observed shell; using durable event time.",
        { threadId: thread.id },
      );
    }
    // The event log is authoritative across restarts; this bounded cache only avoids replay on retries.
    if (cached.size >= 256) cached.clear();
    cached.set(thread.id, { revision, at: eligibleAt });
    return eligibleAt;
  });
});

function holdInputs(shell: OrchestrationV2ThreadShell, runId: string): string | null {
  if (shell.latestRunId !== runId) return null;
  return JSON.stringify([
    shell.latestRunCompletedAt == null ? null : DateTime.toEpochMillis(shell.latestRunCompletedAt),
    holdingTaskInputs(shell.pendingBackgroundTasks ?? []),
  ]);
}

function holdingTaskInputs(tasks: ReadonlyArray<OrchestrationV2PendingBackgroundTask>): string[] {
  return [
    ...new Set(
      tasks
        .filter((task) => backgroundWorkHoldsCompletion([task]))
        .map((task) => JSON.stringify([task.taskId, task.kind, task.awaited === true])),
    ),
  ].sort();
}

function rawHoldInput(
  event: OrchestrationV2DomainEvent,
  runId: string,
  pullRequests: Map<string, ReadonlyArray<ThreadPullRequestLink>>,
  activeProviderThreadId: string | null | undefined,
): { readonly key: string; readonly value: string } | null {
  if (event.type.startsWith("thread.")) {
    if (event.type === "thread.pull-request-link-updated") {
      pullRequests.set(
        event.threadId,
        (pullRequests.get(event.threadId) ?? []).map((link) =>
          threadPullRequestKeysEqual(link, event.payload) ? event.payload : link,
        ),
      );
    } else if ("activeProviderThreadId" in event.payload) {
      pullRequests.set(event.threadId, event.payload.pullRequests ?? []);
    } else {
      return null;
    }
    return {
      key: `pull-requests:${event.threadId}`,
      value: JSON.stringify(
        [
          ...new Set(
            (pullRequests.get(event.threadId) ?? [])
              .filter((link) => link.watch !== undefined && link.source !== "stack-dismissed")
              .map(threadPullRequestKeyOf),
          ),
        ].sort(),
      ),
    };
  }
  switch (event.type) {
    case "provider-thread.updated":
      if (activeProviderThreadId != null && event.payload.id !== activeProviderThreadId)
        return null;
      return {
        key: `provider:${event.payload.id}`,
        value: JSON.stringify(holdingTaskInputs(event.payload.pendingBackgroundTasks ?? [])),
      };
    case "turn-item.updated":
      if (
        event.payload.type !== "subagent" &&
        (event.payload.type !== "dynamic_tool" ||
          (event.payload.input !== null &&
            typeof event.payload.input === "object" &&
            !Array.isArray(event.payload.input) &&
            Reflect.get(event.payload.input, "persistent") === true))
      )
        return null;
      return {
        key: `item:${event.payload.id}`,
        value: isOrchestrationV2WorkActive(event.payload.status) ? "active" : "terminal",
      };
    case "run.created":
    case "run.updated":
      if (event.payload.id !== runId) return null;
      return {
        key: `run:${event.payload.id}`,
        value: JSON.stringify([
          event.payload.status,
          event.payload.completedAt === null
            ? null
            : DateTime.toEpochMillis(event.payload.completedAt),
        ]),
      };
    default:
      return null;
  }
}
