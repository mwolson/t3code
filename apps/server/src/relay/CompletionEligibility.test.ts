import { assert, describe, it, vi } from "@effect/vitest";
import { EventId, MessageId, RunId, type OrchestrationV2Run } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { shell, THREAD_ID } from "./AgentAwarenessRelay.testkit.ts";
import * as CompletionEligibility from "./CompletionEligibility.ts";

describe("CompletionEligibility", () => {
  it.effect("retains the raw run release time and ignores later identical refreshes", () =>
    Effect.gen(function* () {
      const events = yield* EventStore.EventStoreV2;
      const completedAt = DateTime.makeUnsafe("2026-10-17T12:00:00.000Z");
      const releasedAt = DateTime.makeUnsafe("2026-10-17T12:20:00.000Z");
      const refreshedAt = DateTime.makeUnsafe("2026-10-17T12:25:00.000Z");
      const runId = RunId.make("raw-run");
      const observed = shell({
        status: "completed",
        latestRunId: runId,
        latestRunCompletedAt: completedAt,
      });
      // Freeze replay's shell so only the raw run payload can retain the release time.
      const replay = vi.spyOn(ProjectionStore, "threadShellFromProjection").mockReturnValue({
        ...observed,
        pendingBackgroundTasks: [{ taskId: "phantom", kind: "monitor", awaited: true }],
      });
      yield* Effect.addFinalizer(() => Effect.sync(() => replay.mockRestore()));
      const run: OrchestrationV2Run = {
        id: runId,
        threadId: THREAD_ID,
        ordinal: 1,
        providerInstanceId: observed.providerInstanceId,
        modelSelection: observed.modelSelection,
        providerThreadId: null,
        userMessageId: MessageId.make("raw-run-message"),
        rootNodeId: null,
        activeAttemptId: null,
        status: "completed",
        requestedAt: completedAt,
        startedAt: completedAt,
        completedAt,
        checkpointId: null,
        contextHandoffId: null,
      };
      const released = { ...run, status: "waiting" as const };
      yield* events.append({
        events: [
          {
            id: EventId.make("thread"),
            threadId: THREAD_ID,
            type: "thread.created",
            occurredAt: completedAt,
            payload: { ...observed, lastVisitedAt: null },
          },
          {
            id: EventId.make("run"),
            threadId: THREAD_ID,
            type: "run.created",
            occurredAt: completedAt,
            payload: run,
          },
          {
            id: EventId.make("released"),
            threadId: THREAD_ID,
            type: "run.updated",
            occurredAt: releasedAt,
            payload: released,
          },
          {
            id: EventId.make("refreshed"),
            threadId: THREAD_ID,
            type: "run.updated",
            occurredAt: refreshedAt,
            payload: released,
          },
        ],
      });
      const resolve = yield* CompletionEligibility.make;
      assert.equal(DateTime.formatIso((yield* resolve(observed))!), DateTime.formatIso(releasedAt));
    }).pipe(
      Effect.scoped,
      Effect.provide(EventStore.layer.pipe(Layer.provide(SqlitePersistence.layerMemory))),
    ),
  );
});
