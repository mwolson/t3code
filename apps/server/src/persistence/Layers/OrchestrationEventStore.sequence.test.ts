import { CommandId, EventId, ProviderSessionId, RunAttemptId, ThreadId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Tracer from "effect/Tracer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import {
  EventStoreV2,
  layerFromOrchestrationEventStore,
} from "../../orchestration-v2/EventStore.ts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { OrchestrationEventStore } from "../Services/OrchestrationEventStore.ts";
import { OrchestrationEventStoreLive } from "./OrchestrationEventStore.ts";
import { SqlitePersistenceMemory } from "./Sqlite.ts";

const eventStoreLayer = OrchestrationEventStoreLive.pipe(
  Layer.provideMerge(SqlitePersistenceMemory),
);
const occurredAt = "2026-09-03T00:00:00.000Z";
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

function eventRow(
  ordinal: number,
  aggregateKind: "project" | "thread",
  version: number,
  streamId = `${aggregateKind}:${ordinal}`,
  commandId: string | null = null,
) {
  return {
    event_id: `event:${ordinal}`,
    aggregate_kind: aggregateKind,
    stream_id: streamId,
    stream_version: ordinal,
    event_type: aggregateKind === "project" ? "project.created" : "provider-session.detached",
    occurred_at: occurredAt,
    command_id: commandId,
    causation_event_id: null,
    correlation_id: null,
    actor_kind: "server",
    payload_json: encodeJson(
      aggregateKind === "project"
        ? {
            projectId: streamId,
            title: "Sequence fixture",
            workspaceRoot: "/tmp/sequence-fixture",
            defaultModelSelection: null,
            scripts: [],
            createdAt: occurredAt,
            updatedAt: occurredAt,
          }
        : { providerSessionId: `session:${ordinal}`, detachedAt: occurredAt },
    ),
    metadata_json: "{}",
    application_event_version: version,
  };
}

const seedEvents = Effect.fn("test.seedSequenceEvents")(function* (
  rows: ReadonlyArray<ReturnType<typeof eventRow>>,
) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<{
    readonly sequence: number;
    readonly aggregate_kind: string;
    readonly application_event_version: number;
    readonly stream_id: string;
    readonly command_id: string | null;
  }>`
    INSERT INTO orchestration_events ${sql.insert(rows)}
    RETURNING sequence, aggregate_kind, application_event_version, stream_id, command_id
  `;
});

it.effect("filters a source run in SQL before paging or decoding unrelated creation payloads", () =>
  Effect.gen(function* () {
    const store = yield* EventStoreV2;
    const sql = yield* SqlClient.SqlClient;
    const threadId = ThreadId.make("reflection-source-lookup");
    const targetId = "source-run";
    const creation = (ordinal: number, id: string, payload: Record<string, unknown>) => ({
      ...eventRow(ordinal, "thread", 2, threadId, "source-command"),
      event_type: "run.created",
      payload_json: encodeJson({ id, ...payload }),
    });
    // Invalid unrelated payloads prove the exact lookup never decodes the history.
    yield* seedEvents(Array.from({ length: 1_002 }, (_, i) => creation(i, `other-${i}`, {})));
    const [source] = yield* seedEvents([
      creation(1_003, targetId, {
        threadId,
        ordinal: 1_004,
        providerInstanceId: "codex",
        modelSelection: { instanceId: "codex", model: "gpt-5.4" },
        providerThreadId: null,
        userMessageId: "source-message",
        rootNodeId: null,
        activeAttemptId: null,
        status: "queued",
        requestedAt: occurredAt,
        startedAt: null,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
      }),
    ]);
    assert.ok(source);
    const events = yield* store
      .read({ threadId, eventType: "run.created", entityId: targetId, limit: 1 })
      .pipe(Stream.runCollect);
    assert.deepEqual(
      events.map((event) => [event.sequence, event.commandId]),
      [[source.sequence, CommandId.make("source-command")]],
    );
    assert.deepEqual(
      yield* store
        .read({ threadId, eventType: "run.created", entityId: "missing", limit: 1 })
        .pipe(Stream.runCollect),
      [],
    );
    // This generic entity lookup does not exercise the start-admission predicate below.
    const plan = yield* sql<{ readonly detail: string }>`
      EXPLAIN QUERY PLAN SELECT sequence FROM orchestration_events
      WHERE sequence > 0 AND sequence <= ${Number.MAX_SAFE_INTEGER}
        AND application_event_version = 2 AND aggregate_kind = 'thread'
        AND stream_id = ${threadId} AND event_type = 'run.created'
        AND json_extract(payload_json, '$.id') = ${targetId}
      ORDER BY sequence ASC LIMIT 1
    `;
    assert.isTrue(
      plan.some((row) => row.detail.includes("SEARCH orchestration_events USING INDEX")),
      plan.map((row) => row.detail).join("; "),
    );
  }).pipe(
    Effect.provide(
      Layer.fresh(layerFromOrchestrationEventStore.pipe(Layer.provideMerge(eventStoreLayer))),
    ),
  ),
);

it.effect(
  "filters immutable start admission by run, status and exact attempt before decoding",
  () =>
    Effect.gen(function* () {
      const store = yield* EventStoreV2;
      const threadId = ThreadId.make("reflection-start-lookup");
      const attemptId = RunAttemptId.make("source-attempt");
      const row = (ordinal: number, eventType: string, payload: Record<string, unknown>) => ({
        ...eventRow(ordinal, "thread", 2, threadId, "admission-command"),
        event_type: eventType,
        payload_json: encodeJson(payload),
      });
      yield* seedEvents(
        Array.from({ length: 1_002 }, (_, i) =>
          row(i, "run.updated", {
            id: i % 2 === 0 ? "other" : "source",
            status: "starting",
            activeAttemptId: "other-attempt",
          }),
        ),
      );
      yield* seedEvents([
        row(1_003, "run.created", { id: "source", status: "queued", activeAttemptId: attemptId }),
        row(1_004, "run.updated", { id: "source", status: "queued", activeAttemptId: attemptId }),
        row(1_005, "run.updated", { id: "source", status: "running", activeAttemptId: attemptId }),
      ]);
      const payload = {
        id: "source",
        threadId,
        ordinal: 1,
        providerInstanceId: "codex",
        modelSelection: { instanceId: "codex", model: "gpt-5.4" },
        providerThreadId: null,
        userMessageId: "message",
        rootNodeId: null,
        activeAttemptId: attemptId,
        status: "starting",
        requestedAt: occurredAt,
        startedAt: null,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
      };
      const [promoted] = yield* seedEvents([row(1_006, "run.updated", payload)]);
      yield* seedEvents([row(1_007, "run.updated", payload)]);
      const query = { threadId, entityId: "source", runStartAttemptId: attemptId, limit: 1 };
      const statements: Array<string> = [];
      const tracer = Tracer.make({
        span(options) {
          const span = new Tracer.NativeSpan(options);
          const end = span.end.bind(span);
          span.end = (endTime, exit) => {
            end(endTime, exit);
            const text = span.attributes.get("db.query.text");
            if (typeof text === "string" && text.includes("$.activeAttemptId"))
              statements.push(text);
          };
          return span;
        },
      });
      assert.deepEqual(
        yield* store.read({ ...query, eventType: "run.created" }).pipe(Stream.runCollect),
        [],
      );
      assert.deepEqual(
        (yield* store
          .read({ ...query, eventType: "run.updated" })
          .pipe(Stream.runCollect, Effect.withTracer(tracer))).map((event) => event.sequence),
        [promoted!.sequence],
      );
      assert.lengthOf(statements, 1);
      const statement = statements[0]!;
      for (const predicate of [
        "stream_id = ?",
        "event_type = ?",
        "event_type IN ('run.created', 'run.updated')",
        "json_extract(payload_json, '$.id') = ?",
        "json_extract(payload_json, '$.status') = 'starting'",
        "json_extract(payload_json, '$.activeAttemptId') = ?",
        "LIMIT ?",
      ]) {
        assert.include(statement, predicate);
      }
      const sql = yield* SqlClient.SqlClient;
      // Inspect the emitted SQL; bind the same lookup values and a safe high-water bound.
      const plan = yield* sql.unsafe<{ readonly detail: string }>(
        `EXPLAIN QUERY PLAN ${statement}`,
        [0, Number.MAX_SAFE_INTEGER, threadId, "run.updated", "source", attemptId, 1],
      );
      assert.isTrue(
        plan.some((row) => row.detail.includes("SEARCH orchestration_events USING INDEX")),
        plan.map((row) => row.detail).join("; "),
      );
      assert.isFalse(plan.some((row) => row.detail.includes("TEMP B-TREE")));
      assert.deepEqual(
        yield* store
          .read({
            ...query,
            eventType: "run.updated",
            runStartAttemptId: RunAttemptId.make("missing"),
          })
          .pipe(Stream.runCollect),
        [],
      );
      const [immediate] = yield* seedEvents([
        row(1_008, "run.created", { ...payload, id: "immediate" }),
      ]);
      assert.deepEqual(
        (yield* store
          .read({ ...query, entityId: "immediate", eventType: "run.created" })
          .pipe(Stream.runCollect)).map((event) => event.sequence),
        [immediate!.sequence],
      );
      const [restarted] = yield* seedEvents([
        row(1_009, "run.updated", { ...payload, activeAttemptId: "restart-attempt" }),
      ]);
      assert.deepEqual(
        (yield* store
          .read({
            ...query,
            eventType: "run.updated",
            runStartAttemptId: RunAttemptId.make("restart-attempt"),
          })
          .pipe(Stream.runCollect)).map((event) => event.sequence),
        [restarted!.sequence],
      );
    }).pipe(
      Effect.provide(
        Layer.fresh(layerFromOrchestrationEventStore.pipe(Layer.provideMerge(eventStoreLayer))),
      ),
    ),
);

it.effect("keeps application and scoped agent high-water marks separate from legacy history", () =>
  Effect.gen(function* () {
    const store = yield* OrchestrationEventStore;
    const sql = yield* SqlClient.SqlClient;
    const target = ThreadId.make("target");
    assert.equal(yield* store.latestApplicationSequence, 0);
    assert.equal(yield* store.latestAgentSequence(), 0);
    assert.equal(yield* store.latestAgentSequence(target), 0);

    yield* seedEvents([eventRow(1, "thread", 1, "legacy-only")]);
    assert.equal(yield* store.latestApplicationSequence, 0);
    assert.equal(yield* store.latestAgentSequence(ThreadId.make("legacy-only")), 0);

    const [legacyProject, targetEvent, otherEvent] = yield* seedEvents([
      eventRow(2, "project", 1),
      eventRow(3, "thread", 2, target),
      eventRow(4, "thread", 2, "other"),
    ]);
    assert.ok(legacyProject);
    assert.ok(targetEvent);
    assert.ok(otherEvent);
    assert.equal(yield* store.latestApplicationSequence, otherEvent.sequence);
    assert.equal(yield* store.latestAgentSequence(), otherEvent.sequence);
    assert.equal(yield* store.latestAgentSequence(target), targetEvent.sequence);

    const [projectEvent] = yield* seedEvents([
      eventRow(5, "project", 2),
      eventRow(6, "thread", 1, target),
      eventRow(7, "thread", 3, target),
    ]);
    assert.ok(projectEvent);
    assert.equal(yield* store.latestApplicationSequence, projectEvent.sequence);
    assert.equal(yield* store.latestAgentSequence(), otherEvent.sequence);
    assert.equal(yield* store.latestAgentSequence(target), targetEvent.sequence);
    assert.equal(yield* store.latestAgentSequence(ThreadId.make("missing")), 0);

    yield* sql`DELETE FROM orchestration_events WHERE sequence > ${legacyProject.sequence}`;
    assert.equal(yield* store.latestApplicationSequence, legacyProject.sequence);
    assert.equal(yield* store.latestAgentSequence(), 0);
    assert.equal(yield* store.latestAgentSequence(target), 0);
  }).pipe(Effect.provide(Layer.fresh(eventStoreLayer))),
);

it.effect(
  "preserves mixed application paging, scoped replay, and the catch-up to live boundary",
  () =>
    Effect.gen(function* () {
      const store = yield* OrchestrationEventStore;
      const rows = yield* seedEvents(
        Array.from({ length: 1_560 }, (_, index) => {
          const kind = index % 3 === 1 ? "project" : "thread";
          return eventRow(
            index,
            kind,
            index % 3 === 0 ? 1 : 2,
            kind === "project" ? `project:${index}` : index % 2 === 0 ? "target" : "other",
            index % 5 === 0 ? "selected-command" : "other-command",
          );
        }),
      );
      const afterSequence = rows[25]!.sequence;
      const throughSequence = rows[1_525]!.sequence;
      const retained = rows.filter(
        (row) => row.aggregate_kind === "project" || row.application_event_version === 2,
      );
      const replay = yield* store
        .readApplicationEvents({ afterSequence, throughSequence })
        .pipe(Stream.runCollect);
      assert.deepEqual(
        replay.map((event) => event.sequence),
        retained
          .filter((row) => row.sequence > afterSequence && row.sequence <= throughSequence)
          .map((row) => row.sequence),
      );

      const scoped = yield* store
        .readAgentEvents({
          afterSequence,
          throughSequence,
          threadId: ThreadId.make("target"),
          commandId: CommandId.make("selected-command"),
          limit: 17,
        })
        .pipe(Stream.runCollect);
      assert.deepEqual(
        scoped.map((event) => event.sequence),
        retained
          .filter(
            (row) =>
              row.sequence > afterSequence &&
              row.sequence <= throughSequence &&
              row.aggregate_kind === "thread" &&
              row.stream_id === "target" &&
              row.command_id === "selected-command",
          )
          .slice(0, 17)
          .map((row) => row.sequence),
      );

      const catchUp = retained.filter((row) => row.sequence > afterSequence);
      const highWater = catchUp.at(-1)!.sequence;
      let liveSequence = 0;
      const streamed = yield* store.streamApplicationEvents({ afterSequence }).pipe(
        Stream.tap((event) =>
          event.sequence !== highWater
            ? Effect.void
            : Effect.gen(function* () {
                const [stored] = yield* store.appendAgentEvents({
                  events: [
                    {
                      id: EventId.make("live-after-catch-up"),
                      type: "provider-session.detached",
                      threadId: ThreadId.make("target"),
                      occurredAt: DateTime.makeUnsafe(occurredAt),
                      payload: {
                        providerSessionId: ProviderSessionId.make("live-session"),
                        detachedAt: DateTime.makeUnsafe(occurredAt),
                      },
                    },
                  ],
                });
                assert.ok(stored);
                liveSequence = stored.sequence;
                yield* store.publishCommitted([stored]);
              }),
        ),
        Stream.take(catchUp.length + 1),
        Stream.runCollect,
      );
      assert.deepEqual(
        streamed.map((event) => event.sequence),
        [...catchUp.map((row) => row.sequence), liveSequence],
      );
      assert.isAbove(liveSequence, highWater);
    }).pipe(Effect.provide(Layer.fresh(eventStoreLayer))),
);

it.effect("uses indexed high-water lookups for populated history without OR scans", () =>
  Effect.gen(function* () {
    const store = yield* OrchestrationEventStore;
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* sql`
      WITH RECURSIVE history(n) AS (
        SELECT 1 UNION ALL SELECT n + 1 FROM history WHERE n < 25000
      )
      INSERT INTO orchestration_events (
        event_id, aggregate_kind, stream_id, stream_version, event_type,
        occurred_at, actor_kind, payload_json, metadata_json, application_event_version
      )
      SELECT 'history:' || n, 'thread',
        CASE WHEN n = 1 THEN 'target' ELSE 'other' END,
        n, 'provider-session.detached', ${occurredAt}, 'server', '{}', '{}',
        CASE WHEN n % 3 = 0 THEN 1 ELSE 2 END
      FROM history
    `;
    const statements: Array<string> = [];
    const tracer = Tracer.make({
      span(options) {
        const span = new Tracer.NativeSpan(options);
        const end = span.end.bind(span);
        span.end = (endTime, exit) => {
          end(endTime, exit);
          const query = span.attributes.get("db.query.text");
          if (typeof query === "string") statements.push(query);
        };
        return span;
      },
    });
    assert.equal(yield* store.latestApplicationSequence.pipe(Effect.withTracer(tracer)), 25_000);
    assert.equal(
      yield* store.latestAgentSequence(ThreadId.make("target")).pipe(Effect.withTracer(tracer)),
      1,
    );
    assert.equal(statements.length, 2);
    const applicationPlan = yield* sql.unsafe<{ readonly detail: string }>(
      `EXPLAIN QUERY PLAN ${statements[0]}`,
    );
    const agentPlan = yield* sql.unsafe<{ readonly detail: string }>(
      `EXPLAIN QUERY PLAN ${statements[1]}`,
      ["target"],
    );
    assert.match(
      applicationPlan.map((row) => row.detail).join("\n"),
      /SEARCH orchestration_events USING INDEX idx_orchestration_events_application_high_water/,
    );
    assert.match(
      agentPlan.map((row) => row.detail).join("\n"),
      /USING COVERING INDEX idx_orchestration_events_agent_stream_sequence \(stream_id=\?\)/,
    );
    for (const row of [...applicationPlan, ...agentPlan]) {
      assert.notMatch(row.detail, /MULTI-INDEX OR|TEMP B-TREE/);
    }
  }).pipe(
    Effect.provide(
      OrchestrationEventStoreLive.pipe(
        Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
      ),
    ),
  ),
);
