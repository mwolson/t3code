import { assert, it } from "@effect/vitest";
import {
  EventId,
  type ModelSelection,
  OrchestrationV2AppThread,
  OrchestrationV2AppThreadJson,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  ProjectionStoreV2,
  layer as projectionStoreLayer,
  threadShellFromProjection,
} from "../../orchestration-v2/ProjectionStore.ts";
import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.4",
} satisfies ModelSelection;

const encodeThreadPayload = Schema.encodeEffect(
  Schema.fromJsonString(OrchestrationV2AppThreadJson),
);

const MigrationLayer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

MigrationLayer("042_OrchestrationV2ThreadSettled", (it) => {
  it.effect(
    "adds settled columns to a migration-041 V2 database and preserves settled projection reloads",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;

        // Stop before the V2 settled-column migration so we can plant a
        // pre-042 projection_threads row with settled fields only in JSON.
        yield* runMigrations({ toMigrationInclusive: 41 });

        const threadId = ThreadId.make("thread:migration-042-settled");
        const projectId = ProjectId.make("project:migration-042-settled");
        const now = DateTime.makeUnsafe("2026-07-01T12:00:00.000Z");
        const settledAt = DateTime.makeUnsafe("2026-07-01T11:00:00.000Z");
        const payload = {
          createdBy: "user" as const,
          creationSource: "web" as const,
          id: threadId,
          projectId,
          title: "Pre-042 settled thread",
          providerInstanceId: modelSelection.instanceId,
          modelSelection,
          runtimeMode: "full-access" as const,
          interactionMode: "default" as const,
          branch: null,
          worktreePath: null,
          activeProviderThreadId: null,
          lineage: {
            parentThreadId: null,
            relationshipToParent: null,
            rootThreadId: threadId,
          },
          forkedFrom: null,
          createdAt: now,
          updatedAt: now,
          archivedAt: null,
          deletedAt: null,
          settledOverride: "settled" as const,
          settledAt,
        } satisfies OrchestrationV2AppThread;
        const payloadJson = yield* encodeThreadPayload(payload);

        const columnsBefore = yield* sql<{ readonly name: string }>`
          PRAGMA table_info(orchestration_v2_projection_threads)
        `;
        assert.isFalse(columnsBefore.some((column) => column.name === "settled_override"));
        assert.isFalse(columnsBefore.some((column) => column.name === "settled_at"));

        yield* sql`
          INSERT INTO orchestration_v2_projection_threads (
            thread_id,
            project_id,
            title,
            default_provider,
            provider_instance_id,
            runtime_mode,
            interaction_mode,
            active_provider_thread_id,
            created_at,
            updated_at,
            archived_at,
            deleted_at,
            payload_json
          ) VALUES (
            ${threadId},
            ${projectId},
            ${payload.title},
            ${modelSelection.instanceId},
            ${modelSelection.instanceId},
            ${payload.runtimeMode},
            ${payload.interactionMode},
            NULL,
            ${DateTime.formatIso(now)},
            ${DateTime.formatIso(now)},
            NULL,
            NULL,
            ${payloadJson}
          )
        `;

        yield* runMigrations({ toMigrationInclusive: 42 });

        const columnsAfter = yield* sql<{ readonly name: string }>`
          PRAGMA table_info(orchestration_v2_projection_threads)
        `;
        assert.ok(columnsAfter.some((column) => column.name === "settled_override"));
        assert.ok(columnsAfter.some((column) => column.name === "settled_at"));

        // Columns exist but historical rows still carry pins in payload_json
        // until the next thread write. Persist via columns then reload.
        yield* sql`
          UPDATE orchestration_v2_projection_threads
          SET
            settled_override = 'settled',
            settled_at = ${DateTime.formatIso(settledAt)},
            payload_json = ${payloadJson}
          WHERE thread_id = ${threadId}
        `;

        const projectionStoreLayerOnSql = projectionStoreLayer.pipe(
          Layer.provide(Layer.succeed(SqlClient.SqlClient, sql)),
        );

        yield* Effect.gen(function* () {
          const store = yield* ProjectionStoreV2;
          const projection = yield* store.getThreadProjection(threadId);
          assert.equal(projection.thread.settledOverride, "settled");
          assert.equal(
            projection.thread.settledAt === null
              ? null
              : DateTime.formatIso(projection.thread.settledAt),
            DateTime.formatIso(settledAt),
          );

          const shell = threadShellFromProjection(projection);
          assert.equal(shell.settledOverride, "settled");
          assert.equal(
            shell.settledAt === null ? null : DateTime.formatIso(shell.settledAt),
            DateTime.formatIso(settledAt),
          );

          // Round-trip a thread.settled apply so column write paths stay aligned.
          yield* store.apply({
            id: EventId.make("event:migration-042-resettle"),
            type: "thread.settled",
            threadId,
            occurredAt: now,
            payload: {
              ...projection.thread,
              settledOverride: "settled",
              settledAt: now,
              updatedAt: now,
            },
          });
          const reloaded = yield* store.getThreadProjection(threadId);
          assert.equal(reloaded.thread.settledOverride, "settled");
          assert.equal(threadShellFromProjection(reloaded).settledOverride, "settled");

          const columns = yield* sql<{
            readonly settled_override: string | null;
            readonly settled_at: string | null;
          }>`
            SELECT settled_override, settled_at
            FROM orchestration_v2_projection_threads
            WHERE thread_id = ${threadId}
          `;
          assert.equal(columns[0]?.settled_override, "settled");
          assert.isNotNull(columns[0]?.settled_at);
        }).pipe(Effect.provide(projectionStoreLayerOnSql));
      }),
  );
});
