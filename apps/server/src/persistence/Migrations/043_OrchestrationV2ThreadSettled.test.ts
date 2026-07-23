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

const decodeThreadPayload = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2AppThreadJson),
);

const MigrationLayer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

MigrationLayer("043_OrchestrationV2ThreadSettled", (it) => {
  it.effect(
    "upgrades a migration-042 V2 thread payload without settlement fields, then persists first settle",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;

        // Stop before the V2 settled-column migration. Seed a real pre-043
        // projection_threads row whose payload_json omits settled fields.
        yield* runMigrations({ toMigrationInclusive: 42 });

        const threadId = ThreadId.make("thread:migration-043-settled");
        const projectId = ProjectId.make("project:migration-043-settled");
        const nowIso = "2026-07-01T12:00:00.000Z";
        // Historical migration-042 shape: no settledOverride / settledAt keys.
        const pre042PayloadJson = yield* Schema.encodeUnknownEffect(Schema.UnknownFromJsonString)({
          createdBy: "user",
          creationSource: "web",
          id: threadId,
          projectId,
          title: "Pre-043 V2 thread",
          providerInstanceId: modelSelection.instanceId,
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          activeProviderThreadId: null,
          lineage: {
            parentThreadId: null,
            relationshipToParent: null,
            rootThreadId: threadId,
          },
          forkedFrom: null,
          createdAt: nowIso,
          updatedAt: nowIso,
          archivedAt: null,
          deletedAt: null,
        });

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
            ${"Pre-043 V2 thread"},
            ${modelSelection.instanceId},
            ${modelSelection.instanceId},
            ${"full-access"},
            ${"default"},
            NULL,
            ${nowIso},
            ${nowIso},
            NULL,
            NULL,
            ${pre042PayloadJson}
          )
        `;

        yield* runMigrations({ toMigrationInclusive: 43 });

        const columnsAfter = yield* sql<{ readonly name: string }>`
          PRAGMA table_info(orchestration_v2_projection_threads)
        `;
        assert.ok(columnsAfter.some((column) => column.name === "settled_override"));
        assert.ok(columnsAfter.some((column) => column.name === "settled_at"));

        // Decode defaults: historical JSON without settlement keys -> null pins.
        const decoded = yield* decodeThreadPayload(pre042PayloadJson);
        assert.isNull(decoded.settledOverride);
        assert.isNull(decoded.settledAt);

        const projectionStoreLayerOnSql = projectionStoreLayer.pipe(
          Layer.provide(Layer.succeed(SqlClient.SqlClient, sql)),
        );

        yield* Effect.gen(function* () {
          const store = yield* ProjectionStoreV2;
          const beforeSettle = yield* store.getThreadProjection(threadId);
          assert.isNull(beforeSettle.thread.settledOverride);
          assert.isNull(beforeSettle.thread.settledAt);
          assert.equal(threadShellFromProjection(beforeSettle).settledOverride, null);

          // First real settle write after upgrade.
          const settledAt = DateTime.makeUnsafe("2026-07-01T13:00:00.000Z");
          yield* store.apply({
            id: EventId.make("event:migration-043-first-settle"),
            type: "thread.settled",
            threadId,
            occurredAt: settledAt,
            payload: {
              ...beforeSettle.thread,
              settledOverride: "settled",
              settledAt,
              updatedAt: settledAt,
            } satisfies OrchestrationV2AppThread,
          });

          const reloaded = yield* store.getThreadProjection(threadId);
          assert.equal(reloaded.thread.settledOverride, "settled");
          assert.equal(
            reloaded.thread.settledAt === null
              ? null
              : DateTime.formatIso(reloaded.thread.settledAt),
            DateTime.formatIso(settledAt),
          );
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
          assert.equal(columns[0]?.settled_at, DateTime.formatIso(settledAt));
        }).pipe(Effect.provide(projectionStoreLayerOnSql));
      }),
  );
});
