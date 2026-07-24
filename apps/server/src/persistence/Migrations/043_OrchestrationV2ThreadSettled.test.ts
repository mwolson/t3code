import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  EventId,
  type ModelSelection,
  ModelSelection as ModelSelectionSchema,
  OrchestrationV2AppThread,
  OrchestrationV2AppThreadJson,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { ServerConfig } from "../../config.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "../../orchestration/Layers/ProjectionSnapshotQuery.ts";
import { ProjectionSnapshotQuery } from "../../orchestration/Services/ProjectionSnapshotQuery.ts";
import {
  ProjectionStoreV2,
  layer as projectionStoreLayer,
  threadShellFromProjection,
} from "../../orchestration-v2/ProjectionStore.ts";
import * as ProjectEnrichment from "../../project/ProjectEnrichmentService.ts";
import * as ProjectFaviconResolver from "../../project/ProjectFaviconResolver.ts";
import * as RepositoryIdentityResolver from "../../project/RepositoryIdentityResolver.ts";
import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.4",
} satisfies ModelSelection;

const decodeThreadPayload = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2AppThreadJson),
);
const encodeUnknownJson = Schema.encodeUnknownEffect(Schema.UnknownFromJsonString);
const encodeModelSelectionJson = Schema.encodeUnknownEffect(
  Schema.fromJsonString(ModelSelectionSchema),
);

/**
 * Exact published origin/trial/orchestrator-v2.1 ledger (IDs 33-42).
 * That branch never registered ProjectionThreadsSnoozed.
 */
const PUBLISHED_V21_LEDGER_33_TO_42: ReadonlyArray<readonly [number, string]> = [
  [33, "OrchestrationV2"],
  [34, "OrchestrationV2Subagents"],
  [35, "OrchestrationV2Foundation"],
  [36, "OrchestrationV2ProviderSessionBindings"],
  [37, "OrchestrationV2ThreadLaunchWorkflows"],
  [38, "ApplicationEventSource"],
  [39, "OrchestrationV2EffectCancellation"],
  [40, "ScheduledTasks"],
  [41, "ProjectionThreadsSettled"],
  [42, "OrchestrationV2ThreadSettled"],
];

const MigrationLayer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

MigrationLayer("043_OrchestrationV2ThreadSettled", (it) => {
  it.effect(
    "upgrades a migration-042 V2 thread payload without settlement fields, then persists first settle",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;

        // Clean path through 042 (includes 034 snooze). Seed a pre-043 V2 row
        // whose payload_json omits settled fields.
        yield* runMigrations({ toMigrationInclusive: 42 });

        const threadId = ThreadId.make("thread:migration-043-settled");
        const projectId = ProjectId.make("project:migration-043-settled");
        const nowIso = "2026-07-01T12:00:00.000Z";
        const pre043PayloadJson = yield* encodeUnknownJson({
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
            ${pre043PayloadJson}
          )
        `;

        yield* runMigrations({ toMigrationInclusive: 43 });

        const columnsAfter = yield* sql<{ readonly name: string }>`
          PRAGMA table_info(orchestration_v2_projection_threads)
        `;
        assert.ok(columnsAfter.some((column) => column.name === "settled_override"));
        assert.ok(columnsAfter.some((column) => column.name === "settled_at"));

        // Clean install still has snooze columns from 034; 043 is a no-op there.
        const snoozeColumns = yield* sql<{ readonly name: string }>`
          PRAGMA table_info(projection_threads)
        `;
        assert.ok(snoozeColumns.some((column) => column.name === "snoozed_until"));
        assert.ok(snoozeColumns.some((column) => column.name === "snoozed_at"));

        const decoded = yield* decodeThreadPayload(pre043PayloadJson);
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

// Isolated fresh memory DB: must not share it.layer state with the clean-path
// suite above (that suite already runs 043 and would leave settled columns).
it.effect(
  "repairs historical v2.1 ledger at 42 that never applied projection_threads snooze columns",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      // Start from current schema through 042 (has 034 snooze + V2 tables).
      yield* runMigrations({ toMigrationInclusive: 42 });

      // Reconstruct published origin/trial/orchestrator-v2.1 schema at latest=42:
      //   33 OrchestrationV2
      //   34 OrchestrationV2Subagents
      //   35 OrchestrationV2Foundation
      //   36 OrchestrationV2ProviderSessionBindings
      //   37 OrchestrationV2ThreadLaunchWorkflows
      //   38 ApplicationEventSource
      //   39 OrchestrationV2EffectCancellation
      //   40 ScheduledTasks
      //   41 ProjectionThreadsSettled
      //   42 OrchestrationV2ThreadSettled
      // That ledger never registered ProjectionThreadsSnoozed, so drop snooze
      // columns current 034 installed. Old 42 already wrote V2 settled columns;
      // current 042 is ScheduledTasks only.
      yield* sql`ALTER TABLE projection_threads DROP COLUMN snoozed_until`;
      yield* sql`ALTER TABLE projection_threads DROP COLUMN snoozed_at`;

      const projectionThreadColumnsBefore = yield* sql<{ readonly name: string }>`
          PRAGMA table_info(projection_threads)
        `;
      assert.isFalse(
        projectionThreadColumnsBefore.some((column) => column.name === "snoozed_until"),
      );
      assert.isFalse(projectionThreadColumnsBefore.some((column) => column.name === "snoozed_at"));
      // Published 41 ProjectionThreadsSettled already had v1 settled columns
      // (same columns as current 033).
      assert.ok(projectionThreadColumnsBefore.some((column) => column.name === "settled_override"));
      assert.ok(projectionThreadColumnsBefore.some((column) => column.name === "settled_at"));

      const v2ColumnsBefore = yield* sql<{ readonly name: string }>`
          PRAGMA table_info(orchestration_v2_projection_threads)
        `;
      if (!v2ColumnsBefore.some((column) => column.name === "settled_override")) {
        yield* sql`
            ALTER TABLE orchestration_v2_projection_threads
            ADD COLUMN settled_override TEXT
          `;
      }
      if (!v2ColumnsBefore.some((column) => column.name === "settled_at")) {
        yield* sql`
            ALTER TABLE orchestration_v2_projection_threads
            ADD COLUMN settled_at TEXT
          `;
      }

      // Rewrite rows 33-42 to the exact historical published names (not the
      // current ledger names). Migrator only cares about max migration_id.
      for (const [migrationId, name] of PUBLISHED_V21_LEDGER_33_TO_42) {
        yield* sql`
            UPDATE effect_sql_migrations
            SET name = ${name}
            WHERE migration_id = ${migrationId}
          `;
      }

      const ledger33To42 = yield* sql<{
        readonly migration_id: number;
        readonly name: string;
      }>`
          SELECT migration_id, name
          FROM effect_sql_migrations
          WHERE migration_id BETWEEN 33 AND 42
          ORDER BY migration_id
        `;
      assert.deepStrictEqual(
        ledger33To42.map((row) => [row.migration_id, row.name]),
        PUBLISHED_V21_LEDGER_33_TO_42.map(([id, name]) => [id, name]),
      );
      const latestBefore = yield* sql<{ readonly migration_id: number }>`
          SELECT migration_id
          FROM effect_sql_migrations
          ORDER BY migration_id DESC
          LIMIT 1
        `;
      assert.equal(latestBefore[0]?.migration_id, 42);

      // Seed V1 projection rows the live snapshot readers query.
      const nowIso = "2026-07-02T12:00:00.000Z";
      const projectId = ProjectId.make("project:historical-v21");
      const v1ThreadId = ThreadId.make("thread:historical-v21-v1");
      const v2ThreadId = ThreadId.make("thread:historical-v21-v2");
      const modelSelectionJson = yield* encodeModelSelectionJson({
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5.4",
      });

      yield* sql`
          INSERT INTO projection_projects (
            project_id,
            title,
            workspace_root,
            default_model_selection_json,
            scripts_json,
            created_at,
            updated_at,
            deleted_at
          ) VALUES (
            ${projectId},
            ${"Historical project"},
            ${"/tmp/historical"},
            NULL,
            ${"[]"},
            ${nowIso},
            ${nowIso},
            NULL
          )
        `;
      yield* sql`
          INSERT INTO projection_threads (
            thread_id,
            project_id,
            title,
            model_selection_json,
            runtime_mode,
            interaction_mode,
            branch,
            worktree_path,
            latest_turn_id,
            created_at,
            updated_at,
            archived_at,
            settled_override,
            settled_at,
            latest_user_message_at,
            pending_approval_count,
            pending_user_input_count,
            has_actionable_proposed_plan,
            deleted_at
          ) VALUES (
            ${v1ThreadId},
            ${projectId},
            ${"Historical V1 thread"},
            ${modelSelectionJson},
            ${"full-access"},
            ${"default"},
            NULL,
            NULL,
            NULL,
            ${nowIso},
            ${nowIso},
            NULL,
            NULL,
            NULL,
            NULL,
            ${0},
            ${0},
            ${0},
            NULL
          )
        `;

      const v2PayloadJson = yield* encodeUnknownJson({
        createdBy: "user",
        creationSource: "web",
        id: v2ThreadId,
        projectId,
        title: "Historical V2 thread",
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
          rootThreadId: v2ThreadId,
        },
        forkedFrom: null,
        createdAt: nowIso,
        updatedAt: nowIso,
        archivedAt: null,
        deletedAt: null,
        settledOverride: "settled",
        settledAt: nowIso,
      });
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
            settled_override,
            settled_at,
            payload_json
          ) VALUES (
            ${v2ThreadId},
            ${projectId},
            ${"Historical V2 thread"},
            ${modelSelection.instanceId},
            ${modelSelection.instanceId},
            ${"full-access"},
            ${"default"},
            NULL,
            ${nowIso},
            ${nowIso},
            NULL,
            NULL,
            ${"settled"},
            ${nowIso},
            ${v2PayloadJson}
          )
        `;

      // Only 043 runs (Migrator skips every id <= 42).
      yield* runMigrations({ toMigrationInclusive: 43 });

      const latestAfter = yield* sql<{
        readonly migration_id: number;
        readonly name: string;
      }>`
          SELECT migration_id, name
          FROM effect_sql_migrations
          WHERE migration_id = 43
        `;
      assert.deepStrictEqual(latestAfter, [
        { migration_id: 43, name: "OrchestrationV2ThreadSettled" },
      ]);

      const snoozeColumns = yield* sql<{ readonly name: string }>`
          PRAGMA table_info(projection_threads)
        `;
      assert.ok(snoozeColumns.some((column) => column.name === "snoozed_until"));
      assert.ok(snoozeColumns.some((column) => column.name === "snoozed_at"));

      // Live ProjectionSnapshotQuery readers SELECT snoozed_until/snoozed_at.
      // These fail hard when the historical upgrade skips those columns.
      const metadataLayer = Layer.merge(
        Layer.succeed(RepositoryIdentityResolver.RepositoryIdentityResolver, {
          resolve: (workspaceRoot) =>
            Effect.succeed({
              canonicalKey: "example.test/historical-v21",
              locator: {
                source: "git-remote" as const,
                remoteName: "origin",
                remoteUrl: "https://example.test/historical-v21.git",
              },
              rootPath: workspaceRoot,
            }),
        }),
        Layer.succeed(ProjectFaviconResolver.ProjectFaviconResolver, {
          resolvePath: () => Effect.succeed(null),
        }),
      );
      const snapshotQueryLayer = OrchestrationProjectionSnapshotQueryLive.pipe(
        Layer.provideMerge(ProjectEnrichment.layer),
        Layer.provideMerge(metadataLayer),
        Layer.provide(Layer.succeed(SqlClient.SqlClient, sql)),
        Layer.provide(
          ServerConfig.layerTest(process.cwd(), {
            prefix: "migration-043-historical-snapshot-",
          }),
        ),
        Layer.provide(NodeServices.layer),
      );

      yield* Effect.gen(function* () {
        const query = yield* ProjectionSnapshotQuery;

        const command = yield* query.getCommandReadModel();
        const commandThread = command.threads.find((thread) => thread.id === v1ThreadId);
        assert.ok(commandThread, "expected command read model to include historical V1 thread");
        assert.isNull(commandThread.settledOverride);
        assert.isNull(commandThread.settledAt);
        assert.isNull(commandThread.snoozedUntil ?? null);
        assert.isNull(commandThread.snoozedAt ?? null);

        const shell = yield* query.getShellSnapshot();
        const shellThread = shell.threads.find((thread) => thread.id === v1ThreadId);
        assert.ok(shellThread, "expected shell snapshot to include historical V1 thread");
        assert.isNull(shellThread.settledOverride);
        assert.isNull(shellThread.settledAt);
        assert.isNull(shellThread.snoozedUntil ?? null);
        assert.isNull(shellThread.snoozedAt ?? null);

        const byId = yield* query.getThreadShellById(v1ThreadId);
        assert.isTrue(Option.isSome(byId), "expected by-id shell read for historical V1 thread");
        const byIdThread = Option.getOrThrow(byId);
        assert.isNull(byIdThread.settledOverride);
        assert.isNull(byIdThread.settledAt);
        assert.isNull(byIdThread.snoozedUntil ?? null);
        assert.isNull(byIdThread.snoozedAt ?? null);
      }).pipe(Effect.provide(snapshotQueryLayer));

      // V2 settled projection path still loads after the same upgrade step.
      const projectionStoreLayerOnSql = projectionStoreLayer.pipe(
        Layer.provide(Layer.succeed(SqlClient.SqlClient, sql)),
      );
      yield* Effect.gen(function* () {
        const store = yield* ProjectionStoreV2;
        const projection = yield* store.getThreadProjection(v2ThreadId);
        assert.equal(projection.thread.settledOverride, "settled");
        assert.equal(threadShellFromProjection(projection).settledOverride, "settled");
      }).pipe(Effect.provide(projectionStoreLayerOnSql));
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);
