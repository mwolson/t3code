import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/**
 * Trial v2.1 upgrade bridge for renumbered migrations.
 *
 * Exact published `origin/trial/orchestrator-v2.1` ledger (IDs 33-42):
 *   33 OrchestrationV2
 *   34 OrchestrationV2Subagents
 *   35 OrchestrationV2Foundation
 *   36 OrchestrationV2ProviderSessionBindings
 *   37 OrchestrationV2ThreadLaunchWorkflows
 *   38 ApplicationEventSource
 *   39 OrchestrationV2EffectCancellation
 *   40 ScheduledTasks
 *   41 ProjectionThreadsSettled          (v1 projection_threads settled_*)
 *   42 OrchestrationV2ThreadSettled      (v2 settled_*)
 *
 * That ledger never registered ProjectionThreadsSnoozed. Current main/CTM
 * numbering is:
 *   33 ProjectionThreadsSettled
 *   34 ProjectionThreadsSnoozed
 *   35-41 V2 stack
 *   42 ScheduledTasks
 *   43 OrchestrationV2ThreadSettled
 *
 * Effect Migrator skips every ID <= latest recorded ID. A userdata DB already
 * at published 42 therefore only runs this step and would never apply 034
 * snooze columns, while current snapshot SQL still SELECTs
 * projection_threads.snoozed_until / snoozed_at.
 *
 * Idempotent for clean installs and historical upgrades:
 * - repairs v1 projection_threads snooze columns when missing
 * - adds V2 settled columns when missing
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // V1 projection_threads: snooze columns (never present on published v2.1).
  const projectionThreadColumns = yield* sql<{ readonly name: string }>`
    PRAGMA table_info(projection_threads)
  `;

  if (!projectionThreadColumns.some((column) => column.name === "snoozed_until")) {
    yield* sql`
      ALTER TABLE projection_threads
      ADD COLUMN snoozed_until TEXT
    `;
  }

  if (!projectionThreadColumns.some((column) => column.name === "snoozed_at")) {
    yield* sql`
      ALTER TABLE projection_threads
      ADD COLUMN snoozed_at TEXT
    `;
  }

  // V2 projection threads: settled pin columns for shell/list SQL paths.
  // Published 42 already added these; clean installs through 42 (ScheduledTasks)
  // still need them here.
  const v2Columns = yield* sql<{ readonly name: string }>`
    PRAGMA table_info(orchestration_v2_projection_threads)
  `;

  if (!v2Columns.some((column) => column.name === "settled_override")) {
    yield* sql`
      ALTER TABLE orchestration_v2_projection_threads
      ADD COLUMN settled_override TEXT
    `;
  }

  if (!v2Columns.some((column) => column.name === "settled_at")) {
    yield* sql`
      ALTER TABLE orchestration_v2_projection_threads
      ADD COLUMN settled_at TEXT
    `;
  }
});
