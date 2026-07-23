import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/**
 * Persist settled lifecycle pins on V2 projection threads.
 *
 * Full thread payloads already carry settledOverride/settledAt after the schema
 * change; these columns keep shell/list SQL paths aligned with archived_at.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const columns = yield* sql<{ readonly name: string }>`
    PRAGMA table_info(orchestration_v2_projection_threads)
  `;

  if (!columns.some((column) => column.name === "settled_override")) {
    yield* sql`
      ALTER TABLE orchestration_v2_projection_threads
      ADD COLUMN settled_override TEXT
    `;
  }

  if (!columns.some((column) => column.name === "settled_at")) {
    yield* sql`
      ALTER TABLE orchestration_v2_projection_threads
      ADD COLUMN settled_at TEXT
    `;
  }
});
