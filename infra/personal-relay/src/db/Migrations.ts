import * as Crypto from "effect/Crypto";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Hex from "effect/encoding/Hex";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { RelayDb, RelayTransactions } from "t3code-relay/db";

export interface Migration {
  readonly name: string;
  readonly sha256: string;
  readonly sql: string;
}
export class MigrationError extends Schema.TaggedError<MigrationError>()("MigrationError", {
  operation: Schema.Literals(["read", "apply", "verify"]),
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return "Personal relay migration failed.";
  }
}

export const loadMigrations = Effect.fn("personalRelay.loadMigrations")(
  function* (paths: { readonly relay: string; readonly personal: string }) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const crypto = yield* Crypto.Crypto;
    const migrations: Migration[] = [];
    const append = Effect.fnUntraced(function* (name: string, file: string) {
      const sql = yield* fs.readFileString(file);
      const sha256 = Hex.encode(yield* crypto.digest("SHA-256", new TextEncoder().encode(sql)));
      migrations.push({ name, sql, sha256 });
    });
    for (const name of (yield* fs.readDirectory(paths.relay)).sort()) {
      yield* append(`relay/${name}`, path.join(paths.relay, name, "migration.sql"));
    }
    for (const name of (yield* fs.readDirectory(paths.personal))
      .filter((name) => name.endsWith(".sql"))
      .sort()) {
      yield* append(`personal/${name}`, path.join(paths.personal, name));
    }
    return migrations;
  },
  Effect.mapError((cause) => new MigrationError({ operation: "read", cause })),
);

export const migrate = Effect.fn("personalRelay.migrate")(function* (
  migrations: ReadonlyArray<Migration>,
) {
  const db = yield* RelayDb;
  const transactions = yield* RelayTransactions;
  const sql = db.$client;
  for (const migration of migrations) {
    yield* transactions
      .withTransaction(
        Effect.gen(function* () {
          yield* sql`SELECT pg_advisory_xact_lock(hashtextextended('t3-relay:migrate', 0))`;
          yield* sql.unsafe(`CREATE TABLE IF NOT EXISTS personal_relay_migrations (
        name text PRIMARY KEY, sha256 text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now()
      )`);
          const rows = yield* sql<{
            sha256: string;
          }>`SELECT sha256 FROM personal_relay_migrations WHERE name = ${migration.name}`;
          if (rows[0] !== undefined) {
            if (rows[0].sha256 !== migration.sha256)
              return yield* new MigrationError({
                operation: "verify",
                cause: `Checksum mismatch: ${migration.name}`,
              });
            return;
          }
          for (const statement of migration.sql
            .split("--> statement-breakpoint")
            .map((s) => s.trim())
            .filter(Boolean)) {
            yield* sql.unsafe(statement);
          }
          yield* sql`INSERT INTO personal_relay_migrations (name, sha256) VALUES (${migration.name}, ${migration.sha256})`;
        }),
      )
      .pipe(Effect.mapError((cause) => new MigrationError({ operation: "apply", cause })));
  }
});

export const verifyMigrations = Effect.fn("personalRelay.verifyMigrations")(
  function* (migrations: ReadonlyArray<Migration>) {
    const db = yield* RelayDb;
    const sql = db.$client;
    const table = yield* sql<{
      name: string | null;
    }>`SELECT to_regclass('personal_relay_migrations')::text AS name`;
    if (table[0]?.name == null)
      return yield* new MigrationError({ operation: "verify", cause: "Migration ledger missing" });
    const rows = yield* sql<{
      name: string;
      sha256: string;
    }>`SELECT name, sha256 FROM personal_relay_migrations`;
    if (
      rows.length !== migrations.length ||
      migrations.some(
        (file) => !rows.some((row) => row.name === file.name && row.sha256 === file.sha256),
      )
    ) {
      return yield* new MigrationError({
        operation: "verify",
        cause: "Pending, unknown or changed migration",
      });
    }
  },
  Effect.mapError((cause) => new MigrationError({ operation: "verify", cause })),
);
