import type { EnvironmentId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type { SQLiteDatabase } from "expo-sqlite";

const DATABASE_NAME = "t3code-client.db";
const DATABASE_SCHEMA_VERSION = 1;
const LEGACY_CACHE_DIRECTORIES = [
  "connection-shell-snapshots",
  "shell-snapshots",
  "connection-thread-snapshots",
  "connection-server-configs",
  "connection-vcs-refs",
] as const;

export const ClientCacheKind = Schema.Literals(["shell", "thread", "server-config", "vcs-refs"]);
export type ClientCacheKind = typeof ClientCacheKind.Type;

export interface ClientCacheSummaryRow {
  readonly environmentId: EnvironmentId;
  readonly kind: ClientCacheKind;
  readonly recordCount: number;
  readonly payloadBytes: number;
}

export interface StoredPreferencesJson {
  readonly payload: string;
  readonly updatedAt: number;
}

const ClientCacheSummaryRows = Schema.Array(
  Schema.Struct({
    environmentId: Schema.String,
    kind: ClientCacheKind,
    recordCount: Schema.Number,
    payloadBytes: Schema.Number,
  }),
);

const MobileDatabaseOperation = Schema.Literals([
  "open",
  "migrate",
  "load-cache",
  "save-cache",
  "remove-cache",
  "clear-environment-cache",
  "clear-all-caches",
  "inspect-caches",
  "load-preferences",
  "save-preferences",
]);

export class MobileDatabaseError extends Schema.TaggedErrorClass<MobileDatabaseError>()(
  "MobileDatabaseError",
  {
    operation: MobileDatabaseOperation,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Mobile database operation failed: ${this.operation}.`;
  }
}

function databaseError(operation: typeof MobileDatabaseOperation.Type) {
  return (cause: unknown) => new MobileDatabaseError({ operation, cause });
}

interface LegacyCacheRecord {
  readonly environmentId: string;
  readonly kind: ClientCacheKind;
  readonly cacheKey: string;
  readonly schemaVersion: number;
  readonly payload: string;
}

function objectRecord(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null ? (value as Record<string, unknown>) : null;
}

export function decodeLegacyCacheRecord(
  directoryName: (typeof LEGACY_CACHE_DIRECTORIES)[number],
  payload: string,
): LegacyCacheRecord | null {
  let parsed: Record<string, unknown> | null;
  try {
    parsed = objectRecord(JSON.parse(payload));
  } catch {
    return null;
  }
  if (
    parsed === null ||
    typeof parsed.environmentId !== "string" ||
    typeof parsed.schemaVersion !== "number"
  ) {
    return null;
  }

  switch (directoryName) {
    case "connection-shell-snapshots":
    case "shell-snapshots":
      return {
        environmentId: parsed.environmentId,
        kind: "shell",
        cacheKey: "snapshot",
        schemaVersion: parsed.schemaVersion,
        payload,
      };
    case "connection-thread-snapshots":
      return typeof parsed.threadId === "string"
        ? {
            environmentId: parsed.environmentId,
            kind: "thread",
            cacheKey: parsed.threadId,
            schemaVersion: parsed.schemaVersion,
            payload,
          }
        : null;
    case "connection-server-configs":
      return {
        environmentId: parsed.environmentId,
        kind: "server-config",
        cacheKey: "config",
        schemaVersion: parsed.schemaVersion,
        payload,
      };
    case "connection-vcs-refs":
      return typeof parsed.cwd === "string"
        ? {
            environmentId: parsed.environmentId,
            kind: "vcs-refs",
            cacheKey: parsed.cwd,
            schemaVersion: parsed.schemaVersion,
            payload,
          }
        : null;
  }
}

async function migrateLegacyFileCaches(database: SQLiteDatabase): Promise<boolean> {
  try {
    const { Directory, File, Paths } = await import("expo-file-system");
    let complete = true;
    const listFiles = (
      directory: InstanceType<typeof Directory>,
    ): Array<InstanceType<typeof File>> =>
      directory.list().flatMap((entry) => (entry instanceof File ? [entry] : listFiles(entry)));

    for (const directoryName of LEGACY_CACHE_DIRECTORIES) {
      try {
        const directory = new Directory(Paths.document, directoryName);
        if (!directory.exists) continue;
        for (const file of listFiles(directory)) {
          const payload = await file.text();
          const record = decodeLegacyCacheRecord(directoryName, payload);
          if (record === null) continue;
          await database.runAsync(
            `INSERT INTO client_cache
              (environment_id, kind, cache_key, schema_version, payload, updated_at)
             VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT (environment_id, kind, cache_key) DO NOTHING`,
            record.environmentId,
            record.kind,
            record.cacheKey,
            record.schemaVersion,
            record.payload,
            Date.now(),
          );
        }
        directory.delete();
      } catch (cause) {
        complete = false;
        console.warn(`[mobile-database] could not migrate legacy cache ${directoryName}`, cause);
      }
    }
    return complete;
  } catch (cause) {
    console.warn("[mobile-database] could not load legacy cache migration", cause);
    return false;
  }
}

export class MobileDatabase extends Context.Service<
  MobileDatabase,
  {
    readonly loadCache: (
      environmentId: EnvironmentId,
      kind: ClientCacheKind,
      cacheKey: string,
    ) => Effect.Effect<Option.Option<string>, MobileDatabaseError>;
    readonly saveCache: (
      environmentId: EnvironmentId,
      kind: ClientCacheKind,
      cacheKey: string,
      schemaVersion: number,
      payload: string,
    ) => Effect.Effect<void, MobileDatabaseError>;
    readonly removeCache: (
      environmentId: EnvironmentId,
      kind: ClientCacheKind,
      cacheKey: string,
    ) => Effect.Effect<void, MobileDatabaseError>;
    readonly clearEnvironmentCache: (
      environmentId: EnvironmentId,
    ) => Effect.Effect<void, MobileDatabaseError>;
    readonly clearAllCaches: Effect.Effect<void, MobileDatabaseError>;
    readonly inspectCaches: Effect.Effect<
      ReadonlyArray<ClientCacheSummaryRow>,
      MobileDatabaseError
    >;
    readonly loadPreferencesJson: Effect.Effect<
      Option.Option<StoredPreferencesJson>,
      MobileDatabaseError
    >;
    readonly savePreferencesJson: (
      payload: string,
      updatedAt: number,
    ) => Effect.Effect<void, MobileDatabaseError>;
  }
>()("@t3tools/mobile/persistence/MobileDatabase") {}

async function openAndMigrateDatabase(): Promise<SQLiteDatabase> {
  let database: SQLiteDatabase;
  try {
    const SQLite = await import("expo-sqlite");
    database = await SQLite.openDatabaseAsync(DATABASE_NAME);
  } catch (cause) {
    throw databaseError("open")(cause);
  }

  try {
    await database.execAsync("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;");
    const schema = await database.getFirstAsync<{ readonly user_version: number }>(
      "PRAGMA user_version",
    );
    await database.withExclusiveTransactionAsync(async (transaction) => {
      await transaction.execAsync(`
              CREATE TABLE IF NOT EXISTS client_cache (
                environment_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                cache_key TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                payload TEXT NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (environment_id, kind, cache_key)
              ) WITHOUT ROWID;

              CREATE INDEX IF NOT EXISTS client_cache_environment_updated
                ON client_cache (environment_id, updated_at DESC);

              CREATE TABLE IF NOT EXISTS client_preferences (
                singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1),
                payload TEXT NOT NULL,
                updated_at INTEGER NOT NULL
              );
            `);
    });
    if ((schema?.user_version ?? 0) < DATABASE_SCHEMA_VERSION) {
      const migrated = await migrateLegacyFileCaches(database);
      if (migrated) {
        await database.execAsync(`PRAGMA user_version = ${DATABASE_SCHEMA_VERSION};`);
      }
    }
  } catch (cause) {
    if (cause instanceof MobileDatabaseError) {
      throw cause;
    }
    throw databaseError("migrate")(cause);
  }

  return database;
}

/**
 * Build the MobileDatabase service without opening SQLite yet.
 *
 * Opening the native DB during ManagedRuntime/layer construction runs while
 * Hermes is still evaluating the app bundle. That deadlocks the JS bridge
 * (openDatabaseAsync waits for native while native waits for evaluateJavaScript
 * to finish) and leaves the app stuck on the splash screen.
 */
const makeAvailable = Effect.sync(() => {
  let databasePromise: Promise<SQLiteDatabase> | null = null;

  const getDatabase = (): Promise<SQLiteDatabase> => {
    if (databasePromise === null) {
      databasePromise = openAndMigrateDatabase().catch((cause) => {
        databasePromise = null;
        throw cause;
      });
    }
    return databasePromise;
  };

  const withDatabase = <A>(
    operation: typeof MobileDatabaseOperation.Type,
    use: (database: SQLiteDatabase) => Promise<A>,
  ): Effect.Effect<A, MobileDatabaseError> =>
    Effect.tryPromise({
      try: async () => use(await getDatabase()),
      catch: (cause) =>
        cause instanceof MobileDatabaseError ? cause : databaseError(operation)(cause),
    });

  return MobileDatabase.of({
    loadCache: Effect.fn("MobileDatabase.loadCache")((environmentId, kind, cacheKey) =>
      withDatabase("load-cache", (database) =>
        database.getFirstAsync<{ readonly payload: string }>(
          `SELECT payload
                     FROM client_cache
                     WHERE environment_id = ? AND kind = ? AND cache_key = ?`,
          environmentId,
          kind,
          cacheKey,
        ),
      ).pipe(Effect.map((row) => Option.fromNullishOr(row?.payload))),
    ),
    saveCache: Effect.fn("MobileDatabase.saveCache")(
      (environmentId, kind, cacheKey, schemaVersion, payload) =>
        withDatabase("save-cache", (database) =>
          database.runAsync(
            `INSERT INTO client_cache
                      (environment_id, kind, cache_key, schema_version, payload, updated_at)
                     VALUES (?, ?, ?, ?, ?, ?)
                     ON CONFLICT (environment_id, kind, cache_key) DO UPDATE SET
                       schema_version = excluded.schema_version,
                       payload = excluded.payload,
                       updated_at = excluded.updated_at`,
            environmentId,
            kind,
            cacheKey,
            schemaVersion,
            payload,
            Date.now(),
          ),
        ).pipe(Effect.asVoid),
    ),
    removeCache: Effect.fn("MobileDatabase.removeCache")((environmentId, kind, cacheKey) =>
      withDatabase("remove-cache", (database) =>
        database.runAsync(
          `DELETE FROM client_cache
                     WHERE environment_id = ? AND kind = ? AND cache_key = ?`,
          environmentId,
          kind,
          cacheKey,
        ),
      ).pipe(Effect.asVoid),
    ),
    clearEnvironmentCache: Effect.fn("MobileDatabase.clearEnvironmentCache")((environmentId) =>
      withDatabase("clear-environment-cache", (database) =>
        database.runAsync("DELETE FROM client_cache WHERE environment_id = ?", environmentId),
      ).pipe(Effect.asVoid),
    ),
    clearAllCaches: withDatabase("clear-all-caches", (database) =>
      database.runAsync("DELETE FROM client_cache"),
    ).pipe(Effect.asVoid),
    inspectCaches: withDatabase("inspect-caches", (database) =>
      database.getAllAsync<unknown>(`
                SELECT
                  environment_id AS environmentId,
                  kind,
                  COUNT(*) AS recordCount,
                  COALESCE(SUM(LENGTH(CAST(payload AS BLOB))), 0) AS payloadBytes
                FROM client_cache
                GROUP BY environment_id, kind
                ORDER BY environment_id, kind
              `),
    ).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(ClientCacheSummaryRows)),
      Effect.mapError(databaseError("inspect-caches")),
      Effect.map(
        (rows): ReadonlyArray<ClientCacheSummaryRow> =>
          rows.map((row) => ({
            environmentId: row.environmentId as EnvironmentId,
            kind: row.kind,
            recordCount: row.recordCount,
            payloadBytes: row.payloadBytes,
          })),
      ),
    ),
    loadPreferencesJson: withDatabase("load-preferences", (database) =>
      database.getFirstAsync<StoredPreferencesJson>(
        `SELECT payload, updated_at AS updatedAt
                 FROM client_preferences
                 WHERE singleton = 1`,
      ),
    ).pipe(Effect.map(Option.fromNullishOr)),
    savePreferencesJson: Effect.fn("MobileDatabase.savePreferencesJson")((payload, updatedAt) =>
      withDatabase("save-preferences", (database) =>
        database.runAsync(
          `INSERT INTO client_preferences (singleton, payload, updated_at)
                   VALUES (1, ?, ?)
                   ON CONFLICT (singleton) DO UPDATE SET
                     payload = excluded.payload,
                     updated_at = excluded.updated_at`,
          payload,
          updatedAt,
        ),
      ).pipe(Effect.asVoid),
    ),
  });
});

export const make = makeAvailable;

export const layer = Layer.effect(MobileDatabase, make);
