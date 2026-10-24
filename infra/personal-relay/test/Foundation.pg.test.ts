import * as NodeServices from "@effect/platform-node/NodeServices";
import * as PgClient from "@effect/sql-pg/PgClient";
import type { RelayDeviceRegistrationRequest } from "@t3tools/contracts/relay";
import { describe, expect, it } from "@effect/vitest";
import * as NodeProcess from "node:process";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Reactivity from "effect/reactivity/Reactivity";
import { RelayDb } from "t3code-relay/db";
import * as Devices from "t3code-relay/agentActivity/Devices";
import * as PgLayer from "../src/db/PgLayer.ts";
import * as AccountLock from "../src/account/AccountLock.ts";
import { loadMigrations, migrate, verifyMigrations } from "../src/db/Migrations.ts";

const port = NodeProcess.env.PERSONAL_RELAY_TEST_PORT;
const config: PgClient.PgPoolConfig = {
  host: "127.0.0.1",
  port: Number(port),
  username: "t3relay",
  password: Redacted.make("test-only"),
  database: "t3relay",
  maxConnections: 2,
  connectTimeout: "2 seconds",
};
const services = Layer.mergeAll(AccountLock.layer, Devices.layer).pipe(
  Layer.provideMerge(PgLayer.layer(config)),
);

// The port comes from an owned throwaway container, never a production URL.
describe.skipIf(port === undefined)("foundation Postgres spikes (S4)", () => {
  it.live(
    "migrates empty PG17, checksums, atomic repository writes, timeout and session lock",
    () =>
      Effect.gen(function* () {
        const db = yield* RelayDb;
        const sql = db.$client;
        const account = yield* AccountLock.AccountLock;
        const devices = yield* Devices.Devices;
        const files = yield* loadMigrations({
          relay: `${import.meta.dirname}/../../relay/migrations/postgres`,
          personal: `${import.meta.dirname}/../migrations`,
        });
        yield* migrate(files);
        yield* migrate(files);
        yield* verifyMigrations(files);
        const checksumError = yield* Effect.flip(
          verifyMigrations(
            files.map((file, i) => (i === 0 ? { ...file, sha256: "changed" } : file)),
          ),
        );
        expect(checksumError.operation).toBe("verify");
        const bad = {
          name: "personal/test-rollback",
          sha256: "test",
          sql: "CREATE TABLE u0_rollback (id text);\n--> statement-breakpoint\nSELECT missing_u0_column",
        };
        yield* Effect.flip(migrate([bad]));
        const rolledBack = yield* sql<{
          name: string | null;
        }>`SELECT to_regclass('u0_rollback')::text AS name`;
        expect(rolledBack[0]?.name).toBeNull();
        expect(
          (yield* sql`SELECT 1 FROM personal_relay_migrations WHERE name = 'personal/test-rollback'`)
            .length,
        ).toBe(0);

        yield* sql`DELETE FROM relay_mobile_devices WHERE user_id = 'u0-owner'`;
        yield* sql`DELETE FROM personal_relay_accounts WHERE user_id = 'u0-owner'`;
        const registration = {
          deviceId: "u0-device",
          label: "Test",
          platform: "ios",
          iosMajorVersion: 18,
          appVersion: "1.0.0",
          bundleId: "com.example.test",
          apsEnvironment: "sandbox",
          pushToken: "test-token",
          preferences: {
            notificationsEnabled: true,
            liveActivitiesEnabled: true,
            notifyOnApproval: true,
            notifyOnInput: true,
            notifyOnCompletion: true,
            notifyOnFailure: true,
          },
        } as RelayDeviceRegistrationRequest;
        const aborted = yield* Effect.flip(
          account.withAccountLock(
            "u0-owner",
            Effect.gen(function* () {
              yield* devices.register({ userId: "u0-owner", registration });
              yield* account.bumpGeneration("u0-owner");
              return yield* Effect.fail("injected rollback");
            }),
          ),
        );
        expect(aborted).toBe("injected rollback");
        expect(
          (yield* sql`SELECT 1 FROM relay_mobile_devices WHERE user_id = 'u0-owner'`).length,
        ).toBe(0);
        expect(
          (yield* sql`SELECT 1 FROM personal_relay_accounts WHERE user_id = 'u0-owner'`).length,
        ).toBe(0);
        yield* account.withAccountLock(
          "u0-owner",
          Effect.gen(function* () {
            yield* devices.register({ userId: "u0-owner", registration });
            yield* account.bumpGeneration("u0-owner");
          }),
        );
        expect(
          (yield* sql`SELECT 1 FROM relay_mobile_devices WHERE user_id = 'u0-owner'`).length,
        ).toBe(1);

        const acquired = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const holder = yield* account
          .withAccountLock(
            "u0-owner",
            Deferred.succeed(acquired, undefined).pipe(Effect.andThen(Deferred.await(release))),
          )
          .pipe(Effect.forkChild);
        yield* Deferred.await(acquired);
        const start = yield* Clock.currentTimeMillis;
        const timeoutError = yield* Effect.flip(account.withAccountLock("u0-owner", Effect.void));
        const elapsed = (yield* Clock.currentTimeMillis) - start;
        expect(timeoutError._tag).toBe("AccountLockError");
        expect(elapsed).toBeGreaterThanOrEqual(1800);
        expect(elapsed).toBeLessThan(4000);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(holder);
        yield* account.withAccountLock("u0-owner", Effect.void);

        // makeClient owns a physical session outside the serving pool.
        const dedicated = yield* PgClient.makeClient(config).pipe(Effect.provide(Reactivity.layer));
        const second = yield* PgClient.makeClient(config).pipe(Effect.provide(Reactivity.layer));
        const key = "t3-relay:u0-worker-spike";
        expect(
          (yield* dedicated<{
            acquired: boolean;
          }>`SELECT pg_try_advisory_lock(hashtextextended(${key}, 0)) AS acquired`)[0]?.acquired,
        ).toBe(true);
        expect(
          (yield* second<{
            acquired: boolean;
          }>`SELECT pg_try_advisory_lock(hashtextextended(${key}, 0)) AS acquired`)[0]?.acquired,
        ).toBe(false);
        yield* dedicated`SELECT pg_advisory_unlock(hashtextextended(${key}, 0))`;
        expect(
          (yield* second<{
            acquired: boolean;
          }>`SELECT pg_try_advisory_lock(hashtextextended(${key}, 0)) AS acquired`)[0]?.acquired,
        ).toBe(true);
        yield* second`SELECT pg_advisory_unlock(hashtextextended(${key}, 0))`;
      }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(services, NodeServices.layer))),
  );
});
