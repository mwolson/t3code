// @effect-diagnostics-next-line nodeBuiltinImport:off -- Self-check generates a disposable token signing key.
import * as NodeCrypto from "node:crypto";
import * as ConfigProvider from "effect/ConfigProvider";
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeProcess from "node:process";
import * as Path from "effect/Path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as References from "effect/References";
import * as Redacted from "effect/Redacted";
import * as ApnsDeliveries from "t3code-relay/agentActivity/ApnsDeliveries";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import { foundationLayer, servingLayer } from "./layers.ts";
import { activeOverrides, databaseConfig, PersonalRelayConfig } from "./Config.ts";
import * as PgLayer from "./db/PgLayer.ts";
import { loadMigrations, migrate, verifyMigrations } from "./db/Migrations.ts";
import { serve } from "./http/Server.ts";
import { AccountWorker } from "./account/AccountWorker.ts";
import { Pruner } from "./maintenance/Pruner.ts";
import * as WorkerLeadership from "./maintenance/WorkerLeadership.ts";

const program = Effect.gen(function* () {
  const command = NodeProcess.argv[2];
  const path = yield* Path.Path;
  if (command === "self-check") {
    const signingKey = NodeCrypto.generateKeyPairSync("ed25519")
      .privateKey.export({ type: "pkcs8", format: "pem" })
      .toString();
    yield* Layer.build(servingLayer).pipe(
      Effect.provideService(
        ConfigProvider.ConfigProvider,
        ConfigProvider.fromEnvRecord({
          PERSONAL_RELAY_OWNER_SUBJECT: "self-check-owner",
          PERSONAL_RELAY_DATABASE_PASSWORD: "test-only",
          PERSONAL_RELAY_TOKEN_SIGNING_PRIVATE_KEY: signingKey,
          APNS_ENABLED: "false",
        }),
      ),
    );
    yield* Effect.gen(function* () {
      yield* ApnsDeliveries.ApnsDeliveries;
      yield* Effect.logInfo("Personal relay services constructed (APNs disabled)");
    }).pipe(
      Effect.provide(
        foundationLayer({
          host: "127.0.0.1",
          database: "t3relay",
          username: "t3relay",
          password: Redacted.make("test-only"),
          maxConnections: 1,
        }),
      ),
    );
    return;
  }
  if (command === "migrate") {
    const migrationRoot = yield* Config.String("PERSONAL_RELAY_MIGRATIONS_DIR").pipe(
      Config.withDefault(path.join(import.meta.dirname, "migrations")),
    );
    const migrations = yield* loadMigrations({
      relay: path.join(migrationRoot, "relay"),
      personal: path.join(migrationRoot, "personal"),
    });
    const database = yield* databaseConfig;
    yield* migrate(migrations).pipe(
      Effect.provide(PgLayer.layer({ ...database, maxConnections: 1 })),
    );
    return;
  }
  if (command === "serve") {
    const database = yield* databaseConfig;
    return yield* Effect.gen(function* () {
      const leadership = yield* WorkerLeadership.WorkerLeadership;
      const migrationRoot = yield* Config.String("PERSONAL_RELAY_MIGRATIONS_DIR").pipe(
        Config.withDefault(path.join(import.meta.dirname, "migrations")),
      );
      const migrations = yield* loadMigrations({
        relay: path.join(migrationRoot, "relay"),
        personal: path.join(migrationRoot, "personal"),
      });
      return yield* leadership.guard(
        Effect.gen(function* () {
          yield* verifyMigrations(migrations);
          const config = yield* PersonalRelayConfig;
          const worker = yield* AccountWorker;
          const pruner = yield* Pruner;
          return yield* Effect.gen(function* () {
            if (config.apns !== null)
              yield* Effect.logInfo("APNs key loaded", { keyId: config.apns.keyId });
            for (const mode of activeOverrides(config))
              yield* Effect.logWarning("Personal relay override active", { mode });
            yield* pruner.prune(config.ownerSubject);
            return yield* Effect.all(
              [worker.run(config.ownerSubject), pruner.run(config.ownerSubject), serve],
              { concurrency: "unbounded", discard: true },
            );
          }).pipe(Effect.provideService(References.MinimumLogLevel, config.logLevel));
        }).pipe(Effect.provide(servingLayer)),
      );
    }).pipe(Effect.provide(WorkerLeadership.layer(database)));
  }
  return yield* Effect.fail("Usage: main.mjs serve | migrate | self-check");
});
NodeRuntime.runMain(
  program.pipe(
    Effect.scoped,
    Effect.catchTags({
      WorkerLeadershipError: (error) =>
        Effect.sync(() => {
          NodeProcess.stderr.write(`${error.message}\n`);
          process.exitCode = error.exitCode;
        }),
    }),
    Effect.provide(Layer.merge(NodeServices.layer, Logger.layer([Logger.consoleJson]))),
  ),
);
