import * as NodeServices from "@effect/platform-node/NodeServices";
import type * as PgClient from "@effect/sql-pg/PgClient";
import type { RelayAgentActivityState } from "@t3tools/contracts/relay";
import { describe, expect, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import { RelayDb } from "t3code-relay/db";
import * as AgentActivityRows from "t3code-relay/agentActivity/AgentActivityRows";
import * as DpopProofs from "t3code-relay/auth/DpopProofs";
import * as EnvironmentCredentials from "t3code-relay/environments/EnvironmentCredentials";
import * as AccountLock from "../account/AccountLock.ts";
import * as AlertLedger from "../account/AlertLedger.ts";
import * as PgLayer from "../db/PgLayer.ts";
import { loadMigrations, migrate } from "../db/Migrations.ts";
import { alertLedger } from "../testing/Fakes.testkit.ts";
import * as EnvironmentEnrollment from "./EnvironmentEnrollment.ts";
import * as EnvironmentAdministration from "./EnvironmentAdministration.ts";
import { settings, signer } from "./EnrollmentTestSupport.ts";

function database(): PgClient.PgPoolConfig {
  const value = process.env.T3CODE_PERSONAL_RELAY_TEST_DATABASE_URL;
  if (!value)
    throw new Error(
      "T3CODE_PERSONAL_RELAY_TEST_DATABASE_URL must name an owned throwaway Postgres.",
    );
  const url = new URL(value);
  if (
    url.hostname !== "127.0.0.1" ||
    url.username !== "t3relay" ||
    url.pathname !== "/t3relay" ||
    url.password !== "test-only"
  ) {
    throw new Error("Enrollment tests require the loopback-only throwaway Postgres fixture.");
  }
  return {
    host: url.hostname,
    port: Number(url.port),
    username: url.username,
    database: "t3relay",
    password: Redacted.make(url.password),
    maxConnections: 2,
  };
}

function harness() {
  const calls: Array<{ method: string; environmentId: string; key?: string }> = [];
  const failures = { remove: false, supersede: false };
  const ledger = Layer.effect(
    AlertLedger.AlertLedger,
    Effect.gen(function* () {
      const db = yield* RelayDb;
      const sql = db.$client;
      // The fake models the ledger boundary; U2 owns its production SQL and backfill policy.
      return alertLedger({
        removeEnvironmentKey: (input) =>
          Effect.gen(function* () {
            calls.push({
              method: "remove-key",
              environmentId: input.environmentId,
              key: input.environmentPublicKey,
            });
            yield* sql`DELETE FROM personal_relay_alerts WHERE environment_id = ${input.environmentId}
          AND environment_public_key = ${input.environmentPublicKey}`.pipe(Effect.orDie);
            if (failures.remove)
              return yield* new AlertLedger.AlertLedgerError({
                operation: "remove-key",
                cause: "injected",
              });
          }),
        supersedeEnvironment: (input) =>
          Effect.gen(function* () {
            calls.push({ method: "supersede", environmentId: input.environmentId });
            yield* sql`UPDATE personal_relay_alert_deliveries d SET status = 'superseded'
          FROM personal_relay_alerts a WHERE a.alert_id = d.alert_id AND a.user_id = ${input.userId}
          AND a.environment_id = ${input.environmentId} AND d.status = 'pending'`.pipe(
              Effect.orDie,
            );
            if (failures.supersede)
              return yield* new AlertLedger.AlertLedgerError({
                operation: "supersede",
                cause: "injected",
              });
          }),
      });
    }),
  );
  const repositories = Layer.mergeAll(
    AccountLock.layer,
    EnvironmentCredentials.layer,
    DpopProofs.layer,
    AgentActivityRows.layer,
    ledger,
  ).pipe(Layer.provideMerge(PgLayer.layer(database())));
  const services = Layer.mergeAll(
    EnvironmentEnrollment.layer(settings),
    EnvironmentAdministration.layer(settings),
  ).pipe(Layer.provideMerge(repositories), Layer.provideMerge(NodeServices.layer));
  const setup = Effect.gen(function* () {
    const files = yield* loadMigrations({
      relay: `${import.meta.dirname}/../../../relay/migrations/postgres`,
      personal: `${import.meta.dirname}/../../migrations`,
    });
    yield* migrate(files);
    const db = yield* RelayDb;
    yield* db.$client.unsafe(`TRUNCATE personal_relay_alert_deliveries, personal_relay_alerts,
      personal_relay_thread_versions, personal_relay_accounts, personal_relay_target_state,
      relay_environment_links, relay_environment_credentials, relay_agent_activity_rows,
      relay_dpop_proofs, relay_mobile_devices, relay_live_activities`);
  });
  return { calls, failures, services, setup };
}

const seed = Effect.fnUntraced(function* (key: string) {
  const db = yield* RelayDb;
  const sql = db.$client;
  const now = yield* DateTime.now;
  const state: RelayAgentActivityState = {
    environmentId: "u4-env" as RelayAgentActivityState["environmentId"],
    threadId: "thread" as RelayAgentActivityState["threadId"],
    projectTitle: "Project",
    threadTitle: "Thread",
    modelTitle: "Test model",
    phase: "running",
    headline: "Running",
    updatedAt: DateTime.formatIso(now),
    deepLink: "/",
  };
  const rows = yield* AgentActivityRows.AgentActivityRows;
  yield* rows.upsert({ environmentPublicKey: key, state });
  yield* sql`INSERT INTO personal_relay_thread_versions
    (environment_id, environment_public_key, thread_id, last_seq, removed, transition_id)
    VALUES ('u4-env', ${key}, 'thread', 1, false, 'request:test')`;
  yield* sql`INSERT INTO personal_relay_alerts
    (alert_id, user_id, environment_id, environment_public_key, thread_id, transition_id,
      phase, transition_at, eligible_at, row_json, expires_at)
    VALUES ('u4-alert', ${settings.ownerSubject}, 'u4-env', ${key}, 'thread', 'request:test',
      'waiting_for_approval', now(), now(), '{}', now() + interval '30 minutes')`;
  yield* sql`INSERT INTO personal_relay_alert_deliveries (alert_id, device_id, status)
    VALUES ('u4-alert', 'pending-device', 'pending'), ('u4-alert', 'accepted-device', 'accepted')`;
});

describe.skipIf(!process.env.T3CODE_PERSONAL_RELAY_TEST_DATABASE_URL)(
  "enrollment against verbatim Postgres migrations",
  () => {
    it.live(
      "inserts a complete background-only link, rotates same-key credentials, and rejects nonce replay",
      () => {
        const h = harness();
        return Effect.gen(function* () {
          yield* h.setup;
          const db = yield* RelayDb;
          const sql = db.$client;
          const enrollment = yield* EnvironmentEnrollment.EnvironmentEnrollment;
          const credentials = yield* EnvironmentCredentials.EnvironmentCredentials;
          const key = yield* signer;
          const proof = yield* key.sign();
          const first = yield* enrollment.enroll(proof);
          expect(Option.isSome(yield* credentials.authenticate(Redacted.value(first)))).toBe(true);
          const links = yield* sql`SELECT * FROM relay_environment_links`;
          expect(links).toHaveLength(1);
          expect(links[0]).toMatchObject({
            user_id: settings.ownerSubject,
            environment_id: "u4-env",
            environment_public_key: key.publicKey,
            environment_label: "Test environment",
            endpoint_http_base_url: "https://background-only.invalid",
            endpoint_ws_base_url: "wss://background-only.invalid",
            endpoint_provider_kind: "manual",
            notifications_enabled: true,
            live_activities_enabled: true,
            managed_tunnels_enabled: false,
            hold_webhooks_while_offline: false,
            created_by_device_id: null,
            revoked_at: null,
          });
          expect(links[0]?.created_at).toBeTruthy();
          expect(links[0]?.updated_at).toBeTruthy();
          const nonce = (yield* sql`SELECT thumbprint FROM relay_dpop_proofs`)[0];
          expect(nonce?.thumbprint).toMatch(/^env-enroll:/);
          yield* seed(key.publicKey);
          const repeated = yield* Effect.flip(enrollment.enroll(proof));
          expect(repeated).toMatchObject({ reason: "replayed-nonce" });
          const second = yield* enrollment.enroll(yield* key.sign({ label: "Renamed" }));
          expect(Option.isNone(yield* credentials.authenticate(Redacted.value(first)))).toBe(true);
          expect(Option.isSome(yield* credentials.authenticate(Redacted.value(second)))).toBe(true);
          expect((yield* sql`SELECT 1 FROM personal_relay_alerts`).length).toBe(1);
          expect((yield* sql`SELECT 1 FROM personal_relay_thread_versions`).length).toBe(1);
          expect((yield* sql`SELECT 1 FROM relay_agent_activity_rows`).length).toBe(1);
          expect(h.calls).toEqual([]);
          expect(
            (yield* sql`SELECT generation::text FROM personal_relay_accounts`)[0]?.generation,
          ).toBe("2");
          expect(
            (yield* sql`SELECT credential_hash FROM relay_environment_credentials`).every(
              (row) =>
                row.credential_hash !== Redacted.value(first) &&
                row.credential_hash !== Redacted.value(second),
            ),
          ).toBe(true);
        }).pipe(Effect.provide(h.services));
      },
    );

    it.live(
      "replaces a key and removes old activity, versions, alerts and cascaded deliveries atomically",
      () => {
        const h = harness();
        return Effect.gen(function* () {
          yield* h.setup;
          const sql = (yield* RelayDb).$client;
          const enrollment = yield* EnvironmentEnrollment.EnvironmentEnrollment;
          const credentials = yield* EnvironmentCredentials.EnvironmentCredentials;
          const key = yield* signer;
          const replacement = yield* signer;
          const first = yield* enrollment.enroll(yield* key.sign());
          yield* seed(key.publicKey);
          const second = yield* enrollment.enroll(yield* replacement.sign());
          expect(Option.isNone(yield* credentials.authenticate(Redacted.value(first)))).toBe(true);
          expect(Option.isSome(yield* credentials.authenticate(Redacted.value(second)))).toBe(true);
          expect(
            (yield* sql`SELECT revoked_at FROM relay_environment_credentials WHERE environment_public_key = ${key.publicKey}`)[0]
              ?.revoked_at,
          ).toBeTruthy();
          expect(yield* sql`SELECT 1 FROM relay_agent_activity_rows`).toHaveLength(0);
          expect(yield* sql`SELECT 1 FROM personal_relay_thread_versions`).toHaveLength(0);
          expect(yield* sql`SELECT 1 FROM personal_relay_alerts`).toHaveLength(0);
          expect(yield* sql`SELECT 1 FROM personal_relay_alert_deliveries`).toHaveLength(0);
          expect(h.calls).toEqual([
            { method: "remove-key", environmentId: "u4-env", key: key.publicKey },
          ]);
        }).pipe(Effect.provide(h.services));
      },
    );

    it.live("rolls back replacement and nonce consumption when the ledger cleanup fails", () => {
      const h = harness();
      return Effect.gen(function* () {
        yield* h.setup;
        const sql = (yield* RelayDb).$client;
        const enrollment = yield* EnvironmentEnrollment.EnvironmentEnrollment;
        const credentials = yield* EnvironmentCredentials.EnvironmentCredentials;
        const key = yield* signer;
        const replacement = yield* signer;
        const first = yield* enrollment.enroll(yield* key.sign());
        yield* seed(key.publicKey);
        const proof = yield* replacement.sign();
        h.failures.remove = true;
        expect((yield* Effect.flip(enrollment.enroll(proof)))._tag).toBe(
          "EnrollmentPersistenceError",
        );
        expect(
          (yield* sql`SELECT environment_public_key FROM relay_environment_links`)[0]
            ?.environment_public_key,
        ).toBe(key.publicKey);
        expect(Option.isSome(yield* credentials.authenticate(Redacted.value(first)))).toBe(true);
        expect(
          (yield* sql`SELECT generation::text FROM personal_relay_accounts`)[0]?.generation,
        ).toBe("1");
        expect(yield* sql`SELECT 1 FROM personal_relay_alerts`).toHaveLength(1);
        expect(yield* sql`SELECT 1 FROM personal_relay_alert_deliveries`).toHaveLength(2);
        expect(yield* sql`SELECT 1 FROM relay_agent_activity_rows`).toHaveLength(1);
        expect(yield* sql`SELECT 1 FROM personal_relay_thread_versions`).toHaveLength(1);
        expect(yield* sql`SELECT 1 FROM relay_dpop_proofs`).toHaveLength(1);
        h.failures.remove = false;
        yield* enrollment.enroll(proof);
      }).pipe(Effect.provide(h.services));
    });

    it.live(
      "revokes credentials and supersedes only pending deliveries, hiding activity from the worker",
      () => {
        const h = harness();
        return Effect.gen(function* () {
          yield* h.setup;
          const sql = (yield* RelayDb).$client;
          const enrollment = yield* EnvironmentEnrollment.EnvironmentEnrollment;
          const credentials = yield* EnvironmentCredentials.EnvironmentCredentials;
          const rows = yield* AgentActivityRows.AgentActivityRows;
          const key = yield* signer;
          const token = yield* enrollment.enroll(yield* key.sign());
          yield* seed(key.publicKey);
          expect(yield* rows.listForUser({ userId: settings.ownerSubject })).toHaveLength(1);
          h.failures.supersede = true;
          yield* Effect.flip(enrollment.revoke("u4-env"));
          expect(Option.isSome(yield* credentials.authenticate(Redacted.value(token)))).toBe(true);
          expect(
            (yield* sql`SELECT status FROM personal_relay_alert_deliveries WHERE device_id = 'pending-device'`)[0]
              ?.status,
          ).toBe("pending");
          h.failures.supersede = false;
          expect(yield* enrollment.revoke("u4-env")).toBe(true);
          expect(Option.isNone(yield* credentials.authenticate(Redacted.value(token)))).toBe(true);
          expect(yield* rows.listForUser({ userId: settings.ownerSubject })).toHaveLength(0);
          expect(
            (yield* sql`SELECT status FROM personal_relay_alert_deliveries WHERE device_id = 'pending-device'`)[0]
              ?.status,
          ).toBe("superseded");
          expect(
            (yield* sql`SELECT status FROM personal_relay_alert_deliveries WHERE device_id = 'accepted-device'`)[0]
              ?.status,
          ).toBe("accepted");
          expect(
            (yield* sql`SELECT generation::text FROM personal_relay_accounts`)[0]?.generation,
          ).toBe("2");
          expect(yield* enrollment.revoke("missing-env")).toBe(false);
          expect(
            (yield* sql`SELECT generation::text FROM personal_relay_accounts`)[0]?.generation,
          ).toBe("2");
        }).pipe(Effect.provide(h.services));
      },
    );

    it.live("returns sanitized device diagnostics and per-channel status", () => {
      const h = harness();
      return Effect.gen(function* () {
        yield* h.setup;
        const sql = (yield* RelayDb).$client;
        const admin = yield* EnvironmentAdministration.EnvironmentAdministration;
        yield* sql`INSERT INTO relay_mobile_devices
        (user_id, device_id, platform, label, bundle_id, aps_environment, push_token, push_to_start_token, preferences_json, created_at, updated_at)
        VALUES (${settings.ownerSubject}, 'device', 'ios', 'Phone', 'com.example.test', 'sandbox',
          'test-only-push-token-123456', 'short', '{}', 'now', 'now')`;
        yield* sql`INSERT INTO relay_live_activities (user_id, device_id, activity_push_token, created_at, updated_at)
        VALUES (${settings.ownerSubject}, 'device', 'test-only-activity-token-654321', 'now', 'now')`;
        yield* sql`INSERT INTO personal_relay_target_state (device_id, channel, failure_count)
        VALUES ('device', 'notification', 3), ('device', 'live_activity', 1)`;
        const devices = yield* admin.listDevices;
        expect(devices).toEqual([
          expect.objectContaining({
            pushTokenSuffix: "123456",
            pushToStartTokenSuffix: "[redacted]",
            activityTokenSuffix: "654321",
          }),
        ]);
        expect(JSON.stringify(devices)).not.toContain("test-only-push-token");
        const status = yield* admin.status;
        expect(status).toMatchObject({
          account: null,
          backoff: [
            { deviceId: "device", channel: "live_activity", failureCount: 1 },
            { deviceId: "device", channel: "notification", failureCount: 3 },
          ],
          pendingByPhase: {},
          recentEventCounts: null,
        });
        expect(yield* admin.config).toEqual({ ...settings, activeOverrides: [] });
        expect(yield* admin.listEnvironments).toEqual([]);
      }).pipe(Effect.provide(h.services));
    });
  },
);
