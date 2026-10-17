import { networkSender } from "./NetworkSender.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type {
  RelayAgentActivityState,
  RelayDeviceRegistrationRequest,
} from "@t3tools/contracts/relay";
import * as NodeProcess from "node:process";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as AccountLock from "../../src/account/AccountLock.ts";
import * as AccountMutations from "../../src/account/AccountMutations.ts";
import * as AlertLedger from "../../src/account/AlertLedger.ts";
import * as PgLayer from "../../src/db/PgLayer.ts";
import * as AgentActivityRows from "t3code-relay/agentActivity/AgentActivityRows";
import * as Devices from "t3code-relay/agentActivity/Devices";
import * as DeliveryAttempts from "t3code-relay/agentActivity/DeliveryAttempts";
import * as RelayConfig from "t3code-relay/Config";
import * as ApnsClient from "t3code-relay/agentActivity/ApnsClient";
import * as ApnsProviderTokens from "t3code-relay/agentActivity/ApnsProviderTokens";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as TargetBackoff from "../../src/account/TargetBackoff.ts";
import * as DpopProofs from "t3code-relay/auth/DpopProofs";
import * as TargetPlanner from "../../src/account/TargetPlanner.ts";
import * as AccountWorker from "../../src/account/AccountWorker.ts";
import { ApnsSender } from "../../src/apns/ApnsSender.ts";
import * as LiveActivities from "t3code-relay/agentActivity/LiveActivities";
import { RelayDb } from "t3code-relay/db";
import { loadMigrations, migrate } from "../../src/db/Migrations.ts";

const url = NodeProcess.env.T3CODE_PERSONAL_RELAY_TEST_DATABASE_URL;
const parsed = url === undefined ? undefined : new URL(url);
if (parsed !== undefined && (parsed.hostname !== "127.0.0.1" || parsed.pathname !== "/t3relay"))
  throw new Error("Tests require a throwaway loopback t3relay database");
export const config = {
  host: "127.0.0.1",
  port: Number(parsed?.port ?? 0),
  username: "t3relay",
  password: Redacted.make("test-only"),
  database: "t3relay",
  maxConnections: 8,
};
const repositories = Layer.mergeAll(
  AccountLock.layer,
  AgentActivityRows.layer,
  Devices.layer,
  DeliveryAttempts.layer,
  TargetBackoff.layer,
  DpopProofs.layer,
  LiveActivities.layer,
  AlertLedger.layer,
).pipe(Layer.provideMerge(PgLayer.layer(config)), Layer.provideMerge(NodeServices.layer));
export const services = AccountMutations.layer.pipe(Layer.provideMerge(repositories));
export const owner = "u2-owner";
export const environmentId = EnvironmentId.make("u2-environment");
export const key = "u2-public-key";
export const registration: RelayDeviceRegistrationRequest = {
  deviceId: "u2-device",
  label: "test",
  platform: "ios",
  iosMajorVersion: 18,
  bundleId: "test.bundle",
  apsEnvironment: "sandbox",
  pushToken: "push-token",
  preferences: {
    notificationsEnabled: true,
    liveActivitiesEnabled: true,
    notifyOnApproval: true,
    notifyOnInput: true,
    notifyOnCompletion: true,
    notifyOnFailure: true,
  },
};
export function state(
  now: string,
  thread = "thread",
  phase: RelayAgentActivityState["phase"] = "completed",
): RelayAgentActivityState {
  return {
    environmentId,
    threadId: ThreadId.make(thread),
    projectTitle: "Project",
    threadTitle: thread,
    modelTitle: "Model",
    phase,
    headline: "Done",
    updatedAt: now,
    deepLink: "/thread",
  };
}
export const initialize = Effect.gen(function* () {
  yield* migrate(
    yield* loadMigrations({
      relay: `${import.meta.dirname}/../../../relay/migrations/postgres`,
      personal: `${import.meta.dirname}/../../migrations`,
    }),
  );
});
export const reset = Effect.gen(function* () {
  const sql = (yield* RelayDb).$client;
  yield* sql.unsafe(
    "TRUNCATE personal_relay_alerts, personal_relay_thread_versions, personal_relay_target_state, personal_relay_accounts, relay_mobile_devices, relay_live_activities, relay_agent_activity_rows, relay_environment_links, relay_delivery_attempts, relay_dpop_proofs CASCADE",
  );
  yield* sql`INSERT INTO relay_environment_links (environment_id, environment_public_key, user_id, endpoint_http_base_url, endpoint_ws_base_url, endpoint_provider_kind, notifications_enabled, live_activities_enabled, created_at, updated_at)
    VALUES (${environmentId}, ${key}, ${owner}, 'http://test.invalid', 'ws://test.invalid', 'manual', true, true, now(), now())`;
});
export const publish = Effect.fn(function* (
  sequence: number,
  row: RelayAgentActivityState | null,
  transition = "run:1:completed",
  eligibleAt = row?.updatedAt ?? null,
  transitionAt = eligibleAt,
) {
  const mutations = yield* AccountMutations.AccountMutations;
  yield* mutations.publish({
    userId: owner,
    environmentId,
    environmentPublicKey: key,
    threadId: row?.threadId ?? "thread",
    request: { state: row, proof: "test-proof" },
    claims: {
      t3_publish_seq: sequence,
      t3_transition: transitionAt === null ? null : { id: transition, at: transitionAt },
      t3_transition_eligible_at: eligibleAt,
    },
  });
});

const relayConfig = RelayConfig.layer({
  relayIssuer: "https://test.invalid",
  apns: {
    teamId: "test",
    keyId: "test",
    privateKey: Redacted.make("test-only"),
    bundleId: "test.bundle",
    environment: "sandbox",
  },
  clerkSecretKey: Redacted.make("test"),
  clerkPublishableKey: "test",
  clerkJwtAudience: "test",
  apnsDeliveryJobSigningSecret: Redacted.make("test-only"),
  cloudMintPrivateKey: Redacted.make("test-only"),
  cloudMintPublicKey: "test",
  managedEndpointBaseDomain: undefined,
  managedEndpointNamespace: undefined,
});
const apns = ApnsClient.layer.pipe(
  Layer.provide(ApnsProviderTokens.layer),
  Layer.provide(FetchHttpClient.layer),
);
export const plannerServices = TargetPlanner.layer.pipe(
  Layer.provideMerge(services),
  Layer.provideMerge(apns),
  Layer.provideMerge(relayConfig),
);
export const workerServices = (sender: ApnsSender["Service"]) =>
  AccountWorker.layer.pipe(
    Layer.provideMerge(plannerServices),
    Layer.provide(
      Layer.effect(ApnsSender, networkSender(sender)).pipe(Layer.provide(plannerServices)),
    ),
  );
