import * as NodeServices from "@effect/platform-node/NodeServices";
import * as PersonalConfig from "./Config.ts";
import * as AccountMutations from "./account/AccountMutations.ts";
import * as AccountWorker from "./account/AccountWorker.ts";
import * as AlertLedger from "./account/AlertLedger.ts";
import * as TargetBackoff from "./account/TargetBackoff.ts";
import * as TargetPlanner from "./account/TargetPlanner.ts";
import * as ApnsSender from "./apns/ApnsSender.ts";
import * as Http2HttpClient from "./apns/Http2HttpClient.ts";
import * as ClerkJwks from "./auth/ClerkJwks.ts";
import * as EnvironmentAdministration from "./enroll/EnvironmentAdministration.ts";
import * as EnvironmentEnrollment from "./enroll/EnvironmentEnrollment.ts";
import { WorkerWake } from "./http/WorkerWake.ts";
import * as Pruner from "./maintenance/Pruner.ts";
import * as Crypto from "effect/Crypto";
import * as Hex from "effect/encoding/Hex";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as RelayConfig from "t3code-relay/Config";
import * as AgentActivityRows from "t3code-relay/agentActivity/AgentActivityRows";
import * as ApnsClient from "t3code-relay/agentActivity/ApnsClient";
import * as ApnsDeliveries from "t3code-relay/agentActivity/ApnsDeliveries";
import * as ApnsDeliveryQueue from "t3code-relay/agentActivity/ApnsDeliveryQueue";
import * as ApnsProviderTokens from "t3code-relay/agentActivity/ApnsProviderTokens";
import * as DeliveryAttempts from "t3code-relay/agentActivity/DeliveryAttempts";
import * as Devices from "t3code-relay/agentActivity/Devices";
import * as LiveActivities from "t3code-relay/agentActivity/LiveActivities";
import * as DpopProofs from "t3code-relay/auth/DpopProofs";
import * as RelayTokens from "t3code-relay/auth/RelayTokens";
import * as EnvironmentCredentials from "t3code-relay/environments/EnvironmentCredentials";
import * as EnvironmentPublishSignatures from "t3code-relay/environments/EnvironmentPublishSignatures";
import * as AccountLock from "./account/AccountLock.ts";
import { PassJobBuffer, layer as bufferLayer } from "./account/PassJobBuffer.ts";
import * as Effect from "effect/Effect";
import * as PgLayer from "./db/PgLayer.ts";
import type * as PgClient from "@effect/sql-pg/PgClient";

// Constructs the reused planner without sending or querying.
export const foundationLayer = (database: PgClient.PgPoolConfig) => {
  const config = Layer.effect(
    RelayConfig.RelayConfiguration,
    Effect.gen(function* () {
      const crypto = yield* Crypto.Crypto;
      const secret = Hex.encode(yield* crypto.randomBytes(32));
      return RelayConfig.RelayConfiguration.of({
        relayIssuer: "https://personal-relay.invalid",
        apns: null,
        clerkSecretKey: Redacted.make(""),
        clerkPublishableKey: "",
        clerkJwtAudience: "t3-code-relay",
        apnsDeliveryJobSigningSecret: Redacted.make(secret),
        cloudMintPrivateKey: Redacted.make(""),
        cloudMintPublicKey: "",
        managedEndpointBaseDomain: undefined,
        managedEndpointNamespace: undefined,
      });
    }),
  );
  const db = PgLayer.layer(database);
  const repositories = Layer.mergeAll(
    AgentActivityRows.layer,
    DeliveryAttempts.layer,
    Devices.layer,
    LiveActivities.layer,
    DpopProofs.layer,
    EnvironmentCredentials.layer,
    AccountLock.layer,
  ).pipe(Layer.provideMerge(db));
  const auth = Layer.mergeAll(RelayTokens.layer, EnvironmentPublishSignatures.layer).pipe(
    Layer.provideMerge(repositories),
    Layer.provideMerge(config),
  );
  const queue = ApnsDeliveryQueue.layer.pipe(
    Layer.provide(
      Layer.effect(
        ApnsDeliveryQueue.ApnsDeliveryQueueSender,
        Effect.map(PassJobBuffer, (buffer) => buffer.sender),
      ),
    ),
    Layer.provideMerge(bufferLayer),
    Layer.provide(config),
  );
  const apns = ApnsClient.layer.pipe(
    Layer.provide(ApnsProviderTokens.layer),
    Layer.provide(FetchHttpClient.layer),
  );
  return ApnsDeliveries.layer.pipe(
    Layer.provideMerge(auth),
    Layer.provideMerge(queue),
    Layer.provideMerge(apns),
  );
};

export const servingLayer = Layer.unwrap(
  Effect.gen(function* () {
    const database = yield* PersonalConfig.databaseConfig;
    const config = yield* PersonalConfig.load;
    const settings = Layer.succeed(PersonalConfig.PersonalRelayConfig, config);
    const relay = PersonalConfig.relayConfigurationLayer.pipe(Layer.provide(settings));
    const repositories = repositoriesLayer(database);
    const apns = ApnsClient.layer.pipe(
      Layer.provide(ApnsProviderTokens.layer),
      Layer.provide(
        Http2HttpClient.layer(
          config.apnsOriginOverride === undefined
            ? {}
            : { originOverride: config.apnsOriginOverride },
        ),
      ),
    );
    const core = Layer.mergeAll(repositories, settings, relay, apns);
    const planner = TargetPlanner.layer.pipe(Layer.provideMerge(core));
    const sender = ApnsSender.layer.pipe(Layer.provide(core));
    const worker = AccountWorker.layer.pipe(Layer.provideMerge(planner), Layer.provide(sender));
    return Layer.mergeAll(
      AccountMutations.layer.pipe(Layer.provide(repositories)),
      RelayTokens.layer.pipe(Layer.provide(core)),
      EnvironmentPublishSignatures.layer.pipe(Layer.provide(core)),
      ClerkJwks.layer.pipe(Layer.provide(settings)),
      Pruner.layer.pipe(Layer.provide(repositories)),
      Layer.effect(
        WorkerWake,
        Effect.map(AccountWorker.AccountWorker, (worker) => ({
          wake: Effect.asVoid(worker.wake),
        })),
      ).pipe(Layer.provide(worker)),
      worker,
    ).pipe(Layer.provideMerge(core));
  }),
).pipe(Layer.provideMerge(NodeServices.layer));

export const adminLayer = Layer.unwrap(
  Effect.gen(function* () {
    const database = yield* PersonalConfig.databaseConfig;
    const config = yield* PersonalConfig.load;
    const settings = {
      ownerSubject: config.ownerSubject,
      publicOrigin: config.publicOrigin,
      activeOverrides: PersonalConfig.activeOverrides(config),
    };
    return Layer.mergeAll(
      EnvironmentEnrollment.layer(settings),
      EnvironmentAdministration.layer(settings),
    ).pipe(Layer.provide(repositoriesLayer({ ...database, maxConnections: 2 })));
  }),
).pipe(Layer.provide(NodeServices.layer));

function repositoriesLayer(database: PgClient.PgPoolConfig) {
  return Layer.mergeAll(
    AccountLock.layer,
    AgentActivityRows.layer,
    AlertLedger.layer,
    DeliveryAttempts.layer,
    Devices.layer,
    DpopProofs.layer,
    EnvironmentCredentials.layer,
    LiveActivities.layer,
    TargetBackoff.layer,
  ).pipe(Layer.provideMerge(PgLayer.layer(database)));
}
