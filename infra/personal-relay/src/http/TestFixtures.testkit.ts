import type { RelayDpopAccessTokenScope } from "@t3tools/contracts/relay";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Schema from "effect/Schema";
import * as Redacted from "effect/Redacted";
import * as HttpRouter from "effect/http/HttpRouter";
import { generateKeyPair, exportPKCS8, exportJWK, SignJWT } from "jose";
import { PersonalRelayConfig, makeRelayConfiguration } from "../Config.ts";
import { ClerkJwks, make as makeClerk } from "../auth/ClerkJwks.ts";
import { AccountMutations } from "../account/AccountMutations.ts";
import { accountMutations } from "../testing/Fakes.testkit.ts";
import { WorkerWake } from "./WorkerWake.ts";
import { AgentActivityRows } from "t3code-relay/agentActivity/AgentActivityRows";
import { Devices } from "t3code-relay/agentActivity/Devices";
import { RelayDb } from "t3code-relay/db";
import * as RelayConfig from "t3code-relay/Config";
import * as RelayTokens from "t3code-relay/auth/RelayTokens";
import * as DpopProofs from "t3code-relay/auth/DpopProofs";
import * as PublishSignatures from "t3code-relay/environments/EnvironmentPublishSignatures";
import { EnvironmentCredentials } from "t3code-relay/environments/EnvironmentCredentials";
import {
  DpopPublicJwk,
  computeDpopAccessTokenHash,
  computeDpopJwkThumbprint,
} from "@t3tools/shared/dpop";
import { app, webServices } from "./Server.ts";

const decodeDpopJwk = Schema.decodeUnknownSync(DpopPublicJwk);

export const ORIGIN = "https://t3-relay.test";
export const OWNER = "user_owner";
export const ISSUER = "https://clerk.test";
export const AUDIENCE = "t3-code-relay";

let nonce = 0;
export async function keys() {
  const clerk = await generateKeyPair("RS256", { extractable: true });
  const mint = await generateKeyPair("EdDSA", { extractable: true });
  const dpop = await generateKeyPair("ES256", { extractable: true });
  const clerkJwk = {
    ...(await exportJWK(clerk.publicKey)),
    kid: "first",
    alg: "RS256",
    use: "sig",
  };
  const dpopJwk = decodeDpopJwk(await exportJWK(dpop.publicKey));
  const config = PersonalRelayConfig.of({
    publicOrigin: ORIGIN,
    port: 0,
    ownerSubject: OWNER,
    clerkIssuer: ISSUER,
    clerkAudience: AUDIENCE,
    clerkJwksUrl: `${ISSUER}/jwks`,
    clerkJwksFile: undefined,
    allowedBundleIds: ["com.mwolson.t3code.v2.dev"],
    allowedApsEnvironments: ["sandbox"],
    tokenSigningPrivateKey: Redacted.make(await exportPKCS8(mint.privateKey)),
    apns: null,
    apnsOriginOverride: undefined,
    logLevel: "Info",
  });
  return {
    clerk,
    mint,
    dpop,
    clerkJwk,
    dpopJwk,
    config,
    thumbprint: computeDpopJwkThumbprint(dpopJwk),
    clerkToken: (claims: Record<string, unknown> = {}, header: Record<string, unknown> = {}) =>
      new SignJWT({
        iss: ISSUER,
        aud: AUDIENCE,
        sub: OWNER,
        exp: Math.floor(DateTime.nowUnsafe().epochMilliseconds / 1000) + 300,
        ...claims,
      })
        .setProtectedHeader({ alg: "RS256", kid: "first", ...header })
        .sign(clerk.privateKey),
    proof: (method: string, path: string, token?: string, jti = `proof-${++nonce}`) =>
      new SignJWT({
        htm: method,
        htu: `${ORIGIN}${path}`,
        jti,
        iat: Math.floor(DateTime.nowUnsafe().epochMilliseconds / 1000),
        ...(token === undefined ? {} : { ath: computeDpopAccessTokenHash(token) }),
      })
        .setProtectedHeader({ alg: "ES256", typ: "dpop+jwt", jwk: dpopJwk })
        .sign(dpop.privateKey),
  };
}
export type TestKeys = Awaited<ReturnType<typeof keys>>;

export async function makeHttp(
  input: TestKeys,
  options: {
    readonly logs?: Array<unknown>;
    readonly query?: Effect.Effect<ReadonlyArray<never>>;
    readonly mutations?: Partial<AccountMutations["Service"]>;
    readonly clerk?: ClerkJwks["Service"];
    readonly signatures?: PublishSignatures.EnvironmentPublishSignatures["Service"];
    readonly auth?: EnvironmentCredentials["Service"]["authenticate"];
  } = {},
) {
  const calls = { wakes: 0, proofs: 0, reads: 0, queries: 0 };
  const nonces = new Set<string>();
  const db = {
    $client: () => {
      calls.queries++;
      return options.query ?? Effect.succeed([]);
    },
    insert: () => ({
      values: (row: { thumbprint: string; jti: string }) => ({
        onConflictDoNothing: () => ({
          returning: () =>
            Effect.sync(() => {
              calls.proofs++;
              const key = `${row.thumbprint}:${row.jti}`;
              if (nonces.has(key)) return [];
              nonces.add(key);
              return [{ jti: row.jti }];
            }),
        }),
      }),
    }),
  } as unknown as RelayDb["Service"];
  const configLayer = Layer.succeed(PersonalRelayConfig, input.config);
  const relayConfig = Layer.effect(RelayConfig.RelayConfiguration, makeRelayConfiguration).pipe(
    Layer.provide(configLayer),
    Layer.provide(NodeServices.layer),
  );
  const proofLayer = DpopProofs.layer.pipe(Layer.provide(Layer.succeed(RelayDb, db)));
  const tokens = RelayTokens.layer.pipe(Layer.provide(relayConfig));
  const signatures =
    options.signatures === undefined
      ? PublishSignatures.layer.pipe(
          Layer.provide(proofLayer),
          Layer.provide(relayConfig),
          Layer.provide(NodeServices.layer),
        )
      : Layer.succeed(PublishSignatures.EnvironmentPublishSignatures, options.signatures);
  const clerk =
    options.clerk === undefined
      ? Layer.effect(ClerkJwks, makeClerk({ jwks: { keys: [input.clerkJwk] } })).pipe(
          Layer.provide(configLayer),
        )
      : Layer.succeed(ClerkJwks, options.clerk);
  const services = Layer.mergeAll(
    configLayer,
    relayConfig,
    proofLayer,
    tokens,
    signatures,
    clerk,
    Layer.succeed(RelayDb, db),
    Layer.succeed(WorkerWake, {
      wake: Effect.sync(() => {
        calls.wakes++;
      }),
    }),
    Layer.succeed(AccountMutations, accountMutations(options.mutations)),
    Layer.succeed(Devices, {
      register: () => Effect.void,
      unregister: () => Effect.void,
      listForUser: () =>
        Effect.sync(() => {
          calls.reads++;
          return [];
        }),
    }),
    Layer.succeed(AgentActivityRows, {
      listForUser: () =>
        Effect.sync(() => {
          calls.reads++;
          return [];
        }),
      getForUserThread: () => Effect.succeed(null),
      upsert: () => Effect.void,
      remove: () => Effect.void,
      pruneTerminal: () => Effect.void,
    }),
    Layer.succeed(EnvironmentCredentials, {
      authenticate:
        options.auth ??
        (() =>
          Effect.succeedSome({
            credentialId: "test-credential",
            environmentId: "env",
            environmentPublicKey: "test-public-key",
          })),
      create: () => Effect.die("Unused test method"),
      revokeForEnvironmentPublicKey: () => Effect.succeed(false),
    }),
  );
  const appLayer = app.pipe(
    Layer.provide(services),
    Layer.provide(webServices.pipe(Layer.provide(NodeServices.layer))),
    Layer.provide(NodeServices.layer),
    Layer.provide(
      Logger.layer([
        Logger.make((entry) => {
          options.logs?.push(entry.message, entry.cause);
        }),
      ]),
    ),
  );
  const http = HttpRouter.toWebHandler(appLayer, { disableLogger: true });
  const relayToken = (scopes: ReadonlyArray<RelayDpopAccessTokenScope> = ["mobile:registration"]) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const tokens = yield* RelayTokens.RelayTokens;
        return yield* tokens.issueDpopAccessToken({
          userId: OWNER,
          clientId: "t3-mobile",
          scopes,
          proofKeyThumbprint: input.thumbprint,
          jti: "mint",
          issuedAtEpochSeconds: Math.floor(DateTime.nowUnsafe().epochMilliseconds / 1000),
          expiresAtEpochSeconds: Math.floor(DateTime.nowUnsafe().epochMilliseconds / 1000) + 1800,
        });
      }).pipe(Effect.provide(tokens)),
    );
  return { ...http, appLayer, calls, relayToken };
}
