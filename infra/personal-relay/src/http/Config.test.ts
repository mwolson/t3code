import { describe, expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import { exportSPKI } from "jose";
import {
  activeOverrides,
  PersonalRelayConfig,
  PersonalRelayConfigError,
  databaseConfig,
  load,
  makeRelayConfiguration,
} from "../Config.ts";
import { keys } from "./TestFixtures.testkit.ts";

const testKeys = keys();
const isConfigError = Schema.is(PersonalRelayConfigError);
const base = Effect.gen(function* () {
  const input = yield* Effect.promise(() => testKeys);
  return {
    PERSONAL_RELAY_OWNER_SUBJECT: "user_owner",
    PERSONAL_RELAY_TOKEN_SIGNING_PRIVATE_KEY: Redacted.value(input.config.tokenSigningPrivateKey),
    PERSONAL_RELAY_DATABASE_PASSWORD: "test-only-password",
  };
});
const configured = <A, E, R>(effect: Effect.Effect<A, E, R>, env: Record<string, string>) =>
  effect.pipe(
    Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnvRecord(env)),
    Effect.provide(NodeServices.layer),
  );

describe("personal relay config and public origin", () => {
  it.effect("names disabled APNs and test overrides without exposing their values", () =>
    Effect.gen(function* () {
      const config = yield* configured(load, {
        ...(yield* base),
        PERSONAL_RELAY_CLERK_JWKS_FILE: "/private/test-jwks",
        PERSONAL_RELAY_APNS_ORIGIN_OVERRIDE: "https://private.test",
      });
      expect(activeOverrides(config)).toEqual([
        "APNS_DISABLED",
        "PERSONAL_RELAY_CLERK_JWKS_FILE",
        "PERSONAL_RELAY_APNS_ORIGIN_OVERRIDE",
      ]);
    }),
  );
  it.effect("loads safe defaults and produces relay credentials without a Clerk secret", () =>
    Effect.gen(function* () {
      const config = yield* configured(load, yield* base);
      expect(config).toMatchObject({
        publicOrigin: "https://t3-relay.mikemail.work",
        port: 8080,
        clerkIssuer: "https://clerk.t3.codes",
        clerkAudience: "t3-code-relay",
        clerkJwksUrl: "https://clerk.t3.codes/.well-known/jwks.json",
        allowedBundleIds: ["com.mwolson.t3code.v2.dev"],
        allowedApsEnvironments: ["sandbox"],
        apns: null,
      });
      const relay = yield* makeRelayConfiguration.pipe(
        Effect.provideService(PersonalRelayConfig, config),
        Effect.provide(NodeServices.layer),
      );
      const input = yield* Effect.promise(() => testKeys);
      const publicKey = yield* Effect.promise(() => exportSPKI(input.mint.publicKey));
      expect(Redacted.value(relay.clerkSecretKey)).toBe("");
      expect(relay.clerkPublishableKey).toBe("");
      expect(relay.cloudMintPublicKey.trim()).toBe(publicKey.trim());
      expect(Redacted.value(relay.apnsDeliveryJobSigningSecret)).toHaveLength(64);
      expect(relay.relayIssuer).toBe(config.publicOrigin);
    }),
  );
  it.effect("parses explicit lists, test fixtures, port and pool size", () =>
    Effect.gen(function* () {
      const env = {
        ...(yield* base),
        PERSONAL_RELAY_PUBLIC_ORIGIN: "https://relay.test:9443/",
        PERSONAL_RELAY_PORT: "9000",
        PERSONAL_RELAY_ALLOWED_BUNDLE_IDS: " one.bundle, two.bundle ",
        PERSONAL_RELAY_ALLOWED_APS_ENVIRONMENTS: "sandbox,production",
        PERSONAL_RELAY_CLERK_JWKS_FILE: "/fixture/jwks.json",
        PERSONAL_RELAY_DATABASE_POOL_MAX: "2",
        PERSONAL_RELAY_LOG_LEVEL: "Warn",
      };
      expect(yield* configured(load, env)).toMatchObject({
        publicOrigin: "https://relay.test:9443",
        port: 9000,
        allowedBundleIds: ["one.bundle", "two.bundle"],
        allowedApsEnvironments: ["sandbox", "production"],
        clerkJwksFile: "/fixture/jwks.json",
        logLevel: "Warn",
      });
      expect(yield* configured(databaseConfig, env)).toMatchObject({
        host: "127.0.0.1",
        port: 5432,
        database: "t3relay",
        username: "t3relay",
        maxConnections: 2,
      });
    }),
  );
  it.effect("rejects malformed config without exposing values", () =>
    Effect.gen(function* () {
      for (const extra of [
        { PERSONAL_RELAY_PUBLIC_ORIGIN: "https://user:password@relay.test" },
        { PERSONAL_RELAY_OWNER_SUBJECT: "" },
        { APNS_ENABLED: "true" },
      ]) {
        const error = yield* Effect.flip(configured(load, { ...(yield* base), ...extra }));
        expect(isConfigError(error)).toBe(true);
        expect(error.message).not.toContain("password");
      }
    }),
  );
});
