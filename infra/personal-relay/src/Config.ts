// @effect-diagnostics-next-line nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import type * as LogLevel from "effect/LogLevel";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as RelayConfig from "t3code-relay/Config";
import { parsePublicOrigin } from "./http/PublicOrigin.ts";

export class PersonalRelayConfig extends Context.Service<
  PersonalRelayConfig,
  {
    readonly publicOrigin: string;
    readonly port: number;
    readonly ownerSubject: string;
    readonly clerkIssuer: string;
    readonly clerkAudience: string;
    readonly clerkJwksUrl: string;
    readonly clerkJwksFile: string | undefined;
    readonly allowedBundleIds: ReadonlyArray<string>;
    readonly allowedApsEnvironments: ReadonlyArray<RelayConfig.ApnsEnvironment>;
    readonly tokenSigningPrivateKey: Redacted.Redacted<string>;
    readonly apns: RelayConfig.ApnsCredentials | null;
    readonly apnsOriginOverride: string | undefined;
    readonly logLevel: LogLevel.LogLevel;
  }
>()("t3code-personal-relay/Config/PersonalRelayConfig") {}

export class PersonalRelayConfigError extends Schema.TaggedError<PersonalRelayConfigError>()(
  "PersonalRelayConfigError",
  { category: Schema.Literals(["configuration", "signing-key"]) },
) {
  override get message(): string {
    return "Personal relay configuration is invalid.";
  }
}

const csv = (value: string) =>
  value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
const text = (name: string, fallback: string) =>
  Config.String(name).pipe(Config.withDefault(fallback));
const decodeLogLevel = Schema.decodeUnknownEffect(
  Schema.Literals(["All", "Trace", "Debug", "Info", "Warn", "Error", "Fatal", "None"]),
);
const decodeEnvironment = Schema.decodeUnknownEffect(RelayConfig.ApnsEnvironment);
const decodeEnvironments = Schema.decodeUnknownEffect(
  Schema.NonEmptyArray(RelayConfig.ApnsEnvironment),
);

export const databaseConfig = Effect.gen(function* () {
  return {
    host: yield* text("PERSONAL_RELAY_DATABASE_HOST", "127.0.0.1"),
    port: yield* Config.Port("PERSONAL_RELAY_DATABASE_PORT").pipe(Config.withDefault(5432)),
    database: yield* text("PERSONAL_RELAY_DATABASE_NAME", "t3relay"),
    username: yield* text("PERSONAL_RELAY_DATABASE_USER", "t3relay"),
    password: yield* Config.Redacted("PERSONAL_RELAY_DATABASE_PASSWORD"),
    maxConnections: yield* Config.Int("PERSONAL_RELAY_DATABASE_POOL_MAX").pipe(
      Config.withDefault(8),
    ),
  };
}).pipe(
  Effect.filterOrFail(
    (config) => config.maxConnections > 0 && config.maxConnections <= 8,
    () => new PersonalRelayConfigError({ category: "configuration" }),
  ),
  Effect.mapError(() => new PersonalRelayConfigError({ category: "configuration" })),
);

export const load = Effect.gen(function* () {
  const publicOrigin = yield* text(
    "PERSONAL_RELAY_PUBLIC_ORIGIN",
    "https://t3-relay.mikemail.work",
  );
  const clerkIssuer = yield* text("PERSONAL_RELAY_CLERK_ISSUER", "https://clerk.t3.codes");
  const clerkJwksUrl = yield* text(
    "PERSONAL_RELAY_CLERK_JWKS_URL",
    "https://clerk.t3.codes/.well-known/jwks.json",
  );
  const origin = yield* Effect.try(() => parsePublicOrigin(publicOrigin));
  yield* Effect.try(() => {
    if (
      parsePublicOrigin(clerkIssuer) !== clerkIssuer ||
      new URL(clerkJwksUrl).protocol !== "https:"
    ) {
      throw new Error("Invalid Clerk configuration");
    }
  });
  const ownerSubject = (yield* Config.String("PERSONAL_RELAY_OWNER_SUBJECT")).trim();
  const clerkAudience = (yield* text("PERSONAL_RELAY_CLERK_AUDIENCE", "t3-code-relay")).trim();
  const allowedBundleIds = csv(
    yield* text("PERSONAL_RELAY_ALLOWED_BUNDLE_IDS", "com.mwolson.t3code.v2.dev"),
  );
  if (!ownerSubject || !clerkAudience || allowedBundleIds.length === 0) {
    return yield* new PersonalRelayConfigError({ category: "configuration" });
  }
  const allowedApsEnvironments = yield* decodeEnvironments(
    csv(yield* text("PERSONAL_RELAY_ALLOWED_APS_ENVIRONMENTS", "sandbox")),
  );
  const apnsEnabled = yield* Config.Boolean("APNS_ENABLED").pipe(Config.withDefault(false));
  let apns: RelayConfig.ApnsCredentials | null = null;
  if (apnsEnabled) {
    const credentials: RelayConfig.ApnsCredentials = {
      teamId: yield* Config.String("APNS_TEAM_ID"),
      keyId: yield* Config.String("APNS_KEY_ID"),
      privateKey: yield* Config.Redacted("APNS_PRIVATE_KEY"),
      bundleId: yield* Config.String("APNS_BUNDLE_ID"),
      environment: yield* decodeEnvironment(yield* text("APNS_ENVIRONMENT", "sandbox")),
    };
    yield* Effect.try(() => {
      const key = NodeCrypto.createPrivateKey(Redacted.value(credentials.privateKey));
      if (key.asymmetricKeyType !== "ec" || key.asymmetricKeyDetails?.namedCurve !== "prime256v1")
        throw new Error("Invalid APNs key");
    });
    apns = credentials;
  }
  return PersonalRelayConfig.of({
    publicOrigin: origin,
    port: yield* Config.Port("PERSONAL_RELAY_PORT").pipe(Config.withDefault(8080)),
    ownerSubject,
    clerkIssuer,
    clerkAudience,
    clerkJwksUrl,
    clerkJwksFile: (yield* text("PERSONAL_RELAY_CLERK_JWKS_FILE", "")) || undefined,
    allowedBundleIds,
    allowedApsEnvironments,
    tokenSigningPrivateKey: yield* Config.Redacted("PERSONAL_RELAY_TOKEN_SIGNING_PRIVATE_KEY"),
    apns,
    apnsOriginOverride: (yield* text("PERSONAL_RELAY_APNS_ORIGIN_OVERRIDE", "")) || undefined,
    logLevel: yield* decodeLogLevel(
      (yield* text("PERSONAL_RELAY_LOG_LEVEL", "info"))
        .toLowerCase()
        .replace(/^./, (c) => c.toUpperCase()),
    ),
  });
}).pipe(Effect.mapError(() => new PersonalRelayConfigError({ category: "configuration" })));

export const layer = Layer.effect(PersonalRelayConfig, load);

export const makeRelayConfiguration = Effect.gen(function* () {
  const config = yield* PersonalRelayConfig;
  const publicKey = yield* Effect.try({
    try: () => {
      const privateKey = NodeCrypto.createPrivateKey(Redacted.value(config.tokenSigningPrivateKey));
      if (privateKey.asymmetricKeyType !== "ed25519") throw new Error("Invalid relay signing key");
      return NodeCrypto.createPublicKey(privateKey)
        .export({ format: "pem", type: "spki" })
        .toString();
    },
    catch: () => new PersonalRelayConfigError({ category: "signing-key" }),
  });
  const crypto = yield* Crypto.Crypto;
  return RelayConfig.RelayConfiguration.of({
    relayIssuer: config.publicOrigin,
    apns: config.apns,
    clerkSecretKey: Redacted.make(""),
    clerkPublishableKey: "",
    clerkJwtAudience: config.clerkAudience,
    apnsDeliveryJobSigningSecret: Redacted.make(Hex.encode(yield* crypto.randomBytes(32))),
    cloudMintPrivateKey: config.tokenSigningPrivateKey,
    cloudMintPublicKey: publicKey,
    managedEndpointBaseDomain: undefined,
    managedEndpointNamespace: undefined,
  });
});
export const relayConfigurationLayer = Layer.effect(
  RelayConfig.RelayConfiguration,
  makeRelayConfiguration,
);

export function activeOverrides(config: PersonalRelayConfig["Service"]): ReadonlyArray<string> {
  const names: string[] = [];
  if (config.apns === null) names.push("APNS_DISABLED");
  if (config.clerkJwksFile !== undefined) names.push("PERSONAL_RELAY_CLERK_JWKS_FILE");
  if (config.apnsOriginOverride !== undefined) names.push("PERSONAL_RELAY_APNS_ORIGIN_OVERRIDE");
  return names;
}
