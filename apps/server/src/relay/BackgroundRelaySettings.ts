import { normalizeSecureRelayUrl } from "@t3tools/shared/relayUrl";
import * as Crypto from "effect/Crypto";
import * as Hex from "effect/encoding/Hex";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as BackgroundRelayConfig from "./BackgroundRelayConfig.ts";

export const SECRET = "background-relay-config";

const StoredConfig = Schema.Struct({
  version: Schema.Literal(1),
  url: Schema.String,
  issuer: Schema.String,
  environmentCredential: Schema.String,
});
const encodeConfig = Schema.encodeEffect(Schema.fromJsonString(StoredConfig));
const decodeConfig = Schema.decodeUnknownEffect(Schema.fromJsonString(StoredConfig));

interface Snapshot {
  readonly url: string;
  readonly issuer: string;
  readonly environmentCredential: Redacted.Redacted<string>;
  readonly revision: string;
}

export class BackgroundRelaySettingsInvalid extends Schema.TaggedError<BackgroundRelaySettingsInvalid>()(
  "BackgroundRelaySettingsInvalid",
  {
    reason: Schema.Literals(["stdin-required", "stdin-read", "input-too-large", "invalid-config"]),
  },
) {
  override get message(): string {
    switch (this.reason) {
      case "stdin-required":
        return "Supply the credential through piped stdin with --credential-stdin.";
      case "stdin-read":
        return "Could not read the credential from stdin.";
      case "input-too-large":
        return "The credential input exceeds the size limit.";
      case "invalid-config":
        return "Use HTTPS origins and a t3env_ environment credential from stdin.";
    }
  }
}

export class BackgroundRelaySettings extends Context.Service<
  BackgroundRelaySettings,
  {
    // Undefined permits the primary relay; null explicitly disables publishing.
    readonly read: Effect.Effect<Snapshot | null | undefined, ServerSecretStore.SecretStoreError>;
    readonly set: (input: {
      readonly url: string;
      readonly issuer?: string | undefined;
      readonly environmentCredential: Redacted.Redacted<string>;
    }) => Effect.Effect<void, BackgroundRelaySettingsInvalid | ServerSecretStore.SecretStoreError>;
    readonly clear: Effect.Effect<void, ServerSecretStore.SecretStoreError>;
  }
>()("t3/relay/BackgroundRelaySettings") {}

/** @public */
export const make = Effect.gen(function* () {
  const env = yield* BackgroundRelayConfig.BackgroundRelayConfig;
  const secrets = yield* ServerSecretStore.ServerSecretStore;
  const crypto = yield* Crypto.Crypto;
  const revision = (bytes: Uint8Array) =>
    crypto.digest("SHA-256", bytes).pipe(
      Effect.map(Hex.encode),
      Effect.mapError(
        (cause) => new ServerSecretStore.SecretStoreDecodeError({ resource: SECRET, cause }),
      ),
    );
  const environment = env
    ? Object.freeze({
        ...env,
        revision: yield* revision(
          new TextEncoder().encode(
            yield* encodeConfig({
              version: 1,
              ...env,
              environmentCredential: Redacted.value(env.environmentCredential),
            }).pipe(Effect.orDie),
          ),
        ),
      })
    : undefined;

  const read = Effect.gen(function* () {
    if (environment) return environment;
    const bytes = yield* secrets.get(SECRET);
    if (Option.isNone(bytes)) return undefined;
    return yield* decodeConfig(new TextDecoder().decode(bytes.value)).pipe(
      Effect.flatMap(
        Effect.fnUntraced(function* (stored) {
          const url = normalizeSecureRelayUrl(stored.url);
          const issuer = normalizeSecureRelayUrl(stored.issuer);
          if (!url || !issuer || !stored.environmentCredential.trim()) {
            return yield* new BackgroundRelaySettingsInvalid({ reason: "invalid-config" });
          }
          return Object.freeze({
            url,
            issuer,
            environmentCredential: Redacted.make(stored.environmentCredential),
            revision: yield* revision(bytes.value),
          });
        }),
      ),
      Effect.catch(() =>
        Effect.logWarning(
          "Invalid background relay configuration; agent activity publishing disabled.",
        ).pipe(Effect.as(null)),
      ),
    );
  });

  const set: BackgroundRelaySettings["Service"]["set"] = Effect.fn("BackgroundRelaySettings.set")(
    function* (input) {
      const url = normalizeSecureRelayUrl(input.url);
      const issuer = input.issuer === undefined ? url : normalizeSecureRelayUrl(input.issuer);
      const environmentCredential = Redacted.value(input.environmentCredential).trim();
      // EnvironmentCredentials issues t3env_<64 hex>_<96 hex> credentials.
      if (!url || !issuer || !/^t3env_[0-9a-f]{64}_[0-9a-f]{96}$/u.test(environmentCredential)) {
        return yield* new BackgroundRelaySettingsInvalid({ reason: "invalid-config" });
      }
      yield* secrets.set(
        SECRET,
        new TextEncoder().encode(
          yield* encodeConfig({ version: 1, url, issuer, environmentCredential }).pipe(
            Effect.orDie,
          ),
        ),
      );
    },
  );

  return BackgroundRelaySettings.of({ read, set, clear: secrets.remove(SECRET) });
});

export const layerSaved = Layer.effect(BackgroundRelaySettings, make).pipe(
  Layer.provide(Layer.succeed(BackgroundRelayConfig.BackgroundRelayConfig, null)),
);

export const layer = Layer.effect(BackgroundRelaySettings, make).pipe(
  Layer.provide(BackgroundRelayConfig.layer),
);
