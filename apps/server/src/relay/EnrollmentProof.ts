import {
  PERSONAL_RELAY_ENROLLMENT_TYP,
  PersonalRelayEnrollmentClaims,
} from "@t3tools/shared/personalRelay";
import { signRelayJwt } from "@t3tools/shared/relayJwt";
import { normalizeSecureRelayUrl } from "@t3tools/shared/relayUrl";
import { EnvironmentId } from "@t3tools/contracts";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as ServerConfig from "../config.ts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import { getExistingEnvironmentKeyPairFromSecretStore } from "../cloud/environmentKeys.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";

export class EnrollmentProofInvalid extends Schema.TaggedError<EnrollmentProofInvalid>()(
  "EnrollmentProofInvalid",
  {},
) {
  override get message(): string {
    return "Enrollment requires an HTTPS relay origin and a nonempty environment label.";
  }
}

export class EnrollmentIdentityMissing extends Schema.TaggedError<EnrollmentIdentityMissing>()(
  "EnrollmentIdentityMissing",
  {},
) {
  override get message(): string {
    return "Enrollment requires an existing server environment identity and signing key in the selected T3 home.";
  }
}

export const layerIdentity = Layer.effect(
  ServerEnvironment.ServerEnvironmentIdentity,
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const config = yield* ServerConfig.ServerConfig;
    const raw = yield* fs
      .readFileString(config.environmentIdPath)
      .pipe(Effect.mapError(() => new EnrollmentIdentityMissing()));
    if (!raw.trim()) return yield* new EnrollmentIdentityMissing();
    return ServerEnvironment.ServerEnvironmentIdentity.of({
      getEnvironmentId: Effect.succeed(EnvironmentId.make(raw.trim())),
    });
  }),
);

const decodeClaims = Schema.decodeEffect(PersonalRelayEnrollmentClaims);

export const validate = Effect.fn("EnrollmentProof.validate")(function* (input: {
  readonly relayUrl: string;
  readonly label?: string | undefined;
}) {
  const origin = normalizeSecureRelayUrl(input.relayUrl);
  if (origin === null || input.label?.trim() === "" || (input.label?.trim().length ?? 0) > 64)
    return yield* new EnrollmentProofInvalid();
  return origin;
});

export const make = Effect.fn("EnrollmentProof.make")(function* (input: {
  readonly relayUrl: string;
  readonly label?: string | undefined;
}) {
  const origin = yield* validate(input);
  const environment = yield* ServerEnvironment.ServerEnvironmentIdentity;
  const environmentId = yield* environment.getEnvironmentId;
  const secrets = yield* ServerSecretStore.ServerSecretStore;
  const existing = yield* getExistingEnvironmentKeyPairFromSecretStore(secrets);
  if (Option.isNone(existing)) return yield* new EnrollmentIdentityMissing();
  const keys = existing.value;
  const crypto = yield* Crypto.Crypto;
  const now = yield* DateTime.now;
  const iat = Math.floor(now.epochMilliseconds / 1_000);
  const claims = yield* decodeClaims({
    iss: `t3-env:${environmentId}`,
    sub: environmentId,
    aud: origin,
    environmentId,
    environmentPublicKey: keys.publicKey,
    label: input.label?.trim() ?? environmentId,
    jti: yield* crypto.randomUUIDv4(),
    iat,
    exp: iat + 600,
  });
  return {
    proof: yield* signRelayJwt({
      privateKey: keys.privateKey,
      typ: PERSONAL_RELAY_ENROLLMENT_TYP,
      payload: claims,
    }),
  };
});
