import * as NodeServices from "@effect/platform-node/NodeServices";
import { PERSONAL_RELAY_ENROLLMENT_TYP } from "@t3tools/shared/personalRelay";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import { exportSPKI, generateKeyPair, SignJWT } from "jose";

export const settings = { ownerSubject: "u4-owner", publicOrigin: "https://relay.example.test" };

export const signer = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const keys = yield* Effect.promise(() => generateKeyPair("EdDSA", { extractable: true }));
  const publicKey = yield* Effect.promise(() => exportSPKI(keys.publicKey));
  return {
    publicKey,
    sign: Effect.fnUntraced(function* (overrides: Readonly<Record<string, unknown>> = {}) {
      const now = Math.floor((yield* Clock.currentTimeMillis) / 1000);
      const jti = yield* crypto.randomUUIDv4();
      return yield* Effect.promise(() =>
        new SignJWT({
          iss: "t3-env:u4-env",
          sub: "u4-env",
          aud: settings.publicOrigin,
          environmentId: "u4-env",
          environmentPublicKey: publicKey,
          label: "Test environment",
          iat: now,
          exp: now + 600,
          jti,
          ...overrides,
        })
          .setProtectedHeader({ typ: PERSONAL_RELAY_ENROLLMENT_TYP, alg: "EdDSA" })
          .sign(keys.privateKey),
      );
    }),
  };
}).pipe(Effect.provide(NodeServices.layer));
