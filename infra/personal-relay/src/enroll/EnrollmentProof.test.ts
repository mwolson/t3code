import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import { decodeJwt, decodeProtectedHeader } from "jose";
import { EnrollmentProofError, verifyEnrollmentProof } from "./EnrollmentProof.ts";
import { settings, signer } from "./EnrollmentTestSupport.ts";

const verify = Effect.fnUntraced(function* (proof: string, nowMs?: number) {
  return yield* verifyEnrollmentProof(proof, {
    publicOrigin: settings.publicOrigin,
    nowMs: nowMs ?? (yield* Clock.currentTimeMillis),
  });
});

describe("enrollment proof verification", () => {
  it.effect("accepts a signed Ed25519 proof with the exact audience and identity", () =>
    Effect.gen(function* () {
      const key = yield* signer;
      const claims = yield* verify(yield* key.sign());
      expect(claims.environmentId).toBe("u4-env");
      expect(claims.environmentPublicKey).toBe(key.publicKey);
    }),
  );

  it.effect.each([
    ["audience", { aud: "https://other.example.test" }],
    ["environment", { environmentId: "other" }],
  ] as const)("rejects %s", ([_label, claims]) =>
    Effect.gen(function* () {
      const key = yield* signer;
      const proof = yield* key.sign(claims);
      const error = yield* Effect.flip(verify(proof));
      expect(error).toBeInstanceOf(EnrollmentProofError);
      expect(error.reason).toBe("invalid-proof");
    }),
  );

  it.effect("rejects an algorithm substitution, unsigned tokens, and tampered claims or keys", () =>
    Effect.gen(function* () {
      const key = yield* signer;
      const other = yield* signer;
      const proof = yield* key.sign();
      const [header, payload, signature] = proof.split(".");
      const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString("base64url");
      for (const bad of [
        `${encode({ ...decodeProtectedHeader(proof), alg: "HS256" })}.${payload}.${signature}`,
        `${encode({ typ: "t3-env-background-enroll+jwt", alg: "none" })}.${payload}.`,
        `${header}.${encode({ ...decodeJwt(proof), label: "Tampered" })}.${signature}`,
        `${header}.${encode({ ...decodeJwt(proof), environmentPublicKey: other.publicKey })}.${signature}`,
        "not-a-token",
      ]) {
        expect((yield* Effect.flip(verify(bad))).reason).toBe("invalid-proof");
      }
    }),
  );

  it.effect("enforces expiry, maximum lifetime, future iat, and nbf", () =>
    Effect.gen(function* () {
      const key = yield* signer;
      const now = Math.floor((yield* Clock.currentTimeMillis) / 1000);
      for (const claims of [
        { iat: now - 600, exp: now },
        { iat: now, exp: now + 601 },
        { iat: now + 61, exp: now + 600 },
        { iat: now, exp: now },
        { iat: now + 1, exp: now },
        { nbf: now + 1 },
      ]) {
        expect((yield* Effect.flip(verify(yield* key.sign(claims), now * 1000))).reason).toBe(
          "invalid-proof",
        );
      }
      const boundary = yield* key.sign({ iat: now + 60, exp: now + 600 });
      expect((yield* verify(boundary, now * 1000)).iat).toBe(now + 60);
    }),
  );
});
