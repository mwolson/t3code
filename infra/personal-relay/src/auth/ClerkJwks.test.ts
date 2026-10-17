import { describe, expect, it, vi } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { SignJWT, generateKeyPair, exportJWK } from "jose";
import { PersonalRelayConfig } from "../Config.ts";
import { ClerkJwksUnavailable, ClerkVerificationError, make } from "./ClerkJwks.ts";
import { keys, OWNER, AUDIENCE } from "../http/TestFixtures.testkit.ts";

const testKeys = keys();
const isInvalid = Schema.is(ClerkVerificationError);
const isUnavailable = Schema.is(ClerkJwksUnavailable);
const localVerifier = Effect.gen(function* () {
  const input = yield* Effect.promise(() => testKeys);
  const service = yield* make({ jwks: { keys: [input.clerkJwk] } }).pipe(
    Effect.provideService(PersonalRelayConfig, input.config),
    Effect.provide(NodeServices.layer),
  );
  return { input, service };
});

describe("ClerkJwks", () => {
  it.live("accepts the pinned owner with string or array audience and does not restrict azp", () =>
    Effect.gen(function* () {
      const { input, service } = yield* localVerifier;
      for (const claims of [{}, { aud: ["other", AUDIENCE] }, { azp: "native-app" }]) {
        const token = yield* Effect.promise(() => input.clerkToken(claims));
        expect(yield* service.verify(token)).toEqual({ sub: OWNER });
      }
    }),
  );
  it.live("rejects another user's or an expired token", () =>
    Effect.gen(function* () {
      const { input, service } = yield* localVerifier;
      for (const claims of [{ sub: "another-user" }, { exp: 1 }]) {
        const token = yield* Effect.promise(() => input.clerkToken(claims));
        expect(isInvalid(yield* Effect.flip(service.verify(token)))).toBe(true);
      }
    }),
  );
  it.live("rejects an alternate algorithm and an invalid signature", () =>
    Effect.gen(function* () {
      const { input, service } = yield* localVerifier;
      const alternate = yield* Effect.promise(() =>
        new SignJWT({ sub: OWNER }).setProtectedHeader({ alg: "HS256" }).sign(new Uint8Array(32)),
      );
      const valid = yield* Effect.promise(() => input.clerkToken());
      const [header, payload, signature] = valid.split(".");
      const tampered = `${header}.${payload}.${signature![0] === "A" ? "B" : "A"}${signature!.slice(1)}`;
      for (const token of [alternate, tampered])
        expect(isInvalid(yield* Effect.flip(service.verify(token)))).toBe(true);
    }),
  );
  it.live("caches known keys and refetches once when an unknown kid appears", () =>
    Effect.gen(function* () {
      const input = yield* Effect.promise(() => testKeys);
      const rotated = yield* Effect.promise(() => generateKeyPair("RS256"));
      const rotatedJwk = {
        ...(yield* Effect.promise(() => exportJWK(rotated.publicKey))),
        alg: "RS256",
        kid: "rotated",
      };
      let fetches = 0;
      let wallTime = (yield* DateTime.now).epochMilliseconds;
      const clock = vi.spyOn(Date, "now").mockImplementation(() => wallTime);
      yield* Effect.addFinalizer(() => Effect.sync(() => clock.mockRestore()));
      const service = yield* make({
        fetch: async () => {
          fetches++;
          return Response.json({
            keys: fetches === 1 ? [input.clerkJwk] : [input.clerkJwk, rotatedJwk],
          });
        },
      }).pipe(
        Effect.provideService(PersonalRelayConfig, input.config),
        Effect.provide(NodeServices.layer),
      );
      const first = yield* Effect.promise(() => input.clerkToken());
      yield* service.verify(first);
      yield* service.verify(first);
      expect(fetches).toBe(1);
      const now = yield* DateTime.now;
      const rotatedToken = yield* Effect.promise(() =>
        new SignJWT({
          iss: input.config.clerkIssuer,
          aud: AUDIENCE,
          sub: OWNER,
          exp: Math.floor(now.epochMilliseconds / 1000) + 300,
        })
          .setProtectedHeader({ alg: "RS256", kid: "rotated" })
          .sign(rotated.privateKey),
      );
      expect(isInvalid(yield* Effect.flip(service.verify(rotatedToken)))).toBe(true);
      expect(fetches).toBe(1);
      wallTime += 30_001;
      yield* service.verify(rotatedToken);
      expect(fetches).toBe(2);
      const unknown = yield* Effect.promise(() => input.clerkToken({}, { kid: "unknown" }));
      expect(isInvalid(yield* Effect.flip(service.verify(unknown)))).toBe(true);
      expect(fetches).toBe(2);
    }),
  );
  it.live("classifies a JWKS fetch failure as unavailable", () =>
    Effect.gen(function* () {
      const input = yield* Effect.promise(() => testKeys);
      const service = yield* make({
        fetch: async () => {
          throw new Error("synthetic-secret-in-error");
        },
      }).pipe(
        Effect.provideService(PersonalRelayConfig, input.config),
        Effect.provide(NodeServices.layer),
      );
      const token = yield* Effect.promise(() => input.clerkToken());
      const error = yield* Effect.flip(service.verify(token));
      expect(isUnavailable(error)).toBe(true);
      expect(error.message).not.toContain("synthetic-secret");
    }),
  );
});
