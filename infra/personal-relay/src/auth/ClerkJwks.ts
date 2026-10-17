import * as FileSystem from "effect/FileSystem";
import {
  createLocalJWKSet,
  createRemoteJWKSet,
  customFetch,
  errors,
  jwtVerify,
  type JSONWebKeySet,
  type JWTVerifyGetKey,
} from "jose";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { PersonalRelayConfig } from "../Config.ts";

export class ClerkVerificationError extends Schema.TaggedError<ClerkVerificationError>()(
  "ClerkVerificationError",
  { category: Schema.Literal("invalid-token"), cause: Schema.Defect() },
) {
  override get message(): string {
    return "Clerk token verification failed.";
  }
}
export class ClerkJwksUnavailable extends Schema.TaggedError<ClerkJwksUnavailable>()(
  "ClerkJwksUnavailable",
  { category: Schema.Literal("jwks-unavailable"), cause: Schema.Defect() },
) {
  override get message(): string {
    return "Clerk signing keys are unavailable.";
  }
}
export class ClerkJwks extends Context.Service<
  ClerkJwks,
  {
    readonly verify: (
      token: string,
    ) => Effect.Effect<{ readonly sub: string }, ClerkVerificationError | ClerkJwksUnavailable>;
  }
>()("t3code-personal-relay/auth/ClerkJwks") {}

const isUnavailable = Schema.is(ClerkJwksUnavailable);
const decodeJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));

export const make = Effect.fn("ClerkJwks.make")(function* (options?: {
  readonly jwks?: JSONWebKeySet;
  readonly fetch?: typeof fetch;
}) {
  const config = yield* PersonalRelayConfig;
  let keySet = options?.jwks;
  if (keySet === undefined && config.clerkJwksFile !== undefined) {
    const fs = yield* FileSystem.FileSystem;
    keySet = yield* fs.readFileString(config.clerkJwksFile).pipe(
      Effect.flatMap(decodeJson),
      Effect.map((value) => value as JSONWebKeySet),
      Effect.mapError((cause) => new ClerkJwksUnavailable({ category: "jwks-unavailable", cause })),
    );
  }
  const jwks: JWTVerifyGetKey = yield* Effect.try({
    try: () =>
      keySet === undefined
        ? createRemoteJWKSet(new URL(config.clerkJwksUrl), {
            cooldownDuration: 30_000,
            timeoutDuration: 4000,
            [customFetch]: async (url, init) => {
              try {
                const response = await (options?.fetch ?? fetch)(url, init);
                if (!response.ok) throw new Error("JWKS HTTP request failed");
                return response;
              } catch (cause) {
                throw new ClerkJwksUnavailable({ category: "jwks-unavailable", cause });
              }
            },
          })
        : createLocalJWKSet(keySet),
    catch: (cause) => new ClerkJwksUnavailable({ category: "jwks-unavailable", cause }),
  });
  let loggedPresence = false;
  return ClerkJwks.of({
    verify: Effect.fn("personalRelay.clerk.verify")(function* (token) {
      const now = yield* DateTime.now;
      const verified = yield* Effect.tryPromise({
        try: () =>
          jwtVerify(token, jwks, {
            issuer: config.clerkIssuer,
            audience: config.clerkAudience,
            algorithms: ["RS256"],
            requiredClaims: ["exp", "sub"],
            clockTolerance: 5,
            currentDate: DateTime.toDateUtc(now),
          }),
        catch: (cause) => {
          if (isUnavailable(cause)) return cause;
          if (
            cause instanceof errors.JWKSInvalid ||
            cause instanceof errors.JWKSTimeout ||
            !(cause instanceof errors.JOSEError)
          ) {
            return new ClerkJwksUnavailable({ category: "jwks-unavailable", cause });
          }
          return new ClerkVerificationError({ category: "invalid-token", cause });
        },
      });
      if (verified.payload.sub !== config.ownerSubject) {
        return yield* new ClerkVerificationError({
          category: "invalid-token",
          cause: "subject-mismatch",
        });
      }
      if (!loggedPresence) {
        loggedPresence = true;
        yield* Effect.logInfo("Clerk token verified", {
          issuer: config.clerkIssuer,
          audienceMatch: true,
          subjectMatch: true,
          azpPresent: verified.payload.azp !== undefined,
        });
      }
      return { sub: config.ownerSubject };
    }),
  });
});
export const layer = Layer.effect(ClerkJwks, make());
