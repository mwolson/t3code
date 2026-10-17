import {
  RelayClientAuth,
  RelayClientPrincipal,
  RelayDpopClientAuth,
  RelayEnvironmentAuth,
  RelayEnvironmentPrincipal,
  RelayMobileRegistrationScope,
} from "@t3tools/contracts/relay";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as HttpEffect from "effect/http/HttpEffect";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import { DpopProofReplay } from "t3code-relay/auth/DpopProofs";
import { RelayTokens } from "t3code-relay/auth/RelayTokens";
import { EnvironmentCredentials } from "t3code-relay/environments/EnvironmentCredentials";
import { PersonalRelayConfig } from "../Config.ts";
import { authError, internalError, internalResponse, withHandlerBudget } from "../http/Errors.ts";
import { ClerkJwks } from "./ClerkJwks.ts";

export const layerClientAuth = Layer.effect(
  RelayClientAuth,
  Effect.gen(function* () {
    const clerk = yield* ClerkJwks;
    return {
      clientBearer: Effect.fn("personalRelay.auth.client")(function* (httpEffect, { credential }) {
        const token = Redacted.value(credential).trimStart();
        const principal = yield* clerk.verify(token).pipe(
          Effect.map((verified) => ({ _tag: "Verified" as const, verified })),
          Effect.catchTags({
            ClerkVerificationError: () => Effect.fail(authError("invalid_bearer")),
            ClerkJwksUnavailable: () => Effect.succeed({ _tag: "Unavailable" as const }),
          }),
        );
        if (principal._tag === "Unavailable") return internalResponse("upstream_unavailable");
        return yield* httpEffect.pipe(
          Effect.provideService(RelayClientPrincipal, { userId: principal.verified.sub, token }),
        );
      }),
    };
  }),
);

export const layerEnvironmentAuth = Layer.effect(
  RelayEnvironmentAuth,
  Effect.gen(function* () {
    const credentials = yield* EnvironmentCredentials;
    return {
      environmentBearer: Effect.fn("personalRelay.auth.environment")(function* (
        httpEffect,
        { credential },
      ) {
        const principal = yield* credentials
          .authenticate(Redacted.value(credential).trimStart())
          .pipe(Effect.mapError(() => internalError()));
        if (principal._tag === "None") return yield* authError();
        return yield* httpEffect.pipe(
          Effect.provideService(RelayEnvironmentPrincipal, principal.value),
        );
      }, withHandlerBudget),
    };
  }),
);

export const layerDpopClientAuth = Layer.effect(
  RelayDpopClientAuth,
  Effect.gen(function* () {
    const tokens = yield* RelayTokens;
    const config = yield* PersonalRelayConfig;
    return {
      relayDpop: Effect.fn("personalRelay.auth.dpop")(function* (httpEffect, { credential }) {
        yield* HttpEffect.appendPreResponseHandler((_request, response) =>
          Effect.succeed(
            response.status === 401
              ? HttpServerResponse.setHeader(response, "www-authenticate", "DPoP")
              : response,
          ),
        );
        const request = yield* HttpServerRequest.HttpServerRequest;
        if (!/^DPoP +/iu.test(request.headers.authorization ?? ""))
          return yield* authError("invalid_bearer");
        const token = Redacted.value(credential).trimStart();
        const now = yield* DateTime.now;
        const verified = yield* tokens.verifyDpopAccessToken({
          token,
          nowEpochSeconds: Math.floor(now.epochMilliseconds / 1000),
        });
        if (!verified || verified.sub !== config.ownerSubject || verified.client_id !== "t3-mobile")
          return yield* authError("invalid_bearer");
        return yield* httpEffect.pipe(
          Effect.provideService(RelayClientPrincipal, {
            userId: verified.sub,
            token,
            proofKeyThumbprint: verified.cnf.jkt,
            dpopScopes: verified.scope,
          }),
        );
      }),
    };
  }),
);

export const requireMobileProof = Effect.fn("personalRelay.auth.mobileProof")(function* () {
  const principal = yield* RelayClientPrincipal;
  if (
    !principal.proofKeyThumbprint ||
    !principal.dpopScopes?.includes(RelayMobileRegistrationScope)
  )
    return yield* authError();
  yield* requireProof({
    expectedThumbprint: principal.proofKeyThumbprint,
    expectedAccessToken: principal.token,
  });
  return principal;
});
export const requireProof = Effect.fn("personalRelay.auth.proof")(function* (
  options: {
    readonly expectedThumbprint?: string;
    readonly expectedAccessToken?: string;
  } = {},
) {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const url = HttpServerRequest.toURL(request);
  if (url._tag === "None") return yield* authError("invalid_dpop");
  const proofs = yield* DpopProofReplay;
  return yield* proofs.verifyAndConsume({
    proof: request.headers.dpop,
    method: request.method,
    url: url.value.href,
    now: yield* DateTime.now,
    ...options,
  });
});
export const layer = Layer.mergeAll(layerClientAuth, layerDpopClientAuth, layerEnvironmentAuth);
