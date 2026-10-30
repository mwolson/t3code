import {
  RelayAccessTokenType,
  RelayAgentActivityPublishProofExpiredError,
  RelayAgentActivityPublishProofInvalidError,
  RelayEnvironmentPrincipal,
} from "@t3tools/contracts/relay";
import { PersonalPublishClaims } from "@t3tools/shared/personalRelay";
import { encodeOAuthScope } from "@t3tools/shared/oauthScope";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpEffect from "effect/http/HttpEffect";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { decodeJwt } from "jose";
import { RelayDb } from "t3code-relay/db";
import { AgentActivityRows } from "t3code-relay/agentActivity/AgentActivityRows";
import { makeAggregateState } from "t3code-relay/agentActivity/agentActivityAggregate";
import { Devices } from "t3code-relay/agentActivity/Devices";
import { DpopProofReplay } from "t3code-relay/auth/DpopProofs";
import { RelayTokens } from "t3code-relay/auth/RelayTokens";
import {
  EnvironmentPublishSignatureExpired,
  EnvironmentPublishSignatureInvalid,
  EnvironmentPublishSignatures,
} from "t3code-relay/environments/EnvironmentPublishSignatures";
import { PersonalRelayConfig } from "../Config.ts";
import { AccountMutations } from "../account/AccountMutations.ts";
import { ClerkJwks } from "../auth/ClerkJwks.ts";
import { requireMobileProof, requireProof } from "../auth/Middleware.ts";
import { authError, internalError, mapOperationErrors, withHandlerBudget } from "./Errors.ts";
import { PersonalRelayApi } from "./PersonalRelayApi.ts";
import { WorkerWake } from "./WorkerWake.ts";

const isEnvironmentPublishSignatureExpired = Schema.is(EnvironmentPublishSignatureExpired);
const isEnvironmentPublishSignatureInvalid = Schema.is(EnvironmentPublishSignatureInvalid);

const decodeClaims = Schema.decodeUnknownEffect(PersonalPublishClaims);

export const health = HttpApiBuilder.group(
  PersonalRelayApi,
  "health",
  Effect.fnUntraced(function* (handlers) {
    const db = yield* RelayDb;
    return handlers.handle("health", () =>
      db.$client`SELECT 1`.pipe(
        Effect.as({ ok: true as const, service: "relay" as const }),
        Effect.timeout("2 seconds"),
        Effect.mapError(() => internalError("database_unavailable")),
      ),
    );
  }),
);

export const mobile = HttpApiBuilder.group(
  PersonalRelayApi,
  "mobile",
  Effect.fnUntraced(function* (handlers) {
    const config = yield* PersonalRelayConfig;
    const mutations = yield* AccountMutations;
    const rows = yield* AgentActivityRows;
    const proofs = yield* DpopProofReplay;
    const wake = yield* WorkerWake;
    return handlers
      .handle(
        "registerDevice",
        Effect.fn("personalRelay.http.registerDevice")(
          function* ({ payload }) {
            const { userId } = yield* requireMobileProof().pipe(
              Effect.provideService(DpopProofReplay, proofs),
            );
            if (
              payload.platform !== "ios" ||
              !payload.bundleId ||
              !config.allowedBundleIds.includes(payload.bundleId) ||
              !payload.apsEnvironment ||
              !config.allowedApsEnvironments.includes(payload.apsEnvironment)
            ) {
              yield* Effect.logWarning("Personal relay registration rejected", {
                category: "registration-allowlist",
              });
              return yield* authError();
            }
            yield* mutations.registerDevice({ userId, registration: payload });
            yield* wake.wake;
            return { ok: true as const };
          },
          mapOperationErrors,
          withHandlerBudget,
        ),
      )
      .handle(
        "unregisterDevice",
        Effect.fn("personalRelay.http.unregisterDevice")(
          function* ({ params }) {
            const { userId } = yield* requireMobileProof().pipe(
              Effect.provideService(DpopProofReplay, proofs),
            );
            yield* mutations.unregisterDevice({ userId, deviceId: params.deviceId });
            yield* wake.wake;
            return { ok: true as const };
          },
          mapOperationErrors,
          withHandlerBudget,
        ),
      )
      .handle(
        "registerLiveActivity",
        Effect.fn("personalRelay.http.registerLiveActivity")(
          function* ({ payload }) {
            const { userId } = yield* requireMobileProof().pipe(
              Effect.provideService(DpopProofReplay, proofs),
            );
            yield* mutations.registerLiveActivity({ userId, registration: payload });
            yield* wake.wake;
            return { ok: true as const };
          },
          mapOperationErrors,
          withHandlerBudget,
        ),
      )
      .handle(
        "getAgentActivitySnapshot",
        Effect.fn("personalRelay.http.snapshot")(
          function* () {
            const { userId } = yield* requireMobileProof().pipe(
              Effect.provideService(DpopProofReplay, proofs),
            );
            const activeStates = yield* rows.listForUser({ userId });
            return {
              aggregate: makeAggregateState({
                activeStates,
                terminalState: null,
                nowMs: (yield* DateTime.now).epochMilliseconds,
              }),
            };
          },
          mapOperationErrors,
          withHandlerBudget,
        ),
      );
  }),
);

export const client = HttpApiBuilder.group(
  PersonalRelayApi,
  "client",
  Effect.fnUntraced(function* (handlers) {
    const devices = yield* Devices;
    const config = yield* PersonalRelayConfig;
    return handlers.handle("listDevicesV2", () =>
      devices.listForUser({ userId: config.ownerSubject }).pipe(
        Effect.map((devices) => ({ devices })),
        mapOperationErrors,
        withHandlerBudget,
      ),
    );
  }),
);

export const token = HttpApiBuilder.group(
  PersonalRelayApi,
  "token",
  Effect.fnUntraced(function* (handlers) {
    const config = yield* PersonalRelayConfig;
    const clerk = yield* ClerkJwks;
    const proofs = yield* DpopProofReplay;
    const tokens = yield* RelayTokens;
    const crypto = yield* Crypto.Crypto;
    return handlers.handle(
      "exchangeDpopAccessToken",
      Effect.fn("personalRelay.http.exchange")(
        function* ({ payload }) {
          yield* HttpEffect.appendPreResponseHandler((_request, response) =>
            Effect.succeed(
              HttpServerResponse.setHeaders(response, {
                "cache-control": "no-store",
                pragma: "no-cache",
              }),
            ),
          );
          if (payload.client_id !== "t3-mobile" || payload.resource !== config.publicOrigin)
            return yield* authError();
          const scopes = tokens.resolveDpopAccessTokenScopes({
            clientId: payload.client_id,
            scope: payload.scope,
          });
          if (scopes === null) return yield* authError();
          const verified = yield* clerk.verify(payload.subject_token).pipe(
            Effect.catchTags({
              ClerkVerificationError: () => Effect.fail(authError("invalid_bearer")),
              ClerkJwksUnavailable: () => Effect.fail(internalError("upstream_unavailable")),
            }),
          );
          const proofKeyThumbprint = yield* requireProof().pipe(
            Effect.provideService(DpopProofReplay, proofs),
          );
          const now = yield* DateTime.now;
          const jti = yield* crypto
            .randomUUIDv4()
            .pipe(Effect.mapError(() => internalError("internal_error")));
          const accessToken = yield* tokens
            .issueDpopAccessToken({
              userId: verified.sub,
              proofKeyThumbprint,
              jti,
              issuedAtEpochSeconds: Math.floor(now.epochMilliseconds / 1000),
              expiresAtEpochSeconds: Math.floor(now.epochMilliseconds / 1000) + 1800,
              clientId: payload.client_id,
              scopes,
            })
            .pipe(Effect.mapError(() => internalError("internal_error")));
          return {
            access_token: accessToken,
            issued_token_type: RelayAccessTokenType,
            token_type: "DPoP" as const,
            expires_in: 1800,
            scope: encodeOAuthScope(scopes),
          };
        },
        mapOperationErrors,
        withHandlerBudget,
      ),
    );
  }),
);

export const server = HttpApiBuilder.group(
  PersonalRelayApi,
  "server",
  Effect.fnUntraced(function* (handlers) {
    const config = yield* PersonalRelayConfig;
    const signatures = yield* EnvironmentPublishSignatures;
    const mutations = yield* AccountMutations;
    const wake = yield* WorkerWake;
    return handlers.handle(
      "publishAgentActivity",
      Effect.fn("personalRelay.http.publish")(function* ({ params, payload }) {
        const principal = yield* RelayEnvironmentPrincipal;
        if (principal.environmentId !== params.environmentId) return yield* authError();
        const proof = payload.proof;
        yield* signatures
          .verify({
            ...params,
            environmentPublicKey: principal.environmentPublicKey,
            request: payload,
          })
          .pipe(
            Effect.mapError(
              (
                error,
              ):
                | RelayAgentActivityPublishProofExpiredError
                | RelayAgentActivityPublishProofInvalidError
                | ReturnType<typeof authError>
                | ReturnType<typeof internalError> => {
                const traceId = internalError().traceId;
                if (isEnvironmentPublishSignatureExpired(error))
                  return new RelayAgentActivityPublishProofExpiredError({
                    code: "agent_activity_publish_proof_expired",
                    traceId,
                  });
                if (isEnvironmentPublishSignatureInvalid(error))
                  return new RelayAgentActivityPublishProofInvalidError({
                    code: "agent_activity_publish_proof_invalid",
                    reason: error.reason,
                    traceId,
                  });
                return internalError();
              },
            ),
          );
        const claims = yield* Effect.try(() => decodeJwt(proof)).pipe(
          Effect.flatMap(decodeClaims),
          Effect.mapError(
            () =>
              new RelayAgentActivityPublishProofInvalidError({
                code: "agent_activity_publish_proof_invalid",
                reason: "invalid_signature_or_payload",
                traceId: internalError().traceId,
              }),
          ),
        );
        yield* mutations
          .publish({
            userId: config.ownerSubject,
            ...params,
            environmentPublicKey: principal.environmentPublicKey,
            request: payload,
            claims,
          })
          .pipe(mapOperationErrors);
        yield* wake.wake;
        return { ok: true as const, deliveries: [] };
      }, withHandlerBudget),
    );
  }),
);
export const layer = Layer.mergeAll(health, mobile, client, token, server);
