import {
  RelayAuthInvalidError,
  RelayInternalError,
  type RelayAuthInvalidReason,
  type RelayInternalErrorReason,
} from "@t3tools/contracts/relay";
import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { DpopProofRejected } from "t3code-relay/auth/DpopProofs";
import { AccountMutationError } from "../account/AccountMutations.ts";

const isAccountMutationError = Schema.is(AccountMutationError);
const isRelayAuthInvalidError = Schema.is(RelayAuthInvalidError);
const isRelayInternalError = Schema.is(RelayInternalError);
const isDpopProofRejected = Schema.is(DpopProofRejected);

export const authError = (reason: RelayAuthInvalidReason = "not_authorized") =>
  new RelayAuthInvalidError({
    code: "auth_invalid",
    reason,
    // @effect-diagnostics-next-line cryptoRandomUUID:off
    traceId: globalThis.crypto.randomUUID(),
  });
export const internalError = (reason: RelayInternalErrorReason = "persistence_failed") =>
  new RelayInternalError({
    code: "internal_error",
    reason,
    // @effect-diagnostics-next-line cryptoRandomUUID:off
    traceId: globalThis.crypto.randomUUID(),
  });
export const internalResponse = (reason: RelayInternalErrorReason) =>
  HttpServerResponse.jsonUnsafe(internalError(reason), { status: 500 });

export const mapOperationErrors = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.tapError((error) =>
      Effect.logWarning("Personal relay operation failed", {
        category: isAccountMutationError(error) ? error.reason : "operation-failed",
      }),
    ),
    Effect.mapError((error): RelayAuthInvalidError | RelayInternalError => {
      if (isRelayAuthInvalidError(error) || isRelayInternalError(error)) return error;
      if (isDpopProofRejected(error)) return authError("invalid_dpop");
      if (isAccountMutationError(error) && error.reason === "revoked-environment")
        return authError();
      return internalError();
    }),
  );

export const withHandlerBudget = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.timeout("5 seconds"),
    Effect.catchTags({ TimeoutError: () => Effect.fail(internalError("internal_error")) }),
  );
