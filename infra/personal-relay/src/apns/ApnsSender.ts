import type { ApnsDeliveryJobPayload } from "t3code-relay/agentActivity/apnsDeliveryJobs";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import * as Headers from "effect/http/Headers";
import * as HttpClientError from "effect/http/HttpClientError";
import { RelayConfiguration } from "t3code-relay/Config";
import * as ApnsClient from "t3code-relay/agentActivity/ApnsClient";
import { Http2TransportError } from "./Http2HttpClient.ts";
import {
  ApnsPayloadBudgetError,
  ApnsRequestEncodingError,
  prepareRequest,
} from "./PayloadBudget.ts";
import type { AlertDeliveryIdentity, DeliveryChannel } from "../account/AlertLedger.ts";

export interface PlannedApnsJob {
  readonly payload: ApnsDeliveryJobPayload;
  readonly channel: DeliveryChannel;
  readonly deliveries: ReadonlyArray<AlertDeliveryIdentity>;
  readonly firstAlertTitle: string;
}
export type ApnsSendOutcome = {
  readonly classification:
    | "delivered"
    | "token_dead"
    | "retryable_not_sent"
    | "retryable_answered"
    | "retryable_ambiguous"
    | "rejected";
  readonly status: number | null;
  readonly reason: string | null;
  readonly apnsId: string | null;
};

// Network only: dependencies must never include RelayDb or RelayTransactions.
export class ApnsSender extends Context.Service<
  ApnsSender,
  {
    readonly send: (job: PlannedApnsJob) => Effect.Effect<ApnsSendOutcome>;
  }
>()("t3code-personal-relay/apns/ApnsSender") {}

export const make = Effect.gen(function* () {
  const client = yield* ApnsClient.ApnsClient;
  const configuration = yield* RelayConfiguration;
  return ApnsSender.of({
    send: Effect.fn("personalRelay.apns.send")(function* (job) {
      const configured = configuration.apns;
      if (configured === null) {
        return { classification: "rejected", status: null, reason: "apns_disabled", apnsId: null };
      }
      const credentials = {
        ...configured,
        bundleId: job.payload.target.bundleId ?? configured.bundleId,
        environment: job.payload.target.apsEnvironment ?? configured.environment,
      };
      const nowMs = yield* Clock.currentTimeMillis;
      const outcome = yield* Effect.try({
        try: () => prepareRequest(job, client, nowMs),
        catch: (cause) => (isBudgetError(cause) ? cause : new ApnsRequestEncodingError({ cause })),
      }).pipe(
        Effect.flatMap((prepared) => {
          const base = { credentials, issuedAtUnixSeconds: Math.floor(nowMs / 1000) };
          if (prepared.kind === "notification") {
            return client.sendPushNotificationRequest({ ...base, request: prepared.request });
          }
          return client.sendLiveActivityRequest({ ...base, request: prepared.request });
        }),
        Effect.map((result) => classifyResponse(result.status, result.reason, result.apnsId)),
        Effect.catch((error) => Effect.succeed(classifyError(error))),
      );
      if (outcome.classification === "rejected" || outcome.status === 403) {
        yield* Effect.logError("APNs request rejected").pipe(
          Effect.annotateLogs({
            "apns-id": outcome.apnsId,
            reason: outcome.reason,
            status: outcome.status,
            classification: outcome.classification,
            channel: job.channel,
            kind: job.payload.kind,
            keyId: credentials.keyId,
            teamId: credentials.teamId,
          }),
        );
      }
      return outcome;
    }),
  });
});

export const layer = Layer.effect(ApnsSender, make);

const isHttp2TransportError = Schema.is(Http2TransportError);
const isApnsHttpRequestError = Schema.is(ApnsClient.ApnsHttpRequestError);
const isJwtEncodingError = Schema.is(ApnsClient.ApnsJwtEncodingError);
const isJwtSigningError = Schema.is(ApnsClient.ApnsJwtSigningError);
const isBudgetError = Schema.is(ApnsPayloadBudgetError);

function classifyError(error: unknown): ApnsSendOutcome {
  if (isBudgetError(error)) {
    return {
      classification: "rejected",
      status: null,
      reason: "payload_budget_violation",
      apnsId: null,
    };
  }
  if (isJwtEncodingError(error) || isJwtSigningError(error)) {
    return {
      classification: "retryable_not_sent",
      status: null,
      reason: "provider_token_failure",
      apnsId: null,
    };
  }
  if (isApnsHttpRequestError(error)) {
    if (error.status !== null) {
      let apnsId: string | null = null;
      if (HttpClientError.isHttpClientError(error.cause) && error.cause.response !== undefined) {
        apnsId = Option.getOrNull(Headers.get(error.cause.response.headers, "apns-id"));
      }
      if (error.status === 400 || error.status === 403) {
        // These statuses need the reason to distinguish dead tokens, provider retries and rejection.
        return {
          classification: "retryable_answered",
          status: error.status,
          reason: "response_read_failed",
          apnsId: safeApnsId(apnsId),
        };
      }
      return classifyResponse(error.status, undefined, apnsId);
    }
    const transport = transportFailure(error.cause);
    return {
      classification: transport?.sent === false ? "retryable_not_sent" : "retryable_ambiguous",
      status: null,
      reason: transport?.sent === false ? "transport_not_sent" : "transport_ambiguous",
      apnsId: null,
    };
  }
  // Request construction and encoding have not performed network I/O.
  return {
    classification: "retryable_not_sent",
    status: null,
    reason: "request_encoding_failure",
    apnsId: null,
  };
}

function transportFailure(cause: unknown): Http2TransportError | undefined {
  if (isHttp2TransportError(cause)) return cause;
  if (
    HttpClientError.isHttpClientError(cause) &&
    "cause" in cause.reason &&
    isHttp2TransportError(cause.reason.cause)
  )
    return cause.reason.cause;
  return undefined;
}

function classifyResponse(
  status: number,
  rawReason: string | undefined,
  rawId: string | null,
): ApnsSendOutcome {
  const reason = safeReason(rawReason);
  const apnsId = safeApnsId(rawId);
  let classification: ApnsSendOutcome["classification"];
  if (status === 200) classification = "delivered";
  else if (
    status === 410 ||
    (status === 400 &&
      ["BadDeviceToken", "DeviceTokenNotForTopic", "Unregistered"].includes(reason ?? ""))
  )
    classification = "token_dead";
  else if (
    status === 429 ||
    status >= 500 ||
    (status === 403 && ["ExpiredProviderToken", "InvalidProviderToken"].includes(reason ?? ""))
  )
    classification = "retryable_answered";
  else classification = "rejected";
  return {
    classification,
    status,
    reason: reason === "PayloadTooLarge" ? "payload_budget_violation" : reason,
    apnsId,
  };
}

const APNS_REASONS = new Set([
  "BadCollapseId",
  "BadDeviceToken",
  "BadExpirationDate",
  "BadMessageId",
  "BadPriority",
  "BadTopic",
  "DeviceTokenNotForTopic",
  "DuplicateHeaders",
  "ExpiredProviderToken",
  "Forbidden",
  "IdleTimeout",
  "InternalServerError",
  "InvalidProviderToken",
  "MissingDeviceToken",
  "MissingTopic",
  "PayloadEmpty",
  "PayloadTooLarge",
  "ServiceUnavailable",
  "Shutdown",
  "TooManyProviderTokenUpdates",
  "TooManyRequests",
  "TopicDisallowed",
  "Unregistered",
]);
function safeReason(reason: string | undefined): string | null {
  if (reason === undefined) return null;
  return APNS_REASONS.has(reason) ? reason : "unknown_apns_reason";
}

function safeApnsId(value: string | null): string | null {
  return value !== null && /^[a-f0-9-]{36}$/i.test(value) ? value : null;
}
