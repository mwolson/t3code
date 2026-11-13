import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { RelayDb } from "t3code-relay/db";
import type { DeliveryChannel } from "./AlertLedger.ts";

export class TargetBackoffError extends Schema.TaggedError<TargetBackoffError>()(
  "TargetBackoffError",
  { operation: Schema.Literals(["read", "fail", "reset"]), cause: Schema.Defect() },
) {
  override get message(): string {
    return "Personal relay channel backoff failed.";
  }
}
export class TargetBackoff extends Context.Service<
  TargetBackoff,
  {
    readonly ready: (
      deviceId: string,
      channel: DeliveryChannel,
      now: string,
    ) => Effect.Effect<boolean, TargetBackoffError>;
    readonly fail: (
      deviceId: string,
      channel: DeliveryChannel,
      now: string,
      classification:
        | "retryable_not_sent"
        | "retryable_answered"
        | "retryable_ambiguous"
        | "rejected",
    ) => Effect.Effect<void, TargetBackoffError>;
    readonly reset: (
      deviceId: string,
      channel: DeliveryChannel,
      now: string,
    ) => Effect.Effect<void, TargetBackoffError>;
  }
>()("t3code-personal-relay/account/TargetBackoff") {}

// Called only inside the account transaction. A success resets a whole channel attempt.
export const make = Effect.gen(function* () {
  const sql = (yield* RelayDb).$client;
  return TargetBackoff.of({
    ready: Effect.fn("TargetBackoff.ready")(function* (deviceId, channel, now) {
      const rows = yield* sql<{
        ready: boolean;
      }>`SELECT retry_at IS NULL OR retry_at <= ${now}::timestamptz AS ready FROM personal_relay_target_state WHERE device_id = ${deviceId} AND channel = ${channel}`.pipe(
        Effect.mapError((cause) => new TargetBackoffError({ operation: "read", cause })),
      );
      return rows[0]?.ready ?? true;
    }),
    fail: Effect.fn("TargetBackoff.fail")(function* (deviceId, channel, now, classification) {
      yield* sql`INSERT INTO personal_relay_target_state (device_id, channel, failure_count, retry_at, last_error, updated_at)
      VALUES (${deviceId}, ${channel}, 1, ${now}::timestamptz + interval '5 seconds', ${classification}, ${now}::timestamptz)
      ON CONFLICT (device_id, channel) DO UPDATE SET failure_count = personal_relay_target_state.failure_count + 1,
        retry_at = ${now}::timestamptz + make_interval(secs => CASE personal_relay_target_state.failure_count WHEN 0 THEN 5 WHEN 1 THEN 15 WHEN 2 THEN 30 WHEN 3 THEN 60 WHEN 4 THEN 120 ELSE 300 END),
        last_error = ${classification}, updated_at = ${now}::timestamptz`.pipe(
        Effect.mapError((cause) => new TargetBackoffError({ operation: "fail", cause })),
      );
    }),
    reset: Effect.fn("TargetBackoff.reset")(function* (deviceId, channel, now) {
      yield* sql`UPDATE personal_relay_target_state SET failure_count = 0, retry_at = NULL, last_error = NULL, updated_at = ${now}::timestamptz WHERE device_id = ${deviceId} AND channel = ${channel}`.pipe(
        Effect.mapError((cause) => new TargetBackoffError({ operation: "reset", cause })),
      );
    }),
  });
});
export const layer = Layer.effect(TargetBackoff, make);
