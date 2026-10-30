import type { RelayAgentActivityAggregateRow } from "@t3tools/contracts/relay";
import { stableStringify } from "@t3tools/shared/relaySigning";
import type { PersonalPublishClaims } from "@t3tools/shared/personalRelay";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Crypto from "effect/Crypto";
import * as Hex from "effect/encoding/Hex";
import { RelayDb } from "t3code-relay/db";
import {
  RelayAgentActivityAggregateRow as RowSchema,
  RelayAgentAwarenessPreferences,
} from "@t3tools/contracts/relay";
import { alertAllowedForPhase } from "t3code-relay/agentActivity/agentActivityAlerts";
import * as Schema from "effect/Schema";

const decodePreferences = Schema.decodeUnknownEffect(RelayAgentAwarenessPreferences);
const decodeRow = Schema.decodeUnknownEffect(RowSchema);

export type DeliveryChannel = "live_activity" | "notification";
export type AlertDeliveryStatus =
  | "pending"
  | "accepted"
  | "suppressed"
  | "superseded"
  | "expired"
  | "rejected"
  | "ambiguous_abandoned";
export interface AlertDeliveryIdentity {
  readonly alertId: string;
  readonly deviceId: string;
}
export interface PendingAlertDelivery extends AlertDeliveryIdentity {
  readonly row: RelayAgentActivityAggregateRow;
  readonly transitionId: string;
  readonly eligibleAt: string;
  readonly expiresAt: string;
  readonly unknownSends: number;
}
export class AlertLedgerError extends Schema.TaggedError<AlertLedgerError>()("AlertLedgerError", {
  operation: Schema.Literals([
    "create",
    "backfill",
    "list",
    "supersede",
    "remove-key",
    "settle",
    "send-start",
    "outcome",
  ]),
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return "Personal relay alert ledger operation failed.";
  }
}

// All methods are database-only and are called inside AccountLock.withAccountLock.
export class AlertLedger extends Context.Service<
  AlertLedger,
  {
    readonly create: (input: {
      readonly userId: string;
      readonly environmentPublicKey: string;
      readonly row: RelayAgentActivityAggregateRow;
      readonly claims: PersonalPublishClaims;
      readonly now: string;
    }) => Effect.Effect<boolean, AlertLedgerError>;
    readonly backfillDevice: (input: {
      readonly userId: string;
      readonly deviceId: string;
      readonly now: string;
    }) => Effect.Effect<void, AlertLedgerError>;
    readonly listPending: (input: {
      readonly userId: string;
      readonly deviceId: string;
      readonly now: string;
    }) => Effect.Effect<ReadonlyArray<PendingAlertDelivery>, AlertLedgerError>;
    readonly supersedeEnvironment: (input: {
      readonly userId: string;
      readonly environmentId: string;
      readonly now: string;
    }) => Effect.Effect<void, AlertLedgerError>;
    readonly removeEnvironmentKey: (input: {
      readonly environmentId: string;
      readonly environmentPublicKey: string;
    }) => Effect.Effect<void, AlertLedgerError>;
    readonly settle: (input: {
      readonly deliveries: ReadonlyArray<AlertDeliveryIdentity>;
      readonly status: Exclude<AlertDeliveryStatus, "pending">;
      readonly now: string;
    }) => Effect.Effect<void, AlertLedgerError>;
    readonly beginSend: (input: {
      readonly deliveries: ReadonlyArray<AlertDeliveryIdentity>;
      readonly channel: DeliveryChannel;
      readonly now: string;
    }) => Effect.Effect<void, AlertLedgerError>;
    readonly finishSend: (input: {
      readonly deliveries: ReadonlyArray<AlertDeliveryIdentity>;
      readonly definite: boolean;
      readonly status: "accepted" | "rejected" | null;
      readonly now: string;
    }) => Effect.Effect<void, AlertLedgerError>;
  }
>()("t3code-personal-relay/account/AlertLedger") {}

const isAlertLedgerError = Schema.is(AlertLedgerError);

export const make = Effect.gen(function* () {
  const sql = (yield* RelayDb).$client;
  const crypto = yield* Crypto.Crypto;
  const guarded = <A, E, R>(
    operation: AlertLedgerError["operation"],
    effect: Effect.Effect<A, E, R>,
  ) =>
    effect.pipe(
      Effect.mapError((cause) =>
        isAlertLedgerError(cause) ? cause : new AlertLedgerError({ operation, cause }),
      ),
    );
  const settle: AlertLedger["Service"]["settle"] = (input) =>
    guarded(
      "settle",
      Effect.gen(function* () {
        for (const delivery of input.deliveries) {
          yield* sql`UPDATE personal_relay_alert_deliveries SET status = ${input.status}, updated_at = ${input.now}::timestamptz
        WHERE alert_id = ${delivery.alertId} AND device_id = ${delivery.deviceId} AND status = 'pending'`;
        }
      }),
    );
  const backfillDevice: AlertLedger["Service"]["backfillDevice"] = (input) =>
    guarded(
      "backfill",
      Effect.gen(function* () {
        const devices = yield* sql<{
          preferences_json: unknown;
        }>`SELECT preferences_json FROM relay_mobile_devices
      WHERE user_id = ${input.userId} AND device_id = ${input.deviceId} AND platform = 'ios'`;
        if (!devices[0]) return;
        const preferences = yield* decodePreferences(devices[0].preferences_json);
        const alerts = yield* sql<{
          alert_id: string;
          phase: string;
        }>`SELECT a.alert_id, a.phase FROM personal_relay_alerts a
      JOIN relay_environment_links e ON e.environment_id = a.environment_id AND e.environment_public_key = a.environment_public_key
      WHERE a.user_id = ${input.userId} AND e.user_id = ${input.userId} AND e.revoked_at IS NULL
        AND a.expires_at > ${input.now}::timestamptz
        AND (a.phase NOT IN ('waiting_for_approval', 'waiting_for_input') OR EXISTS (
          SELECT 1 FROM personal_relay_thread_versions v WHERE v.environment_id = a.environment_id
            AND v.environment_public_key = a.environment_public_key AND v.thread_id = a.thread_id
            AND NOT v.removed AND v.transition_id = a.transition_id))`;
        for (const alert of alerts) {
          const status = alertAllowedForPhase(preferences, alert.phase) ? "pending" : "suppressed";
          yield* sql`INSERT INTO personal_relay_alert_deliveries (alert_id, device_id, status, updated_at)
        VALUES (${alert.alert_id}, ${input.deviceId}, ${status}, ${input.now}::timestamptz)
        ON CONFLICT (alert_id, device_id) DO NOTHING`;
        }
      }),
    );
  return AlertLedger.of({
    create: Effect.fn("AlertLedger.create")(function* (input) {
      return yield* guarded(
        "create",
        Effect.gen(function* () {
          const transition = input.claims.t3_transition;
          const eligibleAt = input.claims.t3_transition_eligible_at;
          if (
            transition === null ||
            eligibleAt === null ||
            !["waiting_for_approval", "waiting_for_input", "completed", "failed"].includes(
              input.row.phase,
            )
          )
            return false;
          const age = Date.parse(input.now) - Date.parse(eligibleAt);
          if (age > 10 * 60_000) {
            yield* Effect.logInfo("alert_skipped_stale_transition");
            return false;
          }
          const alertId = Hex.encode(
            yield* crypto.digest(
              "SHA-256",
              new TextEncoder().encode(
                stableStringify([input.row.environmentId, input.row.threadId, transition.id]),
              ),
            ),
          );
          const inserted = yield* sql`INSERT INTO personal_relay_alerts
        (alert_id, user_id, environment_id, environment_public_key, thread_id, transition_id, phase, transition_at, eligible_at, row_json, created_at, expires_at)
        VALUES (${alertId}, ${input.userId}, ${input.row.environmentId}, ${input.environmentPublicKey}, ${input.row.threadId}, ${transition.id},
          ${input.row.phase}, ${transition.at}::timestamptz, ${eligibleAt}::timestamptz, ${stableStringify(input.row)}::jsonb,
          ${input.now}::timestamptz, ${eligibleAt}::timestamptz + interval '30 minutes') ON CONFLICT DO NOTHING RETURNING alert_id`;
          if (inserted.length === 0) return false;
          const devices = yield* sql<{
            device_id: string;
          }>`SELECT device_id FROM relay_mobile_devices WHERE user_id = ${input.userId} AND platform = 'ios'`;
          for (const device of devices)
            yield* backfillDevice({ ...input, deviceId: device.device_id });
          return true;
        }),
      );
    }),
    backfillDevice,
    listPending: Effect.fn("AlertLedger.listPending")(function* (input) {
      return yield* guarded(
        "list",
        Effect.gen(function* () {
          const finalized = yield* sql<{
            status: string;
            alert_id: string;
          }>`UPDATE personal_relay_alert_deliveries d SET status = CASE
        WHEN a.expires_at <= ${input.now}::timestamptz THEN 'expired'
        WHEN d.unknown_sends >= 2 THEN 'ambiguous_abandoned'
        ELSE 'superseded' END, updated_at = ${input.now}::timestamptz
        FROM personal_relay_alerts a WHERE d.alert_id = a.alert_id AND a.user_id = ${input.userId}
          AND d.device_id = ${input.deviceId} AND d.status = 'pending' AND (
            a.expires_at <= ${input.now}::timestamptz OR d.unknown_sends >= 2 OR NOT EXISTS (
              SELECT 1 FROM relay_environment_links e WHERE e.environment_id = a.environment_id
                AND e.environment_public_key = a.environment_public_key AND e.user_id = a.user_id AND e.revoked_at IS NULL)
            OR (a.phase IN ('waiting_for_approval', 'waiting_for_input') AND NOT EXISTS (
              SELECT 1 FROM personal_relay_thread_versions v WHERE v.environment_id = a.environment_id
                AND v.environment_public_key = a.environment_public_key AND v.thread_id = a.thread_id
                AND NOT v.removed AND v.transition_id = a.transition_id))) RETURNING d.status, d.alert_id`;
          for (const row of finalized)
            yield* Effect.logInfo("alert_delivery_finalized", {
              status: row.status,
              alertId: row.alert_id,
            });
          const rows = yield* sql<{
            alert_id: string;
            row_json: unknown;
            transition_id: string;
            eligible_at: string;
            expires_at: string;
            unknown_sends: number;
          }>`
        SELECT a.alert_id, a.row_json, a.transition_id, a.eligible_at::text, a.expires_at::text, d.unknown_sends
        FROM personal_relay_alert_deliveries d JOIN personal_relay_alerts a USING (alert_id)
        WHERE a.user_id = ${input.userId} AND d.device_id = ${input.deviceId} AND d.status = 'pending'
          ORDER BY a.transition_at, a.eligible_at, a.alert_id`;
          return yield* Effect.forEach(rows, (row) =>
            decodeRow(row.row_json).pipe(
              Effect.map((value) => ({
                alertId: row.alert_id,
                deviceId: input.deviceId,
                row: value,
                transitionId: row.transition_id,
                eligibleAt: row.eligible_at,
                expiresAt: row.expires_at,
                unknownSends: row.unknown_sends,
              })),
            ),
          );
        }),
      );
    }),
    supersedeEnvironment: Effect.fn("AlertLedger.supersedeEnvironment")(function* (input) {
      return yield* guarded(
        "supersede",
        sql`UPDATE personal_relay_alert_deliveries d
      SET status = 'superseded', updated_at = ${input.now}::timestamptz FROM personal_relay_alerts a
      WHERE d.alert_id = a.alert_id AND a.user_id = ${input.userId} AND a.environment_id = ${input.environmentId} AND d.status = 'pending'`.pipe(
          Effect.asVoid,
        ),
      );
    }),
    removeEnvironmentKey: Effect.fn("AlertLedger.removeEnvironmentKey")(function* (input) {
      return yield* guarded(
        "remove-key",
        sql`DELETE FROM personal_relay_alerts
      WHERE environment_id = ${input.environmentId} AND environment_public_key = ${input.environmentPublicKey}`.pipe(
          Effect.asVoid,
        ),
      );
    }),
    settle,
    beginSend: Effect.fn("AlertLedger.beginSend")(function* (input) {
      return yield* guarded(
        "send-start",
        Effect.gen(function* () {
          for (const delivery of input.deliveries) {
            const rows =
              yield* sql`UPDATE personal_relay_alert_deliveries SET unknown_sends = unknown_sends + 1,
          last_channel = ${input.channel}, updated_at = ${input.now}::timestamptz
          WHERE alert_id = ${delivery.alertId} AND device_id = ${delivery.deviceId} AND status = 'pending' AND unknown_sends < 2 RETURNING alert_id`;
            if (rows.length !== 1)
              return yield* new AlertLedgerError({
                operation: "send-start",
                cause: "Delivery is no longer sendable",
              });
          }
        }),
      );
    }),
    finishSend: Effect.fn("AlertLedger.finishSend")(function* (input) {
      return yield* guarded(
        "outcome",
        Effect.gen(function* () {
          for (const delivery of input.deliveries)
            yield* sql`UPDATE personal_relay_alert_deliveries
        SET unknown_sends = greatest(0, unknown_sends - ${input.definite ? 1 : 0}),
          status = coalesce(${input.status}::text, status), updated_at = ${input.now}::timestamptz
        WHERE alert_id = ${delivery.alertId} AND device_id = ${delivery.deviceId} AND status = 'pending'`;
        }),
      );
    }),
  });
});
export const layer = Layer.effect(AlertLedger, make);
