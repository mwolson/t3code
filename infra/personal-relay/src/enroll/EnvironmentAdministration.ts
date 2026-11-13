import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { RelayDb } from "t3code-relay/db";
import type { EnrollmentSettings } from "./EnvironmentEnrollment.ts";

export class AdministrationError extends Schema.TaggedError<AdministrationError>()(
  "AdministrationError",
  {
    operation: Schema.Literals(["list-environments", "list-devices", "status"]),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return "Personal relay administration query failed.";
  }
}

export class EnvironmentAdministration extends Context.Service<
  EnvironmentAdministration,
  {
    readonly config: Effect.Effect<
      EnrollmentSettings & { readonly activeOverrides?: ReadonlyArray<string> }
    >;
    readonly listEnvironments: Effect.Effect<ReadonlyArray<unknown>, AdministrationError>;
    readonly listDevices: Effect.Effect<ReadonlyArray<unknown>, AdministrationError>;
    readonly status: Effect.Effect<unknown, AdministrationError>;
  }
>()("t3code-personal-relay/enroll/EnvironmentAdministration") {}

const make = Effect.fnUntraced(function* (
  settings: EnrollmentSettings & { readonly activeOverrides?: ReadonlyArray<string> },
) {
  const db = yield* RelayDb;
  const sql = db.$client;
  return EnvironmentAdministration.of({
    config: Effect.succeed({
      ownerSubject: settings.ownerSubject,
      publicOrigin: settings.publicOrigin,
      activeOverrides: settings.activeOverrides ?? [],
    }),
    listEnvironments: sql`
      SELECT environment_id AS "environmentId", environment_label AS label,
        revoked_at AS "revokedAt", created_at AS "createdAt", updated_at AS "updatedAt",
        notifications_enabled AS "notificationsEnabled", live_activities_enabled AS "liveActivitiesEnabled"
      FROM relay_environment_links WHERE user_id = ${settings.ownerSubject} ORDER BY environment_id
    `.pipe(
      Effect.mapError(
        (cause) => new AdministrationError({ operation: "list-environments", cause }),
      ),
    ),
    listDevices: sql`
      SELECT d.device_id AS "deviceId", d.label, d.platform, d.bundle_id AS "bundleId",
        d.aps_environment AS "apsEnvironment", d.updated_at AS "updatedAt",
        CASE WHEN length(d.push_token) > 6 THEN right(d.push_token, 6)
          WHEN d.push_token IS NOT NULL THEN '[redacted]' END AS "pushTokenSuffix",
        CASE WHEN length(d.push_to_start_token) > 6 THEN right(d.push_to_start_token, 6)
          WHEN d.push_to_start_token IS NOT NULL THEN '[redacted]' END AS "pushToStartTokenSuffix",
        CASE WHEN length(a.activity_push_token) > 6 THEN right(a.activity_push_token, 6)
          WHEN a.activity_push_token IS NOT NULL THEN '[redacted]' END AS "activityTokenSuffix"
      FROM relay_mobile_devices d LEFT JOIN relay_live_activities a
        ON a.user_id = d.user_id AND a.device_id = d.device_id
      WHERE d.user_id = ${settings.ownerSubject} ORDER BY d.device_id
    `.pipe(
      Effect.mapError((cause) => new AdministrationError({ operation: "list-devices", cause })),
    ),
    status: sql<{ status: unknown }>`
      SELECT jsonb_build_object(
        'account', (SELECT jsonb_build_object(
          'generation', generation::text, 'nextReconcileAt', next_reconcile_at,
          'lastPassAt', last_pass_at, 'lastPassError', last_pass_error)
          FROM personal_relay_accounts WHERE user_id = ${settings.ownerSubject}),
        'backoff', (SELECT coalesce(jsonb_agg(jsonb_build_object(
          'deviceId', s.device_id, 'channel', s.channel, 'failureCount', s.failure_count,
          'retryAt', s.retry_at, 'lastError', s.last_error) ORDER BY s.device_id, s.channel), '[]'::jsonb)
          FROM personal_relay_target_state s JOIN relay_mobile_devices d ON d.device_id = s.device_id
          WHERE d.user_id = ${settings.ownerSubject}),
        'pendingByPhase', (SELECT coalesce(jsonb_object_agg(phase, count), '{}'::jsonb) FROM (
          SELECT a.phase, count(*)::int AS count FROM personal_relay_alert_deliveries d
          JOIN personal_relay_alerts a USING (alert_id)
          WHERE a.user_id = ${settings.ownerSubject} AND d.status = 'pending' GROUP BY a.phase
        ) pending),
        'recentEventCounts', NULL,
        'recentEventCountsUnavailable', 'Diagnostic log events are not persisted in this schema.'
      ) AS status
    `.pipe(
      Effect.map((rows) => rows[0]?.status ?? null),
      Effect.mapError((cause) => new AdministrationError({ operation: "status", cause })),
    ),
  });
});

export const layer = (
  settings: EnrollmentSettings & { readonly activeOverrides?: ReadonlyArray<string> },
) => Layer.effect(EnvironmentAdministration, make(settings));
