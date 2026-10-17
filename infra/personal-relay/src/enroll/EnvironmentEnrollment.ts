import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Base64Url from "effect/encoding/Base64Url";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import { RelayDb } from "t3code-relay/db";
import * as DpopProofs from "t3code-relay/auth/DpopProofs";
import * as EnvironmentCredentials from "t3code-relay/environments/EnvironmentCredentials";
import * as AccountLock from "../account/AccountLock.ts";
import * as AlertLedger from "../account/AlertLedger.ts";
import { EnrollmentProofError, verifyEnrollmentProof } from "./EnrollmentProof.ts";

const isEnrollmentProofError = Schema.is(EnrollmentProofError);

export interface EnrollmentSettings {
  readonly ownerSubject: string;
  readonly publicOrigin: string;
}

export class EnrollmentPersistenceError extends Schema.TaggedError<EnrollmentPersistenceError>()(
  "EnrollmentPersistenceError",
  {
    operation: Schema.Literals(["enroll", "revoke"]),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return "Environment enrollment transaction failed.";
  }
}

export class EnvironmentEnrollment extends Context.Service<
  EnvironmentEnrollment,
  {
    readonly enroll: (
      proof: string,
    ) => Effect.Effect<
      Redacted.Redacted<string>,
      EnrollmentProofError | EnrollmentPersistenceError
    >;
    readonly revoke: (environmentId: string) => Effect.Effect<boolean, EnrollmentPersistenceError>;
  }
>()("t3code-personal-relay/enroll/EnvironmentEnrollment") {}

export const make = Effect.fnUntraced(function* (settings: EnrollmentSettings) {
  const db = yield* RelayDb;
  const sql = db.$client;
  const account = yield* AccountLock.AccountLock;
  const ledger = yield* AlertLedger.AlertLedger;
  const credentials = yield* EnvironmentCredentials.EnvironmentCredentials;
  const replay = yield* DpopProofs.DpopProofReplay;
  const crypto = yield* Crypto.Crypto;

  return EnvironmentEnrollment.of({
    enroll: Effect.fn("personalRelay.enroll")(function* (proof) {
      const now = yield* DateTime.now;
      const claims = yield* verifyEnrollmentProof(proof, {
        publicOrigin: settings.publicOrigin,
        nowMs: now.epochMilliseconds,
      });
      const thumbprint = yield* crypto
        .digest("SHA-256", new TextEncoder().encode(claims.environmentPublicKey))
        .pipe(
          Effect.map((digest) => `env-enroll:${Base64Url.encode(digest)}`),
          Effect.mapError(
            (cause) => new EnrollmentPersistenceError({ operation: "enroll", cause }),
          ),
        );
      return yield* account
        .withAccountLock(
          settings.ownerSubject,
          Effect.gen(function* () {
            const consumed = yield* replay.consume({
              thumbprint,
              jti: claims.jti,
              iat: claims.iat,
              expiresAt: DateTime.makeUnsafe(claims.exp * 1000),
            });
            if (!consumed) {
              return yield* new EnrollmentProofError({ reason: "replayed-nonce" });
            }
            const previous = yield* sql<{ environment_public_key: string }>`
              SELECT environment_public_key FROM relay_environment_links
              WHERE user_id = ${settings.ownerSubject} AND environment_id = ${claims.environmentId}
            `;
            const timestamp = DateTime.formatIso(now);
            yield* sql`
              INSERT INTO relay_environment_links (
                user_id, environment_id, environment_public_key, environment_label,
                endpoint_http_base_url, endpoint_ws_base_url, endpoint_provider_kind,
                notifications_enabled, live_activities_enabled, managed_tunnels_enabled,
                hold_webhooks_while_offline, created_by_device_id, revoked_at, created_at, updated_at
              ) VALUES (
                ${settings.ownerSubject}, ${claims.environmentId}, ${claims.environmentPublicKey}, ${claims.label},
                'https://background-only.invalid', 'wss://background-only.invalid', 'manual',
                true, true, false, false, NULL, NULL, ${timestamp}, ${timestamp}
              ) ON CONFLICT (user_id, environment_id) DO UPDATE SET
                environment_public_key = EXCLUDED.environment_public_key,
                environment_label = EXCLUDED.environment_label,
                revoked_at = NULL, updated_at = EXCLUDED.updated_at
            `;
            const oldKey = previous[0]?.environment_public_key;
            if (oldKey !== undefined && oldKey !== claims.environmentPublicKey) {
              // Revocation checks for active links, so it must follow the link replacement.
              yield* credentials.revokeForEnvironmentPublicKey({
                environmentId: claims.environmentId,
                environmentPublicKey: oldKey,
              });
              yield* sql`DELETE FROM relay_agent_activity_rows
                WHERE environment_id = ${claims.environmentId} AND environment_public_key = ${oldKey}`;
              yield* sql`DELETE FROM personal_relay_thread_versions
                WHERE environment_id = ${claims.environmentId} AND environment_public_key = ${oldKey}`;
              yield* ledger.removeEnvironmentKey({
                environmentId: claims.environmentId,
                environmentPublicKey: oldKey,
              });
            }
            const credential = yield* credentials.create({
              environmentId: claims.environmentId,
              environmentPublicKey: claims.environmentPublicKey,
            });
            yield* account.bumpGeneration(settings.ownerSubject);
            return Redacted.make(credential);
          }),
        )
        .pipe(
          Effect.mapError((cause) =>
            isEnrollmentProofError(cause)
              ? cause
              : new EnrollmentPersistenceError({ operation: "enroll", cause }),
          ),
        );
    }),
    revoke: Effect.fn("personalRelay.revokeEnvironment")(function* (environmentId) {
      return yield* account
        .withAccountLock(
          settings.ownerSubject,
          Effect.gen(function* () {
            const now = DateTime.formatIso(yield* DateTime.now);
            const links = yield* sql<{ environment_public_key: string }>`
              UPDATE relay_environment_links SET revoked_at = ${now}, updated_at = ${now}
              WHERE user_id = ${settings.ownerSubject} AND environment_id = ${environmentId}
              RETURNING environment_public_key
            `;
            const link = links[0];
            if (link === undefined) return false;
            yield* credentials.revokeForEnvironmentPublicKey({
              environmentId,
              environmentPublicKey: link.environment_public_key,
            });
            yield* ledger.supersedeEnvironment({
              userId: settings.ownerSubject,
              environmentId,
              now,
            });
            yield* account.bumpGeneration(settings.ownerSubject);
            return true;
          }),
        )
        .pipe(
          Effect.mapError(
            (cause) => new EnrollmentPersistenceError({ operation: "revoke", cause }),
          ),
        );
    }),
  });
});

export const layer = (settings: EnrollmentSettings) =>
  Layer.effect(EnvironmentEnrollment, make(settings));
