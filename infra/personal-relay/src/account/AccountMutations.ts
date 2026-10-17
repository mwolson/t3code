import type {
  RelayAgentActivityPublishRequest,
  RelayDeviceRegistrationRequest,
  RelayLiveActivityRegistrationRequest,
} from "@t3tools/contracts/relay";
import type { PersonalPublishClaims } from "@t3tools/shared/personalRelay";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import { stableStringify } from "@t3tools/shared/relaySigning";
import { RelayDb } from "t3code-relay/db";
import { AgentActivityRows } from "t3code-relay/agentActivity/AgentActivityRows";
import { Devices } from "t3code-relay/agentActivity/Devices";
import { LiveActivities } from "t3code-relay/agentActivity/LiveActivities";
import { statusForPhase } from "t3code-relay/agentActivity/agentActivityAggregate";
import { sanitizeAgentActivityAggregateRow } from "t3code-relay/agentActivity/agentActivityPayloads";
import { AccountLock } from "./AccountLock.ts";
import { AlertLedger } from "./AlertLedger.ts";
import * as Schema from "effect/Schema";

export class AccountMutationError extends Schema.TaggedError<AccountMutationError>()(
  "AccountMutationError",
  {
    operation: Schema.Literals([
      "publish",
      "register-device",
      "unregister-device",
      "register-live-activity",
    ]),
    reason: Schema.Literals(["persistence", "stale-sequence", "revoked-environment"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return "Personal relay account mutation failed.";
  }
}
const isAccountMutationError = Schema.is(AccountMutationError);

export class AccountMutations extends Context.Service<
  AccountMutations,
  {
    readonly publish: (input: {
      readonly userId: string;
      readonly environmentId: string;
      readonly environmentPublicKey: string;
      readonly threadId: string;
      readonly request: RelayAgentActivityPublishRequest;
      readonly claims: PersonalPublishClaims;
    }) => Effect.Effect<void, AccountMutationError>;
    readonly registerDevice: (input: {
      readonly userId: string;
      readonly registration: RelayDeviceRegistrationRequest;
    }) => Effect.Effect<void, AccountMutationError>;
    readonly unregisterDevice: (input: {
      readonly userId: string;
      readonly deviceId: string;
    }) => Effect.Effect<void, AccountMutationError>;
    readonly registerLiveActivity: (input: {
      readonly userId: string;
      readonly registration: RelayLiveActivityRegistrationRequest;
    }) => Effect.Effect<void, AccountMutationError>;
  }
>()("t3code-personal-relay/account/AccountMutations") {}

export const make = Effect.gen(function* () {
  const sql = (yield* RelayDb).$client;
  const lock = yield* AccountLock;
  const rows = yield* AgentActivityRows;
  const devices = yield* Devices;
  const activities = yield* LiveActivities;
  const ledger = yield* AlertLedger;
  const guarded = <A, E, R>(
    operation: AccountMutationError["operation"],
    effect: Effect.Effect<A, E, R>,
  ) =>
    effect.pipe(
      Effect.mapError((cause) =>
        isAccountMutationError(cause)
          ? cause
          : new AccountMutationError({ operation, reason: "persistence", cause }),
      ),
    );
  return AccountMutations.of({
    publish: Effect.fn("AccountMutations.publish")(function* (input) {
      return yield* guarded(
        "publish",
        lock.withAccountLock(
          input.userId,
          Effect.gen(function* () {
            const now = DateTime.formatIso(yield* DateTime.now);
            const links =
              yield* sql`SELECT 1 FROM relay_environment_links WHERE user_id = ${input.userId}
        AND environment_id = ${input.environmentId} AND environment_public_key = ${input.environmentPublicKey} AND revoked_at IS NULL`;
            if (links.length === 0)
              return yield* new AccountMutationError({
                operation: "publish",
                reason: "revoked-environment",
              });
            const versions = yield* sql<{
              last_seq: string;
            }>`SELECT last_seq::text FROM personal_relay_thread_versions
        WHERE environment_id = ${input.environmentId} AND environment_public_key = ${input.environmentPublicKey} AND thread_id = ${input.threadId}`;
            if (
              versions[0] &&
              BigInt(versions[0].last_seq) >= BigInt(input.claims.t3_publish_seq)
            ) {
              yield* Effect.logWarning("stale_publish_sequence");
              return yield* new AccountMutationError({
                operation: "publish",
                reason: "stale-sequence",
              });
            }
            const state = input.request.state;
            yield* sql`INSERT INTO personal_relay_thread_versions (environment_id, environment_public_key, thread_id, last_seq, removed, transition_id, accepted_at)
        VALUES (${input.environmentId}, ${input.environmentPublicKey}, ${input.threadId}, ${input.claims.t3_publish_seq}, ${state === null}, ${input.claims.t3_transition?.id ?? null}, ${now}::timestamptz)
        ON CONFLICT (environment_id, environment_public_key, thread_id) DO UPDATE SET last_seq = excluded.last_seq,
          removed = excluded.removed, transition_id = excluded.transition_id, accepted_at = excluded.accepted_at`;
            const previous = yield* rows.getForUserThread(input);
            const changed = stableStringify(previous) !== stableStringify(state);
            if (changed) {
              if (state === null) yield* rows.remove(input);
              else yield* rows.upsert({ environmentPublicKey: input.environmentPublicKey, state });
            }
            let created = false;
            if (state !== null)
              created = yield* ledger.create({
                ...input,
                now,
                row: sanitizeAgentActivityAggregateRow({
                  ...state,
                  status: statusForPhase(state.phase),
                }),
              });
            if (changed || created) yield* lock.bumpGeneration(input.userId);
          }),
        ),
      );
    }),
    registerDevice: Effect.fn("AccountMutations.registerDevice")(function* (input) {
      return yield* guarded(
        "register-device",
        lock.withAccountLock(
          input.userId,
          Effect.gen(function* () {
            yield* devices.register(input);
            yield* ledger.backfillDevice({
              userId: input.userId,
              deviceId: input.registration.deviceId,
              now: DateTime.formatIso(yield* DateTime.now),
            });
            yield* lock.bumpGeneration(input.userId);
          }),
        ),
      );
    }),
    unregisterDevice: Effect.fn("AccountMutations.unregisterDevice")(function* (input) {
      return yield* guarded(
        "unregister-device",
        lock.withAccountLock(
          input.userId,
          Effect.gen(function* () {
            yield* devices.unregister(input);
            yield* sql`DELETE FROM personal_relay_alert_deliveries d USING personal_relay_alerts a
        WHERE d.alert_id = a.alert_id AND a.user_id = ${input.userId} AND d.device_id = ${input.deviceId} AND d.status = 'pending'`;
            yield* sql`DELETE FROM personal_relay_target_state WHERE device_id = ${input.deviceId}`;
            yield* lock.bumpGeneration(input.userId);
          }),
        ),
      );
    }),
    registerLiveActivity: Effect.fn("AccountMutations.registerLiveActivity")(function* (input) {
      return yield* guarded(
        "register-live-activity",
        lock.withAccountLock(
          input.userId,
          Effect.gen(function* () {
            yield* activities.register(input);
            yield* lock.bumpGeneration(input.userId);
          }),
        ),
      );
    }),
  });
});
export const layer = Layer.effect(AccountMutations, make);
