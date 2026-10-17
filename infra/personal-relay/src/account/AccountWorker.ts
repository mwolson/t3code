import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Semaphore from "effect/Semaphore";
import * as Deadlines from "./Deadlines.ts";
import { TargetBackoff } from "./TargetBackoff.ts";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import { RelayConfiguration } from "t3code-relay/Config";
import { RelayDb } from "t3code-relay/db";
import { AgentActivityRows } from "t3code-relay/agentActivity/AgentActivityRows";
import { LiveActivities } from "t3code-relay/agentActivity/LiveActivities";
import { ApnsSender } from "../apns/ApnsSender.ts";
import { AccountLock } from "./AccountLock.ts";
import { AlertLedger } from "./AlertLedger.ts";
import { TargetPlanner, type TargetSnapshot } from "./TargetPlanner.ts";
import { makeAggregateState } from "t3code-relay/agentActivity/agentActivityAggregate";

export class AccountWorkerError extends Schema.TaggedError<AccountWorkerError>()(
  "AccountWorkerError",
  { operation: Schema.Literals(["pass", "snapshot", "outcome"]), cause: Schema.Defect() },
) {
  override get message(): string {
    return "Personal relay account worker failed.";
  }
}
const isAccountWorkerError = Schema.is(AccountWorkerError);
export class AccountWorker extends Context.Service<
  AccountWorker,
  {
    readonly pass: (
      userId: string,
    ) => Effect.Effect<
      { readonly nextDeadline: number | null; readonly generationChanged: boolean },
      AccountWorkerError
    >;
    readonly wake: Effect.Effect<boolean>;
    readonly run: (userId: string) => Effect.Effect<never>;
  }
>()("t3code-personal-relay/account/AccountWorker") {}

export const make = Effect.gen(function* () {
  const sql = (yield* RelayDb).$client;
  const lock = yield* AccountLock;
  const rows = yield* AgentActivityRows;
  const activities = yield* LiveActivities;
  const ledger = yield* AlertLedger;
  const planner = yield* TargetPlanner;
  const sender = yield* ApnsSender;
  const config = yield* RelayConfiguration;
  const cursor = yield* Ref.make(0);
  const wakes = yield* Queue.sliding<void>(1);
  const mutex = yield* Semaphore.make(1);
  const backoff = yield* TargetBackoff;
  const generation = Effect.fn(function* (userId: string) {
    return (
      (yield* sql<{
        generation: string;
      }>`SELECT generation::text FROM personal_relay_accounts WHERE user_id = ${userId}`)[0]
        ?.generation ?? "0"
    );
  });
  const snapshot = Effect.fn(function* (userId: string, deviceId: string) {
    return yield* lock.withAccountLock(
      userId,
      Effect.gen(function* () {
        const now = DateTime.formatIso(yield* DateTime.now);
        const target = (yield* activities.listTargets({ userId })).find(
          (target) => target.device_id === deviceId && target.platform === "ios",
        );
        if (!target) return null;
        return {
          generation: yield* generation(userId),
          target,
          rows: yield* rows.listForUser({ userId }),
          pending: yield* ledger.listPending({ userId, deviceId, now }),
        } satisfies TargetSnapshot;
      }),
    );
  });
  const serveTarget = Effect.fn(function* (userId: string, deviceId: string) {
    for (let round = 0; round < 3; round++) {
      const current = yield* snapshot(userId, deviceId);
      if (current === null) return;
      const now = yield* DateTime.now;
      const plan = yield* planner.plan(current, now.epochMilliseconds);
      yield* lock.withAccountLock(
        userId,
        ledger.settle({
          deliveries: plan.suppressed,
          status: "suppressed",
          now: DateTime.formatIso(now),
        }),
      );
      let replan = false;
      for (const [jobIndex, job] of plan.jobs.entries()) {
        if (
          !(yield* lock.withAccountLock(
            userId,
            backoff.ready(deviceId, job.channel, DateTime.formatIso(yield* DateTime.now)),
          ))
        ) {
          continue;
        }
        const outcome = yield* Effect.gen(function* () {
          const ready = yield* lock.withAccountLock(
            userId,
            Effect.gen(function* () {
              if (
                job.payload.kind === "live_activity_end" &&
                (yield* generation(userId)) !== current.generation
              )
                return false;
              const latest = (yield* activities.listTargets({ userId })).find(
                (target) => target.device_id === deviceId,
              );
              const token =
                job.channel === "notification" ? latest?.push_token : latest?.activity_push_token;
              if (token !== job.payload.target.token) return false;
              const pending = yield* ledger.listPending({
                userId,
                deviceId,
                now: DateTime.formatIso(yield* DateTime.now),
              });
              if (
                job.deliveries.some(
                  (delivery) => !pending.some((row) => row.alertId === delivery.alertId),
                )
              )
                return false;
              yield* ledger.beginSend({
                deliveries: job.deliveries,
                channel: job.channel,
                now: DateTime.formatIso(yield* DateTime.now),
              });
              return true;
            }),
          );
          if (!ready) return null;
          const outcome = yield* sender.send(job);
          yield* lock.withAccountLock(
            userId,
            Effect.gen(function* () {
              const fallback =
                outcome.classification === "rejected" &&
                (outcome.reason === "TopicDisallowed" || outcome.reason === "BadTopic") &&
                plan.preferences.notificationsEnabled &&
                job.channel === "live_activity" &&
                job.deliveries.length > 0 &&
                current.target.push_token !== null;
              const reason =
                outcome.reason === "PayloadTooLarge" ? "payload_budget_violation" : outcome.reason;
              const safeReason =
                reason !== null && /^[A-Za-z][A-Za-z0-9_]{0,63}$/.test(reason) ? reason : null;
              const finishedAt = DateTime.formatIso(yield* DateTime.now);
              let status: "accepted" | "rejected" | null = null;
              if (outcome.classification === "delivered") status = "accepted";
              else if (outcome.classification === "rejected" && !fallback) status = "rejected";
              yield* ledger.finishSend({
                deliveries: job.deliveries,
                definite: outcome.classification !== "retryable_ambiguous",
                status,
                now: finishedAt,
              });
              const latest = (yield* activities.listTargets({ userId })).find(
                (target) => target.device_id === deviceId,
              );
              const token =
                job.channel === "notification" ? latest?.push_token : latest?.activity_push_token;
              if (token === job.payload.target.token) {
                if (outcome.classification === "token_dead" || fallback)
                  yield* activities.invalidateDeliveryToken({
                    userId,
                    deviceId,
                    kind: job.payload.kind,
                    invalidatedAt: finishedAt,
                  });
                else if (outcome.classification === "delivered" && job.channel === "live_activity")
                  yield* activities.markDelivery({
                    userId,
                    deviceId,
                    kind: job.payload.kind,
                    aggregate: plan.aggregate,
                    deliveredAt: finishedAt,
                  });
              } else yield* Effect.logInfo("delivery_token_changed");
              if (
                outcome.classification === "delivered" &&
                job.payload.kind === "live_activity_end" &&
                (yield* generation(userId)) !== current.generation &&
                makeAggregateState({
                  activeStates: yield* rows.listForUser({ userId }),
                  terminalState: null,
                  nowMs: Date.parse(finishedAt),
                }) !== null
              )
                yield* Effect.logWarning("end_raced_new_work");
              if (outcome.classification !== "delivered" && outcome.classification !== "token_dead")
                yield* backoff.fail(deviceId, job.channel, finishedAt, outcome.classification);
              if (
                outcome.classification === "delivered" &&
                !plan.jobs.slice(jobIndex + 1).some((later) => later.channel === job.channel)
              )
                yield* backoff.reset(deviceId, job.channel, finishedAt);
              yield* sql`INSERT INTO relay_delivery_attempts (id, created_at, user_id, environment_id, thread_id, device_id, kind, token_suffix, apns_status, apns_reason, apns_id, transport_error)
                VALUES (gen_random_uuid()::text, ${finishedAt}, ${userId}, ${job.payload.notification?.environmentId ?? null}, ${job.payload.notification?.threadId ?? null}, ${deviceId}, ${job.payload.kind}, ${job.payload.target.token.slice(-8)}, ${outcome.status}, ${safeReason}, ${outcome.apnsId}, ${outcome.classification})`;
              if (fallback) replan = true;
              if (outcome.classification === "rejected" || outcome.status === 403)
                yield* Effect.logError("apns_delivery_rejected", {
                  classification: outcome.classification,
                  status: outcome.status,
                  reason: safeReason,
                  keyId: config.apns?.keyId,
                  teamId: config.apns?.teamId,
                  apnsId: outcome.apnsId,
                  kind: job.payload.kind,
                });
            }),
          );
          return outcome;
        }).pipe(Effect.uninterruptible);
        if (outcome === null) {
          replan = true;
          break;
        }
        if (outcome.classification === "token_dead") {
          replan = true;
          break;
        }
        if (replan) break;
        if (outcome.classification !== "delivered") return;
      }
      if (!replan) return;
    }
  });
  const pass = Effect.fn("AccountWorker.pass")(
    function* (userId: string) {
      const before = yield* lock.withAccountLock(userId, generation(userId));
      const targets = yield* lock.withAccountLock(userId, activities.listTargets({ userId }));
      const ordered = targets
        .filter((target) => target.platform === "ios")
        .toSorted((a, b) => a.device_id.localeCompare(b.device_id));
      const start =
        (yield* Ref.getAndUpdate(cursor, (value) => value + 1)) % Math.max(1, ordered.length);
      let firstError: unknown = undefined;
      for (let index = 0; index < ordered.length; index++) {
        const target = ordered[(start + index) % ordered.length];
        if (target)
          yield* serveTarget(userId, target.device_id).pipe(
            Effect.catch((cause) =>
              Effect.sync(() => {
                firstError ??= cause;
              }),
            ),
          );
      }
      if (firstError !== undefined)
        return yield* new AccountWorkerError({ operation: "pass", cause: firstError });
      return yield* lock.withAccountLock(
        userId,
        Effect.gen(function* () {
          const now = yield* DateTime.now;
          const retry = yield* sql<{
            retry_at: string;
          }>`SELECT s.retry_at::text FROM personal_relay_target_state s JOIN relay_mobile_devices d ON d.device_id = s.device_id WHERE d.user_id = ${userId} AND s.retry_at IS NOT NULL`;
          const pending = yield* sql<{
            expires_at: string;
          }>`SELECT a.expires_at::text FROM personal_relay_alert_deliveries d JOIN personal_relay_alerts a USING (alert_id) WHERE a.user_id = ${userId} AND d.status = 'pending'`;
          const nextDeadline = Deadlines.next({
            nowMs: now.epochMilliseconds,
            rows: yield* rows.listForUser({ userId }),
            targets: yield* activities.listTargets({ userId }),
            retryAts: retry.map((row) => row.retry_at),
            alertExpiries: pending.map((row) => row.expires_at),
          });
          const nextAt =
            nextDeadline === null ? null : DateTime.formatIso(DateTime.makeUnsafe(nextDeadline));
          yield* sql`UPDATE personal_relay_accounts SET next_reconcile_at = ${nextAt}::timestamptz, last_pass_at = ${DateTime.formatIso(now)}::timestamptz, last_pass_error = NULL WHERE user_id = ${userId}`;
          return { generationChanged: (yield* generation(userId)) !== before, nextDeadline };
        }),
      );
    },
    Effect.mapError((cause) =>
      isAccountWorkerError(cause) ? cause : new AccountWorkerError({ operation: "pass", cause }),
    ),
    Semaphore.withPermit(mutex),
  );
  return AccountWorker.of({
    pass,
    wake: Queue.offer(wakes, undefined),
    run: Effect.fn("AccountWorker.run")(function* (userId) {
      while (true) {
        const result = yield* pass(userId).pipe(
          Effect.catch(() =>
            Effect.gen(function* () {
              yield* lock
                .withAccountLock(
                  userId,
                  sql`UPDATE personal_relay_accounts SET last_pass_error = 'pass_failed' WHERE user_id = ${userId}`,
                )
                .pipe(Effect.ignore);
              yield* Effect.logError("relay_account_pass_failed");
              return { generationChanged: false, nextDeadline: null };
            }),
          ),
        );
        if (result.generationChanged) continue;
        const now = (yield* DateTime.now).epochMilliseconds;
        const delay =
          result.nextDeadline === null
            ? 15_000
            : Math.min(15_000, Math.max(1, result.nextDeadline - now + 1000));
        yield* Queue.take(wakes).pipe(
          Effect.timeoutOrElse({ duration: delay, orElse: () => Effect.void }),
        );
      }
    }),
  });
});
export const layer = Layer.effect(AccountWorker, make);
