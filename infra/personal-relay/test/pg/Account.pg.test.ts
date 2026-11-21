import { provideSender } from "./NetworkSender.ts";
import * as Pruner from "../../src/maintenance/Pruner.ts";
import * as Duration from "effect/Duration";
import * as Queue from "effect/Queue";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Ref from "effect/Ref";
import { stableStringify } from "@t3tools/shared/relaySigning";
import * as Worker from "../../src/account/AccountWorker.ts";
import * as Planner from "../../src/account/TargetPlanner.ts";
import { ApnsSender } from "../../src/apns/ApnsSender.ts";
import { prepareRequest } from "../../src/apns/PayloadBudget.ts";
import * as ApnsClient from "t3code-relay/agentActivity/ApnsClient";
import { makeApnsSender } from "../../src/testing/Fakes.testkit.ts";
import { workerServices, plannerServices } from "./Harness.ts";
import { EnvironmentId } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { RelayDb } from "t3code-relay/db";
import * as Mutations from "../../src/account/AccountMutations.ts";
import * as Ledger from "../../src/account/AlertLedger.ts";
import * as Lock from "../../src/account/AccountLock.ts";
import {
  environmentId,
  initialize,
  key,
  owner,
  publish,
  registration,
  reset,
  services,
  state,
} from "./Harness.ts";

const success = { classification: "delivered", status: 200, reason: null, apnsId: "test" } as const;

describe.skipIf(!process.env.T3CODE_PERSONAL_RELAY_TEST_DATABASE_URL)(
  "owned Postgres fixture",
  () => {
    it.live(
      "slice a: atomic mutations, sequences, identities, held eligibility and registration backfill",
      () =>
        Effect.gen(function* () {
          yield* initialize;
          yield* reset;
          const sql = (yield* RelayDb).$client;
          const mutations = yield* Mutations.AccountMutations;
          const lock = yield* Lock.AccountLock;
          const ledger = yield* Ledger.AlertLedger;
          const now = DateTime.formatIso(yield* DateTime.now);
          yield* publish(1, state(now));
          yield* publish(2, state(now, "request", "waiting_for_input"), "request:1");
          expect((yield* sql`SELECT 1 FROM personal_relay_alert_deliveries`).length).toBe(0);
          yield* mutations.registerDevice({ userId: owner, registration });
          yield* mutations.registerDevice({ userId: owner, registration });
          expect((yield* sql`SELECT 1 FROM personal_relay_alert_deliveries`).length).toBe(2);
          const pending = yield* lock.withAccountLock(
            owner,
            ledger.listPending({ userId: owner, deviceId: registration.deviceId, now }),
          );
          yield* lock.withAccountLock(
            owner,
            ledger.settle({ deliveries: pending, status: "accepted", now }),
          );
          yield* mutations.registerDevice({ userId: owner, registration });
          expect(
            (yield* sql`SELECT 1 FROM personal_relay_alert_deliveries WHERE status = 'accepted'`)
              .length,
          ).toBe(2);
          const generation = (yield* sql`SELECT generation FROM personal_relay_accounts`)[0];
          yield* publish(3, state(now));
          expect((yield* sql`SELECT generation FROM personal_relay_accounts`)[0]).toEqual(
            generation,
          );
          const stale = yield* Effect.flip(publish(1, state(now)));
          expect(stale.reason).toBe("stale-sequence");
          const later = DateTime.formatIso(DateTime.makeUnsafe(Date.parse(now) + 1));
          yield* publish(4, { ...state(later), threadTitle: "renamed" });
          expect((yield* sql`SELECT 1 FROM personal_relay_alerts`).length).toBe(2);
          yield* publish(5, state(now), "run:2:completed");
          expect((yield* sql`SELECT 1 FROM personal_relay_alerts`).length).toBe(3);
          yield* publish(6, null);
          expect((yield* Effect.flip(publish(5, state(now)))).reason).toBe("stale-sequence");
          const old = DateTime.formatIso(DateTime.makeUnsafe(Date.parse(now) - 16 * 60_000));
          yield* publish(7, state(now, "held"), "run:held:completed", now, old);
          yield* publish(8, state(now, "old"), "run:old:completed", old, old);
          expect(
            (yield* sql`SELECT 1 FROM personal_relay_alerts WHERE thread_id = 'held'`).length,
          ).toBe(1);
          expect(
            (yield* sql`SELECT 1 FROM personal_relay_alerts WHERE thread_id = 'old'`).length,
          ).toBe(0);
          yield* publish(9, state(now, "request", "running"), "none", null, null);
          const remaining = yield* lock.withAccountLock(
            owner,
            ledger.listPending({ userId: owner, deviceId: registration.deviceId, now }),
          );
          expect(remaining.some((delivery) => delivery.row.threadId === "held")).toBe(true);

          const ahead = DateTime.formatIso(DateTime.makeUnsafe(Date.parse(now) + 15_000));
          yield* publish(10, state(ahead, "clock-ahead"), "run:ahead:completed", ahead, ahead);
          expect(
            (yield* lock.withAccountLock(
              owner,
              ledger.listPending({ userId: owner, deviceId: registration.deviceId, now }),
            )).some((delivery) => delivery.row.threadId === "clock-ahead"),
          ).toBe(true);

          // A failing generation update must roll back the real repository and ledger writes.
          yield* reset;
          yield* sql.unsafe(
            "CREATE OR REPLACE FUNCTION u2_fail_bump() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected'; END $$",
          );
          yield* sql.unsafe(
            "CREATE TRIGGER u2_fail_bump BEFORE UPDATE ON personal_relay_accounts FOR EACH ROW EXECUTE FUNCTION u2_fail_bump()",
          );
          yield* Effect.flip(mutations.registerDevice({ userId: owner, registration }));
          yield* Effect.flip(
            mutations.registerLiveActivity({
              userId: owner,
              registration: { deviceId: registration.deviceId, activityPushToken: "activity" },
            }),
          );
          yield* Effect.flip(publish(1, state(now)));
          for (const table of [
            "relay_mobile_devices",
            "relay_live_activities",
            "relay_agent_activity_rows",
            "personal_relay_alerts",
            "personal_relay_thread_versions",
            "personal_relay_accounts",
          ]) {
            expect((yield* sql.unsafe(`SELECT 1 FROM ${table}`)).length).toBe(0);
          }
          yield* sql.unsafe("DROP TRIGGER u2_fail_bump ON personal_relay_accounts");
          yield* sql.unsafe("DROP FUNCTION u2_fail_bump()");
          yield* mutations.registerLiveActivity({
            userId: owner,
            registration: { deviceId: registration.deviceId, activityPushToken: "activity" },
          });
          yield* mutations.registerDevice({ userId: owner, registration });
          yield* Effect.all([publish(1, state(now)), publish(2, state(now))], {
            concurrency: 2,
            mode: "result",
          });
          expect((yield* sql`SELECT 1 FROM personal_relay_alerts`).length).toBe(1);
          yield* sql`UPDATE relay_environment_links SET revoked_at = now() WHERE environment_id = ${environmentId} AND environment_public_key = ${key}`;
          expect((yield* Effect.flip(publish(3, state(now)))).reason).toBe("revoked-environment");
        }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(services))),
    );

    it.live(
      "slice b: notifications, independent transitions, token fallback, fairness, races and bounded jobs",
      () =>
        Effect.gen(function* () {
          yield* initialize;
          yield* reset;
          const sql = (yield* RelayDb).$client;
          const mutations = yield* Mutations.AccountMutations;
          const now = DateTime.formatIso(yield* DateTime.now);
          const fake = yield* makeApnsSender();
          const run = (
            effect: Effect.Effect<void, Worker.AccountWorkerError, Worker.AccountWorker>,
            sender = fake.service,
          ) => effect.pipe(Effect.provide(workerServices(sender)));
          yield* mutations.registerDevice({ userId: owner, registration });
          yield* publish(1, state(now, "one"), "run:one:completed");
          yield* publish(2, state(now, "two"), "run:two:completed");
          yield* publish(3, state(now, "one", "running"), "none", null, null);
          yield* run(
            Effect.gen(function* () {
              yield* (yield* Worker.AccountWorker).pass(owner);
            }),
          );
          expect(
            (yield* Ref.get(fake.sent)).map((job) => job.payload.notification?.threadId).sort(),
          ).toEqual(["one", "two"]);
          expect(
            (yield* sql`SELECT 1 FROM personal_relay_alert_deliveries WHERE status = 'accepted'`)
              .length,
          ).toBe(2);
          yield* run(
            Effect.gen(function* () {
              yield* (yield* Worker.AccountWorker).pass(owner);
            }),
          );
          expect((yield* Ref.get(fake.sent)).length).toBe(2);

          yield* reset;
          yield* mutations.registerDevice({ userId: owner, registration });
          yield* mutations.registerLiveActivity({
            userId: owner,
            registration: { deviceId: registration.deviceId, activityPushToken: "activity" },
          });
          yield* publish(1, state(now));
          yield* Ref.set(fake.sent, []);
          const fallback = ApnsSender.of({
            send: (job) =>
              fake.service.send(job).pipe(
                Effect.map((success) =>
                  job.channel === "live_activity"
                    ? {
                        classification: "token_dead" as const,
                        status: 410,
                        reason: "Unregistered",
                        apnsId: "test",
                      }
                    : success,
                ),
              ),
          });
          yield* run(
            Effect.gen(function* () {
              yield* (yield* Worker.AccountWorker).pass(owner);
            }),
            fallback,
          );
          expect((yield* Ref.get(fake.sent)).map((job) => job.channel)).toEqual([
            "live_activity",
            "notification",
          ]);
          expect(
            (yield* sql`SELECT 1 FROM personal_relay_alert_deliveries WHERE status = 'accepted'`)
              .length,
          ).toBe(1);
          yield* publish(2, state(now), "run:2:completed");
          const dead = ApnsSender.of({
            send: (job) =>
              fake.service.send(job).pipe(
                Effect.as({
                  classification: "token_dead" as const,
                  status: 410,
                  reason: "Unregistered",
                  apnsId: "test",
                }),
              ),
          });
          yield* run(
            Effect.gen(function* () {
              yield* (yield* Worker.AccountWorker).pass(owner);
            }),
            dead,
          );
          expect(
            (yield* sql`SELECT 1 FROM personal_relay_alert_deliveries WHERE status = 'pending'`)
              .length,
          ).toBe(1);

          yield* reset;
          yield* mutations.registerDevice({ userId: owner, registration });
          yield* mutations.registerLiveActivity({
            userId: owner,
            registration: { deviceId: registration.deviceId, activityPushToken: "activity" },
          });
          const long = '"\\界😀'.repeat(40);
          for (let index = 0; index < 16; index++)
            yield* publish(
              index + 1,
              {
                ...state(now, `backlog-${index}`),
                threadTitle: long,
                modelTitle: long,
                projectTitle: long,
              },
              `run:${index}:completed`,
            );
          yield* Ref.set(fake.sent, []);
          yield* run(
            Effect.gen(function* () {
              yield* (yield* Worker.AccountWorker).pass(owner);
            }),
          );
          const combined = (yield* Ref.get(fake.sent))[0];
          expect(combined?.deliveries.length).toBe(16);
          expect(
            (yield* sql`SELECT 1 FROM personal_relay_alert_deliveries WHERE status = 'accepted' AND unknown_sends = 0`)
              .length,
          ).toBe(16);
          const client = yield* ApnsClient.ApnsClient;
          if (!combined?.payload.aggregate) throw new Error("Expected aggregate");
          const body = prepareRequest(combined, client, Date.parse(now)).request.payload;
          expect(new TextEncoder().encode(stableStringify(body)).byteLength).toBeLessThanOrEqual(
            3800,
          );

          // While APNs is stalled, mutations take the lock and the next device sees their state.
          yield* reset;
          yield* mutations.registerDevice({
            userId: owner,
            registration: { ...registration, deviceId: "a-device", pushToken: "a-push" },
          });
          yield* mutations.registerDevice({
            userId: owner,
            registration: { ...registration, deviceId: "b-device", pushToken: "b-push" },
          });
          for (const deviceId of ["a-device", "b-device"])
            yield* mutations.registerLiveActivity({
              userId: owner,
              registration: { deviceId, activityPushToken: `${deviceId}-activity` },
            });
          yield* publish(1, state(now, "initial", "running"), "none", null, null);
          const entered = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          yield* Ref.set(fake.sent, []);
          const stalled = ApnsSender.of({
            send: (job) =>
              Effect.gen(function* () {
                if (job.payload.target.deviceId === "a-device") {
                  yield* Deferred.succeed(entered, undefined);
                  yield* Deferred.await(release);
                }
                return yield* fake.service.send(job);
              }),
          });
          yield* Effect.gen(function* () {
            const worker = yield* Worker.AccountWorker;
            const pass = yield* worker.pass(owner).pipe(Effect.forkChild);
            yield* Deferred.await(entered);
            yield* publish(2, state(now, "new", "running"), "none", null, null);
            yield* mutations.registerDevice({
              userId: owner,
              registration: { ...registration, deviceId: "b-device", pushToken: "b-push" },
            });
            yield* Deferred.succeed(release, undefined);
            expect((yield* Fiber.join(pass)).generationChanged).toBe(true);
            yield* publish(3, state(now, "third", "running"), "none", null, null);
            yield* worker.pass(owner);
          }).pipe(Effect.provide(workerServices(stalled)));
          const fair = yield* Ref.get(fake.sent);
          expect(fair.map((job) => job.payload.target.deviceId)).toEqual([
            "a-device",
            "b-device",
            "b-device",
            "a-device",
          ]);
          expect(
            fair.find((job) => job.payload.target.deviceId === "b-device")?.payload.aggregate
              ?.activities.length,
          ).toBe(2);

          yield* reset;
          yield* mutations.registerDevice({ userId: owner, registration });
          yield* mutations.registerLiveActivity({
            userId: owner,
            registration: { deviceId: registration.deviceId, activityPushToken: "activity" },
          });
          const secondEnvironment = EnvironmentId.make("second-environment");
          yield* sql`INSERT INTO relay_environment_links (user_id, environment_id, environment_public_key, endpoint_http_base_url, endpoint_ws_base_url, endpoint_provider_kind, notifications_enabled, live_activities_enabled, created_by_device_id, revoked_at, created_at, updated_at, managed_tunnels_enabled, hold_webhooks_while_offline, environment_label) SELECT user_id, ${secondEnvironment}, environment_public_key, endpoint_http_base_url, endpoint_ws_base_url, endpoint_provider_kind, notifications_enabled, live_activities_enabled, created_by_device_id, revoked_at, created_at, updated_at, managed_tunnels_enabled, hold_webhooks_while_offline, environment_label FROM relay_environment_links WHERE environment_id = ${environmentId}`.pipe(
            Effect.orDie,
          );
          yield* Effect.all(
            [
              publish(1, state(now, "first-env", "running"), "none", null, null),
              mutations.publish({
                userId: owner,
                environmentId: secondEnvironment,
                environmentPublicKey: key,
                threadId: "second-env",
                request: {
                  state: {
                    ...state(now, "second-env", "running"),
                    environmentId: secondEnvironment,
                  },
                  proof: "test",
                },
                claims: { t3_publish_seq: 1, t3_transition: null, t3_transition_eligible_at: null },
              }),
            ],
            { concurrency: 2 },
          );
          yield* Ref.set(fake.sent, []);
          yield* run(
            Effect.gen(function* () {
              yield* (yield* Worker.AccountWorker).pass(owner);
            }),
          );
          expect(
            (yield* Ref.get(fake.sent))[0]?.payload.aggregate?.activities
              .map((row) => row.environmentId)
              .sort(),
          ).toEqual([environmentId, secondEnvironment].sort());

          // A new token registered during an end must survive the old token's outcome.
          yield* reset;
          yield* mutations.registerDevice({ userId: owner, registration });
          yield* mutations.registerLiveActivity({
            userId: owner,
            registration: { deviceId: registration.deviceId, activityPushToken: "old-activity" },
          });
          yield* sql`UPDATE relay_live_activities SET remote_started_at = ${DateTime.formatIso(DateTime.makeUnsafe(Date.parse(now) - 180_000))}`;
          const tokenRace = ApnsSender.of({
            send: (job) =>
              Effect.gen(function* () {
                yield* mutations
                  .registerLiveActivity({
                    userId: owner,
                    registration: {
                      deviceId: registration.deviceId,
                      activityPushToken: "new-activity",
                    },
                  })
                  .pipe(Effect.orDie);
                return yield* fake.service.send(job);
              }),
          });
          yield* run(
            Effect.gen(function* () {
              yield* (yield* Worker.AccountWorker).pass(owner);
            }),
            tokenRace,
          );
          expect(
            (yield* sql<{
              activity_push_token: string;
            }>`SELECT activity_push_token FROM relay_live_activities`)[0]?.activity_push_token,
          ).toBe("new-activity");

          // A mutation between planning and the end fence cancels that end.
          yield* reset;
          yield* mutations.registerDevice({ userId: owner, registration });
          yield* mutations.registerLiveActivity({
            userId: owner,
            registration: { deviceId: registration.deviceId, activityPushToken: "activity" },
          });
          yield* sql`UPDATE relay_live_activities SET remote_started_at = ${DateTime.formatIso(DateTime.makeUnsafe(Date.parse(now) - 180_000))}`;
          const planner = yield* Planner.TargetPlanner;
          let injected = false;
          const fenced = Planner.TargetPlanner.of({
            plan: (snapshot, at) =>
              Effect.gen(function* () {
                const plan = yield* planner.plan(snapshot, at);
                if (!injected) {
                  injected = true;
                  yield* publish(1, state(now, "fresh", "running"), "none", null, null).pipe(
                    Effect.provideService(Mutations.AccountMutations, mutations),
                    Effect.mapError((cause) => new Planner.TargetPlanError({ cause })),
                  );
                }
                return plan;
              }),
          });
          yield* Ref.set(fake.sent, []);
          yield* Worker.make.pipe(
            Effect.provideService(Planner.TargetPlanner, fenced),
            provideSender(fake.service),
            Effect.flatMap((worker) => worker.pass(owner)),
          );
          expect((yield* Ref.get(fake.sent)).map((job) => job.payload.kind)).toEqual([
            "live_activity_update",
          ]);
        }).pipe(Effect.scoped, Effect.provide(plannerServices)),
    );

    it.live(
      "slice c: persisted per-channel tiers, batch stops, ambiguity, expiry, future deadlines and retention",
      () =>
        Effect.gen(function* () {
          yield* initialize;
          yield* reset;
          const sql = (yield* RelayDb).$client;
          const mutations = yield* Mutations.AccountMutations;
          const lock = yield* Lock.AccountLock;
          const liveClock = yield* Clock.Clock;
          const base = yield* Clock.currentTimeMillis;
          let time = base;
          const clock = {
            monotonicTimeNanosUnsafe: () => liveClock.monotonicTimeNanosUnsafe(),
            monotonicTimeNanos: liveClock.monotonicTimeNanos,
            sleep: (duration: Duration.Duration) => liveClock.sleep(duration),
            currentTimeMillisUnsafe: () => time,
            currentTimeMillis: Effect.sync(() => time),
            currentTimeNanosUnsafe: () => BigInt(time) * 1_000_000n,
            currentTimeNanos: Effect.sync(() => BigInt(time) * 1_000_000n),
          };
          const iso = () => DateTime.formatIso(DateTime.makeUnsafe(time));
          const fake = yield* makeApnsSender();
          const sentAt: number[] = [];
          const unavailable = ApnsSender.of({
            send: (job) =>
              Effect.gen(function* () {
                sentAt.push((time - base) / 1000);
                yield* fake.service.send(job);
                return {
                  classification: "retryable_answered" as const,
                  status: 503,
                  reason: "ServiceUnavailable",
                  apnsId: "test",
                };
              }),
          });
          yield* Effect.gen(function* () {
            yield* mutations.registerDevice({ userId: owner, registration });
            for (let i = 0; i < 5; i++)
              yield* publish(i + 1, state(iso(), `batch-${i}`), `run:batch-${i}:completed`);
            const first = yield* Worker.make.pipe(provideSender(unavailable));
            let worker = first;
            for (let second = 0; second <= 600; second++) {
              time = base + second * 1000;
              if (second % 2 === 0)
                yield* publish(second + 10, state(iso(), "busy", "running"), "none", null, null);
              if (second === 200) worker = yield* Worker.make.pipe(provideSender(unavailable));
              yield* worker.wake;
              yield* worker.pass(owner);
            }
            expect(sentAt).toEqual([0, 5, 20, 50, 110, 230, 530]);
            expect(
              (yield* sql<{
                failure_count: number;
              }>`SELECT failure_count FROM personal_relay_target_state WHERE channel = 'notification'`)[0]
                ?.failure_count,
            ).toBe(7);
            expect(
              (yield* sql`SELECT 1 FROM personal_relay_alert_deliveries WHERE status = 'pending' AND unknown_sends = 0`)
                .length,
            ).toBe(5);

            // A failed notification stops its quiet end. A later end cannot reset that channel.
            yield* reset;
            time = base;
            yield* mutations.registerDevice({ userId: owner, registration });
            yield* mutations.registerLiveActivity({
              userId: owner,
              registration: { deviceId: registration.deviceId, activityPushToken: "activity" },
            });
            yield* publish(1, state(iso()));
            yield* publish(2, null);
            yield* sql`UPDATE relay_live_activities SET remote_started_at = ${DateTime.formatIso(DateTime.makeUnsafe(time - 180_000))}`;
            yield* Ref.set(fake.sent, []);
            const notificationFailure = ApnsSender.of({
              send: (job) =>
                job.channel === "notification" ? unavailable.send(job) : fake.service.send(job),
            });
            const quiet = yield* Worker.make.pipe(provideSender(notificationFailure));
            yield* quiet.pass(owner);
            expect((yield* Ref.get(fake.sent)).map((job) => job.payload.kind)).toEqual([
              "push_notification",
            ]);
            yield* quiet.pass(owner);
            expect((yield* Ref.get(fake.sent)).map((job) => job.payload.kind)).toEqual([
              "push_notification",
              "live_activity_end",
            ]);
            expect(
              (yield* sql<{
                failure_count: number;
              }>`SELECT failure_count FROM personal_relay_target_state WHERE channel = 'notification'`)[0]
                ?.failure_count,
            ).toBe(1);

            // New successful notifications never erase a repeatedly failing Live Activity series.
            yield* reset;
            time = base;
            yield* mutations.registerDevice({ userId: owner, registration });
            yield* mutations.registerLiveActivity({
              userId: owner,
              registration: { deviceId: registration.deviceId, activityPushToken: "activity" },
            });
            yield* sql`UPDATE relay_mobile_devices SET preferences_json = ${stableStringify({ ...registration.preferences, liveActivitiesEnabled: false })}::jsonb`;
            const mixedTimes: number[] = [];
            const mixed = ApnsSender.of({
              send: (job) =>
                fake.service.send(job).pipe(
                  Effect.map((success) => {
                    if (job.channel === "notification") return success;
                    mixedTimes.push((time - base) / 1000);
                    return {
                      classification: "retryable_answered" as const,
                      status: 503,
                      reason: "ServiceUnavailable",
                      apnsId: "test",
                    };
                  }),
                ),
            });
            const mixedWorker = yield* Worker.make.pipe(provideSender(mixed));
            for (let second = 0; second <= 230; second++) {
              time = base + second * 1000;
              if (second % 2 === 0)
                yield* publish(
                  second + 1,
                  state(iso(), `fresh-${second}`),
                  `run:fresh-${second}:completed`,
                );
              yield* mixedWorker.pass(owner);
            }
            expect(mixedTimes).toEqual([0, 5, 20, 50, 110, 230]);
            expect(
              (yield* sql<{
                failure_count: number;
              }>`SELECT failure_count FROM personal_relay_target_state WHERE channel = 'live_activity'`)[0]
                ?.failure_count,
            ).toBe(6);

            for (const classification of ["retryable_not_sent", "retryable_answered"] as const) {
              yield* reset;
              time = base;
              yield* mutations.registerDevice({ userId: owner, registration });
              yield* publish(1, state(iso()));
              yield* Ref.set(fake.sent, []);
              let failed = false;
              const transient = ApnsSender.of({
                send: (job) =>
                  fake.service.send(job).pipe(
                    Effect.map((success) => {
                      if (failed) return success;
                      failed = true;
                      return {
                        classification,
                        status: classification === "retryable_not_sent" ? null : 503,
                        reason: null,
                        apnsId: null,
                      };
                    }),
                  ),
              });
              const worker = yield* Worker.make.pipe(provideSender(transient));
              yield* worker.pass(owner);
              yield* worker.pass(owner);
              expect((yield* Ref.get(fake.sent)).length).toBe(1);
              time += 5000;
              yield* worker.pass(owner);
              expect((yield* Ref.get(fake.sent)).length).toBe(2);
              expect(
                (yield* sql`SELECT 1 FROM personal_relay_alert_deliveries WHERE status = 'accepted' AND unknown_sends = 0`)
                  .length,
              ).toBe(1);
              expect(
                (yield* sql<{
                  failure_count: number;
                }>`SELECT failure_count FROM personal_relay_target_state WHERE channel = 'notification'`)[0]
                  ?.failure_count,
              ).toBe(0);
            }

            // A deterministic failure finalizes only its own job and stops the remaining batch.
            yield* reset;
            time = base;
            yield* mutations.registerDevice({ userId: owner, registration });
            for (let i = 0; i < 6; i++)
              yield* publish(i + 1, state(iso(), `reject-${i}`), `run:reject-${i}:completed`);
            yield* Ref.set(fake.sent, []);
            const rejected = yield* Worker.make.pipe(
              provideSender(
                ApnsSender.of({
                  send: (job) =>
                    fake.service.send(job).pipe(
                      Effect.as({
                        classification: "rejected" as const,
                        status: 400,
                        reason: "PayloadTooLarge",
                        apnsId: "test",
                      }),
                    ),
                }),
              ),
            );
            yield* rejected.pass(owner);
            expect((yield* Ref.get(fake.sent)).length).toBe(1);
            expect(
              (yield* sql`SELECT 1 FROM personal_relay_alert_deliveries WHERE status = 'rejected'`)
                .length,
            ).toBe(1);
            expect(
              (yield* sql`SELECT 1 FROM personal_relay_alert_deliveries WHERE status = 'pending'`)
                .length,
            ).toBe(5);
            expect(
              (yield* sql<{
                apns_reason: string;
              }>`SELECT apns_reason FROM relay_delivery_attempts`)[0]?.apns_reason,
            ).toBe("payload_budget_violation");
            time += 5000;
            const recover = yield* Worker.make.pipe(provideSender(fake.service));
            yield* recover.pass(owner);
            expect(
              (yield* sql`SELECT 1 FROM personal_relay_alert_deliveries WHERE status = 'accepted'`)
                .length,
            ).toBe(5);

            // An outcome transaction failure retains the reservation and retries without a lease.
            yield* reset;
            time = base;
            yield* mutations.registerDevice({ userId: owner, registration });
            yield* publish(1, state(iso()));
            yield* sql.unsafe(
              "CREATE OR REPLACE FUNCTION u2_fail_outcome() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected'; END $$",
            );
            yield* sql.unsafe(
              "CREATE TRIGGER u2_fail_outcome BEFORE INSERT ON relay_delivery_attempts FOR EACH ROW EXECUTE FUNCTION u2_fail_outcome()",
            );
            const recovering = yield* Worker.make.pipe(provideSender(fake.service));
            yield* Effect.flip(recovering.pass(owner));
            expect(
              (yield* sql`SELECT 1 FROM personal_relay_alert_deliveries WHERE status = 'pending' AND unknown_sends = 1`)
                .length,
            ).toBe(1);
            yield* sql.unsafe("DROP TRIGGER u2_fail_outcome ON relay_delivery_attempts");
            yield* sql.unsafe("DROP FUNCTION u2_fail_outcome()");
            yield* recovering.pass(owner);
            expect(
              (yield* sql`SELECT 1 FROM personal_relay_alert_deliveries WHERE status = 'accepted'`)
                .length,
            ).toBe(1);

            // Both audible channels stop after two unknown sends, including a lost outcome write.
            for (const withActivity of [false, true]) {
              yield* reset;
              time = base;
              yield* mutations.registerDevice({ userId: owner, registration });
              if (withActivity)
                yield* mutations.registerLiveActivity({
                  userId: owner,
                  registration: { deviceId: registration.deviceId, activityPushToken: "activity" },
                });
              yield* publish(1, state(iso()));
              yield* Ref.set(fake.sent, []);
              const ambiguous = ApnsSender.of({
                send: (job) =>
                  fake.service.send(job).pipe(
                    Effect.as({
                      classification: "retryable_ambiguous" as const,
                      status: null,
                      reason: null,
                      apnsId: null,
                    }),
                  ),
              });
              const worker = yield* Worker.make.pipe(provideSender(ambiguous));
              yield* worker.pass(owner);
              time += 5000;
              yield* worker.pass(owner);
              time += 15_000;
              const restarted = yield* Worker.make.pipe(provideSender(fake.service));
              yield* restarted.pass(owner);
              expect(
                (yield* Ref.get(fake.sent)).filter((job) => job.deliveries.length > 0).length,
              ).toBe(2);
              expect(
                (yield* sql`SELECT 1 FROM personal_relay_alert_deliveries WHERE status = 'ambiguous_abandoned' AND unknown_sends = 2`)
                  .length,
              ).toBe(1);
            }
            yield* reset;
            time = base;
            yield* mutations.registerDevice({ userId: owner, registration });
            yield* publish(1, state(iso()));
            const ledger = yield* Ledger.AlertLedger;
            const deliveries = yield* lock.withAccountLock(
              owner,
              ledger.listPending({ userId: owner, deviceId: registration.deviceId, now: iso() }),
            );
            yield* lock.withAccountLock(
              owner,
              ledger.beginSend({ deliveries, channel: "notification", now: iso() }),
            );
            const unknown = yield* Worker.make.pipe(
              provideSender(
                ApnsSender.of({
                  send: (job) =>
                    fake.service.send(job).pipe(
                      Effect.as({
                        classification: "retryable_ambiguous" as const,
                        status: null,
                        reason: null,
                        apnsId: null,
                      }),
                    ),
                }),
              ),
            );
            yield* unknown.pass(owner);
            yield* unknown.pass(owner);
            expect(
              (yield* sql`SELECT 1 FROM personal_relay_alert_deliveries WHERE status = 'ambiguous_abandoned'`)
                .length,
            ).toBe(1);

            yield* reset;
            time = base;
            yield* mutations.registerDevice({ userId: owner, registration });
            yield* publish(1, state(iso(), "attention", "waiting_for_input"), "request:1");
            yield* publish(2, state(iso(), "attention", "running"), "none", null, null);
            const superseding = yield* Worker.make.pipe(provideSender(fake.service));
            yield* superseding.pass(owner);
            expect(
              (yield* sql`SELECT 1 FROM personal_relay_alert_deliveries WHERE status = 'superseded'`)
                .length,
            ).toBe(1);
            yield* publish(3, state(iso(), "expire"), "run:expire:completed");
            time += 31 * 60_000;
            yield* Ref.set(fake.sent, []);
            const expiring = yield* Worker.make.pipe(provideSender(fake.service));
            yield* expiring.pass(owner);
            expect((yield* Ref.get(fake.sent)).length).toBe(0);
            expect(
              (yield* sql`SELECT 1 FROM personal_relay_alert_deliveries WHERE status = 'expired'`)
                .length,
            ).toBe(1);
            time += 25 * 60 * 60_000;
            expect((yield* expiring.pass(owner)).nextDeadline).toBe(null);
            expect((yield* expiring.pass(owner)).nextDeadline).toBe(null);

            // Empty cards, deletion during grace and revocation all reach a single end.
            for (const mode of ["empty", "delete", "revoke"]) {
              yield* reset;
              time = base;
              yield* mutations.registerDevice({ userId: owner, registration });
              yield* mutations.registerLiveActivity({
                userId: owner,
                registration: { deviceId: registration.deviceId, activityPushToken: "activity" },
              });
              if (mode !== "empty") {
                yield* publish(1, state(iso(), "thread", "running"), "none", null, null);
                if (mode === "delete") yield* publish(2, null);
                else
                  yield* lock.withAccountLock(
                    owner,
                    Effect.gen(function* () {
                      yield* sql`UPDATE relay_environment_links SET revoked_at = ${iso()}`;
                      yield* lock.bumpGeneration(owner);
                    }),
                  );
              }
              yield* Ref.set(fake.sent, []);
              const worker = yield* Worker.make.pipe(provideSender(fake.service));
              expect((yield* worker.pass(owner)).nextDeadline).toBe(base + 120_000);
              expect((yield* Ref.get(fake.sent)).length).toBe(0);
              time += 120_001;
              expect((yield* worker.pass(owner)).nextDeadline).toBe(null);
              yield* worker.pass(owner);
              expect((yield* Ref.get(fake.sent)).map((job) => job.payload.kind)).toEqual([
                "live_activity_end",
              ]);
            }

            yield* reset;
            time = base;
            yield* publish(1, state(iso()), "run:old:completed");
            yield* publish(2, state(iso(), "still-running", "running"), "none", null, null);
            yield* sql`UPDATE personal_relay_alerts SET created_at = ${iso()}::timestamptz - interval '49 hours'`;
            yield* sql`UPDATE personal_relay_thread_versions SET accepted_at = ${iso()}::timestamptz - interval '2 hours'`;
            yield* sql`INSERT INTO personal_relay_target_state (device_id, channel) VALUES ('orphan', 'live_activity')`;
            yield* sql`INSERT INTO relay_delivery_attempts (id, kind, created_at) VALUES ('old-attempt', 'push_notification', ${DateTime.formatIso(DateTime.makeUnsafe(time - 8 * 24 * 60 * 60_000))}), ('fresh-attempt', 'push_notification', ${iso()})`;
            yield* sql`INSERT INTO relay_dpop_proofs (thumbprint, jti, iat, expires_at, created_at) VALUES ('test', 'expired', 1, ${iso()}, ${iso()}), ('test', 'fresh', 1, ${DateTime.formatIso(DateTime.makeUnsafe(time + 60 * 60_000))}, ${iso()})`;
            time += 31 * 60_000;
            yield* (yield* Pruner.Pruner).prune(owner);
            expect((yield* sql`SELECT 1 FROM personal_relay_alerts`).length).toBe(0);
            expect((yield* sql`SELECT 1 FROM personal_relay_thread_versions`).length).toBe(0);
            expect((yield* sql`SELECT 1 FROM personal_relay_target_state`).length).toBe(0);
            expect((yield* sql`SELECT 1 FROM relay_agent_activity_rows`).length).toBe(1);
            expect(
              (yield* sql<{ id: string }>`SELECT id FROM relay_delivery_attempts`).map(
                (row) => row.id,
              ),
            ).toEqual(["fresh-attempt"]);
            expect(
              (yield* sql<{ jti: string }>`SELECT jti FROM relay_dpop_proofs`).map(
                (row) => row.jti,
              ),
            ).toEqual(["fresh"]);
          }).pipe(Effect.provideService(Clock.Clock, clock));
        }).pipe(
          Effect.scoped,
          Effect.provide(Pruner.layer.pipe(Layer.provideMerge(plannerServices))),
        ),
    );

    it.live(
      "worker runtime: startup and poll repair a dropped wake, then restart preserves alert acceptance",
      () =>
        Effect.gen(function* () {
          yield* initialize;
          yield* reset;
          const mutations = yield* Mutations.AccountMutations;
          const liveClock = yield* Clock.Clock;
          let now = yield* Clock.currentTimeMillis;
          const parked = yield* Queue.unbounded<void>();
          const ticks = yield* Queue.unbounded<void>();
          const clock = {
            monotonicTimeNanosUnsafe: () => liveClock.monotonicTimeNanosUnsafe(),
            monotonicTimeNanos: liveClock.monotonicTimeNanos,
            currentTimeNanosUnsafe: () => BigInt(now) * 1_000_000n,
            currentTimeNanos: Effect.sync(() => BigInt(now) * 1_000_000n),
            currentTimeMillis: Effect.sync(() => now),
            currentTimeMillisUnsafe: () => now,
            sleep: (duration: Duration.Duration) =>
              Duration.toMillis(duration) === 15_000
                ? Queue.offer(parked, undefined).pipe(Effect.andThen(Queue.take(ticks)))
                : liveClock.sleep(duration),
          };
          const fake = yield* makeApnsSender();
          yield* Effect.gen(function* () {
            yield* mutations.registerDevice({ userId: owner, registration });
            const worker = yield* Worker.make.pipe(provideSender(fake.service));
            const fiber = yield* worker.run(owner).pipe(Effect.forkChild);
            yield* Queue.take(parked);
            yield* publish(1, state(DateTime.formatIso(DateTime.makeUnsafe(now))));
            now += 15_000;
            yield* Queue.offer(ticks, undefined);
            yield* Queue.take(parked);
            expect((yield* Ref.get(fake.sent)).length).toBe(1);
            yield* Fiber.interrupt(fiber);
            const restart = yield* Worker.make.pipe(provideSender(fake.service));
            const restarted = yield* restart.run(owner).pipe(Effect.forkChild);
            yield* Queue.take(parked);
            expect((yield* Ref.get(fake.sent)).length).toBe(1);
            yield* Fiber.interrupt(restarted);
          }).pipe(Effect.provideService(Clock.Clock, clock));
        }).pipe(Effect.scoped, Effect.provide(plannerServices)),
    );

    it.live("keeps first-install alerts pending until registration supplies a usable token", () =>
      Effect.gen(function* () {
        yield* initialize;
        yield* reset;
        const mutations = yield* Mutations.AccountMutations;
        const sql = (yield* RelayDb).$client;
        const fake = yield* makeApnsSender();
        yield* mutations.registerDevice({
          userId: owner,
          registration: { ...registration, pushToken: undefined },
        });
        yield* publish(1, state(DateTime.formatIso(yield* DateTime.now)));
        const pass = Effect.flatMap(Worker.AccountWorker, (worker) => worker.pass(owner));
        yield* pass.pipe(Effect.provide(workerServices(fake.service)));
        expect((yield* Ref.get(fake.sent)).length).toBe(0);
        expect((yield* sql`SELECT status FROM personal_relay_alert_deliveries`)[0]?.status).toBe(
          "pending",
        );
        yield* mutations.registerDevice({ userId: owner, registration });
        yield* pass.pipe(Effect.provide(workerServices(fake.service)));
        yield* pass.pipe(Effect.provide(workerServices(fake.service)));
        expect((yield* Ref.get(fake.sent)).length).toBe(1);
        expect((yield* sql`SELECT status FROM personal_relay_alert_deliveries`)[0]?.status).toBe(
          "accepted",
        );
      }).pipe(Effect.provide(services)),
    );

    it.live("falls back once from an alerting Live Activity topic rejection", () =>
      Effect.gen(function* () {
        yield* initialize;
        yield* reset;
        const mutations = yield* Mutations.AccountMutations;
        const sql = (yield* RelayDb).$client;
        const fake = yield* makeApnsSender();
        yield* mutations.registerDevice({ userId: owner, registration });
        yield* mutations.registerLiveActivity({
          userId: owner,
          registration: { deviceId: registration.deviceId, activityPushToken: "activity" },
        });
        yield* publish(1, state(DateTime.formatIso(yield* DateTime.now)));
        const sender = ApnsSender.of({
          send: (job) =>
            fake.service.send(job).pipe(
              Effect.as(
                job.channel === "live_activity"
                  ? ({
                      classification: "rejected",
                      status: 400,
                      reason: "TopicDisallowed",
                      apnsId: "test",
                    } as const)
                  : success,
              ),
            ),
        });
        yield* Effect.flatMap(Worker.AccountWorker, (worker) => worker.pass(owner)).pipe(
          Effect.provide(workerServices(sender)),
        );
        expect((yield* Ref.get(fake.sent)).map((job) => job.channel)).toEqual([
          "live_activity",
          "notification",
        ]);
        expect(
          (yield* sql`SELECT status, unknown_sends FROM personal_relay_alert_deliveries`)[0],
        ).toMatchObject({ status: "accepted", unknown_sends: 0 });
      }).pipe(Effect.provide(services)),
    );

    it.live("retains the Live Activity token after a rejection without a topic fallback", () =>
      Effect.gen(function* () {
        yield* initialize;
        yield* reset;
        const mutations = yield* Mutations.AccountMutations;
        const sql = (yield* RelayDb).$client;
        const fake = yield* makeApnsSender();
        yield* mutations.registerDevice({ userId: owner, registration });
        yield* mutations.registerLiveActivity({
          userId: owner,
          registration: { deviceId: registration.deviceId, activityPushToken: "activity" },
        });
        yield* publish(1, state(DateTime.formatIso(yield* DateTime.now)));
        const sender = ApnsSender.of({
          send: (job) =>
            fake.service.send(job).pipe(
              Effect.as({
                classification: "rejected" as const,
                status: 413,
                reason: "PayloadTooLarge",
                apnsId: "test",
              }),
            ),
        });
        yield* Effect.flatMap(Worker.AccountWorker, (worker) => worker.pass(owner)).pipe(
          Effect.provide(workerServices(sender)),
        );
        expect((yield* Ref.get(fake.sent)).map((job) => job.channel)).toEqual(["live_activity"]);
        expect(
          (yield* sql`SELECT activity_push_token, ended_at FROM relay_live_activities`)[0],
        ).toMatchObject({
          activity_push_token: "activity",
          ended_at: null,
        });
        expect(
          (yield* sql`SELECT status, unknown_sends FROM personal_relay_alert_deliveries`)[0],
        ).toMatchObject({ status: "rejected", unknown_sends: 0 });
      }).pipe(Effect.provide(services)),
    );

    it.live(
      "suppresses disabled notifications without a Live Activity instead of ringing after re-enable",
      () =>
        Effect.gen(function* () {
          yield* initialize;
          yield* reset;
          const mutations = yield* Mutations.AccountMutations;
          const sql = (yield* RelayDb).$client;
          const fake = yield* makeApnsSender();
          yield* mutations.registerDevice({
            userId: owner,
            registration: {
              ...registration,
              preferences: { ...registration.preferences, notificationsEnabled: false },
            },
          });
          yield* publish(1, state(DateTime.formatIso(yield* DateTime.now)));
          const pass = Effect.flatMap(Worker.AccountWorker, (worker) => worker.pass(owner));
          yield* pass.pipe(Effect.provide(workerServices(fake.service)));
          expect((yield* sql`SELECT status FROM personal_relay_alert_deliveries`)[0]?.status).toBe(
            "suppressed",
          );
          yield* mutations.registerDevice({ userId: owner, registration });
          yield* pass.pipe(Effect.provide(workerServices(fake.service)));
          expect(yield* Ref.get(fake.sent)).toHaveLength(0);
        }).pipe(Effect.provide(services)),
    );

    it.live("preserves every deep link when long CJK titles need sender sanitization", () =>
      Effect.gen(function* () {
        yield* initialize;
        yield* reset;
        const mutations = yield* Mutations.AccountMutations;
        const fake = yield* makeApnsSender();
        yield* mutations.registerDevice({ userId: owner, registration });
        yield* mutations.registerLiveActivity({
          userId: owner,
          registration: { deviceId: registration.deviceId, activityPushToken: "activity" },
        });
        const now = DateTime.formatIso(yield* DateTime.now);
        for (let index = 0; index < 3; index++)
          yield* publish(
            index + 1,
            {
              ...state(now, `cjk-${index}`),
              threadTitle: "界".repeat(500),
              projectTitle: "界".repeat(500),
              modelTitle: "界".repeat(500),
              deepLink: `/thread/cjk-${index}`,
            },
            `cjk:${index}`,
          );
        yield* Effect.flatMap(Worker.AccountWorker, (worker) => worker.pass(owner)).pipe(
          Effect.provide(workerServices(fake.service)),
        );
        const job = (yield* Ref.get(fake.sent))[0]!;
        const client = yield* ApnsClient.ApnsClient;
        const raw = client.makeLiveActivityRequest({
          token: job.payload.target.token,
          event: "update",
          state: job.payload.aggregate!,
          nowEpochSeconds: Math.floor(Date.parse(now) / 1000),
          nowIso: now,
          alert: job.payload.alert ?? null,
        });
        const prepared = prepareRequest(job, client, Date.parse(now));
        expect(Buffer.byteLength(JSON.stringify(prepared.request.payload))).toBeLessThanOrEqual(
          3800,
        );
        const payload = prepared.request.payload as { aps: { "content-state": { props: string } } };
        const rows = JSON.parse(payload.aps["content-state"].props).activities;
        expect(rows).toHaveLength(3);
        expect(rows.map((row: { deepLink: string }) => row.deepLink).toSorted()).toEqual([
          "/thread/cjk-0",
          "/thread/cjk-1",
          "/thread/cjk-2",
        ]);
        expect(job.deliveries).toHaveLength(3);
        expect(Buffer.byteLength(JSON.stringify(raw.payload))).toBeGreaterThan(3800);
      }).pipe(Effect.provide(plannerServices)),
    );

    it.live("commits an in-flight send outcome before honoring interruption", () =>
      Effect.gen(function* () {
        yield* initialize;
        yield* reset;
        const mutations = yield* Mutations.AccountMutations;
        const sql = (yield* RelayDb).$client;
        yield* mutations.registerDevice({ userId: owner, registration });
        yield* publish(1, state(DateTime.formatIso(yield* DateTime.now)));
        const started = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const sender = ApnsSender.of({
          send: () =>
            Deferred.succeed(started, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.as(success),
            ),
        });
        yield* Effect.gen(function* () {
          const worker = yield* Worker.AccountWorker;
          const sending = yield* Effect.forkChild(worker.pass(owner));
          yield* Effect.addFinalizer(() =>
            Deferred.succeed(release, undefined).pipe(Effect.asVoid),
          );
          yield* Deferred.await(started);
          const interrupting = yield* Effect.forkChild(Fiber.interrupt(sending), {
            startImmediately: true,
          });
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(interrupting);
          yield* Fiber.await(sending);
        }).pipe(Effect.provide(workerServices(sender)));
        expect(
          (yield* sql`SELECT status, unknown_sends FROM personal_relay_alert_deliveries`)[0],
        ).toMatchObject({ status: "accepted", unknown_sends: 0 });
      }).pipe(Effect.provide(services)),
    );
  },
);
