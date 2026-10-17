// @effect-diagnostics nodeBuiltinImport:off -- The transport integration generates throwaway signing keys.
import * as NodeCrypto from "node:crypto";
import { describe, expect, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import { RelayDb } from "t3code-relay/db";
import * as RelayConfig from "t3code-relay/Config";
import * as ApnsClient from "t3code-relay/agentActivity/ApnsClient";
import * as ApnsProviderTokens from "t3code-relay/agentActivity/ApnsProviderTokens";
import * as Sender from "../../src/apns/ApnsSender.ts";
import * as Http2Client from "../../src/apns/Http2HttpClient.ts";
import * as Worker from "../../src/account/AccountWorker.ts";
import * as Planner from "../../src/account/TargetPlanner.ts";
import { AccountMutations } from "../../src/account/AccountMutations.ts";
import { startFakeApns, testCa, type FakeApnsMode } from "../apns/FakeApnsServer.ts";
import { services, initialize, reset, owner, registration, publish, state } from "./Harness.ts";

describe.skipIf(!process.env.T3CODE_PERSONAL_RELAY_TEST_DATABASE_URL)(
  "owned Postgres fixture",
  () => {
    it.live(
      "real sender: persisted tiers, stop batch, fallback, ambiguity, bounded ledger finalization and disabled APNs",
      () =>
        Effect.gen(function* () {
          let mode: FakeApnsMode = "503";
          let deadActivity = false;
          const sentAt: number[] = [];
          const liveClock = yield* Clock.Clock;
          const base = yield* Clock.currentTimeMillis;
          let time = base;
          const clock: Clock.Clock = {
            monotonicTimeNanosUnsafe: () => liveClock.monotonicTimeNanosUnsafe(),
            monotonicTimeNanos: liveClock.monotonicTimeNanos,
            sleep: (duration) => liveClock.sleep(duration),
            currentTimeMillisUnsafe: () => time,
            currentTimeMillis: Effect.sync(() => time),
            currentTimeNanosUnsafe: () => BigInt(time) * 1_000_000n,
            currentTimeNanos: Effect.sync(() => BigInt(time) * 1_000_000n),
          };
          const fake = yield* Effect.acquireRelease(
            Effect.promise(() =>
              startFakeApns({
                onRequest: (request, stream) => {
                  sentAt.push((time - base) / 1000);
                  if (mode === "reset-after-send") {
                    stream.close(2);
                    return;
                  }
                  const dead = deadActivity && request.headers["apns-push-type"] === "liveactivity";
                  stream.respond({ ":status": dead ? 410 : mode === "503" ? 503 : 200 });
                  stream.end(
                    dead
                      ? '{"reason":"Unregistered"}'
                      : mode === "503"
                        ? '{"reason":"ServiceUnavailable"}'
                        : "",
                  );
                },
              }),
            ),
            (server) => Effect.promise(() => server.close()),
          );
          const key = NodeCrypto.generateKeyPairSync("ec", { namedCurve: "prime256v1" })
            .privateKey.export({ type: "pkcs8", format: "pem" })
            .toString();
          const configuration = RelayConfig.RelayConfiguration.of({
            relayIssuer: "https://test.invalid",
            apns: {
              teamId: "test",
              keyId: "test",
              privateKey: Redacted.make(key),
              bundleId: "test.bundle",
              environment: "sandbox",
            },
            clerkSecretKey: Redacted.make(""),
            clerkPublishableKey: "",
            clerkJwtAudience: "test",
            apnsDeliveryJobSigningSecret: Redacted.make("test-only"),
            cloudMintPrivateKey: Redacted.make(""),
            cloudMintPublicKey: "",
            managedEndpointBaseDomain: undefined,
            managedEndpointNamespace: undefined,
          });
          const apns = ApnsClient.layer.pipe(
            Layer.provide(ApnsProviderTokens.layer),
            Layer.provide(Http2Client.layer({ originOverride: fake.origin, ca: testCa })),
          );
          const core = Layer.mergeAll(
            services,
            apns,
            Layer.succeed(RelayConfig.RelayConfiguration, configuration),
          );
          const planner = Planner.layer.pipe(Layer.provideMerge(core));
          const sender = Sender.layer.pipe(Layer.provide(core));
          yield* Effect.gen(function* () {
            yield* initialize;
            yield* reset;
            const sql = (yield* RelayDb).$client;
            const mutations = yield* AccountMutations;
            const worker = yield* Worker.make;
            const iso = () => DateTime.formatIso(DateTime.makeUnsafe(time));
            yield* mutations.registerDevice({ userId: owner, registration });
            for (let i = 0; i < 5; i++)
              yield* publish(i + 1, state(iso(), `backoff-${i}`), `run:${i}:completed`);
            for (let second = 0; second <= 600; second++) {
              time = base + second * 1000;
              if (second % 2 === 0)
                yield* publish(second + 10, state(iso(), "busy", "running"), "none", null, null);
              yield* worker.wake;
              yield* worker.pass(owner);
            }
            expect(sentAt).toEqual([0, 5, 20, 50, 110, 230, 530]);
            expect(
              (yield* sql`SELECT failure_count FROM personal_relay_target_state WHERE channel = 'notification'`)[0]
                ?.failure_count,
            ).toBe(7);
            expect(
              (yield* sql`SELECT 1 FROM personal_relay_alert_deliveries WHERE status = 'pending' AND unknown_sends = 0`)
                .length,
            ).toBe(5);
            mode = "success";
            time = base + 831_000;
            yield* worker.pass(owner);
            expect(
              (yield* sql`SELECT 1 FROM personal_relay_alert_deliveries WHERE status = 'accepted'`)
                .length,
            ).toBe(5);
            const acceptedCount = fake.requests.length;
            yield* worker.pass(owner);
            expect(fake.requests.length).toBe(acceptedCount);

            yield* reset;
            yield* mutations.registerDevice({ userId: owner, registration });
            yield* mutations.registerLiveActivity({
              userId: owner,
              registration: { deviceId: registration.deviceId, activityPushToken: "activity" },
            });
            deadActivity = true;
            yield* publish(1, state(iso()));
            const fallbackStart = fake.requests.length;
            yield* worker.pass(owner);
            expect(
              fake.requests
                .slice(fallbackStart)
                .map((request) => request.headers["apns-push-type"]),
            ).toEqual(["liveactivity", "alert"]);
            expect(
              (yield* sql`SELECT status FROM personal_relay_alert_deliveries`)[0]?.status,
            ).toBe("accepted");
            deadActivity = false;

            for (const live of [false, true]) {
              yield* reset;
              yield* mutations.registerDevice({ userId: owner, registration });
              if (live)
                yield* mutations.registerLiveActivity({
                  userId: owner,
                  registration: { deviceId: registration.deviceId, activityPushToken: "activity" },
                });
              yield* publish(1, state(iso()));
              const start = fake.requests.length;
              mode = "reset-after-send";
              yield* worker.pass(owner);
              time += 6000;
              yield* worker.pass(owner);
              time += 16000;
              yield* worker.pass(owner);
              expect(
                fake.requests.slice(start).filter((request) => {
                  const body = JSON.parse(request.body.toString());
                  return body.aps.alert !== undefined;
                }).length,
              ).toBe(2);
              expect(
                (yield* sql`SELECT status, unknown_sends FROM personal_relay_alert_deliveries`)[0],
              ).toMatchObject({ status: "ambiguous_abandoned", unknown_sends: 2 });
            }

            mode = "success";
            yield* reset;
            yield* mutations.registerLiveActivity({
              userId: owner,
              registration: { deviceId: registration.deviceId, activityPushToken: "activity" },
            });
            const text = '界😀"\\'.repeat(20);
            for (let i = 0; i < 18; i++)
              yield* publish(
                i + 1,
                {
                  ...state(iso(), `large-${i}`),
                  threadTitle: text,
                  projectTitle: text,
                  modelTitle: text,
                },
                `run:${i}:completed`,
              );
            yield* mutations.registerDevice({ userId: owner, registration });
            yield* mutations.registerDevice({ userId: owner, registration });
            expect((yield* sql`SELECT 1 FROM personal_relay_alert_deliveries`).length).toBe(18);
            const budgetStart = fake.requests.length;
            yield* worker.pass(owner);
            expect(fake.requests.length - budgetStart).toBe(1);
            expect(fake.requests.at(-1)?.body.byteLength).toBeLessThanOrEqual(3800);
            expect(
              (yield* sql`SELECT 1 FROM personal_relay_alert_deliveries WHERE status = 'accepted' AND unknown_sends = 0`)
                .length,
            ).toBe(18);
            yield* mutations.registerDevice({ userId: owner, registration });
            yield* worker.pass(owner);
            expect(fake.requests.length - budgetStart).toBe(1);

            yield* reset;
            yield* mutations.registerDevice({ userId: owner, registration });
            yield* publish(1, state(iso()));
            const disabledPlanner = yield* Planner.make.pipe(
              Effect.provideService(RelayConfig.RelayConfiguration, {
                ...configuration,
                apns: null,
              }),
            );
            const disabled = yield* Worker.make.pipe(
              Effect.provideService(Planner.TargetPlanner, disabledPlanner),
            );
            const beforeDisabled = fake.requests.length;
            yield* disabled.pass(owner);
            expect(fake.requests.length).toBe(beforeDisabled);
            expect(
              (yield* sql`SELECT status FROM personal_relay_alert_deliveries`)[0]?.status,
            ).toBe("suppressed");
          }).pipe(
            Effect.provideService(Clock.Clock, clock),
            Effect.provide(Layer.mergeAll(planner, sender)),
          );
        }).pipe(Effect.scoped),
    );
  },
);
