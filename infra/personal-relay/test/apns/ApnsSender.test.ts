import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as References from "effect/References";
import * as Redacted from "effect/Redacted";
import * as HttpClient from "effect/http/HttpClient";
import * as RelayConfig from "t3code-relay/Config";
import * as ApnsClient from "t3code-relay/agentActivity/ApnsClient";
import * as ApnsProviderTokens from "t3code-relay/agentActivity/ApnsProviderTokens";
import * as Sender from "../../src/apns/ApnsSender.ts";
import * as Http2Client from "../../src/apns/Http2HttpClient.ts";
import { APNS_PAYLOAD_BUDGET } from "../../src/apns/PayloadBudget.ts";
import { APNS_ID, startFakeApns, testCa } from "./FakeApnsServer.ts";
import { makeJob } from "./Jobs.ts";

const configuration = RelayConfig.RelayConfiguration.of({
  apns: {
    teamId: "test-team",
    keyId: "test-key",
    privateKey: Redacted.make("private-test-key"),
    bundleId: "com.example.fallback",
    environment: "production",
  },
  relayIssuer: "https://relay.invalid",
  clerkSecretKey: Redacted.make(""),
  clerkPublishableKey: "",
  clerkJwtAudience: "test",
  apnsDeliveryJobSigningSecret: Redacted.make("test"),
  cloudMintPrivateKey: Redacted.make(""),
  cloudMintPublicKey: "",
  managedEndpointBaseDomain: undefined,
  managedEndpointNamespace: undefined,
});

function withSender<A, E, R>(
  run: (sender: Sender.ApnsSender["Service"]) => Effect.Effect<A, E, R>,
  options: {
    readonly origin?: string;
    readonly result?: ApnsClient.ApnsDeliveryResult;
    readonly config?: RelayConfig.RelayConfiguration["Service"];
    readonly onSend?: (input: { readonly credentials: RelayConfig.ApnsCredentials }) => void;
  } = {},
) {
  return Effect.scoped(
    Effect.gen(function* () {
      const http = yield* Http2Client.make({
        originOverride: options.origin ?? "https://127.0.0.1:1",
        ca: testCa,
      });
      const client = yield* ApnsClient.make.pipe(
        Effect.provideService(HttpClient.HttpClient, http),
        Effect.provideService(ApnsProviderTokens.ApnsProviderTokens, {
          getJwt: () => Effect.succeed("test-provider-jwt"),
        }),
      );
      const send = (input: { readonly credentials: RelayConfig.ApnsCredentials }) => {
        options.onSend?.(input);
        return Effect.succeed(options.result ?? { ok: true, status: 200, apnsId: APNS_ID });
      };
      const service =
        options.result !== undefined
          ? { ...client, sendLiveActivityRequest: send, sendPushNotificationRequest: send }
          : client;
      const sender = yield* Sender.make.pipe(
        Effect.provideService(ApnsClient.ApnsClient, service),
        Effect.provideService(RelayConfig.RelayConfiguration, options.config ?? configuration),
      );
      return yield* run(sender);
    }),
  );
}

describe("ApnsSender", () => {
  it.live.each([
    [200, undefined, "delivered"],
    [410, "Unregistered", "token_dead"],
    [503, "ServiceUnavailable", "retryable_answered"],
    [403, "ExpiredProviderToken", "retryable_answered"],
    [400, "BadTopic", "rejected"],
  ] as const)("classifies %s %s as %s", ([status, reason, classification]) =>
    Effect.gen(function* () {
      const outcome = yield* withSender((sender) => sender.send(makeJob()), {
        result: {
          ok: status === 200,
          status,
          ...(reason === undefined ? {} : { reason }),
          apnsId: APNS_ID,
        },
      });
      expect(outcome).toEqual({
        classification,
        status,
        reason: reason ?? null,
        apnsId: APNS_ID,
      });
    }),
  );

  it.live("logs bounded 403 and payload budget bug fields without secrets", () =>
    Effect.gen(function* () {
      const logs: Array<unknown> = [];
      const logger = Logger.make(({ message, fiber, logLevel }) =>
        logs.push({
          message,
          logLevel,
          annotations: fiber.getRef(References.CurrentLogAnnotations),
        }),
      );
      yield* withSender((sender) => sender.send(makeJob()), {
        result: { ok: false, status: 403, reason: "InvalidProviderToken", apnsId: APNS_ID },
      }).pipe(Effect.provide(Logger.layer([logger], { mergeWithExisting: false })));
      yield* withSender((sender) => sender.send(makeJob()), {
        result: { ok: false, status: 413, reason: "PayloadTooLarge", apnsId: APNS_ID },
      }).pipe(Effect.provide(Logger.layer([logger], { mergeWithExisting: false })));
      expect(logs).toHaveLength(2);
      expect(logs[0]).toMatchObject({
        annotations: {
          "apns-id": APNS_ID,
          reason: "InvalidProviderToken",
          kind: "live_activity_update",
          keyId: "test-key",
          teamId: "test-team",
          channel: "live_activity",
        },
      });
      expect(logs[1]).toMatchObject({ annotations: { reason: "payload_budget_violation" } });
      const serialized = JSON.stringify(logs);
      for (const forbidden of [
        "private-test-key",
        "test-provider-jwt",
        makeJob().payload.target.token,
        "Thread title",
      ])
        expect(serialized).not.toContain(forbidden);
    }),
  );

  it.live("never logs an untrusted APNs body or id", () =>
    Effect.gen(function* () {
      const outcome = yield* withSender((sender) => sender.send(makeJob()), {
        result: { ok: false, status: 400, reason: "Bearer secret-value", apnsId: "secret-value" },
      });
      expect(outcome).toMatchObject({ reason: "unknown_apns_reason", apnsId: null });
    }),
  );

  it.live.each([
    ["live_activity_update", "liveactivity", "com.example.test.push-type.liveactivity", "10"],
    ["push_notification", "alert", "com.example.test", "10"],
  ] as const)("sends %s with registered device routing", ([kind, pushType, topic, priority]) =>
    Effect.gen(function* () {
      const fake = yield* Effect.promise(() => startFakeApns());
      try {
        const job = makeJob({ kind });
        const outcome = yield* withSender((sender) => sender.send(job), { origin: fake.origin });
        expect(outcome.classification).toBe("delivered");
        expect(fake.requests).toHaveLength(1);
        expect(fake.requests[0]?.headers).toMatchObject({
          ":method": "POST",
          ":path": `/3/device/${job.payload.target.token}`,
          "apns-push-type": pushType,
          "apns-topic": topic,
          "apns-priority": priority,
          authorization: "bearer test-provider-jwt",
        });
      } finally {
        yield* Effect.promise(() => fake.close());
      }
    }),
  );

  it.live("uses priority 5 for a quiet update", () =>
    Effect.gen(function* () {
      const fake = yield* Effect.promise(() => startFakeApns());
      try {
        const job = makeJob();
        yield* withSender(
          (sender) =>
            sender.send({ ...job, payload: { ...job.payload, alert: null }, deliveries: [] }),
          { origin: fake.origin },
        );
        expect(fake.requests[0]?.headers["apns-priority"]).toBe("5");
      } finally {
        yield* Effect.promise(() => fake.close());
      }
    }),
  );

  it.live.each(["503", "reset-after-send", "close-before-end"] as const)(
    "classifies real HTTP/2 %s without retrying",
    (mode) =>
      Effect.gen(function* () {
        const fake = yield* Effect.promise(() => startFakeApns({ mode }));
        try {
          const outcome = yield* withSender((sender) => sender.send(makeJob()), {
            origin: fake.origin,
          });
          expect(outcome.classification).toBe(
            mode === "503"
              ? "retryable_answered"
              : mode === "close-before-end"
                ? "delivered"
                : "retryable_ambiguous",
          );
          expect(fake.requests).toHaveLength(1);
        } finally {
          yield* Effect.promise(() => fake.close());
        }
      }),
  );

  it.live("summarizes three alerts exactly once", () =>
    Effect.gen(function* () {
      const fake = yield* Effect.promise(() => startFakeApns());
      try {
        const job = { ...makeJob({ count: 3 }), firstAlertTitle: "First, complete title" };
        const outcome = yield* withSender((sender) => sender.send(job), { origin: fake.origin });
        expect(outcome.classification).toBe("delivered");
        expect(JSON.parse(fake.requests[0]!.body.toString()).aps.alert.body).toBe(
          "First, complete title and 2 more",
        );
      } finally {
        yield* Effect.promise(() => fake.close());
      }
    }),
  );

  it.live("bounds many carried alerts including UTF-8 and escaping without losing identities", () =>
    Effect.gen(function* () {
      const count = 80;
      const fake = yield* Effect.promise(() => startFakeApns());
      try {
        const job = makeJob({ count, text: '\\"\n漢😀'.repeat(24) });
        const original = JSON.stringify(job);
        const outcome = yield* withSender((sender) => sender.send(job), { origin: fake.origin });
        expect(outcome.classification).toBe("delivered");
        expect(fake.requests).toHaveLength(1);
        expect(fake.requests[0]?.body.byteLength).toBeLessThanOrEqual(APNS_PAYLOAD_BUDGET);
        expect(JSON.stringify(job)).toBe(original);
        const payload = JSON.parse(fake.requests[0]!.body.toString());
        expect(payload.aps.alert.body).toMatch(
          new RegExp(`${count - 1} more|${count} agent updates`),
        );
        expect(JSON.parse(payload.aps["content-state"].props).activeCount).toBe(20);
      } finally {
        yield* Effect.promise(() => fake.close());
      }
    }),
  );

  it.live("budgets quiet card content and standard notifications too", () =>
    Effect.gen(function* () {
      const fake = yield* Effect.promise(() => startFakeApns());
      try {
        const card = makeJob({ text: '\\"漢😀'.repeat(120) });
        const notification = makeJob({ kind: "push_notification", text: '\\"漢😀'.repeat(120) });
        yield* withSender(
          (sender) =>
            Effect.all([
              sender.send({ ...card, deliveries: [], payload: { ...card.payload, alert: null } }),
              sender.send(notification),
            ]),
          { origin: fake.origin },
        );
        expect(fake.requests).toHaveLength(2);
        for (const request of fake.requests)
          expect(request.body.byteLength).toBeLessThanOrEqual(APNS_PAYLOAD_BUDGET);
      } finally {
        yield* Effect.promise(() => fake.close());
      }
    }),
  );

  it.live("returns not sent when the HTTP/2 connection is refused", () =>
    Effect.gen(function* () {
      const fake = yield* Effect.promise(() => startFakeApns());
      yield* Effect.promise(() => fake.close());
      const outcome = yield* withSender((sender) => sender.send(makeJob()), {
        origin: fake.origin,
      });
      expect(outcome.classification).toBe("retryable_not_sent");
    }),
  );

  it.live("rejects impossible routing fields before the network", () =>
    Effect.gen(function* () {
      const fake = yield* Effect.promise(() => startFakeApns());
      try {
        const job = makeJob({ kind: "push_notification" });
        const outcome = yield* withSender(
          (sender) =>
            sender.send({
              ...job,
              payload: {
                ...job.payload,
                notification: { ...job.payload.notification!, threadId: "x".repeat(6000) },
              },
            }),
          { origin: fake.origin },
        );
        expect(outcome).toMatchObject({
          classification: "rejected",
          reason: "payload_budget_violation",
        });
        expect(fake.requests).toEqual([]);
      } finally {
        yield* Effect.promise(() => fake.close());
      }
    }),
  );

  it.live("composes the sender layer and skips the network when APNs is disabled", () =>
    Effect.gen(function* () {
      const apns = ApnsClient.layer.pipe(
        Layer.provide(Http2Client.layer({ originOverride: "https://127.0.0.1:1" })),
        Layer.provide(
          Layer.succeed(ApnsProviderTokens.ApnsProviderTokens, {
            getJwt: () => Effect.die("Must not sign when disabled"),
          }),
        ),
      );
      const layer = Sender.layer.pipe(
        Layer.provide(apns),
        Layer.provide(RelayConfig.layer({ ...configuration, apns: null })),
      );
      const outcome = yield* Effect.gen(function* () {
        return yield* (yield* Sender.ApnsSender).send(makeJob());
      }).pipe(Effect.provide(layer));
      expect(outcome).toMatchObject({ classification: "rejected", reason: "apns_disabled" });
    }),
  );

  it.live("selects each device's environment and falls back only for missing routing", () =>
    Effect.gen(function* () {
      const credentials: Array<RelayConfig.ApnsCredentials> = [];
      const job = makeJob();
      const result = { ok: true, status: 200, apnsId: APNS_ID };
      yield* withSender((sender) => sender.send(job), {
        result,
        onSend: (input) => credentials.push(input.credentials),
      });
      yield* withSender(
        (sender) =>
          sender.send({
            ...job,
            payload: {
              ...job.payload,
              target: { ...job.payload.target, bundleId: null, apsEnvironment: null },
            },
          }),
        { result, onSend: (input) => credentials.push(input.credentials) },
      );
      expect(credentials.map(({ bundleId, environment }) => ({ bundleId, environment }))).toEqual([
        { bundleId: "com.example.test", environment: "sandbox" },
        { bundleId: "com.example.fallback", environment: "production" },
      ]);
    }),
  );
});
