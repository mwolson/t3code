// @effect-diagnostics nodeBuiltinImport:off -- Test-only keys and refused loopback transport exercise the real sender.
import * as NodeCrypto from "node:crypto";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import * as HttpClient from "effect/http/HttpClient";
import * as RelayConfig from "t3code-relay/Config";
import * as ApnsClient from "t3code-relay/agentActivity/ApnsClient";
import * as ApnsProviderTokens from "t3code-relay/agentActivity/ApnsProviderTokens";
import * as Sender from "../../src/apns/ApnsSender.ts";
import * as Http2Client from "../../src/apns/Http2HttpClient.ts";
import { startFakeApns, testCa } from "../apns/FakeApnsServer.ts";

// The scenario callback remains the barrier/response controller, while every result traverses TLS and U1.
export const networkSender = Effect.fnUntraced(function* (
  controller: Sender.ApnsSender["Service"],
) {
  if (!process.env.T3CODE_PERSONAL_RELAY_REAL_APNS) return controller;
  let desired: Sender.ApnsSendOutcome | undefined;
  const server = yield* Effect.acquireRelease(
    Effect.promise(() =>
      startFakeApns({
        onRequest: (_request, stream) => {
          if (desired?.classification === "retryable_ambiguous") {
            stream.close(2);
            return;
          }
          stream.respond({ ":status": desired?.status ?? 200 });
          stream.end(desired?.reason ? JSON.stringify({ reason: desired.reason }) : "");
        },
      }),
    ),
    (server) => Effect.promise(() => server.close()),
  );
  const config = yield* RelayConfig.RelayConfiguration;
  const privateKey = NodeCrypto.generateKeyPairSync("ec", { namedCurve: "prime256v1" })
    .privateKey.export({ type: "pkcs8", format: "pem" })
    .toString();
  const configuration = {
    ...config,
    apns: {
      teamId: "test",
      keyId: "test",
      bundleId: "test.bundle",
      environment: "sandbox" as const,
      privateKey: Redacted.make(privateKey),
    },
  };
  const makeSender = Effect.fnUntraced(function* (originOverride: string) {
    const http = yield* Http2Client.make({ originOverride, ca: testCa });
    const client = yield* ApnsClient.make.pipe(
      Effect.provideService(HttpClient.HttpClient, http),
      Effect.provideService(ApnsProviderTokens.ApnsProviderTokens, ApnsProviderTokens.make()),
    );
    return yield* Sender.make.pipe(
      Effect.provideService(ApnsClient.ApnsClient, client),
      Effect.provideService(RelayConfig.RelayConfiguration, configuration),
    );
  });
  const sender = yield* makeSender(server.origin);
  const refused = yield* makeSender("https://127.0.0.1:1");
  return Sender.ApnsSender.of({
    send: Effect.fnUntraced(function* (job) {
      desired = yield* controller.send(job);
      return yield* (desired.classification === "retryable_not_sent" ? refused : sender).send(job);
    }),
  });
});

export const provideSender = (sender: Sender.ApnsSender["Service"]) =>
  Effect.provideServiceEffect(Sender.ApnsSender, networkSender(sender));
