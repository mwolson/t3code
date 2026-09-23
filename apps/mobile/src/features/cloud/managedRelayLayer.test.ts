/// <reference types="node" />

import * as NodeCrypto from "node:crypto";

import { vi } from "vite-plus/test";
import { assert, describe, it } from "@effect/vitest";
import { ManagedRelay } from "@t3tools/client-runtime/relay";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { FetchHttpClient } from "effect/http";
import { EnvironmentId } from "@t3tools/contracts";
import { RelayEnvironmentStatusScope } from "@t3tools/contracts/relay";
import { resolvePersonalBackgroundRoute } from "./personalBackgroundRoute";

import * as Dpop from "./dpop";
import * as ManagedRelayLayer from "./managedRelayLayer";

vi.mock("expo-constants", () => ({
  default: { expoConfig: { extra: {} } },
}));

vi.mock("expo-crypto", () => ({
  CryptoDigestAlgorithm: { SHA256: "SHA-256" },
  getRandomBytes: (byteCount: number) => new Uint8Array(NodeCrypto.randomBytes(byteCount)),
  getRandomBytesAsync: (byteCount: number) =>
    Promise.resolve(new Uint8Array(NodeCrypto.randomBytes(byteCount))),
  digest: (algorithm: string, data: Uint8Array) =>
    Promise.resolve(new Uint8Array(NodeCrypto.createHash(algorithm).update(data).digest()).buffer),
}));

const secureStore = new Map<string, string>();
let failNextRead = false;
vi.mock("expo-secure-store", () => ({
  getItemAsync: (key: string) => {
    if (failNextRead) {
      failNextRead = false;
      return Promise.reject(new Error("keychain locked"));
    }
    return Promise.resolve(secureStore.get(key) ?? null);
  },
  setItemAsync: (key: string, value: string) => {
    secureStore.set(key, value);
    return Promise.resolve();
  },
  deleteItemAsync: (key: string) => {
    secureStore.delete(key);
    return Promise.resolve();
  },
}));

describe("managed relay DPoP signer", () => {
  it.effect("loads the proof key again after a failed read", () =>
    Effect.gen(function* () {
      const signer = yield* ManagedRelay.ManagedRelayDpopSigner;
      failNextRead = true;
      const failed = yield* Effect.flip(signer.thumbprint);
      assert.equal(failed._tag, "ManagedRelayDpopKeyLoadError");

      const thumbprint = yield* signer.thumbprint;
      assert.equal(yield* signer.thumbprint, thumbprint);
    }).pipe(
      Effect.provide(
        ManagedRelayLayer.layer({
          relayUrl: "https://relay.example.test",
          background: { kind: "default", url: null },
        }).pipe(Layer.provide(Layer.mergeAll(FetchHttpClient.layer, Dpop.layer))),
      ),
    ),
  );
});

describe("mobile background relay", () => {
  it.effect(
    "refuses background calls without a request when a personal build has no private relay",
    () => {
      const primaryUrl = "https://primary.example.test";
      const route = resolvePersonalBackgroundRoute({
        iosPushRelay: true,
        relay: { url: primaryUrl, backgroundUrl: primaryUrl },
      });
      const requests: string[] = [];
      const fetchFn: typeof fetch = async (input, init) => {
        const url = String(input);
        requests.push(url);
        if (url.endsWith("/dpop-token")) {
          const body = new URLSearchParams(new TextDecoder().decode(init?.body as Uint8Array));
          return Response.json({
            access_token: "dpop-token",
            issued_token_type: "urn:ietf:params:oauth:token-type:access_token",
            token_type: "DPoP",
            expires_in: 1800,
            scope: body.get("scope"),
          });
        }
        return Response.json({
          environmentId: "env-1",
          endpoint: {
            httpBaseUrl: "https://desktop.example.test/",
            wsBaseUrl: "wss://desktop.example.test/ws",
            providerKind: "manual",
          },
          status: "offline",
          checkedAt: "2026-10-17T00:00:00.000Z",
        });
      };
      return Effect.gen(function* () {
        const client = yield* ManagedRelay.ManagedRelayClient;
        const clerkToken = "clerk-token";
        const operations: ReadonlyArray<
          Effect.Effect<unknown, ManagedRelay.ManagedRelayClientError>
        > = [
          client.listDevices({ clerkToken }),
          client.unregisterDevice({ clerkToken, deviceId: "device-1" }),
          client.getAgentActivitySnapshot({ clerkToken }),
        ];
        for (const operation of operations) {
          const error = yield* Effect.flip(Effect.asVoid(operation));
          assert.equal(error._tag, "ManagedRelayBackgroundRelayDisabledError");
        }
        assert.deepEqual(requests, []);
        yield* client.getEnvironmentStatus({
          clerkToken,
          environmentId: EnvironmentId.make("env-1"),
          scopes: [RelayEnvironmentStatusScope],
        });
        assert.isTrue(requests.length > 0);
        assert.isTrue(requests.every((url) => new URL(url).origin === primaryUrl));
      }).pipe(
        Effect.provide(
          ManagedRelayLayer.layer({ relayUrl: primaryUrl, background: route }).pipe(
            Layer.provide(Layer.mergeAll(FetchHttpClient.layer, Dpop.layer)),
            Layer.provide(Layer.succeed(FetchHttpClient.Fetch, fetchFn)),
          ),
        ),
      );
    },
  );
});
