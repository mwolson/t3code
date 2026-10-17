import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpBody from "effect/http/HttpBody";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import { SignJWT, exportSPKI } from "jose";
import { RELAY_ACTIVITY_PUBLISH_TYP } from "@t3tools/shared/relayJwt";
import {
  RelayAccessTokenType,
  RelayDpopTokenExchangeGrantType,
  RelayJwtSubjectTokenType,
} from "@t3tools/contracts/relay";
import { AccountMutationError, type AccountMutations } from "../account/AccountMutations.ts";
import { ClerkJwksUnavailable, ClerkVerificationError } from "../auth/ClerkJwks.ts";
import { keys, makeHttp, OWNER, ORIGIN, type TestKeys } from "./TestFixtures.testkit.ts";

const testKeys = keys();
const DEVICE = {
  deviceId: "device",
  label: "iPhone",
  platform: "ios",
  iosMajorVersion: 26,
  bundleId: "com.mwolson.t3code.v2.dev",
  apsEnvironment: "sandbox",
  pushToken: "private-device-token",
  preferences: {
    liveActivitiesEnabled: true,
    notificationsEnabled: true,
    notifyOnApproval: true,
    notifyOnInput: true,
    notifyOnCompletion: true,
    notifyOnFailure: true,
  },
};
const exchangeBody = (subject: string, extra: Record<string, string> = {}) =>
  new URLSearchParams({
    grant_type: RelayDpopTokenExchangeGrantType,
    subject_token: subject,
    subject_token_type: RelayJwtSubjectTokenType,
    requested_token_type: RelayAccessTokenType,
    resource: ORIGIN,
    scope: "mobile:registration",
    client_id: "t3-mobile",
    ...extra,
  });
const exchange = (body: URLSearchParams, proof: string) =>
  new Request("http://internal:8080/v1/client/dpop-token", {
    method: "POST",
    headers: {
      "content-type": "application/x-www-form-urlencoded",
      dpop: proof,
      host: "untrusted.invalid",
      "x-forwarded-proto": "http",
    },
    body,
  });
async function mobileRequest(
  input: TestKeys,
  token: string,
  method: string,
  path: string,
  payload?: unknown,
  proofToken = token,
) {
  return new Request(`http://internal:8080${path}`, {
    method,
    headers: {
      authorization: `DPoP ${token}`,
      dpop: await input.proof(method, path, proofToken),
      "content-type": "application/json",
      host: "attacker.invalid",
      "x-forwarded-proto": "http",
    },
    ...(payload === undefined ? {} : { body: JSON.stringify(payload) }),
  });
}
let publishNonce = 0;
async function publishProof(input: TestKeys, extra: Record<string, unknown> = {}) {
  return new SignJWT({
    iss: "t3-env:env",
    sub: "env",
    aud: ORIGIN,
    environmentId: "env",
    threadId: "thread",
    state: null,
    jti: `publish-${++publishNonce}`,
    iat: Math.floor(DateTime.nowUnsafe().epochMilliseconds / 1000),
    exp: Math.floor(DateTime.nowUnsafe().epochMilliseconds / 1000) + 300,
    t3_publish_seq: 1,
    t3_transition: null,
    t3_transition_eligible_at: null,
    ...extra,
  })
    .setProtectedHeader({ alg: "EdDSA", typ: RELAY_ACTIVITY_PUBLISH_TYP })
    .sign(input.mint.privateKey);
}
const publishRequest = (proof: string, environment = "env") =>
  new Request(`http://internal/v1/environments/${environment}/threads/thread/agent-activity`, {
    method: "POST",
    headers: {
      authorization: "Bearer t3env_private-credential",
      "content-type": "application/json",
    },
    body: JSON.stringify({ proof, state: null }),
  });

describe("personal relay HTTP", () => {
  it.live(
    "rejects oversized streamed bodies without content-length before token verification",
    () =>
      Effect.gen(function* () {
        let verified = false;
        const http = yield* Effect.acquireRelease(
          Effect.promise(async () =>
            makeHttp(await testKeys, {
              clerk: {
                verify: () => {
                  verified = true;
                  return Effect.fail(
                    new ClerkVerificationError({
                      category: "invalid-token",
                      cause: new Error("test verifier"),
                    }),
                  );
                },
              },
            }),
          ),
          (http) => Effect.promise(() => http.dispose()),
        );
        const services = yield* Layer.build(
          HttpRouter.serve(http.appLayer, { disableLogger: true }).pipe(
            Layer.provideMerge(NodeHttpServer.layerTest),
          ),
        );
        const client = Context.get(services, HttpClient.HttpClient);
        const encoded = new TextEncoder().encode(exchangeBody("x".repeat(70_000)).toString());
        const chunks = Array.from({ length: Math.ceil(encoded.length / 8192) }, (_, index) =>
          encoded.slice(index * 8192, (index + 1) * 8192),
        );
        const result = yield* client
          .post("/v1/client/dpop-token", {
            body: HttpBody.stream(Stream.fromIterable(chunks), "application/x-www-form-urlencoded"),
            headers: { dpop: "unused" },
          })
          .pipe(Effect.result);
        if (result._tag === "Success") expect([400, 413]).toContain(result.success.status);
        else expect(result.failure.reason._tag).toBe("TransportError");
        expect(verified).toBe(false);
        const health = yield* client.get("/health");
        expect(health.status).toBe(200);
      }).pipe(Effect.scoped),
  );
  it("serves liveness without DB access, readiness with SELECT 1, and no discovery or OAuth metadata", async () => {
    const http = await makeHttp(await testKeys);
    try {
      expect((await http.handler(new Request(`${ORIGIN}/livez`))).status).toBe(200);
      expect(http.calls.queries).toBe(0);
      expect((await http.handler(new Request(`${ORIGIN}/health`))).status).toBe(200);
      expect(http.calls.queries).toBe(1);
      for (const path of [
        "/v1/environments",
        "/.well-known/oauth-authorization-server",
        "/openapi.json",
        "/docs",
        "/v1/client/devices",
      ]) {
        expect((await http.handler(new Request(`${ORIGIN}${path}`))).status).toBe(404);
      }
    } finally {
      await http.dispose();
    }
  });
  it("exchanges a Clerk JWT and a bootstrap proof for a 30-minute DPoP token behind an untrusted Host", async () => {
    const input = await testKeys;
    const http = await makeHttp(input);
    try {
      const request = exchange(
        exchangeBody(await input.clerkToken()),
        await input.proof("POST", "/v1/client/dpop-token"),
      );
      const replay = request.clone();
      const response = await http.handler(request);
      expect(response.status).toBe(200);
      expect(response.headers.get("cache-control")).toBe("no-store");
      const body = await response.json();
      expect(body).toMatchObject({
        token_type: "DPoP",
        expires_in: 1800,
        scope: "mobile:registration",
        issued_token_type: RelayAccessTokenType,
      });
      const snapshot = await http.handler(
        await mobileRequest(input, body.access_token, "GET", "/v1/mobile/agent-activity"),
      );
      expect(snapshot.status).toBe(200);
      expect(await snapshot.json()).toEqual({ aggregate: null });
      expect((await http.handler(replay)).status).toBe(401);
    } finally {
      await http.dispose();
    }
  });
  it("rejects a token exchange for another resource before consuming a proof", async () => {
    const input = await testKeys;
    const http = await makeHttp(input);
    try {
      const response = await http.handler(
        exchange(
          exchangeBody(await input.clerkToken(), { resource: "https://other.test" }),
          await input.proof("POST", "/v1/client/dpop-token"),
        ),
      );
      expect(response.status).toBe(401);
      expect(http.calls.proofs).toBe(0);
    } finally {
      await http.dispose();
    }
  });
  it.each(["keys-unavailable", "invalid-token"])(
    "maps %s to 500 or 401 for both exchange and bearer device listing",
    async (kind) => {
      const input = await testKeys;
      const http = await makeHttp(input, {
        clerk: {
          verify: () =>
            kind === "keys-unavailable"
              ? Effect.fail(
                  new ClerkJwksUnavailable({
                    category: "jwks-unavailable",
                    cause: "private-error",
                  }),
                )
              : Effect.fail(
                  new ClerkVerificationError({ category: "invalid-token", cause: "private-error" }),
                ),
        },
      });
      try {
        const responses = [
          await http.handler(exchange(exchangeBody("secret-subject-token"), "secret-proof")),
          await http.handler(
            new Request(`${ORIGIN}/v2/client/devices`, {
              headers: { authorization: "Bearer secret-bearer" },
            }),
          ),
        ];
        for (const response of responses) {
          expect(response.status).toBe(kind === "keys-unavailable" ? 500 : 401);
          expect(await response.text()).not.toMatch(/private-error|secret-/);
        }
      } finally {
        await http.dispose();
      }
    },
  );
  it("lists devices with Clerk bearer auth", async () => {
    const input = await testKeys;
    const http = await makeHttp(input);
    try {
      const response = await http.handler(
        new Request(`${ORIGIN}/v2/client/devices`, {
          headers: { authorization: `Bearer ${await input.clerkToken()}` },
        }),
      );
      expect(response.status).toBe(200);
      expect(await response.json()).toEqual({ devices: [] });
      expect(http.calls.reads).toBe(1);
    } finally {
      await http.dispose();
    }
  });
  it("registers both token kinds and unregisters an escaped device id, waking only after each mutation", async () => {
    const input = await testKeys;
    const calls: string[] = [];
    let http: Awaited<ReturnType<typeof makeHttp>>;
    http = await makeHttp(input, {
      mutations: {
        registerDevice: (value) =>
          Effect.sync(() => {
            expect(http.calls.wakes).toBe(0);
            expect(value.userId).toBe(OWNER);
            calls.push(value.registration.deviceId);
          }),
        registerLiveActivity: () =>
          Effect.sync(() => {
            expect(http.calls.wakes).toBe(1);
            calls.push("activity");
          }),
        unregisterDevice: (value) =>
          Effect.sync(() => {
            expect(http.calls.wakes).toBe(2);
            calls.push(value.deviceId);
          }),
      },
    });
    try {
      const token = await http.relayToken();
      expect(
        (
          await http.handler(
            await mobileRequest(input, token, "POST", "/v1/mobile/devices", DEVICE),
          )
        ).status,
      ).toBe(200);
      expect(
        (
          await http.handler(
            await mobileRequest(input, token, "POST", "/v1/mobile/live-activities", {
              deviceId: "device",
              activityPushToken: "activity-token",
            }),
          )
        ).status,
      ).toBe(200);
      expect(
        (
          await http.handler(
            await mobileRequest(
              input,
              token,
              "DELETE",
              "/v1/mobile/devices/device%2Fwith%20space%25",
            ),
          )
        ).status,
      ).toBe(200);
      expect(calls).toEqual(["device", "activity", "device/with space%"]);
      expect(http.calls.wakes).toBe(3);
    } finally {
      await http.dispose();
    }
  });
  it("rejects a registration outside the bundle allowlist", async () => {
    const input = await testKeys;
    const http = await makeHttp(input);
    try {
      const token = await http.relayToken();
      expect(
        (
          await http.handler(
            await mobileRequest(input, token, "POST", "/v1/mobile/devices", {
              ...DEVICE,
              bundleId: "other.bundle",
            }),
          )
        ).status,
      ).toBe(401);
      expect(http.calls.wakes).toBe(0);
    } finally {
      await http.dispose();
    }
  });
  it("rejects ath mismatch, missing scope, invalid htu and Bearer use of a DPoP token", async () => {
    const input = await testKeys;
    const http = await makeHttp(input);
    try {
      const token = await http.relayToken();
      const noScope = await http.relayToken(["environment:status"]);
      const badHash = await mobileRequest(
        input,
        token,
        "GET",
        "/v1/mobile/agent-activity",
        undefined,
        "different-token",
      );
      const badUrl = await mobileRequest(input, token, "GET", "/v1/mobile/agent-activity");
      badUrl.headers.set("dpop", await input.proof("GET", "/wrong-path", token));
      const bearer = await mobileRequest(input, token, "GET", "/v1/mobile/agent-activity");
      bearer.headers.set("authorization", `Bearer ${token}`);
      for (const request of [
        badHash,
        badUrl,
        bearer,
        await mobileRequest(input, noScope, "GET", "/v1/mobile/agent-activity"),
      ]) {
        const response = await http.handler(request);
        expect(response.status).toBe(401);
        expect(response.headers.get("www-authenticate")).toBe("DPoP");
      }
      expect(http.calls.reads).toBe(0);
    } finally {
      await http.dispose();
    }
  });
  it("does not wake or acknowledge a failed account mutation", async () => {
    const input = await testKeys;
    const http = await makeHttp(input);
    try {
      const token = await http.relayToken();
      for (const request of [
        await mobileRequest(input, token, "POST", "/v1/mobile/devices", DEVICE),
        await mobileRequest(input, token, "POST", "/v1/mobile/live-activities", {
          deviceId: "device",
          activityPushToken: "private-activity-token",
        }),
        await mobileRequest(input, token, "DELETE", "/v1/mobile/devices/device"),
      ]) {
        const response = await http.handler(request);
        expect(response.status).toBe(500);
        expect(await response.text()).not.toContain("private-");
      }
      expect(http.calls.wakes).toBe(0);
    } finally {
      await http.dispose();
    }
  });
  it("verifies publish proofs before decoding and passes exact signed private claims through the mutation tag", async () => {
    const input = await testKeys;
    const published: Array<Parameters<AccountMutations["Service"]["publish"]>[0]> = [];
    const environmentPublicKey = await exportSPKI(input.mint.publicKey);
    const http = await makeHttp(input, {
      auth: () =>
        Effect.succeedSome({ credentialId: "id", environmentId: "env", environmentPublicKey }),
      mutations: {
        publish: (value) =>
          Effect.sync(() => {
            expect(http.calls.wakes).toBe(0);
            published.push(value);
          }),
      },
    });
    try {
      const proof = await publishProof(input);
      const response = await http.handler(publishRequest(proof));
      expect(response.status).toBe(200);
      expect(await response.json()).toEqual({ ok: true, deliveries: [] });
      expect(published).toHaveLength(1);
      expect(published[0]).toMatchObject({
        userId: OWNER,
        environmentId: "env",
        threadId: "thread",
        environmentPublicKey,
        request: { proof, state: null },
        claims: { t3_publish_seq: 1, t3_transition: null, t3_transition_eligible_at: null },
      });
      expect(http.calls.wakes).toBe(1);
      const [header, payload, signature] = proof.split(".");
      const tampered = `${header}.${Buffer.from(JSON.stringify({ ...JSON.parse(Buffer.from(payload!, "base64url").toString()), t3_publish_seq: 2 })).toString("base64url")}.${signature}`;
      expect((await http.handler(publishRequest(tampered))).status).toBe(401);
      expect(
        (
          await http.handler(
            publishRequest(await publishProof(input, { t3_publish_seq: "malformed" })),
          )
        ).status,
      ).toBe(401);
      expect((await http.handler(publishRequest(proof))).status).toBe(401);
      for (const extra of [{ t3_transition_eligible_at: undefined }, { exp: 1 }]) {
        expect((await http.handler(publishRequest(await publishProof(input, extra)))).status).toBe(
          401,
        );
      }
      expect(published).toHaveLength(1);
      expect(http.calls.wakes).toBe(1);
    } finally {
      await http.dispose();
    }
  });
  it.each(["stale-sequence", "revoked-environment"] as const)(
    "maps %s and preserves a failed publish",
    async (reason) => {
      const input = await testKeys;
      const http = await makeHttp(input, {
        signatures: { verify: () => Effect.void },
        mutations: {
          publish: () =>
            Effect.fail(
              new AccountMutationError({
                operation: "publish",
                reason,
                cause: "private-environment-credential",
              }),
            ),
        },
      });
      try {
        const response = await http.handler(publishRequest(await publishProof(input)));
        expect(response.status).toBe(reason === "stale-sequence" ? 500 : 401);
        expect(await response.text()).not.toContain("private-environment-credential");
        expect(http.calls.wakes).toBe(0);
      } finally {
        await http.dispose();
      }
    },
  );
  it("rejects inactive or mismatched environments before proof verification", async () => {
    const input = await testKeys;
    let verifications = 0;
    const http = await makeHttp(input, {
      auth: () => Effect.succeedNone,
      signatures: {
        verify: () =>
          Effect.sync(() => {
            verifications++;
          }),
      },
    });
    const mismatch = await makeHttp(input, {
      signatures: {
        verify: () =>
          Effect.sync(() => {
            verifications++;
          }),
      },
    });
    try {
      expect((await http.handler(publishRequest("not-a-jwt"))).status).toBe(401);
      expect((await mismatch.handler(publishRequest("not-a-jwt", "another-env"))).status).toBe(401);
      expect(verifications).toBe(0);
    } finally {
      await http.dispose();
      await mismatch.dispose();
    }
  });
  it("keeps failed-request logs and responses free of auth material and device tokens", async () => {
    const input = await testKeys;
    const logs: Array<unknown> = [];
    const sentinels = [
      "bearer-sentinel",
      "t3env_credential-sentinel",
      "dpop-proof-sentinel",
      "provider-jwt-sentinel",
      "private-key-sentinel",
      "device-token-sentinel",
    ];
    const http = await makeHttp(input, {
      logs,
      mutations: {
        registerDevice: () =>
          Effect.fail(
            new AccountMutationError({
              operation: "register-device",
              reason: "persistence",
              cause: sentinels.join(" "),
            }),
          ),
      },
    });
    try {
      const token = await http.relayToken();
      const responses = [
        await http.handler(
          await mobileRequest(input, token, "POST", "/v1/mobile/devices", {
            ...DEVICE,
            pushToken: sentinels[5],
          }),
        ),
        await http.handler(
          new Request(`${ORIGIN}/v2/client/devices`, {
            headers: { authorization: `Bearer ${sentinels[0]}` },
          }),
        ),
      ];
      const text =
        JSON.stringify(logs) +
        (await Promise.all(responses.map((response) => response.text()))).join(" ");
      for (const sentinel of [...sentinels, token]) expect(text).not.toContain(sentinel);
      expect(logs.length).toBeGreaterThan(0);
    } finally {
      await http.dispose();
    }
  });
  it("returns a bounded database failure for readiness while liveness remains available", async () => {
    const http = await makeHttp(await testKeys, { query: Effect.never });
    try {
      const response = await http.handler(new Request(`${ORIGIN}/health`));
      expect(response.status).toBe(500);
      expect(await response.json()).toMatchObject({ reason: "database_unavailable" });
      expect((await http.handler(new Request(`${ORIGIN}/livez`))).status).toBe(200);
    } finally {
      await http.dispose();
    }
  });
});
