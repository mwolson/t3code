// @effect-diagnostics nodeBuiltinImport:off globalFetch:off globalTimers:off globalConsole:off -- This harness drives real local processes and network boundaries.
import * as NodeHttp from "node:http";
import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeNet from "node:net";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import {
  generateKeyPair,
  exportJWK,
  exportPKCS8,
  exportSPKI,
  calculateJwkThumbprint,
  SignJWT,
} from "jose";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type {
  RelayAgentActivityState,
  RelayDeviceRegistrationRequest,
} from "@t3tools/contracts/relay";
import { computeDpopAccessTokenHash } from "@t3tools/shared/dpop";
import { RELAY_ACTIVITY_PUBLISH_TYP } from "@t3tools/shared/relayJwt";
import * as ManagedRelay from "../../../packages/client-runtime/src/relay/managedRelay.ts";
import * as RpcHttp from "../../../packages/client-runtime/src/rpc/http.ts";
import { ServerSecretStore } from "../../../apps/server/src/auth/ServerSecretStore.ts";
import { ServerEnvironmentIdentity } from "../../../apps/server/src/environment/ServerEnvironment.ts";
import * as EnrollmentProof from "../../../apps/server/src/relay/EnrollmentProof.ts";
import * as PublishSequence from "../../../apps/server/src/relay/PublishSequence.ts";
import * as PgLayer from "../src/db/PgLayer.ts";
import { RelayDb } from "t3code-relay/db";
import { startFakeApns, testCa } from "../test/apns/FakeApnsServer.ts";

const tree = NodePath.resolve(import.meta.dirname, "../../..");
const scratchRoot = NodePath.resolve(tree, "../../tmp/relay/integration/process");
const origin = "https://relay-process.test";
const owner = "relay-process-owner";
const environmentId = EnvironmentId.make("relay-process-environment");
const bundle = "com.mwolson.t3code.v2.dev";
const databaseUrl = process.env.T3CODE_PERSONAL_RELAY_TEST_DATABASE_URL;
if (!databaseUrl) throw new Error("Run e2e-local.ts through scripts/test-postgres.sh");
const database = new URL(databaseUrl);
NodeAssert.equal(database.hostname, "127.0.0.1");
NodeAssert.equal(database.pathname, "/t3relay");
const children = new Set<NodeChildProcess.ChildProcess>();
const logs: string[] = [];
const now = () => DateTime.nowUnsafe().epochMilliseconds;
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

async function run() {
  await NodeFSP.mkdir(scratchRoot, { recursive: true });
  const scratch = await NodeFSP.mkdtemp(NodePath.join(scratchRoot, "run-"));
  const clerk = await generateKeyPair("RS256", { extractable: true });
  const mint = await generateKeyPair("EdDSA", { extractable: true });
  const publishing = await generateKeyPair("EdDSA", { extractable: true });
  const apns = await generateKeyPair("ES256", { extractable: true });
  const dpop = await generateKeyPair("ES256", { extractable: true });
  const publicKey = await exportSPKI(publishing.publicKey);
  const signingKey = await exportPKCS8(publishing.privateKey);
  const dpopJwk = await exportJWK(dpop.publicKey);
  const thumbprint = await calculateJwkThumbprint(dpopJwk);
  await NodeFSP.writeFile(
    NodePath.join(scratch, "jwks.json"),
    JSON.stringify({
      keys: [
        { ...(await exportJWK(clerk.publicKey)), kid: "process-test", alg: "RS256", use: "sig" },
      ],
    }),
  );
  const secrets = new Map<string, Uint8Array>([
    [
      "cloud-link-ed25519-key-pair",
      new TextEncoder().encode(JSON.stringify({ publicKey, privateKey: signingKey })),
    ],
  ]);
  const store = ServerSecretStore.of({
    get: Effect.fn("ServerSecretStore.get")((name) =>
      Effect.sync(() => Option.fromNullishOr(secrets.get(name))),
    ),
    set: Effect.fn("ServerSecretStore.set")((name, value) =>
      Effect.sync(() => {
        secrets.set(name, value);
      }),
    ),
    create: Effect.fn("ServerSecretStore.create")((name, value) =>
      Effect.sync(() => {
        NodeAssert.equal(secrets.has(name), false);
        secrets.set(name, value);
      }),
    ),
    remove: Effect.fn("ServerSecretStore.remove")((name) =>
      Effect.sync(() => {
        secrets.delete(name);
      }),
    ),
    getOrCreateRandom: Effect.fn("ServerSecretStore.getOrCreateRandom")(() =>
      Effect.die("Unused test publisher operation"),
    ),
  });
  const publisherLayer = Layer.mergeAll(
    Layer.succeed(ServerSecretStore, store),
    Layer.succeed(ServerEnvironmentIdentity, { getEnvironmentId: Effect.succeed(environmentId) }),
    NodeServices.layer,
  );
  const publisher = ManagedRuntime.make(publisherLayer);
  const sequence = await publisher.runPromise(PublishSequence.make);
  let unavailable = false;
  const fake = await startFakeApns({ mode: () => (unavailable ? "503" : "success") });
  const certPath = NodePath.join(scratch, `apns-ca-${NodeCrypto.randomUUID()}.crt`);
  await NodeFSP.writeFile(certPath, testCa);
  const port = await freePort();
  const relayUrl = `http://127.0.0.1:${port}`;
  const migrationRoot = NodePath.join(scratch, "migrations");
  await NodeFSP.cp(
    NodePath.join(tree, "infra/relay/migrations/postgres"),
    NodePath.join(migrationRoot, "relay"),
    { recursive: true },
  );
  await NodeFSP.cp(
    NodePath.join(tree, "infra/personal-relay/migrations"),
    NodePath.join(migrationRoot, "personal"),
    { recursive: true },
  );
  const env = {
    ...process.env,
    PERSONAL_RELAY_DATABASE_HOST: "127.0.0.1",
    PERSONAL_RELAY_DATABASE_PORT: database.port,
    PERSONAL_RELAY_DATABASE_USER: "t3relay",
    PERSONAL_RELAY_DATABASE_NAME: "t3relay",
    PERSONAL_RELAY_DATABASE_PASSWORD: "test-only",
    PERSONAL_RELAY_DATABASE_POOL_MAX: "5",
    PERSONAL_RELAY_PUBLIC_ORIGIN: origin,
    PERSONAL_RELAY_PORT: String(port),
    PERSONAL_RELAY_OWNER_SUBJECT: owner,
    PERSONAL_RELAY_CLERK_ISSUER: "https://test-clerk.invalid",
    PERSONAL_RELAY_CLERK_AUDIENCE: "t3-code-relay",
    PERSONAL_RELAY_CLERK_JWKS_URL: "https://test-clerk.invalid/jwks",
    PERSONAL_RELAY_CLERK_JWKS_FILE: NodePath.join(scratch, "jwks.json"),
    PERSONAL_RELAY_TOKEN_SIGNING_PRIVATE_KEY: await exportPKCS8(mint.privateKey),
    PERSONAL_RELAY_MIGRATIONS_DIR: migrationRoot,
    PERSONAL_RELAY_LOG_LEVEL: "info",
    APNS_ENABLED: "true",
    APNS_KEY_ID: "process-test",
    APNS_TEAM_ID: "process-test",
    APNS_BUNDLE_ID: bundle,
    APNS_ENVIRONMENT: "sandbox",
    APNS_PRIVATE_KEY: await exportPKCS8(apns.privateKey),
    PERSONAL_RELAY_APNS_ORIGIN_OVERRIDE: fake.origin,
    NODE_EXTRA_CA_CERTS: certPath,
  };
  const packed = (entry: string, args: string[], input?: string, overrides = {}) =>
    execute(
      NodePath.join(tree, `infra/personal-relay/dist/${entry}.mjs`),
      args,
      { ...env, ...overrides },
      input,
    );
  const db = ManagedRuntime.make(
    PgLayer.layer({
      host: "127.0.0.1",
      port: Number(database.port),
      database: "t3relay",
      username: "t3relay",
      password: Redacted.make("test-only"),
      maxConnections: 2,
    }),
  );
  const query = (text: string) =>
    db.runPromise(Effect.flatMap(RelayDb, (connection) => connection.$client.unsafe(text)));
  const fetchRelay: typeof fetch = (input, init) => {
    const url = new URL(String(input));
    NodeAssert.equal(url.origin, origin, "Only the background relay may receive client traffic");
    return fetch(`${relayUrl}${url.pathname}${url.search}`, init);
  };
  const signer = Layer.succeed(ManagedRelay.ManagedRelayDpopSigner, {
    thumbprint: Effect.succeed(thumbprint),
    createProof: Effect.fn("ManagedRelayDpopSigner.createProof")((input) =>
      Effect.promise(() =>
        new SignJWT({
          htm: input.method,
          htu: input.url,
          jti: NodeCrypto.randomUUID(),
          iat: Math.floor(now() / 1000),
          ...(input.accessToken ? { ath: computeDpopAccessTokenHash(input.accessToken) } : {}),
        })
          .setProtectedHeader({ alg: "ES256", typ: "dpop+jwt", jwk: dpopJwk })
          .sign(dpop.privateKey),
      ),
    ),
  });
  const mobile = ManagedRuntime.make(
    ManagedRelay.layer({
      relayUrl: "https://primary.invalid",
      backgroundRelayUrl: origin,
      clientId: "t3-mobile",
    }).pipe(Layer.provide(signer), Layer.provide(RpcHttp.layerRemoteHttpClient(fetchRelay))),
  );
  const clerkToken = await new SignJWT({
    sub: owner,
    iss: env.PERSONAL_RELAY_CLERK_ISSUER,
    aud: "t3-code-relay",
    exp: Math.floor(now() / 1000) + 3600,
  })
    .setProtectedHeader({ alg: "RS256", kid: "process-test" })
    .sign(clerk.privateKey);
  const preferences = {
    notificationsEnabled: true,
    liveActivitiesEnabled: true,
    notifyOnApproval: true,
    notifyOnInput: true,
    notifyOnCompletion: true,
    notifyOnFailure: true,
  };
  const registration: RelayDeviceRegistrationRequest = {
    deviceId: "process-device",
    label: "Phone",
    platform: "ios",
    iosMajorVersion: 18,
    bundleId: bundle,
    apsEnvironment: "sandbox",
    pushToken: "a".repeat(64),
    preferences,
  };
  const callMobile = <A, E>(
    operation: (client: ManagedRelay.ManagedRelayClient["Service"]) => Effect.Effect<A, E>,
  ) => mobile.runPromise(Effect.flatMap(ManagedRelay.ManagedRelayClient, operation));
  const register = (payload = registration) =>
    callMobile((client) => client.registerDevice({ clerkToken, payload }));
  const enroll = async () => {
    const envelope = await publisher.runPromise(
      EnrollmentProof.make({ relayUrl: origin, label: "Process test" }),
    );
    const result = await packed("admin", ["enroll"], JSON.stringify(envelope));
    NodeAssert.equal(result.code, 0, "Real U5 proof must enroll through packed U4 admin");
    NodeAssert.match(result.stdout.trim(), /^t3env_/);
    return result.stdout.trim();
  };
  let credential = "";
  const publish = async (
    thread: string,
    phase: RelayAgentActivityState["phase"],
    transition = `run:${thread}:${phase}`,
    oldMs = 0,
    title = thread,
  ) => {
    const timestamp = now();
    const state: RelayAgentActivityState = {
      environmentId,
      threadId: ThreadId.make(thread),
      phase,
      projectTitle: "Process",
      threadTitle: title,
      modelTitle: "Test",
      headline: phase,
      updatedAt: DateTime.formatIso(DateTime.makeUnsafe(timestamp)),
      deepLink: "/thread",
    };
    const eligible = DateTime.formatIso(DateTime.makeUnsafe(timestamp));
    const at = DateTime.formatIso(DateTime.makeUnsafe(timestamp - oldMs));
    const alert = phase !== "running";
    const proof = await new SignJWT({
      iss: `t3-env:${environmentId}`,
      sub: environmentId,
      aud: origin,
      environmentId,
      threadId: thread,
      state,
      jti: NodeCrypto.randomUUID(),
      iat: Math.floor(timestamp / 1000),
      exp: Math.floor(timestamp / 1000) + 300,
      t3_publish_seq: await publisher.runPromise(sequence.next),
      t3_transition: alert ? { id: transition, at } : null,
      t3_transition_eligible_at: alert ? eligible : null,
    })
      .setProtectedHeader({ alg: "EdDSA", typ: RELAY_ACTIVITY_PUBLISH_TYP })
      .sign(publishing.privateKey);
    const response = await fetch(
      `${relayUrl}/v1/environments/${environmentId}/threads/${thread}/agent-activity`,
      {
        method: "POST",
        headers: { authorization: `Bearer ${credential}`, "content-type": "application/json" },
        body: JSON.stringify({ state, proof }),
      },
    );
    NodeAssert.equal(response.status, 200, "Signed publish must commit");
  };
  let relay: ReturnType<typeof start> | undefined;
  try {
    const pending = await packed("main", ["serve"]);
    NodeAssert.notEqual(pending.code, 0, "Serve must reject an unmigrated database");
    const migrated = await packed("main", ["migrate"], undefined, {
      PERSONAL_RELAY_OWNER_SUBJECT: "",
      PERSONAL_RELAY_TOKEN_SIGNING_PRIVATE_KEY: "",
      APNS_ENABLED: "false",
    });
    NodeAssert.equal(migrated.code, 0, "Migrate must require database configuration only");
    credential = await enroll();
    relay = start(NodePath.join(tree, "infra/personal-relay/dist/main.mjs"), ["serve"], env);
    await until(
      async () => (await fetch(`${relayUrl}/health`).catch(() => null))?.status === 200,
      "serving readiness",
    );
    NodeAssert.equal((await fetch(`${relayUrl}/livez`)).status, 200);
    NodeAssert.equal((await fetch(`${relayUrl}/v1/environments`)).status, 404);
    const oversizedBody = new URLSearchParams({
      grant_type: "urn:ietf:params:oauth:grant-type:token-exchange",
      subject_token: "x".repeat(70_000),
      subject_token_type: "urn:ietf:params:oauth:token-type:jwt",
      requested_token_type: "urn:ietf:params:oauth:token-type:access_token",
      resource: origin,
      scope: "mobile:registration",
      client_id: "t3-mobile",
    }).toString();
    const oversizedStatus = await new Promise<number>((resolve, reject) => {
      const request = NodeHttp.request(
        `${relayUrl}/v1/client/dpop-token`,
        {
          method: "POST",
          headers: { "content-type": "application/x-www-form-urlencoded", dpop: "test-only" },
        },
        (response) => {
          response.resume();
          response.once("end", () => resolve(response.statusCode ?? 0));
        },
      );
      request.once("error", (error) => {
        if ("code" in error && error.code === "ECONNRESET") resolve(0);
        else reject(error);
      });
      request.write(oversizedBody);
      request.end();
    });
    NodeAssert.ok(
      [0, 400, 413].includes(oversizedStatus),
      "Oversized chunked body must be rejected or its connection closed",
    );

    NodeAssert.equal((await fetch(`${relayUrl}/health`)).status, 200);
    const second = await packed("main", ["serve"], undefined, {
      PERSONAL_RELAY_PORT: String(await freePort()),
    });
    NodeAssert.equal(second.code, 3, "Second worker must exit before listening");

    // Backfill both transitions after re-enrollment and before first phone registration.
    credential = await enroll();
    await publish("approval", "waiting_for_input", "request:approval");
    await publish("held", "completed", "run:held:completed", 20 * 60_000);
    await register();
    await register();
    await until(
      async () =>
        (await query("SELECT 1 FROM personal_relay_alert_deliveries WHERE status = 'accepted'"))
          .length === 2,
      "alert backfill",
    );
    NodeAssert.equal(
      fake.requests.filter((r) => r.headers["apns-push-type"] === "alert").length,
      2,
    );
    await publish("held", "completed", "run:held:completed", 20 * 60_000, "Renamed completion");
    await register();
    NodeAssert.equal((await query("SELECT 1 FROM personal_relay_alert_deliveries")).length, 2);
    await callMobile((client) =>
      client.registerLiveActivity({
        clerkToken,
        payload: { deviceId: registration.deviceId, activityPushToken: "b".repeat(64) },
      }),
    );
    await until(
      async () => fake.requests.some((r) => r.headers["apns-push-type"] === "liveactivity"),
      "Live Activity update",
    );
    const snapshot = await callMobile((client) => client.getAgentActivitySnapshot({ clerkToken }));
    NodeAssert.ok(snapshot.aggregate);
    await register({
      ...registration,
      preferences: { ...preferences, liveActivitiesEnabled: false },
    });
    await until(
      async () => fake.requests.some((r) => JSON.parse(r.body.toString()).aps?.event === "end"),
      "Live Activity end",
    );

    unavailable = true;
    const backoffStart = fake.requests.length;
    for (let i = 0; i < 5; i++) await publish(`outage-${i}`, "completed");
    await until(
      async () =>
        (
          await query(
            "SELECT failure_count FROM personal_relay_target_state WHERE channel = 'notification' AND failure_count > 0",
          )
        ).length === 1,
      "backoff persistence",
    );
    await register({
      ...registration,
      preferences: { ...preferences, liveActivitiesEnabled: false },
    });
    await sleep(500);
    NodeAssert.equal(
      fake.requests.length - backoffStart,
      1,
      "First failure must stop five-alert batch",
    );
    await stop(relay.child);
    relay = start(NodePath.join(tree, "infra/personal-relay/dist/main.mjs"), ["serve"], env);
    await until(
      async () => (await fetch(`${relayUrl}/health`).catch(() => null))?.status === 200,
      "restart readiness",
    );
    unavailable = false;
    await until(
      async () =>
        (await query("SELECT 1 FROM personal_relay_alert_deliveries WHERE status = 'pending'"))
          .length === 0,
      "restart delivery recovery",
      20_000,
    );
    const acceptedBefore = (
      await query("SELECT 1 FROM personal_relay_alert_deliveries WHERE status = 'accepted'")
    ).length;
    NodeAssert.equal(acceptedBefore, 7);
    const restartCount = fake.requests.length;
    await register({
      ...registration,
      preferences: { ...preferences, liveActivitiesEnabled: false },
    });
    await sleep(200);
    NodeAssert.equal(
      fake.requests.length,
      restartCount,
      "Accepted alerts must not repeat after restart",
    );

    const text = '界😀"\\'.repeat(20);
    await query(
      "INSERT INTO personal_relay_target_state (device_id, channel, retry_at) VALUES ('process-device', 'notification', now() + interval '1 hour'), ('process-device', 'live_activity', now() + interval '1 hour') ON CONFLICT (device_id, channel) DO UPDATE SET retry_at = EXCLUDED.retry_at",
    );
    await register();
    await callMobile((client) =>
      client.registerLiveActivity({
        clerkToken,
        payload: { deviceId: registration.deviceId, activityPushToken: "c".repeat(64) },
      }),
    );
    for (let i = 0; i < 18; i++)
      await publish(`large-${i}`, "completed", `run:large-${i}:completed`, 0, text);
    const budgetStart = fake.requests.length;
    await query("UPDATE personal_relay_target_state SET retry_at = NULL");
    await register();
    await until(
      async () =>
        (await query("SELECT 1 FROM personal_relay_alert_deliveries WHERE status = 'accepted'"))
          .length === 25,
      "bounded backlog settlement",
    );
    const budgetRequests = fake.requests.slice(budgetStart);
    NodeAssert.equal(
      budgetRequests.filter((r) => r.headers["apns-push-type"] === "liveactivity").length,
      1,
    );
    NodeAssert.ok(fake.requests.every((r) => r.body.byteLength <= 3800));
    for (const request of fake.requests) {
      NodeAssert.match(String(request.headers[":path"]), /^\/3\/device\/[abc]+$/);
      const activity = request.headers["apns-push-type"] === "liveactivity";
      NodeAssert.equal(
        request.headers["apns-topic"],
        activity ? `${bundle}.push-type.liveactivity` : bundle,
      );
      NodeAssert.ok(["5", "10"].includes(String(request.headers["apns-priority"])));
    }
    const revoke = await packed("admin", ["revoke-environment", environmentId]);
    NodeAssert.equal(revoke.code, 0);
    NodeAssert.equal(revoke.stdout, "");
    await callMobile((client) =>
      client.unregisterDevice({ clerkToken, deviceId: registration.deviceId }),
    );
    NodeAssert.equal((await query("SELECT 1 FROM relay_mobile_devices")).length, 0);
    const configuration = await packed("admin", ["config"]);
    NodeAssert.equal(configuration.code, 0);
    NodeAssert.equal(configuration.stdout, "");
    NodeAssert.deepEqual(JSON.parse(configuration.stderr).activeOverrides, [
      "PERSONAL_RELAY_CLERK_JWKS_FILE",
      "PERSONAL_RELAY_APNS_ORIGIN_OVERRIDE",
    ]);
    for (const mode of ["PERSONAL_RELAY_CLERK_JWKS_FILE", "PERSONAL_RELAY_APNS_ORIGIN_OVERRIDE"]) {
      NodeAssert.ok(
        logs.join("\n").includes(`"mode":"${mode}"`),
        "Active override must be named in startup warnings",
      );
    }
    const status = await packed("admin", ["status"]);
    NodeAssert.equal(status.code, 0);
    NodeAssert.equal(status.stdout, "");
    NodeAssert.ok(
      logs.every(
        (log) =>
          !log.includes(credential) &&
          !log.includes(signingKey) &&
          !log.includes(clerkToken) &&
          !log.includes(registration.pushToken!),
      ),
    );
    console.log(
      "PASS process harness: migrations, enrollment, JWKS/DPoP registration, held completion, backfill, alerts, update/end, persisted backoff, restart, singleton and bounded backlog",
    );
  } catch (error) {
    console.error(
      JSON.stringify({
        deliveries: await query(
          "SELECT status, count(*)::int AS count FROM personal_relay_alert_deliveries GROUP BY status",
        ).catch(() => []),
        attempts: await query(
          "SELECT transport_error, apns_reason, count(*)::int AS count FROM relay_delivery_attempts GROUP BY transport_error, apns_reason",
        ).catch(() => []),
        requests: fake.requests.map((request) => ({
          type: request.headers["apns-push-type"],
          bytes: request.body.byteLength,
        })),
      }),
    );
    throw error;
  } finally {
    for (const child of children) await stop(child);
    await Promise.all([
      mobile.dispose(),
      publisher.dispose(),
      db.dispose(),
      fake.close(),
      NodeFSP.unlink(certPath),
    ]);
  }
}

function start(entry: string, args: string[], env: NodeJS.ProcessEnv) {
  const child = NodeChildProcess.spawn(process.execPath, [entry, ...args], {
    env,
    stdio: ["pipe", "pipe", "pipe"],
  });
  children.add(child);
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => {
    stdout += chunk.toString();
    if (entry.endsWith("/main.mjs")) logs.push(chunk.toString());
  });
  child.stderr.on("data", (chunk) => {
    stderr += chunk.toString();
    if (entry.endsWith("/main.mjs")) logs.push(chunk.toString());
  });
  const completed = new Promise<{ code: number | null; stdout: string; stderr: string }>(
    (resolve, reject) => {
      child.once("error", reject);
      child.once("exit", (code) => {
        children.delete(child);
        resolve({ code, stdout, stderr });
      });
    },
  );
  return { child, completed };
}

async function execute(entry: string, args: string[], env: NodeJS.ProcessEnv, input?: string) {
  const process = start(entry, args, env);
  process.child.stdin?.end(input);
  const timer = setTimeout(() => process.child.kill("SIGKILL"), 20_000);
  try {
    return await process.completed;
  } finally {
    clearTimeout(timer);
  }
}

async function stop(child: NodeChildProcess.ChildProcess) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const completed = new Promise<void>((resolve) => child.once("exit", () => resolve()));
  child.kill("SIGTERM");
  const timer = setTimeout(() => child.kill("SIGKILL"), 5000);
  try {
    await completed;
  } finally {
    clearTimeout(timer);
  }
}

async function freePort() {
  const server = NodeNet.createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  NodeAssert.ok(address && typeof address !== "string");
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return address.port;
}

async function until(check: () => Promise<boolean>, description: string, timeout = 10_000) {
  const deadline = now() + timeout;
  while (now() < deadline) {
    if (await check()) return;
    await sleep(50);
  }
  throw new Error(`Timed out: ${description}`);
}

await run();
