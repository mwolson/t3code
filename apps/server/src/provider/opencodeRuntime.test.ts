import { ClientError } from "@opencode-ai/client";
import { assert, describe, it } from "@effect/vitest";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Effect from "effect/Effect";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import {
  isOpenCodeHostServiceAlive,
  isOpenCodeRuntimeError,
  OpenCodeRuntime,
  layer,
  openCodeAuthorizationHeader,
  openCodeHostStateHome,
  parseOpenCodeServiceUrl,
  readOpenCodeHostService,
  readOpenCodeStatePassword,
  runOpenCodeSdk,
} from "./opencodeRuntime.ts";
import { SpawnedProcessReaper } from "./SpawnedProcessReaper.ts";

describe("parseOpenCodeServiceUrl", () => {
  it("reads the last http URL from service start output", () => {
    assert.strictEqual(
      parseOpenCodeServiceUrl("starting\nhttp://127.0.0.1:4096\n"),
      "http://127.0.0.1:4096/",
    );
  });

  it("returns null when no URL is present", () => {
    assert.isNull(parseOpenCodeServiceUrl("already running\n"));
  });
});

describe("openCodeAuthorizationHeader", () => {
  it("encodes the fixed opencode username with the minted password", () => {
    assert.strictEqual(
      openCodeAuthorizationHeader("s3cr3t"),
      `Basic ${Buffer.from("opencode:s3cr3t", "utf8").toString("base64")}`,
    );
  });
});

describe("readOpenCodeStatePassword", () => {
  it("reads the beta lildax state-dir password file", () => {
    const stateHome = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "opencode2-state-"));
    NodeFS.mkdirSync(NodePath.join(stateHome, "opencode"), { recursive: true });
    NodeFS.writeFileSync(
      NodePath.join(stateHome, "opencode", "password"),
      "file-minted-password\n",
    );
    assert.strictEqual(
      readOpenCodeStatePassword({ XDG_STATE_HOME: stateHome }),
      "file-minted-password",
    );
  });

  it("returns null when the state-dir password is missing", () => {
    const stateHome = NodeFS.mkdtempSync(
      NodePath.join(NodeOS.tmpdir(), "opencode2-state-missing-"),
    );
    assert.isNull(readOpenCodeStatePassword({ XDG_STATE_HOME: stateHome }));
  });
});

describe("readOpenCodeHostService", () => {
  const writeHostService = (home: string, body: unknown) => {
    const dir = NodePath.join(home, ".local", "state", "opencode");
    NodeFS.mkdirSync(dir, { recursive: true });
    NodeFS.writeFileSync(NodePath.join(dir, "service.json"), `${JSON.stringify(body)}\n`);
  };

  it("reads a live user service even when XDG_STATE_HOME is a T3 isolate", () => {
    const home = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "opencode2-host-"));
    writeHostService(home, {
      url: "http://127.0.0.1:49374",
      password: "host-service-password",
      pid: process.pid,
    });
    const environment = {
      HOME: home,
      XDG_STATE_HOME: NodePath.join(NodeOS.tmpdir(), "t3-opencode2-state-deadbeef"),
    };
    assert.strictEqual(openCodeHostStateHome(environment), NodePath.join(home, ".local", "state"));
    const service = readOpenCodeHostService(environment);
    assert.deepStrictEqual(service, {
      url: "http://127.0.0.1:49374",
      password: "host-service-password",
      pid: process.pid,
    });
    assert.isTrue(service !== null && isOpenCodeHostServiceAlive(service));
  });

  it("returns null when the host ledger is missing", () => {
    const home = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "opencode2-host-missing-"));
    assert.isNull(readOpenCodeHostService({ HOME: home }));
  });

  it("treats a missing or unreachable pid as not alive", () => {
    assert.isFalse(
      isOpenCodeHostServiceAlive({
        url: "http://127.0.0.1:1",
        password: "unused",
        pid: null,
      }),
    );
    assert.isFalse(
      isOpenCodeHostServiceAlive({
        url: "http://127.0.0.1:1",
        password: "unused",
        pid: 2_147_483_647,
      }),
    );
  });
});

describe("OpenCodeRuntime errors", () => {
  it.effect("keeps SDK cause text out of the stable model-ready error", () =>
    Effect.gen(function* () {
      const secret = "MODEL_STARTUP_SECRET";
      const cause = new Error(`Model unavailable: ${secret}`);
      const error = yield* runOpenCodeSdk("session.generate", async () => {
        throw cause;
      }).pipe(Effect.flip);

      assert.isTrue(isOpenCodeRuntimeError(error));
      assert.strictEqual(error.category, "model-unavailable");
      assert.strictEqual(error.cause, cause);
      assert.notInclude(error.message, secret);
    }),
  );

  it.effect("normalizes authentication and network failures at the SDK boundary", () =>
    Effect.gen(function* () {
      const authentication = yield* runOpenCodeSdk("health.get", async () => {
        throw new Error("401 Unauthorized: PRIVATE_RESPONSE");
      }).pipe(Effect.flip);
      const network = yield* runOpenCodeSdk("health.get", async () => {
        throw new Error("fetch failed: ECONNREFUSED PRIVATE_ADDRESS");
      }).pipe(Effect.flip);

      assert.strictEqual(authentication.category, "authentication-failed");
      assert.strictEqual(network.category, "network-failed");
      assert.notInclude(authentication.message, "PRIVATE_RESPONSE");
      assert.notInclude(network.message, "PRIVATE_ADDRESS");
    }),
  );

  it.effect("classifies the typed client's transport wrapper as a network failure", () =>
    Effect.gen(function* () {
      const cause = new ClientError("Transport", {
        cause: Object.assign(new Error("connect ECONNREFUSED 127.0.0.1:9"), {
          code: "ECONNREFUSED",
        }),
      });
      const error = yield* runOpenCodeSdk("health.get", async () => {
        throw cause;
      }).pipe(Effect.flip);

      assert.strictEqual(error.category, "network-failed");
      assert.strictEqual(error.cause, cause);
      assert.notInclude(error.message, "ECONNREFUSED");
    }),
  );

  it.effect("does not classify an SDK 404 as a missing executable", () =>
    Effect.gen(function* () {
      const error = yield* runOpenCodeSdk("health.get", async () => {
        throw new Error("NotFoundError: endpoint returned 404");
      }).pipe(Effect.flip);

      assert.strictEqual(error.category, "sdk-request-failed");
    }),
  );
});

describe("OpenCodeRuntime shared server", () => {
  const isolatedHome = () =>
    NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "opencode2-no-host-"));

  const bannerSpawner = (spawnCount: { value: number }) => {
    const encoder = new TextEncoder();
    return ChildProcessSpawner.make(() =>
      Effect.sync(() => {
        spawnCount.value += 1;
        const port = 4700 + spawnCount.value;
        return ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(40 + spawnCount.value),
          exitCode: Effect.never,
          isRunning: Effect.succeed(true),
          kill: () => Effect.void,
          unref: Effect.succeed(Effect.void),
          stdin: Sink.drain,
          stdout: Stream.make(
            encoder.encode(
              `server listening on http://127.0.0.1:${port}\nserver password shared-password\n`,
            ),
          ),
          stderr: Stream.never,
          all: Stream.never,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.never,
        });
      }),
    );
  };

  it.effect("attaches to a live host service instead of spawning", () =>
    Effect.gen(function* () {
      const spawnCount = { value: 0 };
      const home = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "opencode2-host-connect-"));
      const serviceDir = NodePath.join(home, ".local", "state", "opencode");
      NodeFS.mkdirSync(serviceDir, { recursive: true });
      NodeFS.writeFileSync(
        NodePath.join(serviceDir, "service.json"),
        `{"url":"http://127.0.0.1:49374","password":"host-service-password","pid":${process.pid}}\n`,
      );
      const connection = yield* Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* OpenCodeRuntime;
          return yield* runtime.connectToOpenCodeServer({
            binaryPath: "opencode2",
            environment: { HOME: home },
          });
        }),
      ).pipe(
        Effect.provide(layer),
        Effect.provideService(SpawnedProcessReaper, {
          track: () => Effect.void,
          untrack: () => Effect.void,
        }),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, bannerSpawner(spawnCount)),
        Effect.provideService(HostProcessPlatform, "win32"),
      );

      assert.strictEqual(spawnCount.value, 0);
      assert.strictEqual(connection.url, "http://127.0.0.1:49374");
      assert.strictEqual(connection.password, "host-service-password");
      assert.isTrue(connection.external);
    }),
  );

  const serviceSpawner = (spawnCount: { value: number }) => {
    const encoder = new TextEncoder();
    return ChildProcessSpawner.make(() =>
      Effect.sync(() => {
        spawnCount.value += 1;
        const stdout =
          spawnCount.value === 1 ? "http://127.0.0.1:4096\n" : "user-service-password\n";
        return ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(80 + spawnCount.value),
          exitCode: Effect.succeed(0 as never),
          isRunning: Effect.succeed(false),
          kill: () => Effect.void,
          unref: Effect.succeed(Effect.void),
          stdin: Sink.drain,
          stdout: Stream.make(encoder.encode(stdout)),
          stderr: Stream.empty,
          all: Stream.never,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.never,
        });
      }),
    );
  };

  it.effect("starts the user service when none is live", () =>
    Effect.gen(function* () {
      const spawnCount = { value: 0 };
      const connection = yield* Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* OpenCodeRuntime;
          return yield* runtime.connectToOpenCodeServer({
            binaryPath: "opencode2",
            environment: { HOME: isolatedHome() },
          });
        }),
      ).pipe(
        Effect.provide(layer),
        Effect.provideService(SpawnedProcessReaper, {
          track: () => Effect.void,
          untrack: () => Effect.void,
        }),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, serviceSpawner(spawnCount)),
        Effect.provideService(HostProcessPlatform, "win32"),
      );

      assert.strictEqual(spawnCount.value, 2);
      assert.strictEqual(connection.url, "http://127.0.0.1:4096/");
      assert.strictEqual(connection.password, "user-service-password");
      assert.isTrue(connection.external);
    }),
  );
});
