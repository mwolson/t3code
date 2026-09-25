// @effect-diagnostics nodeBuiltinImport:off
import { assert, describe, it } from "@effect/vitest";
import { Context, Deferred, Effect, Exit, Fiber, Layer, Scope } from "effect";
import * as PlatformError from "effect/PlatformError";
import { TestClock } from "effect/testing";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import { afterEach, vi } from "vite-plus/test";
import { OpenCode2Runtime, layer, openCodeAuthorizationHeader } from "./opencode2Runtime.ts";
import {
  connect,
  info,
  ledger,
  ledgerPath,
  provideRuntimeDependencies,
  refusal,
  serviceSpawner,
  temporaryHome,
  writeLedger,
} from "./opencode2Runtime.testkit.ts";

vi.mock(
  "@opencode/client/service",
  async () => (await import("./opencode2Runtime.mocks.ts")).serviceModule,
);
vi.mock(
  "@opencode/client/effect/service",
  async () => (await import("./opencode2Runtime.mocks.ts")).serviceModule,
);
vi.mock("node:child_process", async (importOriginal) => {
  const original = await importOriginal<typeof import("node:child_process")>();
  const { childProcessMethods } = await import("./opencode2Runtime.mocks.ts");
  return { ...original, ...childProcessMethods, default: { ...original, ...childProcessMethods } };
});

const started = {
  ...ledger,
  id: "service-2",
  url: "http://127.0.0.1:4097",
  password: "PRIVATE_STARTED",
  pid: 67890,
};
const startedInfo = { ...info, pid: started.pid, urls: [started.url] };
const spawners: ReturnType<typeof serviceSpawner>[] = [];

afterEach(() => {
  for (const { commands } of spawners.splice(0)) {
    for (const command of commands) assert.deepEqual(command.args, ["service", "start"]);
  }
});

function starter(scripts: Parameters<typeof serviceSpawner>[0]) {
  const recorded = serviceSpawner(scripts);
  spawners.push(recorded);
  return recorded;
}

/** `opencode service start` registers the ledger, then prints the endpoint. */
function registers(home: string, value: unknown = started, stdout = `${started.url}\n`) {
  return { stdout, onSpawn: () => writeLedger(home, value) };
}

/** Lets forked callers run until they block, so the ledger is still absent when they read it. */
const runQueuedFibers = Effect.forEach(Array.from({ length: 10 }), () => Effect.yieldNow, {
  discard: true,
});

/** A start that is spawned but does not exit, or register, until `open` runs. */
const gatedStart = Effect.fnUntraced(function* (home: string) {
  const gate = yield* Deferred.make<void>();
  const spawned = yield* Deferred.make<void>();
  const recorded = starter([
    {
      gate,
      stdout: `${started.url}\n`,
      onSpawn: () => Deferred.doneUnsafe(spawned, Effect.void),
    },
  ]);
  const open = Effect.sync(() => writeLedger(home, started)).pipe(
    Effect.andThen(Deferred.succeed(gate, undefined)),
  );
  return { ...recorded, gate, open, spawned };
});

function absentPid() {
  vi.mocked(process.kill).mockImplementation(() => {
    throw Object.assign(new Error("PRIVATE_ABSENT"), { code: "ESRCH" });
  });
}

function answersAfterStart() {
  vi.mocked(fetch).mockImplementation(async (url) =>
    String(url).startsWith(started.url) ? Response.json(startedInfo) : Promise.reject(refusal()),
  );
}

function assertAttachedToStarted(result: unknown) {
  assert.deepEqual(result, {
    url: started.url,
    password: started.password,
    exitCode: null,
    external: true,
  });
  const calls = vi.mocked(fetch).mock.calls.filter(([url]) => String(url).startsWith(started.url));
  assert.lengthOf(calls, 2);
  for (const [url, options] of calls) {
    assert.strictEqual(String(url), `${started.url}/api/info`);
    assert.strictEqual(
      new Headers(options?.headers).get("authorization"),
      openCodeAuthorizationHeader(started.password),
    );
  }
}

describe("OpenCode 2 host service start", () => {
  it.effect("starts the host service once when no ledger exists, then attaches", () =>
    Effect.gen(function* () {
      const home = temporaryHome();
      answersAfterStart();
      const { commands, released, spawner } = starter([registers(home)]);
      const result = yield* connect({ HOME: home }, undefined, undefined, {
        binaryPath: "/fixture/opencode",
        spawner,
      });
      assertAttachedToStarted(result);
      assert.lengthOf(commands, 1);
      assert.strictEqual(commands[0]!.command, "/fixture/opencode");
      assert.deepEqual(released, [0]);
      assert.strictEqual(vi.mocked(process.kill).mock.calls.length, 0);
    }),
  );
  it.effect.each([undefined, "", "  "])("runs opencode from PATH when binaryPath is %j", (path) =>
    Effect.gen(function* () {
      const home = temporaryHome();
      answersAfterStart();
      const { commands, spawner } = starter([registers(home)]);
      yield* connect({ HOME: home }, undefined, undefined, {
        ...(path === undefined ? {} : { binaryPath: path }),
        spawner,
      });
      assert.strictEqual(commands[0]!.command, "opencode");
    }),
  );
  it.effect.each([
    { url: "http://127.0.0.1:4096", address: "127.0.0.1" },
    { url: "http://[::1]:4096", address: "::1" },
  ])("starts once for a dead PID whose local URL refuses ($url)", ({ url, address }) =>
    Effect.gen(function* () {
      const home = temporaryHome();
      writeLedger(home, { ...ledger, url });
      absentPid();
      vi.mocked(fetch).mockImplementation(async (target) =>
        String(target).startsWith(started.url)
          ? Response.json(startedInfo)
          : Promise.reject(refusal(address)),
      );
      const { commands, spawner } = starter([registers(home)]);
      const result = yield* connect({ HOME: home }, undefined, undefined, { spawner });
      assertAttachedToStarted(result);
      assert.lengthOf(commands, 1);
      assert.isAbove(vi.mocked(process.kill).mock.calls.length, 0);
      for (const call of vi.mocked(process.kill).mock.calls)
        assert.deepEqual(call, [ledger.pid, 0]);
    }),
  );
  it.effect("passes a service environment without T3's AppImage and Electron runtime", () =>
    Effect.gen(function* () {
      const home = temporaryHome();
      answersAfterStart();
      const { commands, spawner } = starter([registers(home)]);
      const mount = "/tmp/.mount_T3Code";
      yield* connect(
        {
          HOME: home,
          APPDIR: mount,
          APPIMAGE: "/apps/T3.AppImage",
          ARGV0: "T3",
          OWD: "/work",
          ELECTRON_RUN_AS_NODE: "1",
          GSETTINGS_SCHEMA_DIR: `${mount}/usr/share/glib-2.0/schemas`,
          LD_LIBRARY_PATH: `${mount}/usr/lib`,
          OPENCODE_EXPERIMENTAL_BACKGROUND_SUBAGENTS: "true",
          PATH: `/home/user/bin:${mount}:${mount}/usr/sbin:/usr/bin`,
          T3CODE_DEV_AUTH_TOKEN: "PRIVATE_TOKEN",
          t3code_home: "/home/user/.t3",
          XDG_DATA_DIRS: `${mount}/usr/share/:/usr/share`,
          XDG_STATE_HOME: "/tmp/t3-opencode2-state-legacy",
        },
        undefined,
        undefined,
        { spawner },
      );
      const options = commands[0]!.options;
      assert.strictEqual(options.extendEnv, false);
      assert.deepEqual(options.env, {
        HOME: home,
        OPENCODE_EXPERIMENTAL_BACKGROUND_SUBAGENTS: "true",
        PATH: "/home/user/bin:/usr/bin",
        XDG_DATA_DIRS: "/usr/share",
        XDG_STATE_HOME: NodePath.join(home, ".local", "state"),
      });
    }),
  );
  it.effect("shares one in-flight start between concurrent callers", () =>
    Effect.gen(function* () {
      const home = temporaryHome();
      answersAfterStart();
      const { commands, gate, open, spawned, spawner } = yield* gatedStart(home);
      yield* Effect.gen(function* () {
        const runtime = yield* OpenCode2Runtime;
        const attach = runtime.connectToOpenCodeServer({ environment: { HOME: home } });
        const first = yield* Effect.forkChild(attach);
        yield* Deferred.await(spawned);
        const second = yield* Effect.forkChild(attach);
        yield* runQueuedFibers;
        yield* open;
        for (const fiber of [first, second]) {
          assert.deepEqual(yield* Fiber.join(fiber), {
            url: started.url,
            password: started.password,
            exitCode: null,
            external: true,
          });
        }
      }).pipe(Effect.provide(layer), Effect.scoped, provideRuntimeDependencies(spawner));
      assert.lengthOf(commands, 1);
      assert.isTrue(yield* Deferred.isDone(gate));
    }),
  );
  it.effect("keeps a shared start running when the caller that began it is interrupted", () =>
    Effect.gen(function* () {
      const home = temporaryHome();
      answersAfterStart();
      const { commands, open, released, spawned, spawner } = yield* gatedStart(home);
      yield* Effect.gen(function* () {
        const runtime = yield* OpenCode2Runtime;
        const attach = runtime.connectToOpenCodeServer({ environment: { HOME: home } });
        const first = yield* Effect.forkChild(attach);
        yield* Deferred.await(spawned);
        yield* Fiber.interrupt(first);
        assert.lengthOf(released, 0);
        const second = yield* Effect.forkChild(attach);
        yield* runQueuedFibers;
        yield* open;
        assertAttachedToStarted(yield* Fiber.join(second));
      }).pipe(Effect.provide(layer), Effect.scoped, provideRuntimeDependencies(spawner));
      assert.lengthOf(commands, 1);
    }),
  );
});

describe("OpenCode 2 host service start fails closed", () => {
  it.effect("reports a start that exits nonzero without starting again", () =>
    Effect.gen(function* () {
      const home = temporaryHome();
      const { commands, spawner } = starter([{ code: 3, stdout: "PRIVATE_OUTPUT" }]);
      const error = yield* connect({ HOME: home }, undefined, undefined, { spawner }).pipe(
        Effect.flip,
      );
      assert.strictEqual(error.category, "service-start-failed");
      assert.strictEqual(error.exitCode, 3);
      assert.notInclude(error.message, "PRIVATE");
      assert.lengthOf(commands, 1);
      assert.strictEqual(vi.mocked(fetch).mock.calls.length, 0);
    }),
  );
  it.effect("reports a missing binary", () =>
    Effect.gen(function* () {
      const home = temporaryHome();
      const { commands, spawner } = starter([
        {
          error: PlatformError.systemError({
            _tag: "NotFound",
            module: "ChildProcess",
            method: "spawn",
          }),
        },
      ]);
      const error = yield* connect({ HOME: home }, undefined, undefined, { spawner }).pipe(
        Effect.flip,
      );
      assert.strictEqual(error.category, "binary-not-found");
      assert.lengthOf(commands, 1);
    }),
  );
  it.effect.each(["linux", "win32"] as const)(
    "bounds a start that never returns on %s and does not start again",
    (platform) =>
      Effect.gen(function* () {
        const home = temporaryHome();
        const spawned = yield* Deferred.make<void>();
        const { commands, released, spawner, unreferenced } = starter([
          { hang: true, onSpawn: () => Deferred.doneUnsafe(spawned, Effect.void) },
        ]);
        const fiber = yield* Effect.gen(function* () {
          const runtime = yield* OpenCode2Runtime;
          return yield* runtime.connectToOpenCodeServer({ environment: { HOME: home } });
        }).pipe(
          Effect.provide(layer),
          Effect.scoped,
          provideRuntimeDependencies(spawner, platform),
          Effect.flip,
          Effect.forkChild,
        );
        yield* Deferred.await(spawned);
        assert.lengthOf(released, 0);
        yield* TestClock.adjust("30 seconds");
        const error = yield* Fiber.join(fiber);
        assert.strictEqual(error.category, "service-start-timeout");
        assert.strictEqual(error.timeoutMs, 30_000);
        assert.deepEqual(released, [0]);
        // An unreferenced handle is released without `taskkill /T`, which on
        // Windows would also end the detached service.
        assert.deepEqual(unreferenced, platform === "win32" ? [0] : []);
        assert.lengthOf(commands, 1);
      }),
  );
  it.effect(
    "unreferences a Windows starter before an interruption during spawn can release it",
    () =>
      Effect.gen(function* () {
        const home = temporaryHome();
        const entered = yield* Deferred.make<void>();
        const spawnGate = yield* Deferred.make<void>();
        const { events, spawner } = starter([
          { hang: true, spawnGate, onSpawn: () => Deferred.doneUnsafe(entered, Effect.void) },
        ]);
        const scope = yield* Scope.make();
        const context = yield* Layer.buildWithScope(layer, scope).pipe(
          provideRuntimeDependencies(spawner, "win32"),
        );
        const runtime = Context.get(context, OpenCode2Runtime);
        const caller = yield* runtime
          .connectToOpenCodeServer({ environment: { HOME: home } })
          .pipe(provideRuntimeDependencies(spawner, "win32"), Effect.exit, Effect.forkChild);
        yield* Deferred.await(entered);
        const closing = yield* Effect.forkChild(Scope.close(scope, Exit.void));
        yield* runQueuedFibers;
        yield* Deferred.succeed(spawnGate, undefined);
        yield* Fiber.join(closing);
        assert.isTrue(Exit.hasInterrupts(yield* Fiber.join(caller)));
        assert.deepEqual(events, ["unref", "release"]);
      }),
  );
  it.effect("settles a start whose fiber is interrupted before it runs", () =>
    Effect.gen(function* () {
      const home = temporaryHome();
      const { commands, spawner } = starter([{ hang: true }]);
      const scope = yield* Scope.make();
      const context = yield* Layer.buildWithScope(layer, scope).pipe(
        provideRuntimeDependencies(spawner),
      );
      const runtime = Context.get(context, OpenCode2Runtime);
      const attach = runtime
        .connectToOpenCodeServer({ environment: { HOME: home } })
        .pipe(provideRuntimeDependencies(spawner), Effect.exit);
      const caller = yield* Effect.forkChild(attach, { startImmediately: true });
      yield* Scope.close(scope, Exit.void);
      assert.isTrue(Exit.hasInterrupts(yield* Fiber.join(caller)));
      assert.isTrue(Exit.hasInterrupts(yield* attach));
      assert.lengthOf(commands, 0);
    }),
  );
  it.effect.each([
    { name: "no printed URL", stdout: "", value: started, category: "service-start-failed" },
    {
      name: "a different printed URL",
      stdout: "http://127.0.0.1:4999\n",
      value: started,
      category: "service-identity-mismatch",
    },
    {
      name: "no ledger",
      stdout: `${started.url}\n`,
      value: undefined,
      category: "service-start-failed",
    },
    {
      name: "an unsupported ledger version",
      stdout: `${started.url}\n`,
      value: { ...started, version: "2.0.16" },
      category: "unsupported-server-version",
    },
    {
      name: "a ledger without credentials",
      stdout: `${started.url}\n`,
      value: { ...started, password: undefined },
      category: "service-credentials-required",
    },
    {
      name: "a malformed ledger",
      stdout: `${started.url}\n`,
      value: { ...started, pid: 0 },
      category: "service-identity-mismatch",
    },
  ])("fails closed on $name after the start", ({ stdout, value, category }) =>
    Effect.gen(function* () {
      const home = temporaryHome();
      answersAfterStart();
      const { commands, spawner } = starter([
        { stdout, onSpawn: () => value !== undefined && writeLedger(home, value) },
      ]);
      const error = yield* connect({ HOME: home }, undefined, undefined, { spawner }).pipe(
        Effect.flip,
      );
      assert.strictEqual(error.category, category);
      assert.lengthOf(commands, 1);
      assert.strictEqual(vi.mocked(fetch).mock.calls.length, 0);
    }),
  );
  it.effect.each([
    { name: "another PID", body: { ...startedInfo, pid: started.pid + 1 } },
    { name: "a malformed identity", body: { version: started.version } },
  ])("fails closed when the started endpoint reports $name", ({ body }) =>
    Effect.gen(function* () {
      const home = temporaryHome();
      vi.mocked(fetch).mockResolvedValue(Response.json(body));
      const { commands, spawner } = starter([registers(home)]);
      const error = yield* connect({ HOME: home }, undefined, undefined, { spawner }).pipe(
        Effect.flip,
      );
      assert.strictEqual(error.category, "service-identity-mismatch");
      assert.lengthOf(commands, 1);
    }),
  );
  it.effect("does not start a second time when the started service is gone too", () =>
    Effect.gen(function* () {
      const home = temporaryHome();
      absentPid();
      vi.mocked(fetch).mockImplementation(async (url) =>
        Promise.reject(refusal("127.0.0.1", Number(new URL(String(url)).port))),
      );
      const { commands, spawner } = starter([registers(home)]);
      const error = yield* connect({ HOME: home }, undefined, undefined, { spawner }).pipe(
        Effect.flip,
      );
      assert.strictEqual(error.category, "network-failed");
      assert.lengthOf(commands, 1);
    }),
  );
  it.effect("does not start when the dead PID is back before dispatch", () =>
    Effect.gen(function* () {
      const home = temporaryHome();
      writeLedger(home);
      vi.mocked(fetch).mockRejectedValue(refusal());
      let probes = 0;
      vi.mocked(process.kill).mockImplementation(() => {
        probes += 1;
        if (probes === 1) throw Object.assign(new Error("PRIVATE"), { code: "ESRCH" });
        return true;
      });
      const { commands, spawner } = starter([]);
      const error = yield* connect({ HOME: home }, undefined, undefined, { spawner }).pipe(
        Effect.flip,
      );
      assert.strictEqual(error.category, "network-failed");
      assert.lengthOf(commands, 0);
    }),
  );
  it.effect("attaches to a service registered by someone else before dispatch", () =>
    Effect.gen(function* () {
      const home = temporaryHome();
      writeLedger(home);
      answersAfterStart();
      let probes = 0;
      vi.mocked(process.kill).mockImplementation(() => {
        probes += 1;
        if (probes === 2) writeLedger(home, started);
        throw Object.assign(new Error("PRIVATE"), { code: "ESRCH" });
      });
      const { commands, spawner } = starter([]);
      const result = yield* connect({ HOME: home }, undefined, undefined, { spawner });
      assertAttachedToStarted(result);
      assert.lengthOf(commands, 0);
      assert.strictEqual(probes, 2);
    }),
  );
  it.effect.each([
    {
      name: "a live PID",
      kill: (): true => true,
      fetch: () => Promise.reject(refusal()),
      category: "network-failed",
    },
    {
      name: "EPERM",
      kill: () => {
        throw Object.assign(new Error("PRIVATE"), { code: "EPERM" });
      },
      fetch: () => Promise.reject(refusal()),
      category: "network-failed",
    },
    {
      name: "an authentication failure",
      kill: () => {
        throw Object.assign(new Error("PRIVATE"), { code: "ESRCH" });
      },
      fetch: async () =>
        Response.json({ _tag: "UnauthorizedError", message: "PRIVATE" }, { status: 401 }),
      category: "authentication-failed",
    },
    {
      name: "an identity mismatch",
      kill: () => {
        throw Object.assign(new Error("PRIVATE"), { code: "ESRCH" });
      },
      fetch: async () => Response.json({ ...info, pid: info.pid + 1 }),
      category: "service-identity-mismatch",
    },
    {
      name: "an unsupported server",
      kill: () => {
        throw Object.assign(new Error("PRIVATE"), { code: "ESRCH" });
      },
      fetch: async () => new Response("PRIVATE", { status: 404 }),
      category: "unsupported-server-version",
    },
  ])("never starts for $name", ({ kill, fetch: respond, category }) =>
    Effect.gen(function* () {
      const home = temporaryHome();
      writeLedger(home);
      vi.mocked(process.kill).mockImplementation(kill);
      vi.mocked(fetch).mockImplementation(respond);
      const { commands, spawner } = starter([]);
      const error = yield* connect({ HOME: home }, undefined, undefined, { spawner }).pipe(
        Effect.flip,
      );
      assert.strictEqual(error.category, category);
      assert.lengthOf(commands, 0);
    }),
  );
  it.effect("never starts after a probe timeout", () =>
    Effect.gen(function* () {
      const home = temporaryHome();
      writeLedger(home);
      absentPid();
      vi.mocked(fetch).mockImplementation(
        (_url, options) =>
          new Promise((_resolve, reject) =>
            options?.signal?.addEventListener("abort", () => reject(options.signal!.reason), {
              once: true,
            }),
          ),
      );
      const { commands, spawner } = starter([]);
      const fiber = yield* connect({ HOME: home }, undefined, undefined, { spawner }).pipe(
        Effect.flip,
        Effect.forkChild,
      );
      yield* Effect.yieldNow;
      yield* TestClock.adjust("5 seconds");
      const error = yield* Fiber.join(fiber);
      assert.strictEqual(error.category, "service-probe-timeout");
      assert.lengthOf(commands, 0);
    }),
  );
  it.effect.each(["{PRIVATE_INVALID", JSON.stringify({ ...ledger, pid: -1 })])(
    "never starts for a malformed ledger %#",
    (text) =>
      Effect.gen(function* () {
        const home = temporaryHome();
        writeLedger(home);
        NodeFS.writeFileSync(ledgerPath(home), text);
        const { commands, spawner } = starter([]);
        const error = yield* connect({ HOME: home }, undefined, undefined, { spawner }).pipe(
          Effect.flip,
        );
        assert.strictEqual(error.category, "service-identity-mismatch");
        assert.lengthOf(commands, 0);
      }),
  );
  it.effect("never starts for an explicit server URL", () =>
    Effect.gen(function* () {
      vi.mocked(fetch).mockRejectedValue(refusal());
      const { commands, spawner } = starter([]);
      const error = yield* connect({}, ledger.url, "secret", { spawner }).pipe(Effect.flip);
      assert.strictEqual(error.category, "network-failed");
      assert.lengthOf(commands, 0);
    }),
  );
});
