// @effect-diagnostics nodeBuiltinImport:off
import { assert } from "@effect/vitest";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { Deferred, Effect, Sink, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import * as PromiseService from "@opencode/client/service";
import * as EffectService from "@opencode/client/effect/service";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { afterEach, beforeEach, vi } from "vite-plus/test";
import { OpenCode2Runtime, layer } from "./opencode2Runtime.ts";

import {
  childProcessMethods,
  forbiddenLifecycle,
  forbiddenProcess,
} from "./opencode2Runtime.mocks.ts";

export const ledger = {
  id: "service-1",
  url: "http://127.0.0.1:4096",
  password: "PRIVATE_PASSWORD",
  pid: 56789,
  version: "2.0.15",
};
export const info = {
  pid: ledger.pid,
  version: ledger.version,
  urls: [ledger.url],
  paths: { tmp: "/tmp/opencode" },
};
const homes: string[] = [];
beforeEach(() => {
  for (const module of [PromiseService, EffectService]) {
    for (const [name, value] of Object.entries(module)) {
      if (name === "Service") continue;
      assert.isTrue(vi.isMockFunction(value), `Unmocked service export: ${name}`);
      assert.strictEqual(value, forbiddenLifecycle);
    }
    for (const value of Object.values(module.Service))
      assert.strictEqual(value, forbiddenLifecycle);
  }
  for (const name of Object.keys(childProcessMethods) as (keyof typeof childProcessMethods)[]) {
    assert.isTrue(
      vi.isMockFunction(NodeChildProcess[name]),
      `Unmocked child process method: ${name}`,
    );
    assert.strictEqual(NodeChildProcess[name], forbiddenProcess);
    assert.strictEqual(
      (NodeChildProcess as unknown as { readonly default: typeof NodeChildProcess }).default[name],
      forbiddenProcess,
    );
  }
  assert.strictEqual(forbiddenLifecycle.mock.calls.length, 0);
  assert.strictEqual(forbiddenProcess.mock.calls.length, 0);
  vi.spyOn(process, "kill").mockReturnValue(true);
  vi.stubGlobal(
    "fetch",
    vi.fn<typeof globalThis.fetch>(async () => Response.json(info)),
  );
});
afterEach(() => {
  assert.strictEqual(forbiddenLifecycle.mock.calls.length, 0);
  assert.strictEqual(forbiddenProcess.mock.calls.length, 0);
  for (const call of vi.mocked(process.kill).mock.calls) assert.strictEqual(call[1], 0);
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  for (const home of homes.splice(0)) NodeFS.rmSync(home, { recursive: true, force: true });
});
export function temporaryHome() {
  const home = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "opencode2-attach-"));
  homes.push(home);
  return home;
}
export function ledgerPath(home: string) {
  return NodePath.join(home, ".local", "state", "opencode", "service.json");
}
export function writeLedger(home: string, value: unknown = ledger) {
  const path = ledgerPath(home);
  NodeFS.mkdirSync(NodePath.dirname(path), { recursive: true });
  NodeFS.writeFileSync(path, JSON.stringify(value));
}
export interface ServiceStartScript {
  readonly code?: number;
  readonly error?: unknown;
  readonly gate?: Deferred.Deferred<void>;
  readonly hang?: boolean;
  readonly onSpawn?: () => void;
  /** Holds acquisition open, uninterruptibly like the real spawner's acquireRelease. */
  readonly spawnGate?: Deferred.Deferred<void>;
  readonly stdout?: string;
}
/** Records every spawn. Scripts run in order; an unscripted spawn fails the test. */
export function serviceSpawner(scripts: ReadonlyArray<ServiceStartScript> = []) {
  const commands: ChildProcess.StandardCommand[] = [];
  const released: number[] = [];
  const unreferenced: number[] = [];
  const events: string[] = [];
  const spawner = ChildProcessSpawner.make((command) =>
    Effect.gen(function* () {
      if (!ChildProcess.isStandardCommand(command)) return yield* Effect.die("Unexpected pipeline");
      const index = commands.length;
      commands.push(command);
      const script = scripts[index];
      if (script === undefined) return yield* Effect.die("Unexpected service spawn");
      if (script.error !== undefined) return yield* Effect.fail(script.error as never);
      script.onSpawn?.();
      // Like the real spawner's acquireRelease, acquisition and release
      // registration are one uninterruptible step.
      yield* Effect.uninterruptible(
        Effect.gen(function* () {
          if (script.spawnGate !== undefined) yield* Deferred.await(script.spawnGate);
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              released.push(index);
              events.push("release");
            }),
          );
        }),
      );
      const exited = Effect.succeed(ChildProcessSpawner.ExitCode(script.code ?? 0));
      return ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(100 + index),
        exitCode: script.hang
          ? Effect.never
          : script.gate === undefined
            ? exited
            : Deferred.await(script.gate).pipe(Effect.andThen(exited)),
        isRunning: Effect.succeed(false),
        kill: () => Effect.die("No explicit kill is permitted"),
        unref: Effect.sync(() => {
          unreferenced.push(index);
          events.push("unref");
          return Effect.void;
        }),
        stdin: Sink.drain,
        stdout: Stream.make(new TextEncoder().encode(script.stdout ?? "")),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      });
    }),
  );
  return { commands, events, released, spawner, unreferenced };
}
export const provideRuntimeDependencies =
  (
    spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
    platform: NodeJS.Platform = "linux",
  ) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Effect.provideService(HostProcessPlatform, platform),
    );
export const connect = (
  environment: NodeJS.ProcessEnv,
  serverUrl?: string,
  serverPassword = "PRIVATE_PASSWORD",
  options: {
    readonly binaryPath?: string;
    readonly spawner?: ChildProcessSpawner.ChildProcessSpawner["Service"];
  } = {},
) =>
  Effect.gen(function* () {
    const runtime = yield* OpenCode2Runtime;
    return yield* runtime.connectToOpenCodeServer({
      environment,
      ...(options.binaryPath === undefined ? {} : { binaryPath: options.binaryPath }),
      ...(serverUrl === undefined ? {} : { serverUrl, serverPassword }),
    });
  }).pipe(
    Effect.provide(layer),
    Effect.scoped,
    provideRuntimeDependencies(options.spawner ?? serviceSpawner().spawner),
  );
export function refusal(address = "127.0.0.1", port = 4096) {
  return new TypeError("PRIVATE_FETCH", {
    cause: Object.assign(new Error("PRIVATE_REFUSAL"), {
      code: "ECONNREFUSED",
      syscall: "connect",
      address,
      port,
    }),
  });
}
