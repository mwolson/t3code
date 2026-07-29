/**
 * Runtime for OpenCode 2.x ("OpenCode 2"), which is a different server than
 * the 1.x one `opencodeRuntime.ts` drives, not a newer build of it.
 *
 * Three differences make a separate runtime necessary rather than a flag:
 *
 *   1. **Startup banner.** 1.x prints `opencode server listening on <url>`;
 *      2.x prints `server listening on <url>`. The 1.x prefix match never fires
 *      and the spawn times out.
 *   2. **Mandatory auth.** 1.x serves unauthenticated and warns about it. 2.x
 *      always mints a password, prints it on stdout beside the URL, and 401s
 *      without it. So startup has to resolve *two* facts, not one.
 *   3. **Route surface.** 2.x serves only `/api/*`. The SDK is versioned to
 *      match and is pinned here under the `@opencode-ai/sdk-next` alias, since
 *      two majors of one package name cannot coexist. Its `client.v2.*`
 *      namespace is the `/api/*` one; `client.session.*` is the legacy surface
 *      and 404s against a 2.x server.
 *
 * `live-scenarios/tests/opencode2-drive-probe.mjs` in the parent workspace
 * exercises this contract against a real binary and fails first if 2.x moves.
 */
import { createOpencodeClient, type OpencodeClient } from "@opencode-ai/sdk-next/v2";
import * as NetService from "@t3tools/shared/Net";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as P from "effect/Predicate";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { openCodeRuntimeErrorDetail } from "./opencodeRuntime.ts";

const DEFAULT_OPENCODE2_SERVER_TIMEOUT_MS = 30_000;
const DEFAULT_HOSTNAME = "127.0.0.1";

const OPENCODE2_RUNTIME_ERROR_TAG = "OpenCode2RuntimeError";
export class OpenCode2RuntimeError extends Data.TaggedError(OPENCODE2_RUNTIME_ERROR_TAG)<{
  readonly operation: string;
  readonly cause?: unknown;
  readonly detail: string;
}> {
  static readonly is = (u: unknown): u is OpenCode2RuntimeError =>
    P.isTagged(u, OPENCODE2_RUNTIME_ERROR_TAG);
}

export const runOpenCode2Sdk = <A>(
  operation: string,
  fn: () => Promise<A>,
): Effect.Effect<A, OpenCode2RuntimeError> =>
  Effect.tryPromise({
    try: fn,
    catch: (cause) =>
      new OpenCode2RuntimeError({ operation, detail: openCodeRuntimeErrorDetail(cause), cause }),
  }).pipe(Effect.withSpan(`opencode2.${operation}`));

/** Both facts the 2.x startup banner carries. Neither is optional. */
export interface OpenCode2ServerCredentials {
  readonly url: string;
  readonly password: string;
}

export interface OpenCode2ServerProcess extends OpenCode2ServerCredentials {
  readonly exitCode: Effect.Effect<number, never>;
}

export interface OpenCode2ServerConnection extends OpenCode2ServerCredentials {
  readonly exitCode: Effect.Effect<number, never> | null;
  readonly external: boolean;
}

/**
 * Read the URL and password out of accumulated server output.
 *
 * Returns `null` until *both* are present: they arrive on separate lines and a
 * chunked read can see one without the other, and a client built without the
 * password gets 401 on every call. Deliberately not anchored to line start —
 * 2.x has changed the surrounding banner text before.
 *
 * @internal exported for tests
 */
export function parseOpenCode2Startup(output: string): OpenCode2ServerCredentials | null {
  const url = output.match(/server listening on\s+(https?:\/\/\S+)/)?.[1];
  const password = output.match(/server password\s+(\S+)/)?.[1];
  return url && password ? { url, password } : null;
}

/** The header the 2.x server checks. Username is fixed; only the password varies. */
export function openCode2AuthorizationHeader(password: string): string {
  return `Basic ${Buffer.from(`opencode:${password}`, "utf8").toString("base64")}`;
}

export interface OpenCode2RuntimeShape {
  /**
   * Spawn a local 2.x server. Lifetime is bound to the caller's scope, so the
   * child dies when that scope closes.
   */
  readonly startOpenCode2ServerProcess: (input: {
    readonly binaryPath: string;
    readonly environment?: NodeJS.ProcessEnv;
    readonly port?: number;
    readonly hostname?: string;
    readonly timeoutMs?: number;
  }) => Effect.Effect<OpenCode2ServerProcess, OpenCode2RuntimeError, Scope.Scope>;
  /**
   * Connect to an externally-managed server, or spawn one. An external server
   * must carry its own password: 2.x has no unauthenticated mode, so there is
   * nothing to fall back to.
   */
  readonly connectToOpenCode2Server: (input: {
    readonly binaryPath: string;
    readonly serverUrl?: string | null;
    readonly serverPassword?: string | null;
    readonly environment?: NodeJS.ProcessEnv;
    readonly port?: number;
    readonly hostname?: string;
    readonly timeoutMs?: number;
  }) => Effect.Effect<OpenCode2ServerConnection, OpenCode2RuntimeError, Scope.Scope>;
  readonly createOpenCode2SdkClient: (input: {
    readonly baseUrl: string;
    readonly directory: string;
    readonly serverPassword: string;
  }) => OpencodeClient;
}

export function escalateOpenCode2ServerTermination(
  kill: (signal: NodeJS.Signals) => Effect.Effect<void>,
): Effect.Effect<void, never> {
  return kill("SIGTERM").pipe(
    Effect.andThen(Effect.sleep("1 second")),
    Effect.andThen(kill("SIGKILL")),
  );
}

const makeOpenCode2Runtime = Effect.gen(function* () {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const netService = yield* NetService.NetService;
  const hostPlatform = yield* HostProcessPlatform;

  const resolveCommand = (command: string, args: ReadonlyArray<string>, env?: NodeJS.ProcessEnv) =>
    resolveSpawnCommand(command, args, env ? { env } : {});

  const startOpenCode2ServerProcess: OpenCode2RuntimeShape["startOpenCode2ServerProcess"] = (
    input,
  ) =>
    Effect.gen(function* () {
      const runtimeScope = yield* Effect.scope;
      const hostname = input.hostname ?? DEFAULT_HOSTNAME;
      const port =
        input.port ??
        (yield* netService.findAvailablePort(0).pipe(
          Effect.mapError(
            (cause) =>
              new OpenCode2RuntimeError({
                operation: "startOpenCode2ServerProcess",
                detail: `Failed to find available port: ${openCodeRuntimeErrorDetail(cause)}`,
                cause,
              }),
          ),
        ));
      const timeoutMs = input.timeoutMs ?? DEFAULT_OPENCODE2_SERVER_TIMEOUT_MS;
      const args = ["serve", `--hostname=${hostname}`, `--port=${port}`];
      const spawnCommand = yield* resolveCommand(input.binaryPath, args, input.environment);

      const child = yield* spawner
        .spawn(
          ChildProcess.make(spawnCommand.command, spawnCommand.args, {
            detached: hostPlatform !== "win32",
            shell: spawnCommand.shell,
            ...(input.environment === undefined ? {} : { env: input.environment }),
            extendEnv: input.environment === undefined,
          }),
        )
        .pipe(
          Effect.provideService(Scope.Scope, runtimeScope),
          Effect.mapError(
            (cause) =>
              new OpenCode2RuntimeError({
                operation: "startOpenCode2ServerProcess",
                detail: `Failed to spawn OpenCode 2 server process: ${openCodeRuntimeErrorDetail(cause)}`,
                cause,
              }),
          ),
        );

      const killOpenCode2ProcessGroup = (signal: NodeJS.Signals) =>
        hostPlatform === "win32"
          ? child
              .kill({ killSignal: signal, forceKillAfter: "1 second" })
              .pipe(Effect.asVoid, Effect.ignore)
          : Effect.sync(() => {
              try {
                process.kill(-Number(child.pid), signal);
              } catch {
                // The direct child may already have exited after starting the
                // server. The group kill still owns any descendants it left.
              }
            });
      const outputRef = yield* Ref.make("");
      const readyDeferred = yield* Deferred.make<
        OpenCode2ServerCredentials,
        OpenCode2RuntimeError
      >();

      // The banner is two lines and the split between them lands wherever the
      // pipe happens to break, so accumulate and re-parse the whole buffer.
      const absorb = (chunk: string) =>
        Ref.updateAndGet(outputRef, (previous) => `${previous}${chunk}`).pipe(
          Effect.flatMap((output) => {
            const parsed = parseOpenCode2Startup(output);
            return parsed
              ? Deferred.succeed(readyDeferred, parsed).pipe(Effect.ignore)
              : Effect.void;
          }),
        );

      const stdoutFiber = yield* child.stdout.pipe(
        Stream.decodeText(),
        Stream.runForEach(absorb),
        Effect.ignore,
        Effect.forkIn(runtimeScope),
      );
      // 2.x has printed the banner to stderr across builds, so watch both.
      const stderrFiber = yield* child.stderr.pipe(
        Stream.decodeText(),
        Stream.runForEach(absorb),
        Effect.ignore,
        Effect.forkIn(runtimeScope),
      );

      const exitFiber = yield* child.exitCode.pipe(
        Effect.flatMap((code) =>
          Effect.gen(function* () {
            const output = yield* Ref.get(outputRef);
            const exitCode = Number(code);
            yield* Deferred.fail(
              readyDeferred,
              new OpenCode2RuntimeError({
                operation: "startOpenCode2ServerProcess",
                detail: [
                  `OpenCode 2 server exited before startup completed (code: ${String(exitCode)}).`,
                  output.trim() ? `output:\n${output.trim()}` : null,
                ]
                  .filter(Boolean)
                  .join("\n\n"),
                cause: { exitCode, output },
              }),
            ).pipe(Effect.ignore);
          }),
        ),
        Effect.ignore,
        Effect.forkIn(runtimeScope),
      );

      // Register this after the exit observer so LIFO scope teardown terminates
      // the server before waiting for that observer to finish. Register before
      // awaiting readiness so startup failures receive the same cleanup.
      yield* Scope.addFinalizer(
        runtimeScope,
        escalateOpenCode2ServerTermination(killOpenCode2ProcessGroup),
      );

      const readyExit = yield* Effect.exit(
        Deferred.await(readyDeferred).pipe(Effect.timeoutOption(timeoutMs)),
      );

      yield* Fiber.interrupt(stdoutFiber).pipe(Effect.ignore);
      yield* Fiber.interrupt(stderrFiber).pipe(Effect.ignore);

      if (Exit.isFailure(readyExit)) {
        yield* Fiber.interrupt(exitFiber).pipe(Effect.ignore);
        const squashed = Cause.squash(readyExit.cause);
        return yield* new OpenCode2RuntimeError({
          operation: "startOpenCode2ServerProcess",
          detail: `Failed while waiting for OpenCode 2 server startup: ${openCodeRuntimeErrorDetail(squashed)}`,
          cause: squashed,
        });
      }

      const ready = readyExit.value;
      if (Option.isNone(ready)) {
        yield* Fiber.interrupt(exitFiber).pipe(Effect.ignore);
        const output = yield* Ref.get(outputRef);
        return yield* new OpenCode2RuntimeError({
          operation: "startOpenCode2ServerProcess",
          detail: [
            `Timed out waiting for OpenCode 2 server start after ${timeoutMs}ms.`,
            // Without this the usual cause, a banner that moved again, is
            // invisible and looks like a hang.
            output.trim() ? `output:\n${output.trim()}` : null,
          ]
            .filter(Boolean)
            .join("\n\n"),
        });
      }

      return {
        url: ready.value.url,
        password: ready.value.password,
        exitCode: child.exitCode.pipe(
          Effect.map(Number),
          Effect.orElseSucceed(() => 0),
        ),
      } satisfies OpenCode2ServerProcess;
    });

  const connectToOpenCode2Server: OpenCode2RuntimeShape["connectToOpenCode2Server"] = (input) => {
    const serverUrl = input.serverUrl?.trim();
    if (serverUrl) {
      const serverPassword = input.serverPassword?.trim();
      if (!serverPassword) {
        return new OpenCode2RuntimeError({
          operation: "connectToOpenCode2Server",
          detail:
            "An external OpenCode 2 server requires a server password: 2.x rejects unauthenticated requests with 401.",
        });
      }
      // Not ours, so no lifetime is attached to the caller's scope.
      return Effect.succeed({
        url: serverUrl,
        password: serverPassword,
        exitCode: null,
        external: true,
      });
    }

    return startOpenCode2ServerProcess({
      binaryPath: input.binaryPath,
      ...(input.environment !== undefined ? { environment: input.environment } : {}),
      ...(input.port !== undefined ? { port: input.port } : {}),
      ...(input.hostname !== undefined ? { hostname: input.hostname } : {}),
      ...(input.timeoutMs !== undefined ? { timeoutMs: input.timeoutMs } : {}),
    }).pipe(
      Effect.map((server) => ({
        url: server.url,
        password: server.password,
        exitCode: server.exitCode,
        external: false,
      })),
    );
  };

  const createOpenCode2SdkClient: OpenCode2RuntimeShape["createOpenCode2SdkClient"] = (input) =>
    createOpencodeClient({
      baseUrl: input.baseUrl,
      directory: input.directory,
      headers: { Authorization: openCode2AuthorizationHeader(input.serverPassword) },
      throwOnError: true,
    });

  return {
    startOpenCode2ServerProcess,
    connectToOpenCode2Server,
    createOpenCode2SdkClient,
  } satisfies OpenCode2RuntimeShape;
});

export class OpenCode2Runtime extends Context.Service<OpenCode2Runtime, OpenCode2RuntimeShape>()(
  "t3/provider/opencode2Runtime",
) {}

export const OpenCode2RuntimeLive = Layer.effect(OpenCode2Runtime, makeOpenCode2Runtime).pipe(
  Layer.provide(NetService.layer),
);
