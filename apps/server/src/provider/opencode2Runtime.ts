// @effect-diagnostics nodeBuiltinImport:off
import {
  ClientError,
  isForbiddenError,
  isUnauthorizedError,
  OpenCode,
  type OpenCodeClient,
} from "@opencode/client";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { stripAppImageRuntimeEnv } from "../process/appImageEnvironment.ts";
import { isWindowsCommandNotFound } from "../processRunner.ts";
import { collectUint8StreamText } from "../stream/collectUint8StreamText.ts";
import { isCommandMissingCause } from "./providerSnapshot.ts";

const OPENCODE_VERSION = "2.0.15";

export const OpenCodeRuntimeOperation = Schema.Literals([
  "agent.list",
  "command.list",
  "connectToOpenCodeServer",
  "event.subscribe",
  "generate.text",
  "integration.list",
  "mcp.add",
  "mcp.list",
  "message.list",
  "model.list",
  "server.info",
  "service.start",
  "session.command",
  "session.compact",
  "session.context",
  "session.create",
  "session.fork",
  "session.form.reply",
  "session.generate",
  "session.get",
  "session.list",
  "session.inbox.list",
  "session.instructions.entry.put",
  "session.interrupt",
  "session.pending.list",
  "session.permission.reply",
  "session.prompt",
  "session.question.reply",
  "session.remove",
  "session.revert.commit",
  "session.revert.stage",
  "session.switchAgent",
  "session.switchModel",
  "session.wait",
  "shell.create",
  "shell.list",
  "shell.output",
  "shell.remove",
  "skill.list",
]);
export type OpenCodeRuntimeOperation = typeof OpenCodeRuntimeOperation.Type;

export const OpenCodeRuntimeErrorCategory = Schema.Literals([
  "authentication-failed",
  "binary-not-found",
  "event-subscription-failed",
  "external-server-password-required",
  "invalid-server-url",
  "mcp-connect-failed",
  "mcp-connect-timeout",
  "missing-response-payload",
  "model-unavailable",
  "network-failed",
  "replay-boundary",
  "sdk-request-failed",
  "service-credentials-required",
  "service-identity-mismatch",
  "service-not-running",
  "service-probe-timeout",
  "service-start-failed",
  "service-start-timeout",
  "session-remove-failed",
  "unsupported-server-version",
]);
export type OpenCodeRuntimeErrorCategory = typeof OpenCodeRuntimeErrorCategory.Type;

export class OpenCodeRuntimeError extends Schema.TaggedError<OpenCodeRuntimeError>()(
  "OpenCodeRuntimeError",
  {
    category: OpenCodeRuntimeErrorCategory,
    cause: Schema.optional(Schema.Defect()),
    exitCode: Schema.optionalKey(Schema.Number),
    operation: OpenCodeRuntimeOperation,
    timeoutMs: Schema.optionalKey(Schema.Number),
  },
) {
  override get message(): string {
    const exitContext = this.exitCode === undefined ? "" : `, exit code ${this.exitCode}`;
    const timeoutContext = this.timeoutMs === undefined ? "" : `, timeout ${this.timeoutMs}ms`;
    return `OpenCode 2 ${this.operation} failed (${this.category}${exitContext}${timeoutContext}).`;
  }
}

export const isOpenCodeRuntimeError = Schema.is(OpenCodeRuntimeError);

export const loadOpenCodeCommands = (client: OpenCodeClient, directory: string) =>
  runOpenCodeSdk("command.list", (signal) =>
    client.command.list({ location: { directory } }, { signal }),
  ).pipe(Effect.map((response) => response.data));

export function parseOpenCodeModelSlug(
  slug: string | null | undefined,
): { readonly providerID: string; readonly modelID: string } | null {
  if (typeof slug !== "string") return null;
  const trimmed = slug.trim();
  const separator = trimmed.indexOf("/");
  if (separator <= 0 || separator === trimmed.length - 1) return null;
  return { providerID: trimmed.slice(0, separator), modelID: trimmed.slice(separator + 1) };
}

function openCode2SdkErrorCategory(
  cause: unknown,
): Extract<
  OpenCodeRuntimeErrorCategory,
  "authentication-failed" | "model-unavailable" | "network-failed" | "sdk-request-failed"
> {
  if (isUnauthorizedError(cause) || isForbiddenError(cause)) return "authentication-failed";
  if (cause instanceof ClientError) {
    if (cause.reason === "Transport") return "network-failed";
    if (
      cause.reason === "UnexpectedStatus" &&
      Predicate.isObject(cause.cause) &&
      (cause.cause.status === 401 || cause.cause.status === 403)
    )
      return "authentication-failed";
    return "sdk-request-failed";
  }
  if (isOpenCodeRuntimeError(cause))
    return cause.category === "model-unavailable" ? "model-unavailable" : "sdk-request-failed";
  if (!(cause instanceof Error)) {
    if (
      Predicate.isObject(cause) &&
      typeof cause.message === "string" &&
      cause.message.slice(0, 18).toLowerCase() === "model unavailable:"
    )
      return "model-unavailable";
    return "sdk-request-failed";
  }
  if (typeof cause.message !== "string") return "sdk-request-failed";
  const detail = cause.message.trim().toLowerCase();
  if (detail.startsWith("model unavailable:")) return "model-unavailable";
  if (
    detail.includes("401") ||
    detail.includes("403") ||
    detail.includes("unauthorized") ||
    detail.includes("forbidden")
  )
    return "authentication-failed";
  if (
    detail.includes("econnrefused") ||
    detail.includes("enotfound") ||
    detail.includes("fetch failed") ||
    detail.includes("networkerror") ||
    detail.includes("socket hang up") ||
    detail.includes("timed out") ||
    detail.includes("timeout")
  )
    return "network-failed";
  return "sdk-request-failed";
}

export const runOpenCodeSdk = <A>(
  operation: OpenCodeRuntimeOperation,
  fn: (signal: AbortSignal) => Promise<A>,
): Effect.Effect<A, OpenCodeRuntimeError> =>
  Effect.tryPromise({
    try: fn,
    catch: (cause) =>
      new OpenCodeRuntimeError({
        operation,
        category: openCode2SdkErrorCategory(cause),
        cause,
      }),
  }).pipe(Effect.withSpan(`opencode2.${operation}`));

export interface OpenCodeServerCredentials {
  readonly url: string;
  readonly password: string;
}

export interface OpenCodeServerConnection extends OpenCodeServerCredentials {
  readonly exitCode: Effect.Effect<number, never> | null;
  readonly external: boolean;
}

export class OpenCode2Runtime extends Context.Service<
  OpenCode2Runtime,
  {
    /**
     * Attaches to an explicit server, or else to the user's host service. When
     * that service is positively absent, starts it once with
     * `opencode service start` and attaches only after the same identity checks.
     */
    readonly connectToOpenCodeServer: (input: {
      readonly binaryPath?: string | null;
      readonly serverUrl?: string | null;
      readonly serverPassword?: string | null;
      readonly environment?: NodeJS.ProcessEnv;
    }) => Effect.Effect<OpenCodeServerConnection, OpenCodeRuntimeError>;
    readonly createOpenCodeSdkClient: (input: {
      readonly baseUrl: string;
      readonly directory: string;
      readonly serverPassword: string;
    }) => OpenCodeClient;
  }
>()("t3/provider/opencode2Runtime") {}

export interface OpenCodeHostService extends OpenCodeServerCredentials {
  readonly id: string;
  readonly pid: number;
  readonly version: string;
}

/** Ignore legacy T3 isolate overrides when locating the user-owned ledger. */
export function openCodeHostStateHome(environment: NodeJS.ProcessEnv = process.env): string {
  const xdg = environment.XDG_STATE_HOME?.trim();
  if (xdg && !xdg.includes("t3-opencode2-state")) return xdg;
  return NodePath.join(environment.HOME?.trim() || NodeOS.homedir(), ".local", "state");
}

/** Only ENOENT proves missing state; unreadable or malformed state fails closed. */
export function readOpenCodeHostService(
  environment: NodeJS.ProcessEnv = process.env,
): OpenCodeHostService | null {
  let raw: string;
  try {
    raw = NodeFS.readFileSync(
      NodePath.join(openCodeHostStateHome(environment), "opencode", "service.json"),
      "utf8",
    );
  } catch (cause) {
    if (Predicate.isObject(cause) && cause.code === "ENOENT") return null;
    throw cause;
  }
  const value: unknown = JSON.parse(raw);
  if (hasUnsupportedVersion(value)) throw unavailable("unsupported-server-version");
  const ledger = decodeOpenCodeHostServiceLedger(value);
  if (!ledger.password?.trim()) throw unavailable("service-credentials-required");
  if (!isHttpUrl(ledger.url)) throw unavailable("service-identity-mismatch");
  return { ...ledger, password: ledger.password };
}

const decodeOpenCodeHostServiceLedger = Schema.decodeUnknownSync(
  Schema.Struct({
    id: Schema.NonEmptyString,
    url: Schema.NonEmptyString,
    password: Schema.optionalKey(Schema.String),
    pid: Schema.Int.check(Schema.isGreaterThan(0)),
    version: Schema.NonEmptyString,
  }),
);

const loadOpenCodeHostService = (environment: NodeJS.ProcessEnv) =>
  Effect.try({
    try: () => readOpenCodeHostService(environment),
    catch: (cause) =>
      isOpenCodeRuntimeError(cause) ? cause : unavailable("service-identity-mismatch", cause),
  });

function unavailable(category: OpenCodeRuntimeErrorCategory, cause?: unknown) {
  return new OpenCodeRuntimeError({ operation: "connectToOpenCodeServer", category, cause });
}

function isHttpUrl(value: string) {
  return URL.canParse(value) && ["http:", "https:"].includes(new URL(value).protocol);
}

function hasUnsupportedVersion(value: unknown) {
  return (
    Predicate.isObject(value) &&
    typeof value.version === "string" &&
    value.version !== OPENCODE_VERSION
  );
}

const isServerInfo = Schema.is(
  Schema.Struct({
    pid: Schema.Int.check(Schema.isGreaterThan(0)),
    version: Schema.NonEmptyString,
    urls: Schema.Array(Schema.String),
    paths: Schema.Struct({ tmp: Schema.String }),
  }),
);
const infoTimeoutMs = 5_000;

const verifyOpenCodeService = Effect.fn("OpenCode2Runtime.verifyOpenCodeService")(function* (
  credentials: OpenCodeServerCredentials,
  ledger?: OpenCodeHostService,
) {
  if (ledger !== undefined && ledger.version !== OPENCODE_VERSION)
    return yield* unavailable("unsupported-server-version");
  const client = OpenCode.make({
    baseUrl: credentials.url,
    headers: { Authorization: openCodeAuthorizationHeader(credentials.password) },
  });
  const info = yield* runOpenCodeSdk("server.info", (signal) =>
    client.server.info({ signal }),
  ).pipe(
    Effect.mapError((error) => {
      const cause = error.cause;
      if (
        cause instanceof ClientError &&
        cause.reason === "UnexpectedStatus" &&
        Predicate.isObject(cause.cause) &&
        cause.cause.status === 404
      )
        return unavailable("unsupported-server-version", cause);
      return error;
    }),
    Effect.timeoutOrElse({
      duration: infoTimeoutMs,
      orElse: () =>
        Effect.fail(
          new OpenCodeRuntimeError({
            operation: "server.info",
            category: "service-probe-timeout",
            timeoutMs: infoTimeoutMs,
          }),
        ),
    }),
  );
  if (hasUnsupportedVersion(info)) return yield* unavailable("unsupported-server-version");
  if (!isServerInfo(info)) return yield* unavailable("service-identity-mismatch");
  if (ledger !== undefined && info.pid !== ledger.pid)
    return yield* unavailable("service-identity-mismatch");
  return info;
});

function isLocalConnectionRefusal(error: OpenCodeRuntimeError, ledger: OpenCodeHostService) {
  const url = new URL(ledger.url);
  if (url.hostname !== "127.0.0.1" && url.hostname !== "[::1]") return false;
  const clientError = error.cause;
  if (!(clientError instanceof ClientError) || clientError.reason !== "Transport") return false;
  const fetchError = clientError.cause;
  if (!(fetchError instanceof TypeError)) return false;
  const refusal = fetchError.cause;
  const address = url.hostname === "[::1]" ? "::1" : url.hostname;
  const defaultPort = url.protocol === "https:" ? 443 : 80;
  const port = url.port === "" ? defaultPort : Number(url.port);
  return (
    refusal instanceof Error &&
    "code" in refusal &&
    refusal.code === "ECONNREFUSED" &&
    "syscall" in refusal &&
    refusal.syscall === "connect" &&
    "address" in refusal &&
    refusal.address === address &&
    "port" in refusal &&
    refusal.port === port
  );
}

const isAbsentOpenCodeProcess = (pid: number) =>
  Effect.try({
    try: () => {
      process.kill(pid, 0);
      return false;
    },
    catch: (cause) => Predicate.isObject(cause) && cause.code === "ESRCH",
  }).pipe(Effect.match({ onSuccess: () => false, onFailure: (absent) => absent }));

const recheckLedger = Effect.fn("OpenCode2Runtime.recheckLedger")(function* (
  environment: NodeJS.ProcessEnv,
  expected: OpenCodeHostService,
) {
  const current = yield* loadOpenCodeHostService(environment);
  if (
    current?.id !== expected.id ||
    current?.url !== expected.url ||
    current?.password !== expected.password ||
    current?.pid !== expected.pid ||
    current?.version !== expected.version
  )
    return yield* unavailable("service-identity-mismatch");
});

function sameLedger(left: OpenCodeHostService, right: OpenCodeHostService) {
  return (
    left.id === right.id &&
    left.url === right.url &&
    left.password === right.password &&
    left.pid === right.pid &&
    left.version === right.version
  );
}

/** Absence must still hold at dispatch; `null` means no ledger was registered. */
const isStillAbsent = Effect.fn("OpenCode2Runtime.isStillAbsent")(function* (
  environment: NodeJS.ProcessEnv,
  expected: OpenCodeHostService | null,
) {
  if (expected !== null && !(yield* isAbsentOpenCodeProcess(expected.pid))) return false;
  const current = yield* loadOpenCodeHostService(environment);
  if (expected === null || current === null) return current === expected;
  return sameLedger(current, expected);
});

/** The last http(s) URL line `opencode service start` prints. */
function parseStartedServiceUrl(stdout: string): string | null {
  for (const line of stdout.trim().split(/\r?\n/u).toReversed()) {
    const candidate = line.trim();
    if (isHttpUrl(candidate)) return new URL(candidate).href;
  }
  return null;
}

/**
 * The started service outlives this server, so it must not inherit what only
 * describes the T3 process: the AppImage mount, Electron's Node mode, or T3's
 * own configuration and credentials.
 */
function openCodeServiceEnvironment(environment: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const rest = Object.fromEntries(
    Object.entries(stripAppImageRuntimeEnv(environment)).filter(([key]) => {
      const name = key.toUpperCase();
      return name !== "ELECTRON_RUN_AS_NODE" && !name.startsWith("T3CODE_");
    }),
  );
  return rest.XDG_STATE_HOME?.includes("t3-opencode2-state")
    ? { ...rest, XDG_STATE_HOME: openCodeHostStateHome(environment) }
    : rest;
}

function startFailure(category: OpenCodeRuntimeErrorCategory, cause?: unknown, exitCode?: number) {
  return new OpenCodeRuntimeError({
    operation: "service.start",
    category,
    cause,
    ...(exitCode === undefined ? {} : { exitCode }),
  });
}

const serviceStartTimeoutMs = 30_000;

export function openCodeAuthorizationHeader(password: string): string {
  return `Basic ${Buffer.from(`opencode:${password}`, "utf8").toString("base64")}`;
}

/** The server's synthetic default is not an explicit model variant. */
const OPENCODE_DEFAULT_VARIANT = "default";
export function normalizeOpenCodeVariant(variant: string | undefined): string | undefined {
  return variant === OPENCODE_DEFAULT_VARIANT ? undefined : variant;
}

const make = Effect.gen(function* () {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const runtimeScope = yield* Effect.scope;
  let pendingStart: Deferred.Deferred<void, OpenCodeRuntimeError> | undefined;

  const runServiceStart = Effect.fn("OpenCode2Runtime.runServiceStart")(function* (
    command: ChildProcess.StandardCommand,
  ) {
    // Windows can stop a running starter only with `taskkill /T`, which also
    // ends the detached service it launched. There T3 unreferences the starter
    // so the spawner leaves it running when T3 stops waiting; OpenCode bounds
    // `service start` itself (120 s). Spawning and unreferencing happen without
    // interruption, so a timeout or shutdown cannot release the starter first.
    // Once the starter has exited, even nonzero, the spawner's own `taskkill`
    // finds no process and the service survives.
    const unreference = (yield* HostProcessPlatform) === "win32";
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const child = yield* spawner.spawn(command);
        if (unreference) yield* Effect.asVoid(child.unref);
        const [stdout, stderr, code] = yield* restore(
          Effect.all(
            [
              collectUint8StreamText({ stream: child.stdout }),
              collectUint8StreamText({ stream: child.stderr }),
              child.exitCode.pipe(Effect.map(Number)),
            ],
            { concurrency: "unbounded" },
          ),
        );
        if (yield* isWindowsCommandNotFound(code, stderr.text))
          return yield* startFailure("binary-not-found", undefined, code);
        return { code, stdout: stdout.text };
      }),
    );
  }, Effect.scoped);

  /**
   * Runs `opencode service start` at most once. Its `Service.ensure` may
   * replace a registration it finds incompatible, and T3 cannot compare and
   * start atomically, so the caller must have proven absence first and the
   * recheck here only narrows that window. T3 never stops, restarts or signals
   * the service; the command's own daemon is detached and outlives it.
   */
  const startOpenCodeHostService = Effect.fn("OpenCode2Runtime.startOpenCodeHostService")(
    function* (input: {
      readonly binaryPath: string;
      readonly environment: NodeJS.ProcessEnv;
      readonly expected: OpenCodeHostService | null;
    }) {
      const env = openCodeServiceEnvironment(input.environment);
      const command = yield* resolveSpawnCommand(input.binaryPath, ["service", "start"], {
        env,
      }).pipe(Effect.mapError((cause) => startFailure("service-start-failed", cause)));
      if (!(yield* isStillAbsent(input.environment, input.expected))) return;
      const result = yield* runServiceStart(
        ChildProcess.make(command.command, command.args, {
          env,
          extendEnv: false,
          forceKillAfter: 1_000,
          shell: command.shell,
        }),
      ).pipe(
        Effect.mapError((cause) =>
          isOpenCodeRuntimeError(cause)
            ? cause
            : startFailure(
                isCommandMissingCause(cause) ? "binary-not-found" : "service-start-failed",
                cause,
              ),
        ),
      );
      if (result.code !== 0)
        return yield* startFailure("service-start-failed", undefined, result.code);
      const printedUrl = parseStartedServiceUrl(result.stdout);
      const started = yield* loadOpenCodeHostService(input.environment);
      if (printedUrl === null || started === null)
        return yield* startFailure("service-start-failed");
      if (new URL(started.url).href !== printedUrl)
        return yield* startFailure("service-identity-mismatch");
    },
    Effect.timeoutOrElse({
      duration: serviceStartTimeoutMs,
      orElse: () =>
        Effect.fail(
          new OpenCodeRuntimeError({
            operation: "service.start",
            category: "service-start-timeout",
            timeoutMs: serviceStartTimeoutMs,
          }),
        ),
    }),
  );

  /**
   * Concurrent callers share one start, which a caller's interruption does not
   * cancel. The fiber observer settles the start even when the runtime scope
   * closes before the fiber runs.
   */
  const startOnce = (input: Parameters<typeof startOpenCodeHostService>[0]) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.suspend(() => {
        if (pendingStart !== undefined) return restore(Deferred.await(pendingStart));
        const pending = Deferred.makeUnsafe<void, OpenCodeRuntimeError>();
        pendingStart = pending;
        return startOpenCodeHostService(input).pipe(
          Effect.forkIn(runtimeScope),
          Effect.tap((fiber) =>
            Effect.sync(() =>
              fiber.addObserver((exit) => {
                if (pendingStart === pending) pendingStart = undefined;
                Deferred.doneUnsafe(pending, exit);
              }),
            ),
          ),
          Effect.andThen(restore(Deferred.await(pending))),
        );
      }),
    );

  const startAndLoad = Effect.fn("OpenCode2Runtime.startAndLoad")(function* (
    input: Parameters<typeof startOpenCodeHostService>[0],
  ) {
    yield* startOnce(input);
    const host = yield* loadOpenCodeHostService(input.environment);
    if (host === null) return yield* unavailable("service-not-running");
    yield* verifyOpenCodeService(host, host);
    return host;
  });

  const connectToOpenCodeServer: OpenCode2Runtime["Service"]["connectToOpenCodeServer"] = Effect.fn(
    "OpenCode2Runtime.connectToOpenCodeServer",
  )(function* (input) {
    const serverUrl = input.serverUrl?.trim();
    if (serverUrl) {
      if (!isHttpUrl(serverUrl)) return yield* unavailable("invalid-server-url");
      const password = input.serverPassword?.trim();
      if (!password) return yield* unavailable("external-server-password-required");
      const credentials = { url: serverUrl, password };
      const initial = yield* verifyOpenCodeService(credentials);
      const current = yield* verifyOpenCodeService(credentials);
      if (current.pid !== initial.pid) return yield* unavailable("service-identity-mismatch");
      return { ...credentials, exitCode: null, external: true };
    }
    const environment = input.environment ?? process.env;
    const binaryPath = input.binaryPath?.trim() || "opencode";
    const registered = yield* loadOpenCodeHostService(environment);
    const host =
      registered === null
        ? yield* startAndLoad({ binaryPath, environment, expected: null })
        : yield* verifyOpenCodeService(registered, registered).pipe(
            Effect.as(registered),
            Effect.catch(
              Effect.fnUntraced(function* (failure) {
                if (
                  isLocalConnectionRefusal(failure, registered) &&
                  (yield* isAbsentOpenCodeProcess(registered.pid))
                ) {
                  yield* recheckLedger(environment, registered);
                  return yield* startAndLoad({ binaryPath, environment, expected: registered });
                }
                return yield* failure;
              }),
            ),
          );
    yield* recheckLedger(environment, host);
    yield* verifyOpenCodeService(host, host);
    yield* recheckLedger(environment, host);
    return { url: host.url, password: host.password, exitCode: null, external: true };
  });

  const createOpenCodeSdkClient: OpenCode2Runtime["Service"]["createOpenCodeSdkClient"] = (input) =>
    OpenCode.make({
      baseUrl: input.baseUrl,
      headers: {
        "x-opencode-directory": encodeURIComponent(input.directory),
        ...(input.serverPassword.trim().length === 0
          ? {}
          : { Authorization: openCodeAuthorizationHeader(input.serverPassword) }),
      },
    });
  return OpenCode2Runtime.of({ connectToOpenCodeServer, createOpenCodeSdkClient });
});

export const layer = Layer.effect(OpenCode2Runtime, make);
