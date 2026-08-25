// @effect-diagnostics nodeBuiltinImport:off
/**
 * Runtime for OpenCode 2. The HTTP client is `@opencode-ai/client`.
 * `connectToOpenCodeServer` attaches to a live host `service.json` or starts
 * the user daemon with `service start`.
 */
import { ClientError, OpenCode, type OpenCodeClient } from "@opencode-ai/client";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { spawnAndCollect } from "./providerSnapshot.ts";

export const OpenCodeRuntimeOperation = Schema.Literals([
  "agent.list",
  "connectToOpenCodeServer",
  "event.subscribe",
  "generate.text",
  "health.get",
  "integration.list",
  "mcp.add",
  "mcp.list",
  "message.list",
  "model.list",
  "session.compact",
  "session.context",
  "session.create",
  "session.fork",
  "session.form.reply",
  "session.generate",
  "session.get",
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
  "service.password",
  "service.start",
  "skill.list",
]);
export type OpenCodeRuntimeOperation = typeof OpenCodeRuntimeOperation.Type;

export const OpenCodeRuntimeErrorCategory = Schema.Literals([
  "authentication-failed",
  "binary-not-found",
  "event-subscription-failed",
  "external-server-password-required",
  "mcp-connect-failed",
  "mcp-connect-timeout",
  "missing-response-payload",
  "model-unavailable",
  "network-failed",
  "placeholder-binary",
  "port-allocation-failed",
  "quarantined-binary",
  "replay-boundary",
  "sdk-request-failed",
  "server-spawn-failed",
  "session-remove-failed",
  "startup-exited",
  "startup-failed",
  "startup-timeout",
]);
export type OpenCodeRuntimeErrorCategory = typeof OpenCodeRuntimeErrorCategory.Type;

export class OpenCodeRuntimeError extends Schema.TaggedErrorClass<OpenCodeRuntimeError>()(
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

export function openCodeRuntimeErrorDetail(cause: unknown): string {
  if (isOpenCodeRuntimeError(cause)) return cause.message;
  if (cause instanceof Error && cause.message.trim().length > 0) return cause.message.trim();
  return String(cause);
}

export function parseOpenCodeModelSlug(
  slug: string | null | undefined,
): { readonly providerID: string; readonly modelID: string } | null {
  if (typeof slug !== "string") {
    return null;
  }
  const trimmed = slug.trim();
  const separator = trimmed.indexOf("/");
  if (separator <= 0 || separator === trimmed.length - 1) {
    return null;
  }
  return {
    providerID: trimmed.slice(0, separator),
    modelID: trimmed.slice(separator + 1),
  };
}

function openCode2SdkErrorCause(cause: unknown): unknown {
  return cause instanceof ClientError && cause.cause !== undefined ? cause.cause : cause;
}

function openCode2SdkErrorCategoryFromText(
  cause: unknown,
): Extract<
  OpenCodeRuntimeErrorCategory,
  "authentication-failed" | "model-unavailable" | "network-failed" | "sdk-request-failed"
> {
  if (cause instanceof ClientError && cause.reason === "Transport") return "network-failed";
  const detail = openCodeRuntimeErrorDetail(openCode2SdkErrorCause(cause)).toLowerCase();
  if (detail.startsWith("model unavailable:")) return "model-unavailable";
  if (
    detail.includes("401") ||
    detail.includes("403") ||
    detail.includes("unauthorized") ||
    detail.includes("forbidden")
  ) {
    return "authentication-failed";
  }
  if (
    detail.includes("econnrefused") ||
    detail.includes("enotfound") ||
    detail.includes("fetch failed") ||
    detail.includes("networkerror") ||
    detail.includes("socket hang up") ||
    detail.includes("timed out") ||
    detail.includes("timeout")
  ) {
    return "network-failed";
  }
  return "sdk-request-failed";
}

function openCode2ExecutableErrorCategoryFromText(
  cause: unknown,
): Extract<
  OpenCodeRuntimeErrorCategory,
  "binary-not-found" | "placeholder-binary" | "quarantined-binary" | "server-spawn-failed"
> {
  const detail = openCodeRuntimeErrorDetail(cause).toLowerCase();
  if (detail.includes("postinstall")) return "placeholder-binary";
  if (detail.includes("quarantine")) return "quarantined-binary";
  if (detail.includes("enoent") || detail.includes("notfound")) return "binary-not-found";
  return "server-spawn-failed";
}

export const runOpenCodeSdk = <A>(
  operation: OpenCodeRuntimeOperation,
  fn: () => Promise<A>,
): Effect.Effect<A, OpenCodeRuntimeError> =>
  Effect.tryPromise({
    try: fn,
    catch: (cause) =>
      new OpenCodeRuntimeError({
        operation,
        category: openCode2SdkErrorCategoryFromText(cause),
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

export class OpenCodeRuntime extends Context.Service<
  OpenCodeRuntime,
  {
    readonly connectToOpenCodeServer: (input: {
      readonly binaryPath: string;
      readonly serverUrl?: string | null;
      readonly serverPassword?: string | null;
      readonly environment?: NodeJS.ProcessEnv;
    }) => Effect.Effect<
      OpenCodeServerConnection,
      OpenCodeRuntimeError,
      ChildProcessSpawner.ChildProcessSpawner
    >;
    readonly createOpenCodeSdkClient: (input: {
      readonly baseUrl: string;
      readonly directory: string;
      readonly serverPassword: string;
    }) => OpenCodeClient;
  }
>()("t3/provider/opencodeRuntime") {}

export function readOpenCodeStatePassword(
  environment: NodeJS.ProcessEnv = process.env,
): string | null {
  const stateHome =
    environment.XDG_STATE_HOME?.trim() ||
    NodePath.join(environment.HOME?.trim() || NodeOS.homedir(), ".local", "state");
  try {
    const password = NodeFS.readFileSync(
      NodePath.join(stateHome, "opencode", "password"),
      "utf8",
    ).trim();
    return password.length > 0 ? password : null;
  } catch {
    return null;
  }
}

export interface OpenCodeHostService {
  readonly url: string;
  readonly password: string;
  readonly pid: number | null;
}

/**
 * Host OpenCode 2 service state, not a T3-managed isolate.
 * Ignore remapped T3 isolate state homes if those are still set.
 *
 * @internal exported for tests
 */
export function openCodeHostStateHome(environment: NodeJS.ProcessEnv = process.env): string {
  const xdg = environment.XDG_STATE_HOME?.trim();
  if (xdg && !xdg.includes("t3-opencode2-state")) {
    return xdg;
  }
  return NodePath.join(environment.HOME?.trim() || NodeOS.homedir(), ".local", "state");
}

/**
 * Read the user-owned `opencode2 service` ledger. Missing or malformed files
 * return null; the caller then starts the user daemon.
 *
 * @internal exported for tests
 */
export function readOpenCodeHostService(
  environment: NodeJS.ProcessEnv = process.env,
): OpenCodeHostService | null {
  try {
    const raw = NodeFS.readFileSync(
      NodePath.join(openCodeHostStateHome(environment), "opencode", "service.json"),
      "utf8",
    );
    const parsed: unknown = JSON.parse(raw);
    if (!isRecord(parsed)) return null;
    const url = typeof parsed["url"] === "string" ? parsed["url"].trim() : "";
    const password = typeof parsed["password"] === "string" ? parsed["password"].trim() : "";
    if (url.length === 0 || password.length === 0) return null;
    if (!URL.canParse(url)) return null;
    const protocol = new URL(url).protocol;
    if (protocol !== "http:" && protocol !== "https:") return null;
    const pid =
      typeof parsed["pid"] === "number" && Number.isInteger(parsed["pid"]) && parsed["pid"] > 0
        ? parsed["pid"]
        : null;
    return { url, password, pid };
  } catch {
    return null;
  }
}

/** @internal exported for tests */
export function isOpenCodeHostServiceAlive(service: OpenCodeHostService): boolean {
  if (service.pid === null) return false;
  try {
    process.kill(service.pid, 0);
    return true;
  } catch {
    return false;
  }
}

/** @internal exported for tests */
export function parseOpenCodeServiceUrl(stdout: string): string | null {
  for (const line of stdout.trim().split(/\r?\n/u).toReversed()) {
    const candidate = line.trim();
    if (!URL.canParse(candidate)) continue;
    const url = new URL(candidate);
    if (url.protocol === "http:" || url.protocol === "https:") return url.toString();
  }
  return null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** The header the 2.x server checks when auth is enabled. Username is fixed. */
export function openCodeAuthorizationHeader(password: string): string {
  return `Basic ${Buffer.from(`opencode:${password}`, "utf8").toString("base64")}`;
}

/**
 * The 2.x server resolves a session with no variant to a synthetic variant
 * literally named "default" and stamps that id on the session and its
 * messages, so this id means "let the server decide" and must never be sent
 * as an explicit variant: it is not in any model's `variants` list, and 2.x
 * silently drops the next prompt (user message recorded, no assistant reply)
 * when the bound variant is unknown.
 */
export const OPENCODE_DEFAULT_VARIANT = "default";

export function normalizeOpenCodeVariant(variant: string | undefined): string | undefined {
  return variant === OPENCODE_DEFAULT_VARIANT ? undefined : variant;
}

/**
 * Synthetic agent-option id meaning "defer to the Build/Plan toggle". The
 * Agent descriptor only appears when custom primary agents exist, listing
 * this sentinel plus the customs; the native `build`/`plan` pair is owned by
 * the interaction-mode toggle. A real agent named "auto" would collide and is
 * excluded from the descriptor.
 */
export const OPENCODE_AUTO_AGENT = "auto";

export const make = Effect.sync(() => {
  const resolveCommand = (command: string, args: ReadonlyArray<string>, env?: NodeJS.ProcessEnv) =>
    resolveSpawnCommand(command, args, env ? { env } : {});

  const connectToOpenCodeServer: OpenCodeRuntime["Service"]["connectToOpenCodeServer"] = (
    input,
  ) => {
    const serverUrl = input.serverUrl?.trim();
    if (serverUrl) {
      const serverPassword = input.serverPassword?.trim();
      if (!serverPassword) {
        return Effect.fail(
          new OpenCodeRuntimeError({
            operation: "connectToOpenCodeServer",
            category: "external-server-password-required",
          }),
        );
      }
      return Effect.succeed({
        url: serverUrl,
        password: serverPassword,
        exitCode: null,
        external: true,
      });
    }

    const environment = input.environment ?? process.env;
    const hostService = readOpenCodeHostService(environment);
    if (hostService !== null && isOpenCodeHostServiceAlive(hostService)) {
      return Effect.succeed({
        url: hostService.url,
        password: hostService.password,
        exitCode: null,
        external: true,
      });
    }

    return startOpenCodeUserService({
      binaryPath: input.binaryPath,
      environment,
    });
  };

  const startOpenCodeUserService = (input: {
    readonly binaryPath: string;
    readonly environment: NodeJS.ProcessEnv;
  }) =>
    Effect.gen(function* () {
      const start = yield* runOpenCodeCli(
        "service.start",
        input.binaryPath,
        ["service", "start"],
        input.environment,
      );
      if (start.code !== 0) {
        return yield* new OpenCodeRuntimeError({
          operation: "service.start",
          category: "server-spawn-failed",
          exitCode: start.code,
        });
      }
      const startedUrl = parseOpenCodeServiceUrl(`${start.stdout}\n${start.stderr}`);
      const ledger = readOpenCodeHostService(input.environment);
      const url = startedUrl ?? ledger?.url;
      if (url === undefined) {
        return yield* new OpenCodeRuntimeError({
          operation: "service.start",
          category: "startup-failed",
        });
      }
      let password = ledger?.password ?? readOpenCodeStatePassword(input.environment);
      if (password === null) {
        const passwordResult = yield* runOpenCodeCli(
          "service.password",
          input.binaryPath,
          ["service", "password"],
          input.environment,
        );
        const fromCommand = passwordResult.stdout.trim();
        password = fromCommand.length > 0 ? fromCommand : null;
      }
      if (password === null) {
        return yield* new OpenCodeRuntimeError({
          operation: "service.password",
          category: "external-server-password-required",
        });
      }
      return {
        url,
        password,
        exitCode: null,
        external: true,
      } satisfies OpenCodeServerConnection;
    });

  const runOpenCodeCli = (
    operation: OpenCodeRuntimeOperation,
    binaryPath: string,
    args: ReadonlyArray<string>,
    environment: NodeJS.ProcessEnv,
  ) =>
    Effect.gen(function* () {
      const spawnCommand = yield* resolveCommand(binaryPath, args, environment).pipe(
        Effect.mapError(
          (cause) =>
            new OpenCodeRuntimeError({
              operation,
              category: openCode2ExecutableErrorCategoryFromText(cause),
              cause,
            }),
        ),
      );
      return yield* spawnAndCollect(
        binaryPath,
        ChildProcess.make(spawnCommand.command, spawnCommand.args, {
          env: environment,
          extendEnv: false,
          shell: spawnCommand.shell,
        }),
      ).pipe(
        Effect.mapError(
          (cause) =>
            new OpenCodeRuntimeError({
              operation,
              category: openCode2ExecutableErrorCategoryFromText(cause),
              cause,
            }),
        ),
      );
    });

  const createOpenCodeSdkClient: OpenCodeRuntime["Service"]["createOpenCodeSdkClient"] = (input) =>
    OpenCode.make({
      baseUrl: input.baseUrl,
      headers: {
        "x-opencode-directory": encodeURIComponent(input.directory),
        ...(input.serverPassword.trim().length === 0
          ? {}
          : { Authorization: openCodeAuthorizationHeader(input.serverPassword) }),
      },
    });

  return {
    connectToOpenCodeServer,
    createOpenCodeSdkClient,
  } satisfies OpenCodeRuntime["Service"];
});

export const layer = Layer.effect(OpenCodeRuntime, make);
export const OpenCodeRuntimeLive = layer;
