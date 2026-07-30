/**
 * Provider status probe for OpenCode 2.x.
 *
 * 1.x's probe cannot be reused. It parses `<binary> --version` with
 * `parseGenericCliVersion` and enumerates models with `<binary> models
 * --verbose` / `<binary> agent list`, and 2.x breaks both:
 *
 *   - `opencode2 --version` prints `opencode2 v0.0.0-next-16339`. The generic
 *     parser's `\b(\d+\.\d+\.\d+)\b` never matches, because the `v` prefix
 *     kills the leading word boundary, so the instance lands in `error` with
 *     "Unable to determine OpenCode version".
 *   - the inventory subcommands are gone. 2.x's default handler treats the
 *     first argument as a directory to `chdir` into, logs `ENOENT`, and exits
 *     0, so the probe cannot even detect its own failure by exit code.
 *     Inventory moved to `/api/model` and `/api/agent`.
 *
 * @module provider/Layers/OpenCode2Provider
 */
import type { AgentInfoV2, ModelInfo } from "@opencode-ai/sdk-next/v2";
import {
  type ModelCapabilities,
  type OpenCode2Settings,
  type ServerProviderModel,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Data from "effect/Data";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";

import { createModelCapabilities } from "@t3tools/shared/model";
import {
  OpenCode2Runtime,
  OpenCode2RuntimeError,
  runOpenCode2Sdk,
  type OpenCode2RuntimeShape,
} from "../opencode2Runtime.ts";
import { OpenCodeRuntime, openCodeRuntimeErrorDetail } from "../opencodeRuntime.ts";
import {
  buildServerProvider,
  nonEmptyTrimmed,
  providerModelsFromSettings,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";

const OPENCODE2_PRESENTATION = {
  displayName: "OpenCode 2.0",
  showInteractionModeToggle: false,
} as const;

/**
 * The `next` build this driver's runtime, event mapping, and route usage were
 * verified against. 2.x has no meaningful semver axis yet — every build on the
 * line is `0.0.0-next-<build>` — so the build number is the only ordering that
 * carries information, and it is only compared when the version still carries
 * a `next` tag. A future stable 2.x is accepted as-is rather than rejected by
 * a rule written for the preview line.
 */
const MINIMUM_OPENCODE2_NEXT_BUILD = 16339;

class OpenCode2ProbeError extends Data.TaggedError("OpenCode2ProbeError")<{
  readonly cause: unknown;
  readonly detail: string;
}> {}

/**
 * Accepts the `v` prefix and the prerelease tail that `parseGenericCliVersion`
 * chokes on.
 *
 * @internal exported for tests
 */
export function parseOpenCode2Version(output: string): string | null {
  return output.match(/v?(\d+\.\d+\.\d+(?:-[0-9A-Za-z][0-9A-Za-z.-]*)?)/)?.[1] ?? null;
}

/**
 * Build number out of a `0.0.0-next-16339` style version, or `null` when the
 * version is not on the `next` line and the build gate does not apply.
 *
 * @internal exported for tests
 */
export function openCode2NextBuild(version: string): number | null {
  const match = version.match(/-next[.-](\d+)/);
  if (!match) return null;
  const build = Number(match[1]);
  return Number.isFinite(build) ? build : null;
}

function normalizeProbeMessage(message: string): string | undefined {
  const trimmed = message.trim();
  if (trimmed.length === 0) return undefined;
  if (
    trimmed === "An error occurred in Effect.tryPromise" ||
    trimmed === "An error occurred in Effect.try"
  ) {
    return undefined;
  }
  return trimmed;
}

function normalizedErrorMessage(cause: unknown): string | undefined {
  if (cause instanceof OpenCode2ProbeError) return normalizeProbeMessage(cause.detail);
  if (!(cause instanceof Error)) return undefined;
  return normalizeProbeMessage(cause.message);
}

function formatOpenCode2ProbeError(input: {
  readonly cause: unknown;
  readonly isExternalServer: boolean;
  readonly serverUrl: string;
}): { readonly installed: boolean; readonly message: string } {
  const detail = normalizedErrorMessage(input.cause);
  const lower = detail?.toLowerCase() ?? "";

  if (input.isExternalServer) {
    if (
      lower.includes("401") ||
      lower.includes("403") ||
      lower.includes("unauthorized") ||
      lower.includes("forbidden")
    ) {
      return {
        installed: true,
        message:
          "OpenCode 2.0 server rejected authentication. Check the server URL and password — 2.x has no unauthenticated mode.",
      };
    }
    if (
      lower.includes("econnrefused") ||
      lower.includes("enotfound") ||
      lower.includes("fetch failed") ||
      lower.includes("networkerror") ||
      lower.includes("timed out") ||
      lower.includes("timeout") ||
      lower.includes("socket hang up")
    ) {
      return {
        installed: true,
        message: `Couldn't reach the configured OpenCode 2.0 server at ${input.serverUrl}. Check that the server is running and the URL is correct.`,
      };
    }
    return {
      installed: true,
      message: detail ?? "Failed to connect to the configured OpenCode 2.0 server.",
    };
  }

  if (lower.includes("enoent") || lower.includes("notfound")) {
    return {
      installed: false,
      message: "OpenCode 2.0 CLI (`opencode2`) is not installed or not on PATH.",
    };
  }
  if (lower.includes("postinstall")) {
    return {
      installed: false,
      message:
        "The `@opencode-ai/cli` package shipped its placeholder binary: its postinstall script never ran. Reinstall with dependency build scripts enabled.",
    };
  }
  if (lower.includes("quarantine")) {
    return {
      installed: true,
      message:
        "macOS is blocking the OpenCode 2.0 binary (quarantine). Run `xattr -d com.apple.quarantine $(which opencode2)` to fix this.",
    };
  }
  return {
    installed: true,
    message: detail
      ? `Failed to execute OpenCode 2.0 CLI health check: ${detail}`
      : "Failed to execute OpenCode 2.0 CLI health check.",
  };
}

function titleCaseSlug(value: string): string {
  if (value === "opencode") return "OpenCode";
  if (value === "openai") return "OpenAI";
  if (value === "xai") return "xAI";
  const segments: Array<string> = [];
  for (const segment of value.split(/[-_/]+/)) {
    if (segment.length > 0) segments.push(segment.charAt(0).toUpperCase() + segment.slice(1));
  }
  return segments.join(" ");
}

const DEFAULT_OPENCODE2_MODEL_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

export interface OpenCode2Inventory {
  readonly models: ReadonlyArray<ModelInfo>;
  readonly agents: ReadonlyArray<AgentInfoV2>;
}

interface OpenCode2InventoryRetryOptions {
  readonly maxAttempts?: number;
  readonly retryDelayMs?: number;
}

/**
 * A newly spawned 2.x server prints its ready banner before model and agent
 * bootstrap has finished. Retry only the empty startup result so a genuinely
 * populated response returns immediately while an installation with no models
 * remains bounded.
 *
 * @internal exported for tests
 */
export const retryEmptyOpenCode2Inventory = Effect.fn("retryEmptyOpenCode2Inventory")(function* <
  E,
  R,
>(
  readInventory: Effect.Effect<OpenCode2Inventory, E, R>,
  options?: OpenCode2InventoryRetryOptions,
): Effect.fn.Return<OpenCode2Inventory, E, R> {
  const maxAttempts = Math.max(1, options?.maxAttempts ?? 5);
  const retryDelayMs = Math.max(0, options?.retryDelayMs ?? 500);
  let inventory = yield* readInventory;

  for (let attempt = 1; inventory.models.length === 0 && attempt < maxAttempts; attempt += 1) {
    yield* Effect.sleep(retryDelayMs);
    inventory = yield* readInventory;
  }

  return inventory;
});

function openCode2CapabilitiesForModel(input: {
  readonly model: ModelInfo;
  readonly agents: ReadonlyArray<AgentInfoV2>;
}): ModelCapabilities {
  const variantOptions = input.model.variants.map((variant) => ({
    id: variant.id,
    label: titleCaseSlug(variant.id),
  }));
  const primaryAgents = input.agents.filter(
    (agent) => !agent.hidden && (agent.mode === "primary" || agent.mode === "all"),
  );
  const defaultAgent =
    primaryAgents.find((agent) => agent.id === "build")?.id ?? primaryAgents[0]?.id;
  const agentOptions = primaryAgents.map((agent) =>
    defaultAgent === agent.id
      ? { id: agent.id, label: titleCaseSlug(agent.name), isDefault: true as const }
      : { id: agent.id, label: titleCaseSlug(agent.name) },
  );
  return createModelCapabilities({
    optionDescriptors: [
      ...(variantOptions.length > 0
        ? [{ id: "variant", label: "Variant", type: "select" as const, options: variantOptions }]
        : []),
      ...(agentOptions.length > 0
        ? [
            {
              id: "agent",
              label: "Agent",
              type: "select" as const,
              options: agentOptions,
              ...(defaultAgent ? { currentValue: defaultAgent } : {}),
            },
          ]
        : []),
    ],
  });
}

export function flattenOpenCode2Models(
  inventory: OpenCode2Inventory,
): ReadonlyArray<ServerProviderModel> {
  const models: Array<ServerProviderModel> = [];
  for (const model of inventory.models) {
    if (!model.enabled) continue;
    const name = nonEmptyTrimmed(model.name);
    if (!name) continue;
    models.push({
      slug: `${model.providerID}/${model.id}`,
      name,
      subProvider: titleCaseSlug(model.providerID),
      isCustom: false,
      capabilities: openCode2CapabilitiesForModel({ model, agents: inventory.agents }),
    });
  }
  return models.toSorted((left, right) => left.name.localeCompare(right.name));
}

/**
 * Reads the 2.x inventory over HTTP, spawning a server when none is
 * configured. `/api/model` and `/api/agent` replaced the `models --verbose`
 * and `agent list` subcommands, which 2.x no longer has.
 */
const loadOpenCode2Inventory = (input: {
  readonly runtime: OpenCode2RuntimeShape;
  readonly settings: OpenCode2Settings;
  readonly cwd: string;
  readonly environment: NodeJS.ProcessEnv;
}): Effect.Effect<OpenCode2Inventory, OpenCode2RuntimeError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const server = yield* input.runtime.connectToOpenCode2Server({
        binaryPath: input.settings.binaryPath,
        serverUrl: input.settings.serverUrl,
        serverPassword: input.settings.serverPassword,
        environment: input.environment,
      });
      const client = input.runtime.createOpenCode2SdkClient({
        baseUrl: server.url,
        directory: input.cwd,
        serverPassword: server.password,
      });
      const location = { directory: input.cwd };
      return yield* retryEmptyOpenCode2Inventory(
        Effect.gen(function* () {
          const modelResponse = yield* runOpenCode2Sdk("model.list", () =>
            client.v2.model.list({ location }),
          );
          const agentResponse = yield* runOpenCode2Sdk("agent.list", () =>
            client.v2.agent.list({ location }),
          );
          return {
            models: modelResponse.data?.data ?? [],
            agents: agentResponse.data?.data ?? [],
          } satisfies OpenCode2Inventory;
        }),
      );
    }),
  );

export const makePendingOpenCode2Provider = (
  settings: OpenCode2Settings,
): Effect.Effect<ServerProviderDraft> =>
  Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = providerModelsFromSettings(
      [],
      settings.customModels,
      DEFAULT_OPENCODE2_MODEL_CAPABILITIES,
    );
    return buildServerProvider({
      presentation: OPENCODE2_PRESENTATION,
      enabled: settings.enabled,
      checkedAt,
      models,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: settings.enabled
          ? "OpenCode 2.0 provider status has not been checked in this session yet."
          : "OpenCode 2.0 is disabled in T3 Code settings.",
      },
    });
  });

export const checkOpenCode2ProviderStatus = Effect.fn("checkOpenCode2ProviderStatus")(function* (
  settings: OpenCode2Settings,
  cwd: string,
  environment?: NodeJS.ProcessEnv,
): Effect.fn.Return<ServerProviderDraft, never, OpenCode2Runtime | OpenCodeRuntime> {
  const runtime = yield* OpenCode2Runtime;
  // The 1.x runtime is only borrowed for its generic `runOpenCodeCommand`
  // process helper; nothing 1.x-specific is used through it.
  const commandRuntime = yield* OpenCodeRuntime;
  const resolvedEnvironment = environment ?? process.env;
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const customModels = settings.customModels;
  const isExternalServer = settings.serverUrl.trim().length > 0;

  const draft = (input: {
    readonly installed: boolean;
    readonly version: string | null;
    readonly status: "ready" | "warning" | "error";
    readonly message: string;
    readonly models?: ReadonlyArray<ServerProviderModel>;
    readonly authenticated?: boolean;
  }) =>
    buildServerProvider({
      presentation: OPENCODE2_PRESENTATION,
      enabled: settings.enabled,
      checkedAt,
      models: providerModelsFromSettings(
        input.models ?? [],
        customModels,
        DEFAULT_OPENCODE2_MODEL_CAPABILITIES,
      ),
      probe: {
        installed: input.installed,
        version: input.version,
        status: input.status,
        auth: input.authenticated
          ? { status: "authenticated", type: "opencode" }
          : { status: "unknown" },
        message: input.message,
      },
    });

  const fallback = (cause: unknown, version: string | null = null) => {
    const failure = formatOpenCode2ProbeError({
      cause,
      isExternalServer,
      serverUrl: settings.serverUrl,
    });
    return draft({
      installed: failure.installed,
      version,
      status: "error",
      message: failure.message,
    });
  };

  if (!settings.enabled) {
    return draft({
      installed: false,
      version: null,
      status: "warning",
      message: isExternalServer
        ? "OpenCode 2.0 is disabled in T3 Code settings. A server URL is configured."
        : "OpenCode 2.0 is disabled in T3 Code settings.",
    });
  }

  let version: string | null = null;
  if (!isExternalServer) {
    const versionExit = yield* Effect.exit(
      commandRuntime
        .runOpenCodeCommand({
          binaryPath: settings.binaryPath,
          args: ["--version"],
          environment: resolvedEnvironment,
        })
        .pipe(
          Effect.mapError(
            (cause) =>
              new OpenCode2ProbeError({ cause, detail: openCodeRuntimeErrorDetail(cause) }),
          ),
        ),
    );
    if (versionExit._tag === "Failure") return fallback(Cause.squash(versionExit.cause));
    version = parseOpenCode2Version(versionExit.value.stdout);
    if (version === null) {
      return fallback(
        new Error("Unable to determine OpenCode 2.0 version from `opencode2 --version` output."),
        null,
      );
    }
    const build = openCode2NextBuild(version);
    if (build !== null && build < MINIMUM_OPENCODE2_NEXT_BUILD) {
      return draft({
        installed: true,
        version,
        status: "error",
        message: `OpenCode 2.0 ${version} is older than the verified build next-${MINIMUM_OPENCODE2_NEXT_BUILD}. Upgrade with \`npm install -g @opencode-ai/cli@next\`.`,
      });
    }
  }

  const inventoryExit = yield* Effect.exit(
    loadOpenCode2Inventory({
      runtime,
      settings,
      cwd,
      environment: resolvedEnvironment,
    }).pipe(
      Effect.mapError(
        (cause) => new OpenCode2ProbeError({ cause, detail: openCodeRuntimeErrorDetail(cause) }),
      ),
    ),
  );
  if (inventoryExit._tag === "Failure") {
    return fallback(Cause.squash(inventoryExit.cause), version);
  }

  const models = flattenOpenCode2Models(inventoryExit.value);
  return draft({
    installed: true,
    version,
    status: models.length > 0 ? "ready" : "warning",
    authenticated: models.length > 0,
    models,
    message:
      models.length > 0
        ? `${models.length} model${models.length === 1 ? "" : "s"} available through ${isExternalServer ? "the configured OpenCode 2.0 server" : "OpenCode 2.0"}.`
        : "Connected to OpenCode 2.0, but it did not report any enabled models.",
  });
});
