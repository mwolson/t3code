import { modelSelectionsEqual } from "@t3tools/shared/model";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import {
  ModelSelection,
  OrchestrationV2DomainEvent,
  OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  OrchestrationV2RuntimeRequest,
  ProviderInstanceId,
  ProviderSessionId,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import { ProviderWorkspaceMissingError } from "../provider/Errors.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as McpProviderSession from "../mcp/McpProviderSession.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as McpSessionRegistry from "../mcp/McpSessionRegistry.ts";
import * as EventSink from "./EventSink.ts";
import * as IdAllocator from "./IdAllocator.ts";
import { makeKeyedSerialExecutor } from "./KeyedSerialExecutor.ts";
import * as ProviderEventIngestor from "./ProviderEventIngestor.ts";
import {
  ProviderAdapterEventStreamError,
  ProviderAdapterBufferedOutputError,
  ProviderAdapterRunningWorkError,
  ProviderAdapterTurnStartError,
  ProviderAdapterV2RuntimePolicy,
  type ProviderAdapterV2Error,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2EventSubscription,
  type ProviderAdapterV2SessionRuntime,
} from "./ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

const DEFAULT_IDLE_TIMEOUT_MS = 30 * 60 * 1000;
const DEFAULT_MAX_IDLE_PIN_MS = 4 * 60 * 60 * 1000;
const RELEASE_SCOPE_CLOSE_TIMEOUT_MS = 30 * 1000;
const UNLOAD_THREAD_TIMEOUT_MS = 10 * 1000;

export const ProviderSessionReleaseReason = Schema.Literals([
  "idle_timeout",
  "runtime_error",
  "manual_shutdown",
  "server_shutdown",
]);
export type ProviderSessionReleaseReason = typeof ProviderSessionReleaseReason.Type;

/**
 * ProviderSessionManager owns live session residency: open sessions, idle release,
 * explicit shutdown, and release-on-runtime-failure.
 *
 * It intentionally does not resurrect persisted sessions. Process-loss recovery
 * terminalizes provider-bound work and retires non-replayable effects; a later
 * user command or durable replay-safe operation opens a session lazily.
 */
export class ProviderSessionOpenError extends Schema.TaggedError<ProviderSessionOpenError>()(
  "ProviderSessionOpenError",
  {
    instanceId: ProviderInstanceId,
    providerSessionId: ProviderSessionId,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to open provider instance ${this.instanceId} session ${this.providerSessionId}.`;
  }
}

export class ProviderSessionLookupError extends Schema.TaggedError<ProviderSessionLookupError>()(
  "ProviderSessionLookupError",
  {
    providerSessionId: ProviderSessionId,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to look up provider session ${this.providerSessionId}.`;
  }
}

export class ProviderSessionCloseError extends Schema.TaggedError<ProviderSessionCloseError>()(
  "ProviderSessionCloseError",
  {
    providerSessionId: ProviderSessionId,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to close provider session ${this.providerSessionId}.`;
  }
}

export class ProviderSessionReleaseError extends Schema.TaggedError<ProviderSessionReleaseError>()(
  "ProviderSessionReleaseError",
  {
    providerSessionId: ProviderSessionId,
    reason: ProviderSessionReleaseReason,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to release provider session ${this.providerSessionId}.`;
  }
}

export class ProviderSessionActivityError extends Schema.TaggedError<ProviderSessionActivityError>()(
  "ProviderSessionActivityError",
  {
    providerSessionId: ProviderSessionId,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to update provider session activity for ${this.providerSessionId}.`;
  }
}

export const ProviderSessionManagerV2Error = Schema.Union([
  ProviderSessionOpenError,
  ProviderWorkspaceMissingError,
  ProviderSessionLookupError,
  ProviderSessionCloseError,
  ProviderSessionReleaseError,
  ProviderSessionActivityError,
]);
export type ProviderSessionManagerV2Error = typeof ProviderSessionManagerV2Error.Type;

export interface ProviderSessionManagerV2Shape {
  readonly shutdown: Effect.Effect<void>;
  readonly open: (input: {
    readonly threadId: ThreadId;
    readonly providerSessionId: ProviderSessionId;
    readonly modelSelection: ModelSelection;
    readonly runtimePolicy: ProviderAdapterV2RuntimePolicy;
    readonly resumeFromSession?: OrchestrationV2ProviderSession;
    readonly initialNativeThreadId?: string;
    readonly initialProviderItemIdentityVersion?: 2;
  }) => Effect.Effect<ProviderAdapterV2SessionRuntime, ProviderSessionManagerV2Error>;
  readonly get: (
    providerSessionId: ProviderSessionId,
  ) => Effect.Effect<Option.Option<ProviderAdapterV2SessionRuntime>, ProviderSessionManagerV2Error>;
  readonly close: (
    providerSessionId: ProviderSessionId,
  ) => Effect.Effect<void, ProviderSessionManagerV2Error>;
  /** Closes every live runtime owned by one provider instance. */
  readonly closeInstance: (
    instanceId: ProviderInstanceId,
  ) => Effect.Effect<void, ProviderSessionManagerV2Error>;
  readonly release: (input: {
    readonly providerSessionId: ProviderSessionId;
    readonly reason: ProviderSessionReleaseReason;
    readonly detail?: string;
  }) => Effect.Effect<void, ProviderSessionManagerV2Error>;
  readonly detach: (input: {
    readonly providerSessionId: ProviderSessionId;
    readonly threadId: ThreadId;
    readonly detail?: string;
    /**
     * True for terminal detaches (thread archived or deleted): the thread's
     * MCP credentials are revoked immediately instead of surviving for a
     * potential re-attach.
     */
    readonly revokeMcpCredential?: boolean;
    readonly preserveBufferedOutput?: boolean;
  }) => Effect.Effect<void, ProviderSessionManagerV2Error>;
}

export class ProviderSessionManagerV2 extends Context.Service<
  ProviderSessionManagerV2,
  ProviderSessionManagerV2Shape
>()("t3/orchestration-v2/ProviderSessionManager/ProviderSessionManagerV2") {}

interface ExecutionSelectionEvidence {
  readonly providerThread?: OrchestrationV2ProviderThread;
  readonly providerThreadId: string;
  readonly nativeThreadId: string | null;
  readonly selection: ModelSelection | null;
  readonly bufferedRunOrdinal?: number;
  readonly operation?: symbol | undefined;
  readonly runAttemptId?: string;
  readonly providerTurnId?: string;
  readonly loadKey?: string;
}

interface LiveSessionEntry {
  readonly executionSelections: ReadonlyMap<ThreadId, ExecutionSelectionEvidence>;
  readonly preservedOutputThreadIds: ReadonlyMap<
    ThreadId,
    {
      readonly binding: OrchestrationV2ProviderThread;
      readonly operation: symbol | undefined;
    }
  >;
  readonly attachedThreadIds: ReadonlySet<ThreadId>;
  readonly loadedProviderThreadKeyByThread: ReadonlyMap<ThreadId, string>;
  /**
   * MCP credential session id issued for each attached thread. Revocation on
   * detach/release is scoped to these ids so tearing down a superseded
   * session cannot revoke a replacement session's credential for the same
   * thread (the workspace-handoff sequence opens the replacement before the
   * outbox executes the old session's detach).
   */
  readonly mcpCredentialIdByThread: ReadonlyMap<ThreadId, string>;
  readonly supportsMultipleProviderThreads: boolean;
  readonly runtime: ProviderAdapterV2SessionRuntime;
  readonly exposedRuntime: ProviderAdapterV2SessionRuntime;
  readonly eventSubscribers: Ref.Ref<
    ReadonlyMap<number, Queue.Queue<ProviderSessionEventSignal, Cause.Done>>
  >;
  readonly requestEventPermit: Semaphore.Semaphore;
  readonly scope: Scope.Closeable;
  readonly idleGeneration: number;
  readonly busyCount: number;
  readonly lastActivityAtMs: number;
  readonly idleFiber: Fiber.Fiber<void, never> | null;
  /** Set when idle release is deferred for pending background work; bounds total deferral. */
  readonly pinnedSinceMs: number | null;
}

type ProviderSessionEventSignal =
  | { readonly type: "event"; readonly event: ProviderAdapterV2Event }
  | {
      readonly type: "failure";
      readonly cause: Cause.Cause<ProviderAdapterV2Error>;
    };

export interface ProviderSessionManagerV2LayerOptions {
  readonly idleTimeoutMs?: number;
  /** Cap on how long idle release may be deferred for pending background work. */
  readonly maxIdlePinMs?: number;
  /** Test replay harnesses can omit T3's MCP server from provider protocol fixtures. */
  readonly configureMcp?: boolean;
}

function releaseStatusFor(
  reason: ProviderSessionReleaseReason,
): OrchestrationV2ProviderSession["status"] {
  return reason === "runtime_error" ? "error" : "stopped";
}

function releasedRuntimeRequestStatusFor(
  reason: ProviderSessionReleaseReason,
): OrchestrationV2RuntimeRequest["status"] {
  return reason === "manual_shutdown" || reason === "server_shutdown" ? "cancelled" : "expired";
}

function sessionKey(providerSessionId: ProviderSessionId): string {
  return String(providerSessionId);
}

/**
 * Runtime requests with no provider turn belong to the live session itself.
 * Their node and transcript item are runless too, so they bypass the normal
 * per-run subscriber and are persisted by the session event pump.
 */
function sessionScopedRuntimeRequestThreadId(event: ProviderAdapterV2Event): ThreadId | undefined {
  switch (event.type) {
    case "runtime_request.updated":
      return event.runtimeRequest.providerTurnId === null ? event.threadId : undefined;
    case "node.updated":
      return event.node.runId === null && event.node.runtimeRequestId !== null
        ? event.node.threadId
        : undefined;
    case "turn_item.updated":
      return event.turnItem.runId === null &&
        (event.turnItem.type === "approval_request" || event.turnItem.type === "user_input_request")
        ? event.turnItem.threadId
        : undefined;
    default:
      return undefined;
  }
}

function providerThreadRuntimeKey(
  providerThread: Parameters<ProviderAdapterV2SessionRuntime["resumeThread"]>[0]["providerThread"],
): string {
  const nativeThreadRef = providerThread.nativeThreadRef;
  return nativeThreadRef === null
    ? String(providerThread.id)
    : `${nativeThreadRef.driver}:${nativeThreadRef.nativeId}`;
}

function providerThreadLoadKey(input: {
  readonly providerThread: Parameters<
    ProviderAdapterV2SessionRuntime["resumeThread"]
  >[0]["providerThread"];
  readonly modelSelection?: ModelSelection;
  readonly runtimePolicy?: ProviderAdapterV2RuntimePolicy;
}): string {
  return JSON.stringify({
    providerThread: providerThreadRuntimeKey(input.providerThread),
    modelSelection: input.modelSelection ?? null,
    runtimePolicy: input.runtimePolicy ?? null,
  });
}

export const layerWithOptions = (
  options: ProviderSessionManagerV2LayerOptions = {},
): Layer.Layer<
  ProviderSessionManagerV2,
  never,
  | EventSink.EventSinkV2
  | FileSystem.FileSystem
  | IdAllocator.IdAllocatorV2
  | McpSessionRegistry.McpSessionRegistry
  | ProjectionStore.ProjectionStoreV2
  | ProviderEventIngestor.ProviderEventIngestorV2
  | ProviderAdapterRegistry.ProviderAdapterRegistryV2
> =>
  Layer.effect(
    ProviderSessionManagerV2,
    Effect.gen(function* () {
      const registry = yield* ProviderAdapterRegistry.ProviderAdapterRegistryV2;
      const fileSystem = yield* FileSystem.FileSystem;
      const mcpSessionRegistry = yield* McpSessionRegistry.McpSessionRegistry;
      /**
       * Optional so the many focused tests that assemble this layer by hand do
       * not each need a settings stub; the production composition always
       * provides it. When present, an unreadable settings file withholds
       * browser access rather than granting it — an explicit "off" silently
       * becoming "on" would violate the user's stated choice, whereas the
       * reverse costs an agent one toolset and is visible immediately (#7083).
       */
      const serverSettings = yield* Effect.serviceOption(ServerSettings.ServerSettingsService);
      const projectService = yield* Effect.serviceOption(ProjectService.ProjectService);
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const providerEventIngestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const agentAccessSettings = Effect.fn("ProviderSessionManagerV2.agentAccessSettings")(
        function* (threadId: ThreadId) {
          if (Option.isNone(serverSettings)) return { browser: true, device: false };
          return yield* Effect.gen(function* () {
            const settings = yield* serverSettings.value.getSettings;
            const thread = yield* projectionStore.getThread(threadId);
            const entries = Object.values(settings.projectSettingsOverrides);
            const browserOverridden = entries.some(
              (entry) => entry.enableAgentBrowserAccess !== undefined,
            );
            const deviceOverridden = entries.some(
              (entry) => entry.enableAgentDeviceAccess !== undefined,
            );
            if (browserOverridden || deviceOverridden) {
              const project = Option.isSome(projectService)
                ? yield* projectService.value.getById(thread.projectId)
                : Option.none();
              if (Option.isNone(project))
                return {
                  browser: browserOverridden ? false : settings.enableAgentBrowserAccess,
                  device: deviceOverridden ? false : settings.enableAgentDeviceAccess,
                };
            }
            const effective = resolveProjectSettings(settings, thread.projectId).settings;
            return {
              browser: effective.enableAgentBrowserAccess,
              device: effective.enableAgentDeviceAccess,
            };
          }).pipe(
            Effect.catch((cause) =>
              Effect.logWarning(
                "Could not resolve agent access; withholding browser and device tools.",
                { threadId, cause },
              ).pipe(Effect.as({ browser: false, device: false })),
            ),
          );
        },
      );
      const layerScope = yield* Effect.scope;
      const sessions = yield* Ref.make(new Map<string, LiveSessionEntry>());
      const nextSubscriberId = yield* Ref.make(0);
      const sessionOpen = yield* makeKeyedSerialExecutor<ProviderSessionId>();
      // Orders a thread's attach against a detach unloading it on the same session.
      const threadAttachment = yield* makeKeyedSerialExecutor<string>();
      const threadAttachmentKey = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
      }) => `${input.providerSessionId}\u0000${input.threadId}`;
      const idleTimeoutMs = Math.max(1, options.idleTimeoutMs ?? DEFAULT_IDLE_TIMEOUT_MS);
      const maxIdlePinMs = Math.max(0, options.maxIdlePinMs ?? DEFAULT_MAX_IDLE_PIN_MS);
      interface PreparedMcpCredential {
        readonly mcpCredentialId: string | undefined;
        /** True when this call minted the credential (vs reusing a live one). */
        readonly issued: boolean;
      }
      /**
       * Reservations protect a credential between prepareMcpSession handing it
       * out and the owning session entry becoming visible in `sessions`.
       * Adapters like ACP and OpenCode consume the credential eagerly during
       * openSession, so a racing release must not revoke it in that window
       * (rotating afterwards cannot repair an already-configured process).
       * The holder MUST drop the reservation once the entry is recorded or the
       * open fails.
       */
      const mcpCredentialReservations = new Map<string, number>();
      const mcpReservationKey = (threadId: ThreadId, mcpCredentialId: string) =>
        `${threadId}\0${mcpCredentialId}`;
      const reserveMcpCredential = (threadId: ThreadId, mcpCredentialId: string) => {
        const key = mcpReservationKey(threadId, mcpCredentialId);
        mcpCredentialReservations.set(key, (mcpCredentialReservations.get(key) ?? 0) + 1);
      };
      const dropMcpCredentialReservation = (threadId: ThreadId, mcpCredentialId: string) => {
        const key = mcpReservationKey(threadId, mcpCredentialId);
        const count = mcpCredentialReservations.get(key) ?? 0;
        if (count <= 1) {
          mcpCredentialReservations.delete(key);
        } else {
          mcpCredentialReservations.set(key, count - 1);
        }
      };
      const isMcpCredentialReserved = (threadId: ThreadId, mcpCredentialId: string) =>
        (mcpCredentialReservations.get(mcpReservationKey(threadId, mcpCredentialId)) ?? 0) > 0;
      const mcpPrepareLock = yield* makeKeyedSerialExecutor<ThreadId>();
      /**
       * Resolves (or mints) the thread's MCP credential and returns it with a
       * reservation held; the caller must drop the reservation exactly once.
       * Serialized per thread so two concurrent prepares cannot interleave
       * their rotate steps and revoke each other's freshly minted credential.
       */
      const prepareMcpSession = (
        threadId: ThreadId,
        providerInstanceId: ProviderInstanceId,
      ): Effect.Effect<PreparedMcpCredential> =>
        options.configureMcp === false
          ? Effect.sync((): PreparedMcpCredential => {
              McpProviderSession.clearMcpProviderSession(threadId);
              return { mcpCredentialId: undefined, issued: false };
            })
          : mcpPrepareLock.withLock(
              threadId,
              Effect.gen(function* () {
                // Reuse a still-valid credential for this thread instead of
                // rotating: long-lived provider processes (codex app-server)
                // build their MCP client once per conversation and keep using
                // the credential it started with, so a thread that detaches and
                // re-attaches across a workspace handoff must come back to the
                // same token or the process's tool calls fail auth.
                const { browser: browserToolsAvailable, device: deviceToolsAvailable } =
                  yield* agentAccessSettings(threadId);
                const capabilities = new Set<
                  import("../mcp/McpInvocationContext.ts").McpCapability
                >(["orchestration", "worktree", "pull-requests"]);
                if (browserToolsAvailable) capabilities.add("preview");
                if (deviceToolsAvailable) capabilities.add("device");
                const existing = McpProviderSession.readMcpProviderSession(threadId);
                if (existing !== undefined) {
                  // Reserve before the async resolve so a release cannot
                  // revoke the credential between validation and reservation.
                  reserveMcpCredential(threadId, existing.providerSessionId);
                  const rawToken = existing.authorizationHeader.replace(/^Bearer\s+/, "");
                  const resolved = yield* mcpSessionRegistry.resolve(rawToken);
                  if (
                    resolved !== undefined &&
                    resolved.threadId === threadId &&
                    resolved.providerInstanceId === providerInstanceId &&
                    // A flipped browser-access setting must not survive through
                    // credential reuse: rotate so the new scope reflects it.
                    resolved.capabilities.has("preview") === browserToolsAvailable &&
                    resolved.capabilities.has("device") === deviceToolsAvailable
                  ) {
                    return { mcpCredentialId: existing.providerSessionId, issued: false };
                  }
                  dropMcpCredentialReservation(threadId, existing.providerSessionId);
                }
                yield* mcpSessionRegistry.revokeThread(threadId);
                const credential = yield* mcpSessionRegistry.issue({
                  threadId,
                  providerInstanceId,
                  browserToolsAvailable,
                  capabilities,
                });
                McpProviderSession.setMcpProviderSession(credential.config);
                reserveMcpCredential(threadId, credential.config.providerSessionId);
                return { mcpCredentialId: credential.config.providerSessionId, issued: true };
              }),
            );
      /**
       * With a credential id, revocation is scoped to that credential and the
       * config slot is cleared only while it still holds it; a replacement
       * session's newer credential survives. Without one (attach failed before
       * a credential was recorded), fall back to thread-wide revocation.
       */
      const clearMcpSession = (threadId: ThreadId, mcpCredentialId?: string) =>
        mcpCredentialId === undefined
          ? mcpSessionRegistry
              .revokeThread(threadId)
              .pipe(
                Effect.tap(() =>
                  Effect.sync(() => McpProviderSession.clearMcpProviderSession(threadId)),
                ),
              )
          : mcpSessionRegistry.revokeProviderSession(mcpCredentialId).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  if (
                    McpProviderSession.readMcpProviderSession(threadId)?.providerSessionId ===
                    mcpCredentialId
                  ) {
                    McpProviderSession.clearMcpProviderSession(threadId);
                  }
                }),
              ),
            );

      const publishToSubscribers = (
        subscribers: Ref.Ref<
          ReadonlyMap<number, Queue.Queue<ProviderSessionEventSignal, Cause.Done>>
        >,
        signal: ProviderSessionEventSignal,
      ) =>
        Ref.get(subscribers).pipe(
          Effect.flatMap((current) =>
            Effect.forEach(current.values(), (queue) => Queue.offer(queue, signal), {
              discard: true,
            }),
          ),
        );

      const failSubscribers = (entry: LiveSessionEntry, detail: string) =>
        Effect.gen(function* () {
          const error = new ProviderAdapterEventStreamError({
            driver: entry.runtime.driver,
            providerSessionId: entry.runtime.providerSessionId,
            cause: detail,
          });
          const subscribers = yield* Ref.getAndSet(entry.eventSubscribers, new Map());
          yield* Effect.forEach(
            subscribers.values(),
            (queue) =>
              Queue.offer(queue, {
                type: "failure",
                cause: Cause.fail(error),
              }),
            { discard: true },
          );
        });

      const closeSubscribers = (entry: LiveSessionEntry) =>
        Effect.gen(function* () {
          const subscribers = yield* Ref.getAndSet(entry.eventSubscribers, new Map());
          yield* Effect.forEach(
            subscribers.values(),
            (queue) => Queue.clear(queue).pipe(Effect.andThen(Queue.end(queue))),
            { discard: true },
          );
        });

      // Preserve already-published terminal events while ending subscriptions.
      // Server shutdown intentionally clears them; a provider-announced Stop
      // must let consumers drain them before the stream completes.
      const endSubscribers = (entry: LiveSessionEntry) =>
        Effect.gen(function* () {
          const subscribers = yield* Ref.getAndSet(entry.eventSubscribers, new Map());
          yield* Effect.forEach(subscribers.values(), (queue) => Queue.end(queue), {
            discard: true,
          });
        });

      const cancelIdleFiber = (fiber: Fiber.Fiber<void, never> | null) =>
        fiber === null ? Effect.void : Fiber.interrupt(fiber).pipe(Effect.ignore);

      const writeProviderSessionEvents = (input: {
        readonly runtime: ProviderAdapterV2SessionRuntime;
        readonly threadIds: Iterable<ThreadId>;
        readonly type: "provider-session.attached" | "provider-session.updated";
        readonly payload: OrchestrationV2ProviderSession;
      }) =>
        Effect.gen(function* () {
          const now = yield* DateTime.now;
          const events = yield* Effect.forEach(input.threadIds, (threadId) =>
            Effect.gen(function* () {
              return {
                id: yield* idAllocator.allocate.event({
                  threadId,
                  providerSessionId: input.runtime.providerSessionId,
                }),
                type: input.type,
                threadId,
                driver: input.runtime.driver,
                providerInstanceId: input.runtime.instanceId,
                occurredAt: now,
                payload: input.payload,
              } satisfies OrchestrationV2DomainEvent;
            }),
          );
          if (events.length > 0) {
            yield* eventSink.write({ events });
          }
        });

      const writeReleasedSessionEvents = (input: {
        readonly entry: LiveSessionEntry;
        readonly reason: ProviderSessionReleaseReason;
        readonly detail?: string;
      }) =>
        Effect.gen(function* () {
          const now = yield* DateTime.now;
          const payload: OrchestrationV2ProviderSession = {
            ...input.entry.runtime.providerSession,
            status: releaseStatusFor(input.reason),
            updatedAt: now,
            lastError:
              input.reason === "runtime_error"
                ? (input.detail ?? "Provider runtime failed.")
                : null,
          };
          yield* writeProviderSessionEvents({
            runtime: input.entry.runtime,
            threadIds: input.entry.attachedThreadIds,
            type: "provider-session.updated",
            payload,
          });
        });

      const writeReleasedRuntimeRequestEvents = (input: {
        readonly entry: LiveSessionEntry;
        readonly reason: ProviderSessionReleaseReason;
      }) =>
        Effect.gen(function* () {
          const providerSessionId = input.entry.runtime.providerSessionId;
          const now = yield* DateTime.now;
          const status = releasedRuntimeRequestStatusFor(input.reason);
          const reason =
            input.reason === "runtime_error"
              ? "Provider session failed before this runtime request was resolved."
              : "Provider session was closed before this runtime request was resolved.";

          const events: Array<OrchestrationV2DomainEvent> = [];
          for (const threadId of input.entry.attachedThreadIds) {
            const projection = yield* projectionStore.getThreadRecords(
              threadId,
              ["runtimeRequests", "nodes", "turnItems"],
              { turnItemTypes: ["approval_request", "user_input_request"] },
            );
            const releasedRequests = projection.runtimeRequests.filter(
              (request) =>
                request.status === "pending" &&
                request.responseCapability.type === "live" &&
                request.responseCapability.providerSessionId === providerSessionId,
            );

            for (const request of releasedRequests) {
              events.push({
                id: yield* idAllocator.allocate.event({
                  threadId,
                  providerSessionId,
                }),
                type: "runtime-request.updated",
                threadId,
                nodeId: request.nodeId,
                driver: input.entry.runtime.driver,
                occurredAt: now,
                payload: {
                  ...request,
                  status,
                  responseCapability: {
                    type: "not_resumable",
                    reason,
                  },
                  resolvedAt: now,
                },
              });

              const requestNode = projection.nodes.find((node) => node.id === request.nodeId);
              if (requestNode !== undefined) {
                events.push({
                  id: yield* idAllocator.allocate.event({
                    threadId,
                    providerSessionId,
                  }),
                  type: "node.updated",
                  threadId,
                  ...(requestNode.runId === null ? {} : { runId: requestNode.runId }),
                  nodeId: requestNode.id,
                  driver: input.entry.runtime.driver,
                  occurredAt: now,
                  payload: {
                    ...requestNode,
                    status: input.reason === "runtime_error" ? "failed" : "cancelled",
                    completedAt: now,
                  },
                });
              }

              const turnItem = projection.turnItems.find(
                (item) =>
                  (item.type === "approval_request" || item.type === "user_input_request") &&
                  item.requestId === request.id,
              );
              if (turnItem !== undefined) {
                events.push({
                  id: yield* idAllocator.allocate.event({
                    threadId,
                    providerSessionId,
                  }),
                  type: "turn-item.updated",
                  threadId,
                  ...(turnItem.runId === null ? {} : { runId: turnItem.runId }),
                  ...(turnItem.nodeId === null ? {} : { nodeId: turnItem.nodeId }),
                  driver: input.entry.runtime.driver,
                  occurredAt: now,
                  payload: {
                    ...turnItem,
                    status: input.reason === "runtime_error" ? "failed" : "cancelled",
                    completedAt: now,
                    updatedAt: now,
                  },
                });
              }
            }
          }

          if (events.length > 0) {
            yield* eventSink.write({ events });
          }
        });

      const releaseEntry = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly reason: ProviderSessionReleaseReason;
        readonly detail?: string;
        readonly cancelIdleFiber?: boolean;
        readonly onlyIfIdleGeneration?: number;
        readonly gracefulSubscribers?: boolean;
      }) =>
        Effect.acquireUseRelease(
          Ref.modify(sessions, (current) => {
            const key = sessionKey(input.providerSessionId);
            const existing = current.get(key);
            if (existing === undefined) {
              return [Option.none<LiveSessionEntry>(), current] as const;
            }
            if (
              input.onlyIfIdleGeneration !== undefined &&
              (existing.busyCount > 0 || existing.idleGeneration !== input.onlyIfIdleGeneration)
            ) {
              return [Option.none<LiveSessionEntry>(), current] as const;
            }
            const updated = new Map(current);
            updated.delete(key);
            return [Option.some(existing), updated] as const;
          }),
          (entry) =>
            Option.match(entry, {
              onNone: () => Effect.void,
              onSome: (entry) =>
                Effect.gen(function* () {
                  if (input.cancelIdleFiber !== false) {
                    yield* cancelIdleFiber(entry.idleFiber);
                  }
                  if (input.gracefulSubscribers === true) {
                    yield* endSubscribers(entry);
                  } else if (input.reason === "server_shutdown") {
                    yield* closeSubscribers(entry);
                  } else {
                    yield* failSubscribers(
                      entry,
                      input.detail ?? `Provider session released: ${input.reason}.`,
                    );
                  }
                  // Scope close can wedge on a misbehaving adapter finalizer
                  // (e.g. a provider process that never yields its message
                  // stream). Time-box it so release still persists released
                  // events and leaves a diagnosable trail instead of silently
                  // parking the session as "ready" forever.
                  const closeFiber = yield* Scope.close(entry.scope, Exit.void).pipe(
                    Effect.exit,
                    Effect.forkDetach({ startImmediately: true }),
                  );
                  const closeExit = yield* Fiber.join(closeFiber).pipe(
                    Effect.timeoutOption(RELEASE_SCOPE_CLOSE_TIMEOUT_MS),
                  );
                  if (Option.isNone(closeExit)) {
                    yield* Effect.logWarning(
                      "orchestration-v2.provider-session-scope-close-timeout",
                      {
                        providerSessionId: input.providerSessionId,
                        reason: input.reason,
                        timeoutMs: RELEASE_SCOPE_CLOSE_TIMEOUT_MS,
                      },
                    );
                    yield* Fiber.join(closeFiber).pipe(
                      Effect.flatMap((exit) =>
                        Exit.isFailure(exit)
                          ? Effect.logWarning(
                              "orchestration-v2.provider-session-scope-close-failed",
                              {
                                providerSessionId: input.providerSessionId,
                                reason: input.reason,
                                cause: exit.cause,
                              },
                            )
                          : Effect.logInfo(
                              "orchestration-v2.provider-session-scope-close-completed-late",
                              {
                                providerSessionId: input.providerSessionId,
                                reason: input.reason,
                              },
                            ),
                      ),
                      Effect.forkDetach,
                    );
                  }
                  yield* writeReleasedSessionEvents({
                    entry,
                    reason: input.reason,
                    ...(input.detail === undefined ? {} : { detail: input.detail }),
                  });
                  yield* writeReleasedRuntimeRequestEvents({
                    entry,
                    reason: input.reason,
                  }).pipe(entry.requestEventPermit.withPermits(1));
                  if (Option.isSome(closeExit) && Exit.isFailure(closeExit.value)) {
                    return yield* Effect.failCause(closeExit.value.cause);
                  }
                }),
            }),
          (entry) =>
            Option.match(entry, {
              onNone: () => Effect.void,
              onSome: (entry) =>
                // Revoke every credential this session recorded, including for
                // threads that detached without re-attaching: the provider
                // process is gone, so nothing holds them anymore. Skip threads
                // a live replacement session took over, since credential reuse
                // means the replacement may hold this very credential.
                Ref.get(sessions).pipe(
                  Effect.flatMap((current) =>
                    Effect.forEach(
                      entry.mcpCredentialIdByThread,
                      ([threadId, mcpCredentialId]) => {
                        // Id-sensitive: a stale record for the same thread but
                        // a DIFFERENT credential (left behind by an old session
                        // the thread rotated away from) must not veto revoking
                        // this session's own credential, or it leaks forever.
                        // A reservation means an in-flight open is configuring
                        // a provider process with this credential right now;
                        // revoking it here would strand that process (eager
                        // adapters cannot pick up a rotated token).
                        const heldElsewhere =
                          isMcpCredentialReserved(threadId, mcpCredentialId) ||
                          Array.from(current.values()).some(
                            (other) =>
                              other !== entry &&
                              (other.attachedThreadIds.has(threadId) ||
                                other.mcpCredentialIdByThread.get(threadId) === mcpCredentialId),
                          );
                        return heldElsewhere
                          ? Effect.void
                          : clearMcpSession(threadId, mcpCredentialId);
                      },
                      { discard: true },
                    ),
                  ),
                ),
            }),
        ).pipe(
          Effect.catchCause((cause) =>
            Effect.fail(
              new ProviderSessionReleaseError({
                providerSessionId: input.providerSessionId,
                reason: input.reason,
                cause,
              }),
            ),
          ),
        );

      const finishDeferredDetaches = Effect.fnUntraced(function* (entry: LiveSessionEntry) {
        const ownedEntry = (yield* Ref.get(sessions)).get(
          sessionKey(entry.runtime.providerSessionId),
        );
        if (ownedEntry?.runtime !== entry.runtime) return;
        for (const [threadId, retained] of ownedEntry.preservedOutputThreadIds) {
          const buffered = yield* (
            entry.runtime.hasBufferedOutputForThread?.(retained.binding) ?? Effect.succeed(false)
          );
          if (buffered) continue;
          yield* Ref.update(sessions, (current) => {
            const key = sessionKey(entry.runtime.providerSessionId);
            const latest = current.get(key);
            if (
              latest?.runtime !== entry.runtime ||
              latest.preservedOutputThreadIds.get(threadId) !== retained
            )
              return current;
            const preservedOutputThreadIds = new Map(latest.preservedOutputThreadIds);
            preservedOutputThreadIds.delete(threadId);
            const executionSelections = new Map(latest.executionSelections);
            const evidence = executionSelections.get(threadId);
            const attachedThreadIds = new Set(latest.attachedThreadIds);
            attachedThreadIds.delete(threadId);
            const loadedProviderThreadKeyByThread = new Map(latest.loadedProviderThreadKeyByThread);
            loadedProviderThreadKeyByThread.delete(threadId);
            if (evidence !== undefined)
              executionSelections.set(threadId, {
                ...evidence,
                selection: null,
                operation: undefined,
              });
            return new Map(current).set(key, {
              ...latest,
              executionSelections,
              preservedOutputThreadIds,
              attachedThreadIds,
              loadedProviderThreadKeyByThread,
            });
          });
        }
      });

      // Annotated to break the releaseIfStillIdle <-> scheduleIdleReleaseInternal
      // inference cycle introduced by the pin re-arm below.
      const releaseIfStillIdle = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly generation: number;
      }): Effect.Effect<void> =>
        Effect.gen(function* () {
          const current = yield* Ref.get(sessions);
          const key = sessionKey(input.providerSessionId);
          const entry = current.get(key);
          if (
            entry === undefined ||
            entry.busyCount > 0 ||
            entry.idleGeneration !== input.generation
          ) {
            return;
          }
          yield* finishDeferredDetaches(entry).pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("orchestration-v2.driver-session.deferred-detach-failed", {
                providerSessionId: entry.runtime.providerSessionId,
                cause,
              }),
            ),
          );
          // Capture runtime identity before yielding: a replacement session
          // can reuse the same providerSessionId while this fiber is parked.
          const probedRuntime = entry.runtime;
          const hasPendingWork =
            probedRuntime.hasPendingBackgroundWork === undefined
              ? false
              : yield* probedRuntime.hasPendingBackgroundWork.pipe(
                  Effect.catchCause(() => Effect.succeed(false)),
                );
          const afterProbe = (yield* Ref.get(sessions)).get(key);
          if (
            afterProbe?.runtime !== probedRuntime ||
            afterProbe.busyCount > 0 ||
            afterProbe.idleGeneration !== input.generation
          )
            return;
          if (hasPendingWork || afterProbe.preservedOutputThreadIds.size > 0) {
            const now = yield* Clock.currentTimeMillis;
            const pinnedSinceMs = afterProbe.pinnedSinceMs ?? now;
            if (
              now - pinnedSinceMs < maxIdlePinMs ||
              afterProbe.preservedOutputThreadIds.size > 0
            ) {
              const shouldContinuePin = yield* Ref.modify(sessions, (latest) => {
                const latestEntry = latest.get(key);
                if (
                  latestEntry === undefined ||
                  latestEntry.busyCount > 0 ||
                  latestEntry.idleGeneration !== input.generation ||
                  latestEntry.runtime !== probedRuntime
                ) {
                  return [false, latest] as const;
                }
                const updated = new Map(latest);
                updated.set(key, { ...latestEntry, pinnedSinceMs });
                return [true, updated] as const;
              });
              if (!shouldContinuePin) {
                // Generation or runtime advanced while we probed pending work;
                // the current owner of the entry owns idle release.
                return;
              }
              yield* Effect.logInfo("orchestration-v2.driver-session.idle-release-deferred", {
                providerSessionId: input.providerSessionId,
                pinnedForMs: now - pinnedSinceMs,
              });
              // Re-check on this fiber after another idle window. Do not call
              // scheduleIdleReleaseInternal: that cancels entry.idleFiber, which
              // is this fiber, and can self-deadlock on Fiber.interrupt.
              yield* Effect.sleep(Duration.millis(idleTimeoutMs));
              return yield* releaseIfStillIdle(input);
            }
            yield* Effect.logWarning("orchestration-v2.driver-session.idle-release-pin-expired", {
              providerSessionId: input.providerSessionId,
              pinnedForMs: now - pinnedSinceMs,
            });
          }
          // hasPendingBackgroundWork yields to the adapter, so the idle
          // decision above can go stale; the generation guard revalidates
          // busyCount and idleGeneration inside releaseEntry's atomic
          // entry removal.
          yield* releaseEntry({
            providerSessionId: input.providerSessionId,
            reason: "idle_timeout",
            cancelIdleFiber: false,
            onlyIfIdleGeneration: input.generation,
          }).pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("orchestration-v2.driver-session.idle-release-failed", {
                providerSessionId: input.providerSessionId,
                cause,
              }),
            ),
          );
        });

      const withActivityError = <A, E, R>(
        providerSessionId: ProviderSessionId,
        effect: Effect.Effect<A, E, R>,
      ): Effect.Effect<A, ProviderSessionActivityError, R> =>
        effect.pipe(
          Effect.catchCause((cause) =>
            Effect.fail(
              new ProviderSessionActivityError({
                providerSessionId,
                cause,
              }),
            ),
          ),
        );

      const scheduleIdleReleaseInternal = (providerSessionId: ProviderSessionId) =>
        Effect.gen(function* () {
          const key = sessionKey(providerSessionId);
          const current = yield* Ref.get(sessions);
          const entry = current.get(key);
          if (entry === undefined || entry.busyCount > 0) {
            return;
          }

          yield* cancelIdleFiber(entry.idleFiber);
          const generation = entry.idleGeneration + 1;
          const idleFiber = yield* Effect.sleep(Duration.millis(idleTimeoutMs)).pipe(
            Effect.andThen(releaseIfStillIdle({ providerSessionId, generation })),
            Effect.forkIn(layerScope),
          );
          const lastActivityAtMs = yield* Clock.currentTimeMillis;
          yield* Ref.update(sessions, (latest) => {
            const latestEntry = latest.get(key);
            if (latestEntry === undefined || latestEntry.busyCount > 0) {
              return latest;
            }
            const updated = new Map(latest);
            updated.set(key, {
              ...latestEntry,
              idleGeneration: generation,
              idleFiber,
              lastActivityAtMs,
            });
            return updated;
          });
        });

      const scheduleIdleRelease = (providerSessionId: ProviderSessionId) =>
        withActivityError(providerSessionId, scheduleIdleReleaseInternal(providerSessionId));

      const touchActivity = (providerSessionId: ProviderSessionId) =>
        withActivityError(
          providerSessionId,
          Effect.gen(function* () {
            const lastActivityAtMs = yield* Clock.currentTimeMillis;
            yield* Ref.update(sessions, (current) => {
              const entry = current.get(sessionKey(providerSessionId));
              if (entry === undefined) {
                return current;
              }
              const updated = new Map(current);
              updated.set(sessionKey(providerSessionId), {
                ...entry,
                lastActivityAtMs,
              });
              return updated;
            });
            yield* scheduleIdleReleaseInternal(providerSessionId);
          }),
        );

      const attachThread = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
      }) =>
        withActivityError(
          input.providerSessionId,
          Ref.modify(sessions, (current) => {
            const entry = current.get(sessionKey(input.providerSessionId));
            if (entry === undefined || entry.attachedThreadIds.has(input.threadId)) {
              return [false, current] as const;
            }
            const updated = new Map(current);
            updated.set(sessionKey(input.providerSessionId), {
              ...entry,
              attachedThreadIds: new Set([...entry.attachedThreadIds, input.threadId]),
            });
            return [true, updated] as const;
          }),
        );

      const removeThreadAttachment = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
      }) =>
        Ref.update(sessions, (current) => {
          const key = sessionKey(input.providerSessionId);
          const entry = current.get(key);
          if (entry === undefined || !entry.attachedThreadIds.has(input.threadId)) {
            return current;
          }
          const attachedThreadIds = new Set(entry.attachedThreadIds);
          attachedThreadIds.delete(input.threadId);
          const loadedProviderThreadKeyByThread = new Map(entry.loadedProviderThreadKeyByThread);
          loadedProviderThreadKeyByThread.delete(input.threadId);
          const updated = new Map(current);
          updated.set(key, {
            ...entry,
            attachedThreadIds,
            loadedProviderThreadKeyByThread,
          });
          return updated;
        });

      const isProviderThreadLoaded = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
        readonly providerThreadKey: string;
      }) =>
        Ref.get(sessions).pipe(
          Effect.map(
            (current) =>
              current
                .get(sessionKey(input.providerSessionId))
                ?.loadedProviderThreadKeyByThread.get(input.threadId) === input.providerThreadKey,
          ),
        );

      const markProviderThreadLoaded = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
        readonly providerThreadKey: string;
      }) =>
        Ref.update(sessions, (current) => {
          const key = sessionKey(input.providerSessionId);
          const entry = current.get(key);
          if (entry === undefined) {
            return current;
          }
          const loadedProviderThreadKeyByThread = new Map(entry.loadedProviderThreadKeyByThread);
          loadedProviderThreadKeyByThread.set(input.threadId, input.providerThreadKey);
          const updated = new Map(current);
          updated.set(key, { ...entry, loadedProviderThreadKeyByThread });
          return updated;
        });

      const ensureThreadAttached = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
        readonly providerInstanceId: ProviderInstanceId;
      }) =>
        Effect.suspend(() => {
          let preparedForCleanup: PreparedMcpCredential | undefined;
          let reservationDropped = false;
          const dropReservation = () => {
            if (!reservationDropped && preparedForCleanup?.mcpCredentialId !== undefined) {
              reservationDropped = true;
              dropMcpCredentialReservation(input.threadId, preparedForCleanup.mcpCredentialId);
            }
          };
          return Effect.gen(function* () {
            const attached = yield* threadAttachment.withLock(
              threadAttachmentKey(input),
              attachThread(input),
            );
            if (attached) {
              const prepared = yield* prepareMcpSession(input.threadId, input.providerInstanceId);
              preparedForCleanup = prepared;
              if (prepared.mcpCredentialId !== undefined) {
                const mcpCredentialId = prepared.mcpCredentialId;
                yield* Ref.update(sessions, (current) => {
                  const key = sessionKey(input.providerSessionId);
                  const entry = current.get(key);
                  if (entry === undefined) return current;
                  const mcpCredentialIdByThread = new Map(entry.mcpCredentialIdByThread);
                  mcpCredentialIdByThread.set(input.threadId, mcpCredentialId);
                  const updated = new Map(current);
                  updated.set(key, { ...entry, mcpCredentialIdByThread });
                  return updated;
                });
              }
              const entry = (yield* Ref.get(sessions)).get(sessionKey(input.providerSessionId));
              if (entry !== undefined) {
                yield* withActivityError(
                  input.providerSessionId,
                  writeProviderSessionEvents({
                    runtime: entry.runtime,
                    threadIds: [input.threadId],
                    type: "provider-session.attached",
                    payload: entry.runtime.providerSession,
                  }),
                );
              }
            }
          }).pipe(
            Effect.tapError(() =>
              removeThreadAttachment(input).pipe(
                // Revoke only a credential this attach freshly minted: a REUSED
                // credential is by definition held by another live provider
                // process, and revoking it thread-wide would break that
                // process's MCP client mid-conversation.
                Effect.andThen(
                  Effect.suspend(() => {
                    dropReservation();
                    return preparedForCleanup?.issued === true
                      ? clearMcpSession(input.threadId, preparedForCleanup.mcpCredentialId)
                      : Effect.void;
                  }),
                ),
              ),
            ),
            // The entry's own record (written above while the thread is
            // attached) guards the credential from here on; the reservation
            // is only needed until then. Ensuring covers defects/interrupts.
            Effect.ensuring(Effect.sync(dropReservation)),
          );
        });

      const markBusy = (providerSessionId: ProviderSessionId) =>
        withActivityError(
          providerSessionId,
          Effect.gen(function* () {
            const key = sessionKey(providerSessionId);
            const now = yield* Clock.currentTimeMillis;
            const idleFiber = yield* Ref.modify(sessions, (current) => {
              const entry = current.get(key);
              if (entry === undefined) {
                return [null, current] as const;
              }
              const updated = new Map(current);
              updated.set(key, {
                ...entry,
                busyCount: entry.busyCount + 1,
                idleFiber: null,
                lastActivityAtMs: now,
                pinnedSinceMs: null,
              });
              return [entry.idleFiber, updated] as const;
            });
            yield* cancelIdleFiber(idleFiber);
          }),
        );

      const markIdle = (providerSessionId: ProviderSessionId) =>
        withActivityError(
          providerSessionId,
          Effect.gen(function* () {
            const key = sessionKey(providerSessionId);
            const now = yield* Clock.currentTimeMillis;
            yield* Ref.update(sessions, (current) => {
              const entry = current.get(key);
              if (entry === undefined) {
                return current;
              }
              const updated = new Map(current);
              updated.set(key, {
                ...entry,
                busyCount: Math.max(0, entry.busyCount - 1),
                lastActivityAtMs: now,
              });
              return updated;
            });
            yield* scheduleIdleReleaseInternal(providerSessionId);
          }),
        );

      const observeActivity = (
        providerSessionId: ProviderSessionId,
        activity: Effect.Effect<void, ProviderSessionActivityError>,
      ) =>
        activity.pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("orchestration-v2.driver-session.activity-failed", {
              providerSessionId,
              cause,
            }),
          ),
        );

      const makeEventSubscription = (
        subscribers: Ref.Ref<
          ReadonlyMap<number, Queue.Queue<ProviderSessionEventSignal, Cause.Done>>
        >,
      ): Effect.Effect<ProviderAdapterV2EventSubscription> =>
        Effect.gen(function* () {
          const queue = yield* Queue.unbounded<ProviderSessionEventSignal, Cause.Done>();
          const subscriberId = yield* Ref.getAndUpdate(nextSubscriberId, (value) => value + 1);
          yield* Ref.update(subscribers, (current) => {
            const updated = new Map(current);
            updated.set(subscriberId, queue);
            return updated;
          });
          const close = Ref.modify(subscribers, (current) => {
            if (!current.has(subscriberId)) {
              return [false, current] as const;
            }
            const updated = new Map(current);
            updated.delete(subscriberId);
            return [true, updated] as const;
          }).pipe(
            Effect.flatMap((removed) =>
              removed
                ? Queue.clear(queue).pipe(Effect.andThen(Queue.end(queue)), Effect.asVoid)
                : Effect.void,
            ),
          );
          const events = Stream.fromQueue(queue).pipe(
            Stream.mapEffect((signal) =>
              signal.type === "event"
                ? Effect.succeed(signal.event)
                : Effect.failCause(signal.cause),
            ),
            Stream.ensuring(close),
          );
          return { events, close } satisfies ProviderAdapterV2EventSubscription;
        });

      const decorateRuntime = (
        runtime: ProviderAdapterV2SessionRuntime,
        eventSubscribers: Ref.Ref<
          ReadonlyMap<number, Queue.Queue<ProviderSessionEventSignal, Cause.Done>>
        >,
      ): ProviderAdapterV2SessionRuntime => {
        const providerSessionId = runtime.providerSessionId;
        const selectionEvidence = Effect.fnUntraced(function* (threadId: ThreadId) {
          const entry = (yield* Ref.get(sessions)).get(sessionKey(providerSessionId));
          return entry?.runtime === runtime ? entry.executionSelections.get(threadId) : undefined;
        });
        const setSelectionEvidence = (
          threadId: ThreadId,
          evidence: ExecutionSelectionEvidence,
          expectedOperation?: symbol,
        ) =>
          Ref.update(sessions, (current) => {
            const key = sessionKey(providerSessionId);
            const entry = current.get(key);
            if (
              entry?.runtime !== runtime ||
              (expectedOperation !== undefined &&
                entry.executionSelections.get(threadId)?.operation !== expectedOperation)
            )
              return current;
            return new Map(current).set(key, {
              ...entry,
              executionSelections: new Map(entry.executionSelections).set(threadId, evidence),
            });
          });
        const subscribeEvents = makeEventSubscription(eventSubscribers);
        const observeSelection = Effect.fnUntraced(function* <A, E>(
          threadId: ThreadId,
          operation: Effect.Effect<A, E>,
          evidence: ExecutionSelectionEvidence,
          acceptedSelection: ModelSelection | null,
          drainOnly = false,
        ) {
          const token = Symbol();
          const previous = yield* selectionEvidence(threadId);
          yield* setSelectionEvidence(threadId, { ...evidence, operation: token });
          const entry = (yield* Ref.get(sessions)).get(sessionKey(providerSessionId));
          const retained = entry?.preservedOutputThreadIds.get(threadId);
          if (entry?.runtime === runtime && retained !== undefined) {
            const preservedOutputThreadIds = new Map(entry.preservedOutputThreadIds);
            if (
              evidence.bufferedRunOrdinal !== undefined &&
              retained.binding.id === evidence.providerThreadId &&
              retained.binding.nativeThreadRef?.nativeId === evidence.nativeThreadId
            ) {
              preservedOutputThreadIds.set(threadId, { ...retained, operation: token });
            } else {
              preservedOutputThreadIds.delete(threadId);
            }
            yield* Ref.update(sessions, (current) =>
              current.get(sessionKey(providerSessionId)) === entry
                ? new Map(current).set(sessionKey(providerSessionId), {
                    ...entry,
                    preservedOutputThreadIds,
                  })
                : current,
            );
          }
          const result = yield* Effect.exit(operation);
          const current = yield* selectionEvidence(threadId);
          if (current?.operation === token) {
            let selection = Exit.isSuccess(result) ? acceptedSelection : null;
            if (drainOnly) {
              selection = previous?.selection ?? null;
            } else if (selection !== null && runtime.selectionAcknowledged !== undefined) {
              if (!(yield* runtime.selectionAcknowledged(selection))) selection = null;
            }
            if (!drainOnly && Exit.isFailure(result) && !Cause.hasInterrupts(result.cause)) {
              const failure = Cause.findErrorOption(result.cause);
              const error =
                Option.isSome(failure) && Schema.is(ProviderAdapterTurnStartError)(failure.value)
                  ? failure.value.cause
                  : Option.getOrUndefined(failure);
              const refused =
                Schema.is(ProviderAdapterRunningWorkError)(error) ||
                Schema.is(ProviderAdapterBufferedOutputError)(error);
              const sameSelection =
                runtime.preservesSelectionOnSameSelectionFailure === true &&
                previous?.selection != null &&
                acceptedSelection !== null &&
                modelSelectionsEqual(previous.selection, acceptedSelection);
              if (refused || sameSelection) selection = previous?.selection ?? null;
              if (selection === null && evidence.providerThread !== undefined)
                selection = yield* (
                  runtime.liveExecutionSelection?.(evidence.providerThread) ?? Effect.succeed(null)
                );
            }
            if ((yield* selectionEvidence(threadId))?.operation === token) {
              yield* setSelectionEvidence(
                threadId,
                {
                  ...current,
                  ...(Exit.isFailure(result) &&
                  selection !== null &&
                  previous?.selection != null &&
                  modelSelectionsEqual(selection, previous.selection)
                    ? previous
                    : {}),
                  operation: token,
                  selection,
                },
                token,
              );
            }
          }
          return yield* result;
        });
        const protectBufferedOutput = Effect.fnUntraced(function* (
          thread: OrchestrationV2ProviderThread | undefined,
          selection?: ModelSelection,
          policy?: ProviderAdapterV2RuntimePolicy,
          starting = false,
        ) {
          if (thread === undefined || thread.appThreadId === null) return;
          const buffered = yield* (
            runtime.hasBufferedOutputForThread?.(thread) ?? Effect.succeed(false)
          );
          if (!buffered) return;
          const evidence = yield* selectionEvidence(thread.appThreadId);
          const bufferedSelection = yield* (
            runtime.bufferedExecutionSelection?.(thread) ?? Effect.succeed(null)
          );
          if (
            starting &&
            bufferedSelection !== null &&
            selection !== undefined &&
            modelSelectionsEqual(bufferedSelection, selection)
          )
            return;
          if (
            starting &&
            evidence?.selection !== null &&
            evidence?.loadKey ===
              providerThreadLoadKey({
                providerThread: thread,
                ...(selection === undefined ? {} : { modelSelection: selection }),
                ...(policy === undefined ? {} : { runtimePolicy: policy }),
              })
          )
            return;
          return yield* new ProviderAdapterBufferedOutputError({
            driver: runtime.driver,
            providerThreadId: thread.id,
          });
        });
        const rememberBinding = Effect.fnUntraced(function* (
          thread: OrchestrationV2ProviderThread,
        ) {
          if (thread.appThreadId === null) return;
          const previous = yield* selectionEvidence(thread.appThreadId);
          if (
            previous?.providerThreadId !== thread.id ||
            previous.nativeThreadId !== (thread.nativeThreadRef?.nativeId ?? null)
          ) {
            yield* setSelectionEvidence(thread.appThreadId, {
              providerThread: thread,
              providerThreadId: thread.id,
              nativeThreadId: thread.nativeThreadRef?.nativeId ?? null,
              selection: null,
            });
          }
        });
        const invalidateBinding = (threadId: ThreadId) =>
          Ref.update(sessions, (current) => {
            const key = sessionKey(providerSessionId);
            const entry = current.get(key);
            if (entry?.runtime !== runtime) return current;
            const executionSelections = new Map(entry.executionSelections);
            const preservedOutputThreadIds = new Map(entry.preservedOutputThreadIds);
            preservedOutputThreadIds.delete(threadId);
            const previous = executionSelections.get(threadId);
            if (previous !== undefined)
              executionSelections.set(threadId, {
                ...previous,
                selection: null,
                operation: undefined,
              });
            return new Map(current).set(key, {
              ...entry,
              executionSelections,
              preservedOutputThreadIds,
            });
          });
        return {
          ...runtime,
          // Only a binding this runtime loaded, resumed or ran has a selection
          // that can be uncertain. A thread merely attached when the session
          // opened (as a rollback opens one) is still a fresh attachment: its
          // next turn applies the selection like any first turn.
          hasResidentBinding: (thread) =>
            Effect.gen(function* () {
              const entry = (yield* Ref.get(sessions)).get(sessionKey(providerSessionId));
              return (
                entry?.runtime === runtime &&
                thread.appThreadId !== null &&
                thread.providerSessionId === providerSessionId &&
                entry.executionSelections.has(thread.appThreadId)
              );
            }),
          executionSelection: (thread) =>
            Effect.gen(function* () {
              const entry = (yield* Ref.get(sessions)).get(sessionKey(providerSessionId));
              if (
                entry?.runtime !== runtime ||
                thread.appThreadId === null ||
                thread.providerSessionId !== providerSessionId ||
                thread.providerInstanceId !== runtime.instanceId ||
                thread.driver !== runtime.driver ||
                (thread.nativeThreadRef !== null &&
                  thread.nativeThreadRef.driver !== runtime.driver) ||
                !entry.attachedThreadIds.has(thread.appThreadId)
              )
                return null;
              const evidence = yield* selectionEvidence(thread.appThreadId);
              return evidence?.providerThreadId === thread.id &&
                evidence.nativeThreadId === (thread.nativeThreadRef?.nativeId ?? null)
                ? evidence.selection
                : null;
            }),
          subscribeEvents,
          events: Stream.unwrap(
            subscribeEvents.pipe(Effect.map((subscription) => subscription.events)),
          ),
          ensureThread: (input) =>
            observeActivity(
              providerSessionId,
              ensureThreadAttached({
                providerSessionId,
                threadId: input.threadId,
                providerInstanceId: runtime.instanceId,
              }),
            ).pipe(
              Effect.andThen(
                protectBufferedOutput(
                  input.existingProviderThread,
                  input.modelSelection,
                  input.runtimePolicy,
                ),
              ),
              Effect.andThen(invalidateBinding(input.threadId)),
              Effect.andThen(runtime.ensureThread(input)),
              Effect.tap(rememberBinding),
              Effect.tap((providerThread) =>
                markProviderThreadLoaded({
                  providerSessionId,
                  threadId: input.threadId,
                  providerThreadKey: providerThreadLoadKey({
                    providerThread,
                    modelSelection: input.modelSelection,
                    runtimePolicy: input.runtimePolicy,
                  }),
                }),
              ),
            ),
          resumeThread: (input) => {
            const threadId = input.threadId ?? input.providerThread.appThreadId;
            if (threadId === null || threadId === undefined) {
              return runtime.resumeThread(input);
            }
            const providerThreadKey = providerThreadLoadKey({
              providerThread: input.providerThread,
              ...(input.modelSelection === undefined
                ? {}
                : { modelSelection: input.modelSelection }),
              ...(input.runtimePolicy === undefined ? {} : { runtimePolicy: input.runtimePolicy }),
            });
            return observeActivity(
              providerSessionId,
              ensureThreadAttached({
                providerSessionId,
                threadId,
                providerInstanceId: runtime.instanceId,
              }),
            ).pipe(
              Effect.andThen(
                isProviderThreadLoaded({ providerSessionId, threadId, providerThreadKey }),
              ),
              Effect.flatMap((loaded) =>
                Effect.gen(function* () {
                  const buffered = yield* (
                    runtime.bufferedExecutionSelection?.(input.providerThread) ??
                      Effect.succeed(null)
                  );
                  return (
                    loaded ||
                    (buffered !== null &&
                      input.modelSelection !== undefined &&
                      modelSelectionsEqual(buffered, input.modelSelection))
                  );
                }),
              ),
              Effect.flatMap((loaded) =>
                loaded
                  ? Effect.succeed(input.providerThread)
                  : protectBufferedOutput(
                      input.providerThread,
                      input.modelSelection,
                      input.runtimePolicy,
                    ).pipe(
                      Effect.andThen(invalidateBinding(threadId)),
                      Effect.andThen(runtime.resumeThread(input)),
                    ),
              ),
              Effect.tap(rememberBinding),
              Effect.tap((providerThread) =>
                markProviderThreadLoaded({
                  providerSessionId,
                  threadId,
                  providerThreadKey: providerThreadLoadKey({
                    providerThread,
                    ...(input.modelSelection === undefined
                      ? {}
                      : { modelSelection: input.modelSelection }),
                    ...(input.runtimePolicy === undefined
                      ? {}
                      : { runtimePolicy: input.runtimePolicy }),
                  }),
                }),
              ),
            );
          },
          forkThread: (input) =>
            observeActivity(
              providerSessionId,
              ensureThreadAttached({
                providerSessionId,
                threadId: input.targetThreadId,
                providerInstanceId: runtime.instanceId,
              }),
            ).pipe(
              Effect.andThen(invalidateBinding(input.targetThreadId)),
              Effect.andThen(runtime.forkThread(input)),
              Effect.tap(rememberBinding),
              Effect.tap((providerThread) =>
                markProviderThreadLoaded({
                  providerSessionId,
                  threadId: input.targetThreadId,
                  providerThreadKey: providerThreadLoadKey({
                    providerThread,
                    ...(input.modelSelection === undefined
                      ? {}
                      : { modelSelection: input.modelSelection }),
                    ...(input.runtimePolicy === undefined
                      ? {}
                      : { runtimePolicy: input.runtimePolicy }),
                  }),
                }),
              ),
            ),
          startTurn: (input) =>
            Effect.gen(function* () {
              const previous = yield* selectionEvidence(input.threadId);
              const reapplySelection =
                previous?.runAttemptId !== undefined && previous.selection == null;
              return yield* protectBufferedOutput(
                input.providerThread,
                input.modelSelection,
                input.runtimePolicy,
                true,
              ).pipe(
                Effect.andThen(
                  observeActivity(
                    providerSessionId,
                    ensureThreadAttached({
                      providerSessionId,
                      threadId: input.threadId,
                      providerInstanceId: runtime.instanceId,
                    }),
                  ).pipe(
                    Effect.andThen(observeActivity(providerSessionId, markBusy(providerSessionId))),
                    Effect.andThen(
                      observeSelection(
                        input.threadId,
                        runtime.startTurn({ ...input, reapplySelection }),
                        {
                          providerThread: input.providerThread,
                          providerThreadId: input.providerThread.id,
                          nativeThreadId: input.providerThread.nativeThreadRef?.nativeId ?? null,
                          selection: null,
                          runAttemptId: input.attemptId,
                          loadKey: providerThreadLoadKey({
                            providerThread: input.providerThread,
                            modelSelection: input.modelSelection,
                            runtimePolicy: input.runtimePolicy,
                          }),
                          ...(input.message.createdBy === "agent" &&
                          input.message.creationSource === "provider"
                            ? { bufferedRunOrdinal: input.runOrdinal }
                            : {}),
                        },
                        input.modelSelection,
                        runtime.continuationDrainsOutput === true &&
                          input.message.createdBy === "agent" &&
                          input.message.creationSource === "provider",
                      ),
                    ),
                    Effect.catch((error) =>
                      observeActivity(providerSessionId, markIdle(providerSessionId)).pipe(
                        Effect.andThen(Effect.fail(error)),
                      ),
                    ),
                  ),
                ),
              );
            }),
          steerTurn: (input) =>
            observeActivity(providerSessionId, touchActivity(providerSessionId)).pipe(
              Effect.andThen(runtime.steerTurn(input)),
            ),
          interruptTurn: (input) =>
            observeActivity(providerSessionId, touchActivity(providerSessionId)).pipe(
              Effect.andThen(runtime.interruptTurn(input)),
            ),
          respondToRuntimeRequest: (input) =>
            observeActivity(providerSessionId, touchActivity(providerSessionId)).pipe(
              Effect.andThen(runtime.respondToRuntimeRequest(input)),
            ),
        };
      };

      const persistProviderSessionUpdate = (
        entry: LiveSessionEntry,
        event: Extract<ProviderAdapterV2Event, { readonly type: "provider_session.updated" }>,
      ) =>
        Effect.gen(function* () {
          const current = (yield* Ref.get(sessions)).get(
            sessionKey(entry.runtime.providerSessionId),
          );
          if (current?.runtime !== entry.runtime) {
            return;
          }
          yield* writeProviderSessionEvents({
            runtime: entry.runtime,
            threadIds: current.attachedThreadIds,
            type: "provider-session.updated",
            payload: event.providerSession,
          });
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("orchestration-v2.driver-session.status-persist-failed", {
              providerSessionId: entry.runtime.providerSessionId,
              cause,
            }),
          ),
        );

      const startEventPump = (entry: LiveSessionEntry) => {
        let stoppedByProvider = false;
        return entry.runtime.events.pipe(
          Stream.runForEach((event) => {
            if (
              event.type === "provider_session.updated" &&
              event.providerSession.status === "stopped"
            ) {
              stoppedByProvider = true;
            }
            const recordEvidence = Ref.update(sessions, (current) => {
              const key = sessionKey(entry.runtime.providerSessionId);
              const latest = current.get(key);
              if (latest?.runtime !== entry.runtime) return current;
              const executionSelections = new Map(latest.executionSelections);
              if (event.type === "provider_turn.updated") {
                for (const [threadId, evidence] of executionSelections) {
                  if (
                    evidence.providerThreadId === event.providerTurn.providerThreadId &&
                    evidence.runAttemptId === event.providerTurn.runAttemptId
                  ) {
                    executionSelections.set(threadId, {
                      ...evidence,
                      providerTurnId: event.providerTurn.id,
                    });
                  }
                }
              }
              if (event.type === "turn.terminal") {
                for (const [threadId, evidence] of executionSelections) {
                  if (
                    evidence.providerThreadId === event.providerThreadId &&
                    evidence.providerTurnId === event.providerTurnId
                  ) {
                    const { bufferedRunOrdinal: _bufferedRunOrdinal, ...completed } = evidence;
                    executionSelections.set(threadId, completed);
                  }
                }
              }
              return new Map(current).set(key, { ...latest, executionSelections });
            });
            return recordEvidence
              .pipe(
                Effect.andThen(
                  observeActivity(
                    entry.runtime.providerSessionId,
                    event.type === "turn.terminal"
                      ? markIdle(entry.runtime.providerSessionId)
                      : touchActivity(entry.runtime.providerSessionId),
                  ),
                ),
              )
              .pipe(
                Effect.andThen(
                  event.type === "turn.terminal"
                    ? finishDeferredDetaches(entry).pipe(
                        Effect.catchCause((cause) =>
                          Effect.logWarning(
                            "orchestration-v2.driver-session.deferred-detach-failed",
                            { providerSessionId: entry.runtime.providerSessionId, cause },
                          ),
                        ),
                      )
                    : Effect.void,
                ),
                Effect.andThen(
                  event.type === "provider_session.updated"
                    ? persistProviderSessionUpdate(entry, event)
                    : Effect.void,
                ),
                Effect.andThen(
                  Effect.gen(function* () {
                    // Some providers can block before a run subscriber exists
                    // (project trust, login, or session-switch hooks). Persist
                    // their runless request artifacts directly so the normal T3
                    // request UI can answer them and unblock session setup.
                    const threadId = sessionScopedRuntimeRequestThreadId(event);
                    if (threadId !== undefined) {
                      yield* Effect.gen(function* () {
                        const current = (yield* Ref.get(sessions)).get(
                          sessionKey(entry.runtime.providerSessionId),
                        );
                        if (current?.runtime !== entry.runtime) return;
                        yield* providerEventIngestor
                          .ingestNormalized({
                            providerSessionId: entry.runtime.providerSessionId,
                            providerInstanceId: entry.runtime.instanceId,
                            threadId,
                            event,
                          })
                          .pipe(
                            Effect.mapError(
                              (cause) =>
                                new ProviderAdapterEventStreamError({
                                  driver: entry.runtime.driver,
                                  providerSessionId: entry.runtime.providerSessionId,
                                  cause,
                                }),
                            ),
                          );
                      }).pipe(entry.requestEventPermit.withPermits(1));
                      return;
                    }
                    yield* publishToSubscribers(entry.eventSubscribers, { type: "event", event });
                  }),
                ),
              );
          }),
          Effect.exit,
          Effect.flatMap((exit) =>
            Effect.gen(function* () {
              const current = (yield* Ref.get(sessions)).get(
                sessionKey(entry.runtime.providerSessionId),
              );
              if (current?.runtime !== entry.runtime) {
                return;
              }
              if (stoppedByProvider && Exit.isSuccess(exit)) {
                yield* releaseEntry({
                  providerSessionId: entry.runtime.providerSessionId,
                  reason: "manual_shutdown",
                  gracefulSubscribers: true,
                }).pipe(Effect.ignore);
                return;
              }
              const cause = Exit.isFailure(exit)
                ? exit.cause
                : Cause.fail(
                    new ProviderAdapterEventStreamError({
                      driver: entry.runtime.driver,
                      providerSessionId: entry.runtime.providerSessionId,
                      cause: "Provider event stream ended unexpectedly.",
                    }),
                  );
              yield* publishToSubscribers(entry.eventSubscribers, {
                type: "failure",
                cause,
              });
              yield* Ref.set(entry.eventSubscribers, new Map());
              yield* releaseEntry({
                providerSessionId: entry.runtime.providerSessionId,
                reason: "runtime_error",
                detail: Cause.pretty(cause),
              }).pipe(Effect.ignore);
            }),
          ),
          Effect.forkIn(layerScope),
        );
      };

      const shutdown = Effect.gen(function* () {
        const activeSessions = [...(yield* Ref.get(sessions)).values()];
        yield* Effect.forEach(
          activeSessions,
          (entry) =>
            releaseEntry({
              providerSessionId: entry.runtime.providerSessionId,
              reason: "server_shutdown",
            }).pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("orchestration-v2.driver-session.shutdown-release-failed", {
                  providerSessionId: entry.runtime.providerSessionId,
                  cause,
                }),
              ),
            ),
          { discard: true },
        );
      });
      yield* Effect.addFinalizer(() => shutdown);

      return ProviderSessionManagerV2.of({
        shutdown,
        open: (input) =>
          sessionOpen.withLock(
            input.providerSessionId,
            Effect.gen(function* () {
              const cwd = input.runtimePolicy.cwd;
              if (cwd !== null) {
                const workspaceIsDirectory = yield* fileSystem.stat(cwd).pipe(
                  Effect.map((stat) => stat.type === "Directory"),
                  Effect.catch((error) => Effect.succeed(error.reason._tag !== "NotFound")),
                );
                if (!workspaceIsDirectory) {
                  return yield* new ProviderWorkspaceMissingError({
                    threadId: input.threadId,
                    cwd,
                  });
                }
              }
              const key = sessionKey(input.providerSessionId);
              const existing = (yield* Ref.get(sessions)).get(key);
              if (existing !== undefined) {
                if (
                  !existing.attachedThreadIds.has(input.threadId) &&
                  !existing.supportsMultipleProviderThreads
                ) {
                  return yield* new ProviderSessionOpenError({
                    instanceId: input.modelSelection.instanceId,
                    providerSessionId: input.providerSessionId,
                    cause: `Provider ${existing.runtime.driver} does not support attaching multiple app threads to one session.`,
                  });
                }
                yield* ensureThreadAttached({
                  providerSessionId: input.providerSessionId,
                  threadId: input.threadId,
                  providerInstanceId: existing.runtime.instanceId,
                });
                yield* touchActivity(input.providerSessionId);
                return existing.exposedRuntime;
              }

              const adapter = yield* registry.get(input.modelSelection.instanceId).pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderSessionOpenError({
                      instanceId: input.modelSelection.instanceId,
                      providerSessionId: input.providerSessionId,
                      cause,
                    }),
                ),
              );
              const prepared = yield* prepareMcpSession(
                input.threadId,
                input.modelSelection.instanceId,
              );
              const mcpCredentialId = prepared.mcpCredentialId;
              // The reservation from prepare protects the credential (which
              // eager adapters bake into the provider process during
              // openSession) from racing releases until this session's entry
              // is recorded below. Dropped exactly once on every path.
              let reservationDropped = mcpCredentialId === undefined;
              const dropReservation = Effect.sync(() => {
                if (!reservationDropped && mcpCredentialId !== undefined) {
                  reservationDropped = true;
                  dropMcpCredentialReservation(input.threadId, mcpCredentialId);
                }
              });
              const sessionScope = yield* Scope.make();
              const runtime = yield* adapter
                .openSession({
                  threadId: input.threadId,
                  providerSessionId: input.providerSessionId,
                  modelSelection: input.modelSelection,
                  runtimePolicy: input.runtimePolicy,
                  ...(input.resumeFromSession === undefined
                    ? {}
                    : { resumeFromSession: input.resumeFromSession }),
                  ...(input.initialNativeThreadId === undefined
                    ? {}
                    : { initialNativeThreadId: input.initialNativeThreadId }),
                  ...(input.initialProviderItemIdentityVersion === undefined
                    ? {}
                    : {
                        initialProviderItemIdentityVersion:
                          input.initialProviderItemIdentityVersion,
                      }),
                })
                .pipe(
                  Effect.provideService(Scope.Scope, sessionScope),
                  Effect.tapError(() =>
                    Scope.close(sessionScope, Exit.void).pipe(
                      Effect.ignore,
                      Effect.andThen(dropReservation),
                      // Revoke only a credential this open freshly minted: a
                      // reused credential is held by another live provider
                      // process and must survive this open's failure.
                      Effect.andThen(
                        prepared.issued
                          ? clearMcpSession(input.threadId, mcpCredentialId)
                          : Effect.void,
                      ),
                    ),
                  ),
                  Effect.onInterrupt(() => dropReservation),
                  Effect.mapError(
                    (cause) =>
                      new ProviderSessionOpenError({
                        instanceId: input.modelSelection.instanceId,
                        providerSessionId: input.providerSessionId,
                        cause,
                      }),
                  ),
                );
              const eventSubscribers = yield* Ref.make<
                ReadonlyMap<number, Queue.Queue<ProviderSessionEventSignal, Cause.Done>>
              >(new Map());
              const executionSelections = new Map<ThreadId, ExecutionSelectionEvidence>();
              const exposedRuntime = decorateRuntime(runtime, eventSubscribers);
              const now = yield* Clock.currentTimeMillis;
              const entry: LiveSessionEntry = {
                attachedThreadIds: new Set([input.threadId]),
                loadedProviderThreadKeyByThread: new Map(),
                executionSelections,
                preservedOutputThreadIds: new Map(),
                mcpCredentialIdByThread:
                  mcpCredentialId === undefined
                    ? new Map()
                    : new Map([[input.threadId, mcpCredentialId]]),
                supportsMultipleProviderThreads:
                  runtime.providerSession.capabilities.sessions
                    .supportsMultipleProviderThreadsPerSession,
                runtime,
                exposedRuntime,
                eventSubscribers,
                requestEventPermit: yield* Semaphore.make(1),
                scope: sessionScope,
                idleGeneration: 0,
                busyCount: 0,
                lastActivityAtMs: now,
                idleFiber: null,
                pinnedSinceMs: null,
              };
              yield* Ref.update(sessions, (current) => {
                const updated = new Map(current);
                updated.set(key, entry);
                return updated;
              });
              // The entry now guards the credential via its recorded id, so
              // the pre-open reservation can be dropped.
              yield* dropReservation;
              yield* withActivityError(
                input.providerSessionId,
                writeProviderSessionEvents({
                  runtime,
                  threadIds: [input.threadId],
                  type: "provider-session.attached",
                  payload: runtime.providerSession,
                }),
              ).pipe(
                Effect.tapError(() =>
                  releaseEntry({
                    providerSessionId: input.providerSessionId,
                    reason: "runtime_error",
                    detail: "Failed to persist the provider-session attachment.",
                  }).pipe(Effect.ignore),
                ),
              );
              yield* startEventPump(entry);
              yield* scheduleIdleRelease(input.providerSessionId);
              return exposedRuntime;
            }),
          ),
        get: (providerSessionId) =>
          Effect.gen(function* () {
            const entry = (yield* Ref.get(sessions)).get(sessionKey(providerSessionId));
            if (entry === undefined) {
              return Option.none<ProviderAdapterV2SessionRuntime>();
            }
            yield* touchActivity(providerSessionId);
            return Option.some(entry.exposedRuntime);
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderSessionLookupError({
                  providerSessionId,
                  cause,
                }),
            ),
          ),
        close: (providerSessionId) =>
          releaseEntry({ providerSessionId, reason: "manual_shutdown" }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderSessionCloseError({
                  providerSessionId,
                  cause,
                }),
            ),
          ),
        closeInstance: (instanceId) =>
          Effect.gen(function* () {
            const active = [...(yield* Ref.get(sessions)).values()].filter(
              (entry) => entry.runtime.instanceId === instanceId,
            );
            const outcomes = yield* Effect.forEach(
              active,
              (entry) =>
                releaseEntry({
                  providerSessionId: entry.runtime.providerSessionId,
                  reason: "manual_shutdown",
                  detail: `Provider instance ${instanceId} logged out.`,
                }).pipe(Effect.exit),
              { concurrency: "unbounded" },
            );
            const failure = outcomes.find(Exit.isFailure);
            if (failure !== undefined && Exit.isFailure(failure)) {
              return yield* Effect.failCause(failure.cause);
            }
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderSessionCloseError({
                  providerSessionId: ProviderSessionId.make(
                    `provider-session:provider-instance:${instanceId}`,
                  ),
                  cause,
                }),
            ),
          ),
        release: releaseEntry,
        detach: (input) =>
          Effect.gen(function* () {
            const key = sessionKey(input.providerSessionId);
            const currentEntry = (yield* Ref.get(sessions)).get(key);
            let detachedProviderThreads: ReadonlyArray<OrchestrationV2ProviderThread> = [];
            if (input.preserveBufferedOutput === true && currentEntry !== undefined) {
              const evidence = currentEntry.executionSelections.get(input.threadId);
              const projectedBinding =
                evidence?.providerThread ??
                (yield* projectionStore.getThreadRecords(input.threadId, [
                  "providerThreads",
                ])).providerThreads.find(
                  (thread) =>
                    thread.id === evidence?.providerThreadId ||
                    thread.providerSessionId === input.providerSessionId,
                );
              const binding =
                projectedBinding === undefined
                  ? undefined
                  : {
                      ...projectedBinding,
                      providerSessionId: input.providerSessionId,
                      nativeThreadRef:
                        evidence?.nativeThreadId == null
                          ? projectedBinding.nativeThreadRef
                          : {
                              driver: currentEntry.runtime.driver,
                              nativeId: evidence.nativeThreadId,
                              strength: "strong" as const,
                            },
                    };
              const buffered =
                currentEntry.executionSelections.get(input.threadId)?.bufferedRunOrdinal !==
                undefined;
              const owned =
                binding === undefined
                  ? false
                  : yield* (
                      currentEntry.runtime.hasBufferedOutputForThread?.(binding) ??
                        Effect.succeed(false)
                    );
              if ((buffered || owned) && binding !== undefined) {
                const latest = (yield* Ref.get(sessions)).get(key);
                if (
                  latest?.runtime !== currentEntry.runtime ||
                  latest.executionSelections.get(input.threadId)?.operation !== evidence?.operation
                )
                  return;
                yield* Ref.update(sessions, (current) => {
                  const active = current.get(key);
                  if (
                    active?.runtime !== currentEntry.runtime ||
                    active.executionSelections.get(input.threadId)?.operation !==
                      evidence?.operation
                  )
                    return current;
                  return new Map(current).set(key, {
                    ...active,
                    preservedOutputThreadIds: new Map(active.preservedOutputThreadIds).set(
                      input.threadId,
                      { binding, operation: evidence?.operation },
                    ),
                  });
                });
                yield* scheduleIdleRelease(input.providerSessionId);
                return;
              }
              const running =
                binding === undefined
                  ? false
                  : yield* (
                      currentEntry.runtime.hasPendingBackgroundWorkForThread?.(binding) ??
                        Effect.succeed(false)
                    );
              if (running)
                return yield* new ProviderAdapterRunningWorkError({
                  driver: currentEntry.runtime.driver,
                });
            }
            if (currentEntry?.supportsMultipleProviderThreads === true) {
              const projection = yield* Effect.option(
                projectionStore.getThreadRecords(input.threadId, [
                  "providerThreads",
                  "providerTurns",
                ]),
              );
              if (Option.isSome(projection)) {
                const providerThreads = new Map(
                  projection.value.providerThreads
                    .filter((thread) => thread.providerSessionId === input.providerSessionId)
                    .map((thread) => [thread.id, thread] as const),
                );
                detachedProviderThreads = [...providerThreads.values()];
                const activeTurns = projection.value.providerTurns.filter(
                  (turn) => turn.status === "running" && providerThreads.has(turn.providerThreadId),
                );
                yield* Effect.forEach(
                  activeTurns,
                  (turn) =>
                    currentEntry.exposedRuntime
                      .interruptTurn({
                        providerThread: providerThreads.get(turn.providerThreadId)!,
                        providerTurnId: turn.id,
                      })
                      .pipe(
                        Effect.catchCause((cause) =>
                          Effect.logWarning(
                            "orchestration-v2.driver-session.detach-interrupt-failed",
                            {
                              providerSessionId: input.providerSessionId,
                              threadId: input.threadId,
                              providerTurnId: turn.id,
                              cause,
                            },
                          ),
                        ),
                      ),
                  { concurrency: 1, discard: true },
                );
              }
            }
            const detached = yield* Ref.modify(sessions, (current) => {
              const entry = current.get(key);
              if (entry === undefined || !entry.attachedThreadIds.has(input.threadId)) {
                return [Option.none<LiveSessionEntry>(), current] as const;
              }
              const attachedThreadIds = new Set(entry.attachedThreadIds);
              attachedThreadIds.delete(input.threadId);
              const loadedProviderThreadKeyByThread = new Map(
                entry.loadedProviderThreadKeyByThread,
              );
              loadedProviderThreadKeyByThread.delete(input.threadId);
              const executionSelections = new Map(entry.executionSelections);
              const preservedOutputThreadIds = new Map(entry.preservedOutputThreadIds);
              if (input.preserveBufferedOutput === true) {
                const evidence = executionSelections.get(input.threadId);
                if (evidence !== undefined)
                  executionSelections.set(input.threadId, {
                    ...evidence,
                    selection: null,
                    operation: undefined,
                  });
              } else {
                executionSelections.delete(input.threadId);
              }
              preservedOutputThreadIds.delete(input.threadId);
              // For a plain (workspace-change) detach, the credential id stays
              // recorded: the thread may re-attach and reuse it, and
              // releaseEntry revokes it when the provider process finally goes
              // away. A terminal detach (archive/delete) prunes the record so
              // nothing vetoes the revocation below.
              const mcpCredentialIdByThread =
                input.revokeMcpCredential === true
                  ? (() => {
                      const pruned = new Map(entry.mcpCredentialIdByThread);
                      pruned.delete(input.threadId);
                      return pruned;
                    })()
                  : entry.mcpCredentialIdByThread;
              const updatedEntry = {
                ...entry,
                executionSelections,
                preservedOutputThreadIds,
                attachedThreadIds,
                loadedProviderThreadKeyByThread,
                mcpCredentialIdByThread,
              };
              const updated = new Map(current);
              updated.set(key, updatedEntry);
              return [Option.some(updatedEntry), updated] as const;
            });
            // Plain detaches deliberately do not revoke: a detached thread's
            // provider process may still be alive (shared multi-thread codex
            // session across a workspace handoff) and holds its MCP client's
            // credential for the thread it will re-attach with. Credentials
            // are revoked when the session entry is released (process gone)
            // or rotated on the next attach if they stopped resolving.
            // Terminal detaches (thread archived or deleted) revoke the
            // thread's credentials immediately, even on a retry where the
            // entry is already gone: there is no legitimate future re-attach,
            // and the token must not outlive the thread.
            if (input.revokeMcpCredential === true) {
              yield* clearMcpSession(input.threadId);
            }
            if (Option.isNone(detached)) {
              return;
            }
            if (
              detached.value.attachedThreadIds.size === 0 &&
              !detached.value.supportsMultipleProviderThreads
            ) {
              yield* releaseEntry({
                providerSessionId: input.providerSessionId,
                reason: "manual_shutdown",
                ...(input.detail === undefined ? {} : { detail: input.detail }),
              });
              return;
            }
            // The shared runtime stays up for other threads, so unload this
            // thread's native state rather than leaving it (and its MCP
            // servers) resident until the whole runtime is released.
            const unloadThread = detached.value.exposedRuntime.unloadThread;
            if (detached.value.supportsMultipleProviderThreads && unloadThread !== undefined) {
              // Serialized with re-attachment: a thread whose next turn
              // attaches first stays loaded, and one that attaches during the
              // unload waits for it, so its resume reloads the native thread.
              yield* threadAttachment.withLock(
                threadAttachmentKey(input),
                Effect.gen(function* () {
                  const entry = (yield* Ref.get(sessions)).get(key);
                  if (
                    entry?.runtime !== detached.value.runtime ||
                    entry.attachedThreadIds.has(input.threadId)
                  ) {
                    return;
                  }
                  yield* Effect.forEach(
                    detachedProviderThreads.filter((thread) => thread.nativeThreadRef !== null),
                    (providerThread) =>
                      unloadThread({ providerThread }).pipe(
                        // Bounded so a wedged provider cannot hold up the
                        // thread's next attach.
                        Effect.timeout(UNLOAD_THREAD_TIMEOUT_MS),
                        Effect.catchCause((cause) =>
                          Effect.logWarning(
                            "orchestration-v2.driver-session.detach-unload-failed",
                            {
                              providerSessionId: input.providerSessionId,
                              threadId: input.threadId,
                              providerThreadId: providerThread.id,
                              cause,
                            },
                          ),
                        ),
                      ),
                    { concurrency: 1, discard: true },
                  );
                }),
              );
            }
            yield* scheduleIdleRelease(input.providerSessionId);
          }).pipe(
            Effect.catchCause((cause) =>
              Effect.fail(
                new ProviderSessionReleaseError({
                  providerSessionId: input.providerSessionId,
                  reason: "manual_shutdown",
                  cause,
                }),
              ),
            ),
          ),
      } satisfies ProviderSessionManagerV2Shape);
    }),
  );

export const layer = layerWithOptions();
