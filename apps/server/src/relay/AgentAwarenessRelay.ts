import type { AgentAwarenessThread } from "../orchestration-v2/ProjectionStore.ts";
import {
  isListedThreadLineage,
  type EnvironmentId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2TurnItem,
  type Project,
  type RuntimeRequestId,
  type ThreadId,
} from "@t3tools/contracts";
import {
  RelayApi,
  type RelayAgentActivityPublishProofPayload,
  type RelayAgentActivityState,
} from "@t3tools/contracts/relay";
import {
  type SubagentRequestAwareness,
  projectThreadAwarenessV2,
  subagentAwaitsApproval,
} from "@t3tools/shared/agentAwareness";
import {
  agentAwarenessTransition,
  personalPublishClaims,
  type PersonalPublishClaims,
} from "@t3tools/shared/personalRelay";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as CompletionEligibility from "./CompletionEligibility.ts";
import * as PublishSequence from "./PublishSequence.ts";
import type { AgentAwarenessState } from "@t3tools/shared/agentAwareness";
import { turnItemUpdateCanEndBackgroundWork } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import { withRelayClientTracing } from "@t3tools/shared/relayTracing";
import { normalizeRelayIssuer, RELAY_ACTIVITY_PUBLISH_TYP } from "@t3tools/shared/relayJwt";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Digest from "effect/crypto/Digest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FiberHandle from "effect/FiberHandle";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Redacted from "effect/Redacted";
import * as Ref from "effect/Ref";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpApiClient from "effect/http-api/HttpApiClient";

import * as Hex from "effect/encoding/Hex";
import * as Schema from "effect/Schema";
import * as BackgroundRelaySettings from "./BackgroundRelaySettings.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import {
  isAgentActivityPublishingEnabledValue,
  PUBLISH_AGENT_ACTIVITY_SECRET,
  RELAY_ENVIRONMENT_CREDENTIAL_SECRET,
  RELAY_ISSUER_SECRET,
  RELAY_URL_SECRET,
} from "../cloud/config.ts";
import { EnvironmentSigningKey } from "../cloud/environmentKeys.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import { forkParked } from "../serverActivation.ts";

export class AgentAwarenessRelay extends Context.Service<
  AgentAwarenessRelay,
  {
    readonly publishThread: (threadId: ThreadId) => Effect.Effect<void>;
    readonly drain: Effect.Effect<void>;
    readonly requestCatchUp: () => Effect.Effect<void>;
    readonly start: () => Effect.Effect<void, never, Scope.Scope>;
  }
>()("t3/relay/AgentAwarenessRelay") {}

function eventThreadId(event: OrchestrationV2DomainEvent): ThreadId {
  return event.threadId;
}

function isTurnItemPayload(
  payload: unknown,
): payload is Pick<OrchestrationV2TurnItem, "type" | "status"> {
  return (
    typeof payload === "object" && payload !== null && "type" in payload && "status" in payload
  );
}

function shouldPublishAgentAwarenessEvent(
  event: Pick<OrchestrationV2DomainEvent, "type"> & { readonly payload?: unknown },
): boolean {
  if (
    event.type === "thread.created" &&
    typeof event.payload === "object" &&
    event.payload !== null &&
    "historyOrigin" in event.payload &&
    event.payload.historyOrigin === "v1_import"
  ) {
    return false;
  }
  switch (event.type) {
    case "thread.created":
    case "thread.archived":
    case "thread.unarchived":
    case "thread.deleted":
    case "thread.metadata-updated":
    case "thread.pull-request-link-updated":
    case "thread.pull-request-synced":
    case "thread.settled":
    case "thread.model-selection-updated":
    case "thread.provider-switched":
    case "run.created":
    case "run.updated":
    case "runtime-request.updated":
    case "subagent.updated":
    case "provider-thread.updated":
      return true;
    // Unsettling must not re-alert a dismissed result.
    case "thread.unsettled":
    case "thread.guest-share-updated":
    case "thread.snoozed":
    case "thread.unsnoozed":
    case "thread.auto-settle-set":
    case "thread.pinned":
    case "thread.unpinned":
    case "thread.pin-reordered":
    case "thread.active-reordered":
    case "thread.tags-updated":
    case "thread.visited":
    case "thread.marked-unread":
    case "thread.runtime-mode-updated":
    case "thread.interaction-mode-updated":
    case "run.background-work-cancelled":
    case "run-attempt.created":
    case "run-attempt.updated":
    case "node.updated":
    case "provider-session.attached":
    case "provider-session.updated":
    case "provider-session.detached":
    case "provider-turn.updated":
    case "turn-item.updated":
      return isTurnItemPayload(event.payload) && turnItemUpdateCanEndBackgroundWork(event.payload);
    case "message.updated":
    case "plan.updated":
    case "checkpoint-scope.created":
    case "checkpoint.captured":
    case "checkpoint.rollback-requested":
    case "context-handoff.updated":
    case "context-transfer.created":
    case "context-transfer.updated":
      return false;
  }
}

export const makeAgentAwarenessPublishWorker = Effect.fnUntraced(function* <R>(
  publish: (threadId: ThreadId) => Effect.Effect<void, never, R>,
) {
  const queued = new Set<ThreadId>();
  const worker = yield* makeDrainableWorker((threadId: ThreadId) =>
    Effect.sync(() => queued.delete(threadId)).pipe(Effect.andThen(publish(threadId))),
  );
  const enqueue = (threadId: ThreadId) =>
    Effect.suspend(() => {
      if (queued.has(threadId)) return Effect.void;
      // Remove queued IDs when processing starts so publication can queue one later update.
      queued.add(threadId);
      return worker.enqueue(threadId);
    }).pipe(Effect.uninterruptible);
  return { enqueue, drain: worker.drain };
});

function agentAwarenessPublishIdentity(
  state: RelayAgentActivityState | null,
  subagentRequest: SubagentRequestAwareness | null = null,
  transitionId: string | null = null,
): string {
  if (state === null) {
    return "null";
  }
  const { updatedAt: _updatedAt, ...meaningfulState } = state;
  return JSON.stringify({
    state: meaningfulState,
    transitionId,
    childRequestId: subagentRequest?.requestId ?? null,
  });
}

const CLEARED_PUBLISH_IDENTITY = agentAwarenessPublishIdentity(null);

// A published tombstone stays recorded so repeats are skipped, but it is not live state.
function hasLivePublishedState(
  publishedStateByThread: ReadonlyMap<
    ThreadId,
    { readonly revision: string; readonly identity: string }
  >,
  threadId: ThreadId,
): boolean {
  const identity = publishedStateByThread.get(threadId)?.identity;
  return identity !== undefined && identity !== CLEARED_PUBLISH_IDENTITY;
}

function resolveAgentActivityPublishingStartupState(input: {
  readonly relayConfigured: boolean;
  readonly publishEnabled: boolean;
}): "waiting-for-link" | "disabled" | "enabled" {
  if (!input.relayConfigured) {
    return "waiting-for-link";
  }
  return input.publishEnabled ? "enabled" : "disabled";
}

const RELAY_AGENT_ACTIVITY_DETAIL_MAX_LENGTH = 160;
const REDACTED_RELAY_AGENT_FAILURE_DETAIL = "The agent run failed.";
const RELAY_AGENT_ACTIVITY_RETRY_DELAYS_MS = [1_000, 2_000, 4_000, 8_000, 16_000] as const;
// The relay sends a card only on change and phones mark it out of date ten minutes after the
// last push, so quiet live work is reported again well inside that window.
const RELAY_AGENT_ACTIVITY_HEARTBEAT_MS = 3 * 60_000;

// Waits and results alert, and a repeat could ring again; only silent live phases repeat.
function phaseHeartbeats(phase: RelayAgentActivityState["phase"]): boolean {
  return phase === "starting" || phase === "running";
}

const encodeRelayConfiguration = Schema.encodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      url: Schema.String,
      issuer: Schema.String,
      environmentCredential: Schema.String,
    }),
  ),
);

function sanitizeRelayAgentActivityState(
  state: RelayAgentActivityState | null,
): AgentAwarenessState | null {
  if (state === null) {
    return null;
  }
  const { detail: _detail, ...rest } = state;
  const detail = (state.phase === "failed" ? REDACTED_RELAY_AGENT_FAILURE_DETAIL : state.detail)
    ?.trim()
    .slice(0, RELAY_AGENT_ACTIVITY_DETAIL_MAX_LENGTH)
    .trim();
  return detail ? { ...rest, detail } : rest;
}

function relayEnvironmentClient(token: string) {
  return HttpClient.mapRequest(HttpClientRequest.setHeader("authorization", `Bearer ${token}`));
}

function deliveryStats(
  deliveries: ReadonlyArray<{
    readonly ok: boolean;
    readonly queued?: boolean | undefined;
    readonly kind: string;
    readonly apnsStatus?: number | null;
    readonly apnsReason?: string | null;
  }>,
) {
  let queued = 0;
  let successful = 0;
  let failed = 0;
  const failedReasons: string[] = [];
  const kinds = new Set<string>();

  for (const delivery of deliveries) {
    kinds.add(delivery.kind);
    if (delivery.queued) {
      queued += 1;
      continue;
    }
    if (delivery.ok) {
      successful += 1;
      continue;
    }
    failed += 1;
    failedReasons.push(`${delivery.apnsStatus ?? "transport"}:${delivery.apnsReason ?? "unknown"}`);
  }

  return {
    total: deliveries.length,
    queued,
    successful,
    failed,
    kinds: [...kinds],
    failedReasons,
  };
}

const makePublishProof = Effect.fn("makePublishProof")(function* (input: {
  readonly relayIssuer: string;
  readonly environmentId: string;
  readonly threadId: ThreadId;
  readonly state: RelayAgentActivityState | null;
  readonly jti: string;
  readonly claims: PersonalPublishClaims;
}) {
  const now = yield* DateTime.now;
  const expiresAt = DateTime.add(now, { minutes: 5 });
  const payload = {
    ...input.claims,
    iss: `t3-env:${input.environmentId}`,
    aud: normalizeRelayIssuer(input.relayIssuer),
    sub: input.environmentId,
    jti: input.jti,
    iat: Math.floor(now.epochMilliseconds / 1_000),
    exp: Math.floor(expiresAt.epochMilliseconds / 1_000),
    environmentId: input.environmentId as RelayAgentActivityPublishProofPayload["environmentId"],
    threadId: input.threadId,
    state: input.state,
  } satisfies RelayAgentActivityPublishProofPayload & PersonalPublishClaims;
  const signingKey = yield* EnvironmentSigningKey;
  return yield* signingKey.sign({ typ: RELAY_ACTIVITY_PUBLISH_TYP, payload });
});

function describeThreadShellForAwareness(
  thread: Option.Option<AgentAwarenessThread>,
): Record<string, unknown> {
  if (Option.isNone(thread)) {
    return { found: false };
  }
  const shell = thread.value;
  return {
    found: true,
    status: shell.status,
    activityRunStatus: shell.activityRunStatus ?? null,
    activeRunId: shell.activeRunId ?? null,
    latestRunId: shell.latestRunId ?? null,
    pendingRuntimeRequestKind: shell.pendingRuntimeRequest?.kind ?? null,
    hasActionableProposedPlan: shell.hasActionableProposedPlan,
  };
}

interface SubagentRequest {
  readonly topLevelThreadId: ThreadId;
  readonly requestId: RuntimeRequestId;
}

interface SubagentAlert {
  readonly requestId: RuntimeRequestId;
  readonly updatedAt: string;
}

const waitingApprovalId = (thread: AgentAwarenessThread | null) =>
  thread !== null && thread.archivedAt === null && subagentAwaitsApproval(thread)
    ? thread.pendingRuntimeRequest?.id
    : undefined;

function resolveAgentAwarenessRelayPublishSnapshot(input: {
  readonly environmentId: EnvironmentId;
  readonly thread: Option.Option<AgentAwarenessThread>;
  readonly project: Option.Option<Project>;
  readonly pendingSubagentRequest: SubagentRequestAwareness | null;
}): {
  readonly projectId: string | null;
  readonly state: AgentAwarenessState | null;
  /** The child request behind the state's wait, when the parent isn't waiting itself. */
  readonly subagentWait: SubagentRequestAwareness | null;
  readonly reason: "snapshot" | "thread-not-found" | "project-not-found";
} {
  if (Option.isNone(input.thread)) {
    return {
      projectId: null,
      state: null,
      subagentWait: null,
      reason: "thread-not-found",
    };
  }
  if (Option.isNone(input.project)) {
    return {
      projectId: input.thread.value.projectId,
      state: null,
      subagentWait: null,
      reason: "project-not-found",
    };
  }
  const awarenessInput = {
    environmentId: input.environmentId,
    project: input.project.value,
    thread: input.thread.value,
  };
  const state = projectThreadAwarenessV2({
    ...awarenessInput,
    pendingSubagentRequest: input.pendingSubagentRequest,
  });
  const childDrivesWait =
    input.pendingSubagentRequest !== null &&
    state?.phase !== projectThreadAwarenessV2(awarenessInput)?.phase;
  return {
    projectId: input.thread.value.projectId,
    state: sanitizeRelayAgentActivityState(state),
    subagentWait: childDrivesWait ? input.pendingSubagentRequest : null,
    reason: "snapshot",
  };
}

function terminalWorkSinceStart(thread: AgentAwarenessThread, startedAt: number): boolean {
  return (
    thread.latestRunCompletedAt != null &&
    (DateTime.toEpochMillis(thread.latestRunCompletedAt) > startedAt ||
      (thread.status === "completed" && DateTime.toEpochMillis(thread.updatedAt) > startedAt))
  );
}

export function resolveAgentAwarenessRelayActiveThreadIds(input: {
  readonly environmentId: EnvironmentId;
  readonly startedAt: number;
  readonly projects: ReadonlyArray<Pick<Project, "id" | "title">>;
  readonly threads: ReadonlyArray<AgentAwarenessThread>;
}): ReadonlyArray<ThreadId> {
  const projectById = new Map(input.projects.map((project) => [project.id, project]));
  return input.threads
    .filter((thread) => {
      // Publishing a waiting subagent republishes its top-level thread.
      if (subagentAwaitsApproval(thread)) return true;
      const project = projectById.get(thread.projectId);
      if (!project) {
        return false;
      }
      const state = projectThreadAwarenessV2({
        environmentId: input.environmentId,
        project,
        thread,
      });
      return (
        state !== null &&
        (state.phase !== "completed" && state.phase !== "failed"
          ? true
          : terminalWorkSinceStart(thread, input.startedAt))
      );
    })
    .map((thread) => thread.id);
}

/** @public */
export const make = Effect.gen(function* () {
  const backgroundRelay = yield* BackgroundRelaySettings.BackgroundRelaySettings;
  const sequence = yield* PublishSequence.PublishSequence;
  const completionEligibility = yield* CompletionEligibility.make;
  const events = yield* EventStore.EventStoreV2;
  const secrets = yield* ServerSecretStore.ServerSecretStore;
  const serverEnvironment = yield* ServerEnvironment.ServerEnvironment;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const projects = yield* ProjectService.ProjectService;
  const random = yield* SecureRandom.SecureRandom;
  const digest = yield* Digest.Digest;
  const signingKey = yield* EnvironmentSigningKey;
  const scope = yield* Effect.scope;
  const startedAt = (yield* DateTime.now).epochMilliseconds;
  const resync = yield* FiberHandle.make<void>();
  const resyncRequests = yield* Queue.dropping<void>(1);
  const pendingResync = new Set<ThreadId>();
  const exhaustedThreads = new Set<ThreadId>();
  const outcomes = new Map<
    ThreadId,
    { readonly revision: string; readonly outcome: "acknowledged" | "deferred" | "failed" }
  >();
  const catchUpRequests = yield* Queue.dropping<void>(1);
  const publishedStateByThreadRef = yield* Ref.make(
    new Map<ThreadId, { readonly revision: string; readonly identity: string }>(),
  );
  // Android keys alerts by phase and time, so a child's unchanged request keeps
  // its first alert time through parent renames, relinks and the parent's own requests.
  const childAlertByThread = new Map<ThreadId, SubagentAlert>();
  const subagentRequests = new Map<ThreadId, SubagentRequest>();

  const readSecretString = (name: string) =>
    secrets
      .get(name)
      .pipe(
        Effect.map((bytes) =>
          Option.isSome(bytes) ? new TextDecoder().decode(bytes.value) : null,
        ),
      );

  const readRelayConfiguration = Effect.gen(function* () {
    const background = yield* backgroundRelay.read;
    if (background !== undefined) {
      return {
        backgroundConfigured: true,
        config:
          background === null
            ? null
            : {
                ...background,
                environmentCredential: Redacted.value(background.environmentCredential),
              },
      };
    }
    const [url, issuer, environmentCredential] = yield* Effect.all([
      readSecretString(RELAY_URL_SECRET),
      readSecretString(RELAY_ISSUER_SECRET),
      readSecretString(RELAY_ENVIRONMENT_CREDENTIAL_SECRET),
    ]);
    if (!url || !environmentCredential) return { backgroundConfigured: false, config: null };
    const config = { url, issuer: issuer ?? url, environmentCredential };
    const encoded = yield* encodeRelayConfiguration(config);
    const revision = yield* digest
      .digest(Digest.sha256, new TextEncoder().encode(encoded))
      .pipe(Effect.map(Hex.encode));
    return { backgroundConfigured: false, config: { ...config, revision } };
  });
  const readRelayConfig = readRelayConfiguration.pipe(Effect.map((state) => state.config));

  const readPublishAgentActivityEnabled = readSecretString(PUBLISH_AGENT_ACTIVITY_SECRET).pipe(
    Effect.map(isAgentActivityPublishingEnabledValue),
  );

  const makeRelayClient = (relayConfig: {
    readonly url: string;
    readonly environmentCredential: string;
  }) =>
    HttpApiClient.make(RelayApi, {
      baseUrl: relayConfig.url,
      transformClient: relayEnvironmentClient(relayConfig.environmentCredential),
    }).pipe(Effect.provide(FetchHttpClient.layer));

  let cachedRelayClient: Effect.Success<ReturnType<typeof makeRelayClient>> | undefined;
  let publishedRelayConfig: NonNullable<Effect.Success<typeof readRelayConfig>> | undefined;

  // Confirm through the same worker so tombstones cannot race live updates; recovery clears the deadline.
  const publishConfirmDeadlines = new Map<ThreadId, number>();
  // Threads whose confirmation holds a completion, so a thread that settles during the hold still alerts.
  const heldCompletions = new Set<ThreadId>();
  let schedulePublishConfirm: (threadId: ThreadId) => Effect.Effect<void> = () => Effect.void;
  const heartbeatDueAt = new Map<ThreadId, number>();
  let scheduleHeartbeat: (threadId: ThreadId) => Effect.Effect<void> = () => Effect.void;
  const publishRetries = new Map<
    ThreadId,
    { readonly attempts: number; timer: Fiber.Fiber<void> | undefined }
  >();
  const cancelPublishRetry = Effect.fnUntraced(function* (threadId: ThreadId) {
    const retry = publishRetries.get(threadId);
    publishRetries.delete(threadId);
    if (retry?.timer !== undefined) yield* Fiber.interrupt(retry.timer);
  });
  const cancelPublishRetries = Effect.suspend(() =>
    Effect.forEach([...publishRetries.keys()], cancelPublishRetry, { discard: true }),
  );
  let schedulePublishRetry: (threadId: ThreadId) => Effect.Effect<void> = () => Effect.void;
  let enqueueTopLevelPublish: (threadId: ThreadId) => Effect.Effect<void> = () => Effect.void;
  const resetPublishedConnection = Effect.gen(function* () {
    publishedRelayConfig = undefined;
    cachedRelayClient = undefined;
    publishConfirmDeadlines.clear();
    heldCompletions.clear();
    heartbeatDueAt.clear();
    yield* Ref.set(publishedStateByThreadRef, new Map());
  });

  const publishThreadUnsafe = Effect.fn("publishThreadUnsafe")(function* (threadId: ThreadId) {
    const publishAgentActivity = yield* readPublishAgentActivityEnabled;
    if (!publishAgentActivity) {
      yield* cancelPublishRetries;
      yield* resetPublishedConnection;
      yield* Effect.logDebug("agent activity publish skipped; publication disabled", {
        threadId,
      });
      return;
    }
    const relayConfig = yield* readRelayConfig;
    if (!relayConfig) {
      yield* cancelPublishRetries;
      yield* resetPublishedConnection;
      yield* Effect.logDebug("agent activity publish skipped; relay link credentials unavailable", {
        threadId,
      });
      return;
    }
    if (publishedRelayConfig?.revision !== relayConfig.revision) {
      yield* resetPublishedConnection;
      publishedRelayConfig = relayConfig;
    }
    const outcome = (result: "acknowledged" | "deferred" | "failed") => {
      outcomes.set(threadId, { revision: relayConfig.revision, outcome: result });
    };
    outcome("failed");
    const environmentId = yield* serverEnvironment.getEnvironmentId;

    const publishState = Effect.fn("relay.agent_activity.publish")(function* (input: {
      readonly projectId: string | null;
      readonly state: RelayAgentActivityState | null;
      readonly reason: string;
      readonly claims: PersonalPublishClaims;
    }) {
      if (cachedRelayClient === undefined) {
        cachedRelayClient = yield* makeRelayClient(relayConfig);
      }
      const relayClient = cachedRelayClient;
      const proof = yield* makePublishProof({
        relayIssuer: relayConfig.issuer,
        environmentId,
        threadId,
        state: input.state,
        claims: input.claims,
        jti: yield* random.uuidV4(),
      }).pipe(Effect.provideService(EnvironmentSigningKey, signingKey));

      yield* Effect.logInfo("publishing agent activity for thread", {
        environmentId,
        threadId,
        projectId: input.projectId,
        statePhase: input.state?.phase ?? null,
        hasState: input.state !== null,
        reason: input.reason,
      });

      const response = yield* relayClient.server
        .publishAgentActivity({
          params: {
            environmentId,
            threadId,
          },
          payload: {
            state: input.state,
            proof,
          },
        })
        .pipe(Effect.timeout("10 seconds"));

      yield* Effect.logInfo("agent activity publish completed", {
        environmentId,
        threadId,
        ok: response.ok,
        deliveries: deliveryStats(response.deliveries),
      });
    });

    // Read only this thread; full shell reads would make each activity event scale with all threads.
    const observedSequence = yield* events.latestSequence({ threadId });
    const threadShell = yield* readAwarenessThread(threadId);
    yield* trackSubagentRequest(threadId, threadShell);
    if (
      threadShell !== null &&
      !isListedThreadLineage(threadShell.lineage) &&
      !hasLivePublishedState(yield* Ref.get(publishedStateByThreadRef), threadId)
    ) {
      outcome("acknowledged");
      // Subagents and side chats have no relay rows to clear; per-event tombstones would needlessly redeliver user aggregates.
      return;
    }
    const thread =
      threadShell === null || threadShell.archivedAt !== null
        ? Option.none<AgentAwarenessThread>()
        : Option.some(threadShell);
    const project = Option.isSome(thread)
      ? yield* projects.getById(thread.value.projectId)
      : Option.none<Project>();
    const snapshot = resolveAgentAwarenessRelayPublishSnapshot({
      environmentId,
      thread,
      project,
      pendingSubagentRequest: yield* currentSubagentRequestFor(threadId),
    });
    const transition = Option.isSome(thread)
      ? agentAwarenessTransition({
          thread: thread.value,
          state: snapshot.state,
          pendingSubagentRequest: snapshot.subagentWait,
        })
      : null;
    const publishIdentity = agentAwarenessPublishIdentity(
      snapshot.state,
      snapshot.subagentWait,
      transition?.id ?? null,
    );
    const publishedStateByThread = yield* Ref.get(publishedStateByThreadRef);
    const published = publishedStateByThread.get(threadId);
    const heartbeatDue =
      (heartbeatDueAt.get(threadId) ?? Number.POSITIVE_INFINITY) <=
      (yield* DateTime.now).epochMilliseconds;
    if (
      published?.revision === relayConfig.revision &&
      published.identity === publishIdentity &&
      !heartbeatDue &&
      // A held completion settled after an earlier clear still has to alert.
      !(snapshot.state === null && heldCompletions.has(threadId))
    ) {
      outcome("acknowledged");
      // Clear moot deadlines so later transient nulls cannot skip their deferral window.
      publishConfirmDeadlines.delete(threadId);
      heldCompletions.delete(threadId);
      yield* Effect.logDebug("agent activity publish skipped; projected state unchanged", {
        environmentId,
        threadId,
        reason: snapshot.reason,
      });
      return;
    }

    const completionEligibleAt =
      snapshot.state?.phase === "completed" && Option.isSome(thread)
        ? yield* completionEligibility(thread.value, observedSequence)
        : null;
    if (
      (snapshot.state?.phase === "completed" || snapshot.state?.phase === "failed") &&
      !hasLivePublishedState(publishedStateByThread, threadId)
    ) {
      const terminalAt =
        completionEligibleAt ?? (Option.isSome(thread) ? thread.value.latestRunCompletedAt : null);
      if (terminalAt == null || DateTime.toEpochMillis(terminalAt) <= startedAt) {
        outcome("acknowledged");
        return;
      }
    }
    // Confirm tombstones over live state to avoid removing active cards.
    // Confirm completion: a run can end moments before its continuation starts, and the relay
    // alerts on every move into completed.
    const requiresConfirmation = snapshot.state === null || snapshot.state?.phase === "completed";
    if (requiresConfirmation) {
      if (snapshot.state?.phase === "completed") heldCompletions.add(threadId);
      const nowMs = (yield* DateTime.now).epochMilliseconds;
      const deadline = publishConfirmDeadlines.get(threadId);
      if (deadline === undefined) {
        publishConfirmDeadlines.set(threadId, nowMs + 5_000);
        yield* Effect.logInfo("agent activity publish deferred pending confirmation", {
          environmentId,
          threadId,
          reason: snapshot.reason,
          statePhase: snapshot.state?.phase ?? null,
          shell: describeThreadShellForAwareness(thread),
        });
        yield* schedulePublishConfirm(threadId);
        outcome("deferred");
        return;
      }
      if (nowMs < deadline) {
        outcome("deferred");
        return;
      }
      yield* Effect.logInfo("agent activity deferred publish confirmed", {
        environmentId,
        threadId,
        reason: snapshot.reason,
        statePhase: snapshot.state?.phase ?? null,
        shell: describeThreadShellForAwareness(thread),
      });
      // Settled during the hold, not resumed: alert the current completion, then clear the card
      // on a new window. Only settlement may hide it; queued or silent runs also project to null.
      const unsettledThread =
        snapshot.state === null &&
        heldCompletions.has(threadId) &&
        Option.isSome(thread) &&
        thread.value.settledOverride === "settled"
          ? { ...thread.value, settledOverride: null }
          : null;
      const unsettled =
        unsettledThread === null
          ? null
          : resolveAgentAwarenessRelayPublishSnapshot({
              environmentId,
              thread: Option.some(unsettledThread),
              project,
              pendingSubagentRequest: null,
            });
      if (unsettledThread !== null && unsettled?.state?.phase === "completed") {
        const completion = unsettled.state;
        const completionClaims = personalPublishClaims({
          sequence: yield* sequence.next,
          thread: unsettledThread,
          state: completion,
          completionEligibleAt: yield* completionEligibility(unsettledThread, observedSequence),
        });
        yield* publishState({
          projectId: unsettled.projectId,
          state: completion,
          reason: unsettled.reason,
          claims: completionClaims,
        }).pipe(withRelayClientTracing);
        heldCompletions.delete(threadId);
        heartbeatDueAt.delete(threadId);
        publishConfirmDeadlines.set(threadId, nowMs + 5_000);
        yield* Ref.update(publishedStateByThreadRef, (publishedStates) =>
          new Map(publishedStates).set(threadId, {
            revision: relayConfig.revision,
            identity: agentAwarenessPublishIdentity(
              completion,
              null,
              completionClaims.t3_transition?.id ?? null,
            ),
          }),
        );
        yield* schedulePublishConfirm(threadId);
        outcome("deferred");
        return;
      }
    } else {
      publishConfirmDeadlines.delete(threadId);
      heldCompletions.delete(threadId);
    }

    if (snapshot.reason === "thread-not-found") {
      yield* Effect.logDebug("publishing agent activity tombstone; thread not found", {
        environmentId,
        threadId,
      });
    } else if (snapshot.reason === "project-not-found") {
      yield* Effect.logDebug("publishing agent activity tombstone; project not found", {
        environmentId,
        threadId,
        projectId: snapshot.projectId,
      });
    }

    const childRequestId = snapshot.subagentWait?.requestId;
    const childAlert = childAlertByThread.get(threadId);
    const state =
      snapshot.state !== null &&
      childRequestId !== undefined &&
      childAlert?.requestId === childRequestId
        ? { ...snapshot.state, updatedAt: childAlert.updatedAt }
        : snapshot.state;
    const heartbeatAt = state !== null && phaseHeartbeats(state.phase) ? yield* DateTime.now : null;
    yield* publishState({
      projectId: snapshot.projectId,
      state:
        state === null || heartbeatAt === null
          ? state
          : { ...state, heartbeatAt: DateTime.formatIso(heartbeatAt) },
      reason: snapshot.reason,
      claims: Option.isSome(thread)
        ? personalPublishClaims({
            sequence: yield* sequence.next,
            thread: thread.value,
            state,
            pendingSubagentRequest: snapshot.subagentWait,
            completionEligibleAt,
          })
        : {
            t3_publish_seq: yield* sequence.next,
            t3_transition: null,
            t3_transition_eligible_at: null,
          },
    }).pipe(withRelayClientTracing);
    if (state !== null && childRequestId !== undefined) {
      childAlertByThread.set(threadId, { requestId: childRequestId, updatedAt: state.updatedAt });
    }
    if (heartbeatAt === null) {
      heartbeatDueAt.delete(threadId);
    } else {
      heartbeatDueAt.set(
        threadId,
        heartbeatAt.epochMilliseconds + RELAY_AGENT_ACTIVITY_HEARTBEAT_MS,
      );
      yield* scheduleHeartbeat(threadId);
    }
    outcome("acknowledged");
    publishConfirmDeadlines.delete(threadId);
    heldCompletions.delete(threadId);
    yield* Ref.update(publishedStateByThreadRef, (publishedStates) =>
      new Map(publishedStates).set(threadId, {
        revision: relayConfig.revision,
        identity: publishIdentity,
      }),
    );
  });

  const readAwarenessThread = (threadId: ThreadId) =>
    threads
      .getAgentAwarenessSnapshot(threadId)
      .pipe(Effect.map((snapshot) => snapshot.threads[0] ?? null));

  // The subagent helpers below are untraced: they run per event inside
  // publishThreadUnsafe, whose span already covers them.
  const findTopLevelThreadId = Effect.fnUntraced(function* (child: AgentAwarenessThread) {
    const seen = new Set<ThreadId>([child.id]);
    let current: AgentAwarenessThread | null = child;
    while (
      current?.lineage.relationshipToParent === "subagent" ||
      current?.lineage.sideChat === true
    ) {
      const parentId: ThreadId | null = current.lineage.parentThreadId;
      if (parentId === null || seen.has(parentId)) return null;
      seen.add(parentId);
      current = yield* readAwarenessThread(parentId);
    }
    return current?.id ?? null;
  });

  const trackSubagentRequest = Effect.fnUntraced(function* (
    threadId: ThreadId,
    threadShell: AgentAwarenessThread | null,
  ) {
    const requestId = waitingApprovalId(threadShell);
    const previous = subagentRequests.get(threadId);
    // An unchanged wait still republishes its parent: a relink catch-up reaches it only here.
    if (threadShell === null || requestId === undefined) {
      if (previous === undefined) return;
      subagentRequests.delete(threadId);
      yield* enqueueTopLevelPublish(previous.topLevelThreadId);
      return;
    }
    const topLevelThreadId =
      previous?.topLevelThreadId ?? (yield* findTopLevelThreadId(threadShell));
    if (topLevelThreadId === null) return;
    subagentRequests.set(threadId, { topLevelThreadId, requestId });
    yield* enqueueTopLevelPublish(topLevelThreadId);
  });

  // The map can miss a child's update while publishing is off or the relay is
  // unlinked, so each waiting child is re-read before its parent is published.
  const currentSubagentRequestFor = Effect.fnUntraced(function* (topLevelThreadId: ThreadId) {
    let pending: SubagentRequestAwareness | null = null;
    for (const [childId, entry] of subagentRequests) {
      if (entry.topLevelThreadId !== topLevelThreadId) continue;
      const child = yield* readAwarenessThread(childId);
      const requestId = waitingApprovalId(child);
      if (child === null || requestId === undefined) {
        subagentRequests.delete(childId);
        continue;
      }
      if (requestId !== entry.requestId) subagentRequests.set(childId, { ...entry, requestId });
      // The newest wait is the one to announce.
      if (pending === null || DateTime.isGreaterThan(child.updatedAt, pending.updatedAt)) {
        pending = { requestId, updatedAt: child.updatedAt };
      }
    }
    return pending;
  });

  const processThreadPublish = Effect.fnUntraced(function* (threadId: ThreadId) {
    const retry = publishRetries.get(threadId);
    if (retry?.timer !== undefined) {
      const timer = retry.timer;
      retry.timer = undefined;
      yield* Fiber.interrupt(timer);
    }
    outcomes.delete(threadId);
    yield* publishThreadUnsafe(threadId).pipe(
      Effect.tap(() => cancelPublishRetry(threadId)),
      Effect.catchCause((cause) => {
        if (Cause.hasInterruptsOnly(cause)) return Effect.void;
        return Effect.logWarning("agent activity publish failed", {
          threadId,
          cause: Cause.pretty(cause),
        }).pipe(Effect.andThen(schedulePublishRetry(threadId)));
      }),
      Effect.withSpan("AgentAwarenessRelay.publishThread"),
    );
  });

  const worker = yield* makeAgentAwarenessPublishWorker(processThreadPublish);
  const enqueueThreadPublish = (threadId: ThreadId) =>
    cancelPublishRetry(threadId).pipe(Effect.andThen(worker.enqueue(threadId)));
  enqueueTopLevelPublish = enqueueThreadPublish;
  const publishThread: AgentAwarenessRelay["Service"]["publishThread"] = (threadId) =>
    enqueueThreadPublish(threadId).pipe(Effect.andThen(worker.drain));

  schedulePublishRetry = Effect.fnUntraced(function* (threadId: ThreadId) {
    if (pendingResync.has(threadId)) return;
    const attempts = publishRetries.get(threadId)?.attempts ?? 0;
    const delayMs = RELAY_AGENT_ACTIVITY_RETRY_DELAYS_MS[attempts];
    if (delayMs === undefined) {
      // Retain exhausted budgets until activity or success resets them; old timers must not restart retries.
      yield* Effect.logWarning("agent activity publish retry budget exhausted", { threadId });
      exhaustedThreads.add(threadId);
      yield* Queue.offer(resyncRequests, undefined);
      return;
    }
    const retry = {
      attempts: attempts + 1,
      timer: undefined as Fiber.Fiber<void> | undefined,
    };
    publishRetries.set(threadId, retry);
    const timer = yield* Effect.sleep(delayMs).pipe(
      Effect.andThen(
        Effect.suspend(() => {
          if (publishRetries.get(threadId) !== retry) return Effect.void;
          retry.timer = undefined;
          return worker.enqueue(threadId);
        }),
      ),
      Effect.forkIn(scope),
    );
    if (publishRetries.get(threadId) !== retry) {
      yield* Fiber.interrupt(timer);
    } else {
      retry.timer = timer;
    }
  });

  const seedResync = Effect.gen(function* () {
    const environmentId = yield* serverEnvironment.getEnvironmentId;
    const projectSnapshot = yield* projects.snapshot;
    const activitySnapshot = yield* threads.getAgentAwarenessSnapshot();
    // Live children aren't listed, but a waiting one makes its top-level thread wait.
    const liveChildren = yield* Effect.forEach(
      (yield* threads.listActiveShellIndex()).filter(
        (thread) => thread.lineage.relationshipToParent === "subagent" && thread.mayBeLive,
      ),
      (thread) => readAwarenessThread(thread.id),
    );
    const activeThreadIds = resolveAgentAwarenessRelayActiveThreadIds({
      environmentId,
      startedAt,
      projects: projectSnapshot.projects,
      threads: [...activitySnapshot.threads, ...liveChildren.filter((thread) => thread !== null)],
    });
    for (const threadId of activeThreadIds) pendingResync.add(threadId);
  });

  const runResync = Effect.fn("AgentAwarenessRelay.runResync")(function* (revision: string) {
    let delayMs = 5_000;
    let snapshotPending = true;
    while (true) {
      if (snapshotPending) {
        snapshotPending = !(yield* seedResync.pipe(
          Effect.as(true),
          Effect.catch(() =>
            Effect.logWarning("Could not read the agent activity resync snapshot.").pipe(
              Effect.as(false),
            ),
          ),
        ));
        if (snapshotPending) {
          yield* Effect.sleep(delayMs);
          delayMs = Math.min(delayMs * 2, 300_000);
          continue;
        }
      }
      for (const threadId of exhaustedThreads) pendingResync.add(threadId);
      if (pendingResync.size === 0) {
        yield* Queue.take(resyncRequests);
        continue;
      }
      yield* Effect.forEach([...pendingResync], worker.enqueue, { discard: true });
      yield* worker.drain;
      for (const threadId of pendingResync) {
        const result = outcomes.get(threadId);
        if (result?.revision === revision && result.outcome === "acknowledged") {
          pendingResync.delete(threadId);
          exhaustedThreads.delete(threadId);
        }
      }
      if (pendingResync.size === 0) {
        yield* Effect.logDebug("agent activity resync acknowledged", { revision });
        delayMs = 5_000;
        continue;
      }
      yield* Effect.race(Effect.sleep(delayMs), Queue.take(resyncRequests));
      delayMs = Math.min(delayMs * 2, 300_000);
    }
  });

  const restartResync = Effect.fn("AgentAwarenessRelay.restartResync")(function* (
    revision: string | undefined,
    enabled: boolean,
  ) {
    yield* FiberHandle.clear(resync);
    pendingResync.clear();
    if (!revision || !enabled) return;
    yield* FiberHandle.run(resync, runResync(revision));
  });

  const watchConfiguration = Effect.gen(function* () {
    let pollDelayMs = 5_000;
    let previous: { revision: string | undefined; enabled: boolean } | undefined;
    while (true) {
      // A read error cannot establish a new destination or authorize fallback.
      yield* Effect.gen(function* () {
        const { config, backgroundConfigured } = yield* readRelayConfiguration;
        pollDelayMs =
          config !== null || backgroundConfigured ? 5_000 : Math.min(60_000, pollDelayMs * 2);
        const enabled = yield* readPublishAgentActivityEnabled;
        const revision = config?.revision;
        const changed = previous === undefined || previous.revision !== revision;
        const sharingChanged = previous !== undefined && previous.enabled !== enabled;
        if (changed || sharingChanged) {
          if (publishedRelayConfig?.revision !== revision || sharingChanged) {
            yield* cancelPublishRetries;
            yield* resetPublishedConnection;
            outcomes.clear();
          }
          yield* restartResync(revision, enabled);
          previous = { revision, enabled };
        }
      }).pipe(
        Effect.catch(() =>
          Effect.logWarning("Could not read agent activity publishing configuration."),
        ),
      );
      const requested = yield* Effect.race(
        Effect.sleep(pollDelayMs).pipe(Effect.as(false)),
        Queue.take(catchUpRequests).pipe(Effect.as(true)),
      );
      if (requested) previous = undefined;
    }
  });

  schedulePublishConfirm = (threadId) =>
    Effect.forkIn(
      Effect.sleep("5 seconds").pipe(
        Effect.andThen(worker.enqueue(threadId)),
        Effect.catchCause((cause) =>
          Effect.logWarning("deferred agent activity confirmation failed", {
            threadId,
            cause: Cause.pretty(cause),
          }),
        ),
      ),
      scope,
    ).pipe(Effect.asVoid);

  // An earlier timer for the same thread finds the heartbeat not yet due and publishes nothing.
  scheduleHeartbeat = (threadId) =>
    Effect.forkIn(
      Effect.sleep(RELAY_AGENT_ACTIVITY_HEARTBEAT_MS).pipe(
        Effect.andThen(worker.enqueue(threadId)),
        Effect.catchCause((cause) =>
          Effect.logWarning("agent activity heartbeat failed", {
            threadId,
            cause: Cause.pretty(cause),
          }),
        ),
      ),
      scope,
    ).pipe(Effect.asVoid);

  const start: AgentAwarenessRelay["Service"]["start"] = Effect.fn("AgentAwarenessRelay.start")(
    function* () {
      const [relayConfig, publishEnabled] = yield* Effect.all([
        readRelayConfig.pipe(Effect.orElseSucceed(() => null)),
        readPublishAgentActivityEnabled.pipe(Effect.orElseSucceed(() => false)),
      ]);
      const startupState = resolveAgentActivityPublishingStartupState({
        relayConfigured: relayConfig !== null,
        publishEnabled,
      });
      switch (startupState) {
        case "waiting-for-link":
          yield* Effect.logInfo(
            (yield* backgroundRelay.read.pipe(Effect.orElseSucceed(() => undefined))) === null
              ? "agent activity publishing disabled by invalid saved background relay configuration"
              : "agent activity publishing standby; waiting for T3 Connect link reconciliation",
          );
          break;
        case "disabled":
          yield* Effect.logInfo("agent activity publishing disabled by T3 Connect configuration");
          break;
        case "enabled":
          yield* Effect.logInfo("agent activity publishing enabled", {
            relayUrl: relayConfig?.url,
          });
          break;
      }
      yield* forkParked(Effect.sleep("1 second").pipe(Effect.andThen(watchConfiguration)));
      yield* forkParked(
        Stream.runForEach(threads.streamDomainEvents, (event) => {
          const threadId = eventThreadId(event);
          if (!shouldPublishAgentAwarenessEvent(event)) {
            return Effect.void;
          }
          return Effect.logDebug("agent activity publishing queued thread publish", {
            eventType: event.type,
            threadId,
          }).pipe(Effect.andThen(enqueueThreadPublish(threadId)));
        }),
      );
    },
  );

  return AgentAwarenessRelay.of({
    publishThread,
    drain: worker.drain,
    requestCatchUp: Effect.fn("AgentAwarenessRelay.requestCatchUp")(function () {
      return Queue.offer(catchUpRequests, undefined).pipe(Effect.asVoid);
    }),
    start,
  });
});

export const layer = Layer.effect(AgentAwarenessRelay, make).pipe(
  Layer.provide(
    Layer.mergeAll(
      BackgroundRelaySettings.layer,
      EventStore.layerFromOrchestrationEventStore,
      PublishSequence.layer,
    ),
  ),
);
