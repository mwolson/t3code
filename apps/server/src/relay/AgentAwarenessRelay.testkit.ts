import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import {
  EnvironmentId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ThreadShell,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import { RelayAgentActivityState } from "@t3tools/contracts/relay";
import { PersonalPublishClaims } from "@t3tools/shared/personalRelay";
import * as PublishSequence from "./PublishSequence.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as BackgroundRelaySettings from "./BackgroundRelaySettings.ts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as FetchHttpClient from "effect/http/FetchHttpClient";

import * as EnvironmentKeys from "../cloud/environmentKeys.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import {
  PUBLISH_AGENT_ACTIVITY_SECRET,
  RELAY_ENVIRONMENT_CREDENTIAL_SECRET,
  RELAY_ISSUER_SECRET,
  RELAY_URL_SECRET,
} from "../cloud/config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as AgentAwarenessRelay from "./AgentAwarenessRelay.ts";

export const THREAD_ID = ThreadId.make("relay-thread");
export const SECOND_THREAD_ID = ThreadId.make("relay-thread-2");
export const PROJECT_ID = ProjectId.make("relay-project");
export const NOW = "2026-09-04T12:00:00.000Z";

export function shell(
  overrides: Partial<OrchestrationV2ThreadShell> = {},
): OrchestrationV2ThreadShell {
  return {
    id: THREAD_ID,
    projectId: PROJECT_ID,
    title: "Thread",
    providerInstanceId: ProviderInstanceId.make("codex"),
    modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "test-model" },
    runtimeMode: "full-access",
    interactionMode: "default",
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { rootThreadId: THREAD_ID, parentThreadId: null, relationshipToParent: null },
    forkedFrom: null,
    createdBy: "user",
    creationSource: "web",
    activeRunId: null,
    latestVisibleMessage: null,
    hasActionableProposedPlan: false,
    itemCount: 0,
    visibleItemCount: 0,
    lastVisitedAt: null,
    deletedAt: null,
    branch: null,
    linkedPullRequest: null,
    status: "running",
    activityRunStatus: null,
    pendingRuntimeRequest: null,
    pendingBackgroundTasks: [],
    latestRunId: null,
    latestRunRequestedAt: null,
    latestRunStartedAt: null,
    latestRunCompletedAt: null,
    latestUserMessageAt: null,
    createdAt: DateTime.makeUnsafe(NOW),
    updatedAt: DateTime.makeUnsafe(NOW),
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    snoozedUntil: null,
    snoozedAt: null,
    pinnedAt: null,
    ...overrides,
  };
}

const PublishPayload = Schema.Struct({
  state: Schema.NullOr(RelayAgentActivityState),
  proof: Schema.String,
});
const decodePublishPayload = Schema.decodeUnknownSync(Schema.fromJsonString(PublishPayload));
export const unused = () => Effect.die("Unexpected test dependency call");

const decodeClaims = Schema.decodeUnknownSync(PersonalPublishClaims);
export const makeTestRelay = Effect.fnUntraced(function* (
  options: {
    readonly eventStore?: EventStore.EventStoreV2["Service"];
    readonly secretStore?: ServerSecretStore.ServerSecretStore["Service"];
    readonly respond?: (
      attempt: number,
      claims: PersonalPublishClaims,
    ) => Response | Promise<Response>;
    readonly snapshotThreads?: () => ReadonlyArray<OrchestrationV2ThreadShell>;
    readonly beforeSet?: (name: string) => Effect.Effect<void>;
    readonly afterSet?: (name: string) => Effect.Effect<void>;
    readonly failSecretRead?: (name: string) => boolean;
    readonly linked?: boolean;
    readonly readShell?: (threadId: ThreadId) => Effect.Effect<OrchestrationV2ThreadShell | null>;
    readonly domainEvents?: Stream.Stream<OrchestrationV2DomainEvent>;
    readonly catchUpThreads?: ReadonlyArray<OrchestrationV2ThreadShell>;
  } = {},
) {
  const values = new Map<string, Uint8Array>(
    options.linked === false
      ? []
      : [
          [PUBLISH_AGENT_ACTIVITY_SECRET, new TextEncoder().encode("true")],
          [RELAY_URL_SECRET, new TextEncoder().encode("https://relay.example.test")],
          [RELAY_ISSUER_SECRET, new TextEncoder().encode("https://relay.example.test")],
          [RELAY_ENVIRONMENT_CREDENTIAL_SECRET, new TextEncoder().encode("credential-1")],
        ],
  );
  const secretReads: string[] = [];
  const secrets =
    options.secretStore ??
    ServerSecretStore.ServerSecretStore.of({
      get: (name) =>
        Effect.suspend(() => {
          secretReads.push(name);
          if (options.failSecretRead?.(name)) {
            return Effect.fail(
              new ServerSecretStore.SecretStoreReadError({
                resource: name,
                cause: "temporary read failure",
              }),
            );
          }
          return Effect.succeed(Option.fromUndefinedOr(values.get(name)));
        }),
      create: (name, value) => Effect.sync(() => void values.set(name, value)),
      set: (name, value) =>
        (options.beforeSet?.(name) ?? Effect.void).pipe(
          Effect.andThen(Effect.sync(() => void values.set(name, value))),
          Effect.andThen(options.afterSet?.(name) ?? Effect.void),
        ),
      remove: (name) => Effect.sync(() => void values.delete(name)),
      getOrCreateRandom: unused,
    });
  const currentShell = yield* Ref.make<OrchestrationV2ThreadShell | null>(shell());
  const shellReads: ThreadId[] = [];
  const catchUp = { activitySnapshotReads: 0 };
  const threads = ThreadManagementService.ThreadManagementService.of({
    getThreadShell: unused,
    getShellSnapshot: unused,
    getAgentAwarenessSnapshot: (threadId) =>
      threadId === undefined
        ? Effect.sync(() => {
            catchUp.activitySnapshotReads += 1;
            return {
              snapshotSequence: 1,
              threads: options.snapshotThreads?.() ?? options.catchUpThreads ?? [],
            };
          })
        : Effect.sync(() => shellReads.push(threadId)).pipe(
            Effect.andThen(options.readShell?.(threadId) ?? Ref.get(currentShell)),
            Effect.map((thread) => ({
              snapshotSequence: 1,
              threads: thread === null ? [] : [thread],
            })),
          ),
    listActiveSubagentThreadIds: unused,
    listAppOwnedChildThreadIds: unused,
    countThreads: unused,
    listActiveShellIndex: () => Effect.succeed([]),
    listOlderSettledThreads: unused,
    readShellSnapshot: unused,
    getThreadTitles: unused,
    getThreadLinkMetadata: unused,
    ensureLegacyTranscript: unused,
    dispatch: unused,
    searchThread: () => Effect.die("unused"),
    searchThreadStream: () => Stream.empty,
    getThreadHistoryPage: () => Effect.die("unused"),
    getTimelinePage: () => Effect.die("Unused timeline read"),
    getMessageCount: () => Effect.die("unused message count"),
    getTurnItem: () => Effect.die("unused turn item read"),
    getThreadRecords: () => Effect.die("unused record read"),
    getThreadProjection: unused,
    getCheckpointContext: unused,
    getThreadSnapshot: unused,
    getThreadTranscript: unused,
    getThreadSnapshotWindow: unused,
    getProjectThreadRecords: () => Effect.die("unused project record read"),
    listProjectThreads: unused,
    sendToThread: unused,
    assertProviderInstanceEnabled: unused,
    isProviderInstanceEnabledForProject: unused,
    hasReceipt: unused,
    waitForThread: unused,
    settleAfterRun: unused,
    settleThread: unused,
    interruptThread: unused,
    stopWorkflow: () => Effect.die("unused stopWorkflow"),
    forceStopThread: () => Effect.die("unused forceStopThread"),
    awaitBackgroundTasks: unused,
    stopDelegatedTasks: unused,
    getThreadEventSequence: unused,
    recoverDelegatedTask: unused,
    delegatedTaskResultPending: unused,
    streamStoredEvents: Stream.empty,
    streamStoredEventsFrom: () => Stream.empty,
    streamDomainEvents: options.domainEvents ?? Stream.empty,
  });
  const publications: Array<{
    readonly url: string;
    readonly authorization: string | null;
    readonly proof: string;
    readonly claims: PersonalPublishClaims;
    readonly issuer: unknown;
    readonly state: RelayAgentActivityState | null;
  }> = [];
  const fetch: typeof globalThis.fetch = Object.assign(
    (
      input: Parameters<typeof globalThis.fetch>[0],
      init?: Parameters<typeof globalThis.fetch>[1],
    ) => {
      const body = init?.body;
      if (typeof body !== "string" && !(body instanceof Uint8Array)) {
        return Promise.reject(new Error("Expected a serialized activity publish payload"));
      }
      const payload = decodePublishPayload(
        typeof body === "string" ? body : new TextDecoder().decode(body),
      );
      const jwtClaims = JSON.parse(
        Buffer.from(payload.proof.split(".")[1] ?? "", "base64url").toString("utf8"),
      );
      const claims = decodeClaims(jwtClaims);
      publications.push({
        url: String(input),
        authorization: new Headers(init?.headers).get("authorization"),
        state: payload.state,
        proof: payload.proof,
        claims,
        issuer: jwtClaims.aud,
      });
      return Promise.resolve(
        options.respond?.(publications.length, claims) ??
          Response.json({ ok: true, deliveries: [] }),
      );
    },
    { preconnect: () => {} },
  );
  const project = {
    id: PROJECT_ID,
    title: "Project",
    workspaceRoot: "/workspace",
    defaultModelSelection: null,
    scripts: [],
    createdAt: NOW,
    updatedAt: NOW,
    deletedAt: null,
  };
  const relay = yield* AgentAwarenessRelay.make.pipe(
    Effect.provide(EnvironmentKeys.layer),
    Effect.provideServiceEffect(
      BackgroundRelaySettings.BackgroundRelaySettings,
      BackgroundRelaySettings.make,
    ),
    Effect.provide(PublishSequence.layer),
    Effect.provideService(
      EventStore.EventStoreV2,
      options.eventStore ??
        EventStore.EventStoreV2.of({
          append: unused,
          appendProjectEvent: unused,
          read: () => Stream.empty,
          readByCommandId: () => Stream.empty,
          latestSequence: () => Effect.succeed(0),
          latestApplicationSequence: unused(),
          publishCommitted: unused,
        }),
    ),
    Effect.provideService(ServerSecretStore.ServerSecretStore, secrets),
    Effect.provideService(ThreadManagementService.ThreadManagementService, threads),
    Effect.provideService(ServerEnvironment.ServerEnvironment, {
      getEnvironmentId: Effect.succeed(EnvironmentId.make("relay-environment")),
      getDescriptor: unused(),
    }),
    Effect.provideService(ProjectService.ProjectService, {
      create: unused,
      bootstrap: unused,
      update: unused,
      delete: unused,
      getByWorkspaceRoot: unused,
      snapshot: Effect.succeed({ projects: [project], updatedAt: NOW }),
      listPage: unused,
      getShell: unused,
      listShells: unused,
      getById: () => Effect.succeedSome(project),
    }),
    Effect.provideService(FetchHttpClient.Fetch, fetch),
    Effect.provide(NodeCrypto.layer),
  );
  return { relay, secrets, secretReads, currentShell, shellReads, publications, catchUp };
});
