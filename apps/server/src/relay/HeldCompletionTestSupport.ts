import { assert, vi } from "@effect/vitest";
import {
  EventId,
  MessageId,
  NodeId,
  ProviderInstanceId,
  ProviderDriverKind,
  ProviderThreadId,
  RunId,
  TurnItemId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2TurnItem,
  isOrchestrationV2WorkActive,
} from "@t3tools/contracts";
import type { RelayAgentActivityState } from "@t3tools/contracts/relay";
import { backgroundWorkHoldsCompletion } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as CompletionEligibility from "./CompletionEligibility.ts";
import { makeTestRelay, THREAD_ID, PROJECT_ID } from "./AgentAwarenessRelay.testkit.ts";

const decodePublicKey = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Struct({ publicKey: Schema.String })),
);

export const heldCompletionKinds = (
  ["background_task", "command", "monitor", "subagent", "pull_request"] as const
).filter((kind) => {
  if (kind === "pull_request") return true;
  const task = { kind, awaited: true as const };
  return backgroundWorkHoldsCompletion([task]);
});

export const heldCompletionScenario = Effect.fn("Test.heldCompletionScenario")(
  function* (
    holdKind:
      | "background_task"
      | "command"
      | "monitor"
      | "subagent"
      | "pull_request"
      | "pull_request_link"
      | "turn_item",
    onCompletion?: (input: {
      readonly state: RelayAgentActivityState;
      readonly proof: string;
      readonly publicKey: string;
      readonly at: DateTime.Utc;
      readonly stale: boolean;
    }) => Effect.Effect<void>,
    replayDisagrees = false,
  ) {
    const store = yield* ProjectionStore.ProjectionStoreV2;
    const eventStore = yield* EventStore.EventStoreV2;
    const watchesPullRequest = holdKind === "pull_request" || holdKind === "pull_request_link";
    if (replayDisagrees) {
      const apply = ProjectionStore.applyToProjection;
      const replay = vi
        .spyOn(ProjectionStore, "applyToProjection")
        .mockImplementation((projection, event) => {
          if (
            holdKind === "turn_item" &&
            event.type === "turn-item.updated" &&
            event.payload.type === "dynamic_tool" &&
            !isOrchestrationV2WorkActive(event.payload.status)
          )
            return projection;
          if (
            watchesPullRequest &&
            (event.type === "thread.pull-request-synced" ||
              event.type === "thread.metadata-updated") &&
            !(event.payload.pullRequests ?? []).some((link) => link.watch !== undefined)
          )
            return apply(projection, {
              ...event,
              payload: { ...event.payload, pullRequests: projection.thread.pullRequests },
            });
          if (
            watchesPullRequest &&
            event.type === "thread.pull-request-link-updated" &&
            event.payload.watch === undefined
          )
            return projection;
          if (event.type !== "provider-thread.updated") return apply(projection, event);
          const tasks = event.payload.pendingBackgroundTasks ?? [];
          return apply(projection, {
            ...event,
            payload: {
              ...event.payload,
              pendingBackgroundTasks: [
                ...tasks.filter((task) => !backgroundWorkHoldsCompletion([task])),
                { taskId: "replay-hold", kind: "monitor", awaited: true },
              ],
            },
          });
        });
      yield* Effect.addFinalizer(() => Effect.sync(() => replay.mockRestore()));
    }
    const events = yield* Queue.unbounded<OrchestrationV2DomainEvent>();
    const pulls = yield* Queue.unbounded<void>();
    let historyReads = 0;
    const replayStore = EventStore.EventStoreV2.of({
      ...eventStore,
      read: (input) => {
        historyReads++;
        return eventStore.read(input);
      },
    });
    const { relay, shellReads, publications, secrets } = yield* makeTestRelay({
      eventStore: replayStore,
      readShell: (threadId) => store.getThreadShell(threadId).pipe(Effect.orDie),
      domainEvents: Stream.fromEffectRepeat(
        Queue.offer(pulls, undefined).pipe(Effect.andThen(Queue.take(events))),
      ),
    });
    yield* relay.start();
    yield* Queue.take(pulls);
    const deliver = Effect.fnUntraced(function* (event: OrchestrationV2DomainEvent) {
      yield* eventStore.append({ events: [event] });
      yield* store.apply(event);
      yield* Queue.offer(events, event);
      yield* Queue.take(pulls);
      yield* relay.drain;
    });

    const now = yield* DateTime.now;
    const runId = RunId.make("run:held-item");
    const run = {
      id: runId,
      threadId: THREAD_ID,
      ordinal: 1,
      providerInstanceId: ProviderInstanceId.make("codex"),
      modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "test-model" },
      providerThreadId: null,
      userMessageId: MessageId.make("message:held-item"),
      rootNodeId: null,
      activeAttemptId: null,
      status: "running" as const,
      requestedAt: now,
      startedAt: now,
      completedAt: null,
      checkpointId: null,
      contextHandoffId: null,
    };
    const item = (
      id: string,
      fields: Pick<OrchestrationV2TurnItem, "status"> &
        (
          | {
              readonly type: "dynamic_tool";
              readonly toolName: string;
              readonly input: unknown;
            }
          | {
              readonly type: "command_execution";
              readonly input: string;
            }
          | {
              readonly type: "assistant_message";
              readonly messageId: MessageId;
              readonly text: string;
              readonly streaming: boolean;
            }
        ),
    ): OrchestrationV2TurnItem => ({
      id: TurnItemId.make(id),
      threadId: THREAD_ID,
      runId,
      nodeId: NodeId.make("node:held-item"),
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: 1,
      title: null,
      startedAt: now,
      completedAt: null,
      updatedAt: now,
      ...fields,
    });
    const itemEvent = (id: string, payload: OrchestrationV2TurnItem) =>
      ({
        id: EventId.make(id),
        type: "turn-item.updated",
        threadId: THREAD_ID,
        runId,
        occurredAt: payload.updatedAt,
        payload,
      }) satisfies OrchestrationV2DomainEvent;
    const background = { type: "dynamic_tool" as const, toolName: "watch", input: {} };

    yield* deliver({
      id: EventId.make("event:held-item:thread"),
      type: "thread.created",
      threadId: THREAD_ID,
      occurredAt: now,
      payload: {
        createdBy: "user",
        creationSource: "web",
        id: THREAD_ID,
        projectId: PROJECT_ID,
        title: "Thread",
        providerInstanceId: ProviderInstanceId.make("codex"),
        modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "test-model" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        activeProviderThreadId: null,
        lineage: { rootThreadId: THREAD_ID, parentThreadId: null, relationshipToParent: null },
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
        deletedAt: null,
      },
    });
    yield* deliver({
      id: EventId.make("event:held-item:run"),
      type: "run.created",
      threadId: THREAD_ID,
      runId,
      occurredAt: now,
      payload: run,
    });
    yield* deliver(
      itemEvent("event:held-item:tool", item("item:tool", { ...background, status: "running" })),
    );
    const providerThread = {
      id: ProviderThreadId.make("provider-thread:held"),
      driver: ProviderDriverKind.make("codex"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      providerSessionId: null,
      appThreadId: THREAD_ID,
      ownerNodeId: null,
      nativeThreadRef: null,
      nativeConversationHeadRef: null,
      status: "idle" as const,
      firstRunOrdinal: 1,
      lastRunOrdinal: 1,
      handoffIds: [],
      forkedFrom: null,
      pendingBackgroundTasks:
        holdKind === "turn_item"
          ? []
          : [
              {
                taskId: "last-hold",
                kind: watchesPullRequest ? ("monitor" as const) : holdKind,
                awaited: true as const,
              },
            ],
      createdAt: now,
      updatedAt: now,
    };
    yield* deliver({
      id: EventId.make("event:held-item:roster"),
      type: "provider-thread.updated",
      threadId: THREAD_ID,
      occurredAt: now,
      payload: providerThread,
    });
    if (watchesPullRequest) {
      const thread = yield* store.getThread(THREAD_ID);
      yield* deliver({
        id: EventId.make("event:held-item:watch"),
        type:
          holdKind === "pull_request_link"
            ? "thread.metadata-updated"
            : "thread.pull-request-synced",
        threadId: THREAD_ID,
        occurredAt: now,
        payload: {
          ...thread,
          pullRequests: [
            {
              host: "github.com",
              repository: "test/relay",
              number: 1,
              url: "https://github.com/test/relay/pull/1",
              source: "agent",
              linkedAt: DateTime.formatIso(now),
              snapshot: null,
              stack: null,
              watch: {
                startedAt: DateTime.formatIso(now),
                headSha: null,
                failedChecks: [],
                passed: false,
                passedChecks: [],
                remarksThrough: DateTime.formatIso(now),
                remarkIds: [],
                conflicting: false,
                wakes: 0,
              },
            },
          ],
        },
      });
    }
    yield* deliver({
      id: EventId.make("event:held-item:run-completed"),
      type: "run.updated",
      threadId: THREAD_ID,
      runId,
      occurredAt: now,
      payload: { ...run, status: "completed", completedAt: now },
    });
    assert.deepEqual(
      publications.map((publication) => publication.state?.phase),
      ["running"],
    );

    const readsBeforeOutput = shellReads.length;
    yield* deliver(
      itemEvent(
        "event:held-item:reply",
        item("item:reply", {
          type: "assistant_message",
          messageId: MessageId.make("message:held-item:reply"),
          text: "Still watching",
          streaming: true,
          status: "running",
        }),
      ),
    );
    yield* deliver(
      itemEvent("event:held-item:tick", item("item:tool", { ...background, status: "running" })),
    );
    assert.equal(shellReads.length, readsBeforeOutput);

    yield* TestClock.adjust("15 minutes");
    const itemEndedAt = yield* DateTime.now;
    yield* deliver(
      itemEvent("event:held-item:end", {
        ...item("item:tool", { ...background, status: "completed" }),
        completedAt: itemEndedAt,
        updatedAt: itemEndedAt,
      }),
    );
    yield* TestClock.adjust("5 seconds");
    yield* relay.drain;
    assert.equal(
      publications.filter((publication) => publication.state?.phase === "completed").length,
      holdKind === "turn_item" ? 1 : 0,
    );
    if (holdKind !== "turn_item") yield* TestClock.adjust("5 minutes");
    let releasedAt = holdKind === "turn_item" ? itemEndedAt : yield* DateTime.now;
    yield* deliver({
      id: EventId.make("event:held-item:roster-released"),
      type: "provider-thread.updated",
      threadId: THREAD_ID,
      occurredAt: releasedAt,
      payload: { ...providerThread, pendingBackgroundTasks: [], updatedAt: releasedAt },
    });
    if (watchesPullRequest) {
      assert.equal(
        publications.filter((publication) => publication.state?.phase === "completed").length,
        0,
      );
      yield* TestClock.adjust("5 minutes");
      releasedAt = yield* DateTime.now;
      const thread = yield* store.getThread(THREAD_ID);
      const links = (thread.pullRequests ?? []).map(({ watch: _watch, ...link }) => link);
      yield* deliver(
        holdKind === "pull_request_link"
          ? {
              id: EventId.make("event:held-item:unwatch"),
              type: "thread.pull-request-link-updated",
              threadId: THREAD_ID,
              occurredAt: releasedAt,
              payload: links[0]!,
            }
          : {
              id: EventId.make("event:held-item:unwatch"),
              type: "thread.pull-request-synced",
              threadId: THREAD_ID,
              occurredAt: releasedAt,
              payload: { ...thread, pullRequests: links, updatedAt: releasedAt },
            },
      );
    }
    yield* TestClock.adjust("5 seconds");
    yield* relay.drain;
    assert.equal(
      publications.filter((publication) => publication.state?.phase === "completed").length,
      1,
    );
    assert.isTrue(
      publications.slice(0, -1).every((publication) => publication.state?.phase === "running"),
    );
    const completion = publications.at(-1)!;
    const replayReadsAfterCompletion = historyReads;
    yield* relay.publishThread(THREAD_ID);
    yield* relay.publishThread(THREAD_ID);
    assert.equal(historyReads, replayReadsAfterCompletion);
    if (onCompletion) {
      const keys = yield* decodePublicKey(
        new TextDecoder().decode(
          Option.getOrThrow(yield* secrets.get("cloud-link-ed25519-key-pair")),
        ),
      ).pipe(Effect.orDie);
      yield* onCompletion({
        state: completion.state!,
        proof: completion.proof,
        publicKey: keys.publicKey,
        at: releasedAt,
        stale: false,
      });
      yield* TestClock.adjust("11 minutes");
      const oldThread = yield* store.getThread(THREAD_ID);
      const replayAt = yield* DateTime.now;
      yield* deliver({
        id: EventId.make("event:held-item:old-replay"),
        type: "thread.metadata-updated",
        threadId: THREAD_ID,
        occurredAt: replayAt,
        payload: { ...oldThread, title: "Old completion replay", updatedAt: replayAt },
      });
      yield* TestClock.adjust("5 seconds");
      yield* relay.drain;
      const oldPublication = publications.at(-1)!;
      assert.equal(oldPublication.state?.threadTitle, "Old completion replay");
      assert.equal(
        oldPublication.claims.t3_transition_eligible_at,
        completion.claims.t3_transition_eligible_at,
      );
      yield* onCompletion({
        state: oldPublication.state!,
        proof: oldPublication.proof,
        publicKey: keys.publicKey,
        at: yield* DateTime.now,
        stale: true,
      });
    }
    assert.deepEqual(completion.claims.t3_transition, {
      id: "run:run:held-item:completed",
      at: DateTime.formatIso(now),
    });
    const expectedAt = releasedAt;
    assert.equal(completion.claims.t3_transition_eligible_at, DateTime.formatIso(expectedAt));
    const restartResolver = yield* CompletionEligibility.make.pipe(
      Effect.provideService(EventStore.EventStoreV2, replayStore),
    );
    const completedShell = (yield* store.getThreadShell(THREAD_ID))!;
    assert.equal(
      DateTime.formatIso((yield* restartResolver(completedShell))!),
      DateTime.formatIso(expectedAt),
    );
    const replayReadsAfterRestart = historyReads;
    yield* TestClock.adjust("1 minute");
    const thread = yield* store.getThread(THREAD_ID);
    yield* deliver({
      id: EventId.make("event:held-item:rename"),
      type: "thread.metadata-updated",
      threadId: THREAD_ID,
      occurredAt: yield* DateTime.now,
      payload: { ...thread, title: "Renamed", updatedAt: yield* DateTime.now },
    });
    yield* TestClock.adjust("5 seconds");
    yield* relay.drain;
    assert.equal(publications.at(-1)?.state?.threadTitle, "Renamed");
    assert.equal(
      publications.at(-1)?.claims.t3_transition_eligible_at,
      DateTime.formatIso(expectedAt),
    );
    assert.deepEqual(publications.at(-1)?.claims.t3_transition, completion.claims.t3_transition);
    assert.equal(historyReads, replayReadsAfterRestart);
    for (const kind of [
      "pull_request",
      "pull_request_link",
      "provider_thread",
      "nonholding-command",
      "nonholding-command-ended",
      "nonholding-item-started",
      "nonholding-item-ended",
      "persistent-tool-started",
      "persistent-tool-ended",
      "inactive-provider-started",
      "inactive-provider-ended",
    ] as const) {
      if (kind === "pull_request_link" && !watchesPullRequest) continue;
      yield* TestClock.adjust("1 minute");
      const refreshedAt = yield* DateTime.now;
      const currentThread = yield* store.getThread(THREAD_ID);
      let refresh: OrchestrationV2DomainEvent;
      if (kind === "nonholding-item-started" || kind === "nonholding-item-ended") {
        refresh = itemEvent(`event:held-item:${kind}`, {
          ...item("item:quiet", {
            type: "command_execution",
            input: "tail -f build.log",
            status: kind === "nonholding-item-started" ? "running" : "completed",
          }),
          updatedAt: refreshedAt,
        });
      } else if (kind === "persistent-tool-started" || kind === "persistent-tool-ended") {
        refresh = itemEvent(`event:held-item:${kind}`, {
          ...item("item:persistent", {
            ...background,
            input: { persistent: true },
            status: kind === "persistent-tool-started" ? "running" : "completed",
          }),
          updatedAt: refreshedAt,
        });
      } else if (kind === "inactive-provider-started" || kind === "inactive-provider-ended") {
        refresh = {
          id: EventId.make(`event:held-item:${kind}`),
          type: "provider-thread.updated",
          threadId: THREAD_ID,
          occurredAt: refreshedAt,
          payload: {
            ...providerThread,
            id: ProviderThreadId.make("provider-thread:queued"),
            status: "not_loaded",
            firstRunOrdinal: null,
            pendingBackgroundTasks:
              kind === "inactive-provider-started"
                ? [{ taskId: "queued-hold", kind: "monitor", awaited: true }]
                : [],
            updatedAt: refreshedAt,
          },
        };
      } else if (kind === "pull_request_link") {
        refresh = {
          id: EventId.make("event:held-item:pr-link-refresh"),
          type: "thread.pull-request-link-updated",
          threadId: THREAD_ID,
          occurredAt: refreshedAt,
          payload: currentThread.pullRequests![0]!,
        };
      } else if (kind === "pull_request") {
        refresh = {
          id: EventId.make("event:held-item:pr-refresh"),
          type: "thread.pull-request-synced",
          threadId: THREAD_ID,
          occurredAt: refreshedAt,
          payload: { ...currentThread, updatedAt: refreshedAt },
        };
      } else {
        refresh = {
          id: EventId.make(`event:held-item:provider-refresh:${kind}`),
          type: "provider-thread.updated",
          threadId: THREAD_ID,
          occurredAt: refreshedAt,
          payload: {
            ...providerThread,
            pendingBackgroundTasks:
              kind === "nonholding-command" ? [{ taskId: "quiet-command", kind: "command" }] : [],
            updatedAt: refreshedAt,
          },
        };
      }
      yield* deliver(refresh);
      if (kind === "inactive-provider-started" || kind === "inactive-provider-ended") {
        assert.equal(
          (yield* store.getThreadShell(THREAD_ID))!.activeProviderThreadId,
          providerThread.id,
        );
      }
      const coldResolver = yield* CompletionEligibility.make.pipe(
        Effect.provideService(EventStore.EventStoreV2, replayStore),
      );
      assert.equal(
        DateTime.formatIso((yield* coldResolver((yield* store.getThreadShell(THREAD_ID))!))!),
        DateTime.formatIso(expectedAt),
        kind,
      );
    }
    const afterRenameResolver = yield* CompletionEligibility.make.pipe(
      Effect.provideService(EventStore.EventStoreV2, replayStore),
    );
    assert.equal(
      DateTime.formatIso((yield* afterRenameResolver((yield* store.getThreadShell(THREAD_ID))!))!),
      DateTime.formatIso(expectedAt),
    );
  },
  Effect.scoped,
  Effect.provide(
    Layer.mergeAll(ProjectionStore.layer, EventStore.layer).pipe(
      Layer.provideMerge(SqlitePersistence.layerMemory),
    ),
  ),
);
