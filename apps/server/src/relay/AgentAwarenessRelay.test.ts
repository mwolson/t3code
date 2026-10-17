import {
  makeTestRelay,
  THREAD_ID,
  SECOND_THREAD_ID,
  PROJECT_ID,
  NOW,
  shell,
} from "./AgentAwarenessRelay.testkit.ts";
import { heldCompletionKinds, heldCompletionScenario } from "./HeldCompletionTestSupport.ts";
import { assert, describe, it } from "@effect/vitest";
import {
  EnvironmentId,
  EventId,
  MessageId,
  NodeId,
  ProviderInstanceId,
  TurnItemId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2TurnItem,
  ProjectId,
  RunId,
  RuntimeRequestId,
  ThreadId,
} from "@t3tools/contracts";
import * as BackgroundRelaySettings from "./BackgroundRelaySettings.ts";
import { BackgroundRelayConfig } from "./BackgroundRelayConfig.ts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Ref from "effect/Ref";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as TestClock from "effect/testing/TestClock";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import {
  PUBLISH_AGENT_ACTIVITY_SECRET,
  RELAY_ENVIRONMENT_CREDENTIAL_SECRET,
  RELAY_URL_SECRET,
} from "../cloud/config.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as AgentAwarenessRelay from "./AgentAwarenessRelay.ts";

describe("startup agent activity", () => {
  it.effect("publishes only to the background relay and still honors the sharing switch", () =>
    Effect.gen(function* () {
      const { relay, secrets, publications } = yield* makeTestRelay();
      yield* relay.publishThread(THREAD_ID);
      assert.equal(publications.length, 1);
      assert.isTrue(publications[0]?.url.startsWith("https://background.example.test/") ?? false);
      assert.equal(publications[0]?.authorization, "Bearer background-credential");
      const publicCredential = yield* secrets.get(RELAY_ENVIRONMENT_CREDENTIAL_SECRET);
      assert.equal(
        Option.getOrNull(Option.map(publicCredential, (bytes) => new TextDecoder().decode(bytes))),
        "credential-1",
      );
      yield* secrets.set(PUBLISH_AGENT_ACTIVITY_SECRET, new TextEncoder().encode("false"));
      yield* relay.publishThread(THREAD_ID);
      assert.equal(publications.length, 1);
    }).pipe(
      Effect.provideService(BackgroundRelayConfig, {
        url: "https://background.example.test",
        issuer: "https://background.example.test",
        environmentCredential: Redacted.make("background-credential"),
      }),
    ),
  );

  it("publishes active work and only terminal runs completed after startup", () => {
    const startedAt = DateTime.toEpochMillis(DateTime.makeUnsafe(NOW));
    const oldCompleted = ThreadId.make("old-completed");
    const newCompleted = ThreadId.make("new-completed");
    const oldFailed = ThreadId.make("old-failed");
    const newFailed = ThreadId.make("new-failed");
    const ids = AgentAwarenessRelay.resolveAgentAwarenessRelayActiveThreadIds({
      environmentId: EnvironmentId.make("relay-env"),
      startedAt,
      projects: [{ id: PROJECT_ID, title: "Project" }],
      threads: [
        shell(),
        shell({
          id: oldCompleted,
          status: "completed",
          latestRunCompletedAt: DateTime.makeUnsafe("2026-09-04T11:59:00.000Z"),
        }),
        shell({
          id: newCompleted,
          status: "completed",
          latestRunCompletedAt: DateTime.makeUnsafe("2026-09-04T12:00:01.000Z"),
        }),
        shell({
          id: oldFailed,
          status: "failed",
          latestRunCompletedAt: DateTime.makeUnsafe("2026-09-04T11:59:00.000Z"),
        }),
        shell({
          id: newFailed,
          status: "failed",
          latestRunCompletedAt: DateTime.makeUnsafe("2026-09-04T12:00:01.000Z"),
        }),
        shell({ id: ThreadId.make("idle"), status: "idle" }),
        shell({ id: ThreadId.make("missing-project"), projectId: ProjectId.make("missing") }),
      ],
    });
    assert.deepStrictEqual(ids, [THREAD_ID, newCompleted, newFailed]);
  });
});

describe("AgentAwarenessRelay", () => {
  it.effect.each(["monitor", "pull_request", "pull_request_link", "turn_item"] as const)(
    "publishes one completion from the raw %s release when replay retains a phantom hold",
    (kind) => heldCompletionScenario(kind, undefined, true),
  );
  it.effect("coalesces queued updates and reruns a thread dirtied during publishing", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const rerunStarted = yield* Deferred.make<void>();
      const releaseRerun = yield* Deferred.make<void>();
      const processed: Array<{ threadId: ThreadId; revision: number }> = [];
      let revision = 1;
      const worker = yield* AgentAwarenessRelay.makeAgentAwarenessPublishWorker((threadId) =>
        Effect.gen(function* () {
          const currentRevision = revision;
          const index = processed.length;
          processed.push({ threadId, revision: currentRevision });
          if (index === 0) {
            yield* Deferred.succeed(started, undefined);
            yield* Deferred.await(release);
          } else if (threadId === THREAD_ID) {
            yield* Deferred.succeed(rerunStarted, undefined);
            yield* Deferred.await(releaseRerun);
          }
        }),
      );
      yield* worker.enqueue(THREAD_ID);
      yield* Deferred.await(started);
      revision = 2;
      for (let i = 0; i < 100; i++) {
        yield* worker.enqueue(SECOND_THREAD_ID);
        yield* worker.enqueue(THREAD_ID);
      }
      const drained = yield* Deferred.make<void>();
      const draining = yield* worker.drain.pipe(
        Effect.andThen(Deferred.succeed(drained, undefined)),
        Effect.forkChild,
      );
      yield* Deferred.succeed(release, undefined);
      yield* Deferred.await(rerunStarted);
      assert.isFalse(yield* Deferred.isDone(drained));
      yield* Deferred.succeed(releaseRerun, undefined);
      yield* Fiber.join(draining);
      assert.deepEqual(processed, [
        { threadId: THREAD_ID, revision: 1 },
        { threadId: SECOND_THREAD_ID, revision: 2 },
        { threadId: THREAD_ID, revision: 2 },
      ]);
    }),
  );

  it.effect("deduplicates state and republishes title changes", () =>
    Effect.gen(function* () {
      const { relay, currentShell, publications } = yield* makeTestRelay();
      yield* relay.publishThread(THREAD_ID);
      yield* Ref.set(
        currentShell,
        shell({ updatedAt: DateTime.makeUnsafe("2026-09-04T13:00:00Z") }),
      );
      yield* relay.publishThread(THREAD_ID);
      assert.equal(publications.length, 1);
      yield* Ref.set(currentShell, shell({ title: "Renamed thread" }));
      yield* relay.publishThread(THREAD_ID);
      assert.equal(publications.length, 2);
      assert.equal(publications[1]?.state?.threadTitle, "Renamed thread");
    }),
  );

  it.effect("republishes unchanged state with fresh credentials after relinking", () =>
    Effect.gen(function* () {
      const { relay, secrets, publications } = yield* makeTestRelay();
      yield* relay.publishThread(THREAD_ID);
      yield* secrets.set(
        RELAY_ENVIRONMENT_CREDENTIAL_SECRET,
        new TextEncoder().encode("credential-2"),
      );
      yield* relay.publishThread(THREAD_ID);
      assert.equal(publications.length, 2);
      assert.equal(publications[0]?.authorization, "Bearer credential-1");
      assert.equal(publications[1]?.authorization, "Bearer credential-2");
      yield* secrets.set(
        RELAY_URL_SECRET,
        new TextEncoder().encode("https://new-relay.example.test"),
      );
      yield* relay.publishThread(THREAD_ID);
      assert.equal(publications.length, 3);
      assert.isTrue(publications[2]?.url.startsWith("https://new-relay.example.test/") ?? false);
    }),
  );

  it.effect("signs the durable creation time and identity of attention requests", () =>
    Effect.gen(function* () {
      const { relay, currentShell, publications } = yield* makeTestRelay();
      const createdAt = DateTime.makeUnsafe("2026-10-17T12:00:00.000Z");
      yield* Ref.set(
        currentShell,
        shell({
          pendingRuntimeRequest: {
            id: RuntimeRequestId.make("request:attention"),
            kind: "permission",
            createdAt,
          },
        }),
      );
      yield* relay.publishThread(THREAD_ID);
      assert.deepEqual(publications[0]?.claims.t3_transition, {
        id: "request:request:attention",
        at: DateTime.formatIso(createdAt),
      });
      assert.equal(
        publications[0]?.claims.t3_transition_eligible_at,
        DateTime.formatIso(createdAt),
      );
    }),
  );

  it.effect(
    "signs stable transitions on retries and sends a fast second completion with identical content",
    () =>
      Effect.gen(function* () {
        const { relay, currentShell, publications } = yield* makeTestRelay({
          respond: (attempt) =>
            attempt === 2
              ? new Response("response lost", { status: 503 })
              : Response.json({ ok: true, deliveries: [] }),
        });
        yield* relay.publishThread(THREAD_ID);
        const completed = shell({
          status: "completed",
          latestRunId: RunId.make("run:first"),
          latestRunCompletedAt: DateTime.add(yield* DateTime.now, { seconds: 1 }),
        });
        yield* Ref.set(currentShell, completed);
        yield* relay.publishThread(THREAD_ID);
        yield* TestClock.adjust("5 seconds");
        yield* relay.drain;
        const first = publications.at(-1)!;
        yield* Ref.set(currentShell, {
          ...completed,
          title: "New title",
          updatedAt: DateTime.add(yield* DateTime.now, { seconds: 2 }),
        });
        yield* TestClock.adjust("1 second");
        yield* relay.drain;
        const retried = publications.at(-1)!;
        assert.deepEqual(retried.claims.t3_transition, first.claims.t3_transition);
        assert.equal(
          retried.claims.t3_transition_eligible_at,
          first.claims.t3_transition_eligible_at,
        );
        assert.isAbove(retried.claims.t3_publish_seq, first.claims.t3_publish_seq);
        assert.equal(retried.state?.threadTitle, "New title");
        const count = publications.length;
        yield* Ref.set(currentShell, {
          ...completed,
          title: "New title",
          latestRunId: RunId.make("run:second"),
        });
        yield* relay.publishThread(THREAD_ID);
        yield* TestClock.adjust("5 seconds");
        yield* relay.drain;
        assert.equal(publications.length, count + 1);
        assert.equal(publications.at(-1)?.claims.t3_transition?.id, "run:run:second:completed");
        yield* Ref.set(currentShell, {
          ...completed,
          title: "New title",
          latestRunId: RunId.make("run:second"),
          updatedAt: DateTime.add(yield* DateTime.now, { minutes: 1 }),
        });
        yield* relay.publishThread(THREAD_ID);
        assert.equal(publications.length, count + 1);
      }),
  );

  it.effect.each(["completed", "failed"] as const)(
    "publishes a tombstone when a %s thread is settled through a domain event",
    (status) =>
      Effect.gen(function* () {
        const events = yield* Queue.unbounded<OrchestrationV2DomainEvent>();
        const pulls = yield* Queue.unbounded<void>();
        const { relay, currentShell, publications } = yield* makeTestRelay({
          domainEvents: Stream.fromEffectRepeat(
            Queue.offer(pulls, undefined).pipe(Effect.andThen(Queue.take(events))),
          ),
        });
        yield* relay.start();
        yield* Queue.take(pulls);
        yield* relay.publishThread(THREAD_ID);
        const terminal = shell({ status });
        yield* Ref.set(currentShell, terminal);
        yield* relay.publishThread(THREAD_ID);
        yield* TestClock.adjust("5 seconds");
        yield* relay.drain;
        assert.equal(publications[1]?.state?.phase, status);

        const now = yield* DateTime.now;
        const settled = {
          ...terminal,
          settledOverride: "settled" as const,
          settledAt: now,
          lastVisitedAt: null,
        };
        yield* Ref.set(currentShell, settled);
        yield* Queue.offer(events, {
          id: EventId.make("event:settled"),
          type: "thread.settled",
          threadId: THREAD_ID,
          occurredAt: now,
          payload: settled,
        });
        yield* Queue.take(pulls);
        yield* relay.drain;
        assert.equal(publications.length, 2);
        yield* TestClock.adjust("5 seconds");
        yield* relay.drain;
        assert.equal(publications.length, 3);
        assert.equal(publications[2]?.state, null);
      }).pipe(Effect.scoped),
  );

  it.effect("holds a completion through a continuation gap, then publishes it once", () =>
    Effect.gen(function* () {
      const { relay, currentShell, publications } = yield* makeTestRelay();
      yield* relay.publishThread(THREAD_ID);
      yield* Ref.set(currentShell, shell({ status: "completed" }));
      yield* relay.publishThread(THREAD_ID);
      yield* Ref.set(currentShell, shell());
      yield* relay.publishThread(THREAD_ID);
      yield* TestClock.adjust("5 seconds");
      yield* relay.drain;
      assert.deepEqual(
        publications.map((publication) => publication.state?.phase),
        ["running"],
      );

      yield* Ref.set(currentShell, shell({ status: "completed" }));
      yield* relay.publishThread(THREAD_ID);
      assert.equal(publications.length, 1);
      yield* TestClock.adjust("5 seconds");
      yield* relay.drain;
      assert.deepEqual(
        publications.map((publication) => publication.state?.phase),
        ["running", "completed"],
      );
    }),
  );

  it.effect("still publishes a completion when the thread settles during the hold", () =>
    Effect.gen(function* () {
      const { relay, currentShell, publications } = yield* makeTestRelay();
      yield* relay.publishThread(THREAD_ID);
      const completed = shell({ status: "completed" });
      yield* Ref.set(currentShell, completed);
      yield* relay.publishThread(THREAD_ID);
      yield* Ref.set(currentShell, {
        ...completed,
        settledOverride: "settled",
        settledAt: yield* DateTime.now,
        lastVisitedAt: null,
      });
      yield* relay.publishThread(THREAD_ID);
      yield* TestClock.adjust("5 seconds");
      yield* relay.drain;
      assert.deepEqual(
        publications.map((publication) => publication.state?.phase ?? null),
        ["running", "completed"],
      );
      assert.match(publications[1]?.claims.t3_transition?.id ?? "", /:completed$/);

      yield* TestClock.adjust("5 seconds");
      yield* relay.drain;
      assert.deepEqual(
        publications.map((publication) => publication.state?.phase ?? null),
        ["running", "completed", null],
      );
    }),
  );

  it.effect("drops a held completion when a queued continuation hides the thread", () =>
    Effect.gen(function* () {
      const { relay, currentShell, publications } = yield* makeTestRelay();
      yield* relay.publishThread(THREAD_ID);
      yield* Ref.set(currentShell, shell({ status: "completed" }));
      yield* relay.publishThread(THREAD_ID);
      yield* Ref.set(currentShell, shell({ status: "queued" }));
      yield* relay.publishThread(THREAD_ID);
      yield* TestClock.adjust("5 seconds");
      yield* relay.drain;
      assert.notInclude(
        publications.map((publication) => publication.state?.phase ?? null),
        "completed",
      );
    }),
  );

  it.effect("retries a failed completion even after the thread settles", () =>
    Effect.gen(function* () {
      const { relay, currentShell, publications } = yield* makeTestRelay({
        respond: (attempt) =>
          attempt === 2
            ? new Response("unavailable", { status: 503 })
            : Response.json({ ok: true, deliveries: [] }),
      });
      yield* relay.publishThread(THREAD_ID);
      const completed = shell({ status: "completed" });
      yield* Ref.set(currentShell, completed);
      yield* relay.publishThread(THREAD_ID);
      yield* TestClock.adjust("5 seconds");
      yield* relay.drain;
      yield* Ref.set(currentShell, {
        ...completed,
        settledOverride: "settled",
        settledAt: yield* DateTime.now,
        lastVisitedAt: null,
      });
      yield* TestClock.adjust("1 second");
      yield* relay.drain;
      assert.deepEqual(
        publications.map((publication) => publication.state?.phase ?? null),
        ["running", "completed", "completed"],
      );

      yield* TestClock.adjust("5 seconds");
      yield* relay.drain;
      assert.equal(publications.at(-1)?.state ?? null, null);
    }),
  );

  it.effect("keeps settled live work until its awaited monitor finishes", () =>
    Effect.gen(function* () {
      const { relay, currentShell, publications } = yield* makeTestRelay();
      yield* relay.publishThread(THREAD_ID);
      const held = shell({
        status: "completed",
        settledOverride: "settled",
        pendingBackgroundTasks: [{ taskId: "watch", kind: "monitor", awaited: true }],
      });
      yield* Ref.set(currentShell, held);
      yield* relay.publishThread(THREAD_ID);
      yield* TestClock.adjust("5 seconds");
      yield* relay.drain;
      assert.equal(publications.length, 1);
      assert.equal(publications[0]?.state?.phase, "running");

      yield* Ref.set(currentShell, { ...held, pendingBackgroundTasks: [] });
      yield* relay.publishThread(THREAD_ID);
      yield* TestClock.adjust("5 seconds");
      yield* relay.drain;
      assert.equal(publications.length, 2);
      assert.equal(publications[1]?.state, null);
    }),
  );
  it.effect("publishes a held completion when its background item ends, not on tool output", () =>
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const events = yield* Queue.unbounded<OrchestrationV2DomainEvent>();
      const pulls = yield* Queue.unbounded<void>();
      const { relay, shellReads, publications } = yield* makeTestRelay({
        readShell: (threadId) => store.getThreadShell(threadId).pipe(Effect.orDie),
        domainEvents: Stream.fromEffectRepeat(
          Queue.offer(pulls, undefined).pipe(Effect.andThen(Queue.take(events))),
        ),
      });
      yield* relay.start();
      yield* Queue.take(pulls);
      const deliver = Effect.fnUntraced(function* (event: OrchestrationV2DomainEvent) {
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
            | { readonly type: "dynamic_tool"; readonly toolName: string; readonly input: unknown }
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
          occurredAt: now,
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

      yield* deliver(
        itemEvent("event:held-item:end", item("item:tool", { ...background, status: "completed" })),
      );
      yield* TestClock.adjust("5 seconds");
      yield* relay.drain;
      assert.deepEqual(
        publications.map((publication) => publication.state?.phase),
        ["running", "completed"],
      );
    }).pipe(
      Effect.scoped,
      Effect.provide(ProjectionStore.layer.pipe(Layer.provide(SqlitePersistence.layerMemory))),
    ),
  );
  it.effect.each(heldCompletionKinds)(
    "publishes a long-held completion only when its last %s hold releases",
    (holdKind) => heldCompletionScenario(holdKind),
  );
});

const secretConfig = (label: string) =>
  new TextEncoder().encode(
    JSON.stringify({
      version: 1,
      url: `https://${label}.example.test`,
      issuer: `https://${label}-issuer.example.test`,
      environmentCredential: `${label}-credential`,
    }),
  );

const setBackground = (secrets: ServerSecretStore.ServerSecretStore["Service"], label: string) =>
  secrets.set(BackgroundRelaySettings.SECRET, secretConfig(label));

describe("background relay resync", () => {
  it.effect("disables a malformed secret without primary fallback and catches up after clear", () =>
    Effect.gen(function* () {
      const { relay, secrets, publications } = yield* makeTestRelay({
        snapshotThreads: () => [shell()],
      });
      yield* secrets.set(BackgroundRelaySettings.SECRET, new TextEncoder().encode('{"version":2}'));
      yield* relay.start();
      yield* TestClock.adjust("1 second");
      yield* relay.publishThread(THREAD_ID);
      yield* TestClock.adjust("1 minute");
      assert.equal(publications.length, 0);
      yield* secrets.remove(BackgroundRelaySettings.SECRET);
      yield* TestClock.adjust("5 seconds");
      yield* relay.drain;
      assert.equal(publications.length, 1);
      assert.equal(publications[0]?.authorization, "Bearer credential-1");
    }).pipe(Effect.scoped),
  );

  it.effect("activates a secret-only destination after startup catch-up has finished", () =>
    Effect.gen(function* () {
      const { relay, secrets, publications, catchUp } = yield* makeTestRelay({
        linked: false,
        snapshotThreads: () => [shell()],
      });
      yield* secrets.set(PUBLISH_AGENT_ACTIVITY_SECRET, new TextEncoder().encode("true"));
      yield* relay.start();
      yield* TestClock.adjust("1 second");
      assert.equal(catchUp.shellSnapshotReads, 0);
      yield* setBackground(secrets, "first");
      yield* TestClock.adjust("1 minute");
      yield* relay.drain;
      assert.equal(publications.length, 1);
      assert.equal(publications[0]?.authorization, "Bearer first-credential");
      yield* setBackground(secrets, "second");
      yield* TestClock.adjust("5 seconds");
      yield* relay.drain;
      assert.equal(publications.length, 2);
      assert.equal(publications[1]?.authorization, "Bearer second-credential");
      yield* TestClock.adjust("10 minutes");
      assert.equal(publications.length, 2);
    }).pipe(Effect.scoped),
  );

  it.effect("recovers exhausted retries quietly after the acknowledged startup resync", () =>
    Effect.gen(function* () {
      let failing = false;
      let title = "Initial";
      const ids = [THREAD_ID, SECOND_THREAD_ID];
      const snapshot = () => ids.map((id) => shell({ id, title }));
      const { relay, publications } = yield* makeTestRelay({
        snapshotThreads: snapshot,
        readShell: (id) => Effect.sync(() => shell({ id, title })),
        respond: () =>
          Response.json(failing ? {} : { ok: true, deliveries: [] }, {
            status: failing ? 503 : 200,
          }),
      });
      yield* relay.start();
      yield* TestClock.adjust("1 second");
      yield* relay.drain;
      assert.equal(publications.length, 2);
      yield* TestClock.adjust("1 minute");
      assert.equal(publications.length, 2);
      failing = true;
      title = "Changed during outage";
      for (const id of ids) yield* relay.publishThread(id);
      for (let i = 0; i < 60; i++) {
        yield* TestClock.adjust("1 second");
        yield* relay.drain;
      }
      const outageAttempts = publications.length;
      assert.isAbove(outageAttempts, 14);
      failing = false;
      yield* TestClock.adjust("5 minutes");
      yield* relay.drain;
      assert.equal(publications.length, outageAttempts + 2);
      assert.deepEqual(
        publications
          .slice(-2)
          .map((p) => p.state?.threadId)
          .sort(),
        [...ids].sort(),
      );
      assert.isTrue(publications.slice(-2).every((p) => p.state?.threadTitle === title));
      const acknowledgedAttempts = publications.length;
      yield* TestClock.adjust("10 minutes");
      assert.equal(publications.length, acknowledgedAttempts);
    }).pipe(Effect.scoped),
  );

  it.effect("restarts resync when sharing is enabled again", () =>
    Effect.gen(function* () {
      const { relay, secrets, publications } = yield* makeTestRelay({
        snapshotThreads: () => [shell()],
      });
      yield* relay.start();
      yield* TestClock.adjust("1 second");
      yield* relay.drain;
      assert.equal(publications.length, 1);
      yield* secrets.set(PUBLISH_AGENT_ACTIVITY_SECRET, new TextEncoder().encode("false"));
      yield* TestClock.adjust("5 seconds");
      yield* setBackground(secrets, "changed");
      yield* TestClock.adjust("5 seconds");
      assert.equal(publications.length, 1);
      yield* secrets.set(PUBLISH_AGENT_ACTIVITY_SECRET, new TextEncoder().encode("true"));
      yield* TestClock.adjust("5 seconds");
      yield* relay.drain;
      assert.equal(publications.length, 2);
      assert.equal(publications[1]?.authorization, "Bearer changed-credential");
    }).pipe(Effect.scoped),
  );

  it.effect("publishes one immutable tuple across atomic replacement and clear barriers", () =>
    Effect.gen(function* () {
      const beforeWrite = yield* Deferred.make<void>();
      const allowWrite = yield* Deferred.make<void>();
      const afterWrite = yield* Deferred.make<void>();
      const finishWrite = yield* Deferred.make<void>();
      let barrier = false;
      const { relay, secrets, publications, currentShell } = yield* makeTestRelay({
        beforeSet: (name) =>
          name === BackgroundRelaySettings.SECRET && barrier
            ? Deferred.succeed(beforeWrite, undefined).pipe(
                Effect.andThen(Deferred.await(allowWrite)),
              )
            : Effect.void,
        afterSet: (name) =>
          name === BackgroundRelaySettings.SECRET && barrier
            ? Deferred.succeed(afterWrite, undefined).pipe(
                Effect.andThen(Deferred.await(finishWrite)),
              )
            : Effect.void,
      });
      yield* setBackground(secrets, "first");
      barrier = true;
      const write = yield* setBackground(secrets, "second").pipe(Effect.forkScoped);
      yield* Deferred.await(beforeWrite);
      yield* relay.publishThread(THREAD_ID);
      yield* Deferred.succeed(allowWrite, undefined);
      yield* Deferred.await(afterWrite);
      yield* Ref.set(currentShell, shell({ title: "After replacement" }));
      yield* relay.publishThread(THREAD_ID);
      yield* Deferred.succeed(finishWrite, undefined);
      yield* Fiber.join(write);
      yield* secrets.remove(BackgroundRelaySettings.SECRET);
      yield* Ref.set(currentShell, shell({ title: "After clear" }));
      yield* relay.publishThread(THREAD_ID);
      assert.deepEqual(
        publications.map((p) => [new URL(p.url).origin, p.issuer, p.authorization]),
        [
          [
            "https://first.example.test",
            "https://first-issuer.example.test",
            "Bearer first-credential",
          ],
          [
            "https://second.example.test",
            "https://second-issuer.example.test",
            "Bearer second-credential",
          ],
          ["https://relay.example.test", "https://relay.example.test", "Bearer credential-1"],
        ],
      );
    }).pipe(Effect.scoped),
  );
});
