import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import type { OpenCodeClient, V2Event } from "@opencode/client";
import {
  MessageId,
  NodeId,
  OpenCode2Settings,
  OrchestrationV2AppThread,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  type ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import { ServerConfig } from "../../config.ts";
import { IdAllocatorV2, layer as idAllocatorLayer } from "../IdAllocator.ts";
import {
  ProviderAdapterProtocolError,
  ProviderAdapterV2RuntimePolicy,
} from "../ProviderAdapter.ts";
import { readProviderReplayTranscript } from "../testkit/ReplayTranscriptNdjson.ts";
import { makeOpenCodeAdapterV2, OPENCODE_PROVIDER } from "./OpenCodeAdapterV2.ts";
import { makeReplayClient, OpenCode2ReplayController } from "./OpenCodeAdapterV2.testkit.ts";

// The stall watchdog runs on TestClock windows between replay frames. The
// scenario runner cannot interleave clock steps with an active OpenCode turn,
// so these tests drive the adapter directly from recorded-shape transcripts.

const decodeSettings = Schema.decodeUnknownEffect(OpenCode2Settings);
const isProtocolError = Schema.is(ProviderAdapterProtocolError);

const TestLayer = Layer.mergeAll(
  idAllocatorLayer,
  ServerConfig.layerTest(process.cwd(), { prefix: "opencode-stall-" }),
).pipe(Layer.provideMerge(NodeServices.layer));

const flush = Effect.yieldNow.pipe(
  Effect.andThen(Effect.promise(() => new Promise<void>((resolve) => setImmediate(resolve)))),
);

interface StallTerminal {
  readonly status: string;
  readonly failure: { readonly code: string | null; readonly class: string } | null;
}

const openStallSession = Effect.fnUntraced(function* (
  fixture: string,
  wrapClient: (client: OpenCodeClient) => void = () => {},
) {
  const transcript = yield* readProviderReplayTranscript(
    new URL(`../testkit/fixtures/${fixture}/opencode2_transcript.ndjson`, import.meta.url),
  );
  const controller = new OpenCode2ReplayController({
    ...transcript,
    provider: OPENCODE_PROVIDER,
    protocol: "opencode2-sdk.sse",
  });
  yield* Effect.addFinalizer(() => Effect.sync(() => controller.abort()));
  const client = makeReplayClient(controller);
  wrapClient(client);
  const instanceId = ProviderInstanceId.make("opencode-stall-test");
  const modelSelection = { instanceId, model: "opencode/big-pickle" };
  const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
    runtimeMode: "full-access",
    interactionMode: "default",
    cwd: process.cwd(),
  });
  const adapter = makeOpenCodeAdapterV2({
    interactionModeReflections: { offer: () => Effect.void },
    instanceId,
    settings: yield* decodeSettings({ serverUrl: "replay://opencode2" }),
    environment: {},
    runtime: {
      connectToOpenCodeServer: () =>
        Effect.succeed({
          url: "replay://opencode2",
          password: "replay",
          external: true,
          exitCode: null,
        }),
      createOpenCodeSdkClient: () => client,
    },
    idAllocator: yield* IdAllocatorV2,
    serverConfig: yield* ServerConfig,
  });
  const session = yield* adapter.openSession({
    threadId: ThreadId.make(`opencode-stall-${fixture}`),
    providerSessionId: ProviderSessionId.make(`opencode-stall-session-${fixture}`),
    modelSelection,
    runtimePolicy,
  });
  const terminals = new Map<string, Array<StallTerminal>>();
  const assistantTexts: Array<string> = [];
  const turnIds = new Map<string, ProviderTurnId>();
  yield* session.events.pipe(
    Stream.runForEach((event) =>
      Effect.sync(() => {
        if (event.type === "provider_turn.updated") {
          turnIds.set(event.providerTurn.providerThreadId, event.providerTurn.id);
        }
        if (event.type === "turn_item.updated" && event.turnItem.type === "assistant_message") {
          assistantTexts.push(event.turnItem.text);
        }
        if (event.type !== "turn.terminal") return;
        const settled = terminals.get(event.providerThreadId) ?? [];
        settled.push({ status: event.status, failure: event.failure });
        terminals.set(event.providerThreadId, settled);
      }),
    ),
    Effect.forkChild({ startImmediately: true }),
  );
  const terminalCount = () =>
    Array.from(terminals.values()).reduce((total, settled) => total + settled.length, 0);
  const prompts = transcript.entries.flatMap((entry) =>
    entry.type === "expect_outbound" && entry.label?.startsWith("session.prompt")
      ? [(entry.frame as { input: { prompt: { text: string } } }).input.prompt.text]
      : [],
  );

  const ensureThread = (name: string) =>
    session.ensureThread({
      threadId: ThreadId.make(`opencode-stall-${fixture}-${name}`),
      modelSelection,
      runtimePolicy,
    });

  const startTurn = Effect.fnUntraced(function* (
    name: string,
    providerThread: Effect.Success<ReturnType<typeof ensureThread>>,
    text: string,
    ordinal = 1,
  ) {
    const threadId = ThreadId.make(`opencode-stall-${fixture}-${name}`);
    const turnName = `${name}-${ordinal}`;
    const now = yield* DateTime.now;
    yield* session.startTurn({
      appThread: OrchestrationV2AppThread.make({
        id: threadId,
        projectId: ProjectId.make("stall-project"),
        title: "Stall regression",
        providerInstanceId: instanceId,
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        createdBy: "user",
        creationSource: "web",
        branch: null,
        worktreePath: process.cwd(),
        activeProviderThreadId: providerThread.id,
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        deletedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
      }),
      threadId,
      runId: RunId.make(`stall-run-${turnName}`),
      runOrdinal: ordinal,
      providerTurnOrdinal: ordinal,
      attemptId: RunAttemptId.make(`stall-attempt-${turnName}`),
      rootNodeId: NodeId.make(`stall-root-${turnName}`),
      providerThread,
      message: {
        messageId: MessageId.make(`stall-message-${turnName}`),
        text,
        attachments: [],
        createdBy: "user",
        creationSource: "web",
      },
      modelSelection,
      runtimePolicy,
    });
  });

  /** Advance TestClock one second at a time until the predicate holds. */
  const advanceUntil = Effect.fnUntraced(function* (
    description: string,
    predicate: () => boolean,
    maxSeconds = 240,
  ) {
    for (let second = 0; second <= maxSeconds; second += 1) {
      for (let tick = 0; tick < 4; tick += 1) yield* flush;
      if (predicate()) return;
      yield* TestClock.adjust("1 second");
    }
    const settled = [...terminals]
      .map(
        ([id, list]) =>
          `${id}=${list
            .map((terminal) =>
              terminal.failure === null
                ? terminal.status
                : `${terminal.status}(${terminal.failure.code})`,
            )
            .join(",")}`,
      )
      .join(" ");
    const next = controller.peek();
    assert.fail(
      `timed out waiting for ${description} in ${fixture}; terminals: ${settled}; next: ${next !== undefined && "label" in next ? next.label : next?.type}`,
    );
  });

  return {
    controller,
    client,
    session,
    modelSelection,
    runtimePolicy,
    prompts,
    terminals,
    terminalCount,
    assistantTexts,
    turnIds,
    ensureThread,
    startTurn,
    advanceUntil,
  };
});

/**
 * Records how many turns had settled when OpenCode was asked to interrupt,
 * after letting the event collector catch up.
 */
function watchInterrupts(client: OpenCodeClient, terminalCount: () => number) {
  const seen: Array<number> = [];
  const interrupt = client.session.interrupt.bind(client.session);
  client.session.interrupt = (async (
    ...args: Parameters<OpenCodeClient["session"]["interrupt"]>
  ) => {
    for (let tick = 0; tick < 8; tick += 1) {
      await new Promise<void>((resolve) => setImmediate(resolve));
    }
    seen.push(terminalCount());
    return interrupt(...args);
  }) as OpenCodeClient["session"]["interrupt"];
  return seen;
}

/**
 * Holds `session.interrupt` for one session until its request is abandoned, so
 * the request never reaches the transcript and times out.
 */
function holdInterrupt(client: OpenCodeClient, sessionID: string) {
  const held = { calls: 0 };
  const interrupt = client.session.interrupt.bind(client.session);
  client.session.interrupt = ((...args: Parameters<OpenCodeClient["session"]["interrupt"]>) => {
    const [parameters, options] = args;
    if (parameters.sessionID !== sessionID) return interrupt(...args);
    held.calls += 1;
    return new Promise((_, reject) => {
      options?.signal?.addEventListener("abort", () => reject(new Error("abandoned")), {
        once: true,
      });
    });
  }) as OpenCodeClient["session"]["interrupt"];
  return held;
}

/**
 * Holds `session.active` liveness probes until they time out, except the
 * numbered calls in `answered`, which reach the transcript.
 */
function holdLivenessProbes(client: OpenCodeClient, answered: ReadonlySet<number> = new Set()) {
  const held = { calls: 0 };
  const active = client.session.active.bind(client.session);
  client.session.active = ((options?: { readonly signal?: AbortSignal }) => {
    held.calls += 1;
    if (answered.has(held.calls)) return active(options);
    return new Promise((_, reject) => {
      options?.signal?.addEventListener("abort", () => reject(new Error("abandoned")), {
        once: true,
      });
    });
  }) as OpenCodeClient["session"]["active"];
  return held;
}

/**
 * Interleaves test-injected events with the replayed stream, and can end the
 * current subscription as a clean EOF.
 */
function injectLocationTraffic(client: OpenCodeClient) {
  const injected: Array<V2Event> = [];
  let endRequested = false;
  let notify: (() => void) | null = null;
  const subscribe = client.event.subscribe.bind(client.event);
  client.event.subscribe = ((options?: { readonly signal?: AbortSignal }) => {
    const base = subscribe(options) as unknown as AsyncIterable<V2Event>;
    return {
      async *[Symbol.asyncIterator]() {
        const iterator = base[Symbol.asyncIterator]();
        let next = iterator.next();
        while (true) {
          if (endRequested) {
            endRequested = false;
            return;
          }
          const queued = injected.shift();
          if (queued !== undefined) {
            yield queued;
            continue;
          }
          const woken = new Promise<"woken">((resolve) => {
            notify = () => resolve("woken");
          });
          const winner = await Promise.race([next, woken]);
          notify = null;
          if (winner === "woken") continue;
          if (winner.done === true) return;
          yield winner.value;
          next = iterator.next();
        }
      },
    };
  }) as unknown as OpenCodeClient["event"]["subscribe"];
  return {
    inject: (event: V2Event) => {
      injected.push(event);
      notify?.();
    },
    end: () => {
      endRequested = true;
      notify?.();
    },
  };
}

const otherSessionStep = (sequence: number) =>
  ({
    id: `event_other_${sequence}`,
    created: 1_790_000_100_000 + sequence,
    type: "session.step.started",
    data: {
      sessionID: "ses_opencode2_busy_location_other",
      assistantMessageID: `message_other_${sequence}`,
    },
  }) as unknown as V2Event;

const startStallScenario = Effect.fnUntraced(function* (
  fixture: string,
  prepare: (run: Effect.Success<ReturnType<typeof openStallSession>>) => void = () => {},
) {
  const run = yield* openStallSession(fixture);
  prepare(run);
  const providerThread = yield* run.ensureThread("root");
  yield* run.startTurn("root", providerThread, run.prompts[0] ?? "");
  const terminals = () => run.terminals.get(providerThread.id) ?? [];
  const settleAndDrain = Effect.gen(function* () {
    yield* run.advanceUntil("the turn to settle", () => terminals().length > 0);
    yield* run.advanceUntil("the transcript to drain", () => run.controller.isDrained());
    return terminals();
  });
  return {
    ...run,
    providerThread,
    threadId: ThreadId.make(`opencode-stall-${fixture}-root`),
    settleAndDrain,
  };
});

it.layer(TestLayer)("OpenCode 2 stall watchdog", (it) => {
  for (const fixture of [
    "opencode2_busy_silent_model",
    "opencode2_retry_silent_model",
    "opencode2_retry_missing_finish_resubscribe",
    "opencode2_busy_output_during_probe",
    "opencode2_quiet_probe_idle_race",
  ]) {
    it.effect(`keeps a working native session's turn through quiet windows: ${fixture}`, () =>
      Effect.gen(function* () {
        const run = yield* startStallScenario(fixture);
        const terminals = yield* run.settleAndDrain;
        assert.deepEqual(terminals, [{ status: "completed", failure: null }]);
        run.controller.assertComplete();
      }),
    );
  }

  it.effect("reconciles a missed execution terminal from the idle session outcome", () =>
    Effect.gen(function* () {
      const run = yield* startStallScenario("opencode2_missed_terminal_reconcile");
      const terminals = yield* run.settleAndDrain;
      assert.deepEqual(terminals, [{ status: "completed", failure: null }]);
      run.controller.assertComplete();
    }),
  );

  for (const fixture of [
    "opencode2_missed_terminal_stale_outcome",
    "opencode2_missed_terminal_older_outcome",
  ]) {
    it.effect(`does not settle a turn from an idle outcome it does not own: ${fixture}`, () =>
      Effect.gen(function* () {
        let interrupts: Array<number> = [];
        const run = yield* startStallScenario(fixture, (opened) => {
          interrupts = watchInterrupts(opened.client, opened.terminalCount);
        });
        const terminals = yield* run.settleAndDrain;
        assert.lengthOf(terminals, 1);
        assert.equal(terminals[0]?.status, "failed");
        assert.equal(terminals[0]?.failure?.code, "event.stream.stall");
        assert.deepEqual(interrupts, [0], "native work stops before the failure is published");
        run.controller.assertComplete();
      }),
    );
  }

  it.effect("ignores a reconcile answer that raced with the terminal and a follow-up turn", () =>
    Effect.gen(function* () {
      const run = yield* openStallSession("opencode2_missed_terminal_race");
      const thread = yield* run.ensureThread("root");
      const settled = () => run.terminals.get(thread.id)?.length ?? 0;
      yield* run.startTurn("root", thread, run.prompts[0] ?? "", 1);
      yield* run.advanceUntil("the first turn to settle", () => settled() >= 1);
      yield* run.startTurn("root", thread, run.prompts[1] ?? "", 2);
      yield* run.advanceUntil("the follow-up turn to settle", () => settled() >= 2);
      yield* run.advanceUntil("the transcript to drain", () => run.controller.isDrained());
      assert.deepEqual(run.terminals.get(thread.id), [
        { status: "completed", failure: null },
        { status: "completed", failure: null },
      ]);
      assert.isTrue(
        run.assistantTexts.some((text) => text.includes("part two")),
        "the follow-up turn keeps its execution until its own terminal",
      );
      run.controller.assertComplete();
    }),
  );

  it.effect("recovers a lost terminal while other sessions keep the stream busy", () =>
    Effect.gen(function* () {
      let stream: ReturnType<typeof injectLocationTraffic> | undefined;
      const run = yield* openStallSession("opencode2_missed_terminal_busy_location", (client) => {
        stream = injectLocationTraffic(client);
      });
      const thread = yield* run.ensureThread("root");
      yield* run.startTurn("root", thread, run.prompts[0] ?? "");
      let sequence = 0;
      yield* run.advanceUntil("the turn to settle", () => {
        sequence += 1;
        stream?.inject(otherSessionStep(sequence));
        return (run.terminals.get(thread.id)?.length ?? 0) > 0;
      });
      yield* run.advanceUntil("the transcript to drain", () => run.controller.isDrained());
      assert.deepEqual(run.terminals.get(thread.id), [{ status: "completed", failure: null }]);
      run.controller.assertComplete();
    }),
  );

  for (const confirmed of [true, false]) {
    const fixture = confirmed
      ? "opencode2_stall_native_abort"
      : "opencode2_stall_native_abort_unconfirmed";
    it.effect(
      `stops owned native work after an unrecoverable stall (interrupt confirmed=${confirmed})`,
      () =>
        Effect.gen(function* () {
          const run = yield* startStallScenario(fixture);
          const terminals = yield* run.settleAndDrain;
          assert.lengthOf(terminals, 1);
          assert.equal(terminals[0]?.status, "failed");
          assert.equal(terminals[0]?.failure?.code, "event.stream.stall");
          assert.equal(terminals[0]?.failure?.class, "transport_error");
          // The strict transcript also proves the sibling session was left alone.
          run.controller.assertComplete();
          if (confirmed) return;
          const reuseError = yield* run.session
            .resumeThread({
              providerThread: run.providerThread,
              threadId: run.threadId,
              modelSelection: run.modelSelection,
              runtimePolicy: run.runtimePolicy,
            })
            .pipe(Effect.flip);
          assert.equal(reuseError._tag, "ProviderAdapterResumeThreadError");
          assert.isTrue(isProtocolError(reuseError.cause));
          if (isProtocolError(reuseError.cause)) {
            assert.include(reuseError.cause.detail, "quarantined");
          }
        }),
    );
  }

  it.effect("fails only the idle session's turn when another session is still running", () =>
    Effect.gen(function* () {
      const run = yield* openStallSession("opencode2_stall_mixed_sessions");
      const running = yield* run.ensureThread("running");
      const idle = yield* run.ensureThread("idle");
      yield* run.startTurn("running", running, run.prompts[0] ?? "");
      yield* run.startTurn("idle", idle, run.prompts[1] ?? "");
      yield* run.advanceUntil("both turns to settle", () =>
        [running.id, idle.id].every((id) => (run.terminals.get(id)?.length ?? 0) > 0),
      );
      yield* run.advanceUntil("the transcript to drain", () => run.controller.isDrained());
      assert.deepEqual(run.terminals.get(running.id), [{ status: "completed", failure: null }]);
      const idleTerminals = run.terminals.get(idle.id) ?? [];
      assert.lengthOf(idleTerminals, 1);
      assert.equal(idleTerminals[0]?.status, "failed");
      assert.equal(idleTerminals[0]?.failure?.code, "event.stream.stall");
      run.controller.assertComplete();
    }),
  );

  it.effect("removes an owned running shell when OpenCode stops answering", () =>
    Effect.gen(function* () {
      const run = yield* openStallSession("opencode2_stall_native_abort_shells");
      const shell = yield* run.ensureThread("shell");
      const quiet = yield* run.ensureThread("quiet");
      yield* run.startTurn("shell", shell, run.prompts[0] ?? "");
      yield* run.startTurn("quiet", quiet, run.prompts[1] ?? "");
      yield* run.advanceUntil("both turns to fail", () => run.terminalCount() >= 2);
      yield* run.advanceUntil("native cleanup", () => run.controller.isDrained());
      for (const thread of [shell, quiet]) {
        const settled = run.terminals.get(thread.id) ?? [];
        assert.lengthOf(settled, 1);
        assert.equal(settled[0]?.status, "failed");
        assert.equal(settled[0]?.failure?.class, "transport_error");
      }
      // The strict transcript requires the shell removal and both child listings.
      run.controller.assertComplete();
    }),
  );

  it.effect("fails a quiet turn whose probes go unanswered while other sessions stay busy", () =>
    Effect.gen(function* () {
      let stream: ReturnType<typeof injectLocationTraffic> | undefined;
      const run = yield* openStallSession(
        "opencode2_quiet_probe_unanswered_busy_location",
        (client) => {
          stream = injectLocationTraffic(client);
        },
      );
      const thread = yield* run.ensureThread("root");
      yield* run.startTurn("root", thread, run.prompts[0] ?? "");
      let sequence = 0;
      yield* run.advanceUntil("the turn to fail", () => {
        sequence += 1;
        stream?.inject(otherSessionStep(sequence));
        return (run.terminals.get(thread.id)?.length ?? 0) > 0;
      });
      yield* run.advanceUntil("native cleanup", () => run.controller.isDrained());
      const settled = run.terminals.get(thread.id) ?? [];
      assert.lengthOf(settled, 1);
      assert.equal(settled[0]?.status, "failed");
      assert.equal(settled[0]?.failure?.code, "event.stream.stall");
      // The strict transcript proves only the owned session was stopped.
      run.controller.assertComplete();
    }),
  );

  it.effect("fails a quiet turn while session metadata keeps arriving for it", () =>
    Effect.gen(function* () {
      const fixture = "opencode2_quiet_metadata_traffic";
      let stream: ReturnType<typeof injectLocationTraffic> | undefined;
      const run = yield* openStallSession(fixture, (client) => {
        stream = injectLocationTraffic(client);
      });
      const thread = yield* run.ensureThread("root");
      yield* run.startTurn("root", thread, run.prompts[0] ?? "");
      let second = 0;
      yield* run.advanceUntil("the turn to fail", () => {
        second += 1;
        if (second % 10 === 0) {
          stream?.inject({
            id: `event_viewed_${second}`,
            created: 1_790_000_100_000 + second,
            type: "session.viewed",
            data: { sessionID: `ses_${fixture}` },
          } as unknown as V2Event);
        }
        return (run.terminals.get(thread.id)?.length ?? 0) > 0;
      });
      yield* run.advanceUntil("native cleanup", () => run.controller.isDrained());
      const settled = run.terminals.get(thread.id) ?? [];
      assert.lengthOf(settled, 1);
      assert.equal(settled[0]?.status, "failed");
      assert.equal(settled[0]?.failure?.code, "event.stream.stall");
      run.controller.assertComplete();
    }),
  );

  it.effect("leaves a follow-up turn alone while another quiet session is still checked", () =>
    Effect.gen(function* () {
      const run = yield* openStallSession("opencode2_quiet_exhausted_follow_up");
      const first = yield* run.ensureThread("a");
      const second = yield* run.ensureThread("b");
      yield* run.startTurn("a", first, run.prompts[0] ?? "");
      yield* run.startTurn("b", second, run.prompts[1] ?? "");
      const settled = (id: string) => run.terminals.get(id)?.length ?? 0;
      yield* run.advanceUntil("the first session's real terminal", () => settled(first.id) >= 1);
      yield* run.startTurn("a", first, run.prompts[2] ?? "", 2);
      yield* run.advanceUntil(
        "the follow-up and the second session to settle",
        () => settled(first.id) >= 2 && settled(second.id) >= 1,
      );
      yield* run.advanceUntil("the transcript to drain", () => run.controller.isDrained());
      assert.deepEqual(run.terminals.get(first.id), [
        { status: "completed", failure: null },
        { status: "completed", failure: null },
      ]);
      const secondTerminals = run.terminals.get(second.id) ?? [];
      assert.lengthOf(secondTerminals, 1);
      assert.equal(secondTerminals[0]?.status, "failed");
      assert.equal(secondTerminals[0]?.failure?.code, "event.stream.stall");
      assert.isTrue(run.assistantTexts.some((text) => text.includes("follow-up done")));
      run.controller.assertComplete();
    }),
  );

  for (const nativeTerminal of [true, false]) {
    const fixture = nativeTerminal
      ? "opencode2_quiet_cleanup_holds_session"
      : "opencode2_quiet_cleanup_holds_session_unconfirmed";
    it.effect(
      `keeps a quiet turn's session until its cleanup ends (native terminal=${nativeTerminal})`,
      () =>
        Effect.gen(function* () {
          const sessionID = `ses_${fixture}`;
          let stream: ReturnType<typeof injectLocationTraffic> | undefined;
          let held = { calls: 0 };
          const run = yield* openStallSession(fixture, (client) => {
            stream = injectLocationTraffic(client);
            held = holdInterrupt(client, sessionID);
          });
          const thread = yield* run.ensureThread("root");
          yield* run.startTurn("root", thread, run.prompts[0] ?? "");
          const settled = () => run.terminals.get(thread.id) ?? [];
          yield* run.advanceUntil("cleanup to interrupt the session", () => held.calls > 0);
          if (nativeTerminal) {
            stream?.inject({
              id: "event_late_interrupted",
              created: 1_790_000_200_000,
              type: "session.execution.interrupted",
              data: { sessionID },
            } as unknown as V2Event);
            // Another client switches the session while cleanup holds it.
            stream?.inject({
              id: "event_agent_selected_during_cleanup",
              created: 1_790_000_200_010,
              type: "session.agent.selected",
              data: { sessionID, agent: "plan" },
              durable: { aggregateID: sessionID, seq: 100, version: 1 },
            } as unknown as V2Event);
            stream?.inject({
              id: "event_model_selected_during_cleanup",
              created: 1_790_000_200_020,
              type: "session.model.selected",
              data: { sessionID, model: { id: "expensive", providerID: "other" } },
              durable: { aggregateID: sessionID, seq: 101, version: 1 },
            } as unknown as V2Event);
            for (let tick = 0; tick < 8; tick += 1) yield* flush;
            assert.deepEqual(settled(), [], "the native terminal does not settle the turn");
          }
          const providerTurnId = run.turnIds.get(thread.id);
          assert.isDefined(providerTurnId);
          const stop = yield* run.session
            .interruptTurn({ providerThread: thread, providerTurnId: providerTurnId! })
            .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));
          const steer = yield* run.session
            .steerTurn({
              threadId: ThreadId.make(`opencode-stall-${fixture}-root`),
              runId: RunId.make("stall-run-root-1"),
              providerThread: thread,
              providerTurnId: providerTurnId!,
              message: {
                messageId: MessageId.make("stall-message-steer"),
                text: "Steer the stopped turn",
                attachments: [],
                createdBy: "user",
                creationSource: "web",
              },
            })
            .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));
          const retry = yield* run
            .startTurn("root", thread, run.prompts[1] ?? "Retry after cleanup", 2)
            .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));
          // Resuming a confirmed session would read it back; the transcript
          // covers that only when the session ends up quarantined.
          const resume = nativeTerminal
            ? undefined
            : yield* run.session
                .resumeThread({
                  providerThread: thread,
                  threadId: ThreadId.make(`opencode-stall-${fixture}-root`),
                  modelSelection: run.modelSelection,
                  runtimePolicy: run.runtimePolicy,
                })
                .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));
          stream?.end();
          for (let tick = 0; tick < 8; tick += 1) yield* flush;
          assert.deepEqual(settled(), [], "no terminal releases the session during cleanup");
          assert.isUndefined(stop.pollUnsafe(), "Stop waits for cleanup");
          assert.isUndefined(steer.pollUnsafe(), "steering waits for cleanup");
          assert.isUndefined(retry.pollUnsafe(), "the retry waits for cleanup");
          assert.isUndefined(resume?.pollUnsafe(), "resume waits for cleanup");

          yield* run.advanceUntil("cleanup to fail the quiet turn", () => settled().length > 0);
          assert.equal(settled()[0]?.status, "failed");
          assert.equal(settled()[0]?.failure?.code, "event.stream.stall");
          assert.isTrue(Exit.isSuccess(yield* Fiber.join(stop)), "Stop returns once cleanup ends");
          assert.isTrue(
            Exit.isFailure(yield* Fiber.join(steer)),
            "the failed turn no longer accepts steering",
          );
          if (resume !== undefined) {
            const resumed = yield* Fiber.join(resume);
            assert.isTrue(Exit.isFailure(resumed));
            if (Exit.isFailure(resumed)) {
              const error = Cause.squash(resumed.cause) as {
                readonly _tag: string;
                cause?: unknown;
              };
              assert.equal(error._tag, "ProviderAdapterResumeThreadError");
              assert.isTrue(isProtocolError(error.cause));
              if (isProtocolError(error.cause)) assert.include(error.cause.detail, "quarantined");
            }
          }
          const retried = yield* Fiber.join(retry);
          if (nativeTerminal) {
            assert.isTrue(Exit.isSuccess(retried));
            yield* run.advanceUntil("the retry to complete", () => settled().length > 1);
            assert.deepEqual(settled()[1], { status: "completed", failure: null });
          } else {
            assert.isTrue(Exit.isFailure(retried));
            if (Exit.isFailure(retried)) {
              const error = Cause.squash(retried.cause) as {
                readonly _tag: string;
                cause?: unknown;
              };
              assert.equal(error._tag, "ProviderAdapterTurnStartError");
              assert.isTrue(isProtocolError(error.cause));
              if (isProtocolError(error.cause)) assert.include(error.cause.detail, "quarantined");
            }
          }
          yield* run.advanceUntil("the transcript to drain", () => run.controller.isDrained());
          assert.equal(held.calls, 1);
          // The strict transcript requires the reopened stream and the child's cleanup.
          run.controller.assertComplete();
        }),
    );
  }

  it.effect("keeps a quiet turn whose execution reports usage during an unanswered probe", () =>
    Effect.gen(function* () {
      let stream: ReturnType<typeof injectLocationTraffic> | undefined;
      const run = yield* openStallSession(
        "opencode2_quiet_probe_unanswered_usage_progress",
        (client) => {
          stream = injectLocationTraffic(client);
        },
      );
      const thread = yield* run.ensureThread("root");
      yield* run.startTurn("root", thread, run.prompts[0] ?? "");
      let sequence = 0;
      yield* run.advanceUntil("the turn to settle", () => {
        sequence += 1;
        stream?.inject(otherSessionStep(sequence));
        return (run.terminals.get(thread.id)?.length ?? 0) > 0;
      });
      yield* run.advanceUntil("the transcript to drain", () => run.controller.isDrained());
      assert.deepEqual(run.terminals.get(thread.id), [{ status: "completed", failure: null }]);
      run.controller.assertComplete();
    }),
  );

  it.effect("spends the shared budget when timed-out probes miss their stall checks", () =>
    Effect.gen(function* () {
      let held = { calls: 0 };
      const run = yield* openStallSession(
        "opencode2_stall_probe_timeout_reconnect_only",
        (client) => {
          held = holdLivenessProbes(client);
        },
      );
      const shell = yield* run.ensureThread("shell");
      const quiet = yield* run.ensureThread("quiet");
      yield* run.startTurn("shell", shell, run.prompts[0] ?? "");
      yield* run.startTurn("quiet", quiet, run.prompts[1] ?? "");
      yield* run.advanceUntil("both turns to fail", () => run.terminalCount() >= 2);
      yield* run.advanceUntil("native cleanup", () => run.controller.isDrained());
      // Only the shared transport failure also fails the session whose shell
      // explains its quiet.
      for (const thread of [shell, quiet]) {
        const settled = run.terminals.get(thread.id) ?? [];
        assert.lengthOf(settled, 1);
        assert.equal(settled[0]?.status, "failed");
        assert.equal(settled[0]?.failure?.code, "event.stream.stall");
      }
      assert.isAtLeast(held.calls, 2);
      run.controller.assertComplete();
    }),
  );

  it.effect("restarts the shared budget when a probe is answered between timeouts", () =>
    Effect.gen(function* () {
      let held = { calls: 0 };
      const run = yield* openStallSession("opencode2_stall_probe_recovers_budget", (client) => {
        held = holdLivenessProbes(client, new Set([3]));
      });
      const shell = yield* run.ensureThread("shell");
      const quiet = yield* run.ensureThread("quiet");
      yield* run.startTurn("shell", shell, run.prompts[0] ?? "");
      yield* run.startTurn("quiet", quiet, run.prompts[1] ?? "");
      yield* run.advanceUntil("both turns to settle", () => run.terminalCount() >= 2);
      yield* run.advanceUntil("the transcript to drain", () => run.controller.isDrained());
      for (const thread of [shell, quiet]) {
        assert.deepEqual(run.terminals.get(thread.id), [{ status: "completed", failure: null }]);
      }
      assert.equal(held.calls, 4);
      run.controller.assertComplete();
    }),
  );
});
