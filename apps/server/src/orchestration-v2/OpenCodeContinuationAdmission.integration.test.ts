// OpenCode 2 continuation offers and prompt admission correlation, driven
// through the real adapter (fake 2.0.15 client) and, for offers, the real
// continuation worker. Cancelled queued continuations also run through the
// real orchestrator.
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  NodeId,
  OpenCodeSettings,
  OrchestrationV2AppThread,
  type OrchestrationV2ThreadProjection,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  RunAttemptId,
  RunId,
  ThreadId,
  type ModelSelection,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import type { ServerConfig } from "../config.ts";
import type { OpenCode2Runtime } from "../provider/opencode2Runtime.ts";
import { makeOpenCodeAdapterV2 } from "./Adapters/OpenCodeAdapterV2.ts";
import { OrchestrationEffectWorkerV2 } from "./EffectWorker.ts";
import { layer as idAllocatorLayer, IdAllocatorV2 } from "./IdAllocator.ts";
import { OrchestratorV2 } from "./Orchestrator.ts";
import {
  type ProviderAdapterV2Event,
  ProviderAdapterV2RuntimePolicy,
  type ProviderAdapterV2Shape,
} from "./ProviderAdapter.ts";
import { makeSingleLayer } from "./ProviderAdapterRegistry.ts";
import {
  ProviderContinuationRequests,
  layer as continuationRequestsLayer,
} from "./ProviderContinuationRequests.ts";
import { workerLive } from "./ProviderContinuationService.ts";
import { make as makeInteractionModeReflections } from "./ProviderInteractionModeReflections.ts";
import { ThreadManagementService } from "./ThreadManagementService.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "./testkit/ReplayFixtureWorkspace.ts";

const SETTINGS = Schema.decodeSync(OpenCodeSettings)({
  serverUrl: "http://test.invalid",
  serverPassword: "secret",
});
const instanceId = ProviderInstanceId.make("opencode-continuation-admission");
const selection: ModelSelection = { instanceId, model: "anthropic/claude-sonnet", options: [] };
const SESSION_ID = "root";

/** Push-driven native event stream with the released `AsyncIterable` shape. */
function eventStream() {
  const pending: Array<unknown> = [];
  const waiters: Array<(value: IteratorResult<unknown>) => void> = [];
  return {
    emit(event: unknown) {
      const waiter = waiters.shift();
      if (waiter === undefined) pending.push(event);
      else waiter({ value: event, done: false });
    },
    iterable: {
      [Symbol.asyncIterator]: () => ({
        next: () =>
          pending.length > 0
            ? Promise.resolve({ value: pending.shift(), done: false })
            : new Promise<IteratorResult<unknown>>((resolve) => waiters.push(resolve)),
      }),
    },
  };
}

/** Released 2.0.15 wire events for the root session. */
const wire = {
  userAdmitted: (inboxID: string) => ({
    type: "session.inbox.enqueued",
    data: {
      sessionID: SESSION_ID,
      inboxID,
      item: { type: "user", payload: { text: inboxID }, delivery: "steer" },
    },
  }),
  wakeAdmitted: (inboxID: string) => ({
    type: "session.inbox.enqueued",
    data: {
      sessionID: SESSION_ID,
      inboxID,
      item: {
        type: "synthetic",
        payload: {
          text: '<subagent state="completed">CHILD_DONE</subagent>',
          description: "background child",
        },
        delivery: "queue",
      },
    },
  }),
  executionStarted: () => ({ type: "session.execution.started", data: { sessionID: SESSION_ID } }),
  executionSucceeded: () => ({
    type: "session.execution.succeeded",
    data: { sessionID: SESSION_ID },
  }),
  text: (assistantMessageID: string, text: string) => [
    {
      type: "session.text.started",
      data: { sessionID: SESSION_ID, assistantMessageID, ordinal: 0 },
    },
    {
      type: "session.text.delta",
      data: { sessionID: SESSION_ID, assistantMessageID, ordinal: 0, delta: text },
    },
    {
      type: "session.text.ended",
      data: { sessionID: SESSION_ID, assistantMessageID, ordinal: 0, text },
    },
  ],
};

/**
 * Released 2.0.15 client shapes. `session.prompt` returns what the promise
 * client returns: the admitted inbox item with a top-level `id`, because the
 * client unwraps the HTTP `data` envelope.
 */
function makeClient(cwd: string) {
  const events = eventStream();
  const location = { directory: cwd };
  let prompts = 0;
  const client = {
    agent: {
      list: async () => ({
        location,
        data: [
          { id: "build", mode: "primary" },
          { id: "plan", mode: "primary" },
        ],
      }),
    },
    model: { list: async () => ({ location, data: [] }) },
    mcp: { list: async () => ({ location, data: [] }) },
    event: { subscribe: () => events.iterable },
    session: {
      create: async () => ({
        id: SESSION_ID,
        projectID: "global",
        time: { created: 1, updated: 1 },
        location,
      }),
      prompt: async (input: { readonly text: string }) => {
        prompts++;
        return {
          id: `input-${prompts}`,
          sessionID: SESSION_ID,
          time: { created: prompts },
          type: "user",
          payload: { text: input.text },
          delivery: "steer",
        };
      },
      interrupt: async () => ({ interrupted: false }),
      switchModel: async () => {},
      switchAgent: async () => {},
      list: async () => ({ data: [], cursor: {} }),
      inbox: { list: async () => [] },
    },
    shell: { list: async () => ({ location, data: [] }) },
  };
  return { client, events };
}

type ContinuationRequestsService = Parameters<
  typeof makeOpenCodeAdapterV2
>[0]["continuationRequests"];

const makeAdapter = (
  cwd: string,
  client: ReturnType<typeof makeClient>["client"],
  continuationRequests?: ContinuationRequestsService,
) =>
  Effect.gen(function* () {
    return makeOpenCodeAdapterV2({
      interactionModeReflections: yield* makeInteractionModeReflections,
      instanceId,
      settings: SETTINGS,
      environment: {},
      runtime: {
        connectToOpenCodeServer: () =>
          Effect.succeed({
            url: "http://test.invalid",
            password: "secret",
            exitCode: null,
            external: true,
          }),
        createOpenCodeSdkClient: () => client,
      } as unknown as OpenCode2Runtime["Service"],
      idAllocator: yield* IdAllocatorV2,
      serverConfig: { cwd, attachmentsDir: "/tmp/attachments" } as ServerConfig["Service"],
      ...(continuationRequests === undefined ? {} : { continuationRequests }),
    });
  }).pipe(Effect.provide(Layer.merge(idAllocatorLayer, NodeServices.layer)));

const openRootSession = Effect.fnUntraced(function* (input: {
  readonly label: string;
  readonly continuationRequests?: ContinuationRequestsService;
}) {
  const cwd = yield* checkpointWorkspace(`opencode-continuation-admission-${input.label}`);
  const { client, events } = makeClient(cwd);
  const adapter = yield* makeAdapter(cwd, client, input.continuationRequests);
  const threadId = ThreadId.make(`thread:${input.label}`);
  const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
    runtimeMode: "full-access",
    interactionMode: "default",
    cwd,
  });
  const session = yield* adapter.openSession({
    threadId,
    providerSessionId: ProviderSessionId.make(`session:${input.label}`),
    modelSelection: selection,
    runtimePolicy,
  });
  const providerThread = yield* session.ensureThread({
    threadId,
    modelSelection: selection,
    runtimePolicy,
  });
  const now = yield* DateTime.now;
  const appThread = OrchestrationV2AppThread.make({
    id: threadId,
    projectId: ProjectId.make(`project:${input.label}`),
    title: input.label,
    providerInstanceId: instanceId,
    modelSelection: selection,
    runtimeMode: "full-access",
    interactionMode: "default",
    createdBy: "user",
    creationSource: "web",
    branch: null,
    worktreePath: cwd,
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
  });
  const turnInput = (
    ordinal: number,
    source: "user" | "continuation" = "user",
  ): Parameters<typeof session.startTurn>[0] => ({
    appThread,
    threadId,
    runId: RunId.make(`run:${input.label}:${ordinal}`),
    runOrdinal: ordinal,
    providerTurnOrdinal: ordinal,
    attemptId: RunAttemptId.make(`attempt:${input.label}:${ordinal}`),
    rootNodeId: NodeId.make(`node:${input.label}:${ordinal}`),
    providerThread,
    message: {
      messageId: MessageId.make(`message:${input.label}:${ordinal}`),
      text: `turn ${ordinal}`,
      attachments: [],
      ...(source === "user"
        ? { createdBy: "user" as const, creationSource: "web" as const }
        : { createdBy: "agent" as const, creationSource: "provider" as const }),
    },
    modelSelection: selection,
    runtimePolicy,
  });
  // The adapter's event stream is a single queue, so one consumer records
  // every event and the helpers below read the record.
  const collected: Array<ProviderAdapterV2Event> = [];
  yield* session.events.pipe(
    Stream.runForEach((event) => Effect.sync(() => collected.push(event))),
    Effect.forkChild({ startImmediately: true }),
  );
  const awaitEvent = (
    predicate: (event: ProviderAdapterV2Event) => boolean,
    timeoutMs: number,
    from = 0,
  ) =>
    Effect.gen(function* () {
      for (let waited = 0; waited < timeoutMs; waited += 10) {
        const found = collected.slice(from).find(predicate);
        if (found !== undefined) return found;
        yield* Effect.sleep("10 millis");
      }
      return collected.slice(from).find(predicate);
    });
  const awaitTerminal = (ordinal: number, timeoutMs: number) =>
    awaitEvent(
      (event) => event.type === "turn.terminal" && event.runOrdinal === ordinal,
      timeoutMs,
    );
  /** Runs one ordinary turn to completion with the released event order. */
  const completeTurn = Effect.fnUntraced(function* (ordinal: number) {
    yield* session.startTurn(turnInput(ordinal));
    events.emit(wire.userAdmitted(`input-${ordinal}`));
    events.emit(wire.executionStarted());
    for (const event of wire.text(`message-${ordinal}`, `REPLY_${ordinal}`)) events.emit(event);
    events.emit(wire.executionSucceeded());
    const terminal = yield* awaitTerminal(ordinal, 5_000);
    assert.equal(
      terminal?.type === "turn.terminal" ? terminal.status : undefined,
      "completed",
      `turn ${ordinal} settles`,
    );
  });
  const hasBufferedOutput = session.hasBufferedOutputForThread!(providerThread);
  const releaseCancelledContinuation = session.releaseCancelledContinuation!(providerThread);
  return {
    session,
    events,
    turnInput,
    collected,
    awaitEvent,
    awaitTerminal,
    completeTurn,
    hasBufferedOutput,
    releaseCancelledContinuation,
  };
});

/**
 * The real continuation worker, with thread management reduced to a live
 * thread whose read or dispatch fails or succeeds as the test chooses.
 */
const continuationWorker = (dispatch: {
  readonly attempts: Ref.Ref<number>;
  readonly attempted: Deferred.Deferred<void>;
  readonly fail: boolean;
  readonly failRead?: Deferred.Deferred<void>;
  /** Decides per 1-based attempt whether dispatch fails, overriding `fail`. */
  readonly failAttempt?: (attempt: number) => Effect.Effect<boolean>;
}) =>
  workerLive.pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        idAllocatorLayer,
        continuationRequestsLayer,
        Layer.mock(ThreadManagementService)({
          getThreadRecords: () =>
            dispatch.failRead === undefined
              ? Effect.succeed({
                  thread: { archivedAt: null, deletedAt: null },
                  messages: [],
                } as unknown as OrchestrationV2ThreadProjection)
              : Deferred.succeed(dispatch.failRead, undefined).pipe(
                  Effect.andThen(Effect.fail(new Error("thread read failed") as never)),
                ),
          dispatch: () =>
            Ref.updateAndGet(dispatch.attempts, (value) => value + 1).pipe(
              Effect.tap(() => Deferred.succeed(dispatch.attempted, undefined)),
              Effect.flatMap(
                (attempt) => dispatch.failAttempt?.(attempt) ?? Effect.succeed(dispatch.fail),
              ),
              Effect.flatMap((fail) =>
                fail
                  ? Effect.fail(new Error("continuation dispatch failed") as never)
                  : Effect.succeed({} as never),
              ),
            ),
        }),
      ),
    ),
  );

it.live("OpenCode 2 releases a continuation offer whose dispatch fails", () =>
  Effect.gen(function* () {
    const attempts = yield* Ref.make(0);
    const attempted = yield* Deferred.make<void>();
    yield* Effect.gen(function* () {
      const root = yield* openRootSession({
        label: "failed-dispatch",
        continuationRequests: yield* ProviderContinuationRequests,
      });
      yield* root.completeTurn(1);
      assert.isFalse(yield* root.hasBufferedOutput);
      root.events.emit(wire.wakeAdmitted("wake-1"));
      yield* Deferred.await(attempted).pipe(Effect.timeout("5 seconds"));
      yield* Effect.sleep("100 millis");
      assert.equal(yield* Ref.get(attempts), 1, "the failed continuation is not re-offered");
      assert.isFalse(
        yield* root.hasBufferedOutput,
        "a failed dispatch releases its offer, so later admission is not refused",
      );
    }).pipe(Effect.provide(continuationWorker({ attempts, attempted, fail: true })), Effect.scoped);
  }),
);

it.live("OpenCode 2 releases a continuation offer dropped before dispatch", () =>
  Effect.gen(function* () {
    const attempts = yield* Ref.make(0);
    const attempted = yield* Deferred.make<void>();
    const readFailed = yield* Deferred.make<void>();
    yield* Effect.gen(function* () {
      const root = yield* openRootSession({
        label: "failed-read",
        continuationRequests: yield* ProviderContinuationRequests,
      });
      yield* root.completeTurn(1);
      root.events.emit(wire.wakeAdmitted("wake-1"));
      yield* Deferred.await(readFailed).pipe(Effect.timeout("5 seconds"));
      yield* Effect.sleep("100 millis");
      assert.equal(yield* Ref.get(attempts), 0, "the worker drops the request before dispatch");
      assert.isFalse(
        yield* root.hasBufferedOutput,
        "a request dropped before dispatch releases its offer, so later admission is not refused",
      );
    }).pipe(
      Effect.provide(
        continuationWorker({ attempts, attempted, fail: false, failRead: readFailed }),
      ),
      Effect.scoped,
    );
  }),
);

it.live("OpenCode 2 keeps a sibling offer counted when another offer's dispatch fails", () =>
  Effect.gen(function* () {
    const attempts = yield* Ref.make(0);
    const attempted = yield* Deferred.make<void>();
    const offers = yield* Ref.make(0);
    const bothOffered = yield* Deferred.make<void>();
    const secondDispatched = yield* Deferred.make<void>();
    yield* Effect.gen(function* () {
      const requests = yield* ProviderContinuationRequests;
      const root = yield* openRootSession({
        label: "sibling-offer",
        continuationRequests: {
          ...requests,
          offer: (request) =>
            requests.offer(request).pipe(
              Effect.andThen(Ref.updateAndGet(offers, (value) => value + 1)),
              Effect.flatMap((count) =>
                count === 2 ? Deferred.succeed(bothOffered, undefined) : Effect.void,
              ),
            ),
        },
      });
      yield* root.completeTurn(1);
      root.events.emit(wire.wakeAdmitted("wake-1"));
      root.events.emit(wire.wakeAdmitted("wake-2"));
      yield* Deferred.await(secondDispatched).pipe(Effect.timeout("5 seconds"));
      yield* Effect.sleep("100 millis");
      assert.equal(yield* Ref.get(attempts), 2, "each offer is dispatched once");
      assert.isTrue(
        yield* root.hasBufferedOutput,
        "the failed dispatch releases only its own offer, so later turns stay refused",
      );
      yield* root.session.startTurn(root.turnInput(2, "continuation"));
      assert.isFalse(
        yield* root.hasBufferedOutput,
        "delivering the sibling continuation consumes the last offer",
      );
    }).pipe(
      Effect.provide(
        continuationWorker({
          attempts,
          attempted,
          fail: false,
          // The first dispatch fails only once both offers are counted, and
          // the second succeeds.
          failAttempt: (attempt) =>
            attempt === 1
              ? Deferred.await(bothOffered).pipe(Effect.as(true))
              : Deferred.succeed(secondDispatched, undefined).pipe(Effect.as(false)),
        }),
      ),
      Effect.scoped,
    );
  }),
);

it.live("OpenCode 2 keeps a dispatched continuation offer until its turn starts", () =>
  Effect.gen(function* () {
    const attempts = yield* Ref.make(0);
    const attempted = yield* Deferred.make<void>();
    yield* Effect.gen(function* () {
      const root = yield* openRootSession({
        label: "dispatched",
        continuationRequests: yield* ProviderContinuationRequests,
      });
      yield* root.completeTurn(1);
      root.events.emit(wire.wakeAdmitted("wake-1"));
      yield* Deferred.await(attempted).pipe(Effect.timeout("5 seconds"));
      yield* Effect.sleep("100 millis");
      assert.equal(yield* Ref.get(attempts), 1);
      assert.isTrue(
        yield* root.hasBufferedOutput,
        "the queued continuation still owns the buffered wake output",
      );
      yield* root.session.startTurn(root.turnInput(2, "continuation"));
      assert.isFalse(yield* root.hasBufferedOutput, "the continuation turn consumed its offer");
    }).pipe(
      Effect.provide(continuationWorker({ attempts, attempted, fail: false })),
      Effect.scoped,
    );
  }),
);

it.live("OpenCode 2 releases one dispatched offer per cancelled continuation", () =>
  Effect.gen(function* () {
    const attempts = yield* Ref.make(0);
    const attempted = yield* Deferred.make<void>();
    yield* Effect.gen(function* () {
      const root = yield* openRootSession({
        label: "cancelled-continuation",
        continuationRequests: yield* ProviderContinuationRequests,
      });
      yield* root.completeTurn(1);
      root.events.emit(wire.wakeAdmitted("wake-1"));
      root.events.emit(wire.wakeAdmitted("wake-2"));
      for (let waited = 0; waited < 5_000 && (yield* Ref.get(attempts)) < 2; waited += 10) {
        yield* Effect.sleep("10 millis");
      }
      assert.equal(yield* Ref.get(attempts), 2, "both continuations are dispatched");
      yield* root.releaseCancelledContinuation;
      assert.isTrue(
        yield* root.hasBufferedOutput,
        "cancelling one queued continuation keeps its sibling's offer",
      );
      yield* root.releaseCancelledContinuation;
      assert.isFalse(
        yield* root.hasBufferedOutput,
        "once every dispatched continuation is cancelled, later admission is not refused",
      );
      yield* root.releaseCancelledContinuation;
      assert.isFalse(yield* root.hasBufferedOutput, "an extra release cannot go negative");
    }).pipe(
      Effect.provide(continuationWorker({ attempts, attempted, fail: false })),
      Effect.scoped,
    );
  }),
);

for (const cancelBy of ["queue cancel", "archive"] as const) {
  it.live(`OpenCode 2 releases the offer of a queued continuation cancelled by ${cancelBy}`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const cwd = yield* checkpointWorkspace(
          `opencode-continuation-admission-${cancelBy.replace(" ", "-")}`,
        );
        const { client } = makeClient(cwd);
        // Admission acknowledges the selection only for a model the service lists.
        client.model.list = async () => ({
          location: { directory: cwd },
          data: [
            {
              id: "claude-sonnet",
              providerID: "anthropic",
              name: "Claude Sonnet",
              enabled: true,
              limit: { context: 200_000, output: 32_000 },
            },
          ] as never[],
        });
        const inner = yield* makeAdapter(cwd, client);
        const released: Array<string | null> = [];
        const adapter: ProviderAdapterV2Shape = {
          ...inner,
          openSession: (input) =>
            inner.openSession(input).pipe(
              Effect.map((runtime) => ({
                ...runtime,
                releaseCancelledContinuation: (providerThread) =>
                  Effect.sync(() => released.push(providerThread.id)).pipe(
                    Effect.andThen(runtime.releaseCancelledContinuation!(providerThread)),
                  ),
              })),
            ),
        };
        yield* Effect.gen(function* () {
          const orchestrator = yield* OrchestratorV2;
          const worker = yield* OrchestrationEffectWorkerV2;
          const threadId = ThreadId.make("thread:opencode-queue-cancel");
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create"),
            threadId,
            projectId: ProjectId.make("project:opencode-queue-cancel"),
            title: "OpenCode",
            modelSelection: selection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
          // The first turn is admitted and stays running, which establishes the
          // selection a provider continuation inherits and keeps it queued.
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("first"),
            threadId,
            messageId: MessageId.make("message:first"),
            text: "first",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* worker.drain();
          // The continuation worker's dispatch shape, queued behind the active run.
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("provider-continuation"),
            threadId,
            messageId: MessageId.make("message:continuation"),
            text: "Background task completed.",
            notification: {
              source: { kind: "background_task" },
              outcome: "updated",
              summary: "Background activity updated",
            },
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            createdBy: "agent",
            creationSource: "provider",
          });
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("queued-user"),
            threadId,
            messageId: MessageId.make("message:queued-user"),
            text: "later",
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            createdBy: "user",
            creationSource: "web",
          });
          const queued = yield* orchestrator.getThreadProjection(threadId);
          const runFor = (messageId: string) =>
            queued.runs.find((run) => run.userMessageId === messageId)!;
          const continuationRun = runFor("message:continuation");
          const userRun = runFor("message:queued-user");
          assert.equal(continuationRun.status, "queued");
          assert.equal(userRun.status, "queued");
          const cancelled = (runId: RunId) =>
            orchestrator.streamDomainEvents.pipe(
              Stream.filter(
                (event) =>
                  event.type === "run.updated" &&
                  event.payload.id === runId &&
                  event.payload.status === "cancelled",
              ),
              Stream.runHead,
              Effect.forkChild({ startImmediately: true }),
            );
          const userCancelled = yield* cancelled(userRun.id);
          yield* orchestrator.dispatch({
            type: "queued-run.cancel",
            commandId: CommandId.make("cancel-user"),
            threadId,
            runId: userRun.id,
          });
          assert.isTrue(Option.isSome(yield* Fiber.join(userCancelled)));
          // Archiving cancels every queued run, including the continuation.
          yield* orchestrator.dispatch(
            cancelBy === "queue cancel"
              ? {
                  type: "queued-run.cancel",
                  commandId: CommandId.make("cancel-continuation"),
                  threadId,
                  runId: continuationRun.id,
                }
              : { type: "thread.archive", commandId: CommandId.make("archive"), threadId },
          );
          for (let waited = 0; waited < 5_000 && released.length === 0; waited += 10) {
            yield* Effect.sleep("10 millis");
          }
          // Terminal runs are handled in order, so a release for the user run
          // would already be recorded; the pause catches any late duplicate.
          yield* Effect.sleep("100 millis");
          assert.deepEqual(
            released,
            [continuationRun.providerThreadId],
            "only the cancelled provider continuation releases an offer",
          );
        }).pipe(
          Effect.provide(
            makeOrchestratorV2ReplayLayerWithRegistry(
              { name: `opencode-continuation-${cancelBy.replace(" ", "-")}` },
              makeSingleLayer(adapter),
              { runEffectWorker: false },
            ),
          ),
        );
      }),
    ),
  );
}

it.live("OpenCode 2 correlates a prompt from its 2.0.15 response before admission", () =>
  Effect.gen(function* () {
    const root = yield* openRootSession({ label: "prompt-correlation" });
    yield* root.completeTurn(1);
    // A background child finished after the first turn settled; its synthetic
    // input is pending when the user sends the next prompt.
    root.events.emit(wire.wakeAdmitted("wake-1"));
    yield* Effect.sleep("50 millis");
    const before = root.collected.length;
    yield* root.session.startTurn(root.turnInput(2));
    const correlated = yield* root.awaitEvent(
      (event) =>
        event.type === "provider_turn.updated" &&
        event.providerTurn.status === "running" &&
        event.providerTurn.nativeTurnRef?.nativeId === "input-2",
      1_000,
      before,
    );
    assert.isDefined(
      correlated,
      "the prompt response id correlates the turn before admission is observed",
    );
    // The execution starts before `session.inbox.enqueued` reaches the stream.
    root.events.emit(wire.executionStarted());
    for (const event of wire.text("message-2", "USER_REPLY")) root.events.emit(event);
    root.events.emit(wire.executionSucceeded());
    root.events.emit(wire.userAdmitted("input-2"));
    const terminal = yield* root.awaitTerminal(2, 2_000);
    assert.equal(
      terminal?.type === "turn.terminal" ? terminal.status : undefined,
      "completed",
      "the user's execution settles the user's turn instead of the pending wake",
    );
    assert.isTrue(
      root.collected.some(
        (event) =>
          event.type === "turn_item.updated" && JSON.stringify(event).includes("USER_REPLY"),
      ),
      "the user's reply is projected on the visible turn",
    );
  }).pipe(Effect.scoped),
);
