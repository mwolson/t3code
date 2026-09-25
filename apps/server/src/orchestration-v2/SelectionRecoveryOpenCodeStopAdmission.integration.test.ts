// Stop winning OpenCode 2 prompt admission, driven through the real
// orchestrator, manager, RunExecutionService and effect worker (fake client).
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  NodeId,
  OpenCodeSettings,
  OrchestrationV2AppThread,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  type ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  type ModelSelection,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
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
import { makeOrchestratorV2ReplayLayerWithRegistry } from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "./testkit/ReplayFixtureWorkspace.ts";

const SETTINGS = Schema.decodeSync(OpenCodeSettings)({
  serverUrl: "http://test.invalid",
  serverPassword: "secret",
});
const instanceId = ProviderInstanceId.make("opencode-stop-admission");
const selection: ModelSelection = { instanceId, model: "anthropic/claude-sonnet", options: [] };

function gate<A = void>() {
  let resolve!: (value: A) => void;
  const promise = new Promise<A>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

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

/**
 * Released 2.0.15 client shapes for one root turn whose prompt admission stays
 * parked until the test releases it, then succeeds or fails.
 */
function makeParkedAdmission(cwd: string, reply: "success" | "failure") {
  const promptCalled = gate();
  const releasePrompt = gate();
  const events = eventStream();
  const calls: Array<string> = [];
  let executing = false;
  let prompts = 0;
  let admitting = false;
  let interruptsDuringAdmission = 0;
  const interruptCalled = gate();
  const location = { directory: cwd };
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
        id: "root",
        projectID: "global",
        time: { created: 1, updated: 1 },
        location,
      }),
      prompt: async () => {
        calls.push("session.prompt");
        prompts++;
        promptCalled.resolve();
        admitting = true;
        await releasePrompt.promise;
        admitting = false;
        // A cancelled request can still reach the service after it is sent.
        executing = true;
        if (reply === "failure" && prompts === 1) throw new Error("cancelled");
        return { id: `input-${prompts}` };
      },
      interrupt: async (input: { readonly sessionID: string }) => {
        calls.push("session.interrupt");
        if (admitting) interruptsDuringAdmission++;
        interruptCalled.resolve();
        if (!executing) return { interrupted: false };
        executing = false;
        events.emit({
          type: "session.execution.interrupted",
          data: { sessionID: input.sessionID, reason: "user" },
        });
        return { interrupted: true };
      },
      switchModel: async () => {
        calls.push("session.switchModel");
      },
      switchAgent: async () => {
        calls.push("session.switchAgent");
      },
      list: async () => ({ data: [], cursor: {} }),
      inbox: { list: async () => [] },
    },
    shell: { list: async () => ({ location, data: [] }) },
  };
  const adapter = Effect.gen(function* () {
    return makeOpenCodeAdapterV2({
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
    });
  }).pipe(Effect.provide(Layer.merge(idAllocatorLayer, NodeServices.layer)));
  return {
    adapter,
    calls,
    promptCalled,
    releasePrompt,
    interruptCalled,
    interruptsDuringAdmission: () => interruptsDuringAdmission,
  };
}

/** Opens a root session on the parked client and builds start inputs for it. */
const openRootSession = Effect.fnUntraced(function* (
  parked: ReturnType<typeof makeParkedAdmission>,
  cwd: string,
  label: string,
) {
  const adapter = yield* parked.adapter;
  const threadId = ThreadId.make(`thread:${label}`);
  const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
    runtimeMode: "full-access",
    interactionMode: "default",
    cwd,
  });
  const session = yield* adapter.openSession({
    threadId,
    providerSessionId: ProviderSessionId.make(`session:${label}`),
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
    projectId: ProjectId.make(`project:${label}`),
    title: label,
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
  const turnInput = (ordinal: number) => ({
    appThread,
    threadId,
    runId: RunId.make(`run:${label}:${ordinal}`),
    runOrdinal: ordinal,
    providerTurnOrdinal: ordinal,
    attemptId: RunAttemptId.make(`attempt:${label}:${ordinal}`),
    rootNodeId: NodeId.make(`node:${label}:${ordinal}`),
    providerThread,
    message: {
      messageId: MessageId.make(`message:${label}:${ordinal}`),
      text: `turn ${ordinal}`,
      attachments: [],
      createdBy: "user" as const,
      creationSource: "web" as const,
    },
    modelSelection: selection,
    runtimePolicy,
  });
  const firstEvent = <A>(predicate: (event: ProviderAdapterV2Event) => boolean) =>
    session.events.pipe(
      Stream.filter(predicate),
      Stream.runHead,
      Effect.map((event) => Option.getOrThrow(event) as A),
      Effect.forkChild({ startImmediately: true }),
    );
  return { session, providerThread, turnInput, firstEvent };
});

for (const reply of ["success", "failure"] as const) {
  it.effect(`OpenCode 2 cancelled admission never certifies the selection (${reply})`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const cwd = yield* checkpointWorkspace(`opencode-cancelled-admission-${reply}`);
        const parked = makeParkedAdmission(cwd, reply);
        const root = yield* openRootSession(parked, cwd, `cancelled-admission-${reply}`);
        const running = yield* root.firstEvent<{
          readonly providerTurn: { readonly id: ProviderTurnId };
        }>(
          (event) =>
            event.type === "provider_turn.updated" && event.providerTurn.status === "running",
        );
        const starting = yield* root.session
          .startTurn(root.turnInput(1))
          .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));
        yield* Effect.promise(() => parked.promptCalled.promise);
        const providerTurn = yield* Fiber.join(running);
        const interrupting = yield* root.session
          .interruptTurn({
            providerThread: root.providerThread,
            providerTurnId: providerTurn.providerTurn.id,
          })
          .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));
        parked.releasePrompt.resolve();
        const started = yield* Fiber.join(starting);
        assert.isTrue(
          Exit.isFailure(started) && Cause.hasInterruptsOnly(started.cause),
          "cancelled admission reports interruption, not success or failure",
        );
        yield* Effect.promise(() => parked.interruptCalled.promise);
        assert.equal(
          parked.interruptsDuringAdmission(),
          0,
          "the native interrupt waits for the admission it cancels",
        );
        assert.isTrue(Exit.isSuccess(yield* Fiber.join(interrupting)));
        assert.deepEqual(parked.calls, ["session.prompt", "session.interrupt"]);
        // The native interrupt targeted the admitted input, so its terminal
        // confirms the stop and the session stays reusable.
        yield* root.session.startTurn(root.turnInput(2));
        assert.deepEqual(parked.calls.slice(2), [
          "session.switchModel",
          "session.switchAgent",
          "session.prompt",
        ]);
      }),
    ),
  );
}

it.effect("OpenCode 2 background Stop only targets the latest settled root turn", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("opencode-stale-stop-target");
      const parked = makeParkedAdmission(cwd, "success");
      const root = yield* openRootSession(parked, cwd, "stale-stop-target");
      const running = yield* root.firstEvent<{
        readonly providerTurn: { readonly id: ProviderTurnId };
      }>(
        (event) =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "running",
      );
      const terminal = yield* root.firstEvent<{ readonly status: string }>(
        (event) => event.type === "turn.terminal",
      );
      parked.releasePrompt.resolve();
      yield* root.session.startTurn(root.turnInput(1));
      const providerTurn = yield* Fiber.join(running);
      yield* root.session.interruptTurn({
        providerThread: root.providerThread,
        providerTurnId: providerTurn.providerTurn.id,
      });
      assert.equal((yield* Fiber.join(terminal)).status, "interrupted");
      const callsAfterStop = [...parked.calls];
      const stale = yield* Effect.exit(
        root.session.interruptTurn({
          providerThread: root.providerThread,
          providerTurnId: "provider-turn:stale" as ProviderTurnId,
        }),
      );
      assert.isTrue(Exit.isFailure(stale), "an old Stop request cannot stop newer work");
      assert.deepEqual(parked.calls, callsAfterStop, "no native request for a stale target");
    }),
  ),
);

it.effect("OpenCode 2 stops a start that Stop cancels before its provider turn is recorded", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("opencode-abandoned-admission");
      const parked = makeParkedAdmission(cwd, "success");
      const root = yield* openRootSession(parked, cwd, "abandoned-admission");
      const terminal = yield* root.firstEvent<{ readonly status: string }>(
        (event) => event.type === "turn.terminal",
      );
      const starting = yield* root.session
        .startTurn(root.turnInput(1))
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Effect.promise(() => parked.promptCalled.promise);
      yield* Fiber.interrupt(starting);
      assert.deepEqual(parked.calls, ["session.prompt", "session.interrupt"]);
      assert.equal((yield* Fiber.join(terminal)).status, "interrupted");
      parked.releasePrompt.resolve();
      yield* root.session.startTurn(root.turnInput(2));
      assert.deepEqual(
        parked.calls,
        [
          "session.prompt",
          "session.interrupt",
          "session.switchModel",
          "session.switchAgent",
          "session.prompt",
        ],
        "the abandoned turn neither wedges the thread nor trusts its binding",
      );
    }),
  ),
);

for (const reply of ["success", "failure"] as const) {
  it.effect(`OpenCode 2 Stop during prompt admission (${reply}) through the pipeline`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const cwd = yield* checkpointWorkspace(`opencode-stop-admission-${reply}`);
        const parked = makeParkedAdmission(cwd, reply);
        const inner = yield* parked.adapter;
        // Release the parked admission only once Stop has reached the adapter,
        // so the interrupt deterministically wins the admission race.
        const adapter: ProviderAdapterV2Shape = {
          ...inner,
          openSession: (input) =>
            inner.openSession(input).pipe(
              Effect.map((runtime) => ({
                ...runtime,
                interruptTurn: (interruptInput) =>
                  Effect.gen(function* () {
                    const interrupting = yield* runtime
                      .interruptTurn(interruptInput)
                      .pipe(Effect.forkChild({ startImmediately: true }));
                    parked.releasePrompt.resolve();
                    return yield* Fiber.join(interrupting);
                  }),
              })),
            ),
        };
        yield* Effect.gen(function* () {
          const orchestrator = yield* OrchestratorV2;
          const worker = yield* OrchestrationEffectWorkerV2;
          const threadId = ThreadId.make(`thread:opencode-stop-admission:${reply}`);
          const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
            orchestrator.streamDomainEvents.pipe(
              Stream.filter(predicate),
              Stream.runHead,
              Effect.forkChild({ startImmediately: true }),
            );
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create"),
            threadId,
            projectId: ProjectId.make("project:opencode-stop-admission"),
            title: "OpenCode",
            modelSelection: selection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
          const terminal = yield* watch(
            (event) =>
              event.type === "run.updated" &&
              ["failed", "interrupted", "completed", "waiting"].includes(event.payload.status),
          );
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
          const firstDrain = yield* worker
            .drain()
            .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));
          yield* Effect.promise(() => parked.promptCalled.promise);
          const beforeStop = yield* orchestrator.getThreadProjection(threadId);
          yield* orchestrator.dispatch({
            type: "run.interrupt",
            commandId: CommandId.make("stop"),
            threadId,
            runId: beforeStop.runs[0]!.id,
          });
          const secondDrain = yield* worker
            .drain()
            .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));
          const settled = yield* Fiber.join(terminal);
          yield* Fiber.join(firstDrain);
          yield* Fiber.join(secondDrain);
          yield* worker.drain().pipe(Effect.exit);
          const after = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(
            settled._tag === "Some" && settled.value.type === "run.updated"
              ? settled.value.payload.status
              : null,
            "interrupted",
          );
          assert.equal(
            after.runs[0]?.status,
            "interrupted",
            "Stop during admission settles the run as interrupted",
          );
          assert.lengthOf(
            after.turnItems.filter((item) => item.type === "error"),
            0,
            "no provider error item for an explicit Stop",
          );
          assert.deepEqual(
            parked.calls,
            ["session.prompt", "session.interrupt"],
            "the native interrupt follows the settled admission",
          );
        }).pipe(
          Effect.provide(
            makeOrchestratorV2ReplayLayerWithRegistry(
              { name: `opencode-stop-admission-${reply}` },
              makeSingleLayer(adapter),
              { runEffectWorker: false },
            ),
          ),
        );
      }),
    ),
  );
}
