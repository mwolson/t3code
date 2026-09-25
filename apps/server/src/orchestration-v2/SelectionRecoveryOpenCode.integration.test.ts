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
  RunAttemptId,
  RunId,
  ThreadId,
  type ModelSelection,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type { ServerConfig } from "../config.ts";
import type { OpenCode2Runtime } from "../provider/opencode2Runtime.ts";
import { makeOpenCodeAdapterV2 } from "./Adapters/OpenCodeAdapterV2.ts";
import { OrchestrationEffectWorkerV2 } from "./EffectWorker.ts";
import { layer as idAllocatorLayer, IdAllocatorV2 } from "./IdAllocator.ts";
import { OrchestratorV2 } from "./Orchestrator.ts";
import { ProviderAdapterV2RuntimePolicy } from "./ProviderAdapter.ts";
import { makeSingleLayer } from "./ProviderAdapterRegistry.ts";
import { make as makeInteractionModeReflections } from "./ProviderInteractionModeReflections.ts";
import { ProviderSessionManagerV2 } from "./ProviderSessionManager.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "./testkit/ReplayFixtureWorkspace.ts";

const settings = Schema.decodeSync(OpenCodeSettings)({
  serverUrl: "http://test.invalid",
  serverPassword: "secret",
});
const instanceId = ProviderInstanceId.make("opencode-selection");
const selectionWith = (variant: string): ModelSelection => ({
  instanceId,
  model: "anthropic/claude-sonnet",
  options: [
    { id: "agent", value: "build" },
    { id: "variant", value: variant },
  ],
});
const selection = selectionWith("high");

interface WireRequest {
  readonly method: string;
  readonly body: Record<string, unknown>;
}

/** Released 2.0.15 client shapes for the calls a root turn makes. */
function makeClient(input: {
  readonly requests: Array<WireRequest>;
  readonly prompt: (attempt: number) => Promise<void>;
  readonly switchModel?: (body: Record<string, unknown>) => Promise<void>;
}) {
  const location = { directory: "/workspace" };
  let creates = 0;
  let prompts = 0;
  const record = (method: string, body: Record<string, unknown>) =>
    input.requests.push({ method, body });
  return {
    agent: {
      list: async () => ({
        location,
        data: [
          { id: "build", mode: "primary" },
          { id: "plan", mode: "primary" },
        ],
      }),
    },
    model: {
      list: async () => ({
        location,
        data: [
          {
            id: "claude-sonnet",
            providerID: "anthropic",
            name: "Claude Sonnet",
            enabled: true,
            variants: [{ id: "high" }, { id: "max" }],
            limit: { context: 200_000, output: 32_000 },
          },
        ],
      }),
    },
    mcp: { list: async () => ({ location, data: [] }) },
    event: {
      subscribe: () => ({
        [Symbol.asyncIterator]: () => ({
          next: () => new Promise<IteratorResult<unknown>>(() => {}),
        }),
      }),
    },
    session: {
      create: async (body: Record<string, unknown>) => {
        record("session.create", body);
        creates++;
        return {
          id: `root-${creates}`,
          projectID: "global",
          time: { created: 1, updated: 1 },
          location,
          ...(body.agent === undefined ? {} : { agent: body.agent }),
          ...(body.model === undefined ? {} : { model: body.model }),
        };
      },
      prompt: async (body: Record<string, unknown>) => {
        record("session.prompt", body);
        prompts++;
        await input.prompt(prompts);
        return { id: `input-${prompts}` };
      },
      switchModel: async (body: Record<string, unknown>) => {
        record("session.switchModel", body);
        await input.switchModel?.(body);
      },
      switchAgent: async (body: Record<string, unknown>) => {
        record("session.switchAgent", body);
      },
      interrupt: async () => ({ interrupted: true }),
      list: async () => ({ data: [], cursor: {} }),
      inbox: { list: async () => [] },
    },
    shell: { list: async () => ({ location, data: [] }) },
  };
}

const makeAdapter = (input: {
  readonly cwd: string;
  readonly client: ReturnType<typeof makeClient>;
  readonly onConnect: () => void;
}) =>
  Effect.gen(function* () {
    return makeOpenCodeAdapterV2({
      interactionModeReflections: yield* makeInteractionModeReflections,
      instanceId,
      settings,
      environment: {},
      runtime: {
        connectToOpenCodeServer: () =>
          Effect.sync(() => {
            input.onConnect();
            return {
              url: "http://test.invalid",
              password: "secret",
              exitCode: null,
              external: true,
            };
          }),
        createOpenCodeSdkClient: () => input.client,
      } as unknown as OpenCode2Runtime["Service"],
      idAllocator: yield* IdAllocatorV2,
      serverConfig: {
        cwd: input.cwd,
        attachmentsDir: "/tmp/attachments",
      } as ServerConfig["Service"],
    });
  }).pipe(Effect.provide(Layer.merge(idAllocatorLayer, NodeServices.layer)));

const createThread = (threadId: ThreadId, cwd: string) =>
  Effect.gen(function* () {
    const orchestrator = yield* OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("create"),
      threadId,
      projectId: ProjectId.make("project:selection"),
      title: "selection",
      modelSelection: selection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: cwd,
      createdBy: "user",
      creationSource: "web",
    });
  });

const send = (threadId: ThreadId, name: string, modelSelection: ModelSelection) =>
  Effect.gen(function* () {
    const orchestrator = yield* OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(name),
      threadId,
      messageId: MessageId.make(name),
      text: name,
      attachments: [],
      dispatchMode: { type: "start_immediately" },
      createdBy: "user",
      creationSource: "web",
      modelSelection,
    });
    yield* (yield* OrchestrationEffectWorkerV2).drain();
    return yield* orchestrator.getThreadProjection(threadId);
  });

const executionSelectionOf = (projection: {
  readonly thread: { readonly activeProviderThreadId: OrchestrationV2ProviderThread["id"] | null };
  readonly providerThreads: ReadonlyArray<OrchestrationV2ProviderThread>;
}) =>
  Effect.gen(function* () {
    const manager = yield* ProviderSessionManagerV2;
    const binding = projection.providerThreads.find(
      (thread) => thread.id === projection.thread.activeProviderThreadId,
    )!;
    const runtime = Option.getOrThrow(yield* manager.get(binding.providerSessionId!));
    return { runtime, selection: yield* runtime.executionSelection!(binding) };
  });

for (const missing of [null, "agent", "variant"] as const) {
  it.effect(`OpenCode 2 real-adapter selection recovery with omitted=${missing}`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const cwd = yield* checkpointWorkspace("opencode-selection-recovery");
        let opens = 0;
        const requests: Array<WireRequest> = [];
        const client = makeClient({
          requests,
          prompt: async (attempt) => {
            if (attempt === 1) throw new Error("fixture rejected admission");
          },
        });
        const adapter = yield* makeAdapter({ cwd, client, onConnect: () => opens++ });
        yield* Effect.gen(function* () {
          const threadId = ThreadId.make("thread:opencode-selection");
          yield* createThread(threadId, cwd);
          const failed = yield* send(threadId, "first", selection);
          assert.equal(failed.runs.at(-1)?.status, "failed");
          const before = yield* executionSelectionOf(failed);
          assert.isNull(before.selection);
          const target = {
            ...selection,
            options: selection.options!.filter((option) => option.id !== missing),
          };
          assert.equal(yield* before.runtime.reappliesFullSelection!(target), missing === null);
          assert.isFalse(
            yield* before.runtime.reappliesFullSelection!({
              ...selection,
              options: [...selection.options!, { id: "fastMode", value: true }],
            }),
            "an option the next request cannot carry blocks in-place recovery",
          );
          const requestsBeforeRetry = requests.length;
          const current = yield* send(threadId, "explicit-retry", target);
          assert.equal(current.runs.at(-1)?.status, "running");
          assert.equal(opens, missing === null ? 1 : 2);
          const retry = requests.slice(requestsBeforeRetry);
          assert.lengthOf(
            retry.filter((request) => request.method === "session.prompt"),
            1,
            "only the explicit retry is sent",
          );
          if (missing === null) {
            // In-place recovery re-sends every explicit field on the reused session.
            assert.deepEqual(
              retry
                .filter((request) => request.method !== "session.prompt")
                .map((request) => [request.method, request.body]),
              [
                [
                  "session.switchModel",
                  {
                    sessionID: "root-1",
                    model: { id: "claude-sonnet", providerID: "anthropic", variant: "high" },
                  },
                ],
                ["session.switchAgent", { sessionID: "root-1", agent: "build" }],
              ],
            );
          } else {
            const created = retry.find((request) => request.method === "session.create");
            assert.isDefined(created, "an incomplete selection reopens a fresh session");
            assert.equal(
              (created!.body.model as { variant?: string }).variant,
              missing === "variant" ? undefined : "high",
            );
            assert.equal(created!.body.agent, "build");
          }
          assert.deepEqual((yield* executionSelectionOf(current)).selection, target);
        }).pipe(
          Effect.provide(
            makeOrchestratorV2ReplayLayerWithRegistry(
              { name: "opencode-selection" },
              makeSingleLayer(adapter),
              { runEffectWorker: false },
            ),
          ),
        );
      }),
    ),
  );
}

it.effect("OpenCode 2 re-sends a selection after a switch whose response was lost", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("opencode-selection-aba");
      const requests: Array<WireRequest> = [];
      const client = makeClient({
        requests,
        prompt: async () => {},
        // The native session applies the switch, but the response is lost.
        switchModel: async (body) => {
          if ((body.model as { variant?: string }).variant === "max") {
            throw new Error("connection reset after the switch applied");
          }
        },
      });
      const adapter = yield* makeAdapter({ cwd, client, onConnect: () => {} });
      const threadId = ThreadId.make("thread:opencode-selection-aba");
      const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
        runtimeMode: "full-access",
        interactionMode: "default",
        cwd,
      });
      const session = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("session:opencode-selection-aba"),
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
        projectId: ProjectId.make("project:selection-aba"),
        title: "selection ABA",
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
      const start = (ordinal: number, modelSelection: ModelSelection) =>
        session.startTurn({
          appThread,
          threadId,
          runId: RunId.make(`run:selection-aba:${ordinal}`),
          runOrdinal: ordinal,
          providerTurnOrdinal: ordinal,
          attemptId: RunAttemptId.make(`attempt:selection-aba:${ordinal}`),
          rootNodeId: NodeId.make(`node:selection-aba:${ordinal}`),
          providerThread,
          message: {
            messageId: MessageId.make(`message:selection-aba:${ordinal}`),
            text: `turn ${ordinal}`,
            attachments: [],
            createdBy: "user",
            creationSource: "web",
          },
          modelSelection,
          runtimePolicy,
        });
      const lost = yield* Effect.exit(start(1, selectionWith("max")));
      assert.isTrue(Exit.isFailure(lost));
      const requestsBeforeRetry = requests.length;
      yield* start(2, selection);
      assert.deepEqual(
        requests
          .slice(requestsBeforeRetry)
          .filter((request) => request.method === "session.switchModel")
          .map((request) => request.body),
        [
          {
            sessionID: "root-1",
            model: { id: "claude-sonnet", providerID: "anthropic", variant: "high" },
          },
        ],
        "the original selection is re-applied instead of trusting the stale binding",
      );
    }),
  ),
);
