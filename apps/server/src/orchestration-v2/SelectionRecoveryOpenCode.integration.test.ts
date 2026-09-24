import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  OpenCodeSettings,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type ModelSelection,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type { ServerConfig } from "../config.ts";
import type { OpenCodeRuntimeShape } from "../provider/opencodeRuntime.ts";
import { makeOpenCodeAdapterV2 } from "./Adapters/OpenCodeAdapterV2.ts";
import { OrchestrationEffectWorkerV2 } from "./EffectWorker.ts";
import { layer as idAllocatorLayer, IdAllocatorV2 } from "./IdAllocator.ts";
import { OrchestratorV2 } from "./Orchestrator.ts";
import { makeSingleLayer } from "./ProviderAdapterRegistry.ts";
import { ProviderSessionManagerV2 } from "./ProviderSessionManager.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "./testkit/ReplayFixtureWorkspace.ts";

const settings = Schema.decodeSync(OpenCodeSettings)({ serverUrl: "http://test.invalid" });
for (const missing of [null, "agent", "variant"] as const) {
  it.effect(`OpenCode real-adapter selection recovery with omitted=${missing}`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const cwd = yield* checkpointWorkspace("opencode-selection-recovery");
        const instanceId = ProviderInstanceId.make("opencode-selection");
        const selection: ModelSelection = {
          instanceId,
          model: "anthropic/claude-sonnet",
          options: [
            { id: "agent", value: "build" },
            { id: "variant", value: "high" },
          ],
        };
        let opens = 0;
        let creates = 0;
        const prompts: Array<Record<string, unknown>> = [];
        const adapter = yield* Effect.gen(function* () {
          return makeOpenCodeAdapterV2({
            instanceId,
            settings,
            environment: {},
            runtime: {
              connectToOpenCodeServer: () =>
                Effect.sync(() => {
                  opens++;
                  return { url: "http://test.invalid", external: true };
                }),
              createOpenCodeSdkClient: () => ({
                event: {
                  subscribe: async () => ({
                    stream: {
                      [Symbol.asyncIterator]: () => ({
                        next: () => new Promise<IteratorResult<unknown>>(() => {}),
                      }),
                    },
                  }),
                },
                session: {
                  create: async () => {
                    creates++;
                    return { data: { id: `root-${creates}`, time: { created: 1, updated: 1 } } };
                  },
                  get: async () => ({ data: { id: "root-1", time: { created: 1, updated: 1 } } }),
                  promptAsync: async (input: Record<string, unknown>) => {
                    prompts.push(input);
                    if (prompts.length === 1) throw new Error("fixture rejected admission");
                    return { data: true };
                  },
                  abort: async () => ({ data: true }),
                  children: async () => ({ data: [] }),
                  messages: async () => ({ data: [] }),
                  status: async () => ({ data: {} }),
                },
              }),
            } as unknown as OpenCodeRuntimeShape,
            idAllocator: yield* IdAllocatorV2,
            serverConfig: { cwd, attachmentsDir: "/tmp/attachments" } as ServerConfig["Service"],
          });
        }).pipe(Effect.provide(Layer.merge(idAllocatorLayer, NodeServices.layer)));
        yield* Effect.gen(function* () {
          const orchestrator = yield* OrchestratorV2;
          const worker = yield* OrchestrationEffectWorkerV2;
          const manager = yield* ProviderSessionManagerV2;
          const threadId = ThreadId.make("thread:opencode-selection");
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
          const send = (name: string, modelSelection: ModelSelection) =>
            orchestrator.dispatch({
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
          yield* send("first", selection);
          yield* worker.drain();
          const failed = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(failed.runs.at(-1)?.status, "failed");
          const binding = failed.providerThreads[0]!;
          const runtime = Option.getOrThrow(yield* manager.get(binding.providerSessionId!));
          assert.isNull(yield* runtime.executionSelection!(binding));
          const target = {
            ...selection,
            options: selection.options!.filter((option) => option.id !== missing),
          };
          assert.equal(yield* runtime.reappliesFullSelection!(target), missing === null);
          yield* send("explicit-retry", target);
          yield* worker.drain();
          const current = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(current.runs.at(-1)?.status, "running");
          assert.equal(opens, missing === null ? 1 : 2);
          assert.lengthOf(prompts, 2, "only the explicit retry is sent");
          const currentBinding = current.providerThreads.find(
            (thread) => thread.id === current.thread.activeProviderThreadId,
          )!;
          const currentRuntime = Option.getOrThrow(
            yield* manager.get(currentBinding.providerSessionId!),
          );
          assert.deepEqual(yield* currentRuntime.executionSelection!(currentBinding), target);
          assert.equal(prompts[1]?.agent, missing === "agent" ? undefined : "build");
          assert.equal(prompts[1]?.variant, missing === "variant" ? undefined : "high");
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
