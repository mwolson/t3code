// Review probe: Stop winning OpenCode prompt admission, driven through the real
// orchestrator, manager, RunExecutionService and effect worker (fake SDK client).
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
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import type { ServerConfig } from "../config.ts";
import type { OpenCodeRuntimeShape } from "../provider/opencodeRuntime.ts";
import { makeOpenCodeAdapterV2 } from "./Adapters/OpenCodeAdapterV2.ts";
import { OrchestrationEffectWorkerV2 } from "./EffectWorker.ts";
import { layer as idAllocatorLayer, IdAllocatorV2 } from "./IdAllocator.ts";
import { OrchestratorV2 } from "./Orchestrator.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "./testkit/ReplayFixtureWorkspace.ts";

const SETTINGS = Schema.decodeSync(OpenCodeSettings)({ serverUrl: "http://test.invalid" });
const instanceId = ProviderInstanceId.make("opencode-review");
const selection: ModelSelection = { instanceId, model: "anthropic/claude-sonnet", options: [] };

function gate() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

const unexpected: Array<string> = [];
function tolerant(path: string, known: object): unknown {
  return new Proxy(known, {
    get(target, key) {
      if (typeof key !== "string") return Reflect.get(target, key);
      if (key in target) return Reflect.get(target, key);
      if (key === "then") return undefined;
      return tolerant(
        `${path}.${key}`,
        Object.assign(async () => {
          unexpected.push(`${path}.${key}`);
          return { data: undefined };
        }, {}),
      );
    },
  });
}

it.effect.each(
  (() => {
    const cases = [];
    for (const reply of ["success", "failure"] as const) {
      cases.push({ reply });
    }
    return cases;
  })(),
)("OpenCode Stop during prompt admission ($reply) through pipeline", ({ reply }) =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace(`review-opencode-${reply}`);
      const called = gate();
      const events: Array<(value: IteratorResult<unknown>) => void> = [];
      const eventStream = {
        [Symbol.asyncIterator]() {
          return {
            next: () => new Promise<IteratorResult<unknown>>((resolve) => events.push(resolve)),
          };
        },
      };
      const client = tolerant("client", {
        event: tolerant("event", { subscribe: async () => ({ stream: eventStream }) }),
        session: tolerant("session", {
          create: async () => ({ data: { id: "root", time: { created: 1, updated: 1 } } }),
          promptAsync: async (_input: unknown, options: { signal: AbortSignal }) => {
            called.resolve();
            return new Promise((resolve, reject) => {
              options.signal.addEventListener(
                "abort",
                () =>
                  reply === "success" ? resolve({ data: true }) : reject(new Error("cancelled")),
                { once: true },
              );
            });
          },
          abort: async () => ({ data: true }),
          children: async () => ({ data: [] }),
          messages: async () => ({ data: [] }),
          status: async () => ({ data: {} }),
        }),
      });
      const adapter = yield* Effect.gen(function* () {
        return makeOpenCodeAdapterV2({
          instanceId,
          settings: SETTINGS,
          environment: {},
          runtime: {
            connectToOpenCodeServer: () =>
              Effect.succeed({ url: "http://test.invalid", external: true }),
            createOpenCodeSdkClient: () => client,
          } as unknown as OpenCodeRuntimeShape,
          idAllocator: yield* IdAllocatorV2,
          serverConfig: { cwd, attachmentsDir: "/tmp/attachments" } as ServerConfig["Service"],
        });
      }).pipe(Effect.provide(Layer.merge(idAllocatorLayer, NodeServices.layer)));
      yield* Effect.gen(function* () {
        const orchestrator = yield* OrchestratorV2;
        const worker = yield* OrchestrationEffectWorkerV2;
        const threadId = ThreadId.make(`thread:review-opencode:${reply}`);
        const projection = orchestrator.getThreadProjection(threadId);
        const log = (label: string, value: unknown) => Effect.logDebug(label, value);
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
          projectId: ProjectId.make("project:review-opencode"),
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
        yield* Effect.promise(() => called.promise);
        const beforeStop = yield* projection;
        yield* log(
          "beforeStop.runs",
          beforeStop.runs.map((run) => [run.ordinal, run.status]),
        );
        const stop = yield* Effect.exit(
          orchestrator.dispatch({
            type: "run.interrupt",
            commandId: CommandId.make("stop"),
            threadId,
            runId: beforeStop.runs[0]!.id,
          }),
        );
        yield* log("stop.exit", stop._tag === "Failure" ? String(stop.cause) : "Success");
        const secondDrain = yield* worker
          .drain()
          .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));
        const settled = yield* Fiber.join(terminal);
        yield* Fiber.join(firstDrain);
        yield* Fiber.join(secondDrain);
        yield* worker.drain().pipe(Effect.exit);
        const after = yield* projection;
        yield* log(
          "firstTerminal",
          settled._tag === "Some" && settled.value.type === "run.updated"
            ? settled.value.payload.status
            : null,
        );
        yield* log(
          "after.runs",
          after.runs.map((run) => [run.ordinal, run.status]),
        );
        yield* log(
          "after.errorItems",
          after.turnItems
            .filter((item) => item.type === "error")
            .map((item) => [item.title, item.failure?.message]),
        );
        yield* log(
          "after.interruptItems",
          after.turnItems
            .filter((item) => item.type === "run_interrupt_result")
            .map((item) => [item.title, item.status]),
        );
        yield* log("unexpectedClientCalls", [...new Set(unexpected)]);
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
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry(
            { name: `review-opencode-${reply}` },
            ProviderAdapterRegistry.layerSingle(adapter),
            { runEffectWorker: false },
          ),
        ),
      );
    }),
  ),
);
