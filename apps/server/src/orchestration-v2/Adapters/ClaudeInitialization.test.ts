import type { Query } from "@anthropic-ai/claude-agent-sdk";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { ProviderSessionId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import { vi } from "vite-plus/test";
import {
  NoOpProviderEventLoggers,
  ProviderEventLoggers,
} from "../../provider/Layers/ProviderEventLoggers.ts";
import {
  ClaudeAgentSdkQueryRunner,
  claudeAgentSdkQueryRunnerLiveLayer,
} from "./ClaudeAdapterV2.ts";

const mock = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("@anthropic-ai/claude-agent-sdk", async (load) => ({
  ...(await load<typeof import("@anthropic-ai/claude-agent-sdk")>()),
  query: mock.query,
}));

for (const outcome of ["success", "failure", "interruption"] as const) {
  it.effect(`Claude runner acknowledges initialization and cleans up ${outcome}`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const initialize = Promise.withResolvers<unknown>();
        const initializing = Promise.withResolvers<void>();
        let closed = 0;
        let consumed = 0;
        let completedOpen = false;
        mock.query.mockImplementation(() => {
          initializing.resolve();
          return {
            initializationResult: () => initialize.promise,
            close: () => {
              closed++;
            },
            next: () => {
              consumed++;
              return Promise.resolve({ done: true, value: undefined });
            },
            return: () => Promise.resolve({ done: true, value: undefined }),
            [Symbol.asyncIterator]() {
              return this;
            },
          } as unknown as Query;
        });
        const runner = yield* ClaudeAgentSdkQueryRunner;
        const opening = yield* runner
          .open({
            threadId: ThreadId.make("initialization"),
            providerSessionId: ProviderSessionId.make("initialization"),
            options: {
              sessionId: "initialization",
              model: "claude-sonnet-4-6",
              tools: [],
              permissionMode: "default",
              effort: "high",
            },
          })
          .pipe(
            Effect.tap(() =>
              Effect.sync(() => {
                completedOpen = true;
              }),
            ),
            Effect.forkChild({ startImmediately: true }),
          );
        yield* Effect.promise(() => initializing.promise);
        assert.isFalse(completedOpen, "scheduling initialization is not acknowledgment");
        assert.equal(
          consumed,
          0,
          "initialization acknowledgment does not depend on event consumption",
        );
        if (outcome === "success") {
          initialize.resolve({ commands: [], models: [] });
          const session = yield* Fiber.join(opening);
          assert.isTrue(completedOpen);
          assert.equal(closed, 0);
          yield* session.close;
        } else if (outcome === "failure") {
          initialize.reject(new Error("initialization rejected"));
          assert.isTrue(Exit.isFailure(yield* Fiber.await(opening)));
          assert.isFalse(completedOpen);
        } else {
          yield* Fiber.interrupt(opening);
          assert.isFalse(completedOpen);
        }
        assert.equal(closed, 1);
        mock.query.mockReset();
      }),
    ).pipe(
      Effect.provide(
        claudeAgentSdkQueryRunnerLiveLayer.pipe(
          Layer.provide(
            Layer.merge(
              NodeServices.layer,
              Layer.succeed(ProviderEventLoggers, NoOpProviderEventLoggers),
            ),
          ),
        ),
      ),
    ),
  );
}
