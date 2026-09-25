import { assert, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderThreadId,
  RunId,
  RunAttemptId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Scope from "effect/Scope";
import {
  make,
  type ProviderInteractionModeReflection,
} from "./ProviderInteractionModeReflections.ts";

const request: ProviderInteractionModeReflection = {
  threadId: ThreadId.make("thread"),
  driver: ProviderDriverKind.make("opencode"),
  sourceRunId: RunId.make("run"),
  sourceAttemptId: RunAttemptId.make("attempt"),
  providerThreadId: ProviderThreadId.make("binding"),
  nativeThreadId: "session",
  expectedInteractionMode: "plan",
  expectedRuntimeMode: "full-access",
  interactionMode: "default",
  dedupeKey: "event",
};

it.effect("offers without a consumer and retains the newest 128 immutable observations", () =>
  Effect.gen(function* () {
    const channel = yield* make;
    for (let i = 0; i < 257; i++) {
      const value = { ...request, dedupeKey: String(i) };
      yield* channel.offer(value);
      value.dedupeKey = "mutated";
    }
    for (let i = 129; i < 257; i++) {
      const received = yield* channel.take;
      assert.equal(received.dedupeKey, String(i));
      assert.isTrue(Object.isFrozen(received));
    }
  }),
);

it.effect("scope shutdown releases a pending consumer and closes subsequent offers", () =>
  Effect.gen(function* () {
    const scope = yield* Scope.make();
    const channel = yield* make.pipe(Effect.provideService(Scope.Scope, scope));
    const consumer = yield* channel.take.pipe(Effect.forkScoped);
    yield* Scope.close(scope, Exit.void);
    assert.isTrue(Exit.isFailure(yield* Fiber.await(consumer)));
    // Queue shutdown returns immediately; it does not leave a blocked offer behind.
    yield* channel.offer(request);
  }),
);
