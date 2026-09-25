import { assert, it } from "@effect/vitest";
import {
  CommandId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as TestClock from "effect/testing/TestClock";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { EffectOutboxV2, layer as effectOutboxLayer } from "./EffectOutbox.ts";
import {
  layerWithOptions,
  OrchestrationEffectExecutionError,
  OrchestrationEffectExecutorV2,
  OrchestrationEffectWorkerV2,
} from "./EffectWorker.ts";

it.effect(
  "retries repair past exhaustion with capped exponential backoff and no provider start",
  () =>
    Effect.gen(function* () {
      const executions = yield* Ref.make(0);
      const compensations = yield* Ref.make(0);
      const executor = Layer.succeed(
        OrchestrationEffectExecutorV2,
        OrchestrationEffectExecutorV2.of({
          execute: (effect) =>
            Ref.update(executions, (n) => n + 1).pipe(
              Effect.andThen(
                Effect.fail(
                  new OrchestrationEffectExecutionError({
                    effectId: effect.id,
                    effectType: effect.request.type,
                    cause: "provider start failed",
                  }),
                ),
              ),
            ),
          compensateDeadLetter: (effect) =>
            Ref.update(compensations, (n) => n + 1).pipe(
              Effect.andThen(
                Effect.fail(
                  new OrchestrationEffectExecutionError({
                    effectId: effect.id,
                    effectType: effect.request.type,
                    cause: "deterministic compensation failure",
                  }),
                ),
              ),
            ),
        }),
      );
      const outboxLayer = effectOutboxLayer.pipe(Layer.provideMerge(SqlitePersistenceMemory));
      const layer = layerWithOptions({ workerId: "review-probe", maxAttempts: 1 }).pipe(
        Layer.provideMerge(Layer.merge(outboxLayer, executor)),
      );
      yield* Effect.gen(function* () {
        const outbox = yield* EffectOutboxV2;
        const worker = yield* OrchestrationEffectWorkerV2;
        const effectId = "effect:review-repair-loop";
        yield* outbox.enqueue([
          {
            id: effectId,
            commandId: CommandId.make(effectId),
            threadId: ThreadId.make("thread:review-repair-loop"),
            request: { type: "provider-turn.start", runId: RunId.make("run:review-repair-loop") },
          },
        ]);
        for (let i = 0; i < 12; i += 1) {
          const now = yield* DateTime.now;
          const result = yield* worker.runOnce.pipe(Effect.result);
          const stored = Option.getOrUndefined(yield* outbox.get(effectId));
          const nextClaimableAt = yield* outbox.nextClaimableAt;
          const delayMs = Math.min(30_000, 100 * 2 ** i);
          const availableAt = DateTime.formatIso(DateTime.add(now, { milliseconds: delayMs }));
          assert.equal(result._tag, "Failure");
          assert.equal(stored?.request.type, "provider-turn.repair");
          assert.equal(stored?.status, "pending");
          assert.equal(stored?.attemptCount, i + 1);
          assert.equal(stored?.availableAt, availableAt);
          assert.equal(DateTime.formatIso(Option.getOrThrow(nextClaimableAt)), availableAt);
          assert.isFalse(yield* worker.runOnce);
          yield* TestClock.adjust(delayMs);
        }
        assert.equal(yield* Ref.get(executions), 1);
        assert.equal(yield* Ref.get(compensations), 12);
        const final = Option.getOrUndefined(yield* outbox.get(effectId));
        assert.equal(final?.request.type, "provider-turn.repair");
        assert.equal(final?.status, "pending");
        assert.equal(final?.attemptCount, 12);
      }).pipe(Effect.provide(layer));
    }),
);

for (const type of ["provider-turn.start", "provider-turn.restart"] as const) {
  it.effect(`repairs a reclaimed ${type} past its retry budget without execution`, () =>
    Effect.gen(function* () {
      const executions = yield* Ref.make(0);
      const repairs = yield* Ref.make(0);
      const dependencies = Layer.merge(
        effectOutboxLayer.pipe(Layer.provideMerge(SqlitePersistenceMemory)),
        Layer.succeed(OrchestrationEffectExecutorV2, {
          execute: () => Ref.update(executions, (n) => n + 1),
          compensateDeadLetter: () => Ref.update(repairs, (n) => n + 1),
        }),
      );
      yield* Effect.gen(function* () {
        const outbox = yield* EffectOutboxV2;
        const worker = yield* OrchestrationEffectWorkerV2;
        const id = `exhausted-claim:${type}`;
        yield* outbox.enqueue([
          {
            id,
            commandId: CommandId.make(id),
            threadId: ThreadId.make(id),
            request:
              type === "provider-turn.start"
                ? { type, runId: RunId.make(id) }
                : {
                    type,
                    runId: RunId.make(id),
                    providerSessionId: ProviderSessionId.make(id),
                    providerThreadId: ProviderThreadId.make(id),
                    providerTurnId: ProviderTurnId.make(id),
                    interruptedAttemptId: RunAttemptId.make(id),
                    sessionTransition: { type: "detach" },
                  },
          },
        ]);
        assert.isTrue(
          Option.isSome(yield* outbox.claimNext({ workerId: "previous", leaseDurationMs: 1000 })),
        );
        assert.isTrue(
          yield* outbox.retry({
            effectId: id,
            workerId: "previous",
            error: "settlement failed",
            delayMs: 0,
          }),
        );
        assert.isTrue(yield* worker.runOnce);
        assert.equal(yield* Ref.get(executions), 0);
        assert.equal(yield* Ref.get(repairs), 1);
        const stored = Option.getOrThrow(yield* outbox.get(id));
        assert.equal(stored.attemptCount, 2);
        assert.equal(stored.status, "failed");
        assert.equal(stored.request.type, "provider-turn.repair");
      }).pipe(
        Effect.provide(
          layerWithOptions({ workerId: "next", maxAttempts: 1 }).pipe(
            Layer.provideMerge(dependencies),
          ),
        ),
      );
    }),
  );
}
