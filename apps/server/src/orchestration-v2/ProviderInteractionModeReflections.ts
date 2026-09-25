import type {
  ProviderDriverKind,
  ProviderInteractionMode,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  RuntimeMode,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";

export interface ProviderInteractionModeReflection {
  readonly threadId: ThreadId;
  readonly driver: ProviderDriverKind;
  readonly sourceRunId: RunId;
  readonly sourceAttemptId: RunAttemptId;
  readonly providerThreadId: ProviderThreadId;
  readonly nativeThreadId: string | null;
  readonly expectedInteractionMode: ProviderInteractionMode;
  readonly expectedRuntimeMode: RuntimeMode;
  readonly interactionMode: ProviderInteractionMode;
  readonly dedupeKey: string;
}

export class ProviderInteractionModeReflections extends Context.Service<
  ProviderInteractionModeReflections,
  {
    readonly offer: (request: ProviderInteractionModeReflection) => Effect.Effect<void>;
    readonly take: Effect.Effect<ProviderInteractionModeReflection>;
  }
>()("t3/orchestration-v2/ProviderInteractionModeReflections") {}

export const make = Effect.gen(function* () {
  // Sliding offers keep the native event pump independent of command processing.
  const queue = yield* Queue.sliding<ProviderInteractionModeReflection>(128);
  yield* Effect.addFinalizer(() => Queue.shutdown(queue));
  return ProviderInteractionModeReflections.of({
    offer: (request) => Queue.offer(queue, Object.freeze({ ...request })).pipe(Effect.asVoid),
    take: Queue.take(queue),
  });
});

export const layer = Layer.effect(ProviderInteractionModeReflections, make);
