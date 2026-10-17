import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";

const SECRET = "personal-relay-publish-seq-floor";
const RESERVATION = 3_600_000_000;

export class PublishSequenceError extends Schema.TaggedError<PublishSequenceError>()(
  "PublishSequenceError",
  {
    operation: Schema.Literals(["read", "decode", "reserve", "exhausted"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return "Could not allocate a durable relay publish sequence.";
  }
}

export class PublishSequence extends Context.Service<
  PublishSequence,
  { readonly next: Effect.Effect<number, PublishSequenceError> }
>()("t3/relay/PublishSequence") {}

export const make = Effect.gen(function* () {
  const secrets = yield* ServerSecretStore.ServerSecretStore;
  const lock = yield* Semaphore.make(1);
  let floor: number | undefined;
  let last = 0;
  const next = Effect.gen(function* () {
    const now = (yield* DateTime.now).epochMilliseconds * 1_000;
    if (floor === undefined) {
      const stored = yield* secrets
        .get(SECRET)
        .pipe(Effect.mapError((cause) => new PublishSequenceError({ operation: "read", cause })));
      const text = Option.isSome(stored) ? new TextDecoder().decode(stored.value) : "0";
      const persisted = Number(text);
      if (!/^\d+$/u.test(text) || !Number.isSafeInteger(persisted) || persisted < 0) {
        return yield* new PublishSequenceError({ operation: "decode" });
      }
      floor = persisted;
      last = Math.max(floor, now) - 1;
    }
    const sequence = Math.max(1, now, last + 1);
    if (
      !Number.isSafeInteger(sequence) ||
      sequence <= 0 ||
      sequence > Number.MAX_SAFE_INTEGER - RESERVATION
    ) {
      return yield* new PublishSequenceError({ operation: "exhausted" });
    }
    if (sequence >= floor) {
      const reserved = sequence + RESERVATION;
      yield* secrets
        .set(SECRET, new TextEncoder().encode(String(reserved)))
        .pipe(
          Effect.mapError((cause) => new PublishSequenceError({ operation: "reserve", cause })),
        );
      floor = reserved;
    }
    last = sequence;
    return sequence;
  }).pipe(Effect.uninterruptible, lock.withPermit);
  return PublishSequence.of({ next });
});

export const layer = Layer.effect(PublishSequence, make);
