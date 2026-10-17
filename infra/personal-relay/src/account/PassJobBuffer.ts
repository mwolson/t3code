import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import { ApnsDeliveryQueueSender } from "t3code-relay/agentActivity/ApnsDeliveryQueue";
import type { SignedApnsDeliveryJob } from "t3code-relay/agentActivity/apnsDeliveryJobs";

export class PassJobBuffer extends Context.Service<
  PassJobBuffer,
  {
    readonly sender: ApnsDeliveryQueueSender["Service"];
    readonly drain: Effect.Effect<ReadonlyArray<SignedApnsDeliveryJob>>;
  }
>()("t3code-personal-relay/account/PassJobBuffer") {}

// Construct once per planning round, never once for the whole serving process.
export const make = Effect.gen(function* () {
  const jobs = yield* Ref.make<ReadonlyArray<SignedApnsDeliveryJob>>([]);
  return PassJobBuffer.of({
    sender: ApnsDeliveryQueueSender.of({
      send: Effect.fn("PassJobBuffer.send")(function* (body) {
        yield* Ref.update(jobs, (all) => [...all, body]);
      }),
    }),
    drain: Ref.getAndSet(jobs, []),
  });
});
export const layer = Layer.effect(PassJobBuffer, make);
