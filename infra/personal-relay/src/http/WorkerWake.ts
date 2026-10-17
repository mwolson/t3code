import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

export class WorkerWake extends Context.Service<
  WorkerWake,
  {
    readonly wake: Effect.Effect<void>;
  }
>()("t3code-personal-relay/http/WorkerWake") {}
