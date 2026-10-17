import * as PgClient from "@effect/sql-pg/PgClient";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Reactivity from "effect/reactivity/Reactivity";

export class WorkerLeadershipError extends Schema.TaggedError<WorkerLeadershipError>()(
  "WorkerLeadershipError",
  { reason: Schema.Literals(["held", "connection"]), cause: Schema.optional(Schema.Defect()) },
) {
  override get message(): string {
    return this.reason === "held"
      ? "Another relay worker holds this database."
      : "Personal relay worker leadership connection failed.";
  }
  get exitCode(): number {
    return this.reason === "held" ? 3 : 1;
  }
}
const isWorkerLeadershipError = Schema.is(WorkerLeadershipError);
export class WorkerLeadership extends Context.Service<
  WorkerLeadership,
  {
    readonly lost: Effect.Effect<never, WorkerLeadershipError>;
    readonly guard: <A, E, R>(
      effect: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | WorkerLeadershipError, R>;
  }
>()("t3code-personal-relay/maintenance/WorkerLeadership") {}

// Acquire this layer before starting the listener and guard the serving lifetime.
export const make = Effect.fn("WorkerLeadership.make")(
  function* (config: PgClient.PgClientConfig) {
    const connection = yield* PgClient.makeClient({
      ...config,
      acquireForStream: false,
      connectTimeout: "2 seconds",
      keepAlive: "30 seconds",
      applicationName: "t3-relay-worker-leadership",
    }).pipe(Effect.provide(Reactivity.layer));
    const result = yield* connection<{
      acquired: boolean;
    }>`SELECT pg_try_advisory_lock(hashtextextended('t3-relay:worker', 0)) AS acquired`;
    if (!result[0]?.acquired) return yield* new WorkerLeadershipError({ reason: "held" });
    const notifications = yield* connection.listen("t3_relay_worker_liveness");
    const lost = Effect.forever(Queue.take(notifications)).pipe(
      Effect.mapError((cause) => new WorkerLeadershipError({ reason: "connection", cause })),
    );
    return WorkerLeadership.of({
      lost,
      guard: Effect.fn("WorkerLeadership.guard")(function* <A, E, R>(
        effect: Effect.Effect<A, E, R>,
      ) {
        return yield* Effect.raceFirst(effect, lost);
      }),
    });
  },
  Effect.mapError((cause) =>
    isWorkerLeadershipError(cause)
      ? cause
      : new WorkerLeadershipError({ reason: "connection", cause }),
  ),
);
export const layer = (config: PgClient.PgClientConfig) =>
  Layer.effect(WorkerLeadership, make(config));
