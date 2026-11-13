import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { RelayDb } from "t3code-relay/db";
import { AgentActivityRows } from "t3code-relay/agentActivity/AgentActivityRows";
import { DpopProofReplay } from "t3code-relay/auth/DpopProofs";
import { AccountLock } from "../account/AccountLock.ts";

export class PrunerError extends Schema.TaggedError<PrunerError>()("PrunerError", {
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return "Personal relay retention failed.";
  }
}
export class Pruner extends Context.Service<
  Pruner,
  {
    readonly prune: (userId: string) => Effect.Effect<void, PrunerError>;
    readonly run: (userId: string) => Effect.Effect<never>;
  }
>()("t3code-personal-relay/maintenance/Pruner") {}
export const make = Effect.gen(function* () {
  const sql = (yield* RelayDb).$client;
  const lock = yield* AccountLock;
  const rows = yield* AgentActivityRows;
  const proofs = yield* DpopProofReplay;
  const prune = Effect.fn("personalRelay.prune")(
    function* (userId: string) {
      yield* lock.withAccountLock(
        userId,
        Effect.gen(function* () {
          const now = DateTime.formatIso(yield* DateTime.now);
          yield* proofs.pruneExpired;
          yield* rows.pruneTerminal({
            updatedBefore: DateTime.formatIso(DateTime.makeUnsafe(Date.parse(now) - 30 * 60_000)),
          });
          yield* sql`DELETE FROM relay_delivery_attempts WHERE created_at::timestamptz < ${now}::timestamptz - interval '7 days'`;
          yield* sql`DELETE FROM personal_relay_thread_versions WHERE accepted_at < ${now}::timestamptz - interval '1 hour'`;
          yield* sql`DELETE FROM personal_relay_alerts WHERE created_at < ${now}::timestamptz - interval '48 hours'`;
          yield* sql`DELETE FROM personal_relay_target_state s WHERE NOT EXISTS (SELECT 1 FROM relay_mobile_devices d WHERE d.device_id = s.device_id)`;
        }),
      );
    },
    Effect.mapError((cause) => new PrunerError({ cause })),
  );
  return Pruner.of({
    prune,
    run: Effect.fn("Pruner.run")(function* (userId) {
      return yield* Effect.forever(
        prune(userId).pipe(
          Effect.catch(() => Effect.logError("relay_retention_failed")),
          Effect.andThen(Effect.sleep("5 minutes")),
        ),
      );
    }),
  });
});
export const layer = Layer.effect(Pruner, make);
