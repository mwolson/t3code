import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import type { SqlError } from "effect/sql/SqlError";
import { RelayDb, RelayTransactions } from "t3code-relay/db";

export class AccountLockError extends Schema.TaggedError<AccountLockError>()("AccountLockError", {
  operation: Schema.Literals(["lock", "generation"]),
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return "Personal relay account transaction failed.";
  }
}

export class AccountLock extends Context.Service<
  AccountLock,
  {
    readonly withAccountLock: <A, E, R>(
      userId: string,
      effect: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | AccountLockError | SqlError, R>;
    readonly bumpGeneration: (userId: string) => Effect.Effect<string, AccountLockError>;
  }
>()("t3code-personal-relay/account/AccountLock") {}

export const make = Effect.gen(function* () {
  const db = yield* RelayDb;
  const transactions = yield* RelayTransactions;
  const sql = db.$client;
  return AccountLock.of({
    withAccountLock: Effect.fn("AccountLock.withAccountLock")(function* <A, E, R>(
      userId: string,
      effect: Effect.Effect<A, E, R>,
    ) {
      return yield* transactions.withTransaction(
        Effect.gen(function* () {
          yield* Effect.gen(function* () {
            yield* sql.unsafe("SET LOCAL lock_timeout = '2s'");
            yield* sql.unsafe("SET LOCAL statement_timeout = '3s'");
            yield* sql`SELECT pg_advisory_xact_lock(hashtextextended('t3-relay:account:' || ${userId}, 0))`;
            yield* sql`INSERT INTO personal_relay_accounts (user_id) VALUES (${userId}) ON CONFLICT DO NOTHING`;
          }).pipe(Effect.mapError((cause) => new AccountLockError({ operation: "lock", cause })));
          return yield* effect;
        }),
      );
    }),
    bumpGeneration: Effect.fn("AccountLock.bumpGeneration")(function* (userId) {
      const rows = yield* sql<{ generation: string }>`
      UPDATE personal_relay_accounts SET generation = generation + 1, updated_at = now()
      WHERE user_id = ${userId} RETURNING generation::text
    `.pipe(Effect.mapError((cause) => new AccountLockError({ operation: "generation", cause })));
      if (rows[0] === undefined) {
        return yield* new AccountLockError({
          operation: "generation",
          cause: "Account row missing",
        });
      }
      return rows[0].generation;
    }),
  });
});
export const layer = Layer.effect(AccountLock, make);
