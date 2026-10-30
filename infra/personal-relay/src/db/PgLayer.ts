import * as PgClient from "@effect/sql-pg/PgClient";
import * as Layer from "effect/Layer";
import { RelayDb, RelayTransactions, makeRelayDb } from "t3code-relay/db";

export const layer = (config: PgClient.PgPoolConfig) => {
  const db = Layer.effect(RelayDb, makeRelayDb()).pipe(
    Layer.provide(PgClient.layer({ ...config, connectTimeout: "2 seconds", multiplex: false })),
  );
  return RelayTransactions.layer.pipe(Layer.provideMerge(db));
};
