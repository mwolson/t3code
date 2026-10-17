import type { RelayAgentActivityState } from "@t3tools/contracts/relay";
import { PersonalPublishClaims } from "@t3tools/shared/personalRelay";
import { verifyRelayJwt, RELAY_ACTIVITY_PUBLISH_TYP } from "@t3tools/shared/relayJwt";
import { assert } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import { RelayDb } from "t3code-relay/db";
import { AccountLock } from "../../src/account/AccountLock.ts";
import { AccountMutations } from "../../src/account/AccountMutations.ts";
import { AccountWorker } from "../../src/account/AccountWorker.ts";
import { AlertLedger } from "../../src/account/AlertLedger.ts";
import { makeApnsSender } from "../../src/testing/Fakes.testkit.ts";
import { initialize, services, registration, workerServices } from "./Harness.ts";

const decodeClaims = Schema.decodeUnknownEffect(PersonalPublishClaims);

export const assertSignedCompletion = Effect.fn("test.assertSignedCompletion")(function* (input: {
  readonly state: RelayAgentActivityState;
  readonly proof: string;
  readonly publicKey: string;
  readonly at: DateTime.Utc;
  readonly stale: boolean;
}) {
  yield* Effect.gen(function* () {
    yield* initialize;
    const sql = (yield* RelayDb).$client;
    yield* sql.unsafe(
      "TRUNCATE personal_relay_alerts, personal_relay_thread_versions, personal_relay_target_state, personal_relay_accounts, relay_mobile_devices, relay_live_activities, relay_agent_activity_rows, relay_environment_links, relay_delivery_attempts CASCADE",
    );
    const claims = yield* decodeClaims(
      yield* verifyRelayJwt({
        publicKey: input.publicKey,
        token: input.proof,
        typ: RELAY_ACTIVITY_PUBLISH_TYP,
        issuer: "t3-env:relay-environment",
        audience: "https://relay.example.test",
        nowEpochSeconds: Math.floor(input.at.epochMilliseconds / 1000),
      }),
    );
    const owner = "held-completion-owner";
    yield* sql`INSERT INTO relay_environment_links (environment_id, environment_public_key, user_id, endpoint_http_base_url, endpoint_ws_base_url, endpoint_provider_kind, notifications_enabled, live_activities_enabled, created_at, updated_at)
      VALUES ('relay-environment', ${input.publicKey}, ${owner}, 'https://background-only.invalid', 'wss://background-only.invalid', 'manual', true, true, now(), now())`;
    yield* (yield* AccountMutations).registerDevice({ userId: owner, registration });
    const ledger = yield* AlertLedger;
    const lock = yield* AccountLock;
    const creation = {
      userId: owner,
      environmentPublicKey: input.publicKey,
      row: { ...input.state, status: "Done" },
      claims,
      now: DateTime.formatIso(input.at),
    };
    assert.equal(yield* lock.withAccountLock(owner, ledger.create(creation)), !input.stale);
    assert.isFalse(yield* lock.withAccountLock(owner, ledger.create(creation)));
    const expected = input.stale ? 0 : 1;
    assert.equal((yield* sql`SELECT 1 FROM personal_relay_alert_deliveries`).length, expected);
    const fake = yield* makeApnsSender();
    const pass = Effect.flatMap(AccountWorker, (worker) => worker.pass(owner));
    yield* pass.pipe(Effect.provide(workerServices(fake.service)));
    yield* pass.pipe(Effect.provide(workerServices(fake.service)));
    assert.equal((yield* Ref.get(fake.sent)).length, expected);
    assert.equal(
      (yield* sql`SELECT 1 FROM personal_relay_alert_deliveries WHERE status = 'accepted'`).length,
      expected,
    );
  }).pipe(Effect.provide(services));
});
