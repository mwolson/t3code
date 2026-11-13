import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import { AccountMutations, AccountMutationError } from "../account/AccountMutations.ts";
import { AlertLedger } from "../account/AlertLedger.ts";
import { ApnsSender, type ApnsSendOutcome, type PlannedApnsJob } from "../apns/ApnsSender.ts";

export const makeApnsSender = (
  outcome: ApnsSendOutcome = {
    classification: "delivered",
    status: 200,
    reason: null,
    apnsId: "test",
  },
) =>
  Effect.gen(function* () {
    const sent = yield* Ref.make<ReadonlyArray<PlannedApnsJob>>([]);
    const service = ApnsSender.of({
      send: (job) => Ref.update(sent, (jobs) => [...jobs, job]).pipe(Effect.as(outcome)),
    });
    return { sent, service, layer: Layer.succeed(ApnsSender, service) };
  });

export const accountMutations = (overrides: Partial<AccountMutations["Service"]> = {}) =>
  AccountMutations.of({
    publish: () =>
      Effect.fail(new AccountMutationError({ operation: "publish", reason: "persistence" })),
    registerDevice: () =>
      Effect.fail(
        new AccountMutationError({ operation: "register-device", reason: "persistence" }),
      ),
    unregisterDevice: () =>
      Effect.fail(
        new AccountMutationError({ operation: "unregister-device", reason: "persistence" }),
      ),
    registerLiveActivity: () =>
      Effect.fail(
        new AccountMutationError({ operation: "register-live-activity", reason: "persistence" }),
      ),
    ...overrides,
  });
export const alertLedger = (overrides: Partial<AlertLedger["Service"]> = {}) =>
  AlertLedger.of({
    create: () => Effect.succeed(false),
    backfillDevice: () => Effect.void,
    listPending: () => Effect.succeed([]),
    supersedeEnvironment: () => Effect.void,
    removeEnvironmentKey: () => Effect.void,
    settle: () => Effect.void,
    beginSend: () => Effect.void,
    finishSend: () => Effect.void,
    ...overrides,
  });
