import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import { runAdmin } from "../admin.ts";
import { EnvironmentAdministration } from "./EnvironmentAdministration.ts";
import { EnvironmentEnrollment, EnrollmentPersistenceError } from "./EnvironmentEnrollment.ts";
import { settings } from "./EnrollmentTestSupport.ts";

runAdmin(
  Layer.mergeAll(
    Layer.succeed(EnvironmentEnrollment, {
      enroll: Effect.fn("Test.enroll")(function* () {
        yield* Effect.logInfo("test-only-log-that-must-not-reach-stdout");
        if (process.env.U4_TEST_FAIL) {
          return yield* new EnrollmentPersistenceError({
            operation: "enroll",
            cause: "test-only-sensitive-cause",
          });
        }
        return Redacted.make("test-only-issued-credential");
      }),
      revoke: Effect.fn("EnvironmentEnrollment.revoke")(() => Effect.succeed(true)),
    }),
    Layer.effect(
      EnvironmentAdministration,
      Effect.gen(function* () {
        yield* Effect.logInfo("test-only-construction-log-that-must-not-reach-stdout");
        return EnvironmentAdministration.of({
          config: Effect.succeed(settings),
          listEnvironments: Effect.succeed([]),
          listDevices: Effect.succeed([]),
          status: Effect.succeed({ generation: "0" }),
        });
      }),
    ),
  ),
);
