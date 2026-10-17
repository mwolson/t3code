import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as EnvironmentAdministration from "./EnvironmentAdministration.ts";
import * as EnvironmentEnrollment from "./EnvironmentEnrollment.ts";

export class AdminCommandError extends Schema.TaggedError<AdminCommandError>()(
  "AdminCommandError",
  {
    reason: Schema.Literals(["usage", "terminal-output", "input", "output"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    switch (this.reason) {
      case "usage":
        return "Usage: admin.mjs enroll | revoke-environment <id> | list-environments | list-devices | config | status";
      case "terminal-output":
        return "Enrollment requires redirected stdout; pipe the credential into the host secret store.";
      case "input":
        return "Enrollment requires a JSON object with a proof on stdin (at most 64 KiB).";
      case "output":
        return "Admin output failed.";
    }
  }
}

export class AdminIO extends Context.Service<
  AdminIO,
  {
    readonly stdoutIsTerminal: boolean;
    readonly readInput: Effect.Effect<string, AdminCommandError>;
    readonly writeCredential: (
      credential: Redacted.Redacted<string>,
    ) => Effect.Effect<void, AdminCommandError>;
    readonly writeDiagnostic: (message: string) => Effect.Effect<void, AdminCommandError>;
  }
>()("t3code-personal-relay/enroll/AdminCommand/AdminIO") {}

const decodeInput = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Struct({ proof: Schema.String.check(Schema.isMinLength(1)) })),
);
const encodeDiagnostic = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

export const execute = Effect.fnUntraced(function* (args: ReadonlyArray<string>) {
  const io = yield* AdminIO;
  const command = args[0];
  if (command === "enroll" && args.length === 1) {
    if (io.stdoutIsTerminal) return yield* new AdminCommandError({ reason: "terminal-output" });
    const text = yield* io.readInput;
    if (Buffer.byteLength(text, "utf8") > 65_536) {
      return yield* new AdminCommandError({ reason: "input" });
    }
    const { proof } = yield* decodeInput(text).pipe(
      Effect.mapError((cause) => new AdminCommandError({ reason: "input", cause })),
    );
    const enrollment = yield* EnvironmentEnrollment.EnvironmentEnrollment;
    const credential = yield* enrollment.enroll(proof);
    yield* io.writeCredential(credential);
    yield* io.writeDiagnostic("Environment enrolled.");
    return;
  }
  if (command === "revoke-environment" && args.length === 2 && args[1]?.trim()) {
    const enrollment = yield* EnvironmentEnrollment.EnvironmentEnrollment;
    const revoked = yield* enrollment.revoke(args[1]);
    yield* io.writeDiagnostic(revoked ? "Environment revoked." : "Environment not found.");
    return;
  }
  if (args.length !== 1) return yield* new AdminCommandError({ reason: "usage" });
  const admin = yield* EnvironmentAdministration.EnvironmentAdministration;
  const print = (value: unknown) =>
    encodeDiagnostic(value).pipe(
      Effect.mapError((cause) => new AdminCommandError({ reason: "output", cause })),
      Effect.flatMap(io.writeDiagnostic),
    );
  switch (command) {
    case "list-environments":
      yield* print(yield* admin.listEnvironments);
      return;
    case "list-devices":
      yield* print(yield* admin.listDevices);
      return;
    case "config":
      yield* print(yield* admin.config);
      return;
    case "status":
      yield* print(yield* admin.status);
      return;
    default:
      return yield* new AdminCommandError({ reason: "usage" });
  }
});
