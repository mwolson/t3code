import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeProcess from "node:process";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as AdminCommand from "./enroll/AdminCommand.ts";
import type * as EnvironmentAdministration from "./enroll/EnvironmentAdministration.ts";
import type * as EnvironmentEnrollment from "./enroll/EnvironmentEnrollment.ts";

const isAdminCommandError = Schema.is(AdminCommand.AdminCommandError);

const streams = AdminCommand.AdminIO.of({
  stdoutIsTerminal: NodeProcess.stdout.isTTY === true,
  readInput: Effect.tryPromise({
    try: async (signal) => {
      const chunks: Buffer[] = [];
      let length = 0;
      const abort = () => {
        NodeProcess.stdin.destroy();
      };
      signal.addEventListener("abort", abort, { once: true });
      try {
        for await (const chunk of NodeProcess.stdin) {
          const bytes = Buffer.from(chunk);
          length += bytes.length;
          if (length > 65_536) throw new Error("Input exceeds limit");
          chunks.push(bytes);
        }
        return Buffer.concat(chunks).toString("utf8");
      } finally {
        signal.removeEventListener("abort", abort);
      }
    },
    catch: (cause) => new AdminCommand.AdminCommandError({ reason: "input", cause }),
  }),
  writeCredential: Effect.fn("AdminIO.writeCredential")((credential) =>
    write(NodeProcess.stdout, Redacted.value(credential)),
  ),
  writeDiagnostic: Effect.fn("AdminIO.writeDiagnostic")((message) =>
    write(NodeProcess.stderr, message),
  ),
});

export const program = Effect.fn("personalRelay.admin.program")(function* (
  args: ReadonlyArray<string>,
  io: AdminCommand.AdminIO["Service"],
) {
  let readInput = io.readInput;
  if (args.length === 1 && args[0] === "enroll" && !io.stdoutIsTerminal) {
    readInput = Effect.succeed(yield* io.readInput.pipe(Effect.timeout("60 seconds")));
  }
  return yield* AdminCommand.execute(args).pipe(
    Effect.timeout("5 seconds"),
    Effect.provideService(AdminCommand.AdminIO, { ...io, readInput }),
  );
});

// The composition root supplies the real ledger together with the shared repositories.
export function runAdmin<E>(
  services: Layer.Layer<
    | EnvironmentAdministration.EnvironmentAdministration
    | EnvironmentEnrollment.EnvironmentEnrollment,
    E
  >,
): void {
  const execution = program(NodeProcess.argv.slice(2), streams).pipe(
    Effect.provide(services.pipe(Layer.provideMerge(Logger.layer([])))),
    Effect.catchCause((cause) =>
      Effect.gen(function* () {
        const error = Cause.findErrorOption(cause);
        const message =
          error._tag === "Some" && isAdminCommandError(error.value)
            ? error.value.message
            : "Personal relay admin command failed.";
        yield* write(NodeProcess.stderr, message).pipe(Effect.ignore);
        process.exitCode = 1;
      }),
    ),
  );
  NodeRuntime.runMain(execution, { disableErrorReporting: true });
}

function write(stream: NodeJS.WritableStream, text: string) {
  return Effect.tryPromise({
    try: () =>
      new Promise<void>((resolve, reject) => {
        stream.once("error", reject);
        stream.write(`${text}\n`, (error) => {
          if (error) {
            reject(error);
          } else {
            stream.removeListener("error", reject);
            resolve();
          }
        });
      }),
    catch: (cause) => new AdminCommand.AdminCommandError({ reason: "output", cause }),
  });
}
