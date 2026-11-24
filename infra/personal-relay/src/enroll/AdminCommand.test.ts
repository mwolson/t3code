import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import { AdminCommandError, AdminIO, execute } from "./AdminCommand.ts";
import { EnvironmentAdministration } from "./EnvironmentAdministration.ts";
import { EnvironmentEnrollment } from "./EnvironmentEnrollment.ts";
import { settings } from "./EnrollmentTestSupport.ts";

function harness(input = '{"proof":"test-proof"}', terminal = false) {
  const stdout: string[] = [];
  const stderr: string[] = [];
  const enrollments: string[] = [];
  const revocations: string[] = [];
  let reads = 0;
  const services = Layer.mergeAll(
    Layer.succeed(AdminIO, {
      stdoutIsTerminal: terminal,
      readInput: Effect.sync(() => {
        reads += 1;
        return input;
      }),
      writeCredential: (credential) =>
        Effect.sync(() => {
          stdout.push(Redacted.value(credential));
        }),
      writeDiagnostic: (message) =>
        Effect.sync(() => {
          stderr.push(message);
        }),
    }),
    Layer.succeed(EnvironmentEnrollment, {
      enroll: (proof) =>
        Effect.sync(() => {
          enrollments.push(proof);
          return Redacted.make("test-only-issued-credential");
        }),
      revoke: (id) =>
        Effect.sync(() => {
          revocations.push(id);
          return true;
        }),
    }),
    Layer.succeed(EnvironmentAdministration, {
      config: Effect.succeed(settings),
      listEnvironments: Effect.succeed([{ environmentId: "env" }]),
      listDevices: Effect.succeed([{ pushTokenSuffix: "suffix" }]),
      status: Effect.succeed({ generation: "1" }),
    }),
  );
  return {
    run: (args: string[]) => execute(args).pipe(Effect.provide(services)),
    stdout,
    stderr,
    enrollments,
    revocations,
    reads: () => reads,
  };
}

describe("admin command credential boundary", () => {
  it.effect("writes only the committed credential to stdout", () =>
    Effect.gen(function* () {
      const h = harness();
      yield* h.run(["enroll"]);
      expect(h.stdout).toEqual(["test-only-issued-credential"]);
      expect(h.stderr).toEqual(["Environment enrolled."]);
      expect(h.enrollments).toEqual(["test-proof"]);
    }),
  );

  it.effect("refuses terminal credential output before reading input or mutating", () =>
    Effect.gen(function* () {
      const h = harness(undefined, true);
      const error = yield* Effect.flip(h.run(["enroll"]));
      expect(error).toMatchObject({ reason: "terminal-output" });
      expect(h.reads()).toBe(0);
      expect(h.enrollments).toEqual([]);
      expect(h.stdout).toEqual([]);
    }),
  );

  it.effect.each(["not-json", "x".repeat(65_537)])(
    "rejects malformed or oversized input",
    (input) =>
      Effect.gen(function* () {
        const h = harness(input);
        const error = yield* Effect.flip(h.run(["enroll"]));
        expect(error).toMatchObject({ reason: "input" });
        expect(h.enrollments).toEqual([]);
        expect(h.stdout).toEqual([]);
      }),
  );

  it.effect("writes diagnostics exclusively to stderr", () =>
    Effect.gen(function* () {
      const h = harness();
      yield* h.run(["list-devices"]);
      expect(h.stdout).toEqual([]);
      expect(h.stderr).toHaveLength(1);
      expect(h.stderr.join()).not.toContain("test-only-issued-credential");
    }),
  );

  it.effect("revokes without credential output", () =>
    Effect.gen(function* () {
      const h = harness();
      yield* h.run(["revoke-environment", "env"]);
      expect(h.revocations).toEqual(["env"]);
      expect(h.stdout).toEqual([]);
    }),
  );

  it.effect("rejects invalid arguments", () =>
    Effect.gen(function* () {
      const h = harness();
      const error = yield* Effect.flip(h.run(["enroll", "extra"]));
      expect(error).toBeInstanceOf(AdminCommandError);
      expect(h.enrollments).toEqual([]);
    }),
  );
});
