import * as NodeServices from "@effect/platform-node/NodeServices";
// @effect-diagnostics nodeBuiltinImport:off - Exercise the CLI with an isolated Node stdin stream.
import * as NodeStream from "node:stream";
import { expect, it, vi } from "@effect/vitest";
import * as NetService from "@t3tools/shared/Net";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import { Command } from "effect/cli";

import { connectCommand } from "./connect.ts";

const credential = `t3env_${"a".repeat(64)}_${"b".repeat(96)}`;
const runtime = Layer.mergeAll(NodeServices.layer, NetService.layer);

const setup = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-background-relay-cli-" });
  const output: string[] = [];
  const warnings: string[] = [];
  const runConnect = (args: ReadonlyArray<string>, env: Record<string, string> = {}) =>
    Command.runWith(connectCommand, { version: "0.0.0" })([...args, "--base-dir", baseDir]).pipe(
      Effect.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ env }))),
      Effect.provideService(Console.Console, {
        ...globalThis.console,
        error: (...values) => {
          warnings.push(values.map(String).join(" "));
        },
        log: (...values) => {
          output.push(values.map(String).join(" "));
        },
      }),
    );
  const secretPath = path.join(baseDir, "userdata", "secrets", "background-relay-config.bin");
  const run = (args: ReadonlyArray<string>, env: Record<string, string> = {}) =>
    runConnect(["background-relay", ...args], env);
  return { fs, run, runConnect, output, warnings, secretPath };
});

it.layer(runtime)("background relay CLI", (it) => {
  it.effect("sets from stdin, shows redacted status, and clears the single secret", () =>
    Effect.gen(function* () {
      const { fs, run, output, secretPath } = yield* setup;
      const input = vi
        .spyOn(process, "stdin", "get")
        .mockReturnValue(
          NodeStream.Readable.from([
            new TextEncoder().encode(`${credential}\n`),
          ]) as typeof process.stdin,
        );
      yield* Effect.addFinalizer(() => Effect.sync(() => input.mockRestore()));
      yield* run([
        "set",
        "--url",
        "https://background.test/",
        "--issuer",
        "https://issuer.test/",
        "--credential-stdin",
      ]);
      const stored = JSON.parse(yield* fs.readFileString(secretPath));
      expect(stored).toEqual({
        version: 1,
        url: "https://background.test",
        issuer: "https://issuer.test",
        environmentCredential: credential,
      });
      const stat = yield* fs.stat(secretPath);
      expect(stat.mode & 0o777).toBe(0o600);
      yield* run(["status", "--json"]);
      expect(JSON.parse(output.at(-1) ?? "")).toMatchObject({
        state: "configured",
        url: "https://background.test",
        issuer: "https://issuer.test",
      });
      expect(JSON.parse(output.at(-1) ?? "").revision).toMatch(/^[0-9a-f]{12}$/u);
      yield* run(["status"]);
      expect(output.at(-1)).toContain("https://background.test");
      yield* run(["clear"]);
      expect(yield* fs.exists(secretPath)).toBe(false);
      yield* run(["status", "--json"]);
      expect(JSON.parse(output.at(-1) ?? "")).toEqual({
        state: "unconfigured",
        url: null,
        issuer: null,
        revision: null,
      });
      expect(output.join("\n")).not.toContain(credential);
    }),
  );

  it.effect("rejects an invalid credential from stdin without exposing it", () =>
    Effect.gen(function* () {
      const invalid = "bad-secret";
      const { fs, run, output, secretPath } = yield* setup;
      const input = vi
        .spyOn(process, "stdin", "get")
        .mockReturnValue(
          NodeStream.Readable.from([new TextEncoder().encode(invalid)]) as typeof process.stdin,
        );
      yield* Effect.addFinalizer(() => Effect.sync(() => input.mockRestore()));
      const error = yield* run([
        "set",
        "--url",
        "https://background.test",
        "--credential-stdin",
      ]).pipe(Effect.flip);
      expect(error._tag).toBe("BackgroundRelaySettingsInvalid");
      expect(JSON.stringify(error)).not.toContain(invalid);
      expect(output.join("\n")).not.toContain(invalid);
      expect(yield* fs.exists(secretPath)).toBe(false);
    }),
  );

  it.effect(
    "reads saved configuration despite partial or different CLI environment overrides",
    () =>
      Effect.gen(function* () {
        const { fs, run, runConnect, output, warnings, secretPath } = yield* setup;
        const input = vi
          .spyOn(process, "stdin", "get")
          .mockReturnValue(
            NodeStream.Readable.from([
              new TextEncoder().encode(credential),
            ]) as typeof process.stdin,
          );
        yield* Effect.addFinalizer(() => Effect.sync(() => input.mockRestore()));
        yield* run(["set", "--url", "https://saved.test", "--credential-stdin"], {
          T3CODE_BACKGROUND_RELAY_URL: "https://shell.test",
        });
        expect(warnings.join("\n")).toContain("take precedence");
        yield* run(["status", "--json"], { T3CODE_BACKGROUND_RELAY_URL: "https://shell.test" });
        expect(JSON.parse(output.at(-1) ?? "")).toMatchObject({
          state: "configured",
          url: "https://saved.test",
        });
        yield* runConnect(["status", "--json"], {
          T3CODE_BACKGROUND_RELAY_URL: "https://shell.test",
        });
        expect(JSON.parse(output.at(-1) ?? "").backgroundRelayUrl).toBe("https://saved.test");
        yield* fs.remove(secretPath);
        yield* run(["status", "--json"], { T3CODE_BACKGROUND_RELAY_URL: "https://shell.test" });
        expect(JSON.parse(output.at(-1) ?? "")).toMatchObject({ state: "unconfigured", url: null });
        yield* runConnect(["status", "--json"], {
          T3CODE_BACKGROUND_RELAY_URL: "https://shell.test",
        });
        expect(JSON.parse(output.at(-1) ?? "").backgroundRelayUrl).toBeNull();
        expect(output.join("\n")).not.toContain(credential);
      }),
  );
});
