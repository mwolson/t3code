import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import { vi } from "vite-plus/test";

vi.mock("electron", () => ({
  safeStorage: {
    isEncryptionAvailable: () => true,
    encryptString: (value: string) => Buffer.from(value),
    decryptString: (value: Buffer) => value.toString(),
  },
}));

import { storage } from "@clerk/electron/storage";

const fileMode = (path: string) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const info = yield* fileSystem.stat(path);
    return info.mode & 0o777;
  });

describe("Clerk token storage", () => {
  it.effect("writes the token file readable and writable only by its owner", () =>
    Effect.gen(function* () {
      if ((yield* HostProcess.Platform) === "win32") return;
      const fileSystem = yield* FileSystem.FileSystem;
      const stateDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-clerk-tokens-" });

      yield* Effect.promise(async () => {
        await storage({ path: stateDir }).setItem("__clerk_client_jwt", "t");
      });

      assert.equal(yield* fileMode(`${stateDir}/clerk-tokens.json`), 0o600);
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
  );

  it.effect("tightens a token file left world-writable by an earlier version", () =>
    Effect.gen(function* () {
      if ((yield* HostProcess.Platform) === "win32") return;
      const fileSystem = yield* FileSystem.FileSystem;
      const stateDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-clerk-tokens-" });
      const tokenFile = `${stateDir}/clerk-tokens.json`;
      yield* fileSystem.writeFileString(tokenFile, "{}");
      yield* fileSystem.chmod(tokenFile, 0o666);

      storage({ path: stateDir });

      assert.equal(yield* fileMode(tokenFile), 0o600);
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
  );
});
