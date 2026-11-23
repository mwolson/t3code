import { describe, expect, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import { BackgroundRelayConfig } from "./BackgroundRelayConfig.ts";
import * as Settings from "./BackgroundRelaySettings.ts";

const credential = `t3env_${"a".repeat(64)}_${"b".repeat(96)}`;
const makeSettings = Effect.gen(function* () {
  const values = new Map<string, Uint8Array>();
  const writes: string[] = [];
  const secrets = ServerSecretStore.ServerSecretStore.of({
    get: (name) => Effect.sync(() => Option.fromUndefinedOr(values.get(name))),
    set: (name, bytes) =>
      Effect.sync(() => {
        writes.push(name);
        values.set(name, bytes);
      }),
    remove: (name) => Effect.sync(() => void values.delete(name)),
    create: () => Effect.die("unexpected create"),
    getOrCreateRandom: () => Effect.die("unexpected random"),
  });
  const settings = yield* Settings.make.pipe(
    Effect.provideService(ServerSecretStore.ServerSecretStore, secrets),
  );
  return { settings, values, writes };
});

describe("BackgroundRelaySettings", () => {
  it.effect("writes one versioned secret, normalizes origins, and removes only that secret", () =>
    Effect.gen(function* () {
      const { settings, values, writes } = yield* makeSettings;
      expect(yield* settings.read).toBeUndefined();
      yield* settings.set({
        url: "https://background.example.test/",
        environmentCredential: Redacted.make(`${credential}\n`),
      });
      expect(writes).toEqual([Settings.SECRET]);
      const stored = new TextDecoder().decode(values.get(Settings.SECRET));
      expect(JSON.parse(stored)).toEqual({
        version: 1,
        url: "https://background.example.test",
        issuer: "https://background.example.test",
        environmentCredential: credential,
      });
      const first = yield* settings.read;
      expect(first?.revision).toMatch(/^[0-9a-f]{64}$/u);
      expect(Object.isFrozen(first)).toBe(true);
      expect(JSON.stringify(first)).not.toContain(credential);
      expect((yield* settings.read)?.revision).toBe(first?.revision);
      yield* settings.set({
        url: "https://new.example.test",
        issuer: "https://issuer.example.test",
        environmentCredential: Redacted.make(credential),
      });
      expect((yield* settings.read)?.revision).not.toBe(first?.revision);
      expect(first?.url).toBe("https://background.example.test");
      values.set("primary", new Uint8Array([1]));
      yield* settings.clear;
      expect(yield* settings.read).toBeUndefined();
      expect([...values.keys()]).toEqual(["primary"]);
    }).pipe(Effect.provide(NodeCrypto.layer)),
  );

  it.effect("keeps environment overrides fixed ahead of the saved secret", () =>
    Effect.gen(function* () {
      const { settings } = yield* makeSettings;
      const before = yield* settings.read;
      yield* settings.set({
        url: "https://saved.example.test",
        environmentCredential: Redacted.make(credential),
      });
      expect(yield* settings.read).toBe(before);
      yield* settings.clear;
      expect(yield* settings.read).toBe(before);
      expect(before?.url).toBe("https://env.example.test");
    }).pipe(
      Effect.provideService(BackgroundRelayConfig, {
        url: "https://env.example.test",
        issuer: "https://env.example.test",
        environmentCredential: Redacted.make("env-credential"),
      }),
      Effect.provide(NodeCrypto.layer),
    ),
  );
});
