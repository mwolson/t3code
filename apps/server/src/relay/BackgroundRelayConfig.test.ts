import { describe, expect, it } from "@effect/vitest";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as BackgroundRelayConfig from "./BackgroundRelayConfig.ts";

const load = (env: Record<string, string>) =>
  Effect.gen(function* () {
    return yield* BackgroundRelayConfig.BackgroundRelayConfig;
  }).pipe(
    Effect.provide(
      BackgroundRelayConfig.layer.pipe(
        Layer.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ env }))),
      ),
    ),
  );

const valid = {
  T3CODE_BACKGROUND_RELAY_URL: "https://background.example.test/",
  T3CODE_BACKGROUND_RELAY_ENVIRONMENT_CREDENTIAL: "test-credential",
};

describe("BackgroundRelayConfig", () => {
  it.effect("leaves existing relay configuration alone when unset", () =>
    Effect.gen(function* () {
      expect(yield* load({})).toBeNull();
    }),
  );
  it.effect("normalizes the origins and keeps the credential redacted", () =>
    Effect.gen(function* () {
      const config = yield* load(valid);
      expect(config?.url).toBe("https://background.example.test");
      expect(config?.issuer).toBe(config?.url);
      expect(config && Redacted.value(config.environmentCredential)).toBe("test-credential");
      expect(JSON.stringify(config)).not.toContain("test-credential");
      const withIssuer = yield* load({
        ...valid,
        T3CODE_BACKGROUND_RELAY_ISSUER: "https://issuer.example.test/",
      });
      expect(withIssuer?.issuer).toBe("https://issuer.example.test");
    }),
  );
  it.effect.each(
    Object.entries({
      "URL without credential": { T3CODE_BACKGROUND_RELAY_URL: valid.T3CODE_BACKGROUND_RELAY_URL },
      "insecure URL": { ...valid, T3CODE_BACKGROUND_RELAY_URL: "http://background.example.test" },
    }),
  )("rejects %s without disclosing credentials", ([_name, env]) =>
    Effect.gen(function* () {
      const error = yield* load(env).pipe(Effect.flip);
      expect(error._tag).toBe("BackgroundRelayConfigInvalid");
      expect(JSON.stringify(error)).not.toContain("test-credential");
    }),
  );
});
