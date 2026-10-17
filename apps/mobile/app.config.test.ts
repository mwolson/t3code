import type { ExpoConfig } from "expo/config";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

const { loadRepoEnv } = vi.hoisted(() => ({
  loadRepoEnv: vi.fn<() => Record<string, string | undefined>>(),
}));

vi.mock("../../scripts/lib/public-config.ts", () => ({ loadRepoEnv }));

afterEach(() => {
  vi.resetModules();
  loadRepoEnv.mockReset();
});

const variants = [
  { value: undefined, name: "T3 Code", suffix: "", policy: "fingerprint" },
  { value: "production", name: "T3 Code", suffix: "", policy: "fingerprint" },
  { value: "development", name: "T3 Code Dev", suffix: ".dev", policy: "appVersion" },
  { value: "preview", name: "T3 Code Preview", suffix: ".preview", policy: "fingerprint" },
] as const;

const personalBundleIdentifier = "com.example.t3code.personal";

describe("mobile app configuration", () => {
  describe.each(
    variants.map((variant) => ({ ...variant, label: variant.value ?? "default release" })),
  )("$label", (variant) => {
    it("retains the standard identity and team capabilities by default", async () => {
      const config = await evaluateConfig({ APP_VARIANT: variant.value });
      const bundleIdentifier = `com.t3tools.t3code${variant.suffix}`;

      expect(config.name).toBe(variant.name);
      expect(config.scheme).toBe(`t3code${variant.suffix.replace(".", "-")}`);
      expect(config.runtimeVersion).toEqual({ policy: variant.policy });
      expect(config.ios).toMatchObject({
        bundleIdentifier,
        appleTeamId: "ARK85ZXQ4Z",
        associatedDomains: ["applinks:clerk.t3.codes", "webcredentials:clerk.t3.codes"],
        entitlements: {
          "keychain-access-groups": [`$(AppIdentifierPrefix)${bundleIdentifier}`],
        },
      });
      expect(config.android?.package).toBe(bundleIdentifier);
      expect(config.extra?.iosPersonalTeamBuild).toBe(false);
      expect(plugin(config, "@clerk/expo")).toEqual([
        "@clerk/expo",
        { theme: "./clerk-theme.json", appleSignIn: true },
      ]);
      expect(plugin(config, "expo-widgets")).toEqual([
        "expo-widgets",
        expect.objectContaining({
          bundleIdentifier: `${bundleIdentifier}.widgets`,
          groupIdentifier: `group.${bundleIdentifier}`,
          enablePushNotifications: true,
          frequentUpdates: true,
        }),
      ]);
      expect(plugin(config, "expo-sharing")).toEqual([
        "expo-sharing",
        expect.objectContaining({
          ios: expect.objectContaining({
            enabled: true,
            extensionBundleIdentifier: `${bundleIdentifier}.sharing`,
            appGroupId: `group.${bundleIdentifier}`,
          }),
        }),
      ]);
      const names = pluginNames(config);
      expect(names.indexOf("./plugins/withWidgetLogoAsset.cjs")).toBeLessThan(
        names.indexOf("expo-widgets"),
      );
      expect(names.indexOf("./plugins/withShareExtensionDisplayName.cjs")).toBeLessThan(
        names.indexOf("expo-sharing"),
      );
      expect(plugin(config, "./plugins/withoutIosPersonalTeamCapabilities.cjs")).toBeUndefined();
    });

    it.each(["", "0", "true"])(
      "requires explicit opt-in, not flag %j or a bundle override alone",
      async (flag) => {
        const standard = await evaluateConfig({ APP_VARIANT: variant.value });
        const flagOff = await evaluateConfig({
          APP_VARIANT: variant.value,
          T3CODE_IOS_PERSONAL_TEAM: flag,
          T3CODE_IOS_TEAM_ID: "726ZUB39ZR",
          T3CODE_IOS_PERSONAL_PUSH_RELAY: "1",
          T3CODE_IOS_PERSONAL_TEAM_BUNDLE_ID: "invalid bundle ignored without opt-in",
        });
        expect(flagOff).toEqual(standard);
        expect(
          await evaluateConfig({
            APP_VARIANT: variant.value,
            T3CODE_IOS_PERSONAL_TEAM_BUNDLE_ID: personalBundleIdentifier,
          }),
        ).toEqual(standard);
      },
    );

    it.each([undefined, "0", "true", "1"])(
      "keeps personal extensions with push relay flag %j",
      async (pushRelay) => {
        const standard = await evaluateConfig({ APP_VARIANT: variant.value });
        const config = await evaluateConfig({
          APP_VARIANT: variant.value,
          T3CODE_IOS_PERSONAL_TEAM: "1",
          T3CODE_IOS_PERSONAL_TEAM_BUNDLE_ID: `  ${personalBundleIdentifier}  `,
          T3CODE_IOS_PERSONAL_PUSH_RELAY: pushRelay,
        });

        expect(config.ios?.bundleIdentifier).toBe(personalBundleIdentifier);
        expect(config.ios?.appleTeamId).toBeUndefined();
        expect(config.ios).not.toHaveProperty("associatedDomains");
        expect(config.ios?.entitlements).toEqual({
          "keychain-access-groups": [`$(AppIdentifierPrefix)${personalBundleIdentifier}`],
        });
        const expectedIos = {
          ...standard.ios,
          appleTeamId: undefined,
          bundleIdentifier: personalBundleIdentifier,
          entitlements: {
            "keychain-access-groups": [`$(AppIdentifierPrefix)${personalBundleIdentifier}`],
          },
        };
        delete expectedIos.associatedDomains;
        expect(config.ios).toEqual(expectedIos);
        expect(config.extra?.iosPersonalTeamBuild).toBe(true);
        expect(config.extra?.iosPushRelay).toBe(pushRelay === "1");
        expect(pluginNames(config)).toEqual([
          ...pluginNames(standard),
          "./plugins/withoutIosPersonalTeamCapabilities.cjs",
        ]);
        expect(plugin(config, "expo-widgets")).toEqual([
          "expo-widgets",
          {
            ...pluginOptions(standard, "expo-widgets"),
            bundleIdentifier: `${personalBundleIdentifier}.widgets`,
            groupIdentifier: `group.${personalBundleIdentifier}`,
          },
        ]);
        expect(plugin(config, "./plugins/withWidgetLogoAsset.cjs")).toBeDefined();
        expect(plugin(config, "./plugins/withShareExtensionDisplayName.cjs")).toBeDefined();
        expect(plugin(config, "@clerk/expo")).toEqual([
          "@clerk/expo",
          { theme: "./clerk-theme.json", appleSignIn: false },
        ]);
        expect(plugin(config, "expo-sharing")).toEqual([
          "expo-sharing",
          expect.objectContaining({
            ios: expect.objectContaining({
              enabled: true,
              extensionBundleIdentifier: `${personalBundleIdentifier}.sharing`,
              appGroupId: `group.${personalBundleIdentifier}`,
            }),
          }),
        ]);
        expect(plugin(config, "./plugins/withoutIosPersonalTeamCapabilities.cjs")).toBeDefined();
      },
    );

    it("leaves Android values and plugins unchanged by the iOS opt-in", async () => {
      const env = {
        APP_VARIANT: variant.value,
        T3CODE_ANDROID_GOOGLE_SERVICES_FILE: "./fixture-google-services.json",
      };
      const standard = await evaluateConfig(env);
      const personal = await evaluateConfig({
        ...env,
        T3CODE_IOS_PERSONAL_TEAM: "1",
        T3CODE_IOS_PERSONAL_TEAM_BUNDLE_ID: personalBundleIdentifier,
      });

      expect(personal.android).toEqual(standard.android);
      expect(personal.android?.googleServicesFile).toBe(env.T3CODE_ANDROID_GOOGLE_SERVICES_FILE);
      expect(androidPlugins(personal)).toEqual(androidPlugins(standard));
      expect(plugin(personal, "expo-notifications")).toEqual(
        plugin(standard, "expo-notifications"),
      );
      expect(plugin(personal, "expo-quick-actions")).toEqual(
        plugin(standard, "expo-quick-actions"),
      );
    });
  });

  it("uses the personal signing team supplied by the build", async () => {
    const config = await evaluateConfig({
      T3CODE_IOS_PERSONAL_TEAM: "1",
      T3CODE_IOS_PERSONAL_TEAM_BUNDLE_ID: personalBundleIdentifier,
      T3CODE_IOS_TEAM_ID: "726ZUB39ZR",
    });
    expect(config.ios?.appleTeamId).toBe("726ZUB39ZR");
  });

  it.each(["", "  ", "726ZUB39Z", "726ZUB39ZRR", "726zub39zr", "726ZUB39Z!"])(
    "rejects invalid personal signing team %j",
    async (teamId) => {
      await expect(
        evaluateConfig({
          T3CODE_IOS_PERSONAL_TEAM: "1",
          T3CODE_IOS_PERSONAL_TEAM_BUNDLE_ID: personalBundleIdentifier,
          T3CODE_IOS_TEAM_ID: teamId,
        }),
      ).rejects.toThrow("T3CODE_IOS_TEAM_ID must be ten uppercase letters or digits.");
    },
  );

  it.each([undefined, "", "  ", "singleword", "com..example", "com.example_bad", "com.example.*"])(
    "rejects missing or invalid personal bundle identifier %j",
    async (bundleIdentifier) => {
      await expect(
        evaluateConfig({
          T3CODE_IOS_PERSONAL_TEAM: "1",
          T3CODE_IOS_PERSONAL_TEAM_BUNDLE_ID: bundleIdentifier,
        }),
      ).rejects.toThrow(
        "T3CODE_IOS_PERSONAL_TEAM_BUNDLE_ID must be a reverse-DNS identifier such as com.example.t3code when T3CODE_IOS_PERSONAL_TEAM=1.",
      );
    },
  );
});

async function evaluateConfig(env: Record<string, string | undefined>) {
  const originalEnv = process.env;
  // Evaluate the real module afresh without reading repository env files or host values.
  process.env = { NODE_ENV: "test" };
  loadRepoEnv.mockReturnValue(env);
  vi.resetModules();
  try {
    return (await import("./app.config.ts")).default;
  } finally {
    process.env = originalEnv;
  }
}

function plugin(config: ExpoConfig, name: string) {
  return config.plugins?.find((entry) => (Array.isArray(entry) ? entry[0] : entry) === name);
}

function pluginNames(config: ExpoConfig) {
  return config.plugins?.map((entry) => (Array.isArray(entry) ? entry[0] : entry)) ?? [];
}

function pluginOptions(config: ExpoConfig, name: string) {
  const entry = plugin(config, name);
  return Array.isArray(entry) ? entry[1] : undefined;
}

function androidPlugins(config: ExpoConfig) {
  return config.plugins?.flatMap<{ name: string; android?: unknown }>((entry) => {
    if (typeof entry === "string") {
      return entry.startsWith("./plugins/withAndroid") ? [{ name: entry }] : [];
    }
    if (entry[0] === undefined || entry[1]?.android === undefined) return [];
    return [{ name: entry[0], android: entry[1].android }];
  });
}
