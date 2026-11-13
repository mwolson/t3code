import { DEFAULT_HOSTED_APP_URL } from "@t3tools/shared/connectAuth";
import appleAppSiteAssociation from "../web/public/.well-known/apple-app-site-association.json";
import assetLinks from "../web/public/.well-known/assetlinks.json";
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

const personalBundleIdentifier = "com.example.t3code.personal";
const personalEnv = {
  T3CODE_IOS_PERSONAL_TEAM: "1",
  T3CODE_IOS_PERSONAL_TEAM_BUNDLE_ID: personalBundleIdentifier,
};

describe("mobile app configuration", () => {
  it("signs personal builds and their extensions under the personal team", async () => {
    const config = await evaluateConfig({
      ...personalEnv,
      T3CODE_IOS_TEAM_ID: "726ZUB39ZR",
      T3CODE_IOS_PERSONAL_PUSH_RELAY: "1",
      T3CODE_RELAY_URL: "https://primary.example.test",
      T3CODE_BACKGROUND_RELAY_URL: "https://private.example.test",
    });

    expect(config.updates?.enabled).toBe(false);
    expect(config.ios?.appleTeamId).toBe("726ZUB39ZR");
    expect(config.ios).not.toHaveProperty("associatedDomains");
    expect(config.ios?.entitlements).toEqual({
      "keychain-access-groups": [`$(AppIdentifierPrefix)${personalBundleIdentifier}`],
    });
    expect(config.extra?.iosPushRelay).toBe(true);
    expect(plugin(config, "./plugins/withWidgetLogoAsset.cjs")).toBeDefined();
    expect(plugin(config, "./plugins/withShareExtensionDisplayName.cjs")).toBeDefined();
    expect(plugin(config, "expo-widgets")).toEqual([
      "expo-widgets",
      expect.objectContaining({
        bundleIdentifier: `${personalBundleIdentifier}.widgets`,
        groupIdentifier: `group.${personalBundleIdentifier}`,
      }),
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
  });

  it.each([undefined, "https://primary.example.test", "https://relay.t3.codes"])(
    "rejects personal push without a private background origin (%j)",
    async (backgroundUrl) => {
      await expect(
        evaluateConfig({
          ...personalEnv,
          T3CODE_IOS_PERSONAL_PUSH_RELAY: "1",
          T3CODE_RELAY_URL: "https://primary.example.test",
          T3CODE_BACKGROUND_RELAY_URL: backgroundUrl,
        }),
      ).rejects.toThrow("requires T3CODE_BACKGROUND_RELAY_URL");
    },
  );

  it("rejects an invalid personal signing team", async () => {
    await expect(
      evaluateConfig({ ...personalEnv, T3CODE_IOS_TEAM_ID: "726zub39zr" }),
    ).rejects.toThrow("T3CODE_IOS_TEAM_ID must be ten uppercase letters or digits.");
  });
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

async function loadConfig(variant: string | undefined) {
  return evaluateConfig({ APP_VARIANT: variant });
}

it.each(["prod", ""])("rejects APP_VARIANT=%j before selecting production IDs", async (value) => {
  await expect(loadConfig(value)).rejects.toThrow(
    "APP_VARIANT must be development, preview, or production.",
  );
});

it.each([
  [undefined, "com.t3tools.t3code"],
  ["development", "com.t3tools.t3code.dev"],
  ["preview", "com.t3tools.t3code.preview"],
  ["production", "com.t3tools.t3code"],
])("keeps the iOS and Android identifiers for APP_VARIANT=%j", async (value, identifier) => {
  const config = await loadConfig(value);
  expect(config.ios?.bundleIdentifier).toBe(identifier);
  expect(config.android?.package).toBe(identifier);
  // Copied thread links open this app only while app.t3.codes lists it.
  const host = new URL(DEFAULT_HOSTED_APP_URL).host;
  expect(config.ios?.associatedDomains).toContain(`applinks:${host}`);
  expect(appleAppSiteAssociation.applinks.details[0]?.appIDs).toContain(
    `${config.ios?.appleTeamId}.${identifier}`,
  );
  expect(config.android?.intentFilters?.[0]?.data).toEqual([
    { scheme: "https", host, pathPrefix: "/threads/" },
  ]);
  expect(assetLinks.map((link) => link.target.package_name)).toContain(identifier);
});
