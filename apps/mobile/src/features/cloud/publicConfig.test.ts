import { describe, expect, it, vi } from "vite-plus/test";

import { BackgroundRelayUrlInvalidError, resolveCloudPublicConfig } from "./publicConfig";

vi.mock("expo-constants", () => ({
  default: {
    expoConfig: {
      extra: {},
    },
  },
}));

describe("resolveCloudPublicConfig", () => {
  it("normalizes an independent background relay without changing T3 Connect", () => {
    expect(
      resolveCloudPublicConfig({
        relay: {
          url: "https://relay.example.test",
          backgroundUrl: " https://background.example.test/// ",
        },
      }).relay,
    ).toEqual({
      url: "https://relay.example.test",
      backgroundUrl: "https://background.example.test",
    });
  });

  it("rejects invalid background configuration instead of silently using the primary relay", () => {
    expect(() =>
      resolveCloudPublicConfig({ relay: { backgroundUrl: "http://background.example.test" } }),
    ).toThrow(BackgroundRelayUrlInvalidError);
  });
  it("rejects an insecure relay URL", () => {
    expect(
      resolveCloudPublicConfig({
        clerk: { publishableKey: "pk_test_example", jwtTemplate: "t3-relay" },
        relay: { url: "http://relay.example.test" },
      }),
    ).toEqual({
      clerk: {
        publishableKey: "pk_test_example",
        jwtTemplate: "t3-relay",
      },
      relay: {
        url: null,
        backgroundUrl: null,
      },
      observability: {
        tracesUrl: null,
        tracesDataset: null,
        tracesToken: null,
      },
    });
  });
});
