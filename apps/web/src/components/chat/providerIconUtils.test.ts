import { ProviderDriverKind } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { OpenCode2Icon, OpenCodeIcon } from "../Icons";
import {
  DRIVER_OPTION_BY_VALUE,
  PROVIDER_CLIENT_DEFINITIONS,
} from "../settings/providerDriverMeta";
import {
  getProviderIconBadgeLabel,
  PROVIDER_ICON_BY_PROVIDER,
  resolveProviderModelPickerAriaLabel,
} from "./providerIconUtils";

describe("OpenCode provider icons", () => {
  it("uses a distinct icon component for OpenCode 2 in chat and settings surfaces", () => {
    expect(PROVIDER_ICON_BY_PROVIDER[ProviderDriverKind.make("opencode")]).toBe(OpenCodeIcon);
    expect(PROVIDER_ICON_BY_PROVIDER[ProviderDriverKind.make("opencode2")]).toBe(OpenCode2Icon);
    expect(DRIVER_OPTION_BY_VALUE[ProviderDriverKind.make("opencode2")]?.icon).toBe(OpenCode2Icon);
  });

  it("keeps chat and settings icon registries aligned", () => {
    for (const definition of PROVIDER_CLIENT_DEFINITIONS) {
      expect(PROVIDER_ICON_BY_PROVIDER[definition.value]).toBe(definition.icon);
    }
  });

  it("marks OpenCode 2 as Beta and leaves other drivers unmarked", () => {
    expect(getProviderIconBadgeLabel(ProviderDriverKind.make("opencode2"))).toBe("Beta");
    expect(getProviderIconBadgeLabel(ProviderDriverKind.make("opencode"))).toBeUndefined();
    expect(getProviderIconBadgeLabel(ProviderDriverKind.make("codex"))).toBeUndefined();
    expect(getProviderIconBadgeLabel(ProviderDriverKind.make("acpRegistry"))).toBeUndefined();
  });
});

describe("provider model picker accessibility", () => {
  it("preserves a caller-specific trigger label", () => {
    expect(
      resolveProviderModelPickerAriaLabel(
        "Source control writer model",
        "OpenCode 2.0 (Beta), Big Pickle",
      ),
    ).toBe("Source control writer model");
  });

  it("uses the generated provider and model label by default", () => {
    expect(resolveProviderModelPickerAriaLabel(undefined, "OpenCode 2.0 (Beta), Big Pickle")).toBe(
      "OpenCode 2.0 (Beta), Big Pickle",
    );
  });
});
