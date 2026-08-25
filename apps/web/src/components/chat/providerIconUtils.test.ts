import { ProviderDriverKind } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { OpenCodeIcon } from "../Icons";
import { PROVIDER_CLIENT_DEFINITIONS } from "../settings/providerDriverMeta";
import {
  PROVIDER_ICON_BY_PROVIDER,
  resolveProviderModelPickerAriaLabel,
} from "./providerIconUtils";

describe("OpenCode provider icons", () => {
  it("uses the OpenCode mark", () => {
    expect(PROVIDER_ICON_BY_PROVIDER[ProviderDriverKind.make("opencode")]).toBe(OpenCodeIcon);
  });

  it("keeps chat and settings icon registries aligned", () => {
    for (const definition of PROVIDER_CLIENT_DEFINITIONS) {
      expect(PROVIDER_ICON_BY_PROVIDER[definition.value]).toBe(definition.icon);
    }
  });
});

describe("provider model picker accessibility", () => {
  it("preserves a caller-specific trigger label", () => {
    expect(
      resolveProviderModelPickerAriaLabel("Source control writer model", "OpenCode, Big Pickle"),
    ).toBe("Source control writer model");
  });

  it("uses the generated provider and model label by default", () => {
    expect(resolveProviderModelPickerAriaLabel(undefined, "OpenCode, Big Pickle")).toBe(
      "OpenCode, Big Pickle",
    );
  });
});
