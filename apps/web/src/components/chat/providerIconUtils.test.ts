import { ProviderDriverKind } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { OpenCode2Icon, OpenCodeIcon } from "../Icons";
import {
  DRIVER_OPTION_BY_VALUE,
  PROVIDER_CLIENT_DEFINITIONS,
} from "../settings/providerDriverMeta";
import { PROVIDER_ICON_BY_PROVIDER } from "./providerIconUtils";

describe("OpenCode provider icons", () => {
  it("uses the versioned icon for OpenCode 2 in chat and settings surfaces", () => {
    expect(PROVIDER_ICON_BY_PROVIDER[ProviderDriverKind.make("opencode")]).toBe(OpenCodeIcon);
    expect(PROVIDER_ICON_BY_PROVIDER[ProviderDriverKind.make("opencode2")]).toBe(OpenCode2Icon);
    expect(DRIVER_OPTION_BY_VALUE[ProviderDriverKind.make("opencode2")]?.icon).toBe(OpenCode2Icon);
  });

  it("keeps chat and settings icon registries aligned", () => {
    for (const definition of PROVIDER_CLIENT_DEFINITIONS) {
      expect(PROVIDER_ICON_BY_PROVIDER[definition.value]).toBe(definition.icon);
    }
  });
});
