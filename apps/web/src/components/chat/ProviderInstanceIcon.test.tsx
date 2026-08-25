import { ProviderDriverKind } from "@t3tools/contracts";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vite-plus/test";

import { ProviderInstanceIcon } from "./ProviderInstanceIcon";

const OPENCODE = ProviderDriverKind.make("opencode");

describe("ProviderInstanceIcon OpenCode marks", () => {
  it("renders the OpenCode mark without an icon badge", () => {
    const opencode = renderToStaticMarkup(
      <ProviderInstanceIcon driverKind={OPENCODE} displayName="OpenCode" />,
    );

    expect(opencode).toContain('viewBox="0 0 32 40"');
    expect(opencode).not.toContain("data-provider-kind-badge");
  });

  it("keeps custom instance badges independent of the provider mark", () => {
    const markup = renderToStaticMarkup(
      <ProviderInstanceIcon
        driverKind={OPENCODE}
        displayName="OpenCode Personal"
        accentColor="#3355ff"
        showBadge
      />,
    );

    expect(markup).toContain("OP");
    expect(markup).toMatch(/class="[^"]*bottom-0[^"]*"[^>]*style="border-color/);
    expect(markup).not.toContain("data-provider-kind-badge");
  });
});
