import { ProviderDriverKind } from "@t3tools/contracts";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vite-plus/test";

import { ProviderInstanceIcon } from "./ProviderInstanceIcon";

const OPENCODE = ProviderDriverKind.make("opencode");
const OPENCODE2 = ProviderDriverKind.make("opencode2");

function kindBadge(markup: string): { className: string; text: string } | null {
  const match = markup.match(
    /<span class="([^"]*)" data-provider-kind-badge="[^"]*"[^>]*>([^<]*)</,
  );
  return match ? { className: match[1]!, text: match[2]! } : null;
}

describe("ProviderInstanceIcon kind badge", () => {
  it("marks OpenCode 2 as Beta so it stays distinguishable from OpenCode 1", () => {
    const markup = renderToStaticMarkup(
      <ProviderInstanceIcon
        driverKind={OPENCODE2}
        displayName="OpenCode 2.0"
        showKindBadge
        kindBadgeVariant="full"
      />,
    );

    expect(kindBadge(markup)?.text).toBe("Beta");
    // Absolutely positioned, so the control's layout size does not change.
    expect(kindBadge(markup)?.className).toContain("absolute");
  });

  it("shows the version number on compact surfaces, where the word would not fit", () => {
    const markup = renderToStaticMarkup(
      <ProviderInstanceIcon
        driverKind={OPENCODE2}
        displayName="OpenCode 2.0"
        showKindBadge
        kindBadgeVariant="compact"
      />,
    );

    expect(kindBadge(markup)?.text).toBe("2");
    // The full label stays on the element for callers and DOM assertions.
    expect(markup).toContain('data-provider-kind-badge="Beta"');
  });

  it("stays decorative: the owning control names the marker", () => {
    const markup = renderToStaticMarkup(
      <ProviderInstanceIcon driverKind={OPENCODE2} displayName="OpenCode 2.0" showKindBadge />,
    );

    expect(kindBadge(markup)?.className).toBeDefined();
    expect(markup).toContain('aria-hidden="true"');
    // A bare "Beta" read out with no context would be worse than silence.
    expect(markup).not.toContain("sr-only");
  });

  it("keeps the kind marker clear of the custom instance badge", () => {
    const markup = renderToStaticMarkup(
      <ProviderInstanceIcon
        driverKind={OPENCODE2}
        displayName="OpenCode Personal"
        accentColor="#3355ff"
        showBadge
        showKindBadge
        kindBadgeVariant="full"
      />,
    );

    // Instance initials keep the bottom-right corner they already owned…
    expect(markup).toContain("OP");
    expect(markup).toMatch(/class="[^"]*bottom-0[^"]*"[^>]*style="border-color/);
    // …so the driver marker takes the top-right one.
    const badge = kindBadge(markup);
    expect(badge?.text).toBe("Beta");
    expect(badge?.className).toContain("-top-1");
    expect(badge?.className).not.toContain("bottom");
  });

  it("leaves OpenCode 1 unmarked", () => {
    const markup = renderToStaticMarkup(
      <ProviderInstanceIcon driverKind={OPENCODE} displayName="OpenCode" showKindBadge />,
    );

    expect(markup).not.toContain("data-provider-kind-badge");
    expect(markup).toContain('data-provider-icon="opencode"');
  });

  it("stays opt-in so dense surfaces are unaffected", () => {
    const markup = renderToStaticMarkup(
      <ProviderInstanceIcon driverKind={OPENCODE2} displayName="OpenCode 2.0" />,
    );

    expect(markup).not.toContain("data-provider-kind-badge");
  });

  it("renders the brand mark rather than an initials fallback", () => {
    const markup = renderToStaticMarkup(
      <ProviderInstanceIcon driverKind={OPENCODE2} displayName="OpenCode 2.0" showKindBadge />,
    );

    expect(markup).toContain('data-provider-icon="opencode2"');
    expect(markup).not.toContain(">O2<");
  });
});
