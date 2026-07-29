export type ProviderIconKind = "claude" | "opencode" | "opencode2" | "openai";

export function providerIconKind(provider: string | null | undefined): ProviderIconKind {
  switch (provider) {
    case "claudeAgent":
      return "claude";
    case "opencode":
      return "opencode";
    case "opencode2":
      return "opencode2";
    default:
      return "openai";
  }
}

/**
 * Corner marker for kinds that share another kind's brand mark. Mirrors the
 * web client's `iconBadgeLabel` provider metadata.
 */
export function providerIconBetaLabel(kind: ProviderIconKind): string | undefined {
  return kind === "opencode2" ? "Beta" : undefined;
}

/**
 * What the marker actually draws. These icons render at 14-16px, where the
 * whole word is unreadable, so it degrades to the version number rather than a
 * dot that could be mistaken for a status indicator. The owning row or control
 * names the provider in full.
 */
export function providerIconBetaMarker(kind: ProviderIconKind): string | undefined {
  return kind === "opencode2" ? "2" : undefined;
}
