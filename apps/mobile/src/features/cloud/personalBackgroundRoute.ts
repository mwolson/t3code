export type PersonalBackgroundRoute =
  | { readonly kind: "private"; readonly url: string }
  | {
      readonly kind: "disabled";
      readonly reason: "push-not-enabled" | "missing-url" | "invalid-url" | "primary-host";
    };

interface PersonalBackgroundExtra {
  readonly iosPushRelay?: unknown;
  readonly relay?: { readonly url?: unknown; readonly backgroundUrl?: unknown };
}

export function resolvePersonalBackgroundRoute(
  extra: PersonalBackgroundExtra | undefined,
): PersonalBackgroundRoute {
  if (extra?.iosPushRelay !== true) return { kind: "disabled", reason: "push-not-enabled" };
  const raw = extra.relay?.backgroundUrl;
  if (typeof raw !== "string" || !raw.trim()) return { kind: "disabled", reason: "missing-url" };
  try {
    const url = new URL(raw.trim());
    if (
      url.protocol !== "https:" ||
      url.pathname !== "/" ||
      url.href.includes("?") ||
      url.href.includes("#") ||
      url.username ||
      url.password
    ) {
      return { kind: "disabled", reason: "invalid-url" };
    }
    const hostname = url.hostname.toLowerCase().replace(/\.+$/u, "");
    if (!hostname) return { kind: "disabled", reason: "invalid-url" };
    const primaryUrl = extra.relay?.url;
    let primaryHostname: string | undefined;
    if (typeof primaryUrl === "string") {
      try {
        primaryHostname = new URL(primaryUrl.trim()).hostname.toLowerCase().replace(/\.+$/u, "");
      } catch {
        // An invalid primary URL cannot authorize a hosted background route.
      }
    }
    if (hostname === primaryHostname || hostname === "t3.codes" || hostname.endsWith(".t3.codes")) {
      return { kind: "disabled", reason: "primary-host" };
    }
    url.hostname = hostname;
    return { kind: "private", url: url.origin };
  } catch {
    return { kind: "disabled", reason: "invalid-url" };
  }
}
