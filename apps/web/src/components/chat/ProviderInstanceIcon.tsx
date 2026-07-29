import { type CSSProperties, memo } from "react";
import { type ProviderDriverKind } from "@t3tools/contracts";

import { getProviderIconBadgeLabel, PROVIDER_ICON_BY_PROVIDER } from "./providerIconUtils";
import { cn } from "~/lib/utils";

/**
 * Corner marker used where two drivers share one brand mark (OpenCode and
 * OpenCode 2). Absolutely positioned so it never changes the control's
 * layout size. It sits at the *top* right because the bottom right is
 * already the instance badge's corner (accent / initials for custom
 * instances) — both have to stay readable on the same glyph.
 */
const KIND_BADGE_BASE_CLASS =
  "pointer-events-none absolute -top-1 z-20 rounded-[3px] border border-amber-500/40 bg-amber-100 font-bold uppercase leading-[1.5] tracking-tight text-amber-800 shadow-sm dark:border-amber-400/35 dark:bg-amber-950 dark:text-amber-200";
/** Roomy surfaces (picker rail) can carry the whole word. */
const KIND_BADGE_FULL_CLASS = `${KIND_BADGE_BASE_CLASS} -right-1.5 px-[2px] text-[6px]`;
/**
 * The composer trigger renders at 16px next to the model label, where a
 * four-letter pill is both unreadable and liable to run into the text, so it
 * degrades to the version number. The owning control names the Beta status in
 * full.
 */
const KIND_BADGE_COMPACT_CLASS = `${KIND_BADGE_BASE_CLASS} -right-1 px-[1.5px] text-[7px]`;

export function providerInstanceInitials(label: string): string {
  const words = label.replace(/[_-]+/g, " ").split(/\s+/u).filter(Boolean);
  if (words.length === 0) return "";
  if (words.length === 1) return words[0]!.slice(0, 2).toUpperCase();
  return words
    .slice(0, 2)
    .map((word) => word[0]?.toUpperCase() ?? "")
    .join("");
}

export const ProviderInstanceIcon = memo(function ProviderInstanceIcon(props: {
  driverKind: ProviderDriverKind;
  displayName: string;
  accentColor?: string | undefined;
  showBadge?: boolean;
  badgeContent?: "initials" | "none";
  /**
   * Render the driver's corner marker (e.g. OpenCode 2's "Beta"). Opt-in so
   * dense surfaces like sidebar thread rows stay uncluttered; identity and
   * selection surfaces turn it on. The marker is decorative — the calling
   * control owns the accessible name and must spell the marker out there.
   */
  showKindBadge?: boolean;
  /** "full" spells the marker out; "compact" shows the version number. */
  kindBadgeVariant?: "full" | "compact";
  className?: string;
  iconClassName?: string;
  badgeClassName?: string;
  statusDotClassName?: string;
  indicatorBackground?: string;
}) {
  const Icon = PROVIDER_ICON_BY_PROVIDER[props.driverKind] ?? null;
  const indicatorBackground = props.indicatorBackground ?? "var(--card)";
  const accentStyle = props.accentColor
    ? ({ "--provider-accent": props.accentColor } as CSSProperties)
    : undefined;
  const badgeContent = props.badgeContent ?? "initials";
  const kindBadgeLabel = props.showKindBadge
    ? getProviderIconBadgeLabel(props.driverKind)
    : undefined;
  const isCompactKindBadge = props.kindBadgeVariant === "compact";

  return (
    <span
      className={cn(
        "relative isolate inline-flex shrink-0 items-center justify-center overflow-visible",
        props.className,
      )}
      style={accentStyle}
      data-provider-accent-color={props.accentColor}
    >
      {Icon ? (
        <Icon className={cn("size-5 shrink-0", props.iconClassName)} aria-hidden />
      ) : (
        <span className={cn("text-[10px] font-semibold leading-none", props.iconClassName)}>
          {providerInstanceInitials(props.displayName)}
        </span>
      )}
      {props.statusDotClassName ? (
        <span
          className={cn(
            "pointer-events-none absolute -left-0.5 -top-0.5 z-10 size-2 rounded-full",
            props.statusDotClassName,
          )}
          style={{ boxShadow: `0 0 0 2px ${indicatorBackground}` }}
          aria-hidden
        />
      ) : null}
      {props.showBadge ? (
        <span
          className={cn(
            "pointer-events-none absolute right-0 bottom-0 z-10 flex h-3.5 min-w-3.5 items-center justify-center rounded-full border px-0.5 text-[8px] font-semibold leading-none shadow-sm",
            props.accentColor
              ? "bg-[var(--provider-accent)] text-white"
              : "bg-muted text-muted-foreground",
            props.badgeClassName,
          )}
          style={{ borderColor: indicatorBackground }}
          aria-hidden
        >
          {badgeContent === "initials" ? providerInstanceInitials(props.displayName) : null}
        </span>
      ) : null}
      {kindBadgeLabel ? (
        <span
          className={isCompactKindBadge ? KIND_BADGE_COMPACT_CLASS : KIND_BADGE_FULL_CLASS}
          data-provider-kind-badge={kindBadgeLabel}
          aria-hidden
        >
          {isCompactKindBadge ? "2" : kindBadgeLabel}
        </span>
      ) : null}
    </span>
  );
});
