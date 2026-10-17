import type { MenuAction } from "@react-native-menu/menu";
import type { PullRequestChecksState, ThreadPullRequestLink } from "@t3tools/contracts";
import type { PlatformOSType } from "react-native";
import {
  resolveThreadPullRequestChains,
  threadPullRequestKeyOf,
  visibleThreadPullRequests,
} from "@t3tools/shared/threadPullRequests";

import type { AppSymbolName } from "../../components/ui/AppSymbol";
import type { ThreadPrPresentation } from "../../state/thread-pr-presentation";
import { pullRequestStatus } from "../pull-requests/pullRequestStatus";

interface ThreadPrHeaderGlyph {
  readonly icon: AppSymbolName;
  readonly tintClassName: string;
  readonly label: string | null;
}

export interface ThreadPrHeaderPresentation {
  readonly icon: AppSymbolName;
  readonly tintClassName: string;
  readonly textClassName: string;
  readonly text: string;
  readonly status: ThreadPrHeaderGlyph | null;
  readonly opensMenu: boolean;
  readonly accessibilityLabel: string;
}

const CHECKS_GLYPH = {
  passing: {
    icon: "checkmark.circle",
    tintClassName: "accent-adaptive-emerald-600-400",
    label: "checks passing",
  },
  failing: {
    icon: "xmark.circle.fill",
    tintClassName: "accent-danger-foreground",
    label: "checks failing",
  },
  pending: { icon: "clock", tintClassName: "accent-warning-foreground", label: "checks running" },
} as const satisfies Record<PullRequestChecksState, ThreadPrHeaderGlyph>;

const CONFLICT_GLYPH: ThreadPrHeaderGlyph = {
  icon: "exclamationmark.triangle",
  tintClassName: "accent-danger-foreground",
  label: "has conflicts",
};

// The accessibility label already names the pending or stack state, so these glyphs add no text.
const PENDING_GLYPH: ThreadPrHeaderGlyph = {
  icon: "ellipsis.circle",
  tintClassName: "accent-icon-muted",
  label: null,
};

type PullRequestState = ReturnType<typeof pullRequestStatus>;

// Mirrors the desktop thread panel row: a conflict replaces the checks rollup, and drafts hide it.
// Stacks and unsynced links add a state shape so grayscale screens do not rely on tint alone.
function statusGlyph(
  pr: ThreadPrPresentation,
  state: PullRequestState | null,
): ThreadPrHeaderGlyph | null {
  if (state === null) return PENDING_GLYPH;
  if (pr.kind === "stack") return { icon: state.icon, tintClassName: state.tint, label: null };
  if (pr.others > 0) return null;
  if (pr.state === "open" && pr.mergeability === "conflicting") return CONFLICT_GLYPH;
  if (pr.isDraft || pr.checksState === null) return null;
  return CHECKS_GLYPH[pr.checksState];
}

export function presentThreadPrHeader(pr: ThreadPrPresentation): ThreadPrHeaderPresentation {
  const opensMenu = pr.kind === "stack" || pr.others > 0;
  const state =
    pr.state === null ? null : pullRequestStatus({ state: pr.state, isDraft: pr.isDraft });
  const status = statusGlyph(pr, state);
  return {
    icon: pr.kind === "stack" ? "square.3.layers.3d" : (state?.icon ?? "arrow.triangle.pull"),
    tintClassName: state?.tint ?? "accent-icon-muted",
    textClassName: state?.text ?? "text-foreground-muted",
    text: opensMenu ? pr.label : `#${pr.number}`,
    status,
    opensMenu,
    accessibilityLabel:
      status === null || status.label === null
        ? pr.accessibilityLabel
        : `${pr.accessibilityLabel}, ${status.label}`,
  };
}

export interface ThreadPrMenuEntry {
  readonly id: string;
  readonly url: string;
  readonly action: MenuAction;
}

function activityAt(link: ThreadPullRequestLink): number {
  const ms = Date.parse(link.snapshot?.updatedAt ?? link.linkedAt);
  return Number.isNaN(ms) ? 0 : ms;
}

function menuSubtitle(link: ThreadPullRequestLink): string | undefined {
  const snapshot = link.snapshot;
  if (snapshot === null) return undefined;
  const parts: string[] = [pullRequestStatus(snapshot).label];
  if (snapshot.state === "open" && snapshot.mergeability === "conflicting") {
    parts.push("Has conflicts");
  } else if (!snapshot.isDraft && snapshot.checksState) {
    parts.push(`Checks ${snapshot.checksState}`);
  }
  return parts.join(" · ");
}

// Native iOS menus load images as SF Symbols; the draft and closed aliases exist only in Tabler.
const IOS_MENU_IMAGES: Readonly<Record<string, string>> = {
  "git.pull-request.closed": "xmark.circle",
  "git.pull-request.draft": "circle.dashed",
};

function menuImage(link: ThreadPullRequestLink, platform: PlatformOSType): string {
  const icon =
    link.snapshot === null ? "arrow.triangle.pull" : pullRequestStatus(link.snapshot).icon;
  return platform === "ios" ? (IOS_MENU_IMAGES[icon] ?? icon) : icon;
}

/** Same order as the desktop pull request list: most recently active chain first, base layer up. */
export function threadPrMenuEntries(
  links: ReadonlyArray<ThreadPullRequestLink>,
  platform: PlatformOSType,
): ReadonlyArray<ThreadPrMenuEntry> {
  const chains = [...resolveThreadPullRequestChains(visibleThreadPullRequests(links))].sort(
    (left, right) =>
      Math.max(...right.layers.map(activityAt)) - Math.max(...left.layers.map(activityAt)),
  );
  return chains.flatMap((chain) =>
    chain.layers.map((link) => {
      const id = threadPullRequestKeyOf(link);
      const subtitle = menuSubtitle(link);
      return {
        id,
        url: link.url,
        action: {
          id,
          title: `#${link.number} ${link.snapshot?.title ?? link.repository}`,
          image: menuImage(link, platform),
          ...(subtitle === undefined ? {} : { subtitle }),
        },
      };
    }),
  );
}
