import { PullRequestGlyph } from "../pull-requests/pullRequestGlyphs";
import type { MenuAction } from "@react-native-menu/menu";
import type { PullRequestChecksState, ThreadPullRequestLink } from "@t3tools/contracts";
import type { PlatformOSType } from "react-native";
import {
  resolveThreadPullRequestChains,
  threadPullRequestKeyOf,
  visibleThreadPullRequests,
} from "@t3tools/client-runtime/thread-pull-requests";

import type { AppSymbolName } from "../../components/ui/AppSymbol";
import type { ThreadPrPresentation } from "../../state/thread-pr-presentation";
import { pullRequestStatus } from "../pull-requests/pullRequestStatus";

interface ThreadPrHeaderGlyph {
  readonly icon: AppSymbolName;
  readonly tintClassName: string;
}

/**
 * `compact` fits a narrow header beside Terminal, Files and Git, where a wider button makes
 * UIKit hide the title. It drops a single pull request's number, which stays in the
 * accessibility label and on the screen a tap opens, and shows a stack as its state glyph
 * and layer count.
 */
export type ThreadPrHeaderForm = "regular" | "compact";

// Android's header also moves its actions into overflow below this width.
const REGULAR_FORM_MIN_PANE_WIDTH = 600;

/**
 * Takes the width of the pane the header spans rather than the window's: a split layout
 * can still leave a phone-sized detail pane, such as the Duo inner display in portrait.
 */
export function threadPrHeaderForm(paneWidth: number): ThreadPrHeaderForm {
  return paneWidth >= REGULAR_FORM_MIN_PANE_WIDTH ? "regular" : "compact";
}

export interface ThreadPrHeaderPresentation {
  readonly icon: AppSymbolName;
  readonly tintClassName: string;
  readonly textClassName: string;
  readonly text: string | null;
  readonly status: ThreadPrHeaderGlyph | null;
  readonly opensMenu: boolean;
  readonly accessibilityLabel: string;
}

// A status's header glyph, if it shows one, and the text it adds to the accessibility label.
interface ThreadPrHeaderSignal {
  readonly glyph: ThreadPrHeaderGlyph | null;
  readonly label: string | null;
}

const NO_SIGNAL: ThreadPrHeaderSignal = { glyph: null, label: null };

// Only failing checks get a glyph: passing and running checks are the usual state, so they
// cost the header no room and stay in the accessibility label.
const CHECKS_SIGNAL = {
  passing: { glyph: null, label: "checks passing" },
  failing: {
    glyph: { icon: "xmark.circle.fill", tintClassName: "accent-danger-foreground" },
    label: "checks failing",
  },
  pending: { glyph: null, label: "checks running" },
} as const satisfies Record<PullRequestChecksState, ThreadPrHeaderSignal>;

const CONFLICT_SIGNAL: ThreadPrHeaderSignal = {
  glyph: { icon: "exclamationmark.triangle", tintClassName: "accent-danger-foreground" },
  label: "has conflicts",
};

// The accessibility label already names the pending or stack state, so these glyphs add no text.
const PENDING_SIGNAL: ThreadPrHeaderSignal = {
  glyph: { icon: "ellipsis.circle", tintClassName: "accent-icon-muted" },
  label: null,
};

type PullRequestState = ReturnType<typeof pullRequestStatus>;

// Mirrors the desktop thread panel row: a conflict replaces the checks rollup, and drafts hide it.
// Stacks and unsynced links add a state shape so grayscale screens do not rely on tint alone.
function statusSignal(
  pr: ThreadPrPresentation,
  state: PullRequestState | null,
  form: ThreadPrHeaderForm,
): ThreadPrHeaderSignal {
  if (state === null) return PENDING_SIGNAL;
  if (pr.kind === "stack") {
    return form === "compact"
      ? NO_SIGNAL
      : { glyph: { icon: state.icon, tintClassName: state.tint }, label: null };
  }
  if (pr.others > 0) return NO_SIGNAL;
  if (pr.state === "open" && pr.mergeability === "conflicting") return CONFLICT_SIGNAL;
  if (pr.isDraft || pr.checksState === null) return NO_SIGNAL;
  return CHECKS_SIGNAL[pr.checksState];
}

export function presentThreadPrHeader(
  pr: ThreadPrPresentation,
  form: ThreadPrHeaderForm = "regular",
): ThreadPrHeaderPresentation {
  const opensMenu = pr.kind === "stack" || pr.others > 0;
  const state =
    pr.state === null ? null : pullRequestStatus({ state: pr.state, isDraft: pr.isDraft });
  const signal = statusSignal(pr, state, form);
  return {
    icon:
      pr.kind === "stack" && form === "regular"
        ? PullRequestGlyph.stack
        : (state?.icon ?? PullRequestGlyph.pullRequest),
    tintClassName: state?.tint ?? "accent-icon-muted",
    textClassName: state?.text ?? "text-foreground-muted",
    text: opensMenu ? pr.label : form === "compact" ? null : `#${pr.number}`,
    status: signal.glyph,
    opensMenu,
    accessibilityLabel:
      signal.label === null ? pr.accessibilityLabel : `${pr.accessibilityLabel}, ${signal.label}`,
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
  [PullRequestGlyph.closed]: "xmark.circle",
  [PullRequestGlyph.draft]: "circle.dashed",
};

function menuImage(link: ThreadPullRequestLink, platform: PlatformOSType): string {
  const icon =
    link.snapshot === null ? PullRequestGlyph.pullRequest : pullRequestStatus(link.snapshot).icon;
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
