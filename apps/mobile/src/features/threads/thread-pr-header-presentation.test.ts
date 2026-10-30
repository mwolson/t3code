import { PullRequestGlyph } from "../pull-requests/pullRequestGlyphs";
import type { ThreadPullRequestLink, ThreadPullRequestSnapshot } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  presentThreadLinkedPullRequests,
  presentThreadPr,
} from "../../state/thread-pr-presentation";
import { deriveLayout, deriveWorkspacePaneLayout } from "../../lib/layout";
import type { NativeLayoutMetrics } from "../../lib/reserved-regions";
import {
  presentThreadPrHeader,
  threadPrHeaderForm,
  threadPrMenuEntries,
} from "./thread-pr-header-presentation";

function link(
  number: number,
  snapshot: Partial<ThreadPullRequestSnapshot> | null = {},
  linkedAt = "2026-10-01T00:00:00.000Z",
): ThreadPullRequestLink {
  return {
    host: "github.com",
    repository: "t3tools/t3code",
    number,
    url: `https://github.com/t3tools/t3code/pull/${number}`,
    source: "manual",
    linkedAt,
    stack: null,
    snapshot:
      snapshot === null
        ? null
        : {
            state: "open",
            title: `Change ${number}`,
            headBranch: `change-${number}`,
            baseBranch: "main",
            isDraft: false,
            updatedAt: null,
            syncedAt: "2026-10-01T00:00:00.000Z",
            ...snapshot,
          },
  };
}

function header(
  links: ReadonlyArray<ThreadPullRequestLink>,
  form: "regular" | "compact" = "regular",
) {
  const pr = presentThreadLinkedPullRequests(links);
  if (pr === null) throw new Error("expected a pull request presentation");
  return presentThreadPrHeader(pr, form);
}

describe("presentThreadPrHeader", () => {
  it("shows each pull request state with its own glyph and tint", () => {
    expect(header([link(1006)])).toEqual({
      icon: PullRequestGlyph.pullRequest,
      tintClassName: "accent-adaptive-emerald-600-400",
      textClassName: "text-adaptive-emerald-600-400",
      text: "#1006",
      status: null,
      opensMenu: false,
      accessibilityLabel: "#1006 pull request open",
    });
    expect(header([link(7, { isDraft: true })])).toMatchObject({
      icon: PullRequestGlyph.draft,
      tintClassName: "accent-icon-muted",
      accessibilityLabel: "#7 pull request draft",
    });
    expect(header([link(8, { state: "merged" })])).toMatchObject({
      icon: PullRequestGlyph.merged,
      tintClassName: "accent-adaptive-violet-600-400",
      accessibilityLabel: "#8 pull request merged",
    });
    expect(header([link(9, { state: "closed" })])).toMatchObject({
      icon: PullRequestGlyph.closed,
      tintClassName: "accent-danger-foreground",
      accessibilityLabel: "#9 pull request closed",
    });
    expect(header([link(12, null)])).toMatchObject({
      icon: PullRequestGlyph.pullRequest,
      tintClassName: "accent-icon-muted",
      text: "#12",
      status: { icon: "ellipsis.circle", tintClassName: "accent-icon-muted" },
      accessibilityLabel: "#12 pull request status pending",
    });
  });

  it("shows failing checks and conflicts as glyphs, and other checks only in the label", () => {
    expect(header([link(1006, { checksState: "failing" })])).toMatchObject({
      status: { icon: "xmark.circle.fill", tintClassName: "accent-danger-foreground" },
      accessibilityLabel: "#1006 pull request open, checks failing",
    });
    expect(
      header([link(10, { mergeability: "conflicting", checksState: "failing" })]),
    ).toMatchObject({
      status: { icon: "exclamationmark.triangle", tintClassName: "accent-danger-foreground" },
      accessibilityLabel: "#10 pull request open, has conflicts",
    });
    expect(header([link(1006, { checksState: "passing" })])).toMatchObject({
      status: null,
      accessibilityLabel: "#1006 pull request open, checks passing",
    });
    expect(header([link(1006, { checksState: "pending" })])).toMatchObject({
      status: null,
      accessibilityLabel: "#1006 pull request open, checks running",
    });
    expect(header([link(7, { isDraft: true, checksState: "failing" })])).toMatchObject({
      status: null,
      accessibilityLabel: "#7 pull request draft",
    });

    const branchPr = presentThreadPr(
      {
        number: 5,
        title: "Branch change",
        url: "https://github.com/t3tools/t3code/pull/5",
        baseRef: "main",
        headRef: "branch",
        state: "open",
      },
      undefined,
      { checksState: "failing" },
    );
    expect(presentThreadPrHeader(branchPr)).toMatchObject({
      text: "#5",
      status: { icon: "xmark.circle.fill" },
    });
  });

  it("counts several unrelated pull requests and opens a menu", () => {
    expect(
      header([link(1, { state: "merged" }), link(2, { checksState: "failing" })]),
    ).toMatchObject({
      icon: PullRequestGlyph.pullRequest,
      tintClassName: "accent-adaptive-emerald-600-400",
      text: "+2",
      status: null,
      opensMenu: true,
      accessibilityLabel: "2 linked pull requests, overall open",
    });
  });

  it("shows a stack with the layer count and a shape for each state", () => {
    const stackIn = (
      state: "open" | "merged" | "closed",
      isDraft = false,
      form: "regular" | "compact" = "regular",
    ) =>
      header(
        [
          link(21, { headBranch: "layer-1", state, isDraft }),
          link(22, { headBranch: "layer-2", baseBranch: "layer-1", state, isDraft }),
          link(23, { headBranch: "layer-3", baseBranch: "layer-2", state, isDraft }),
        ],
        form,
      );
    expect(stackIn("open")).toMatchObject({
      icon: PullRequestGlyph.stack,
      tintClassName: "accent-adaptive-emerald-600-400",
      text: "3",
      status: {
        icon: PullRequestGlyph.pullRequest,
        tintClassName: "accent-adaptive-emerald-600-400",
      },
      opensMenu: true,
      accessibilityLabel: "3 pull requests in stack, open",
    });
    const stacks = [stackIn("open"), stackIn("open", true), stackIn("merged"), stackIn("closed")];
    expect(stacks.map((stack) => stack.accessibilityLabel)).toEqual([
      "3 pull requests in stack, open",
      "3 pull requests in stack, draft",
      "3 pull requests in stack, merged",
      "3 pull requests in stack, closed",
    ]);
    const shapes = stacks.map(({ icon, text, status }) => `${icon} ${text} ${status?.icon}`);
    expect(new Set(shapes).size).toBe(stacks.length);
  });

  it("drops a single pull request's number and shows a stack as its state glyph when compact", () => {
    const links = [link(20912, { checksState: "failing" })];
    expect(header(links, "compact")).toEqual({ ...header(links), text: null });
    expect(header(links, "compact").accessibilityLabel).toBe(
      "#20912 pull request open, checks failing",
    );

    const stack = [
      link(21, { headBranch: "layer-1", baseBranch: "main", state: "merged" }),
      link(22, { headBranch: "layer-2", baseBranch: "layer-1", state: "merged" }),
    ];
    expect(header(stack, "compact")).toEqual({
      icon: PullRequestGlyph.merged,
      tintClassName: "accent-adaptive-violet-600-400",
      textClassName: "text-adaptive-violet-600-400",
      text: "2",
      status: null,
      opensMenu: true,
      accessibilityLabel: "2 pull requests in stack, merged",
    });
  });
});

describe("threadPrHeaderForm", () => {
  function detailPaneWidth(input: {
    readonly width: number;
    readonly height: number;
    readonly nativeMetrics?: NativeLayoutMetrics;
    readonly sidebarVisible?: boolean;
  }) {
    const layout = deriveLayout(input);
    const panes = deriveWorkspacePaneLayout({
      layout,
      viewportWidth: input.width,
      primarySidebarPreferredVisible: input.sidebarVisible ?? true,
      auxiliaryPanePreferredVisible: false,
    });
    return { usesSplitView: layout.usesSplitView, width: panes.contentPaneWidth };
  }

  it("uses the compact form for phone-sized panes and the regular form for wide ones", () => {
    const phone = detailPaneWidth({ width: 402, height: 874 });
    expect(phone).toEqual({ usesSplitView: false, width: 402 });
    expect(threadPrHeaderForm(phone.width)).toBe("compact");

    // The Duo inner display in portrait, as in reserved-regions.test.ts.
    const duoPortrait: NativeLayoutMetrics = {
      width: 669,
      height: 951,
      hasHinge: true,
      horizontalSizeClass: "regular",
      verticalBarEdge: "none",
      safeArea: { top: 82, bottom: 34, left: 0, right: 0 },
      reservedRegions: [{ kind: "occlusion", x: 535, y: 0, width: 134, height: 82 }],
    };
    const duo = detailPaneWidth({ ...duoPortrait, nativeMetrics: duoPortrait });
    expect(duo).toEqual({ usesSplitView: true, width: 389 });
    expect(threadPrHeaderForm(duo.width)).toBe("compact");

    const ipadLandscape = detailPaneWidth({ width: 1376, height: 1032 });
    expect(ipadLandscape.width).toBe(996);
    expect(threadPrHeaderForm(ipadLandscape.width)).toBe("regular");
  });
});

describe("threadPrMenuEntries", () => {
  it("lists every visible pull request, most recently active chain first", () => {
    const links: ReadonlyArray<ThreadPullRequestLink> = [
      link(1, { state: "merged" }, "2026-10-01T00:00:00.000Z"),
      link(2, { headBranch: "layer-1", checksState: "passing" }, "2026-10-03T00:00:00.000Z"),
      link(3, { headBranch: "layer-2", baseBranch: "layer-1", isDraft: true }),
      { ...link(4), source: "stack-dismissed" },
      link(5, { mergeability: "conflicting" }, "2026-10-02T00:00:00.000Z"),
    ];
    const entries = threadPrMenuEntries(links, "android");
    expect(entries.map((entry) => entry.action)).toEqual([
      {
        id: "github.com/t3tools/t3code#2",
        title: "#2 Change 2",
        image: PullRequestGlyph.pullRequest,
        subtitle: "Open · Checks passing",
      },
      {
        id: "github.com/t3tools/t3code#3",
        title: "#3 Change 3",
        image: PullRequestGlyph.draft,
        subtitle: "Draft",
      },
      {
        id: "github.com/t3tools/t3code#5",
        title: "#5 Change 5",
        image: PullRequestGlyph.pullRequest,
        subtitle: "Open · Has conflicts",
      },
      {
        id: "github.com/t3tools/t3code#1",
        title: "#1 Change 1",
        image: PullRequestGlyph.merged,
        subtitle: "Merged",
      },
    ]);
    expect(entries[0]?.url).toBe("https://github.com/t3tools/t3code/pull/2");

    expect(threadPrMenuEntries(links, "ios")[1]?.action.image).toBe("circle.dashed");
  });
});
