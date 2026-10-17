import type { ThreadPullRequestLink, ThreadPullRequestSnapshot } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  presentThreadLinkedPullRequests,
  presentThreadPr,
} from "../../state/thread-pr-presentation";
import { presentThreadPrHeader, threadPrMenuEntries } from "./thread-pr-header-presentation";

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

function header(links: ReadonlyArray<ThreadPullRequestLink>) {
  const pr = presentThreadLinkedPullRequests(links);
  if (pr === null) throw new Error("expected a pull request presentation");
  return presentThreadPrHeader(pr);
}

describe("presentThreadPrHeader", () => {
  it("shows an open pull request number with the open glyph", () => {
    expect(header([link(1006)])).toEqual({
      icon: "arrow.triangle.pull",
      tintClassName: "accent-adaptive-emerald-600-400",
      textClassName: "text-adaptive-emerald-600-400",
      text: "#1006",
      status: null,
      opensMenu: false,
      accessibilityLabel: "#1006 pull request open",
    });
  });

  it.each([
    ["passing", "checkmark.circle", "accent-adaptive-emerald-600-400", "checks passing"],
    ["failing", "xmark.circle.fill", "accent-danger-foreground", "checks failing"],
    ["pending", "clock", "accent-warning-foreground", "checks running"],
  ] as const)("adds the %s checks rollup", (checksState, icon, tintClassName, label) => {
    const presentation = header([link(1006, { checksState })]);
    expect(presentation.status).toEqual({ icon, tintClassName, label });
    expect(presentation.accessibilityLabel).toBe(`#1006 pull request open, ${label}`);
  });

  it("uses the draft glyph and hides checks for drafts", () => {
    expect(header([link(7, { isDraft: true, checksState: "failing" })])).toMatchObject({
      icon: "git.pull-request.draft",
      tintClassName: "accent-icon-muted",
      textClassName: "text-foreground-muted",
      status: null,
      accessibilityLabel: "#7 pull request draft",
    });
  });

  it("uses the merged glyph and keeps the checks rollup like the desktop row", () => {
    expect(header([link(8, { state: "merged", checksState: "passing" })])).toMatchObject({
      icon: "arrow.triangle.merge",
      tintClassName: "accent-adaptive-violet-600-400",
      status: { icon: "checkmark.circle" },
      accessibilityLabel: "#8 pull request merged, checks passing",
    });
  });

  it("uses the closed glyph in the danger tint", () => {
    expect(header([link(9, { state: "closed" })])).toMatchObject({
      icon: "git.pull-request.closed",
      tintClassName: "accent-danger-foreground",
      textClassName: "text-danger-foreground",
      status: null,
      accessibilityLabel: "#9 pull request closed",
    });
  });

  it("shows conflicts in place of checks on open pull requests", () => {
    expect(
      header([link(10, { mergeability: "conflicting", checksState: "passing" })]).status,
    ).toEqual({
      icon: "exclamationmark.triangle",
      tintClassName: "accent-danger-foreground",
      label: "has conflicts",
    });
    expect(header([link(11, { state: "merged", mergeability: "conflicting" })]).status).toBeNull();
  });

  it("hides checks for a closed draft like the desktop row", () => {
    expect(
      header([link(13, { state: "closed", isDraft: true, checksState: "passing" })]),
    ).toMatchObject({
      icon: "git.pull-request.closed",
      status: null,
      accessibilityLabel: "#13 pull request closed",
    });
  });

  it("keeps an unsynced link neutral with a pending glyph until its status arrives", () => {
    expect(header([link(12, null)])).toMatchObject({
      icon: "arrow.triangle.pull",
      tintClassName: "accent-icon-muted",
      textClassName: "text-foreground-muted",
      text: "#12",
      status: { icon: "ellipsis.circle", tintClassName: "accent-icon-muted", label: null },
      accessibilityLabel: "#12 pull request status pending",
    });
  });

  it("tells an unsynced link from an open one without relying on color", () => {
    const shape = ({ icon, text, status }: ReturnType<typeof header>) => [icon, text, status?.icon];
    expect(shape(header([link(12, null)]))).not.toEqual(shape(header([link(12)])));
  });

  it("counts several unrelated pull requests and opens a menu", () => {
    expect(
      header([link(1, { state: "merged" }), link(2, { checksState: "failing" })]),
    ).toMatchObject({
      icon: "arrow.triangle.pull",
      tintClassName: "accent-adaptive-emerald-600-400",
      text: "+2",
      status: null,
      opensMenu: true,
      accessibilityLabel: "2 linked pull requests, overall open",
    });
  });

  it("uses the aggregate state when every linked pull request is merged", () => {
    expect(header([link(1, { state: "merged" }), link(2, { state: "merged" })])).toMatchObject({
      icon: "arrow.triangle.merge",
      tintClassName: "accent-adaptive-violet-600-400",
      text: "+2",
    });
  });

  it("shows a stack glyph with the layer count", () => {
    const stack = [
      link(21, { headBranch: "layer-1", baseBranch: "main" }),
      link(22, { headBranch: "layer-2", baseBranch: "layer-1", isDraft: true }),
      link(23, { headBranch: "layer-3", baseBranch: "layer-2" }),
    ];
    expect(header(stack)).toMatchObject({
      icon: "square.3.layers.3d",
      tintClassName: "accent-adaptive-emerald-600-400",
      text: "3",
      status: {
        icon: "arrow.triangle.pull",
        tintClassName: "accent-adaptive-emerald-600-400",
        label: null,
      },
      opensMenu: true,
      accessibilityLabel: "3 pull requests in stack, open",
    });
  });

  it("gives each stack state its own shape so grayscale screens can tell them apart", () => {
    const stackIn = (state: "open" | "merged" | "closed", isDraft = false) =>
      header([
        link(21, { headBranch: "layer-1", state, isDraft }),
        link(22, { headBranch: "layer-2", baseBranch: "layer-1", state, isDraft }),
        link(23, { headBranch: "layer-3", baseBranch: "layer-2", state, isDraft }),
      ]);
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

  it("shows checks for a branch pull request summary", () => {
    const pr = presentThreadPr(
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
    expect(presentThreadPrHeader(pr)).toMatchObject({
      text: "#5",
      status: { icon: "xmark.circle.fill" },
    });
  });

  it("hides checks for a closed draft branch pull request summary", () => {
    const pr = presentThreadPr(
      {
        number: 6,
        title: "Abandoned draft",
        url: "https://github.com/t3tools/t3code/pull/6",
        baseRef: "main",
        headRef: "draft-branch",
        state: "closed",
        isDraft: true,
      },
      undefined,
      { checksState: "failing" },
    );
    expect(presentThreadPrHeader(pr)).toMatchObject({
      icon: "git.pull-request.closed",
      status: null,
      accessibilityLabel: "#6 pull request closed",
    });
  });
});

describe("threadPrMenuEntries", () => {
  it("lists every visible pull request, most recently active chain first", () => {
    const entries = threadPrMenuEntries(
      [
        link(1, { state: "merged" }, "2026-10-01T00:00:00.000Z"),
        link(2, { headBranch: "layer-1", checksState: "passing" }, "2026-10-03T00:00:00.000Z"),
        link(3, { headBranch: "layer-2", baseBranch: "layer-1", isDraft: true }),
        { ...link(4), source: "stack-dismissed" },
        link(5, { mergeability: "conflicting" }, "2026-10-02T00:00:00.000Z"),
      ],
      "android",
    );
    expect(entries.map((entry) => entry.action)).toEqual([
      {
        id: "github.com/t3tools/t3code#2",
        title: "#2 Change 2",
        image: "arrow.triangle.pull",
        subtitle: "Open · Checks passing",
      },
      {
        id: "github.com/t3tools/t3code#3",
        title: "#3 Change 3",
        image: "git.pull-request.draft",
        subtitle: "Draft",
      },
      {
        id: "github.com/t3tools/t3code#5",
        title: "#5 Change 5",
        image: "arrow.triangle.pull",
        subtitle: "Open · Has conflicts",
      },
      {
        id: "github.com/t3tools/t3code#1",
        title: "#1 Change 1",
        image: "arrow.triangle.merge",
        subtitle: "Merged",
      },
    ]);
    expect(entries[0]?.url).toBe("https://github.com/t3tools/t3code/pull/2");
  });

  it("uses SF Symbols for draft and closed entries in native iOS menus", () => {
    const links = [
      link(1, { isDraft: true }, "2026-10-03T00:00:00.000Z"),
      link(2, { state: "closed" }, "2026-10-02T00:00:00.000Z"),
      link(3, { state: "closed", isDraft: true, checksState: "passing" }),
    ];
    expect(threadPrMenuEntries(links, "ios").map((entry) => entry.action.image)).toEqual([
      "circle.dashed",
      "xmark.circle",
      "xmark.circle",
    ]);
    expect(threadPrMenuEntries(links, "android").map((entry) => entry.action.image)).toEqual([
      "git.pull-request.draft",
      "git.pull-request.closed",
      "git.pull-request.closed",
    ]);
    expect(threadPrMenuEntries(links, "ios")[2]?.action.subtitle).toBe("Closed");
  });
});
