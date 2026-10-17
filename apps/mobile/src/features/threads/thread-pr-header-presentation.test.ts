import type { ThreadPullRequestLink, ThreadPullRequestSnapshot } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { presentThreadLinkedPullRequests } from "../../state/thread-pr-presentation";
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

function header(links: ReadonlyArray<ThreadPullRequestLink>, width = 600) {
  const pr = presentThreadLinkedPullRequests(links);
  if (pr === null) throw new Error("expected a pull request presentation");
  return presentThreadPrHeader(pr, threadPrHeaderForm(width));
}

it("keeps a single PR accessible in narrow panes and exposes checks and conflicts", () => {
  const links = [link(20912, { checksState: "failing" })];
  const regular = header(links);
  expect(regular.text).toBe("#20912");
  expect(regular.opensMenu).toBe(false);
  expect(regular.accessibilityLabel).toBe("#20912 pull request open, Checks failing");
  expect(header(links, 599)).toEqual({ ...regular, text: null });
  expect(header([link(10, { mergeability: "conflicting", checksState: "failing" })])).toMatchObject(
    {
      accessibilityLabel: "#10 pull request open, Has conflicts",
    },
  );
  expect(header([link(7, { isDraft: true, checksState: "failing" })])).toMatchObject({
    accessibilityLabel: "#7 pull request draft",
    status: null,
  });
  expect(header([link(12, null)]).accessibilityLabel).toBe("#12 pull request status pending");
});

it("counts linked PRs and stacks while retaining a menu in narrow panes", () => {
  expect(header([link(1, { state: "merged" }), link(2)])).toMatchObject({
    text: "+2",
    opensMenu: true,
    accessibilityLabel: "2 linked pull requests, overall open",
  });
  const stack = [
    link(21, { headBranch: "layer-1" }),
    link(22, { headBranch: "layer-2", baseBranch: "layer-1" }),
  ];
  for (const width of [599, 600]) {
    expect(header(stack, width)).toMatchObject({
      text: "2",
      opensMenu: true,
      accessibilityLabel: "2 pull requests in stack, open",
    });
  }
});

describe("threadPrMenuEntries", () => {
  it("lists visible PRs by chain activity, preserving base-to-tip order and destinations", () => {
    const links = [
      link(1, { state: "merged" }),
      link(2, { headBranch: "layer-1", checksState: "passing" }, "2026-10-03T00:00:00.000Z"),
      link(3, { headBranch: "layer-2", baseBranch: "layer-1", isDraft: true }),
      { ...link(4), source: "stack-dismissed" as const },
      link(5, { mergeability: "conflicting" }, "2026-10-02T00:00:00.000Z"),
    ];
    const entries = threadPrMenuEntries(links, "android");
    expect(entries.map((entry) => [entry.action.title, entry.action.subtitle, entry.url])).toEqual([
      ["#2 Change 2", "Open · Checks passing", links[1]!.url],
      ["#3 Change 3", "Draft", links[2]!.url],
      ["#5 Change 5", "Open · Has conflicts", links[4]!.url],
      ["#1 Change 1", "Merged", links[0]!.url],
    ]);
    expect(new Set(entries.map((entry) => entry.id)).size).toBe(entries.length);
    expect(threadPrMenuEntries(links, "ios")[1]?.action.image).toBe("circle.dashed");
  });
});
