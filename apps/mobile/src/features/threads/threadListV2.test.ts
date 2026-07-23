import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import { EnvironmentId, ProjectId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { makeThreadShellFixture } from "../../test-fixtures";
import {
  buildThreadListV2Items,
  isThreadListV2Thread,
  resolveThreadListV2Status,
  sortThreadsForListV2,
} from "./threadListV2";

const NOW = "2026-06-02T00:00:00.000Z";
const environmentId = EnvironmentId.make("environment-1");

function makeThread(
  input: Partial<EnvironmentThreadShell> & Pick<EnvironmentThreadShell, "id" | "title">,
): EnvironmentThreadShell {
  return makeThreadShellFixture({ environmentId, ...input });
}

describe("resolveThreadListV2Status", () => {
  it("prioritizes approval over a running session", () => {
    const thread = makeThread({
      id: ThreadId.make("t"),
      title: "t",
      hasPendingApprovals: true,
      runtime: {
        status: "running",
        activeRunId: null,
        providerInstanceId: ProviderInstanceId.make("codex"),
        providerName: "Codex",
        lastError: null,
        updatedAt: NOW,
      },
    });
    expect(resolveThreadListV2Status(thread)).toBe("approval");
  });

  it("reports working when pending background tasks outlive a terminal run", () => {
    expect(
      resolveThreadListV2Status(
        makeThread({
          id: ThreadId.make("t"),
          title: "t",
          pendingBackgroundTasks: [{ taskId: "bg-1", description: "Run Codex review" }],
          runtime: {
            status: "completed",
            activeRunId: null,
            providerInstanceId: ProviderInstanceId.make("codex"),
            providerName: "Codex",
            lastError: null,
            updatedAt: NOW,
          },
        }),
      ),
    ).toBe("working");
  });

  it("resolves ready for quiescent threads", () => {
    expect(resolveThreadListV2Status(makeThread({ id: ThreadId.make("t"), title: "t" }))).toBe(
      "ready",
    );
  });
});

describe("sortThreadsForListV2", () => {
  it("orders by creation time, newest first, ignoring activity", () => {
    const sorted = sortThreadsForListV2([
      { id: "oldest", createdAt: "2026-06-01T08:00:00.000Z" },
      { id: "newest", createdAt: "2026-06-01T12:00:00.000Z" },
      { id: "middle", createdAt: "2026-06-01T10:00:00.000Z" },
    ]);
    expect(sorted.map((thread) => thread.id)).toEqual(["newest", "middle", "oldest"]);
  });
});

describe("buildThreadListV2Items", () => {
  it("keeps root and fork threads while excluding subagents and archived threads", () => {
    const parentId = ThreadId.make("parent");
    const root = makeThread({ id: parentId, title: "Root" });
    const fork = makeThread({
      id: ThreadId.make("fork"),
      title: "Fork",
      lineage: {
        rootThreadId: parentId,
        parentThreadId: parentId,
        relationshipToParent: "fork",
      },
    });
    const subagent = makeThread({
      id: ThreadId.make("subagent"),
      title: "Subagent",
      lineage: {
        rootThreadId: parentId,
        parentThreadId: parentId,
        relationshipToParent: "subagent",
      },
    });
    const archived = makeThread({
      id: ThreadId.make("archived"),
      title: "Archived",
      archivedAt: NOW,
    });

    expect(isThreadListV2Thread(root)).toBe(true);
    expect(isThreadListV2Thread(fork)).toBe(true);
    expect(isThreadListV2Thread(subagent)).toBe(false);
    expect(isThreadListV2Thread(archived)).toBe(false);

    const { items } = buildThreadListV2Items({
      threads: [root, fork, subagent, archived],
      environmentId: null,
      searchQuery: "",
      now: NOW,
    });

    expect(items.map((item) => item.thread.id)).toEqual(["fork", "parent"]);
  });

  it("partitions settled threads into a slim tail with one divider", () => {
    const { items } = buildThreadListV2Items({
      threads: [
        makeThread({ id: ThreadId.make("active"), title: "Active" }),
        makeThread({
          id: ThreadId.make("settled"),
          title: "Settled",
          settledOverride: "settled",
          settledAt: NOW,
        }),
        makeThread({
          id: ThreadId.make("settled-2"),
          title: "Settled 2",
          settledOverride: "settled",
          settledAt: NOW,
        }),
      ],
      environmentId: null,
      searchQuery: "",
      now: NOW,
    });

    expect(items.map((item) => [item.thread.id, item.variant])).toEqual([
      ["active", "card"],
      ["settled", "slim"],
      ["settled-2", "slim"],
    ]);
    expect(items.map((item) => item.showSettledDivider)).toEqual([false, true, false]);
    expect(items.map((item) => item.isLast)).toEqual([false, false, true]);
  });

  it("keeps cards in creation order while settled sorts by recency", () => {
    const { items } = buildThreadListV2Items({
      threads: [
        makeThread({
          id: ThreadId.make("older-created"),
          title: "Older",
          createdAt: "2026-06-01T08:00:00.000Z",
          updatedAt: NOW,
        }),
        makeThread({
          id: ThreadId.make("newer-created"),
          title: "Newer",
          createdAt: "2026-06-01T12:00:00.000Z",
        }),
      ],
      environmentId: null,
      searchQuery: "",
      now: NOW,
    });

    expect(items.map((item) => item.thread.id)).toEqual(["newer-created", "older-created"]);
  });

  it("keeps settled threads in the tail and filters by search query", () => {
    const { items } = buildThreadListV2Items({
      threads: [
        makeThread({ id: ThreadId.make("match"), title: "Fix login bug" }),
        makeThread({ id: ThreadId.make("miss"), title: "Greeting" }),
        makeThread({
          id: ThreadId.make("settled"),
          title: "Fix login again",
          settledOverride: "settled",
          settledAt: NOW,
        }),
      ],
      environmentId: null,
      searchQuery: "login",
      now: NOW,
    });

    expect(items.map((item) => [item.thread.id, item.variant])).toEqual([
      ["match", "card"],
      ["settled", "slim"],
    ]);
  });

  it("filters by environment and search", () => {
    const otherEnvironmentId = EnvironmentId.make("environment-2");
    const { items } = buildThreadListV2Items({
      threads: [
        makeThread({ id: ThreadId.make("keep"), title: "Alpha", environmentId }),
        makeThread({
          id: ThreadId.make("drop-env"),
          title: "Alpha",
          environmentId: otherEnvironmentId,
        }),
        makeThread({ id: ThreadId.make("drop-search"), title: "Beta", environmentId }),
      ],
      environmentId,
      searchQuery: "alp",
      now: NOW,
    });
    expect(items.map((item) => item.thread.id)).toEqual(["keep"]);
  });

  it("scopes the flat list to one project", () => {
    const projectId = ProjectId.make("project-1");
    const otherProjectId = ProjectId.make("project-2");
    const { items } = buildThreadListV2Items({
      threads: [
        makeThread({ id: ThreadId.make("included"), projectId, title: "Included" }),
        makeThread({
          id: ThreadId.make("excluded"),
          projectId: otherProjectId,
          title: "Excluded",
        }),
      ],
      environmentId: null,
      projectRefs: [{ environmentId, projectId }],
      searchQuery: "",
      now: NOW,
    });

    expect(items.map((item) => item.thread.id)).toEqual(["included"]);
  });

  it("scopes the flat list to every environment member of a logical project", () => {
    const projectId = ProjectId.make("project-1");
    const remoteEnvironmentId = EnvironmentId.make("environment-remote");
    const { items } = buildThreadListV2Items({
      threads: [
        makeThread({ id: ThreadId.make("local"), projectId, title: "Local" }),
        makeThread({
          environmentId: remoteEnvironmentId,
          id: ThreadId.make("remote"),
          projectId,
          title: "Remote",
        }),
      ],
      environmentId: null,
      projectRefs: [
        { environmentId, projectId },
        { environmentId: remoteEnvironmentId, projectId },
      ],
      searchQuery: "",
      now: NOW,
    });

    expect(items.map((item) => item.thread.id)).toEqual(["local", "remote"]);
  });
});

describe("buildThreadListV2Items settled paging", () => {
  it("caps the settled tail at settledLimit and reports the hidden count", () => {
    const threads = [
      makeThread({ id: ThreadId.make("active"), title: "Active" }),
      ...Array.from({ length: 4 }, (_, index) =>
        makeThread({
          id: ThreadId.make(`settled-${index}`),
          title: `Settled ${index}`,
          settledOverride: "settled",
          settledAt: NOW,
          // Adopted latest run timestamps so the row is not a queued-turn start.
          latestUserMessageAt: `2026-06-01T0${index}:00:00.000Z`,
          latestRun: {
            runId: ThreadId.make(`run-${index}`) as never,
            status: "completed",
            requestedAt: `2026-06-01T0${index}:00:00.000Z`,
            startedAt: `2026-06-01T0${index}:00:00.000Z`,
            completedAt: `2026-06-01T0${index}:10:00.000Z`,
            assistantMessageId: null,
          },
        }),
      ),
    ];

    const layout = buildThreadListV2Items({
      threads,
      environmentId: null,
      searchQuery: "",
      settledLimit: 2,
      now: NOW,
    });

    expect(layout.hiddenSettledCount).toBe(2);
    expect(layout.items.filter((item) => item.variant === "slim")).toHaveLength(2);
    // Most recent settled first; the hidden ones are the oldest.
    expect(layout.items.map((item) => item.thread.id)).toEqual([
      "active",
      "settled-3",
      "settled-2",
    ]);
  });
});
