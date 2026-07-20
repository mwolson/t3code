import type {
  OrchestrationV2ShellSnapshot,
  OrchestrationV2ShellStreamItem,
  OrchestrationV2ThreadShell,
} from "@t3tools/contracts";

function upsertById<T extends { readonly id: unknown }>(
  items: ReadonlyArray<T>,
  item: T,
): ReadonlyArray<T> {
  const index = items.findIndex((candidate) => candidate.id === item.id);
  if (index === -1) return [...items, item];
  return items.map((candidate, candidateIndex) => (candidateIndex === index ? item : candidate));
}

/**
 * Re-partitions active/archive membership from each thread's `archivedAt`.
 * Defends against stale full snapshots or mis-tagged deltas that would otherwise
 * keep an archived thread on the home list.
 */
export function normalizeShellThreadMembership(
  snapshot: OrchestrationV2ShellSnapshot,
): OrchestrationV2ShellSnapshot {
  const byId = new Map<string, OrchestrationV2ThreadShell>();

  for (const thread of snapshot.threads) {
    byId.set(String(thread.id), thread);
  }
  for (const thread of snapshot.archivedThreads) {
    const existing = byId.get(String(thread.id));
    if (existing === undefined) {
      byId.set(String(thread.id), thread);
      continue;
    }
    // Prefer a non-null archivedAt when the same id appears in both lists.
    if (existing.archivedAt === null && thread.archivedAt !== null) {
      byId.set(String(thread.id), thread);
    }
  }

  const threads: OrchestrationV2ThreadShell[] = [];
  const archivedThreads: OrchestrationV2ThreadShell[] = [];
  for (const thread of byId.values()) {
    if (thread.archivedAt !== null) {
      archivedThreads.push(thread);
    } else {
      threads.push(thread);
    }
  }

  if (
    threads.length === snapshot.threads.length &&
    archivedThreads.length === snapshot.archivedThreads.length &&
    threads.every((thread, index) => thread === snapshot.threads[index]) &&
    archivedThreads.every((thread, index) => thread === snapshot.archivedThreads[index])
  ) {
    return snapshot;
  }

  return {
    ...snapshot,
    threads,
    archivedThreads,
  };
}

/** Applies one committed V2 shell delta while preserving active/archive exclusivity. */
export function applyShellStreamEvent(
  snapshot: OrchestrationV2ShellSnapshot,
  event: Exclude<
    OrchestrationV2ShellStreamItem,
    { readonly kind: "snapshot" } | { readonly kind: "synchronized" }
  >,
): OrchestrationV2ShellSnapshot {
  if (event.sequence <= snapshot.snapshotSequence) return snapshot;

  switch (event.kind) {
    case "project.updated":
      return {
        ...snapshot,
        projects: upsertById(snapshot.projects, event.project),
        snapshotSequence: event.sequence,
      };
    case "project.removed":
      return {
        ...snapshot,
        projects: snapshot.projects.filter((project) => project.id !== event.projectId),
        snapshotSequence: event.sequence,
      };
    case "thread.updated": {
      // Trust archivedAt over location so a mis-tagged active delta cannot keep
      // an archived thread on the home list after another client archives it.
      const location = event.thread.archivedAt !== null ? "archive" : event.location;
      const withoutThread = (threads: OrchestrationV2ShellSnapshot["threads"]) =>
        threads.filter((thread) => thread.id !== event.thread.id);
      return normalizeShellThreadMembership({
        ...snapshot,
        threads:
          location === "active"
            ? upsertById(withoutThread(snapshot.threads), event.thread)
            : withoutThread(snapshot.threads),
        archivedThreads:
          location === "archive"
            ? upsertById(withoutThread(snapshot.archivedThreads), event.thread)
            : withoutThread(snapshot.archivedThreads),
        snapshotSequence: event.sequence,
      });
    }
    case "thread.removed":
      return {
        ...snapshot,
        threads: snapshot.threads.filter((thread) => thread.id !== event.threadId),
        archivedThreads: snapshot.archivedThreads.filter((thread) => thread.id !== event.threadId),
        snapshotSequence: event.sequence,
      };
    default:
      return snapshot;
  }
}
