export type ThreadNotificationKind = "input" | "completion";

export interface ThreadNotificationObservation {
  /** Run and status key while the thread needs attention, otherwise null. */
  readonly attention: string | null;
  /** The thread is idle and its latest run completed. */
  readonly settled: boolean;
  /** When the latest run completed, if it did. */
  readonly completedAt: number | null;
  /** The latest run completed and background work still keeps the thread waiting. */
  readonly held?: boolean;
}

export function createThreadNotificationGate<Key>(
  notify: (key: Key, kind: ThreadNotificationKind) => void,
  /** Runs `confirmed` after a short wait, unless the returned cancel is called first. */
  confirmLater: (confirmed: () => void) => () => void = (confirmed) => {
    confirmed();
    return () => {};
  },
) {
  // The newest completion already notified, or deliberately skipped.
  const handled = new Map<Key, number>();
  // Threads whose latest run was held by background work, which often ends just before the
  // run that wakes the agent is created. Their completion waits to see that none takes over.
  const wasHeld = new Set<Key>();
  const confirming = new Map<Key, { readonly at: number; cancel: () => void }>();
  const skip = (key: Key, at: number | null) => {
    if (at !== null && at > (handled.get(key) ?? -Infinity)) handled.set(key, at);
  };
  const stopConfirming = (key: Key) => {
    confirming.get(key)?.cancel();
    confirming.delete(key);
  };
  return {
    observe(
      key: Key,
      prior: ThreadNotificationObservation | undefined,
      next: ThreadNotificationObservation,
      options: {
        readonly archived: boolean;
        readonly quiet: boolean;
        readonly quietInput?: boolean;
      },
    ): void {
      // The first sighting, archived threads, and stopped or silent runs never notify completion.
      if (!prior || options.archived || options.quiet) skip(key, next.completedAt);
      if (next.held === true) wasHeld.add(key);
      else if (!next.settled && next.attention === null) wasHeld.delete(key);
      const at = next.settled ? next.completedAt : null;
      const due =
        prior !== undefined &&
        !options.archived &&
        at !== null &&
        at > (handled.get(key) ?? -Infinity);
      if (!due || confirming.get(key)?.at !== at) stopConfirming(key);
      if (!prior || options.archived) return;
      if (next.attention && next.attention !== prior.attention && !options.quietInput) {
        notify(key, "input");
        return;
      }
      if (!due || confirming.has(key)) return;
      const complete = () => {
        confirming.delete(key);
        wasHeld.delete(key);
        handled.set(key, at);
        notify(key, "completion");
      };
      if (!wasHeld.has(key)) {
        complete();
        return;
      }
      const pending = { at, cancel: () => {} };
      confirming.set(key, pending);
      pending.cancel = confirmLater(complete);
    },
    /** Stops confirming while the snapshot is stale; the next observation starts over. */
    suspend(): void {
      for (const key of confirming.keys()) stopConfirming(key);
    },
    /** Forgets threads that left the snapshot. */
    retain(keys: ReadonlySet<Key>): void {
      for (const key of handled.keys()) if (!keys.has(key)) handled.delete(key);
      for (const key of wasHeld) if (!keys.has(key)) wasHeld.delete(key);
      for (const key of confirming.keys()) if (!keys.has(key)) stopConfirming(key);
    },
    clear(): void {
      handled.clear();
      wasHeld.clear();
      for (const key of confirming.keys()) stopConfirming(key);
    },
  };
}
