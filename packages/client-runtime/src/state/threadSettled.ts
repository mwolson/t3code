/**
 * Client-side settled-lifecycle helpers for orchestrator v2 shells.
 *
 * Field mapping from the original v1 helpers:
 * - session.status starting/running → runtime/latestRun blocking statuses
 * - latestTurn timestamps → latestRun timestamps
 * - settledOverride / settledAt stay server-projected fields
 */

export type ChangeRequestStateLike = "open" | "closed" | "merged";

const DAY_MS = 24 * 60 * 60 * 1_000;

const BLOCKING_RUNTIME_STATUSES = new Set([
  "preparing",
  "starting",
  "running",
  "waiting",
  "queued",
]);

export type SettledThreadView = {
  readonly latestUserMessageAt: string | null;
  readonly latestRun?: {
    readonly requestedAt: string | null;
    readonly startedAt: string | null;
    readonly completedAt: string | null;
  } | null;
  /** @deprecated Prefer latestRun; retained for transitional fixtures. */
  readonly latestTurn?: {
    readonly requestedAt: string | null;
    readonly startedAt: string | null;
    readonly completedAt: string | null;
  } | null;
  readonly runtime?: { readonly status: string } | null;
  /** @deprecated Prefer runtime; retained for transitional fixtures. */
  readonly session?: { readonly status: string } | null;
  readonly settledOverride: "settled" | "active" | null;
  readonly settledAt: string | null;
  readonly hasPendingApprovals: boolean;
  readonly hasPendingUserInput: boolean;
};

function activityTimestamps(shell: SettledThreadView): ReadonlyArray<string | null | undefined> {
  const run = shell.latestRun ?? shell.latestTurn ?? null;
  return [shell.latestUserMessageAt, run?.requestedAt, run?.startedAt, run?.completedAt];
}

function isBlockingWork(shell: SettledThreadView): boolean {
  if (shell.hasPendingApprovals || shell.hasPendingUserInput) return true;
  const runtimeStatus = shell.runtime?.status ?? shell.session?.status;
  if (runtimeStatus !== undefined && BLOCKING_RUNTIME_STATUSES.has(runtimeStatus)) {
    return true;
  }
  // v1 session "running"/"starting" mapped through session field in legacy fixtures
  if (runtimeStatus === "running" || runtimeStatus === "starting") return true;
  return false;
}

export function threadLastActivityAt(shell: SettledThreadView): string | null {
  let latest: string | null = null;
  let latestTimestamp = Number.NEGATIVE_INFINITY;

  for (const candidate of activityTimestamps(shell)) {
    if (candidate === null || candidate === undefined) continue;
    const timestamp = Date.parse(candidate);
    if (timestamp > latestTimestamp) {
      latest = candidate;
      latestTimestamp = timestamp;
    }
  }

  return latest;
}

/**
 * A queued turn start lives for at most this long: session adoption takes
 * seconds, so a user message still unadopted after the grace window is a
 * failed start (or stale data), not pending work.
 */
export const QUEUED_TURN_START_GRACE_MS = 2 * 60 * 1_000;

/**
 * A user message no run has picked up yet: detectable as a user message
 * strictly newer than every timestamp on the latest run, only within the
 * adoption grace window.
 */
export function hasQueuedTurnStart(
  shell: Pick<
    SettledThreadView,
    "latestUserMessageAt" | "latestRun" | "latestTurn" | "runtime" | "session"
  >,
  options: { readonly now: string },
): boolean {
  if (shell.latestUserMessageAt == null) return false;
  const runtimeStatus = shell.runtime?.status ?? shell.session?.status;
  // A failed/error session start clears the queued state.
  if (runtimeStatus === "error" || runtimeStatus === "failed") return false;
  const messageAt = Date.parse(shell.latestUserMessageAt);
  if (Number.isNaN(messageAt)) return false;
  const nowMs = Date.parse(options.now);
  if (Number.isNaN(nowMs)) return false;
  if (Math.abs(nowMs - messageAt) > QUEUED_TURN_START_GRACE_MS) return false;
  const run = shell.latestRun ?? shell.latestTurn ?? null;
  if (run === null) return true;
  return [run.requestedAt, run.startedAt, run.completedAt].every(
    (candidate) => candidate == null || Date.parse(candidate) < messageAt,
  );
}

/**
 * A thread may be settled only when none of effectiveSettled's activity
 * blockers hold. Server enforces the same invariants; this twin disables
 * the action before a round trip.
 */
export function canSettle(shell: SettledThreadView, options: { readonly now: string }): boolean {
  if (isBlockingWork(shell)) return false;
  if (hasQueuedTurnStart(shell, options)) return false;
  return true;
}

/**
 * Settled resolution over the server-backed settled lifecycle. Explicit
 * user override (thread.settle / thread.unsettle) wins; without one, a
 * thread auto-settles on a merged/closed PR or inactivity past the window.
 */
export function effectiveSettled(
  shell: SettledThreadView,
  options: {
    readonly now: string;
    readonly autoSettleAfterDays: number | null;
    readonly changeRequestState?: ChangeRequestStateLike | null;
  },
): boolean {
  if (isBlockingWork(shell)) return false;
  if (hasQueuedTurnStart(shell, { now: options.now })) {
    const serverAdjudicated =
      shell.settledOverride === "settled" &&
      shell.settledAt !== null &&
      shell.latestUserMessageAt !== null &&
      Date.parse(shell.settledAt) >= Date.parse(shell.latestUserMessageAt);
    if (!serverAdjudicated) return false;
  }
  if (shell.settledOverride === "settled") return true;
  if (shell.settledOverride === "active") return false;
  if (options.changeRequestState === "merged" || options.changeRequestState === "closed") {
    return true;
  }
  if (options.autoSettleAfterDays === null) return false;

  const lastActivityAt = threadLastActivityAt(shell);
  if (lastActivityAt === null) return false;

  return (
    Date.parse(lastActivityAt) < Date.parse(options.now) - options.autoSettleAfterDays * DAY_MS
  );
}
