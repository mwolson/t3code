import type { EnvironmentThreadShell } from "./state/models.ts";

export type InteractionSoundCue = "bloom" | "success";

interface ThreadSoundState {
  readonly completedRun: string | null;
  readonly userInitiatedRun: string | null;
  readonly hasPendingUserInput: boolean;
  readonly hasPendingApprovals: boolean;
}

export type ThreadSoundStateByKey = ReadonlyMap<string, ThreadSoundState>;

export function shouldPlayInteractionSound(
  cue: InteractionSoundCue,
  completionSoundEnabled: boolean,
): boolean {
  return cue !== "success" || completionSoundEnabled;
}

export function selectLiveThreadShells(
  threads: ReadonlyArray<EnvironmentThreadShell>,
  liveEnvironmentIds: ReadonlySet<EnvironmentThreadShell["environmentId"]>,
): ReadonlyArray<EnvironmentThreadShell> {
  return threads.filter((thread) => liveEnvironmentIds.has(thread.environmentId));
}

function threadKey(thread: EnvironmentThreadShell): string {
  return `${thread.environmentId}:${thread.id}`;
}

function completedRun(thread: EnvironmentThreadShell): string | null {
  const latestRun = thread.latestRun;
  if (latestRun === null || latestRun.completedAt === null) {
    return null;
  }
  if (latestRun.status !== "completed") {
    return null;
  }
  return latestRun.runId;
}

const USER_RUN_START_WINDOW_MS = 2 * 60 * 1_000;

function userInitiatedRun(thread: EnvironmentThreadShell): string | null {
  const latestRun = thread.latestRun;
  if (latestRun === null) {
    return null;
  }

  // V2 shells do not yet expose initiatingUserMessageId on latestRun. Associate
  // a completed run with a nearby user message so synthetic background work
  // does not fire success cues. A later steering message falls after
  // requestedAt and is excluded by the positive startup delay check.
  if (thread.latestUserMessageAt === null || latestRun.requestedAt === null) {
    return null;
  }

  const requestedAt = Date.parse(latestRun.requestedAt);
  const latestUserMessageAt = Date.parse(thread.latestUserMessageAt);
  if (!Number.isFinite(requestedAt) || !Number.isFinite(latestUserMessageAt)) {
    return null;
  }

  const startupDelay = requestedAt - latestUserMessageAt;
  if (startupDelay < 0 || startupDelay > USER_RUN_START_WINDOW_MS) {
    return null;
  }

  return latestRun.runId;
}

export function captureThreadSoundState(
  threads: ReadonlyArray<EnvironmentThreadShell>,
): ThreadSoundStateByKey {
  return new Map(
    threads.map((thread) => [
      threadKey(thread),
      {
        completedRun: completedRun(thread),
        userInitiatedRun: userInitiatedRun(thread),
        hasPendingUserInput: thread.hasPendingUserInput,
        hasPendingApprovals: thread.hasPendingApprovals,
      },
    ]),
  );
}

/**
 * Update state for currently live threads while retaining the last trustworthy
 * baseline for threads that still exist but are temporarily synchronizing.
 */
export function captureThreadSoundStatePreservingUnobserved(
  previous: ThreadSoundStateByKey,
  liveThreads: ReadonlyArray<EnvironmentThreadShell>,
  threads: ReadonlyArray<EnvironmentThreadShell>,
): ThreadSoundStateByKey {
  const existingThreadKeys = new Set(threads.map(threadKey));
  const next = new Map([...previous].filter(([key]) => existingThreadKeys.has(key)));
  for (const [key, state] of captureThreadSoundState(liveThreads)) {
    next.set(key, state);
  }
  return next;
}

/**
 * While client settings are still hydrating, keep a sound baseline without
 * advancing known thread state. Newly seen threads are admitted so later
 * transitions can still produce cues once hydration completes.
 */
export function captureThreadSoundStateWhileSettingsHydrating(
  previous: ThreadSoundStateByKey | null,
  threads: ReadonlyArray<EnvironmentThreadShell>,
): ThreadSoundStateByKey {
  const next = captureThreadSoundState(threads);
  if (previous === null) {
    return next;
  }

  const merged = new Map(previous);
  for (const [key, state] of next) {
    if (!merged.has(key)) {
      merged.set(key, state);
    }
  }
  return merged;
}

export function deriveInteractionSoundCues(
  previous: ThreadSoundStateByKey,
  threads: ReadonlyArray<EnvironmentThreadShell>,
): InteractionSoundCue[] {
  const cues: InteractionSoundCue[] = [];

  for (const thread of threads) {
    const prior = previous.get(threadKey(thread));
    const nextCompletedRun = completedRun(thread);
    const nextUserInitiatedRun = userInitiatedRun(thread);

    if (
      prior &&
      nextCompletedRun !== null &&
      prior.completedRun !== nextCompletedRun &&
      nextUserInitiatedRun === nextCompletedRun
    ) {
      cues.push("success");
    }
    if (
      prior &&
      ((thread.hasPendingUserInput && !prior.hasPendingUserInput) ||
        (thread.hasPendingApprovals && !prior.hasPendingApprovals))
    ) {
      cues.push("bloom");
    }
  }

  return cues;
}
