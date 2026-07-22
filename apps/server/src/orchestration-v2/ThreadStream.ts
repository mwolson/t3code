/**
 * Maximum number of persisted events a thread resume may replay before the
 * server falls back to a fresh projection snapshot. Matches the shell
 * subscription bound so long-idle cached cursors do not flood the renderer.
 */
export const THREAD_RESUME_MAX_REPLAY_EVENTS = 1_000;

export type ThreadResumePlan =
  | {
      readonly mode: "replay";
      readonly afterSequence: number;
      readonly throughSequence: number;
    }
  | { readonly mode: "snapshot" };

/**
 * Decide whether a thread subscription should replay the event gap after the
 * client's cursor or send a fresh snapshot instead.
 *
 * A client cursor above the high water mark is stale or invalid. More than
 * {@link THREAD_RESUME_MAX_REPLAY_EVENTS} retained thread events is too large
 * to stream cheaply.
 */
export function decideThreadResume(input: {
  readonly afterSequence: number;
  readonly highWater: number;
  readonly replayEventCount: number;
}): ThreadResumePlan {
  if (
    input.afterSequence > input.highWater ||
    input.replayEventCount > THREAD_RESUME_MAX_REPLAY_EVENTS
  ) {
    return { mode: "snapshot" };
  }
  return {
    mode: "replay",
    afterSequence: input.afterSequence,
    throughSequence: input.highWater,
  };
}
