// Mobile thread routes unmount during back navigation. Retain the stream-backed
// state across short subscriber gaps without keeping every opened thread alive.

/** Default for web / desktop: long enough for tab switches and brief blips. */
export const THREAD_STATE_IDLE_TTL_MS = 5 * 60_000;

/**
 * Mobile grace after the last subscriber leaves. Long enough for back/forward
 * within a session, short enough to drop multi-MB projections and live WS
 * subscriptions before Hermes heap piles up.
 */
export const THREAD_STATE_MOBILE_IDLE_TTL_MS = 30_000;

let threadStateIdleTtlMs: number | null = null;

/**
 * Override the idle TTL used when creating thread state atoms. Pass `null` to
 * restore the default (`THREAD_STATE_IDLE_TTL_MS`). Call once at app startup
 * before any thread atom is subscribed.
 */
export function setThreadStateIdleTtlMs(ms: number | null): void {
  if (ms === null) {
    threadStateIdleTtlMs = null;
    return;
  }
  threadStateIdleTtlMs = Number.isFinite(ms) && ms > 0 ? Math.floor(ms) : null;
}

export function getThreadStateIdleTtlMs(): number {
  return threadStateIdleTtlMs ?? THREAD_STATE_IDLE_TTL_MS;
}
