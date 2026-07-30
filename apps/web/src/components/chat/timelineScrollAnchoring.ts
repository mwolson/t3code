export type TimelineScrollMode = "following-end" | "anchoring-new-turn" | "free-scrolling";
export type TimelineManualScrollDirection = "toward-end" | "toward-start";

export const TIMELINE_AUTO_FOLLOW_SETTLED_DELAY_MS = 1_000;
export const TIMELINE_MANUAL_SCROLL_COOLDOWN_MS = 30_000;

export interface TimelineListMeasurementState {
  readonly data: readonly unknown[];
  readonly scroll: number;
  readonly scrollLength: number;
  readonly positionAtIndex: (index: number) => number | undefined;
  readonly sizeAtIndex: (index: number) => number | undefined;
}

export interface AnchoredTurnMetrics {
  readonly anchorTop: number;
  readonly lastBottom: number;
  readonly turnHeight: number;
  readonly usableViewportHeight: number;
  readonly visibleUsableBottom: number;
  readonly overflowsUsableViewport: boolean;
  readonly targetScrollToRevealEnd: number;
  readonly scrollDeltaToRevealEnd: number;
}

export interface TimelineAutoFollowGate {
  readonly endSpaceVisible: boolean | null;
  readonly followLatched: boolean;
  readonly mainThreadHasActiveStatus: boolean;
  readonly mainThreadSettled: boolean;
  readonly manualScrollCooldownActive: boolean;
  readonly userSendFollowActive: boolean;
}

export function getRowBottom(state: TimelineListMeasurementState, index: number): number | null {
  const top = state.positionAtIndex(index);
  const height = state.sizeAtIndex(index);
  if (
    typeof top !== "number" ||
    typeof height !== "number" ||
    !Number.isFinite(top) ||
    !Number.isFinite(height)
  ) {
    return null;
  }

  return top + Math.max(1, height);
}

export function getAnchoredTurnMetrics({
  state,
  anchorIndex,
  composerOverlayHeight,
  anchorOffset,
}: {
  readonly state: TimelineListMeasurementState;
  readonly anchorIndex: number;
  readonly composerOverlayHeight: number;
  readonly anchorOffset: number;
}): AnchoredTurnMetrics | null {
  if (state.data.length === 0) {
    return null;
  }

  const boundedAnchorIndex = Math.max(0, Math.min(anchorIndex, state.data.length - 1));
  const anchorTop = state.positionAtIndex(boundedAnchorIndex);
  const lastBottom = getRowBottom(state, state.data.length - 1);
  if (typeof anchorTop !== "number" || !Number.isFinite(anchorTop) || lastBottom === null) {
    return null;
  }

  const usableViewportHeight = Math.max(
    0,
    state.scrollLength - composerOverlayHeight - anchorOffset,
  );
  const turnHeight = Math.max(0, lastBottom - anchorTop);
  const visibleUsableBottom = state.scroll + usableViewportHeight;
  const targetScrollToRevealEnd = Math.max(0, lastBottom - usableViewportHeight);
  const scrollDeltaToRevealEnd = Math.max(0, targetScrollToRevealEnd - state.scroll);

  return {
    anchorTop,
    lastBottom,
    turnHeight,
    usableViewportHeight,
    visibleUsableBottom,
    overflowsUsableViewport: turnHeight > usableViewportHeight,
    targetScrollToRevealEnd,
    scrollDeltaToRevealEnd,
  };
}

export function getTimelineEndSpaceVisibility({
  state,
  composerOverlayHeight,
  anchorOffset,
}: {
  readonly state: TimelineListMeasurementState;
  readonly composerOverlayHeight: number;
  readonly anchorOffset: number;
}): boolean | null {
  if (state.data.length === 0) {
    return null;
  }

  const lastBottom = getRowBottom(state, state.data.length - 1);
  if (lastBottom === null) {
    return null;
  }

  const usableViewportHeight = Math.max(
    0,
    state.scrollLength - composerOverlayHeight - anchorOffset,
  );
  return state.scroll + usableViewportHeight > lastBottom + 1;
}

export function shouldAutoFollowTimeline({
  endSpaceVisible,
  followLatched,
  mainThreadHasActiveStatus,
  mainThreadSettled,
  manualScrollCooldownActive,
  userSendFollowActive,
}: TimelineAutoFollowGate): boolean {
  if (!followLatched || manualScrollCooldownActive) {
    return false;
  }

  if (userSendFollowActive) {
    return true;
  }

  if (mainThreadHasActiveStatus || !mainThreadSettled || endSpaceVisible !== false) {
    return false;
  }

  return true;
}
