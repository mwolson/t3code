import { describe, expect, it } from "vite-plus/test";
import {
  getAnchoredTurnMetrics,
  getRowBottom,
  getTimelineEndSpaceVisibility,
  shouldAutoFollowTimeline,
} from "./timelineScrollAnchoring";

function buildState({
  positions,
  sizes,
  scroll = 0,
  scrollLength = 700,
}: {
  readonly positions: readonly number[];
  readonly sizes: readonly number[];
  readonly scroll?: number;
  readonly scrollLength?: number;
}) {
  return {
    data: positions.map((_, index) => index),
    scroll,
    scrollLength,
    positionAtIndex: (index: number) => positions[index],
    sizeAtIndex: (index: number) => sizes[index],
  };
}

describe("timeline scroll anchoring", () => {
  it("measures row bottoms from LegendList row position and size", () => {
    const state = buildState({
      positions: [0, 120],
      sizes: [80, 40],
    });

    expect(getRowBottom(state, 1)).toBe(160);
  });

  it("treats the active turn as fitting when it fits above the composer", () => {
    const state = buildState({
      positions: [0, 300, 460],
      sizes: [240, 80, 140],
      scrollLength: 760,
    });

    const metrics = getAnchoredTurnMetrics({
      state,
      anchorIndex: 1,
      composerOverlayHeight: 180,
      anchorOffset: 16,
    });

    expect(metrics?.turnHeight).toBe(300);
    expect(metrics?.usableViewportHeight).toBe(564);
    expect(metrics?.overflowsUsableViewport).toBe(false);
    expect(metrics?.targetScrollToRevealEnd).toBe(36);
    expect(metrics?.scrollDeltaToRevealEnd).toBe(36);
  });

  it("targets the real row end instead of any temporary reserved tail", () => {
    const state = buildState({
      positions: [0, 1720, 1880],
      sizes: [1600, 80, 120],
      scroll: 1900,
      scrollLength: 760,
    });

    const metrics = getAnchoredTurnMetrics({
      state,
      anchorIndex: 1,
      composerOverlayHeight: 180,
      anchorOffset: 16,
    });

    expect(metrics?.lastBottom).toBe(2000);
    expect(metrics?.targetScrollToRevealEnd).toBe(1436);
    expect(metrics?.scrollDeltaToRevealEnd).toBe(0);
  });

  it("reports overflow only for the current anchored turn", () => {
    const state = buildState({
      positions: [0, 900, 1180],
      sizes: [800, 220, 300],
      scroll: 900,
      scrollLength: 760,
    });

    const metrics = getAnchoredTurnMetrics({
      state,
      anchorIndex: 1,
      composerOverlayHeight: 180,
      anchorOffset: 16,
    });

    expect(metrics?.turnHeight).toBe(580);
    expect(metrics?.usableViewportHeight).toBe(564);
    expect(metrics?.overflowsUsableViewport).toBe(true);
  });

  it("returns the minimal positive scroll delta needed to reveal the turn end", () => {
    const state = buildState({
      positions: [0, 900, 1180],
      sizes: [800, 220, 360],
      scroll: 900,
      scrollLength: 760,
    });

    const metrics = getAnchoredTurnMetrics({
      state,
      anchorIndex: 1,
      composerOverlayHeight: 180,
      anchorOffset: 16,
    });

    expect(metrics?.lastBottom).toBe(1540);
    expect(metrics?.visibleUsableBottom).toBe(1464);
    expect(metrics?.scrollDeltaToRevealEnd).toBe(76);
  });

  it("subtracts composer height from usable viewport height", () => {
    const state = buildState({
      positions: [0, 300],
      sizes: [120, 470],
      scrollLength: 700,
    });

    const withoutComposer = getAnchoredTurnMetrics({
      state,
      anchorIndex: 1,
      composerOverlayHeight: 0,
      anchorOffset: 16,
    });
    const withComposer = getAnchoredTurnMetrics({
      state,
      anchorIndex: 1,
      composerOverlayHeight: 220,
      anchorOffset: 16,
    });

    expect(withoutComposer?.overflowsUsableViewport).toBe(false);
    expect(withComposer?.overflowsUsableViewport).toBe(true);
  });

  it("detects blank end space within the usable viewport", () => {
    const state = buildState({
      positions: [0, 520],
      sizes: [480, 80],
      scroll: 120,
      scrollLength: 700,
    });

    expect(
      getTimelineEndSpaceVisibility({
        state,
        composerOverlayHeight: 120,
        anchorOffset: 16,
      }),
    ).toBe(true);
  });

  it("does not report end space when the last row reaches below the viewport", () => {
    const state = buildState({
      positions: [0, 520],
      sizes: [480, 240],
      scroll: 120,
      scrollLength: 700,
    });

    expect(
      getTimelineEndSpaceVisibility({
        state,
        composerOverlayHeight: 120,
        anchorOffset: 16,
      }),
    ).toBe(false);
  });
});

describe("timeline auto-follow gate", () => {
  const readyGate = {
    endSpaceVisible: false,
    followLatched: true,
    mainThreadHasActiveStatus: false,
    mainThreadSettled: true,
    manualScrollCooldownActive: false,
    userSendFollowActive: false,
  } as const;

  it("allows follow after the thread settles with hidden end space and no recent input", () => {
    expect(shouldAutoFollowTimeline(readyGate)).toBe(true);
  });

  it("blocks follow while Working or Waiting and during the settled delay", () => {
    expect(
      shouldAutoFollowTimeline({
        ...readyGate,
        mainThreadHasActiveStatus: true,
      }),
    ).toBe(false);
    expect(
      shouldAutoFollowTimeline({
        ...readyGate,
        mainThreadSettled: false,
      }),
    ).toBe(false);
  });

  it("allows a user send to follow while active even when end space is visible", () => {
    expect(
      shouldAutoFollowTimeline({
        ...readyGate,
        endSpaceVisible: true,
        mainThreadHasActiveStatus: true,
        mainThreadSettled: false,
        userSendFollowActive: true,
      }),
    ).toBe(true);
  });

  it("blocks follow when blank end space is visible or cannot be measured", () => {
    expect(
      shouldAutoFollowTimeline({
        ...readyGate,
        endSpaceVisible: true,
      }),
    ).toBe(false);
    expect(
      shouldAutoFollowTimeline({
        ...readyGate,
        endSpaceVisible: null,
      }),
    ).toBe(false);
  });

  it("requires both an armed latch and an expired manual-scroll cooldown", () => {
    expect(
      shouldAutoFollowTimeline({
        ...readyGate,
        manualScrollCooldownActive: true,
      }),
    ).toBe(false);
    expect(
      shouldAutoFollowTimeline({
        ...readyGate,
        followLatched: false,
      }),
    ).toBe(false);
  });
});
