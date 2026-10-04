import type { PreviewMiniPlayerFrame } from "../preview/previewMiniPlayerLayout";

export function resolveThreadDetailsCardDensity(
  height: number,
  content: { full: number; compact: number },
) {
  if (content.full === 0 || content.full <= height) return "full";
  if (content.compact === 0 || content.compact <= height) return "compact";
  return "essential";
}

export interface ThreadDetailsContentHeights {
  readonly full: number;
  readonly compact: number;
}

export interface ThreadDetailsContentMeasurement {
  readonly key: string;
  /** Stable per-density baselines, used only to choose a density. */
  readonly heights: ThreadDetailsContentHeights;
  /**
   * Latest measured full natural height. This is the card's real footprint when
   * open, so obstacles that avoid the card use it rather than a density
   * baseline, which stays at the collapsed minimum.
   */
  readonly latestFull: number;
}

/**
 * Density is chosen from the smallest height each density has measured for the
 * current key. Watching the latest measurement instead would let the bounded
 * Previous-agents lineage, once expanded, grow the full height past the card,
 * fold it to compact, and unmount the section the user just opened. The list
 * scrolls inside the card rather than lowering the density, and the card still
 * folds below its collapsed content. The latest full height is kept separately
 * for the card's reported footprint, which must clear the real expanded card
 * rather than a density baseline and survives folding.
 */
export function recordThreadDetailsContentHeight(
  current: ThreadDetailsContentMeasurement,
  key: string,
  density: "full" | "compact",
  measured: number,
): ThreadDetailsContentMeasurement {
  const sameKey = current.key === key;
  const heights = sameKey ? current.heights : { full: 0, compact: 0 };
  const baseline = heights[density];
  const value = baseline === 0 ? measured : Math.min(baseline, measured);
  const latestFull = density === "full" ? measured : sameKey ? current.latestFull : 0;
  if (sameKey && baseline === value && current.latestFull === latestFull) return current;
  return { key, heights: { ...heights, [density]: value }, latestFull };
}

/** The card uses leftover space. It never changes the conversation's width. */
export function resolveThreadDetailsCardLayout({
  container,
  chat,
  frame,
  overlapsDetailsCard = false,
}: {
  container: { width: number; height: number };
  chat: { left: number; width: number };
  frame: PreviewMiniPlayerFrame | null;
  overlapsDetailsCard?: boolean;
}) {
  const gap = 12;
  const width = Math.min(312, container.width - chat.left - chat.width - gap * 2);
  if (width < 240) return null;
  const x = container.width - width - gap;
  // Resizing consumes the height above the player. Dragging first tries to
  // clear the full card and folds it only when there is no readable placement.
  const height =
    overlapsDetailsCard && frame && frame.x + frame.width > x - gap && frame.x < x + width + gap
      ? Math.min(container.height - gap * 2, frame.y - gap * 2)
      : container.height - gap * 2;
  if (height < 160) return null;
  return {
    x,
    width,
    y: gap,
    height,
  } as const;
}
