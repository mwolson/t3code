import { describe, expect, it } from "vite-plus/test";

import { collapsedComposerActions } from "./ThreadComposer.logic";

describe("collapsed composer actions", () => {
  it("keeps Send primary when the draft has content", () => {
    expect(
      collapsedComposerActions({
        canStopThread: true,
        hasContent: true,
      }),
    ).toEqual({ showStopPrimary: false });
  });

  it("uses Stop as the primary action when the draft is empty", () => {
    expect(
      collapsedComposerActions({
        canStopThread: true,
        hasContent: false,
      }),
    ).toEqual({ showStopPrimary: true });
  });

  it("hides Stop when the thread cannot be interrupted", () => {
    expect(
      collapsedComposerActions({
        canStopThread: false,
        hasContent: false,
      }),
    ).toEqual({ showStopPrimary: false });
  });
});
