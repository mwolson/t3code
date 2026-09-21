import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vite-plus/test";

import {
  dismissThreadErrorBannerForSession,
  getThreadErrorBannerKey,
  isThreadErrorBannerDismissedForSession,
  shouldShowThreadErrorBanner,
  resolveThreadErrorBanner,
  ThreadErrorBanner,
} from "./ThreadErrorBanner";

describe("ThreadErrorBanner", () => {
  it("stays hidden after its current error is dismissed", () => {
    const bannerKey = getThreadErrorBannerKey("env:thread-a", "Aborted");
    dismissThreadErrorBannerForSession(bannerKey);

    expect(
      shouldShowThreadErrorBanner(
        "env:thread-a",
        "Aborted",
        isThreadErrorBannerDismissedForSession(bannerKey),
      ),
    ).toBe(false);
  });

  it("reappears when a new error arrives on the same thread", () => {
    dismissThreadErrorBannerForSession(getThreadErrorBannerKey("env:thread-b", "Turn failed"));
    const newErrorKey = getThreadErrorBannerKey("env:thread-b", "Provider crashed");

    expect(isThreadErrorBannerDismissedForSession(newErrorKey)).toBe(false);
    expect(
      shouldShowThreadErrorBanner(
        "env:thread-b",
        "Provider crashed",
        isThreadErrorBannerDismissedForSession(newErrorKey),
      ),
    ).toBe(true);
  });

  it("scopes dismissals to the thread that dismissed them", () => {
    dismissThreadErrorBannerForSession(getThreadErrorBannerKey("env:thread-c", "Aborted"));
    const otherThreadKey = getThreadErrorBannerKey("env:other-thread", "Aborted");

    expect(isThreadErrorBannerDismissedForSession(otherThreadKey)).toBe(false);
    expect(
      shouldShowThreadErrorBanner(
        "env:other-thread",
        "Aborted",
        isThreadErrorBannerDismissedForSession(otherThreadKey),
      ),
    ).toBe(true);
  });

  it("keeps a dismissal across visiting threads with no error", () => {
    const bannerKey = getThreadErrorBannerKey("env:thread-d", "Aborted");
    dismissThreadErrorBannerForSession(bannerKey);

    expect(shouldShowThreadErrorBanner("env:thread-d", null, false)).toBe(false);
    expect(isThreadErrorBannerDismissedForSession(bannerKey)).toBe(true);
    expect(
      shouldShowThreadErrorBanner(
        "env:thread-d",
        "Aborted",
        isThreadErrorBannerDismissedForSession(bannerKey),
      ),
    ).toBe(false);
  });

  it("distinguishes same-text occurrences and local errors from hidden runtime failures", () => {
    const thread = "env:occurrence";
    const first = getThreadErrorBannerKey(thread, "Failed", "2026-09-20T01:00:00Z");
    dismissThreadErrorBannerForSession(first);
    expect(
      isThreadErrorBannerDismissedForSession(
        getThreadErrorBannerKey(thread, "Failed", "2026-09-20T01:00:00Z"),
      ),
    ).toBe(true);
    expect(
      isThreadErrorBannerDismissedForSession(
        getThreadErrorBannerKey(thread, "Failed", "2026-09-20T02:00:00Z"),
      ),
    ).toBe(false);
    const local = getThreadErrorBannerKey(thread, "Failed", 12, "local");
    dismissThreadErrorBannerForSession(local);
    expect(
      isThreadErrorBannerDismissedForSession(
        getThreadErrorBannerKey(thread, "Failed", 13, "local"),
      ),
    ).toBe(false);
    expect(
      isThreadErrorBannerDismissedForSession(
        getThreadErrorBannerKey(thread, "Failed", 12, "runtime"),
      ),
    ).toBe(false);
  });

  it("uses the visible source occurrence and ignores hidden runtime refreshes", () => {
    const input = {
      threadKey: "env:precedence",
      localError: "Failed",
      localErrorAt: 1,
      runtime: { lastError: "Failed", lastErrorAt: "2026-09-20T01:00:00Z" },
    };
    const local = resolveThreadErrorBanner(input);
    dismissThreadErrorBannerForSession(local.key);
    expect(
      resolveThreadErrorBanner({
        ...input,
        runtime: { ...input.runtime, lastErrorAt: "2026-09-20T02:00:00Z" },
      }),
    ).toEqual(local);
    const laterLocal = resolveThreadErrorBanner({ ...input, localErrorAt: 2 });
    expect(isThreadErrorBannerDismissedForSession(laterLocal.key)).toBe(false);
    const runtime = resolveThreadErrorBanner({ ...input, localError: null });
    expect(isThreadErrorBannerDismissedForSession(runtime.key)).toBe(false);
    const unknownAt = resolveThreadErrorBanner({
      ...input,
      localError: null,
      runtime: { lastError: "Failed", lastErrorAt: null },
    });
    expect(unknownAt.key).toBe(getThreadErrorBannerKey(input.threadKey, "Failed"));
  });

  it("never shows a null error", () => {
    expect(shouldShowThreadErrorBanner("env:thread-e", null, false)).toBe(false);
  });
  it("aligns the warning and dismiss icons with the first line of a multi-line error", () => {
    const markup = renderToStaticMarkup(
      <ThreadErrorBanner
        error={"The first error line\ncontinues on a second line"}
        onDismiss={() => {}}
      />,
    );

    expect(markup).toContain('role="alert"');
    expect(markup).toContain('aria-label="Dismiss error"');
    expect(markup).not.toContain("controlAlignment");
    expect(markup).toContain("flex gap-2 items-start");
    expect(markup).toContain("min-h-7 pt-1 sm:min-h-6 sm:pt-0.5");
    expect(markup).toContain("h-lh w-4");
    expect(markup).toContain("h-lh self-start");
  });
});
