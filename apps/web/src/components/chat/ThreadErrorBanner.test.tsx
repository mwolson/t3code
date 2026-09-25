import { describe, expect, it } from "vite-plus/test";

import {
  dismissThreadErrorBannerForSession,
  getThreadErrorBannerKey,
  getThreadErrorBannerMessage,
  isThreadErrorBannerDismissedForSession,
  shouldShowThreadErrorBanner,
  resolveThreadErrorBanner,
} from "./ThreadErrorBanner";

describe("getThreadErrorBannerMessage", () => {
  it("extracts a readable nested message without trimming its whitespace", () => {
    const message =
      "  This model is not supported with a ChatGPT account.\nChoose another model.  ";
    const error = ` \n${JSON.stringify({ error: { message, code: "unsupported_model" } })}\t `;

    expect(getThreadErrorBannerMessage(error)).toBe(message);
  });

  it("keeps the diagnostics when the nested message is blank", () => {
    const error = JSON.stringify({ error: { message: " \n\t ", code: "unsupported_model" } });

    expect(getThreadErrorBannerMessage(error)).toBe(error);
  });
});

describe("ThreadErrorBanner", () => {
  it("reappears for different diagnostics with the same headline on the same thread", () => {
    const firstError = JSON.stringify({ error: { message: "Turn failed", code: "first" } });
    const secondError = JSON.stringify({ error: { message: "Turn failed", code: "second" } });
    const dismissedKey = getThreadErrorBannerKey("env:thread-b", firstError);
    dismissThreadErrorBannerForSession(dismissedKey);
    const newErrorKey = getThreadErrorBannerKey("env:thread-b", secondError);

    expect(isThreadErrorBannerDismissedForSession(dismissedKey)).toBe(true);
    expect(isThreadErrorBannerDismissedForSession(newErrorKey)).toBe(false);
    expect(
      shouldShowThreadErrorBanner(
        "env:thread-b",
        secondError,
        isThreadErrorBannerDismissedForSession(newErrorKey),
      ),
    ).toBe(true);
  });

  it("shows the same message again for a later occurrence", () => {
    const input = {
      threadKey: "env:repeated-runtime-failure",
      localError: null,
      localErrorAt: null,
      runtime: { lastError: "Provider failed", lastErrorAt: "2026-10-16T01:00:00Z" },
    };
    const first = resolveThreadErrorBanner(input);
    dismissThreadErrorBannerForSession(first.key);
    const revisited = resolveThreadErrorBanner(input);
    expect(
      shouldShowThreadErrorBanner(
        input.threadKey,
        revisited.error,
        isThreadErrorBannerDismissedForSession(revisited.key),
      ),
    ).toBe(false);

    const later = resolveThreadErrorBanner({
      ...input,
      runtime: { ...input.runtime, lastErrorAt: "2026-10-16T02:00:00Z" },
    });
    expect(later.error).toBe(first.error);
    expect(
      shouldShowThreadErrorBanner(
        input.threadKey,
        later.error,
        isThreadErrorBannerDismissedForSession(later.key),
      ),
    ).toBe(true);
  });
});
