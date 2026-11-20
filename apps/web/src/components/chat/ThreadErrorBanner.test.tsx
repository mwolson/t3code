import { describe, expect, it } from "vite-plus/test";

import {
  dismissThreadErrorBannerForSession,
  getThreadErrorBannerKey,
  getThreadErrorBannerMessage,
  isThreadErrorBannerDismissedForSession,
  resolveThreadErrorBanner,
  shouldShowThreadErrorBanner,
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

  it.each([
    { label: "the same text in a later occurrence", localError: null },
    { label: "a local copy of the runtime error", localError: "Turn failed" },
  ])("uses the new runtime key for $label", ({ label, localError }) => {
    const threadKey = `env:thread-c:${label}`;
    const lastError = "Turn failed";
    const runtime = { lastError, lastErrorAt: "2026-09-25T00:00:00Z" };
    const first = resolveThreadErrorBanner({
      threadKey,
      localError: null,
      localErrorAt: null,
      runtime,
    });
    dismissThreadErrorBannerForSession(first.key);
    const lastErrorAt = "2026-09-25T00:01:00Z";
    const later = resolveThreadErrorBanner({
      threadKey,
      localError,
      localErrorAt: localError === null ? null : 1,
      runtime: { ...runtime, lastErrorAt },
    });
    const expectedKey = getThreadErrorBannerKey(threadKey, lastError, lastErrorAt);

    expect(later).toEqual({ error: lastError, key: expectedKey, runtimeKey: expectedKey });
    expect(isThreadErrorBannerDismissedForSession(first.key)).toBe(true);
    expect(isThreadErrorBannerDismissedForSession(later.key)).toBe(false);
  });
});
