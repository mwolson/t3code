import { assert, describe, it } from "@effect/vitest";

import { providerIconKind } from "./providerIconKind";

describe("providerIconKind", () => {
  it("keeps both OpenCode generations distinct", () => {
    assert.strictEqual(providerIconKind("opencode"), "opencode");
    assert.strictEqual(providerIconKind("opencode2"), "opencode2");
  });

  it("preserves existing provider fallbacks", () => {
    assert.strictEqual(providerIconKind("claudeAgent"), "claude");
    assert.strictEqual(providerIconKind("codex"), "openai");
    assert.strictEqual(providerIconKind(undefined), "openai");
  });
});
