import { assert, describe, it } from "@effect/vitest";

import {
  providerIconBetaLabel,
  providerIconBetaMarker,
  providerIconKind,
} from "./providerIconKind";

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

describe("providerIconBetaLabel", () => {
  it("marks only OpenCode 2, which shares OpenCode 1's brand mark", () => {
    assert.strictEqual(providerIconBetaLabel("opencode2"), "Beta");
    assert.strictEqual(providerIconBetaLabel("opencode"), undefined);
    assert.strictEqual(providerIconBetaLabel("claude"), undefined);
    assert.strictEqual(providerIconBetaLabel("openai"), undefined);
  });
});

describe("providerIconBetaMarker", () => {
  it("draws the version number, not a dot, so it does not read as a status indicator", () => {
    assert.strictEqual(providerIconBetaMarker("opencode2"), "2");
  });

  it("draws nothing for kinds that need no marker", () => {
    assert.strictEqual(providerIconBetaMarker("opencode"), undefined);
    assert.strictEqual(providerIconBetaMarker("claude"), undefined);
    assert.strictEqual(providerIconBetaMarker("openai"), undefined);
  });
});
