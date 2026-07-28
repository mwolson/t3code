import { assert, it } from "@effect/vitest";
import { describe } from "vite-plus/test";

import { parseGenericCliVersion } from "../providerSnapshot.ts";
import { openCode2NextBuild, parseOpenCode2Version } from "./OpenCode2Provider.ts";

const OPENCODE2_BANNER = "opencode2 v0.0.0-next-16339\n";

describe("parseOpenCode2Version", () => {
  // The reason this parser exists: the generic one anchors on `\b`, and the
  // `v` prefix kills the word boundary before the leading digit.
  it("parses the banner the generic CLI parser returns null for", () => {
    assert.strictEqual(parseGenericCliVersion(OPENCODE2_BANNER), null);
    assert.strictEqual(parseOpenCode2Version(OPENCODE2_BANNER), "0.0.0-next-16339");
  });

  it("parses a plain release version", () => {
    assert.strictEqual(parseOpenCode2Version("opencode2 2.1.4\n"), "2.1.4");
  });

  it("returns null when there is no version at all", () => {
    assert.strictEqual(
      parseOpenCode2Version("Error: @opencode-ai/cli's postinstall script was not run."),
      null,
    );
  });
});

describe("openCode2NextBuild", () => {
  it("reads the build number off the next line", () => {
    assert.strictEqual(openCode2NextBuild("0.0.0-next-16339"), 16339);
  });

  // A stable 2.x is not on the preview line, so the build gate must not apply
  // to it rather than rejecting it for lacking a build number.
  it("returns null for a version that is not on the next line", () => {
    assert.strictEqual(openCode2NextBuild("2.1.4"), null);
    assert.strictEqual(openCode2NextBuild("2.1.4-rc.1"), null);
  });
});
