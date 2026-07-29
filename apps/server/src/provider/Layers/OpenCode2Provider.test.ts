import { assert, it } from "@effect/vitest";
import type { ModelInfo } from "@opencode-ai/sdk-next/v2";
import * as Effect from "effect/Effect";
import { describe } from "vite-plus/test";

import { parseGenericCliVersion } from "../providerSnapshot.ts";
import {
  openCode2NextBuild,
  parseOpenCode2Version,
  retryEmptyOpenCode2Inventory,
} from "./OpenCode2Provider.ts";

const OPENCODE2_BANNER = "opencode2 v0.0.0-next-16339\n";
const BIG_PICKLE_MODEL = {
  id: "big-pickle",
  modelID: "big-pickle",
  providerID: "opencode",
  name: "Big Pickle",
  capabilities: {
    tools: true,
    input: ["text"],
    output: ["text"],
  },
  variants: [],
  time: {
    released: 0,
  },
  cost: [],
  status: "active",
  enabled: true,
  limit: {
    context: 128_000,
    output: 16_384,
  },
} satisfies ModelInfo;

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

describe("retryEmptyOpenCode2Inventory", () => {
  it.effect("retries an empty startup inventory and stops when models appear", () =>
    Effect.gen(function* () {
      let reads = 0;
      const inventory = yield* retryEmptyOpenCode2Inventory(
        Effect.sync(() => {
          reads += 1;
          return reads === 1
            ? { models: [], agents: [] }
            : { models: [BIG_PICKLE_MODEL], agents: [] };
        }),
        { maxAttempts: 5, retryDelayMs: 0 },
      );

      assert.strictEqual(reads, 2);
      assert.strictEqual(inventory.models[0]?.providerID, "opencode");
      assert.strictEqual(inventory.models[0]?.modelID, "big-pickle");
    }),
  );

  it.effect("stops after the configured number of empty attempts", () =>
    Effect.gen(function* () {
      let reads = 0;
      const inventory = yield* retryEmptyOpenCode2Inventory(
        Effect.sync(() => {
          reads += 1;
          return { models: [], agents: [] };
        }),
        { maxAttempts: 3, retryDelayMs: 0 },
      );

      assert.strictEqual(reads, 3);
      assert.deepStrictEqual(inventory, { models: [], agents: [] });
    }),
  );
});
