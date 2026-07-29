import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";

import {
  escalateOpenCode2ServerTermination,
  openCode2AuthorizationHeader,
  parseOpenCode2Startup,
} from "./opencode2Runtime.ts";

describe("parseOpenCode2Startup", () => {
  // Exact banner emitted by @opencode-ai/cli 0.0.0-next-16339, captured by
  // live-scenarios/tests/opencode2-drive-probe.mjs.
  const banner = [
    "server listening on http://127.0.0.1:4711",
    "server password Yb4ypFttKtPUvcKlnzQ4iOEUezhRpP4A",
    "",
  ].join("\n");

  it("reads both facts out of the real banner", () => {
    assert.deepStrictEqual(parseOpenCode2Startup(banner), {
      url: "http://127.0.0.1:4711",
      password: "Yb4ypFttKtPUvcKlnzQ4iOEUezhRpP4A",
    });
  });

  it("withholds a result until the password line has also arrived", () => {
    // The two lines land in whatever chunks the pipe produces. Resolving on the
    // URL alone would build a client with no credentials, and 2.x answers every
    // unauthenticated request with 401 rather than an obvious startup failure.
    assert.isNull(parseOpenCode2Startup("server listening on http://127.0.0.1:4711\n"));
    assert.isNotNull(parseOpenCode2Startup(banner));
  });

  it("withholds a result until the url line has also arrived", () => {
    assert.isNull(parseOpenCode2Startup("server password abc123\n"));
  });

  it("does not match the 1.x banner", () => {
    // 1.x prints `opencode server listening on ...` and never prints a
    // password, so it must not satisfy the 2.x contract.
    assert.isNull(
      parseOpenCode2Startup(
        [
          "Warning: OPENCODE_SERVER_PASSWORD is not set; server is unsecured.",
          "opencode server listening on http://127.0.0.1:4607",
          "",
        ].join("\n"),
      ),
    );
  });

  it("tolerates surrounding log noise", () => {
    const noisy = [
      "[00:00:00.000] INFO (#1): starting",
      "server listening on http://127.0.0.1:9999",
      "some unrelated line",
      "server password s3cr3t-token_ABC",
    ].join("\n");
    assert.deepStrictEqual(parseOpenCode2Startup(noisy), {
      url: "http://127.0.0.1:9999",
      password: "s3cr3t-token_ABC",
    });
  });
});

describe("openCode2AuthorizationHeader", () => {
  it("encodes the fixed opencode username with the minted password", () => {
    assert.strictEqual(
      openCode2AuthorizationHeader("s3cr3t"),
      `Basic ${Buffer.from("opencode:s3cr3t", "utf8").toString("base64")}`,
    );
  });
});

describe("escalateOpenCode2ServerTermination", () => {
  it.effect("sends TERM, waits for the grace period, then sends KILL", () =>
    Effect.gen(function* () {
      const signals: Array<NodeJS.Signals> = [];
      const termination = yield* escalateOpenCode2ServerTermination((signal) =>
        Effect.sync(() => {
          signals.push(signal);
        }),
      ).pipe(Effect.forkScoped);

      yield* Effect.yieldNow;
      assert.deepStrictEqual(signals, ["SIGTERM"]);

      yield* TestClock.adjust("999 millis");
      assert.deepStrictEqual(signals, ["SIGTERM"]);

      yield* TestClock.adjust("1 millis");
      yield* Fiber.join(termination);
      assert.deepStrictEqual(signals, ["SIGTERM", "SIGKILL"]);
    }),
  );
});
