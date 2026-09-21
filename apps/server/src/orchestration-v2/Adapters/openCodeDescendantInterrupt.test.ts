import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { OpenCodeRuntimeError } from "../../provider/opencodeRuntime.ts";
import { interruptOpenCode2Descendants } from "./OpenCodeAdapterV2.ts";

const empty = { data: [], cursor: {} };

describe("OpenCode descendant Stop transport boundary", () => {
  it.effect(
    "preserves an abort failure after visiting later pages, siblings, and grandchildren",
    () =>
      Effect.gen(function* () {
        const stopped: string[] = [];
        const failure = new OpenCodeRuntimeError({
          operation: "session.interrupt",
          category: "network-failed",
        });
        const result = yield* interruptOpenCode2Descendants({
          rootId: "root",
          list: ({ parentID, cursor }) =>
            Effect.succeed(
              parentID === "root"
                ? {
                    data: [{ id: cursor === undefined ? "first" : "second" }],
                    cursor: { next: cursor === undefined ? "page2" : null },
                  }
                : {
                    data: parentID === "first" ? [{ id: "grandchild" }, { id: "root" }] : [],
                    cursor: {},
                  },
            ),
          interrupt: (id) =>
            Effect.gen(function* () {
              stopped.push(id);
              if (id === "first") return yield* failure;
            }),
        }).pipe(Effect.result);
        assert.deepEqual(stopped, ["first", "second", "grandchild"]);
        assert.strictEqual(result._tag, "Failure");
        if (result._tag === "Failure") assert.strictEqual(result.failure, failure);
      }),
  );

  it.effect("treats a disappeared child as success but does not swallow other list errors", () =>
    Effect.gen(function* () {
      const missing = new OpenCodeRuntimeError({
        operation: "session.interrupt",
        category: "sdk-request-failed",
        cause: { status: 404 },
      });
      yield* interruptOpenCode2Descendants({
        rootId: "root",
        list: ({ parentID }) =>
          Effect.succeed(parentID === "root" ? { data: [{ id: "gone" }], cursor: {} } : empty),
        interrupt: () => missing,
      });
      const failure = new OpenCodeRuntimeError({
        operation: "session.list",
        category: "network-failed",
      });
      const result = yield* interruptOpenCode2Descendants({
        rootId: "root",
        list: () => failure,
        interrupt: () => Effect.void,
      }).pipe(Effect.result);
      assert.strictEqual(result._tag, "Failure");
      if (result._tag === "Failure") assert.strictEqual(result.failure, failure);
    }),
  );

  it.effect("rejects a repeated pagination cursor rather than silently missing descendants", () =>
    Effect.gen(function* () {
      const result = yield* interruptOpenCode2Descendants({
        rootId: "root",
        list: () => Effect.succeed({ data: [], cursor: { next: "same" } }),
        interrupt: () => Effect.void,
      }).pipe(Effect.result);
      assert.strictEqual(result._tag, "Failure");
    }),
  );
});
