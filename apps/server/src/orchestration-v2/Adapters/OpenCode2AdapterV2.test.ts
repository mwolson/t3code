import { assert, it } from "@effect/vitest";
import { describe } from "vite-plus/test";

import {
  openCode2AutoPermissionReply,
  openCode2QuestionId,
  unwrapOpenCode2Data,
} from "./OpenCode2AdapterV2.ts";

/**
 * `OpenCode2RuntimeError` is a `Data.TaggedError` whose text lives on `detail`,
 * not on `message`, so a regex match against the thrown value reads as empty.
 */
function detailOfThrow(run: () => unknown): string {
  try {
    run();
  } catch (cause) {
    return (cause as { readonly detail?: string }).detail ?? String(cause);
  }
  throw new Error("expected the call to throw");
}

describe("unwrapOpenCode2Data", () => {
  it("reads through both envelopes", () => {
    assert.deepStrictEqual(
      unwrapOpenCode2Data("session.create", { data: { data: { id: "ses_1" } } }),
      {
        id: "ses_1",
      },
    );
  });

  // The failure mode this guards is silent: reading one layer yields the
  // envelope, which looks like a valid object and fails much later.
  it("rejects a payload that carries only the outer envelope", () => {
    assert.match(
      detailOfThrow(() => unwrapOpenCode2Data("session.create", { data: {} })),
      /session.create returned no response payload/,
    );
  });

  it("rejects a missing payload", () => {
    assert.match(
      detailOfThrow(() => unwrapOpenCode2Data("session.get", {})),
      /session.get returned no response payload/,
    );
  });
});

describe("openCode2QuestionId", () => {
  it("slugs the header so answers keyed by id resolve", () => {
    assert.strictEqual(openCode2QuestionId(0, "Pick a Branch!"), "question-0-pick-a-branch");
  });

  it("falls back to the index when the header carries no usable characters", () => {
    assert.strictEqual(openCode2QuestionId(2, "  ???  "), "question-2");
  });
});

describe("openCode2AutoPermissionReply", () => {
  const policy = (overrides: Record<string, unknown>) =>
    ({
      cwd: "/tmp",
      runtimeMode: "default",
      interactionMode: "default",
      ...overrides,
    }) as never;

  it("auto-approves in full-access mode, since 2.x has no session permission ruleset", () => {
    assert.strictEqual(
      openCode2AutoPermissionReply(policy({ runtimeMode: "full-access" })),
      "always",
    );
  });

  it("auto-approves when the approval policy is explicitly never", () => {
    assert.strictEqual(
      openCode2AutoPermissionReply(policy({ runtimeMode: "default", approvalPolicy: "never" })),
      "always",
    );
  });

  it("surfaces the request when an approval policy asks for one", () => {
    assert.strictEqual(
      openCode2AutoPermissionReply(
        policy({ runtimeMode: "full-access", approvalPolicy: "always" }),
      ),
      null,
    );
  });

  // A structured approval policy is a request for interactive review, so
  // full-access must not silently override it.
  it("surfaces the request for a structured approval policy", () => {
    assert.strictEqual(
      openCode2AutoPermissionReply(
        policy({ runtimeMode: "full-access", approvalPolicy: { type: "onRequest" } }),
      ),
      null,
    );
  });

  it("surfaces the request outside full-access", () => {
    assert.strictEqual(
      openCode2AutoPermissionReply(policy({ runtimeMode: "auto-accept-edits" })),
      null,
    );
  });
});
