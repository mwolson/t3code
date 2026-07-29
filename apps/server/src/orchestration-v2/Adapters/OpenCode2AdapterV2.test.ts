import type { SessionPendingInfo, ShellInfoV2 } from "@opencode-ai/sdk-next/v2";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { describe } from "vite-plus/test";

import {
  openCode2AutoPermissionReply,
  openCode2PendingWorkForSession,
  openCode2QuestionId,
  openCode2ToolNeedsTerminalOverride,
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

describe("openCode2PendingWorkForSession", () => {
  const sessionID = "ses_target";
  const pending = (owner: string): SessionPendingInfo => ({
    admittedSeq: 1,
    id: "pending-1",
    sessionID: owner,
    timeCreated: 1,
    type: "compaction",
  });
  const shell = (owner: string, status: ShellInfoV2["status"]): ShellInfoV2 => ({
    id: "shell-1",
    status,
    command: "sleep 20",
    cwd: "/workspace",
    shell: "/bin/bash",
    file: "/workspace/shell.out",
    metadata: { sessionID: owner },
    time: { started: 1 },
  });

  it.effect("pins the thread for its durable pending input without listing shells", () =>
    Effect.gen(function* () {
      let listedShells = false;
      const result = yield* openCode2PendingWorkForSession({
        sessionID,
        pending: Effect.succeed([pending(sessionID)]),
        shells: Effect.sync(() => {
          listedShells = true;
          return [];
        }),
      });

      assert.isTrue(result);
      assert.isFalse(listedShells);
    }),
  );

  it.effect("pins only running shells owned by the same native session", () =>
    Effect.gen(function* () {
      assert.isTrue(
        yield* openCode2PendingWorkForSession({
          sessionID,
          pending: Effect.succeed([]),
          shells: Effect.succeed([shell(sessionID, "running")]),
        }),
      );
      assert.isFalse(
        yield* openCode2PendingWorkForSession({
          sessionID,
          pending: Effect.succeed([pending("ses_sibling")]),
          shells: Effect.succeed([shell("ses_sibling", "running"), shell(sessionID, "exited")]),
        }),
      );
    }),
  );
});

describe("openCode2ToolNeedsTerminalOverride", () => {
  const part = (status: "pending" | "running" | "completed" | "error", errorMessage?: string) => ({
    status,
    errorMessage,
  });

  it("terminalizes tools that have no native terminal state", () => {
    assert.isTrue(openCode2ToolNeedsTerminalOverride(part("pending"), "failed"));
    assert.isTrue(openCode2ToolNeedsTerminalOverride(part("running"), "interrupted"));
  });

  it("restamps only the provider's interrupt-specific tool failure", () => {
    assert.isTrue(
      openCode2ToolNeedsTerminalOverride(
        part("error", "Tool execution interrupted"),
        "interrupted",
      ),
    );
    assert.isFalse(
      openCode2ToolNeedsTerminalOverride(part("error", "command failed"), "interrupted"),
    );
    assert.isFalse(
      openCode2ToolNeedsTerminalOverride(part("error", "Tool execution interrupted"), "failed"),
    );
  });

  it("preserves completed tools", () => {
    assert.isFalse(openCode2ToolNeedsTerminalOverride(part("completed"), "interrupted"));
  });
});
