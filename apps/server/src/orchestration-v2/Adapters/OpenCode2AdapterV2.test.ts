import type { SessionPendingInfo, ShellInfoV2 } from "@opencode-ai/sdk-next/v2";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { describe } from "vite-plus/test";

import {
  openCode2AutoPermissionReply,
  openCode2EnvironmentWithT3Mcp,
  openCode2InterruptedThreadDisposition,
  openCode2PendingWorkForSession,
  openCode2PermissionAutoReply,
  openCode2QuestionId,
  openCode2SessionErrorMessage,
  openCode2SessionErrorStatus,
  openCode2SessionErrorTargetSessionIds,
  openCode2ShouldSettleTurn,
  openCode2ToolNeedsTerminalOverride,
  removeOpenCode2Session,
  unwrapOpenCode2Data,
} from "./OpenCode2AdapterV2.ts";

const t3McpSession = {
  environmentId: EnvironmentId.make("environment:test"),
  threadId: ThreadId.make("thread:test"),
  providerSessionId: "provider-session:test",
  providerInstanceId: ProviderInstanceId.make("opencode2"),
  endpoint: "http://127.0.0.1:43123/mcp",
  authorizationHeader: "Bearer test-token",
};
const decodeJson = Schema.decodeUnknownSync(Schema.UnknownFromJsonString);
const encodeJson = Schema.encodeSync(Schema.UnknownFromJsonString);

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

describe("removeOpenCode2Session", () => {
  it.effect("treats an already-missing native session as deleted", () =>
    removeOpenCode2Session(
      "ses_missing",
      Effect.succeed({
        data: undefined,
        error: { name: "SessionNotFoundError" },
        response: { status: 404 },
      }),
    ),
  );

  it.effect("retains non-idempotent native deletion failures", () =>
    Effect.gen(function* () {
      const failure = yield* removeOpenCode2Session(
        "ses_broken",
        Effect.succeed({
          data: undefined,
          error: { name: "InternalServerError" },
          response: { status: 500 },
        }),
      ).pipe(Effect.flip);

      assert.strictEqual(failure.operation, "session.remove");
      assert.match(failure.detail, /InternalServerError/);
    }),
  );
});

describe("openCode2QuestionId", () => {
  it("slugs the header so answers keyed by id resolve", () => {
    assert.strictEqual(openCode2QuestionId(0, "Pick a Branch!"), "question-0-pick-a-branch");
  });

  it("falls back to the index when the header carries no usable characters", () => {
    assert.strictEqual(openCode2QuestionId(2, "  ???  "), "question-2");
  });
});

describe("openCode2EnvironmentWithT3Mcp", () => {
  it.effect("merges a per-thread server into process-local inline config", () =>
    Effect.gen(function* () {
      const environment = {
        CUSTOM_ENV: "preserved",
        OPENCODE_CONFIG_CONTENT: encodeJson({
          agent: { build: { mode: "primary" } },
          mcp: {
            existing: {
              type: "local",
              command: ["existing-mcp"],
            },
          },
        }),
      };
      const result = yield* openCode2EnvironmentWithT3Mcp(environment, t3McpSession);
      const resultEnvironment: NodeJS.ProcessEnv = result;

      assert.strictEqual(resultEnvironment.CUSTOM_ENV, "preserved");
      assert.deepStrictEqual(decodeJson(result.OPENCODE_CONFIG_CONTENT ?? ""), {
        agent: { build: { mode: "primary" } },
        mcp: {
          existing: {
            type: "local",
            command: ["existing-mcp"],
          },
          "t3-code": {
            type: "remote",
            url: t3McpSession.endpoint,
            headers: { Authorization: t3McpSession.authorizationHeader },
            oauth: false,
          },
        },
      });
      assert.notStrictEqual(result, environment);
    }),
  );

  it.effect("rejects inline config whose MCP field cannot be merged safely", () =>
    Effect.gen(function* () {
      const failure = yield* openCode2EnvironmentWithT3Mcp(
        { OPENCODE_CONFIG_CONTENT: encodeJson({ mcp: false }) },
        t3McpSession,
      ).pipe(Effect.flip);

      assert.isDefined(failure);
    }),
  );
});

describe("openCode2AutoPermissionReply", () => {
  const policy = (overrides: Record<string, unknown>) =>
    ({
      cwd: "/tmp",
      runtimeMode: "default",
      interactionMode: "default",
      ...overrides,
    }) as never;
  const reply = (
    overrides: Record<string, unknown>,
    action: string,
    resources: ReadonlyArray<string> = ["*"],
  ) => openCode2AutoPermissionReply(policy(overrides), { action, resources });

  it("approves only the current request in full-access mode", () => {
    assert.strictEqual(reply({ runtimeMode: "full-access" }, "bash"), "once");
  });

  it("does not turn approval never into implicit full access", () => {
    assert.strictEqual(reply({ runtimeMode: "auto", approvalPolicy: "never" }, "bash"), "reject");
    assert.strictEqual(reply({ runtimeMode: "auto", approvalPolicy: "never" }, "read"), "once");
  });

  it("surfaces the request when an approval policy asks for one", () => {
    assert.strictEqual(
      reply({ runtimeMode: "full-access", approvalPolicy: "always" }, "bash"),
      null,
    );
  });

  // A structured approval policy is a request for interactive review, so
  // full-access must not silently override it.
  it("surfaces the request for a structured approval policy", () => {
    assert.strictEqual(
      reply({ runtimeMode: "full-access", approvalPolicy: { type: "onRequest" } }, "bash"),
      null,
    );
  });

  it("auto-accepts edits but still asks for shell access", () => {
    assert.strictEqual(reply({ runtimeMode: "auto-accept-edits" }, "edit"), "once");
    assert.strictEqual(reply({ runtimeMode: "auto-accept-edits" }, "bash"), null);
  });

  it("enforces workspace-write and network policy without native persistent grants", () => {
    const sandboxPolicy = {
      type: "workspaceWrite",
      networkAccess: true,
      writableRoots: ["/workspace/shared"],
    };
    const overrides = {
      runtimeMode: "auto",
      approvalPolicy: "never",
      sandboxPolicy,
    };
    assert.strictEqual(reply(overrides, "edit"), "once");
    assert.strictEqual(reply(overrides, "bash"), "reject");
    assert.strictEqual(reply(overrides, "websearch"), "once");
    assert.strictEqual(
      reply(overrides, "external_directory", ["/workspace/shared/file.txt"]),
      "once",
    );
    assert.strictEqual(reply(overrides, "external_directory", ["/outside/file.txt"]), "reject");
  });

  it("does not let a remembered session grant override a later policy denial", () => {
    assert.strictEqual(
      openCode2PermissionAutoReply(
        policy({ runtimeMode: "auto", approvalPolicy: "never" }),
        [{ action: "bash", resources: ["*"] }],
        { action: "bash", resources: ["*"] },
      ),
      "reject",
    );
  });

  it("uses a remembered session grant when policy still requires approval", () => {
    assert.strictEqual(
      openCode2PermissionAutoReply(
        policy({ runtimeMode: "default" }),
        [{ action: "bash", resources: ["/workspace/*"] }],
        { action: "bash", resources: ["/workspace/file.txt"] },
      ),
      "once",
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

describe("OpenCode 2 session errors", () => {
  it("fans an unscoped error out to every active native session", () => {
    assert.deepStrictEqual(
      openCode2SessionErrorTargetSessionIds(undefined, ["ses_first", "ses_second"]),
      ["ses_first", "ses_second"],
    );
    assert.deepStrictEqual(
      openCode2SessionErrorTargetSessionIds("ses_second", ["ses_first", "ses_second"]),
      ["ses_second"],
    );
    assert.deepStrictEqual(
      openCode2SessionErrorTargetSessionIds("ses_missing", ["ses_first", "ses_second"]),
      [],
    );
  });

  it("normalizes provider abort errors without poisoning the provider session", () => {
    const error = {
      sessionID: "ses_target",
      error: {
        name: "MessageAbortedError",
        data: { message: "The user aborted the request." },
      },
    } as const;

    assert.strictEqual(openCode2SessionErrorMessage(error), "The user aborted the request.");
    assert.strictEqual(openCode2SessionErrorStatus(error, false), "interrupted");
  });

  it("preserves ordinary provider failures", () => {
    const error = {
      error: {
        name: "UnknownError",
        data: { message: "Provider exploded." },
      },
    } as const;

    assert.strictEqual(openCode2SessionErrorMessage(error), "Provider exploded.");
    assert.strictEqual(openCode2SessionErrorStatus(error, false), "failed");
    assert.strictEqual(openCode2SessionErrorStatus(error, true), "interrupted");
  });

  it("breaks a native thread only when the provider shuts down", () => {
    assert.strictEqual(openCode2InterruptedThreadDisposition("user"), "reusable");
    assert.strictEqual(openCode2InterruptedThreadDisposition("superseded"), "reusable");
    assert.strictEqual(openCode2InterruptedThreadDisposition("shutdown"), "broken");
  });

  it("uses idle only before the authoritative execution lifecycle starts", () => {
    assert.isTrue(openCode2ShouldSettleTurn("idle", false));
    assert.isFalse(openCode2ShouldSettleTurn("execution-terminal", false));
    assert.isFalse(openCode2ShouldSettleTurn("execution-interrupted", false));
    assert.isTrue(openCode2ShouldSettleTurn("execution-interrupted", false, true));
    assert.isFalse(openCode2ShouldSettleTurn("idle", true));
    assert.isTrue(openCode2ShouldSettleTurn("execution-terminal", true));
    assert.isTrue(openCode2ShouldSettleTurn("execution-interrupted", true));
  });
});
