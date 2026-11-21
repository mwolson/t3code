// @effect-diagnostics nodeBuiltinImport:off - This test verifies actual Node process streams and exit status.
import { describe, expect, it } from "@effect/vitest";
import * as NodeChildProcess from "node:child_process";
import * as NodeURL from "node:url";

function run(args: string[], input = '{"proof":"test-proof"}', fail = false) {
  return NodeChildProcess.spawnSync(
    process.execPath,
    [NodeURL.fileURLToPath(new URL("./AdminRuntime.fixture.ts", import.meta.url)), ...args],
    {
      input,
      encoding: "utf8",
      env: { ...process.env, U4_TEST_FAIL: fail ? "yes" : "" },
      timeout: 15_000,
    },
  );
}

describe("admin Node runtime composition hook", () => {
  it("keeps the credential pipe clean, including Effect logs", () => {
    const result = run(["enroll"]);
    expect(result.error).toBeUndefined();
    expect(result.status).toBe(0);
    expect(result.stdout).toBe("test-only-issued-credential\n");
    expect(result.stderr).toBe("Environment enrolled.\n");
  });

  it("reports sanitized failure on stderr with a nonzero exit", () => {
    const result = run(["enroll"], undefined, true);
    expect(result.error).toBeUndefined();
    expect(result.status).toBe(1);
    expect(result.stdout).toBe("");
    expect(result.stderr).toBe("Personal relay admin command failed.\n");
  });
});
