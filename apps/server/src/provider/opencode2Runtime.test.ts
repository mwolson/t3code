// @effect-diagnostics nodeBuiltinImport:off
import { assert, describe, it } from "@effect/vitest";
import { ClientError } from "@opencode/client";
import * as Effect from "effect/Effect";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { afterEach, vi } from "vite-plus/test";
import "./opencode2Runtime.testkit.ts";

import {
  isOpenCodeRuntimeError,
  normalizeOpenCodeVariant,
  openCodeAuthorizationHeader,
  openCodeHostStateHome,
  parseOpenCodeModelSlug,
  readOpenCodeHostService,
  runOpenCodeSdk,
} from "./opencode2Runtime.ts";

vi.mock(
  "@opencode/client/service",
  async () => (await import("./opencode2Runtime.mocks.ts")).serviceModule,
);
vi.mock(
  "@opencode/client/effect/service",
  async () => (await import("./opencode2Runtime.mocks.ts")).serviceModule,
);
vi.mock("node:child_process", async (importOriginal) => {
  const original = await importOriginal<typeof import("node:child_process")>();
  const { childProcessMethods } = await import("./opencode2Runtime.mocks.ts");
  return { ...original, ...childProcessMethods, default: { ...original, ...childProcessMethods } };
});

const homes: string[] = [];
afterEach(() => {
  for (const home of homes.splice(0)) NodeFS.rmSync(home, { recursive: true, force: true });
});
function home() {
  const result = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "opencode2-runtime-"));
  homes.push(result);
  return result;
}

describe("OpenCode 2 values and ledger", () => {
  it("uses fixed Basic auth without disclosing the password", () => {
    assert.strictEqual(
      openCodeAuthorizationHeader("s3cr3t"),
      `Basic ${Buffer.from("opencode:s3cr3t").toString("base64")}`,
    );
  });
  it("preserves slash-bearing models and rejects incomplete slugs", () => {
    assert.deepEqual(parseOpenCodeModelSlug(" vendor/team/model "), {
      providerID: "vendor",
      modelID: "team/model",
    });
    for (const slug of [null, undefined, "", "/model", "vendor/", "model"])
      assert.isNull(parseOpenCodeModelSlug(slug));
  });
  it("omits only the synthetic default variant", () => {
    assert.isUndefined(normalizeOpenCodeVariant("default"));
    assert.isUndefined(normalizeOpenCodeVariant(undefined));
    assert.strictEqual(normalizeOpenCodeVariant("high"), "high");
  });
  it("reads host state rather than the legacy T3 isolate", () => {
    const root = home();
    const directory = NodePath.join(root, ".local", "state", "opencode");
    NodeFS.mkdirSync(directory, { recursive: true });
    const ledger = {
      id: "service-id",
      url: "http://127.0.0.1:4096",
      password: "secret",
      pid: 23,
      version: "2.0.15",
    };
    NodeFS.writeFileSync(NodePath.join(directory, "service.json"), JSON.stringify(ledger));
    const environment = { HOME: root, XDG_STATE_HOME: "/tmp/t3-opencode2-state-old" };
    assert.strictEqual(openCodeHostStateHome(environment), NodePath.join(root, ".local", "state"));
    assert.deepEqual(readOpenCodeHostService(environment), ledger);
    assert.strictEqual(openCodeHostStateHome({ XDG_STATE_HOME: "/custom/state" }), "/custom/state");
  });
  it("returns null only for missing state", () => {
    assert.isNull(readOpenCodeHostService({ HOME: home() }));
  });
});

describe("OpenCode 2 errors", () => {
  it.effect.each([
    { cause: new Error("Model unavailable: PRIVATE_MODEL"), category: "model-unavailable" },
    { cause: new Error("401 Unauthorized: PRIVATE_AUTH"), category: "authentication-failed" },
    { cause: new Error("fetch failed: PRIVATE_ADDRESS"), category: "network-failed" },
    {
      cause: new ClientError("Transport", { cause: new Error("PRIVATE_TRANSPORT") }),
      category: "network-failed",
    },
    {
      cause: new Error("NotFoundError: endpoint returned 404 PRIVATE"),
      category: "sdk-request-failed",
    },
  ])("normalizes and retains cause %# without exposing it", ({ cause, category }) =>
    Effect.gen(function* () {
      const error = yield* runOpenCodeSdk("server.info", async () => {
        throw cause;
      }).pipe(Effect.flip);
      assert.isTrue(isOpenCodeRuntimeError(error));
      assert.strictEqual(error.category, category);
      assert.strictEqual(error.cause, cause);
      assert.notInclude(error.message, "PRIVATE");
    }),
  );
});
