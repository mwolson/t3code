// @effect-diagnostics nodeBuiltinImport:off
import { assert, describe, it } from "@effect/vitest";
import { Effect } from "effect";
import * as NodeFS from "node:fs";
import { vi } from "vite-plus/test";
import { openCodeAuthorizationHeader } from "./opencode2Runtime.ts";
import {
  connect,
  info,
  ledger,
  ledgerPath,
  temporaryHome,
  writeLedger,
} from "./opencode2Runtime.testkit.ts";

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

describe("OpenCode 2 host service attach", () => {
  it.effect("attaches to the registered identity with authenticated endpoint rechecks", () =>
    Effect.gen(function* () {
      const home = temporaryHome();
      writeLedger(home);
      const before = NodeFS.readFileSync(ledgerPath(home));
      const result = yield* connect({ HOME: home });
      assert.deepEqual(result, {
        url: ledger.url,
        password: ledger.password,
        exitCode: null,
        external: true,
      });
      assert.deepEqual(NodeFS.readFileSync(ledgerPath(home)), before);
      assert.strictEqual(vi.mocked(fetch).mock.calls.length, 2);
      for (const [url, options] of vi.mocked(fetch).mock.calls) {
        assert.strictEqual(String(url), `${ledger.url}/api/info`);
        assert.strictEqual(
          new Headers(options?.headers).get("authorization"),
          openCodeAuthorizationHeader(ledger.password),
        );
      }
    }),
  );
  it.effect.each([undefined, "", " "])("requires registered credentials %#", (password) =>
    Effect.gen(function* () {
      const home = temporaryHome();
      writeLedger(home, { ...ledger, password });
      const category = yield* connect({ HOME: home }).pipe(
        Effect.match({
          onFailure: (error) => error.category,
          onSuccess: () => "unexpected-success",
        }),
      );
      assert.strictEqual(category, "service-credentials-required");
      assert.strictEqual(vi.mocked(fetch).mock.calls.length, 0);
    }),
  );
  it.effect.each([
    null,
    [],
    {},
    { ...ledger, id: "" },
    { ...ledger, pid: 0 },
    { ...ledger, pid: -1 },
    { ...ledger, pid: "123" },
    { ...ledger, pid: 1.5 },
    { ...ledger, url: "file:///secret" },
  ])("fails closed on invalid ledger %#", (value) =>
    Effect.gen(function* () {
      const home = temporaryHome();
      writeLedger(home, value);
      const error = yield* connect({ HOME: home }).pipe(Effect.flip);
      assert.strictEqual(error.category, "service-identity-mismatch");
      assert.strictEqual(vi.mocked(fetch).mock.calls.length, 0);
    }),
  );
  it.effect.each(["malformed", "unreadable"])("fails closed on %s ledger", (state) =>
    Effect.gen(function* () {
      const home = temporaryHome();
      writeLedger(home);
      if (state === "malformed") NodeFS.writeFileSync(ledgerPath(home), "{PRIVATE_INVALID");
      else {
        NodeFS.unlinkSync(ledgerPath(home));
        NodeFS.mkdirSync(ledgerPath(home));
      }
      const error = yield* connect({ HOME: home }).pipe(Effect.flip);
      assert.strictEqual(error.category, "service-identity-mismatch");
      assert.notInclude(error.message, "PRIVATE_");
    }),
  );
  it.effect.each(["2.0.14", "2.0.16", "0.0.0-beta-18999", "1.18.32", "v2.0.15"])(
    "rejects unsupported ledger version %s before HTTP",
    (version) =>
      Effect.gen(function* () {
        const home = temporaryHome();
        writeLedger(home, { ...ledger, version });
        const category = yield* connect({ HOME: home }).pipe(
          Effect.match({
            onFailure: (error) => error.category,
            onSuccess: () => "unexpected-success",
          }),
        );
        assert.strictEqual(category, "unsupported-server-version");
        assert.strictEqual(vi.mocked(fetch).mock.calls.length, 0);
      }),
  );
  it.effect.each([
    { version: "0.0.0-beta-18999", url: ledger.url, pid: ledger.pid, password: ledger.password },
    { ...ledger, version: "0.0.0-beta-18999", password: undefined },
    { version: "2.0.16" },
  ])("reports unsupported ledger version before missing identity or credentials %#", (value) =>
    Effect.gen(function* () {
      const home = temporaryHome();
      writeLedger(home, value);
      const error = yield* connect({ HOME: home }).pipe(Effect.flip);
      assert.strictEqual(error.category, "unsupported-server-version");
      assert.strictEqual(vi.mocked(fetch).mock.calls.length, 0);
      assert.strictEqual(vi.mocked(process.kill).mock.calls.length, 0);
    }),
  );
  it.effect.each([false, true])("reports an unsupported info route (external=%s)", (external) =>
    Effect.gen(function* () {
      const home = temporaryHome();
      writeLedger(home);
      vi.mocked(fetch).mockResolvedValue(new Response("PRIVATE_NOT_FOUND", { status: 404 }));
      const error = yield* connect({ HOME: home }, external ? ledger.url : undefined).pipe(
        Effect.flip,
      );
      assert.strictEqual(error.category, "unsupported-server-version");
      assert.notInclude(error.message, "PRIVATE");
      assert.isDefined(error.cause);
      assert.strictEqual(vi.mocked(fetch).mock.calls.length, 1);
      assert.strictEqual(vi.mocked(process.kill).mock.calls.length, 0);
    }),
  );
  it.effect.each([false, true])(
    "reports unsupported info version before identity shape (external=%s)",
    (external) =>
      Effect.gen(function* () {
        const home = temporaryHome();
        writeLedger(home);
        vi.mocked(fetch).mockResolvedValue(Response.json({ version: "0.0.0-beta-18999" }));
        const error = yield* connect({ HOME: home }, external ? ledger.url : undefined).pipe(
          Effect.flip,
        );
        assert.strictEqual(error.category, "unsupported-server-version");
        assert.strictEqual(vi.mocked(fetch).mock.calls.length, 1);
        assert.strictEqual(vi.mocked(process.kill).mock.calls.length, 0);
      }),
  );
  it.effect("verifies an explicit external endpoint without inspecting local state", () =>
    Effect.gen(function* () {
      const home = temporaryHome();
      writeLedger(home, { malformed: true });
      const error = yield* connect({ HOME: home }, "http://external.invalid", " ").pipe(
        Effect.flip,
      );
      assert.strictEqual(error.category, "external-server-password-required");
      assert.strictEqual(vi.mocked(fetch).mock.calls.length, 0);
      const result = yield* connect({ HOME: home }, " http://external.invalid ", " secret ");
      assert.deepEqual(result, {
        url: "http://external.invalid",
        password: "secret",
        exitCode: null,
        external: true,
      });
      assert.strictEqual(vi.mocked(fetch).mock.calls.length, 2);
    }),
  );
  it.effect.each(["2.0.14", "2.0.16", "0.0.0-beta-18999", "1.18.32"])(
    "also rejects external server version %s",
    (version) =>
      Effect.gen(function* () {
        vi.mocked(fetch).mockResolvedValue(Response.json({ ...info, version }));
        const category = yield* connect({}, "http://external.invalid").pipe(
          Effect.match({
            onFailure: (error) => error.category,
            onSuccess: () => "unexpected-success",
          }),
        );
        assert.strictEqual(category, "unsupported-server-version");
      }),
  );
  it.effect.each(["file:///tmp/service", "bad url"])("rejects invalid external URL %s", (url) =>
    Effect.gen(function* () {
      const error = yield* connect({}, url).pipe(Effect.flip);
      assert.strictEqual(error.category, "invalid-server-url");
      assert.strictEqual(vi.mocked(fetch).mock.calls.length, 0);
    }),
  );
});
