// @effect-diagnostics nodeBuiltinImport:off
import { assert, describe, it } from "@effect/vitest";
import { Deferred, Effect, Fiber } from "effect";
import { TestClock } from "effect/testing";
import * as NodeFS from "node:fs";
import { vi } from "vite-plus/test";
import {
  connect,
  info,
  ledger,
  ledgerPath,
  refusal,
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

describe("OpenCode 2 attachment identity and absence", () => {
  it.effect.each([
    { ...ledger, id: "replacement" },
    { ...ledger, password: "PRIVATE_NEW" },
    { ...ledger, url: "http://127.0.0.1:4097" },
    { ...ledger, pid: ledger.pid + 1 },
    { ...ledger, version: "2.0.16" },
  ])("rejects every captured ledger identity change %#", (changed) =>
    Effect.gen(function* () {
      const home = temporaryHome();
      writeLedger(home);
      vi.mocked(fetch).mockImplementationOnce(async () => {
        writeLedger(home, changed);
        return Response.json(info);
      });
      const error = yield* connect({ HOME: home }).pipe(Effect.flip);
      assert.strictEqual(
        error.category,
        changed.version === ledger.version
          ? "service-identity-mismatch"
          : "unsupported-server-version",
      );
      assert.strictEqual(vi.mocked(fetch).mock.calls.length, 1);
    }),
  );
  it.effect.each(["missing", "malformed", "unreadable"])(
    "rejects ledger becoming %s during recheck",
    (state) =>
      Effect.gen(function* () {
        const home = temporaryHome();
        writeLedger(home);
        vi.mocked(fetch).mockImplementationOnce(async () => {
          NodeFS.unlinkSync(ledgerPath(home));
          if (state === "malformed") NodeFS.writeFileSync(ledgerPath(home), "{PRIVATE");
          if (state === "unreadable") NodeFS.mkdirSync(ledgerPath(home));
          return Response.json(info);
        });
        const error = yield* connect({ HOME: home }).pipe(Effect.flip);
        assert.strictEqual(error.category, "service-identity-mismatch");
      }),
  );
  it.effect("rechecks the ledger after the final endpoint response", () =>
    Effect.gen(function* () {
      const home = temporaryHome();
      writeLedger(home);
      vi.mocked(fetch)
        .mockResolvedValueOnce(Response.json(info))
        .mockImplementationOnce(async () => {
          writeLedger(home, { ...ledger, id: "new-generation" });
          return Response.json(info);
        });
      const error = yield* connect({ HOME: home }).pipe(Effect.flip);
      assert.strictEqual(error.category, "service-identity-mismatch");
    }),
  );
  it.effect.each([false, true])("rechecks endpoint PID (external=%s)", (external) =>
    Effect.gen(function* () {
      const home = temporaryHome();
      writeLedger(home);
      vi.mocked(fetch)
        .mockResolvedValueOnce(Response.json(info))
        .mockResolvedValueOnce(Response.json({ ...info, pid: info.pid + 1 }));
      const error = yield* connect({ HOME: home }, external ? ledger.url : undefined).pipe(
        Effect.flip,
      );
      assert.strictEqual(error.category, "service-identity-mismatch");
    }),
  );
  it.effect.each([
    null,
    {},
    { ...info, pid: 0 },
    { ...info, pid: -1 },
    { ...info, pid: "123" },
    { ...info, pid: info.pid + 1 },
    { ...info, urls: null },
    { ...info, paths: {} },
  ])("rejects malformed or mismatched server identity %#", (body) =>
    Effect.gen(function* () {
      const home = temporaryHome();
      writeLedger(home);
      vi.mocked(fetch).mockResolvedValue(Response.json(body));
      const error = yield* connect({ HOME: home }).pipe(Effect.flip);
      assert.strictEqual(error.category, "service-identity-mismatch");
    }),
  );
  it.effect.each([401, 403, 500, 503])("fails closed on HTTP %s", (status) =>
    Effect.gen(function* () {
      const home = temporaryHome();
      writeLedger(home);
      vi.mocked(fetch).mockResolvedValue(
        Response.json(
          status === 401 ? { _tag: "UnauthorizedError", message: "PRIVATE_AUTH" } : info,
          { status },
        ),
      );
      const error = yield* connect({ HOME: home }).pipe(Effect.flip);
      assert.strictEqual(
        error.category,
        status === 401 || status === 403 ? "authentication-failed" : "sdk-request-failed",
      );
    }),
  );
  it.effect.each(["alive", "EPERM", "EACCES", "unknown"])(
    "does not infer absence from PID state %s",
    (state) =>
      Effect.gen(function* () {
        const home = temporaryHome();
        writeLedger(home);
        vi.mocked(process.kill).mockImplementation(() => {
          if (state === "alive") return true;
          throw Object.assign(new Error("PRIVATE"), { code: state });
        });
        vi.mocked(fetch).mockRejectedValue(refusal());
        const error = yield* connect({ HOME: home }).pipe(Effect.flip);
        assert.strictEqual(error.category, "network-failed");
        assert.deepEqual(vi.mocked(process.kill).mock.calls, [[ledger.pid, 0]]);
      }),
  );
  it.effect.each([
    new TypeError("fetch failed ECONNREFUSED"),
    refusal().cause,
    new TypeError("fetch failed", { cause: { code: "ECONNREFUSED" } }),
    refusal("192.0.2.1"),
    refusal("127.0.0.1", 4999),
    new TypeError("fetch failed", { cause: new AggregateError([refusal().cause]) }),
    ...["ENOTFOUND", "CERT_HAS_EXPIRED", "ETIMEDOUT"].map(
      (code) =>
        new TypeError("fetch failed", { cause: Object.assign(new Error("PRIVATE"), { code }) }),
    ),
  ])("does not infer absence from uncertain transport %#", (cause) =>
    Effect.gen(function* () {
      const home = temporaryHome();
      writeLedger(home);
      vi.mocked(fetch).mockRejectedValue(cause);
      const error = yield* connect({ HOME: home }).pipe(Effect.flip);
      assert.strictEqual(error.category, "network-failed");
      assert.strictEqual(vi.mocked(process.kill).mock.calls.length, 0);
    }),
  );
  it.effect.each(["http://remote.invalid:4096", "http://192.0.2.1:4096", "http://localhost:4096"])(
    "does not infer local absence for %s",
    (url) =>
      Effect.gen(function* () {
        const home = temporaryHome();
        writeLedger(home, { ...ledger, url });
        vi.mocked(fetch).mockRejectedValue(refusal());
        const error = yield* connect({ HOME: home }).pipe(Effect.flip);
        assert.strictEqual(error.category, "network-failed");
        assert.strictEqual(vi.mocked(process.kill).mock.calls.length, 0);
      }),
  );
  it.effect.each([
    { url: "http://192.0.2.1:4096", address: "192.0.2.1" },
    { url: "http://[2001:db8::1]:4096", address: "2001:db8::1" },
    { url: "http://0.0.0.0:4096", address: "0.0.0.0" },
    { url: "http://127.0.0.2:4096", address: "127.0.0.2" },
  ])(
    "does not infer local absence from a matching non-loopback refusal at $url",
    ({ url, address }) =>
      Effect.gen(function* () {
        const home = temporaryHome();
        writeLedger(home, { ...ledger, url });
        vi.mocked(process.kill).mockImplementation(() => {
          throw Object.assign(new Error("PRIVATE"), { code: "ESRCH" });
        });
        vi.mocked(fetch).mockRejectedValue(refusal(address));
        const error = yield* connect({ HOME: home }).pipe(Effect.flip);
        assert.strictEqual(error.category, "network-failed");
        assert.strictEqual(vi.mocked(process.kill).mock.calls.length, 0);
      }),
  );
  it.effect("rejects an identity change while classifying absence", () =>
    Effect.gen(function* () {
      const home = temporaryHome();
      writeLedger(home);
      vi.mocked(fetch).mockRejectedValue(refusal());
      vi.mocked(process.kill).mockImplementation(() => {
        writeLedger(home, { ...ledger, id: "replaced" });
        throw Object.assign(new Error("PRIVATE"), { code: "ESRCH" });
      });
      const error = yield* connect({ HOME: home }).pipe(Effect.flip);
      assert.strictEqual(error.category, "service-identity-mismatch");
    }),
  );
  it.effect("bounds a hung probe and aborts its request without lifecycle actions", () =>
    Effect.gen(function* () {
      const home = temporaryHome();
      writeLedger(home);
      const entered = yield* Deferred.make<AbortSignal>();
      vi.mocked(fetch).mockImplementation((_url, options) => {
        const signal = options?.signal;
        assert.instanceOf(signal, AbortSignal);
        Deferred.doneUnsafe(entered, Effect.succeed(signal!));
        return new Promise((_resolve, reject) =>
          signal!.addEventListener("abort", () => reject(signal!.reason), { once: true }),
        );
      });
      const fiber = yield* connect({ HOME: home }).pipe(Effect.flip, Effect.forkChild);
      const signal = yield* Deferred.await(entered);
      yield* TestClock.adjust("5 seconds");
      const error = yield* Fiber.join(fiber);
      assert.strictEqual(error.category, "service-probe-timeout");
      assert.strictEqual(error.timeoutMs, 5000);
      assert.isTrue(signal.aborted);
    }),
  );
});
