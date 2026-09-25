import { assert, describe, it } from "@effect/vitest";
import { ClientError, OpenCode } from "@opencode/client";
import { Deferred, Effect, Fiber } from "effect";
import { vi } from "vite-plus/test";
import { provideRuntimeDependencies, serviceSpawner } from "./opencode2Runtime.testkit.ts";
import {
  layer,
  OpenCode2Runtime,
  loadOpenCodeCommands,
  openCodeAuthorizationHeader,
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
const session = {
  id: "ses_test",
  projectID: "prj_test",
  cost: 0,
  tokens: { input: 10, output: 3, reasoning: 2, cache: { read: 4, write: 5 } },
  time: { created: 1700000000000, updated: 1700000001000 },
  location: { directory: "/work" },
};

describe("OpenCode 2 published promise client", () => {
  it.effect("round-trips stateful session HTTP using the released routes", () =>
    Effect.gen(function* () {
      let exists = false;
      let interrupted = false;
      const calls: string[] = [];
      vi.stubGlobal(
        "fetch",
        vi.fn<typeof globalThis.fetch>(async (input, options) => {
          const url = new URL(String(input));
          const method = options?.method ?? "GET";
          calls.push(`${method} ${url.pathname}`);
          assert.strictEqual(
            new Headers(options?.headers).get("authorization"),
            openCodeAuthorizationHeader("private"),
          );
          if (url.pathname === "/api/info")
            return Response.json({
              version: "2.0.15",
              pid: 123,
              urls: ["http://fixture.invalid"],
              paths: { tmp: "/tmp/opencode" },
            });
          if (url.pathname === "/api/session" && method === "POST") {
            exists = true;
            return Response.json({ data: session });
          }
          assert.isTrue(exists);
          if (url.pathname === "/api/session/ses_test" && method === "GET")
            return Response.json({ data: session });
          if (url.pathname === "/api/session/ses_test/interrupt") {
            interrupted = true;
            return Response.json({ interrupted: true });
          }
          if (url.pathname === "/api/experimental/session/ses_test/wait") {
            assert.isTrue(interrupted);
            return new Response(null, { status: 204 });
          }
          if (url.pathname === "/api/session/ses_test" && method === "DELETE") {
            exists = false;
            return new Response(null, { status: 204 });
          }
          throw new Error(`Unexpected request ${method} ${url.pathname}`);
        }),
      );
      const client = OpenCode.make({
        baseUrl: "http://fixture.invalid",
        headers: { Authorization: openCodeAuthorizationHeader("private") },
      });
      const identity = yield* runOpenCodeSdk("server.info", (signal) =>
        client.server.info({ signal }),
      );
      assert.strictEqual(identity.version, "2.0.15");
      const created = yield* runOpenCodeSdk("session.create", (signal) =>
        client.session.create({ location: { directory: "/work" } }, { signal }),
      );
      assert.deepEqual(created, session);
      const found = yield* runOpenCodeSdk("session.get", (signal) =>
        client.session.get({ sessionID: created.id }, { signal }),
      );
      assert.deepEqual(found, session);
      const stopped = yield* runOpenCodeSdk("session.interrupt", (signal) =>
        client.session.interrupt({ sessionID: created.id }, { signal }),
      );
      assert.isTrue(stopped.interrupted);
      yield* runOpenCodeSdk("session.wait", (signal) =>
        client.session.wait({ sessionID: created.id }, { signal }),
      );
      yield* runOpenCodeSdk("session.remove", (signal) =>
        client.session.remove({ sessionID: created.id }, { signal }),
      );
      assert.isFalse(exists);
      assert.deepEqual(calls, [
        "GET /api/info",
        "POST /api/session",
        "GET /api/session/ses_test",
        "POST /api/session/ses_test/interrupt",
        "POST /api/experimental/session/ses_test/wait",
        "DELETE /api/session/ses_test",
      ]);
    }),
  );
  it.effect("sends encoded directory and Basic auth on directory-scoped commands", () =>
    Effect.gen(function* () {
      vi.stubGlobal(
        "fetch",
        vi.fn<typeof globalThis.fetch>(async () =>
          Response.json({ data: [{ name: "review", description: "Review changes" }] }),
        ),
      );
      const runtime = yield* OpenCode2Runtime;
      const client = runtime.createOpenCodeSdkClient({
        baseUrl: "http://fixture.invalid",
        directory: "/work/a project/日本語",
        serverPassword: "private",
      });
      assert.deepEqual(yield* loadOpenCodeCommands(client, "/work/a project/日本語"), [
        { name: "review", description: "Review changes" },
      ]);
      const [input, options] = vi.mocked(fetch).mock.calls[0]!;
      const url = new URL(String(input));
      assert.strictEqual(url.pathname, "/api/command");
      assert.strictEqual(url.searchParams.get("location[directory]"), "/work/a project/日本語");
      const headers = new Headers(options?.headers);
      assert.strictEqual(headers.get("authorization"), openCodeAuthorizationHeader("private"));
      assert.strictEqual(
        headers.get("x-opencode-directory"),
        encodeURIComponent("/work/a project/日本語"),
      );
    }).pipe(Effect.provide(layer), provideRuntimeDependencies(serviceSpawner().spawner)),
  );
  it.effect("aborts actual client fetch on fiber interruption", () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<AbortSignal>();
      vi.stubGlobal(
        "fetch",
        vi.fn<typeof globalThis.fetch>((_url, options) => {
          const signal = options?.signal;
          assert.instanceOf(signal, AbortSignal);
          Deferred.doneUnsafe(entered, Effect.succeed(signal!));
          return new Promise((_resolve, reject) =>
            signal!.addEventListener("abort", () => reject(signal!.reason), { once: true }),
          );
        }),
      );
      const client = OpenCode.make({ baseUrl: "http://fixture.invalid" });
      const fiber = yield* runOpenCodeSdk("server.info", (signal) =>
        client.server.info({ signal }),
      ).pipe(Effect.forkChild);
      const signal = yield* Deferred.await(entered);
      yield* Fiber.interrupt(fiber);
      assert.isTrue(signal.aborted);
    }),
  );
  it.effect.each([
    {
      status: 401,
      body: { _tag: "UnauthorizedError", message: "PRIVATE_AUTH" },
      category: "authentication-failed",
    },
    {
      status: 400,
      body: { _tag: "ForbiddenError", message: "PRIVATE_AUTH" },
      category: "authentication-failed",
    },
    { status: 403, body: { message: "PRIVATE_AUTH" }, category: "authentication-failed" },
    {
      status: 400,
      body: { _tag: "InvalidRequestError", message: "Model unavailable: PRIVATE_MODEL" },
      category: "model-unavailable",
    },
    {
      status: 400,
      body: { message: ["Model unavailable: PRIVATE_MODEL"] },
      category: "sdk-request-failed",
    },
  ])("classifies real-client errors without payload leaks %#", ({ status, body, category }) =>
    Effect.gen(function* () {
      vi.stubGlobal(
        "fetch",
        vi.fn<typeof globalThis.fetch>(async () => Response.json(body, { status })),
      );
      const client = OpenCode.make({ baseUrl: "http://fixture.invalid" });
      const error = yield* runOpenCodeSdk("server.info", (signal) =>
        client.server.info({ signal }),
      ).pipe(Effect.flip);
      assert.strictEqual(error.category, category);
      assert.notInclude(error.message, "PRIVATE");
      assert.isDefined(error.cause);
    }),
  );
  it.effect.each([401, 200])("keeps unsupported HTML (%s) sanitized", (status) =>
    Effect.gen(function* () {
      vi.stubGlobal(
        "fetch",
        vi.fn<typeof globalThis.fetch>(
          async () =>
            new Response("PRIVATE_HTML", { status, headers: { "content-type": "text/html" } }),
        ),
      );
      const client = OpenCode.make({ baseUrl: "http://fixture.invalid" });
      const error = yield* runOpenCodeSdk("server.info", (signal) =>
        client.server.info({ signal }),
      ).pipe(Effect.flip);
      assert.strictEqual(error.category, "sdk-request-failed");
      assert.instanceOf(error.cause, ClientError);
      assert.notInclude(error.message, "PRIVATE_HTML");
    }),
  );
});
