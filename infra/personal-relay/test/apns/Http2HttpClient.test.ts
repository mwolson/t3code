import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Cause from "effect/Cause";
import * as Fiber from "effect/Fiber";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as Scope from "effect/Scope";
import * as Http2Client from "../../src/apns/Http2HttpClient.ts";
import { APNS_ID, startFakeApns, testCa } from "./FakeApnsServer.ts";

function latch<A>() {
  let resolve!: (value: A) => void;
  const promise = new Promise<A>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

describe("Http2HttpClient", () => {
  it.live("returns status, headers and body without pseudo-headers", () =>
    Effect.gen(function* () {
      const status = 400;
      const fake = yield* Effect.promise(() =>
        startFakeApns({ response: { status, reason: "BadDeviceToken" } }),
      );
      try {
        yield* Effect.scoped(
          Effect.gen(function* () {
            const client = yield* Http2Client.make({ ca: testCa });
            const response = yield* client.post(`${fake.origin}/3/device/test`);
            expect(response.status).toBe(status);
            expect(response.headers["apns-id"]).toBe(APNS_ID);
            expect(response.headers[":status"]).toBeUndefined();
            expect(yield* response.text).toBe('{"reason":"BadDeviceToken"}');
          }),
        );
      } finally {
        yield* Effect.promise(() => fake.close());
      }
    }),
  );

  it.live("sends and receives byte bodies and reuses a session per origin", () =>
    Effect.gen(function* () {
      const bytes = new Uint8Array([0, 255, 128, 10, 1]);
      const fake = yield* Effect.promise(() =>
        startFakeApns({ response: { status: 200, body: bytes } }),
      );
      try {
        yield* Effect.scoped(
          Effect.gen(function* () {
            const client = yield* Http2Client.make({ ca: testCa });
            for (let index = 0; index < 2; index++) {
              const response = yield* client.execute(
                HttpClientRequest.post(`${fake.origin}/bytes?q=1`).pipe(
                  HttpClientRequest.bodyUint8Array(bytes),
                ),
              );
              expect(new Uint8Array(yield* response.arrayBuffer)).toEqual(bytes);
            }
            expect(fake.requests.map((request) => request.body)).toEqual([
              Buffer.from(bytes),
              Buffer.from(bytes),
            ]);
            expect(fake.requests[0]?.headers[":path"]).toBe("/bytes?q=1");
            expect(fake.connections).toBe(1);
          }),
        );
      } finally {
        yield* Effect.promise(() => fake.close());
      }
    }),
  );

  it.live("reconnects after GOAWAY", () =>
    Effect.gen(function* () {
      const fake = yield* Effect.promise(() => startFakeApns({ mode: "goaway" }));
      try {
        yield* Effect.scoped(
          Effect.gen(function* () {
            const client = yield* Http2Client.make({ ca: testCa });
            for (let index = 0; index < 2; index++) {
              const response = yield* client.post(`${fake.origin}/goaway`);
              yield* response.text;
            }
            expect(fake.connections).toBe(2);
          }),
        );
      } finally {
        yield* Effect.promise(() => fake.close());
      }
    }),
  );

  it.live("cancels the stream when interrupted while waiting for a response", () =>
    Effect.gen(function* () {
      const received = latch<void>();
      const closed = latch<void>();
      const fake = yield* Effect.promise(() =>
        startFakeApns({
          onRequest: (_request, stream) => {
            stream.once("close", () => closed.resolve());
            received.resolve();
          },
        }),
      );
      try {
        yield* Effect.scoped(
          Effect.gen(function* () {
            const client = yield* Http2Client.make({ ca: testCa });
            const fiber = yield* Effect.gen(function* () {
              const response = yield* client.post(`${fake.origin}/stall`);
              yield* response.text;
            }).pipe(Effect.forkChild);
            yield* Effect.promise(() => received.promise);
            yield* Fiber.interrupt(fiber);
            const exit = yield* Fiber.await(fiber);
            expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true);
            yield* Effect.promise(() => closed.promise);
          }),
        );
      } finally {
        yield* Effect.promise(() => fake.close());
      }
    }),
  );

  it.live("destroys sessions on scope finalization, including a stalled response", () =>
    Effect.gen(function* () {
      const closed = latch<void>();
      const fake = yield* Effect.promise(() =>
        startFakeApns({
          onRequest: (_request, stream) => {
            stream.session?.once("close", () => closed.resolve());
            stream.respond({ ":status": 200 });
            stream.write("{");
          },
        }),
      );
      try {
        const scope = yield* Scope.make();
        const client = yield* Http2Client.make({ ca: testCa }).pipe(
          Effect.provideService(Scope.Scope, scope),
        );
        yield* client.post(`${fake.origin}/scope`);
        yield* Scope.close(scope, Exit.void);
        yield* Effect.promise(() => closed.promise);
      } finally {
        yield* Effect.promise(() => fake.close());
      }
    }),
  );
});
