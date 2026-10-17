import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as TestClock from "effect/testing/TestClock";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerConfig from "../config.ts";
import * as PublishSequence from "./PublishSequence.ts";

const SECRET = "personal-relay-publish-seq-floor";
const runtime = ServerSecretStore.layer.pipe(
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-publish-seq-" })),
  Layer.provideMerge(NodeServices.layer),
);

it.layer(runtime)("PublishSequence", (it) => {
  it.effect(
    "reserves before returning, survives restart and a backward clock, and advances tombstones",
    () =>
      Effect.gen(function* () {
        const secrets = yield* ServerSecretStore.ServerSecretStore;
        const config = yield* ServerConfig.ServerConfig;
        const fs = yield* FileSystem.FileSystem;
        yield* TestClock.setTime(DateTime.makeUnsafe("2026-10-17T12:00:00.000Z").epochMilliseconds);
        const first = yield* PublishSequence.make;
        const initial = yield* first.next;
        const persisted = yield* secrets.get(SECRET);
        assert.isTrue(Option.isSome(persisted));
        const floor = Number(new TextDecoder().decode(Option.getOrThrow(persisted)));
        assert.equal(floor, initial + 3_600_000_000);
        const stat = yield* fs.stat(`${config.secretsDir}/${SECRET}.bin`);
        assert.equal(stat.mode & 0o777, 0o600);
        assert.equal(yield* first.next, initial + 1);
        yield* TestClock.adjust("1 hour");
        const crossed = yield* first.next;
        assert.equal(crossed, floor);
        yield* TestClock.setTime(DateTime.makeUnsafe("2026-10-17T11:00:00.000Z").epochMilliseconds);
        const restarted = yield* PublishSequence.make;
        const tombstone = yield* restarted.next;
        const unchangedSnapshot = yield* restarted.next;
        assert.isAbove(tombstone, crossed);
        assert.equal(unchangedSnapshot, tombstone + 1);
        const simultaneous = yield* Effect.all(
          Array.from({ length: 40 }, () => restarted.next),
          { concurrency: "unbounded" },
        );
        assert.equal(new Set(simultaneous).size, 40);
        assert.equal(Math.min(...simultaneous), unchangedSnapshot + 1);
      }),
  );

  it.effect(
    "returns no sequence until the floor write succeeds and retries a failed reservation",
    () =>
      Effect.gen(function* () {
        const real = yield* ServerSecretStore.ServerSecretStore;
        yield* real.remove(SECRET);
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        let writes = 0;
        const allocator = yield* PublishSequence.make.pipe(
          Effect.provideService(ServerSecretStore.ServerSecretStore, {
            ...real,
            set: (name, bytes) =>
              Effect.gen(function* () {
                writes += 1;
                if (writes === 1)
                  return yield* new ServerSecretStore.SecretStorePersistError({
                    resource: "sequence floor",
                    cause: "injected",
                  });
                yield* Deferred.succeed(entered, undefined);
                yield* Deferred.await(release);
                yield* real.set(name, bytes);
              }),
          }),
        );
        const failure = yield* allocator.next.pipe(Effect.flip);
        assert.equal(failure.operation, "reserve");
        assert.isTrue(Option.isNone(yield* real.get(SECRET)));
        const retry = yield* allocator.next.pipe(Effect.forkChild);
        yield* Deferred.await(entered);
        assert.isUndefined(retry.pollUnsafe());
        yield* Deferred.succeed(release, undefined);
        assert.isAbove(yield* Fiber.join(retry), 0);
        assert.equal(writes, 2);
      }),
  );

  it.effect("fails closed on a corrupt floor", () =>
    Effect.gen(function* () {
      const secrets = yield* ServerSecretStore.ServerSecretStore;
      yield* secrets.set(SECRET, new TextEncoder().encode("invalid"));
      const allocator = yield* PublishSequence.make;
      const error = yield* allocator.next.pipe(Effect.flip);
      assert.equal(error.operation, "decode");
    }),
  );
});
