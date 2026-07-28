import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe } from "vite-plus/test";

import { OpenCode2Runtime, OpenCode2RuntimeLive, runOpenCode2Sdk } from "./opencode2Runtime.ts";

/**
 * Spawns a real `opencode2` binary, so it is gated the same way the other live
 * orchestrator suites are.
 *
 *   T3_OPENCODE2_LIVE=1 vp test src/provider/opencode2Runtime.live.test.ts
 *
 * The unit tests cover the banner parser against captured output. This covers
 * what they cannot: that the layer wiring actually produces a spawned server
 * and an authenticated client. Auth failures are the interesting case, because
 * 2.x answers an unauthenticated request with 401 rather than a startup error,
 * so a mis-wired client looks healthy until the first call.
 */
// `NodeServices.layer` is where the real child-process spawner comes from; it
// is what apps/server/src/bin.ts composes for the production runtime.
// `HostProcessPlatform` is a Context.Reference with a default, so it needs no
// layer of its own.
const layer = OpenCode2RuntimeLive.pipe(Layer.provide(NodeServices.layer));

describe.runIf(process.env.T3_OPENCODE2_LIVE === "1")("OpenCode 2 runtime (live)", () => {
  it.effect(
    "spawns a server, reads both startup facts, and builds a client that authenticates",
    () =>
      Effect.gen(function* () {
        const runtime = yield* OpenCode2Runtime;
        const scope = yield* Scope.make();

        const server = yield* runtime
          .startOpenCode2ServerProcess({ binaryPath: "opencode2" })
          .pipe(Effect.provideService(Scope.Scope, scope));

        assert.match(server.url, /^http:\/\/127\.0\.0\.1:\d+$/);
        assert.isAbove(server.password.length, 0);

        const client = runtime.createOpenCode2SdkClient({
          baseUrl: server.url,
          directory: process.cwd(),
          serverPassword: server.password,
        });

        // Any authenticated call proves the header is right; session.create is
        // the one the adapter reaches for first. `client.v2.*` is the /api/*
        // namespace, and the response carries its own `data` envelope.
        const created = yield* runOpenCode2Sdk("session.create", () =>
          client.v2.session.create({ location: { directory: process.cwd() } }),
        );
        const sessionId = (created.data as { data?: { id?: string } } | undefined)?.data?.id;
        assert.isString(sessionId, "session.create returned no id");

        yield* Scope.close(scope, Effect.void as never).pipe(Effect.ignore);
      }).pipe(Effect.provide(layer)),
    { timeout: 60_000 },
  );
});
