import { assert, it } from "@effect/vitest";
import { OpenCode2Settings as OpenCode2SettingsSchema } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe } from "vite-plus/test";

import { checkOpenCodeProviderStatus } from "./Layers/OpenCodeProvider.ts";
import * as SpawnedProcessReaper from "./SpawnedProcessReaper.ts";
import * as OpenCodeRuntime from "./opencodeRuntime.ts";

const decodeOpenCode2Settings = Schema.decodeEffect(OpenCode2SettingsSchema);

/**
 * Spawns a real `opencode2` binary, so it is gated the same way the other live
 * orchestrator suites are.
 *
 *   T3_OPENCODE2_LIVE=1 vp test src/provider/opencode2Runtime.live.test.ts
 *
 * This covers what unit tests cannot: that connect attaches to the user
 * service and builds an authenticated client. Auth failures are the
 * interesting case, because
 * 2.x answers an unauthenticated request with 401 rather than a startup error,
 * so a mis-wired client looks healthy until the first call.
 */
// `NodeServices.layer` is where the real child-process spawner comes from; it
// is what apps/server/src/bin.ts composes for the production runtime.
// `HostProcessPlatform` is a Context.Reference with a default, so it needs no
// layer of its own.
const layer = OpenCodeRuntime.layer.pipe(
  Layer.provide(SpawnedProcessReaper.layer),
  Layer.provide(NodeServices.layer),
);

describe.runIf(process.env.T3_OPENCODE2_LIVE === "1")("OpenCode 2 runtime (live)", () => {
  it.live(
    "attaches to the user service and builds a client that authenticates",
    () =>
      Effect.gen(function* () {
        const runtime = yield* OpenCodeRuntime.OpenCodeRuntime;

        const server = yield* runtime.connectToOpenCodeServer({ binaryPath: "opencode2" });

        assert.match(server.url, /^http:\/\/127\.0\.0\.1:\d+$/);
        assert.isAbove(server.password.length, 0);

        const client = runtime.createOpenCodeSdkClient({
          baseUrl: server.url,
          directory: process.cwd(),
          serverPassword: server.password,
        });

        // Any authenticated call proves the header is right; session.create is
        // the one the adapter reaches for first.
        const created = yield* OpenCodeRuntime.runOpenCodeSdk("session.create", () =>
          client.session.create({ location: { directory: process.cwd() } }),
        );
        const sessionId = created.id;
        assert.isString(sessionId, "session.create returned no id");
      }).pipe(Effect.provide(layer)),
    { timeout: 60_000 },
  );

  it.live(
    "waits for startup inventory before publishing the provider snapshot",
    () =>
      Effect.gen(function* () {
        const settings = yield* decodeOpenCode2Settings({});
        const provider = yield* checkOpenCodeProviderStatus(settings, process.cwd());

        assert.strictEqual(provider.status, "ready");
        assert.match(provider.version ?? "", /^0\.0\.0-next-\d+$/);
        assert.isAbove(provider.models.length, 0);
        assert.isTrue(provider.models.some((model) => model.slug === "opencode/glm-5.2"));
      }).pipe(Effect.provide(layer)),
    { timeout: 60_000 },
  );
});
