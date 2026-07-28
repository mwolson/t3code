import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  OpenCode2Settings,
  ProviderInstanceId,
  ProviderSessionId,
  ThreadId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { describe } from "vite-plus/test";

import { ServerConfig } from "../../config.ts";
import {
  OpenCode2Runtime,
  OpenCode2RuntimeLive,
  runOpenCode2Sdk,
} from "../../provider/opencode2Runtime.ts";
import { IdAllocatorV2, layer as idAllocatorLayer } from "../IdAllocator.ts";
import { ProviderAdapterV2RuntimePolicy } from "../ProviderAdapter.ts";
import { makeOpenCode2AdapterV2, unwrapOpenCode2Data } from "./OpenCode2AdapterV2.ts";

const serverConfigLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-opencode2-pending-work-",
}).pipe(Layer.provide(NodeServices.layer));
const decodeOpenCode2Settings = Schema.decodeUnknownEffect(OpenCode2Settings);
const layer = Layer.mergeAll(
  OpenCode2RuntimeLive.pipe(Layer.provide(NodeServices.layer)),
  idAllocatorLayer,
  serverConfigLayer,
);

describe.runIf(process.env.T3_OPENCODE2_LIVE === "1")(
  "OpenCode 2 adapter pending work (live)",
  () => {
    it.effect(
      "pins only its running shells and emits a wake when the shell exits",
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const serverConfig = yield* ServerConfig;
            const idAllocator = yield* IdAllocatorV2;
            const runtime = yield* OpenCode2Runtime;
            const server = yield* runtime.startOpenCode2ServerProcess({
              binaryPath: "opencode2",
            });
            const client = runtime.createOpenCode2SdkClient({
              baseUrl: server.url,
              directory: process.cwd(),
              serverPassword: server.password,
            });
            const instanceId = ProviderInstanceId.make("opencode2-live-test");
            const modelSelection = {
              instanceId,
              model: "opencode/glm-5.2",
              options: [],
            };
            const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
              runtimeMode: "full-access",
              interactionMode: "default",
              cwd: process.cwd(),
            });
            const adapter = makeOpenCode2AdapterV2({
              instanceId,
              settings: yield* decodeOpenCode2Settings({
                binaryPath: "opencode2",
                serverUrl: server.url,
                serverPassword: server.password,
              }),
              environment: process.env,
              runtime,
              idAllocator,
              serverConfig,
            });
            const session = yield* adapter.openSession({
              threadId: ThreadId.make("thread-opencode2-live-test"),
              providerSessionId: ProviderSessionId.make("provider-session-opencode2-live-test"),
              modelSelection,
              runtimePolicy,
            });
            const providerThread = yield* session.ensureThread({
              threadId: ThreadId.make("thread-opencode2-live-test"),
              modelSelection,
              runtimePolicy,
            });
            const sessionID = providerThread.nativeThreadRef?.nativeId;
            assert.isString(sessionID);
            assert.isDefined(session.hasPendingBackgroundWorkForThread);

            const created = yield* runOpenCode2Sdk("shell.create", () =>
              client.v2.shell.create({
                location: { directory: process.cwd() },
                command: "sleep 20",
                timeout: 30_000,
                metadata: { sessionID },
              }),
            );
            const shell = unwrapOpenCode2Data<{ readonly id: string }>("shell.create", created);

            assert.isTrue(yield* session.hasPendingBackgroundWorkForThread!(providerThread));
            const createdWake = yield* session.events.pipe(
              Stream.filter((event) => event.type === "provider_thread.updated"),
              Stream.runHead,
              Effect.timeoutOption("5 seconds"),
            );
            assert.isTrue(Option.isSome(createdWake));

            yield* runOpenCode2Sdk("shell.remove", () =>
              client.v2.shell.remove({
                id: shell.id,
                location: { directory: process.cwd() },
              }),
            );
            const exitedWake = yield* session.events.pipe(
              Stream.filter((event) => event.type === "provider_thread.updated"),
              Stream.runHead,
              Effect.timeoutOption("5 seconds"),
            );
            assert.isTrue(Option.isSome(exitedWake));
            assert.isFalse(yield* session.hasPendingBackgroundWorkForThread!(providerThread));
          }),
        ).pipe(Effect.provide(layer)),
      { timeout: 60_000 },
    );
  },
);
