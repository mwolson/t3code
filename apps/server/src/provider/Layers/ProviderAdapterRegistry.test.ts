import {
  defaultInstanceIdForDriver,
  ProviderDriverKind,
  ThreadId,
  type ServerProvider,
} from "@t3tools/contracts";
import { it, assert, vi } from "@effect/vitest";

import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";

import { ClaudeAdapter, type ClaudeAdapterShape } from "../Services/ClaudeAdapter.ts";
import { CodexAdapter, type CodexAdapterShape } from "../Services/CodexAdapter.ts";
import { CursorAdapter, type CursorAdapterShape } from "../Services/CursorAdapter.ts";
import { ProviderAdapterRegistry } from "../Services/ProviderAdapterRegistry.ts";
import { ProviderAdapterRegistryLive } from "./ProviderAdapterRegistry.ts";
import { ProviderUnsupportedError } from "../Errors.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";

const CODEX_DRIVER = ProviderDriverKind.make("codex");
const CLAUDE_AGENT_DRIVER = ProviderDriverKind.make("claudeAgent");
const OPENCODE_DRIVER = ProviderDriverKind.make("opencode");
const CURSOR_DRIVER = ProviderDriverKind.make("cursor");

const fakeCodexAdapter: CodexAdapter.CodexAdapterShape = {
  provider: CODEX_DRIVER,
  capabilities: { sessionModelSwitch: "in-session" },
  startSession: vi.fn(),
  sendTurn: vi.fn(),
  interruptTurn: vi.fn(),
  respondToRequest: vi.fn(),
  respondToUserInput: vi.fn(),
  stopSession: vi.fn(),
  listSessions: vi.fn(),
  hasSession: vi.fn(),
  readThread: vi.fn(),
  rollbackThread: vi.fn(),
  uploadFeedback: vi.fn(),
  stopAll: vi.fn(),
  streamEvents: Stream.empty,
};

const fakeClaudeAdapter: ClaudeAdapter.ClaudeAdapterShape = {
  provider: CLAUDE_AGENT_DRIVER,
  capabilities: { sessionModelSwitch: "in-session" },
  startSession: vi.fn(),
  sendTurn: vi.fn(),
  interruptTurn: vi.fn(),
  respondToRequest: vi.fn(),
  respondToUserInput: vi.fn(),
  stopSession: vi.fn(),
  listSessions: vi.fn(),
  hasSession: vi.fn(),
  readThread: vi.fn(),
  rollbackThread: vi.fn(),
  stopAll: vi.fn(),
  streamEvents: Stream.empty,
};

const fakeCursorAdapter: CursorAdapterShape = {
  provider: "cursor",
  capabilities: { sessionModelSwitch: "in-session" },
  startSession: vi.fn(),
  sendTurn: vi.fn(),
  interruptTurn: vi.fn(),
  respondToRequest: vi.fn(),
  respondToUserInput: vi.fn(),
  stopSession: vi.fn(),
  listSessions: vi.fn(),
  hasSession: vi.fn(),
  readThread: vi.fn(),
  rollbackThread: vi.fn(),
  stopAll: vi.fn(),
  streamEvents: Stream.empty,
};

const layer = it.layer(
  Layer.mergeAll(
    Layer.provide(
      ProviderAdapterRegistryLive,
      Layer.mergeAll(
        Layer.succeed(CodexAdapter, fakeCodexAdapter),
        Layer.succeed(ClaudeAdapter, fakeClaudeAdapter),
        Layer.succeed(CursorAdapter, fakeCursorAdapter),
      ),
    ),
    NodeServices.layer,
  ),
  NodeServices.layer,
);

it.layer(layer)("ProviderAdapterRegistryLive", (it) => {
  it("resolves adapters and routing metadata from provider instances", () =>
    Effect.gen(function* () {
      const registry = yield* ProviderAdapterRegistry;
      const codex = yield* registry.getByProvider("codex");
      const claude = yield* registry.getByProvider("claudeAgent");
      const cursor = yield* registry.getByProvider("cursor");
      assert.equal(codex, fakeCodexAdapter);
      assert.equal(claude, fakeClaudeAdapter);
      assert.equal(cursor, fakeCursorAdapter);

      const providers = yield* registry.listProviders();
      assert.deepEqual(providers, ["codex", "claudeAgent", "cursor"]);
    }),
  );

  it.effect("fails with ProviderUnsupportedError for unknown providers", () =>
    Effect.gen(function* () {
      const registry = yield* ProviderAdapterRegistry;
      const adapter = yield* registry.getByProvider("unknown" as ProviderKind).pipe(Effect.result);
      assertFailure(adapter, new ProviderUnsupportedError({ provider: "unknown" }));
    }),
  );
});

it.effect("blocks shared credential session startup and preserves guarded adapter identity", () =>
  Effect.gen(function* () {
    const target = fakeInstances[0]!;
    const peer = fakeInstances[1]!;
    const auth = yield* ProviderAuthFlow.make({
      instanceId: target.instanceId,
      credentialBinding: { owner: "t3", key: "shared-auth" },
      methods: Effect.succeed([
        { id: "browser", name: "Browser", description: null, type: "agent" },
      ]),
      authenticate: () => Effect.never,
      logout: Effect.void,
    });
    const peerAuth = yield* ProviderAuthFlow.make({
      instanceId: peer.instanceId,
      credentialBinding: { owner: "t3", key: "shared-auth" },
      methods: Effect.succeed([]),
      authenticate: () => Effect.void,
      logout: Effect.void,
    });
    const session = {
      threadId: ThreadId.make("new-session"),
      provider: peer.driverKind,
      providerInstanceId: peer.instanceId,
      status: "ready" as const,
      runtimeMode: "approval-required" as const,
      createdAt: "2026-09-21T00:00:00.000Z",
      updatedAt: "2026-09-21T00:00:00.000Z",
    };
    const start = vi.fn(() => Effect.succeed(session));
    const instances = [
      { ...target, auth },
      { ...peer, auth: peerAuth, adapter: { ...peer.adapter, startSession: start } },
    ];
    const registry = yield* ProviderAdapterRegistry.ProviderAdapterRegistry.pipe(
      Effect.provide(
        ProviderAdapterRegistryLayer.ProviderAdapterRegistryLive.pipe(
          Layer.provide(
            Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)({
              getInstance: (id) =>
                Effect.succeed(instances.find((instance) => instance.instanceId === id)),
              listInstances: Effect.succeed(instances),
            }),
          ),
        ),
      ),
    );
    const guarded = yield* registry.getByInstance(peer.instanceId);
    assert.strictEqual(yield* registry.getByInstance(peer.instanceId), guarded);
    const flow = yield* auth.start("owner");
    const error = yield* guarded
      .startSession({
        threadId: session.threadId,
        providerInstanceId: peer.instanceId,
        runtimeMode: "approval-required",
      })
      .pipe(Effect.flip);
    assert.strictEqual(error._tag, "ProviderAdapterValidationError");
    assert.strictEqual(start.mock.calls.length, 0);
    yield* auth.cancel("owner", flow.flowId!);
    assert.deepStrictEqual(
      yield* guarded.startSession({
        threadId: session.threadId,
        providerInstanceId: peer.instanceId,
        runtimeMode: "approval-required",
      }),
      session,
    );
    assert.strictEqual(start.mock.calls.length, 1);
    const entered = yield* Deferred.make<void>();
    const stopped = yield* Deferred.make<void>();
    start.mockImplementation(() =>
      Effect.gen(function* () {
        yield* Deferred.succeed(entered, undefined);
        return yield* Effect.never;
      }).pipe(Effect.ensuring(Deferred.succeed(stopped, undefined))),
    );
    const startup = yield* guarded
      .startSession({
        threadId: session.threadId,
        providerInstanceId: peer.instanceId,
        runtimeMode: "approval-required",
      })
      .pipe(Effect.forkChild);
    yield* Deferred.await(entered);
    // Signing out through another instance must drain its peer's startup too.
    yield* auth.logout(Effect.void);
    yield* Deferred.await(stopped);
    assert.strictEqual(Exit.isFailure(yield* Fiber.await(startup)), true);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
