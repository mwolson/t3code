import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  MessageId,
  type ModelSelection,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcessSpawner } from "effect/unstable/process";
import type * as EffectAcpProtocol from "effect-acp/protocol";

import { ServerConfig } from "../../config.ts";
import * as AcpSessionRuntime from "../../provider/acp/AcpSessionRuntime.ts";
import { normalizeXAiAcpToolCallState } from "../../provider/acp/XAiAcpExtension.ts";
import { layer as idAllocatorLayer, IdAllocatorV2 } from "../IdAllocator.ts";
import {
  ProviderAdapterV2RuntimePolicy,
  type ProviderAdapterV2TurnInput,
} from "../ProviderAdapter.ts";
import {
  AcpProviderCapabilitiesV2,
  acpPostSettleContinuationOfferEvidence,
  acpPostSettleWakeEvidence,
  makeAcpAdapterV2,
  type AcpAdapterV2Flavor,
} from "./AcpAdapterV2.ts";

const serverConfigLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-acp-v2-adapter-",
}).pipe(Layer.provide(NodeServices.layer));

const testLayer = Layer.mergeAll(NodeServices.layer, idAllocatorLayer, serverConfigLayer);
const ACP_TEST_DRIVER = ProviderDriverKind.make("acp-test");
const decodeUnknownJson = Schema.decodeUnknownOption(Schema.UnknownFromJsonString);

function makeMockRuntime(input: {
  readonly childProcessSpawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly mockAgentPath: string;
  readonly environment?: Readonly<Record<string, string>>;
  readonly protocolEvents?: Queue.Queue<EffectAcpProtocol.AcpProtocolLogEvent>;
}): AcpAdapterV2Flavor["makeRuntime"] {
  return (runtimeInput) =>
    Effect.gen(function* () {
      const protocolEvents = input.protocolEvents;
      const protocolLogging =
        protocolEvents === undefined
          ? runtimeInput.protocolLogging
          : {
              ...runtimeInput.protocolLogging,
              logger: (event: EffectAcpProtocol.AcpProtocolLogEvent) =>
                Queue.offer(protocolEvents, event).pipe(
                  Effect.andThen(runtimeInput.protocolLogging.logger?.(event) ?? Effect.void),
                  Effect.asVoid,
                ),
            };
      const context = yield* Layer.build(
        AcpSessionRuntime.layer({
          ...runtimeInput,
          protocolLogging,
          spawn: {
            command: process.execPath,
            args: [input.mockAgentPath],
            cwd: runtimeInput.cwd,
            env: { T3_ACP_SESSION_LIFECYCLE: "1", ...input.environment },
          },
          authMethodId: "test",
        }).pipe(
          Layer.provide(
            Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, input.childProcessSpawner),
          ),
        ),
      );
      return yield* Effect.service(AcpSessionRuntime.AcpSessionRuntime).pipe(
        Effect.provide(context),
      );
    });
}

function rawProtocolMethod(event: EffectAcpProtocol.AcpProtocolLogEvent): string | undefined {
  if (event.stage !== "raw" || typeof event.payload !== "string") return undefined;
  for (const line of event.payload.split("\n")) {
    const trimmed = line.trim();
    if (trimmed.length === 0) continue;
    const decoded = Option.getOrUndefined(decodeUnknownJson(trimmed));
    if (typeof decoded === "object" && decoded !== null && "method" in decoded) {
      const method = (decoded as { readonly method?: unknown }).method;
      if (typeof method === "string") return method;
    }
  }
  return undefined;
}

function makeTurnInput(input: {
  readonly threadId: ThreadId;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly instanceId: ProviderInstanceId;
  readonly runtimePolicy: ProviderAdapterV2RuntimePolicy;
  readonly now: DateTime.Utc;
  readonly ordinal?: number;
  readonly modelSelection?: ModelSelection;
}): ProviderAdapterV2TurnInput {
  const ordinal = input.ordinal ?? 1;
  const suffix = `${input.threadId}:${ordinal}`;
  const modelSelection =
    input.modelSelection ?? ({ instanceId: input.instanceId, model: "default" } as const);
  return {
    appThread: {
      createdBy: "user",
      creationSource: "web",
      id: input.threadId,
      projectId: ProjectId.make(`project:${input.threadId}`),
      title: "ACP adapter test",
      providerInstanceId: input.instanceId,
      modelSelection,
      runtimeMode: "approval-required",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: input.providerThread.id,
      lineage: {
        parentThreadId: null,
        relationshipToParent: null,
        rootThreadId: input.threadId,
      },
      forkedFrom: null,
      createdAt: input.now,
      updatedAt: input.now,
      archivedAt: null,
      deletedAt: null,
    },
    threadId: input.threadId,
    runId: RunId.make(`run:${suffix}`),
    runOrdinal: ordinal,
    providerTurnOrdinal: ordinal,
    attemptId: RunAttemptId.make(`attempt:${suffix}`),
    rootNodeId: NodeId.make(`node:${suffix}`),
    providerThread: input.providerThread,
    message: {
      createdBy: "user",
      creationSource: "web",
      messageId: MessageId.make(`message:${suffix}`),
      text: "test prompt",
      attachments: [],
    },
    modelSelection,
    runtimePolicy: input.runtimePolicy,
  };
}

describe("AcpAdapterV2", () => {
  it.effect("negotiates and executes optional native session forks through the ACP runtime", () =>
    Effect.gen(function* () {
      const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fileSystem = yield* FileSystem.FileSystem;
      const idAllocator = yield* IdAllocatorV2;
      const path = yield* Path.Path;
      const serverConfig = yield* ServerConfig;
      const mockAgentPath = yield* path.fromFileUrl(
        new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
      );
      const makeRuntime = makeMockRuntime({ childProcessSpawner, mockAgentPath });

      const instanceId = ProviderInstanceId.make("acp-test");
      const adapter = makeAcpAdapterV2({
        instanceId,
        flavor: {
          driver: ACP_TEST_DRIVER,
          capabilities: AcpProviderCapabilitiesV2,
          makeRuntime,
        },
        fileSystem,
        idAllocator,
        serverConfig,
      });
      const sourceThreadId = ThreadId.make("thread-acp-native-fork-source");
      const targetThreadId = ThreadId.make("thread-acp-native-fork-target");
      const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
        runtimeMode: "full-access",
        interactionMode: "default",
        cwd: process.cwd(),
      });
      const modelSelection = { instanceId, model: "default" } as const;
      const runtime = yield* adapter.openSession({
        threadId: sourceThreadId,
        providerSessionId: ProviderSessionId.make("provider-session-acp-native-fork"),
        modelSelection,
        runtimePolicy,
      });

      assert.isTrue(runtime.providerSession.capabilities.threads.canForkThread);
      assert.isTrue(runtime.providerSession.capabilities.threads.canReadThreadSnapshot);

      const sourceProviderThread = yield* runtime.ensureThread({
        threadId: sourceThreadId,
        modelSelection,
        runtimePolicy,
      });
      const forkedProviderThread = yield* runtime.forkThread({
        sourceProviderThread,
        targetThreadId,
      });

      assert.equal(sourceProviderThread.nativeThreadRef?.nativeId, "mock-session-1");
      assert.equal(forkedProviderThread.nativeThreadRef?.nativeId, "mock-session-1-fork");
      assert.equal(forkedProviderThread.appThreadId, targetThreadId);
      assert.equal(forkedProviderThread.forkedFrom?.providerThreadId, sourceProviderThread.id);
    }).pipe(Effect.provide(testLayer), Effect.scoped),
  );

  it.effect("rejects requested options that the active ACP session does not expose", () =>
    Effect.gen(function* () {
      const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fileSystem = yield* FileSystem.FileSystem;
      const idAllocator = yield* IdAllocatorV2;
      const path = yield* Path.Path;
      const serverConfig = yield* ServerConfig;
      const mockAgentPath = yield* path.fromFileUrl(
        new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
      );
      const instanceId = ProviderInstanceId.make("acp-test");
      const adapter = makeAcpAdapterV2({
        instanceId,
        flavor: {
          driver: ACP_TEST_DRIVER,
          capabilities: AcpProviderCapabilitiesV2,
          makeRuntime: makeMockRuntime({ childProcessSpawner, mockAgentPath }),
        },
        fileSystem,
        idAllocator,
        serverConfig,
      });
      const threadId = ThreadId.make("thread-acp-unsupported-option");
      const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
        runtimeMode: "full-access",
        interactionMode: "default",
        cwd: process.cwd(),
      });
      const error = yield* adapter
        .openSession({
          threadId,
          providerSessionId: ProviderSessionId.make("provider-session-acp-unsupported-option"),
          modelSelection: {
            instanceId,
            model: "default",
            options: [{ id: "missing-option", value: "high" }],
          },
          runtimePolicy,
        })
        .pipe(Effect.flip);

      assert.equal(error._tag, "ProviderAdapterOpenSessionError");
      assert.include(String(error.cause), "does not expose requested configuration option(s)");
      assert.include(String(error.cause), "missing-option");
    }).pipe(Effect.provide(testLayer), Effect.scoped),
  );

  it.effect("reconfigures a loaded ACP session from its own active setup metadata", () =>
    Effect.gen(function* () {
      const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fileSystem = yield* FileSystem.FileSystem;
      const idAllocator = yield* IdAllocatorV2;
      const path = yield* Path.Path;
      const serverConfig = yield* ServerConfig;
      const mockAgentPath = yield* path.fromFileUrl(
        new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
      );
      const protocolEvents = yield* Queue.bounded<EffectAcpProtocol.AcpProtocolLogEvent>(256);
      const instanceId = ProviderInstanceId.make("acp-test");
      const adapter = makeAcpAdapterV2({
        instanceId,
        flavor: {
          driver: ACP_TEST_DRIVER,
          capabilities: AcpProviderCapabilitiesV2,
          makeRuntime: makeMockRuntime({ childProcessSpawner, mockAgentPath, protocolEvents }),
        },
        fileSystem,
        idAllocator,
        serverConfig,
      });
      const firstThreadId = ThreadId.make("thread-acp-active-setup:first");
      const secondThreadId = ThreadId.make("thread-acp-active-setup:second");
      const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
        runtimeMode: "full-access",
        interactionMode: "default",
        cwd: process.cwd(),
      });
      const initialSelection = { instanceId, model: "default" } satisfies ModelSelection;
      const alternateSelection = {
        instanceId,
        model: "grok-mock-alt",
      } satisfies ModelSelection;
      const originalSelection = { instanceId, model: "grok-build" } satisfies ModelSelection;
      const runtime = yield* adapter.openSession({
        threadId: firstThreadId,
        providerSessionId: ProviderSessionId.make("provider-session-acp-active-setup"),
        modelSelection: initialSelection,
        runtimePolicy,
      });
      const firstProviderThread = yield* runtime.ensureThread({
        threadId: firstThreadId,
        modelSelection: initialSelection,
        runtimePolicy,
      });
      const now = yield* DateTime.now;
      yield* runtime.startTurn(
        makeTurnInput({
          threadId: firstThreadId,
          providerThread: firstProviderThread,
          instanceId,
          runtimePolicy,
          modelSelection: alternateSelection,
          now,
        }),
      );
      yield* runtime.events.pipe(
        Stream.filter((event) => event.type === "turn.terminal"),
        Stream.runHead,
      );

      const secondProviderThread: OrchestrationV2ProviderThread = {
        ...firstProviderThread,
        id: ProviderThreadId.make("provider-thread-acp-active-setup:second"),
        appThreadId: secondThreadId,
        nativeThreadRef: {
          driver: ACP_TEST_DRIVER,
          nativeId: "mock-session-2",
          strength: "strong",
        },
        status: "idle",
      };
      yield* runtime.resumeThread({
        providerThread: secondProviderThread,
        modelSelection: alternateSelection,
        runtimePolicy,
      });
      yield* runtime.startTurn(
        makeTurnInput({
          threadId: secondThreadId,
          providerThread: secondProviderThread,
          instanceId,
          runtimePolicy,
          modelSelection: originalSelection,
          now,
          ordinal: 2,
        }),
      );
      yield* runtime.events.pipe(
        Stream.filter((event) => event.type === "turn.terminal"),
        Stream.runHead,
      );

      const setModelRequests = Array.from(yield* Queue.takeAll(protocolEvents)).filter(
        (event) =>
          event.direction === "outgoing" && rawProtocolMethod(event) === "session/set_model",
      );
      assert.lengthOf(setModelRequests, 2);
    }).pipe(Effect.provide(testLayer), Effect.scoped),
  );

  it.effect("cancels pending permission requests while interrupting an ACP turn", () =>
    Effect.gen(function* () {
      const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fileSystem = yield* FileSystem.FileSystem;
      const idAllocator = yield* IdAllocatorV2;
      const path = yield* Path.Path;
      const serverConfig = yield* ServerConfig;
      const mockAgentPath = yield* path.fromFileUrl(
        new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
      );
      const instanceId = ProviderInstanceId.make("acp-test");
      const adapter = makeAcpAdapterV2({
        instanceId,
        flavor: {
          driver: ACP_TEST_DRIVER,
          capabilities: AcpProviderCapabilitiesV2,
          makeRuntime: makeMockRuntime({
            childProcessSpawner,
            mockAgentPath,
            environment: { T3_ACP_EMIT_TOOL_CALLS: "1" },
          }),
        },
        fileSystem,
        idAllocator,
        serverConfig,
      });
      const threadId = ThreadId.make("thread-acp-cancel-permission");
      const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
        runtimeMode: "approval-required",
        interactionMode: "default",
        cwd: process.cwd(),
      });
      const modelSelection = { instanceId, model: "default" } as const;
      const runtime = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("provider-session-acp-cancel-permission"),
        modelSelection,
        runtimePolicy,
      });
      const providerThread = yield* runtime.ensureThread({
        threadId,
        modelSelection,
        runtimePolicy,
      });
      const now = yield* DateTime.now;
      yield* runtime.startTurn(
        makeTurnInput({ threadId, providerThread, instanceId, runtimePolicy, now }),
      );

      const pendingRequest = Option.getOrThrow(
        yield* runtime.events.pipe(
          Stream.filter(
            (event) =>
              event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
          ),
          Stream.runHead,
        ),
      );
      if (
        pendingRequest.type !== "runtime_request.updated" ||
        pendingRequest.runtimeRequest.providerTurnId === null
      ) {
        return yield* Effect.die("Expected a pending ACP permission request with a provider turn");
      }

      yield* runtime.interruptTurn({
        providerThread,
        providerTurnId: pendingRequest.runtimeRequest.providerTurnId,
      });

      const cancelledRequest = Option.getOrThrow(
        yield* runtime.events.pipe(
          Stream.filter(
            (event) =>
              event.type === "runtime_request.updated" &&
              event.runtimeRequest.id === pendingRequest.runtimeRequest.id &&
              event.runtimeRequest.status === "cancelled",
          ),
          Stream.runHead,
        ),
      );
      assert.equal(cancelledRequest.type, "runtime_request.updated");
    }).pipe(Effect.provide(testLayer), Effect.scoped),
  );

  it.effect("does not release an ACP turn when cancellation is not acknowledged", () =>
    Effect.gen(function* () {
      const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fileSystem = yield* FileSystem.FileSystem;
      const idAllocator = yield* IdAllocatorV2;
      const path = yield* Path.Path;
      const serverConfig = yield* ServerConfig;
      const mockAgentPath = yield* path.fromFileUrl(
        new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
      );
      const instanceId = ProviderInstanceId.make("acp-test");
      const protocolEvents = yield* Queue.bounded<EffectAcpProtocol.AcpProtocolLogEvent>(256);
      const adapter = makeAcpAdapterV2({
        instanceId,
        flavor: {
          driver: ACP_TEST_DRIVER,
          capabilities: AcpProviderCapabilitiesV2,
          makeRuntime: makeMockRuntime({
            childProcessSpawner,
            mockAgentPath,
            environment: { T3_ACP_PROMPT_DELAY_MS: "5000" },
            protocolEvents,
          }),
        },
        fileSystem,
        idAllocator,
        serverConfig,
      });
      const threadId = ThreadId.make("thread-acp-cancel-timeout");
      const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
        runtimeMode: "full-access",
        interactionMode: "default",
        cwd: process.cwd(),
      });
      const modelSelection = { instanceId, model: "default" } as const;
      const runtime = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("provider-session-acp-cancel-timeout"),
        modelSelection,
        runtimePolicy,
      });
      const providerThread = yield* runtime.ensureThread({
        threadId,
        modelSelection,
        runtimePolicy,
      });
      const now = yield* DateTime.now;
      const firstTurn = makeTurnInput({
        threadId,
        providerThread,
        instanceId,
        runtimePolicy,
        now,
      });
      yield* runtime.startTurn(firstTurn);
      yield* Stream.fromQueue(protocolEvents).pipe(
        Stream.filter(
          (event) =>
            event.direction === "outgoing" && rawProtocolMethod(event) === "session/prompt",
        ),
        Stream.runHead,
      );
      const providerTurnId = idAllocator.derive.providerTurn({
        driver: ACP_TEST_DRIVER,
        nativeTurnId: "mock-session-1:turn:1",
      });
      const interruptFiber = yield* runtime
        .interruptTurn({ providerThread, providerTurnId })
        .pipe(Effect.flip, Effect.forkScoped);
      yield* Stream.fromQueue(protocolEvents).pipe(
        Stream.filter(
          (event) =>
            event.direction === "outgoing" && rawProtocolMethod(event) === "session/cancel",
        ),
        Stream.runHead,
      );
      yield* TestClock.adjust("10 seconds");
      const interruptError = yield* Fiber.join(interruptFiber);
      assert.equal(interruptError._tag, "ProviderAdapterInterruptError");

      const secondTurnError = yield* runtime
        .startTurn(
          makeTurnInput({
            threadId,
            providerThread,
            instanceId,
            runtimePolicy,
            now,
            ordinal: 2,
          }),
        )
        .pipe(Effect.flip);
      assert.equal(secondTurnError._tag, "ProviderAdapterTurnStartError");
    }).pipe(Effect.provide(testLayer), Effect.scoped),
  );

  it.effect("restarts the ACP child process before the next prompt after interrupt", () =>
    Effect.gen(function* () {
      const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fileSystem = yield* FileSystem.FileSystem;
      const idAllocator = yield* IdAllocatorV2;
      const path = yield* Path.Path;
      const serverConfig = yield* ServerConfig;
      const mockAgentPath = yield* path.fromFileUrl(
        new URL("../../../scripts/acp-mock-agent.ts", import.meta.url),
      );
      const protocolEvents = yield* Queue.bounded<EffectAcpProtocol.AcpProtocolLogEvent>(256);
      const instanceId = ProviderInstanceId.make("acp-test");
      const adapter = makeAcpAdapterV2({
        instanceId,
        flavor: {
          driver: ACP_TEST_DRIVER,
          capabilities: AcpProviderCapabilitiesV2,
          restartRuntimeAfterInterrupt: true,
          makeRuntime: makeMockRuntime({
            childProcessSpawner,
            mockAgentPath,
            protocolEvents,
          }),
        },
        fileSystem,
        idAllocator,
        serverConfig,
      });
      const threadId = ThreadId.make("thread-acp-restart-after-interrupt");
      const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
        runtimeMode: "full-access",
        interactionMode: "default",
        cwd: process.cwd(),
      });
      const modelSelection = { instanceId, model: "default" } as const;
      const runtime = yield* adapter.openSession({
        threadId,
        providerSessionId: ProviderSessionId.make("provider-session-acp-restart-after-interrupt"),
        modelSelection,
        runtimePolicy,
      });
      const providerThread = yield* runtime.ensureThread({
        threadId,
        modelSelection,
        runtimePolicy,
      });
      const now = yield* DateTime.now;
      yield* runtime.startTurn(
        makeTurnInput({ threadId, providerThread, instanceId, runtimePolicy, now, ordinal: 1 }),
      );
      yield* Stream.fromQueue(protocolEvents).pipe(
        Stream.filter(
          (event) =>
            event.direction === "outgoing" && rawProtocolMethod(event) === "session/prompt",
        ),
        Stream.runHead,
      );
      const providerTurnId = idAllocator.derive.providerTurn({
        driver: ACP_TEST_DRIVER,
        nativeTurnId: `${providerThread.nativeThreadRef?.nativeId}:turn:1`,
      });
      yield* runtime.interruptTurn({
        providerThread,
        providerTurnId,
        requestRuntimeRestart: true,
      });
      yield* Stream.fromQueue(protocolEvents).pipe(
        Stream.filter(
          (event) =>
            event.direction === "outgoing" && rawProtocolMethod(event) === "session/cancel",
        ),
        Stream.runHead,
      );

      yield* runtime.startTurn(
        makeTurnInput({ threadId, providerThread, instanceId, runtimePolicy, now, ordinal: 2 }),
      );
      const loadAfterRestart = yield* Stream.fromQueue(protocolEvents).pipe(
        Stream.filter(
          (event) => event.direction === "outgoing" && rawProtocolMethod(event) === "session/load",
        ),
        Stream.runHead,
      );
      assert.isTrue(
        Option.isSome(loadAfterRestart),
        "post-interrupt startTurn should respawn the runtime and replay session/load",
      );
    }).pipe(Effect.provide(testLayer), Effect.scoped),
  );
});

describe("acpPostSettleWakeEvidence", () => {
  const sessionId = "session-wake";

  it("accepts assistant text and tool updates as wake evidence", () => {
    assert.isTrue(
      acpPostSettleWakeEvidence({
        sessionId,
        update: {
          sessionUpdate: "agent_message_chunk",
          content: { type: "text", text: "Subagent finished. SUBAGENT_DONE" },
        },
      }),
    );
    assert.isTrue(
      acpPostSettleWakeEvidence({
        sessionId,
        update: {
          sessionUpdate: "tool_call",
          toolCallId: "call-1",
          title: "get_command_or_subagent_output",
          status: "pending",
          kind: "other",
          content: [],
          locations: [],
          rawInput: {},
        },
      }),
    );
  });

  it("rejects monitor end chatter and background mutations", () => {
    assert.isFalse(
      acpPostSettleWakeEvidence({
        sessionId,
        update: {
          sessionUpdate: "agent_message_chunk",
          content: {
            type: "text",
            text: 'Monitor "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" ended',
          },
        },
      }),
    );
    assert.isFalse(
      acpPostSettleWakeEvidence(
        {
          sessionId,
          update: {
            sessionUpdate: "user_message_chunk",
            content: { type: "text", text: "task-1 completed" },
          },
        },
        {
          extractBackgroundToolMutation: () => ({
            taskId: "task-1",
            status: "completed",
            appendOutput: "",
          }),
        },
      ),
    );
  });
});

describe("acpPostSettleContinuationOfferEvidence", () => {
  const sessionId = "session-wake-offer";

  it("offers on assistant text and terminal tool status", () => {
    assert.isTrue(
      acpPostSettleContinuationOfferEvidence({
        sessionId,
        update: {
          sessionUpdate: "agent_message_chunk",
          content: { type: "text", text: "Background shell finished." },
        },
      }),
    );
    assert.isTrue(
      acpPostSettleContinuationOfferEvidence({
        sessionId,
        update: {
          sessionUpdate: "tool_call_update",
          toolCallId: "call-1",
          title: "run_terminal_command",
          status: "completed",
          kind: "other",
          content: [],
          locations: [],
          rawInput: {},
        },
      }),
    );
    assert.isTrue(
      acpPostSettleContinuationOfferEvidence({
        sessionId,
        update: {
          sessionUpdate: "tool_call_update",
          toolCallId: "call-2",
          title: "run_terminal_command",
          status: "failed",
          kind: "other",
          content: [],
          locations: [],
          rawInput: {},
        },
      }),
    );
  });

  it("buffers in-progress tool updates without offering", () => {
    assert.isTrue(
      acpPostSettleWakeEvidence({
        sessionId,
        update: {
          sessionUpdate: "tool_call_update",
          toolCallId: "call-1",
          title: "run_terminal_command",
          status: "in_progress",
          kind: "other",
          content: [],
          locations: [],
          rawInput: {},
        },
      }),
    );
    assert.isFalse(
      acpPostSettleContinuationOfferEvidence({
        sessionId,
        update: {
          sessionUpdate: "tool_call_update",
          toolCallId: "call-1",
          title: "run_terminal_command",
          status: "in_progress",
          kind: "other",
          content: [],
          locations: [],
          rawInput: {},
        },
      }),
    );
    assert.isFalse(
      acpPostSettleContinuationOfferEvidence({
        sessionId,
        update: {
          sessionUpdate: "tool_call",
          toolCallId: "call-1",
          title: "run_terminal_command",
          status: "pending",
          kind: "other",
          content: [],
          locations: [],
          rawInput: {},
        },
      }),
    );
  });

  it("does not offer filtered monitor chatter", () => {
    assert.isFalse(
      acpPostSettleContinuationOfferEvidence({
        sessionId,
        update: {
          sessionUpdate: "agent_message_chunk",
          content: {
            type: "text",
            text: 'Monitor "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" ended',
          },
        },
      }),
    );
  });

  it("does not offer on a normalized monitor start ACK despite raw completed status", () => {
    const monitorStartAck = {
      sessionId,
      update: {
        sessionUpdate: "tool_call_update",
        toolCallId: "call-monitor",
        title: "Tool",
        status: "completed",
        kind: "other",
        content: [],
        locations: [],
        rawInput: { variant: "Monitor", description: "stream test" },
        rawOutput: {
          type: "Monitor",
          taskId: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
          timeoutMs: 60000,
        },
      },
    } as const;
    // Raw frame looks terminal; the Grok flavor knows it is a running monitor.
    assert.isTrue(acpPostSettleContinuationOfferEvidence(monitorStartAck));
    assert.isFalse(
      acpPostSettleContinuationOfferEvidence(monitorStartAck, {
        normalizeToolCall: normalizeXAiAcpToolCallState,
      }),
    );
  });
});
