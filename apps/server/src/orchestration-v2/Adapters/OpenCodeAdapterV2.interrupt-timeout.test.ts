import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import type { V2Event } from "@opencode/client";
import {
  MessageId,
  NodeId,
  OpenCode2Settings,
  OrchestrationV2AppThread,
  ProjectId,
  ProviderInstanceId,
  ProviderReplayEntry,
  ProviderSessionId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import { ServerConfig } from "../../config.ts";
import { IdAllocatorV2, layer as idAllocatorLayer } from "../IdAllocator.ts";
import {
  ProviderAdapterProtocolError,
  ProviderAdapterV2RuntimePolicy,
} from "../ProviderAdapter.ts";
import { readProviderReplayTranscript } from "../testkit/ReplayTranscriptNdjson.ts";
import {
  makeOpenCodeAdapterV2,
  OPENCODE2_INTERRUPT_REQUEST_TIMEOUT_MS,
  OPENCODE_PROVIDER,
} from "./OpenCodeAdapterV2.ts";
import type { ProviderInteractionModeReflection } from "../ProviderInteractionModeReflections.ts";
import { makeReplayClient, OpenCode2ReplayController } from "./OpenCodeAdapterV2.testkit.ts";

const decodeSettings = Schema.decodeUnknownEffect(OpenCode2Settings);
const isProtocolError = Schema.is(ProviderAdapterProtocolError);

const TestLayer = Layer.mergeAll(
  idAllocatorLayer,
  ServerConfig.layerTest(process.cwd(), { prefix: "opencode-stop-timeout-" }),
).pipe(Layer.provideMerge(NodeServices.layer));

it.layer(TestLayer)("OpenCode settled Stop timeout", (it) => {
  for (const failEnumeration of [false, true]) {
    it.effect(
      `keeps unconfirmed root Stop failed, quarantined and without reflection authority; known cleanup survives enumeration failure=${failEnumeration}`,
      () =>
        Effect.gen(function* () {
          const raw = yield* readProviderReplayTranscript(
            new URL(
              "../testkit/fixtures/opencode2_settled_background_stop/opencode2_transcript.ndjson",
              import.meta.url,
            ),
          );
          const end = raw.entries.findIndex(
            (entry) =>
              entry.type === "emit_inbound" && entry.label === "child.children.list.response",
          );
          assert.isAtLeast(end, 0);
          const rootId = "ses_opencode2_settled_background_stop";
          const childId = "ses_opencode2_settled_background_stop_child";
          // Reuse the real adapter transcript up to cleanup; the root request has
          // no response. Its abort is driven by TestClock, not a delayed fixture.
          const entries = raw.entries
            .slice(0, end + 1)
            .filter(
              (entry) =>
                entry.type !== "emit_inbound" || entry.label !== "root.session.interrupt.response",
            )
            .flatMap((entry): ProviderReplayEntry[] => {
              if (entry.type !== "emit_inbound") return [entry];
              // A native child's selection never has root reflection authority.
              if (entry.label === "child.execution.started")
                return [entry, selected(childId, "plan", 7)];
              // The settled ordinary root turn still owns a later session selection.
              if (entry.label === "root.execution.succeeded")
                return [entry, selected(rootId, "plan", 50)];
              return [entry];
            })
            .concat(
              // After an unconfirmed Stop the quarantined session cannot reflect.
              selected(rootId, "build", 51),
              {
                type: "emit_inbound",
                frame: {
                  type: "sdk.event",
                  event: {
                    id: "evt_opencode2_stop_window_drained",
                    created: 1785297600052,
                    type: "session.renamed",
                    durable: { aggregateID: rootId, seq: 52, version: 1 },
                    data: { sessionID: rootId, title: "reflection window drained" },
                  } satisfies V2Event,
                },
              },
            )
            .map((entry) =>
              failEnumeration &&
              entry.type === "emit_inbound" &&
              entry.label === "root.children.list.response"
                ? {
                    ...entry,
                    type: "emit_inbound" as const,
                    frame: {
                      type: "sdk.error",
                      operation: "session.list",
                      error: "enumeration unavailable",
                    },
                  }
                : entry,
            );
          const controller = new OpenCode2ReplayController({
            ...raw,
            provider: OPENCODE_PROVIDER,
            protocol: "opencode2-sdk.sse",
            entries,
          });
          yield* Effect.addFinalizer(() => Effect.sync(() => controller.abort()));
          const client = makeReplayClient(controller);
          const windowDrained = Promise.withResolvers<void>();
          const subscribe = client.event.subscribe.bind(client.event);
          client.event.subscribe = (options) => {
            const events = subscribe(options);
            return {
              async *[Symbol.asyncIterator]() {
                for await (const event of events) {
                  yield event;
                  // Iterator resume proves the adapter finished handling the marker.
                  if (
                    event.type === "session.renamed" &&
                    event.data.title === "reflection window drained"
                  )
                    windowDrained.resolve();
                }
              },
            };
          };
          const rootInterruptStarted = Promise.withResolvers<void>();
          let aborted = false;
          const interrupt = client.session.interrupt.bind(client.session);
          client.session.interrupt = async (input, options) => {
            if (input.sessionID !== rootId) return interrupt(input, options);
            await controller.expectOutbound({ type: "session.interrupt", input });
            rootInterruptStarted.resolve();
            return new Promise((_resolve, reject) => {
              options?.signal?.addEventListener(
                "abort",
                () => {
                  aborted = true;
                  reject(new Error("root request aborted without acknowledgement"));
                },
                { once: true },
              );
            });
          };
          const instanceId = ProviderInstanceId.make("opencode-timeout-test");
          const modelSelection = { instanceId, model: "opencode/big-pickle" };
          const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
            runtimeMode: "full-access",
            interactionMode: "default",
            cwd: process.cwd(),
          });
          const offers: ProviderInteractionModeReflection[] = [];
          const adapter = makeOpenCodeAdapterV2({
            interactionModeReflections: {
              offer: (request) =>
                Effect.sync(() => {
                  offers.push(request);
                }),
            },
            instanceId,
            settings: yield* decodeSettings({
              serverUrl: "replay://opencode2",
            }),
            environment: {},
            runtime: {
              connectToOpenCodeServer: () =>
                Effect.succeed({
                  url: "replay://opencode2",
                  password: "replay",
                  external: true,
                  exitCode: null,
                }),
              createOpenCodeSdkClient: () => client,
            },
            idAllocator: yield* IdAllocatorV2,
            serverConfig: yield* ServerConfig,
          });
          const threadId = ThreadId.make("opencode-settled-timeout");
          const session = yield* adapter.openSession({
            threadId,
            providerSessionId: ProviderSessionId.make("opencode-settled-timeout-session"),
            modelSelection,
            runtimePolicy,
          });
          const providerThread = yield* session.ensureThread({
            threadId,
            modelSelection,
            runtimePolicy,
          });
          const now = yield* DateTime.now;
          const appThread = OrchestrationV2AppThread.make({
            id: threadId,
            projectId: ProjectId.make("timeout-project"),
            title: "Timeout regression",
            providerInstanceId: instanceId,
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            createdBy: "user",
            creationSource: "web",
            branch: null,
            worktreePath: process.cwd(),
            activeProviderThreadId: providerThread.id,
            lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
            forkedFrom: null,
            createdAt: now,
            updatedAt: now,
            archivedAt: null,
            deletedAt: null,
            settledOverride: null,
            settledAt: null,
            lastVisitedAt: null,
          });
          const terminal = yield* session.events.pipe(
            Stream.filter(
              (event) =>
                event.type === "turn.terminal" && event.providerThreadId === providerThread.id,
            ),
            Stream.runHead,
            Effect.forkChild({ startImmediately: true }),
          );
          yield* session.startTurn({
            appThread,
            threadId,
            runId: RunId.make("timeout-run"),
            runOrdinal: 1,
            providerTurnOrdinal: 1,
            attemptId: RunAttemptId.make("timeout-attempt"),
            rootNodeId: NodeId.make("timeout-root"),
            providerThread,
            message: {
              messageId: MessageId.make("timeout-message"),
              text: "Start one background subagent with description background child fixture and prompt Respond exactly CHILD_BACKGROUND_OK. Then respond exactly PARENT_RELEASED without waiting for the child.",
              attachments: [],
              createdBy: "user",
              creationSource: "web",
            },
            modelSelection,
            runtimePolicy,
          });
          const settled = yield* Fiber.join(terminal);
          assert.isTrue(Option.isSome(settled));
          const rootTerminal = Option.getOrThrow(settled);
          assert.equal(rootTerminal.type, "turn.terminal");
          if (rootTerminal.type !== "turn.terminal")
            return yield* Effect.die("expected root terminal");
          assert.equal(rootTerminal.status, "completed");
          const stop = yield* session
            .interruptTurn({ providerThread, providerTurnId: rootTerminal.providerTurnId })
            .pipe(Effect.flip, Effect.forkChild({ startImmediately: true }));
          yield* Effect.promise(() => rootInterruptStarted.promise);
          yield* TestClock.adjust(OPENCODE2_INTERRUPT_REQUEST_TIMEOUT_MS);
          const error = yield* Fiber.join(stop);
          assert.equal(error._tag, "ProviderAdapterInterruptError");
          assert.isTrue(aborted);
          yield* Effect.promise(() => windowDrained.promise);
          controller.assertComplete(); // Includes root shell, known child interrupt and child shell removal.
          assert.deepEqual(
            offers.map((request) => [
              request.sourceRunId,
              request.interactionMode,
              request.dedupeKey,
            ]),
            [["timeout-run", "plan", `opencode:${rootId}:50`]],
          );
          const reuseError = yield* session
            .resumeThread({ providerThread, threadId, modelSelection, runtimePolicy })
            .pipe(Effect.flip);
          assert.equal(reuseError._tag, "ProviderAdapterResumeThreadError");
          assert.isTrue(isProtocolError(reuseError.cause));
          if (isProtocolError(reuseError.cause))
            assert.include(reuseError.cause.detail, "quarantined");
          controller.assertComplete();
        }),
    );
  }
});

function selected(sessionID: string, agent: string, seq: number): ProviderReplayEntry {
  return {
    type: "emit_inbound",
    frame: {
      type: "sdk.event",
      event: {
        id: `evt_opencode2_stop_selection_${seq}`,
        created: 1785297600000 + seq,
        type: "session.agent.selected",
        durable: { aggregateID: sessionID, seq, version: 1 },
        data: { sessionID, agent },
      } satisfies V2Event,
    },
  };
}
