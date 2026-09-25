import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import type { SessionAgentSelected, SessionModelSelected, V2Event } from "@opencode/client";
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

import { ServerConfig } from "../../config.ts";
import { IdAllocatorV2, layer as idAllocatorLayer } from "../IdAllocator.ts";
import { ProviderAdapterV2RuntimePolicy } from "../ProviderAdapter.ts";
import { readProviderReplayTranscript } from "../testkit/ReplayTranscriptNdjson.ts";
import { makeOpenCodeAdapterV2, OPENCODE_PROVIDER } from "./OpenCodeAdapterV2.ts";
import { makeReplayClient, OpenCode2ReplayController } from "./OpenCodeAdapterV2.testkit.ts";

const decodeSettings = Schema.decodeEffect(OpenCode2Settings);
const decodeReplayEntry = Schema.decodeEffect(Schema.fromJsonString(ProviderReplayEntry));
const sessionID = "ses_opencode2_multi_turn";
const model = { id: "big-pickle", providerID: "opencode" };
const instanceId = ProviderInstanceId.make("opencode");
const TestLayer = Layer.mergeAll(
  idAllocatorLayer,
  ServerConfig.layerTest(process.cwd(), { prefix: "opencode-selection-" }),
).pipe(Layer.provideMerge(NodeServices.layer));

it.layer(TestLayer)("OpenCode pinned native selection", (it) => {
  for (const test of [
    {
      name: "variant-only external change reasserts explicit high",
      variant: "high",
      native: { ...model, variant: "low" },
      switchModel: { ...model, variant: "high" },
    },
    { name: "missing variant preserves native variant", native: { ...model, variant: "high" } },
    {
      name: "explicit default resets native variant",
      variant: "default",
      native: { ...model, variant: "high" },
      switchModel: model,
    },
    {
      name: "model change reasserts explicit model without a variant",
      native: { id: "glm-5.2", providerID: "opencode", variant: "high" },
      switchModel: model,
    },
    {
      name: "unknown requested variant remains clamped",
      variant: "bogus",
      native: { ...model, variant: "high" },
      switchModel: model,
    },
    {
      name: "duplicate and stale events cannot hide an external variant",
      variant: "high",
      native: { ...model, variant: "low" },
      switchModel: { ...model, variant: "high" },
      stale: true,
    },
  ]) {
    it.effect(test.name, () =>
      Effect.gen(function* () {
        const raw = yield* readProviderReplayTranscript(
          new URL("../testkit/fixtures/multi_turn/opencode2_transcript.ndjson", import.meta.url),
        );
        const selected: SessionModelSelected = {
          id: "evt_00000000000000000020",
          created: 1785297600020,
          type: "session.model.selected",
          durable: { aggregateID: sessionID, seq: 20, version: 1 },
          data: { sessionID, model: test.native },
        };
        const events: SessionModelSelected[] = [selected];
        if (test.stale) {
          events.push(
            selected,
            {
              ...selected,
              id: "evt_00000000000000000021",
              durable: { aggregateID: sessionID, seq: 19, version: 1 },
              data: { sessionID, model: { ...model, variant: "high" } },
            },
            {
              ...selected,
              id: "evt_00000000000000000022",
              durable: { aggregateID: "ses_other", seq: 22, version: 1 },
              data: { sessionID: "ses_other", model: { ...model, variant: "high" } },
            },
          );
        }
        const entries: ProviderReplayEntry[] = [];
        for (const entry of raw.entries) {
          if (
            entry.type === "expect_outbound" &&
            entry.label === "session.create" &&
            test.variant !== undefined &&
            test.variant !== "default"
          ) {
            entries.push(
              ...rpc("model.list", { location: { directory: "<workspace>" } }, [
                {
                  ...model,
                  variants: [{ id: "low" }, { id: "high" }],
                  limit: { context: 100000, output: 1000 },
                },
              ]),
            );
          }
          if (
            entry.type === "emit_inbound" &&
            entry.label === "session.execution.succeeded.first"
          ) {
            entries.push(
              ...events.map((event): ProviderReplayEntry => ({
                type: "emit_inbound",
                frame: { type: "sdk.event", event },
              })),
            );
          }
          if (
            entry.type === "expect_outbound" &&
            entry.label === "session.prompt.second" &&
            test.switchModel !== undefined
          ) {
            entries.push(
              ...rpc("session.switchModel", { sessionID, model: test.switchModel }, null),
            );
          }
          entries.push(entry);
        }
        yield* runTurns(entries, test.variant);
      }),
    );
  }

  for (const test of [
    {
      name: "first agent selection at sequence zero is applied",
      events: [agentSelected("plan", 0)],
    },
    {
      name: "increasing agent sequence survives reversed lexical IDs",
      events: [
        agentSelected("build", 20, "evt_fac077e14001OE0SG5VNRNycXw"),
        agentSelected("plan", 21, "evt_fac077a2c001ldlXPC2cKagZ8p", 1785297599020),
      ],
    },
    {
      name: "older agent sequence is rejected despite a greater lexical ID",
      events: [
        agentSelected("plan", 20, "evt_fac077e14001OE0SG5VNRNycXw"),
        agentSelected("build", 19, "evt_fac0781fc001MTedT6NP7p0q3A"),
      ],
    },
    {
      name: "foreign session agent selection does not alter this session",
      events: [
        agentSelected("plan", 20),
        {
          ...agentSelected("build", 21),
          durable: { aggregateID: "ses_other", seq: 21, version: 1 as const },
          data: { sessionID: "ses_other", agent: "build" },
        },
      ],
    },
    {
      name: "mismatched agent aggregate cannot advance the session watermark",
      events: [
        {
          ...agentSelected("build", 40),
          durable: { aggregateID: "ses_other", seq: 40, version: 1 as const },
        },
        agentSelected("plan", 20),
      ],
    },
    {
      name: "mismatched agent aggregate cannot replace the selected agent",
      events: [
        agentSelected("plan", 20),
        {
          ...agentSelected("build", 40),
          durable: { aggregateID: "ses_other", seq: 40, version: 1 as const },
        },
      ],
    },
    {
      name: "model watermark does not suppress an otherwise valid agent sequence",
      events: [modelSelected(30), agentSelected("plan", 21)],
      switchModel: true,
    },
    {
      name: "agent watermark does not suppress an otherwise valid model sequence",
      events: [agentSelected("plan", 30), modelSelected(21)],
      switchModel: true,
    },
  ]) {
    it.effect(test.name, () =>
      Effect.gen(function* () {
        const raw = yield* readProviderReplayTranscript(
          new URL("../testkit/fixtures/multi_turn/opencode2_transcript.ndjson", import.meta.url),
        );
        const events: (SessionAgentSelected | SessionModelSelected)[] = test.events;
        const entries: ProviderReplayEntry[] = [];
        for (const entry of raw.entries) {
          if (
            entry.type === "emit_inbound" &&
            entry.label === "session.execution.succeeded.first"
          ) {
            entries.push(
              ...events.map((event): ProviderReplayEntry => ({
                type: "emit_inbound",
                frame: { type: "sdk.event", event },
              })),
            );
          }
          if (entry.type === "expect_outbound" && entry.label === "session.prompt.second") {
            if (test.switchModel) {
              entries.push(...rpc("session.switchModel", { sessionID, model }, null));
            }
            entries.push(...rpc("session.switchAgent", { sessionID, agent: "build" }, null));
          }
          entries.push(entry);
        }
        yield* runTurns(entries);
      }),
    );
  }

  for (const id of ["evt_fac077e14001OE0SG5VNRNycXw", "evt_fac0781fc001MTedT6NP7p0q3A"]) {
    it.effect(
      `duplicate agent sequence after app alignment does not resurrect native Plan (${id})`,
      () =>
        Effect.gen(function* () {
          const raw = yield* readProviderReplayTranscript(
            new URL("../testkit/fixtures/multi_turn/opencode2_transcript.ndjson", import.meta.url),
          );
          const selected = agentSelected("plan", 20, "evt_fac077e14001OE0SG5VNRNycXw");
          const entries: ProviderReplayEntry[] = [];
          for (const entry of raw.entries) {
            if (entry.type === "runtime_exit") continue;
            if (
              entry.type === "emit_inbound" &&
              entry.label === "session.execution.succeeded.first"
            ) {
              entries.push({ type: "emit_inbound", frame: { type: "sdk.event", event: selected } });
            }
            if (entry.type === "expect_outbound" && entry.label === "session.prompt.second") {
              entries.push(...rpc("session.switchAgent", { sessionID, agent: "build" }, null));
            }
            if (
              entry.type === "emit_inbound" &&
              entry.label === "session.execution.succeeded.second"
            ) {
              entries.push({
                type: "emit_inbound",
                frame: {
                  type: "sdk.event",
                  event: {
                    ...selected,
                    id,
                  },
                },
              });
            }
            entries.push(entry);
          }
          const third = raw.entries.slice(
            raw.entries.findIndex(
              (entry) =>
                entry.type === "expect_outbound" && entry.label === "session.prompt.second",
            ),
          );
          entries.push(
            ...(yield* Effect.forEach(third, (entry) =>
              decodeReplayEntry(JSON.stringify(entry).replaceAll("second", "third")),
            )),
          );
          yield* runTurns(entries, undefined, "default", ["first", "second", "third"]);
        }),
    );
  }

  for (const position of ["executing", "settled", "continuation"] as const) {
    for (const selection of ["agent", "model"] as const) {
      it.effect(`${selection} selection during ${position} wake aligns next turn`, () =>
        Effect.gen(function* () {
          const raw = yield* readProviderReplayTranscript(
            new URL("../testkit/fixtures/multi_turn/opencode2_transcript.ndjson", import.meta.url),
          );
          const selected = selection === "agent" ? agentSelected("plan", 20) : modelSelected(20);
          const finished = nativeEvent(
            "session.execution.succeeded",
            position === "settled" ? 19 : 30,
            { sessionID },
          );
          const wake = [
            nativeEvent("session.inbox.enqueued", 10, {
              sessionID,
              inboxID: "msg_cancelled_wake",
              item: {
                type: "synthetic",
                payload: {
                  text:
                    position === "continuation"
                      ? '<subagent state="completed">CHILD_COMPLETE</subagent>'
                      : '<subagent state="cancelled">CANCELLED</subagent>',
                  description: "child result",
                },
                delivery: "queue",
              },
            }),
            nativeEvent("session.inbox.delivered", 11, {
              sessionID,
              inboxID: "msg_cancelled_wake",
            }),
            nativeEvent("session.execution.started", 12, { sessionID }),
            ...(position === "settled"
              ? [finished, sdkEvent(selected)]
              : [sdkEvent(selected), finished]),
            nativeEvent("session.renamed", 40, { sessionID, title: "selection window drained" }),
          ];
          const entries: ProviderReplayEntry[] = [];
          for (const entry of raw.entries) {
            if (position === "continuation" && entry.type === "runtime_exit") continue;
            if (entry.type === "expect_outbound" && entry.label === "session.prompt.second") {
              entries.push(
                ...rpc(
                  selection === "agent" ? "session.switchAgent" : "session.switchModel",
                  selection === "agent" ? { sessionID, agent: "build" } : { sessionID, model },
                  null,
                ),
              );
            }
            entries.push(entry);
            if (
              entry.type === "emit_inbound" &&
              entry.label === "session.execution.succeeded.first"
            ) {
              entries.push(...wake);
            }
          }
          if (position === "continuation") {
            const fourth = raw.entries.slice(
              raw.entries.findIndex(
                (entry) =>
                  entry.type === "expect_outbound" && entry.label === "session.prompt.second",
              ),
            );
            entries.push(
              ...(yield* Effect.forEach(fourth, (entry) =>
                decodeReplayEntry(JSON.stringify(entry).replaceAll("second", "fourth")),
              )),
            );
          }
          yield* runTurns(
            entries,
            undefined,
            "default",
            position === "continuation"
              ? ["first", "second", "continuation", "fourth"]
              : ["first", "second"],
            true,
          );
        }),
      );
    }
  }

  it.effect("native Build cannot override the next app Plan turn", () =>
    Effect.gen(function* () {
      const raw = yield* readProviderReplayTranscript(
        new URL("../testkit/fixtures/multi_turn/opencode2_transcript.ndjson", import.meta.url),
      );
      const event: SessionAgentSelected = {
        id: "evt_00000000000000000020",
        created: 1785297600020,
        type: "session.agent.selected",
        durable: { aggregateID: sessionID, seq: 20, version: 1 },
        data: { sessionID, agent: "build", previous: "plan" },
      };
      const entries: ProviderReplayEntry[] = [];
      for (const entry of raw.entries) {
        if (entry.type === "expect_outbound" && entry.label === "session.create") {
          entries.push({
            ...entry,
            frame: {
              type: "session.create",
              input: { model, agent: "plan", location: { directory: "<workspace>" } },
            },
          });
          continue;
        }
        if (entry.type === "emit_inbound" && entry.label === "session.execution.succeeded.first") {
          entries.push({ type: "emit_inbound", frame: { type: "sdk.event", event } });
        }
        if (entry.type === "expect_outbound" && entry.label === "session.prompt.second") {
          entries.push(...rpc("session.switchAgent", { sessionID, agent: "plan" }, null));
        }
        entries.push(entry);
      }
      yield* runTurns(entries, undefined, "plan");
    }),
  );
});

const runTurns = Effect.fnUntraced(function* (
  entries: ProviderReplayEntry[],
  variant?: string,
  interactionMode: "default" | "plan" = "default",
  turns: readonly string[] = ["first", "second"],
  awaitWindow = false,
) {
  const controller = new OpenCode2ReplayController({
    provider: OPENCODE_PROVIDER,
    protocol: "opencode2-sdk.sse",
    version: "0.0.0-beta-18999",
    scenario: "native-selection",
    entries: entries.filter(
      (entry) =>
        !(
          "label" in entry &&
          (entry.label?.startsWith("session.pending.list") || entry.label?.startsWith("shell.list"))
        ),
    ),
  });
  yield* Effect.addFinalizer(() => Effect.sync(() => controller.abort()));
  const client = makeReplayClient(controller);
  const windowDrained = Promise.withResolvers<void>();
  if (awaitWindow) {
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
              event.data.title === "selection window drained"
            ) {
              windowDrained.resolve();
            }
          }
        },
      };
    };
  }
  const modelSelection = {
    instanceId,
    model: "opencode/big-pickle",
    ...(variant === undefined ? {} : { options: [{ id: "variant", value: variant }] }),
  };
  const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
    runtimeMode: "full-access",
    interactionMode,
    cwd: process.cwd(),
  });
  const adapter = makeOpenCodeAdapterV2({
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
  const threadId = ThreadId.make("selection-thread");
  const session = yield* adapter.openSession({
    threadId,
    providerSessionId: ProviderSessionId.make("selection-session"),
    modelSelection,
    runtimePolicy,
  });
  const providerThread = yield* session.ensureThread({ threadId, modelSelection, runtimePolicy });
  const now = yield* DateTime.now;
  const appThread = OrchestrationV2AppThread.make({
    id: threadId,
    projectId: ProjectId.make("selection-project"),
    title: "Selection regression",
    providerInstanceId: instanceId,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode,
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
  for (const [index, word] of turns.entries()) {
    if (awaitWindow && index === 1) yield* Effect.promise(() => windowDrained.promise);
    const terminal = yield* session.events.pipe(
      Stream.filter((event) => event.type === "turn.terminal"),
      Stream.runHead,
      Effect.forkChild({ startImmediately: true }),
    );
    yield* session.startTurn({
      appThread,
      threadId,
      runId: RunId.make(`selection-run-${index}`),
      runOrdinal: index + 1,
      providerTurnOrdinal: index + 1,
      attemptId: RunAttemptId.make(`selection-attempt-${index}`),
      rootNodeId: NodeId.make(`selection-root-${index}`),
      providerThread,
      message: {
        messageId: MessageId.make(`selection-message-${index}`),
        text: `Respond with exactly: ${word} fixture turn complete`,
        attachments: [],
        createdBy: word === "continuation" ? "agent" : "user",
        creationSource: word === "continuation" ? "provider" : "web",
      },
      modelSelection,
      runtimePolicy,
    });
    const result = Option.getOrThrow(yield* Fiber.join(terminal));
    assert.strictEqual(result.type, "turn.terminal");
    if (result.type === "turn.terminal") assert.strictEqual(result.status, "completed");
  }
  controller.assertComplete();
});

function agentSelected(
  agent: string,
  seq: number,
  id = `evt_${seq.toString().padStart(26, "0")}`,
  created = 1785297600020,
): SessionAgentSelected {
  return {
    id,
    created,
    type: "session.agent.selected",
    durable: { aggregateID: sessionID, seq, version: 1 },
    data: { sessionID, agent },
  };
}

function modelSelected(seq: number): SessionModelSelected {
  return {
    id: `evt_${seq.toString().padStart(26, "0")}`,
    created: 1785297600020,
    type: "session.model.selected",
    durable: { aggregateID: sessionID, seq, version: 1 },
    data: { sessionID, model: { id: "glm-5.2", providerID: "opencode" } },
  };
}

function sdkEvent(event: V2Event): ProviderReplayEntry {
  return { type: "emit_inbound", frame: { type: "sdk.event", event } };
}

function nativeEvent<T extends V2Event["type"]>(
  type: T,
  seq: number,
  data: Extract<V2Event, { type: T }>["data"],
): ProviderReplayEntry {
  return {
    type: "emit_inbound",
    frame: {
      type: "sdk.event",
      event: {
        id: `evt_${seq.toString().padStart(26, "0")}`,
        created: 1785297600020 + seq,
        type,
        durable: { aggregateID: sessionID, seq, version: 1 },
        data,
      },
    },
  };
}

function rpc(operation: string, input: unknown, data: unknown): ProviderReplayEntry[] {
  return [
    { type: "expect_outbound", frame: { type: operation, input } },
    { type: "emit_inbound", frame: { type: "sdk.response", operation, data } },
  ];
}
