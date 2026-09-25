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
import {
  NoOpProviderEventLoggers,
  ProviderEventLoggers,
} from "../../provider/Layers/ProviderEventLoggers.ts";
import { OpenCode2Runtime } from "../../provider/opencode2Runtime.ts";
import { IdAllocatorV2, layer as idAllocatorLayer } from "../IdAllocator.ts";
import { ProviderAdapterV2RuntimePolicy } from "../ProviderAdapter.ts";
import {
  make as makeInteractionModeReflections,
  ProviderInteractionModeReflections,
  type ProviderInteractionModeReflection,
} from "../ProviderInteractionModeReflections.ts";
import { readProviderReplayTranscript } from "../testkit/ReplayTranscriptNdjson.ts";
import {
  makeOpenCodeAdapterV2,
  OPENCODE_PROVIDER,
  OpenCodeAdapterV2Driver,
} from "./OpenCodeAdapterV2.ts";
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
  for (const sequence of [39, 40, 41]) {
    it.effect(`reflection requires a sequence above the admission watermark (${sequence})`, () =>
      Effect.gen(function* () {
        const raw = yield* readProviderReplayTranscript(
          new URL("../testkit/fixtures/multi_turn/opencode2_transcript.ndjson", import.meta.url),
        );
        const entries: ProviderReplayEntry[] = [];
        for (const entry of raw.entries) {
          entries.push(entry);
          if (
            entry.type === "emit_inbound" &&
            entry.label === "session.execution.succeeded.first"
          ) {
            entries.push(
              sdkEvent({
                ...agentSelected("build", 100),
                durable: { aggregateID: "ses_other", seq: 100, version: 1 },
              }),
              nativeEvent("session.renamed", 40, { sessionID, title: "high sequence" }),
              nativeEvent("session.renamed", 30, { sessionID, title: "selection window drained" }),
            );
          }
          if (entry.type === "emit_inbound" && entry.label === "session.execution.started.second") {
            entries.push(
              nativeEvent("session.renamed", 80, { sessionID, title: "after admission" }),
              sdkEvent(agentSelected("plan", sequence)),
            );
          }
        }
        const reflected = yield* runTurns(entries, undefined, "default", ["first", "second"], true);
        assert.deepStrictEqual(
          reflected.map((item) => [item.sourceRunId, item.interactionMode]),
          sequence > 40 ? [["selection-run-1", "plan"]] : [],
        );
        for (const reflection of reflected) {
          assert.equal("admissionSequence" in reflection, false);
        }
      }),
    );
  }
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

  for (const position of [
    "active",
    "settled",
    "pending",
    "suppressed",
    "after-suppressed",
    "replayable",
  ] as const) {
    it.effect(
      `reflection authority is separate from cache freshness during ${position} selection`,
      () =>
        Effect.gen(function* () {
          const raw = yield* readProviderReplayTranscript(
            new URL("../testkit/fixtures/multi_turn/opencode2_transcript.ndjson", import.meta.url),
          );
          const entries: ProviderReplayEntry[] = [];
          const selected = sdkEvent(agentSelected("plan", 20));
          // A pending wake is admitted but not executing, so background
          // ownership has not yet withdrawn the settled turn's source.
          const background =
            position === "pending" ||
            position === "suppressed" ||
            position === "after-suppressed" ||
            position === "replayable";
          const wake = [
            nativeEvent("session.inbox.enqueued", 10, {
              sessionID,
              inboxID: "reflection-wake",
              item: {
                type: "synthetic",
                payload: {
                  text:
                    position === "replayable"
                      ? '<subagent state="completed">DONE</subagent>'
                      : '<subagent state="cancelled">CANCELLED</subagent>',
                  description: "result",
                },
                delivery: "queue",
              },
            }),
            ...(position === "pending" ? [selected] : []),
            nativeEvent("session.inbox.delivered", 11, { sessionID, inboxID: "reflection-wake" }),
            nativeEvent("session.execution.started", 12, { sessionID }),
            ...(position === "after-suppressed" || position === "pending" ? [] : [selected]),
            nativeEvent("session.execution.succeeded", 30, { sessionID }),
            ...(position === "after-suppressed" ? [selected] : []),
          ];
          for (const entry of raw.entries) {
            if (
              entry.type === "emit_inbound" &&
              entry.label === "session.execution.succeeded.first" &&
              position === "active"
            )
              entries.push(selected);
            if (entry.type === "expect_outbound" && entry.label === "session.prompt.second")
              entries.push(...rpc("session.switchAgent", { sessionID, agent: "build" }, null));
            entries.push(entry);
            if (
              entry.type === "emit_inbound" &&
              entry.label === "session.execution.succeeded.first"
            ) {
              if (background) entries.push(...wake);
              if (position === "settled") entries.push(selected);
              entries.push(
                nativeEvent("session.renamed", 40, {
                  sessionID,
                  title: "selection window drained",
                }),
              );
            }
          }
          const observed = yield* runTurns(
            entries,
            undefined,
            "default",
            ["first", "second"],
            true,
          );
          assert.equal(observed.length, background ? 0 : 1);
          if (!background) {
            assert.equal(observed[0]!.sourceRunId, "selection-run-0");
            assert.equal(observed[0]!.nativeThreadId, sessionID);
            assert.equal(observed[0]!.expectedRuntimeMode, "full-access");
            assert.equal(observed[0]!.interactionMode, "plan");
          }
        }),
    );
  }

  it.effect("a live selection during a provider-buffered continuation turn is not offered", () =>
    Effect.gen(function* () {
      const raw = yield* readProviderReplayTranscript(
        new URL("../testkit/fixtures/multi_turn/opencode2_transcript.ndjson", import.meta.url),
      );
      // The wake starts while the app is idle, so background ownership withdraws the
      // settled source first. The selection is held until the continuation turn has
      // taken over the still-running wake execution.
      const selected = agentSelected("plan", 20);
      const entries: ProviderReplayEntry[] = [];
      for (const entry of raw.entries) {
        if (entry.type === "expect_outbound" && entry.label === "session.prompt.second")
          entries.push(...rpc("session.switchAgent", { sessionID, agent: "build" }, null));
        entries.push(entry);
        if (entry.type === "emit_inbound" && entry.label === "session.execution.succeeded.first") {
          entries.push(
            nativeEvent("session.inbox.enqueued", 10, {
              sessionID,
              inboxID: "continuation-wake",
              item: {
                type: "synthetic",
                payload: {
                  text: '<subagent state="completed">DONE</subagent>',
                  description: "result",
                },
                delivery: "queue",
              },
            }),
            nativeEvent("session.inbox.delivered", 11, { sessionID, inboxID: "continuation-wake" }),
            nativeEvent("session.execution.started", 12, { sessionID }),
            nativeEvent("session.renamed", 13, { sessionID, title: "selection window drained" }),
            sdkEvent(selected),
            nativeEvent("session.execution.succeeded", 30, { sessionID }),
          );
        }
      }
      const observed = yield* runTurns(
        entries,
        undefined,
        "default",
        ["first", "continuation", "second"],
        true,
        "default",
        "completed",
        false,
        { eventId: selected.id, releaseAfterTurn: "continuation" },
      );
      assert.deepEqual(
        observed.map((request) => [request.sourceRunId, request.interactionMode]),
        [],
      );
    }),
  );

  it.effect(
    "an interrupted source cannot authorize a later selection while its cache still updates",
    () =>
      Effect.gen(function* () {
        const raw = yield* readProviderReplayTranscript(
          new URL("../testkit/fixtures/multi_turn/opencode2_transcript.ndjson", import.meta.url),
        );
        const entries: ProviderReplayEntry[] = [];
        for (const entry of raw.entries) {
          if (
            entry.type === "emit_inbound" &&
            entry.label === "session.execution.succeeded.first"
          ) {
            entries.push(
              nativeEvent("session.execution.interrupted", 19, { sessionID, reason: "user" }),
              sdkEvent(agentSelected("plan", 20)),
              nativeEvent("session.renamed", 40, { sessionID, title: "selection window drained" }),
            );
            continue;
          }
          if (entry.type === "expect_outbound" && entry.label === "session.prompt.second")
            entries.push(...rpc("session.switchAgent", { sessionID, agent: "build" }, null));
          entries.push(entry);
        }
        assert.deepEqual(
          yield* runTurns(
            entries,
            undefined,
            "default",
            ["first", "second"],
            true,
            "default",
            "interrupted",
          ),
          [],
        );
      }),
  );

  it.effect("a restrictive effective Plan policy cannot confer reflection authority", () =>
    Effect.gen(function* () {
      const raw = yield* readProviderReplayTranscript(
        new URL("../testkit/fixtures/multi_turn/opencode2_transcript.ndjson", import.meta.url),
      );
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
        if (entry.type === "emit_inbound" && entry.label === "session.execution.succeeded.first")
          entries.push(sdkEvent(agentSelected("build", 20)));
        if (entry.type === "expect_outbound" && entry.label === "session.prompt.second")
          entries.push(...rpc("session.switchAgent", { sessionID, agent: "plan" }, null));
        entries.push(entry);
      }
      assert.deepEqual(
        yield* runTurns(entries, undefined, "plan", ["first", "second"], false, "default"),
        [],
      );
    }),
  );

  it.effect(
    "matching echoes and later actual starts retain distinct source ownership without model reflection",
    () =>
      Effect.gen(function* () {
        const raw = yield* readProviderReplayTranscript(
          new URL("../testkit/fixtures/multi_turn/opencode2_transcript.ndjson", import.meta.url),
        );
        const entries: ProviderReplayEntry[] = [];
        for (const entry of raw.entries) {
          if (
            entry.type === "emit_inbound" &&
            entry.label === "session.execution.succeeded.first"
          ) {
            entries.push(
              sdkEvent(agentSelected("plan", 20)),
              sdkEvent(agentSelected("build", 21)),
              sdkEvent(agentSelected("build", 21)),
            );
            entries.push(sdkEvent({ ...modelSelected(22), data: { sessionID, model } }));
          }
          if (entry.type === "emit_inbound" && entry.label === "session.execution.succeeded.second")
            entries.push(sdkEvent(agentSelected("build", 23)));
          entries.push(entry);
        }
        const observed = yield* runTurns(entries);
        assert.deepEqual(
          observed.map((request) => [
            request.sourceRunId,
            request.sourceAttemptId,
            request.interactionMode,
            request.dedupeKey,
          ]),
          [
            ["selection-run-0", "selection-attempt-0", "plan", `opencode:${sessionID}:20`],
            ["selection-run-0", "selection-attempt-0", "default", `opencode:${sessionID}:21`],
            ["selection-run-1", "selection-attempt-1", "default", `opencode:${sessionID}:23`],
          ],
        );
      }),
  );

  it.effect("stale, duplicate and foreign durable selections never reach reflection", () =>
    Effect.gen(function* () {
      const raw = yield* readProviderReplayTranscript(
        new URL("../testkit/fixtures/multi_turn/opencode2_transcript.ndjson", import.meta.url),
      );
      const entries: ProviderReplayEntry[] = [];
      for (const entry of raw.entries) {
        if (entry.type === "emit_inbound" && entry.label === "session.execution.succeeded.first") {
          entries.push(
            sdkEvent(agentSelected("plan", 20)),
            // Later wall clock and a greater event id cannot outrank the durable sequence.
            sdkEvent(agentSelected("build", 19, "evt_zzzzzzzzzzzzzzzzzzzzzzzzzz", 1785297609999)),
            sdkEvent(agentSelected("plan", 20)),
            sdkEvent({
              ...agentSelected("build", 40),
              durable: { aggregateID: "ses_unrelated", seq: 40, version: 1 },
            }),
            sdkEvent(agentSelected("build", 21)),
          );
        }
        entries.push(entry);
      }
      const observed = yield* runTurns(entries);
      assert.deepEqual(
        observed.map((request) => [request.interactionMode, request.dedupeKey]),
        [
          ["plan", `opencode:${sessionID}:20`],
          ["default", `opencode:${sessionID}:21`],
        ],
      );
    }),
  );

  it.effect("the registered driver passes its required reflection channel to the adapter", () =>
    Effect.gen(function* () {
      const raw = yield* readProviderReplayTranscript(
        new URL("../testkit/fixtures/multi_turn/opencode2_transcript.ndjson", import.meta.url),
      );
      const entries: ProviderReplayEntry[] = [];
      for (const entry of raw.entries) {
        if (entry.type === "emit_inbound" && entry.label === "session.execution.succeeded.first")
          entries.push(sdkEvent(agentSelected("plan", 20)));
        if (entry.type === "expect_outbound" && entry.label === "session.prompt.second")
          entries.push(...rpc("session.switchAgent", { sessionID, agent: "build" }, null));
        entries.push(entry);
      }
      const observed = yield* runTurns(
        entries,
        undefined,
        "default",
        ["first", "second"],
        false,
        "default",
        "completed",
        true,
      );
      assert.deepEqual(
        observed.map((request) => [request.sourceRunId, request.interactionMode]),
        [["selection-run-0", "plan"]],
      );
    }),
  );

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
  appInteractionMode = interactionMode,
  firstStatus: "completed" | "interrupted" = "completed",
  viaDriver = false,
  gate?: { readonly eventId: string; readonly releaseAfterTurn: string },
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
  const gateReleased = Promise.withResolvers<void>();
  if (gate !== undefined) {
    const subscribe = client.event.subscribe.bind(client.event);
    client.event.subscribe = (options) => {
      const events = subscribe(options);
      return {
        async *[Symbol.asyncIterator]() {
          for await (const event of events) {
            if ("id" in event && event.id === gate.eventId) await gateReleased.promise;
            yield event;
          }
        },
      };
    };
  }
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
  const channel = yield* makeInteractionModeReflections;
  const observed: ProviderInteractionModeReflection[] = [];
  const interactionModeReflections = ProviderInteractionModeReflections.of({
    offer: (request) =>
      channel.offer(request).pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            observed.push(request);
          }),
        ),
      ),
    take: channel.take,
  });
  const settings = yield* decodeSettings({
    serverUrl: "replay://opencode2",
  });
  const runtime = OpenCode2Runtime.of({
    connectToOpenCodeServer: () =>
      Effect.succeed({
        url: "replay://opencode2",
        password: "replay",
        external: true,
        exitCode: null,
      }),
    createOpenCodeSdkClient: () => client,
  });
  const adapter = viaDriver
    ? yield* OpenCodeAdapterV2Driver.create({
        instanceId,
        displayName: undefined,
        environment: [],
        enabled: true,
        config: settings,
      }).pipe(
        Effect.provideService(OpenCode2Runtime, runtime),
        Effect.provideService(ProviderEventLoggers, NoOpProviderEventLoggers),
        Effect.provideService(ProviderInteractionModeReflections, interactionModeReflections),
      )
    : makeOpenCodeAdapterV2({
        interactionModeReflections,
        instanceId,
        settings,
        environment: {},
        runtime,
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
    interactionMode: appInteractionMode,
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
    if (word === gate?.releaseAfterTurn) gateReleased.resolve();
    const result = Option.getOrThrow(yield* Fiber.join(terminal));
    assert.strictEqual(result.type, "turn.terminal");
    if (result.type === "turn.terminal")
      assert.strictEqual(result.status, index === 0 ? firstStatus : "completed");
  }
  controller.assertComplete();
  return observed;
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
