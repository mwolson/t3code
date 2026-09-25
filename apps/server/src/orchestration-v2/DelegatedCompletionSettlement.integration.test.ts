import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunId,
  ThreadId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import { OrchestrationEffectWorkerV2 } from "./EffectWorker.ts";
import { EventSinkV2 } from "./EventSink.ts";
import { layer as idAllocatorLayer } from "./IdAllocator.ts";
import { OrchestratorV2 } from "./Orchestrator.ts";
import { ProjectionStoreV2 } from "./ProjectionStore.ts";
import {
  ProviderAdapterSteerRunError,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2Shape,
  type ProviderAdapterV2TurnInput,
} from "./ProviderAdapter.ts";
import { makeSingleLayer } from "./ProviderAdapterRegistry.ts";
import {
  ProviderContinuationRequests,
  type ProviderContinuationRequest,
} from "./ProviderContinuationRequests.ts";
import { workerLive as continuationWorker } from "./ProviderContinuationService.ts";
import { ThreadManagementService } from "./ThreadManagementService.ts";
import { makeOrchestratorV2ReplayLayerWithRegistry } from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "./testkit/ReplayFixtureWorkspace.ts";

// A provider transcript cannot reject steering while the orchestrator still
// projects the turn as running. These cases keep the real orchestrator, SQLite
// projection, outbox retries and continuation worker, and control only that
// adapter boundary. Retry time is virtual; ordering uses event barriers.

const driver = ProviderDriverKind.make("codex");
const instanceId = ProviderInstanceId.make("codex");
const runningSelection = { instanceId, model: "running-model" };
const desiredSelection = {
  instanceId,
  model: "next-turn-model",
  options: [{ id: "effort", value: "high" }],
};
const desiredSelectionMessageId = MessageId.make("message:desired-selection");
const threadId = ThreadId.make("thread:completion-settlement");
const markerThreadId = ThreadId.make("thread:completion-settlement-marker");
const markerRunId = RunId.make("run:completion-settlement-marker");

interface AdapterControls {
  readonly events: Queue.Queue<ProviderAdapterV2Event>;
  readonly started: Array<ProviderAdapterV2TurnInput>;
  readonly steered: Array<MessageId>;
  steer: "reject" | "accept";
  sessionOpens: number;
  interrupts: number;
}

it.effect("recovers a completion whose steering retries exhausted before the parent settled", () =>
  runScenario("exhausted-before-terminal", (h) =>
    Effect.gen(function* () {
      const parent = yield* h.startRun("message:parent");
      yield* h.saveDesiredSelection(parent.runId);
      const { messageId, taskId } = yield* h.reserveSteer(parent.runId);
      yield* h.exhaustSteering();
      assert.deepEqual(h.controls.steered, [messageId, messageId, messageId, messageId, messageId]);
      const stalled = yield* h.projection();
      assert.equal(stalled.runs.length, 1);
      assert.equal(h.task(stalled, taskId)?.completionDelivery?.state, "claimed");
      const siblingIds = yield* h.addPendingSiblings(parent.runId, 3);
      const userMessages = stalled.messages.filter((message) => message.createdBy === "user");

      yield* h.finishTurn(parent, "completed");
      yield* h.settled();
      assert.deepEqual(
        h.offers.map((offer) => offer.delegatedCompletion),
        [{ parentRunId: parent.runId, generation: 1, messageId }],
        "the terminal boundary reoffers the exhausted reservation",
      );

      yield* h.startContinuationWorker();
      const wake = yield* h.awaitDispatchedTurn(2);
      assert.equal(wake.message.messageId, messageId);
      assert.include(wake.message.text, String(taskId));
      for (const siblingId of siblingIds) assert.notInclude(wake.message.text, String(siblingId));
      assert.deepEqual(wake.modelSelection, desiredSelection);
      const recovered = yield* h.projection();
      assert.deepEqual(recovered.thread.modelSelection, desiredSelection);
      assert.deepEqual(h.run(recovered, parent.runId)?.modelSelection, runningSelection);
      assert.equal(recovered.messages.filter((message) => message.id === messageId).length, 1);
      assert.equal(
        recovered.messages.find((message) => message.id === messageId)?.runId,
        wake.runId,
      );
      assert.equal(recovered.turnItems.filter((item) => item.type === "notification").length, 1);

      // A duplicate terminal event and a duplicate request reuse the same identity.
      yield* h.repeatRunEvent(parent.runId);
      yield* h.settled();
      assert.equal(h.offers.length, 1);
      yield* Queue.offer(h.requests, h.offers[0]!);

      yield* h.finishTurn(wake, "completed");
      const siblingsWake = yield* h.awaitDispatchedTurn(3);
      assert.equal(h.controls.started.length, 3);
      assert.equal(h.offers[1]?.delegatedCompletion?.generation, 2);
      assert.notEqual(siblingsWake.message.messageId, messageId);
      for (const siblingId of siblingIds)
        assert.include(siblingsWake.message.text, String(siblingId));
      assert.notInclude(siblingsWake.message.text, String(taskId));
      assert.deepEqual(siblingsWake.modelSelection, desiredSelection);

      yield* h.finishTurn(siblingsWake, "completed");
      yield* h.settled();
      const delivered = yield* h.projection();
      const cohort = h.run(delivered, parent.runId)?.delegatedCompletion;
      assert.equal(cohort?.delivery, null);
      assert.equal(cohort?.settledDeliveryCount, 2);
      assert.deepEqual(
        delivered.subagents.map((task) => task.completionDelivery?.state),
        ["delivered", "delivered", "delivered", "delivered"],
      );
      assert.deepEqual(
        delivered.messages.filter((message) => message.createdBy === "user"),
        userMessages,
        "recovery needs no new user input",
      );
      assert.deepEqual(delivered.thread.modelSelection, desiredSelection);
      assert.equal(h.controls.sessionOpens, 1);
      assert.equal(h.controls.interrupts, 0);

      // Late or repeated acknowledgments of the recovered delivery change nothing.
      yield* h.orchestrator.dispatch({
        type: "notification.delivery.accept",
        commandId: CommandId.make("late-acceptance"),
        threadId,
        messageId,
      });
      const afterLateAcceptance = yield* h.projection();
      assert.deepEqual(afterLateAcceptance.subagents, delivered.subagents);
      assert.deepEqual(afterLateAcceptance.runs, delivered.runs);
      for (const task of delivered.subagents) {
        yield* h.orchestrator.dispatch({
          type: "delegated_task.completion-delivery.acknowledge",
          commandId: CommandId.make(`read-result:${task.id}`),
          parentThreadId: threadId,
          taskId: task.id,
          observedByRunId: siblingsWake.runId,
        });
      }
      yield* h.repeatRunEvent(parent.runId);
      yield* h.settled();
      assert.equal(h.offers.length, 2);
      assert.equal(h.controls.started.length, 3);
      assert.isTrue(
        (yield* h.projection()).subagents.every(
          (task) => task.completionDelivery?.state === "acknowledged",
        ),
      );
    }),
  ),
);

it.effect("recovers when settlement is visible before retries exhaust", () =>
  runScenario("terminal-before-exhaustion", (h) =>
    Effect.gen(function* () {
      const parent = yield* h.startRun("message:parent");
      const { messageId } = yield* h.reserveSteer(parent.runId);
      yield* h.worker.drain();
      assert.deepEqual(h.controls.steered, [messageId]);
      assert.isTrue(Option.isSome(yield* h.worker.nextClaimableAt));

      // The provider ends the turn without a T3 Stop, so the cohort stays open.
      yield* h.finishTurn(parent, "interrupted");
      yield* h.settled();
      assert.deepEqual(
        h.offers.map((offer) => offer.delegatedCompletion),
        [{ parentRunId: parent.runId, generation: 1, messageId }],
      );
      yield* h.exhaustSteering();
      assert.deepEqual(h.controls.steered, [messageId], "later retries never reach the provider");

      yield* h.startContinuationWorker();
      const wake = yield* h.awaitDispatchedTurn(2);
      assert.equal(wake.message.messageId, messageId);
      const recovered = yield* h.projection();
      assert.equal(recovered.messages.filter((message) => message.id === messageId).length, 1);
      assert.equal(recovered.turnItems.filter((item) => item.type === "notification").length, 1);
      assert.equal(h.controls.started.length, 2);
    }),
  ),
);

it.effect("recovers the reservation of the earlier cohort that owned the steer", () =>
  runScenario("earlier-cohort", (h) =>
    Effect.gen(function* () {
      const earlier = yield* h.startRun("message:earlier");
      yield* h.finishTurn(earlier, "completed");
      yield* h.settled();
      const parent = yield* h.startRun("message:parent");
      const { messageId, taskId } = yield* h.reserveSteer(earlier.runId);
      yield* h.exhaustSteering();
      const stalled = yield* h.projection();
      assert.equal(
        stalled.messages.find((message) => message.id === messageId)?.runId,
        parent.runId,
      );

      yield* h.finishTurn(parent, "completed");
      yield* h.settled();
      assert.deepEqual(
        h.offers.map((offer) => offer.delegatedCompletion),
        [{ parentRunId: earlier.runId, generation: 1, messageId }],
      );

      yield* h.startContinuationWorker();
      const wake = yield* h.awaitDispatchedTurn(3);
      assert.equal(wake.message.messageId, messageId);
      yield* h.finishTurn(wake, "completed");
      yield* h.settled();
      const delivered = yield* h.projection();
      assert.equal(h.task(delivered, taskId)?.completionDelivery?.state, "delivered");
      assert.equal(h.run(delivered, earlier.runId)?.delegatedCompletion?.delivery, null);
      assert.isUndefined(h.run(delivered, parent.runId)?.delegatedCompletion);
    }),
  ),
);

it.effect("does not reoffer a steer the provider accepted before the parent settled", () =>
  runScenario("accepted-before-terminal", (h) =>
    Effect.gen(function* () {
      const parent = yield* h.startRun("message:parent");
      const { messageId, taskId } = yield* h.reserveSteer(parent.runId);
      const siblingIds = yield* h.addPendingSiblings(parent.runId, 1);
      h.controls.steer = "accept";
      yield* h.worker.drain();
      assert.deepEqual(h.controls.steered, [messageId]);
      const accepted = yield* h.projection();
      assert.equal(h.task(accepted, taskId)?.completionDelivery?.state, "delivered");
      const successor = h.run(accepted, parent.runId)?.delegatedCompletion?.delivery;
      assert.deepEqual(successor?.taskIds, siblingIds);
      assert.notEqual(successor?.messageId, messageId);
      const offersBeforeTerminal = h.offers.length;

      yield* h.finishTurn(parent, "completed");
      assert.isAtLeast(yield* h.settled(), 1, "the terminal handler ran");
      assert.equal(
        h.offers.length,
        offersBeforeTerminal,
        "an accepted steer does not reoffer the successor reservation",
      );
      const settled = yield* h.projection();
      assert.equal(h.task(settled, taskId)?.completionDelivery?.state, "delivered");
      assert.deepEqual(h.run(settled, parent.runId)?.delegatedCompletion?.delivery, successor);
    }),
  ),
);

it.effect("collapses duplicate and stale recovery offers into one wake", () =>
  runScenario("duplicate-stale-offers", (h) =>
    Effect.gen(function* () {
      const parent = yield* h.startRun("message:parent");
      const { messageId, taskId } = yield* h.reserveSteer(parent.runId);
      yield* h.exhaustSteering();
      yield* h.finishTurn(parent, "completed");
      yield* h.settled();
      yield* h.repeatRunEvent(parent.runId);
      yield* h.settled();
      const recovery = { parentRunId: parent.runId, generation: 1, messageId };
      assert.deepEqual(
        h.offers.map((offer) => offer.delegatedCompletion),
        [recovery, recovery],
        "each terminal event reoffers the same reservation identity",
      );

      // The worker handles requests serially, so a third read proves the
      // duplicate before it was handled.
      yield* h.startContinuationWorker();
      const wake = yield* h.awaitDispatchedTurn(2);
      yield* Queue.offer(h.requests, h.offers[0]!);
      yield* h.awaitContinuationReads(3);
      assert.equal(h.dispatches.length, 1);
      assert.equal(wake.message.messageId, messageId);
      const recovered = yield* h.projection();
      assert.equal(recovered.messages.filter((message) => message.id === messageId).length, 1);
      assert.equal(recovered.turnItems.filter((item) => item.type === "notification").length, 1);

      yield* h.finishTurn(wake, "completed");
      yield* h.settled();
      const delivered = yield* h.projection();
      assert.equal(h.task(delivered, taskId)?.completionDelivery?.state, "delivered");
      assert.equal(h.run(delivered, parent.runId)?.delegatedCompletion?.delivery, null);
      yield* Queue.offer(h.requests, h.offers[0]!);
      yield* Queue.offer(h.requests, h.offers[0]!);
      yield* h.awaitContinuationReads(2);
      assert.equal(h.dispatches.length, 1, "a settled generation is never dispatched again");
      assert.equal(h.controls.started.length, 2);
    }),
  ),
);

it.effect("does not revive an exhausted steer when its parent is rolled back", () =>
  runScenario("rollback", (h) =>
    Effect.gen(function* () {
      const parent = yield* h.startRun("message:parent");
      const { messageId, taskId } = yield* h.reserveSteer(parent.runId);
      yield* h.exhaustSteering();
      const queuedMessageId = MessageId.make("message:queued-user-work");
      yield* h.orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("queued-user-work"),
        threadId,
        messageId: queuedMessageId,
        text: "explicit queued user work",
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "user",
        creationSource: "web",
      });
      const before = yield* h.projection();
      const parentRun = h.run(before, parent.runId)!;
      yield* h.rollBack(parent.runId);
      assert.isAtLeast(yield* h.settled(), 1, "the terminal handler ran");
      assert.deepEqual(h.offers, []);
      const rolledBack = yield* h.projection();
      assert.equal(h.run(rolledBack, parent.runId)?.status, "rolled_back");
      assert.deepEqual(
        h.run(rolledBack, parent.runId)?.delegatedCompletion,
        parentRun.delegatedCompletion,
      );
      assert.equal(h.task(rolledBack, taskId)?.completionDelivery?.state, "claimed");
      assert.equal(
        rolledBack.messages.find((message) => message.id === messageId)?.runId,
        parent.runId,
      );
      assert.equal(
        rolledBack.runs.find((run) => run.userMessageId === queuedMessageId)?.status,
        "starting",
        "ordinary queue promotion still follows the terminal boundary",
      );
    }),
  ),
);

it.effect("drops a queued recovery offer whose parent is rolled back before dispatch", () =>
  runScenario("rollback-after-offer", (h) =>
    Effect.gen(function* () {
      const parent = yield* h.startRun("message:parent");
      const { messageId, taskId } = yield* h.reserveSteer(parent.runId);
      yield* h.exhaustSteering();
      yield* h.finishTurn(parent, "completed");
      yield* h.settled();
      assert.deepEqual(
        h.offers.map((offer) => offer.delegatedCompletion),
        [{ parentRunId: parent.runId, generation: 1, messageId }],
      );
      const completed = yield* h.projection();

      yield* h.rollBack(parent.runId);
      assert.isAtLeast(yield* h.settled(), 1, "the terminal handler ran");
      assert.equal(h.offers.length, 1, "the rollback event does not offer again");
      assert.equal(
        yield* h.dispatchStaleCompletion(parent.runId, messageId, [taskId]),
        "Delegated completion delivery is no longer dispatchable.",
      );

      // The worker handles requests serially, so reading a repeated request
      // proves the queued offer was handled.
      yield* h.startContinuationWorker();
      yield* Queue.offer(h.requests, h.offers[0]!);
      yield* h.awaitContinuationReads(2);
      assert.deepEqual(h.dispatchAttempts, [], "the queued offer is dropped, not dispatched");
      assert.equal(h.offers.length, 1, "the dropped offer is not retried");
      assert.equal(h.controls.started.length, 1);
      const rolledBack = yield* h.projection();
      assert.deepEqual(
        rolledBack.runs.map((run) => run.id),
        completed.runs.map((run) => run.id),
      );
      assert.equal(h.run(rolledBack, parent.runId)?.status, "rolled_back");
      assert.deepEqual(
        h.run(rolledBack, parent.runId)?.delegatedCompletion,
        h.run(completed, parent.runId)?.delegatedCompletion,
      );
      assert.equal(h.task(rolledBack, taskId)?.completionDelivery?.state, "claimed");
      assert.equal(
        rolledBack.messages.find((message) => message.id === messageId)?.runId,
        parent.runId,
      );

      // Reading the result later does not release the discarded cohort to a sibling.
      const [siblingId] = yield* h.addPendingSiblings(parent.runId, 1);
      yield* h.orchestrator.dispatch({
        type: "delegated_task.completion-delivery.acknowledge",
        commandId: CommandId.make("read-after-rollback"),
        parentThreadId: threadId,
        taskId,
        observedByRunId: parent.runId,
      });
      const read = yield* h.projection();
      assert.deepEqual(h.run(read, parent.runId)?.delegatedCompletion?.delivery, {
        generation: 1,
        messageId,
        taskIds: [],
      });
      assert.equal(h.task(read, siblingId!)?.completionDelivery?.state, "pending");
      assert.equal(h.offers.length, 1);
      assert.equal(h.controls.started.length, 1);
    }),
  ),
);

it.effect("drops a queued recovery offer whose result was read before dispatch", () =>
  runScenario("acknowledged-after-offer", (h) =>
    Effect.gen(function* () {
      const parent = yield* h.startRun("message:parent");
      const { messageId, taskId } = yield* h.reserveSteer(parent.runId);
      yield* h.exhaustSteering();
      yield* h.finishTurn(parent, "completed");
      yield* h.settled();
      assert.deepEqual(
        h.offers.map((offer) => offer.delegatedCompletion),
        [{ parentRunId: parent.runId, generation: 1, messageId }],
      );

      yield* h.orchestrator.dispatch({
        type: "delegated_task.completion-delivery.acknowledge",
        commandId: CommandId.make("read-after-offer"),
        parentThreadId: threadId,
        taskId,
        observedByRunId: parent.runId,
      });
      const acknowledged = yield* h.projection();
      assert.equal(
        h.run(acknowledged, parent.runId)?.delegatedCompletion?.delivery,
        null,
        "a settled steer with no unread result releases its reservation",
      );
      assert.equal(h.offers.length, 1, "with no pending sibling nothing new is offered");
      assert.equal(
        yield* h.dispatchStaleCompletion(parent.runId, messageId, [taskId]),
        "Delegated completion delivery is no longer dispatchable.",
      );

      yield* h.startContinuationWorker();
      yield* Queue.offer(h.requests, h.offers[0]!);
      yield* h.awaitContinuationReads(2);
      assert.deepEqual(h.dispatchAttempts, [], "an emptied reservation starts no wake");
      assert.equal(h.offers.length, 1, "the dropped offer is not retried");
      assert.equal(h.controls.started.length, 1);
      const read = yield* h.projection();
      assert.deepEqual(
        read.runs.map((run) => run.id),
        acknowledged.runs.map((run) => run.id),
      );
      assert.deepEqual(
        read.messages.find((message) => message.id === messageId),
        acknowledged.messages.find((message) => message.id === messageId),
      );
      assert.equal(h.task(read, taskId)?.completionDelivery?.state, "acknowledged");
    }),
  ),
);

it.effect("delivers a later sibling once after the settled steer's result was read", () =>
  runScenario("read-then-later-sibling", (h) =>
    Effect.gen(function* () {
      const parent = yield* h.startRun("message:parent");
      const sibling = yield* h.requestChild(parent);
      const { messageId, taskId } = yield* h.reserveSteer(parent.runId);
      yield* h.exhaustSteering();
      yield* h.finishTurn(parent, "completed");
      yield* h.settled();
      assert.deepEqual(
        h.offers.map((offer) => offer.delegatedCompletion),
        [{ parentRunId: parent.runId, generation: 1, messageId }],
      );

      yield* h.orchestrator.dispatch({
        type: "delegated_task.completion-delivery.acknowledge",
        commandId: CommandId.make("read-settled-steer"),
        parentThreadId: threadId,
        taskId,
        observedByRunId: parent.runId,
      });
      assert.equal(h.run(yield* h.projection(), parent.runId)?.delegatedCompletion?.delivery, null);
      assert.equal(h.offers.length, 1);

      yield* h.finishTurn(sibling.turn, "completed");
      yield* h.settled();
      const reserved = yield* h.projection();
      const delivery = h.run(reserved, parent.runId)?.delegatedCompletion?.delivery;
      assert.equal(delivery?.generation, 2);
      assert.deepEqual(delivery?.taskIds, [sibling.taskId]);
      assert.equal(h.task(reserved, sibling.taskId)?.completionDelivery?.state, "claimed");

      // The stale recovery offer is read first and dropped; only the sibling wakes.
      yield* h.startContinuationWorker();
      const wake = yield* h.awaitDispatchedTurn(3);
      assert.equal(h.dispatches.length, 1);
      assert.equal(wake.message.messageId, delivery?.messageId);
      assert.include(wake.message.text, String(sibling.taskId));
      assert.notInclude(wake.message.text, String(taskId));
      yield* h.finishTurn(wake, "completed");
      yield* h.settled();
      yield* h.repeatRunEvent(parent.runId);
      yield* h.settled();

      const delivered = yield* h.projection();
      assert.equal(h.task(delivered, sibling.taskId)?.completionDelivery?.state, "delivered");
      assert.equal(h.task(delivered, taskId)?.completionDelivery?.state, "acknowledged");
      assert.equal(h.run(delivered, parent.runId)?.delegatedCompletion?.delivery, null);
      assert.equal(h.run(delivered, parent.runId)?.delegatedCompletion?.settledDeliveryCount, 1);
      assert.equal(h.offers.length, 2);
      assert.equal(h.dispatches.length, 1);
      assert.equal(h.controls.started.length, 3, "one wake, and no empty wake");
    }),
  ),
);

for (const release of ["acknowledge", "dispose"] as const) {
  it.effect(
    `reserves waiting siblings when the settled steer's result is ${release === "acknowledge" ? "read" : "disposed"}`,
    () =>
      runScenario(`release-settled-${release}`, (h) =>
        Effect.gen(function* () {
          const parent = yield* h.startRun("message:parent");
          const { messageId, taskId } = yield* h.reserveSteer(parent.runId);
          const siblingIds = yield* h.addPendingSiblings(parent.runId, 2);
          yield* h.exhaustSteering();
          yield* h.finishTurn(parent, "completed");
          yield* h.settled();

          yield* h.orchestrator.dispatch(
            release === "acknowledge"
              ? {
                  type: "delegated_task.completion-delivery.acknowledge",
                  commandId: CommandId.make("release-settled"),
                  parentThreadId: threadId,
                  taskId,
                  observedByRunId: parent.runId,
                }
              : {
                  type: "delegated_task.completion-delivery.dispose",
                  commandId: CommandId.make("release-settled"),
                  parentThreadId: threadId,
                  taskId,
                },
          );
          const released = yield* h.projection();
          const delivery = h.run(released, parent.runId)?.delegatedCompletion?.delivery;
          assert.equal(delivery?.generation, 2);
          assert.notEqual(delivery?.messageId, messageId);
          assert.deepEqual(delivery?.taskIds, siblingIds);
          for (const siblingId of siblingIds)
            assert.equal(h.task(released, siblingId)?.completionDelivery?.state, "claimed");
          assert.deepEqual(
            h.offers.map((offer) => offer.delegatedCompletion),
            [
              { parentRunId: parent.runId, generation: 1, messageId },
              { parentRunId: parent.runId, generation: 2, messageId: delivery!.messageId },
            ],
          );

          yield* h.startContinuationWorker();
          const wake = yield* h.awaitDispatchedTurn(2);
          assert.equal(wake.message.messageId, delivery?.messageId);
          for (const siblingId of siblingIds) assert.include(wake.message.text, String(siblingId));
          assert.notInclude(wake.message.text, String(taskId));
          yield* h.finishTurn(wake, "completed");
          yield* h.settled();
          const delivered = yield* h.projection();
          for (const siblingId of siblingIds)
            assert.equal(h.task(delivered, siblingId)?.completionDelivery?.state, "delivered");
          assert.equal(h.run(delivered, parent.runId)?.delegatedCompletion?.delivery, null);
          assert.equal(h.dispatches.length, 1);
          assert.equal(h.controls.started.length, 2);
        }),
      ),
  );
}

it.effect("leaves a completed wake whose result was read to the terminal listener", () =>
  runScenario("read-completed-wake", (h) =>
    Effect.gen(function* () {
      const parent = yield* h.startRun("message:parent");
      const { messageId, taskId } = yield* h.reserveSteer(parent.runId);
      yield* h.exhaustSteering();
      yield* h.finishTurn(parent, "completed");
      yield* h.settled();
      yield* h.startContinuationWorker();
      const wake = yield* h.awaitDispatchedTurn(2);
      const [siblingId] = yield* h.addPendingSiblings(parent.runId, 1);

      const listener = yield* h.holdTerminalListener();
      yield* h.finishTurn(wake, "completed");
      yield* h.orchestrator.dispatch({
        type: "delegated_task.completion-delivery.acknowledge",
        commandId: CommandId.make("read-completed-wake"),
        parentThreadId: threadId,
        taskId,
        observedByRunId: wake.runId,
      });
      const read = yield* h.projection();
      assert.deepEqual(
        h.run(read, parent.runId)?.delegatedCompletion?.delivery,
        { generation: 1, messageId, taskIds: [] },
        "the completed wake is still settled by the terminal listener",
      );
      assert.equal(h.task(read, siblingId!)?.completionDelivery?.state, "pending");

      yield* Deferred.succeed(listener, undefined);
      yield* h.settled();
      const siblingsWake = yield* h.awaitDispatchedTurn(3);
      assert.include(siblingsWake.message.text, String(siblingId));
      assert.notInclude(siblingsWake.message.text, String(taskId));
      const cohort = h.run(yield* h.projection(), parent.runId)?.delegatedCompletion;
      assert.equal(cohort?.settledDeliveryCount, 1);
      assert.equal(cohort?.delivery?.generation, 2);
      assert.deepEqual(cohort?.delivery?.taskIds, [siblingId!]);
    }),
  ),
);

it.effect("keeps an emptied reservation while its steer run is still live", () =>
  runScenario("read-while-live", (h) =>
    Effect.gen(function* () {
      const parent = yield* h.startRun("message:parent");
      const { messageId, taskId } = yield* h.reserveSteer(parent.runId);
      const [siblingId] = yield* h.addPendingSiblings(parent.runId, 1);
      yield* h.exhaustSteering();
      yield* h.completeProviderTurn(parent);
      const current = yield* h.projection();
      assert.equal(h.run(current, parent.runId)?.status, "running");
      const providerThreadId = h.run(current, parent.runId)?.providerThreadId;
      if (providerThreadId == null) return yield* Effect.die("parent provider thread missing");
      // A startup or wake-policy offer can see the unaccepted steer in this window.
      yield* Queue.offer(h.requests, {
        threadId,
        providerThreadId,
        driver,
        detail: null,
        delivery: "message_text",
        delegatedCompletion: { parentRunId: parent.runId, generation: 1, messageId },
      });

      yield* h.orchestrator.dispatch({
        type: "delegated_task.completion-delivery.acknowledge",
        commandId: CommandId.make("read-while-live"),
        parentThreadId: threadId,
        taskId,
        observedByRunId: parent.runId,
      });
      const read = yield* h.projection();
      assert.deepEqual(h.run(read, parent.runId)?.delegatedCompletion?.delivery, {
        generation: 1,
        messageId,
        taskIds: [],
      });
      assert.equal(h.task(read, siblingId!)?.completionDelivery?.state, "pending");
      assert.deepEqual(h.offers, []);
      assert.equal(
        yield* h.dispatchStaleCompletion(parent.runId, messageId, [taskId]),
        "Delegated completion delivery is no longer dispatchable.",
      );

      yield* h.startContinuationWorker();
      yield* h.awaitContinuationReads(1);
      assert.deepEqual(h.dispatchAttempts, [], "an emptied reservation starts no wake");
      assert.equal(h.controls.started.length, 1);
    }),
  ),
);

it.effect("does not reoffer a steer after Stop disposes its cohort", () =>
  runScenario("stop", (h) =>
    Effect.gen(function* () {
      const parent = yield* h.startRun("message:parent");
      const { taskId } = yield* h.reserveSteer(parent.runId);
      yield* h.exhaustSteering();
      yield* h.orchestrator.dispatch({
        type: "run.interrupt",
        commandId: CommandId.make("stop"),
        threadId,
        runId: parent.runId,
      });
      yield* h.worker.drain();
      yield* h.finishTurn(parent, "interrupted");
      assert.isAtLeast(yield* h.settled(), 1, "the terminal handler ran");
      assert.deepEqual(h.offers, []);
      const stopped = yield* h.projection();
      assert.equal(h.run(stopped, parent.runId)?.delegatedCompletion?.disposition, "stopped");
      assert.equal(h.task(stopped, taskId)?.completionDelivery?.state, "disposed");
    }),
  ),
);

for (const release of ["acknowledge", "dispose"] as const) {
  it.effect(
    `does not reoffer a reservation whose result was ${release === "acknowledge" ? "read" : "disposed"}`,
    () =>
      runScenario(`released-${release}`, (h) =>
        Effect.gen(function* () {
          const parent = yield* h.startRun("message:parent");
          const { taskId } = yield* h.reserveSteer(parent.runId);
          yield* h.exhaustSteering();
          yield* h.orchestrator.dispatch(
            release === "acknowledge"
              ? {
                  type: "delegated_task.completion-delivery.acknowledge",
                  commandId: CommandId.make("release-result"),
                  parentThreadId: threadId,
                  taskId,
                  observedByRunId: parent.runId,
                }
              : {
                  type: "delegated_task.completion-delivery.dispose",
                  commandId: CommandId.make("release-result"),
                  parentThreadId: threadId,
                  taskId,
                },
          );
          const released = yield* h.projection();
          assert.deepEqual(
            h.run(released, parent.runId)?.delegatedCompletion?.delivery?.taskIds,
            [],
          );

          yield* h.finishTurn(parent, "completed");
          assert.isAtLeast(yield* h.settled(), 1, "the terminal handler ran");
          assert.equal(
            h.run(yield* h.projection(), parent.runId)?.delegatedCompletion?.delivery,
            null,
            "settlement releases a steer with no unread result",
          );
          assert.deepEqual(h.offers, []);
          assert.equal(h.controls.started.length, 1);
        }),
      ),
  );
}

for (const release of ["acknowledge", "dispose"] as const) {
  it.effect(
    `delivers a waiting sibling once after a steer ${release === "acknowledge" ? "read" : "disposed"} while live settles`,
    () =>
      runScenario(`live-${release}-then-settle`, (h) =>
        Effect.gen(function* () {
          const parent = yield* h.startRun("message:parent");
          const { messageId, taskId } = yield* h.reserveSteer(parent.runId);
          const [siblingId] = yield* h.addPendingSiblings(parent.runId, 1);
          yield* h.exhaustSteering();
          yield* h.resolveResult(release, taskId, parent.runId);
          const userMessages = (yield* h.projection()).messages.filter(
            (message) => message.createdBy === "user",
          );
          assert.deepEqual(
            h.run(yield* h.projection(), parent.runId)?.delegatedCompletion?.delivery,
            { generation: 1, messageId, taskIds: [] },
            "the live steer keeps its emptied reservation",
          );

          yield* h.finishTurn(parent, "completed");
          yield* h.settled();
          const released = yield* h.projection();
          const delivery = h.run(released, parent.runId)?.delegatedCompletion?.delivery;
          assert.equal(delivery?.generation, 2);
          assert.notEqual(delivery?.messageId, messageId);
          assert.deepEqual(delivery?.taskIds, [siblingId!]);
          assert.equal(h.task(released, siblingId!)?.completionDelivery?.state, "claimed");
          assert.deepEqual(
            h.offers.map((offer) => offer.delegatedCompletion),
            [{ parentRunId: parent.runId, generation: 2, messageId: delivery!.messageId }],
          );

          yield* h.startContinuationWorker();
          const wake = yield* h.awaitDispatchedTurn(2);
          assert.equal(wake.message.messageId, delivery?.messageId);
          assert.include(wake.message.text, String(siblingId));
          assert.notInclude(wake.message.text, String(taskId));
          yield* h.finishTurn(wake, "completed");
          yield* h.settled();
          const delivered = yield* h.projection();
          assert.equal(h.task(delivered, siblingId!)?.completionDelivery?.state, "delivered");
          assert.equal(
            h.task(delivered, taskId)?.completionDelivery?.state,
            release === "acknowledge" ? "acknowledged" : "disposed",
          );
          const cohort = h.run(delivered, parent.runId)?.delegatedCompletion;
          assert.equal(cohort?.delivery, null);
          assert.equal(cohort?.settledDeliveryCount, 1);

          // A repeated settlement leaves the delivered cohort alone.
          yield* h.repeatRunEvent(parent.runId);
          yield* h.settled();
          const repeated = yield* h.projection();
          assert.deepEqual(h.run(repeated, parent.runId)?.delegatedCompletion, cohort);
          assert.deepEqual(repeated.subagents, delivered.subagents);
          assert.equal(h.offers.length, 1);
          assert.equal(h.dispatches.length, 1);
          assert.equal(h.controls.started.length, 2, "one wake, and no empty wake");
          assert.deepEqual(
            repeated.messages.filter((message) => message.createdBy === "user"),
            userMessages,
            "recovery needs no new user input",
          );
        }),
      ),
  );
}

it.effect("keeps a read steer reserved when a rollback lands before its settlement", () =>
  runScenario("live-read-then-rollback", (h) =>
    Effect.gen(function* () {
      const parent = yield* h.startRun("message:parent");
      const { messageId, taskId } = yield* h.reserveSteer(parent.runId);
      const [siblingId] = yield* h.addPendingSiblings(parent.runId, 1);
      yield* h.exhaustSteering();
      yield* h.resolveResult("acknowledge", taskId, parent.runId);

      // The completed event is handled after the rollback is already visible.
      const listener = yield* h.holdTerminalListener();
      yield* h.finishTurn(parent, "completed");
      yield* h.rollBack(parent.runId);
      yield* Deferred.succeed(listener, undefined);
      assert.isAtLeast(yield* h.settled(), 2, "both terminal events were handled");
      const rolledBack = yield* h.projection();
      assert.equal(h.run(rolledBack, parent.runId)?.status, "rolled_back");
      assert.deepEqual(h.run(rolledBack, parent.runId)?.delegatedCompletion?.delivery, {
        generation: 1,
        messageId,
        taskIds: [],
      });
      assert.equal(h.task(rolledBack, siblingId!)?.completionDelivery?.state, "pending");
      assert.deepEqual(h.offers, []);
      assert.equal(h.controls.started.length, 1);
    }),
  ),
);

it.effect("serializes the settlement release with a concurrent sibling read", () =>
  runScenario("release-under-parent-lock", (h) =>
    Effect.gen(function* () {
      const parent = yield* h.startRun("message:parent");
      const { taskId } = yield* h.reserveSteer(parent.runId);
      const [siblingId] = yield* h.addPendingSiblings(parent.runId, 1);
      yield* h.exhaustSteering();
      yield* h.resolveResult("acknowledge", taskId, parent.runId);
      const listener = yield* h.holdTerminalListener();
      yield* h.finishTurn(parent, "completed");

      // Park the release while it reserves the sibling, then read that sibling.
      const reservation = yield* h.holdNextReservation();
      yield* Deferred.succeed(listener, undefined);
      const handled = yield* h.settled().pipe(Effect.forkChild);
      yield* Effect.raceFirst(
        Deferred.await(reservation.entered),
        Fiber.join(handled).pipe(
          Effect.andThen(
            Effect.sync(() => assert.fail("settlement did not reserve the waiting sibling")),
          ),
        ),
      );
      const read = yield* h
        .resolveResult("acknowledge", siblingId!, parent.runId)
        .pipe(Effect.forkChild);
      yield* TestClock.adjust(0);
      yield* Deferred.succeed(reservation.release, undefined);
      yield* Fiber.join(read);
      yield* Fiber.join(handled);

      const after = yield* h.projection();
      assert.equal(
        h.task(after, siblingId!)?.completionDelivery?.state,
        "acknowledged",
        "the read committed after the release, not under a stale claim",
      );
      assert.equal(h.run(after, parent.runId)?.delegatedCompletion?.delivery, null);
      assert.equal(h.offers.length, 1);
      yield* h.startContinuationWorker();
      yield* Queue.offer(h.requests, h.offers[0]!);
      yield* h.awaitContinuationReads(2);
      assert.deepEqual(h.dispatchAttempts, [], "a reservation read before dispatch starts no wake");
      assert.equal(h.controls.started.length, 1);
    }),
  ),
);

for (const removal of ["archive", "delete"] as const) {
  it.effect(`does not reoffer an exhausted steer after the thread is ${removal}d`, () =>
    runScenario(`removed-${removal}`, (h) =>
      Effect.gen(function* () {
        const parent = yield* h.startRun("message:parent");
        yield* h.reserveSteer(parent.runId);
        yield* h.exhaustSteering();
        yield* h.orchestrator.dispatch({
          type: removal === "archive" ? "thread.archive" : "thread.delete",
          commandId: CommandId.make(`thread-${removal}`),
          threadId,
        });
        yield* h.worker.drain();
        const removed = yield* h.projection();
        const run = h.run(removed, parent.runId)!;
        if (!isTerminalRunStatus(run.status)) {
          const now = yield* DateTime.now;
          yield* h.sink.write({
            events: [
              {
                id: EventId.make(`event:${removal}-terminal`),
                type: "run.updated",
                threadId,
                runId: run.id,
                occurredAt: now,
                payload: { ...run, status: "completed", completedAt: now },
              },
            ],
          });
        }
        assert.isAtLeast(yield* h.settled(), 1, "the terminal handler ran");
        assert.deepEqual(h.offers, []);
        assert.equal(h.controls.started.length, 1);
      }),
    ),
  );
}

const isTerminalRunStatus = (status: OrchestrationV2Run["status"]) =>
  ["completed", "failed", "interrupted", "cancelled", "rolled_back"].includes(status);

const runScenario = <E>(
  name: string,
  body: (harness: Harness) => Effect.Effect<void, E, Scope.Scope>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace(`completion-settlement-${name}`);
      const controls: AdapterControls = {
        events: yield* Queue.unbounded<ProviderAdapterV2Event>(),
        started: [],
        steered: [],
        steer: "reject",
        sessionOpens: 0,
        interrupts: 0,
      };
      const requests = yield* Queue.unbounded<ProviderContinuationRequest>();
      const offers: Array<ProviderContinuationRequest> = [];
      yield* Effect.gen(function* () {
        const harness = yield* makeHarness(controls, requests, offers, cwd);
        yield* body(harness);
      }).pipe(
        Effect.provide(
          makeOrchestratorV2ReplayLayerWithRegistry(
            { name: `completion-settlement-${name}` },
            makeSingleLayer(makeAdapter(cwd, controls)),
            { runEffectWorker: false },
          ),
        ),
        Effect.provideService(ProviderContinuationRequests, {
          offer: (request) =>
            Effect.sync(() => offers.push(request)).pipe(
              Effect.andThen(Queue.offer(requests, request)),
            ),
          take: Queue.take(requests),
        }),
      );
    }),
  );

type Harness = Effect.Success<ReturnType<typeof makeHarness>>;

const makeHarness = Effect.fnUntraced(function* (
  controls: AdapterControls,
  requests: Queue.Queue<ProviderContinuationRequest>,
  offers: ReadonlyArray<ProviderContinuationRequest>,
  cwd: string,
) {
  const orchestrator = yield* OrchestratorV2;
  const worker = yield* OrchestrationEffectWorkerV2;
  const sink = yield* EventSinkV2;
  const store = yield* ProjectionStoreV2;
  const dispatches: Array<CommandId> = [];
  const dispatchAttempts: Array<CommandId> = [];
  const dispatched = yield* Queue.unbounded<void>();
  const continuationReads = yield* Queue.unbounded<void>();

  // The terminal-run listener handles run updates one at a time, and each
  // makes a queue check. A marker run's check on another thread therefore
  // proves every earlier terminal event was handled, without adding input to
  // this thread. Counting this thread's checks shows its handler ran.
  const markerChecks = yield* Queue.unbounded<void>();
  let parentChecks = 0;
  let parentChecksAtMarker = 0;
  let markerHold: Deferred.Deferred<void> | undefined;
  const canStartQueuedRun = store.canStartQueuedRun;
  Object.assign(store, {
    canStartQueuedRun: (id: ThreadId) =>
      canStartQueuedRun(id).pipe(
        Effect.tap(() => {
          if (id === threadId) parentChecks += 1;
          if (id !== markerThreadId) return Effect.void;
          const hold = markerHold;
          return Queue.offer(markerChecks, undefined).pipe(
            Effect.andThen(hold === undefined ? Effect.void : Deferred.await(hold)),
          );
        }),
      ),
  });
  // Pauses the next reservation on this thread after it takes the parent lock,
  // at the point the rearm lock regression in DelegatedCompletionDelivery uses.
  let reservationHold:
    | { readonly entered: Deferred.Deferred<void>; readonly release: Deferred.Deferred<void> }
    | undefined;
  const getMessageCount = store.getMessageCount;
  Object.assign(store, {
    getMessageCount: (id: ThreadId) => {
      const hold = id === threadId ? reservationHold : undefined;
      if (hold === undefined) return getMessageCount(id);
      reservationHold = undefined;
      return Deferred.succeed(hold.entered, undefined).pipe(
        Effect.andThen(Deferred.await(hold.release)),
        Effect.andThen(getMessageCount(id)),
      );
    },
  });
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => Object.assign(store, { canStartQueuedRun, getMessageCount })),
  );
  let markers = 0;

  for (const [id, title] of [
    [threadId, "Completion settlement"],
    [markerThreadId, "Terminal listener marker"],
  ] as const) {
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make(`create:${id}`),
      threadId: id,
      projectId: ProjectId.make("project:completion-settlement"),
      title,
      modelSelection: runningSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: cwd,
      createdBy: "user",
      creationSource: "web",
    });
  }

  const projection = () => orchestrator.getThreadProjection(threadId);
  const run = (
    current: { readonly runs: ReadonlyArray<OrchestrationV2Run> },
    runId: RunId,
  ): OrchestrationV2Run | undefined => current.runs.find((candidate) => candidate.id === runId);
  const task = (current: Effect.Success<ReturnType<typeof projection>>, taskId: NodeId) =>
    current.subagents.find((candidate) => candidate.id === taskId);

  const watch = Effect.fnUntraced(function* (
    predicate: (event: OrchestrationV2DomainEvent) => boolean,
  ) {
    const afterSequence = yield* sink.latestSequence();
    return yield* sink.stream({ afterSequence }).pipe(
      Stream.map((stored) => stored.event),
      Stream.filter(predicate),
      Stream.take(1),
      Stream.runDrain,
      Effect.forkScoped,
    );
  });

  const settled = Effect.fnUntraced(function* () {
    markers += 1;
    const current = yield* orchestrator.getThreadProjection(markerThreadId);
    const now = yield* DateTime.now;
    const template = current.runs[0] ?? (yield* projection()).runs[0]!;
    const { delegatedCompletion: _cohort, ...base } = template;
    yield* sink.write({
      events: [
        {
          id: EventId.make(`event:marker:${markers}`),
          type: "run.updated",
          threadId: markerThreadId,
          runId: markerRunId,
          occurredAt: now,
          payload: {
            ...base,
            id: markerRunId,
            threadId: markerThreadId,
            ordinal: 1,
            status: "completed",
            completedAt: now,
          },
        },
      ],
    });
    yield* Queue.take(markerChecks);
    const handled = parentChecks - parentChecksAtMarker;
    parentChecksAtMarker = parentChecks;
    return handled;
  });

  // Parks the listener on a marker so a command can take the parent lock
  // before a later terminal event is handled. Complete the result to release it.
  const holdTerminalListener = Effect.fnUntraced(function* () {
    const hold = yield* Deferred.make<void>();
    markerHold = hold;
    yield* settled();
    markerHold = undefined;
    return hold;
  });

  const holdNextReservation = Effect.fnUntraced(function* () {
    const hold = { entered: yield* Deferred.make<void>(), release: yield* Deferred.make<void>() };
    reservationHold = hold;
    return hold;
  });

  // Acknowledges (reads) or disposes one delegated result.
  const resolveResult = (
    release: "acknowledge" | "dispose",
    taskId: NodeId,
    observedByRunId: RunId,
  ) =>
    orchestrator.dispatch(
      release === "acknowledge"
        ? {
            type: "delegated_task.completion-delivery.acknowledge",
            commandId: CommandId.make(`acknowledge:${taskId}`),
            parentThreadId: threadId,
            taskId,
            observedByRunId,
          }
        : {
            type: "delegated_task.completion-delivery.dispose",
            commandId: CommandId.make(`dispose:${taskId}`),
            parentThreadId: threadId,
            taskId,
          },
    );

  const startRun = Effect.fnUntraced(function* (id: string) {
    const running = yield* watch(
      (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
    );
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(`dispatch:${id}`),
      threadId,
      messageId: MessageId.make(id),
      text: id,
      attachments: [],
      dispatchMode: { type: "start_immediately" },
      createdBy: "user",
      creationSource: "web",
    });
    yield* worker.drain();
    yield* Fiber.join(running);
    return controls.started.at(-1)!;
  });

  const finishTurn = Effect.fnUntraced(function* (
    turnInput: ProviderAdapterV2TurnInput,
    status: "completed" | "interrupted",
  ) {
    const current = yield* orchestrator.getThreadProjection(turnInput.threadId);
    const providerTurn = current.providerTurns.find(
      (candidate) => candidate.runAttemptId === turnInput.attemptId,
    )!;
    const reached = yield* watch(
      (event) =>
        event.type === "run.updated" &&
        event.payload.id === turnInput.runId &&
        event.payload.status === (status === "completed" ? "waiting" : status),
    );
    yield* Queue.offer(controls.events, {
      type: "provider_turn.updated",
      driver,
      providerTurn: { ...providerTurn, status, completedAt: yield* DateTime.now },
    });
    yield* Queue.offer(controls.events, {
      type: "turn.terminal",
      driver,
      providerThreadId: providerTurn.providerThreadId,
      providerTurnId: providerTurn.id,
      runOrdinal: turnInput.runOrdinal,
      status,
      failure: null,
      threadDisposition: "reusable",
    });
    yield* Fiber.join(reached);
    yield* worker.drain();
    assert.equal(
      run(yield* orchestrator.getThreadProjection(turnInput.threadId), turnInput.runId)?.status,
      status,
    );
  });

  // The provider reports the turn complete before its terminal event arrives,
  // so the run is still live while its unaccepted steer is already undelivered.
  const completeProviderTurn = Effect.fnUntraced(function* (turnInput: ProviderAdapterV2TurnInput) {
    const providerTurn = (yield* projection()).providerTurns.find(
      (candidate) => candidate.runAttemptId === turnInput.attemptId,
    )!;
    const reached = yield* watch(
      (event) =>
        event.type === "provider-turn.updated" &&
        event.payload.id === providerTurn.id &&
        event.payload.status === "completed",
    );
    yield* Queue.offer(controls.events, {
      type: "provider_turn.updated",
      driver,
      providerTurn: { ...providerTurn, status: "completed", completedAt: yield* DateTime.now },
    });
    yield* Fiber.join(reached);
    yield* worker.drain();
  });

  const saveDesiredSelection = Effect.fnUntraced(function* (runId: RunId) {
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("desired-selection"),
      threadId,
      messageId: desiredSelectionMessageId,
      text: "use the new model next turn",
      attachments: [],
      modelSelection: desiredSelection,
      dispatchMode: { type: "steer_active", targetRunId: runId },
      createdBy: "user",
      creationSource: "web",
    });
    yield* worker.drain();
    const selected = yield* projection();
    assert.deepEqual(selected.thread.modelSelection, desiredSelection);
    assert.deepEqual(run(selected, runId)?.modelSelection, runningSelection);
  });

  // Seeds the claimed first result of an open cohort, then routes its durable
  // mailbox message through the orchestrator, which steers the active run.
  const reserveSteer = Effect.fnUntraced(function* (ownerRunId: RunId) {
    const messageId = MessageId.make("message:completion-steer");
    const taskId = NodeId.make("task:first");
    const current = yield* projection();
    const owner = run(current, ownerRunId)!;
    const ownerTurn = controls.started.find((turn) => turn.runId === ownerRunId)!;
    const now = yield* DateTime.now;
    yield* sink.write({
      events: [
        {
          id: EventId.make("event:cohort"),
          type: "run.updated",
          threadId,
          runId: ownerRunId,
          occurredAt: now,
          payload: {
            ...owner,
            delegatedCompletion: {
              disposition: "open",
              nextGeneration: 2,
              settledDeliveryCount: 0,
              delivery: { generation: 1, messageId, taskIds: [taskId] },
            },
          },
        },
        {
          id: EventId.make("event:first-task"),
          type: "subagent.updated",
          threadId,
          runId: ownerRunId,
          nodeId: taskId,
          occurredAt: now,
          payload: {
            id: taskId,
            threadId,
            runId: ownerRunId,
            parentNodeId: ownerTurn.rootNodeId,
            origin: "app_owned",
            createdBy: "agent",
            driver,
            providerInstanceId: instanceId,
            providerThreadId: null,
            childThreadId: null,
            nativeTaskRef: null,
            prompt: "Do background work",
            title: "First result",
            model: null,
            completionWake: "always",
            completionDelivery: { state: "claimed", observedByRunId: null },
            status: "completed",
            result: "done",
            startedAt: now,
            completedAt: now,
            updatedAt: now,
          },
        },
      ],
    });
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("completion-delivery"),
      threadId,
      messageId,
      text: "Delegated task completed.",
      attachments: [],
      dispatchMode: { type: "queue_after_active" },
      createdBy: "agent",
      creationSource: "server",
      delegatedCompletion: { parentRunId: ownerRunId, generation: 1, taskIds: [taskId] },
    });
    return { messageId, taskId };
  });

  // Siblings that finished while the first delivery was in flight wait as pending.
  const addPendingSiblings = Effect.fnUntraced(function* (ownerRunId: RunId, count: number) {
    const first = (yield* projection()).subagents.find(
      (candidate) => candidate.runId === ownerRunId,
    )!;
    const now = yield* DateTime.now;
    const ids = Array.from({ length: count }, (_, index) =>
      NodeId.make(`task:sibling-${index + 1}`),
    );
    yield* sink.write({
      events: ids.map((id) => ({
        id: EventId.make(`event:${id}`),
        type: "subagent.updated" as const,
        threadId,
        runId: ownerRunId,
        nodeId: id,
        occurredAt: now,
        payload: {
          ...first,
          id,
          title: `Sibling ${id}`,
          completionDelivery: { state: "pending" as const, observedByRunId: null },
        },
      })),
    });
    return ids;
  });

  // Requests a real app-owned child of the parent run and returns its running turn.
  const requestChild = Effect.fnUntraced(function* (parent: ProviderAdapterV2TurnInput) {
    const requested = yield* orchestrator.dispatch({
      type: "delegated_task.request",
      createdBy: "agent",
      creationSource: "mcp",
      commandId: CommandId.make("request-later-sibling"),
      parentThreadId: threadId,
      parentRunId: parent.runId,
      parentNodeId: parent.rootNodeId,
      task: "Finish after the first result was read",
      title: "Later sibling",
      modelSelection: runningSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      completionWake: "always",
    });
    yield* worker.drain();
    const child = requested.storedEvents
      .map((stored) => stored.event)
      .find(
        (event): event is Extract<OrchestrationV2DomainEvent, { type: "subagent.updated" }> =>
          event.type === "subagent.updated" && event.payload.origin === "app_owned",
      )!.payload;
    const turn = controls.started.find((started) => started.threadId === child.childThreadId)!;
    assert.isDefined(turn, "the child turn started");
    return { taskId: child.id, turn };
  });

  // Drives every real outbox retry, moving virtual time only to scheduled boundaries.
  const exhaustSteering = Effect.fnUntraced(function* () {
    for (let attempt = 0; attempt < 5; attempt += 1) {
      yield* worker.drain();
      const next = yield* worker.nextClaimableAt;
      if (Option.isSome(next)) yield* TestClock.setTime(DateTime.toEpochMillis(next.value));
    }
    assert.isTrue(Option.isNone(yield* worker.nextClaimableAt));
  });

  const repeatRunEvent = Effect.fnUntraced(function* (runId: RunId) {
    const current = run(yield* projection(), runId)!;
    yield* sink.write({
      events: [
        {
          id: EventId.make(`event:repeat:${runId}:${yield* sink.latestSequence()}`),
          type: "run.updated",
          threadId,
          runId,
          occurredAt: yield* DateTime.now,
          payload: current,
        },
      ],
    });
  });

  // CheckpointRollbackService spreads the discarded run and keeps its cohort.
  const rollBack = Effect.fnUntraced(function* (runId: RunId) {
    const current = run(yield* projection(), runId)!;
    const now = yield* DateTime.now;
    yield* sink.write({
      events: [
        {
          id: EventId.make(`event:rollback:${runId}`),
          type: "run.updated",
          threadId,
          runId,
          occurredAt: now,
          payload: { ...current, status: "rolled_back", completedAt: now },
        },
      ],
    });
  });

  // Sends what the continuation worker builds from a read taken before the
  // reservation changed, and returns why the orchestrator refused it.
  const dispatchStaleCompletion = (
    parentRunId: RunId,
    messageId: MessageId,
    taskIds: ReadonlyArray<NodeId>,
  ) =>
    orchestrator
      .dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`stale-completion:${messageId}`),
        threadId,
        messageId,
        text: "Delegated task completed.",
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "agent",
        creationSource: "server",
        delegatedCompletion: { parentRunId, generation: 1, taskIds },
      })
      .pipe(
        Effect.as("dispatched"),
        Effect.catchTag("OrchestratorDispatchError", (error) =>
          Effect.succeed(String(error.cause)),
        ),
      );

  const startContinuationWorker = () =>
    Layer.build(
      continuationWorker.pipe(
        Layer.provide(idAllocatorLayer),
        Layer.provide(
          Layer.mock(ThreadManagementService)({
            getThreadRecords: (id, fields, filter) =>
              orchestrator
                .getThreadRecords(id, fields, filter)
                .pipe(Effect.tap(() => Queue.offer(continuationReads, undefined))),
            dispatch: (command) =>
              Effect.sync(() => dispatchAttempts.push(command.commandId)).pipe(
                Effect.andThen(orchestrator.dispatch(command)),
                Effect.tap(() =>
                  Effect.sync(() => dispatches.push(command.commandId)).pipe(
                    Effect.andThen(Queue.offer(dispatched, undefined)),
                  ),
                ),
              ),
          }),
        ),
      ),
    );

  // Waits for the next continuation dispatch to start provider turn `count`.
  // An earlier drain may already have started it, so check before joining.
  const awaitDispatchedTurn = Effect.fnUntraced(function* (count: number) {
    yield* Queue.take(dispatched);
    const isRunning = (event: OrchestrationV2DomainEvent) =>
      event.type === "provider-turn.updated" &&
      event.payload.status === "running" &&
      event.payload.runAttemptId === controls.started[count - 1]?.attemptId;
    const running = yield* watch(isRunning);
    yield* worker.drain();
    const turn = controls.started[count - 1];
    const alreadyRunning =
      turn !== undefined &&
      (yield* projection()).providerTurns.some(
        (candidate) => candidate.runAttemptId === turn.attemptId && candidate.status === "running",
      );
    yield* alreadyRunning ? Fiber.interrupt(running) : Fiber.join(running);
    assert.equal(controls.started.length, count);
    return controls.started[count - 1]!;
  });

  const awaitContinuationReads = Effect.fnUntraced(function* (count: number) {
    for (let read = 0; read < count; read += 1) yield* Queue.take(continuationReads);
  });

  return {
    controls,
    requests,
    offers,
    dispatches,
    dispatchAttempts,
    orchestrator,
    worker,
    sink,
    projection,
    run,
    task,
    settled,
    holdTerminalListener,
    holdNextReservation,
    resolveResult,
    startRun,
    finishTurn,
    completeProviderTurn,
    saveDesiredSelection,
    reserveSteer,
    requestChild,
    addPendingSiblings,
    exhaustSteering,
    repeatRunEvent,
    rollBack,
    dispatchStaleCompletion,
    startContinuationWorker,
    awaitDispatchedTurn,
    awaitContinuationReads,
  };
});

function makeAdapter(cwd: string, controls: AdapterControls): ProviderAdapterV2Shape {
  return {
    instanceId,
    driver,
    getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: (input) =>
      Effect.gen(function* () {
        controls.sessionOpens += 1;
        const now = yield* DateTime.now;
        return {
          instanceId,
          driver,
          providerSessionId: input.providerSessionId,
          providerSession: {
            id: input.providerSessionId,
            driver,
            providerInstanceId: instanceId,
            status: "ready",
            cwd,
            model: runningSelection.model,
            capabilities: CodexProviderCapabilitiesV2,
            createdAt: now,
            updatedAt: now,
            lastError: null,
          },
          events: Stream.fromQueue(controls.events),
          ensureThread: ({ threadId: appThreadId }) =>
            Effect.succeed({
              id: ProviderThreadId.make(`provider-thread:${appThreadId}`),
              driver,
              providerInstanceId: instanceId,
              providerSessionId: input.providerSessionId,
              appThreadId,
              ownerNodeId: null,
              nativeThreadRef: { driver, nativeId: "native-thread", strength: "strong" },
              nativeConversationHeadRef: null,
              status: "idle",
              firstRunOrdinal: null,
              lastRunOrdinal: null,
              handoffIds: [],
              forkedFrom: null,
              createdAt: now,
              updatedAt: now,
            }),
          resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
          startTurn: (turn) =>
            Effect.gen(function* () {
              controls.started.push(turn);
              yield* Queue.offer(controls.events, {
                type: "provider_turn.updated",
                driver,
                providerTurn: {
                  id: ProviderTurnId.make(`provider-turn:${turn.attemptId}`),
                  providerThreadId: turn.providerThread.id,
                  nodeId: turn.rootNodeId,
                  runAttemptId: turn.attemptId,
                  nativeTurnRef: {
                    driver,
                    nativeId: `native:${turn.attemptId}`,
                    strength: "strong",
                  },
                  ordinal: turn.providerTurnOrdinal,
                  status: "running",
                  startedAt: yield* DateTime.now,
                  completedAt: null,
                },
              });
            }),
          steerTurn: (turn) =>
            Effect.gen(function* () {
              if (turn.message.messageId === desiredSelectionMessageId) return;
              controls.steered.push(turn.message.messageId);
              if (controls.steer === "accept") return;
              return yield* new ProviderAdapterSteerRunError({
                driver,
                providerThreadId: turn.providerThread.id,
                providerTurnId: turn.providerTurnId,
                cause: "Pi turn is not active.",
              });
            }),
          interruptTurn: () =>
            Effect.sync(() => {
              controls.interrupts += 1;
            }),
          respondToRuntimeRequest: () => Effect.void,
          readThreadSnapshot: () => Effect.die("unused"),
          rollbackThread: () => Effect.die("unused"),
          forkThread: () => Effect.die("unused"),
        };
      }),
  };
}
