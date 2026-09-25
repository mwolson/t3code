import { modelSelectionsEqual } from "@t3tools/shared/model";
import { projectComposerContextForProvider } from "@t3tools/shared/composerContextReferences";
import {
  CommandId,
  latestProviderTurnForAttempt,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
  type OrchestrationV2TurnItem,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Struct from "effect/Struct";
import * as Schema from "effect/Schema";

import * as GitWorkflowService from "../git/GitWorkflowService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProviderAuthService from "../provider/ProviderAuthService.ts";
import * as EventSink from "./EventSink.ts";
import * as ContextHandoffService from "./ContextHandoffService.ts";
import {
  DEFAULT_HANDOFF_TOKEN_CAP,
  handoffTokenCapConfig,
  handoffBudget,
  attachmentTokenAllowance,
  contextUsageForHandoff,
  historicalMessage,
  latestNativeContextUsage,
} from "./ContextHandoffBudget.ts";
import { deliverContextHandoffs } from "./ContextHandoffDelivery.ts";
import {
  ProviderAdapterTurnStartError,
  ProviderAdapterBufferedOutputError,
  type ProviderAdapterV2Error,
  type ProviderAdapterV2HistoricalContext,
  type ProviderAdapterV2SessionRuntime,
} from "./ProviderAdapter.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import { makeProviderFailure, makeProviderFailureTurnItem } from "./ProviderFailure.ts";
import * as RunExecutionService from "./RunExecutionService.ts";
import * as RuntimePolicy from "./RuntimePolicy.ts";
import {
  isRestartNoteContinuation,
  pendingRestartCancelledBackgroundWork,
  restartCancelledBackgroundWorkNote,
} from "./RestartBackgroundNote.ts";

export class ProviderTurnStartError extends Schema.TaggedError<ProviderTurnStartError>()(
  "ProviderTurnStartError",
  {
    runId: RunId,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

const isProviderTurnStartError = Schema.is(ProviderTurnStartError);

export interface ProviderTurnStartServiceV2Shape {
  /**
   * Starts the run's provider turn. When `willRetry` is true, a session open
   * failure is returned so the caller can retry. Otherwise the run is settled
   * as failed.
   */
  readonly start: (input: {
    readonly threadId: ThreadId;
    readonly runId: RunId;
    readonly willRetry?: boolean;
  }) => Effect.Effect<void, ProviderTurnStartError>;
  readonly failFromDeadLetter: (input: {
    readonly threadId: ThreadId;
    readonly runId: RunId;
    readonly error: string;
    readonly expectedAttemptId?: OrchestrationV2RunAttempt["id"] | null;
  }) => Effect.Effect<void, ProviderTurnStartError>;
}

export class ProviderTurnStartServiceV2 extends Context.Service<
  ProviderTurnStartServiceV2,
  ProviderTurnStartServiceV2Shape
>()("t3/orchestration-v2/ProviderTurnStartService/ProviderTurnStartServiceV2") {}

export const layer: Layer.Layer<
  ProviderTurnStartServiceV2,
  never,
  | EventSink.EventSinkV2
  | ContextHandoffService.ContextHandoffServiceV2
  | IdAllocator.IdAllocatorV2
  | FileSystem.FileSystem
  | GitWorkflowService.GitWorkflowService
  | ProjectService.ProjectService
  | ProviderAuthService.ProviderAuthService
  | ProjectionStore.ProjectionStoreV2
  | ProviderSessionManager.ProviderSessionManagerV2
  | RunExecutionService.RunExecutionServiceV2
  | RuntimePolicy.RuntimePolicyV2
> = Layer.effect(
  ProviderTurnStartServiceV2,
  Effect.gen(function* () {
    const eventSink = yield* EventSink.EventSinkV2;
    const contextHandoffService = yield* ContextHandoffService.ContextHandoffServiceV2;
    const idAllocator = yield* IdAllocator.IdAllocatorV2;
    const fileSystem = yield* FileSystem.FileSystem;
    const gitWorkflow = yield* GitWorkflowService.GitWorkflowService;
    const projects = yield* ProjectService.ProjectService;
    const providerAuth = yield* ProviderAuthService.ProviderAuthService;
    const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
    const providerSessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
    const runExecution = yield* RunExecutionService.RunExecutionServiceV2;
    const runtimePolicy = yield* RuntimePolicy.RuntimePolicyV2;

    // These callbacks outlive startup while a run drains background work. Build
    // them outside start's scope so they cannot retain its full thread history.
    const makeRunControls = (input: {
      readonly threadId: ThreadId;
      readonly runId: RunId;
      readonly attemptId: OrchestrationV2RunAttempt["id"];
      readonly providerThreadId: OrchestrationV2ProviderThread["id"];
      readonly runOrdinal: number;
      readonly inheritedBackgroundTurnItems: ReturnType<
        typeof RunExecutionService.selectInheritedBackgroundTurnItems
      >;
    }) => {
      // Guards and background routing need live execution state, not a fresh
      // allocation of every completed message and tool output in the thread.
      // `false` means the run moved on or is gone. A failed read is an error,
      // so the caller fails the start or the run instead of skipping it.
      const isCurrentAttemptInStatus = (expectedStatus: OrchestrationV2Run["status"]) =>
        projectionStore.getRuntimeRecoveryProjection(input.threadId).pipe(
          Effect.map((current) => {
            const run = current.runs.find((candidate) => candidate.id === input.runId);
            return run?.activeAttemptId === input.attemptId && run.status === expectedStatus;
          }),
        );
      return {
        isCurrentAttemptInStatus,
        loadInheritedBackgroundTurnItems: () =>
          projectionStore.getRuntimeRecoveryProjection(input.threadId).pipe(
            Effect.map((current) =>
              RunExecutionService.selectInheritedBackgroundTurnItems({
                threadId: input.threadId,
                currentProviderThreadId: input.providerThreadId,
                currentRunOrdinal: input.runOrdinal,
                runs: current.runs,
                turnItems: current.turnItems,
              }),
            ),
            Effect.catchCause(() => Effect.succeed(input.inheritedBackgroundTurnItems)),
          ),
        shouldStartProviderTurn: () => isCurrentAttemptInStatus("running"),
        shouldFinalizeRun: () =>
          projectionStore.getRuntimeRecoveryProjection(input.threadId).pipe(
            Effect.map((current) => {
              const run = current.runs.find((candidate) => candidate.id === input.runId);
              return (
                run?.activeAttemptId === input.attemptId &&
                (run.status === "starting" || run.status === "running")
              );
            }),
          ),
        hasUnpairedRunInterruptRequest: () =>
          projectionStore
            .hasUnpairedRunInterruptRequest(
              input.threadId,
              idAllocator.derive.runSignalTurnItem({
                runId: input.runId,
                signal: "interrupt-request",
              }),
              idAllocator.derive.runSignalTurnItem({
                runId: input.runId,
                signal: "interrupt-result",
              }),
            )
            .pipe(Effect.catchCause(() => Effect.succeed(false))),
      };
    };

    const makeDeliverySession = (
      session: ProviderAdapterV2SessionRuntime,
      startWithHandoffs: (
        input: Parameters<ProviderAdapterV2SessionRuntime["startTurn"]>[0],
        compact?: boolean,
      ) => ReturnType<ProviderAdapterV2SessionRuntime["startTurn"]>,
    ) => {
      let deliver: typeof startWithHandoffs | undefined = startWithHandoffs;
      const start = (
        input: Parameters<ProviderAdapterV2SessionRuntime["startTurn"]>[0],
        compact = false,
      ) =>
        Effect.suspend(() => {
          if (deliver !== undefined) return deliver(input, compact);
          return compact && session.compactThread !== undefined
            ? session.compactThread(input)
            : session.startTurn(input);
        }).pipe(
          // Only startup needs the handoff history. The event worker keeps this
          // session alive afterward, including when background work remains.
          Effect.ensuring(
            Effect.sync(() => {
              deliver = undefined;
            }),
          ),
        );
      return {
        ...session,
        startTurn: (input: Parameters<typeof session.startTurn>[0]) => start(input),
        ...(session.compactThread === undefined
          ? {}
          : {
              compactThread: (input: Parameters<typeof session.startTurn>[0]) => start(input, true),
            }),
      };
    };

    const start = Effect.fn("orchestrationV2.providerTurnStart.start")(function* (input: {
      readonly threadId: ThreadId;
      readonly runId: RunId;
      readonly willRetry?: boolean;
    }) {
      const { runId } = input;
      const projection = yield* projectionStore.getTurnStartContext(input.threadId, runId);
      const run = projection.runs.find((candidate) => candidate.id === runId);
      if (run === undefined) {
        return yield* new ProviderTurnStartError({ runId, cause: `Run ${runId} was not found.` });
      }
      if (run.status !== "starting") {
        // The effect is idempotent once the run has advanced or terminalized.
        return;
      }
      const rootNode = projection.nodes.find((candidate) => candidate.id === run.rootNodeId);
      const attempt = projection.attempts.find((candidate) => candidate.id === run.activeAttemptId);
      const providerThread = projection.providerThreads.find(
        (candidate) => candidate.id === run.providerThreadId,
      );
      const message = projection.messages.find((candidate) => candidate.id === run.userMessageId);
      const checkpointScope = projection.checkpointScopes.find(
        (candidate) => candidate.id === rootNode?.checkpointScopeId,
      );
      const handoffs = projection.contextHandoffs.filter(
        (handoff) =>
          handoff.status === "ready" &&
          (handoff.targetRunId === run.id ||
            (handoff.toProviderThreadId === run.providerThreadId &&
              projection.runs.some(
                (source) =>
                  source.id === handoff.targetRunId &&
                  (source.status === "failed" ||
                    source.status === "interrupted" ||
                    (source.status === "completed" &&
                      handoff.delivery === undefined &&
                      projection.messages.some(
                        (message) =>
                          message.id === source.userMessageId &&
                          message.attachments.length === 0 &&
                          message.text.trim().toLowerCase() === "/compact",
                      ))),
              ))),
      );
      const nativeForkTransfer = projection.contextTransfers.find(
        (transfer) =>
          transfer.type === "fork" &&
          transfer.targetThreadId === input.threadId &&
          transfer.targetRunId === run.id &&
          transfer.status === "pending" &&
          transfer.resolution === null,
      );
      if (
        rootNode === undefined ||
        attempt === undefined ||
        providerThread === undefined ||
        providerThread.providerSessionId === null ||
        message === undefined ||
        checkpointScope === undefined
      ) {
        return yield* new ProviderTurnStartError({
          runId,
          cause: `Run ${runId} is missing its execution projection state.`,
        });
      }
      // Settles a run that never reached the provider: one signal turn item plus
      // terminal run, attempt and root node, written only while the run is still
      // the current starting attempt.
      const settleRunBeforeStart = Effect.fn("orchestrationV2.providerTurnStart.settleBeforeStart")(
        function* (input: {
          readonly signal: string;
          readonly status: "completed" | "failed";
          readonly now: DateTime.Utc;
          /** Omitted when the run never started, so `startedAt` stays as projected. */
          readonly startedAt?: DateTime.Utc;
          readonly providerInstanceId: OrchestrationV2Run["providerInstanceId"];
          readonly itemProviderThreadId: OrchestrationV2ProviderThread["id"];
          readonly item:
            | Pick<
                Extract<OrchestrationV2TurnItem, { type: "error" }>,
                "type" | "title" | "failure"
              >
            | Pick<
                Extract<OrchestrationV2TurnItem, { type: "command_execution" }>,
                "type" | "title" | "input" | "output" | "exitCode"
              >;
          /** Emitted after the run events when the provider thread should go idle. */
          readonly providerThreadUpdate?: OrchestrationV2ProviderThread;
        }) {
          const { now, status } = input;
          const started = input.startedAt === undefined ? {} : { startedAt: input.startedAt };
          const item: OrchestrationV2TurnItem = {
            id: idAllocator.derive.runSignalTurnItem({ runId, signal: input.signal }),
            threadId: projection.thread.id,
            runId,
            nodeId: rootNode.id,
            providerThreadId: input.itemProviderThreadId,
            providerTurnId: null,
            nativeItemRef: null,
            parentItemId: null,
            ordinal:
              Math.max(
                0,
                ...projection.turnItems
                  .filter((item) => item.runId === runId)
                  .map((item) => item.ordinal),
              ) + 1,
            status,
            startedAt: now,
            completedAt: now,
            updatedAt: now,
            ...input.item,
          };
          const eventPayloads = [
            { type: "turn-item.updated", payload: item },
            { type: "run.updated", payload: { ...run, status, ...started, completedAt: now } },
            {
              type: "run-attempt.updated",
              payload: { ...attempt, status, ...started, completedAt: now },
            },
            {
              type: "node.updated",
              payload: { ...rootNode, status, ...started, completedAt: now },
            },
            ...(input.providerThreadUpdate === undefined
              ? []
              : [
                  {
                    type: "provider-thread.updated" as const,
                    payload: input.providerThreadUpdate,
                  },
                ]),
          ] as const;
          const events = yield* Effect.forEach(eventPayloads, (event) =>
            Effect.gen(function* () {
              return {
                ...event,
                id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
                threadId: projection.thread.id,
                runId,
                nodeId: rootNode.id,
                providerInstanceId: input.providerInstanceId,
                occurredAt: now,
              } satisfies OrchestrationV2DomainEvent;
            }),
          );
          yield* eventSink.writeIfRunCurrent({
            threadId: projection.thread.id,
            runId,
            activeAttemptId: attempt.id,
            expectedStatus: "starting",
            events,
          });
        },
      );
      if (message.attachments.length === 0 && message.text.trimStart().startsWith("/")) {
        const isEmptyCompaction =
          message.text.trim().toLowerCase() === "/compact" && !projection.hasConversation;
        // Preparing a run may already point the thread at a newly selected
        // provider. Account commands still belong to its last native session.
        const nativeThreads = new Map(
          projection.providerThreads
            .filter(
              (candidate) => candidate.ownerNodeId === null && candidate.nativeThreadRef !== null,
            )
            .map((candidate) => [candidate.id, candidate]),
        );
        const previousNativeRun = projection.runs.reduce<OrchestrationV2Run | undefined>(
          (previous, candidate) =>
            candidate.ordinal < run.ordinal &&
            candidate.providerThreadId !== null &&
            nativeThreads.has(candidate.providerThreadId) &&
            (previous === undefined || candidate.ordinal > previous.ordinal)
              ? candidate
              : previous,
          undefined,
        );
        const nativeThread = nativeThreads.get(
          previousNativeRun?.providerThreadId ??
            projection.thread.activeProviderThreadId ??
            providerThread.id,
        );
        const authInstanceId = nativeThread?.providerInstanceId ?? run.providerInstanceId;
        const authResult = isEmptyCompaction
          ? null
          : yield* Effect.result(
              providerAuth.tryHandlePromptCommand({
                instanceId: authInstanceId,
                text: projectComposerContextForProvider({
                  text: message.text,
                  records: message.context?.records ?? [],
                }),
                hasAttachments: false,
              }),
            );
        if (isEmptyCompaction || authResult?._tag === "Failure" || authResult?.success) {
          const now = yield* DateTime.now;
          const failure = isEmptyCompaction
            ? makeProviderFailure({
                class: "validation_error",
                message: "Start a conversation before compacting this thread.",
              })
            : authResult?._tag === "Failure"
              ? makeProviderFailure({
                  class: "permission_error",
                  message: authResult.failure.detail,
                })
              : undefined;
          const status = failure === undefined ? "completed" : "failed";
          yield* settleRunBeforeStart({
            signal: isEmptyCompaction ? "empty-compaction" : "provider-sign-out",
            status,
            now,
            startedAt: now,
            providerInstanceId: authInstanceId,
            itemProviderThreadId: nativeThread?.id ?? providerThread.id,
            item:
              failure !== undefined
                ? {
                    type: "error",
                    title: isEmptyCompaction
                      ? "Cannot compact an empty thread"
                      : "Provider sign-out failed",
                    failure,
                  }
                : {
                    type: "command_execution",
                    title: "Provider signed out",
                    input: message.text.trim(),
                    output: "Provider signed out",
                    exitCode: 0,
                  },
            providerThreadUpdate: {
              ...providerThread,
              status: providerThread.nativeThreadRef === null ? "not_loaded" : "idle",
              updatedAt: now,
            },
          });
          return;
        }
      }
      const { worktreePath, branch } = projection.thread;
      if (worktreePath !== null && branch !== null) {
        const exists = yield* fileSystem
          .exists(worktreePath)
          .pipe(Effect.orElseSucceed(() => true));
        if (!exists) {
          const project = yield* projects.getById(projection.thread.projectId).pipe(
            Effect.map(Option.getOrUndefined),
            Effect.orElseSucceed(() => undefined),
          );
          if (project !== undefined) {
            yield* Effect.logWarning("provider turn start recreating missing worktree", {
              threadId: projection.thread.id,
              worktreePath,
              branch,
            });
            yield* gitWorkflow.pruneWorktrees({ cwd: project.workspaceRoot }).pipe(
              Effect.andThen(
                gitWorkflow.createWorktree({
                  cwd: project.workspaceRoot,
                  refName: branch,
                  path: worktreePath,
                }),
              ),
              Effect.catchCause((cause) =>
                Cause.hasInterruptsOnly(cause)
                  ? Effect.failCause(cause)
                  : Effect.logWarning("provider turn start failed to recreate worktree", {
                      threadId: projection.thread.id,
                      worktreePath,
                      cause: Cause.pretty(cause),
                    }),
              ),
            );
          }
        }
      }
      const selectInheritedBackgroundItems = (
        current: ProjectionStore.ProjectionRuntimeRecoveryState,
      ): ReturnType<typeof RunExecutionService.selectInheritedBackgroundTurnItems> =>
        RunExecutionService.selectInheritedBackgroundTurnItems({
          threadId: current.thread.id,
          currentProviderThreadId: providerThread.id,
          currentRunOrdinal: run.ordinal,
          runs: current.runs,
          turnItems: current.turnItems,
        });
      const inheritedBackgroundTurnItems = yield* projectionStore
        .getRuntimeRecoveryProjection(projection.thread.id)
        .pipe(Effect.map(selectInheritedBackgroundItems));
      const providerSessionId = providerThread.providerSessionId;
      const runControls = makeRunControls({
        threadId: projection.thread.id,
        runId: run.id,
        attemptId: attempt.id,
        providerThreadId: providerThread.id,
        runOrdinal: run.ordinal,
        inheritedBackgroundTurnItems,
      });
      const { isCurrentAttemptInStatus } = runControls;

      const resolvedRuntimePolicy = yield* runtimePolicy.resolve({
        thread: projection.thread,
        modelSelection: run.modelSelection,
      });
      const existingSessionProjection = projection.providerSessions.find(
        (candidate) => candidate.id === providerSessionId,
      );
      const sessionResult = yield* Effect.result(
        providerSessions.open({
          threadId: projection.thread.id,
          providerSessionId,
          modelSelection: run.modelSelection,
          runtimePolicy: resolvedRuntimePolicy,
          ...(existingSessionProjection === undefined
            ? {}
            : { resumeFromSession: existingSessionProjection }),
          ...(providerThread.nativeThreadRef?.nativeId == null
            ? {}
            : { initialNativeThreadId: providerThread.nativeThreadRef.nativeId }),
          ...(providerThread.nativeMetadata?.itemIdentityVersion === undefined
            ? {}
            : {
                initialProviderItemIdentityVersion:
                  providerThread.nativeMetadata.itemIdentityVersion,
              }),
        }),
      );
      // The last start attempt fails the run with the provider's own reason
      // instead of leaving it `starting` after the effect gives up. A run that
      // already left `starting` is not overwritten, and a failed write returns
      // its error to the effect worker.
      const startFailure = (error: Error) => {
        const nestedCause = "cause" in error ? error.cause : undefined;
        return makeProviderFailure({
          cause: error,
          message:
            nestedCause instanceof Error
              ? nestedCause.message
              : typeof nestedCause === "string"
                ? nestedCause
                : error.message,
          class: "provider_error",
        });
      };
      const settleStartFailure = (failed: {
        readonly signal: string;
        readonly title: string;
        readonly error: Error;
      }) =>
        Effect.gen(function* () {
          // A restart that cannot reopen its session or reload its native
          // thread also settles the work it inherited from the earlier attempt.
          if (attempt.attemptOrdinal > 1) {
            return yield* settleFailedStart({
              threadId: projection.thread.id,
              runId,
              error: failed.error.message,
              openFailure: {
                signal: failed.signal,
                title: failed.title,
                failure: startFailure(failed.error),
                now: yield* DateTime.now,
                attemptId: attempt.id,
              },
            });
          }
          yield* settleRunBeforeStart({
            signal: failed.signal,
            status: "failed",
            now: yield* DateTime.now,
            providerInstanceId: run.providerInstanceId,
            itemProviderThreadId: providerThread.id,
            item: { type: "error", title: failed.title, failure: startFailure(failed.error) },
          });
        });
      if (sessionResult._tag === "Failure") {
        if (input.willRetry === true) return yield* sessionResult.failure;
        yield* settleStartFailure({
          signal: "provider-session-open-failure",
          title: "Provider session failed to open",
          error: sessionResult.failure,
        });
        return;
      }
      const session = sessionResult.success;
      // Only the provider's own thread load fails the run on the last attempt;
      // store, id and handoff failures around it keep their typed errors.
      const loadFromProvider = (
        load: Effect.Effect<OrchestrationV2ProviderThread, ProviderAdapterV2Error>,
      ) =>
        Effect.gen(function* () {
          const loaded = yield* Effect.result(load);
          if (loaded._tag === "Success") return loaded.success;
          if (
            input.willRetry === true ||
            Schema.is(ProviderAdapterBufferedOutputError)(loaded.failure)
          )
            return yield* loaded.failure;
          yield* settleStartFailure({
            signal: "provider-thread-load-failure",
            title: "Provider turn failed to start",
            error: loaded.failure,
          });
          return undefined;
        });
      let effectiveHandoffs = handoffs;
      const bindingResult = yield* Effect.result(
        Effect.gen(function* () {
          if (nativeForkTransfer !== undefined) {
            const sourceProjection = yield* projectionStore.getThreadRecords(
              nativeForkTransfer.sourceThreadId,
              ["runs", "providerThreads", "attempts", "providerTurns"],
            );
            const sourceRun = sourceProjection.runs.find(
              (candidate) => candidate.id === nativeForkTransfer.sourcePoint.runId,
            );
            const sourceProviderThread = sourceProjection.providerThreads.find(
              (candidate) => candidate.id === sourceRun?.providerThreadId,
            );
            const sourceAttempt = sourceProjection.attempts.find(
              (candidate) => candidate.id === sourceRun?.activeAttemptId,
            );
            const sourceProviderTurn =
              latestProviderTurnForAttempt(sourceProjection.providerTurns, sourceAttempt?.id) ??
              sourceProjection.providerTurns.find(
                (candidate) => candidate.id === sourceAttempt?.providerTurnId,
              );
            if (sourceRun === undefined || sourceProviderThread === undefined) {
              return yield* new ProviderTurnStartError({
                runId,
                cause: `Native fork transfer ${nativeForkTransfer.id} has no source provider execution.`,
              });
            }
            return yield* loadFromProvider(
              session.forkThread({
                sourceProviderThread,
                sourceProviderTurns: sourceProjection.providerTurns,
                targetThreadId: projection.thread.id,
                modelSelection: run.modelSelection,
                runtimePolicy: resolvedRuntimePolicy,
                ...(sourceProviderTurn === undefined
                  ? {}
                  : { providerTurnId: sourceProviderTurn.id }),
              }),
            );
          }
          if (providerThread.nativeThreadRef === null) {
            // Hand the run's provider thread to the adapter so it adopts this
            // row's identity when attaching native state. An adapter that mints
            // its own row instead leaves two live rows per app thread, and
            // `activeProviderThreadId` then flaps between them on every update.
            return yield* loadFromProvider(
              session.ensureThread({
                threadId: projection.thread.id,
                modelSelection: run.modelSelection,
                runtimePolicy: resolvedRuntimePolicy,
                providerSessionId,
                existingProviderThread: providerThread,
              }),
            );
          }
          const uncertainDelivery = projection.contextHandoffs.some(
            (handoff) =>
              handoff.toProviderThreadId === providerThread.id &&
              handoff.delivery?.nativeThreadId === providerThread.nativeThreadRef?.nativeId &&
              handoff.delivery?.status === "pending",
          );
          const resumed = yield* Effect.result(
            uncertainDelivery
              ? Effect.fail(
                  new ProviderAdapterTurnStartError({
                    driver: session.driver,
                    threadId: projection.thread.id,
                    providerThreadId: providerThread.id,
                    runId,
                    cause: "Uncertain native history injection",
                  }),
                )
              : session.resumeThread({
                  providerThread,
                  threadId: projection.thread.id,
                  modelSelection: run.modelSelection,
                  runtimePolicy: resolvedRuntimePolicy,
                }),
          );
          if (resumed._tag === "Success") {
            return resumed.success;
          }

          if (Schema.is(ProviderAdapterBufferedOutputError)(resumed.failure))
            return yield* resumed.failure;
          yield* Effect.logWarning("Provider resume failed; attempting a fresh native session", {
            driver: session.driver,
            providerThreadId: providerThread.id,
            runId,
            reason: uncertainDelivery ? "uncertain_history_delivery" : "resume_failed",
            errorTag: resumed.failure._tag,
          });
          const replacement = yield* loadFromProvider(
            session.ensureThread({
              threadId: projection.thread.id,
              modelSelection: run.modelSelection,
              runtimePolicy: resolvedRuntimePolicy,
              providerSessionId,
              // The native ref is dropped so the adapter binds a fresh native
              // session instead of retrying the resume that just failed, while
              // still adopting this row's identity.
              existingProviderThread: { ...providerThread, nativeThreadRef: null },
            }),
          );
          if (replacement === undefined) return undefined;
          const transferId = yield* idAllocator.allocate.contextTransfer({
            sourceThreadId: projection.thread.id,
            targetThreadId: projection.thread.id,
            type: "provider_resume_fallback",
          });
          const createdAt = yield* DateTime.now;
          const handoff = yield* contextHandoffService.prepareProviderHandoff({
            threadId: projection.thread.id,
            targetRunId: run.id,
            transferId,
            fromProviderThreadIds: [providerThread.id],
            toProviderThreadId: providerThread.id,
            fromProviderInstanceId: providerThread.providerInstanceId,
            toProviderInstanceId: run.providerInstanceId,
            coveredRunOrdinals: { from: 1, to: Math.max(1, run.ordinal - 1) },
            strategy: "full_thread_summary",
            runs: projection.runs,
            items: (yield* projectionStore.getTurnStartHistory(input.threadId)).filter(
              (item) =>
                item.runId === null ||
                projection.runs.some(
                  (source) => source.id === item.runId && source.ordinal < run.ordinal,
                ),
            ),
            createdAt,
          });
          effectiveHandoffs = [handoff, ...effectiveHandoffs];
          yield* eventSink.write({
            events: [
              {
                id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
                type: "context-handoff.updated",
                threadId: projection.thread.id,
                runId: run.id,
                providerInstanceId: run.providerInstanceId,
                occurredAt: createdAt,
                payload: handoff,
              },
              {
                id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
                type: "context-transfer.updated",
                threadId: projection.thread.id,
                runId: run.id,
                providerInstanceId: run.providerInstanceId,
                occurredAt: createdAt,
                payload: {
                  id: transferId,
                  type: "provider_handoff",
                  sourceThreadId: projection.thread.id,
                  targetThreadId: projection.thread.id,
                  sourcePoint: { threadId: projection.thread.id },
                  basePoint: null,
                  sourceProviderInstanceId: providerThread.providerInstanceId,
                  targetProviderInstanceId: run.providerInstanceId,
                  targetRunId: run.id,
                  status: "resolved_portable",
                  resolution: { strategy: "portable_context", contextHandoffId: handoff.id },
                  createdBy: "system",
                  error: null,
                  createdAt,
                  updatedAt: createdAt,
                  consumedAt: null,
                },
              },
            ],
          });
          return replacement;
        }),
      );
      if (bindingResult._tag === "Failure") {
        if (!Schema.is(ProviderAdapterBufferedOutputError)(bindingResult.failure))
          return yield* bindingResult.failure;
        yield* settleRunBeforeStart({
          signal: "provider-buffered-output-owned",
          status: "failed",
          now: yield* DateTime.now,
          providerInstanceId: run.providerInstanceId,
          itemProviderThreadId: providerThread.id,
          item: {
            type: "error",
            title: "Provider output must be delivered first",
            failure: makeProviderFailure({
              cause: bindingResult.failure,
              class: "provider_error",
              message: "Buffered provider output must be delivered before changing its binding.",
            }),
          },
        });
        return;
      }
      const loadedProviderThread = bindingResult.success;
      // The last attempt already failed the run.
      if (loadedProviderThread === undefined) return;
      if (!(yield* isCurrentAttemptInStatus("starting"))) {
        return;
      }
      const now = yield* DateTime.now;
      // Only started runs reached the provider-thread update below. Queued runs and
      // failures during session setup cannot establish a new telemetry selection.
      const measuredContext = latestNativeContextUsage(projection, providerThread);
      const previousSelection =
        measuredContext?.modelSelection ??
        projection.runs.findLast(
          (source) =>
            source.ordinal < run.ordinal &&
            source.startedAt !== null &&
            source.providerThreadId === providerThread.id,
        )?.modelSelection;
      const sameSelection =
        previousSelection === undefined ||
        modelSelectionsEqual(previousSelection, run.modelSelection);
      const sameNativeThread =
        loadedProviderThread.nativeThreadRef?.nativeId === providerThread.nativeThreadRef?.nativeId;
      const threadUsage = loadedProviderThread.contextUsage ?? providerThread.contextUsage;
      const previousUsage = measuredContext
        ? { ...threadUsage, ...measuredContext.usage }
        : threadUsage;
      const reuseTelemetry =
        sameSelection ||
        (previousSelection !== undefined &&
          session.canReuseContextUsage?.(previousSelection, run.modelSelection) === true);
      const knownModelWindow = session.getModelContextWindow?.(
        run.modelSelection,
        resolvedRuntimePolicy.cwd,
      );
      // Persist before delivery. Keep this native transcript's measured
      // occupancy. A different model drops compaction telemetry and uses the
      // new window when that window is known.
      const handoffUsage = contextUsageForHandoff({
        sameNativeThread,
        sameSelection,
        reuseTelemetry,
        previousUsage,
        knownModelWindow,
      });
      const runningProviderThread: OrchestrationV2ProviderThread = {
        ...loadedProviderThread,
        contextUsage: handoffUsage,
        id: providerThread.id,
        driver: session.driver,
        providerInstanceId: run.providerInstanceId,
        providerSessionId,
        appThreadId: projection.thread.id,
        ownerNodeId: providerThread.ownerNodeId,
        firstRunOrdinal: providerThread.firstRunOrdinal ?? run.ordinal,
        lastRunOrdinal: run.ordinal,
        handoffIds: providerThread.handoffIds,
        forkedFrom: providerThread.forkedFrom,
        status: "active",
        createdAt: providerThread.createdAt,
        updatedAt: now,
      };
      const runningRun: OrchestrationV2Run = {
        ...run,
        status: "running",
        startedAt: now,
      };
      const runningAttempt: OrchestrationV2RunAttempt = {
        ...attempt,
        ...(runningProviderThread.nativeThreadRef?.nativeId == null
          ? {}
          : { nativeThreadId: runningProviderThread.nativeThreadRef.nativeId }),
        status: "running",
        startedAt: now,
      };
      const runningRootNode: OrchestrationV2ExecutionNode = {
        ...rootNode,
        status: "running",
        startedAt: now,
      };
      const events: Array<OrchestrationV2DomainEvent> = [
        {
          id: yield* idAllocator.allocate.event({
            threadId: projection.thread.id,
            providerSessionId,
          }),
          type: "provider-session.updated",
          threadId: projection.thread.id,
          driver: session.driver,
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: session.providerSession,
        },
        {
          id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
          type: "provider-thread.updated",
          threadId: projection.thread.id,
          driver: session.driver,
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: runningProviderThread,
        },
        ...(nativeForkTransfer === undefined || runningProviderThread.nativeThreadRef === null
          ? []
          : [
              {
                id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
                type: "context-transfer.updated" as const,
                threadId: projection.thread.id,
                runId: run.id,
                driver: session.driver,
                providerInstanceId: run.providerInstanceId,
                occurredAt: now,
                payload: {
                  ...nativeForkTransfer,
                  targetProviderInstanceId: run.providerInstanceId,
                  targetRunId: run.id,
                  status: "consumed" as const,
                  resolution: {
                    strategy: "native_fork" as const,
                    providerThreadRef: runningProviderThread.nativeThreadRef,
                  },
                  error: null,
                  updatedAt: now,
                  consumedAt: now,
                },
              },
            ]),
        {
          id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
          type: "run.updated",
          threadId: projection.thread.id,
          runId: run.id,
          nodeId: rootNode.id,
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: runningRun,
        },
        {
          id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
          type: "run-attempt.updated",
          threadId: projection.thread.id,
          runId: run.id,
          nodeId: rootNode.id,
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: runningAttempt,
        },
        {
          id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
          type: "node.updated",
          threadId: projection.thread.id,
          runId: run.id,
          nodeId: rootNode.id,
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: runningRootNode,
        },
      ];
      const runningWrite = yield* eventSink.writeIfRunCurrent({
        threadId: projection.thread.id,
        runId: run.id,
        activeAttemptId: attempt.id,
        expectedStatus: "starting",
        events,
      });
      if (!runningWrite.committed) {
        return;
      }
      const routableSubagents = projection.subagents.filter((subagent) =>
        RunExecutionService.canRouteRelatedSubagent(subagent.status),
      );
      const userText = projectComposerContextForProvider({
        text: message.text,
        records: message.context?.records ?? [],
      });
      // Delivered once: this run's provider turn marks the work as told. A
      // restart continuation is prompted by its own text or resumes natively.
      const noteContinuation = isRestartNoteContinuation(
        run,
        projection.runs,
        projection.providerTurns,
        projection.attempts,
      );
      const restartCancelledWork = pendingRestartCancelledBackgroundWork({
        runs: projection.runs,
        providerTurns: projection.providerTurns,
        compactionMessageIds: new Set(
          projection.messages
            .filter(
              (candidate) =>
                candidate.attachments.length === 0 &&
                candidate.text.trim().toLowerCase() === "/compact",
            )
            .map((candidate) => candidate.id),
        ),
        run,
        attempts: projection.attempts,
      });
      const restartNote =
        restartCancelledWork.length === 0
          ? ""
          : restartCancelledBackgroundWorkNote(restartCancelledWork);
      const tokenCap = yield* handoffTokenCapConfig.pipe(
        Effect.orElseSucceed(() => DEFAULT_HANDOFF_TOKEN_CAP),
      );
      const settledHandoffs = projection.contextHandoffs.filter(
        (handoff) =>
          handoff.toProviderThreadId === providerThread.id &&
          handoff.delivery?.nativeThreadId === runningProviderThread.nativeThreadRef?.nativeId &&
          handoff.delivery?.status !== "pending",
      );
      const deliveredItemIds = new Set(
        settledHandoffs.flatMap((handoff) => handoff.delivery?.itemIds ?? []),
      );
      const coveredItemIds = new Set([
        ...deliveredItemIds,
        ...settledHandoffs.flatMap((handoff) => handoff.delivery?.omittedItemIds ?? []),
      ]);
      const deliveredAttemptIds = new Set(
        projection.providerTurns.map((turn) => turn.runAttemptId),
      );
      const acceptedAttempts = projection.attempts.filter(
        (source) =>
          source.providerThreadId === providerThread.id && deliveredAttemptIds.has(source.id),
      );
      const nativeInputRunIds = new Set(
        acceptedAttempts
          .filter(
            (source) =>
              source.nativeThreadId !== undefined &&
              source.nativeThreadId === runningProviderThread.nativeThreadRef?.nativeId,
          )
          .map((source) => source.runId),
      );
      const legacyInputRunIds = new Set(
        acceptedAttempts
          .filter((source) => source.nativeThreadId === undefined)
          .map((source) => source.runId),
      );
      const legacyRecoveredRunIds = new Set(
        projection.runs
          .filter(
            (source) =>
              source.providerThreadId === providerThread.id &&
              settledHandoffs.some(
                (handoff) =>
                  handoff.strategy === "full_thread_summary" &&
                  handoff.fromProviderThreadIds.includes(providerThread.id) &&
                  source.ordinal >= handoff.coveredRunOrdinals.from &&
                  source.ordinal <= handoff.coveredRunOrdinals.to,
              ),
          )
          .map((source) => source.id),
      );
      // Use saved text and actual native attachments when telemetry is absent.
      // Legacy attempts lack native identity; exclude their explicitly recovered
      // history, whose attachments were not replayed into the replacement thread.
      const nativeContextEstimate = Effect.gen(function* () {
        return sameNativeThread
          ? (yield* projectionStore.getTurnStartHistory(input.threadId)).reduce((sum, item) => {
              if (
                item.runId === run.id ||
                (item.runId !== null &&
                  missedRunIds.has(item.runId) &&
                  !deliveredItemIds.has(item.id)) ||
                (item.providerThreadId !== providerThread.id && !deliveredItemIds.has(item.id))
              )
                return sum;
              const historical = historicalMessage(item);
              const nativeAttachments =
                item.type === "user_message" &&
                item.providerThreadId === providerThread.id &&
                item.runId !== null &&
                (nativeInputRunIds.has(item.runId) ||
                  (legacyInputRunIds.has(item.runId) &&
                    !coveredItemIds.has(item.id) &&
                    !legacyRecoveredRunIds.has(item.runId)))
                  ? attachmentTokenAllowance(item.attachments)
                  : 0;
              return (
                sum +
                (historical === null ? 0 : Buffer.byteLength(historical.text)) +
                nativeAttachments
              );
            }, 0)
          : 0;
      });
      const modelContextWindow =
        knownModelWindow ??
        (handoffUsage !== null || reuseTelemetry ? previousUsage?.maxTokens : undefined);
      // Replacing a native thread clears its usage, not the selected model's capacity.
      const budgetProviderThread = {
        ...runningProviderThread,
        contextUsage: handoffUsage,
      };
      const missedRuns = projection.runs.filter(
        (source) =>
          source.ordinal < run.ordinal &&
          source.providerThreadId === providerThread.id &&
          (source.status === "failed" || source.status === "interrupted") &&
          !deliveredAttemptIds.has(source.activeAttemptId),
      );
      const missedRunIds = new Set(missedRuns.map((source) => source.id));
      const missedItems =
        missedRunIds.size === 0
          ? []
          : (yield* projectionStore.getTurnStartHistory(input.threadId, [...missedRunIds])).filter(
              (item) =>
                item.runId !== null &&
                missedRunIds.has(item.runId) &&
                !coveredItemIds.has(item.id) &&
                historicalMessage(item) !== null,
            );
      const startWithHandoffs = (
        turnInput: Parameters<typeof session.startTurn>[0],
        compact = false,
      ) =>
        Effect.gen(function* () {
          // A failed turn/start can leave the requested turn absent from
          // native history even when its preceding handoff was injected.
          const retryHandoff =
            missedItems.length === 0
              ? []
              : [
                  yield* contextHandoffService.prepareProviderHandoff({
                    threadId: projection.thread.id,
                    targetRunId: run.id,
                    transferId: null,
                    fromProviderThreadIds: [providerThread.id],
                    toProviderThreadId: providerThread.id,
                    fromProviderInstanceId: run.providerInstanceId,
                    toProviderInstanceId: run.providerInstanceId,
                    coveredRunOrdinals: {
                      from: missedRuns[0]!.ordinal,
                      to: missedRuns.at(-1)!.ordinal,
                    },
                    strategy: "delta_since_target_last_seen",
                    items: missedItems,
                    runs: projection.runs,
                    createdAt: yield* DateTime.now,
                  }),
                ];
          const delivery = yield* deliverContextHandoffs({
            handoffs: [...effectiveHandoffs, ...retryHandoff],
            deferInline: compact,
            providerThread: runningProviderThread,
            budget: Effect.gen(function* () {
              return handoffBudget({
                tokenCap,
                modelContextWindow,
                // The note is sent with the user text, so it spends the same allowance.
                userText: restartNote === "" ? userText : `${restartNote}\n\n${userText}`,
                attachments: message.attachments,
                providerThread: budgetProviderThread,
                nativeContextEstimate:
                  budgetProviderThread.contextUsage?.usedTokens === undefined
                    ? yield* nativeContextEstimate
                    : 0,
              });
            }),
            alreadyDeliveredItemIds: deliveredItemIds,
            ...(session.injectHistory === undefined
              ? {}
              : {
                  inject: (history: ProviderAdapterV2HistoricalContext) =>
                    session.injectHistory!({
                      providerThread: runningProviderThread,
                      ...history,
                    }),
                }),
            persist: (handoff) =>
              Effect.gen(function* () {
                const updatedAt = yield* DateTime.now;
                yield* eventSink.write({
                  events: [
                    {
                      id: yield* idAllocator.allocate.event({
                        threadId: projection.thread.id,
                      }),
                      type: "context-handoff.updated",
                      threadId: projection.thread.id,
                      runId: run.id,
                      providerInstanceId: run.providerInstanceId,
                      occurredAt: updatedAt,
                      payload: { ...handoff, updatedAt },
                    },
                  ],
                });
              }),
          });
          if (!(yield* isCurrentAttemptInStatus("running"))) return;
          const start = compact ? session.compactThread! : session.startTurn;
          const context = [delivery.context, restartNote]
            .filter((part) => part !== "")
            .join("\n\n");
          // A note continuation has no turn to resume; its text is the prompt.
          const { restartContinuationOfRunId: _resumedRunId, ...promptedInput } = turnInput;
          yield* start({
            ...(noteContinuation ? promptedInput : turnInput),
            message: {
              ...turnInput.message,
              text: context === "" ? userText : `${context}\n\nUser message:\n${userText}`,
            },
          });
          // The provider already accepted the turn. A stale pending marker
          // can force a fresh thread later, but must not stop live ingestion.
          yield* delivery.delivered.pipe(
            Effect.catchCause(() =>
              Effect.logWarning("Failed to record accepted context handoff delivery", {
                runId: run.id,
                deliveryStatus: "pending",
              }),
            ),
          );
        }).pipe(
          Effect.mapError((cause) =>
            cause._tag === "ProviderAdapterTurnStartError"
              ? cause
              : new ProviderAdapterTurnStartError({
                  driver: session.driver,
                  threadId: projection.thread.id,
                  providerThreadId: providerThread.id,
                  runId: run.id,
                  cause,
                }),
          ),
        );
      const deliverySession =
        effectiveHandoffs.length === 0 &&
        missedItems.length === 0 &&
        restartNote === "" &&
        !noteContinuation
          ? session
          : makeDeliverySession(session, startWithHandoffs);
      yield* runExecution.startRootRun({
        commandId: CommandId.make(`command:effect:provider-turn.start:${run.id}`),
        appThread: projection.thread,
        providerSessionId,
        session: deliverySession,
        run: runningRun,
        rootNode: runningRootNode,
        checkpointScope,
        providerThread: runningProviderThread,
        attempt: runningAttempt,
        attemptId: attempt.id,
        loadInheritedBackgroundTurnItems: runControls.loadInheritedBackgroundTurnItems,
        relatedThreadIds: routableSubagents.flatMap((subagent) =>
          subagent.childThreadId === null ? [] : [subagent.childThreadId],
        ),
        relatedProviderThreadIds: routableSubagents.flatMap((subagent) =>
          subagent.providerThreadId === null ? [] : [subagent.providerThreadId],
        ),
        providerTurnOrdinal:
          Math.max(
            0,
            ...projection.providerTurns
              .filter((turn) => turn.providerThreadId === providerThread.id)
              .map((turn) => turn.ordinal),
          ) + 1,
        // Legacy accepted attempts have no native id. They count only before
        // a replacement, while no accepted attempt records a native identity.
        nativeThreadHasTurns:
          nativeInputRunIds.size > 0 ||
          (legacyInputRunIds.size > 0 &&
            sameNativeThread &&
            !acceptedAttempts.some((source) => source.nativeThreadId !== undefined)),
        shouldStartProviderTurn: runControls.shouldStartProviderTurn,
        shouldFinalizeRun: runControls.shouldFinalizeRun,
        hasUnpairedRunInterruptRequest: runControls.hasUnpairedRunInterruptRequest,
        message: {
          messageId: message.id,
          text: userText,
          attachments: message.attachments,
          createdBy: message.createdBy,
          creationSource: message.creationSource,
          ...(message.scheduledTaskId === undefined
            ? {}
            : { scheduledTaskId: message.scheduledTaskId }),
          ...(message.senderThreadId === undefined
            ? {}
            : { senderThreadId: message.senderThreadId }),
        },
        modelSelection: run.modelSelection,
        runtimePolicy: resolvedRuntimePolicy,
      });
    });

    // Failed restarts and exhausted start effects must settle inherited work
    // along with the root. The attempt CAS protects a concurrently replaced run.
    const settleFailedStart = Effect.fn("orchestrationV2.providerTurnStart.settleFailedStart")(
      function* (input: {
        readonly threadId: ThreadId;
        readonly runId: RunId;
        readonly error: string;
        readonly expectedAttemptId?: OrchestrationV2RunAttempt["id"] | null;
        readonly openFailure?: {
          readonly signal: string;
          readonly title: string;
          readonly failure: ReturnType<typeof makeProviderFailure>;
          readonly now: DateTime.Utc;
          readonly attemptId: OrchestrationV2RunAttempt["id"];
        };
      }) {
        // The failure cascade needs terminal lifetime links and runless child rows.
        // Successful starts and fresh open failures retain their bounded reads.
        const projection = yield* projectionStore.getThreadProjection(input.threadId);
        const run = projection.runs.find((candidate) => candidate.id === input.runId);
        if (run === undefined || run.status !== "starting" || run.activeAttemptId === null) {
          return;
        }
        const activeAttemptId = run.activeAttemptId;
        if (input.expectedAttemptId !== undefined && input.expectedAttemptId !== activeAttemptId)
          return;
        if (input.openFailure !== undefined && input.openFailure.attemptId !== activeAttemptId) {
          return;
        }
        const attempt = projection.attempts.find((candidate) => candidate.id === activeAttemptId);
        const rootNode = projection.nodes.find((candidate) => candidate.id === run.rootNodeId);
        const providerThread = projection.providerThreads.find(
          (candidate) => candidate.id === run.providerThreadId,
        );
        const now = input.openFailure?.now ?? (yield* DateTime.now);
        const failure =
          input.openFailure?.failure ??
          makeProviderFailure({
            message: "Starting the provider turn failed permanently.",
            retryable: false,
          });
        if (input.openFailure === undefined) {
          yield* Effect.logError("Provider turn start exhausted its retry budget", {
            threadId: input.threadId,
            runId: input.runId,
            error: input.error,
          });
        }
        const runtimeRequestCancellationReason =
          "The provider turn failed before this runtime request was resolved.";
        const events: Array<OrchestrationV2DomainEvent> = [];
        if (attempt !== undefined) {
          events.push({
            id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
            type: "run-attempt.updated",
            threadId: projection.thread.id,
            runId: run.id,
            nodeId: attempt.rootNodeId,
            providerInstanceId: run.providerInstanceId,
            occurredAt: now,
            payload: { ...attempt, status: "failed", completedAt: now },
          });
        }
        if (rootNode !== undefined) {
          events.push({
            id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
            type: "node.updated",
            threadId: projection.thread.id,
            runId: run.id,
            nodeId: rootNode.id,
            providerInstanceId: run.providerInstanceId,
            occurredAt: now,
            payload: { ...rootNode, status: "failed", completedAt: now },
          });
        }
        // App-owned delegations settle from their child results, independently
        // of this failed attempt. Match runtime recovery's ownership boundary.
        const isAppOwnedDelegation = (task: {
          readonly origin: "app_owned" | "provider_native";
          readonly childThreadId: ThreadId | null;
        }) => task.origin === "app_owned" && task.childThreadId !== null;
        const delegatedTaskNodeIds = new Set([
          ...projection.subagents.filter(isAppOwnedDelegation).map((task) => task.id),
          ...projection.turnItems.flatMap((item) =>
            item.type === "subagent" && isAppOwnedDelegation(item) ? [item.subagentId] : [],
          ),
        ]);
        for (const subagent of projection.subagents.filter(
          (candidate) =>
            candidate.runId === run.id &&
            !isAppOwnedDelegation(candidate) &&
            (candidate.status === "pending" ||
              candidate.status === "running" ||
              candidate.status === "waiting"),
        )) {
          events.push({
            id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
            type: "subagent.updated",
            threadId: projection.thread.id,
            runId: run.id,
            nodeId: subagent.id,
            driver: subagent.driver,
            providerInstanceId: subagent.providerInstanceId,
            occurredAt: now,
            payload: { ...subagent, status: "failed", completedAt: now, updatedAt: now },
          });
        }
        for (const node of projection.nodes.filter(
          (candidate) =>
            candidate.runId === run.id &&
            candidate.id !== run.rootNodeId &&
            !delegatedTaskNodeIds.has(candidate.id) &&
            (candidate.status === "pending" ||
              candidate.status === "running" ||
              candidate.status === "waiting"),
        )) {
          events.push({
            id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
            type: "node.updated",
            threadId: projection.thread.id,
            runId: run.id,
            nodeId: node.id,
            providerInstanceId: run.providerInstanceId,
            occurredAt: now,
            payload: { ...node, status: "failed", completedAt: now },
          });
        }
        for (const providerTurn of projection.providerTurns.filter(
          (candidate) =>
            candidate.runAttemptId !== null &&
            projection.attempts.some(
              (attemptRow) =>
                attemptRow.id === candidate.runAttemptId && attemptRow.runId === run.id,
            ) &&
            (candidate.status === "pending" || candidate.status === "running"),
        )) {
          events.push({
            id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
            type: "provider-turn.updated",
            threadId: projection.thread.id,
            runId: run.id,
            nodeId: providerTurn.nodeId,
            providerInstanceId: run.providerInstanceId,
            occurredAt: now,
            payload: { ...providerTurn, status: "cancelled", completedAt: now },
          });
        }
        for (const request of projection.runtimeRequests.filter(
          (candidate) =>
            candidate.status === "pending" &&
            projection.nodes.some((node) => node.id === candidate.nodeId && node.runId === run.id),
        )) {
          events.push({
            id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
            type: "runtime-request.updated",
            threadId: projection.thread.id,
            runId: run.id,
            nodeId: request.nodeId,
            providerInstanceId: run.providerInstanceId,
            occurredAt: now,
            payload: {
              ...request,
              status: "cancelled",
              responseCapability: {
                type: "not_resumable",
                reason: runtimeRequestCancellationReason,
              },
              resolvedAt: now,
            },
          });
        }
        for (const message of projection.messages.filter(
          (candidate) => candidate.runId === run.id && candidate.streaming,
        )) {
          events.push({
            id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
            type: "message.updated",
            threadId: projection.thread.id,
            runId: run.id,
            ...(message.nodeId === null ? {} : { nodeId: message.nodeId }),
            providerInstanceId: run.providerInstanceId,
            occurredAt: now,
            payload: { ...message, streaming: false, updatedAt: now },
          });
        }
        for (const item of projection.turnItems.filter(
          (candidate) =>
            candidate.runId === run.id &&
            !(candidate.type === "subagent" && isAppOwnedDelegation(candidate)) &&
            (candidate.status === "pending" ||
              candidate.status === "running" ||
              candidate.status === "waiting"),
        )) {
          events.push({
            id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
            type: "turn-item.updated",
            threadId: projection.thread.id,
            runId: run.id,
            ...(item.nodeId === null ? {} : { nodeId: item.nodeId }),
            providerInstanceId: run.providerInstanceId,
            occurredAt: now,
            payload: {
              ...item,
              ...("streaming" in item ? { streaming: false } : {}),
              status: "failed",
              completedAt: now,
              updatedAt: now,
            },
          });
        }
        // Linked child threads carry the interrupted attempt's routed rows,
        // usually with `runId: null`, and provider-native children have no
        // runs of their own, so neither the per-run sweep above nor startup
        // reconcile (which iterates per-run rows) ever settles them. Follow
        // lifetime linkage the way `cascadeTerminalizeRunOwnedSubagents`
        // does: child thread ids come from all of this run's subagent rows
        // and subagent turn items, terminal links included (a link can
        // terminalize before the child settles), recursing through nested
        // subagents. A child row that names a nonterminal run in its own
        // thread belongs to an independently live child (an app-owned
        // delegation) and is left alone; the same guard decides which nested
        // links to follow.
        const isNonterminalRunStatus = (status: string) =>
          status === "queued" ||
          status === "preparing" ||
          status === "starting" ||
          status === "running" ||
          status === "waiting";
        const isOpenRowStatus = (status: string) =>
          status === "pending" || status === "running" || status === "waiting";
        const visitedThreadIds = new Set<ThreadId>([projection.thread.id]);
        const childThreadQueue: Array<ThreadId> = [];
        const enqueueChildThread = (childThreadId: ThreadId | null) => {
          if (childThreadId === null || visitedThreadIds.has(childThreadId)) {
            return;
          }
          visitedThreadIds.add(childThreadId);
          childThreadQueue.push(childThreadId);
        };
        for (const subagent of projection.subagents) {
          if (subagent.runId === run.id) enqueueChildThread(subagent.childThreadId);
        }
        for (const item of projection.turnItems) {
          if (item.type === "subagent" && item.runId === run.id) {
            enqueueChildThread(item.childThreadId);
          }
        }
        while (childThreadQueue.length > 0) {
          const childThreadId = childThreadQueue.shift();
          if (childThreadId === undefined) break;
          const childResult = yield* Effect.result(
            projectionStore.getThreadProjection(childThreadId),
          );
          if (childResult._tag === "Failure") {
            if (childResult.failure._tag !== "ProjectionStoreThreadNotFoundError") {
              return yield* Effect.fail(childResult.failure);
            }
            continue;
          }
          const child = childResult.success;
          // Live app-owned threads can also carry session-scoped, runless work.
          if (child.runs.some((candidate) => isNonterminalRunStatus(candidate.status))) continue;
          const sweepable = (rowRunId: RunId | null) => {
            if (rowRunId === null || rowRunId === run.id) return true;
            const owningRun = child.runs.find((candidate) => candidate.id === rowRunId);
            return owningRun === undefined || !isNonterminalRunStatus(owningRun.status);
          };
          for (const subagent of child.subagents) {
            if (!sweepable(subagent.runId)) continue;
            enqueueChildThread(subagent.childThreadId);
            if (!isOpenRowStatus(subagent.status)) continue;
            events.push({
              id: yield* idAllocator.allocate.event({ threadId: childThreadId }),
              type: "subagent.updated",
              threadId: childThreadId,
              runId: subagent.runId ?? run.id,
              nodeId: subagent.id,
              driver: subagent.driver,
              providerInstanceId: subagent.providerInstanceId,
              occurredAt: now,
              payload: { ...subagent, status: "failed", completedAt: now, updatedAt: now },
            });
          }
          for (const item of child.turnItems) {
            if (!sweepable(item.runId)) continue;
            if (item.type === "subagent") enqueueChildThread(item.childThreadId);
            if (!isOpenRowStatus(item.status)) continue;
            events.push({
              id: yield* idAllocator.allocate.event({ threadId: childThreadId }),
              type: "turn-item.updated",
              threadId: childThreadId,
              runId: item.runId ?? run.id,
              ...(item.nodeId === null ? {} : { nodeId: item.nodeId }),
              providerInstanceId: run.providerInstanceId,
              occurredAt: now,
              payload: {
                ...item,
                ...("streaming" in item ? { streaming: false } : {}),
                status: "failed",
                completedAt: now,
                updatedAt: now,
              },
            });
          }
          for (const node of child.nodes) {
            if (!sweepable(node.runId) || !isOpenRowStatus(node.status)) continue;
            events.push({
              id: yield* idAllocator.allocate.event({ threadId: childThreadId }),
              type: "node.updated",
              threadId: childThreadId,
              runId: node.runId ?? run.id,
              nodeId: node.id,
              providerInstanceId: run.providerInstanceId,
              occurredAt: now,
              payload: { ...node, status: "failed", completedAt: now },
            });
          }
          for (const providerTurn of child.providerTurns) {
            if (providerTurn.status !== "pending" && providerTurn.status !== "running") {
              continue;
            }
            const node = child.nodes.find((candidate) => candidate.id === providerTurn.nodeId);
            const attempt =
              providerTurn.runAttemptId === null
                ? undefined
                : child.attempts.find((candidate) => candidate.id === providerTurn.runAttemptId);
            const ownerRunIds = [
              ...(node === undefined ? [] : [node.runId]),
              ...(attempt === undefined ? [] : [attempt.runId]),
            ];
            if (
              ownerRunIds.length === 0 ||
              ownerRunIds.some((ownerRunId) => !sweepable(ownerRunId))
            ) {
              continue;
            }
            events.push({
              id: yield* idAllocator.allocate.event({ threadId: childThreadId }),
              type: "provider-turn.updated",
              threadId: childThreadId,
              runId: attempt?.runId ?? node?.runId ?? run.id,
              nodeId: providerTurn.nodeId,
              providerInstanceId: run.providerInstanceId,
              occurredAt: now,
              payload: { ...providerTurn, status: "cancelled", completedAt: now },
            });
          }
          for (const request of child.runtimeRequests) {
            const node = child.nodes.find((candidate) => candidate.id === request.nodeId);
            if (request.status !== "pending" || node === undefined || !sweepable(node.runId)) {
              continue;
            }
            events.push({
              id: yield* idAllocator.allocate.event({ threadId: childThreadId }),
              type: "runtime-request.updated",
              threadId: childThreadId,
              runId: node.runId ?? run.id,
              nodeId: request.nodeId,
              providerInstanceId: run.providerInstanceId,
              occurredAt: now,
              payload: {
                ...request,
                status: "cancelled",
                responseCapability: {
                  type: "not_resumable",
                  reason: runtimeRequestCancellationReason,
                },
                resolvedAt: now,
              },
            });
          }
          for (const message of child.messages) {
            if (!message.streaming || !sweepable(message.runId)) continue;
            events.push({
              id: yield* idAllocator.allocate.event({ threadId: childThreadId }),
              type: "message.updated",
              threadId: childThreadId,
              ...(message.runId === null ? {} : { runId: message.runId }),
              ...(message.nodeId === null ? {} : { nodeId: message.nodeId }),
              providerInstanceId: run.providerInstanceId,
              occurredAt: now,
              payload: { ...message, streaming: false, updatedAt: now },
            });
          }
        }
        if (providerThread !== undefined) {
          events.push({
            id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
            type: "turn-item.updated",
            threadId: projection.thread.id,
            runId: run.id,
            ...(rootNode === undefined ? {} : { nodeId: rootNode.id }),
            providerInstanceId: run.providerInstanceId,
            occurredAt: now,
            payload: {
              ...makeProviderFailureTurnItem({
                idAllocator,
                driver: providerThread.driver,
                threadId: projection.thread.id,
                runId: run.id,
                nodeId: rootNode?.id ?? null,
                providerThreadId: providerThread.id,
                providerTurnId:
                  attempt?.providerTurnId ??
                  idAllocator.derive.providerTurn({
                    driver: providerThread.driver,
                    nativeTurnId: `failed:${activeAttemptId}`,
                  }),
                itemOrdinal: Math.max(0, ...projection.turnItems.map((item) => item.ordinal)) + 1,
                failure,
                occurredAt: now,
              }),
              ...(input.openFailure === undefined
                ? {}
                : {
                    id: idAllocator.derive.runSignalTurnItem({
                      runId: run.id,
                      signal: input.openFailure.signal,
                    }),
                    title: input.openFailure.title,
                    providerTurnId: null,
                  }),
            },
          });
        }
        if (providerThread !== undefined && providerThread.status === "active") {
          // This rides the same guarded commit, so it only lands while the run
          // is still `starting` on our attempt and the binding is still ours.
          events.push({
            id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
            type: "provider-thread.updated",
            threadId: projection.thread.id,
            driver: providerThread.driver,
            providerInstanceId: providerThread.providerInstanceId,
            occurredAt: now,
            payload: { ...providerThread, status: "idle", updatedAt: now },
          });
        }
        events.push({
          id: yield* idAllocator.allocate.event({ threadId: projection.thread.id }),
          type: "run.updated",
          threadId: projection.thread.id,
          runId: run.id,
          ...(rootNode === undefined ? {} : { nodeId: rootNode.id }),
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          // Omission preserves the current cohort in both SQL and event replay;
          // this snapshot may predate a child's completion reservation.
          payload: {
            ...Struct.omit(run, ["delegatedCompletion"]),
            status: "failed",
            queuePosition: null,
            completedAt: now,
          },
        });
        yield* eventSink.writeIfRunCurrent({
          threadId: projection.thread.id,
          runId: run.id,
          activeAttemptId,
          expectedStatus: "starting",
          events,
        });
      },
    );

    return ProviderTurnStartServiceV2.of({
      start: (input) =>
        start(input).pipe(
          Effect.mapError((cause) =>
            isProviderTurnStartError(cause)
              ? cause
              : new ProviderTurnStartError({ runId: input.runId, cause }),
          ),
        ),
      failFromDeadLetter: (input) =>
        settleFailedStart(input).pipe(
          Effect.mapError((cause) =>
            isProviderTurnStartError(cause)
              ? cause
              : new ProviderTurnStartError({ runId: input.runId, cause }),
          ),
        ),
    });
  }),
);
