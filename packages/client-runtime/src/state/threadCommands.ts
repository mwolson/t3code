import type { ThreadId } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { Atom } from "effect/unstable/reactivity";

import { createAtomCommandScheduler, createEnvironmentCommand } from "./runtime.ts";
import {
  type ArchiveThreadInput,
  type CreateThreadInput,
  type DeleteThreadInput,
  type InterruptThreadTurnInput,
  type ForkThreadFromRunInput,
  type MergeThreadBackInput,
  type PromoteQueuedRunInput,
  type ReorderQueuedRunInput,
  type RespondToThreadApprovalInput,
  type RespondToThreadUserInputInput,
  type RevertThreadCheckpointInput,
  type SetThreadInteractionModeInput,
  type SetThreadRuntimeModeInput,
  type SettleThreadInput,
  type StartThreadTurnInput,
  type StopThreadSessionInput,
  type UnarchiveThreadInput,
  type UnsettleThreadInput,
  type UpdateThreadMetadataInput,
  archiveThread,
  createThread,
  deleteThread,
  interruptThreadTurn,
  forkThreadFromRun,
  mergeThreadBack,
  promoteQueuedRun,
  reorderQueuedRun,
  respondToThreadApproval,
  respondToThreadUserInput,
  revertThreadCheckpoint,
  setThreadInteractionMode,
  setThreadRuntimeMode,
  settleThread,
  startThreadTurn,
  stopThreadSession,
  unarchiveThread,
  unsettleThread,
  updateThreadMetadata,
} from "../operations/commands.ts";
import type { EnvironmentRegistry } from "../connection/registry.ts";
import { EnvironmentSupervisor } from "../connection/supervisor.ts";
import {
  ThreadHistoryController,
  type ThreadHistoryLoadEarlierResult,
} from "./threadHistoryController.ts";

export type LoadEarlierThreadHistoryInput = {
  readonly threadId: ThreadId;
};

export type {
  ArchiveThreadInput,
  CreateThreadInput,
  DeleteThreadInput,
  InterruptThreadTurnInput,
  ForkThreadFromRunInput,
  MergeThreadBackInput,
  PromoteQueuedRunInput,
  ReorderQueuedRunInput,
  RespondToThreadApprovalInput,
  RespondToThreadUserInputInput,
  RevertThreadCheckpointInput,
  SetThreadInteractionModeInput,
  SetThreadRuntimeModeInput,
  SettleThreadInput,
  StartThreadTurnInput,
  StopThreadSessionInput,
  ThreadCommandInput,
  UnarchiveThreadInput,
  UnsettleThreadInput,
  UpdateThreadMetadataInput,
} from "../operations/commands.ts";

export function createThreadEnvironmentAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | Crypto.Crypto | R, E>,
) {
  const scheduler = createAtomCommandScheduler();
  const concurrency = {
    mode: "serial" as const,
    key: ({ environmentId, input }: { environmentId: string; input: { threadId: string } }) =>
      JSON.stringify([environmentId, input.threadId]),
  };
  return {
    create: createEnvironmentCommand(runtime, {
      label: "environment-data:commands:thread:create",
      execute: (input: CreateThreadInput) => createThread(input),
      scheduler,
      concurrency,
    }),
    delete: createEnvironmentCommand(runtime, {
      label: "environment-data:commands:thread:delete",
      execute: (input: DeleteThreadInput) => deleteThread(input),
      scheduler,
      concurrency,
    }),
    archive: createEnvironmentCommand(runtime, {
      label: "environment-data:commands:thread:archive",
      execute: (input: ArchiveThreadInput) => archiveThread(input),
      scheduler,
      concurrency,
    }),
    unarchive: createEnvironmentCommand(runtime, {
      label: "environment-data:commands:thread:unarchive",
      execute: (input: UnarchiveThreadInput) => unarchiveThread(input),
      scheduler,
      concurrency,
    }),
    settle: createEnvironmentCommand(runtime, {
      label: "environment-data:commands:thread:settle",
      execute: (input: SettleThreadInput) => settleThread(input),
      scheduler,
      concurrency,
    }),
    unsettle: createEnvironmentCommand(runtime, {
      label: "environment-data:commands:thread:unsettle",
      execute: (input: UnsettleThreadInput) => unsettleThread(input),
      scheduler,
      concurrency,
    }),
    updateMetadata: createEnvironmentCommand(runtime, {
      label: "environment-data:commands:thread:update-metadata",
      execute: (input: UpdateThreadMetadataInput) => updateThreadMetadata(input),
      scheduler,
      concurrency,
    }),
    setRuntimeMode: createEnvironmentCommand(runtime, {
      label: "environment-data:commands:thread:set-runtime-mode",
      execute: (input: SetThreadRuntimeModeInput) => setThreadRuntimeMode(input),
      scheduler,
      concurrency,
    }),
    setInteractionMode: createEnvironmentCommand(runtime, {
      label: "environment-data:commands:thread:set-interaction-mode",
      execute: (input: SetThreadInteractionModeInput) => setThreadInteractionMode(input),
      scheduler,
      concurrency,
    }),
    startTurn: createEnvironmentCommand(runtime, {
      label: "environment-data:commands:thread:start-turn",
      execute: (input: StartThreadTurnInput) => startThreadTurn(input),
      scheduler,
      concurrency,
    }),
    interruptTurn: createEnvironmentCommand(runtime, {
      label: "environment-data:commands:thread:interrupt-turn",
      execute: (input: InterruptThreadTurnInput) => interruptThreadTurn(input),
      scheduler,
      concurrency,
    }),
    respondToApproval: createEnvironmentCommand(runtime, {
      label: "environment-data:commands:thread:respond-to-approval",
      execute: (input: RespondToThreadApprovalInput) => respondToThreadApproval(input),
      scheduler,
      concurrency,
    }),
    respondToUserInput: createEnvironmentCommand(runtime, {
      label: "environment-data:commands:thread:respond-to-user-input",
      execute: (input: RespondToThreadUserInputInput) => respondToThreadUserInput(input),
      scheduler,
      concurrency,
    }),
    revertCheckpoint: createEnvironmentCommand(runtime, {
      label: "environment-data:commands:thread:revert-checkpoint",
      execute: (input: RevertThreadCheckpointInput) => revertThreadCheckpoint(input),
      scheduler,
      concurrency,
    }),
    stopSession: createEnvironmentCommand(runtime, {
      label: "environment-data:commands:thread:stop-session",
      execute: (input: StopThreadSessionInput) => stopThreadSession(input),
      scheduler,
      concurrency,
    }),
    forkFromRun: createEnvironmentCommand(runtime, {
      label: "environment-data:commands:thread:fork-from-run",
      execute: (input: ForkThreadFromRunInput) => forkThreadFromRun(input),
      scheduler,
      concurrency: {
        mode: "serial",
        key: ({ environmentId, input }) => JSON.stringify([environmentId, input.sourceThreadId]),
      },
    }),
    mergeBack: createEnvironmentCommand(runtime, {
      label: "environment-data:commands:thread:merge-back",
      execute: (input: MergeThreadBackInput) => mergeThreadBack(input),
      scheduler,
      concurrency: {
        mode: "serial",
        key: ({ environmentId, input }) =>
          JSON.stringify([environmentId, input.sourceThreadId, input.targetThreadId]),
      },
    }),
    reorderQueuedRun: createEnvironmentCommand(runtime, {
      label: "environment-data:commands:thread:reorder-queued-run",
      execute: (input: ReorderQueuedRunInput) => reorderQueuedRun(input),
      scheduler,
      concurrency,
    }),
    promoteQueuedRun: createEnvironmentCommand(runtime, {
      label: "environment-data:commands:thread:promote-queued-run",
      execute: (input: PromoteQueuedRunInput) => promoteQueuedRun(input),
      scheduler,
      concurrency,
    }),
    loadEarlierHistory: createEnvironmentCommand(runtime, {
      label: "environment-data:commands:thread:load-earlier-history",
      execute: (input: LoadEarlierThreadHistoryInput) =>
        Effect.gen(function* () {
          const supervisor = yield* EnvironmentSupervisor;
          const controller = yield* Effect.serviceOption(ThreadHistoryController);
          if (Option.isNone(controller)) {
            return { _tag: "noop" } satisfies ThreadHistoryLoadEarlierResult;
          }
          return yield* controller.value.loadEarlier(
            supervisor.target.environmentId,
            input.threadId,
          );
        }),
      scheduler,
      concurrency: {
        mode: "serial",
        key: ({ environmentId, input }) => JSON.stringify([environmentId, input.threadId]),
      },
    }),
  };
}
