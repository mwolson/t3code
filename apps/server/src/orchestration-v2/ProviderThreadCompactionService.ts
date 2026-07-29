import { CommandId, ProviderThreadId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { EventSinkV2 } from "./EventSink.ts";
import { IdAllocatorV2 } from "./IdAllocator.ts";
import { ProjectionStoreV2 } from "./ProjectionStore.ts";
import { ProviderSessionManagerV2 } from "./ProviderSessionManager.ts";
import { RuntimePolicyV2 } from "./RuntimePolicy.ts";

export class ProviderThreadCompactionExecutionError extends Schema.TaggedErrorClass<ProviderThreadCompactionExecutionError>()(
  "ProviderThreadCompactionExecutionError",
  {
    threadId: ThreadId,
    providerThreadId: ProviderThreadId,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

const isProviderThreadCompactionExecutionError = Schema.is(ProviderThreadCompactionExecutionError);

export interface ProviderThreadCompactionServiceV2Shape {
  readonly execute: (input: {
    readonly commandId: CommandId;
    readonly threadId: ThreadId;
    readonly providerThreadId: ProviderThreadId;
  }) => Effect.Effect<void, ProviderThreadCompactionExecutionError>;
}

export class ProviderThreadCompactionServiceV2 extends Context.Service<
  ProviderThreadCompactionServiceV2,
  ProviderThreadCompactionServiceV2Shape
>()("t3/orchestration-v2/ProviderThreadCompactionService/ProviderThreadCompactionServiceV2") {}

export const layer: Layer.Layer<
  ProviderThreadCompactionServiceV2,
  never,
  EventSinkV2 | IdAllocatorV2 | ProjectionStoreV2 | ProviderSessionManagerV2 | RuntimePolicyV2
> = Layer.effect(
  ProviderThreadCompactionServiceV2,
  Effect.gen(function* () {
    const eventSink = yield* EventSinkV2;
    const ids = yield* IdAllocatorV2;
    const projections = yield* ProjectionStoreV2;
    const sessions = yield* ProviderSessionManagerV2;
    const runtimePolicy = yield* RuntimePolicyV2;

    const execute = Effect.fn("orchestrationV2.providerThreadCompaction.execute")(
      function* (input: {
        readonly commandId: CommandId;
        readonly threadId: ThreadId;
        readonly providerThreadId: ProviderThreadId;
      }) {
        const projection = yield* projections.getThreadProjection(input.threadId);
        const providerThread = projection.providerThreads.find(
          (candidate) => candidate.id === input.providerThreadId,
        );
        if (providerThread === undefined || providerThread.providerSessionId === null) {
          return yield* new ProviderThreadCompactionExecutionError({
            threadId: input.threadId,
            providerThreadId: input.providerThreadId,
            cause: "The persisted compaction target is incomplete or no longer valid.",
          });
        }

        const modelSelection = projection.thread.modelSelection;
        if (modelSelection.instanceId !== providerThread.providerInstanceId) {
          return yield* new ProviderThreadCompactionExecutionError({
            threadId: input.threadId,
            providerThreadId: input.providerThreadId,
            cause: "The active model selection no longer owns the compaction target.",
          });
        }
        const resolvedRuntimePolicy = yield* runtimePolicy.resolve({
          thread: projection.thread,
          modelSelection,
        });
        const existingSession = projection.providerSessions.find(
          (candidate) => candidate.id === providerThread.providerSessionId,
        );
        const session = yield* sessions.open({
          threadId: input.threadId,
          providerSessionId: providerThread.providerSessionId,
          modelSelection,
          runtimePolicy: resolvedRuntimePolicy,
          ...(existingSession === undefined ? {} : { resumeFromSession: existingSession }),
        });
        const compactThread = session.compactThread;
        if (compactThread === undefined) {
          return yield* new ProviderThreadCompactionExecutionError({
            threadId: input.threadId,
            providerThreadId: input.providerThreadId,
            cause: `${session.driver} does not implement manual compaction.`,
          });
        }
        const resumedThread = yield* session.resumeThread({
          providerThread,
          threadId: input.threadId,
          modelSelection,
          runtimePolicy: resolvedRuntimePolicy,
        });
        yield* compactThread(resumedThread);

        const now = yield* DateTime.now;
        yield* eventSink.write({
          commandId: input.commandId,
          events: [
            {
              id: yield* ids.allocate.event({ threadId: input.threadId }),
              type: "provider-thread.compacted",
              threadId: input.threadId,
              driver: resumedThread.driver,
              providerInstanceId: resumedThread.providerInstanceId,
              occurredAt: now,
              payload: {
                ...resumedThread,
                id: providerThread.id,
                appThreadId: input.threadId,
                status: "idle",
                updatedAt: now,
              },
            },
          ],
        });
      },
    );

    return ProviderThreadCompactionServiceV2.of({
      execute: (input) =>
        execute(input).pipe(
          Effect.mapError((cause) =>
            isProviderThreadCompactionExecutionError(cause)
              ? cause
              : new ProviderThreadCompactionExecutionError({
                  threadId: input.threadId,
                  providerThreadId: input.providerThreadId,
                  cause,
                }),
          ),
        ),
    });
  }),
);
