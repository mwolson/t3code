import {
  CommandId,
  MessageId,
  type OrchestrationV2Command,
  type OrchestrationV2Subagent,
  type RunId,
  type RuntimeRequestId,
  type ThreadId,
} from "@t3tools/contracts";
import { reportLabel } from "@t3tools/provider-core/server/notification";

export function questionWakeMessageId(
  parentThreadId: ThreadId,
  requestId: RuntimeRequestId,
  environmentId?: string,
): MessageId {
  const remote = environmentId === undefined ? "" : `${environmentId}:`;
  return MessageId.make(`message:delegated-task-question:${parentThreadId}:${remote}${requestId}`);
}

export function questionWake(
  task: OrchestrationV2Subagent,
  request: {
    readonly id: RuntimeRequestId;
    readonly responseMode: "live" | "message" | "not_resumable";
  },
): OrchestrationV2Command {
  const childThreadId = task.remoteChild?.threadId ?? task.childThreadId!;
  const environmentId = task.remoteChild?.environmentId;
  const messageId = questionWakeMessageId(task.threadId, request.id, environmentId);
  const label = reportLabel(task.title?.trim() || task.prompt);
  const situation =
    request.responseMode === "message"
      ? `Delegated task ${task.id} asked a question. Its answer will arrive as a message.`
      : `Delegated task ${task.id} is blocked on a question and cannot continue until it is answered.`;
  const target = environmentId === undefined ? "" : `environmentId ${environmentId}, `;
  return {
    type: "message.dispatch",
    commandId: CommandId.make(`server:${messageId.slice("message:".length)}`),
    threadId: task.threadId,
    messageId,
    text: `${situation} Read it with t3_pending_request action read, ${target}threadId ${childThreadId}, and requestId ${request.id}. Answer it with action respond using the same target when the answer follows from the task or the user's instructions; otherwise ask your own question, then answer it. If it is no longer pending, it was answered or dismissed.`,
    notification: {
      source: {
        kind: "delegated_task",
        taskIds: [task.id],
        ...(environmentId === undefined ? { childThreadId } : {}),
      },
      outcome: "updated",
      summary: `Delegated task${label === undefined ? "" : ` "${label}"`} has a question`,
    },
    attachments: [],
    dispatchMode: { type: "queue_after_active" },
    createdBy: "agent",
    creationSource: "server",
  };
}

export function cancelQuestionWake(
  threadId: ThreadId,
  messageId: MessageId,
  runId: RunId,
): OrchestrationV2Command {
  return {
    type: "queued-run.cancel",
    commandId: CommandId.make(
      `server:delegated-task-question-cancel:${messageId.slice("message:delegated-task-question:".length)}`,
    ),
    threadId,
    runId,
  };
}
