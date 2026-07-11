import type {
  OrchestrationV2ThreadProjection,
  ProviderRequestKind,
  RuntimeRequestId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

export interface ThreadPendingApproval {
  readonly requestId: RuntimeRequestId;
  readonly requestKind: ProviderRequestKind;
  readonly createdAt: string;
  readonly detail?: string;
  readonly responseCapability: "live" | "not_resumable";
}

export interface ThreadUserInputQuestion {
  readonly id: string;
  readonly header: string;
  readonly question: string;
  readonly options: ReadonlyArray<{
    readonly label: string;
    readonly description: string;
  }>;
  readonly multiSelect: boolean;
}

export interface ThreadPendingUserInput {
  readonly requestId: RuntimeRequestId;
  readonly createdAt: string;
  readonly questions: ReadonlyArray<ThreadUserInputQuestion>;
  readonly responseCapability: "live" | "not_resumable";
}

export interface PendingThreadRequests {
  readonly approvals: ReadonlyArray<ThreadPendingApproval>;
  readonly userInputs: ReadonlyArray<ThreadPendingUserInput>;
}

/** Joins pending request entities to the request items that carry display data. */
export function derivePendingThreadRequests(
  projection: OrchestrationV2ThreadProjection,
): PendingThreadRequests {
  const approvals: ThreadPendingApproval[] = [];
  const userInputs: ThreadPendingUserInput[] = [];

  for (const request of projection.runtimeRequests) {
    if (request.status !== "pending") continue;
    const responseCapability = request.responseCapability.type;
    if (request.kind === "user_input") {
      let item: (typeof projection.turnItems)[number] | undefined;
      for (let index = projection.turnItems.length - 1; index >= 0; index -= 1) {
        const candidate = projection.turnItems[index];
        if (candidate?.type === "user_input_request" && candidate.requestId === request.id) {
          item = candidate;
          break;
        }
      }
      if (item === undefined || item.type !== "user_input_request") continue;
      userInputs.push({
        requestId: request.id,
        createdAt: DateTime.formatIso(request.createdAt),
        questions: item.questions.map((question) => ({ ...question, multiSelect: false })),
        responseCapability,
      });
      continue;
    }

    if (request.kind === "auth_refresh" || request.kind === "dynamic_tool_call") continue;
    let item: (typeof projection.turnItems)[number] | undefined;
    for (let index = projection.turnItems.length - 1; index >= 0; index -= 1) {
      const candidate = projection.turnItems[index];
      if (candidate?.type === "approval_request" && candidate.requestId === request.id) {
        item = candidate;
        break;
      }
    }
    approvals.push({
      requestId: request.id,
      requestKind: request.kind,
      createdAt: DateTime.formatIso(request.createdAt),
      ...(item?.type === "approval_request" && item.prompt ? { detail: item.prompt } : {}),
      responseCapability,
    });
  }

  return { approvals, userInputs };
}
