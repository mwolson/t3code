import { MessageId, ThreadId, OrchestratorMcpFailure, ProjectId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as ThreadLaunch from "../../../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadMessageIntake from "../../../orchestration-v2/ThreadMessageIntake.ts";
import * as Claims from "../../../orchestration-v2/AttachmentClaims.ts";
import * as Project from "../../../project/ProjectService.ts";
import * as Repositories from "../../../sourceControl/SourceControlRepositoryService.ts";
import { OrchestratorMcpService } from "../../OrchestratorMcpService.ts";
import { newCommandId, readCaller, readMutationCaller, unavailable } from "../../threadAccess.ts";
import { ProjectToolkit } from "./tools.ts";

function projectFailure(error: Project.ProjectServiceError) {
  if (error._tag === "ProjectOperationError") return unavailable();
  const message =
    error._tag === "ProjectNotFoundError"
      ? "The project was not found."
      : error._tag === "ProjectConflictError"
        ? "The workspace is already registered to a project."
        : "The project is not empty; force=true is required to delete it.";
  return new OrchestratorMcpFailure({ code: "invalid_request", message });
}

const isOrchestratorMcpFailure = Schema.is(OrchestratorMcpFailure);

function launchFailure(error: Claims.AttachmentClaimError | ThreadLaunch.ThreadLaunchError) {
  if (error._tag === "AttachmentClaimError")
    return new OrchestratorMcpFailure({ code: "orchestration_error", message: error.message });
  if (error.operation === "record-creation" && isOrchestratorMcpFailure(error.cause))
    return error.cause;
  return unavailable();
}

const access = Effect.gen(function* () {
  yield* readCaller();
  return yield* Project.ProjectService;
});
const mutation = Effect.gen(function* () {
  const { caller } = yield* readMutationCaller();
  if (
    caller.archivedAt !== null ||
    caller.runtimeMode !== "full-access" ||
    caller.interactionMode !== "default"
  )
    return yield* new OrchestratorMcpFailure({
      code: "capability_denied",
      message: "Project changes require a live full-access/default calling thread.",
    });
  return yield* Project.ProjectService;
});
export const ProjectHandlersLive = ProjectToolkit.toLayer({
  t3_thread_launch: (input) =>
    Effect.gen(function* () {
      const { scope, caller } = yield* readMutationCaller();
      if (caller.runtimeMode !== "full-access" || caller.interactionMode !== "default")
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "Project launches require a full-access/default calling thread.",
        });
      // The caller's grant lets it reach the launched thread in any project.
      const recordCreation = yield* (yield* OrchestratorMcpService).launchedThreadGrant(scope);
      const commandId = yield* newCommandId();
      const threadId = ThreadId.make(commandId);
      const messageId = MessageId.make(commandId);
      const attachments = input.attachments ?? [];
      if (attachments.some((attachment) => !Claims.attachmentIsPendingUpload(attachment)))
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "A new thread accepts only pending attachment uploads.",
        });
      const result = yield* ThreadMessageIntake.launchThread({
        commandId,
        threadId,
        projectId: input.projectId ?? caller.projectId,
        title: input.title,
        modelSelection: input.modelSelection ?? caller.modelSelection,
        runtimeMode: input.runtimeMode ?? caller.runtimeMode,
        interactionMode: input.interactionMode ?? caller.interactionMode,
        workspaceStrategy: input.workspaceStrategy ?? { type: "root" },
        ...(input.message === undefined && attachments.length === 0
          ? {}
          : {
              initialMessage: {
                messageId,
                text: input.message ?? "",
                attachments,
              },
            }),
        createdBy: "agent",
        creationSource: "mcp",
        onThreadCreated: (createdThreadId) => recordCreation(createdThreadId, null),
      }).pipe(Effect.mapError(launchFailure));
      const thread = result.projection.thread;
      const run = result.projection.runs.find((run) => run.userMessageId === messageId);
      if (run !== undefined) {
        yield* recordCreation(thread.id, run.id).pipe(
          Effect.retry({
            times: 3,
            schedule: Schedule.exponential("25 millis"),
            while: (error) => error.code === "orchestration_error",
          }),
          Effect.catch((error) => {
            if (error.code !== "orchestration_error") return Effect.fail(error);
            return Effect.logWarning("orchestrator-mcp.thread-launch.run-link-failed", {
              threadId: thread.id,
              runId: run.id,
              code: error.code,
            });
          }),
        );
      }
      return {
        threadId: thread.id,
        projectId: thread.projectId,
        modelSelection: thread.modelSelection,
        runId: run?.id ?? null,
        status: run?.status ?? null,
      };
    }),
  t3_project_list: (input) =>
    Effect.gen(function* () {
      const projects = yield* access;
      const snapshot = yield* projects.snapshot.pipe(Effect.mapError(unavailable));
      const rows = snapshot.projects.filter((project) => project.deletedAt === null);
      const start = input.cursor ?? 0,
        end = start + (input.limit ?? 20);
      return { projects: rows.slice(start, end), nextCursor: end < rows.length ? end : null };
    }),
  t3_project_read: (input) =>
    Effect.gen(function* () {
      const projects = yield* access;
      const result = yield* projects.getById(input.projectId).pipe(Effect.mapError(unavailable));
      if (Option.isNone(result))
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "The project was not found.",
        });
      return result.value;
    }),
  t3_project_create: (input) =>
    Effect.gen(function* () {
      const projects = yield* mutation;
      const commandId = yield* newCommandId();
      return yield* projects
        .create({ ...input, commandId, projectId: ProjectId.make(commandId) })
        .pipe(Effect.mapError(projectFailure));
    }),
  t3_project_update: (input) =>
    Effect.gen(function* () {
      const projects = yield* mutation;
      return yield* projects
        .update({ ...input, commandId: yield* newCommandId() })
        .pipe(Effect.mapError(projectFailure));
    }),
  t3_project_delete: (input) =>
    Effect.gen(function* () {
      const projects = yield* mutation;
      return yield* projects
        .delete({ ...input, commandId: yield* newCommandId() })
        .pipe(Effect.mapError(projectFailure));
    }),
  t3_project_clone: (input) =>
    Effect.gen(function* () {
      yield* mutation;
      const repositories = yield* Repositories.SourceControlRepositoryService;
      return yield* repositories.cloneRepository(input).pipe(
        Effect.mapError(
          (error) =>
            new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: error.detail,
            }),
        ),
      );
    }),
});
