import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  EnvironmentId,
  NodeId,
  type Project,
  ProjectId,
  ProviderInstanceId,
  RunId,
  type ServerProvider,
  ThreadId,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as NodeOS from "node:os";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";

import { ThreadManagementService } from "../orchestration-v2/ThreadManagementService.ts";
import { ProviderRegistry } from "../provider/Services/ProviderRegistry.ts";
import * as ProjectService from "../project/ProjectService.ts";
import { ScheduledTaskService } from "../scheduledTasks/ScheduledTaskService.ts";
import type { McpInvocationScope } from "./McpInvocationContext.ts";
import * as OrchestratorMcpService from "./OrchestratorMcpService.ts";

describe("OrchestratorMcpService", () => {
  it.effect("retries terminal acknowledgement with a fresh command id", () =>
    Effect.gen(function* () {
      const parentThreadId = ThreadId.make("thread:mcp-ack-parent");
      const childThreadId = ThreadId.make("thread:mcp-ack-child");
      const childRunId = RunId.make("run:mcp-ack-child");
      const taskId = NodeId.make("node:mcp-ack-task");
      const acknowledgementCommandIds = yield* Ref.make<ReadonlyArray<string>>([]);
      const acknowledgementAttempts = yield* Ref.make(0);
      const parentProjection = {
        thread: { id: parentThreadId },
        runs: [],
        contextTransfers: [],
        subagents: [
          {
            id: taskId,
            threadId: parentThreadId,
            origin: "app_owned",
            childThreadId,
            driver: "codex",
            model: "gpt-5.6-terra",
            result: "terminal result",
            completionDelivery: { state: "pending" },
          },
        ],
      } as unknown as OrchestrationV2ThreadProjection;
      const childProjection = {
        thread: { id: childThreadId },
        runs: [{ id: childRunId, ordinal: 1, status: "completed" }],
        contextTransfers: [],
      } as unknown as OrchestrationV2ThreadProjection;
      const dependencies = Layer.mergeAll(
        NodeServices.layer,
        Layer.mock(ThreadManagementService)({
          getThreadProjection: (threadId) =>
            Effect.succeed(threadId === parentThreadId ? parentProjection : childProjection),
          dispatch: (command) =>
            Ref.update(acknowledgementCommandIds, (commandIds) => [
              ...commandIds,
              String(command.commandId),
            ]).pipe(
              Effect.andThen(Ref.updateAndGet(acknowledgementAttempts, (count) => count + 1)),
              Effect.flatMap((attempt) =>
                attempt === 1
                  ? Effect.fail(new Error("simulated acknowledgement failure") as never)
                  : Effect.succeed({} as never),
              ),
            ),
        }),
        Layer.mock(ProviderRegistry)({ getProviders: Effect.succeed([]) }),
        Layer.mock(ScheduledTaskService)({}),
        Layer.mock(ProjectService.ProjectService)({}),
      );
      const scope: McpInvocationScope = {
        environmentId: EnvironmentId.make("environment:mcp-ack"),
        threadId: parentThreadId,
        providerSessionId: "provider-session:mcp-ack",
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: new Set(["orchestration"]),
        issuedAt: 1,
      };

      yield* Effect.gen(function* () {
        const service = yield* OrchestratorMcpService.OrchestratorMcpService;
        const error = yield* service.taskStatus(scope, taskId).pipe(Effect.flip);
        assert.equal(error.code, "orchestration_error");

        const result = yield* service.taskStatus(scope, taskId);
        assert.equal(result.status, "completed");
        assert.equal(result.summary, "terminal result");
        const commandIds = yield* Ref.get(acknowledgementCommandIds);
        assert.equal(commandIds.length, 2);
        assert.notEqual(commandIds[0], commandIds[1]);
      }).pipe(Effect.provide(OrchestratorMcpService.layer.pipe(Layer.provide(dependencies))));
    }),
  );

  it.effect("does not dispose delivery when a nonterminal task has no active child run", () =>
    Effect.gen(function* () {
      const parentThreadId = ThreadId.make("thread:mcp-cancel-parent");
      const childThreadId = ThreadId.make("thread:mcp-cancel-child");
      const taskId = NodeId.make("node:mcp-cancel-task");
      const dispatched = yield* Ref.make<ReadonlyArray<unknown>>([]);
      const parentProjection = {
        thread: { id: parentThreadId },
        runs: [],
        contextTransfers: [],
        subagents: [
          {
            id: taskId,
            threadId: parentThreadId,
            origin: "app_owned",
            childThreadId,
            driver: "codex",
            model: "gpt-5.6-terra",
            result: null,
            completionDelivery: { state: "pending" },
          },
        ],
      } as unknown as OrchestrationV2ThreadProjection;
      const childProjection = {
        thread: { id: childThreadId },
        runs: [],
        contextTransfers: [],
      } as unknown as OrchestrationV2ThreadProjection;
      const dependencies = Layer.mergeAll(
        NodeServices.layer,
        Layer.mock(ThreadManagementService)({
          getThreadProjection: (threadId) =>
            Effect.succeed(threadId === parentThreadId ? parentProjection : childProjection),
          dispatch: (command) =>
            Ref.update(dispatched, (commands) => [...commands, command]).pipe(
              Effect.as({} as never),
            ),
        }),
        Layer.mock(ProviderRegistry)({ getProviders: Effect.succeed([]) }),
        Layer.mock(ScheduledTaskService)({}),
        Layer.mock(ProjectService.ProjectService)({}),
      );
      const scope: McpInvocationScope = {
        environmentId: EnvironmentId.make("environment:mcp-cancel"),
        threadId: parentThreadId,
        providerSessionId: "provider-session:mcp-cancel",
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: new Set(["orchestration"]),
        issuedAt: 1,
      };

      yield* Effect.gen(function* () {
        const service = yield* OrchestratorMcpService.OrchestratorMcpService;
        const error = yield* service
          .cancelTask(scope, { taskId, clientRequestId: "cancel-unstarted-task" })
          .pipe(Effect.flip);
        assert.equal(error.code, "task_not_cancellable");
        assert.deepEqual(yield* Ref.get(dispatched), []);
      }).pipe(Effect.provide(OrchestratorMcpService.layer.pipe(Layer.provide(dependencies))));
    }),
  );

  it.effect("does not dispose delivery when the child interrupt fails", () =>
    Effect.gen(function* () {
      const parentThreadId = ThreadId.make("thread:mcp-cancel-failed-parent");
      const childThreadId = ThreadId.make("thread:mcp-cancel-failed-child");
      const childRunId = RunId.make("run:mcp-cancel-failed-child");
      const taskId = NodeId.make("node:mcp-cancel-failed-task");
      const dispatched = yield* Ref.make<ReadonlyArray<unknown>>([]);
      const parentProjection = {
        thread: { id: parentThreadId },
        runs: [],
        contextTransfers: [],
        subagents: [
          {
            id: taskId,
            threadId: parentThreadId,
            origin: "app_owned",
            childThreadId,
            driver: "codex",
            model: "gpt-5.6-terra",
            result: null,
            completionDelivery: { state: "pending" },
          },
        ],
      } as unknown as OrchestrationV2ThreadProjection;
      const childProjection = {
        thread: { id: childThreadId },
        runs: [{ id: childRunId, status: "running" }],
        contextTransfers: [],
      } as unknown as OrchestrationV2ThreadProjection;
      const dependencies = Layer.mergeAll(
        NodeServices.layer,
        Layer.mock(ThreadManagementService)({
          getThreadProjection: (threadId) =>
            Effect.succeed(threadId === parentThreadId ? parentProjection : childProjection),
          dispatch: (command) =>
            Ref.update(dispatched, (commands) => [...commands, command]).pipe(
              Effect.andThen(Effect.fail(new Error("simulated interrupt failure") as never)),
            ),
        }),
        Layer.mock(ProviderRegistry)({ getProviders: Effect.succeed([]) }),
        Layer.mock(ScheduledTaskService)({}),
        Layer.mock(ProjectService.ProjectService)({}),
      );
      const scope: McpInvocationScope = {
        environmentId: EnvironmentId.make("environment:mcp-cancel-failed"),
        threadId: parentThreadId,
        providerSessionId: "provider-session:mcp-cancel-failed",
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: new Set(["orchestration"]),
        issuedAt: 1,
      };

      yield* Effect.gen(function* () {
        const service = yield* OrchestratorMcpService.OrchestratorMcpService;
        const error = yield* service
          .cancelTask(scope, { taskId, clientRequestId: "cancel-failed-task" })
          .pipe(Effect.flip);
        assert.equal(error.code, "task_not_cancellable");
        assert.deepEqual(
          (yield* Ref.get(dispatched)).map((command) => (command as { type: string }).type),
          ["run.interrupt"],
        );
      }).pipe(Effect.provide(OrchestratorMcpService.layer.pipe(Layer.provide(dependencies))));
    }),
  );

  it.effect("returns cancel requested when post-interrupt disposal fails", () =>
    Effect.gen(function* () {
      const parentThreadId = ThreadId.make("thread:mcp-cancel-dispose-failed-parent");
      const childThreadId = ThreadId.make("thread:mcp-cancel-dispose-failed-child");
      const childRunId = RunId.make("run:mcp-cancel-dispose-failed-child");
      const taskId = NodeId.make("node:mcp-cancel-dispose-failed-task");
      const dispatched = yield* Ref.make<ReadonlyArray<unknown>>([]);
      const parentProjection = {
        thread: { id: parentThreadId },
        runs: [],
        contextTransfers: [],
        subagents: [
          {
            id: taskId,
            threadId: parentThreadId,
            origin: "app_owned",
            childThreadId,
            driver: "codex",
            model: "gpt-5.6-terra",
            result: null,
            completionDelivery: { state: "pending" },
          },
        ],
      } as unknown as OrchestrationV2ThreadProjection;
      const childProjection = {
        thread: { id: childThreadId },
        runs: [{ id: childRunId, status: "running" }],
        contextTransfers: [],
      } as unknown as OrchestrationV2ThreadProjection;
      const dependencies = Layer.mergeAll(
        NodeServices.layer,
        Layer.mock(ThreadManagementService)({
          getThreadProjection: (threadId) =>
            Effect.succeed(threadId === parentThreadId ? parentProjection : childProjection),
          dispatch: (command) =>
            Ref.update(dispatched, (commands) => [...commands, command]).pipe(
              Effect.andThen(
                command.type === "delegated_task.completion-delivery.dispose"
                  ? Effect.fail(new Error("simulated disposal failure") as never)
                  : Effect.succeed({} as never),
              ),
            ),
        }),
        Layer.mock(ProviderRegistry)({ getProviders: Effect.succeed([]) }),
        Layer.mock(ScheduledTaskService)({}),
        Layer.mock(ProjectService.ProjectService)({}),
      );
      const scope: McpInvocationScope = {
        environmentId: EnvironmentId.make("environment:mcp-cancel-dispose-failed"),
        threadId: parentThreadId,
        providerSessionId: "provider-session:mcp-cancel-dispose-failed",
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: new Set(["orchestration"]),
        issuedAt: 1,
      };

      yield* Effect.gen(function* () {
        const service = yield* OrchestratorMcpService.OrchestratorMcpService;
        const result = yield* service.cancelTask(scope, {
          taskId,
          clientRequestId: "cancel-dispose-failed-task",
        });
        assert.equal(result.status, "cancel_requested");
        assert.deepEqual(
          (yield* Ref.get(dispatched)).map((command) => (command as { type: string }).type),
          ["run.interrupt", "delegated_task.completion-delivery.dispose"],
        );
      }).pipe(Effect.provide(OrchestratorMcpService.layer.pipe(Layer.provide(dependencies))));
    }),
  );

  it.effect("rejects a relative project directory before creating a top-level thread", () =>
    Effect.gen(function* () {
      const parentThreadId = ThreadId.make("thread:mcp-project-relative-parent");
      const dispatched = yield* Ref.make<ReadonlyArray<unknown>>([]);
      const parentProjection = {
        thread: {
          id: parentThreadId,
          projectId: ProjectId.make("project:parent"),
          title: "Parent",
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: "main",
          worktreePath: "/workspace/parent",
        },
        runs: [
          {
            id: RunId.make("run:mcp-project-relative-parent"),
            ordinal: 1,
            status: "running",
            rootNodeId: NodeId.make("node:mcp-project-relative-root"),
            providerInstanceId: ProviderInstanceId.make("codex"),
          },
        ],
        subagents: [],
        turnItems: [],
        contextTransfers: [],
      } as unknown as OrchestrationV2ThreadProjection;
      const dependencies = Layer.mergeAll(
        NodeServices.layer,
        Layer.mock(ThreadManagementService)({
          getThreadProjection: () => Effect.succeed(parentProjection),
          dispatch: (command) =>
            Ref.update(dispatched, (commands) => [...commands, command]).pipe(
              Effect.as({} as never),
            ),
        }),
        Layer.mock(ProviderRegistry)({
          getProviders: Effect.succeed([
            {
              instanceId: ProviderInstanceId.make("codex"),
              driver: "codex",
              enabled: true,
              installed: true,
              version: "test",
              status: "ready",
              auth: { status: "authenticated" },
              checkedAt: "2026-06-17T00:00:00.000Z",
              models: [{ slug: "gpt-5.4", name: "gpt-5.4", isCustom: false, capabilities: null }],
              slashCommands: [],
              skills: [],
            } as unknown as ServerProvider,
          ]),
        }),
        Layer.mock(ScheduledTaskService)({}),
        Layer.mock(ProjectService.ProjectService)({
          getByWorkspaceRoot: () => Effect.succeed(Option.none()),
        }),
      );
      const scope: McpInvocationScope = {
        environmentId: EnvironmentId.make("environment:mcp-project-relative"),
        threadId: parentThreadId,
        providerSessionId: "provider-session:mcp-project-relative",
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: new Set(["orchestration"]),
        issuedAt: 1,
      };

      yield* Effect.gen(function* () {
        const service = yield* OrchestratorMcpService.OrchestratorMcpService;
        const error = yield* service
          .createThreads(scope, {
            threads: [{ title: "Other workspace", projectDirectory: "other" }],
            clientRequestId: "create-relative-project",
          })
          .pipe(Effect.flip);
        assert.equal(error.code, "invalid_request");
        assert.match(error.message, /absolute path/);
        assert.deepEqual(yield* Ref.get(dispatched), []);
      }).pipe(Effect.provide(OrchestratorMcpService.layer.pipe(Layer.provide(dependencies))));
    }),
  );

  it.effect("rejects a project directory T3 does not know", () =>
    Effect.gen(function* () {
      const parentThreadId = ThreadId.make("thread:mcp-project-unknown-parent");
      const dispatched = yield* Ref.make<ReadonlyArray<unknown>>([]);
      const parentProjection = {
        thread: {
          id: parentThreadId,
          projectId: ProjectId.make("project:parent"),
          title: "Parent",
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: "main",
          worktreePath: "/workspace/parent",
        },
        runs: [
          {
            id: RunId.make("run:mcp-project-unknown-parent"),
            ordinal: 1,
            status: "running",
            rootNodeId: NodeId.make("node:mcp-project-unknown-root"),
            providerInstanceId: ProviderInstanceId.make("codex"),
          },
        ],
        subagents: [],
        turnItems: [],
        contextTransfers: [],
      } as unknown as OrchestrationV2ThreadProjection;
      const dependencies = Layer.mergeAll(
        NodeServices.layer,
        Layer.mock(ThreadManagementService)({
          getThreadProjection: () => Effect.succeed(parentProjection),
          dispatch: (command) =>
            Ref.update(dispatched, (commands) => [...commands, command]).pipe(
              Effect.as({} as never),
            ),
        }),
        Layer.mock(ProviderRegistry)({
          getProviders: Effect.succeed([
            {
              instanceId: ProviderInstanceId.make("codex"),
              driver: "codex",
              enabled: true,
              installed: true,
              version: "test",
              status: "ready",
              auth: { status: "authenticated" },
              checkedAt: "2026-06-17T00:00:00.000Z",
              models: [{ slug: "gpt-5.4", name: "gpt-5.4", isCustom: false, capabilities: null }],
              slashCommands: [],
              skills: [],
            } as unknown as ServerProvider,
          ]),
        }),
        Layer.mock(ScheduledTaskService)({}),
        Layer.mock(ProjectService.ProjectService)({
          getByWorkspaceRoot: () => Effect.succeed(Option.none()),
        }),
      );
      const scope: McpInvocationScope = {
        environmentId: EnvironmentId.make("environment:mcp-project-unknown"),
        threadId: parentThreadId,
        providerSessionId: "provider-session:mcp-project-unknown",
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: new Set(["orchestration"]),
        issuedAt: 1,
      };

      yield* Effect.gen(function* () {
        const service = yield* OrchestratorMcpService.OrchestratorMcpService;
        const error = yield* service
          .createThreads(scope, {
            threads: [{ title: "Unknown workspace", projectDirectory: "/workspace/unknown" }],
            clientRequestId: "create-unknown-project",
          })
          .pipe(Effect.flip);
        assert.equal(error.code, "invalid_request");
        assert.match(error.message, /not a known T3 project/);
        assert.deepEqual(yield* Ref.get(dispatched), []);
      }).pipe(Effect.provide(OrchestratorMcpService.layer.pipe(Layer.provide(dependencies))));
    }),
  );

  it.effect("creates a top-level thread in a known project directory", () =>
    Effect.gen(function* () {
      const parentThreadId = ThreadId.make("thread:mcp-project-known-parent");
      const otherProjectId = ProjectId.make("project:other");
      const dispatched = yield* Ref.make<ReadonlyArray<{ type: string }>>([]);
      const parentProjection = {
        thread: {
          id: parentThreadId,
          projectId: ProjectId.make("project:parent"),
          title: "Parent",
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: "main",
          worktreePath: "/workspace/parent",
        },
        runs: [
          {
            id: RunId.make("run:mcp-project-known-parent"),
            ordinal: 1,
            status: "running",
            rootNodeId: NodeId.make("node:mcp-project-known-root"),
            providerInstanceId: ProviderInstanceId.make("codex"),
          },
        ],
        subagents: [],
        turnItems: [],
        contextTransfers: [],
      } as unknown as OrchestrationV2ThreadProjection;
      const otherProject = {
        id: otherProjectId,
        title: "Other",
        workspaceRoot: "/workspace/other",
        defaultModelSelection: null,
        scripts: [],
        createdAt: "2026-01-01T00:00:00.000Z",
        updatedAt: "2026-01-01T00:00:00.000Z",
        deletedAt: null,
      } as Project;
      const dependencies = Layer.mergeAll(
        NodeServices.layer,
        Layer.mock(ThreadManagementService)({
          getThreadProjection: (threadId) =>
            Effect.succeed(
              threadId === parentThreadId
                ? parentProjection
                : ({
                    thread: {
                      id: threadId,
                      projectId: otherProjectId,
                      title: "Other workspace",
                      createdBy: "agent",
                      creationSource: "mcp",
                      worktreePath: "/workspace/other",
                    },
                    runs: [],
                  } as unknown as OrchestrationV2ThreadProjection),
            ),
          dispatch: (command) =>
            Ref.update(dispatched, (commands) => [...commands, command as { type: string }]).pipe(
              Effect.as({} as never),
            ),
        }),
        Layer.mock(ProviderRegistry)({
          getProviders: Effect.succeed([
            {
              instanceId: ProviderInstanceId.make("codex"),
              driver: "codex",
              enabled: true,
              installed: true,
              version: "test",
              status: "ready",
              auth: { status: "authenticated" },
              checkedAt: "2026-06-17T00:00:00.000Z",
              models: [{ slug: "gpt-5.4", name: "gpt-5.4", isCustom: false, capabilities: null }],
              slashCommands: [],
              skills: [],
            } as unknown as ServerProvider,
          ]),
        }),
        Layer.mock(ScheduledTaskService)({}),
        Layer.mock(ProjectService.ProjectService)({
          getByWorkspaceRoot: (workspaceRoot) =>
            Effect.succeed(
              workspaceRoot === "/workspace/other" ? Option.some(otherProject) : Option.none(),
            ),
        }),
      );
      const scope: McpInvocationScope = {
        environmentId: EnvironmentId.make("environment:mcp-project-known"),
        threadId: parentThreadId,
        providerSessionId: "provider-session:mcp-project-known",
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: new Set(["orchestration"]),
        issuedAt: 1,
      };

      yield* Effect.gen(function* () {
        const service = yield* OrchestratorMcpService.OrchestratorMcpService;
        const result = yield* service.createThreads(scope, {
          threads: [{ title: "Other workspace", projectDirectory: "/workspace/other" }],
          clientRequestId: "create-known-project",
        });
        assert.equal(result.threads[0]?.title, "Other workspace");
        const command = (yield* Ref.get(dispatched)).find(
          (entry) => entry.type === "thread.create",
        ) as {
          type: string;
          projectId?: string;
          worktreePath?: string | null;
          branch?: string | null;
        };
        assert.equal(command.type, "thread.create");
        assert.equal(command.projectId, otherProjectId);
        assert.equal(command.worktreePath, null);
        assert.equal(command.branch, null);
      }).pipe(Effect.provide(OrchestratorMcpService.layer.pipe(Layer.provide(dependencies))));
    }),
  );

  it.effect("expands a home-relative project directory", () =>
    Effect.gen(function* () {
      const parentThreadId = ThreadId.make("thread:mcp-project-home-parent");
      const otherProjectId = ProjectId.make("project:home-known");
      const homeKnown = `${NodeOS.homedir()}/known-project`;
      const dispatched = yield* Ref.make<ReadonlyArray<{ type: string }>>([]);
      const parentProjection = {
        thread: {
          id: parentThreadId,
          projectId: ProjectId.make("project:parent"),
          title: "Parent",
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: "main",
          worktreePath: "/workspace/parent",
        },
        runs: [
          {
            id: RunId.make("run:mcp-project-home-parent"),
            ordinal: 1,
            status: "running",
            rootNodeId: NodeId.make("node:mcp-project-home-root"),
            providerInstanceId: ProviderInstanceId.make("codex"),
          },
        ],
        subagents: [],
        turnItems: [],
        contextTransfers: [],
      } as unknown as OrchestrationV2ThreadProjection;
      const otherProject = {
        id: otherProjectId,
        title: "Home known",
        workspaceRoot: homeKnown,
        defaultModelSelection: null,
        scripts: [],
        createdAt: "2026-01-01T00:00:00.000Z",
        updatedAt: "2026-01-01T00:00:00.000Z",
        deletedAt: null,
      } as Project;
      const dependencies = Layer.mergeAll(
        NodeServices.layer,
        Layer.mock(ThreadManagementService)({
          getThreadProjection: (threadId) =>
            Effect.succeed(
              threadId === parentThreadId
                ? parentProjection
                : ({
                    thread: {
                      id: threadId,
                      projectId: otherProjectId,
                      title: "Home known",
                      createdBy: "agent",
                      creationSource: "mcp",
                      worktreePath: null,
                    },
                    runs: [],
                  } as unknown as OrchestrationV2ThreadProjection),
            ),
          dispatch: (command) =>
            Ref.update(dispatched, (commands) => [...commands, command as { type: string }]).pipe(
              Effect.as({} as never),
            ),
        }),
        Layer.mock(ProviderRegistry)({
          getProviders: Effect.succeed([
            {
              instanceId: ProviderInstanceId.make("codex"),
              driver: "codex",
              enabled: true,
              installed: true,
              version: "test",
              status: "ready",
              auth: { status: "authenticated" },
              checkedAt: "2026-06-17T00:00:00.000Z",
              models: [{ slug: "gpt-5.4", name: "gpt-5.4", isCustom: false, capabilities: null }],
              slashCommands: [],
              skills: [],
            } as unknown as ServerProvider,
          ]),
        }),
        Layer.mock(ScheduledTaskService)({}),
        Layer.mock(ProjectService.ProjectService)({
          getByWorkspaceRoot: (workspaceRoot) =>
            Effect.succeed(workspaceRoot === homeKnown ? Option.some(otherProject) : Option.none()),
        }),
      );
      const scope: McpInvocationScope = {
        environmentId: EnvironmentId.make("environment:mcp-project-home"),
        threadId: parentThreadId,
        providerSessionId: "provider-session:mcp-project-home",
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: new Set(["orchestration"]),
        issuedAt: 1,
      };

      yield* Effect.gen(function* () {
        const service = yield* OrchestratorMcpService.OrchestratorMcpService;
        yield* service.createThreads(scope, {
          threads: [{ title: "Home known", projectDirectory: "~/known-project" }],
          clientRequestId: "create-home-project",
        });
        const command = (yield* Ref.get(dispatched)).find(
          (entry) => entry.type === "thread.create",
        ) as { type: string; projectId?: string; worktreePath?: string | null };
        assert.equal(command.projectId, otherProjectId);
        assert.equal(command.worktreePath, null);
      }).pipe(Effect.provide(OrchestratorMcpService.layer.pipe(Layer.provide(dependencies))));
    }),
  );

  it.effect("reports project lookup faults as orchestration errors", () =>
    Effect.gen(function* () {
      const parentThreadId = ThreadId.make("thread:mcp-project-fault-parent");
      const parentProjection = {
        thread: {
          id: parentThreadId,
          projectId: ProjectId.make("project:parent"),
          title: "Parent",
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: "main",
          worktreePath: "/workspace/parent",
        },
        runs: [
          {
            id: RunId.make("run:mcp-project-fault-parent"),
            ordinal: 1,
            status: "running",
            rootNodeId: NodeId.make("node:mcp-project-fault-root"),
            providerInstanceId: ProviderInstanceId.make("codex"),
          },
        ],
        subagents: [],
        turnItems: [],
        contextTransfers: [],
      } as unknown as OrchestrationV2ThreadProjection;
      const dependencies = Layer.mergeAll(
        NodeServices.layer,
        Layer.mock(ThreadManagementService)({
          getThreadProjection: () => Effect.succeed(parentProjection),
          dispatch: () => Effect.succeed({} as never),
        }),
        Layer.mock(ProviderRegistry)({
          getProviders: Effect.succeed([
            {
              instanceId: ProviderInstanceId.make("codex"),
              driver: "codex",
              enabled: true,
              installed: true,
              version: "test",
              status: "ready",
              auth: { status: "authenticated" },
              checkedAt: "2026-06-17T00:00:00.000Z",
              models: [{ slug: "gpt-5.4", name: "gpt-5.4", isCustom: false, capabilities: null }],
              slashCommands: [],
              skills: [],
            } as unknown as ServerProvider,
          ]),
        }),
        Layer.mock(ScheduledTaskService)({}),
        Layer.mock(ProjectService.ProjectService)({
          getByWorkspaceRoot: () =>
            Effect.fail(
              new ProjectService.ProjectOperationError({
                operation: "list-projects",
                cause: "simulated list failure",
              }),
            ),
        }),
      );
      const scope: McpInvocationScope = {
        environmentId: EnvironmentId.make("environment:mcp-project-fault"),
        threadId: parentThreadId,
        providerSessionId: "provider-session:mcp-project-fault",
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: new Set(["orchestration"]),
        issuedAt: 1,
      };

      yield* Effect.gen(function* () {
        const service = yield* OrchestratorMcpService.OrchestratorMcpService;
        const error = yield* service
          .createThreads(scope, {
            threads: [{ title: "Fault", projectDirectory: "/workspace/other" }],
            clientRequestId: "create-fault-project",
          })
          .pipe(Effect.flip);
        assert.equal(error.code, "orchestration_error");
        assert.match(error.message, /Unable to resolve project directory/);
      }).pipe(Effect.provide(OrchestratorMcpService.layer.pipe(Layer.provide(dependencies))));
    }),
  );
});
