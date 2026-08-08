import { EnvironmentId, ProjectId, ProviderInstanceId, RunId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import type { EnvironmentThreadShell, ThreadRunSummary } from "./state/models.ts";
import {
  captureThreadSoundState,
  captureThreadSoundStatePreservingUnobserved,
  captureThreadSoundStateWhileSettingsHydrating,
  deriveInteractionSoundCues,
  selectLiveThreadShells,
  shouldPlayInteractionSound,
} from "./interactionSounds.ts";

function makeRun(overrides: Partial<ThreadRunSummary> = {}): ThreadRunSummary {
  return {
    runId: RunId.make("run-1"),
    status: "running",
    requestedAt: "2026-07-11T12:00:02.000Z",
    startedAt: "2026-07-11T12:00:03.000Z",
    completedAt: null,
    assistantMessageId: null,
    ...overrides,
  };
}

function makeThread(overrides: Partial<EnvironmentThreadShell> = {}): EnvironmentThreadShell {
  return {
    environmentId: EnvironmentId.make("environment-1"),
    id: ThreadId.make("thread-1"),
    projectId: ProjectId.make("project-1"),
    title: "Thread",
    providerInstanceId: ProviderInstanceId.make("provider-1"),
    modelSelection: {
      instanceId: ProviderInstanceId.make("provider-1"),
      model: "claude-sonnet",
    },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    lineage: {
      parentThreadId: null,
      relationshipToParent: null,
      rootThreadId: ThreadId.make("thread-1"),
    },
    forkedFrom: null,
    activeProviderThreadId: null,
    latestRun: null,
    runtime: null,
    latestUserMessageAt: null,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    hasActionableProposedPlan: false,
    pendingBackgroundTasks: [],
    itemCount: 0,
    visibleItemCount: 0,
    createdAt: "2026-07-11T12:00:00.000Z",
    updatedAt: "2026-07-11T12:00:00.000Z",
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    snoozedUntil: null,
    snoozedAt: null,
    pinnedAt: null,
    titleRegeneration: null,
    deletedAt: null,
    source: {} as EnvironmentThreadShell["source"],
    ...overrides,
  };
}

describe("interaction sounds", () => {
  it("plays success when a run is associated with a nearby user message", () => {
    const running = makeThread({
      latestUserMessageAt: "2026-07-11T12:00:00.000Z",
      latestRun: makeRun({ status: "running" }),
    });
    const completed = makeThread({
      latestUserMessageAt: running.latestUserMessageAt,
      latestRun: makeRun({
        status: "completed",
        completedAt: "2026-07-11T12:00:05.000Z",
      }),
    });

    expect(deriveInteractionSoundCues(captureThreadSoundState([running]), [completed])).toEqual([
      "success",
    ]);
  });

  it("does not associate an old user message with later background work", () => {
    const beforeBackgroundWork = makeThread({
      latestUserMessageAt: "2026-07-11T12:00:00.000Z",
    });
    const completedBackgroundRun = makeThread({
      latestUserMessageAt: beforeBackgroundWork.latestUserMessageAt,
      latestRun: makeRun({
        runId: RunId.make("background-run"),
        status: "completed",
        requestedAt: "2026-07-11T12:05:00.000Z",
        startedAt: "2026-07-11T12:05:00.000Z",
        completedAt: "2026-07-11T12:05:05.000Z",
      }),
    });

    expect(
      deriveInteractionSoundCues(captureThreadSoundState([beforeBackgroundWork]), [
        completedBackgroundRun,
      ]),
    ).toEqual([]);
  });

  it("does not let a later steering message associate a background run", () => {
    const backgroundRunning = makeThread({
      latestUserMessageAt: "2026-07-11T12:00:00.000Z",
      latestRun: makeRun({
        runId: RunId.make("subagent-run"),
        status: "running",
        requestedAt: "2026-07-11T12:05:00.000Z",
        startedAt: "2026-07-11T12:05:00.000Z",
      }),
    });
    const completedAfterSteering = makeThread({
      latestUserMessageAt: "2026-07-11T12:06:00.000Z",
      latestRun: makeRun({
        runId: RunId.make("subagent-run"),
        status: "completed",
        requestedAt: "2026-07-11T12:05:00.000Z",
        startedAt: "2026-07-11T12:05:00.000Z",
        completedAt: "2026-07-11T12:06:05.000Z",
      }),
    });

    expect(
      deriveInteractionSoundCues(captureThreadSoundState([backgroundRunning]), [
        completedAfterSteering,
      ]),
    ).toEqual([]);
  });

  it("plays bloom when a thread starts requesting user input", () => {
    const thread = makeThread();

    expect(
      deriveInteractionSoundCues(captureThreadSoundState([thread]), [
        makeThread({ hasPendingUserInput: true }),
      ]),
    ).toEqual(["bloom"]);
  });

  it("plays bloom when a thread starts requesting approval", () => {
    const thread = makeThread();

    expect(
      deriveInteractionSoundCues(captureThreadSoundState([thread]), [
        makeThread({ hasPendingApprovals: true }),
      ]),
    ).toEqual(["bloom"]);
  });

  it("plays bloom when pending input changes directly to pending approval", () => {
    const pendingInput = makeThread({ hasPendingUserInput: true });
    const pendingApproval = makeThread({ hasPendingApprovals: true });

    expect(
      deriveInteractionSoundCues(captureThreadSoundState([pendingInput]), [pendingApproval]),
    ).toEqual(["bloom"]);
  });

  it("plays bloom when pending approval changes directly to pending input", () => {
    const pendingApproval = makeThread({ hasPendingApprovals: true });
    const pendingInput = makeThread({ hasPendingUserInput: true });

    expect(
      deriveInteractionSoundCues(captureThreadSoundState([pendingApproval]), [pendingInput]),
    ).toEqual(["bloom"]);
  });

  it("does not replay cues for unchanged state", () => {
    const thread = makeThread({
      latestUserMessageAt: "2026-07-11T12:00:00.000Z",
      hasPendingUserInput: true,
      hasPendingApprovals: true,
      latestRun: makeRun({
        status: "completed",
        requestedAt: "2026-07-11T12:00:00.000Z",
        startedAt: "2026-07-11T12:00:01.000Z",
        completedAt: "2026-07-11T12:00:05.000Z",
      }),
    });

    expect(deriveInteractionSoundCues(captureThreadSoundState([thread]), [thread])).toEqual([]);
  });

  it("does not replay success when a completed run timestamp is corrected", () => {
    const completed = makeThread({
      latestUserMessageAt: "2026-07-11T12:00:00.000Z",
      latestRun: makeRun({
        status: "completed",
        requestedAt: "2026-07-11T12:00:01.000Z",
        startedAt: "2026-07-11T12:00:02.000Z",
        completedAt: "2026-07-11T12:00:05.000Z",
      }),
    });
    const corrected = makeThread({
      latestUserMessageAt: completed.latestUserMessageAt,
      latestRun: makeRun({
        status: "completed",
        requestedAt: "2026-07-11T12:00:01.000Z",
        startedAt: "2026-07-11T12:00:02.000Z",
        completedAt: "2026-07-11T12:00:06.000Z",
      }),
    });

    expect(deriveInteractionSoundCues(captureThreadSoundState([completed]), [corrected])).toEqual(
      [],
    );
  });

  it("does not play cues while existing threads are first hydrated", () => {
    const thread = makeThread({
      hasPendingUserInput: true,
      latestRun: makeRun({
        status: "completed",
        requestedAt: "2026-07-11T12:00:00.000Z",
        startedAt: "2026-07-11T12:00:01.000Z",
        completedAt: "2026-07-11T12:00:05.000Z",
      }),
    });

    expect(deriveInteractionSoundCues(new Map(), [thread])).toEqual([]);
  });

  it("preserves pre-hydration thread state so cues can play after settings hydrate", () => {
    const running = makeThread({
      latestUserMessageAt: "2026-07-11T12:00:00.000Z",
      latestRun: makeRun({ status: "running", requestedAt: "2026-07-11T12:00:01.000Z" }),
    });
    const completed = makeThread({
      latestUserMessageAt: running.latestUserMessageAt,
      latestRun: makeRun({
        status: "completed",
        requestedAt: "2026-07-11T12:00:01.000Z",
        completedAt: "2026-07-11T12:00:05.000Z",
      }),
    });

    const seeded = captureThreadSoundStateWhileSettingsHydrating(null, [running]);
    const frozen = captureThreadSoundStateWhileSettingsHydrating(seeded, [completed]);

    expect(deriveInteractionSoundCues(frozen, [completed])).toEqual(["success"]);
  });

  it("preserves a thread baseline while its environment is synchronizing", () => {
    const running = makeThread({
      latestUserMessageAt: "2026-07-11T12:00:00.000Z",
      latestRun: makeRun({ status: "running", requestedAt: "2026-07-11T12:00:01.000Z" }),
    });
    const completedDuringSync = makeThread({
      latestUserMessageAt: running.latestUserMessageAt,
      latestRun: makeRun({
        status: "completed",
        requestedAt: "2026-07-11T12:00:01.000Z",
        completedAt: "2026-07-11T12:00:05.000Z",
      }),
    });
    const beforeSync = captureThreadSoundState([running]);
    const whileSynchronizing = captureThreadSoundStatePreservingUnobserved(
      beforeSync,
      [],
      [completedDuringSync],
    );

    expect(deriveInteractionSoundCues(whileSynchronizing, [completedDuringSync])).toEqual([
      "success",
    ]);
  });

  it("detects a user-input request received while its environment is synchronizing", () => {
    const idle = makeThread();
    const pendingInputDuringSync = makeThread({ hasPendingUserInput: true });
    const beforeSync = captureThreadSoundState([idle]);
    const whileSynchronizing = captureThreadSoundStatePreservingUnobserved(
      beforeSync,
      [],
      [pendingInputDuringSync],
    );

    expect(deriveInteractionSoundCues(whileSynchronizing, [pendingInputDuringSync])).toEqual([
      "bloom",
    ]);
  });

  it("drops retained baselines for threads that no longer exist", () => {
    const thread = makeThread({ hasPendingUserInput: true });
    const beforeRemoval = captureThreadSoundState([thread]);
    const afterRemoval = captureThreadSoundStatePreservingUnobserved(beforeRemoval, [], []);

    expect(afterRemoval.size).toBe(0);
  });

  it("admits newly seen threads while settings are hydrating", () => {
    const seeded = captureThreadSoundStateWhileSettingsHydrating(null, []);
    const withThread = captureThreadSoundStateWhileSettingsHydrating(seeded, [
      makeThread({ hasPendingUserInput: true }),
    ]);

    expect(
      deriveInteractionSoundCues(withThread, [makeThread({ hasPendingUserInput: true })]),
    ).toEqual([]);
  });

  it("keeps input-request cues enabled when completion sounds are disabled", () => {
    expect(shouldPlayInteractionSound("success", false)).toBe(false);
    expect(shouldPlayInteractionSound("bloom", false)).toBe(true);
  });

  it("excludes cached thread shells until their environment is live", () => {
    const cached = makeThread({ environmentId: EnvironmentId.make("cached-environment") });
    const live = makeThread({
      environmentId: EnvironmentId.make("live-environment"),
      id: ThreadId.make("thread-2"),
    });

    expect(
      selectLiveThreadShells([cached, live], new Set([live.environmentId])).map(
        (thread) => thread.id,
      ),
    ).toEqual(["thread-2"]);
  });
});
