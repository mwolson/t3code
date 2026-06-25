import { act, createElement } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import {
  DEFAULT_CLIENT_SETTINGS,
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type ServerSettings,
  type T3ProjectFile,
} from "@t3tools/contracts";
import { scopeProjectRef, scopeThreadRef } from "@t3tools/client-runtime/environment";
import { createModelSelection } from "@t3tools/shared/model";
import { DraftId, useComposerDraftStore } from "../composerDraftStore";
import {
  deriveLogicalProjectKeyFromSettings,
  selectProjectGroupingSettings,
} from "../logicalProject";
import type { Project } from "../types";

const testState = vi.hoisted(() => {
  const router = {
    state: {
      location: { href: "/" },
      matches: [
        { params: {} as Partial<Record<"environmentId" | "threadId" | "draftId", string>> },
      ],
    },
    navigate: vi.fn(async (request: { params: { draftId: string } }) => {
      router.state.location.href = `/draft/${request.params.draftId}`;
    }),
  };
  return {
    router,
    primarySettings: {} as ServerSettings,
    targetSettings: {} as ServerSettings,
    projects: [] as Project[],
    projectFileRead: Promise.resolve<T3ProjectFile | null>(null),
    readProjectFile: vi.fn(),
  };
});
vi.mock("@effect/atom-react", () => ({
  useAtomValue: () =>
    new Map([
      ["environment-primary", { settings: testState.primarySettings }],
      ["environment-ssh", { settings: testState.targetSettings }],
    ]),
}));
vi.mock("@tanstack/react-router", () => ({
  useParams: () => null,
  useRouter: () => testState.router,
}));
vi.mock("../components/Sidebar.logic", () => ({ orderItemsByPreferredIds: () => [] }));
vi.mock("../lib/t3ProjectFileDefaults", () => ({
  readT3ProjectFile: (...args: unknown[]) => {
    testState.readProjectFile(...args);
    return testState.projectFileRead;
  },
}));
vi.mock("../lib/utils", () => ({
  newDraftId: () => DraftId.make("draft-delayed"),
  newThreadId: () => ThreadId.make("thread-delayed"),
}));
vi.mock("../state/entities", () => ({
  readProjects: () => testState.projects,
  readThreadShell: () => null,
  useProjects: () => testState.projects,
  useThreadShell: () => null,
}));
vi.mock("../state/server", () => ({ environmentServerConfigsAtom: {} }));
vi.mock("../uiStateStore", () => ({
  legacyProjectCwdPreferenceKey: () => "remote-project",
  useUiStateStore: () => [],
}));
vi.mock("./useSettings", () => ({
  useClientSettings: (select: (settings: typeof DEFAULT_CLIENT_SETTINGS) => unknown) =>
    select(DEFAULT_CLIENT_SETTINGS),
}));

import { useNewThreadHandler } from "./useHandleNewThread";

const projectRef = scopeProjectRef(
  EnvironmentId.make("environment-ssh"),
  ProjectId.make("project-remote"),
);
const instance = ProviderInstanceId.make("codex");
const sticky = createModelSelection(instance, "last-used", [
  { id: "reasoningEffort", value: "high" },
]);
const projectModel = createModelSelection(instance, "project-model");
const explicit = createModelSelection(instance, "explicit-model");
const existingDraft = DraftId.make("draft-existing");
let completeFileRead: (file: T3ProjectFile | null) => void;
let renderer: ReactTestRenderer | undefined;
let openThread: ReturnType<typeof useNewThreadHandler>;
let logicalProjectKey: string;

function setup(reuse: boolean, settings: Partial<ServerSettings> = {}) {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  useComposerDraftStore.setState({
    draftsByThreadKey: {},
    draftThreadsByThreadKey: {},
    logicalProjectDraftThreadKeyByLogicalProjectKey: {},
    stickyActiveProvider: null,
    stickyModelSelectionByProvider: {},
  });
  testState.targetSettings = {
    ...DEFAULT_SERVER_SETTINGS,
    defaultThreadEnvMode: null,
    ...settings,
  };
  testState.primarySettings = {
    ...DEFAULT_SERVER_SETTINGS,
    defaultRuntimeMode:
      testState.targetSettings.defaultRuntimeMode === "full-access"
        ? "approval-required"
        : "full-access",
    defaultThreadEnvMode:
      testState.targetSettings.defaultThreadEnvMode === "local" ? "worktree" : "local",
    newWorktreesStartFromOrigin: !testState.targetSettings.newWorktreesStartFromOrigin,
  };
  testState.projects = [
    {
      id: projectRef.projectId,
      environmentId: projectRef.environmentId,
      title: "Remote",
      workspaceRoot: "/remote/project",
      repositoryIdentity: null,
      defaultModelSelection: null,
      defaultThreadEnvMode: null,
      createdAt: "2026-09-23T00:00:00.000Z",
      updatedAt: "2026-09-23T00:00:00.000Z",
      scripts: [],
    },
  ];
  logicalProjectKey = deriveLogicalProjectKeyFromSettings(
    testState.projects[0]!,
    selectProjectGroupingSettings(DEFAULT_CLIENT_SETTINGS),
  );
  if (reuse) {
    useComposerDraftStore
      .getState()
      .setLogicalProjectDraftThreadId(logicalProjectKey, projectRef, existingDraft, {
        threadId: ThreadId.make("thread-existing"),
        envMode: "local",
      });
    useComposerDraftStore
      .getState()
      .setModelSelection(existingDraft, createModelSelection(instance, "old-implicit-seed"));
  }
  testState.router.state.location.href = "/";
  testState.router.state.matches = [{ params: {} }];
  testState.router.navigate.mockClear();
  testState.readProjectFile.mockClear();
  testState.projectFileRead = new Promise((resolve) => {
    completeFileRead = resolve;
  });
  function Probe() {
    openThread = useNewThreadHandler();
    return null;
  }
  act(() => {
    renderer = create(createElement(Probe));
  });
}

async function finish(pending: ReturnType<typeof openThread>, file: T3ProjectFile | null = null) {
  completeFileRead(file);
  const opened = await pending;
  expect(opened).not.toBeNull();
  return opened!;
}
function selection(id: DraftId) {
  const draft = useComposerDraftStore.getState().getComposerDraft(id);
  return draft?.activeProvider ? draft.modelSelectionByProvider[draft.activeProvider] : null;
}

afterEach(() => {
  act(() => renderer?.unmount());
  renderer = undefined;
  vi.unstubAllGlobals();
});

describe.each([false, true])("new-thread real store with reuse=%s", (reuse) => {
  it("reads the latest sticky model after inherited workspace resolution", async () => {
    setup(reuse, { defaultModelSelection: projectModel });
    const pending = openThread(projectRef);
    useComposerDraftStore.getState().setStickyModelSelection(sticky);
    const opened = await finish(pending);
    expect(selection(opened.draftId)).toEqual(sticky);
    expect(opened.draftId).toBe(reuse ? existingDraft : "draft-delayed");
    expect(useComposerDraftStore.getState().getDraftSession(opened.draftId)).toMatchObject(
      projectRef,
    );
    expect(
      useComposerDraftStore.getState().getComposerDraft(opened.draftId)?.modelSelectionExplicit,
    ).toBeUndefined();
  });

  it("carries the current server thread's composer model ahead of sticky and project defaults", async () => {
    setup(reuse, { defaultModelSelection: projectModel });
    const sourceRef = scopeThreadRef(
      EnvironmentId.make("environment-primary"),
      ThreadId.make("thread-current"),
    );
    const currentModel = createModelSelection(instance, "current-composer", [
      { id: "reasoningEffort", value: "low" },
    ]);
    const store = useComposerDraftStore.getState();
    store.setModelSelection(sourceRef, currentModel);
    store.setStickyModelSelection(sticky);
    testState.router.state.matches = [{ params: sourceRef }];
    testState.router.state.location.href = "/environment-primary/thread-current";

    const opened = await finish(openThread(projectRef));

    expect(opened.draftId).toBe(reuse ? existingDraft : "draft-delayed");
    expect(selection(opened.draftId)).toEqual(currentModel);
    expect(store.getDraftSession(opened.draftId)).toMatchObject(projectRef);
    expect(testState.router.navigate).toHaveBeenCalledExactlyOnceWith({
      to: "/draft/$draftId",
      params: { draftId: opened.draftId },
      replace: false,
    });
  });

  it("falls back to the project model without sticky intent", async () => {
    setup(reuse, { defaultModelSelection: projectModel });
    const opened = await finish(openThread(projectRef));
    expect(selection(opened.draftId)).toEqual(projectModel);
  });

  it.each(["approval-required", "auto-accept-edits", "auto", "full-access"] as const)(
    "uses the destination's %s runtime mode",
    async (runtimeMode) => {
      setup(reuse, { defaultRuntimeMode: runtimeMode });
      const opened = await finish(openThread(projectRef));
      expect(useComposerDraftStore.getState().getDraftSession(opened.draftId)?.runtimeMode).toBe(
        runtimeMode,
      );
    },
  );

  it("abandons a delayed draft open after navigation", async () => {
    setup(reuse);
    const before = useComposerDraftStore.getState().draftThreadsByThreadKey;
    const pending = openThread(projectRef, { replace: true });
    testState.router.state.location.href = "/usage";
    completeFileRead(null);
    expect(await pending).toBeNull();
    expect(testState.router.state.location.href).toBe("/usage");
    expect(testState.router.navigate).not.toHaveBeenCalled();
    expect(useComposerDraftStore.getState().draftThreadsByThreadKey).toBe(before);
  });

  it.each(["local", "worktree"] as const)(
    "resolves inherited null from t3.json to %s",
    async (envMode) => {
      setup(reuse, { newWorktreesStartFromOrigin: true });
      const opened = await finish(openThread(projectRef), { defaultThreadEnvMode: envMode });
      expect(testState.readProjectFile).toHaveBeenCalledExactlyOnceWith(
        projectRef.environmentId,
        "/remote/project",
      );
      expect(useComposerDraftStore.getState().getDraftSession(opened.draftId)).toMatchObject({
        envMode,
        startFromOrigin: envMode === "worktree",
      });
    },
  );

  it.each(["local", "worktree"] as const)(
    "honors explicit environment default %s without consulting t3.json",
    async (envMode) => {
      setup(reuse, { defaultThreadEnvMode: envMode });
      const opened = await openThread(projectRef);
      expect(testState.readProjectFile).not.toHaveBeenCalled();
      expect(useComposerDraftStore.getState().getDraftSession(opened!.draftId)?.envMode).toBe(
        envMode,
      );
    },
  );

  it.each([true, false])(
    "uses the destination's start-from-origin default %s",
    async (startFromOrigin) => {
      setup(reuse, {
        defaultThreadEnvMode: "worktree",
        newWorktreesStartFromOrigin: startFromOrigin,
      });
      const opened = await openThread(projectRef);
      expect(useComposerDraftStore.getState().getDraftSession(opened!.draftId)).toMatchObject({
        envMode: "worktree",
        startFromOrigin,
      });
    },
  );

  it.each([true, false])(
    "preserves explicit workspace options including start-from-origin %s",
    async (startFromOrigin) => {
      setup(reuse, {
        defaultThreadEnvMode: "local",
        newWorktreesStartFromOrigin: !startFromOrigin,
      });
      const options = {
        envMode: "worktree" as const,
        startFromOrigin,
        branch: "topic",
        worktreePath: "/remote/worktree",
      };
      const opened = await openThread(projectRef, options);
      expect(useComposerDraftStore.getState().getDraftSession(opened!.draftId)).toMatchObject(
        options,
      );
      expect(testState.readProjectFile).not.toHaveBeenCalled();
    },
  );
});

describe("reusable draft intent", () => {
  it("does not carry an already-open draft's implicit model back into itself", async () => {
    setup(true, { defaultModelSelection: projectModel });
    const store = useComposerDraftStore.getState();
    store.setStickyModelSelection(sticky);
    testState.router.state.matches = [{ params: { draftId: existingDraft } }];
    testState.router.state.location.href = `/draft/${existingDraft}`;

    const opened = await openThread(projectRef);

    expect(opened?.draftId).toBe(existingDraft);
    expect(selection(existingDraft)).toEqual(sticky);
    expect(testState.readProjectFile).not.toHaveBeenCalled();
    expect(testState.router.navigate).not.toHaveBeenCalled();
  });

  it("preserves an explicit model instead of sticky and project defaults", async () => {
    setup(true, { defaultModelSelection: projectModel });
    const store = useComposerDraftStore.getState();
    store.setStickyModelSelection(sticky);
    store.setModelSelection(existingDraft, explicit, { explicit: true });
    const opened = await finish(openThread(projectRef));
    expect(opened.draftId).toBe(existingDraft);
    expect(selection(existingDraft)).toEqual(explicit);
    expect(store.getComposerDraft(existingDraft)?.modelSelectionExplicit).toBe(true);
  });

  it("keeps an invested draft's prompt and target when minting a fresh draft", async () => {
    setup(true, { defaultModelSelection: projectModel });
    const store = useComposerDraftStore.getState();
    store.setPrompt(existingDraft, "Keep this draft intact");
    store.setModelSelection(existingDraft, explicit, { explicit: true });
    store.setStickyModelSelection(sticky);
    const opened = await finish(openThread(projectRef));
    expect(opened.draftId).not.toBe(existingDraft);
    expect(selection(opened.draftId)).toEqual(sticky);
    expect(selection(existingDraft)).toEqual(explicit);
    expect(store.getComposerDraft(existingDraft)?.prompt).toBe("Keep this draft intact");
    expect(store.getDraftSession(existingDraft)).toMatchObject(projectRef);
  });
});
