import { scopeProjectRef } from "@t3tools/client-runtime/environment";
import {
  DEFAULT_CLIENT_SETTINGS,
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  type ClientSettings,
  type ServerSettings,
} from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import { act, type ReactNode } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

import { DraftId, useComposerDraftStore } from "~/composerDraftStore";
import type { Project } from "~/types";

const fixtures = vi.hoisted(() => ({
  projects: [] as Project[],
  environments: [] as {
    environmentId: EnvironmentId;
    label: string;
    serverConfig: { settings: ServerSettings; environment: { platform: "linux" } } | null;
  }[],
}));
vi.mock("~/state/entities", () => ({
  useProjects: () => fixtures.projects,
  useThreadShells: () => [],
}));
vi.mock("~/state/environments", () => ({
  useEnvironments: () => ({ environments: fixtures.environments }),
  usePrimaryEnvironmentId: () => fixtures.projects[0]?.environmentId ?? null,
}));
vi.mock("~/hooks/useSettings", () => ({
  useClientSettings: (select: (settings: ClientSettings) => unknown) =>
    select(DEFAULT_CLIENT_SETTINGS),
}));
// Only replace presentation primitives, leaving the project handler and draft store real.
vi.mock("../ui/menu", () => {
  const Container = ({ children }: { children?: ReactNode }) => <>{children}</>;
  return {
    Menu: Container,
    MenuItem: Container,
    MenuPopup: Container,
    MenuRadioGroup: ({ children }: { children?: ReactNode }) => <>{children}</>,
    MenuRadioItem: ({ children }: { children?: ReactNode }) => <>{children}</>,
    MenuSeparator: () => null,
    MenuTrigger: Container,
  };
});
vi.mock("../ui/tooltip", () => ({
  Tooltip: ({ children }: { children?: ReactNode }) => <>{children}</>,
  TooltipTrigger: ({ children }: { children?: ReactNode }) => <>{children}</>,
  TooltipPopup: () => null,
}));
vi.mock("../ProjectFavicon", () => ({ ProjectFavicon: () => null }));
vi.mock("../ProjectEnvironmentBadge", () => ({ ProjectEnvironmentBadge: () => null }));

import { MenuRadioGroup, MenuRadioItem } from "../ui/menu";
import { DraftHeroHeadline } from "./DraftHeroHeadline";

const sourceEnvironment = EnvironmentId.make("source-environment");
const targetEnvironment = EnvironmentId.make("target-environment");
const draftId = DraftId.make("open-draft");
const instance = ProviderInstanceId.make("codex");
const previousSeed = createModelSelection(instance, "old-project-model");
const projectSelection = createModelSelection(instance, "project-model");
const stickySelection = createModelSelection(instance, "last-used-model", [
  { id: "reasoningEffort", value: "high" },
]);
const explicitSelection = createModelSelection(instance, "explicit-model");
let renderer: ReactTestRenderer | undefined;

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  useComposerDraftStore.setState({
    draftsByThreadKey: {},
    draftThreadsByThreadKey: {},
    logicalProjectDraftThreadKeyByLogicalProjectKey: {},
    stickyActiveProvider: null,
    stickyModelSelectionByProvider: {},
  });
  fixtures.projects = [
    makeProject("source", sourceEnvironment),
    makeProject("target", targetEnvironment),
  ];
  fixtures.environments = [
    { environmentId: sourceEnvironment, label: "Source", serverConfig: null },
    { environmentId: targetEnvironment, label: "Target", serverConfig: null },
  ];
  const store = useComposerDraftStore.getState();
  store.setLogicalProjectDraftThreadId(
    "source-key",
    scopeProjectRef(sourceEnvironment, fixtures.projects[0]!.id),
    draftId,
  );
  store.setPrompt(draftId, "Keep this prompt while switching projects");
  store.setModelSelection(draftId, previousSeed);
});

afterEach(() => {
  act(() => renderer?.unmount());
  renderer = undefined;
  vi.unstubAllGlobals();
});

describe("draft hero project switching", () => {
  it("uses the current sticky choice over the destination project default", () => {
    mount();
    // A choice made after render must win over both the old implicit seed and project pin.
    useComposerDraftStore.getState().setStickyModelSelection(stickySelection);
    chooseTarget();
    expectSelection(stickySelection);
    expect(
      useComposerDraftStore.getState().getComposerDraft(draftId)?.modelSelectionExplicit,
    ).toBeUndefined();
    expectContinuity();
  });

  it("falls back to the project default without mistaking the old implicit seed for sticky", () => {
    mount();
    chooseTarget();
    expectSelection(projectSelection);
    expect(useComposerDraftStore.getState().stickyActiveProvider).toBeNull();
    expectContinuity();
  });

  it("retains an explicit draft choice despite sticky and project defaults", () => {
    const store = useComposerDraftStore.getState();
    store.setModelSelection(draftId, explicitSelection, { explicit: true });
    store.setStickyModelSelection(stickySelection);
    mount();
    chooseTarget();
    expectSelection(explicitSelection);
    expect(useComposerDraftStore.getState().getComposerDraft(draftId)?.modelSelectionExplicit).toBe(
      true,
    );
    expectContinuity();
  });

  it.each(["project override", "environment fallback"] as const)(
    "resolves the destination's %s when sticky is absent",
    (source) => {
      const resolvedSelection = createModelSelection(instance, "resolved-model");
      const target = fixtures.projects[1]!;
      fixtures.environments[1]!.serverConfig = {
        environment: { platform: "linux" },
        settings: {
          ...DEFAULT_SERVER_SETTINGS,
          projectSettingsFolded: true,
          defaultModelSelection: resolvedSelection,
          projectSettingsOverrides:
            source === "project override"
              ? { [target.id]: { defaultModelSelection: projectSelection } }
              : {},
        },
      };
      mount();
      chooseTarget();
      expectSelection(source === "project override" ? projectSelection : resolvedSelection);
      expectContinuity();
    },
  );
});

function mount() {
  act(() => {
    renderer = create(
      <DraftHeroHeadline
        draftId={draftId}
        activeProjectRef={scopeProjectRef(sourceEnvironment, fixtures.projects[0]!.id)}
        activeProjectTitle="source"
      />,
    );
  });
}

function chooseTarget() {
  const picker = renderer!.root.findByType(MenuRadioGroup);
  const target = renderer!.root.findAllByType(MenuRadioItem)[1]!;
  act(() => picker.props.onValueChange(target.props.value));
}

function expectSelection(expected: ReturnType<typeof createModelSelection>) {
  const draft = useComposerDraftStore.getState().getComposerDraft(draftId);
  expect(draft).not.toBeNull();
  expect(draft!.activeProvider).toBe(expected.instanceId);
  expect(draft!.modelSelectionByProvider[expected.instanceId]).toEqual(expected);
}

function expectContinuity() {
  const store = useComposerDraftStore.getState();
  expect(store.getComposerDraft(draftId)?.prompt).toBe("Keep this prompt while switching projects");
  expect(store.getDraftSession(draftId)).toMatchObject({
    environmentId: targetEnvironment,
    projectId: fixtures.projects[1]!.id,
  });
  expect(Object.keys(store.draftsByThreadKey)).toEqual([draftId]);
  expect(Object.keys(store.draftThreadsByThreadKey)).toEqual([draftId]);
  expect(Object.values(store.logicalProjectDraftThreadKeyByLogicalProjectKey)).toEqual([draftId]);
  expect(store.logicalProjectDraftThreadKeyByLogicalProjectKey["source-key"]).toBeUndefined();
}

function makeProject(title: string, environmentId: EnvironmentId): Project {
  return {
    id: ProjectId.make(title),
    environmentId,
    title,
    workspaceRoot: `/work/${title}`,
    repositoryIdentity: null,
    defaultModelSelection: projectSelection,
    createdAt: "2026-09-22T00:00:00.000Z",
    updatedAt: "2026-09-22T00:00:00.000Z",
    scripts: [],
  };
}
