import { scopeProjectRef } from "@t3tools/client-runtime/environment";
import {
  DEFAULT_CLIENT_SETTINGS,
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  type ClientSettings,
  type ServerSettings,
} from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import { type ReactNode } from "react";
import { render, act } from "../../../test/render";
import { describe, expect, it, vi } from "vite-plus/test";

import { DraftId, useComposerDraftStore } from "~/composerDraftStore";
import type { Project } from "~/types";

const fixtures = vi.hoisted(() => ({
  projects: [] as Project[],
  choose: (_value: unknown) => {},
  items: [] as { label: string }[],
  environments: [] as {
    environmentId: EnvironmentId;
    label: string;
    connection: { phase: "connected" };
    serverConfig: { settings: ServerSettings; environment: { platform: "linux" } } | null;
  }[],
}));
vi.mock("~/hooks/useHandleNewThread", () => ({ useNewThreadHandler: () => () => {} }));
vi.mock("~/hooks/useScratchProject", () => ({
  useScratchProject: () => ({
    scratchEnvironmentId: () => null,
    scratchWorkspaceRootFor: () => null,
    openScratchProject: () => {},
  }),
}));
vi.mock("~/state/server", () => ({ primaryServerKeybindingsAtom: {}, serverEnvironment: {} }));
vi.mock("@effect/atom-react", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@effect/atom-react")>()),
  useAtomValue: () => [],
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
vi.mock("../ui/combobox", () => {
  const Container = ({ children }: { children?: ReactNode }) => <>{children}</>;
  return {
    Combobox: (props: {
      children?: ReactNode;
      onValueChange: (value: unknown) => void;
      items: { label: string }[];
    }) => {
      fixtures.choose = props.onValueChange;
      fixtures.items = props.items;
      return <>{props.children}</>;
    },
    ComboboxEmpty: () => null,
    ComboboxItem: Container,
    ComboboxList: () => null,
    ComboboxPopup: Container,
    ComboboxSearchInput: () => null,
    ComboboxTrigger: Container,
    useComboboxFilter: () => ({ contains: () => true }),
  };
});
vi.mock("../ui/tooltip", () => ({
  Tooltip: ({ children }: { children?: ReactNode }) => <>{children}</>,
  TooltipTrigger: ({ children }: { children?: ReactNode }) => <>{children}</>,
  TooltipPopup: () => null,
}));
vi.mock("./DraftProjectName", () => ({ DraftProjectName: () => null }));
vi.mock("./DraftHeroProjectHeading", () => ({
  DraftHeroProjectHeading: ({ children }: { children?: ReactNode }) => <>{children}</>,
}));
vi.mock("../ProjectFavicon", () => ({ ProjectFavicon: () => null }));
vi.mock("../ProjectEnvironmentBadge", () => ({ ProjectEnvironmentBadge: () => null }));

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

function setup() {
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
    {
      environmentId: sourceEnvironment,
      label: "Source",
      connection: { phase: "connected" },
      serverConfig: null,
    },
    {
      environmentId: targetEnvironment,
      label: "Target",
      connection: { phase: "connected" },
      serverConfig: null,
    },
  ];
  const store = useComposerDraftStore.getState();
  store.setLogicalProjectDraftThreadId(
    "source-key",
    scopeProjectRef(sourceEnvironment, fixtures.projects[0]!.id),
    draftId,
  );
  store.setPrompt(draftId, "Keep this prompt while switching projects");
  store.setModelSelection(draftId, previousSeed);
}

describe("draft hero project switching", () => {
  it("uses the current sticky choice over the destination project default", async () => {
    setup();
    await using _screen = await mount();
    // A choice made after render must win over both the old implicit seed and project pin.
    useComposerDraftStore.getState().setStickyModelSelection(stickySelection);
    await chooseTarget();
    expectSelection(stickySelection);
  });
});

function mount() {
  return render(
    <DraftHeroHeadline
      draftId={draftId}
      activeProjectRef={scopeProjectRef(sourceEnvironment, fixtures.projects[0]!.id)}
      activeProjectTitle="source"
    />,
  );
}

async function chooseTarget() {
  expect(fixtures.items).toContainEqual(expect.objectContaining({ label: "target" }));
  await act(() => fixtures.choose(fixtures.items.find((item) => item.label === "target")));
}

function expectSelection(expected: ReturnType<typeof createModelSelection>) {
  const draft = useComposerDraftStore.getState().getComposerDraft(draftId);
  expect(draft).not.toBeNull();
  expect(draft!.activeProvider).toBe(expected.instanceId);
  expect(draft!.modelSelectionByProvider[expected.instanceId]).toEqual(expected);
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
