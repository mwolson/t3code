import { act, type ComponentProps, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { EnvironmentId, ProjectId, ThreadId } from "@t3tools/contracts";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import type { ThreadListV2ListItem } from "../threads/threadListV2";
import { makeThreadShellFixture } from "../../test-fixtures";

const probe = vi.hoisted(() => ({
  empty: vi.fn(),
  list: vi.fn(),
  capabilities: new Set<string>(),
}));
vi.mock("react-native", () => ({
  Platform: { OS: "ios" },
  View: ({ children }: { children?: ReactNode }) => children,
  ActivityIndicator: () => null,
}));
vi.mock("@legendapp/list/react-native", () => ({
  LegendList: (props: { data: ThreadListV2ListItem[]; ListEmptyComponent: ReactNode }) => {
    probe.list(props.data);
    return props.data.length === 0 ? props.ListEmptyComponent : null;
  },
}));
vi.mock("../../components/EmptyState", () => ({
  EmptyState: (props: { title: string }) => {
    probe.empty(props.title);
    return null;
  },
}));
vi.mock("../../components/MaterialFloatingActionButton", () => ({
  MaterialFloatingActionButton: () => null,
}));
vi.mock("../../components/useAndroidControlSizing", async () => {
  const { resolveAndroidControlSizing } = await import("../../lib/androidControlSizing");
  const { DEFAULT_BASE_FONT_SIZE } = await import("../../lib/appearancePreferences");
  return { useAndroidControlSizing: () => resolveAndroidControlSizing(DEFAULT_BASE_FONT_SIZE) };
});
vi.mock("@react-navigation/native", () => ({ useFocusEffect: () => {} }));
vi.mock("react-native-screens", () => ({
  ScrollViewMarker: ({ children }: { children?: ReactNode }) => children,
}));
vi.mock("../../native/native-layout-metrics", () => ({
  useNativeLayoutMetrics: () => null,
  useNativeColumnLayoutMetrics: () => null,
}));
vi.mock("../../native/NativeWorkspaceColumns", () => ({
  useNativeWorkspaceColumnsSupported: () => false,
}));
vi.mock("react-native-safe-area-context", () => ({
  useSafeAreaInsets: () => ({ top: 0, bottom: 0 }),
}));
vi.mock("../../native/native-glass", () => ({ NATIVE_LIQUID_GLASS_SUPPORTED: false }));
vi.mock("../../state/queries", () => ({
  useThreadSearch: () => ({ matches: [], isPending: false }),
}));
vi.mock("../keyboard/threadKeyboardShortcuts", () => ({ useThreadJumpShortcuts: () => {} }));
vi.mock("../../state/thread-order", () => ({ usePendingThreadOrder: () => null }));
vi.mock("../../state/use-thread-outbox", () => ({ useQueuedThreadKeys: () => new Set() }));
vi.mock("../../state/server", () => ({ threadListEnvironmentsAtom: {} }));
vi.mock("@effect/atom-react", () => ({
  useAtomValue: () => ({
    providersByEnvironmentId: new Map(),
    machineByEnvironmentId: new Map(),
    settlementEnvironmentIds: probe.capabilities,
    snoozeEnvironmentIds: probe.capabilities,
    pinningEnvironmentIds: probe.capabilities,
    pinReorderEnvironmentIds: new Set(),
    activeReorderEnvironmentIds: new Set(),
    titleRegenerationEnvironmentIds: new Set(),
    autoSettleOptOutEnvironmentIds: new Set(),
  }),
}));
vi.mock("../threads/thread-provider-instance", () => ({
  useThreadRowProviderInstanceResolver: () => () => null,
}));
vi.mock("../threads/thread-list-v2-items", () => ({
  ThreadListV2PendingRow: () => null,
  ThreadListV2Row: () => null,
  ThreadListV2SettledShelfHeader: () => null,
  ThreadListV2ShowMoreRow: () => null,
  ThreadListV2SnoozedShelfHeader: () => null,
  ThreadListV2WorkingShelfHeader: () => null,
}));
vi.mock("../threads/use-thread-list-v2-shelf-preferences", () => ({
  useThreadListV2ShelfPreferences: () => ({
    loaded: true,
    workingShelfEnabled: false,
    workingShelfExpanded: true,
    toggleWorkingShelf: () => {},
    settledShelfExpanded: true,
    snoozedShelfExpanded: true,
    toggleSettledShelf: () => {},
    toggleSnoozedShelf: () => {},
  }),
}));
vi.mock("./thread-swipe-actions", () => ({
  SwipeableScrollGateProvider: ({ children }: { children?: ReactNode }) => children,
  useSwipeableScrollGate: () => ({ swipeEnabled: true, scrollGateHandlers: {} }),
}));
vi.mock("./MaterialFabScrollContext", () => ({ useMaterialFabScroll: () => () => {} }));
import { HomeScreen } from "./HomeScreen";

const environmentId = EnvironmentId.make("home-test");
const projectId = ProjectId.make("home-project");
const thread = makeThreadShellFixture({
  environmentId,
  projectId,
  id: ThreadId.make("root"),
  title: "Root",
});
const child = makeThreadShellFixture({
  ...thread,
  id: ThreadId.make("child"),
  lineage: { parentThreadId: thread.id, rootThreadId: thread.id, relationshipToParent: "subagent" },
});
const noop = () => {};
const success = async () => true;
const props: ComponentProps<typeof HomeScreen> = {
  threads: [],
  pendingTasks: [],
  projects: [
    {
      environmentId,
      id: projectId,
      title: "Project",
      workspaceRoot: "/project",
      repositoryIdentity: null,
      defaultModelSelection: null,
      scripts: [],
      createdAt: thread.createdAt,
      updatedAt: thread.updatedAt,
    },
  ],
  catalogState: {
    isLoadingConnections: false,
    hasConnections: true,
    hasReadyEnvironment: true,
    hasConnectingEnvironment: false,
    connectingEnvironments: [],
    connectionState: "connected",
    connectionError: null,
    networkStatus: "online",
    hasLoadedShellSnapshot: true,
    hasPendingShellSnapshot: false,
    shellSnapshotError: null,
  },
  savedConnectionsById: {},
  environments: [],
  searchQuery: "",
  selectedEnvironmentId: null,
  selectedProjectKey: null,
  projectSortOrder: "created_at",
  projectGroupingMode: "repository",
  onSearchQueryChange: noop,
  onEnvironmentChange: noop,
  onProjectChange: noop,
  onAddConnection: noop,
  onOpenSettings: noop,
  onStartNewTask: noop,
  onSelectThread: noop,
  onArchiveThread: noop,
  onDeleteThread: noop,
  onSettleThread: success,
  onSnoozeThread: success,
  onUnsnoozeThread: success,
  onUnsettleThread: noop,
  onSetThreadAutoSettle: success,
  onPinThread: success,
  onUnpinThread: success,
  onMoveThread: success,
  onRenameThread: noop,
  onRegenerateThreadTitle: success,
  onSelectPendingTask: noop,
  onDeletePendingTask: noop,
  onNewThreadOnBranch: noop,
  onNewThreadInProject: noop,
};
let root: Root;
beforeEach(() => {
  probe.empty.mockClear();
  probe.list.mockClear();
  probe.capabilities.add(environmentId);
  const document = { nodeType: 9, addEventListener() {}, removeEventListener() {} };
  const container = {
    nodeType: 1,
    tagName: "DIV",
    namespaceURI: "http://www.w3.org/1999/xhtml",
    ownerDocument: document,
    addEventListener() {},
    removeEventListener() {},
  };
  vi.stubGlobal("document", document);
  vi.stubGlobal("window", { document, HTMLIFrameElement: EventTarget });
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  root = createRoot(container as unknown as HTMLElement);
});
afterEach(async () => {
  await act(() => root.unmount());
  vi.unstubAllGlobals();
});
async function render(overrides: Partial<ComponentProps<typeof HomeScreen>>) {
  await act(() => root.render(<HomeScreen {...props} {...overrides} />));
}
it("shows the full-page empty state for child-only and archived history", async () => {
  await render({
    threads: [child, { ...thread, archivedAt: thread.createdAt }],
    searchQuery: "missing",
  });
  expect(probe.empty).toHaveBeenLastCalledWith("No threads yet");
  expect(probe.list).not.toHaveBeenCalled();
});
it("keeps roots and user forks while omitting children from the current list", async () => {
  const fork = {
    ...thread,
    id: ThreadId.make("fork"),
    lineage: { ...child.lineage, relationshipToParent: "fork" as const },
  };
  await render({
    threads: [
      thread,
      child,
      fork,
      { ...child, id: ThreadId.make("archived-child"), archivedAt: thread.createdAt },
    ],
  });
  const items: ThreadListV2ListItem[] = probe.list.mock.lastCall![0];
  expect(
    items.flatMap((item) => (item.type === "v2-thread" ? [item.item.thread.id] : [])).sort(),
  ).toEqual([fork.id, thread.id].sort());
  expect(probe.empty).not.toHaveBeenCalled();
});
it.each(["pinned", "settled", "snoozed"] as const)(
  "counts %s roots as global content",
  async (section) => {
    await render({
      threads: [
        {
          ...thread,
          ...(section === "pinned" ? { pinnedAt: thread.createdAt } : {}),
          ...(section === "settled"
            ? { settledOverride: "settled" as const, settledAt: thread.createdAt }
            : {}),
          ...(section === "snoozed"
            ? { snoozedAt: thread.createdAt, snoozedUntil: "2099-01-01T00:00:00.000Z" }
            : {}),
        },
        child,
      ],
    });
    const items: ThreadListV2ListItem[] = probe.list.mock.lastCall![0];
    expect(
      items.some((item) => item.type === "v2-thread" && item.item.thread.id === thread.id),
    ).toBe(true);
    expect(probe.empty).not.toHaveBeenCalled();
  },
);
it("uses in-list no-results states without confusing filters with global emptiness", async () => {
  await render({ threads: [thread, child], searchQuery: "missing" });
  expect(probe.empty).toHaveBeenLastCalledWith("No results");
  expect(probe.list).toHaveBeenLastCalledWith([]);
  await render({ threads: [thread, child], selectedEnvironmentId: EnvironmentId.make("other") });
  expect(probe.empty).toHaveBeenLastCalledWith("No threads in this environment");
  expect(probe.list).toHaveBeenLastCalledWith([]);
});
it("counts pending drafts even with only child history", async () => {
  await render({
    threads: [child],
    pendingTasks: [
      {
        kind: "draft",
        key: "pending",
        environmentId,
        projectId,
        projectTitle: undefined,
        projectCwd: undefined,
        branch: null,
        title: "Draft",
        createdAt: thread.createdAt,
        draftKey: "draft",
        draft: { text: "Draft", attachments: [] },
      },
    ],
  });
  const items: ThreadListV2ListItem[] = probe.list.mock.lastCall![0];
  expect(items.map((item) => item.type)).toEqual(["v2-pending"]);
  expect(probe.empty).not.toHaveBeenCalled();
});
