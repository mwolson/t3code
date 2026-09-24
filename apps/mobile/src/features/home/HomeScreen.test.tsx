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
  useWindowDimensions: () => ({ width: 390, height: 844 }),
  ScrollView: () => null,
  View: ({ children }: { children?: ReactNode }) => children,
  ActivityIndicator: () => null,
}));
vi.mock("react-native-gesture-handler", () => ({
  GestureDetector: ({ children }: { children?: ReactNode }) => children,
  useNativeGesture: () => ({}),
}));
vi.mock("../threads/use-open-thread-pull-requests", () => ({
  useOpenThreadPullRequests: () => ({}),
}));
vi.mock("../../native/useRestingTopOffset", () => ({ useRestingTopOffset: () => 0 }));
vi.mock("../workspace/WorkspaceTabsContext", () => ({ useWorkspaceTabs: () => null }));
vi.mock("./ThreadListFilters", () => ({ ThreadListFilters: () => null }));
vi.mock("../threads/threadListAnimations", () => ({ THREAD_SIDEBAR_ROW_LAYOUT: undefined }));
vi.mock("@legendapp/list/reanimated", () => ({
  AnimatedLegendList: (props: { data: ThreadListV2ListItem[]; ListEmptyComponent: ReactNode }) => {
    probe.list(props.data);
    return props.data.length === 0 ? props.ListEmptyComponent : null;
  },
}));
vi.mock("../../components/ui/EmptyState", () => ({
  EmptyState: (props: { title: string }) => {
    probe.empty(props.title);
    return null;
  },
}));
vi.mock("../../components/ui/FloatingActionButton", () => ({
  FloatingActionButton: () => null,
}));
vi.mock("../../components/ui/useAndroidControlSizing", async () => {
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
  useThreadSearch: (_environmentIds: ReadonlyArray<string>, query: string) => ({
    matches: [],
    query,
    isPending: false,
  }),
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
vi.mock("./FabScrollContext", () => ({ useFabScroll: () => () => {} }));
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
    hasSyncingConnectedEnvironment: false,
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
it("shows the full-page empty state when only subagent children remain", async () => {
  await render({
    threads: [child, { ...thread, archivedAt: thread.createdAt }],
    searchQuery: "missing",
  });
  expect(probe.empty).toHaveBeenLastCalledWith("No threads yet");
  expect(probe.list).not.toHaveBeenCalled();

  probe.empty.mockClear();
  await render({ threads: [thread, child], searchQuery: "missing" });
  expect(probe.empty).toHaveBeenLastCalledWith("No results");
  expect(probe.list).toHaveBeenLastCalledWith([]);
});
