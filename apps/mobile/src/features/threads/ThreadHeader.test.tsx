import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import type { VcsStatusResult } from "@t3tools/contracts";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

// Native header item factories are only read when options are applied, so the fake
// navigation snapshots the right items at each setOptions, as native-stack does.
const harness = vi.hoisted(() => ({
  setOptionsCalls: 0,
  renderedRightItems: [] as Array<ReadonlyArray<Record<string, unknown>>>,
  navigatedRoutes: [] as string[],
  pr: null as import("../../state/thread-pr-presentation").ThreadPrPresentation | null,
  usesSplitView: false,
  contentPaneWidth: 0,
  buttonSize: 48,
  overflowLabels: [] as string[],
  iosTrailingItems: [] as ReadonlyArray<Record<string, unknown>>,
  appBarProps: null as import("../../components/ui/AppBar.types").AppBarProps | null,
  onHeaderLayout: undefined as
    | ((event: { nativeEvent: { layout: { width: number } } }) => void)
    | undefined,
}));

vi.mock("@react-navigation/native", async () => {
  const { createContext } = await import("react");
  const navigation = {
    canGoBack: () => true,
    dispatch: () => {},
    goBack: () => {},
    navigate: (route: string) => {
      harness.navigatedRoutes.push(route);
    },
    addListener: () => () => {},
    isFocused: () => true,
    setOptions: (options: { unstable_headerRightItems?: () => Array<Record<string, unknown>> }) => {
      harness.setOptionsCalls += 1;
      harness.renderedRightItems.push(options.unstable_headerRightItems?.() ?? []);
    },
  };
  return {
    NavigationContext: createContext<unknown>(navigation),
    NavigationRouteContext: createContext<unknown>(undefined),
    StackActions: { replace: () => ({}) },
    useIsFocused: () => true,
    useNavigation: () => navigation,
    useRoute: () => ({ key: "thread", name: "Thread", params: {} }),
  };
});
vi.mock("../../native/useOpenPullRequest", () => ({ useOpenPullRequest: () => () => {} }));
vi.mock("../workspace/useAndroidWorkspaceNavigation", () => ({
  useWorkspaceBackNavigation: () => () => {},
}));
vi.mock("../../state/use-thread-pr", () => ({ useThreadPr: () => harness.pr }));
vi.mock("./ThreadPrHeaderButton", () => ({
  ThreadPrHeaderButton: () => <button aria-label="Pull request" />,
}));
vi.mock("../../components/ui/ScreenHeaderButton", () => ({
  ScreenHeaderButton: (props: { accessibilityLabel?: string }) => (
    <button aria-label={props.accessibilityLabel} />
  ),
}));
vi.mock("../../native/NativeWorkspaceColumns", () => ({
  useNativeWorkspaceColumnsSupported: () => false,
}));
vi.mock("../../native/native-layout-metrics", async (importOriginal) => ({
  ...(await importOriginal<object>()),
  useDeviceType: () => "phone",
}));
// V5StackHeader hands its converted items to the native header config.
vi.mock("react-native-screens", () => ({
  SearchBar: () => null,
  Stack: {
    HeaderConfig: (props: { ios?: { trailingItems?: ReadonlyArray<Record<string, unknown>> } }) => {
      harness.iosTrailingItems = props.ios?.trailingItems ?? [];
      return null;
    },
  },
}));
vi.mock("../../lib/useMobileNavigationTheme", () => ({
  useMobileNavigationTheme: () => ({ colors: { card: "white" } }),
}));
// AppBar.android and AndroidScreenHeader dependencies, so the test can lay out the Android row.
vi.mock("../layout/workspace-sidebar-toolbar", () => ({
  AndroidWorkspaceSidebarButton: () => null,
}));
vi.mock("../../components/ui/ScreenHeaderButton.android", () => ({
  ScreenHeaderButton: () => null,
}));
vi.mock("../../components/ui/AppSymbol", () => ({ SymbolView: () => null }));
vi.mock("../../components/ui/Menu", () => ({ Menu: () => null }));
vi.mock("../../components/ui/SearchField", () => ({ SearchField: () => null }));
vi.mock("../../components/ui/NativeAppBar.android", () => ({
  NativeAppBar: (props: { actions?: React.ReactNode }) => <>{props.actions}</>,
}));
vi.mock("../../components/ui/IconButton", () => ({
  IconButton: (props: { accessibilityLabel?: string }) => (
    <button aria-label={props.accessibilityLabel} />
  ),
}));
vi.mock("../../components/ui/AndroidAnchoredMenu", () => ({
  AndroidAnchoredMenu: (props: {
    actions: ReadonlyArray<{ title: string }>;
    children: (open: () => void) => React.ReactNode;
  }) => {
    harness.overflowLabels = props.actions.map((action) => action.title);
    return props.children(() => {});
  },
}));
vi.mock("../../components/ui/AppActivityIndicator", () => ({ AppActivityIndicator: () => null }));
vi.mock("@expo/ui/jetpack-compose", () => ({
  Column: () => null,
  RNHostView: () => null,
  Text: () => null,
}));
vi.mock("../settings/appearance/useScaledTextRole", () => ({ useScaledTextRole: () => ({}) }));
vi.mock("../../components/ui/useToolbarLayout", () => ({
  useToolbarLayout: () => ({ paddingTop: 0, paddingBottom: 0 }),
}));
vi.mock("../../components/ui/useControlTheme", () => ({ useControlTheme: () => ({ colors: {} }) }));

vi.mock("../../state/session", () => ({ useEnvironmentScope: () => true }));
vi.mock("../../components/ui/AppBar", async () => {
  const { NativeStackScreenOptions } = await import("../../native/StackHeader");
  return {
    AppBar: (props: import("../../components/ui/AppBar.types").AppBarProps) => {
      harness.appBarProps = props;
      return (
        <NativeStackScreenOptions options={props.options} optionsVersion={props.optionsVersion} />
      );
    },
  };
});
vi.mock("react-native", () => ({
  Alert: { alert: () => {} },
  Linking: {},
  Platform: { OS: "ios" },
  StyleSheet: { flatten: (style: unknown) => style },
  View: (props: { children?: React.ReactNode; onLayout?: typeof harness.onHeaderLayout }) => {
    if (props.onLayout) harness.onHeaderLayout = props.onLayout;
    return <>{props.children}</>;
  },
}));
vi.mock("react-native-keyboard-controller", () => ({
  KeyboardController: { dismiss: async () => {} },
  useKeyboardState: (select: (state: { isVisible: boolean }) => unknown) =>
    select({ isVisible: false }),
}));
vi.mock("../../components/ui/useAndroidControlSizing", () => ({
  useAndroidControlSizing: () => ({ buttonSize: harness.buttonSize }),
}));
vi.mock("../layout/AdaptiveWorkspaceLayout", () => ({
  useAdaptiveWorkspaceLayout: () => ({
    layout: { usesSplitView: harness.usesSplitView },
    panes: {
      primarySidebarVisible: false,
      auxiliaryPaneVisible: false,
      contentPaneWidth: harness.contentPaneWidth,
    },
    togglePrimarySidebar: () => {},
    toggleAuxiliaryPane: () => {},
  }),
}));
vi.mock("../layout/native-mail-search-toolbar", () => ({
  NATIVE_MAIL_SEARCH_TOOLBAR_SUPPORTED: true,
  createNativeMailSearchToolbarItem: () => ({}),
}));
vi.mock("../settings/appearance/AppearancePreferencesProvider", () => ({
  useAppearancePreferences: () => ({ themeVariables: {} }),
}));

import { AppBar as AndroidAppBar } from "../../components/ui/AppBar.android";
import { V5StackHeader } from "../../native/V5StackHeader.ios";
import { ThreadHeader } from "./ThreadHeader";
import { threadPrHeaderForm } from "./thread-pr-header-presentation";

const projectScripts: never[] = [];
const thread = { pullRequests: [] } as unknown as EnvironmentThreadShell;

function status(
  files: ReadonlyArray<string>,
  overrides: Partial<VcsStatusResult> = {},
): VcsStatusResult {
  return {
    isRepo: true,
    hasPrimaryRemote: false,
    isDefaultRef: true,
    refName: "master",
    hasWorkingTreeChanges: files.length > 0,
    workingTree: {
      files: files.map((path) => ({ path, insertions: 0, deletions: 0 })),
      insertions: 0,
      deletions: 0,
    },
    hasUpstream: false,
    aheadCount: 0,
    behindCount: 0,
    pr: null,
    ...overrides,
  } as unknown as VcsStatusResult;
}

// Detail pane widths: the 402-point iPhone, the Duo inner display in portrait beside its
// 280-point sidebar, and a 13-inch iPad in landscape beside its 380-point sidebar.
const PHONE_PANE_WIDTH = 402;
const DUO_PORTRAIT_PANE_WIDTH = 389;
const IPAD_LANDSCAPE_PANE_WIDTH = 996;

function Header(props: {
  readonly gitStatus: VcsStatusResult | null;
  readonly title?: string;
  readonly paneWidth?: number;
  readonly fileInspectorSupported?: boolean;
}) {
  return (
    <ThreadHeader
      thread={thread}
      title={props.title ?? "Thread"}
      subtitle=""
      headerColor="#000"
      usesNativeHeaderGlass
      gitControls={{
        environmentId: "environment-1",
        threadId: "thread-1",
        currentBranch: "master",
        gitStatus: props.gitStatus,
        gitOperationLabel: null,
        onPull: async () => {},
        onRunAction: async () => null,
        canOpenTerminal: true,
        canOperateTerminal: true,
        canOpenFiles: true,
        projectScripts,
        terminalSessions: [],
        onOpenTerminal: () => {},
        onOpenNewTerminal: () => {},
        onRunProjectScript: async () => {},
      }}
      hasThreadCwd
      hasWorkspaceRoot
      fileInspectorSupported={props.fileInspectorSupported ?? false}
      inspectorMode={null}
      onToggleInspector={() => {}}
      onOpenGitInspector={() => {}}
      onOpenFilesInspector={() => {}}
      pullRequestForm={threadPrHeaderForm(props.paneWidth ?? PHONE_PANE_WIDTH)}
    />
  );
}

function pullRequest(state: "open" | "merged") {
  return {
    number: 1006,
    state,
    kind: "pull-request",
    others: 0,
    isDraft: false,
    updatedAt: null,
    url: "https://github.com/t3tools/t3code/pull/1006",
    label: "1006",
    accessibilityLabel: `#1006 pull request ${state}`,
    textClassName: "",
    checksState: null,
    mergeability: null,
    changeRequest: {
      shortLabel: "PR",
      shortLabelWithArticle: "a PR",
      singular: "pull request",
      plural: "pull requests",
    },
  } as const;
}

// Runs the last applied right items through V5StackHeader, the iOS workspace header adapter.
// UIKit lays out `rightBarButtonItems` from the trailing edge, so the result is reversed into
// on-screen order.
function lastRightItemsOnScreen(): string[] {
  const items = harness.renderedRightItems.at(-1) ?? [];
  const headerRoot = createRoot(document.createElement("div"));
  act(() =>
    headerRoot.render(
      <V5StackHeader options={{ unstable_headerRightItems: () => items } as never} canGoBack />,
    ),
  );
  act(() => headerRoot.unmount());
  return harness.iosTrailingItems
    .map((item) => {
      if (item.type === "spacer") return "spacing";
      if (typeof item.render === "function") return "pull request";
      return String(item.identifier ?? item.title).replace("thread-right-", "");
    })
    .reverse();
}

function pullRequestForm(): unknown {
  const items = harness.renderedRightItems.at(-1) ?? [];
  const item = items.find((candidate) => candidate.type === "custom");
  return (item?.element as React.ReactElement<{ form?: unknown }> | undefined)?.props.form;
}

// Renders the props ThreadHeader gave AppBar through AppBar.android and reads its action row
// left to right.
function androidControlsOnScreen(headerWidth: number): string[] {
  const props = harness.appBarProps;
  if (props === null) throw new Error("expected AppBar props");
  const threadHeaderLayout = harness.onHeaderLayout;
  const container = document.createElement("div");
  const headerRoot = createRoot(container);
  act(() => headerRoot.render(<AndroidAppBar {...props} />));
  act(() => harness.onHeaderLayout?.({ nativeEvent: { layout: { width: headerWidth } } }));
  const labels = [...container.querySelectorAll("[aria-label]")].map((element) =>
    String(element.getAttribute("aria-label")),
  );
  act(() => headerRoot.unmount());
  harness.onHeaderLayout = threadHeaderLayout;
  return labels;
}

function overflowLabels(): string[] {
  return harness.overflowLabels;
}

// The react project compiles sources for iOS, which inlines `Platform.OS`, so ThreadHeader's
// Android-only layout wrapper never renders here. The pane width seeds the header width instead.
function renderAndroid(element: React.ReactElement, headerWidth: number): string[] {
  harness.contentPaneWidth = headerWidth;
  render(element);
  return androidControlsOnScreen(headerWidth);
}

let root: Root | null = null;

function render(element: React.ReactElement) {
  (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  root ??= createRoot(document.createElement("div"));
  act(() => root!.render(element));
}

afterEach(() => {
  act(() => root?.unmount());
  root = null;
  harness.setOptionsCalls = 0;
  harness.renderedRightItems = [];
  harness.navigatedRoutes = [];
  harness.pr = null;
  harness.usesSplitView = false;
  harness.contentPaneWidth = 0;
  harness.buttonSize = 48;
  harness.appBarProps = null;
  harness.overflowLabels = [];
  harness.iosTrailingItems = [];
  harness.onHeaderLayout = undefined;
});

describe("thread pull request header", () => {
  it("puts the iOS pull request button just left of the rightmost control and keeps it current", () => {
    render(<Header gitStatus={status([])} />);
    expect(lastRightItemsOnScreen()).toEqual(["terminal", "files", "git"]);

    harness.pr = pullRequest("open");
    render(<Header gitStatus={status([])} />);
    expect(lastRightItemsOnScreen()).toEqual(["terminal", "files", "pull request", "git"]);
    expect(pullRequestForm()).toBe("compact");

    harness.usesSplitView = true;
    render(<Header gitStatus={status([])} paneWidth={IPAD_LANDSCAPE_PANE_WIDTH} />);
    expect(lastRightItemsOnScreen()).toEqual(["terminal", "git", "pull request", "files"]);
    expect(pullRequestForm()).toBe("regular");
    expect(harness.renderedRightItems.at(-1)?.some((item) => item.type === "spacing")).toBe(false);

    const applied = harness.setOptionsCalls;
    render(<Header gitStatus={status([])} paneWidth={DUO_PORTRAIT_PANE_WIDTH} />);
    expect(pullRequestForm()).toBe("compact");
    expect(harness.setOptionsCalls).toBeGreaterThan(applied);

    const resized = harness.setOptionsCalls;
    harness.pr = pullRequest("merged");
    render(<Header gitStatus={status([])} paneWidth={DUO_PORTRAIT_PANE_WIDTH} />);
    expect(harness.setOptionsCalls).toBeGreaterThan(resized);
  });

  it("puts the Android pull request button left of the inspector toggle or the last control", () => {
    harness.pr = pullRequest("open");
    expect(
      renderAndroid(<Header gitStatus={status([])} fileInspectorSupported />, PHONE_PANE_WIDTH),
    ).toEqual(["Open files", "More actions", "Pull request", "Show inspector"]);
    expect(renderAndroid(<Header gitStatus={status([])} />, PHONE_PANE_WIDTH)).toEqual([
      "Open files",
      "Pull request",
      "More actions",
    ]);
  });

  // A hinge-aligned 320-dp split detail pane, whose leading slot holds the sidebar toggle, at
  // the default text size and at in-app base font size 20 (60 dp controls).
  it("folds Android actions and then the inspector toggle into overflow in a narrow pane", () => {
    harness.usesSplitView = true;
    harness.pr = pullRequest("open");
    const header = () => <Header gitStatus={status([])} paneWidth={320} fileInspectorSupported />;
    expect(renderAndroid(header(), 320)).toEqual([
      "More actions",
      "Pull request",
      "Show inspector",
    ]);
    expect(overflowLabels()).toEqual(["Open files", "Open terminal", "Open git controls"]);

    harness.buttonSize = 60;
    expect(renderAndroid(header(), 320)).toEqual(["Pull request", "More actions"]);
    expect(overflowLabels()).toEqual([
      "Open files",
      "Open terminal",
      "Open git controls",
      "Show inspector",
    ]);
  });
});
