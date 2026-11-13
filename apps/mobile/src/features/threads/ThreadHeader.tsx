import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import { Platform } from "react-native";
import { useOpenPullRequest } from "../../native/useOpenPullRequest";
import { useThreadPr } from "../../state/use-thread-pr";
import { useAndroidControlSizing } from "../../components/ui/useAndroidControlSizing";
import { ThreadPrHeaderButton } from "./ThreadPrHeaderButton";
import { budgetAndroidThreadHeader } from "./thread-header-android-budget";
import { threadPrHeaderForm } from "./thread-pr-header-presentation";
import { AppBar } from "../../components/ui/AppBar";
import type { AppBarAction, AppBarMenu } from "../../components/ui/AppBar.types";
import { useAdaptiveWorkspaceLayout } from "../layout/AdaptiveWorkspaceLayout";
import type { ThreadInspectorMode } from "./thread-inspector-navigation";
import { useWorkspaceBackNavigation } from "../layout/workspace/useAndroidWorkspaceNavigation";
import {
  threadTerminalMenu,
  useThreadGitMenuDefinition,
  type ThreadGitControlsProps,
} from "./ThreadGitControls";

export function ThreadHeader(props: {
  readonly thread: EnvironmentThreadShell;
  readonly paneWidth?: number;
  readonly title: string;
  readonly subtitle: string;
  readonly gitControls: ThreadGitControlsProps;
  readonly onReturnToThread?: () => void;
  readonly draft?: boolean;
  readonly hasThreadCwd: boolean;
  readonly inspectorMode: ThreadInspectorMode | null;
  readonly onOpenFilesInspector: () => void;
}) {
  const goBack = useWorkspaceBackNavigation("threads");
  const { panes, toggleAuxiliaryPane, composerKeyboardActive, startNewTask } =
    useAdaptiveWorkspaceLayout();
  const gitMenu = useThreadGitMenuDefinition(props.gitControls);
  const terminalMenu = threadTerminalMenu(props.gitControls);
  const filesVisible = props.inspectorMode === "files" && panes.auxiliaryPaneVisible;
  const actions: AppBarAction[] = props.hasThreadCwd
    ? [
        {
          accessibilityLabel: filesVisible ? "Close files" : "Open files",
          icon: "folder",
          selected: filesVisible,
          disabled: !props.gitControls.canOpenFiles,
          separateBackground: false,
          onPress: filesVisible ? toggleAuxiliaryPane : props.onOpenFilesInspector,
        },
      ]
    : [];
  const menus = [gitMenu, terminalMenu].filter((menu): menu is AppBarMenu => menu !== null);
  const paneWidth = props.paneWidth ?? panes.contentPaneWidth;
  const form = threadPrHeaderForm(paneWidth);
  const pr = useThreadPr(props.thread, props.gitControls.gitStatus);
  const openPullRequest = useOpenPullRequest();
  const pullRequest =
    pr === null ? undefined : (
      <ThreadPrHeaderButton
        pr={pr}
        links={props.thread.pullRequests}
        form={form}
        onOpen={(url) => void openPullRequest(url)}
      />
    );
  const { buttonSize } = useAndroidControlSizing();
  const directActions = budgetAndroidThreadHeader({
    headerWidth: paneWidth,
    buttonSize,
    actionCount: actions.length,
    trailingControls: 1 + (pr === null ? 0 : form === "regular" ? 2 : 1),
  });
  const androidMenuItems: AppBarMenu["items"] = [
    ...actions.slice(directActions).map((action) => ({
      id: "thread-files",
      title: action.accessibilityLabel,
      icon: typeof action.icon === "string" ? action.icon : action.icon.android,
      selected: action.selected,
      disabled: action.disabled,
      onPress: action.onPress,
    })),
    ...menus.map((menu) => ({ id: menu.title, title: menu.title, items: menu.items })),
  ];
  // A PR with no workspace has no Git, Terminal or overflowed actions to put in the menu.
  const androidMenus: AppBarMenu[] =
    androidMenuItems.length === 0
      ? []
      : [{ title: "Thread controls", icon: "ellipsis", items: androidMenuItems }];

  const showAndroidPrMenu = Platform.OS === "android" && pr !== null;
  return (
    <AppBar
      title={props.title}
      subtitle={props.subtitle}
      onBackToList={goBack}
      onCompose={props.draft ? undefined : startNewTask}
      backInSplitView={
        props.onReturnToThread
          ? {
              accessibilityLabel: "Return to chat",
              icon: "chevron.left",
              onPress: props.onReturnToThread,
            }
          : undefined
      }
      actions={showAndroidPrMenu ? actions.slice(0, directActions) : actions}
      menus={showAndroidPrMenu ? androidMenus : menus}
      beforeLastControl={pullRequest}
      hideBottomBorder
      compact={composerKeyboardActive}
    />
  );
}
