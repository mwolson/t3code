import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import { useMemo, useState } from "react";
import { Platform, View } from "react-native";
import { AppBar } from "../../components/ui/AppBar";
import { useAndroidControlSizing } from "../../components/ui/useAndroidControlSizing";
import { useOpenPullRequest } from "../../native/useOpenPullRequest";
import { useThreadPr } from "../../state/use-thread-pr";
import { ScreenHeaderButton } from "../../components/ui/ScreenHeaderButton";
import type { AppBarAction } from "../../components/ui/AppBar.types";
import { useAdaptiveWorkspaceLayout } from "../layout/AdaptiveWorkspaceLayout";
import { budgetAndroidThreadHeader } from "./thread-header-android-budget";
import type { ThreadInspectorMode } from "./thread-inspector-content-stack";
import { threadPrHeaderForm, type ThreadPrHeaderForm } from "./thread-pr-header-presentation";
import { ThreadPrHeaderButton } from "./ThreadPrHeaderButton";
import { useThreadHeaderOptions } from "./useThreadHeaderOptions";
import { useWorkspaceBackNavigation } from "../workspace/useAndroidWorkspaceNavigation";
import { PullRequestGlyph } from "../pull-requests/pullRequestGlyphs";

export function ThreadHeader(
  props: Omit<Parameters<typeof useThreadHeaderOptions>[0], "pullRequest"> & {
    readonly thread: EnvironmentThreadShell;
    readonly hasThreadCwd: boolean;
    readonly hasWorkspaceRoot: boolean;
    readonly fileInspectorSupported: boolean;
    readonly inspectorMode: ThreadInspectorMode | null;
    readonly onToggleInspector: () => void;
    readonly onOpenGitInspector: () => void;
    readonly onOpenFilesInspector: () => void;
    /** Defaults to the layout's pane width when the caller has no measured width. */
    readonly pullRequestForm?: ThreadPrHeaderForm;
  },
) {
  const goBack = useWorkspaceBackNavigation("threads");
  const { layout, panes, toggleAuxiliaryPane, composerKeyboardActive } =
    useAdaptiveWorkspaceLayout();
  const pullRequestForm = props.pullRequestForm ?? threadPrHeaderForm(panes.contentPaneWidth);
  const { onOpenTerminal, onMergeBack } = props.gitControls;
  const pr = useThreadPr(props.thread, props.gitControls.gitStatus);
  const openPullRequest = useOpenPullRequest();
  const links = props.thread.pullRequests;
  const pullRequest = useMemo(
    () =>
      pr === null
        ? null
        : {
            key: JSON.stringify([pr, links, pullRequestForm]),
            element: (
              <ThreadPrHeaderButton
                pr={pr}
                links={links}
                form={pullRequestForm}
                onOpen={(url) => void openPullRequest(url)}
              />
            ),
          },
    [links, openPullRequest, pr, pullRequestForm],
  );
  const native = useThreadHeaderOptions({ ...props, pullRequest });
  const { buttonSize } = useAndroidControlSizing();
  const [headerWidth, setHeaderWidth] = useState(panes.contentPaneWidth);
  const inspectorVisible = props.inspectorMode !== null && panes.auxiliaryPaneVisible;
  const inspectorToggle = useMemo<AppBarAction | null>(
    () =>
      props.fileInspectorSupported && props.hasThreadCwd
        ? {
            accessibilityLabel: inspectorVisible ? "Hide inspector" : "Show inspector",
            icon: "sidebar.right",
            selected: inspectorVisible,
            onPress: props.onToggleInspector,
          }
        : null,
    [inspectorVisible, props.fileInspectorSupported, props.hasThreadCwd, props.onToggleInspector],
  );
  const headerActions = useMemo<ReadonlyArray<AppBarAction>>(() => {
    const actions: AppBarAction[] = [];
    if (props.onReturnToThread) {
      actions.push({
        accessibilityLabel: "Return to chat",
        icon: "chevron.left",
        onPress: props.onReturnToThread,
      });
    }
    if (props.hasThreadCwd) {
      const filesVisible = props.inspectorMode === "files" && panes.auxiliaryPaneVisible;
      actions.push({
        accessibilityLabel: filesVisible ? "Close files" : "Open files",
        selected: filesVisible,
        icon: "folder",
        onPress: filesVisible ? toggleAuxiliaryPane : props.onOpenFilesInspector,
      });
    }
    if (props.hasWorkspaceRoot && props.gitControls.canOpenTerminal) {
      actions.push({
        accessibilityLabel: "Open terminal",
        icon: "terminal",
        onPress: () => onOpenTerminal(null),
      });
    }
    actions.push({
      accessibilityLabel: "Open git controls",
      icon: "point.topleft.down.curvedto.point.bottomright.up",
      onPress: props.onOpenGitInspector,
    });
    if (onMergeBack) {
      actions.push({
        accessibilityLabel: "Merge back to source",
        icon: PullRequestGlyph.merged,
        onPress: onMergeBack,
      });
    }
    return actions;
  }, [
    props.inspectorMode,
    panes.auxiliaryPaneVisible,
    props.onOpenFilesInspector,
    onOpenTerminal,
    onMergeBack,
    props.onOpenGitInspector,
    toggleAuxiliaryPane,
    props.onReturnToThread,
    props.hasThreadCwd,
    props.hasWorkspaceRoot,
    props.gitControls.canOpenTerminal,
  ]);
  const budget = budgetAndroidThreadHeader({
    headerWidth,
    buttonSize,
    // The back button on phones, the sidebar toggle in split view.
    leadingControls: 1,
    actionCount: headerActions.length,
    pullRequestControls: pullRequest === null ? 0 : pullRequestForm === "regular" ? 2 : 1,
    hasInspectorToggle: inspectorToggle !== null,
  });
  const androidHeaderActions = useMemo(
    () => [
      ...headerActions.map((action, index) =>
        index < budget.directActions ? action : { ...action, overflowOnly: true },
      ),
      ...(budget.inspectorInOverflow && inspectorToggle
        ? [{ ...inspectorToggle, overflowOnly: true }]
        : []),
    ],
    [budget.directActions, budget.inspectorInOverflow, headerActions, inspectorToggle],
  );

  const appBar = (
    <AppBar
      title={props.title}
      subtitle={props.subtitle}
      sidebar={native.sidebar}
      options={native.options}
      optionsVersion={native.optionsVersion}
      trailing={
        inspectorToggle && !budget.inspectorInOverflow ? (
          <ScreenHeaderButton
            accessibilityLabel={inspectorToggle.accessibilityLabel}
            icon="sidebar.right"
            selected={inspectorToggle.selected}
            onPress={inspectorToggle.onPress}
          />
        ) : undefined
      }
      beforeLastControl={pullRequest?.element}
      onBack={layout.usesSplitView ? undefined : goBack}
      actions={androidHeaderActions}
      hideBottomBorder
      compact={composerKeyboardActive}
    />
  );

  return (
    <>
      {Platform.OS === "android" ? (
        <View onLayout={(event) => setHeaderWidth(event.nativeEvent.layout.width)}>{appBar}</View>
      ) : (
        appBar
      )}
      {native.fallback}
    </>
  );
}
