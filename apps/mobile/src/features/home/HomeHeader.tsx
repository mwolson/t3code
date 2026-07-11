import type {
  EnvironmentId,
  SidebarProjectGroupingMode,
  SidebarThreadSortOrder,
} from "@t3tools/contracts";
import {
  NativeHeaderToolbar,
  NativeStackScreenOptions,
  type AppNativeStackNavigationOptions,
} from "../../native/StackHeader";
import { useCallback, useMemo, useRef } from "react";
import { Platform } from "react-native";
import type { SearchBarCommands } from "react-native-screens";

import { useThemeColor } from "../../lib/useThemeColor";
import { useHardwareKeyboardCommand } from "../keyboard/hardwareKeyboardCommands";
import { withNativeGlassHeaderItem } from "../layout/native-glass-header-items";
import { createNativeMailSearchToolbarItem } from "../layout/native-mail-search-toolbar";
import type { HomeProjectSortOrder } from "./homeThreadList";
import {
  buildHomeListFilterMenu,
  type HomeListFilterMenuEnvironment,
} from "./home-list-filter-menu";
import {
  hasCustomHomeListOptions,
  PROJECT_GROUPING_OPTIONS,
  PROJECT_SORT_OPTIONS,
  THREAD_SORT_OPTIONS,
} from "./home-list-options";

export type HomeHeaderEnvironment = HomeListFilterMenuEnvironment;

export function HomeHeader(props: {
  readonly environments: ReadonlyArray<HomeHeaderEnvironment>;
  readonly selectedEnvironmentId: EnvironmentId | null;
  readonly projectSortOrder: HomeProjectSortOrder;
  readonly threadSortOrder: SidebarThreadSortOrder;
  readonly projectGroupingMode: SidebarProjectGroupingMode;
  readonly onSearchQueryChange: (query: string) => void;
  readonly onEnvironmentChange: (environmentId: EnvironmentId | null) => void;
  readonly onProjectSortOrderChange: (sortOrder: HomeProjectSortOrder) => void;
  readonly onThreadSortOrderChange: (sortOrder: SidebarThreadSortOrder) => void;
  readonly onProjectGroupingModeChange: (mode: SidebarProjectGroupingMode) => void;
  readonly onOpenSettings: () => void;
  readonly onStartNewTask: () => void;
}) {
  const searchBarRef = useRef<SearchBarCommands>(null);
  const propsRef = useRef(props);
  propsRef.current = props;
  const iconColor = String(useThemeColor("--color-icon"));
  const hasCustomListOptions = hasCustomHomeListOptions(props);
  const focusSearch = useCallback(() => {
    searchBarRef.current?.focus();
    return searchBarRef.current !== null;
  }, []);
  useHardwareKeyboardCommand("focusSearch", focusSearch);

  // Keep navigation options referentially stable. A fresh options object each
  // render re-enters navigation.setOptions via NativeStackScreenOptions and can
  // trip "Maximum update depth exceeded" under PreventRemoveProvider on iOS.
  const screenOptions = useMemo((): AppNativeStackNavigationOptions => {
    const filterSystemImageName = hasCustomListOptions
      ? "line.3.horizontal.decrease.circle.fill"
      : "line.3.horizontal.decrease";
    return {
      // Static header config (glass, title, fonts) lives in Stack.tsx
      // (GLASS_HEADER_OPTIONS). Only dynamic values are set here.
      headerTintColor: iconColor,
      unstable_headerRightItems:
        Platform.OS === "ios"
          ? () => [
              withNativeGlassHeaderItem({
                accessibilityLabel: "Open settings",
                icon: { name: "ellipsis", type: "sfSymbol" } as const,
                identifier: "home-settings",
                label: "",
                onPress: () => propsRef.current.onOpenSettings(),
                type: "button",
              }),
            ]
          : undefined,
      unstable_headerToolbarItems:
        Platform.OS === "ios"
          ? () => [
              createNativeMailSearchToolbarItem({
                composeButtonId: "home-new-task",
                composeSystemImageName: "square.and.pencil",
                filterMenu: buildHomeListFilterMenu(propsRef.current),
                filterButtonId: "home-filter",
                filterSystemImageName,
                onComposePress: () => propsRef.current.onStartNewTask(),
                onSearchTextChange: (query) => propsRef.current.onSearchQueryChange(query),
                placeholder: "Search",
                searchTextChangeId: "home-search-text",
              }),
            ]
          : undefined,
      headerSearchBarOptions:
        Platform.OS === "ios"
          ? undefined
          : {
              ref: searchBarRef,
              allowToolbarIntegration: true,
              hideNavigationBar: false,
              placeholder: "Search",
              onCancelButtonPress: () => {
                propsRef.current.onSearchQueryChange("");
              },
              onChangeText: (event) => {
                propsRef.current.onSearchQueryChange(event.nativeEvent.text);
              },
            },
    };
  }, [
    hasCustomListOptions,
    iconColor,
    props.environments,
    props.projectGroupingMode,
    props.projectSortOrder,
    props.selectedEnvironmentId,
    props.threadSortOrder,
  ]);

  return (
    <>
      <NativeStackScreenOptions options={screenOptions} />

      {Platform.OS === "ios" ? null : (
        <NativeHeaderToolbar placement="right">
          <NativeHeaderToolbar.Button
            accessibilityLabel="Open settings"
            icon="gearshape"
            onPress={props.onOpenSettings}
            separateBackground
          />
        </NativeHeaderToolbar>
      )}

      {Platform.OS === "ios" ? null : (
        <NativeHeaderToolbar placement="bottom">
          <NativeHeaderToolbar.Menu
            accessibilityLabel="Filter and sort threads"
            icon={
              hasCustomListOptions
                ? "line.3.horizontal.decrease.circle.fill"
                : "line.3.horizontal.decrease.circle"
            }
            title="Thread list options"
            separateBackground
          >
            <NativeHeaderToolbar.MenuAction onPress={props.onOpenSettings}>
              <NativeHeaderToolbar.Label>Settings</NativeHeaderToolbar.Label>
            </NativeHeaderToolbar.MenuAction>

            <NativeHeaderToolbar.Menu title="Environment">
              <NativeHeaderToolbar.Label>Environment</NativeHeaderToolbar.Label>
              <NativeHeaderToolbar.MenuAction
                isOn={props.selectedEnvironmentId === null}
                onPress={() => props.onEnvironmentChange(null)}
                subtitle="Show threads from every environment"
              >
                <NativeHeaderToolbar.Label>All environments</NativeHeaderToolbar.Label>
              </NativeHeaderToolbar.MenuAction>
              {props.environments.map((environment) => (
                <NativeHeaderToolbar.MenuAction
                  key={environment.environmentId}
                  isOn={props.selectedEnvironmentId === environment.environmentId}
                  onPress={() => props.onEnvironmentChange(environment.environmentId)}
                >
                  <NativeHeaderToolbar.Label>{environment.label}</NativeHeaderToolbar.Label>
                </NativeHeaderToolbar.MenuAction>
              ))}
            </NativeHeaderToolbar.Menu>

            <NativeHeaderToolbar.Menu title="Sort projects">
              <NativeHeaderToolbar.Label>Sort projects</NativeHeaderToolbar.Label>
              {PROJECT_SORT_OPTIONS.map((option) => (
                <NativeHeaderToolbar.MenuAction
                  key={option.value}
                  isOn={props.projectSortOrder === option.value}
                  onPress={() => props.onProjectSortOrderChange(option.value)}
                >
                  <NativeHeaderToolbar.Label>{option.label}</NativeHeaderToolbar.Label>
                </NativeHeaderToolbar.MenuAction>
              ))}
            </NativeHeaderToolbar.Menu>

            <NativeHeaderToolbar.Menu title="Sort threads">
              <NativeHeaderToolbar.Label>Sort threads</NativeHeaderToolbar.Label>
              {THREAD_SORT_OPTIONS.map((option) => (
                <NativeHeaderToolbar.MenuAction
                  key={option.value}
                  isOn={props.threadSortOrder === option.value}
                  onPress={() => props.onThreadSortOrderChange(option.value)}
                >
                  <NativeHeaderToolbar.Label>{option.label}</NativeHeaderToolbar.Label>
                </NativeHeaderToolbar.MenuAction>
              ))}
            </NativeHeaderToolbar.Menu>

            <NativeHeaderToolbar.Menu title="Group projects">
              <NativeHeaderToolbar.Label>Group projects</NativeHeaderToolbar.Label>
              {PROJECT_GROUPING_OPTIONS.map((option) => (
                <NativeHeaderToolbar.MenuAction
                  key={option.value}
                  isOn={props.projectGroupingMode === option.value}
                  onPress={() => props.onProjectGroupingModeChange(option.value)}
                  subtitle={option.subtitle}
                >
                  <NativeHeaderToolbar.Label>{option.label}</NativeHeaderToolbar.Label>
                </NativeHeaderToolbar.MenuAction>
              ))}
            </NativeHeaderToolbar.Menu>
          </NativeHeaderToolbar.Menu>
          <NativeHeaderToolbar.Spacer width={8} sharesBackground={false} />
          <NativeHeaderToolbar.SearchBarSlot />
          <NativeHeaderToolbar.Spacer width={8} sharesBackground={false} />
          <NativeHeaderToolbar.Button
            accessibilityLabel="New task"
            icon="square.and.pencil"
            onPress={props.onStartNewTask}
            separateBackground
          />
        </NativeHeaderToolbar>
      )}
    </>
  );
}
