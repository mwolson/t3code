import { AppActivityIndicator } from "./AppActivityIndicator";
import { Column, Text as NativeText, RNHostView } from "@expo/ui/jetpack-compose";
import { useGT } from "gt-react-native";
import { Fragment, use, useState, type ReactNode } from "react";
import { View } from "react-native";

import type { AppSymbolName } from "./AppSymbol";
import { NativeAppBar } from "./NativeAppBar.android";
import { IconButton } from "./IconButton";
import { AndroidAnchoredMenu } from "./AndroidAnchoredMenu.android";
import { splitAndroidHeaderActions } from "./headerMenu.android";
import { useScaledTextRole } from "../../features/settings/appearance/useScaledTextRole";
import { useToolbarLayout } from "./useToolbarLayout";
import { useControlTheme } from "./useControlTheme";
import { RoutedFormSheetContext } from "./RoutedFormSheetContext";

export interface AndroidHeaderAction {
  readonly accessibilityLabel: string;
  readonly icon: AppSymbolName;
  readonly onPress: () => void;
  readonly disabled?: boolean;
  readonly loading?: boolean;
  readonly selected?: boolean;
  readonly subtitle?: string;
  readonly destructive?: boolean;
  readonly iconScale?: number;
  readonly overflowOnly?: boolean;
}

export function AndroidHeaderIconButton(props: {
  readonly accessibilityLabel: string;
  readonly icon: AppSymbolName;
  readonly onPress?: () => void;
  readonly disabled?: boolean;
  readonly selected?: boolean;
  readonly iconScale?: number;
}) {
  return <IconButton {...props} tintColorClassName="accent-header-foreground" />;
}

export function AndroidScreenHeader(props: {
  readonly title: string;
  readonly titleContent?: ReactNode;
  readonly subtitle?: string | null;
  readonly actions?: ReadonlyArray<AndroidHeaderAction>;
  readonly leading?: ReactNode;
  readonly trailing?: ReactNode;
  readonly beforeLastControl?: ReactNode;
  readonly onBack?: () => void;
  readonly embedded?: boolean;
  readonly hideBottomBorder?: boolean;
}) {
  const gt = useGT();
  const { colors } = useControlTheme();
  const titleTypography = useScaledTextRole("title");
  const subtitleTypography = useScaledTextRole("label");
  const inFormSheet = use(RoutedFormSheetContext);
  const { paddingTop, paddingBottom } = useToolbarLayout(props.embedded || inFormSheet);
  const [headerWidth, setHeaderWidth] = useState(0);
  const { visibleActions, overflowActions } = splitAndroidHeaderActions(
    props.actions ?? [],
    headerWidth >= 600,
  );
  const controls: ReactNode[] = visibleActions.map((action) =>
    action.loading ? (
      <View
        key={action.accessibilityLabel}
        accessibilityLabel={action.accessibilityLabel}
        className="size-12 items-center justify-center"
      >
        <AppActivityIndicator colorClassName="accent-header-foreground" />
      </View>
    ) : (
      <AndroidHeaderIconButton
        key={action.accessibilityLabel}
        accessibilityLabel={action.accessibilityLabel}
        disabled={action.disabled}
        selected={action.selected}
        icon={action.icon}
        iconScale={action.iconScale}
        onPress={action.onPress}
      />
    ),
  );
  if (overflowActions.length > 0) {
    controls.push(
      <AndroidAnchoredMenu
        key="overflow"
        actions={overflowActions.map((action, index) => ({
          id: String(index),
          title: action.accessibilityLabel,
          subtitle: action.subtitle,
          state: action.selected ? "on" : undefined,
          attributes: {
            disabled: Boolean(action.disabled),
            destructive: action.destructive,
          },
        }))}
        onPressAction={({ nativeEvent }) => overflowActions[Number(nativeEvent.event)]?.onPress()}
      >
        {(open) => (
          <IconButton
            accessibilityLabel={gt("More actions")}
            icon="ellipsis"
            tintColorClassName="accent-header-foreground"
            onPress={open}
          />
        )}
      </AndroidAnchoredMenu>,
    );
  }
  if (props.trailing) controls.push(<Fragment key="trailing">{props.trailing}</Fragment>);
  if (props.beforeLastControl) {
    controls.splice(
      Math.max(controls.length - 1, 0),
      0,
      <Fragment key="before-last">{props.beforeLastControl}</Fragment>,
    );
  }

  return (
    <View
      onLayout={(event) => setHeaderWidth(event.nativeEvent.layout.width)}
      className="border-b border-header-border bg-header"
      style={{
        paddingTop,
        paddingBottom,
        borderBottomWidth: props.hideBottomBorder ? 0 : undefined,
      }}
    >
      {inFormSheet ? (
        <View
          pointerEvents="none"
          className="absolute top-1 h-1 w-8 self-center rounded-full bg-header-foreground/40"
        />
      ) : null}
      <NativeAppBar
        navigationIcon={
          props.onBack || props.leading ? (
            <View collapsable={false} className="flex-row items-center">
              {props.onBack ? (
                <IconButton
                  accessibilityLabel={gt("Navigate up")}
                  icon="arrow.left"
                  tintColorClassName="accent-header-foreground"
                  onPress={props.onBack}
                />
              ) : null}

              {props.leading}
            </View>
          ) : undefined
        }
        title={
          props.titleContent ? (
            <RNHostView matchContents={{ vertical: true }}>
              <View collapsable={false}>{props.titleContent}</View>
            </RNHostView>
          ) : !props.subtitle ? (
            <NativeText
              color={colors["--color-header-foreground"]}
              maxLines={1}
              overflow="ellipsis"
              style={titleTypography}
            >
              {props.title}
            </NativeText>
          ) : (
            <Column>
              <NativeText
                color={colors["--color-header-foreground"]}
                maxLines={1}
                overflow="ellipsis"
                style={titleTypography}
              >
                {props.title}
              </NativeText>
              {props.subtitle ? (
                <NativeText
                  color={colors["--color-foreground-muted"]}
                  maxLines={1}
                  overflow="ellipsis"
                  style={subtitleTypography}
                >
                  {props.subtitle}
                </NativeText>
              ) : null}
            </Column>
          )
        }
        actions={
          controls.length > 0 ? (
            <View collapsable={false} className="flex-row items-center">
              {controls}
            </View>
          ) : undefined
        }
      />
    </View>
  );
}
