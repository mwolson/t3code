import type { ThreadPullRequestLink } from "@t3tools/contracts";
import { useMemo } from "react";
import { Platform, Pressable, View } from "react-native";

import { SymbolView } from "../../components/ui/AppSymbol";
import { AppText as Text } from "../../components/ui/AppText";
import { Menu } from "../../components/ui/Menu";
import { useAndroidControlSizing } from "../../components/ui/useAndroidControlSizing";
import { cn } from "../../lib/cn";
import type { ThreadPrPresentation } from "../../state/thread-pr-presentation";
import {
  presentThreadPrHeader,
  threadPrMenuEntries,
  type ThreadPrHeaderForm,
} from "./thread-pr-header-presentation";

export function ThreadPrHeaderButton(props: {
  readonly pr: ThreadPrPresentation;
  readonly links: ReadonlyArray<ThreadPullRequestLink>;
  readonly form: ThreadPrHeaderForm;
  readonly onOpen: (url: string) => void;
}) {
  const { pr, links, form, onOpen } = props;
  const { buttonSize } = useAndroidControlSizing();
  const presentation = presentThreadPrHeader(pr, form);
  const entries = useMemo(
    () => (presentation.opensMenu ? threadPrMenuEntries(links, Platform.OS) : []),
    [links, presentation.opensMenu],
  );
  const content = (
    <>
      <SymbolView
        name={presentation.icon}
        size={16}
        tintColorClassName={presentation.tintClassName}
        type="monochrome"
      />
      {presentation.text === null ? null : (
        <Text
          className={cn("text-sm font-medium", presentation.textClassName)}
          style={{ fontVariant: ["tabular-nums"] }}
        >
          {presentation.text}
        </Text>
      )}
      {presentation.status ? (
        <SymbolView
          name={presentation.status.icon}
          size={14}
          tintColorClassName={presentation.status.tintClassName}
          type="monochrome"
        />
      ) : null}
    </>
  );
  const className = cn(
    "flex-row items-center justify-center active:opacity-60",
    form === "compact" ? "gap-0.5" : "gap-1 px-2.5",
    form === "compact" && Platform.OS !== "ios" && "px-1",
  );
  // Sized on the actionable view itself: the native menu triggers ignore hit slop on content.
  // iOS matches its 36-point glass bar items: a few points wider and UIKit hides a phone's title.
  const targetSize = Platform.OS === "ios" ? 36 : buttonSize;
  const target = { minHeight: targetSize, minWidth: targetSize };

  if (!presentation.opensMenu) {
    return (
      <Pressable
        accessibilityRole="button"
        accessibilityLabel={presentation.accessibilityLabel}
        accessibilityHint="Opens the pull request"
        className={className}
        style={target}
        onPress={() => onOpen(pr.url)}
      >
        {content}
      </Pressable>
    );
  }
  return (
    <Menu
      accessible
      accessibilityRole="button"
      accessibilityLabel={presentation.accessibilityLabel}
      accessibilityHint="Lists the linked pull requests"
      title="Linked pull requests"
      style={{ ...target, alignItems: "center", justifyContent: "center" }}
      actions={entries.map((entry) => entry.action)}
      onPressAction={({ nativeEvent }) => {
        const entry = entries.find((candidate) => candidate.id === nativeEvent.event);
        if (entry) onOpen(entry.url);
      }}
    >
      <View className={className}>{content}</View>
    </Menu>
  );
}
