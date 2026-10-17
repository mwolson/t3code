import type { ThreadPullRequestLink } from "@t3tools/contracts";
import { useMemo } from "react";
import { Platform, Pressable, View } from "react-native";

import { SymbolView } from "../../components/ui/AppSymbol";
import { AppText as Text } from "../../components/ui/AppText";
import { Menu } from "../../components/ui/Menu";
import { cn } from "../../lib/cn";
import type { ThreadPrPresentation } from "../../state/thread-pr-presentation";
import { presentThreadPrHeader, threadPrMenuEntries } from "./thread-pr-header-presentation";

export function ThreadPrHeaderButton(props: {
  readonly pr: ThreadPrPresentation;
  readonly links: ReadonlyArray<ThreadPullRequestLink>;
  readonly onOpen: (url: string) => void;
}) {
  const { pr, links, onOpen } = props;
  const presentation = presentThreadPrHeader(pr);
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
      <Text
        className={cn("text-sm font-medium", presentation.textClassName)}
        style={{ fontVariant: ["tabular-nums"] }}
      >
        {presentation.text}
      </Text>
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
    "flex-row items-center gap-1 px-2.5 active:opacity-60",
    Platform.OS === "ios" ? "h-9" : "h-12",
  );

  if (!presentation.opensMenu) {
    return (
      <Pressable
        accessibilityRole="button"
        accessibilityLabel={presentation.accessibilityLabel}
        accessibilityHint="Opens the pull request"
        hitSlop={8}
        className={className}
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
