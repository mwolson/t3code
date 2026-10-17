// @vitest-environment jsdom
import type { ThreadPullRequestLink } from "@t3tools/contracts";
import { act, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

// Renders the real Android menu adapter with React Native primitives reduced to DOM nodes,
// so the accessible tree and activation come from the adapter rather than a stub.
vi.mock("react-native", async () => {
  const { useEffect, useImperativeHandle } = await import("react");
  const useMeasurable = (ref: unknown) => {
    useImperativeHandle(ref as never, () => ({
      measureInWindow: (callback: (x: number, y: number, w: number, h: number) => void) =>
        callback(0, 0, 40, 40),
    }));
  };
  return {
    BackHandler: { addEventListener: () => ({ remove: () => {} }) },
    Platform: { OS: "android" },
    Pressable: (props: {
      readonly ref?: unknown;
      readonly accessible?: boolean;
      readonly accessibilityHint?: string;
      readonly accessibilityLabel?: string;
      readonly accessibilityRole?: string;
      readonly onPress?: () => void;
      readonly children?: ReactNode;
    }) => {
      useMeasurable(props.ref);
      return props.accessible === false ? (
        <div onClick={props.onPress}>{props.children}</div>
      ) : (
        <div
          role={props.accessibilityRole}
          aria-label={props.accessibilityLabel}
          aria-description={props.accessibilityHint}
          onClick={props.onPress}
        >
          {props.children}
        </div>
      );
    },
    ScrollView: (props: { readonly children?: ReactNode }) => <div>{props.children}</div>,
    View: (props: {
      readonly ref?: unknown;
      readonly onLayout?: () => void;
      readonly children?: ReactNode;
    }) => {
      useMeasurable(props.ref);
      const { onLayout } = props;
      useEffect(() => onLayout?.(), [onLayout]);
      return <div>{props.children}</div>;
    },
  };
});
vi.mock("react-native-keyboard-controller", () => ({
  useKeyboardState: (select: (state: { isVisible: boolean; height: number }) => unknown) =>
    select({ isVisible: false, height: 0 }),
}));
vi.mock("react-native-reanimated", () => ({ default: {}, FadeIn: {} }));
vi.mock("expo-haptics", () => ({}));
vi.mock("../../components/ui/OverlayPortal", () => ({
  OverlayPortal: (props: { readonly children: ReactNode }) => props.children,
}));
vi.mock("../../components/ui/useAndroidControlSizing", () => ({
  useAndroidControlSizing: () => ({ scale: 1, menuWidth: 200 }),
}));
vi.mock("../../components/ui/MenuPopup", () => ({
  MenuPopup: (props: import("../../components/ui/MenuPopup").MenuPopupProps) => (
    <ul>
      {props.actions.map((action) => (
        <li key={action.id} onClick={() => props.onPress(action)}>
          {action.title}
        </li>
      ))}
    </ul>
  ),
}));
vi.mock("../../components/ui/Menu", () => import("../../components/ui/Menu.android"));
vi.mock("../../components/ui/AppSymbol", () => ({ SymbolView: () => null }));
vi.mock("../../components/ui/AppText", () => ({
  AppText: (props: { readonly children: ReactNode }) => <span>{props.children}</span>,
}));

import { presentThreadLinkedPullRequests } from "../../state/thread-pr-presentation";
import { ThreadPrHeaderButton } from "./ThreadPrHeaderButton";

function link(number: number, headBranch: string, baseBranch: string): ThreadPullRequestLink {
  return {
    host: "github.com",
    repository: "t3tools/t3code",
    number,
    url: `https://github.com/t3tools/t3code/pull/${number}`,
    source: "manual",
    linkedAt: "2026-10-01T00:00:00.000Z",
    stack: null,
    snapshot: {
      state: "open",
      title: `Change ${number}`,
      headBranch,
      baseBranch,
      isDraft: false,
      updatedAt: null,
      syncedAt: "2026-10-01T00:00:00.000Z",
    },
  };
}

let root: Root | null = null;
let container: HTMLDivElement | null = null;

function render(links: ReadonlyArray<ThreadPullRequestLink>, onOpen: (url: string) => void) {
  const pr = presentThreadLinkedPullRequests(links);
  if (pr === null) throw new Error("expected a pull request presentation");
  (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  container = document.createElement("div");
  root = createRoot(container);
  act(() => root!.render(<ThreadPrHeaderButton pr={pr} links={links} onOpen={onOpen} />));
  return container;
}

function buttons(element: HTMLElement) {
  return [...element.querySelectorAll('[role="button"]')];
}

afterEach(() => {
  act(() => root?.unmount());
  root = null;
  container = null;
});

describe("ThreadPrHeaderButton on Android", () => {
  it("makes the stack menu trigger the one labeled, actionable button", () => {
    const opened: string[] = [];
    const element = render(
      [link(21, "layer-1", "main"), link(22, "layer-2", "layer-1"), link(23, "layer-3", "layer-2")],
      (url) => opened.push(url),
    );

    const [trigger, ...nested] = buttons(element);
    expect(nested).toEqual([]);
    expect(trigger?.getAttribute("aria-label")).toBe("3 pull requests in stack, open");
    expect(trigger?.getAttribute("aria-description")).toBe("Lists the linked pull requests");

    act(() => (trigger as HTMLElement).click());
    const entries = [...element.querySelectorAll("li")];
    expect(entries.map((entry) => entry.textContent)).toEqual([
      "#21 Change 21",
      "#22 Change 22",
      "#23 Change 23",
    ]);

    act(() => entries[1]!.click());
    expect(opened).toEqual(["https://github.com/t3tools/t3code/pull/22"]);
  });

  it("opens a single pull request directly from its labeled button", () => {
    const opened: string[] = [];
    const element = render([link(1006, "change", "main")], (url) => opened.push(url));

    const [button, ...nested] = buttons(element);
    expect(nested).toEqual([]);
    expect(button?.getAttribute("aria-label")).toBe("#1006 pull request open");

    act(() => (button as HTMLElement).click());
    expect(opened).toEqual(["https://github.com/t3tools/t3code/pull/1006"]);
  });
});
