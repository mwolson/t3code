import type { ThreadPullRequestLink } from "@t3tools/contracts";
import { act, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

const harness = vi.hoisted(() => ({ os: "android" as "android" | "ios" }));

type TargetStyle = { readonly minHeight?: number; readonly minWidth?: number } | undefined;

// Renders the real menu adapters with React Native primitives reduced to DOM nodes, so the
// accessible tree, activation and target size come from the adapter rather than a stub.
vi.mock("react-native", async () => {
  const { useEffect, useImperativeHandle } = await import("react");
  const useMeasurable = (ref: unknown) => {
    useImperativeHandle(
      ref as never,
      () => ({
        measureInWindow: (callback: (x: number, y: number, w: number, h: number) => void) =>
          callback(0, 0, 40, 40),
      }),
      [],
    );
  };
  return {
    Platform: {
      get OS() {
        return harness.os;
      },
    },
    Pressable: (props: {
      readonly ref?: unknown;
      readonly accessible?: boolean;
      readonly accessibilityHint?: string;
      readonly accessibilityLabel?: string;
      readonly accessibilityRole?: string;
      readonly style?: TargetStyle;
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
          data-min-width={props.style?.minWidth}
          data-min-height={props.style?.minHeight}
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
vi.mock("../../lib/backHandler", () => ({ useHardwareBack: () => {} }));
vi.mock("../../components/ui/OverlayPortal", () => ({
  OverlayPortal: (props: { readonly children: ReactNode }) => props.children,
}));
vi.mock("../../components/ui/useAndroidControlSizing", () => ({
  useAndroidControlSizing: () => ({ scale: 1, menuWidth: 200, buttonSize: 56 }),
}));
vi.mock("@react-native-menu/menu", () => ({
  MenuView: (props: {
    readonly accessibilityLabel?: string;
    readonly accessibilityRole?: string;
    readonly style?: TargetStyle;
    readonly children?: ReactNode;
  }) => (
    <div
      role={props.accessibilityRole}
      aria-label={props.accessibilityLabel}
      data-min-width={props.style?.minWidth}
      data-min-height={props.style?.minHeight}
    >
      {props.children}
    </div>
  ),
}));
vi.mock("uniwind", () => ({ withUniwind: (component: unknown) => component }));
vi.mock("../settings/appearance/AppearancePreferencesProvider", () => ({
  useAppearancePreferences: () => ({ themeAppearance: "light" }),
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
vi.mock("../../components/ui/Menu", async () => {
  const android = await import("../../components/ui/Menu.android");
  const ios = await vi.importActual<typeof import("../../components/ui/Menu")>(
    "../../components/ui/Menu",
  );
  return {
    Menu: (props: import("../../components/ui/Menu.types").MenuProps) =>
      harness.os === "ios" ? <ios.Menu {...props} /> : <android.Menu {...props} />,
  };
});
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

function render(
  links: ReadonlyArray<ThreadPullRequestLink>,
  onOpen: (url: string) => void,
  form: "regular" | "compact" = "regular",
) {
  const pr = presentThreadLinkedPullRequests(links);
  if (pr === null) throw new Error("expected a pull request presentation");
  (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  act(() => root?.unmount());
  container = document.createElement("div");
  root = createRoot(container);
  act(() =>
    root!.render(<ThreadPrHeaderButton pr={pr} links={links} form={form} onOpen={onOpen} />),
  );
  return container;
}

function buttons(element: HTMLElement) {
  return [...element.querySelectorAll('[role="button"]')];
}

function targetSize(element: Element | undefined) {
  return [element?.getAttribute("data-min-width"), element?.getAttribute("data-min-height")];
}

afterEach(() => {
  act(() => root?.unmount());
  root = null;
  container = null;
  harness.os = "android";
});

const stack = () => [
  link(21, "layer-1", "main"),
  link(22, "layer-2", "layer-1"),
  link(23, "layer-3", "layer-2"),
];

describe("ThreadPrHeaderButton", () => {
  it("opens a single pull request directly, keeping its number in the compact label", () => {
    for (const form of ["regular", "compact"] as const) {
      const opened: string[] = [];
      const element = render([link(1006, "change", "main")], (url) => opened.push(url), form);

      const [button, ...nested] = buttons(element);
      expect(nested).toEqual([]);
      expect(button?.textContent).toBe(form === "regular" ? "#1006" : "");
      expect(button?.getAttribute("aria-label")).toBe("#1006 pull request open");

      act(() => (button as HTMLElement).click());
      expect(opened).toEqual(["https://github.com/t3tools/t3code/pull/1006"]);
    }
  });

  it("lists a stack's layers from one labeled menu trigger and opens the chosen one", () => {
    const opened: string[] = [];
    const element = render(stack(), (url) => opened.push(url), "compact");

    const [trigger, ...nested] = buttons(element);
    expect(nested).toEqual([]);
    expect(trigger?.textContent).toBe("3");
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

  // The react project compiles sources for iOS, so this covers the 36-point target; `harness.os`
  // still picks which menu adapter must carry it to the actionable view.
  it("sizes the actionable target in both menu adapters", () => {
    for (const os of ["android", "ios"] as const) {
      harness.os = os;
      const [trigger, ...nested] = buttons(render(stack(), () => {}, "compact"));
      expect(nested).toEqual([]);
      expect(targetSize(trigger)).toEqual(["36", "36"]);
      const [single] = buttons(render([link(1006, "change", "main")], () => {}, "compact"));
      expect(targetSize(single)).toEqual(["36", "36"]);
    }
  });
});
