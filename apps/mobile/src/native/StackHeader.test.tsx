import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

const fixture = vi.hoisted(() => ({
  focused: false,
  listeners: new Map<string, Set<() => void>>(),
  setOptions: vi.fn(),
}));
vi.mock("@react-navigation/native", async () => {
  const { createContext } = await vi.importActual<typeof import("react")>("react");
  const navigation = {
    isFocused: () => fixture.focused,
    setOptions: fixture.setOptions,
    addListener: (event: string, callback: () => void) => {
      const listeners = fixture.listeners.get(event) ?? new Set();
      fixture.listeners.set(event, listeners);
      listeners.add(callback);
      return () => listeners.delete(callback);
    },
  };
  return {
    NavigationContext: createContext(navigation),
    NavigationRouteContext: createContext(undefined),
    useNavigation: () => navigation,
  };
});
vi.mock("./NativeHeaderToolbar", () => ({ NativeHeaderToolbar: () => null }));
import { NativeStackScreenOptions } from "./StackHeader";
import { NativePrimaryColumnContext } from "./v5-workspace-context";

let root: Root;
beforeEach(() => {
  fixture.focused = false;
  fixture.listeners.clear();
  fixture.setOptions.mockClear();
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
async function focus() {
  fixture.focused = true;
  await act(() => {
    fixture.listeners.get("focus")?.forEach((listener) => listener());
  });
}

describe("focused native headers", () => {
  it("defers hidden-screen changes until focus and reapplies on returning", async () => {
    await act(() => root.render(<NativeStackScreenOptions options={{ title: "Home" }} />));
    expect(fixture.setOptions).not.toHaveBeenCalled();
    await focus();
    expect(fixture.setOptions).toHaveBeenLastCalledWith({ title: "Home" });
    fixture.focused = false;
    await act(() => root.render(<NativeStackScreenOptions options={{ title: "Updated" }} />));
    expect(fixture.setOptions).toHaveBeenCalledTimes(1);
    await focus();
    expect(fixture.setOptions).toHaveBeenLastCalledWith({ title: "Updated" });
    fixture.focused = false;
    await focus();
    expect(fixture.setOptions).toHaveBeenCalledTimes(3);
  });

  it("applies a presented primary column's options while its detail owns focus", async () => {
    const column = { presented: true, selectedThreadKey: null, selectedPullRequestKey: null };
    await act(() =>
      root.render(
        <NativePrimaryColumnContext value={column}>
          <NativeStackScreenOptions options={{ title: "Search" }} />
        </NativePrimaryColumnContext>,
      ),
    );
    expect(fixture.setOptions).toHaveBeenLastCalledWith({ title: "Search" });
  });
});
