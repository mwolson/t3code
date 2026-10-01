import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

const fixture = vi.hoisted(() => ({
  focused: false,
  listeners: new Map<string, Set<() => void>>(),
  setOptions: vi.fn(),
}));
vi.mock("@react-navigation/native", () => {
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
  return { useNavigation: () => navigation };
});
vi.mock("./NativeHeaderToolbar", () => ({ NativeHeaderToolbar: () => null }));
import { NativeStackScreenOptions } from "./StackHeader";

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
    await act(() => root.render(<NativeStackScreenOptions options={{ title: "Home" }} />));
    expect(fixture.setOptions).toHaveBeenCalledTimes(1);
    fixture.focused = false;
    await act(() => root.render(<NativeStackScreenOptions options={{ title: "Updated" }} />));
    expect(fixture.setOptions).toHaveBeenCalledTimes(1);
    await focus();
    expect(fixture.setOptions).toHaveBeenLastCalledWith({ title: "Updated" });
    fixture.focused = false;
    await focus();
    expect(fixture.setOptions).toHaveBeenCalledTimes(3);
  });

  it("keeps caller blur cleanup and removes all listeners on unmount", async () => {
    const blur = vi.fn();
    await act(() =>
      root.render(<NativeStackScreenOptions options={{ title: "Thread" }} listeners={{ blur }} />),
    );
    fixture.listeners.get("blur")?.forEach((listener) => listener());
    expect(blur).toHaveBeenCalledOnce();
    await act(() =>
      root.render(<NativeStackScreenOptions options={{ title: "New" }} listeners={{ blur }} />),
    );
    expect(fixture.listeners.get("focus")?.size).toBe(1);
    await act(() => root.render(null));
    expect(fixture.listeners.get("focus")?.size).toBe(0);
    expect(fixture.listeners.get("blur")?.size).toBe(0);
    await focus();
    expect(fixture.setOptions).not.toHaveBeenCalled();
  });
});
