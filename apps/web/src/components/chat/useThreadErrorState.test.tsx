import { act, useLayoutEffect } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import { useThreadErrorState } from "./useThreadErrorState";

let renderer: ReactTestRenderer;
let value: ReturnType<typeof useThreadErrorState>;
let input: Parameters<typeof useThreadErrorState>[0];
let serial = 0;
function Probe() {
  const state = useThreadErrorState(input);
  useLayoutEffect(() => {
    value = state;
  });
  return <div>{state.error}</div>;
}
function render() {
  act(() => renderer.update(<Probe />));
}
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.spyOn(Date, "now").mockReturnValue(100);
  input = {
    threadKey: `env:hook-${++serial}`,
    draftId: "draft",
    isServerThread: true,
    runtime: {
      lastError: "Durable failure",
      lastErrorAt: "2026-09-25T00:00:00Z",
      lastErrorClass: "usage_limit",
    },
  };
  act(() => {
    renderer = create(<Probe />);
  });
});
afterEach(() => {
  act(() => renderer.unmount());
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});
it("dismisses a local failure without revealing the unchanged durable failure", () => {
  act(() => value.setError(input.threadKey, true, "Save failed"));
  expect(value.error).toBe("Save failed");
  act(() => value.dismiss());
  expect(value.error).toBeNull();
  render();
  expect(value.error).toBeNull();
  input.runtime = { ...input.runtime!, lastErrorAt: "2026-09-25T00:01:00Z" };
  render();
  expect(value.error).toBe("Durable failure");
  expect(value.errorClass).toBe("usage_limit");
});
it("reappears for an identical local write even in the same clock tick", () => {
  act(() => value.setError(input.threadKey, true, "Save failed"));
  act(() => value.dismiss());
  expect(value.error).toBeNull();
  act(() => value.setError(input.threadKey, true, "Save failed"));
  expect(value.error).toBe("Save failed");
  expect(value.errorClass).toBeNull();
});
it("keeps a newer undismissed local failure visible", () => {
  act(() => value.setError(input.threadKey, true, "Save failed"));
  act(() => value.dismiss());
  input.runtime = { lastError: "Provider failure", lastErrorAt: "2026-09-25T00:01:00Z" };
  render();
  expect(value.error).toBe("Provider failure");
  act(() => value.setError(input.threadKey, true, "Save failed"));
  expect(value.error).toBe("Save failed");
});
it("dismisses runtime errors immediately and remembers them across remounts", () => {
  act(() => value.dismiss());
  expect(value.error).toBeNull();
  act(() => {
    renderer.unmount();
    renderer = create(<Probe />);
  });
  expect(value.error).toBeNull();
  input.runtime = { lastError: "Durable failure", lastErrorAt: "2026-09-25T00:01:00Z" };
  render();
  expect(value.error).toBe("Durable failure");
});
it("preserves repeated draft occurrences and migrates the latest write on promotion", () => {
  input.isServerThread = false;
  input.runtime = null;
  render();
  act(() => value.setError("draft", false, "Send failed"));
  act(() => value.dismiss());
  act(() => value.setError("draft", false, "Send failed"));
  expect(value.error).toBe("Send failed");
  input.isServerThread = true;
  render();
  expect(value.error).toBe("Send failed");
  act(() => value.setError(input.threadKey, true, null));
  expect(value.error).toBeNull();
});
