import { act, type ComponentProps, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { ProviderInstanceId, RunId } from "@t3tools/contracts";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import { makeThreadShellFixture } from "../../test-fixtures";

const rendered = vi.hoisted(() => ({ text: vi.fn() }));
vi.mock("react-native", () => ({
  View: ({ children }: { children?: ReactNode }) => children,
  Pressable: ({ children }: { children?: ReactNode }) => children,
  Alert: { alert: vi.fn() },
  useWindowDimensions: () => ({ width: 400 }),
}));
vi.mock("../../components/AppText", () => ({
  AppText: ({ children }: { children?: ReactNode }) => {
    rendered.text(children);
    return null;
  },
}));
vi.mock("../../components/RowPressable", () => ({
  RowPressable: ({ children }: { children?: ReactNode }) => children,
}));
vi.mock("../../components/ControlPill", () => ({
  ControlPillMenu: ({ children }: { children?: ReactNode }) => children,
}));
vi.mock("../home/thread-swipe-actions", () => ({
  ThreadSwipeable: ({ children }: { children: (close: () => void) => ReactNode }) =>
    children(() => {}),
}));
vi.mock("./CustomSnoozeSheet", () => ({ CustomSnoozeSheet: () => null }));
vi.mock("../../state/atom-registry", () => ({ appAtomRegistry: { get: () => false } }));
vi.mock("../../state/thread-order", () => ({ threadArrangementOpenAtom: {} }));
vi.mock("../../components/AppSymbol", () => ({ SymbolView: () => null }));
vi.mock("../../components/EnvironmentMachineSymbol", () => ({
  EnvironmentMachineSymbol: () => null,
}));
vi.mock("../../components/ProjectFavicon", () => ({ ProjectFavicon: () => null }));
vi.mock("../../components/ProviderIcon", () => ({
  ProviderIcon: () => null,
  ProviderInstanceIcon: () => null,
}));
vi.mock("../../lib/copyTextWithHaptic", () => ({ copyTextWithHaptic: vi.fn() }));
vi.mock("../../lib/useUniwindTheme", () => ({ useUniwindTheme: () => "light" }));
vi.mock("../../state/use-thread-pr", () => ({ useThreadPr: () => null }));
vi.mock("./queued-message-icon", () => ({ QueuedMessageIcon: () => null }));
vi.mock("./thread-search-match", () => ({ ThreadSearchMatchExcerpt: () => null }));
import { ThreadListV2Row } from "./thread-list-v2-items";

const thread = makeThreadShellFixture({
  title: "Thread",
  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "test" },
});
const noop = () => {};
const props: ComponentProps<typeof ThreadListV2Row> = {
  thread,
  variant: "card",
  timeLabel: "",
  snoozePresetMinute: "2026-06-02T00:00:00.000Z",
  project: null,
  providers: undefined,
  providerInstance: null,
  environmentLabel: null,
  onSelectThread: noop,
  onDeleteThread: noop,
  onNewThreadOnBranch: noop,
  onRenameThread: noop,
  onRegenerateThreadTitle: noop,
  onSettleThread: async () => true,
  onSnoozeThread: noop,
  onUnsnoozeThread: noop,
  onUnsettleThread: noop,
  onSetThreadAutoSettle: noop,
  onArchiveThread: noop,
  onPinThread: noop,
  onUnpinThread: noop,
  settlementSupported: true,
  autoSettleOptOutSupported: false,
  snoozeSupported: true,
  pinningSupported: true,
  titleRegenerationSupported: false,
  onSwipeableWillOpen: noop,
  onSwipeableClose: noop,
};
let root: Root;
beforeEach(() => {
  rendered.text.mockClear();
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
async function render(overrides: Partial<ComponentProps<typeof ThreadListV2Row>>) {
  rendered.text.mockClear();
  await act(() => root.render(<ThreadListV2Row {...props} {...overrides} />));
}
it.each(["idle", "running", "waiting"] as const)(
  "renders the current %s runtime status",
  async (status) => {
    await render({
      thread: {
        ...thread,
        runtime: {
          status,
          activeRunId: null,
          providerInstanceId: ProviderInstanceId.make("codex"),
          providerName: "Codex",
          lastError: null,
          updatedAt: "2026-06-02T00:00:00.000Z",
        },
      },
    });
    expect(rendered.text).toHaveBeenCalledWith(status === "idle" ? "Waiting" : "Working");
  },
);
it("keeps Done, settled timestamps and snoozed wake labels on recycled rows", async () => {
  await render({
    thread: {
      ...thread,
      lastVisitedAt: "2026-06-01T00:00:00.000Z",
      latestRun: {
        runId: RunId.make("completed"),
        status: "completed",
        requestedAt: "2026-06-02T00:00:00.000Z",
        startedAt: "2026-06-02T00:00:00.000Z",
        completedAt: "2026-06-02T01:00:00.000Z",
        assistantMessageId: null,
      },
    },
  });
  expect(rendered.text).toHaveBeenCalledWith("Done");
  await render({
    variant: "slim",
    timeLabel: "3d",
    thread: {
      ...thread,
      settledOverride: "settled",
      settledAt: thread.createdAt,
      runtime: {
        status: "idle",
        activeRunId: null,
        providerInstanceId: ProviderInstanceId.make("codex"),
        providerName: "Codex",
        lastError: null,
        updatedAt: "2026-06-02T00:00:00.000Z",
      },
    },
  });
  expect(rendered.text).not.toHaveBeenCalledWith("Waiting");
  expect(rendered.text).toHaveBeenCalledWith("3d");
  await render({ variant: "slim", snoozed: true, snoozeWakeLabelText: "Wakes in 1h", thread });
  expect(rendered.text).toHaveBeenCalledWith("Wakes in 1h");
});
