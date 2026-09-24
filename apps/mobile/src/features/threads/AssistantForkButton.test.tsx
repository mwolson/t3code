import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import {
  EnvironmentId,
  MessageId,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2ProjectedTurnItem,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";

const fixture = vi.hoisted(() => ({
  button: vi.fn(),
  alert: vi.fn(),
  fork: vi.fn(),
  navigate: vi.fn(),
  ready: vi.fn(),
}));
vi.mock("react-native", () => ({
  Pressable: (props: { onPress: () => void; disabled: boolean }) => {
    fixture.button(props);
    return null;
  },
  Alert: { alert: fixture.alert },
  ActivityIndicator: () => null,
}));
vi.mock("@react-navigation/native", () => ({
  useNavigation: () => ({ navigate: fixture.navigate }),
}));
vi.mock("expo-haptics", () => ({ selectionAsync: async () => {} }));
vi.mock("../../components/AppSymbol", () => ({ SymbolView: () => null }));
vi.mock("../../lib/uuid", () => ({ uuidv4: () => "target" }));
vi.mock("../../state/atom-registry", () => ({ appAtomRegistry: { get: () => null } }));
vi.mock("../../state/threads", () => ({
  environmentThreadShells: { threadShellAtom: () => ({}) },
  threadEnvironment: { forkFromRun: {} },
}));
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: () => fixture.fork }));
vi.mock("../../state/v2-item-support", () => ({
  useV2ItemSupport: () => ({ providerSession: null }),
}));
vi.mock("./threadForkNavigation", () => ({ waitForThreadShellReady: fixture.ready }));
import { AssistantForkButton } from "./AssistantForkButton";

const environmentId = EnvironmentId.make("environment");
const threadId = ThreadId.make("source");
const itemId = TurnItemId.make("answer");
const at = DateTime.makeUnsafe("2026-06-02T00:00:00.000Z");
const projectedItem: OrchestrationV2ProjectedTurnItem = {
  position: 0,
  visibility: "local",
  sourceThreadId: threadId,
  sourceItemId: itemId,
  item: {
    id: itemId,
    threadId,
    runId: RunId.make("run"),
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 0,
    status: "completed",
    title: null,
    startedAt: at,
    completedAt: at,
    updatedAt: at,
    type: "assistant_message",
    messageId: MessageId.make("answer"),
    text: "Done",
    streaming: false,
  },
};
let root: Root;
beforeEach(() => {
  vi.clearAllMocks();
  fixture.ready.mockResolvedValue(true);
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
async function mount() {
  await act(() =>
    root.render(
      <AssistantForkButton
        environmentId={environmentId}
        iconColor="black"
        projectedItem={projectedItem}
        sourceTitle="Source"
      />,
    ),
  );
}
it("reports an unconfirmed fork and re-enables the button without navigating", async () => {
  let finish!: (result: { _tag: "Failure"; cause: Cause.Cause<unknown> }) => void;
  fixture.fork.mockImplementation(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  await mount();
  await act(() => fixture.button.mock.lastCall![0].onPress());
  expect(fixture.button.mock.lastCall![0].disabled).toBe(true);
  await act(async () => finish({ _tag: "Failure", cause: Cause.fail("connection lost") }));
  expect(fixture.alert).toHaveBeenCalledWith(
    "Could not confirm fork",
    "Unable to confirm whether the forked thread was created. Check the existing thread list before retrying to avoid creating a duplicate.",
  );
  expect(fixture.ready).not.toHaveBeenCalled();
  expect(fixture.navigate).not.toHaveBeenCalled();
  expect(fixture.button.mock.lastCall![0].disabled).toBe(false);
});
it("silently clears busy after an interrupted fork without navigating", async () => {
  let finish!: (result: { _tag: "Failure"; cause: Cause.Cause<never> }) => void;
  fixture.fork.mockImplementation(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  await mount();
  await act(() => fixture.button.mock.lastCall![0].onPress());
  expect(fixture.button.mock.lastCall![0].disabled).toBe(true);
  await act(async () => finish({ _tag: "Failure", cause: Cause.interrupt() }));
  expect(fixture.alert).not.toHaveBeenCalled();
  expect(fixture.ready).not.toHaveBeenCalled();
  expect(fixture.navigate).not.toHaveBeenCalled();
  expect(fixture.button.mock.lastCall![0].disabled).toBe(false);
});
it("navigates only after the fork shell is ready", async () => {
  fixture.fork.mockResolvedValue({ _tag: "Success" });
  await mount();
  await act(async () => fixture.button.mock.lastCall![0].onPress());
  expect(fixture.fork).toHaveBeenCalledWith({
    environmentId,
    input: {
      sourceThreadId: threadId,
      targetThreadId: "target",
      runId: "run",
      title: "Source fork",
      creationSource: "mobile",
    },
  });
  expect(fixture.navigate).toHaveBeenCalledWith("Thread", { environmentId, threadId: "target" });
  expect(fixture.alert).not.toHaveBeenCalled();
  expect(fixture.button.mock.lastCall![0].disabled).toBe(false);
});
it("preserves the shell-ready timeout feedback without navigating", async () => {
  fixture.fork.mockResolvedValue({ _tag: "Success" });
  fixture.ready.mockResolvedValue(false);
  await mount();
  await act(async () => fixture.button.mock.lastCall![0].onPress());
  expect(fixture.alert).toHaveBeenCalledWith("Fork created", expect.stringContaining("Reconnect"));
  expect(fixture.navigate).not.toHaveBeenCalled();
  expect(fixture.button.mock.lastCall![0].disabled).toBe(false);
});
