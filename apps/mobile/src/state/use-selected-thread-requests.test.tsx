import { act } from "react";
import { createRoot } from "react-dom/client";
import { beforeEach, describe, expect, it, vi } from "vite-plus/test";

const fixture = vi.hoisted(() => ({
  drafts: {} as Record<string, unknown>,
  uploads: {} as Record<string, unknown>,
  preparations: {} as Record<string, number>,
  preparationAtom: Symbol("preparation"),
  requestIds: ["request-1"],
  selectedThread: { environmentId: "environment-1", id: "thread-1" },
  set: vi.fn(),
}));
vi.mock("react-native", () => ({ Alert: { alert: vi.fn() } }));
vi.mock("./atom-registry", () => ({ appAtomRegistry: { get: () => ({}), set: fixture.set } }));
vi.mock("@effect/atom-react", () => ({
  useAtomValue: (atom: unknown) =>
    atom === "drafts"
      ? fixture.drafts
      : atom === "uploads"
        ? fixture.uploads
        : atom === fixture.preparationAtom
          ? fixture.preparations
          : {},
}));
vi.mock("./use-composer-drafts", () => ({
  composerDraftsAtom: "drafts",
  clearComposerDraft: vi.fn(),
  questionAttachmentDraftsAtom: () => "drafts",
}));
vi.mock("./composer-attachment-uploads", async () => ({
  ...(await import("../lib/composerAttachmentUploadQueue")),
  composerAttachmentUploadsAtom: "uploads",
}));
vi.mock("./question-attachments", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./question-attachments")>()),
  questionAttachmentPreparationAtom: fixture.preparationAtom,
}));
vi.mock("./entities", () => ({
  useServerConfigs: () =>
    new Map([
      [
        "environment-1",
        {
          environment: {
            capabilities: {
              questionAttachments: true,
              attachmentUploads: true,
              fileAttachments: { maxUploadBytes: 20_000_000 },
            },
          },
        },
      ],
    ]),
}));
vi.mock("./session", () => ({ readEnvironmentScope: () => true }));
vi.mock("./threads", () => ({ threadEnvironment: {} }));
vi.mock("./use-atom-command", () => ({ useAtomCommand: () => vi.fn() }));
vi.mock("./use-thread-selection", () => ({
  useThreadSelection: () => ({
    selectedThread: fixture.selectedThread,
  }),
}));
vi.mock("./use-thread-detail", () => ({
  useSelectedThreadPendingRequests: () => ({
    approvals: [],
    userInputs: fixture.requestIds.map((requestId) => ({
      requestId,
      createdAt: "2026-09-08T00:00:00Z",
      responseCapability: "live",
      dismissible: false,
      questions: ["first", "second"].map((id) => ({
        id,
        header: id,
        question: `Attach ${id} file`,
        options: [],
        allowCustomAnswer: true,
        multiSelect: false,
      })),
    })),
  }),
}));

import { RuntimeRequestId } from "@t3tools/contracts";
import { useSelectedThreadRequests } from "./use-selected-thread-requests";

beforeEach(() => {
  fixture.requestIds = ["request-1"];
  fixture.selectedThread = { environmentId: "environment-1", id: "thread-1" };
  fixture.set.mockClear();
  fixture.drafts = {};
  fixture.preparations = {};
  fixture.uploads = {};
});
describe("question draft ownership", () => {
  it("writes custom answers only to the displayed request", async () => {
    fixture.requestIds = ["request-1", "request-2"];
    let update!: ReturnType<typeof useSelectedThreadRequests>["onChangeUserInputCustomAnswer"];
    function Probe() {
      update = useSelectedThreadRequests().onChangeUserInputCustomAnswer;
      return null;
    }
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
    const root = createRoot(container as unknown as HTMLElement);
    try {
      await act(() => root.render(<Probe />));
      update(RuntimeRequestId.make("request-2"), "first", "hidden");
      expect(fixture.set).not.toHaveBeenCalled();
      update(RuntimeRequestId.make("request-1"), "first", "current");
      expect(fixture.set).toHaveBeenCalledOnce();

      const retained = update;
      fixture.set.mockClear();
      fixture.requestIds = ["request-2", "request-1"];
      await act(() => root.render(<Probe />));
      retained(RuntimeRequestId.make("request-1"), "first", "delayed");
      expect(fixture.set).not.toHaveBeenCalled();
      update(RuntimeRequestId.make("request-2"), "first", "new current");
      expect(fixture.set).toHaveBeenCalledOnce();
    } finally {
      await act(() => root.unmount());
      vi.unstubAllGlobals();
    }
  });
});
