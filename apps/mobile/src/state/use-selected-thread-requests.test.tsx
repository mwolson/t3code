import { act } from "react";
import { createRoot } from "react-dom/client";
import * as DateTime from "effect/DateTime";
import { renderToStaticMarkup } from "react-dom/server";
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
vi.mock("./threads", () => ({ threadEnvironment: {} }));
vi.mock("./use-atom-command", () => ({ useAtomCommand: () => vi.fn() }));
vi.mock("./use-thread-selection", () => ({
  useThreadSelection: () => ({
    selectedThread: fixture.selectedThread,
  }),
}));
vi.mock("./use-thread-detail", () => ({
  useSelectedThreadProjection: () => ({
    id: fixture.selectedThread.id,
    projection: {
      runtimeRequests: fixture.requestIds.map((requestId) => ({
        id: requestId,
        kind: "user_input",
        status: "pending",
        createdAt: DateTime.makeUnsafe("2026-09-08T00:00:00Z"),
        responseCapability: { type: "live" },
      })),
      turnItems: fixture.requestIds.map((requestId) => ({
        type: "user_input_request",
        requestId,
        questions: ["first", "second"].map((id) => ({
          id,
          header: id,
          question: `Attach ${id} file`,
          options: [],
          allowCustomAnswer: true,
        })),
      })),
    },
  }),
}));

import { RuntimeRequestId, EnvironmentId, ThreadId } from "@t3tools/contracts";
import { questionAttachmentDraftKey } from "./question-attachments";
import { useSelectedThreadRequests } from "./use-selected-thread-requests";

const environmentId = EnvironmentId.make("environment-1");
const key = (question: string) =>
  questionAttachmentDraftKey(
    environmentId,
    ThreadId.make("thread-1"),
    RuntimeRequestId.make("request-1"),
    question,
  );
function submitButtonMarkup() {
  function Probe() {
    const { activePendingUserInputAnswers } = useSelectedThreadRequests();
    return <button disabled={activePendingUserInputAnswers === null}>Submit answers</button>;
  }
  return renderToStaticMarkup(<Probe />);
}
beforeEach(() => {
  fixture.requestIds = ["request-1"];
  fixture.selectedThread = { environmentId: "environment-1", id: "thread-1" };
  fixture.set.mockClear();
  fixture.preparations = {};
  fixture.drafts = Object.fromEntries(
    ["first", "second"].map((id) => [
      key(id),
      {
        attachments: [
          {
            id,
            type: "file",
            name: `${id}.txt`,
            mimeType: "text/plain",
            sizeBytes: 4,
            fileUri: `file:///${id}.txt`,
          },
        ],
      },
    ]),
  );
  fixture.uploads = { "environment-1:first": { status: "ready" } };
});
describe("question draft ownership", () => {
  it.each(["id", "environmentId"] as const)(
    "rejects stale request, %s and unmounted callbacks",
    async (scope) => {
      fixture.drafts = {};
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
        update(RuntimeRequestId.make("missing"), "first", "missing");
        update(RuntimeRequestId.make("request-1"), "missing", "unknown question");
        expect(fixture.set).not.toHaveBeenCalled();
        update(RuntimeRequestId.make("request-1"), "first", "current");
        expect(fixture.set).toHaveBeenCalledOnce();
        const retained = update;
        fixture.set.mockClear();
        fixture.requestIds = ["request-2", "request-1"];
        await act(() => root.render(<Probe />));
        retained(RuntimeRequestId.make("request-1"), "first", "delayed");
        update(RuntimeRequestId.make("request-1"), "first", "hidden");
        expect(fixture.set).not.toHaveBeenCalled();
        update(RuntimeRequestId.make("request-2"), "first", "new current");
        expect(fixture.set).toHaveBeenCalledOnce();
        fixture.set.mockClear();
        fixture.requestIds = [];
        await act(() => root.render(<Probe />));
        retained(RuntimeRequestId.make("request-1"), "first", "removed");
        expect(fixture.set).not.toHaveBeenCalled();
        fixture.requestIds = ["request-1"];
        await act(() => root.render(<Probe />));
        const beforeSwitch = update;
        fixture.selectedThread = { ...fixture.selectedThread, [scope]: "other-scope" };
        await act(() => root.render(<Probe />));
        beforeSwitch(RuntimeRequestId.make("request-1"), "first", "old scope");
        expect(fixture.set).not.toHaveBeenCalled();
        update(RuntimeRequestId.make("request-1"), "first", "current scope");
        expect(fixture.set).toHaveBeenCalledOnce();
        fixture.set.mockClear();
        const beforeUnmount = update;
        await act(() => root.render(null));
        beforeUnmount(RuntimeRequestId.make("request-1"), "first", "unmounted");
        expect(fixture.set).not.toHaveBeenCalled();
      } finally {
        await act(() => root.unmount());
        vi.unstubAllGlobals();
      }
    },
  );
});

describe("question attachment submission readiness", () => {
  it.each([
    undefined,
    { status: "uploading", progress: 0.5 },
    { status: "failed", reason: "Offline" },
  ])("keeps Submit disabled until all question uploads finish: %j", (state) => {
    if (state) fixture.uploads["environment-1:second"] = state;
    expect(submitButtonMarkup()).toContain("disabled");
    fixture.uploads["environment-1:second"] = { status: "ready" };
    expect(submitButtonMarkup()).not.toContain("disabled");
  });
  it("ignores an upload in another environment", () => {
    fixture.uploads["environment-1:second"] = { status: "ready" };
    fixture.uploads["environment-2:second"] = { status: "uploading", progress: 0.5 };
    expect(submitButtonMarkup()).not.toContain("disabled");
  });
  it("waits for attachment preparation even when uploads are ready", () => {
    fixture.uploads["environment-1:second"] = { status: "ready" };
    fixture.preparations[key("first")] = 1;
    expect(submitButtonMarkup()).toContain("disabled");
  });
});
