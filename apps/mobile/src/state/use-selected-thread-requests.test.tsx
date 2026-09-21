import { renderToStaticMarkup } from "react-dom/server";
import { beforeEach, describe, expect, it, vi } from "vite-plus/test";

const fixture = vi.hoisted(() => ({
  drafts: {} as Record<string, unknown>,
  uploads: {} as Record<string, unknown>,
  preparations: {} as Record<string, number>,
  preparationAtom: Symbol("preparation"),
  requestIds: ["request-1"],
}));
vi.mock("react-native", () => ({ Alert: { alert: vi.fn() } }));
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
    selectedThread: { environmentId: "environment-1", id: "thread-1" },
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

import { ApprovalRequestId, EnvironmentId, RuntimeRequestId, ThreadId } from "@t3tools/contracts";
import { questionAttachmentDraftKey } from "./question-attachments";
import { useSelectedThreadRequests } from "./use-selected-thread-requests";
import { appAtomRegistry } from "./atom-registry";

const environmentId = EnvironmentId.make("environment-1");
const key = (question: string) =>
  questionAttachmentDraftKey(
    environmentId,
    ThreadId.make("thread-1"),
    ApprovalRequestId.make("request-1"),
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
  it("ignores custom-answer events from a request that is no longer displayed", () => {
    fixture.requestIds = ["request-2", "request-1"];
    const callbacks: Array<
      ReturnType<typeof useSelectedThreadRequests>["onChangeUserInputCustomAnswer"]
    > = [];
    function Probe() {
      callbacks.push(useSelectedThreadRequests().onChangeUserInputCustomAnswer);
      return null;
    }
    renderToStaticMarkup(<Probe />);
    const update = callbacks[0]!;
    const set = vi.spyOn(appAtomRegistry, "set");
    try {
      update(RuntimeRequestId.make("request-1"), "first", "stale answer");
      expect(set).not.toHaveBeenCalled();
      update(RuntimeRequestId.make("request-2"), "first", "current answer");
      expect(set).toHaveBeenCalledOnce();
    } finally {
      set.mockRestore();
    }
  });
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
