import { ProviderInstanceId, type ModelSelection } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import { resolveNewThreadModelSelectionOverride } from "./chatThreadActions";

const PROJECT_DEFAULT_SELECTION: ModelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "project-default",
};
const CARRIED_SELECTION: ModelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "carried-model",
};
const STICKY_SELECTION: ModelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "last-used-model",
};

describe("chatThreadActions", () => {
  it("does not carry a non-explicit model from the destination draft back into itself", () => {
    expect(
      resolveNewThreadModelSelectionOverride({
        projectDefaultSelection: null,
        stickySelection: null,
        carrySelection: CARRIED_SELECTION,
        carrySourceDraftId: "draft-a",
        destinationDraftId: "draft-a",
      }),
    ).toBeNull();
  });

  it("prefers the carried model, then the last-used model, then the project default", () => {
    const resolve = (carrySelection: ModelSelection | null, sticky: ModelSelection | null) =>
      resolveNewThreadModelSelectionOverride({
        projectDefaultSelection: PROJECT_DEFAULT_SELECTION,
        stickySelection: sticky,
        carrySelection,
        carrySourceDraftId: "draft-a",
        destinationDraftId: "draft-b",
      });
    expect(resolve(CARRIED_SELECTION, STICKY_SELECTION)).toEqual(CARRIED_SELECTION);
    expect(resolve(null, STICKY_SELECTION)).toEqual(STICKY_SELECTION);
    expect(resolve(null, null)).toEqual(PROJECT_DEFAULT_SELECTION);
  });
});
