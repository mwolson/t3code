import { ProviderInstanceId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { openCode2SessionSelectionParameters } from "./OpenCode2AdapterV2.ts";

describe("openCode2SessionSelectionParameters", () => {
  it("sends the canonical option id when creating or switching a native session", () => {
    expect(
      openCode2SessionSelectionParameters({
        instanceId: ProviderInstanceId.make("opencode2"),
        model: "opencode/glm-5.2",
        options: [{ id: "agent", value: "build" }],
      }),
    ).toEqual({
      model: { id: "glm-5.2", providerID: "opencode" },
      agent: "build",
    });
  });

  it("preserves custom executable agent ids verbatim", () => {
    expect(
      openCode2SessionSelectionParameters({
        instanceId: ProviderInstanceId.make("opencode2"),
        model: "opencode/glm-5.2",
        options: [{ id: "agent", value: "Release-Captain" }],
      }).agent,
    ).toBe("Release-Captain");
  });
});
