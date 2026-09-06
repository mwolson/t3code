import { describe, expect, it } from "vite-plus/test";

import {
  applyOpenCodeProviderEnvironment,
  OPENCODE_BACKGROUND_SUBAGENTS_ENV,
} from "./OpenCodeProviderEnvironment.ts";

describe("applyOpenCodeProviderEnvironment", () => {
  it("does not remap host homes", () => {
    const environment = {
      HOME: "/home/alice",
      XDG_DATA_HOME: "/host/data",
      XDG_STATE_HOME: "/host/state",
    };
    const applied = applyOpenCodeProviderEnvironment(
      { backgroundSubagents: true, serverUrl: "" },
      environment,
      "oc2",
    );
    expect(applied.HOME).toBe("/home/alice");
    expect(applied.XDG_DATA_HOME).toBe("/host/data");
    expect(applied.XDG_STATE_HOME).toBe("/host/state");
    expect(applied[OPENCODE_BACKGROUND_SUBAGENTS_ENV]).toBe("true");
  });

  it("explicitly enables background subagents", () => {
    expect(
      applyOpenCodeProviderEnvironment(
        { backgroundSubagents: true, serverUrl: "" },
        {
          OPENCODE_EXPERIMENTAL: "false",
          [OPENCODE_BACKGROUND_SUBAGENTS_ENV]: "false",
        },
      ),
    ).toMatchObject({
      OPENCODE_EXPERIMENTAL: "false",
      [OPENCODE_BACKGROUND_SUBAGENTS_ENV]: "true",
    });
  });

  it("explicitly disables background subagents even under the umbrella experiment", () => {
    expect(
      applyOpenCodeProviderEnvironment(
        { backgroundSubagents: false, serverUrl: "" },
        { OPENCODE_EXPERIMENTAL: "true" },
      ),
    ).toMatchObject({
      OPENCODE_EXPERIMENTAL: "true",
      [OPENCODE_BACKGROUND_SUBAGENTS_ENV]: "false",
    });
  });
});
