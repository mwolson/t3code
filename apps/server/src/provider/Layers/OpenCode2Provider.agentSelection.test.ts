import type { AgentInfoV2, ModelInfo } from "@opencode-ai/sdk-next/v2";
import { describe, expect, it } from "vite-plus/test";

import { flattenOpenCode2Models } from "./OpenCode2Provider.ts";

const MODEL = {
  id: "glm-5.2",
  modelID: "glm-5.2",
  providerID: "opencode",
  name: "GLM-5.2",
  capabilities: {
    tools: true,
    input: ["text"],
    output: ["text"],
  },
  variants: [],
  time: { released: 0 },
  cost: [],
  status: "active",
  enabled: true,
  limit: {
    context: 128_000,
    output: 16_384,
  },
} satisfies ModelInfo;

const BUILD_AGENT = {
  id: "build",
  name: "Build",
  request: { settings: {}, headers: {}, body: {} },
  mode: "primary",
  hidden: false,
  permissions: [],
} satisfies AgentInfoV2;

describe("OpenCode 2 agent inventory", () => {
  it("keeps executable agent ids separate from title-cased labels", () => {
    const customAgent = {
      ...BUILD_AGENT,
      id: "Release-Captain",
      name: "Release Captain",
    } satisfies AgentInfoV2;
    const [model] = flattenOpenCode2Models({
      models: [MODEL],
      agents: [customAgent, BUILD_AGENT],
    });
    const descriptor = model?.capabilities?.optionDescriptors?.find(
      (candidate) => candidate.id === "agent",
    );

    expect(descriptor).toEqual({
      id: "agent",
      label: "Agent",
      type: "select",
      currentValue: "build",
      options: [
        { id: "Release-Captain", label: "Release Captain" },
        { id: "build", label: "Build", isDefault: true },
      ],
    });
  });
});
