import type { OpenCode2Settings } from "@t3tools/contracts";

export const OPENCODE_BACKGROUND_SUBAGENTS_ENV = "OPENCODE_EXPERIMENTAL_BACKGROUND_SUBAGENTS";

export function applyOpenCodeProviderEnvironment(
  settings: Pick<OpenCode2Settings, "backgroundSubagents" | "serverUrl">,
  environment: NodeJS.ProcessEnv,
  _instanceId?: string,
  _environmentIdentity?: string,
): NodeJS.ProcessEnv {
  return {
    ...environment,
    [OPENCODE_BACKGROUND_SUBAGENTS_ENV]: settings.backgroundSubagents ? "true" : "false",
  };
}
