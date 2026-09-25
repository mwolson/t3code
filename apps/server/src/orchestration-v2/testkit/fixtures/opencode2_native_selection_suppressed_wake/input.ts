import { openCode2RetiredSuppressWakeInput } from "../opencode2_retired_suppress_wake/input.ts";
import type { OrchestratorFixtureInput } from "../shared.ts";

export function openCode2NativeSelectionSuppressedWakeInput(): OrchestratorFixtureInput {
  const input = openCode2RetiredSuppressWakeInput();
  return {
    ...input,
    steps: input.steps.map((step, index) =>
      index === input.steps.length - 1 && step.type === "message"
        ? { ...step, text: `/review ${step.text}` }
        : step,
    ),
  };
}
