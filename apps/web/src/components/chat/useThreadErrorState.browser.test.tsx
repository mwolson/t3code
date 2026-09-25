import { expect, it, vi } from "vite-plus/test";
import { render } from "../../../test/render";
import { useThreadErrorState } from "./useThreadErrorState";

type Input = Parameters<typeof useThreadErrorState>[0];

function Probe({ input }: { input: Input }) {
  const state = useThreadErrorState(input);
  return (
    <div>
      <output aria-label="Error">{state.error ?? "No error"}</output>
      <output aria-label="Class">{state.errorClass ?? "No class"}</output>
      <button onClick={state.dismiss}>Dismiss</button>
      <button onClick={() => state.setError(input.threadKey, true, "Save failed")}>Fail</button>
    </div>
  );
}

function runtimeInput(threadKey: string): Input {
  return {
    threadKey,
    draftId: "draft",
    isServerThread: true,
    runtime: {
      lastError: "Durable failure",
      lastErrorAt: "2026-09-25T00:00:00Z",
      lastErrorClass: "usage_limit",
    },
  };
}

it("dismisses a local failure until the provider reports another occurrence", async () => {
  const input = runtimeInput("error-hook-local");
  // This hook owns local error state without reading routes.
  await using screen = await render(<Probe input={input} />, { router: false });
  await screen.getByRole("button", { name: "Fail", exact: true }).click();
  await expect
    .element(screen.getByRole("status", { name: "Error" }))
    .toHaveTextContent("Save failed");
  await screen.getByRole("button", { name: "Dismiss" }).click();
  await expect.element(screen.getByRole("status", { name: "Error" })).toHaveTextContent("No error");
  await screen.rerender(
    <Probe
      input={{ ...input, runtime: { ...input.runtime!, lastErrorAt: "2026-09-25T00:01:00Z" } }}
    />,
  );
  await expect
    .element(screen.getByRole("status", { name: "Error" }))
    .toHaveTextContent("Durable failure");
  await expect
    .element(screen.getByRole("status", { name: "Class" }))
    .toHaveTextContent("usage_limit");
});

it("shows a repeated local failure even in the same clock tick", async () => {
  using _now = vi.spyOn(Date, "now").mockReturnValue(100);
  // No route is involved in error occurrence ownership.
  await using screen = await render(<Probe input={runtimeInput("error-hook-repeat")} />, {
    router: false,
  });
  await screen.getByRole("button", { name: "Fail", exact: true }).click();
  await screen.getByRole("button", { name: "Dismiss" }).click();
  await screen.getByRole("button", { name: "Fail", exact: true }).click();
  await expect
    .element(screen.getByRole("status", { name: "Error" }))
    .toHaveTextContent("Save failed");
  await expect.element(screen.getByRole("status", { name: "Class" })).toHaveTextContent("No class");
});
