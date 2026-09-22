import * as NodeModule from "node:module";
import { act, type ComponentType, type ReactNode, useLayoutEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import { expect, it, vi } from "vite-plus/test";

// The real @clerk/react behind @clerk/expo, so this pins the provider's reload contract.
vi.mock("@clerk/expo", async () => {
  const NodeModule = await import("node:module");
  const clerkExpo = NodeModule.createRequire(import.meta.url).resolve("@clerk/expo/package.json");
  const { useAuth, useClerk } = NodeModule.createRequire(clerkExpo)("@clerk/react");
  return { useAuth, useClerk };
});

import { useAuth } from "@clerk/expo";
import { useClerkLoadRetry } from "./clerkLoadRecovery";

const { InternalClerkProvider } = NodeModule.createRequire(
  NodeModule.createRequire(import.meta.url).resolve("@clerk/expo/package.json"),
)("@clerk/react/internal") as {
  readonly InternalClerkProvider: ComponentType<{
    readonly Clerk: unknown;
    readonly children: ReactNode;
    readonly experimental: { readonly runtimeEnvironment: "headless" };
    readonly publishableKey: string;
  }>;
};

it("retries a failed Clerk load in place, one load at a time, without remounting", async () => {
  const { app, clerk, failPending, probe, root, succeedNewest } = await renderApp();
  await act(async () => {
    for (let tap = 0; tap < 3; tap++) probe.retry?.();
  });
  expect(clerk.load).toHaveBeenCalledTimes(1);

  await act(async () => failPending());
  expect(app.isLoaded).toBe(false);
  await act(async () => {
    const retry = probe.retry;
    for (let tap = 0; tap < 3; tap++) retry?.();
  });
  expect(clerk.load).toHaveBeenCalledTimes(2);
  expect(probe.retry).toBeNull();

  await act(async () => succeedNewest());
  expect(app.isLoaded).toBe(true);
  expect(app.mounts).toBe(1);
  act(() => root.unmount());
});

it("keeps Clerk loaded when an older load fails after a newer one succeeds", async () => {
  const { app, failPending, probe, root, succeedNewest } = await renderApp();
  await act(async () => probe.retry?.());
  await act(async () => succeedNewest());
  await act(async () => failPending());
  expect(app.isLoaded).toBe(true);
  act(() => root.unmount());
});

async function renderApp() {
  const fake = fakeNativeClerk();
  const app = { mounts: 0, isLoaded: false };
  const probe = { retry: null as (() => void) | null };
  function App() {
    const [mountId] = useState(() => app.mounts + 1);
    const retry = useClerkLoadRetry();
    const { isLoaded } = useAuth();
    useLayoutEffect(() => {
      Object.assign(app, { mounts: mountId, isLoaded });
      probe.retry = retry;
    });
    return null;
  }
  const root = createRoot(document.createElement("div"));
  await act(async () =>
    root.render(
      <InternalClerkProvider
        Clerk={fake.clerk}
        experimental={{ runtimeEnvironment: "headless" }}
        publishableKey="pk_test_ZXhhbXBsZS5jbGVyay5hY2NvdW50cy5kZXYk"
      >
        <App />
      </InternalClerkProvider>,
    ),
  );
  return { ...fake, app, probe, root };
}

// Mirrors clerk-js: loaded derives from status, and a failed load emits "error".
function fakeNativeClerk() {
  const listeners = new Set<(resources: unknown) => void>();
  const statusListeners = new Set<(status: string) => void>();
  const pending: Array<{ resolve: () => void; reject: (error: Error) => void }> = [];
  const setStatus = (status: string) => {
    clerk.status = status;
    for (const listener of statusListeners) listener(status);
  };
  const clerk = {
    status: "loading",
    get loaded() {
      return clerk.status === "ready" || clerk.status === "degraded";
    },
    __internal_lastEmittedResources: undefined as unknown,
    load: vi.fn(() => new Promise<void>((resolve, reject) => pending.push({ resolve, reject }))),
    addListener: (listener: (resources: unknown) => void) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    on: (_event: string, listener: (status: string) => void, options?: { notify?: boolean }) => {
      statusListeners.add(listener);
      if (options?.notify) listener(clerk.status);
    },
    off: (_event: string, listener: (status: string) => void) => {
      statusListeners.delete(listener);
    },
  };
  return {
    clerk,
    failPending: () => {
      for (const load of pending.splice(0)) {
        setStatus("error");
        load.reject(new Error("Clerk is unreachable"));
      }
    },
    succeedNewest: () => {
      const load = pending.pop();
      clerk.__internal_lastEmittedResources = {
        client: { sessions: [], signedInSessions: [] },
        session: null,
        user: null,
        organization: null,
      };
      setStatus("ready");
      for (const listener of listeners) listener(clerk.__internal_lastEmittedResources);
      load?.resolve();
    },
  };
}
