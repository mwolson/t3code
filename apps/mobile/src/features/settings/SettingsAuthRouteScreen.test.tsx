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
import { SettingsAuthRouteScreen } from "./SettingsAuthRouteScreen";
vi.mock("@clerk/expo/native-components", () => ({
  AuthView: () => null,
  UserProfileView: () => null,
}));
vi.mock("@react-navigation/native", () => ({ useNavigation: () => ({ goBack() {} }) }));
vi.mock("react-native", async () => {
  const { createElement } = await import("react");
  return { View: ({ children }: { children: ReactNode }) => createElement("div", {}, children) };
});
vi.mock("../cloud/publicConfig", () => ({ hasCloudPublicConfig: () => true }));
vi.mock("../cloud/T3ConnectProfilePage", () => ({ T3ConnectProfilePage: () => null }));
vi.mock("../../components/ui/AppActivityIndicator", () => ({ AppActivityIndicator: () => null }));
vi.mock("../../components/ui/AppText", async () => {
  const { createElement } = await import("react");
  return {
    AppText: ({ children }: { children: ReactNode }) => createElement("span", {}, children),
  };
});
vi.mock("../../components/ui/Button", async () => {
  const { createElement } = await import("react");
  return {
    Button: ({ label, onPress }: { label: string; onPress: () => void }) =>
      createElement("button", { onClick: onPress }, label),
  };
});

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

it("recovers a failed account load in place without overlapping retries", async () => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  const { app, clerk, container, failPending, root, succeedNewest } = await renderApp();
  try {
    expect(container.querySelector("button")).toBeNull();
    await act(async () => failPending());
    expect(app.isLoaded).toBe(false);
    const retry = container.querySelector("button");
    expect(retry?.textContent).toBe("Retry");
    await act(async () => {
      for (let tap = 0; tap < 3; tap++) retry?.click();
    });
    expect(clerk.load).toHaveBeenCalledTimes(2);
    expect(container.querySelector("button")).toBeNull();
    await act(async () => succeedNewest());
    expect(app.isLoaded).toBe(true);
    expect(app.mounts).toBe(1);
  } finally {
    await act(() => root.unmount());
    container.remove();
    vi.unstubAllGlobals();
  }
});

async function renderApp() {
  const fake = fakeNativeClerk();
  const app = { mounts: 0, isLoaded: false };
  function App() {
    const [mountId] = useState(() => app.mounts + 1);
    const { isLoaded } = useAuth();
    useLayoutEffect(() => {
      Object.assign(app, { mounts: mountId, isLoaded });
    });
    return <SettingsAuthRouteScreen />;
  }
  const container = document.createElement("div");
  document.body.append(container);
  const root = createRoot(container);
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
  return { ...fake, app, container, root };
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
