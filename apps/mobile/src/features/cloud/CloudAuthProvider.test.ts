import { managedRelaySessionAtom } from "@t3tools/client-runtime/relay";
import * as Exit from "effect/Exit";
import { act, createElement, useEffect, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { appAtomRegistry } from "../../state/atom-registry";
import {
  activateCloudRelayAccount,
  CloudAuthProvider,
  deactivateCloudRelayAccount,
  useCloudAuthLoadState,
} from "./CloudAuthProvider";
import { setAgentAwarenessRelayTokenProvider } from "../agent-awareness/remoteRegistration";

const fixture = vi.hoisted(() => ({
  auth: { isLoaded: true, isSignedIn: true, userId: "alice", getToken: async () => "first-token" },
  mounts: 0,
  remove: vi.fn(async (_account: string | null) => ({
    _tag: "Success" as const,
    value: undefined,
  })),
}));

vi.mock("@clerk/expo", () => ({
  ClerkProvider: ({ children }: { children: ReactNode }) => {
    useEffect(() => {
      fixture.mounts++;
    }, []);
    return children;
  },
  useAuth: () => fixture.auth,
}));

vi.mock("@clerk/expo/token-cache", () => ({
  tokenCache: {},
}));

vi.mock("../../lib/runtime", () => ({
  runtime: {
    runPromiseExit: vi.fn(async () => Exit.succeed(undefined)),
  },
}));

vi.mock("../../connection/catalog", () => ({
  environmentCatalog: {
    removeRelayEnvironments: {},
  },
}));

vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: () => fixture.remove }));
vi.mock("./cloud-drafts", () => ({ removeCloudEnvironments: {} }));
vi.mock("../../state/use-composer-drafts", () => ({
  getComposerCloudAccountId: vi.fn(async () => null),
  restoreCloudComposerDrafts: vi.fn(async () => undefined),
}));
vi.mock("./connectOnboarding", () => ({
  clearConnectOnboardingRequest: vi.fn(),
  requestConnectOnboarding: vi.fn(),
}));

vi.mock("./publicConfig", () => ({
  resolveCloudPublicConfig: vi.fn(() => ({
    clerk: { publishableKey: "test-key" },
    relay: { url: "https://relay.test" },
  })),
  resolveRelayClerkTokenOptions: vi.fn(() => ({})),
}));

vi.mock("../agent-awareness/remoteRegistration", () => ({
  releaseAgentAwarenessRelayTokenProvider: vi.fn(),
  setAgentAwarenessRelayTokenProvider: vi.fn(),
  unregisterAgentAwarenessDeviceForCurrentUser: vi.fn(),
}));

let root: Root | undefined;

afterEach(async () => {
  if (root) await act(() => root?.unmount());
  root = undefined;
  vi.unstubAllGlobals();
  deactivateCloudRelayAccount();
  vi.clearAllMocks();
});

describe("CloudAuthProvider relay account isolation", () => {
  it("clears relay and agent-awareness credentials before cleanup can fail", async () => {
    const tokenProvider = async () => "account-1-token";
    activateCloudRelayAccount("account-1", tokenProvider);
    expect(appAtomRegistry.get(managedRelaySessionAtom)?.accountId).toBe("account-1");

    deactivateCloudRelayAccount();
    const cleanup = Promise.reject(new Error("Persistence removal failed.")).catch(() => undefined);

    expect(appAtomRegistry.get(managedRelaySessionAtom)).toBeNull();
    expect(vi.mocked(setAgentAwarenessRelayTokenProvider)).toHaveBeenLastCalledWith(null);
    await cleanup;
  });
});

describe("CloudAuthProvider Clerk retry", () => {
  it("retries loading and rebinds the same account without signing out or removing environments", async () => {
    let remount!: () => void;
    function Probe() {
      remount = useCloudAuthLoadState().remount;
      return null;
    }
    const container = stubDom();
    root = createRoot(container);
    const render = () =>
      act(async () => root?.render(createElement(CloudAuthProvider, null, createElement(Probe))));

    await render();
    expect(appAtomRegistry.get(managedRelaySessionAtom)?.accountId).toBe("alice");

    fixture.auth = { ...fixture.auth, isLoaded: false };
    await act(async () => remount());
    expect(fixture.mounts).toBe(2);
    expect(appAtomRegistry.get(managedRelaySessionAtom)?.accountId).toBe("alice");

    fixture.auth = { ...fixture.auth, isLoaded: true, getToken: async () => "fresh-token" };
    await render();
    expect(appAtomRegistry.get(managedRelaySessionAtom)?.accountId).toBe("alice");
    const [tokenProvider, accountId] =
      vi.mocked(setAgentAwarenessRelayTokenProvider).mock.lastCall ?? [];
    expect(accountId).toBe("alice");
    expect(await tokenProvider?.()).toBe("fresh-token");
    expect(fixture.remove).not.toHaveBeenCalled();
  });
});

function stubDom(): HTMLElement {
  const document = { nodeType: 9, addEventListener() {}, removeEventListener() {} };
  vi.stubGlobal("document", document);
  vi.stubGlobal("window", { document, HTMLIFrameElement: EventTarget });
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  return {
    nodeType: 1,
    tagName: "DIV",
    namespaceURI: "http://www.w3.org/1999/xhtml",
    ownerDocument: document,
    addEventListener() {},
    removeEventListener() {},
  } as unknown as HTMLElement;
}
