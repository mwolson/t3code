import { act, useEffect, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import * as Exit from "effect/Exit";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

const fixture = vi.hoisted(() => ({
  auth: { isLoaded: true, isSignedIn: true, userId: "alice", getToken: async () => "first-token" },
  mounts: 0,
  release: vi.fn(),
  session: vi.fn(),
  awareness: vi.fn(),
  remove: vi.fn(async (_account: string | null) => ({
    _tag: "Success" as const,
    value: undefined,
  })),
  onboarding: vi.fn(),
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
vi.mock("@clerk/expo/token-cache", () => ({ tokenCache: {} }));
vi.mock("@t3tools/client-runtime/relay", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@t3tools/client-runtime/relay")>()),
  setManagedRelaySession: fixture.session,
}));
vi.mock("../../lib/runtime", () => ({
  runtime: { runPromiseExit: async () => Exit.succeed(undefined) },
}));
vi.mock("../../state/atom-registry", () => ({ appAtomRegistry: {} }));
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: () => fixture.remove }));
vi.mock("./cloud-drafts", () => ({ removeCloudEnvironments: {} }));
vi.mock("../../state/use-composer-drafts", () => ({
  getComposerCloudAccountId: async () => null,
  restoreCloudComposerDrafts: async () => undefined,
}));
vi.mock("./connectOnboarding", () => ({
  clearConnectOnboardingRequest: vi.fn(),
  requestConnectOnboarding: fixture.onboarding,
}));
vi.mock("./publicConfig", () => ({
  resolveCloudPublicConfig: () => ({
    clerk: { publishableKey: "test-key" },
    relay: { url: "https://relay.test" },
  }),
  resolveRelayClerkTokenOptions: () => ({}),
}));
vi.mock("../agent-awareness/remoteRegistration", () => ({
  releaseAgentAwarenessRelayTokenProvider: fixture.release,
  setAgentAwarenessRelayTokenProvider: fixture.awareness,
  unregisterAgentAwarenessDeviceForCurrentUser: vi.fn(),
}));
import { CloudAuthProvider, useCloudAuthLoadState } from "./CloudAuthProvider";

let root: Root;
let remount: () => void;
function Probe() {
  remount = useCloudAuthLoadState().remount;
  return null;
}
async function render() {
  await act(async () => {
    root.render(
      <CloudAuthProvider>
        <Probe />
      </CloudAuthProvider>,
    );
  });
}
beforeEach(() => {
  vi.clearAllMocks();
  fixture.mounts = 0;
  fixture.auth = {
    isLoaded: true,
    isSignedIn: true,
    userId: "alice",
    getToken: async () => "first-token",
  };
  const document = { nodeType: 9, addEventListener() {}, removeEventListener() {} };
  const container = {
    nodeType: 1,
    tagName: "DIV",
    namespaceURI: "http://www.w3.org/1999/xhtml",
    ownerDocument: document,
    addEventListener() {},
    removeEventListener() {},
  };
  vi.stubGlobal("document", document);
  vi.stubGlobal("window", { document, HTMLIFrameElement: EventTarget });
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  root = createRoot(container as unknown as HTMLElement);
});
afterEach(async () => {
  await act(() => root.unmount());
  vi.unstubAllGlobals();
});

describe("manual Clerk recovery", () => {
  it("retries loading and rebinds the same account without signing out or removing environments", async () => {
    await render();
    expect(fixture.session).toHaveBeenLastCalledWith(
      {},
      { accountId: "alice", readClerkToken: expect.any(Function) },
    );
    fixture.session.mockClear();
    fixture.auth = { ...fixture.auth, isLoaded: false };
    await act(async () => remount());
    expect(fixture.mounts).toBe(2);
    expect(fixture.release).toHaveBeenCalledOnce();
    expect(fixture.session).not.toHaveBeenCalled();
    expect(fixture.remove).not.toHaveBeenCalled();
    fixture.auth = { ...fixture.auth, isLoaded: true, getToken: async () => "fresh-token" };
    await render();
    const session = fixture.session.mock.lastCall?.[1];
    expect(session.accountId).toBe("alice");
    expect(await session.readClerkToken()).toBe("fresh-token");
    expect(fixture.onboarding).not.toHaveBeenCalled();
  });

  it("preserves a pending account cleanup across retry before activating a different account", async () => {
    await render();
    let finish!: () => void;
    const pending = new Promise<void>((resolve) => {
      finish = resolve;
    });
    fixture.remove.mockImplementationOnce(async () => {
      await pending;
      return { _tag: "Success", value: undefined };
    });
    fixture.auth = { ...fixture.auth, userId: "bob" };
    await render();
    expect(fixture.remove).toHaveBeenCalledExactlyOnceWith("alice");
    expect(fixture.session).toHaveBeenLastCalledWith({}, null);
    await act(async () => remount());
    expect(fixture.session).toHaveBeenLastCalledWith({}, null);
    await act(async () => finish());
    expect(fixture.remove).toHaveBeenCalledTimes(1);
    expect(fixture.session).toHaveBeenLastCalledWith(
      {},
      { accountId: "bob", readClerkToken: expect.any(Function) },
    );
  });

  it("still clears the session and removes the old account on sign-out", async () => {
    await render();
    fixture.auth = { ...fixture.auth, isSignedIn: false };
    await render();
    expect(fixture.session).toHaveBeenLastCalledWith({}, null);
    expect(fixture.remove).toHaveBeenCalledExactlyOnceWith("alice");
    expect(fixture.awareness).toHaveBeenLastCalledWith(null);
  });
});
