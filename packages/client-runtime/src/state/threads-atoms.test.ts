import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Layer from "effect/Layer";
import { Atom } from "effect/unstable/reactivity";

import type { EnvironmentRegistry } from "../connection/registry.ts";
import type { EnvironmentCacheStore } from "../platform/persistence.ts";
import {
  getThreadStateIdleTtlMs,
  setThreadStateIdleTtlMs,
  THREAD_STATE_IDLE_TTL_MS,
  THREAD_STATE_MOBILE_IDLE_TTL_MS,
} from "./threadRetention.ts";
import { createEnvironmentThreadStateAtoms, type ThreadSnapshotLoader } from "./threads.ts";

describe("createEnvironmentThreadStateAtoms", () => {
  it("retains thread state across short subscriber gaps", () => {
    setThreadStateIdleTtlMs(null);
    const runtime = Atom.runtime(Layer.empty) as unknown as Atom.AtomRuntime<
      EnvironmentRegistry | EnvironmentCacheStore | ThreadSnapshotLoader,
      never
    >;
    const threads = createEnvironmentThreadStateAtoms(runtime);
    const environmentId = EnvironmentId.make("environment-1");
    const threadId = ThreadId.make("thread-1");
    const atom = threads.stateAtom(environmentId, threadId);

    expect(atom.idleTTL).toBe(THREAD_STATE_IDLE_TTL_MS);
    expect(getThreadStateIdleTtlMs()).toBe(THREAD_STATE_IDLE_TTL_MS);
    expect(threads.stateAtom(environmentId, threadId)).toBe(atom);
    expect(threads.stateAtom(environmentId, ThreadId.make("thread-2"))).not.toBe(atom);
  });

  it("uses a mobile idle TTL override when configured before atom creation", () => {
    setThreadStateIdleTtlMs(THREAD_STATE_MOBILE_IDLE_TTL_MS);
    try {
      const runtime = Atom.runtime(Layer.empty) as unknown as Atom.AtomRuntime<
        EnvironmentRegistry | EnvironmentCacheStore | ThreadSnapshotLoader,
        never
      >;
      const threads = createEnvironmentThreadStateAtoms(runtime);
      const atom = threads.stateAtom(
        EnvironmentId.make("environment-mobile"),
        ThreadId.make("thread-mobile"),
      );
      expect(atom.idleTTL).toBe(THREAD_STATE_MOBILE_IDLE_TTL_MS);
      expect(getThreadStateIdleTtlMs()).toBe(THREAD_STATE_MOBILE_IDLE_TTL_MS);
    } finally {
      setThreadStateIdleTtlMs(null);
    }
  });
});
