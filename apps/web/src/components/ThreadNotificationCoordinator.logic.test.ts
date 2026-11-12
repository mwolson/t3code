import { describe, expect, it, vi } from "vite-plus/test";

import {
  createThreadNotificationGate,
  type ThreadNotificationObservation,
} from "./ThreadNotificationCoordinator.logic";

const working: ThreadNotificationObservation = {
  attention: null,
  settled: false,
  completedAt: null,
};
const completed = (at: number): ThreadNotificationObservation => ({
  attention: null,
  settled: true,
  completedAt: at,
});
const visible = { archived: false, quiet: false };

function harness() {
  const notify = vi.fn();
  const gate = createThreadNotificationGate<string>(notify);
  let prior: ThreadNotificationObservation | undefined;
  const observe = (next: ThreadNotificationObservation, options = visible) => {
    gate.observe("thread", prior, next, options);
    prior = next;
  };
  return { gate, notify, observe };
}

describe("createThreadNotificationGate", () => {
  it("notifies a completion immediately and only once", () => {
    const { notify, observe } = harness();
    observe(working);
    observe(completed(1));
    expect(notify.mock.calls).toEqual([["thread", "completion"]]);
    observe({ ...completed(1) });
    observe(working);
    observe(completed(1));
    expect(notify).toHaveBeenCalledTimes(1);
  });

  it("notifies each newer completion immediately", () => {
    const { notify, observe } = harness();
    observe(working);
    observe(completed(1));
    observe(working);
    observe(completed(2));
    expect(notify.mock.calls).toEqual([
      ["thread", "completion"],
      ["thread", "completion"],
    ]);
    observe(completed(1));
    expect(notify).toHaveBeenCalledTimes(2);
  });

  it("notifies attention once and still notifies the subsequent completion", () => {
    const { notify, observe } = harness();
    observe(working);
    const attention = { attention: "run-1:approval", settled: false, completedAt: null };
    observe(attention);
    expect(notify.mock.calls).toEqual([["thread", "input"]]);
    observe({ ...attention });
    expect(notify).toHaveBeenCalledTimes(1);
    observe(working);
    observe(completed(1));
    expect(notify.mock.calls).toEqual([
      ["thread", "input"],
      ["thread", "completion"],
    ]);
  });

  it("silences a delegated question while other attention still notifies", () => {
    const { gate, notify } = harness();
    const question = { ...working, attention: "run-1:input" };
    gate.observe("thread", working, question, { ...visible, quietInput: true });
    expect(notify).not.toHaveBeenCalled();
    gate.observe("thread", question, { ...working, attention: "run-1:approval" }, visible);
    expect(notify.mock.calls).toEqual([["thread", "input"]]);
  });

  it("does not replay a completion after attention resolves", () => {
    const { notify, observe } = harness();
    observe(working);
    observe(completed(1));
    observe({ attention: "run-1:limited", settled: false, completedAt: null });
    observe(completed(1));
    expect(notify.mock.calls).toEqual([
      ["thread", "completion"],
      ["thread", "input"],
    ]);
  });

  it("never notifies existing, archived or stopped completions, including when unarchived", () => {
    const { notify, observe } = harness();
    observe(completed(1));
    observe(working);
    observe(completed(2), { ...visible, quiet: true });
    observe(completed(2));
    observe(working);
    observe(completed(3), { ...visible, archived: true });
    observe(completed(3));
    expect(notify).not.toHaveBeenCalled();
    observe(working);
    observe(completed(4));
    expect(notify.mock.calls).toEqual([["thread", "completion"]]);
  });

  it("confirms a completion that follows held work before notifying it", () => {
    const held = { ...working, held: true };
    const approval = { ...held, attention: "run-1:approval" };
    function confirmingHarness() {
      const notify = vi.fn();
      const confirmations: Array<() => void> = [];
      const cancel = vi.fn();
      const gate = createThreadNotificationGate<string>(notify, (confirmed) => {
        confirmations.push(confirmed);
        return cancel;
      });
      let prior: ThreadNotificationObservation = working;
      const observe = (next: ThreadNotificationObservation) => {
        gate.observe("thread", prior, next, visible);
        prior = next;
      };
      return { gate, notify, confirmations, cancel, observe };
    }

    // No wake follows: the completion notifies once its confirmation runs.
    const done = confirmingHarness();
    for (const next of [held, completed(1), completed(1)]) done.observe(next);
    expect(done.notify).not.toHaveBeenCalled();
    done.confirmations[0]?.();
    done.observe(completed(1));
    expect(done.notify.mock.calls).toEqual([["thread", "completion"]]);
    expect(done.confirmations).toHaveLength(1);

    // A wake run takes over, and only its own completion notifies, at once.
    const woken = confirmingHarness();
    for (const next of [held, completed(1), working]) woken.observe(next);
    expect(woken.cancel).toHaveBeenCalledTimes(1);
    woken.observe(completed(2));
    expect(woken.notify.mock.calls).toEqual([["thread", "completion"]]);

    // A request while held, or a stale snapshot, does not skip the confirmation.
    const interrupted = confirmingHarness();
    for (const next of [held, approval, completed(1)]) interrupted.observe(next);
    interrupted.gate.suspend();
    expect(interrupted.cancel).toHaveBeenCalledTimes(1);
    interrupted.observe(completed(1));
    expect(interrupted.notify.mock.calls).toEqual([["thread", "input"]]);
    expect(interrupted.confirmations).toHaveLength(2);
  });

  it("forgets handled completions when a thread is removed or the gate is cleared", () => {
    for (const reset of [
      (gate: ReturnType<typeof createThreadNotificationGate<string>>) => gate.retain(new Set()),
      (gate: ReturnType<typeof createThreadNotificationGate<string>>) => gate.clear(),
    ]) {
      const { gate, notify, observe } = harness();
      observe(completed(1));
      reset(gate);
      gate.observe("thread", undefined, working, visible);
      gate.observe("thread", working, completed(1), visible);
      expect(notify.mock.calls).toEqual([["thread", "completion"]]);
    }
  });
});
