import { useClerk } from "@clerk/expo";
import { useSyncExternalStore } from "react";

type HeadlessReloadableClerk = ReturnType<typeof useClerk> & {
  readonly loadHeadlessClerk?: () => void;
};

const retryingClerks = new WeakSet<HeadlessReloadableClerk>();
const retrySubscribers = new Set<() => void>();

// ClerkProvider loads the native singleton once and never retries. @clerk/react's
// headless loader reruns load() and rewires hooks in place without a remount. Null
// until a load has failed and none is in flight: overlapping loads replay listeners
// twice, and a stale failure unloads Clerk. Also null if an upgrade drops the loader.
export function useClerkLoadRetry(): (() => void) | null {
  const clerk: HeadlessReloadableClerk = useClerk();
  const retrying = useSyncExternalStore(subscribeToRetries, () => retryingClerks.has(clerk));
  if (typeof clerk.loadHeadlessClerk !== "function") return null;
  if (clerk.status !== "error" || retrying) return null;
  return () => retryClerkLoad(clerk);
}

function retryClerkLoad(clerk: HeadlessReloadableClerk) {
  if (clerk.status !== "error" || retryingClerks.has(clerk)) return;
  const settle = (status: string) => {
    if (status === "loading") return;
    clerk.off("status", settle);
    retryingClerks.delete(clerk);
    notifyRetrySubscribers();
  };
  retryingClerks.add(clerk);
  clerk.on("status", settle);
  notifyRetrySubscribers();
  clerk.loadHeadlessClerk?.();
}

function subscribeToRetries(subscriber: () => void) {
  retrySubscribers.add(subscriber);
  return () => {
    retrySubscribers.delete(subscriber);
  };
}

function notifyRetrySubscribers() {
  for (const subscriber of retrySubscribers) subscriber();
}
