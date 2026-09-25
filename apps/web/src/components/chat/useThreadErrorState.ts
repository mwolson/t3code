import type { OrchestrationV2ProviderFailureClass } from "@t3tools/contracts";
import { useCallback, useEffect, useState } from "react";
import { sanitizeThreadErrorMessage } from "~/rpc/transportError";
import {
  dismissThreadErrorBannerForSession,
  getThreadErrorBannerKey,
  isThreadErrorBannerDismissedForSession,
  resolveThreadErrorBanner,
  shouldShowThreadErrorBanner,
} from "./ThreadErrorBanner";

type LocalErrorEntry = { message: string | null; at: number };

export function useThreadErrorState(input: {
  threadKey: string;
  draftId: string | null;
  isServerThread: boolean;
  runtime: {
    readonly lastError: string | null;
    readonly lastErrorAt?: string | null;
    readonly lastErrorClass?: OrchestrationV2ProviderFailureClass | null;
  } | null;
}) {
  const { threadKey, draftId, isServerThread, runtime } = input;
  const [serverErrors, setServerErrors] = useState<Record<string, LocalErrorEntry>>({});
  const [draftErrors, setDraftErrors] = useState<Record<string, LocalErrorEntry>>({});
  const [, setDismissTick] = useState(0);
  useEffect(() => {
    if (!isServerThread || !draftId) return;
    const pending = draftErrors[draftId];
    if (pending === undefined) return;
    setDraftErrors((existing) => {
      const next = { ...existing };
      delete next[draftId];
      return next;
    });
    setServerErrors((existing) => {
      const current = existing[threadKey];
      if (current !== undefined && current.at > pending.at) return existing;
      return { ...existing, [threadKey]: pending };
    });
  }, [draftErrors, draftId, isServerThread, threadKey]);

  const setError = useCallback((key: string, isServer: boolean, error: string | null) => {
    const message = sanitizeThreadErrorMessage(error);
    const at = Date.now();
    const update = isServer ? setServerErrors : setDraftErrors;
    update((existing) => {
      if (message === null && existing[key]?.message == null) return existing;
      return { ...existing, [key]: { message, at: Math.max(at, (existing[key]?.at ?? 0) + 1) } };
    });
  }, []);
  const local = isServerThread ? serverErrors[threadKey] : draftErrors[draftId ?? ""];
  const resolved = resolveThreadErrorBanner({
    threadKey,
    localError: local?.message ?? null,
    localErrorAt: local?.at ?? null,
    runtime,
  });
  const runtimeKey = getThreadErrorBannerKey(
    threadKey,
    runtime?.lastError ?? null,
    runtime?.lastErrorAt ?? null,
  );
  const dismiss = () => {
    dismissThreadErrorBannerForSession(resolved.key, runtimeKey);
    setDismissTick((tick) => tick + 1);
  };
  return {
    setError,
    dismiss,
    currentError: resolved.error,
    error: shouldShowThreadErrorBanner(
      threadKey,
      resolved.error,
      isThreadErrorBannerDismissedForSession(resolved.key),
    )
      ? resolved.error
      : null,
    errorClass: resolved.key === runtimeKey ? (runtime?.lastErrorClass ?? null) : null,
  };
}
