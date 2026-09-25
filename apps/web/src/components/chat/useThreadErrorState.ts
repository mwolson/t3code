import type { OrchestrationV2ProviderFailureClass } from "@t3tools/contracts";
import { useState } from "react";
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
    readonly lastErrorCode?: string | null;
  } | null;
}) {
  const { threadKey, draftId, isServerThread, runtime } = input;
  const [serverErrors, setServerErrors] = useState<Record<string, LocalErrorEntry>>({});
  const [draftErrors, setDraftErrors] = useState<Record<string, LocalErrorEntry>>({});
  const [dismissedKey, setDismissedKey] = useState<string | null>(null);
  const promotedError = isServerThread && draftId ? draftErrors[draftId] : undefined;
  if (promotedError !== undefined && draftId) {
    const remaining = { ...draftErrors };
    delete remaining[draftId];
    setDraftErrors(remaining);
    const current = serverErrors[threadKey];
    if (current === undefined || current.at <= promotedError.at) {
      setServerErrors({ ...serverErrors, [threadKey]: promotedError });
    }
  }

  const setError = (key: string, isServer: boolean, error: string | null) => {
    const message = sanitizeThreadErrorMessage(error);
    const at = Date.now();
    const update = isServer ? setServerErrors : setDraftErrors;
    update((existing) => {
      if (message === null && existing[key]?.message == null) return existing;
      return { ...existing, [key]: { message, at: Math.max(at, (existing[key]?.at ?? 0) + 1) } };
    });
  };
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
    setDismissedKey(resolved.key);
  };
  return {
    setError,
    dismiss,
    currentError: resolved.error,
    error: shouldShowThreadErrorBanner(
      threadKey,
      resolved.error,
      dismissedKey === resolved.key || isThreadErrorBannerDismissedForSession(resolved.key),
    )
      ? resolved.error
      : null,
    errorClass: resolved.key === runtimeKey ? (runtime?.lastErrorClass ?? null) : null,
    errorCode: resolved.key === runtimeKey ? (runtime?.lastErrorCode ?? null) : null,
  };
}
