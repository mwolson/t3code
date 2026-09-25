import type { OrchestrationV2ProviderFailureClass } from "@t3tools/contracts";
import { memo } from "react";
import { Alert, AlertAction, AlertDescription } from "../ui/alert";
import { Button } from "../ui/button";
import { CircleAlertIcon, XIcon } from "lucide-react";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";
import { OpenAI } from "../Icons";
import { ChatGptUsageButton } from "../settings/ChatGptUsageButton";

export function getThreadErrorBannerKey(
  threadKey: string,
  error: string | null,
  occurrence: string | number | null = null,
  source: "local" | "runtime" = "runtime",
): string | null {
  return error === null
    ? null
    : `${threadKey}\u0000${source}\u0000${occurrence ?? ""}\u0000${error}`;
}

export function resolveThreadErrorBanner(input: {
  threadKey: string;
  localError: string | null;
  localErrorAt: number | null;
  runtime: { readonly lastError: string | null; readonly lastErrorAt?: string | null } | null;
}) {
  const localKey = getThreadErrorBannerKey(
    input.threadKey,
    input.localError,
    input.localErrorAt,
    "local",
  );
  const runtimeKey = getThreadErrorBannerKey(
    input.threadKey,
    input.runtime?.lastError ?? null,
    input.runtime?.lastErrorAt ?? null,
  );
  const isLocal =
    input.localError !== null &&
    !(
      localKey !== null &&
      sessionDismissedThreadErrorBannerKeys.has(localKey) &&
      sessionDismissedThreadErrorBannerKeys.get(localKey) !== runtimeKey
    );
  const error = isLocal ? input.localError : (input.runtime?.lastError ?? null);
  return {
    error,
    key: getThreadErrorBannerKey(
      input.threadKey,
      error,
      isLocal ? input.localErrorAt : (input.runtime?.lastErrorAt ?? null),
      isLocal ? "local" : "runtime",
    ),
  };
}

export function shouldShowThreadErrorBanner(
  threadKey: string,
  error: string | null,
  isDismissed: boolean,
): boolean {
  return getThreadErrorBannerKey(threadKey, error) !== null && !isDismissed;
}

// Session-scoped (module-level so it survives ChatView remounts, e.g. route
// changes between threads). Mirrors the branch-mismatch banner: a dismissal
// is remembered per thread and visible error occurrence. Navigation cannot
// resurrect that occurrence, but a later failure with the same message appears.
const sessionDismissedThreadErrorBannerKeys = new Map<string, string | null>();

export function dismissThreadErrorBannerForSession(
  bannerKey: string | null,
  runtimeKey: string | null = null,
): void {
  if (bannerKey !== null) {
    sessionDismissedThreadErrorBannerKeys.set(bannerKey, runtimeKey);
  }
}

export function isThreadErrorBannerDismissedForSession(bannerKey: string | null): boolean {
  return bannerKey !== null && sessionDismissedThreadErrorBannerKeys.has(bannerKey);
}

export const ThreadErrorBanner = memo(function ThreadErrorBanner({
  error,
  onDismiss,
  errorClass,
  chatGptUsageLimit = false,
}: {
  error: string | null;
  errorClass?: OrchestrationV2ProviderFailureClass | null;
  onDismiss?: () => void;
  chatGptUsageLimit?: boolean;
}) {
  if (!error) return null;
  const variant = errorClass === "usage_limit" ? "warning" : "error";
  return (
    <div className="pointer-events-auto mx-auto w-fit max-w-[min(48rem,calc(100%-2rem))] pt-3">
      <Alert variant={variant} surface="glass" controlAlignment="first-line" data-variant={variant}>
        {chatGptUsageLimit ? (
          <OpenAI className="size-4 text-foreground!" aria-hidden="true" />
        ) : (
          <CircleAlertIcon />
        )}
        <AlertDescription>
          {chatGptUsageLimit ? (
            <div className="space-y-1">
              <p className="font-medium">ChatGPT usage limit reached</p>
              <p>Review your usage settings in ChatGPT to continue.</p>
            </div>
          ) : (
            <Tooltip>
              <TooltipTrigger render={<div className="line-clamp-3" />}>{error}</TooltipTrigger>
              <TooltipPopup side="top" className="whitespace-pre-wrap">
                {error}
              </TooltipPopup>
            </Tooltip>
          )}
        </AlertDescription>
        {(chatGptUsageLimit || onDismiss) && (
          <AlertAction>
            {chatGptUsageLimit ? <ChatGptUsageButton variant="default" size="sm" /> : null}
            {onDismiss ? (
              <Button variant="ghost" size="icon-xs" aria-label="Dismiss error" onClick={onDismiss}>
                <XIcon />
              </Button>
            ) : null}
          </AlertAction>
        )}
      </Alert>
    </div>
  );
});
