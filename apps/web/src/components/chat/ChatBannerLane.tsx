import type {
  OrchestrationV2ProviderFailureClass,
  ProviderInstanceId,
  ServerProvider,
} from "@t3tools/contracts";
import type { Dispatch, SetStateAction } from "react";
import { ProviderStatusBanner } from "./ProviderStatusBanner";
import { ThreadErrorBanner } from "./ThreadErrorBanner";

/**
 * The banners pinned above the timeline. The dismiss handlers are built here from plain
 * values, so a ChatView render that changes neither banner leaves both cached.
 */
export function ChatBannerLane(props: {
  providerStatus: ServerProvider | null;
  providerStatusBannerKey: string | null;
  setDismissedProviderStatusBannerKey: Dispatch<SetStateAction<string | null>>;
  onOpenProviderSetup: (instanceId: ProviderInstanceId) => void;
  threadError: string | null;
  threadErrorClass: OrchestrationV2ProviderFailureClass | null;
  threadErrorCode: string | null;
  onDismissThreadError: () => void;
}) {
  const {
    providerStatus,
    providerStatusBannerKey,
    setDismissedProviderStatusBannerKey,
    onOpenProviderSetup,
    threadError,
    threadErrorClass,
    threadErrorCode,
    onDismissThreadError,
  } = props;
  return (
    <div className="chat-banner-lane pointer-events-none absolute top-0 z-30 flex flex-col">
      <ProviderStatusBanner
        status={providerStatus}
        onDismiss={() => setDismissedProviderStatusBannerKey(providerStatusBannerKey)}
        onOpenProviderSetup={onOpenProviderSetup}
      />
      <ThreadErrorBanner
        error={threadError}
        errorClass={threadErrorClass}
        errorCode={threadErrorCode}
        onDismiss={onDismissThreadError}
      />
    </div>
  );
}
