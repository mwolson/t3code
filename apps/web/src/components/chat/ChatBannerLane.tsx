import type {
  EnvironmentId,
  OrchestrationV2ProviderFailureClass,
  ProviderInstanceId,
  ServerProvider,
} from "@t3tools/contracts";
import {
  dismissProviderStatusBannerForSession,
  ProviderStatusBanner,
} from "./ProviderStatusBanner";
import { ThreadErrorBanner } from "./ThreadErrorBanner";

/**
 * The banners pinned above the timeline. The dismiss handlers are built here from plain
 * values, so a ChatView render that changes neither banner leaves both cached.
 */
export function ChatBannerLane(props: {
  environmentId: EnvironmentId;
  providerStatus: ServerProvider | null;
  providerStatusDismissalKey: string | null;
  onOpenProviderSetup: (instanceId: ProviderInstanceId) => void;
  threadError: string | null;
  threadErrorClass: OrchestrationV2ProviderFailureClass | null;
  threadErrorCode: string | null;
  onDismissThreadError: () => void;
}) {
  const {
    providerStatus,
    providerStatusDismissalKey,
    onOpenProviderSetup,
    threadError,
    threadErrorClass,
    threadErrorCode,
    onDismissThreadError,
  } = props;
  return (
    <div className="chat-banner-lane pointer-events-none absolute top-0 z-30 flex flex-col">
      <ProviderStatusBanner
        environmentId={props.environmentId}
        status={providerStatus}
        onDismiss={() => dismissProviderStatusBannerForSession(providerStatusDismissalKey)}
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
