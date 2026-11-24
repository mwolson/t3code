import { DpopProof, ManagedRelay } from "@t3tools/client-runtime/relay";
import { RelayMobileClientId } from "@t3tools/contracts/relay";
import * as Layer from "effect/Layer";

import { loadOrCreateProofKey } from "./dpop";
import type { PersonalBackgroundRoute } from "./personalBackgroundRoute";
import { managedRelayAccessTokenStore } from "./managedRelayTokenStore";

const layerRelayDpopSigner = DpopProof.layerSigner({
  keyStore: "expo-secure-store",
  loadOrCreateKey: loadOrCreateProofKey(),
});

export const layer = (options: {
  readonly relayUrl: string;
  readonly background:
    | PersonalBackgroundRoute
    | { readonly kind: "default"; readonly url: string | null };
}) =>
  ManagedRelay.layer({
    relayUrl: options.relayUrl,
    backgroundRelayDisabled: options.background.kind === "disabled",
    ...(options.background.kind !== "disabled" && options.background.url
      ? { backgroundRelayUrl: options.background.url }
      : {}),
    clientId: RelayMobileClientId,
    accessTokenStore: managedRelayAccessTokenStore,
  }).pipe(Layer.provideMerge(layerRelayDpopSigner));
