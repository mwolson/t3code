/**
 * OpenCode2Driver — `ProviderDriver` for the OpenCode 2.x runtime.
 *
 * Separate from `OpenCodeDriver` rather than a binary-path variant of it: 2.x
 * serves a different route surface, mints a mandatory server password, and
 * emits a different event vocabulary, so the two share no runtime, no adapter,
 * and no probe. See `OpenCode2AdapterV2` for the protocol contract.
 *
 * The driver ships with `hasDefaultInstance: false` (see
 * `apps/web/src/components/settings/providerDriverMeta.ts`), so there is no
 * built-in instance and no `providers.opencode2`-backed default: an instance
 * exists only when the user adds one.
 *
 * @module provider/Drivers/OpenCode2Driver
 */
import { OpenCode2Settings, ProviderDriverKind, type ServerProvider } from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { ServerConfig } from "../../config.ts";
import {
  OpenCode2AdapterV2Driver,
  type OpenCode2AdapterV2DriverEnv,
} from "../../orchestration-v2/Adapters/OpenCode2AdapterV2.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { makeOpenCode2TextGeneration } from "../../textGeneration/OpenCode2TextGeneration.ts";
import { ProviderDriverError } from "../Errors.ts";
import {
  checkOpenCode2ProviderStatus,
  makePendingOpenCode2Provider,
} from "../Layers/OpenCode2Provider.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import { OpenCode2Runtime } from "../opencode2Runtime.ts";
import { OpenCodeRuntime } from "../opencodeRuntime.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import { makeManualOnlyProviderMaintenanceCapabilities } from "../providerMaintenance.ts";
import type { ServerProviderDraft } from "../providerSnapshot.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "../providerUpdateSettings.ts";

const decodeOpenCode2Settings = Schema.decodeSync(OpenCode2Settings);

const DRIVER_KIND = ProviderDriverKind.make("opencode2");
const SNAPSHOT_REFRESH_INTERVAL = Duration.minutes(5);

const withInstanceIdentity =
  (input: {
    readonly instanceId: ProviderInstance["instanceId"];
    readonly displayName: string | undefined;
    readonly accentColor: string | undefined;
    readonly continuationGroupKey: string;
  }) =>
  (snapshot: ServerProviderDraft): ServerProvider => ({
    ...snapshot,
    instanceId: input.instanceId,
    driver: DRIVER_KIND,
    ...(input.displayName ? { displayName: input.displayName } : {}),
    ...(input.accentColor ? { accentColor: input.accentColor } : {}),
    continuation: { groupKey: input.continuationGroupKey },
  });

export type OpenCode2DriverEnv =
  | OpenCode2AdapterV2DriverEnv
  | OpenCode2Runtime
  | OpenCodeRuntime
  | ServerConfig
  | ServerSettingsService;

export const OpenCode2Driver: ProviderDriver<OpenCode2Settings, OpenCode2DriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "OpenCode 2",
    supportsMultipleInstances: true,
  },
  configSchema: OpenCode2Settings,
  defaultConfig: (): OpenCode2Settings => decodeOpenCode2Settings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const openCode2Runtime = yield* OpenCode2Runtime;
      const openCodeRuntime = yield* OpenCodeRuntime;
      const serverConfig = yield* ServerConfig;
      const serverSettings = yield* ServerSettingsService;
      const processEnv = mergeProviderInstanceEnvironment(environment);
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const effectiveConfig = { ...config, enabled } satisfies OpenCode2Settings;

      const orchestrationAdapter = yield* OpenCode2AdapterV2Driver.create({
        instanceId,
        displayName,
        accentColor,
        environment,
        enabled,
        config,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: "Failed to build OpenCode 2 orchestration adapter.",
              cause,
            }),
        ),
      );
      const textGeneration = yield* makeOpenCode2TextGeneration(effectiveConfig, processEnv);

      const checkProvider = checkOpenCode2ProviderStatus(
        effectiveConfig,
        serverConfig.cwd,
        processEnv,
      ).pipe(
        Effect.map(stampIdentity),
        Effect.provideService(OpenCode2Runtime, openCode2Runtime),
        Effect.provideService(OpenCodeRuntime, openCodeRuntime),
      );

      const snapshotSettings = makeProviderSnapshotSettingsSource(effectiveConfig, serverSettings);
      const snapshot = yield* makeManagedServerProvider<
        ProviderSnapshotSettings<OpenCode2Settings>
      >({
        // `@opencode-ai/cli` ships a placeholder binary that a postinstall
        // script replaces, so an automated in-app update would reproduce the
        // broken-shim failure this driver's probe reports on. Manual only.
        maintenanceCapabilities: makeManualOnlyProviderMaintenanceCapabilities({
          provider: DRIVER_KIND,
          packageName: "@opencode-ai/cli",
        }),
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (settings) =>
          makePendingOpenCode2Provider(settings.provider).pipe(Effect.map(stampIdentity)),
        checkProvider,
        refreshInterval: SNAPSHOT_REFRESH_INTERVAL,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: `Failed to build OpenCode 2 snapshot: ${cause.message ?? String(cause)}`,
              cause,
            }),
        ),
      );

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        orchestrationAdapter,
        textGeneration,
      } satisfies ProviderInstance;
    }),
};
