/**
 * OpenCodeDriver — `ProviderDriver` for the OpenCode runtime.
 *
 * OpenCode 1.x is retired. This driver is the built-in OpenCode identity and
 * runs the OpenCode 2 HTTP/SSE stack.
 *
 * @module provider/Drivers/OpenCodeDriver
 */
import { OpenCode2Settings, ProviderDriverKind, type ServerProvider } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { HttpClient } from "effect/unstable/http";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerConfig } from "../../config.ts";
import {
  OpenCodeAdapterV2Driver,
  type OpenCodeAdapterV2DriverEnv,
} from "../../orchestration-v2/Adapters/OpenCodeAdapterV2.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { makeOpenCodeTextGeneration } from "../../textGeneration/OpenCodeTextGeneration.ts";
import { ProviderDriverError } from "../Errors.ts";
import {
  checkOpenCodeProviderStatus,
  listOpenCodeSkillsForDirectory,
  makePendingOpenCodeProvider,
} from "../Layers/OpenCodeProvider.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import { applyOpenCodeProviderEnvironment } from "../OpenCodeProviderEnvironment.ts";
import { OpenCodeRuntime } from "../opencodeRuntime.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import {
  enrichProviderSnapshotWithVersionAdvisory,
  makeManualOnlyProviderMaintenanceCapabilities,
  makePackageManagedProviderMaintenanceResolver,
  makeStaticProviderMaintenanceResolver,
  resolveProviderMaintenanceCapabilitiesEffect,
  type ProviderMaintenanceCapabilitiesResolver,
} from "../providerMaintenance.ts";
import type { ServerProviderDraft } from "../providerSnapshot.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "../providerUpdateSettings.ts";

const decodeOpenCode2Settings = Schema.decodeSync(OpenCode2Settings);

const DRIVER_KIND = ProviderDriverKind.make("opencode");
const NPM_DIST_TAG = "next";
const NPM_PACKAGE_NAME = "@opencode-ai/cli";
const UPDATE = makePackageManagedProviderMaintenanceResolver({
  provider: DRIVER_KIND,
  npmPackageName: NPM_PACKAGE_NAME,
  npmDistTag: NPM_DIST_TAG,
  requiresInstallScripts: true,
  homebrewFormula: null,
  nativeUpdate: null,
});

function openCodeProviderMaintenanceResolver(
  settings: Pick<OpenCode2Settings, "serverUrl">,
): ProviderMaintenanceCapabilitiesResolver {
  return settings.serverUrl
    ? makeStaticProviderMaintenanceResolver(
        makeManualOnlyProviderMaintenanceCapabilities({
          provider: DRIVER_KIND,
          packageName: NPM_PACKAGE_NAME,
          npmDistTag: NPM_DIST_TAG,
        }),
      )
    : UPDATE;
}

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

export type OpenCodeDriverEnv =
  | OpenCodeAdapterV2DriverEnv
  | BackgroundPolicy.BackgroundPolicy
  | FileSystem.FileSystem
  | HttpClient.HttpClient
  | OpenCodeRuntime
  | Path.Path
  | ServerConfig
  | ServerSettingsService;

export const OpenCodeDriver: ProviderDriver<OpenCode2Settings, OpenCodeDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "OpenCode",
    supportsMultipleInstances: false,
  },
  configSchema: OpenCode2Settings,
  defaultConfig: (): OpenCode2Settings => decodeOpenCode2Settings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const openCode2Runtime = yield* OpenCodeRuntime;
      const serverConfig = yield* ServerConfig;
      const httpClient = yield* HttpClient.HttpClient;
      const serverSettings = yield* ServerSettingsService;
      const effectiveConfig = { ...config, enabled } satisfies OpenCode2Settings;
      const processEnv = yield* Effect.try({
        try: () =>
          applyOpenCodeProviderEnvironment(
            effectiveConfig,
            mergeProviderInstanceEnvironment(environment),
            instanceId,
            serverConfig.stateDir,
          ),
        catch: (cause) =>
          new ProviderDriverError({
            driver: DRIVER_KIND,
            instanceId,
            detail: "Failed to prepare OpenCode provider environment.",
            cause,
          }),
      });
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const maintenanceCapabilities = yield* resolveProviderMaintenanceCapabilitiesEffect(
        openCodeProviderMaintenanceResolver(effectiveConfig),
        {
          binaryPath: effectiveConfig.binaryPath,
          env: processEnv,
        },
      );
      const stampIdentity = withInstanceIdentity({
        instanceId,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });

      const orchestrationAdapter = yield* OpenCodeAdapterV2Driver.create({
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
              detail: "Failed to build OpenCode orchestration adapter.",
              cause,
            }),
        ),
      );
      const textGeneration = yield* makeOpenCodeTextGeneration(effectiveConfig, processEnv);

      const checkProvider = checkOpenCodeProviderStatus(
        effectiveConfig,
        serverConfig.cwd,
        processEnv,
      ).pipe(Effect.map(stampIdentity), Effect.provideService(OpenCodeRuntime, openCode2Runtime));

      const snapshotSettings = makeProviderSnapshotSettingsSource(effectiveConfig, serverSettings);
      const snapshot = yield* makeManagedServerProvider<
        ProviderSnapshotSettings<OpenCode2Settings>
      >({
        maintenanceCapabilities,
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (settings) =>
          makePendingOpenCodeProvider(settings.provider).pipe(Effect.map(stampIdentity)),
        checkProvider,
        enrichSnapshot: ({ settings, snapshot, publishSnapshot }) =>
          enrichProviderSnapshotWithVersionAdvisory(snapshot, maintenanceCapabilities, {
            enableProviderUpdateChecks: settings.enableProviderUpdateChecks,
          }).pipe(
            Effect.provideService(HttpClient.HttpClient, httpClient),
            Effect.flatMap((enrichedSnapshot) => publishSnapshot(enrichedSnapshot)),
          ),
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: "Failed to build OpenCode snapshot.",
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
        listSkills: (cwd) =>
          listOpenCodeSkillsForDirectory(effectiveConfig, cwd, processEnv).pipe(
            Effect.provideService(OpenCodeRuntime, openCode2Runtime),
          ),
      } satisfies ProviderInstance;
    }),
};
