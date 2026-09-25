/**
 * OpenCodeDriver — `ProviderDriver` for the OpenCode runtime.
 *
 * OpenCode 1.x is retired. This driver is the built-in OpenCode identity and
 * attaches to the user's OpenCode 2 service over HTTP/SSE. T3 starts that
 * service only when it is not running, and never installs, upgrades, stops or
 * restarts it, so maintenance is manual-only.
 *
 * @module provider/Drivers/OpenCodeDriver
 */
import { OpenCode2Settings, ProviderDriverKind, type ServerProvider } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
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
import { makeUnavailableUsageLimits } from "../providerUsageLimits.ts";
import {
  checkOpenCodeProviderStatus,
  makePendingOpenCodeProvider,
  listOpenCodeCommandsForDirectory,
  listOpenCodeSkillsForDirectory,
} from "../Layers/OpenCodeProvider.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import { applyOpenCodeProviderEnvironment } from "../OpenCodeProviderEnvironment.ts";
import { OpenCode2Runtime } from "../opencode2Runtime.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import {
  enrichProviderSnapshotWithVersionAdvisory,
  makeManualOnlyProviderMaintenanceCapabilities,
} from "../providerMaintenance.ts";
import type { ServerProviderDraft } from "../providerSnapshot.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "../providerUpdateSettings.ts";

const decodeOpenCode2Settings = Schema.decodeSync(OpenCode2Settings);

const DRIVER_KIND = ProviderDriverKind.make("opencode");
// The runtime attaches only to an exact OpenCode release, so T3 neither offers
// an update action nor advises a newer package version.
const MAINTENANCE_CAPABILITIES = makeManualOnlyProviderMaintenanceCapabilities({
  provider: DRIVER_KIND,
  packageName: null,
});

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
  | OpenCode2Runtime
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
      const openCode2Runtime = yield* OpenCode2Runtime;
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
      const maintenanceCapabilities = MAINTENANCE_CAPABILITIES;
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
      ).pipe(
        Effect.flatMap((provider) =>
          DateTime.now.pipe(
            Effect.map((now) => ({
              ...provider,
              // OpenCode 2 exposes no account quota API. Never infer one from the
              // OpenCode 1 auth.json of whichever local account happens to exist.
              usageLimits: makeUnavailableUsageLimits({
                checkedAt: DateTime.formatIso(now),
                reason: "unsupported",
              }),
            })),
          ),
        ),
        Effect.map(stampIdentity),
        Effect.provideService(OpenCode2Runtime, openCode2Runtime),
      );

      const snapshotSettings = makeProviderSnapshotSettingsSource(effectiveConfig, serverSettings);
      const snapshot = yield* makeManagedServerProvider<
        ProviderSnapshotSettings<OpenCode2Settings>
      >({
        resolveMaintenance: () => Effect.succeed(maintenanceCapabilities),
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
        snapshotForCwd: (cwd) =>
          !effectiveConfig.enabled
            ? snapshot.getSnapshot
            : Effect.all(
                {
                  provider: snapshot.getSnapshot,
                  skills: listOpenCodeSkillsForDirectory(effectiveConfig, cwd, processEnv),
                  slashCommands: listOpenCodeCommandsForDirectory(effectiveConfig, cwd, processEnv),
                },
                { concurrency: "unbounded" },
              ).pipe(
                Effect.map(({ provider, skills, slashCommands }) => ({
                  ...provider,
                  skills,
                  slashCommands,
                })),
                Effect.provideService(OpenCode2Runtime, openCode2Runtime),
              ),
        orchestrationAdapter,
        textGeneration,
      } satisfies ProviderInstance;
    }),
};
