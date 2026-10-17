import * as NodeStream from "@effect/platform-node/NodeStream";
import * as Config from "effect/Config";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { Command, Flag, GlobalFlag } from "effect/cli";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerConfig from "../config.ts";
import * as BackgroundRelaySettings from "../relay/BackgroundRelaySettings.ts";
import * as EnrollmentProof from "../relay/EnrollmentProof.ts";
import { projectLocationFlags, resolveCliAuthConfig } from "./config.ts";

const encodeStatus = Schema.encodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      url: Schema.NullOr(Schema.String),
      issuer: Schema.NullOr(Schema.String),
      revision: Schema.NullOr(Schema.String),
      state: Schema.String,
    }),
  ),
);

const runSettings = Effect.fn("backgroundRelay.cli")(function* <A, E>(
  flags: { readonly baseDir: Option.Option<string> },
  effect: Effect.Effect<A, E, BackgroundRelaySettings.BackgroundRelaySettings>,
) {
  const config = yield* resolveCliAuthConfig(flags, yield* GlobalFlag.LogLevel);
  return yield* effect.pipe(
    Effect.provide(
      BackgroundRelaySettings.layerSaved.pipe(
        Layer.provide(ServerSecretStore.layer),
        Layer.provide(ServerConfig.layer(config)),
        Layer.provide(Layer.succeed(References.MinimumLogLevel, "Error")),
      ),
    ),
    Effect.provideService(References.MinimumLogLevel, "Error"),
  );
});

const set = Command.make("set", {
  ...projectLocationFlags,
  url: Flag.String("url"),
  issuer: Flag.String("issuer").pipe(Flag.optional),
  credentialStdin: Flag.Boolean("credential-stdin").pipe(Flag.withDefault(false)),
}).pipe(
  Command.withDescription("Save a background relay using an environment credential from stdin."),
  Command.withHandler((flags) =>
    runSettings(
      flags,
      Effect.gen(function* () {
        if (!flags.credentialStdin || process.stdin.isTTY) {
          return yield* new BackgroundRelaySettings.BackgroundRelaySettingsInvalid({
            reason: "stdin-required",
          });
        }
        const credential = yield* NodeStream.fromReadable<
          Uint8Array,
          BackgroundRelaySettings.BackgroundRelaySettingsInvalid
        >({
          evaluate: () => process.stdin,
          onError: () =>
            new BackgroundRelaySettings.BackgroundRelaySettingsInvalid({ reason: "stdin-read" }),
        }).pipe(
          Stream.decodeText(),
          Stream.runFoldEffect(
            () => "",
            (all, chunk) => {
              if (all.length + chunk.length > 1024)
                return Effect.fail(
                  new BackgroundRelaySettings.BackgroundRelaySettingsInvalid({
                    reason: "input-too-large",
                  }),
                );
              return Effect.succeed(all + chunk);
            },
          ),
        );
        const settings = yield* BackgroundRelaySettings.BackgroundRelaySettings;
        yield* settings.set({
          url: flags.url,
          issuer: Option.getOrUndefined(flags.issuer),
          environmentCredential: Redacted.make(credential),
        });
        const overrides = yield* Config.all([
          Config.String("T3CODE_BACKGROUND_RELAY_URL").pipe(Config.option),
          Config.String("T3CODE_BACKGROUND_RELAY_ISSUER").pipe(Config.option),
          Config.String("T3CODE_BACKGROUND_RELAY_ENVIRONMENT_CREDENTIAL").pipe(Config.option),
        ]);
        if (overrides.some(Option.isSome)) {
          yield* Console.error(
            "Runtime background relay environment overrides take precedence over saved configuration. The server may have different overrides.",
          );
        }
        yield* Console.log("Background relay configuration saved.");
      }),
    ),
  ),
);

const status = Command.make("status", {
  ...projectLocationFlags,
  json: Flag.Boolean("json").pipe(Flag.withDefault(false)),
}).pipe(
  Command.withDescription("Show background relay configuration without its credential."),
  Command.withHandler((flags) =>
    runSettings(
      flags,
      Effect.gen(function* () {
        const settings = yield* BackgroundRelaySettings.BackgroundRelaySettings;
        const config = yield* settings.read;
        const status = {
          url: config?.url ?? null,
          issuer: config?.issuer ?? null,
          revision: config?.revision.slice(0, 12) ?? null,
          state:
            config === undefined ? "unconfigured" : config === null ? "disabled" : "configured",
        };
        yield* Console.log(
          flags.json
            ? yield* encodeStatus(status)
            : [
                "Background relay saved configuration",
                `  State: ${status.state}`,
                `  URL: ${status.url ?? "none"}`,
                `  Issuer: ${status.issuer ?? "none"}`,
                `  Revision: ${status.revision ?? "none"}`,
              ].join("\n"),
        );
      }),
    ),
  ),
);

const clear = Command.make("clear", projectLocationFlags).pipe(
  Command.withDescription("Remove the saved background relay configuration."),
  Command.withHandler((flags) =>
    runSettings(
      flags,
      Effect.gen(function* () {
        const settings = yield* BackgroundRelaySettings.BackgroundRelaySettings;
        yield* settings.clear;
        yield* Console.log("Background relay configuration cleared.");
      }),
    ),
  ),
);

const encodeProof = Schema.encodeEffect(
  Schema.fromJsonString(Schema.Struct({ proof: Schema.String })),
);

const enrollmentProof = Command.make("enrollment-proof", {
  ...projectLocationFlags,
  relayUrl: Flag.String("relay-url"),
  label: Flag.String("label").pipe(Flag.optional),
}).pipe(
  Command.withDescription("Create a ten-minute proof for private background relay enrollment."),
  Command.withHandler((flags) =>
    Effect.gen(function* () {
      yield* EnrollmentProof.validate({
        relayUrl: flags.relayUrl,
        label: Option.getOrUndefined(flags.label),
      });
      const config = yield* resolveCliAuthConfig(flags, yield* GlobalFlag.LogLevel, {
        readOnly: true,
      });
      const result = yield* EnrollmentProof.make({
        relayUrl: flags.relayUrl,
        label: Option.getOrUndefined(flags.label),
      }).pipe(
        Effect.provide(
          Layer.mergeAll(EnrollmentProof.layerIdentity, ServerSecretStore.layerReadOnly).pipe(
            Layer.provide(ServerConfig.layer(config)),
            Layer.provide(Layer.succeed(References.MinimumLogLevel, "Error")),
          ),
        ),
      );
      yield* Console.log(yield* encodeProof(result));
    }).pipe(Effect.provideService(References.MinimumLogLevel, "Error")),
  ),
);

export const backgroundRelayCommand = Command.make("background-relay").pipe(
  Command.withDescription("Configure a separate relay for background agent activity."),
  Command.withSubcommands([set, status, clear, enrollmentProof]),
);
