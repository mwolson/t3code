// @effect-diagnostics-next-line nodeBuiltinImport:off
import * as NodeHttp from "node:http";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as Effect from "effect/Effect";
import * as ByteSize from "effect/ByteSize";
import * as Etag from "effect/http/Etag";
import * as HttpPlatform from "effect/http/HttpPlatform";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpIncomingMessage from "effect/http/HttpIncomingMessage";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as Logger from "effect/Logger";
import * as References from "effect/References";
import * as Layer from "effect/Layer";
import { PersonalRelayConfig } from "../Config.ts";
import * as Middleware from "../auth/Middleware.ts";
import { internalResponse } from "./Errors.ts";
import * as Handlers from "./Handlers.ts";
import { PersonalRelayApi } from "./PersonalRelayApi.ts";
import { withPublicOrigin } from "./PublicOrigin.ts";

const publicOrigin = HttpRouter.middleware(
  Effect.gen(function* () {
    const config = yield* PersonalRelayConfig;
    return (effect) =>
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const length = Number(request.headers["content-length"]);
        if (Number.isFinite(length) && length > 65_536)
          return HttpServerResponse.empty({ status: 413 });
        return yield* effect;
      }).pipe(
        Effect.provideService(HttpIncomingMessage.MaxBodySize, ByteSize.kibibytes(64)),
        withPublicOrigin(config.publicOrigin),
        Effect.timeout("5 seconds"),
        Effect.catchTags({
          TimeoutError: () => Effect.succeed(internalResponse("internal_error")),
        }),
      );
  }),
  { global: true },
);

const livez = HttpRouter.add("GET", "/livez", HttpServerResponse.jsonUnsafe({ ok: true }));
export const app = Layer.mergeAll(
  HttpApiBuilder.layer(PersonalRelayApi).pipe(
    Layer.provide(Handlers.layer),
    Layer.provide(Middleware.layer),
  ),
  livez,
).pipe(Layer.provide(publicOrigin));

// The caller acquires worker leadership and verifies migrations before running this effect.
export const serve = Effect.gen(function* () {
  const config = yield* PersonalRelayConfig;
  const listener = HttpRouter.serve(app, { disableLogger: true }).pipe(
    Layer.provide(
      NodeHttpServer.layer(() => NodeHttp.createServer(), {
        port: config.port,
        host: "0.0.0.0",
        gracefulShutdownTimeout: "25 seconds",
      }),
    ),
  );
  return yield* Layer.launch(listener).pipe(
    Effect.provideService(References.MinimumLogLevel, config.logLevel),
    Effect.provide(Logger.layer([Logger.consoleJson])),
  );
});

export const webServices = Layer.mergeAll(HttpPlatform.layer, Etag.layerWeak);
