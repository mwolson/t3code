import * as Effect from "effect/Effect";
import * as Headers from "effect/http/Headers";
import * as HttpServerRequest from "effect/http/HttpServerRequest";

export function parsePublicOrigin(value: string): string {
  const url = new URL(value);
  if (
    url.protocol !== "https:" ||
    url.username ||
    url.password ||
    url.pathname !== "/" ||
    url.search ||
    url.hash ||
    !/^https:\/\/[^\\\s/?#]+\/?$/i.test(value)
  ) {
    throw new Error("Personal relay public origin must be an HTTPS origin.");
  }
  return url.origin;
}

export const withPublicOrigin = (origin: string) => {
  const url = new URL(parsePublicOrigin(origin));
  return <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      return yield* effect.pipe(
        Effect.provideService(
          HttpServerRequest.HttpServerRequest,
          request.modify({
            headers: Headers.setAll(request.headers, {
              host: url.host,
              "x-forwarded-host": url.host,
              "x-forwarded-proto": "https",
            }),
          }),
        ),
      );
    });
};
