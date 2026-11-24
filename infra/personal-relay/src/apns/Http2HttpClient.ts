import * as NodeHttp2 from "node:http2";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientError from "effect/http/HttpClientError";
import * as HttpClientResponse from "effect/http/HttpClientResponse";

export class Http2TransportError extends Schema.TaggedError<Http2TransportError>()(
  "Http2TransportError",
  { sent: Schema.Boolean, cause: Schema.Defect() },
) {
  override get message(): string {
    return "APNs HTTP/2 transport failed.";
  }
}

interface Options {
  readonly originOverride?: string;
  readonly ca?: string;
}

export const make = Effect.fn("Http2HttpClient.make")(function* (options: Options = {}) {
  const sessions = new Map<string, NodeHttp2.ClientHttp2Session>();
  const allSessions = new Set<NodeHttp2.ClientHttp2Session>();
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      for (const session of allSessions) session.destroy();
      sessions.clear();
    }),
  );
  return HttpClient.make((request, url, signal) =>
    Effect.callback<HttpClientResponse.HttpClientResponse, HttpClientError.HttpClientError>(
      (resume) => {
        let sent = false;
        let responded = false;
        let ended = false;
        let failed = false;
        let stream: NodeHttp2.ClientHttp2Stream | undefined;
        let bodyController: ReadableStreamDefaultController<Uint8Array> | undefined;
        const fail = (cause: unknown) => {
          if (failed || ended) return;
          failed = true;
          const transport = new Http2TransportError({ sent, cause });
          if (responded) bodyController?.error(transport);
          else {
            resume(
              Effect.fail(
                new HttpClientError.HttpClientError({
                  reason: new HttpClientError.TransportError({ request, cause: transport }),
                }),
              ),
            );
          }
          signal.removeEventListener("abort", abort);
        };
        const abort = () => {
          fail("Request interrupted");
          stream?.close(NodeHttp2.constants.NGHTTP2_CANCEL);
        };
        signal.addEventListener("abort", abort, { once: true });
        if (signal.aborted) {
          abort();
          return;
        }
        try {
          const origin = options.originOverride ?? url.origin;
          let session = sessions.get(origin);
          if (session === undefined || session.closed || session.destroyed) {
            session = NodeHttp2.connect(origin, options.ca ? { ca: options.ca } : {});
            const current = session;
            sessions.set(origin, current);
            allSessions.add(current);
            const discard = () => {
              if (sessions.get(origin) === current) sessions.delete(origin);
            };
            current.on("goaway", () => {
              discard();
              current.close();
            });
            current.on("error", discard);
            current.on("close", () => {
              discard();
              allSessions.delete(current);
            });
            current.unref();
          }
          const current = session;
          const onSessionError = (cause: unknown) => fail(cause);
          current.on("error", onSessionError);
          const onSessionClose = () => fail("Session closed before response completed");
          current.on("close", onSessionClose);
          const cleanup = () => {
            signal.removeEventListener("abort", abort);
            current.removeListener("error", onSessionError);
            current.removeListener("close", onSessionClose);
            current.removeListener("connect", send);
          };
          const send = () => {
            if (failed) return;
            try {
              const headers: NodeHttp2.OutgoingHttpHeaders = {
                ...request.headers,
                ":method": request.method,
                ":path": `${url.pathname}${url.search}`,
              };
              delete headers.host;
              stream = current.request(headers);
              stream.once("finish", () => {
                sent = true;
              });
              stream.on("error", fail);
              stream.on("aborted", () => fail("Response aborted"));
              stream.on("close", () => {
                if (!ended) fail("Stream closed before response ended");
                cleanup();
              });
              stream.on("response", (received) => {
                if (failed) return;
                try {
                  const status = Number(received[":status"]);
                  const responseHeaders = new Headers();
                  for (const [name, value] of Object.entries(received)) {
                    if (name.startsWith(":") || value === undefined) continue;
                    if (Array.isArray(value)) {
                      for (const item of value) responseHeaders.append(name, item);
                    } else responseHeaders.set(name, String(value));
                  }
                  const body = new ReadableStream<Uint8Array>({
                    start(controller) {
                      bodyController = controller;
                    },
                    cancel() {
                      abort();
                    },
                  });
                  const response = new Response(body, { status, headers: responseHeaders });
                  responded = true;
                  resume(Effect.succeed(HttpClientResponse.fromWeb(request, response)));
                } catch (cause) {
                  fail(cause);
                  stream?.close(NodeHttp2.constants.NGHTTP2_CANCEL);
                }
              });
              stream.on("data", (chunk: Buffer) => {
                if (!failed) bodyController?.enqueue(new Uint8Array(chunk));
              });
              stream.on("end", () => {
                if (failed) return;
                if (stream?.rstCode !== NodeHttp2.constants.NGHTTP2_NO_ERROR) {
                  fail("Response ended with a stream reset");
                  return;
                }
                if (!responded) {
                  fail("Response ended without headers");
                  return;
                }
                ended = true;
                bodyController?.close();
                cleanup();
              });
              switch (request.body._tag) {
                case "Empty":
                  stream.end();
                  break;
                case "Uint8Array":
                  stream.end(request.body.body);
                  break;
                case "Raw": {
                  const body: unknown = request.body.body;
                  if (typeof body !== "string" && !(body instanceof Uint8Array)) {
                    fail("Unsupported HTTP/2 request body");
                    stream.close(NodeHttp2.constants.NGHTTP2_CANCEL);
                  } else stream.end(body);
                  break;
                }
                default:
                  fail("Unsupported HTTP/2 request body");
                  stream.close(NodeHttp2.constants.NGHTTP2_CANCEL);
              }
            } catch (cause) {
              fail(cause);
              cleanup();
            }
          };
          if (current.connecting) current.once("connect", send);
          else send();
          return Effect.sync(() => {
            if (!responded) {
              abort();
              cleanup();
            }
          });
        } catch (cause) {
          fail(cause);
        }
      },
    ),
  );
});

export const layer = (options: Options = {}) => HttpClient.layerMergedContext(make(options));
