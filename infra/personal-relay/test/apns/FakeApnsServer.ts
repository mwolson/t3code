import * as NodeHttp2 from "node:http2";
import { generate } from "selfsigned";

export type FakeApnsMode =
  | "success"
  | "stall"
  | "body-stall"
  | "reset-after-send"
  | "close-before-end"
  | "503"
  | "goaway";

export interface FakeApnsRequest {
  readonly headers: NodeHttp2.IncomingHttpHeaders;
  readonly body: Buffer;
  readonly receivedAt: number;
}

export const APNS_ID = "aabbccdd-1111-2222-3333-001122334455";
const tls = generate([{ name: "commonName", value: "localhost" }], {
  algorithm: "sha256",
  keySize: 2048,
  days: 1,
  extensions: [
    { name: "basicConstraints", cA: false },
    { name: "keyUsage", digitalSignature: true, keyEncipherment: true },
    { name: "extKeyUsage", serverAuth: true },
    {
      name: "subjectAltName",
      altNames: [
        { type: 2, value: "localhost" },
        { type: 7, ip: "127.0.0.1" },
      ],
    },
  ],
});
export const testCa = tls.cert;

export async function startFakeApns(
  options: {
    readonly mode?: FakeApnsMode | ((request: FakeApnsRequest) => FakeApnsMode);
    readonly response?: {
      readonly status: number;
      readonly reason?: string;
      readonly body?: Uint8Array;
    };
    readonly onRequest?: (request: FakeApnsRequest, stream: NodeHttp2.ServerHttp2Stream) => void;
  } = {},
) {
  const requests: Array<FakeApnsRequest> = [];
  const sessions = new Set<NodeHttp2.ServerHttp2Session>();
  let connections = 0;
  const server = NodeHttp2.createSecureServer({
    key: tls.private,
    cert: testCa,
  });
  server.on("session", (session) => {
    connections++;
    sessions.add(session);
    session.on("error", () => {});
    session.on("close", () => sessions.delete(session));
  });
  server.on("stream", (stream, headers) => {
    stream.on("error", () => {});
    const chunks: Array<Buffer> = [];
    stream.on("data", (chunk: Buffer) => chunks.push(chunk));
    stream.on("end", () => {
      const request = {
        headers,
        body: Buffer.concat(chunks),
        receivedAt: performance.timeOrigin + performance.now(),
      };
      requests.push(request);
      if (options.onRequest !== undefined) {
        options.onRequest(request, stream);
        return;
      }
      const mode =
        typeof options.mode === "function" ? options.mode(request) : (options.mode ?? "success");
      if (mode === "stall") return;
      if (mode === "reset-after-send") {
        stream.close(NodeHttp2.constants.NGHTTP2_INTERNAL_ERROR);
        return;
      }
      const status = options.response?.status ?? (mode === "503" ? 503 : 200);
      stream.respond({ ":status": status, "apns-id": APNS_ID, "content-type": "application/json" });
      if (mode === "body-stall") {
        stream.write("{");
        return;
      }
      if (mode === "close-before-end") {
        stream.write("{", () => stream.session?.destroy());
        return;
      }
      const reason =
        options.response?.reason ?? (mode === "503" ? "ServiceUnavailable" : undefined);
      stream.end(
        options.response?.body ?? (reason === undefined ? "" : JSON.stringify({ reason })),
      );
      if (mode === "goaway") stream.session?.goaway();
    });
  });
  server.listen(0, "127.0.0.1");
  await new Promise<void>((resolve) => server.once("listening", resolve));
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("Missing fake APNs port");
  return {
    origin: `https://127.0.0.1:${address.port}`,
    requests,
    get connections() {
      return connections;
    },
    close: async () => {
      for (const session of sessions) session.destroy();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    },
  };
}
