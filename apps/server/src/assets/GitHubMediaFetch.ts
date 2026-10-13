import * as Mime from "effect/http/Mime";
import { githubMediaFileName } from "@t3tools/shared/githubMedia";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import {
  FetchHttpClient,
  HttpClient,
  HttpClientRequest,
  HttpServerResponse,
  type HttpClientResponse,
} from "effect/http";

import * as GitHubRest from "@t3tools/source-control-github/server/GitHubRest";
import * as GitHubCredentials from "@t3tools/source-control-github/server/GitHubCredentials";

import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";

/**
 * Everything a redirect leads to — the presigned object stores GitHub hands assets off to —
 * authorizes with its own signature, and some of them reject a request that also carries a
 * bearer token.
 */
const CREDENTIALED_HOSTS = new Set([
  "github.com",
  "www.github.com",
  "raw.githubusercontent.com",
  "media.githubusercontent.com",
]);
const isCredentialedHost = (url: string) => {
  try {
    return CREDENTIALED_HOSTS.has(new URL(url).hostname.toLowerCase());
  } catch {
    return false;
  }
};

const MAX_REDIRECTS = 3;
const MANUAL_REDIRECT: RequestInit = { redirect: "manual" };
const FORWARDED_REQUEST_HEADERS = ["range", "if-range"] as const;
const FORWARDED_RESPONSE_HEADERS = [
  "content-type",
  "content-length",
  "content-range",
  "accept-ranges",
  "etag",
  "last-modified",
] as const;
const MEDIA_CONTENT_TYPE_PATTERN = /^(?:image|video|audio)\/[\w!#$&^.+-]+$/i;
const SVG_CONTENT_TYPE = "image/svg+xml";
// An SVG is a document: same policy the asset route gives a workspace SVG, so one embedded in a
// body cannot run script against this origin.
const SVG_CONTENT_SECURITY_POLICY = "default-src 'none'; style-src 'unsafe-inline'; sandbox";

const githubToken = GitHubCredentials.GitHubCredentials.pipe(
  Effect.flatMap((credentials) => credentials.get("github.com")),
  Effect.map((credential) => credential.token),
  Effect.orElseSucceed(() => null),
);

/**
 * An upload in a body: `user-attachments`, or the older form that names its repository.
 * github.com serves these to a browser session, so an organization that enforces SAML SSO answers
 * an API token with its sign-in page instead of the bytes.
 */
const ATTACHMENT_URL_PATTERN =
  /^https:\/\/github\.com\/(?:user-attachments\/assets\/|([^/]+\/[^/]+)\/assets\/\d+\/)/u;
const SIGNED_ATTACHMENT_URL_PATTERN =
  /src="(https:\/\/private-user-images\.githubusercontent\.com\/[^"]+)"/u;
/** GitHub signs these for five minutes; reuse one for less so a video seek never gets a stale one. */
const SIGNED_URL_CACHE_TTL_MS = 4 * 60_000;
const SIGNED_URL_CACHE_MAX_ENTRIES = 64;
const signedUrlCache = new Map<string, { readonly at: number; readonly url: string }>();

const cachedSignedUrl = (url: string, now: number) => {
  const cached = signedUrlCache.get(url);
  return cached !== undefined && now - cached.at < SIGNED_URL_CACHE_TTL_MS ? cached.url : null;
};

/**
 * The signed `private-user-images` URL GitHub's own pages carry for an upload, or null. The API
 * honours an SSO-authorized token where github.com does not, but only signs uploads in markdown
 * rendered with a repository as context. Any repository the token can read works, so the
 * checkout's own is enough even when the pull request lives on a fork's parent.
 */
const signedAttachmentUrl = Effect.fn("GitHubMediaFetch.signedAttachmentUrl")(function* (
  url: string,
  cwd: string,
  now: number,
) {
  const match = ATTACHMENT_URL_PATTERN.exec(url);
  if (match === null) return null;
  const identity = match[1]
    ? null
    : yield* (yield* RepositoryIdentityResolver.RepositoryIdentityResolver).resolve(cwd);
  const repository =
    match[1] ?? /^github\.com\/([^/]+\/[^/]+)$/u.exec(identity?.canonicalKey ?? "")?.[1];
  if (repository === undefined) return null;
  const api = yield* GitHubRest.GitHubRest;
  const html = yield* api
    .rest({
      host: "github.com",
      operation: "renderMarkdown",
      method: "POST",
      path: "markdown",
      accept: "text/html",
      body: { text: `![](${url})`, mode: "gfm", context: repository },
    })
    .pipe(
      Effect.map((response) => response.body),
      Effect.orElseSucceed(() => ""),
    );
  const signed = SIGNED_ATTACHMENT_URL_PATTERN.exec(html)?.[1]?.replaceAll("&amp;", "&");
  if (signed === undefined) return null;
  if (signedUrlCache.size >= SIGNED_URL_CACHE_MAX_ENTRIES) {
    signedUrlCache.delete(signedUrlCache.keys().next().value!);
  }
  signedUrlCache.set(url, { at: now, url: signed });
  return signed;
});

const fetchFollowingRedirects = Effect.fn("GitHubMediaFetch.fetchFollowingRedirects")(function* (
  url: string,
  headers: Record<string, string>,
  token: Redacted.Redacted | null,
) {
  const httpClient = HttpClient.withScope(yield* HttpClient.HttpClient);
  let target = url;
  for (let hop = 0; ; hop += 1) {
    const authorization =
      token !== null && isCredentialedHost(target) ? `Bearer ${Redacted.value(token)}` : null;
    const response: HttpClientResponse.HttpClientResponse = yield* httpClient
      .execute(
        HttpClientRequest.get(target).pipe(
          HttpClientRequest.setHeaders({
            ...headers,
            // The bytes are streamed straight through, so never let an encoding layer in.
            "accept-encoding": "identity",
            ...(authorization === null ? {} : { authorization }),
          }),
        ),
      )
      .pipe(Effect.provideService(FetchHttpClient.RequestInit, MANUAL_REDIRECT));
    const location = response.headers.location;
    if (response.status < 300 || response.status >= 400) return response;
    if (!location || hop >= MAX_REDIRECTS) return null;
    const next = new URL(location, target);
    if (next.protocol !== "https:") return null;
    target = next.toString();
  }
});

export const githubMediaResponse = Effect.fn("GitHubMediaFetch.githubMediaResponse")(function* (
  asset: { readonly url: string; readonly cwd: string; readonly expiresAt: number },
  requestHeaders: Record<string, string | undefined>,
) {
  // Both media hosts are served by github.com's account, which is the host `gh` stores it under.
  const token = yield* githubToken;
  const forwarded: Record<string, string> = {};
  for (const name of FORWARDED_REQUEST_HEADERS) {
    const value = requestHeaders[name];
    if (value !== undefined) forwarded[name] = value;
  }
  // A signed URL authorizes on its own, so it is fetched like any redirect target: no token.
  const now = yield* Clock.currentTimeMillis;
  const cached = cachedSignedUrl(asset.url, now);
  let response = yield* fetchFollowingRedirects(cached ?? asset.url, forwarded, token);
  if (cached === null && response?.headers["content-type"]?.startsWith("text/html")) {
    const signed = yield* signedAttachmentUrl(asset.url, asset.cwd, now);
    if (signed !== null) response = yield* fetchFollowingRedirects(signed, forwarded, token);
  }
  const remainingSeconds = Math.floor((asset.expiresAt - (yield* Clock.currentTimeMillis)) / 1000);
  const headers: Record<string, string> = {
    "cache-control":
      remainingSeconds > 0 ? `private, max-age=${remainingSeconds}` : "private, no-store",
    "x-content-type-options": "nosniff",
  };
  if (response === null) return HttpServerResponse.empty({ status: 502, headers });
  // It carries none of the upstream entity headers: a `content-length` with no body behind it
  // holds the connection open until the browser gives up on it.
  if (response.status >= 400) {
    return HttpServerResponse.empty({
      status: response.status >= 500 ? 502 : response.status,
      headers,
    });
  }
  const upstreamType =
    response.headers["content-type"]?.split(";", 1)[0]?.trim().toLowerCase() ?? "";
  const contentType = MEDIA_CONTENT_TYPE_PATTERN.test(upstreamType)
    ? upstreamType
    : Option.getOrElse(Mime.getType(githubMediaFileName(asset.url)), () => "").toLowerCase();
  if (!MEDIA_CONTENT_TYPE_PATTERN.test(contentType)) {
    return HttpServerResponse.empty({ status: 415, headers });
  }
  for (const name of FORWARDED_RESPONSE_HEADERS) {
    const value = response.headers[name];
    if (value !== undefined) headers[name] = value;
  }
  headers["content-type"] = contentType;
  if (contentType === SVG_CONTENT_TYPE) {
    headers["content-security-policy"] = SVG_CONTENT_SECURITY_POLICY;
  }
  return HttpServerResponse.stream(response.stream, {
    status: response.status,
    headers,
    contentType,
  });
});
