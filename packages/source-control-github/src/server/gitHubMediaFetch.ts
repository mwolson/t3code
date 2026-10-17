import type { SourceControlMediaRequest } from "@t3tools/source-control-core/server/driver";
import * as SourceControlHost from "@t3tools/source-control-core/server/SourceControlHost";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import {
  FetchHttpClient,
  HttpClient,
  HttpClientRequest,
  type HttpClientResponse,
} from "effect/http";

import { GITHUB_CREDENTIALED_MEDIA_HOSTS, gitHubReleaseAsset } from "../shared/gitHubMedia.ts";

import * as GitHubCredentials from "./GitHubCredentials.ts";
import { resolveGitHubRepository } from "./gitHubRepositoryResolution.ts";
import * as GitHubRest from "./GitHubRest.ts";

/** A private release's bytes download only through the API, by asset id. */
const API_HOST = "api.github.com";

/**
 * Everything a redirect leads to — the presigned object stores GitHub hands assets off to —
 * authorizes with its own signature, and some of them reject a request that also carries a
 * bearer token.
 */
const isCredentialedHost = (url: string) => {
  try {
    const hostname = new URL(url).hostname.toLowerCase();
    return hostname === API_HOST || GITHUB_CREDENTIALED_MEDIA_HOSTS.has(hostname);
  } catch {
    return false;
  }
};

const MAX_REDIRECTS = 3;
const MANUAL_REDIRECT: RequestInit = { redirect: "manual" };

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

const decodeRelease = Schema.decodeUnknownEffect(
  Schema.Struct({
    truncated: Schema.Literal(false),
    body: Schema.fromJsonString(
      Schema.Struct({
        assets: Schema.Array(
          Schema.Struct({ id: Schema.Int.check(Schema.isGreaterThan(0)), name: Schema.String }),
        ),
      }),
    ),
  }),
);

export const make = Effect.gen(function* () {
  const credentials = yield* GitHubCredentials.GitHubCredentials;
  const httpClient = HttpClient.withScope(yield* HttpClient.HttpClient);
  const api = yield* GitHubRest.GitHubRest;
  const host = yield* SourceControlHost.SourceControlHost;
  const signedUrlCache = new Map<string, { readonly at: number; readonly url: string }>();

  // Both media hosts are served by github.com's account, which is the host `gh` stores it under.
  // Read on every fetch, so media loads as soon as the user signs in.
  const token = Effect.suspend(() => credentials.get("github.com")).pipe(
    Effect.map((credential) => credential.token),
    Effect.orElseSucceed(() => null),
  );

  const fetchFollowingRedirects = Effect.fn("GitHubMediaFetch.fetchFollowingRedirects")(function* (
    url: string,
    headers: Readonly<Record<string, string>>,
    bearer: Redacted.Redacted<string> | null,
  ) {
    let target = url;
    for (let hop = 0; ; hop += 1) {
      const authorization =
        bearer !== null && isCredentialedHost(target) ? `Bearer ${Redacted.value(bearer)}` : null;
      const response: HttpClientResponse.HttpClientResponse = yield* httpClient
        .execute(
          HttpClientRequest.get(target).pipe(
            HttpClientRequest.setHeaders({
              ...headers,
              ...(new URL(target).hostname === API_HOST
                ? { "x-github-api-version": "2022-11-28", "user-agent": "t3code" }
                : {}),
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

  /** The checkout's own GitHub repository, which is enough context to sign any upload it can read. */
  const checkoutRepository = Effect.fn("GitHubMediaFetch.checkoutRepository")(function* (
    cwd: string,
  ) {
    // safe.bareRepository=explicit refuses a bare root unless it is named as the git directory.
    let remotes: string | null = null;
    for (const gitDir of [[], ["--git-dir=."]]) {
      const result = yield* host.git
        .execute({
          operation: "GitHubMediaFetch.checkoutRepository",
          cwd,
          args: [...gitDir, "remote", "-v"],
        })
        .pipe(Effect.orElseSucceed(() => null));
      if (result !== null && result.exitCode === 0) {
        remotes = result.stdout;
        break;
      }
    }
    if (remotes === null) return null;
    const { locator } = resolveGitHubRepository({
      remotes,
      resolved: "",
      hostHint: "github.com",
      defaultHost: "github.com",
    });
    return locator?.host === "github.com" ? `${locator.owner}/${locator.name}` : null;
  });

  /**
   * The signed `private-user-images` URL GitHub's own pages carry for an upload, or null. The API
   * honours an SSO-authorized token where github.com does not, but only signs uploads in markdown
   * rendered with a repository as context. Any repository the token can read works, so the
   * checkout's own is enough even when the pull request lives on a fork's parent.
   */
  const signedAttachmentUrl = Effect.fn("GitHubMediaFetch.signedAttachmentUrl")(function* (
    url: string,
    cwd: string | undefined,
    now: number,
  ) {
    const match = ATTACHMENT_URL_PATTERN.exec(url);
    if (match === null) return null;
    const repository = match[1] ?? (cwd === undefined ? null : yield* checkoutRepository(cwd));
    if (repository === null) return null;
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

  return Effect.fn("GitHubMediaFetch.fetchMedia")(function* (request: SourceControlMediaRequest) {
    const bearer = yield* token;
    // A signed URL authorizes on its own, so it is fetched like any redirect target: no token.
    const now = yield* Clock.currentTimeMillis;
    const cachedEntry = signedUrlCache.get(request.url);
    const cached =
      cachedEntry !== undefined && now - cachedEntry.at < SIGNED_URL_CACHE_TTL_MS
        ? cachedEntry.url
        : null;
    const release = gitHubReleaseAsset(new URL(request.url));
    let response: HttpClientResponse.HttpClientResponse | null = null;
    if (release !== null && bearer !== null) {
      response = yield* Effect.gen(function* () {
        const path = `repos/${encodeURIComponent(release.owner)}/${encodeURIComponent(release.repo)}/releases`;
        const metadata = yield* api.rest({
          host: "github.com",
          operation: "getReleaseByTag",
          path: `${path}/tags/${encodeURIComponent(release.tag)}`,
        });
        const decoded = yield* decodeRelease(metadata);
        const match = decoded.body.assets.find((candidate) => candidate.name === release.name);
        if (match === undefined) return null;
        return yield* fetchFollowingRedirects(
          `https://${API_HOST}/${path}/assets/${match.id}`,
          { ...request.headers, accept: "application/octet-stream" },
          bearer,
        );
      }).pipe(Effect.orElseSucceed(() => null));
    }
    // A public release loads without the token, which a release download never needs.
    if (response === null || (release !== null && response.status >= 400)) {
      response = yield* fetchFollowingRedirects(
        cached ?? request.url,
        request.headers,
        release === null ? bearer : null,
      );
    }
    if (
      release === null &&
      cached === null &&
      response?.headers["content-type"]?.startsWith("text/html")
    ) {
      const signed = yield* signedAttachmentUrl(request.url, request.cwd, now);
      if (signed !== null) {
        response = yield* fetchFollowingRedirects(signed, request.headers, bearer);
      }
    }
    return response;
  });
});
