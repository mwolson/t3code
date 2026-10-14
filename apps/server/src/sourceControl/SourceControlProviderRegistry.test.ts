import { SourceControlProviderKind } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PlatformError from "effect/PlatformError";
import * as PubSub from "effect/PubSub";
import * as Redacted from "effect/Redacted";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcessSpawner } from "effect/process";
import {
  DEFAULT_SERVER_SETTINGS,
  VcsProcessSpawnError,
  VcsProcessTimeoutError,
  VcsRepositoryDetectionError,
} from "@t3tools/contracts";

import * as ServerSettings from "../serverSettings.ts";
import * as ServerConfig from "../config.ts";
import type * as VcsDriver from "../vcs/VcsDriver.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as AzureDevOpsCli from "@t3tools/source-control-azure-devops/server/AzureDevOpsCli";
import * as AzureDevOpsPullRequestCli from "@t3tools/source-control-azure-devops/server/AzureDevOpsPullRequestCli";
import * as BitbucketApi from "@t3tools/source-control-bitbucket/server/BitbucketApi";
import * as BitbucketPullRequestApi from "@t3tools/source-control-bitbucket/server/BitbucketPullRequestApi";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as GitHubApi from "@t3tools/source-control-github/server/GitHubApi";
import * as GitHubPullRequestApi from "@t3tools/source-control-github/server/GitHubPullRequestApi";
import * as GitHubCredentials from "@t3tools/source-control-github/server/GitHubCredentials";
import * as GitLabCli from "@t3tools/source-control-gitlab/server/GitLabCli";
import * as GitLabPullRequestCli from "@t3tools/source-control-gitlab/server/GitLabPullRequestCli";
import * as ForgejoCli from "@t3tools/source-control-forgejo/server/ForgejoCli";
import * as GitCafeApi from "@t3tools/source-control-gitcafe/server/GitCafeApi";
import * as GitCafeCredentials from "@t3tools/source-control-gitcafe/server/GitCafeCredentials";
import * as SourceControlProviderRegistry from "./SourceControlProviderRegistry.ts";
import * as ServerSourceControlHost from "./ServerSourceControlHost.ts";

const TEST_EPOCH = DateTime.makeUnsafe("1970-01-01T00:00:00.000Z");

const processOutput = (
  stdout: string,
  options?: {
    readonly stderr?: string;
    readonly exitCode?: ChildProcessSpawner.ExitCode;
  },
): VcsProcess.VcsProcessOutput => ({
  exitCode: options?.exitCode ?? ChildProcessSpawner.ExitCode(0),
  stdout,
  stderr: options?.stderr ?? "",
  stdoutTruncated: false,
  stderrTruncated: false,
});

function makeRegistry(input: {
  readonly remotes: ReadonlyArray<{
    readonly name: string;
    readonly url: string;
  }>;
  readonly process?: Partial<VcsProcess.VcsProcess["Service"]>;
  readonly githubApi?: Partial<GitHubApi.GitHubApi["Service"]>;
  readonly gitlab?: Partial<GitLabCli.GitLabCli["Service"]>;
  readonly forgejo?: Partial<ForgejoCli.ForgejoCli["Service"]>;
  readonly settings?: Partial<ServerSettings.ServerSettingsService["Service"]>;
  readonly started?: Effect.Effect<unknown>;
  readonly resolve?: VcsDriverRegistry.VcsDriverRegistry["Service"]["resolve"];
}) {
  const driver = {
    listRemotes: () =>
      Effect.succeed({
        remotes: input.remotes.map((remote) => ({
          ...remote,
          pushUrl: Option.none(),
          isPrimary: remote.name === "origin",
        })),
        freshness: {
          source: "live-local" as const,
          observedAt: TEST_EPOCH,
          expiresAt: Option.none(),
        },
      }),
  } satisfies Partial<VcsDriver.VcsDriver["Service"]>;

  const layerRegistry = Layer.mock(VcsDriverRegistry.VcsDriverRegistry)({
    get: () => Effect.succeed(driver as unknown as VcsDriver.VcsDriver["Service"]),
    resolve:
      input.resolve ??
      (() =>
        Effect.succeed({
          kind: "git",
          repository: {
            kind: "git",
            rootPath: "/repo",
            metadataPath: null,
            freshness: {
              source: "live-local" as const,
              observedAt: TEST_EPOCH,
              expiresAt: Option.none(),
            },
          },
          driver: driver as unknown as VcsDriver.VcsDriver["Service"],
        })),
  });

  const layerProcess = Layer.mock(VcsProcess.VcsProcess)({
    run: () => Effect.succeed(processOutput("")),
    ...input.process,
  });

  return SourceControlProviderRegistry.make.pipe(
    Effect.tap(input.started ?? Effect.void),
    Effect.provide(
      Layer.mergeAll(
        NodeServices.layer,
        layerRegistry,
        layerProcess,
        ServerSourceControlHost.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              layerProcess,
              layerRegistry,
              ServerSettings.ServerSettingsService.layerTest(),
              Layer.mock(GitVcsDriver.GitVcsDriver)({}),
            ),
          ),
        ),
        Layer.mock(AzureDevOpsCli.AzureDevOpsCli)({}),
        Layer.mock(AzureDevOpsPullRequestCli.AzureDevOpsPullRequestCli)({}),
        Layer.mock(BitbucketApi.BitbucketApi)({}),
        Layer.mock(BitbucketPullRequestApi.BitbucketPullRequestApi)({}),
        input.settings
          ? Layer.mock(ServerSettings.ServerSettingsService)(input.settings)
          : ServerSettings.ServerSettingsService.layerTest(),
        Layer.mock(GitHubPullRequestApi.GitHubPullRequestApi)({}),
        Layer.mock(GitHubApi.GitHubApi)({
          // No GitHub credential unless a test supplies one, so custom hosts stay unclaimed.
          credential: (host) => Effect.fail(new GitHubCredentials.GitHubNotSignedInError({ host })),
          ...input.githubApi,
        }),
        Layer.mock(GitVcsDriver.GitVcsDriver)({}),
        Layer.mock(GitLabCli.GitLabCli)(input.gitlab ?? {}),
        Layer.mock(GitLabPullRequestCli.GitLabPullRequestCli)({}),
        Layer.mock(ForgejoCli.ForgejoCli)({
          listLogins: () => Effect.succeed([]),
          ...input.forgejo,
        }),
        Layer.mock(GitCafeApi.GitCafeApi)({}),
        Layer.mock(GitCafeCredentials.GitCafeCredentials)({
          cliEnv: () => Effect.succeed({}),
        }),
        ServerConfig.layerTest(process.cwd(), {
          prefix: "t3-source-control-registry-test-",
        }).pipe(Layer.provide(NodeServices.layer)),
      ),
    ),
  );
}

it.effect("routes GitHub remotes to the GitHub provider", () =>
  Effect.gen(function* () {
    const registry = yield* makeRegistry({
      remotes: [{ name: "origin", url: "git@github.com:pingdotgg/t3code.git" }],
    });

    const provider = yield* registry.resolve({ cwd: "/repo" });

    assert.strictEqual(provider.kind, "github");
  }),
);

it.effect("routes directly by provider kind for remote-first workflows", () =>
  Effect.gen(function* () {
    const registry = yield* makeRegistry({
      remotes: [],
    });

    const provider = yield* registry.get(SourceControlProviderKind.make("github"));

    assert.strictEqual(provider.kind, "github");
  }),
);

it.effect("includes the request cwd when an unregistered provider is used", () =>
  Effect.gen(function* () {
    const registry = yield* makeRegistry({ remotes: [] });
    const provider = yield* registry.get(SourceControlProviderKind.make("unknown"));

    const error = yield* provider
      .getChangeRequest({ cwd: "/repo", reference: "#42" })
      .pipe(Effect.flip);

    assert.strictEqual(error.provider, "unknown");
    assert.strictEqual(error.operation, "getChangeRequest");
    assert.strictEqual(error.cwd, "/repo");
    assert.strictEqual(error.reference, "#42");
  }),
);

it.effect("retains VCS detection failures with structured cwd context", () =>
  Effect.gen(function* () {
    const cause = new VcsRepositoryDetectionError({
      operation: "resolve",
      cwd: "/repo",
      detail: "raw VCS detection failure",
      cause: new Error("raw nested failure"),
    });
    const registry = yield* makeRegistry({
      remotes: [],
      resolve: () => Effect.fail(cause),
    });

    const error = yield* registry.resolve({ cwd: "/repo" }).pipe(Effect.flip);

    assert.strictEqual(error.provider, "unknown");
    assert.strictEqual(error.operation, "detectProvider");
    assert.strictEqual(error.cwd, "/repo");
    assert.strictEqual(error.detail, "Failed to detect source control provider.");
    assert.strictEqual(error.cause, cause);
    assert.equal(error.message.includes(cause.message), false);
  }),
);

it.effect("routes GitLab remotes to the GitLab provider", () =>
  Effect.gen(function* () {
    const registry = yield* makeRegistry({
      remotes: [{ name: "origin", url: "git@gitlab.com:group/project.git" }],
    });

    const provider = yield* registry.resolve({ cwd: "/repo" });

    assert.strictEqual(provider.kind, "gitlab");
  }),
);

it.effect("routes authenticated self-hosted GitLab remotes without relying on host naming", () =>
  Effect.gen(function* () {
    const registry = yield* makeRegistry({
      remotes: [{ name: "origin", url: "https://self-hosted.example.test/group/project.git" }],
      process: {
        run: () =>
          Effect.succeed(
            processOutput(
              `gitlab.com
  x gitlab.com: API call failed: 401 Unauthorized
  ! No token found
self-hosted.example.test
  ✓ Logged in to self-hosted.example.test as gitlab-user
  ✓ Token found: ******
`,
              { exitCode: ChildProcessSpawner.ExitCode(1) },
            ),
          ),
      },
    });

    const provider = yield* registry.resolve({ cwd: "/repo" });

    assert.strictEqual(provider.kind, "gitlab");
  }),
);

it.effect("refines the caller-selected remote instead of choosing another configured remote", () =>
  Effect.gen(function* () {
    const registry = yield* makeRegistry({
      remotes: [{ name: "origin", url: "git@github.com:fork/project.git" }],
      process: {
        run: () =>
          Effect.succeed(
            processOutput(`self-hosted.example.test
  ✓ Logged in to self-hosted.example.test as gitlab-user
`),
          ),
      },
    });

    const handle = yield* registry.resolveHandle({
      cwd: "/repo",
      context: {
        provider: {
          kind: SourceControlProviderKind.make("unknown"),
          name: "self-hosted.example.test",
          baseUrl: "https://self-hosted.example.test",
        },
        remoteName: "upstream",
        remoteUrl: "https://self-hosted.example.test/group/project.git",
      },
    });

    assert.strictEqual(handle.context?.provider.kind, "gitlab");
    assert.strictEqual(handle.context?.remoteName, "upstream");
  }),
);

it.effect("routes authenticated self-hosted GitLab remotes on non-standard ports", () =>
  Effect.gen(function* () {
    const registry = yield* makeRegistry({
      remotes: [{ name: "origin", url: "https://self-hosted.example.test:8443/group/project.git" }],
      process: {
        run: () =>
          Effect.succeed(
            processOutput(
              `self-hosted.example.test:8443
  ✓ Logged in to self-hosted.example.test:8443 as gitlab-user
  ✓ Token found: ******
`,
            ),
          ),
      },
    });

    const provider = yield* registry.resolve({ cwd: "/repo" });

    assert.strictEqual(provider.kind, "gitlab");
  }),
);

it.effect("routes Bitbucket remotes to the Bitbucket provider", () =>
  Effect.gen(function* () {
    const registry = yield* makeRegistry({
      remotes: [{ name: "origin", url: "git@bitbucket.org:pingdotgg/t3code.git" }],
    });

    const provider = yield* registry.resolve({ cwd: "/repo" });

    assert.strictEqual(provider.kind, "bitbucket");
  }),
);

it.effect("routes Azure DevOps remotes to the Azure DevOps provider", () =>
  Effect.gen(function* () {
    const registry = yield* makeRegistry({
      remotes: [{ name: "origin", url: "https://dev.azure.com/acme/project/_git/repo" }],
    });

    const provider = yield* registry.resolve({ cwd: "/repo" });

    assert.strictEqual(provider.kind, "azure-devops");
  }),
);

it.effect("falls back to a non-origin remote when origin is not configured", () =>
  Effect.gen(function* () {
    const registry = yield* makeRegistry({
      remotes: [{ name: "upstream", url: "https://dev.azure.com/acme/project/_git/repo" }],
    });

    const provider = yield* registry.resolve({ cwd: "/repo" });

    assert.strictEqual(provider.kind, "azure-devops");
  }),
);

it.effect(
  "routes linked subjects by URL independently of the checkout and skips unsupported links",
  () =>
    Effect.gen(function* () {
      const registry = yield* makeRegistry({
        remotes: [{ name: "origin", url: "https://github.com/unrelated/checkout.git" }],
        githubApi: {
          rest: () =>
            Effect.succeed({
              status: 200,
              headers: {},
              body: JSON.stringify({ title: "GitHub issue", body: null }),
              truncated: false,
              invalidUtf8: false,
            }),
        },
        gitlab: {
          execute: () =>
            Effect.succeed(
              processOutput(JSON.stringify({ title: "GitLab MR", description: "Nested project" })),
            ),
        },
      });
      for (const [url, expected] of [
        ["https://github.com/team/project/issues/1", { title: "GitHub issue", body: null }],
        [
          "https://gitlab.com/team/sub/project/-/merge_requests/2",
          { title: "GitLab MR", body: "Nested project" },
        ],
      ] as const) {
        const lookup = registry.resolveLink({ cwd: "/unrelated", url: new URL(url) });
        assert.ok(lookup);
        assert.deepStrictEqual(yield* lookup, expected);
      }
      for (const url of [
        "https://example.test/team/project/issues/1",
        "https://github.attacker.test/team/project/issues/1",
        "https://gitlab.attacker.test/team/project/-/issues/1",
        "https://github.com/team/project",
        "https://codeberg.org/team/project/issues/1",
        "https://bitbucket.org/team/project/pull-requests/1",
        "https://dev.azure.com/org/project/_git/repo/pullrequest/1",
        "http://github.com/team/project/issues/1",
        "https://user:secret@github.com/team/project/issues/1",
      ]) {
        assert.strictEqual(
          registry.resolveLink({ cwd: "/unrelated", url: new URL(url) }),
          undefined,
        );
      }
    }).pipe(Effect.scoped),
);

it.effect.each([
  { name: "a credential", credential: "found", expected: "github" },
  // Claimed so the error says the host is turned off, not that it is unsupported.
  { name: "a host turned off in Settings", credential: "disabled", expected: "github" },
  { name: "no credential", credential: "missing", expected: "unknown" },
] as const)("resolves custom-host remotes with $name", (scenario) =>
  Effect.gen(function* () {
    const hosts: Array<string> = [];
    const commands: Array<string> = [];
    const registry = yield* makeRegistry({
      remotes: [{ name: "origin", url: "git@code.example.test:team/project.git" }],
      process: {
        run: ({ command }) => {
          commands.push(command);
          return Effect.succeed(processOutput(""));
        },
      },
      githubApi: {
        credential: (host) => {
          hosts.push(host);
          return scenario.credential === "found"
            ? Effect.succeed({ token: Redacted.make("token"), fingerprint: `${host}:fingerprint` })
            : scenario.credential === "disabled"
              ? Effect.fail(new GitHubCredentials.GitHubHostDisabledError({ host }))
              : Effect.fail(new GitHubCredentials.GitHubNotSignedInError({ host }));
        },
      },
    });
    const handle = yield* registry.resolveHandle({ cwd: "/repo" });
    assert.strictEqual(handle.provider.kind, SourceControlProviderKind.make(scenario.expected));
    assert.strictEqual(handle.context?.provider.baseUrl, "https://code.example.test");
    assert.deepStrictEqual(hosts, ["code.example.test"]);
    // Recognition goes through the credential layer; gh is only one of its sources.
    assert.notInclude(commands, "gh");
  }),
);

it.effect("skips GitHub discovery for the identity resolver's empty base URL", () =>
  Effect.gen(function* () {
    const hosts: string[] = [];
    const remoteUrl = "git@githubenterprise.dev.example.com:team/workspace.git";
    const registry = yield* makeRegistry({
      remotes: [{ name: "origin", url: remoteUrl }],
      githubApi: {
        credential: (host) => {
          hosts.push(host);
          return Effect.succeed({
            token: Redacted.make("token"),
            fingerprint: `${host}:fingerprint`,
          });
        },
      },
    });
    const context = {
      provider: { kind: SourceControlProviderKind.make("unknown"), name: "Unknown", baseUrl: "" },
      remoteName: "origin",
      remoteUrl,
    };
    const unresolved = yield* registry.resolveHandle({ cwd: "/repo", context });
    assert.deepStrictEqual(unresolved.context, context);
    assert.deepStrictEqual(hosts, []);

    const resolved = yield* registry.resolveHandle({ cwd: "/repo" });
    assert.strictEqual(resolved.provider.kind, "github");
    assert.strictEqual(
      resolved.context?.provider.baseUrl,
      "https://githubenterprise.dev.example.com",
    );
    assert.deepStrictEqual(hosts, ["githubenterprise.dev.example.com"]);
  }),
);

const notFound = (command: string, cwd: string, module = "ChildProcess", method = "spawn") =>
  new VcsProcessSpawnError({
    operation: "test",
    command,
    cwd,
    cause: PlatformError.systemError({ _tag: "NotFound", module, method }),
  });

const resolveUnknown = (
  registry: SourceControlProviderRegistry.SourceControlProviderRegistry["Service"],
  repository: string,
  host = "git.example.internal",
) =>
  registry.resolveHandle({
    cwd: `/${repository}`,
    context: {
      provider: {
        kind: SourceControlProviderKind.make("unknown"),
        name: host.toLowerCase(),
        baseUrl: `https://${host.toLowerCase()}`,
      },
      remoteName: "origin",
      remoteUrl: `git@${host}:team/${repository}.git`,
    },
  });

it.effect("asks once for every checkout of a server no integration claims", () =>
  Effect.gen(function* () {
    const asked: Array<string> = [];
    const release = yield* Deferred.make<void>();
    const registry = yield* makeRegistry({
      remotes: [],
      process: {
        run: ({ command, cwd }) => {
          asked.push(command);
          return Deferred.await(release).pipe(Effect.andThen(Effect.fail(notFound(command, cwd))));
        },
      },
      forgejo: {
        listLogins: ({ command = "tea", cwd }) => {
          asked.push(command);
          const cause = notFound(command, cwd);
          return Effect.fail(new ForgejoCli.ForgejoCliError({ command, cwd, detail: "", cause }));
        },
      },
    });

    // Each caller runs until it waits, so once all have started the first probe is still open.
    const started = yield* Effect.forEach(["a", "b", "c"], () => Deferred.make<void>());
    const callers = yield* Effect.forEach(
      ["a", "b", "c"],
      (repository, index) =>
        Deferred.succeed(started[index]!, undefined).pipe(
          Effect.andThen(resolveUnknown(registry, repository)),
        ),
      { concurrency: "unbounded" },
    ).pipe(Effect.forkChild);
    yield* Effect.forEach(started, Deferred.await);
    assert.deepStrictEqual(asked, ["glab"]);
    yield* Deferred.succeed(release, undefined);
    const handles = yield* Fiber.join(callers);
    yield* resolveUnknown(registry, "d");
    assert.deepStrictEqual(
      handles.map((handle) => [handle.provider.kind, handle.context?.remoteUrl]),
      ["a", "b", "c"].map((name) => ["unknown", `git@git.example.internal:team/${name}.git`]),
    );
    assert.deepStrictEqual(asked, ["glab", "fj", "tea"]);

    // A login made in a terminal sends no signal, so the server is asked again later.
    yield* TestClock.adjust("5 minutes");
    yield* resolveUnknown(registry, "a");
    assert.strictEqual(asked.length, 6);
  }),
);

it.effect.each(["tea", "gh"] as const)("asks again after %s could not run", (failing) =>
  Effect.gen(function* () {
    let attempts = 0;
    const fail = <E>(error: E) => {
      attempts += 1;
      return Effect.fail(error);
    };
    const registry = yield* makeRegistry({
      remotes: [],
      process: { run: ({ command, cwd }) => Effect.fail(notFound(command, cwd)) },
      githubApi: {
        credential: (host) =>
          failing === "gh"
            ? fail(new GitHubCredentials.GitHubCliFailedError({ host, cause: "timeout" }))
            : Effect.fail(new GitHubCredentials.GitHubNotSignedInError({ host })),
      },
      forgejo: {
        listLogins: ({ command, cwd }) =>
          failing === "tea" && command !== "fj"
            ? fail(new ForgejoCli.ForgejoCliError({ command: "tea", cwd, detail: "Failed." }))
            : Effect.succeed([]),
      },
    });
    for (const repository of ["a", "b"]) {
      const handle = yield* resolveUnknown(registry, repository);
      assert.strictEqual(handle.provider.kind, "unknown");
    }
    assert.strictEqual(attempts, 2);
  }),
);

const glabReply = (stderr: string, exitCode = 1) =>
  Effect.succeed(processOutput("", { stderr, exitCode: ChildProcessSpawner.ExitCode(exitCode) }));
const glabLogin = (host = "git.example.internal") =>
  glabReply(`${host}\n  ✓ Logged in to ${host} as maria (keyring)\n`, 0);

it.effect.each([
  { name: "a login", replies: [glabLogin()], found: ["gitlab", "gitlab"], asks: 1 },
  {
    name: "a login under the remote's spelling",
    host: "Git.Example.Internal",
    replies: [glabLogin("Git.Example.Internal")],
    found: ["gitlab", "gitlab"],
    asks: 1,
  },
  ...[
    ["no login", "x git.example.internal has not been authenticated with glab"],
    ["no login on glab 1.22", "x git.example.internal not authenticated with glab"],
    ["no instances", "no GitLab instances have been authenticated with glab"],
    ["no instance on glab 1.22", "No GitLab instance has been authenticated with glab"],
  ].map(([name, message]) => ({
    name,
    host: undefined,
    replies: [glabReply(message!)],
    found: ["unknown", "unknown"],
    asks: 1,
  })),
  {
    name: "a failed check",
    replies: [glabReply("git.example.internal\n  x git.example.internal: API call failed\n")],
    found: ["unknown", "unknown"],
    asks: 2,
  },
  {
    name: "a timeout",
    replies: [
      Effect.fail(
        new VcsProcessTimeoutError({ operation: "test", command: "glab", cwd: "/a", timeoutMs: 1 }),
      ),
    ],
    found: ["unknown", "unknown"],
    asks: 2,
  },
  {
    name: "a check failed in a missing checkout",
    replies: [Effect.fail(notFound("glab", "/a", "FileSystem", "access")), glabLogin()],
    found: ["unknown", "gitlab"],
    asks: 2,
  },
])("asks glab about the remote's own host and keeps $name", (scenario) =>
  Effect.gen(function* () {
    const host = scenario.host ?? "git.example.internal";
    const asked: Array<ReadonlyArray<string>> = [];
    const registry = yield* makeRegistry({
      remotes: [],
      process: {
        // glab holds a login only under the host's exact spelling.
        run: ({ args, cwd }) => {
          asked.push([cwd, ...args]);
          return args[3] === host
            ? (scenario.replies[asked.length - 1] ?? scenario.replies[0]!)
            : glabReply("x git.example.internal has not been authenticated with glab");
        },
      },
    });
    const found = yield* Effect.forEach(["a", "b"], (repository) =>
      resolveUnknown(registry, repository, host).pipe(Effect.map(({ provider }) => provider.kind)),
    );
    assert.deepStrictEqual(found, scenario.found);
    assert.deepStrictEqual(
      asked,
      ["/a", "/b"]
        .slice(0, scenario.asks)
        .map((cwd) => [cwd, "auth", "status", "--hostname", host]),
    );
  }),
);

it.effect("does not keep a tea login found while fj could not be read", () =>
  Effect.gen(function* () {
    let fjReadable = false;
    const loginAt = (port: number) => ({
      name: "login",
      url: `https://git.example.internal:${port}`,
      ssh_host: "git.example.internal",
      user: "maria",
      default: "true",
    });
    const registry = yield* makeRegistry({
      remotes: [],
      process: { run: ({ command, cwd }) => Effect.fail(notFound(command, cwd)) },
      forgejo: {
        listLogins: ({ command, cwd }) =>
          command === "fj" && !fjReadable
            ? Effect.fail(new ForgejoCli.ForgejoCliError({ command, cwd, detail: "Unreadable." }))
            : Effect.succeed([loginAt(command === "fj" ? 4000 : 3000)]),
      },
    });
    const fallback = yield* resolveUnknown(registry, "a");
    assert.strictEqual(fallback.context?.provider.baseUrl, "https://git.example.internal:3000");
    fjReadable = true;
    const recovered = yield* resolveUnknown(registry, "b");
    assert.strictEqual(recovered.context?.provider.baseUrl, "https://git.example.internal:4000");
  }),
);

it.effect.each([
  { first: "https://host-a.example/repo.git", second: "https://host-b.example/repo.git" },
  { first: "ssh://git@host-a.example/repo.git", second: "ssh://git@host-b.example/repo.git" },
  { first: "https://host.example:8443/repo.git", second: "https://host.example:9443/repo.git" },
])("keeps the lookups for $first and $second apart", ({ first, second }) =>
  Effect.gen(function* () {
    const asked: Array<string | undefined> = [];
    const registry = yield* makeRegistry({
      remotes: [],
      process: { run: ({ command, cwd }) => Effect.fail(notFound(command, cwd)) },
      forgejo: {
        listLogins: ({ command, remoteUrl }) => {
          if (command === "tea") asked.push(remoteUrl);
          return Effect.succeed([]);
        },
      },
    });
    for (const remoteUrl of [first, second]) {
      // The identity resolver's context, where the remote alone names the server.
      const provider = {
        kind: SourceControlProviderKind.make("unknown"),
        name: "Unknown",
        baseUrl: "",
      };
      yield* registry.resolveHandle({
        cwd: "/repo",
        context: { provider, remoteName: "origin", remoteUrl },
      });
    }
    assert.deepStrictEqual(asked, [first, second]);
  }),
);

it.effect("asks again once Settings change", () =>
  Effect.gen(function* () {
    const changes = yield* PubSub.unbounded<typeof DEFAULT_SERVER_SETTINGS>();
    const handled = yield* Effect.forEach([0, 1], () => Deferred.make<void>());
    let changeCount = 0;
    const markHandled = Effect.suspend(() => Deferred.succeed(handled[changeCount++]!, undefined));
    let signedIn = false;
    const registry = yield* makeRegistry({
      remotes: [],
      settings: {
        subscribeChanges: PubSub.subscribe(changes).pipe(
          Effect.map((subscription) =>
            // A change is marked handled once the registry asks for the next one.
            Stream.fromSubscription(subscription).pipe(
              Stream.flatMap((settings) =>
                Stream.concat(Stream.make(settings), Stream.fromEffectDrain(markHandled)),
              ),
            ),
          ),
        ),
      },
      // Saved as the registry finishes starting, before its consumer has run.
      started: PubSub.publish(changes, DEFAULT_SERVER_SETTINGS),
      process: { run: ({ command, cwd }) => Effect.fail(notFound(command, cwd)) },
      githubApi: {
        credential: (host) =>
          signedIn
            ? Effect.succeed({ token: Redacted.make("token"), fingerprint: `${host}:fingerprint` })
            : Effect.fail(new GitHubCredentials.GitHubNotSignedInError({ host })),
      },
    });
    yield* Deferred.await(handled[0]!);
    assert.strictEqual((yield* resolveUnknown(registry, "a")).provider.kind, "unknown");
    signedIn = true;
    assert.strictEqual((yield* resolveUnknown(registry, "b")).provider.kind, "unknown");

    // A token saved for the host in Settings.
    yield* PubSub.publish(changes, DEFAULT_SERVER_SETTINGS);
    yield* Deferred.await(handled[1]!);
    assert.strictEqual((yield* resolveUnknown(registry, "c")).provider.kind, "github");
  }),
);
