// @ts-nocheck — inventory fixtures predate ModelV2Info/AgentV2Info shape.
// @effect-diagnostics nodeBuiltinImport:off
// Model/agent fixtures are structural for inventory tests across SDK generations.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { assert, it } from "@effect/vitest";
import { OpenCode2Settings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import { describe } from "vite-plus/test";

import * as OpenCode2Runtime from "../opencode2Runtime.ts";
import { parseGenericCliVersion } from "../providerSnapshot.ts";
import {
  checkOpenCodeProviderStatus,
  flattenOpenCodeModels,
  isOpenCodeInventorySettlementError,
  listOpenCodeSkillsForDirectory,
  openCodeCommandsToServerProviderSlashCommands,
  parseOpenCodeVersion,
  settleOpenCodeInventory,
} from "./OpenCodeProvider.ts";

const OPENCODE2_BANNER = "opencode2 v0.0.0-next-16339\n";
const BIG_PICKLE_MODEL = {
  id: "big-pickle",
  modelID: "big-pickle",
  providerID: "opencode",
  name: "Big Pickle",
  capabilities: {
    tools: true,
    input: ["text"],
    output: ["text"],
  },
  variants: [],
  time: {
    released: 0,
  },
  cost: [],
  status: "active",
  enabled: true,
  limit: {
    context: 128_000,
    output: 16_384,
  },
} satisfies any;
const BIG_PICKLE_FAST_MODEL = {
  ...BIG_PICKLE_MODEL,
  id: "big-pickle-fast",
  name: "Big Pickle Fast",
} satisfies any;
const OPENAI_MODEL = {
  ...BIG_PICKLE_MODEL,
  id: "gpt-test",
  modelID: "gpt-test",
  providerID: "openai",
  name: "GPT Test",
} satisfies any;
// The runtime is replaced in these tests, so `binaryPath` is never run.
const OPENCODE2_TEST_SETTINGS = Schema.decodeSync(OpenCode2Settings)({
  enabled: true,
  binaryPath: "fake-opencode2",
});
const OPENCODE2_EXTERNAL_TEST_SETTINGS = Schema.decodeSync(OpenCode2Settings)({
  enabled: true,
  serverPassword: "external-secret",
  serverUrl: "http://127.0.0.1:9998",
});

function failingOpenCodeRuntime(
  category: OpenCode2Runtime.OpenCodeRuntimeErrorCategory,
  cause?: unknown,
): OpenCode2Runtime.OpenCode2Runtime["Service"] {
  const failure = new OpenCode2Runtime.OpenCodeRuntimeError({
    operation: "connectToOpenCodeServer",
    category,
    cause,
  });
  return OpenCode2Runtime.OpenCode2Runtime.of({
    connectToOpenCodeServer: () => Effect.fail(failure),
    createOpenCodeSdkClient: () => {
      throw new Error("unexpected SDK client creation");
    },
  });
}

/** Released 2.0.15 client shapes for an attached service reporting `version`. */
function openCode2RuntimeWithServerVersion(
  version: string,
  models: () => Array<any> = () => [BIG_PICKLE_MODEL],
): OpenCode2Runtime.OpenCode2Runtime["Service"] {
  const location = { directory: "/workspace" };
  const client = {
    server: {
      info: async () => ({ version, pid: 4242, urls: [], paths: { tmp: "/tmp" } }),
    },
    agent: {
      list: async () => ({ location, data: [BUILD_AGENT] }),
    },
    integration: {
      list: async () => ({
        location,
        data: [
          {
            id: "opencode",
            name: "OpenCode",
            methods: [],
            connections: [{ type: "env", name: "OPENCODE_TEST_KEY" }],
          } satisfies IntegrationInfo,
        ],
      }),
    },
    model: {
      list: async () => ({ location, data: models() }),
    },
  } as never;

  return OpenCode2Runtime.OpenCode2Runtime.of({
    connectToOpenCodeServer: () =>
      Effect.succeed({
        exitCode: null,
        external: true,
        password: "test-password",
        url: "http://127.0.0.1:1234",
      }),
    createOpenCodeSdkClient: () => client,
  });
}

const BUILD_AGENT = {
  id: "build",
  name: "Build",
  request: { settings: {}, headers: {}, body: {} },
  mode: "primary",
  hidden: false,
  permissions: [],
} satisfies any;

describe("parseOpenCodeVersion", () => {
  // The reason this parser exists: the generic one anchors on `\b`, and the
  // `v` prefix kills the word boundary before the leading digit.
  it("parses the banner the generic CLI parser returns null for", () => {
    assert.strictEqual(parseGenericCliVersion(OPENCODE2_BANNER), "0.0.0");
    assert.strictEqual(parseOpenCodeVersion(OPENCODE2_BANNER), "0.0.0-next-16339");
  });

  it("parses a plain release version", () => {
    assert.strictEqual(parseOpenCodeVersion("opencode2 2.1.4\n"), "2.1.4");
  });

  it("parses a beta CLI banner", () => {
    assert.strictEqual(parseOpenCodeVersion("opencode2 v0.0.0-beta-17498\n"), "0.0.0-beta-17498");
  });

  it("returns null when there is no version at all", () => {
    assert.strictEqual(
      parseOpenCodeVersion("Error: @opencode-ai/cli's postinstall script was not run."),
      null,
    );
  });
});

describe("openCodeCommandsToServerProviderSlashCommands", () => {
  it("keeps compaction first and deduplicates native commands", () => {
    assert.deepStrictEqual(
      openCodeCommandsToServerProviderSlashCommands([
        { name: " review ", description: " Review changes " },
        { name: "review" },
        { name: "compact", description: "Native compaction" },
        { name: "  " },
        { name: "mcp:search" },
      ]).slice(1),
      [{ name: "review", description: "Review changes" }, { name: "mcp:search" }],
    );
  });
});

describe("checkOpenCodeProviderStatus", () => {
  it.effect("reports the attached service version from /api/info", () =>
    Effect.gen(function* () {
      const providerFiber = yield* checkOpenCodeProviderStatus(
        OPENCODE2_TEST_SETTINGS,
        "/workspace",
        {},
      ).pipe(
        Effect.provideService(
          OpenCode2Runtime.OpenCode2Runtime,
          openCode2RuntimeWithServerVersion("2.0.15"),
        ),
        Effect.forkChild,
      );

      yield* Effect.yieldNow;
      yield* TestClock.adjust("500 millis");
      const provider = yield* Fiber.join(providerFiber);

      assert.strictEqual(provider.status, "ready");
      assert.strictEqual(provider.version, "2.0.15");
      assert.isTrue(provider.installed);
    }),
  );

  for (const [category, expected, installed] of [
    ["service-not-running", "T3 Code starts it when it is missing", false],
    ["binary-not-found", "could not find the `opencode` command", false],
    [
      "service-start-failed",
      "could not start the OpenCode 2 service. Run `opencode service start`",
      true,
    ],
    ["service-start-timeout", "did not finish starting in time", true],
    ["unsupported-server-version", "is not version 2.0.15", true],
    ["service-identity-mismatch", "Restart the OpenCode 2 service yourself", true],
    ["service-credentials-required", "registration has no password", true],
    ["service-probe-timeout", "did not answer in time", true],
    ["authentication-failed", "rejected T3 Code's credentials", true],
    ["network-failed", "Couldn't reach the running OpenCode 2 service", true],
  ] as const) {
    it.effect(`explains ${category} without leaking its cause`, () =>
      Effect.gen(function* () {
        const secret = "OPENCODE2_ATTACH_SECRET";
        const provider = yield* checkOpenCodeProviderStatus(
          OPENCODE2_TEST_SETTINGS,
          "/workspace",
          {},
        ).pipe(
          Effect.provideService(
            OpenCode2Runtime.OpenCode2Runtime,
            failingOpenCodeRuntime(category, new Error(secret)),
          ),
        );

        assert.strictEqual(provider.status, "error");
        assert.strictEqual(provider.installed, installed);
        assert.include(provider.message ?? "", expected);
        assert.notInclude(provider.message ?? "", secret);
        assert.notInclude(provider.message ?? "", "npm install");
      }),
    );
  }

  it.effect("passes the configured binary to the runtime for a service start", () =>
    Effect.gen(function* () {
      const inputs: Array<{ readonly binaryPath?: string | null }> = [];
      const failing = failingOpenCodeRuntime("service-start-failed");
      yield* checkOpenCodeProviderStatus(OPENCODE2_TEST_SETTINGS, "/workspace", {}).pipe(
        Effect.provideService(
          OpenCode2Runtime.OpenCode2Runtime,
          OpenCode2Runtime.OpenCode2Runtime.of({
            ...failing,
            connectToOpenCodeServer: (input) => {
              inputs.push(input);
              return failing.connectToOpenCodeServer(input);
            },
          }),
        ),
      );
      assert.deepEqual(
        inputs.map((input) => input.binaryPath),
        ["fake-opencode2"],
      );
    }),
  );

  it.effect("reports the exit code of a failed service start", () =>
    Effect.gen(function* () {
      const failure = new OpenCode2Runtime.OpenCodeRuntimeError({
        operation: "service.start",
        category: "service-start-failed",
        exitCode: 3,
      });
      const provider = yield* checkOpenCodeProviderStatus(
        OPENCODE2_TEST_SETTINGS,
        "/workspace",
        {},
      ).pipe(
        Effect.provideService(
          OpenCode2Runtime.OpenCode2Runtime,
          OpenCode2Runtime.OpenCode2Runtime.of({
            ...failingOpenCodeRuntime("service-start-failed"),
            connectToOpenCodeServer: () => Effect.fail(failure),
          }),
        ),
      );
      assert.include(
        provider.message ?? "",
        "could not start the OpenCode 2 service (exit code 3)",
      );
    }),
  );

  it.effect("names the configured server when its version is unsupported", () =>
    Effect.gen(function* () {
      const provider = yield* checkOpenCodeProviderStatus(
        OPENCODE2_EXTERNAL_TEST_SETTINGS,
        "/workspace",
        {},
      ).pipe(
        Effect.provideService(
          OpenCode2Runtime.OpenCode2Runtime,
          failingOpenCodeRuntime("unsupported-server-version"),
        ),
      );

      assert.include(provider.message ?? "", "configured OpenCode 2 server");
      assert.include(provider.message ?? "", "exactly OpenCode 2.0.15");
    }),
  );

  it.effect("reports an unreadable /api/info version", () =>
    Effect.gen(function* () {
      const providerFiber = yield* checkOpenCodeProviderStatus(
        OPENCODE2_TEST_SETTINGS,
        "/workspace",
        {},
      ).pipe(
        Effect.provideService(
          OpenCode2Runtime.OpenCode2Runtime,
          openCode2RuntimeWithServerVersion("not-a-version"),
        ),
        Effect.forkChild,
      );

      yield* Effect.yieldNow;
      yield* TestClock.adjust("500 millis");
      const provider = yield* Fiber.join(providerFiber);

      assert.strictEqual(provider.status, "error");
      assert.strictEqual(
        provider.message,
        "OpenCode 2 did not report a readable version from `/api/info`.",
      );
    }),
  );

  it.effect("reports inventory instability without blaming the service", () =>
    Effect.gen(function* () {
      let reads = 0;
      const providerFiber = yield* checkOpenCodeProviderStatus(
        OPENCODE2_TEST_SETTINGS,
        "/workspace",
        {},
      ).pipe(
        Effect.provideService(
          OpenCode2Runtime.OpenCode2Runtime,
          openCode2RuntimeWithServerVersion("2.0.15", () => {
            reads += 1;
            return [{ ...BIG_PICKLE_MODEL, id: `big-pickle-${reads}` }];
          }),
        ),
        Effect.forkChild,
      );

      yield* Effect.yieldNow;
      yield* TestClock.adjust("6 seconds");
      const provider = yield* Fiber.join(providerFiber);

      assert.strictEqual(provider.status, "error");
      assert.include(provider.message ?? "", "inventory did not stabilize");
      assert.notInclude(provider.message ?? "", "service");
    }),
  );
});

describe("settleOpenCodeInventory", () => {
  it.effect("uses a 500ms healthy-path floor by default", () =>
    Effect.gen(function* () {
      const reads = yield* Ref.make(0);
      const settlement = yield* settleOpenCodeInventory(
        Ref.update(reads, (count) => count + 1).pipe(
          Effect.as({
            models: [BIG_PICKLE_MODEL],
            agents: [BUILD_AGENT],
            connectedIntegrationIDs: ["opencode"],
          }),
        ),
      ).pipe(Effect.forkChild);

      yield* Effect.yieldNow;
      yield* TestClock.adjust("499 millis");
      assert.strictEqual(yield* Ref.get(reads), 5);

      yield* TestClock.adjust("1 millis");
      yield* Fiber.join(settlement);
      assert.strictEqual(yield* Ref.get(reads), 6);
    }),
  );

  it.effect("waits through a non-empty baseline until connected integrations settle", () =>
    Effect.gen(function* () {
      let reads = 0;
      const inventory = yield* settleOpenCodeInventory(
        Effect.sync(() => {
          reads += 1;
          return reads < 4
            ? { models: [BIG_PICKLE_MODEL], agents: [BUILD_AGENT], connectedIntegrationIDs: [] }
            : {
                models: [BIG_PICKLE_MODEL, OPENAI_MODEL],
                agents: [BUILD_AGENT],
                connectedIntegrationIDs: ["openai", "opencode"],
              };
        }),
        { maxAttempts: 6, minimumAttempts: 4, quietAttempts: 2, retryDelayMs: 0 },
      );

      assert.strictEqual(reads, 5);
      assert.deepStrictEqual(inventory.connectedIntegrationIDs, ["openai", "opencode"]);
      assert.deepStrictEqual(
        inventory.models.map((model) => model.providerID),
        ["opencode", "openai"],
      );
    }),
  );

  it.effect("returns a logged-out free catalog only at the bounded deadline", () =>
    Effect.gen(function* () {
      let reads = 0;
      const inventory = yield* settleOpenCodeInventory(
        Effect.sync(() => {
          reads += 1;
          return {
            models: [BIG_PICKLE_MODEL],
            agents: [BUILD_AGENT],
            connectedIntegrationIDs: [],
          };
        }),
        { maxAttempts: 6, minimumAttempts: 4, quietAttempts: 2, retryDelayMs: 0 },
      );

      assert.strictEqual(reads, 6);
      assert.deepStrictEqual(inventory.models, [BIG_PICKLE_MODEL]);
      assert.deepStrictEqual(inventory.connectedIntegrationIDs, []);
    }),
  );

  it.effect("keeps the last settled catalog when the final attempt changes", () =>
    Effect.gen(function* () {
      let reads = 0;
      const inventory = yield* settleOpenCodeInventory(
        Effect.sync(() => {
          reads += 1;
          return reads < 6
            ? { models: [BIG_PICKLE_MODEL], agents: [BUILD_AGENT], connectedIntegrationIDs: [] }
            : {
                models: [BIG_PICKLE_MODEL, OPENAI_MODEL],
                agents: [BUILD_AGENT],
                connectedIntegrationIDs: ["openai", "opencode"],
              };
        }),
        { maxAttempts: 6, minimumAttempts: 4, quietAttempts: 2, retryDelayMs: 0 },
      );

      assert.strictEqual(reads, 6);
      assert.deepStrictEqual(inventory.connectedIntegrationIDs, []);
      assert.deepStrictEqual(
        inventory.models.map((model) => model.providerID),
        ["opencode"],
      );
    }),
  );

  it.effect("fails when no catalog fingerprint stabilizes", () =>
    Effect.gen(function* () {
      let reads = 0;
      const error = yield* settleOpenCodeInventory(
        Effect.sync(() => {
          reads += 1;
          return {
            models: [{ ...BIG_PICKLE_MODEL, id: `big-pickle-${reads}` }],
            agents: [BUILD_AGENT],
            connectedIntegrationIDs: [],
          };
        }),
        { maxAttempts: 6, minimumAttempts: 4, quietAttempts: 2, retryDelayMs: 0 },
      ).pipe(Effect.flip);

      assert.strictEqual(reads, 6);
      assert.ok(isOpenCodeInventorySettlementError(error));
      assert.strictEqual(error.attempts, 6);
      assert.strictEqual(
        error.message,
        "OpenCode 2 inventory did not stabilize before the retry limit.",
      );
    }),
  );

  it.effect("ignores a non-model integration when another connection supplies models", () =>
    Effect.gen(function* () {
      let reads = 0;
      const inventory = yield* settleOpenCodeInventory(
        Effect.sync(() => {
          reads += 1;
          return {
            models: [BIG_PICKLE_MODEL],
            agents: [BUILD_AGENT],
            connectedIntegrationIDs: ["openai", "opencode"],
          };
        }),
        { maxAttempts: 3, minimumAttempts: 2, quietAttempts: 2, retryDelayMs: 0 },
      );

      assert.strictEqual(reads, 2);
      assert.deepStrictEqual(inventory.connectedIntegrationIDs, ["openai", "opencode"]);
    }),
  );

  it.effect("stops at the deadline when no connected integration supplies models", () =>
    Effect.gen(function* () {
      let reads = 0;
      const inventory = yield* settleOpenCodeInventory(
        Effect.sync(() => {
          reads += 1;
          return {
            models: [BIG_PICKLE_MODEL],
            agents: [BUILD_AGENT],
            connectedIntegrationIDs: ["openai"],
          };
        }),
        { maxAttempts: 3, minimumAttempts: 2, quietAttempts: 2, retryDelayMs: 0 },
      );

      assert.strictEqual(reads, 3);
      assert.deepStrictEqual(inventory.connectedIntegrationIDs, ["openai"]);
    }),
  );

  it.effect("stops at the deadline when the catalog stays empty", () =>
    Effect.gen(function* () {
      let reads = 0;
      const inventory = yield* settleOpenCodeInventory(
        Effect.sync(() => {
          reads += 1;
          return { models: [], agents: [], connectedIntegrationIDs: [] };
        }),
        { maxAttempts: 3, retryDelayMs: 0 },
      );

      assert.strictEqual(reads, 3);
      assert.deepStrictEqual(inventory, {
        models: [],
        agents: [],
        connectedIntegrationIDs: [],
      });
    }),
  );
});

describe("flattenOpenCodeModels", () => {
  it("uses a readable upstream provider label", () => {
    assert.deepStrictEqual(flattenOpenCodeModels({ models: [BIG_PICKLE_MODEL], agents: [] }), [
      {
        slug: "opencode/big-pickle",
        name: "Big Pickle",
        subProvider: "OpenCode",
        isCustom: false,
        capabilities: {
          optionDescriptors: [],
        },
      },
    ]);
  });

  it("uses the selectable model ref id when models share an underlying model id", () => {
    assert.deepStrictEqual(
      flattenOpenCodeModels({
        models: [BIG_PICKLE_MODEL, BIG_PICKLE_FAST_MODEL],
        agents: [],
      }).map((model) => model.slug),
      ["opencode/big-pickle", "opencode/big-pickle-fast"],
    );
  });

  it("keeps a structured model whose id contains a slash", () => {
    const slashModel = {
      ...BIG_PICKLE_MODEL,
      id: "qwen/qwen3-coder",
      modelID: "qwen/qwen3-coder",
      providerID: "openrouter",
      name: "qwen3-coder",
    } satisfies any;

    // Unlike the 1.x text parser fixed by #5072 opencode-model-slug-misclassification,
    // 2.x receives a structured SDK model and constructs the selectable
    // provider/model ref directly.
    assert.deepStrictEqual(
      flattenOpenCodeModels({ models: [slashModel], agents: [] }).map((model) => model.slug),
      ["openrouter/qwen/qwen3-coder"],
    );
  });

  it("marks the inferred reasoning default without a synthetic Default option", () => {
    const [model] = flattenOpenCodeModels({
      models: [
        {
          ...BIG_PICKLE_MODEL,
          variants: [
            { id: "low" },
            { id: "medium" },
            { id: "high" },
            { id: "xhigh" },
            { id: "max" },
          ],
        },
      ],
      agents: [],
    });

    assert.deepStrictEqual(model?.capabilities?.optionDescriptors, [
      {
        id: "variant",
        label: "Reasoning",
        type: "select",
        currentValue: "medium",
        options: [
          { id: "low", label: "Low" },
          { id: "medium", label: "Medium", isDefault: true },
          { id: "high", label: "High" },
          { id: "xhigh", label: "Extra High" },
          { id: "max", label: "Max" },
        ],
      },
    ]);
  });

  it("hides a catalog-supplied Default sentinel", () => {
    const [model] = flattenOpenCodeModels({
      models: [
        {
          ...BIG_PICKLE_MODEL,
          variants: [{ id: "default" }, { id: "high" }],
        },
      ],
      agents: [],
    });
    const descriptor = model?.capabilities?.optionDescriptors?.find(
      (candidate) => candidate.id === "variant",
    );

    assert.deepStrictEqual(descriptor?.type === "select" ? descriptor.options : [], [
      { id: "high", label: "High", isDefault: true },
    ]);
  });

  it("chooses a concrete fallback default for a thinking toggle", () => {
    const [model] = flattenOpenCodeModels({
      models: [
        {
          ...BIG_PICKLE_MODEL,
          variants: [{ id: "none" }, { id: "thinking" }],
        },
      ],
      agents: [],
    });
    const descriptor = model?.capabilities?.optionDescriptors?.find(
      (candidate) => candidate.id === "variant",
    );

    assert.deepStrictEqual(descriptor, {
      id: "variant",
      label: "Reasoning",
      type: "select",
      currentValue: "thinking",
      options: [
        { id: "none", label: "None" },
        { id: "thinking", label: "Thinking", isDefault: true },
      ],
    });
  });
});

function listOpenCodeSkillsForDirectoryWithRuntime(
  runtime: OpenCode2Runtime.OpenCode2Runtime["Service"],
  cwd: string,
  environment: NodeJS.ProcessEnv,
) {
  return listOpenCodeSkillsForDirectory(OPENCODE2_TEST_SETTINGS, cwd, environment).pipe(
    Effect.provideService(OpenCode2Runtime.OpenCode2Runtime, runtime),
  );
}

function openCode2SkillListRuntime(
  list: () => Promise<unknown>,
): OpenCode2Runtime.OpenCode2Runtime["Service"] {
  return OpenCode2Runtime.OpenCode2Runtime.of({
    connectToOpenCodeServer: () =>
      Effect.succeed({
        exitCode: null,
        external: false,
        password: "test-password",
        url: "http://127.0.0.1:1234",
      }),
    createOpenCodeSdkClient: () =>
      ({
        skill: { list },
      }) as never,
  });
}

function makeSkillListWorkspace(): { environment: NodeJS.ProcessEnv; workspace: string } {
  const tempDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-opencode2-skill-list-"));
  const home = NodePath.join(tempDir, "home");
  const workspace = NodePath.join(tempDir, "workspace");
  NodeFS.mkdirSync(NodePath.join(workspace, ".git"), { recursive: true });
  const skillDir = NodePath.join(workspace, ".opencode", "skills", "disk-skill");
  NodeFS.mkdirSync(skillDir, { recursive: true });
  NodeFS.writeFileSync(
    NodePath.join(skillDir, "SKILL.md"),
    ["---", "name: disk-skill", "description: Disk fallback skill.", "---"].join("\n"),
  );
  return { environment: { HOME: home }, workspace };
}

describe("listOpenCodeSkillsForDirectory", () => {
  it.effect("maps OpenCode skill.list without sending skill bodies", () =>
    Effect.gen(function* () {
      const client = {
        skill: {
          list: async () => ({
            location: { directory: "/workspace" },
            data: [
              {
                id: "git-release",
                name: "git-release",
                description: "Release notes.",
                path: "/cache/opencode/skills/git-release/git-release.md",
                content: "# Do not ship this",
              },
              {
                id: "blank",
                name: "  ",
                path: "/cache/opencode/skills/blank/SKILL.md",
                content: "blank",
              },
            ],
          }),
        },
      };
      const runtime = OpenCode2Runtime.OpenCode2Runtime.of({
        connectToOpenCodeServer: () =>
          Effect.succeed({
            exitCode: null,
            external: false,
            password: "test-password",
            url: "http://127.0.0.1:1234",
          }),
        createOpenCodeSdkClient: () => client as never,
      });

      const skills = yield* listOpenCodeSkillsForDirectory(
        OPENCODE2_TEST_SETTINGS,
        "/workspace",
        {},
      ).pipe(Effect.provideService(OpenCode2Runtime.OpenCode2Runtime, runtime));

      assert.deepEqual(skills, [
        {
          name: "git-release",
          path: "/cache/opencode/skills/git-release/git-release.md",
          enabled: true,
          description: "Release notes.",
          shortDescription: "Release notes.",
        },
      ]);
    }),
  );

  it.effect("accepts a single-wrapped skill.list payload", () =>
    Effect.gen(function* () {
      const client = {
        skill: {
          list: async () => ({
            data: [
              {
                id: "deploy",
                name: "deploy",
                description: "Deploy.",
                path: "/repo/.opencode/skills/deploy/SKILL.md",
                content: "# hidden",
              },
            ],
          }),
        },
      };
      const runtime = OpenCode2Runtime.OpenCode2Runtime.of({
        connectToOpenCodeServer: () =>
          Effect.succeed({
            exitCode: null,
            external: false,
            password: "test-password",
            url: "http://127.0.0.1:1234",
          }),
        createOpenCodeSdkClient: () => client as never,
      });

      const skills = yield* listOpenCodeSkillsForDirectory(
        OPENCODE2_TEST_SETTINGS,
        "/workspace",
        {},
      ).pipe(Effect.provideService(OpenCode2Runtime.OpenCode2Runtime, runtime));

      assert.deepEqual(
        skills.map((skill) => skill.name),
        ["deploy"],
      );
    }),
  );

  it.effect("keeps a successful empty skill.list instead of disk skills", () =>
    Effect.gen(function* () {
      const { environment, workspace } = makeSkillListWorkspace();
      const skills = yield* listOpenCodeSkillsForDirectoryWithRuntime(
        openCode2SkillListRuntime(async () => ({ data: { data: [] } })),
        workspace,
        environment,
      );

      assert.deepEqual(skills, []);
    }),
  );

  it.effect("keeps a mapped-out skill.list instead of disk skills", () =>
    Effect.gen(function* () {
      const { environment, workspace } = makeSkillListWorkspace();
      const skills = yield* listOpenCodeSkillsForDirectoryWithRuntime(
        openCode2SkillListRuntime(async () => ({
          location: { directory: workspace },
          data: [
            {
              id: "blank",
              name: " ",
              path: "/cache/opencode/skills/blank/SKILL.md",
              content: "blank",
            },
          ],
        })),
        workspace,
        environment,
      );

      assert.deepEqual(skills, []);
    }),
  );

  it.effect("falls back to disk skills after the skill.list timeout", () =>
    Effect.gen(function* () {
      const { environment, workspace } = makeSkillListWorkspace();
      const skillsFiber = yield* listOpenCodeSkillsForDirectoryWithRuntime(
        openCode2SkillListRuntime(() => new Promise(() => {})),
        workspace,
        environment,
      ).pipe(Effect.forkChild);

      yield* Effect.yieldNow;
      yield* TestClock.adjust("8 seconds");
      const skills = yield* Fiber.join(skillsFiber);

      assert.deepEqual(
        skills.map((skill) => skill.name),
        ["disk-skill"],
      );
    }),
  );

  it.effect("falls back to disk skills when skill.list fails", () =>
    Effect.gen(function* () {
      const { environment, workspace } = makeSkillListWorkspace();
      const sdkFailure = yield* listOpenCodeSkillsForDirectoryWithRuntime(
        openCode2SkillListRuntime(async () => {
          throw new Error("skill.list failed");
        }),
        workspace,
        environment,
      );
      const connectionFailure = yield* listOpenCodeSkillsForDirectoryWithRuntime(
        failingOpenCodeRuntime("network-failed"),
        workspace,
        environment,
      );

      assert.deepEqual(
        sdkFailure.map((skill) => skill.name),
        ["disk-skill"],
      );
      assert.deepEqual(
        connectionFailure.map((skill) => skill.name),
        ["disk-skill"],
      );
    }),
  );
});
