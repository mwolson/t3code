// @effect-diagnostics nodeBuiltinImport:off - Fixture graphs need real node_modules layouts, including isolated-store sibling symlinks.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { afterEach, assert, describe, it } from "@effect/vitest";

import {
  collectInstalledPackageManifests,
  declaredPackageDependencyNames,
  resolvePackageManifestPath,
} from "./cli-external-package-resolution.ts";

const temporaryDirectories: string[] = [];

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    NodeFS.rmSync(directory, { recursive: true, force: true });
  }
});

function makeTemporaryDirectory(): string {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3code-cli-ext-res-"));
  temporaryDirectories.push(directory);
  return directory;
}

function writeFile(path: string, contents: string): void {
  NodeFS.mkdirSync(NodePath.dirname(path), { recursive: true });
  NodeFS.writeFileSync(path, contents);
}

function writeJson(path: string, value: unknown): void {
  writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}

describe("resolvePackageManifestPath", () => {
  it("resolves a package whose exports hide package.json", () => {
    const root = makeTemporaryDirectory();
    const consumerManifest = NodePath.join(root, "consumer", "package.json");
    writeJson(consumerManifest, {
      name: "consumer",
      dependencies: { "hidden-exports": "1.0.0" },
    });
    writeJson(NodePath.join(root, "consumer", "node_modules", "hidden-exports", "package.json"), {
      name: "hidden-exports",
      exports: { ".": "./index.js" },
    });
    writeFile(NodePath.join(root, "consumer", "node_modules", "hidden-exports", "index.js"), "");

    const resolved = resolvePackageManifestPath("hidden-exports", consumerManifest);
    assert.ok(resolved?.endsWith(`${NodePath.sep}hidden-exports${NodePath.sep}package.json`));
  });

  it("resolves an isolated-store sibling after realpathing the owner symlink", () => {
    const root = makeTemporaryDirectory();
    const storeOwner = NodePath.join(root, "store", "owner@1.0.0", "node_modules");
    writeJson(NodePath.join(storeOwner, "owner", "package.json"), {
      name: "owner",
      dependencies: { sibling: "1.0.0" },
    });
    writeJson(NodePath.join(storeOwner, "sibling", "package.json"), {
      name: "sibling",
      exports: { ".": "./index.js" },
    });
    writeFile(NodePath.join(storeOwner, "sibling", "index.js"), "");

    const consumerManifest = NodePath.join(root, "consumer", "package.json");
    writeJson(consumerManifest, {
      name: "consumer",
      dependencies: { owner: "1.0.0" },
    });
    const consumerOwner = NodePath.join(root, "consumer", "node_modules", "owner");
    NodeFS.mkdirSync(NodePath.dirname(consumerOwner), { recursive: true });
    NodeFS.symlinkSync(NodePath.join(storeOwner, "owner"), consumerOwner);

    const installed = collectInstalledPackageManifests([consumerManifest]);
    assert.ok(installed.some((manifest) => manifest.name === "owner"));
    assert.ok(installed.some((manifest) => manifest.name === "sibling"));
  });

  it("returns undefined for a missing optional dependency", () => {
    const root = makeTemporaryDirectory();
    const ownerManifest = NodePath.join(root, "owner", "package.json");
    writeJson(ownerManifest, {
      name: "owner",
      optionalDependencies: { "not-installed": "1.0.0" },
    });

    assert.equal(resolvePackageManifestPath("not-installed", ownerManifest), undefined);
    const installed = collectInstalledPackageManifests([ownerManifest]);
    assert.deepStrictEqual(
      installed.map((manifest) => manifest.name),
      ["owner"],
    );
  });

  it("walks through a non-external intermediate to reach a nested package", () => {
    const root = makeTemporaryDirectory();
    const consumerManifest = NodePath.join(root, "consumer", "package.json");
    writeJson(consumerManifest, {
      name: "consumer",
      dependencies: { mid: "1.0.0" },
    });
    writeJson(NodePath.join(root, "consumer", "node_modules", "mid", "package.json"), {
      name: "mid",
      dependencies: { leaf: "1.0.0" },
    });
    writeJson(
      NodePath.join(
        root,
        "consumer",
        "node_modules",
        "mid",
        "node_modules",
        "leaf",
        "package.json",
      ),
      {
        name: "leaf",
      },
    );

    const installed = collectInstalledPackageManifests([consumerManifest]);
    assert.ok(installed.some((manifest) => manifest.name === "mid"));
    assert.ok(installed.some((manifest) => manifest.name === "leaf"));
  });

  it("resolves a package with hidden package.json, no main, and no default export", () => {
    const root = makeTemporaryDirectory();
    const consumerManifest = NodePath.join(root, "consumer", "package.json");
    writeJson(consumerManifest, {
      name: "consumer",
      dependencies: { "hidden-no-main": "1.0.0" },
    });
    writeJson(NodePath.join(root, "consumer", "node_modules", "hidden-no-main", "package.json"), {
      name: "hidden-no-main",
      exports: { "./readme": "./readme.txt" },
    });
    writeFile(NodePath.join(root, "consumer", "node_modules", "hidden-no-main", "readme.txt"), "");

    const resolved = resolvePackageManifestPath("hidden-no-main", consumerManifest);
    assert.ok(resolved?.endsWith(`${NodePath.sep}hidden-no-main${NodePath.sep}package.json`));
  });

  it("keeps every reachable instance when two versions share a name", () => {
    const root = makeTemporaryDirectory();
    const consumerManifest = NodePath.join(root, "consumer", "package.json");
    writeJson(consumerManifest, {
      name: "consumer",
      dependencies: { left: "1.0.0", right: "1.0.0" },
    });
    writeJson(NodePath.join(root, "consumer", "node_modules", "left", "package.json"), {
      name: "left",
      dependencies: { twinned: "1.0.0" },
    });
    writeJson(
      NodePath.join(
        root,
        "consumer",
        "node_modules",
        "left",
        "node_modules",
        "twinned",
        "package.json",
      ),
      { name: "twinned" },
    );
    writeJson(NodePath.join(root, "consumer", "node_modules", "right", "package.json"), {
      name: "right",
      dependencies: { twinned: "2.0.0" },
    });
    writeJson(
      NodePath.join(
        root,
        "consumer",
        "node_modules",
        "right",
        "node_modules",
        "twinned",
        "package.json",
      ),
      {
        name: "twinned",
        dependencies: { ordinary: "1.0.0" },
      },
    );

    const installed = collectInstalledPackageManifests([consumerManifest]);
    const twinned = installed.filter((manifest) => manifest.name === "twinned");
    assert.equal(twinned.length, 2);
    assert.ok(
      twinned.some((manifest) => declaredPackageDependencyNames(manifest).includes("ordinary")),
    );
  });
});
