import * as NodeURL from "node:url";

import { assert, describe, it } from "@effect/vitest";

import {
  collectInstalledPackageManifests,
  declaredPackageDependencyNames,
} from "./cli-external-package-resolution.ts";
import {
  CLI_EXTERNAL_PACKAGE_PREFIXES,
  CLI_EXTERNAL_PACKAGE_UNPACK_GLOBS,
  CLI_RUNTIME_EXTERNAL_PREFIXES,
  findInlinedExternalPackages,
  shouldBundleCliDependency,
} from "./cli-external-packages.ts";

describe("shouldBundleCliDependency", () => {
  it("bundles ordinary runtime dependencies", () => {
    for (const id of ["effect", "@effect/platform", "hono", "@t3tools/shared/hostProcess"]) {
      assert.strictEqual(shouldBundleCliDependency(id), true, id);
    }
  });

  it("never bundles node: builtins", () => {
    assert.strictEqual(shouldBundleCliDependency("node:fs"), false);
  });

  it("leaves native addons and their dlopen wrappers external", () => {
    for (const id of [
      "node-pty",
      "ffi-rs",
      "@yuuang/ffi-rs-win32-x64-msvc",
      "@ff-labs/fff-node",
      "@clerk/electron-passkeys",
      "msgpackr-extract",
      "@msgpackr-extract/msgpackr-extract-win32-x64",
    ]) {
      assert.strictEqual(shouldBundleCliDependency(id), false, id);
    }
  });

  it("leaves bun-only entry points external", () => {
    assert.strictEqual(shouldBundleCliDependency("@effect/platform-bun"), false);
    assert.strictEqual(shouldBundleCliDependency("@effect/sql-sqlite-bun"), false);
  });

  // The real package is `node-gyp-build-optional-packages`, reached by prefix.
  // Matching it as external while failing to unpack it is invisible on the
  // Windows primary (which reads app.asar) and breaks only under WSL.
  it("treats prefix-matched siblings as external", () => {
    assert.strictEqual(shouldBundleCliDependency("node-gyp-build-optional-packages"), false);
  });
});

describe("CLI_EXTERNAL_PACKAGE_UNPACK_GLOBS", () => {
  it("unpacks every external prefix from both the top level and the pnpm store", () => {
    for (const prefix of CLI_EXTERNAL_PACKAGE_PREFIXES) {
      assert.include(CLI_EXTERNAL_PACKAGE_UNPACK_GLOBS, `node_modules/${prefix}*/**/*`, prefix);
      assert.include(
        CLI_EXTERNAL_PACKAGE_UNPACK_GLOBS,
        `node_modules/.pnpm/**/node_modules/${prefix}*/**/*`,
        prefix,
      );
    }
  });

  // Without the trailing `*` the globs stop covering prefix-matched siblings,
  // which is exactly how a package ends up external but not unpacked.
  it("keeps the trailing wildcard that matches prefix siblings", () => {
    assert.include(CLI_EXTERNAL_PACKAGE_UNPACK_GLOBS, "node_modules/node-gyp-build*/**/*");
  });
});

// The failure this guards is invisible on Windows and fatal under WSL.
//
// An external package is loaded from the real filesystem, so its own `require`
// also resolves from the real filesystem. If one of its dependencies was
// bundled away instead of left external, that dependency exists only inside
// app.asar — which the Windows primary reads transparently under
// ELECTRON_RUN_AS_NODE, and plain `node` under WSL cannot.
//
// Found the hard way: node-gyp-build-optional-packages requires detect-libc,
// which was bundled. Windows was fine; WSL got MODULE_NOT_FOUND.
describe("external package dependency closure", () => {
  // Do not scan a package-manager store directory. pnpm's `.pnpm` tree is
  // absent under aube, and `require("<name>/package.json")` from this file
  // cannot see isolated transitives or packages that hide `package.json` in
  // `exports`. Walk declared dependencies from the workspace packages that own
  // the CLI-relevant native families, resolving each name from its owner.
  const installed = collectInstalledPackageManifests([
    NodeURL.fileURLToPath(new URL("../../apps/server/package.json", import.meta.url)),
    NodeURL.fileURLToPath(new URL("../../apps/desktop/package.json", import.meta.url)),
  ]);

  // Runtime-external only. The build-only entries resolve `bun:*` and are never
  // loaded by Node, so their closure genuinely does not need to be external.
  const isRuntimeExternal = (name: string) =>
    CLI_RUNTIME_EXTERNAL_PREFIXES.some((prefix) => name.startsWith(prefix));

  it("finds the runtime-external packages on disk", () => {
    const found = [...new Set(installed.map((manifest) => manifest.name))].filter(
      isRuntimeExternal,
    );

    // Without this the closure check below can pass vacuously: if nothing is
    // read, nothing is checked. These are the packages whose closure actually
    // broke WSL, so require them by name.
    for (const required of ["node-pty", "node-gyp-build-optional-packages", "detect-libc"]) {
      assert.ok(
        found.includes(required),
        `expected ${required} among CLI-reachable installed packages; the closure check is only meaningful if it can read these (found ${found.length})`,
      );
    }
  });

  it("keeps every runtime dependency of an external package external too", () => {
    const violations: string[] = [];
    // Check every reachable instance, not the first copy of each name. Two
    // versions can declare different dependencies. Seeded from what is actually
    // installed and matches a prefix, so scoped prefixes like "@yuuang/" and
    // "@ff-labs/" are covered too.
    for (const manifest of installed) {
      if (!isRuntimeExternal(manifest.name)) continue;

      for (const dependency of declaredPackageDependencyNames(manifest)) {
        if (!isRuntimeExternal(dependency)) {
          violations.push(`${manifest.name} -> ${dependency}`);
        }
      }
    }

    assert.deepStrictEqual(
      violations,
      [],
      `these dependencies of external packages would be bundled away and fail to resolve under WSL: ${violations.join(", ")}`,
    );
  });
});

// Configuring the bundler is not the same as checking what it emitted. These
// exercise the scanner against the marker shape rolldown actually produces.
describe("findInlinedExternalPackages", () => {
  const region = (path: string) => `//#region ${path}
var x = 1;
//#endregion
`;

  it("flags an external package that was inlined", () => {
    const source =
      region("../../node_modules/.pnpm/detect-libc@2.1.2/node_modules/detect-libc/lib/process.js") +
      region(
        "../../node_modules/.pnpm/msgpackr-extract@3.0.4/node_modules/msgpackr-extract/index.js",
      );
    const result = findInlinedExternalPackages(source);

    assert.deepStrictEqual(result.inlined, ["detect-libc", "msgpackr-extract"]);
    assert.strictEqual(result.regionCount, 2);
  });

  it("flags scoped external packages", () => {
    const result = findInlinedExternalPackages(
      region("../../node_modules/@ff-labs/fff-node/dist/src/index.js"),
    );
    assert.deepStrictEqual(result.inlined, ["@ff-labs/fff-node"]);
  });

  it("ignores packages that are meant to be bundled", () => {
    const source =
      region("../../node_modules/.pnpm/effect@4.0.0/node_modules/effect/dist/index.js") +
      region("../../src/server/main.ts");
    const result = findInlinedExternalPackages(source);

    assert.deepStrictEqual(result.inlined, []);
    assert.strictEqual(result.regionCount, 2);
  });

  // regionCount is what separates "clean" from "this scan went blind because the
  // marker format changed". A caller that ignores it gets a vacuous pass.
  // The scan has to answer both directions. Checking only that externals are
  // absent still passes on a bundle that externalized everything, which is the
  // failure this whole change prevents.
  it("reports the packages that were inlined, not just the violations", () => {
    const source =
      region("../../node_modules/.pnpm/effect@4.0.0/node_modules/effect/dist/index.js") +
      region("../../node_modules/.pnpm/yaml@2.4.0/node_modules/yaml/dist/index.js") +
      region("../../src/server/main.ts");
    const result = findInlinedExternalPackages(source);

    assert.deepStrictEqual(result.inlinedPackages, ["effect", "yaml"]);
    assert.deepStrictEqual(result.inlined, []);
  });

  it("does not report the pnpm store directory as a package", () => {
    const result = findInlinedExternalPackages(
      region("../../node_modules/.pnpm/effect@4.0.0/node_modules/effect/dist/index.js"),
    );
    assert.deepStrictEqual(result.inlinedPackages, ["effect"]);
  });

  it("reports no regions when the marker format is absent", () => {
    const result = findInlinedExternalPackages("var x = 1; // node_modules/detect-libc/lib.js");
    assert.strictEqual(result.regionCount, 0);
    assert.deepStrictEqual(result.inlined, []);
  });
});
