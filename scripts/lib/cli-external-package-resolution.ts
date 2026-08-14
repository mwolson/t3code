// @effect-diagnostics nodeBuiltinImport:off - Isolated pnpm/aube sibling deps are only visible after Node realpath; Effect Path cannot replace that resolver.
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

export interface InstalledPackageManifest {
  readonly name: string;
  readonly manifestPath: string;
  readonly dependencies?: Readonly<Record<string, string>>;
  readonly optionalDependencies?: Readonly<Record<string, string>>;
  readonly peerDependencies?: Readonly<Record<string, string>>;
}

/**
 * Locate `name`'s package.json from an owning manifest without assuming a
 * package-manager directory shape.
 *
 * `require("<name>/package.json")` from a random file is not enough: isolated
 * layouts hide transitive packages from the caller, and `exports` can refuse
 * the `package.json` subpath. Anchor at the owner, then realpath that owner so
 * sibling virtual-store dependencies remain visible.
 */
export function resolvePackageManifestPath(
  name: string,
  fromManifestPath: string,
): string | undefined {
  for (const base of resolutionBases(fromManifestPath)) {
    const found = resolveFromBase(name, base);
    if (found !== undefined) return canonicalizePath(found);
  }
  return undefined;
}

export function declaredPackageDependencyNames(
  manifest: Pick<
    InstalledPackageManifest,
    "dependencies" | "optionalDependencies" | "peerDependencies"
  >,
): ReadonlyArray<string> {
  return [
    ...Object.keys(manifest.dependencies ?? {}),
    ...Object.keys(manifest.optionalDependencies ?? {}),
    ...Object.keys(manifest.peerDependencies ?? {}),
  ];
}

export function collectInstalledPackageManifests(
  seedManifestPaths: ReadonlyArray<string>,
): ReadonlyArray<InstalledPackageManifest> {
  const installed: InstalledPackageManifest[] = [];
  const queue = seedManifestPaths.map((seed) => canonicalizePath(seed));
  const seen = new Set<string>();

  while (queue.length > 0) {
    const manifestPath = queue.pop();
    if (manifestPath === undefined || seen.has(manifestPath)) continue;
    seen.add(manifestPath);

    const manifest = readInstalledManifest(manifestPath);
    if (manifest === undefined) continue;
    // Keep every reachable instance. Two versions of the same name can declare
    // different runtime dependencies, so first-one-wins by name would hide a
    // violating copy.
    installed.push(manifest);

    for (const dependency of declaredPackageDependencyNames(manifest)) {
      const dependencyManifest = resolvePackageManifestPath(dependency, manifestPath);
      if (dependencyManifest !== undefined) queue.push(dependencyManifest);
    }
  }

  return installed;
}

function resolveFromBase(name: string, fromManifestPath: string): string | undefined {
  try {
    const found = NodeModule.findPackageJSON(name, NodeURL.pathToFileURL(fromManifestPath));
    if (typeof found === "string") return found;
  } catch {
    // Isolated layouts miss siblings until the owner is realpathed. Hidden
    // exports and packages with no main fall through to createRequire.
  }

  const requireFromOwner = NodeModule.createRequire(fromManifestPath);
  try {
    return requireFromOwner.resolve(`${name}/package.json`);
  } catch {
    // package.json may be excluded from exports.
  }

  try {
    return findOwningManifest(requireFromOwner.resolve(name), name);
  } catch {
    return undefined;
  }
}

function findOwningManifest(entryPath: string, name: string): string | undefined {
  let directory = NodePath.dirname(entryPath);
  while (true) {
    const candidate = NodePath.join(directory, "package.json");
    const manifest = readInstalledManifest(candidate);
    if (manifest?.name === name) return candidate;
    const parent = NodePath.dirname(directory);
    if (parent === directory) return undefined;
    directory = parent;
  }
}

function resolutionBases(fromManifestPath: string): ReadonlyArray<string> {
  const real = canonicalizePath(fromManifestPath);
  if (real === fromManifestPath) return [fromManifestPath];
  // Prefer the realpath. Isolated stores put dependencies next to the real
  // package, and a workspace symlink does not see those siblings.
  return [real, fromManifestPath];
}

function canonicalizePath(path: string): string {
  try {
    return NodeFS.realpathSync(path);
  } catch {
    return path;
  }
}

function readInstalledManifest(manifestPath: string): InstalledPackageManifest | undefined {
  let raw: unknown;
  try {
    raw = JSON.parse(NodeFS.readFileSync(manifestPath, "utf8"));
  } catch {
    return undefined;
  }
  if (raw === null || typeof raw !== "object" || Array.isArray(raw)) return undefined;

  const record = raw as Record<string, unknown>;
  if (typeof record.name !== "string" || record.name.length === 0) return undefined;

  const dependencies = stringRecord(record.dependencies);
  const optionalDependencies = stringRecord(record.optionalDependencies);
  const peerDependencies = stringRecord(record.peerDependencies);
  return {
    name: record.name,
    manifestPath,
    ...(dependencies === undefined ? {} : { dependencies }),
    ...(optionalDependencies === undefined ? {} : { optionalDependencies }),
    ...(peerDependencies === undefined ? {} : { peerDependencies }),
  };
}

function stringRecord(value: unknown): Record<string, string> | undefined {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return undefined;

  const result: Record<string, string> = {};
  for (const [key, entry] of Object.entries(value)) {
    if (typeof entry === "string") result[key] = entry;
  }
  return Object.keys(result).length > 0 ? result : undefined;
}
