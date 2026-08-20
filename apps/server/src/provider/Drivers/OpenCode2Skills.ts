// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalFetch:off
/**
 * OpenCode2Skills — filesystem discovery of OpenCode 2 skills for the `$` picker.
 *
 * OpenCode 2 ids come from the path, not frontmatter `name`. Later sources win.
 * Discovery is plain Node fs so provider status probes can run it under
 * TestClock without providing NodeServices.
 *
 * @module provider/Drivers/OpenCode2Skills
 */
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import type { ServerProviderSkill } from "@t3tools/contracts";
import { parse as parseYamlDocument } from "yaml";

type OpenCode2SkillScope = "user" | "project";

export interface OpenCode2HttpCatalogRef {
  readonly url: string;
  readonly scope: OpenCode2SkillScope;
}

const FRONTMATTER_PATTERN = /^---\r?\n([\s\S]*?)\r?\n---(?:\r?\n|$)/;
const NATIVE_SKILL_FOLDERS = ["skill", "skills"] as const;
const CONFIG_FILE_NAMES = ["opencode.json", "opencode.jsonc"] as const;

type SkillFrontmatter =
  | { readonly kind: "missing" }
  | { readonly kind: "malformed" }
  | {
      readonly kind: "parsed";
      readonly displayName?: string;
      readonly description?: string;
      readonly hiddenFromCatalog: boolean;
    };

function parseSkillFrontmatter(contents: string): SkillFrontmatter {
  const match = FRONTMATTER_PATTERN.exec(contents);
  if (!match) {
    return { kind: "missing" };
  }

  let parsed: unknown;
  try {
    parsed = parseYamlDocument(match[1] ?? "");
  } catch {
    return { kind: "malformed" };
  }
  if (typeof parsed !== "object" || parsed === null) {
    return { kind: "malformed" };
  }

  const record = parsed as Record<string, unknown>;
  const displayName = typeof record.name === "string" ? record.name.trim() : "";
  const description = typeof record.description === "string" ? record.description.trim() : "";
  return {
    kind: "parsed",
    hiddenFromCatalog: isHiddenFromCatalog(record),
    ...(displayName ? { displayName } : {}),
    ...(description ? { description } : {}),
  };
}

function isHiddenFromCatalog(record: Record<string, unknown>): boolean {
  const metadata = record.metadata;
  if (typeof metadata === "object" && metadata !== null) {
    const slashOverride = (metadata as Record<string, unknown>)["opencode/slash"];
    if (typeof slashOverride === "boolean") {
      return slashOverride === false;
    }
    if (typeof slashOverride === "string") {
      const normalized = slashOverride.trim().toLowerCase();
      if (normalized === "false") return true;
      if (normalized === "true") return false;
    }
  }
  return record.slash === false;
}

function envHome(environment: NodeJS.ProcessEnv, allowProcessHome: boolean): string {
  const fromEnv = environment.HOME?.trim() ?? "";
  if (fromEnv.length > 0) {
    return fromEnv;
  }
  return allowProcessHome ? NodeOS.homedir() : "";
}

function resolveOpenCode2ConfigDir(
  environment: NodeJS.ProcessEnv,
  cwd: string | undefined,
  allowProcessHome: boolean,
): string | null {
  const configDirEnv = environment.OPENCODE_CONFIG_DIR?.trim() ?? "";
  if (configDirEnv.length > 0) {
    return cwd ? NodePath.resolve(cwd, configDirEnv) : NodePath.resolve(configDirEnv);
  }
  const xdgConfigHome = environment.XDG_CONFIG_HOME?.trim() ?? "";
  if (xdgConfigHome.length > 0) {
    return NodePath.join(xdgConfigHome, "opencode");
  }
  const home = envHome(environment, allowProcessHome);
  if (home.length === 0) {
    return null;
  }
  return NodePath.join(home, ".config", "opencode");
}

function pathExists(target: string): boolean {
  try {
    NodeFS.accessSync(target);
    return true;
  } catch {
    return false;
  }
}

function readDirectory(directory: string): ReadonlyArray<string> {
  try {
    return NodeFS.readdirSync(directory);
  } catch {
    return [];
  }
}

function collectWorkspaceAncestors(cwd: string): ReadonlyArray<string> {
  const chain: string[] = [];
  const seen = new Set<string>();
  let current = NodePath.resolve(cwd);
  let foundGit = false;

  while (!seen.has(current)) {
    seen.add(current);
    chain.push(current);
    if (pathExists(NodePath.join(current, ".git"))) {
      foundGit = true;
      break;
    }
    const parent = NodePath.resolve(current, "..");
    if (parent === current) {
      break;
    }
    current = parent;
  }

  if (!foundGit) {
    return [NodePath.resolve(cwd)];
  }
  return chain.toReversed();
}

function ingestSkillFile(
  skillsByName: Map<string, ServerProviderSkill>,
  skillPath: string,
  skillId: string,
  scope: OpenCode2SkillScope,
): void {
  const id = skillId.trim();
  if (!id) {
    return;
  }

  let contents: string;
  try {
    contents = NodeFS.readFileSync(skillPath, "utf8");
  } catch {
    return;
  }

  const frontmatter = parseSkillFrontmatter(contents);
  if (frontmatter.kind === "malformed") {
    return;
  }
  if (frontmatter.kind === "parsed" && frontmatter.hiddenFromCatalog) {
    skillsByName.delete(id);
    return;
  }

  const displayName = frontmatter.kind === "parsed" ? frontmatter.displayName : undefined;
  const description = frontmatter.kind === "parsed" ? frontmatter.description : undefined;
  skillsByName.set(id, {
    name: id,
    path: skillPath,
    enabled: true,
    scope,
    ...(displayName && displayName !== id ? { displayName } : {}),
    ...(description ? { description } : {}),
  });
}

function collectNestedSkillMarkdown(
  directory: string,
  seen: Set<string>,
  files: Array<{ path: string; id: string }>,
): void {
  let realPath = directory;
  try {
    realPath = NodeFS.realpathSync(directory);
  } catch {
    return;
  }
  if (seen.has(realPath)) {
    return;
  }
  seen.add(realPath);

  const skillPath = NodePath.join(directory, "SKILL.md");
  if (pathExists(skillPath)) {
    files.push({ path: skillPath, id: NodePath.basename(directory) });
  }
  for (const entry of [...readDirectory(directory)].sort()) {
    const child = NodePath.join(directory, entry);
    try {
      if (!NodeFS.statSync(child).isDirectory()) {
        continue;
      }
    } catch {
      continue;
    }
    collectNestedSkillMarkdown(child, seen, files);
  }
}

function scanSkillRoot(
  skillsByName: Map<string, ServerProviderSkill>,
  directory: string,
  scope: OpenCode2SkillScope,
): void {
  const rootMarkdown: Array<{ path: string; id: string }> = [];
  for (const entry of [...readDirectory(directory)].sort()) {
    if (!entry.toLowerCase().endsWith(".md")) {
      continue;
    }
    const fullPath = NodePath.join(directory, entry);
    try {
      if (!NodeFS.statSync(fullPath).isFile()) {
        continue;
      }
    } catch {
      continue;
    }
    rootMarkdown.push({ path: fullPath, id: entry.slice(0, -3) });
  }
  for (const file of rootMarkdown) {
    ingestSkillFile(skillsByName, file.path, file.id, scope);
  }

  const nested: Array<{ path: string; id: string }> = [];
  collectNestedSkillMarkdown(directory, new Set<string>(), nested);
  for (const file of nested) {
    ingestSkillFile(skillsByName, file.path, file.id, scope);
  }
}

function scanNativeSkillFolders(
  skillsByName: Map<string, ServerProviderSkill>,
  root: string,
  scope: OpenCode2SkillScope,
): void {
  for (const folder of NATIVE_SKILL_FOLDERS) {
    scanSkillRoot(skillsByName, NodePath.join(root, folder), scope);
  }
}

function parseJsoncObject(text: string): Record<string, unknown> | null {
  const withoutBlockComments = text.replace(/\/\*[\s\S]*?\*\//g, "");
  const withoutLineComments = withoutBlockComments.replace(/(^|[^:])\/\/.*$/gm, "$1");
  try {
    const parsed: unknown = JSON.parse(withoutLineComments);
    if (typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)) {
      return parsed as Record<string, unknown>;
    }
  } catch {
    return null;
  }
  return null;
}

function readConfigSkillsEntries(configPath: string): ReadonlyArray<string> {
  let contents: string;
  try {
    contents = NodeFS.readFileSync(configPath, "utf8");
  } catch {
    return [];
  }
  const parsed = parseJsoncObject(contents);
  const skills = parsed?.skills;
  if (!Array.isArray(skills)) {
    return [];
  }
  return skills.filter(
    (entry): entry is string => typeof entry === "string" && entry.trim().length > 0,
  );
}

function expandConfiguredSkillSource(
  entry: string,
  cwd: string | undefined,
  home: string,
):
  | { readonly kind: "dir"; readonly path: string }
  | { readonly kind: "url"; readonly url: string }
  | null {
  const trimmed = entry.trim();
  if (trimmed.startsWith("http://") || trimmed.startsWith("https://")) {
    return { kind: "url", url: trimmed };
  }
  if (trimmed.startsWith("~/")) {
    if (home.length === 0) {
      return null;
    }
    return { kind: "dir", path: NodePath.join(home, trimmed.slice(2)) };
  }
  if (NodePath.isAbsolute(trimmed)) {
    return { kind: "dir", path: trimmed };
  }
  if (cwd === undefined || cwd.trim().length === 0) {
    return null;
  }
  return { kind: "dir", path: NodePath.resolve(cwd, trimmed) };
}

function collectConfiguredSkillSources(
  cwd: string | undefined,
  environment: NodeJS.ProcessEnv,
  allowProcessHome: boolean,
): {
  readonly directories: ReadonlyArray<{ path: string; scope: OpenCode2SkillScope }>;
  readonly catalogs: ReadonlyArray<OpenCode2HttpCatalogRef>;
} {
  const home = envHome(environment, allowProcessHome);
  const configDir = resolveOpenCode2ConfigDir(environment, cwd, allowProcessHome);
  const ancestors =
    cwd === undefined || cwd.trim().length === 0 ? [] : collectWorkspaceAncestors(cwd);
  const configPaths: Array<{ path: string; scope: OpenCode2SkillScope }> = [];
  if (configDir !== null) {
    for (const name of CONFIG_FILE_NAMES) {
      configPaths.push({ path: NodePath.join(configDir, name), scope: "user" });
    }
  }
  for (const ancestor of ancestors) {
    for (const name of CONFIG_FILE_NAMES) {
      configPaths.push({ path: NodePath.join(ancestor, name), scope: "project" });
      configPaths.push({ path: NodePath.join(ancestor, ".opencode", name), scope: "project" });
    }
  }

  const directories: Array<{ path: string; scope: OpenCode2SkillScope }> = [];
  const catalogs: Array<OpenCode2HttpCatalogRef> = [];
  for (const config of configPaths) {
    for (const entry of readConfigSkillsEntries(config.path)) {
      const expanded = expandConfiguredSkillSource(entry, cwd, home);
      if (expanded === null) {
        continue;
      }
      if (expanded.kind === "url") {
        catalogs.push({ url: expanded.url, scope: config.scope });
        continue;
      }
      directories.push({ path: expanded.path, scope: config.scope });
    }
  }
  return { directories, catalogs };
}

/**
 * HTTP catalog URLs from OpenCode config files, in config priority order.
 */
export function collectOpenCode2SkillHttpCatalogs(
  cwd?: string,
  environment?: NodeJS.ProcessEnv,
): ReadonlyArray<OpenCode2HttpCatalogRef> {
  const resolvedEnvironment = environment ?? process.env;
  return collectConfiguredSkillSources(cwd, resolvedEnvironment, environment === undefined)
    .catalogs;
}

/**
 * Map an OpenCode HTTP catalog `index.json` body to picker skills.
 */
export async function loadOpenCode2HttpCatalogSkills(
  catalogs: ReadonlyArray<OpenCode2HttpCatalogRef>,
): Promise<ReadonlyArray<ServerProviderSkill>> {
  const skills: Array<ServerProviderSkill> = [];
  for (const catalog of catalogs) {
    try {
      const response = await fetch(catalog.url, { signal: AbortSignal.timeout(2000) });
      if (!response.ok) {
        continue;
      }
      const payload: unknown = await response.json();
      skills.push(...parseOpenCode2HttpCatalogIndex(catalog.url, payload, catalog.scope));
    } catch {
      continue;
    }
  }
  return skills;
}

export function parseOpenCode2HttpCatalogIndex(
  catalogUrl: string,
  payload: unknown,
  scope: OpenCode2SkillScope = "user",
): ReadonlyArray<ServerProviderSkill> {
  const record =
    typeof payload === "object" && payload !== null ? (payload as Record<string, unknown>) : null;
  const entries = record?.skills;
  if (!Array.isArray(entries)) {
    return [];
  }
  const skills: Array<ServerProviderSkill> = [];
  for (const entry of entries) {
    if (typeof entry !== "object" || entry === null) {
      continue;
    }
    const name =
      typeof (entry as { name?: unknown }).name === "string"
        ? (entry as { name: string }).name.trim()
        : "";
    if (!name) {
      continue;
    }
    const description =
      typeof (entry as { description?: unknown }).description === "string"
        ? (entry as { description: string }).description.trim()
        : "";
    const base = catalogUrl.endsWith("/") ? catalogUrl : `${catalogUrl}/`;
    skills.push({
      name,
      path: `${base}${name}`,
      enabled: true,
      scope,
      ...(description ? { description } : {}),
    });
  }
  return skills;
}

/**
 * Enumerate OpenCode 2 skills in CLI load order. Missing roots and malformed
 * SKILL.md files are skipped. On name collisions, later sources win.
 */
export function discoverOpenCode2Skills(
  cwd?: string,
  environment?: NodeJS.ProcessEnv,
): ReadonlyArray<ServerProviderSkill> {
  const resolvedEnvironment = environment ?? process.env;
  const allowProcessHome = environment === undefined;
  const home = envHome(resolvedEnvironment, allowProcessHome);
  const configDir = resolveOpenCode2ConfigDir(resolvedEnvironment, cwd, allowProcessHome);
  const skillsByName = new Map<string, ServerProviderSkill>();
  const ancestors =
    cwd === undefined || cwd.trim().length === 0 ? [] : collectWorkspaceAncestors(cwd);
  const configured = collectConfiguredSkillSources(cwd, resolvedEnvironment, allowProcessHome);

  if (home.length > 0) {
    scanSkillRoot(skillsByName, NodePath.join(home, ".claude", "skills"), "user");
  }
  for (const ancestor of ancestors) {
    scanSkillRoot(skillsByName, NodePath.join(ancestor, ".claude", "skills"), "project");
  }
  if (home.length > 0) {
    scanSkillRoot(skillsByName, NodePath.join(home, ".agents", "skills"), "user");
  }
  for (const ancestor of ancestors) {
    scanSkillRoot(skillsByName, NodePath.join(ancestor, ".agents", "skills"), "project");
  }
  if (configDir !== null) {
    scanNativeSkillFolders(skillsByName, configDir, "user");
  }
  for (const ancestor of ancestors) {
    scanNativeSkillFolders(skillsByName, NodePath.join(ancestor, ".opencode"), "project");
  }
  for (const directory of configured.directories) {
    scanSkillRoot(skillsByName, directory.path, directory.scope);
  }

  return [...skillsByName.values()].sort((left, right) => left.name.localeCompare(right.name));
}
