// @effect-diagnostics nodeBuiltinImport:off
/**
 * OpenCode2Skills — filesystem discovery of OpenCode 2 skills for the `$` picker.
 *
 * OpenCode 2 loads SKILL.md trees from Claude/agent compatibility dirs, then
 * its own config dir, then project `.opencode` dirs. Later sources win on
 * name collisions. The snapshot scans those same locations so the composer
 * picker matches what the spawned CLI would load.
 *
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

const FRONTMATTER_PATTERN = /^---\r?\n([\s\S]*?)\r?\n---(?:\r?\n|$)/;
const NATIVE_SKILL_FOLDERS = ["skill", "skills"] as const;

type SkillFrontmatter =
  | { readonly kind: "missing" }
  | { readonly kind: "malformed" }
  | { readonly kind: "parsed"; readonly name?: string; readonly description?: string };

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
  const name = typeof record.name === "string" ? record.name.trim() : "";
  const description = typeof record.description === "string" ? record.description.trim() : "";
  return {
    kind: "parsed",
    ...(name ? { name } : {}),
    ...(description ? { description } : {}),
  };
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
  directoryName: string,
  scope: OpenCode2SkillScope,
): void {
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

  const parsedName = frontmatter.kind === "parsed" ? frontmatter.name : undefined;
  const name = parsedName ?? directoryName.trim();
  if (!name) {
    return;
  }

  skillsByName.set(name, {
    name,
    path: skillPath,
    enabled: true,
    scope,
    ...(frontmatter.kind === "parsed" && frontmatter.description
      ? { description: frontmatter.description }
      : {}),
  });
}

function scanSkillRoot(
  skillsByName: Map<string, ServerProviderSkill>,
  directory: string,
  scope: OpenCode2SkillScope,
): void {
  for (const entry of [...readDirectory(directory)].sort()) {
    const trimmed = entry.trim();
    if (!trimmed) {
      continue;
    }
    ingestSkillFile(skillsByName, NodePath.join(directory, trimmed, "SKILL.md"), trimmed, scope);
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

  return [...skillsByName.values()].sort((left, right) => left.name.localeCompare(right.name));
}
