/**
 * OpenCode tool, permission, and turn-boundary mapping shared by every
 * OpenCode adapter. These classifiers are about vendor vocabulary, not a
 * particular server generation.
 *
 * @module orchestration-v2/Adapters/openCodeProjection
 */
import type {
  OrchestrationV2ExecutionNode,
  OrchestrationV2ProviderTurn,
  OrchestrationV2TurnItem,
  ProviderRequestKind,
} from "@t3tools/contracts";

import type { ProviderAdapterV2RuntimePolicy } from "../ProviderAdapter.ts";

export type PermissionRuleset = ReadonlyArray<{
  readonly action: "allow" | "ask" | "deny";
  readonly pattern: string;
  readonly permission: string;
}>;

type TerminalTurnStatus = Extract<
  OrchestrationV2ProviderTurn["status"],
  "completed" | "interrupted" | "failed" | "cancelled"
>;

type OpenCodePermissionRequestKind = Extract<
  ProviderRequestKind,
  "command" | "file-read" | "file-change"
>;

function nonEmptyString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value : undefined;
}

function recordValue(input: unknown, key: string): unknown {
  return typeof input === "object" && input !== null && key in input
    ? (input as Record<string, unknown>)[key]
    : undefined;
}

function recordString(input: unknown, ...keys: ReadonlyArray<string>): string | undefined {
  for (const key of keys) {
    const value = nonEmptyString(recordValue(input, key));
    if (value !== undefined) return value;
  }
  return undefined;
}

export function openCodePermissionRequestKind(
  permission: string,
  toolName?: string,
): OpenCodePermissionRequestKind {
  const normalized = permission.toLowerCase();
  const normalizedTool = toolName?.toLowerCase() ?? "";
  if (
    normalized === "edit" ||
    normalized === "write" ||
    normalized === "patch" ||
    normalized === "apply_patch" ||
    normalizedTool === "edit" ||
    normalizedTool === "write" ||
    normalizedTool === "patch" ||
    normalizedTool === "apply_patch"
  ) {
    return "file-change";
  }
  if (
    normalized === "read" ||
    normalized === "glob" ||
    normalized === "grep" ||
    normalized === "lsp" ||
    normalized === "external_directory" ||
    normalizedTool === "read" ||
    normalizedTool.includes("glob") ||
    normalizedTool.includes("grep") ||
    normalizedTool.includes("search")
  ) {
    return "file-read";
  }
  return "command";
}

export function openCodeToolProjectionKind(
  toolName: string,
): "command_execution" | "file_change" | "file_search" | "web_search" | "dynamic_tool" {
  const normalized = toolName.toLowerCase();
  if (normalized === "todowrite") {
    return "dynamic_tool";
  }
  if (normalized.includes("bash") || normalized.includes("shell")) {
    return "command_execution";
  }
  if (normalized.includes("edit") || normalized.includes("write") || normalized.includes("patch")) {
    return "file_change";
  }
  if (normalized.includes("web") || normalized === "codesearch" || normalized === "code_search") {
    return "web_search";
  }
  if (
    normalized === "read" ||
    normalized.includes("glob") ||
    normalized.includes("grep") ||
    normalized.includes("search") ||
    normalized.includes("lsp")
  ) {
    return "file_search";
  }
  return "dynamic_tool";
}

const OPENCODE_ALWAYS_ALLOWED_PERMISSIONS = [
  "question",
  "read",
  "glob",
  "grep",
  "lsp",
  "todowrite",
  "task",
  "skill",
] as const;

const OPENCODE_RESTRICTED_PERMISSIONS = [
  "bash",
  "edit",
  "webfetch",
  "websearch",
  "codesearch",
  "external_directory",
  "doom_loop",
] as const;

/**
 * OpenCode does not provide an OS sandbox, so permission rules are also the
 * enforcement boundary for non-interactive policies. Read/planning tools are
 * safe by default; edits are auto-approved only for workspace-write modes,
 * while shell/network/external access remains gated unless policy explicitly
 * allows it.
 */
export function openCodePermissionRules(
  runtimePolicy: ProviderAdapterV2RuntimePolicy,
): PermissionRuleset {
  const sandboxPolicy = recordValue(runtimePolicy, "sandboxPolicy");
  const sandboxType = recordString(sandboxPolicy, "type");
  const rawApprovalPolicy = runtimePolicy.approvalPolicy;
  const approvalPolicy = nonEmptyString(rawApprovalPolicy);
  const requiresApproval =
    approvalPolicy === undefined
      ? (typeof rawApprovalPolicy === "object" && rawApprovalPolicy !== null) ||
        runtimePolicy.runtimeMode !== "full-access"
      : approvalPolicy !== "never";
  const externallySandboxed = sandboxType === "externalSandbox";
  const dangerFullAccess = sandboxType === "dangerFullAccess";
  const implicitFullAccess =
    sandboxType === undefined && runtimePolicy.runtimeMode === "full-access";

  if (!requiresApproval && (externallySandboxed || dangerFullAccess || implicitFullAccess)) {
    return [{ permission: "*", pattern: "*", action: "allow" }];
  }

  // Task sessions initially inherit only parent deny rules. Seed explicit
  // denies before the effective ask/allow overrides so a child is safe during
  // the short interval before emitSubagent installs its complete policy.
  const rules: Array<PermissionRuleset[number]> = [
    { permission: "*", pattern: "*", action: "deny" },
    ...OPENCODE_RESTRICTED_PERMISSIONS.map((permission) => ({
      permission,
      pattern: "*",
      action: "deny" as const,
    })),
  ];

  if (requiresApproval) {
    rules.push({ permission: "*", pattern: "*", action: "ask" });
    for (const permission of OPENCODE_RESTRICTED_PERMISSIONS) {
      rules.push({ permission, pattern: "*", action: "ask" });
    }
  }

  rules.push(
    ...OPENCODE_ALWAYS_ALLOWED_PERMISSIONS.map((permission) => ({
      permission,
      pattern: "*",
      action: "allow" as const,
    })),
  );

  if (
    runtimePolicy.runtimeMode === "auto-accept-edits" ||
    (!requiresApproval && sandboxType === "workspaceWrite")
  ) {
    rules.push({ permission: "edit", pattern: "*", action: "allow" });
  }

  if (!requiresApproval && recordValue(sandboxPolicy, "networkAccess") === true) {
    for (const permission of ["webfetch", "websearch", "codesearch"] as const) {
      rules.push({ permission, pattern: "*", action: "allow" });
    }
  }

  if (!requiresApproval && sandboxType === "readOnly") {
    const access = recordValue(sandboxPolicy, "access");
    if (recordString(access, "type") === "fullAccess") {
      rules.push({ permission: "external_directory", pattern: "*", action: "allow" });
    }
  }

  if (!requiresApproval && sandboxType === "workspaceWrite") {
    const writableRoots = recordValue(sandboxPolicy, "writableRoots");
    if (Array.isArray(writableRoots)) {
      for (const root of writableRoots) {
        if (typeof root === "string" && root.trim().length > 0) {
          rules.push({
            permission: "external_directory",
            pattern: `${root.replace(/\/$/, "")}/*`,
            action: "allow",
          });
        }
      }
    }
  }

  return rules;
}

function permissionRuleEquals(
  left: PermissionRuleset[number],
  right: PermissionRuleset[number],
): boolean {
  return (
    left.permission === right.permission &&
    left.pattern === right.pattern &&
    left.action === right.action
  );
}

/**
 * OpenCode task sessions inherit only the parent's deny/external-directory
 * rules and then add agent-specific restrictions such as disabling nested
 * tasks. Install the complete parent policy while retaining only rules that
 * were added specifically for the selected child agent.
 */
export function openCodeChildPermissionRules(
  runtimePolicy: ProviderAdapterV2RuntimePolicy,
  nativeChildRules: PermissionRuleset,
): PermissionRuleset {
  const parentRules = openCodePermissionRules(runtimePolicy);
  const inheritedRules = parentRules.filter(
    (rule) => rule.permission === "external_directory" || rule.action === "deny",
  );
  const childSpecificRules = nativeChildRules.filter(
    (childRule) =>
      !inheritedRules.some((inheritedRule) => permissionRuleEquals(childRule, inheritedRule)),
  );
  return [...parentRules, ...childSpecificRules];
}

/**
 * OpenCode's fork/revert boundary is exclusive. To retain the selected app
 * turn, address the next native user message; omitting a boundary retains the
 * current head when the selected turn is already last.
 */
export function openCodeBoundaryAfterProviderTurn(
  providerTurns: ReadonlyArray<OrchestrationV2ProviderTurn>,
  selectedProviderTurnId: OrchestrationV2ProviderTurn["id"],
): string | undefined {
  const selected = providerTurns.find((turn) => turn.id === selectedProviderTurnId);
  if (selected === undefined) return undefined;
  return providerTurns
    .filter((turn) => turn.ordinal > selected.ordinal)
    .toSorted((left, right) => left.ordinal - right.ordinal)
    .map((turn) => turn.nativeTurnRef?.nativeId)
    .find((nativeId): nativeId is string => nativeId !== null && nativeId !== undefined);
}

/**
 * Node/item statuses for a tool that never reported its own terminal state.
 * Mirrors the turn's outcome so an interrupted turn does not leave a spinner.
 */
export function terminalToolStatus(status: TerminalTurnStatus): {
  readonly node: OrchestrationV2ExecutionNode["status"];
  readonly item: OrchestrationV2TurnItem["status"];
} {
  switch (status) {
    case "completed":
      return { node: "completed", item: "completed" };
    case "interrupted":
      return { node: "interrupted", item: "interrupted" };
    case "cancelled":
      return { node: "cancelled", item: "cancelled" };
    case "failed":
      return { node: "failed", item: "failed" };
  }
}
