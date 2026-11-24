import {
  type AssetResource,
  type FilesystemEntryMetadata,
  ProjectReadFileError,
  type ThreadId,
} from "@t3tools/contracts";
import { filePreviewDelimiter } from "@t3tools/shared/delimitedPreview";
import { joinPath, resolvePathLinkTarget } from "@t3tools/shared/fileLinks";
import {
  hostPreviewMimeTypeFromExtension,
  isWorkspaceAudioPreviewPath,
  isWorkspaceVideoPreviewPath,
} from "@t3tools/shared/filePreview";
import * as Schema from "effect/Schema";

function isWindowsAbsolutePath(value: string): boolean {
  return /^[A-Za-z]:[\\/]/.test(value) || value.startsWith("\\\\");
}

export function isAbsolutePath(value: string): boolean {
  return value.startsWith("/") || isWindowsAbsolutePath(value);
}

export function fileRoutePathSegments(path: string): string[] {
  const segments = path.split("/").filter((segment) => segment.length > 0);
  return path.startsWith("/") ? ["", ...segments] : segments;
}

function isWindowsPathStyle(value: string): boolean {
  return isWindowsAbsolutePath(value) || /^[A-Za-z]:\\/.test(value);
}

export function basename(path: string): string {
  const parts = path.split(/[\\/]/).filter(Boolean);
  return parts.at(-1) ?? path;
}

export function resolveWorkspaceFilePath(cwd: string, relativePath: string): string {
  if (isAbsolutePath(relativePath)) {
    return relativePath;
  }

  const separator: "/" | "\\" = isWindowsPathStyle(cwd) ? "\\" : "/";
  return joinPath(cwd, relativePath, separator);
}

function normalizeRelativePath(value: string): string | null {
  const segments: string[] = [];
  for (const segment of value.replaceAll("\\", "/").split("/")) {
    if (segment.length === 0 || segment === ".") {
      continue;
    }
    if (segment === "..") {
      if (segments.length === 0) {
        return null;
      }
      segments.pop();
      continue;
    }
    segments.push(segment);
  }
  return segments.length > 0 ? segments.join("/") : null;
}

export function resolveFileLinkPath(
  workspaceRoot: string | null | undefined,
  targetPath: string,
): string | null {
  if (targetPath.startsWith("~/") || targetPath.startsWith("~\\")) {
    if (!workspaceRoot) return null;
    const expanded = resolvePathLinkTarget(targetPath, workspaceRoot);
    return isAbsolutePath(expanded) ? expanded : null;
  }
  return isAbsolutePath(targetPath) ? targetPath : normalizeRelativePath(targetPath);
}

export function resolveWorkspaceRelativeFilePath(
  workspaceRoot: string | null | undefined,
  targetPath: string,
): string | null {
  const resolved = resolveFileLinkPath(workspaceRoot, targetPath);
  if (resolved === null) return null;
  targetPath = resolved;
  if (!isAbsolutePath(targetPath)) return targetPath;
  if (!workspaceRoot) {
    return null;
  }

  const normalizedTarget = targetPath.replaceAll("\\", "/");
  const normalizedRoot = workspaceRoot.replaceAll("\\", "/").replace(/\/+$/, "");
  const caseInsensitive = isWindowsAbsolutePath(targetPath) || isWindowsAbsolutePath(workspaceRoot);
  const comparableTarget = caseInsensitive ? normalizedTarget.toLowerCase() : normalizedTarget;
  const comparableRoot = caseInsensitive ? normalizedRoot.toLowerCase() : normalizedRoot;
  if (!comparableTarget.startsWith(`${comparableRoot}/`)) {
    return null;
  }

  const relativePath = normalizedTarget.slice(normalizedRoot.length + 1);
  if (relativePath.split("/").includes("..")) {
    return null;
  }
  return normalizeRelativePath(relativePath);
}

export function isVideoPreviewFile(path: string): boolean {
  return isWorkspaceVideoPreviewPath(path);
}

export function isAudioPreviewFile(path: string): boolean {
  return isWorkspaceAudioPreviewPath(path);
}

export function isSvgImagePreviewFile(path: string): boolean {
  return /\.svg$/i.test(path);
}

export function isMarkdownPreviewFile(path: string): boolean {
  return /\.(?:md|mdx|markdown|mdown|mkd)$/i.test(path);
}

export function needsWorkspaceFileContents(input: {
  readonly relativePath: string | null;
  readonly metadataKind: FilesystemEntryMetadata["kind"] | undefined;
  readonly activeMode: "preview" | "source";
}): boolean {
  const path = input.relativePath;
  return (
    input.metadataKind !== "directory" &&
    path !== null &&
    !isVideoPreviewFile(path) &&
    !isAudioPreviewFile(path) &&
    (input.activeMode === "source" ||
      isMarkdownPreviewFile(path) ||
      filePreviewDelimiter({ name: path }) !== null)
  );
}

export function fileHeaderSubtitle(projectName: string, relativePath: string): string {
  const parentDir = relativePath.slice(
    0,
    Math.max(relativePath.lastIndexOf("/"), relativePath.lastIndexOf("\\"), 0),
  );
  return isAbsolutePath(relativePath)
    ? parentDir
    : [projectName, parentDir].filter(Boolean).join(" · ");
}

const isProjectReadFileError = Schema.is(ProjectReadFileError);

export function isBinaryFileReadError(failure: unknown): boolean {
  return isProjectReadFileError(failure) && failure.failure === "binary_file";
}

export function workspaceFileMimeType(path: string): string {
  const name = basename(path);
  const dotIndex = name.lastIndexOf(".");
  const extension = dotIndex > 0 && dotIndex < name.length - 1 ? name.slice(dotIndex) : "";
  return (
    (extension ? hostPreviewMimeTypeFromExtension(extension) : null) ?? "application/octet-stream"
  );
}

export function workspaceFileDownloadResource(input: {
  readonly threadId: ThreadId | null;
  readonly cwd: string;
  readonly targetPath: string;
}): Extract<AssetResource, { _tag: "media-file" | "draft-workspace-file" }> {
  return input.threadId !== null
    ? {
        _tag: "media-file",
        threadId: input.threadId,
        path: input.targetPath,
        disposition: "attachment",
      }
    : {
        _tag: "draft-workspace-file",
        cwd: input.cwd,
        path: input.targetPath,
        disposition: "attachment",
      };
}
