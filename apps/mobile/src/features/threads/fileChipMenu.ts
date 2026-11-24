import { formatFilePathForClipboard } from "@t3tools/client-runtime/media-actions";
import { EXTENSIONLESS_FILE_NAMES } from "@t3tools/shared/markdownLinks";
import { fileBasename } from "@t3tools/shared/path";
import type { ThreadId } from "@t3tools/contracts";
import { resolveMarkdownLinkPresentation } from "../../lib/markdownLinks";
import type { MarkdownFileContextMenu } from "../../native/SelectableMarkdownText.types";
import { hostPreviewMimeTypeFromExtension } from "@t3tools/shared/filePreview";
import { msg } from "gt-react-native";

import { m, type MessageResolver } from "../../i18n/messages";

import {
  isAbsolutePath,
  resolveFileLinkPath,
  resolveWorkspaceFilePath,
  resolveWorkspaceRelativeFilePath,
} from "../files/filePath";

export interface FileChipTarget {
  readonly fullPath?: string;
  readonly relativePath?: string;
  readonly isDirectory?: boolean;
}

export function fileChipClipboardPath(
  target: FileChipTarget,
  action: "copy-full-path" | "copy-relative-path",
): string | undefined {
  const path = action === "copy-full-path" ? target.fullPath : target.relativePath;
  return path === undefined ? undefined : formatFilePathForClipboard(path, target.fullPath);
}

export function resolveFileChipTarget(
  href: string,
  workspaceRoot: string | null | undefined,
): FileChipTarget | null {
  const presentation = resolveMarkdownLinkPresentation(href);
  if (presentation.kind !== "file") return null;
  const isDirectory = /[\\/]$/.test(presentation.path);
  const path = resolveFileLinkPath(workspaceRoot, presentation.path);
  if (path === null) return null;
  const relativePath = resolveWorkspaceRelativeFilePath(workspaceRoot, path);
  const fullPath = isAbsolutePath(path)
    ? path
    : workspaceRoot && relativePath
      ? resolveWorkspaceFilePath(workspaceRoot, relativePath)
      : undefined;
  if (!fullPath && !relativePath) return null;
  return {
    ...(fullPath ? { fullPath } : {}),
    ...(relativePath ? { relativePath } : {}),
    ...(isDirectory ? { isDirectory: true } : {}),
  };
}

function fileChipMetadata(target: FileChipTarget) {
  if (target.isDirectory) return null;
  const path = target.fullPath ?? target.relativePath;
  if (!path || /[\\/]$/.test(path)) return null;
  const name = fileBasename(path);
  if (!name) return null;
  const dot = name.lastIndexOf(".");
  const hasExtension = dot > 0 && dot < name.length - 1;
  if (!hasExtension && !EXTENSIONLESS_FILE_NAMES.has(name) && !name.startsWith(".")) {
    return null;
  }
  const mimeType =
    (hasExtension ? hostPreviewMimeTypeFromExtension(name.slice(dot)) : null) ??
    "application/octet-stream";
  return { path, name, mimeType };
}

/** Use literal resolved paths so encoded filename characters are not decoded twice. */
export function fileChipShareSource(target: FileChipTarget, threadId: ThreadId) {
  const metadata = fileChipMetadata(target);
  return metadata
    ? {
        name: metadata.name,
        mimeType: metadata.mimeType,
        resource: {
          _tag: "media-file" as const,
          threadId,
          path: metadata.path,
          disposition: "attachment" as const,
        },
      }
    : null;
}

export function fileChipMenu(
  target: FileChipTarget,
  resolve: MessageResolver = m,
  options?: { readonly canSave?: boolean },
): MarkdownFileContextMenu {
  return {
    title: target.fullPath ?? target.relativePath ?? "",
    actions: [
      ...(target.fullPath ? [{ id: "copy-full-path", title: resolve(msg("Copy full path")) }] : []),
      ...(target.relativePath
        ? [{ id: "copy-relative-path", title: resolve(msg("Copy relative path")) }]
        : []),
      { id: "open-file", title: resolve(msg("Open in file viewer")) },
      ...(options?.canSave !== false && fileChipMetadata(target)
        ? [
            {
              id: "save",
              title: resolve(msg("Save or share")),
            },
          ]
        : []),
    ],
  };
}
