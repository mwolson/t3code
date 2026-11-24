import { ProjectReadFileError, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  fileRoutePathSegments,
  isAudioPreviewFile,
  isBinaryFileReadError,
  isMarkdownPreviewFile,
  needsWorkspaceFileContents,
  resolveFileLinkPath,
  resolveWorkspaceRelativeFilePath,
  workspaceFileDownloadResource,
  workspaceFileMimeType,
} from "./filePath";

describe("resolveFileLinkPath", () => {
  it.each([
    ["/home/maria/project", "~/.agents/AGENTS.md", "/home/maria/.agents/AGENTS.md"],
    ["/Users/maria/project", "~/notes.md", "/Users/maria/notes.md"],
    ["C:\\Users\\maria\\project", "~\\.agents\\AGENTS.md", "C:\\Users\\maria\\.agents\\AGENTS.md"],
  ])("expands a home link from %s", (cwd, path, expected) => {
    expect(resolveFileLinkPath(cwd, path)).toBe(expected);
    expect(fileRoutePathSegments(expected).join("/")).toBe(expected);
  });

  it("does not turn an unresolved home into a workspace path", () => {
    expect(resolveFileLinkPath("/repo", "~/notes.md")).toBeNull();
    expect(resolveFileLinkPath(null, "~/notes.md")).toBeNull();
    expect(resolveFileLinkPath("/repo", "../notes.md")).toBeNull();
  });
});

describe("resolveWorkspaceRelativeFilePath", () => {
  it("converts absolute paths inside the workspace", () => {
    expect(
      resolveWorkspaceRelativeFilePath("/Users/julius/repo", "/Users/julius/repo/src/main.ts"),
    ).toBe("src/main.ts");
    expect(resolveWorkspaceRelativeFilePath("C:\\repo", "c:\\repo\\src\\main.ts")).toBe(
      "src/main.ts",
    );
  });

  it("recognizes home links inside the workspace", () => {
    expect(resolveWorkspaceRelativeFilePath("/home/maria/project", "~/project/src/main.ts")).toBe(
      "src/main.ts",
    );
    expect(
      resolveWorkspaceRelativeFilePath("/home/maria/project", "~/.agents/AGENTS.md"),
    ).toBeNull();
  });

  it("rejects paths outside the workspace", () => {
    expect(resolveWorkspaceRelativeFilePath("/repo", "/other/main.ts")).toBeNull();
    expect(resolveWorkspaceRelativeFilePath("/repo", "../other/main.ts")).toBeNull();
    expect(resolveWorkspaceRelativeFilePath("/repo", "/repo/../outside.txt")).toBeNull();
    expect(resolveWorkspaceRelativeFilePath(null, "/repo/main.ts")).toBeNull();
  });
});

describe("needsWorkspaceFileContents", () => {
  it("reads a text file while its metadata is still pending, but not a directory", () => {
    const input = { relativePath: "package.json", activeMode: "source" } as const;
    expect(needsWorkspaceFileContents({ ...input, metadataKind: undefined })).toBe(true);
    expect(needsWorkspaceFileContents({ ...input, metadataKind: "directory" })).toBe(false);
  });
});

describe("workspace preview classifiers", () => {
  it("recognize preview extensions and treat # and ? as literal filename characters", () => {
    for (const path of ["notes/clip#1.wav", "notes/clip?1.wav", "notes#old/clip.wav"]) {
      expect(isAudioPreviewFile(path)).toBe(true);
    }
    for (const path of [
      "docs/draft#1.md",
      "docs/draft?1.md",
      "docs#old/draft.md",
      "CHANGELOG.markdown",
      "notes.MDOWN",
      "todo.mkd",
    ]) {
      expect(isMarkdownPreviewFile(path)).toBe(true);
    }
  });
});

describe("workspace file download helpers", () => {
  it("recognizes binary_file read errors only", () => {
    const binaryError = new ProjectReadFileError({
      cwd: "/repo",
      relativePath: "/home/mwolson/Downloads/amp-1.7.0.apk",
      failure: "binary_file",
    });
    const missingError = new ProjectReadFileError({
      cwd: "/repo",
      relativePath: "/home/mwolson/Downloads/missing.apk",
      failure: "operation_failed",
    });
    expect(isBinaryFileReadError(binaryError)).toBe(true);
    expect(isBinaryFileReadError(missingError)).toBe(false);
    expect(isBinaryFileReadError(new Error("binary_file"))).toBe(false);
  });

  it("builds attachment download resources for thread and draft files", () => {
    const threadId = ThreadId.make("thread-1");
    expect(
      workspaceFileDownloadResource({
        threadId,
        cwd: "/repo",
        targetPath: "/home/mwolson/Downloads/amp-1.7.0.apk",
      }),
    ).toEqual({
      _tag: "media-file",
      threadId,
      path: "/home/mwolson/Downloads/amp-1.7.0.apk",
      disposition: "attachment",
    });
    expect(
      workspaceFileDownloadResource({
        threadId: null,
        cwd: "/repo",
        targetPath: "/home/mwolson/Downloads/amp-1.7.0.apk",
      }),
    ).toEqual({
      _tag: "draft-workspace-file",
      cwd: "/repo",
      path: "/home/mwolson/Downloads/amp-1.7.0.apk",
      disposition: "attachment",
    });
  });

  it("derives download MIME types from the basename rather than dot directories", () => {
    expect(workspaceFileMimeType("/home/mwolson/Downloads/amp-1.7.0.apk")).toBe(
      "application/octet-stream",
    );
    expect(workspaceFileMimeType("/home/mwolson/.t3/binary")).toBe("application/octet-stream");
    expect(workspaceFileMimeType("/home/mwolson/Downloads/report.pdf")).toBe("application/pdf");
  });
});
