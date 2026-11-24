import { ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  fileChipClipboardPath,
  fileChipMenu,
  fileChipShareSource,
  resolveFileChipTarget,
} from "./fileChipMenu";

it("copies native paths from a relative file chip in a Windows workspace", () => {
  const target = resolveFileChipTarget("reports/Device Users.csv", "C:/repo");
  if (!target) throw new Error("File-chip target unavailable");
  expect(fileChipClipboardPath(target, "copy-full-path")).toBe(
    String.raw`C:\repo\reports\Device Users.csv`,
  );
  expect(fileChipClipboardPath(target, "copy-relative-path")).toBe(
    String.raw`reports\Device Users.csv`,
  );
});

describe("resolveFileChipTarget", () => {
  it("resolves a workspace-relative link to both paths", () => {
    expect(resolveFileChipTarget("src/app.ts:12", "/repo")).toEqual({
      fullPath: "/repo/src/app.ts",
      relativePath: "src/app.ts",
    });
  });

  it("resolves home links for navigation and menu actions", () => {
    expect(resolveFileChipTarget("~/.agents/AGENTS.md:314", "/home/maria/repo")).toEqual({
      fullPath: "/home/maria/.agents/AGENTS.md",
    });
    expect(resolveFileChipTarget("~/repo/src/main.ts:12", "/home/maria/repo")).toEqual({
      fullPath: "/home/maria/repo/src/main.ts",
      relativePath: "src/main.ts",
    });
    expect(resolveFileChipTarget("~/.agents/AGENTS.md", "C:\\Users\\maria\\repo")).toEqual({
      fullPath: "C:\\Users\\maria\\.agents\\AGENTS.md",
    });
  });

  it("ignores links that are not files or cannot be opened", () => {
    expect(resolveFileChipTarget("https://example.com/app.ts", "/repo")).toBeNull();
    expect(resolveFileChipTarget("~/report.md", "/repo")).toBeNull();
    expect(resolveFileChipTarget("../other/file.ts", "/repo")).toBeNull();
  });
});

describe("file chip downloads", () => {
  const threadId = ThreadId.make("thread-1");

  it.each([
    [
      "/tmp/maria-counter/maria-counter-final.mp4",
      "/tmp/maria-counter/maria-counter-final.mp4",
      "video/mp4",
    ],
    ["/tmp/take%2520%23one.mp4:12", "/tmp/take%20#one.mp4", "video/mp4"],
    ["/tmp/report.pdf", "/tmp/report.pdf", "application/pdf"],
    ["screens/image.PNG", "/repo/screens/image.PNG", "image/png"],
    ["/tmp/archive.zip", "/tmp/archive.zip", "application/zip"],
    ["/tmp/archive.jar", "/tmp/archive.jar", "application/java-archive"],
    ["/tmp/archive.tar", "/tmp/archive.tar", "application/x-tar"],
    ["/tmp/archive.gz", "/tmp/archive.gz", "application/gzip"],
    ["/tmp/archive.tgz", "/tmp/archive.tgz", "application/gzip"],
    ["/tmp/data.sqlite", "/tmp/data.sqlite", "application/vnd.sqlite3"],
    ["/tmp/data.sqlite3", "/tmp/data.sqlite3", "application/vnd.sqlite3"],
    ["/tmp/data.db", "/tmp/data.db", "application/vnd.sqlite3"],
    [
      "file:///home/mwolson/Downloads/amp-1.7.0.apk",
      "/home/mwolson/Downloads/amp-1.7.0.apk",
      "application/octet-stream",
    ],
    [
      "file:///home/mwolson/Downloads/Alairo-AMP-1.7.0-2611241529-Val.ipa",
      "/home/mwolson/Downloads/Alairo-AMP-1.7.0-2611241529-Val.ipa",
      "application/octet-stream",
    ],
    ["/tmp/archive.7z", "/tmp/archive.7z", "application/octet-stream"],
    ["src/app.ts", "/repo/src/app.ts", "application/octet-stream"],
    ["Makefile", "/repo/Makefile", "application/octet-stream"],
  ])("offers a host download for %s", (href, path, mimeType) => {
    const target = resolveFileChipTarget(href, "/repo")!;
    expect(fileChipMenu(target).actions).toContainEqual({
      id: "save",
      title: "Save or share",
    });
    expect(fileChipShareSource(target, threadId)).toEqual({
      name: path.split("/").at(-1),
      mimeType,
      resource: { _tag: "media-file", threadId, path, disposition: "attachment" },
    });
  });

  it("does not offer downloads for directory links including dotted folder names", () => {
    for (const href of ["docs/My Folder/", "docs/release.v1/", "file:///home/mwolson/Downloads"]) {
      const target = resolveFileChipTarget(href, "/repo")!;
      expect(fileChipShareSource(target, threadId)).toBeNull();
      expect(fileChipMenu(target).actions.some(({ id }) => id === "save")).toBe(false);
    }
  });

  it("omits Save or share when host filesystem read access is unavailable", () => {
    const target = resolveFileChipTarget("file:///home/mwolson/Downloads/amp-1.7.0.apk", "/repo")!;
    expect(
      fileChipMenu(target, undefined, { canSave: false }).actions.some(({ id }) => id === "save"),
    ).toBe(false);
  });
});
