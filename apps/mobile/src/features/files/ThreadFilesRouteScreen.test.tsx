import {
  AuthFilesystemReadScope,
  EnvironmentId,
  ProjectReadFileError,
  ThreadId,
} from "@t3tools/contracts";
import { AsyncResult } from "effect/reactivity";
import { act, type ReactNode } from "react";
import { createRoot } from "react-dom/client";
import { beforeEach, describe, expect, it, vi } from "vite-plus/test";

const mocks = vi.hoisted(() => ({
  sessionScopes: [] as ReadonlyArray<string>,
  metadata: {
    data: { kind: "file", byteLength: 40_774_636 } as { kind: "file"; byteLength: number } | null,
    error: null as string | null,
    failure: null as unknown,
    isSuccess: true,
    isPending: false,
    refresh: vi.fn(),
  },
  readFile: {
    data: null as {
      exists: boolean;
      contents: string;
      byteLength: number;
      truncated: boolean;
    } | null,
    error: null as string | null,
    failure: null as unknown,
    isSuccess: false,
    isPending: false,
    refresh: vi.fn(),
  },
  refreshAssetUrl: vi.fn<() => Promise<string | null>>(
    async () => "http://127.0.0.1:37891/api/assets/amp-1.7.0.apk",
  ),
  assetRequests: [] as Array<{ environmentId: unknown; resource: unknown }>,
  downloadAndShareAttachment: vi.fn(async () => {}),
  headerProps: null as {
    title: string;
    subtitle: string;
    menus?: ReadonlyArray<{ items: ReadonlyArray<Record<string, unknown>> }>;
  } | null,
}));

vi.mock("@react-navigation/native", () => ({
  StackActions: { push: vi.fn(), replace: vi.fn() },
  useNavigation: () => ({
    canGoBack: () => true,
    goBack: vi.fn(),
    dispatch: vi.fn(),
    navigate: vi.fn(),
  }),
  usePreventRemove: () => {},
}));
vi.mock("@t3tools/client-runtime/state/filesystem", () => ({
  resolveFilesystemReadAccess: () => ({ canReadFiles: true, isPending: false, error: null }),
}));
vi.mock("../../state/session", () => ({
  environmentSession: { sessionStateAtom: () => "session" },
}));
vi.mock("../../state/atom-registry", () => ({
  appAtomRegistry: {
    get: () => AsyncResult.success({ authenticated: true, scopes: mocks.sessionScopes }),
  },
}));
vi.mock("../../state/assets", () => ({
  useRefreshAssetUrl: (environmentId: unknown, resource: unknown) => {
    mocks.assetRequests.push({ environmentId, resource });
    return mocks.refreshAssetUrl;
  },
}));
vi.mock("../../state/projects", () => ({
  projectEnvironment: {
    fileMetadata: () => "metadata",
    readFile: () => "readFile",
  },
}));
vi.mock("../../state/query", () => ({
  useEnvironmentQuery: (atom: unknown) => {
    if (atom === "session") {
      return {
        data: { authenticated: true, scopes: mocks.sessionScopes },
        error: null,
        failure: null,
        isSuccess: true,
        isPending: false,
        refresh: vi.fn(),
      };
    }
    if (atom === "metadata") return mocks.metadata;
    if (atom === "readFile") return mocks.readFile;
    return {
      data: null,
      error: null,
      failure: null,
      isSuccess: true,
      isPending: false,
      refresh: vi.fn(),
    };
  },
}));
vi.mock("../../state/presentation", () => ({
  useEnvironmentPresentation: () => ({ isReady: true, presentation: null }),
}));
vi.mock("../../state/use-thread-selection", () => ({
  useThreadSelection: () => ({
    selectedThread: { environmentId: EnvironmentId.make("env-1") },
    selectedThreadProject: { title: "alairo", workspaceRoot: "/home/mwolson/devel/alairo" },
  }),
}));
vi.mock("../../state/use-selected-thread-worktree", () => ({
  useSelectedThreadWorktree: () => ({ selectedThreadCwd: "/home/mwolson/devel/alairo" }),
}));
vi.mock("../../lib/attachmentDownload", () => ({
  downloadAndShareAttachment: mocks.downloadAndShareAttachment,
  downloadAttachmentForPreview: vi.fn(),
  shareLocalAttachment: vi.fn(),
}));
vi.mock("../../lib/localAttachmentPreview", () => ({
  loadLocalAttachmentPreview: vi.fn(),
}));
vi.mock("../../lib/saveImageToLibrary", () => ({
  canSaveImageToLibrary: () => false,
  saveImageToLibrary: vi.fn(),
}));
vi.mock("../../components/ui/Dialog", () => ({ alert: vi.fn() }));
vi.mock("expo-haptics", () => ({
  notificationAsync: vi.fn(async () => {}),
  NotificationFeedbackType: { Success: "success" },
}));
vi.mock("../layout/AdaptiveWorkspaceLayout", () => ({
  Inspector: () => null,
  useAdaptiveWorkspaceLayout: () => ({ toggleAuxiliaryPane: vi.fn() }),
  useWorkspacePaneState: () => ({
    fileInspector: { supported: false },
    panes: { auxiliaryPaneVisible: false },
  }),
}));
vi.mock("../settings/appearance/AppearancePreferencesProvider", () => ({
  useAppearancePreferences: () => ({
    appearance: { codeWordBreak: false },
    themeAppearance: "dark",
    setCodeWordBreak: vi.fn(),
  }),
}));
vi.mock("../threads/ThreadRouteScreen", () => ({ ThreadRouteScreen: () => null }));
vi.mock("./FileTreeBrowser", () => ({ FileTreeBrowser: () => null }));
vi.mock("./useFileTreeEntries", () => ({
  useFileTreeEntries: () => ({
    entries: [],
    expandedPaths: new Set(),
    setExpandedPaths: vi.fn(),
    loadingDirectories: new Set(),
    loadDirectory: vi.fn(),
    error: null,
    isPending: false,
    isRefreshing: false,
    searchResultPaths: null,
    searchTruncated: false,
    selectedPath: null,
    revealSelection: vi.fn(),
    refresh: vi.fn(),
    selectPath: vi.fn(),
  }),
}));
vi.mock("./preload-workspace-file", () => ({ preloadWorkspaceFileContents: vi.fn() }));
vi.mock("./SourceFileSurface", () => ({ SourceFileSurface: () => null }));
vi.mock("./thread-file-navigator-pane", () => ({ ThreadFileNavigatorPane: () => null }));
vi.mock("./FileMarkdownPreview", () => ({ FileMarkdownPreview: () => null }));
vi.mock("./DelimitedFilePreview", () => ({ DelimitedFilePreview: () => null }));
vi.mock("./AudioFilePreview", () => ({ AudioFilePreview: () => null }));
vi.mock("./WorkspaceFileImagePreview", () => ({ WorkspaceFileImagePreview: () => null }));
vi.mock("./WorkspaceFilePreviewError", () => ({ WorkspaceFilePreviewError: () => null }));
vi.mock("./WorkspaceFileVideoPreview", () => ({ WorkspaceFileVideoPreview: () => null }));
vi.mock("./WorkspaceFileWebPreview", () => ({ WorkspaceFileWebPreview: () => null }));
vi.mock("./workspaceFileAssetUrl", () => ({
  useWorkspaceFileAssetUrlState: () => ({ _tag: "Idle", resource: null, refresh: vi.fn() }),
}));
vi.mock("./useFullFileText", () => ({
  MOBILE_FULL_FILE_MAX_BYTES: 8 * 1024 * 1024,
  useFullFileText: () => null,
}));
vi.mock("../../components/ui/FilePreviewModal", () => ({ FilePreviewModal: () => null }));
vi.mock("../../components/LoadingScreen", () => ({
  LoadingScreen: ({ message }: { message: string }) => <div>{message}</div>,
}));
vi.mock("../../components/ui/ScreenContent", () => ({
  ScreenContent: ({ children }: { children: ReactNode }) => <div>{children}</div>,
}));
vi.mock("../../components/ui/useBarInset", () => ({ useContentTopInset: () => 0 }));
vi.mock("../../components/ui/AppBar", () => ({
  AppBar: (props: {
    title: string;
    subtitle?: string;
    menus?: ReadonlyArray<{ items: ReadonlyArray<Record<string, unknown>> }>;
  }) => {
    mocks.headerProps = {
      title: props.title,
      subtitle: props.subtitle ?? "",
      menus: props.menus,
    };
    return null;
  },
}));
vi.mock("../../components/ui/EmptyState", () => ({
  EmptyState: (props: { title: string; detail: string; action?: ReactNode }) => (
    <div>
      <h1>{props.title}</h1>
      <p>{props.detail}</p>
      {props.action}
    </div>
  ),
}));
vi.mock("../../components/ui/Button", () => ({
  Button: (props: {
    label: string;
    onPress: () => void;
    disabled?: boolean;
    loading?: boolean;
  }) => (
    <button type="button" disabled={props.disabled || props.loading} onClick={props.onPress}>
      {props.label}
    </button>
  ),
}));
vi.mock("../../lib/copyTextWithHaptic", () => ({ copyTextWithHaptic: vi.fn() }));
vi.mock("../../lib/openExternalUrl", () => ({ tryOpenExternalUrl: vi.fn() }));
vi.mock("../../lib/useUniwindTheme", () => ({
  useUniwindTheme: () => ({ "--color-sheet": "#000000" }),
}));
vi.mock("react-native-svg", () => ({
  default: () => null,
  Defs: () => null,
  LinearGradient: () => null,
  Rect: () => null,
  Stop: () => null,
}));
vi.mock("./FilePreviewFeedback", () => ({
  FilePreviewLoading: ({ message }: { message: string }) => <div>{message}</div>,
  FilePreviewNotice: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
}));
vi.mock("react-native", () => ({
  ActivityIndicator: () => null,
  Platform: { OS: "ios" },
  View: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
}));

import { ThreadFileScreen } from "./ThreadFilesRouteScreen";

function renderScreen(params: {
  readonly environmentId: string;
  readonly threadId?: string;
  readonly cwd?: string;
  readonly path: string[];
}) {
  (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  const container = document.body.appendChild(document.createElement("div"));
  const root = createRoot(container);
  act(() => {
    root.render(<ThreadFileScreen route={{ params } as never} />);
  });
  return {
    container,
    unmount: () => {
      act(() => root.unmount());
      container.remove();
    },
  };
}

beforeEach(() => {
  mocks.sessionScopes = [AuthFilesystemReadScope];
  mocks.refreshAssetUrl.mockClear();
  mocks.assetRequests.length = 0;
  mocks.downloadAndShareAttachment.mockReset();
  mocks.headerProps = null;
  mocks.metadata.data = { kind: "file", byteLength: 40_774_636 };
  mocks.metadata.error = null;
  mocks.metadata.failure = null;
  mocks.metadata.isSuccess = true;
  mocks.readFile.data = null;
  mocks.readFile.error = null;
  mocks.readFile.failure = null;
  mocks.readFile.isSuccess = false;
});

describe("ThreadFileScreen binary files", () => {
  it("renders a binary fallback and hands off Save or share through useMediaActions", async () => {
    const binaryError = new ProjectReadFileError({
      cwd: "/home/mwolson/devel/alairo",
      relativePath: "/home/mwolson/Downloads/amp-1.7.0.apk",
      failure: "binary_file",
    });
    mocks.readFile.failure = binaryError;
    mocks.readFile.error = binaryError.message;

    const screen = renderScreen({
      environmentId: "env-1",
      threadId: "thread-1",
      path: ["", "home", "mwolson", "Downloads", "amp-1.7.0.apk"],
    });
    try {
      expect(screen.container.querySelector("h1")?.textContent).toBe("No preview for this file");
      expect(mocks.headerProps?.title).toBe("amp-1.7.0.apk");
      expect(mocks.headerProps?.subtitle).toContain("/home/mwolson/Downloads");
      expect(mocks.headerProps?.subtitle).toContain("MB");
      expect(mocks.headerProps?.menus?.[0]?.items.map((item) => item.id)).toEqual([
        "copy-full-path",
        "save",
      ]);

      const button = screen.container.querySelector("button");
      expect(button?.textContent).toBe("Save or share");
      expect(mocks.assetRequests.at(-1)).toEqual({
        environmentId: EnvironmentId.make("env-1"),
        resource: {
          _tag: "media-file",
          threadId: ThreadId.make("thread-1"),
          path: "/home/mwolson/Downloads/amp-1.7.0.apk",
          disposition: "attachment",
        },
      });

      await act(async () => {
        button?.click();
      });

      expect(mocks.refreshAssetUrl).toHaveBeenCalledTimes(1);
      expect(mocks.downloadAndShareAttachment).toHaveBeenCalledWith(
        expect.objectContaining({
          url: "http://127.0.0.1:37891/api/assets/amp-1.7.0.apk",
          attachment: {
            name: "amp-1.7.0.apk",
            mimeType: "application/octet-stream",
          },
        }),
      );
    } finally {
      screen.unmount();
    }
  });

  it("blocks draft binary file sharing when the session lacks filesystem:read scope", async () => {
    mocks.sessionScopes = [];
    const binaryError = new ProjectReadFileError({
      cwd: "/home/mwolson/devel/alairo",
      relativePath: "/home/mwolson/Downloads/Alairo-AMP-1.7.0-2611241529-Val.ipa",
      failure: "binary_file",
    });
    mocks.readFile.failure = binaryError;
    mocks.readFile.error = binaryError.message;

    const screen = renderScreen({
      environmentId: "env-1",
      cwd: "/home/mwolson/devel/alairo",
      path: ["", "home", "mwolson", "Downloads", "Alairo-AMP-1.7.0-2611241529-Val.ipa"],
    });
    try {
      const saveMenuItem = mocks.headerProps?.menus?.[0]?.items.find((item) => item.id === "save");
      expect(saveMenuItem?.disabled).toBe(true);

      await act(async () => {
        screen.container.querySelector("button")?.click();
      });
      expect(mocks.downloadAndShareAttachment).not.toHaveBeenCalled();
    } finally {
      screen.unmount();
    }
  });

  it("does not offer Save or share when a missing file returns exists: false", () => {
    mocks.metadata.data = null;
    mocks.readFile.data = { exists: false, contents: "", byteLength: 0, truncated: false };
    mocks.readFile.isSuccess = true;

    const screen = renderScreen({
      environmentId: "env-1",
      threadId: "thread-1",
      path: ["", "home", "mwolson", "Downloads", "missing.txt"],
    });
    try {
      expect(mocks.headerProps?.menus?.[0]?.items.some((item) => item.id === "save")).toBe(false);
      expect(mocks.assetRequests.at(-1)?.resource).toBeNull();
    } finally {
      screen.unmount();
    }
  });

  it("keeps File unavailable without Save or share for non-binary read errors", () => {
    const missingError = new ProjectReadFileError({
      cwd: "/home/mwolson/devel/alairo",
      relativePath: "/home/mwolson/Downloads/missing.apk",
      failure: "operation_failed",
    });
    mocks.readFile.failure = missingError;
    mocks.readFile.error = missingError.message;

    const screen = renderScreen({
      environmentId: "env-1",
      threadId: "thread-1",
      path: ["", "home", "mwolson", "Downloads", "missing.apk"],
    });
    try {
      expect(screen.container.querySelector("h1")?.textContent).toBe("File unavailable");
      expect(screen.container.querySelector("button")).toBeNull();
      expect(mocks.assetRequests.at(-1)?.resource).toBeNull();
    } finally {
      screen.unmount();
    }
  });
});
