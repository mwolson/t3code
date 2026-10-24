import {
  createContext,
  useContext,
  useLayoutEffect,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from "react";
import { View } from "react-native";
import { ThreadDetailScreen, type ThreadDetailScreenProps } from "./ThreadDetailScreen";

type ThreadScreenContent =
  | { readonly props: ThreadDetailScreenProps }
  | { readonly fallback: ReactNode };

function createThreadScreenStore() {
  let current: ThreadScreenContent | null = null;
  const listeners = new Set<() => void>();
  return {
    getSnapshot: () => current,
    subscribe: (listener: () => void) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    publish: (content: ThreadScreenContent) => {
      current = content;
      for (const listener of listeners) listener();
    },
  };
}
const ThreadScreenContext = createContext<ReturnType<typeof createThreadScreenStore> | null>(null);

function createPaneWidthStore() {
  let width: number | null = null;
  const listeners = new Set<() => void>();
  return {
    getSnapshot: () => width,
    subscribe: (listener: () => void) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    set: (next: number) => {
      if (next === width) return;
      width = next;
      for (const listener of listeners) listener();
    },
  };
}
const ThreadScreenPaneWidthContext = createContext<ReturnType<typeof createPaneWidthStore> | null>(
  null,
);

function ThreadScreenOutlet() {
  const store = useContext(ThreadScreenContext)!;
  const content = useSyncExternalStore(store.subscribe, store.getSnapshot);
  if (content === null) return null;
  return "props" in content ? (
    <>
      {content.props.header}
      <ThreadDetailScreen {...content.props} />
    </>
  ) : (
    content.fallback
  );
}

// Controllers can change from draft to live without replacing the native screen,
// the keyboard host, or the editor. Publish before paint to avoid an intermediate frame.
export function ThreadScreenHost({ children }: { readonly children: ReactNode }) {
  const [store] = useState(createThreadScreenStore);
  const [paneWidth] = useState(createPaneWidthStore);
  return (
    <ThreadScreenContext.Provider value={store}>
      <ThreadScreenPaneWidthContext.Provider value={paneWidth}>
        {children}
        <View
          className="flex-1 bg-screen android:overflow-hidden android:rounded-t-[28px] android:bg-thread-canvas"
          onLayout={(event) => paneWidth.set(event.nativeEvent.layout.width)}
        >
          <ThreadScreenOutlet />
        </View>
      </ThreadScreenPaneWidthContext.Provider>
    </ThreadScreenContext.Provider>
  );
}

export function ThreadScreenBinding(props: ThreadDetailScreenProps) {
  const store = useContext(ThreadScreenContext)!;
  useLayoutEffect(() => {
    store.publish({ props });
  });
  return null;
}

export function ThreadScreenFallback({ children }: { readonly children: ReactNode }) {
  const store = useContext(ThreadScreenContext)!;
  useLayoutEffect(() => {
    store.publish({ fallback: children });
  });
  return null;
}

/** Selects from the thread pane's measured width (null before the first layout). */
export function useThreadScreenPaneWidth<T>(select: (width: number | null) => T): T {
  const store = useContext(ThreadScreenPaneWidthContext)!;
  return useSyncExternalStore(store.subscribe, () => select(store.getSnapshot()));
}
