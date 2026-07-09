import * as Layer from "effect/Layer";

import * as MobileDatabase from "./mobile-database";
import * as MobilePreferences from "./mobile-preferences";
import * as MobileSecureStorage from "./mobile-secure-storage";
import * as MobileStorage from "./mobile-storage";

const baseLayer = Layer.merge(MobileDatabase.layer, MobileSecureStorage.layer);

// Preferences and connection-blob storage only. Do not merge EnvironmentCacheStore
// into the app ManagedRuntime: connection snapshot loaders pull it during
// Atom.runtime construction at module import time, which opens expo-sqlite while
// Hermes is still inside evaluateJavaScript and deadlocks the iOS bridge
// (infinite splash). Connection continues to use its file-backed cache store.
const dependentLayer = Layer.mergeAll(MobilePreferences.layer, MobileStorage.layer).pipe(
  Layer.provide(baseLayer),
);

export const layer = Layer.merge(baseLayer, dependentLayer);
