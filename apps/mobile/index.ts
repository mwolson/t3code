import { registerRootComponent } from "expo";
import "react-native-gesture-handler";
import { LogBox } from "react-native";
import { featureFlags } from "react-native-screens";
import {
  setThreadOpenTailLimit,
  setThreadStateIdleTtlMs,
  THREAD_STATE_MOBILE_IDLE_TTL_MS,
} from "@t3tools/client-runtime/state/threads";

import App from "./src/App";

// Long agent threads can exceed 1k turn items. Keep only the newest window in
// the mobile projection so open stays usable; server still has full history.
setThreadOpenTailLimit(100);
// Drop live stream + heavy projection soon after leave so Hermes can reclaim;
// 30s still covers quick back/forward. Lean offline cache keeps reopen snappy.
setThreadStateIdleTtlMs(THREAD_STATE_MOBILE_IDLE_TTL_MS);

// Required for react-native-screens' iOS FormSheet sizing fix when a nested
// native stack is rendered inside a non-fitToContents formSheet.
featureFlags.experiment.synchronousScreenUpdatesEnabled = true;

if (process.env.EXPO_PUBLIC_SHOWCASE === "1") {
  LogBox.ignoreAllLogs();
}

registerRootComponent(App);
