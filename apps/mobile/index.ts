import { registerRootComponent } from "expo";
import "react-native-gesture-handler";
import { LogBox } from "react-native";
import { featureFlags } from "react-native-screens";
import { setThreadOpenTailLimit } from "@t3tools/client-runtime/state/threads";

import App from "./src/App";

// Long agent threads can exceed 1k turn items. Keep only the newest window in
// the mobile projection so open stays usable; server still has full history.
setThreadOpenTailLimit(100);

// Required for react-native-screens' iOS FormSheet sizing fix when a nested
// native stack is rendered inside a non-fitToContents formSheet.
featureFlags.experiment.synchronousScreenUpdatesEnabled = true;

if (process.env.EXPO_PUBLIC_SHOWCASE === "1") {
  LogBox.ignoreAllLogs();
}

registerRootComponent(App);
