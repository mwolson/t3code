import Constants from "expo-constants";
import { Platform } from "react-native";
import { supportsAndroidAgentNotifications } from "./androidNotifications";

export function supportsAgentAwarenessPush() {
  const extra = Constants.expoConfig?.extra;
  return Platform.OS === "android"
    ? supportsAndroidAgentNotifications()
    : Platform.OS === "ios" && (extra?.iosPushRelay ?? extra?.iosPersonalTeamBuild !== true);
}
