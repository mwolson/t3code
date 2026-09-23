import Constants from "expo-constants";
import { Platform } from "react-native";
import { resolvePersonalBackgroundRoute } from "../cloud/personalBackgroundRoute";
import { supportsAndroidAgentNotifications } from "./androidNotifications";

export function supportsAgentAwarenessPush() {
  const extra = Constants.expoConfig?.extra;
  if (Platform.OS === "android") return supportsAndroidAgentNotifications();
  if (Platform.OS !== "ios") return false;
  if (extra?.iosPersonalTeamBuild === true) {
    return resolvePersonalBackgroundRoute(extra).kind === "private";
  }
  return extra?.iosPushRelay ?? true;
}
