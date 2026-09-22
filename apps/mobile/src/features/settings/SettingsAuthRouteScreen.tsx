import { useAuth } from "@clerk/expo";
import {
  AuthView,
  type UserProfileCustomPage,
  UserProfileView,
} from "@clerk/expo/native-components";
import { useNavigation } from "@react-navigation/native";
import { useEffect, useState } from "react";
import { View } from "react-native";

import { AppActivityIndicator } from "../../components/ui/AppActivityIndicator";
import { AppText as Text } from "../../components/ui/AppText";
import { Button } from "../../components/ui/Button";
import { useClerkLoadRetry } from "../cloud/clerkLoadRecovery";
import { hasCloudPublicConfig } from "../cloud/publicConfig";
import { T3ConnectProfilePage } from "../cloud/T3ConnectProfilePage";

const USER_PROFILE_CUSTOM_PAGES = [
  {
    path: "t3-connect",
    label: "T3 Connect",
    icon: "globe",
    content: <T3ConnectProfilePage />,
  },
] satisfies UserProfileCustomPage[];

// Without cloud config the app has no entry point here and appLinking drops the deep link.
export function SettingsAuthRouteScreen() {
  return hasCloudPublicConfig() ? <ConfiguredSettingsAuthRouteScreen /> : null;
}

function ConfiguredSettingsAuthRouteScreen() {
  const { isLoaded, isSignedIn } = useAuth({ treatPendingAsSignedOut: false });
  const navigation = useNavigation();
  const handleHostBack = () => navigation.goBack();
  // Once signed in, stay on the profile view so signing out pops back instead of
  // flashing the sign-in form.
  const [hasBeenSignedIn, setHasBeenSignedIn] = useState(isSignedIn === true);
  if (isSignedIn && !hasBeenSignedIn) setHasBeenSignedIn(true);

  // Clerk reports sign-out only through its auth store (the native profile view
  // and the T3 Connect page can both sign out), so this follows that store.
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- pops the navigator when Clerk's auth store reports a sign-out from the profile view or Connect page
  useEffect(() => {
    if (hasBeenSignedIn && isLoaded && isSignedIn === false) {
      navigation.goBack();
    }
  }, [hasBeenSignedIn, isLoaded, isSignedIn, navigation]);

  return (
    <View collapsable={false} className="flex-1 overflow-hidden bg-sheet">
      {isLoaded ? (
        hasBeenSignedIn ? (
          <UserProfileView
            customPages={USER_PROFILE_CUSTOM_PAGES}
            isDismissible={false}
            onHostBack={handleHostBack}
          />
        ) : (
          <AuthView isDismissible={false} onHostBack={handleHostBack} />
        )
      ) : (
        <ClerkLoadPendingView />
      )}
    </View>
  );
}

function ClerkLoadPendingView() {
  const retry = useClerkLoadRetry();
  return (
    <View collapsable={false} className="flex-1 items-center justify-center gap-4 px-8">
      <AppActivityIndicator />
      <Text className="text-center text-base font-t3-bold text-foreground">
        Checking T3 Account
      </Text>
      {retry ? (
        <>
          <Text className="text-center text-sm leading-normal text-foreground-muted">
            T3 Account did not load. Retry without clearing app data.
          </Text>
          <Button label="Retry" onPress={retry} />
        </>
      ) : null}
    </View>
  );
}
