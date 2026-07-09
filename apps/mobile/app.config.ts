import type { ExpoConfig } from "expo/config";

import { loadRepoEnv } from "../../scripts/lib/public-config.ts";

type AppVariant = "development" | "preview" | "production";
type ExpoPlugin = NonNullable<ExpoConfig["plugins"]>[number];

const repoEnv = loadRepoEnv();
Object.assign(process.env, repoEnv);

const APP_VARIANT = resolveAppVariant(repoEnv.APP_VARIANT);

const VARIANT_CONFIG: Record<
  AppVariant,
  {
    readonly appName: string;
    readonly scheme: string;
    readonly iosIcon: string;
    readonly splashIcon: string;
    readonly iosBundleIdentifier: string;
    readonly androidPackage: string;
    readonly relyingParty?: string;
  }
> = {
  development: {
    appName: "T3 Code Dev",
    scheme: "t3code-dev",
    iosIcon: "./assets/icon-composer-dev.icon",
    splashIcon: "./assets/splash-icon-dev.png",
    iosBundleIdentifier: "com.t3tools.t3code.dev",
    androidPackage: "com.t3tools.t3code.dev",
    relyingParty: "clerk.t3.codes",
  },
  preview: {
    appName: "T3 Code Preview",
    scheme: "t3code-preview",
    iosIcon: "./assets/icon-composer-prod.icon",
    splashIcon: "./assets/splash-icon-prod.png",
    iosBundleIdentifier: "com.t3tools.t3code.preview",
    androidPackage: "com.t3tools.t3code.preview",
    relyingParty: "clerk.t3.codes",
  },
  production: {
    appName: "T3 Code",
    scheme: "t3code",
    iosIcon: "./assets/icon-composer-prod.icon",
    splashIcon: "./assets/splash-icon-prod.png",
    iosBundleIdentifier: "com.t3tools.t3code",
    androidPackage: "com.t3tools.t3code",
    relyingParty: "clerk.t3.codes",
  },
};

function resolveAppVariant(value: string | undefined): AppVariant {
  switch (value) {
    case "development":
    case "preview":
    case "production":
      return value;
    default:
      return "production";
  }
}

const variant = VARIANT_CONFIG[APP_VARIANT];

const dmSansFonts = {
  bold: "@expo-google-fonts/dm-sans/700Bold/DMSans_700Bold.ttf",
  medium: "@expo-google-fonts/dm-sans/500Medium/DMSans_500Medium.ttf",
  regular: "@expo-google-fonts/dm-sans/400Regular/DMSans_400Regular.ttf",
} as const;

// These aliases match the fonts' PostScript names on iOS. Register the same
// names on Android so React Native and the native composer use one set of
// family names without waiting for runtime font loading.

const localIosBundleIdentifier = process.env.T3CODE_LOCAL_IOS_BUNDLE_IDENTIFIER?.trim() || null;
const localIosAppleTeamId = process.env.T3CODE_LOCAL_IOS_TEAM_ID?.trim() || null;
const isLocalIosBuild = localIosBundleIdentifier !== null;
const iosBundleIdentifier = localIosBundleIdentifier ?? variant.iosBundleIdentifier;
const iosAppleTeamId = localIosAppleTeamId ?? (isLocalIosBuild ? null : "ARK85ZXQ4Z");
const iosAssociatedDomains = isLocalIosBuild
  ? undefined
  : [`applinks:${variant.relyingParty}`, `webcredentials:${variant.relyingParty}`];
const localIosSigningPlugin: ExpoPlugin[] = isLocalIosBuild
  ? ["./plugins/withLocalIosSigning.cjs"]
  : [];
const widgetPlugin: ExpoPlugin[] = isLocalIosBuild
  ? []
  : [
      [
        "expo-widgets",
        {
          bundleIdentifier: `${iosBundleIdentifier}.widgets`,
          groupIdentifier: `group.${iosBundleIdentifier}`,
          enablePushNotifications: true,
          // Agent activity can update many times an hour; without the
          // frequent-updates entitlement iOS throttles the update budget sooner.
          frequentUpdates: true,
          widgets: [
            {
              name: "AgentActivity",
              displayName: "Agent Activity",
              description: "Shows the current state of active T3 Code agents.",
              supportedFamilies: ["systemSmall", "systemMedium", "accessoryRectangular"],
            },
          ],
        },
      ],
    ];
// withWidgetLogoAsset requires ExpoWidgetsTarget; skip when local builds disable widgets.
const widgetLogoAssetPlugin: ExpoPlugin[] = isLocalIosBuild
  ? []
  : ["./plugins/withWidgetLogoAsset.cjs"];

const config: ExpoConfig = {
  name: variant.appName,
  slug: "t3-code",
  platforms: ["ios", "android"],
  scheme: variant.scheme,
  version: "0.1.0",
  runtimeVersion: {
    // Fingerprint (not appVersion) so an OTA only reaches binaries whose native
    // project — native deps, config plugins, AND patches/ — matches the update.
    // With appVersion, every 0.1.0 build shares a runtime version, so a JS update
    // could land on a binary missing the native changes it needs and crash.
    policy: process.env.MOBILE_VERSION_POLICY ?? "fingerprint",
  },
  orientation: "portrait",
  icon: "./assets/icon.png",
  userInterfaceStyle: "automatic",
  updates: isLocalIosBuild
    ? { enabled: false }
    : {
        enabled: true,
        url: "https://u.expo.dev/d763fcb8-d37c-41ea-a773-b54a0ab4a454",
        checkAutomatically: "ON_LOAD",
        fallbackToCacheTimeout: 0,
      },
  ios: {
    icon: variant.iosIcon,
    supportsTablet: true,
    bundleIdentifier: iosBundleIdentifier,
    ...(iosAppleTeamId === null ? {} : { appleTeamId: iosAppleTeamId }),
    ...(iosAssociatedDomains === undefined ? {} : { associatedDomains: iosAssociatedDomains }),
    infoPlist: {
      NSAppTransportSecurity: {
        NSAllowsArbitraryLoads: true,
      },
      NSLocalNetworkUsageDescription:
        "Allow T3 Code to connect to T3 Code servers on your local network or tailnet.",
      ITSAppUsesNonExemptEncryption: false,
    },
  },
  android: {
    icon: "./assets/icon.png",
    package: variant.androidPackage,
    adaptiveIcon: {
      backgroundColor: "#E6F4FE",
      foregroundImage: "./assets/android-icon-foreground.png",
      backgroundImage: "./assets/android-icon-background.png",
      monochromeImage: "./assets/android-icon-monochrome.png",
    },
    predictiveBackGestureEnabled: false,
  },
  web: {
    favicon: "./assets/favicon.png",
  },
  plugins: [
    ...localIosSigningPlugin,
    [
      "expo-font",
      {
        ios: {
          fonts: [dmSansFonts.regular, dmSansFonts.medium, dmSansFonts.bold],
        },
        android: {
          fonts: [
            {
              fontFamily: "DMSans-Regular",
              fontDefinitions: [{ path: dmSansFonts.regular, weight: 400 }],
            },
            {
              fontFamily: "DMSans-Medium",
              fontDefinitions: [{ path: dmSansFonts.medium, weight: 500 }],
            },
            {
              fontFamily: "DMSans-Bold",
              fontDefinitions: [{ path: dmSansFonts.bold, weight: 700 }],
            },
          ],
        },
      },
    ],
    "expo-secure-store",
    "expo-sqlite",
    ["@clerk/expo", { theme: "./clerk-theme.json" }],
    "expo-web-browser",
    [
      "expo-camera",
      {
        cameraPermission: "Allow T3 Code to access your camera so you can scan pairing QR codes.",
        barcodeScannerEnabled: true,
      },
    ],
    [
      "expo-splash-screen",
      {
        image: variant.splashIcon,
        resizeMode: "contain",
        backgroundColor: "#ffffff",
        imageWidth: 220,
        dark: {
          image: variant.splashIcon,
          backgroundColor: "#0a0a0a",
        },
      },
    ],
    [
      "expo-build-properties",
      {
        ios: {
          deploymentTarget: "18.0",
          // AppCheckCore 11.3+ includes Swift and needs module maps for these Objective-C dependencies.
          extraPods: [
            { name: "GoogleUtilities", modular_headers: true },
            { name: "RecaptchaInterop", modular_headers: true },
          ],
        },
      },
    ],
    "./plugins/withIosCocoaPodsUuidCache.cjs",
    // Must be listed BEFORE expo-widgets: same-type mods run last-registered-
    // first, so registering earlier makes this plugin's mods run AFTER
    // expo-widgets' — its dangerous mod wipes ios/ExpoWidgetsTarget/ (which
    // would delete the asset catalog) and its xcodeproj mod creates the widget
    // target (which must exist before the compile phase can be attached).
    ...widgetLogoAssetPlugin,
    ...widgetPlugin,
    "./plugins/withIosSceneLifecycle.cjs",
    "./plugins/withAndroidCleartextTraffic.cjs",
  ],
  extra: {
    appVariant: APP_VARIANT,
    relay: {
      url: repoEnv.T3CODE_RELAY_URL ?? null,
    },
    clerk: {
      publishableKey: repoEnv.EXPO_PUBLIC_CLERK_PUBLISHABLE_KEY ?? null,
      jwtTemplate: repoEnv.EXPO_PUBLIC_CLERK_JWT_TEMPLATE ?? null,
    },
    // Native Google sign-in credentials. @clerk/expo reads these from `extra`
    // under their exact env-var names (not nested), and its config plugin reads
    // the iOS URL scheme at prebuild to register it in Info.plist.
    // Unset values must be omitted (not null): the public manifest serializes
    // null to {}, which is truthy and would defeat Clerk's fallback checks.
    EXPO_PUBLIC_CLERK_GOOGLE_WEB_CLIENT_ID: repoEnv.EXPO_PUBLIC_CLERK_GOOGLE_WEB_CLIENT_ID,
    EXPO_PUBLIC_CLERK_GOOGLE_IOS_CLIENT_ID: repoEnv.EXPO_PUBLIC_CLERK_GOOGLE_IOS_CLIENT_ID,
    EXPO_PUBLIC_CLERK_GOOGLE_ANDROID_CLIENT_ID: repoEnv.EXPO_PUBLIC_CLERK_GOOGLE_ANDROID_CLIENT_ID,
    EXPO_PUBLIC_CLERK_GOOGLE_IOS_URL_SCHEME: repoEnv.EXPO_PUBLIC_CLERK_GOOGLE_IOS_URL_SCHEME,
    observability: {
      tracesUrl: repoEnv.EXPO_PUBLIC_OTLP_TRACES_URL ?? "https://api.axiom.co/v1/traces",
      tracesDataset: repoEnv.EXPO_PUBLIC_OTLP_TRACES_DATASET ?? null,
      tracesToken: repoEnv.EXPO_PUBLIC_OTLP_TRACES_TOKEN ?? null,
    },
    eas: {
      projectId: "d763fcb8-d37c-41ea-a773-b54a0ab4a454",
    },
  },
  owner: "pingdotgg",
};

export default config;
