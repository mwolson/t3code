const { withGradleProperties, withProjectBuildGradle } = require("expo/config-plugins");

// Google's prefab CLI (2.1.0, bundled with AGP) splits positional arguments on
// '=', and pnpm store directories for patched packages contain 'patch_hash='.
// AGP passes each native module's build/intermediates/cxx refs path to prefab,
// so every CMake-based module living under such a store path fails with
// [CXX1429] "no such option ..._patch_hash". Relocating those modules' build
// directories under the app project keeps '=' out of prefab's argument list.
//
// Only '='-path projects move: the RN root plugin expects the default layout
// for autolinking outputs (autolinking.json), and relocating everything breaks
// :app:generateAutolinkingNewArchitectureFiles.
//
// RN's Android-autolinking.cmake still hardcodes each library's codegen output
// at <store module>/android/build/generated/source/codegen/jni, which this
// relocation moves. Run scripts/android-store-fixups.sh after the first build
// attempt to link the default paths back (see that script's header).
const RELOCATION_MARKER = "build/relocated/";
const RELOCATION_BLOCK = [
  "",
  "// Added by withAndroidRelocatedStoreBuildDirs.cjs: prefab cannot parse",
  "// package paths containing '=' (pnpm patch_hash store dirs).",
  "allprojects {",
  "    if (projectDir.path.contains('=')) {",
  '        layout.buildDirectory.set(rootProject.file("build/relocated/${project.name}"))',
  "    }",
  "}",
  "",
].join("\n");

module.exports = function withAndroidRelocatedStoreBuildDirs(config) {
  return withArchitectureOverride(withRelocationBlock(config));
};

function withRelocationBlock(config) {
  return withProjectBuildGradle(config, (nextConfig) => {
    if (!nextConfig.modResults.contents.includes(RELOCATION_MARKER)) {
      nextConfig.modResults.contents += RELOCATION_BLOCK;
    }
    return nextConfig;
  });
}

// Optional ABI trim, e.g. MOBILE_ANDROID_ARCHS=x86_64 for Waydroid. Building a
// single ABI roughly quarters native build time.
function withArchitectureOverride(config) {
  const archs = process.env.MOBILE_ANDROID_ARCHS;
  if (!archs) {
    return config;
  }
  return withGradleProperties(config, (nextConfig) => {
    const properties = nextConfig.modResults.filter(
      (item) => !(item.type === "property" && item.key === "reactNativeArchitectures"),
    );

    properties.push({
      type: "property",
      key: "reactNativeArchitectures",
      value: archs,
    });

    nextConfig.modResults = properties;
    return nextConfig;
  });
}
