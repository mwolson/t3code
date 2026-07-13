#!/bin/bash
#
# Repairs the pnpm store side effects of building apps/mobile for Android with
# withAndroidRelocatedStoreBuildDirs.cjs (see that plugin for the prefab '='
# background). Idempotent; run it from anywhere.
#
# What it does:
#   1. Patches react-native-shiki-engine's android/CMakeLists.txt in the store.
#      Its find_library(... NO_CMAKE_FIND_ROOT_PATH) searches host dirs before
#      the bundled PATHS, so Linux x86_64 builds link the host libonig.so
#      (SONAME libonig.so.5) and the app red-screens with SoLoader "couldn't
#      find DSO to load: libonig.so.5". Points ONIG_LIB at the bundled
#      prebuilt instead and clears the affected CMake caches.
#   2. Wipes '='-path store modules' android/build dirs (stale pre-relocation
#      codegen there causes "duplicate class ...ViewManagerInterface").
#   3. Symlinks each relocated module's codegen output back to the store path
#      hardcoded by RN's Android-autolinking.cmake.
#
# Usage: run once after `expo prebuild`, and again after the first gradle
# build attempt (step 3 needs android/build/relocated/ populated, so the
# first build fails at add_subdirectory before the symlinks exist).

set -euo pipefail

for cmd in find ln python3; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "error: missing required command: $cmd" >&2
        exit 1
    fi
done

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
mobile_android="$repo_root/apps/mobile/android"
cd "$repo_root"

main() {
    patch_shiki_onig
    clean_stale_store_builds
    link_relocated_codegen
}

patch_shiki_onig() {
    local patched=""
    for cmake in node_modules/.aube/react-native-shiki-engine@*/node_modules/react-native-shiki-engine/android/CMakeLists.txt \
                 node_modules/.pnpm/react-native-shiki-engine@*/node_modules/react-native-shiki-engine/android/CMakeLists.txt; do
        [[ -f "$cmake" ]] || continue
        if ! grep -q 'find_library(ONIG_LIB' "$cmake"; then
            continue
        fi
        python3 - "$cmake" <<'EOF'
import re, sys
path = sys.argv[1]
text = open(path).read()
replacement = (
    "# Patched by android-store-fixups.sh: find_library with\n"
    "# NO_CMAKE_FIND_ROOT_PATH links the host libonig; use the bundled one.\n"
    "set(ONIG_LIB ${CMAKE_CURRENT_SOURCE_DIR}/src/main/jniLibs/${ANDROID_ABI}/libonig.so)\n"
)
text = re.sub(r"find_library\(ONIG_LIB.*?\n\)\n", replacement, text, flags=re.S)
open(path, "w").write(text)
EOF
        echo "patched: $cmake"
        rm -rf "$(dirname "$cmake")/.cxx" "$(dirname "$cmake")/build/intermediates/cxx"
        patched=1
    done
    if [[ -n "$patched" ]]; then
        rm -rf "$mobile_android/app/.cxx"
        echo "cleared CMake caches (module .cxx and app/.cxx) for relink"
    fi
}

clean_stale_store_builds() {
    find node_modules/.aube node_modules/.pnpm -maxdepth 6 -type d \
        -path '*/android/build' 2>/dev/null | grep '=' | while read -r dir; do
        rm -rf "$dir"
    done || true
}

link_relocated_codegen() {
    local linked=0 pkg name reloc
    for d in node_modules/.aube/*patch_hash*/node_modules/*/android \
             node_modules/.aube/*patch_hash*/node_modules/@*/*/android \
             node_modules/.pnpm/*patch_hash*/node_modules/*/android \
             node_modules/.pnpm/*patch_hash*/node_modules/@*/*/android; do
        [[ -d "$d" ]] || continue
        pkg=${d%/android}
        pkg=${pkg##*/node_modules/}
        name=${pkg#@}
        name=${name//\//_}
        reloc="$mobile_android/build/relocated/$name/generated/source/codegen/jni"
        [[ -d "$reloc" ]] || continue
        mkdir -p "$d/build/generated/source/codegen"
        ln -sfn "$reloc" "$d/build/generated/source/codegen/jni"
        linked=$((linked + 1))
    done
    echo "codegen symlinks: $linked (0 is expected before the first build populates build/relocated/)"
}

main
