#!/usr/bin/env bash
set -euo pipefail

# Canonical build for the Sidemantic browser WASM bundle.
#
# Produces the wasm-bindgen output (sidemantic.js, sidemantic_bg.wasm, .d.ts)
# into OUT_DIR. Used both to populate this package's wasm/ dir on publish and
# by the demo at examples/sidemantic_wasm_demo to vendor a copy.
#
# Usage: build.sh [OUT_DIR]
#   OUT_DIR defaults to <package>/wasm
#   SIDEMANTIC_WASM_PROFILE=release|debug (default release)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOT_DIR="$(cd "$PKG_DIR/.." && pwd)"

OUT_DIR="${1:-$PKG_DIR/wasm}"
PROFILE="${SIDEMANTIC_WASM_PROFILE:-release}"
TARGET_DIR="$ROOT_DIR/sidemantic-rs/target"

if [[ "$PROFILE" == "release" ]]; then
  # Size-optimized profile (see [profile.wasm-release] in the workspace Cargo.toml).
  CARGO_PROFILE_ARGS=(--profile wasm-release)
  WASM_PATH="$TARGET_DIR/wasm32-unknown-unknown/wasm-release/sidemantic.wasm"
else
  CARGO_PROFILE_ARGS=()
  WASM_PATH="$TARGET_DIR/wasm32-unknown-unknown/debug/sidemantic.wasm"
fi

cargo build \
  --manifest-path "$ROOT_DIR/sidemantic-rs/Cargo.toml" \
  --target-dir "$TARGET_DIR" \
  --target wasm32-unknown-unknown \
  --features wasm \
  --lib \
  "${CARGO_PROFILE_ARGS[@]}"

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

wasm-bindgen \
  --target web \
  --out-dir "$OUT_DIR" \
  "$WASM_PATH"

# No wasm-opt pass. The size-optimized wasm-release profile already does the
# heavy lifting (~18 MB debug -> ~5.7 MB). wasm-opt -Oz trims a little more raw
# size but *increases* the brotli transfer size (what browsers actually
# download), and its output varies by binaryen version (older binaryen + -all
# emitted a module some wasm runtimes could not parse). For a published browser
# bundle the plain wasm-bindgen output is smaller over the wire and avoids that
# fragility.

cat > "$OUT_DIR/manifest.json" <<JSON
{
  "crate": "sidemantic-rs",
  "profile": "$PROFILE",
  "target": "wasm32-unknown-unknown",
  "bindings": "wasm-bindgen"
}
JSON
