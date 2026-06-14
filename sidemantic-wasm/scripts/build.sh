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

# Shrink the bindgen output further with wasm-opt (binaryen) when available.
# Optional: a missing wasm-opt only forgoes the extra size reduction. The
# feature flags enable the wasm features rustc emits so wasm-opt accepts the
# module rather than rejecting it.
if [[ "$PROFILE" == "release" ]] && command -v wasm-opt >/dev/null 2>&1; then
  wasm-opt -Oz \
    --enable-bulk-memory \
    --enable-bulk-memory-opt \
    --enable-nontrapping-float-to-int \
    --enable-sign-ext \
    --enable-mutable-globals \
    --enable-multivalue \
    --enable-reference-types \
    "$OUT_DIR/sidemantic_bg.wasm" \
    -o "$OUT_DIR/sidemantic_bg.wasm.opt"
  mv "$OUT_DIR/sidemantic_bg.wasm.opt" "$OUT_DIR/sidemantic_bg.wasm"
fi

cat > "$OUT_DIR/manifest.json" <<JSON
{
  "crate": "sidemantic-rs",
  "profile": "$PROFILE",
  "target": "wasm32-unknown-unknown",
  "bindings": "wasm-bindgen"
}
JSON
