#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PROFILE="${SIDEMANTIC_WASM_PROFILE:-release}"
OUT_DIR="$ROOT_DIR/examples/sidemantic_wasm_demo/vendor/sidemantic"
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
# Optional: a missing wasm-opt only forgoes the extra size reduction.
if [[ "$PROFILE" == "release" ]] && command -v wasm-opt >/dev/null 2>&1; then
  # Enable the wasm features rustc emits by default so wasm-opt can process
  # (rather than reject) the module. Write to a temp file so a failure never
  # corrupts the bindgen output.
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
