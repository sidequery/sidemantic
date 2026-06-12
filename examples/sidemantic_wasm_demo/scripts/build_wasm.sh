#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PROFILE="${SIDEMANTIC_WASM_PROFILE:-release}"
OUT_DIR="$ROOT_DIR/examples/sidemantic_wasm_demo/vendor/sidemantic"
TARGET_DIR="$ROOT_DIR/sidemantic-rs/target"

if [[ "$PROFILE" == "release" ]]; then
  WASM_PATH="$ROOT_DIR/sidemantic-rs/target/wasm32-unknown-unknown/release/sidemantic.wasm"
else
  WASM_PATH="$ROOT_DIR/sidemantic-rs/target/wasm32-unknown-unknown/debug/sidemantic.wasm"
fi

if [[ "$PROFILE" == "release" ]]; then
  cargo build \
    --manifest-path "$ROOT_DIR/sidemantic-rs/Cargo.toml" \
    --target-dir "$TARGET_DIR" \
    --target wasm32-unknown-unknown \
    --features wasm \
    --lib \
    --release
else
  cargo build \
    --manifest-path "$ROOT_DIR/sidemantic-rs/Cargo.toml" \
    --target-dir "$TARGET_DIR" \
    --target wasm32-unknown-unknown \
    --features wasm \
    --lib
fi

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

wasm-bindgen \
  --target web \
  --out-dir "$OUT_DIR" \
  "$WASM_PATH"

cat > "$OUT_DIR/manifest.json" <<JSON
{
  "crate": "sidemantic-rs",
  "profile": "$PROFILE",
  "target": "wasm32-unknown-unknown",
  "bindings": "wasm-bindgen"
}
JSON
