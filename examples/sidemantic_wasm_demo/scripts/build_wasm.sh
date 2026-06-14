#!/usr/bin/env bash
set -euo pipefail

# Build the Sidemantic WASM bundle and vendor it into this demo.
#
# The canonical build lives in the sidemantic-wasm package; this script just
# points it at the demo's vendor dir so the no-build static demo stays
# self-contained. Honors SIDEMANTIC_WASM_PROFILE (release|debug).

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
OUT_DIR="$ROOT_DIR/examples/sidemantic_wasm_demo/vendor/sidemantic"

exec "$ROOT_DIR/sidemantic-wasm/scripts/build.sh" "$OUT_DIR"
