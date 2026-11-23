#!/usr/bin/env bash
set -euo pipefail

# Build the sqlctx WASM bundle and vendor it into this demo.
#
# The canonical build lives in the sqlctx-wasm package; this script just
# points it at the demo's vendor dir so the no-build static demo stays
# self-contained. Honors SQLCTX_WASM_PROFILE (release|debug).

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
OUT_DIR="$ROOT_DIR/examples/sqlctx_wasm_demo/vendor/sqlctx"

exec "$ROOT_DIR/sqlctx-wasm/scripts/build.sh" "$OUT_DIR"
