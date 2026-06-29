#!/usr/bin/env python
"""Build the embeddable web UI and copy the bundle into both backends.

Produces `webapp/dist/` (via bun) and syncs it to the committed embed locations:
  - `sidemantic/ui/static/`   (served by the Python `api-serve --ui`)
  - `sidemantic-rs/ui/`       (baked into the Rust `sidemantic-server` via rust-embed)

These copies are committed (mirroring the committed `widget.js`), so neither backend's build
needs a JS toolchain. Re-run this whenever the webapp changes and commit the result.

Run: uv run scripts/build_webapp.py
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WEBAPP = ROOT / "webapp"
DIST = WEBAPP / "dist"
TARGETS = [ROOT / "sidemantic" / "ui" / "static", ROOT / "sidemantic-rs" / "ui"]


def main() -> int:
    if not (WEBAPP / "node_modules").exists():
        subprocess.run(["bun", "install"], cwd=WEBAPP, check=True)
    subprocess.run(["bun", "run", "build"], cwd=WEBAPP, check=True)
    if not DIST.exists():
        print("build produced no dist/", file=sys.stderr)
        return 1
    for target in TARGETS:
        if target.exists():
            shutil.rmtree(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(DIST, target)
        print(f"  synced dist -> {target.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
