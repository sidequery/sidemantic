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

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WEBAPP = ROOT / "webapp"
DIST = WEBAPP / "dist"
TARGETS = [ROOT / "sidemantic" / "ui" / "static", ROOT / "sidemantic-rs" / "ui"]


def directories_match(source: Path, target: Path) -> bool:
    """Return whether two directory trees contain the same relative files and bytes."""
    if not source.is_dir() or not target.is_dir():
        return False
    source_files = {path.relative_to(source): path for path in source.rglob("*") if path.is_file()}
    target_files = {path.relative_to(target): path for path in target.rglob("*") if path.is_file()}
    if source_files.keys() != target_files.keys():
        return False
    return all(
        source_path.read_bytes() == target_files[relative].read_bytes()
        for relative, source_path in source_files.items()
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Build and exit nonzero if committed copies differ")
    args = parser.parse_args(argv)

    if not (WEBAPP / "node_modules").exists():
        subprocess.run(["bun", "install"], cwd=WEBAPP, check=True)
    subprocess.run(["bun", "run", "build"], cwd=WEBAPP, check=True)
    if not DIST.exists():
        print("build produced no dist/", file=sys.stderr)
        return 1
    if args.check:
        mismatched = [target for target in TARGETS if not directories_match(DIST, target)]
        if mismatched:
            for target in mismatched:
                print(f"out of sync: {target.relative_to(ROOT)}", file=sys.stderr)
            return 1
        print("web UI copies are in sync")
        return 0

    for target in TARGETS:
        if target.exists():
            shutil.rmtree(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(DIST, target)
        print(f"  synced dist -> {target.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
