#!/usr/bin/env python
"""Build the AnyWidget JavaScript bundle or verify its committed artifact."""

from __future__ import annotations

import argparse
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
JS_ROOT = ROOT / "js"
SOURCE = JS_ROOT / "widget.js"
TARGET = ROOT / "sidemantic" / "widget" / "static" / "widget.js"


def files_match(left: Path, right: Path) -> bool:
    return left.is_file() and right.is_file() and left.read_bytes() == right.read_bytes()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Build to a temporary file and verify the committed copy")
    args = parser.parse_args(argv)

    if not (JS_ROOT / "node_modules").exists():
        subprocess.run(["bun", "install"], cwd=JS_ROOT, check=True)

    if not args.check:
        subprocess.run(["bun", "run", "build"], cwd=JS_ROOT, check=True)
        return 0

    with tempfile.TemporaryDirectory(prefix="sidemantic-widget-") as temp_dir:
        candidate = Path(temp_dir) / "widget.js"
        subprocess.run(
            [
                "bun",
                "run",
                "esbuild",
                str(SOURCE),
                "--bundle",
                "--format=esm",
                f"--outfile={candidate}",
                "--minify",
                '--define:process.env.NODE_ENV="production"',
            ],
            cwd=JS_ROOT,
            check=True,
        )
        if not files_match(candidate, TARGET):
            print(f"out of sync: {TARGET.relative_to(ROOT)}")
            return 1

    print("widget JavaScript bundle is in sync")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
