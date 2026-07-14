#!/usr/bin/env python3
"""Build the single-source React UI distribution used by skills and runtimes."""

from __future__ import annotations

import argparse
import filecmp
import shutil
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENTRY = ROOT / "webapp" / "src" / "ui.ts"
STATIC_ENTRY = ROOT / "webapp" / "src" / "static-api.tsx"
DEST = ROOT / "plugins" / "sidemantic" / "skills" / "webapp-builder" / "assets" / "ui-dist"


def build(target: Path) -> None:
    target.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "bun",
            "build",
            str(ENTRY),
            "--target=browser",
            "--format=esm",
            '--define=process.env.NODE_ENV="production"',
            "--external=react",
            "--external=react-dom",
            f"--outfile={target / 'sidemantic-ui.js'}",
        ],
        cwd=ROOT,
        check=True,
    )
    subprocess.run(
        [
            "bun",
            "build",
            str(STATIC_ENTRY),
            "--target=browser",
            "--format=esm",
            '--define=process.env.NODE_ENV="production"',
            f"--outfile={target / 'sidemantic-ui-static.js'}",
        ],
        cwd=ROOT,
        check=True,
    )
    subprocess.run(
        [
            "bunx",
            "tailwindcss",
            "-c",
            str(ROOT / "webapp" / "tailwind.config.ts"),
            "-i",
            str(ROOT / "webapp" / "src" / "index.css"),
            "-o",
            str(target / "sidemantic-ui.css"),
            "--minify",
        ],
        cwd=ROOT / "webapp",
        check=True,
    )


def matches(left: Path, right: Path) -> bool:
    names = {path.name for path in left.iterdir()} | {path.name for path in right.iterdir()}
    return all(
        (left / name).is_file() and (right / name).is_file() and filecmp.cmp(left / name, right / name, shallow=False)
        for name in names
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.check:
        with tempfile.TemporaryDirectory(prefix="sidemantic-ui-") as directory:
            candidate = Path(directory)
            build(candidate)
            if not DEST.exists() or not matches(candidate, DEST):
                print("UI distribution is out of sync")
                return 1
        print("UI distribution is in sync")
        return 0
    if DEST.exists():
        shutil.rmtree(DEST)
    build(DEST)
    print(f"Built canonical UI distribution -> {DEST}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
