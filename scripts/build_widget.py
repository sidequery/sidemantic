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
HOST_CSS = JS_ROOT / "widget.css"
UI_CSS = ROOT / "plugins" / "sidemantic" / "skills" / "webapp-builder" / "assets" / "ui-dist" / "sidemantic-ui.css"
CSS_TARGET = ROOT / "sidemantic" / "widget" / "static" / "widget.css"


def build_css(target: Path) -> None:
    target.write_text(
        "/* Generated from the canonical UI distribution and js/widget.css. */\n"
        + UI_CSS.read_text()
        + "\n"
        + HOST_CSS.read_text()
    )


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
        build_css(CSS_TARGET)
        return 0

    with tempfile.TemporaryDirectory(prefix="sidemantic-widget-") as temp_dir:
        candidate = Path(temp_dir) / "widget.js"
        css_candidate = Path(temp_dir) / "widget.css"
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
        build_css(css_candidate)
        if not files_match(candidate, TARGET) or not files_match(css_candidate, CSS_TARGET):
            print(f"out of sync: {TARGET.relative_to(ROOT)}")
            return 1

    print("widget JavaScript and CSS bundles are in sync")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
