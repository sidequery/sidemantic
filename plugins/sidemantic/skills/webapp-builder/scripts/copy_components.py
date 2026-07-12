#!/usr/bin/env python3
"""Copy Sidemantic webapp components into a target project."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

SKILL_ROOT = Path(__file__).resolve().parents[1]
COMPONENT_ROOT = SKILL_ROOT / "assets" / "components"

REACT_COMPONENTS: dict[str, list[str]] = {
    "ui": ["sidemantic-ui.js", "sidemantic-ui.css"],
}

STATIC_COMPONENTS: dict[str, list[str]] = {
    "static-components": ["sidemantic-ui-static.js", "sidemantic-ui.css"],
}

KINDS = {
    "react-tailwind": REACT_COMPONENTS,
    "static": STATIC_COMPONENTS,
}


def _files_for(kind: str, components: list[str]) -> list[Path]:
    manifest = KINDS[kind]
    source_dir = COMPONENT_ROOT.parent / "ui-dist"
    copy_all = "all" in components
    requested = list(manifest) if copy_all else components
    unknown = sorted(set(requested) - set(manifest))
    if unknown:
        raise ValueError(f"Unknown {kind} component(s): {', '.join(unknown)}")

    filenames: list[str] = []
    for component in requested:
        filenames.extend(manifest[component])
    return [source_dir / filename for filename in sorted(set(filenames))]


def _list_components() -> None:
    payload = {
        kind: {
            "components": sorted(manifest),
            "default": "all",
            "files": sorted(path.name for path in (COMPONENT_ROOT / kind).iterdir() if path.is_file()),
        }
        for kind, manifest in KINDS.items()
    }
    print(json.dumps(payload, indent=2, sort_keys=True))


def copy_components(args: argparse.Namespace) -> list[Path]:
    target = args.target.resolve()
    files = _files_for(args.kind, args.components)
    copied: list[Path] = []

    for source in files:
        destination = target / source.name
        if destination.exists() and not args.force:
            raise FileExistsError(f"{destination} already exists. Use --force to overwrite.")
        if not args.dry_run:
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
        copied.append(destination)

    return copied


def check_components(args: argparse.Namespace) -> list[Path]:
    """Return target files that are missing or differ from the canonical assets."""
    target = args.target.resolve()
    mismatched: list[Path] = []

    for source in _files_for(args.kind, args.components):
        destination = target / source.name
        if not destination.exists() or destination.read_bytes() != source.read_bytes():
            mismatched.append(destination)

    return mismatched


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", choices=sorted(KINDS), default="react-tailwind")
    parser.add_argument("--target", type=Path, help="Directory that should receive copied component source")
    parser.add_argument(
        "--component",
        action="append",
        dest="components",
        help="Component to copy. Repeat for several. Defaults to all.",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--force", action="store_true", help="Overwrite existing target files")
    mode.add_argument("--dry-run", action="store_true", help="Print target paths without writing files")
    mode.add_argument("--check", action="store_true", help="Exit nonzero when target files differ from the source")
    parser.add_argument("--list", action="store_true", help="List available component groups and exit")
    args = parser.parse_args()

    if args.list:
        _list_components()
        return 0
    if args.target is None:
        parser.error("--target is required unless --list is used")

    args.components = args.components or ["all"]
    if args.check:
        mismatched = check_components(args)
        if mismatched:
            for path in mismatched:
                print(f"Out of sync {path}", file=sys.stderr)
            return 1
        print(f"Components are in sync: {args.target.resolve()}")
        return 0

    try:
        copied = copy_components(args)
    except (FileExistsError, ValueError) as error:
        print(f"copy_components.py: {error}", file=sys.stderr)
        return 1

    action = "Would copy" if args.dry_run else "Copied"
    for path in copied:
        print(f"{action} {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
