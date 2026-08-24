#!/usr/bin/env python3
"""Verify the bundled Malloy grammar against its recorded provenance."""

from __future__ import annotations

import argparse
import hashlib
import json
import urllib.request
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
GRAMMAR_DIR = REPO_ROOT / "sidemantic" / "adapters" / "malloy_grammar"
PROVENANCE_PATH = GRAMMAR_DIR / "UPSTREAM.json"
_NON_GENERATED_FILES = {
    "__init__.py",
    "MalloyLexer.g4",
    "MalloyParser.g4",
    "UPSTREAM.json",
    "UPSTREAM.md",
}


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _read_provenance() -> dict[str, Any]:
    return json.loads(PROVENANCE_PATH.read_text())


def _check_digest(label: str, content: bytes, expected: str) -> None:
    actual = _sha256(content)
    if actual != expected:
        raise RuntimeError(f"{label} drifted: expected sha256 {expected}, got {actual}")


def check_local_bundle(provenance: dict[str, Any]) -> None:
    for name, metadata in provenance["grammar_files"].items():
        _check_digest(name, (GRAMMAR_DIR / name).read_bytes(), metadata["vendored_sha256"])
    generated_files = provenance["generated_files"]
    recorded = set(generated_files)
    actual = {path.name for path in GRAMMAR_DIR.iterdir() if path.is_file()} - _NON_GENERATED_FILES
    if actual != recorded:
        missing = sorted(recorded - actual)
        unrecorded = sorted(actual - recorded)
        details = []
        if missing:
            details.append(f"missing from grammar directory: {', '.join(missing)}")
        if unrecorded:
            details.append(f"unrecorded generated files: {', '.join(unrecorded)}")
        raise RuntimeError(f"Malloy generated-file inventory drifted ({'; '.join(details)})")
    for name, expected in generated_files.items():
        _check_digest(name, (GRAMMAR_DIR / name).read_bytes(), expected)


def check_upstream_pin(provenance: dict[str, Any]) -> None:
    repository = provenance["repository"].removeprefix("https://github.com/")
    commit = provenance["commit"]
    for name, metadata in provenance["grammar_files"].items():
        url = f"https://raw.githubusercontent.com/{repository}/{commit}/{metadata['upstream_path']}"
        request = urllib.request.Request(url, headers={"User-Agent": "sidemantic-grammar-drift-check"})
        with urllib.request.urlopen(request, timeout=30) as response:
            content = response.read()
        _check_digest(f"upstream {name} at {commit}", content, metadata["upstream_sha256"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--verify-upstream",
        action="store_true",
        help="also download the grammar from the pinned immutable upstream commit",
    )
    args = parser.parse_args()

    provenance = _read_provenance()
    check_local_bundle(provenance)
    if args.verify_upstream:
        check_upstream_pin(provenance)
    print(f"Malloy grammar matches {provenance['version']} ({provenance['commit']}) and its recorded generated bundle.")


if __name__ == "__main__":
    main()
