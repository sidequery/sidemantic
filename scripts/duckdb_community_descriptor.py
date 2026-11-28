"""Render the community registry descriptor from an immutable, complete commit."""

import argparse
import re
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = "sqlctx-duckdb/community/description.yml.in"


def git(repo: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(repo), *args], text=True, stderr=subprocess.PIPE).strip()


def render_descriptor(repo: Path, ref: str) -> str:
    commit = git(repo, "rev-parse", "--verify", "--end-of-options", f"{ref}^{{commit}}")
    # Read all metadata from the selected commit, never from a newer worktree.
    manifest = tomllib.loads(git(repo, "show", f"{commit}:sqlctx-rs/Cargo.toml"))
    version = manifest["package"]["version"]
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?", version):
        raise ValueError(f"Invalid extension version: {version!r}")
    for path in (
        "sqlctx-duckdb/Makefile",
        "sqlctx-duckdb/CMakeLists.txt",
        "sqlctx-duckdb/extension_config.cmake",
        "sqlctx-duckdb/LICENSE",
        "sqlctx-rs/Cargo.lock",
        "sqlctx-rs/include/sqlctx.h",
        "LICENSE",
    ):
        git(repo, "cat-file", "-e", f"{commit}:{path}")
    template = git(repo, "show", f"{commit}:{TEMPLATE}")
    return template.replace("@VERSION@", version).replace("@COMMIT@", commit) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ref", required=True, help="Final tested commit or immutable release tag to submit")
    args = parser.parse_args()
    try:
        sys.stdout.write(render_descriptor(ROOT, args.ref))
    except (subprocess.CalledProcessError, ValueError) as error:
        detail = error.stderr.strip() if isinstance(error, subprocess.CalledProcessError) else str(error)
        parser.exit(1, f"Cannot prepare community descriptor: {detail}\n")


if __name__ == "__main__":
    main()
