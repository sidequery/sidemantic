"""Boundaries for automatically discovered inputs and generated output names."""

from pathlib import Path, PureWindowsPath


def is_within(path: Path, root: Path) -> bool:
    """Check resolved containment, including symlinks in parent directories."""
    return path.resolve().is_relative_to(root.resolve())


def output_child(directory: Path, name: str) -> Path:
    """Resolve a generated single-component name without escaping its directory.

    The caller chooses the output directory explicitly; semantic names must not
    introduce paths or redirect a write through an existing symlink.
    """
    if not name or name in {".", ".."} or any(c in name for c in ("/", "\\", "\0")) or PureWindowsPath(name).drive:
        raise ValueError(f"Unsafe output filename: {name!r}")
    path = directory / name
    if not is_within(path, directory):
        raise ValueError(f"Output path escapes destination: {path}")
    return path
