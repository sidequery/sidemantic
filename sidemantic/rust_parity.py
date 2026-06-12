"""Rust parity gating utilities.

Used to enforce strict no-Python-fallback behavior during migration.
"""

from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


@lru_cache(maxsize=1)
def _load_parity_matrix() -> dict:
    matrix_path = _repo_root() / "docs" / "rust-parity-matrix.json"
    if not matrix_path.exists():
        return {"subsystems": {}}
    try:
        return json.loads(matrix_path.read_text())
    except Exception:
        return {"subsystems": {}}


@lru_cache(maxsize=1)
def strict_targets() -> set[str]:
    raw = os.getenv("SIDEMANTIC_RS_STRICT_SUBSYSTEMS", "").strip()
    if not raw:
        return set()
    return {part.strip() for part in raw.split(",") if part.strip()}


def is_strict_mode() -> bool:
    return bool(strict_targets())


def is_strict_for(subsystem: str) -> bool:
    targets = strict_targets()
    if not targets:
        return False
    if "all" in targets:
        return True
    return subsystem in targets


def subsystem_status(subsystem: str) -> str:
    matrix = _load_parity_matrix()
    subsystems = matrix.get("subsystems", {})
    record = subsystems.get(subsystem, {})
    return record.get("status", "python_only")


def require_rust_subsystem(subsystem: str, feature: str) -> None:
    """Raise when strict mode requires a subsystem that is not rust-backed."""
    if not is_strict_for(subsystem):
        return

    status = subsystem_status(subsystem)
    if status == "rust_backed":
        return

    raise RuntimeError(
        f"[rust-strict:{subsystem}] Feature '{feature}' is not rust-backed (status={status}). "
        "Set SIDEMANTIC_RS_STRICT_SUBSYSTEMS to a narrower scope, or implement this subsystem in sidemantic-rs."
    )
