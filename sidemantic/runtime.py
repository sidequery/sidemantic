"""Shared runtime defaults without importing the native extension."""

import os
from typing import Literal, cast


def default_engine() -> Literal["python", "rust", "auto"]:
    """Keep Python as the default; allow explicit Rust or auto selection."""
    engine = os.environ.get("SIDEMANTIC_ENGINE", "python").lower()
    if engine not in {"python", "rust", "auto"}:
        raise ValueError("SIDEMANTIC_ENGINE must be one of: python, rust, auto")
    return cast(Literal["python", "rust", "auto"], engine)
