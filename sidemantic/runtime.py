"""Shared runtime defaults without importing the native extension."""

import os
import sys
from typing import Literal, cast


def default_engine() -> Literal["python", "rust", "auto"]:
    """Use Rust on native hosts and Python in Pyodide; allow a process override."""
    engine = os.environ.get("SIDEMANTIC_ENGINE", "python" if sys.platform == "emscripten" else "rust").lower()
    if engine not in {"python", "rust", "auto"}:
        raise ValueError("SIDEMANTIC_ENGINE must be one of: python, rust, auto")
    return cast(Literal["python", "rust", "auto"], engine)
