"""OSI adapter test configuration.

By default these tests exercise the pure-Python ``OSIAdapter``. Set the
environment variable ``SIDEMANTIC_OSI_BACKEND=rust`` to transparently run the
same tests against the sidemantic-rs implementation via a drop-in shim:

    SIDEMANTIC_OSI_BACKEND=rust uv run pytest tests/adapters/osi

The shim requires the ``sidemantic_rs`` extension to be built
(``maturin develop --manifest-path sidemantic-rs/Cargo.toml --features python``).
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from sidemantic.adapters.osi import OSIAdapter as _PythonOSIAdapter
from sidemantic.core.semantic_graph import SemanticGraph


class RustOSIAdapter:
    """Drop-in ``OSIAdapter`` replacement backed by sidemantic-rs."""

    OSI_VERSION = _PythonOSIAdapter.OSI_VERSION
    SUPPORTED_EXPORT_DIALECTS = _PythonOSIAdapter.SUPPORTED_EXPORT_DIALECTS
    DIALECT_PREFERENCE = _PythonOSIAdapter.DIALECT_PREFERENCE

    def parse(self, source) -> SemanticGraph:
        from sidemantic.rust_bridge import load_osi_graph_with_rust

        source_path = Path(source)
        if not source_path.exists():
            raise FileNotFoundError(f"Path does not exist: {source_path}")
        return load_osi_graph_with_rust(source_path)

    def export(self, graph, output_path, dialects=None) -> None:
        from sidemantic.rust_bridge import export_osi_with_rust

        if not dialects:
            dialects = ["ANSI_SQL"]
        unsupported = [d for d in dialects if d not in self.SUPPORTED_EXPORT_DIALECTS]
        if unsupported:
            supported = ", ".join(self.SUPPORTED_EXPORT_DIALECTS)
            raise ValueError(f"Unsupported OSI export dialect(s): {', '.join(unsupported)}. Supported: {supported}")
        text = export_osi_with_rust(graph, dialects)
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(text)


def _backend_is_rust() -> bool:
    return os.environ.get("SIDEMANTIC_OSI_BACKEND", "python").lower() == "rust"


@pytest.fixture(autouse=True)
def _osi_backend(request, monkeypatch):
    """Swap ``OSIAdapter`` for the Rust-backed shim when the backend is rust."""
    if not _backend_is_rust():
        return
    pytest.importorskip("sidemantic_rs")
    module = request.module
    # Only patch modules that import the symbol under its canonical name. The
    # differential parity test imports it as ``PyOSIAdapter`` and is left alone.
    if getattr(module, "OSIAdapter", None) is not None:
        monkeypatch.setattr(module, "OSIAdapter", RustOSIAdapter, raising=False)
