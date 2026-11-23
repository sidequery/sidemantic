"""Fast safe YAML loading with a pure-Python fallback."""

from __future__ import annotations

from typing import IO, Any

import yaml

try:  # pragma: no cover - depends on how PyYAML was built
    from yaml import CSafeLoader as _SafeLoader
except ImportError:  # pragma: no cover - Pyodide does not ship libyaml
    from yaml import SafeLoader as _SafeLoader


def safe_load(stream: str | bytes | IO) -> Any:
    """Load one YAML document using libyaml when it is available."""
    return yaml.load(stream, Loader=_SafeLoader)


def safe_load_all(stream: str | bytes | IO) -> Any:
    """Load all YAML documents using libyaml when it is available."""
    return yaml.load_all(stream, Loader=_SafeLoader)
