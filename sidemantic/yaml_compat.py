"""Fast YAML loading helpers.

``yaml.safe_load`` always uses the pure-Python ``SafeLoader`` even when libyaml
is installed; requesting ``CSafeLoader`` explicitly is ~7x faster on real model
files (measured 189.6ms vs 26.0ms on a 39KB semantic-model document). These
helpers pick the C loader when available and fall back transparently (libyaml is
absent in some environments, e.g. Pyodide builds).
"""

from __future__ import annotations

from typing import IO, Any

import yaml

try:  # pragma: no cover - depends on how PyYAML was built
    from yaml import CSafeLoader as _SafeLoader
except ImportError:  # pragma: no cover
    from yaml import SafeLoader as _SafeLoader


def safe_load(stream: str | bytes | IO) -> Any:
    """Drop-in ``yaml.safe_load`` using libyaml's C loader when available."""
    return yaml.load(stream, Loader=_SafeLoader)


def safe_load_all(stream: str | bytes | IO) -> Any:
    """Drop-in ``yaml.safe_load_all`` using libyaml's C loader when available."""
    return yaml.load_all(stream, Loader=_SafeLoader)
