"""DuckDB result API compatibility without eager optional dependencies."""

from typing import Any


def fetch_arrow_reader(result: Any) -> Any:
    """Read Arrow results across DuckDB versions without importing PyArrow eagerly."""
    # DuckDB 2 removed the connection's fetch_record_batch alias. Older
    # supported versions and adapter result wrappers still expose that method.
    fetch = getattr(result, "fetch_record_batch", None)
    if fetch is not None:
        return fetch()
    return result.to_arrow_reader()
