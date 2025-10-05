from __future__ import annotations

from datetime import time
from typing import Any, Tuple

import pandas as pd


def df_rows(df: pd.DataFrame) -> list[Tuple[Any, ...]]:
    """Convert a pandas DataFrame into row tuples with Python primitives."""

    def _normalize(value: Any) -> Any:
        if value is None or pd.isna(value):
            return None
        if isinstance(value, pd.Timestamp):
            if value.time() == time(0, 0):
                return value.date()
            return value.to_pydatetime()
        if hasattr(value, "item"):
            try:
                return value.item()
            except Exception:  # pragma: no cover - best effort fallback
                return value
        return value

    rows: list[Tuple[Any, ...]] = []
    for row in df.itertuples(index=False, name=None):
        rows.append(tuple(_normalize(value) for value in row))
    return rows
