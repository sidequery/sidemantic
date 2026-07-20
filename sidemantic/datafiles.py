"""Shared helpers for querying raw data files (CSV/Parquet/JSON) through DuckDB.

These helpers let ``--db data/orders.csv``, the ``files`` connection type, and
``sidemantic init --from`` all agree on which files are queryable, what table
name a file maps to, and which DuckDB reader loads it.
"""

from __future__ import annotations

import re
from pathlib import Path

DATA_FILE_SUFFIXES = {".csv", ".tsv", ".parquet", ".json", ".jsonl", ".ndjson"}

_READERS = {
    ".csv": "read_csv_auto",
    ".tsv": "read_csv_auto",
    ".parquet": "read_parquet",
    ".json": "read_json_auto",
    ".jsonl": "read_json_auto",
    ".ndjson": "read_json_auto",
}


def is_data_file(path: Path) -> bool:
    """Return whether a path looks like a raw data file DuckDB can read."""

    return path.suffix.lower() in DATA_FILE_SUFFIXES


def table_name_for(path: Path) -> str:
    """Derive a SQL identifier from a data file name (orders.csv -> orders)."""

    stem = re.sub(r"[^0-9A-Za-z_]", "_", path.stem).strip("_").lower() or "data"
    if stem[0].isdigit():
        stem = f"t_{stem}"
    return stem


def reader_sql(path: Path) -> str:
    """Return the DuckDB table function call that reads this file."""

    reader = _READERS[path.suffix.lower()]
    escaped = str(path).replace("'", "''")
    return f"{reader}('{escaped}')"


def build_file_views(paths: list[Path]) -> list[str]:
    """Return CREATE VIEW statements exposing each data file as a table.

    Raises ValueError when two files would map to the same table name, since
    silently shadowing one file would produce wrong results.
    """

    statements: list[str] = []
    seen: dict[str, Path] = {}
    for path in paths:
        if not is_data_file(path):
            supported = ", ".join(sorted(DATA_FILE_SUFFIXES))
            raise ValueError(f"Unsupported data file type: {path} (supported: {supported})")
        name = table_name_for(path)
        if name in seen:
            raise ValueError(f"Data files {seen[name]} and {path} both map to table '{name}'; rename one")
        seen[name] = path
        statements.append(f'CREATE OR REPLACE VIEW "{name}" AS SELECT * FROM {reader_sql(path)}')
    return statements


def discover_data_files(directory: Path) -> list[Path]:
    """Return the raw data files directly inside a directory, sorted by name."""

    if not directory.is_dir():
        return []
    return sorted(
        (path for path in directory.iterdir() if path.is_file() and is_data_file(path)),
        key=lambda path: path.name,
    )
