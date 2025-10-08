"""Database adapter abstraction layer."""

from sidemantic.db.base import BaseDatabaseAdapter

__all__ = ["BaseDatabaseAdapter"]


def __getattr__(name):
    """Lazy import database adapters to avoid importing optional dependencies."""
    if name == "DuckDBAdapter":
        from sidemantic.db.duckdb import DuckDBAdapter

        return DuckDBAdapter
    if name == "PostgreSQLAdapter":
        from sidemantic.db.postgres import PostgreSQLAdapter

        return PostgreSQLAdapter
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
