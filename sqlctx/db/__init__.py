"""Database adapter abstraction layer."""

from sqlctx.db.base import BaseDatabaseAdapter

__all__ = ["BaseDatabaseAdapter"]


def __getattr__(name):
    """Lazy import database adapters to avoid importing optional dependencies."""
    if name == "DuckDBAdapter":
        from sqlctx.db.duckdb import DuckDBAdapter

        return DuckDBAdapter
    if name == "PostgreSQLAdapter":
        from sqlctx.db.postgres import PostgreSQLAdapter

        return PostgreSQLAdapter
    if name == "BigQueryAdapter":
        from sqlctx.db.bigquery import BigQueryAdapter

        return BigQueryAdapter
    if name == "SnowflakeAdapter":
        from sqlctx.db.snowflake import SnowflakeAdapter

        return SnowflakeAdapter
    if name == "ClickHouseAdapter":
        from sqlctx.db.clickhouse import ClickHouseAdapter

        return ClickHouseAdapter
    if name == "DatabricksAdapter":
        from sqlctx.db.databricks import DatabricksAdapter

        return DatabricksAdapter
    if name == "MotherDuckAdapter":
        from sqlctx.db.motherduck import MotherDuckAdapter

        return MotherDuckAdapter
    if name == "ADBCAdapter":
        from sqlctx.db.adbc import ADBCAdapter

        return ADBCAdapter
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
