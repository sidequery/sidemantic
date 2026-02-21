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
    if name == "BigQueryAdapter":
        from sidemantic.db.bigquery import BigQueryAdapter

        return BigQueryAdapter
    if name == "SnowflakeAdapter":
        from sidemantic.db.snowflake import SnowflakeAdapter

        return SnowflakeAdapter
    if name == "ClickHouseAdapter":
        from sidemantic.db.clickhouse import ClickHouseAdapter

        return ClickHouseAdapter
    if name == "DatabricksAdapter":
        from sidemantic.db.databricks import DatabricksAdapter

        return DatabricksAdapter
    if name == "MotherDuckAdapter":
        from sidemantic.db.motherduck import MotherDuckAdapter

        return MotherDuckAdapter
    if name == "ADBCAdapter":
        from sidemantic.db.adbc import ADBCAdapter

        return ADBCAdapter
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
