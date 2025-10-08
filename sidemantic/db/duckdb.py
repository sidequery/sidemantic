"""DuckDB database adapter."""

from typing import Any

import duckdb

from sidemantic.db.base import BaseDatabaseAdapter


class DuckDBAdapter(BaseDatabaseAdapter):
    """DuckDB database adapter.

    Wraps DuckDB connection to provide unified adapter interface.
    """

    def __init__(self, path: str = ":memory:"):
        """Initialize DuckDB adapter.

        Args:
            path: Database file path or ":memory:" for in-memory database
        """
        self.conn = duckdb.connect(path)

    def execute(self, sql: str) -> Any:
        """Execute SQL and return DuckDB relation."""
        return self.conn.execute(sql)

    def executemany(self, sql: str, params: list) -> Any:
        """Execute SQL with multiple parameter sets."""
        return self.conn.executemany(sql, params)

    def fetchone(self, result: Any) -> tuple | None:
        """Fetch one row from result."""
        return result.fetchone()

    def fetch_record_batch(self, result: Any) -> Any:
        """Fetch result as Arrow RecordBatch."""
        return result.fetch_record_batch()

    def get_tables(self) -> list[dict]:
        """Get list of tables in database."""
        result = self.conn.execute(
            """
            SELECT table_name, schema_name as schema
            FROM duckdb_tables()
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog')
        """
        )
        rows = result.fetchall()
        return [{"table_name": row[0], "schema": row[1]} for row in rows]

    def get_columns(self, table_name: str, schema: str | None = None) -> list[dict]:
        """Get columns for a table."""
        schema_filter = f"AND table_schema = '{schema}'" if schema else ""
        result = self.conn.execute(
            f"""
            SELECT column_name, data_type
            FROM duckdb_columns()
            WHERE table_name = '{table_name}' {schema_filter}
        """
        )
        rows = result.fetchall()
        return [{"column_name": row[0], "data_type": row[1]} for row in rows]

    def close(self) -> None:
        """Close database connection."""
        self.conn.close()

    @property
    def dialect(self) -> str:
        """Get SQLGlot dialect name."""
        return "duckdb"

    @property
    def raw_connection(self) -> Any:
        """Get underlying DuckDB connection."""
        return self.conn

    @classmethod
    def from_url(cls, url: str) -> "DuckDBAdapter":
        """Create adapter from connection URL.

        Args:
            url: Connection URL (e.g., "duckdb:///:memory:" or "duckdb:///path/to/db.duckdb")

        Returns:
            DuckDBAdapter instance
        """
        if not url.startswith("duckdb://"):
            raise ValueError(f"Invalid DuckDB URL: {url}")

        # Remove protocol prefix while preserving leading slash in file paths
        # duckdb:///:memory: -> :memory:
        # duckdb:///tmp/app.db -> /tmp/app.db
        # duckdb:/// -> :memory:
        db_path = url[len("duckdb://") :]

        # Handle :memory: special case (may have leading slash from URI)
        if db_path in ("/:memory:", ":memory:", "", "/"):
            db_path = ":memory:"

        return cls(db_path)
