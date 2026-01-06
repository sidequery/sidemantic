"""DuckDB database adapter."""

from typing import Any
from urllib.parse import parse_qs, urlparse

import duckdb

from sidemantic.db.base import BaseDatabaseAdapter, validate_identifier


class DuckDBAdapter(BaseDatabaseAdapter):
    """DuckDB database adapter.

    Wraps DuckDB connection to provide unified adapter interface.
    """

    def __init__(self, path: str = ":memory:", read_only: bool = False, config: dict[str, Any] | None = None):
        """Initialize DuckDB adapter.

        Args:
            path: Database file path or ":memory:" for in-memory database
        """
        if not read_only and config is None:
            self.conn = duckdb.connect(path)
        elif config is None:
            self.conn = duckdb.connect(path, read_only=read_only)
        else:
            self.conn = duckdb.connect(path, read_only=read_only, config=config)

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
        # Validate identifiers to prevent SQL injection
        validate_identifier(table_name, "table name")
        if schema:
            validate_identifier(schema, "schema")

        schema_filter = f"AND schema_name = '{schema}'" if schema else ""
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

        parsed = urlparse(url)
        if parsed.scheme != "duckdb":
            raise ValueError(f"Invalid DuckDB URL: {url}")

        # Remove protocol prefix while preserving leading slash in file paths
        # duckdb:///:memory: -> :memory:
        # duckdb:///tmp/app.db -> /tmp/app.db
        # duckdb:/// -> :memory:
        db_path = parsed.path

        # Handle :memory: special case (may have leading slash from URI)
        if db_path in ("/:memory:", ":memory:", "", "/"):
            db_path = ":memory:"

        query = parse_qs(parsed.query)
        read_only = False
        config: dict[str, Any] = {}

        def parse_value(value: str) -> Any:
            lowered = value.lower()
            if lowered in ("true", "false"):
                return lowered == "true"
            if value.isdigit():
                return int(value)
            try:
                return float(value)
            except ValueError:
                return value

        for key, values in query.items():
            if not values:
                continue
            value = values[-1]
            if key == "read_only":
                read_only = bool(parse_value(value))
            else:
                config[key] = parse_value(value)

        return cls(db_path, read_only=read_only, config=config or None)
