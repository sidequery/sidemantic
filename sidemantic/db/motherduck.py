"""MotherDuck database adapter."""

from typing import Any

import duckdb

from sidemantic.db.base import BaseDatabaseAdapter


class MotherDuckAdapter(BaseDatabaseAdapter):
    """MotherDuck database adapter.

    MotherDuck is a cloud-based DuckDB service. This adapter wraps DuckDB
    connections using the MotherDuck extension.

    Example:
        >>> adapter = MotherDuckAdapter(database="my_db", token="...")
        >>> result = adapter.execute("SELECT * FROM table")
    """

    def __init__(self, database: str = "my_db", token: str | None = None, **kwargs):
        """Initialize MotherDuck adapter.

        Args:
            database: MotherDuck database name (default: "my_db")
            token: MotherDuck service token (optional, uses environment if not provided)
            **kwargs: Additional connection parameters
        """
        # Build MotherDuck connection string
        # Format: md:database_name or md:database_name?motherduck_token=...
        conn_str = f"md:{database}"

        # DuckDB will use MOTHERDUCK_TOKEN env var if token not in connection string
        if token:
            conn_str = f"{conn_str}?motherduck_token={token}"

        self.conn = duckdb.connect(conn_str, **kwargs)
        self.database = database

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
    def from_url(cls, url: str) -> "MotherDuckAdapter":
        """Create adapter from connection URL.

        URL format: duckdb://md:database_name or duckdb://md:
        Examples:
            - duckdb://md:my_db
            - duckdb://md: (uses default database)

        Args:
            url: Connection URL

        Returns:
            MotherDuckAdapter instance
        """
        if not url.startswith("duckdb://md:"):
            raise ValueError(f"Invalid MotherDuck URL: {url} (expected format: duckdb://md:database_name)")

        # Extract everything after duckdb://
        # duckdb://md:my_db -> md:my_db
        conn_part = url[len("duckdb://") :]

        # Extract database name from md:database_name
        if conn_part == "md:" or conn_part == "md":
            database = "my_db"
        else:
            database = conn_part[3:]  # Remove "md:" prefix

        # Token should come from MOTHERDUCK_TOKEN environment variable
        return cls(database=database, token=None)
