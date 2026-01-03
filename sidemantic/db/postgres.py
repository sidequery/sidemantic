"""PostgreSQL database adapter."""

from typing import Any
from urllib.parse import parse_qs, urlparse

from sidemantic.db.base import BaseDatabaseAdapter, validate_identifier


class PostgresResult:
    """Wrapper for PostgreSQL cursor to match DuckDB result API."""

    def __init__(self, cursor):
        """Initialize result wrapper.

        Args:
            cursor: psycopg cursor object
        """
        self.cursor = cursor
        self._description = cursor.description

    def fetchone(self) -> tuple | None:
        """Fetch one row."""
        return self.cursor.fetchone()

    def fetchall(self) -> list[tuple]:
        """Fetch all rows."""
        return self.cursor.fetchall()

    @property
    def description(self):
        """Get column descriptions."""
        return self._description

    def fetch_record_batch(self) -> Any:
        """Fetch result as Arrow RecordBatch.

        Note: Requires psycopg[binary] with Arrow support.
        """
        try:
            import pyarrow as pa

            # Fetch all rows and convert to Arrow
            rows = self.cursor.fetchall()
            if not rows:
                # Empty result
                schema = pa.schema([(desc.name, pa.string()) for desc in self._description])
                return pa.RecordBatchReader.from_batches(schema, [])

            # Build Arrow table from rows
            columns = {desc.name: [row[i] for row in rows] for i, desc in enumerate(self._description)}
            table = pa.table(columns)
            return pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
        except ImportError:
            raise ImportError("PyArrow is required for Arrow support. Install with: pip install pyarrow")


class PostgreSQLAdapter(BaseDatabaseAdapter):
    """PostgreSQL database adapter.

    Uses psycopg3 for connection management.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "postgres",
        user: str | None = None,
        password: str | None = None,
        **kwargs,
    ):
        """Initialize PostgreSQL adapter.

        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Username
            password: Password
            **kwargs: Additional psycopg connection parameters
        """
        try:
            import psycopg
        except ImportError:
            raise ImportError(
                "psycopg is required for PostgreSQL support. "
                "Install with: pip install 'sidemantic[postgres]' or pip install psycopg[binary]"
            )

        conninfo = f"host={host} port={port} dbname={database}"
        if user:
            conninfo += f" user={user}"
        if password:
            conninfo += f" password={password}"

        self.conn = psycopg.connect(conninfo, **kwargs)
        self.conn.autocommit = True  # Auto-commit for ease of use

    def execute(self, sql: str) -> PostgresResult:
        """Execute SQL and return wrapped cursor."""
        cursor = self.conn.cursor()
        cursor.execute(sql)
        return PostgresResult(cursor)

    def executemany(self, sql: str, params: list) -> PostgresResult:
        """Execute SQL with multiple parameter sets."""
        cursor = self.conn.cursor()
        cursor.executemany(sql, params)
        return PostgresResult(cursor)

    def fetchone(self, result: PostgresResult) -> tuple | None:
        """Fetch one row from result."""
        return result.fetchone()

    def fetch_record_batch(self, result: PostgresResult) -> Any:
        """Fetch result as Arrow RecordBatch."""
        return result.fetch_record_batch()

    def get_tables(self) -> list[dict]:
        """Get list of tables in database."""
        result = self.execute(
            """
            SELECT table_name, table_schema as schema
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                AND table_type = 'BASE TABLE'
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

        schema_filter = f"AND table_schema = '{schema}'" if schema else ""
        result = self.execute(
            f"""
            SELECT column_name, data_type
            FROM information_schema.columns
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
        return "postgres"

    @property
    def raw_connection(self) -> Any:
        """Get underlying psycopg connection."""
        return self.conn

    @classmethod
    def from_url(cls, url: str) -> "PostgreSQLAdapter":
        """Create adapter from connection URL.

        Args:
            url: Connection URL (e.g., "postgres://user:pass@localhost:5432/dbname")

        Returns:
            PostgreSQLAdapter instance
        """
        if not url.startswith(("postgres://", "postgresql://")):
            raise ValueError(f"Invalid PostgreSQL URL: {url}")

        parsed = urlparse(url)

        # Parse query parameters
        params = {}
        if parsed.query:
            params = {k: v[0] if len(v) == 1 else v for k, v in parse_qs(parsed.query).items()}

        return cls(
            host=parsed.hostname or "localhost",
            port=parsed.port or 5432,
            database=parsed.path.lstrip("/") if parsed.path else "postgres",
            user=parsed.username,
            password=parsed.password,
            **params,
        )
