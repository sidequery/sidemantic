"""Spark SQL database adapter using PyHive."""

from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from sidemantic.db.base import BaseDatabaseAdapter, validate_identifier


class SparkResult:
    """Wrapper for PyHive cursor to match DuckDB result API."""

    def __init__(self, cursor):
        """Initialize Spark result wrapper.

        Args:
            cursor: PyHive cursor object
        """
        self.cursor = cursor
        self._description = cursor.description

    def fetchone(self) -> tuple | None:
        """Fetch one row from the result."""
        return self.cursor.fetchone()

    def fetchall(self) -> list[tuple]:
        """Fetch all remaining rows."""
        return self.cursor.fetchall()

    def fetch_record_batch(self) -> Any:
        """Convert result to PyArrow RecordBatchReader."""
        import pyarrow as pa

        rows = self.cursor.fetchall()
        if not rows:
            # Empty result
            schema = pa.schema([(desc[0], pa.string()) for desc in self._description])
            return pa.RecordBatchReader.from_batches(schema, [])

        # Build Arrow table from rows
        columns = {desc[0]: [row[i] for row in rows] for i, desc in enumerate(self._description)}
        table = pa.table(columns)
        return pa.RecordBatchReader.from_batches(table.schema, table.to_batches())

    @property
    def description(self):
        """Get column descriptions."""
        return self._description


class SparkAdapter(BaseDatabaseAdapter):
    """Spark SQL database adapter using PyHive for Thrift server connections.

    Example:
        >>> adapter = SparkAdapter(
        ...     host="localhost",
        ...     port=10000,
        ...     database="default"
        ... )
        >>> result = adapter.execute("SELECT * FROM table")
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 10000,
        database: str = "default",
        username: str | None = None,
        password: str | None = None,
        **kwargs,
    ):
        """Initialize Spark adapter.

        Args:
            host: Spark Thrift server hostname
            port: Thrift server port (default: 10000)
            database: Database name (default: "default")
            username: Username (optional)
            password: Password (optional)
            **kwargs: Additional arguments passed to pyhive.hive.connect
        """
        try:
            from pyhive import hive
        except ImportError as e:
            raise ImportError(
                "Spark support requires PyHive. "
                "Install with: pip install sidemantic[spark] or pip install 'PyHive[hive]'"
            ) from e

        # Build connection params
        conn_params = {
            "host": host,
            "port": port,
            "database": database,
        }

        if username:
            conn_params["username"] = username
        if password:
            conn_params["password"] = password

        # Merge with additional kwargs
        conn_params.update(kwargs)

        self.conn = hive.connect(**conn_params)
        self.database = database

    def execute(self, sql: str) -> SparkResult:
        """Execute SQL query."""
        cursor = self.conn.cursor()
        cursor.execute(sql)
        return SparkResult(cursor)

    def executemany(self, sql: str, params: list) -> SparkResult:
        """Execute SQL with multiple parameter sets."""
        cursor = self.conn.cursor()
        cursor.executemany(sql, params)
        return SparkResult(cursor)

    def fetchone(self, result: SparkResult) -> tuple | None:
        """Fetch one row from result."""
        return result.fetchone()

    def fetch_record_batch(self, result: SparkResult) -> Any:
        """Fetch result as PyArrow RecordBatchReader."""
        return result.fetch_record_batch()

    def get_tables(self) -> list[dict]:
        """List all tables in the database."""
        # Validate database to prevent SQL injection
        validate_identifier(self.database, "database")
        sql = f"SHOW TABLES IN {self.database}"
        result = self.execute(sql)
        rows = result.fetchall()
        return [{"table_name": row[1], "schema": row[0]} for row in rows]

    def get_columns(self, table_name: str, schema: str | None = None) -> list[dict]:
        """Get column information for a table."""
        # Validate identifiers to prevent SQL injection
        validate_identifier(table_name, "table name")
        schema = schema or self.database
        if schema:
            validate_identifier(schema, "schema")

        table_ref = f"{schema}.{table_name}" if schema else table_name

        sql = f"DESCRIBE {table_ref}"
        result = self.execute(sql)
        rows = result.fetchall()
        return [{"column_name": row[0], "data_type": row[1]} for row in rows]

    def close(self) -> None:
        """Close the Spark connection."""
        self.conn.close()

    @property
    def dialect(self) -> str:
        """Return SQL dialect."""
        return "spark"

    @property
    def raw_connection(self) -> Any:
        """Return raw PyHive connection."""
        return self.conn

    @classmethod
    def from_url(cls, url: str) -> "SparkAdapter":
        """Create adapter from connection URL.

        URL format: spark://host:port/database
        Example: spark://localhost:10000/default

        Args:
            url: Connection URL

        Returns:
            SparkAdapter instance
        """
        if not url.startswith("spark://"):
            raise ValueError(f"Invalid Spark URL: {url}")

        parsed = urlparse(url)

        # Parse hostname and port
        host = parsed.hostname or "localhost"
        port = parsed.port or 10000

        # Parse database from path
        database = parsed.path.lstrip("/") if parsed.path else "default"

        # Parse username
        username = unquote(parsed.username) if parsed.username else None
        password = unquote(parsed.password) if parsed.password else None

        # Parse query parameters
        params = {}
        if parsed.query:
            params = {k: v[0] if len(v) == 1 else v for k, v in parse_qs(parsed.query).items()}

        return cls(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            **params,
        )
