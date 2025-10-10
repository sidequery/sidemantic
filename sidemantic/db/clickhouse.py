"""ClickHouse database adapter."""

from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from sidemantic.db.base import BaseDatabaseAdapter


class ClickHouseResult:
    """Wrapper for ClickHouse query result to match DuckDB result API."""

    def __init__(self, result):
        """Initialize ClickHouse result wrapper.

        Args:
            result: ClickHouse query result from clickhouse-connect
        """
        self._result = result
        self._row_index = 0

    def fetchone(self) -> tuple | None:
        """Fetch one row from the result."""
        if self._row_index >= self._result.row_count:
            return None
        row = self._result.result_rows[self._row_index]
        self._row_index += 1
        return row

    def fetchall(self) -> list[tuple]:
        """Fetch all remaining rows."""
        remaining = self._result.result_rows[self._row_index :]
        self._row_index = self._result.row_count
        return remaining

    def fetch_record_batch(self) -> Any:
        """Convert result to PyArrow RecordBatchReader."""
        import pyarrow as pa

        # Convert ClickHouse result to Arrow
        rows = self._result.result_rows
        if not rows:
            # Empty result
            schema = pa.schema([(name, pa.string()) for name in self._result.column_names])
            return pa.RecordBatchReader.from_batches(schema, [])

        # Build Arrow table from rows
        columns = {name: [row[i] for row in rows] for i, name in enumerate(self._result.column_names)}
        table = pa.table(columns)
        return pa.RecordBatchReader.from_batches(table.schema, table.to_batches())

    @property
    def description(self):
        """Get column descriptions."""
        return [(name, None) for name in self._result.column_names]


class ClickHouseAdapter(BaseDatabaseAdapter):
    """ClickHouse database adapter.

    Example:
        >>> adapter = ClickHouseAdapter(
        ...     host="localhost",
        ...     port=8123,
        ...     database="default",
        ...     user="default",
        ...     password=""
        ... )
        >>> result = adapter.execute("SELECT * FROM table")
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8123,
        database: str = "default",
        user: str | None = None,
        password: str | None = None,
        secure: bool = False,
        **kwargs,
    ):
        """Initialize ClickHouse adapter.

        Args:
            host: ClickHouse host
            port: ClickHouse HTTP port (default: 8123)
            database: Database name
            user: Username
            password: Password
            secure: Use HTTPS instead of HTTP
            **kwargs: Additional arguments passed to clickhouse_connect.get_client
        """
        try:
            import clickhouse_connect
        except ImportError as e:
            raise ImportError(
                "ClickHouse support requires clickhouse-connect. "
                "Install with: pip install sidemantic[clickhouse] or pip install clickhouse-connect"
            ) from e

        # Build connection params
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            database=database,
            username=user,
            password=password,
            secure=secure,
            **kwargs,
        )
        self.database = database

    def execute(self, sql: str) -> ClickHouseResult:
        """Execute SQL query."""
        result = self.client.query(sql)
        return ClickHouseResult(result)

    def executemany(self, sql: str, params: list) -> ClickHouseResult:
        """Execute SQL with multiple parameter sets.

        Note: ClickHouse doesn't have native executemany, so we run queries sequentially.
        """
        results = []
        for param_set in params:
            result = self.client.query(sql, parameters=param_set)
            results.append(ClickHouseResult(result))
        # Return last result for compatibility
        return results[-1] if results else ClickHouseResult(self.client.query("SELECT 1"))

    def fetchone(self, result: ClickHouseResult) -> tuple | None:
        """Fetch one row from result."""
        return result.fetchone()

    def fetch_record_batch(self, result: ClickHouseResult) -> Any:
        """Fetch result as PyArrow RecordBatchReader."""
        return result.fetch_record_batch()

    def get_tables(self) -> list[dict]:
        """List all tables in the database."""
        sql = """
            SELECT name as table_name, database as schema
            FROM system.tables
            WHERE database = %(database)s
                AND engine NOT LIKE '%View%'
        """
        result = self.client.query(sql, parameters={"database": self.database})
        return [{"table_name": row[0], "schema": row[1]} for row in result.result_rows]

    def get_columns(self, table_name: str, schema: str | None = None) -> list[dict]:
        """Get column information for a table."""
        schema = schema or self.database

        sql = """
            SELECT name as column_name, type as data_type
            FROM system.columns
            WHERE database = %(schema)s
                AND table = %(table)s
        """
        result = self.client.query(sql, parameters={"schema": schema, "table": table_name})
        return [{"column_name": row[0], "data_type": row[1]} for row in result.result_rows]

    def get_query_history(self, days_back: int = 7, limit: int = 1000) -> list[str]:
        """Fetch query history from ClickHouse.

        Queries system.query_log to find queries with sidemantic instrumentation.

        Args:
            days_back: Number of days of history to fetch (default: 7)
            limit: Maximum number of queries to return (default: 1000)

        Returns:
            List of SQL query strings containing '-- sidemantic:' comments
        """
        sql = f"""
        SELECT query
        FROM system.query_log
        WHERE event_time >= now() - INTERVAL {days_back} DAY
          AND query LIKE '%-- sidemantic:%'
          AND type = 'QueryFinish'
          AND exception = ''
        ORDER BY event_time DESC
        LIMIT {limit}
        """

        result = self.client.query(sql)
        return [row[0] for row in result.result_rows if row[0]]

    def close(self) -> None:
        """Close the ClickHouse client."""
        self.client.close()

    @property
    def dialect(self) -> str:
        """Return SQL dialect."""
        return "clickhouse"

    @property
    def raw_connection(self) -> Any:
        """Return raw ClickHouse client."""
        return self.client

    @classmethod
    def from_url(cls, url: str) -> "ClickHouseAdapter":
        """Create adapter from connection URL.

        URL format: clickhouse://user:password@host:port/database
        or: clickhouse://host/database  (default user/password)

        Args:
            url: Connection URL

        Returns:
            ClickHouseAdapter instance
        """
        if not url.startswith("clickhouse://"):
            raise ValueError(f"Invalid ClickHouse URL: {url}")

        parsed = urlparse(url)

        # Parse path: /database
        database = parsed.path.lstrip("/") if parsed.path else "default"

        # Parse query parameters
        params = {}
        if parsed.query:
            params = {k: v[0] if len(v) == 1 else v for k, v in parse_qs(parsed.query).items()}

        # Check for secure parameter
        secure = params.pop("secure", "false").lower() in ("true", "1", "yes")

        return cls(
            host=parsed.hostname or "localhost",
            port=parsed.port or 8123,
            database=database,
            user=unquote(parsed.username) if parsed.username else "default",
            password=unquote(parsed.password) if parsed.password else "",
            secure=secure,
            **params,
        )
