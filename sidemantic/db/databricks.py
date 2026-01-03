"""Databricks/Spark SQL database adapter."""

from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from sidemantic.db.base import BaseDatabaseAdapter, validate_identifier


class DatabricksResult:
    """Wrapper for Databricks cursor to match DuckDB result API."""

    def __init__(self, cursor):
        """Initialize Databricks result wrapper.

        Args:
            cursor: Databricks cursor object
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

        # Databricks cursor may support Arrow format directly
        # For now, convert from standard result
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


class DatabricksAdapter(BaseDatabaseAdapter):
    """Databricks/Spark SQL database adapter.

    Example:
        >>> adapter = DatabricksAdapter(
        ...     server_hostname="your-workspace.cloud.databricks.com",
        ...     http_path="/sql/1.0/warehouses/abc123",
        ...     access_token="dapi..."
        ... )
        >>> result = adapter.execute("SELECT * FROM table")
    """

    def __init__(
        self,
        server_hostname: str,
        http_path: str,
        access_token: str | None = None,
        catalog: str | None = None,
        schema: str | None = None,
        **kwargs,
    ):
        """Initialize Databricks adapter.

        Args:
            server_hostname: Databricks workspace hostname
            http_path: SQL warehouse HTTP path
            access_token: Personal access token or service principal token
            catalog: Unity Catalog name (optional)
            schema: Schema/database name (optional)
            **kwargs: Additional arguments passed to databricks.sql.connect
        """
        try:
            from databricks import sql
        except ImportError as e:
            raise ImportError(
                "Databricks support requires databricks-sql-connector. "
                "Install with: pip install sidemantic[databricks] or pip install databricks-sql-connector"
            ) from e

        # Build connection params
        conn_params = {
            "server_hostname": server_hostname,
            "http_path": http_path,
        }

        if access_token:
            conn_params["access_token"] = access_token

        if catalog:
            conn_params["catalog"] = catalog

        if schema:
            conn_params["schema"] = schema

        # Merge with additional kwargs
        conn_params.update(kwargs)

        self.conn = sql.connect(**conn_params)
        self.catalog = catalog
        self.schema = schema

    def execute(self, sql: str) -> DatabricksResult:
        """Execute SQL query."""
        cursor = self.conn.cursor()
        cursor.execute(sql)
        return DatabricksResult(cursor)

    def executemany(self, sql: str, params: list) -> DatabricksResult:
        """Execute SQL with multiple parameter sets."""
        cursor = self.conn.cursor()
        cursor.executemany(sql, params)
        return DatabricksResult(cursor)

    def fetchone(self, result: DatabricksResult) -> tuple | None:
        """Fetch one row from result."""
        return result.fetchone()

    def fetch_record_batch(self, result: DatabricksResult) -> Any:
        """Fetch result as PyArrow RecordBatchReader."""
        return result.fetch_record_batch()

    def get_tables(self) -> list[dict]:
        """List all tables in the catalog/schema."""
        if self.schema:
            # Validate schema to prevent SQL injection
            validate_identifier(self.schema, "schema")
            sql = f"SHOW TABLES IN {self.schema}"
        elif self.catalog:
            # Validate catalog to prevent SQL injection
            validate_identifier(self.catalog, "catalog")
            sql = f"SHOW TABLES IN {self.catalog}"
        else:
            sql = "SHOW TABLES"

        result = self.execute(sql)
        rows = result.fetchall()
        return [{"table_name": row[1], "schema": row[0]} for row in rows]

    def get_columns(self, table_name: str, schema: str | None = None) -> list[dict]:
        """Get column information for a table."""
        # Validate identifiers to prevent SQL injection
        validate_identifier(table_name, "table name")
        schema = schema or self.schema
        if schema:
            validate_identifier(schema, "schema")

        table_ref = f"{schema}.{table_name}" if schema else table_name

        sql = f"DESCRIBE {table_ref}"
        result = self.execute(sql)
        rows = result.fetchall()
        return [{"column_name": row[0], "data_type": row[1]} for row in rows]

    def get_query_history(self, days_back: int = 7, limit: int = 1000) -> list[str]:
        """Fetch query history from Databricks.

        Queries system.query.history (Unity Catalog) to find queries with sidemantic instrumentation.

        Args:
            days_back: Number of days of history to fetch (default: 7)
            limit: Maximum number of queries to return (default: 1000)

        Returns:
            List of SQL query strings containing '-- sidemantic:' comments

        Note:
            Requires Unity Catalog and appropriate permissions to query system.query.history
        """
        sql = f"""
        SELECT statement_text
        FROM system.query.history
        WHERE start_time >= CURRENT_TIMESTAMP() - INTERVAL {days_back} DAYS
          AND statement_text LIKE '%-- sidemantic:%'
          AND status = 'FINISHED'
        ORDER BY start_time DESC
        LIMIT {limit}
        """

        result = self.execute(sql)
        rows = result.fetchall()
        return [row[0] for row in rows if row[0]]

    def close(self) -> None:
        """Close the Databricks connection."""
        self.conn.close()

    @property
    def dialect(self) -> str:
        """Return SQL dialect."""
        return "databricks"

    @property
    def raw_connection(self) -> Any:
        """Return raw Databricks connection."""
        return self.conn

    @classmethod
    def from_url(cls, url: str) -> "DatabricksAdapter":
        """Create adapter from connection URL.

        URL format: databricks://token@server-hostname/http-path?catalog=x&schema=y
        Example: databricks://dapi123@my-workspace.cloud.databricks.com/sql/1.0/warehouses/abc?catalog=main&schema=default

        Args:
            url: Connection URL

        Returns:
            DatabricksAdapter instance
        """
        if not url.startswith("databricks://"):
            raise ValueError(f"Invalid Databricks URL: {url}")

        parsed = urlparse(url)

        # Parse hostname
        server_hostname = parsed.hostname
        if not server_hostname:
            raise ValueError("Databricks URL must include server hostname")

        # Parse path as http_path (everything after hostname)
        http_path = parsed.path or ""

        # Parse token from username (password is ignored)
        access_token = unquote(parsed.username) if parsed.username else None

        # Parse query parameters for catalog and schema
        params = {}
        if parsed.query:
            params = {k: v[0] if len(v) == 1 else v for k, v in parse_qs(parsed.query).items()}

        catalog = params.pop("catalog", None)
        schema = params.pop("schema", None)

        return cls(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
            catalog=catalog,
            schema=schema,
            **params,
        )
