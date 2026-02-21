"""Snowflake database adapter."""

from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from sidemantic.db.base import BaseDatabaseAdapter, validate_identifier


class SnowflakeResult:
    """Wrapper for Snowflake cursor to match DuckDB result API."""

    def __init__(self, cursor):
        """Initialize Snowflake result wrapper.

        Args:
            cursor: Snowflake cursor object
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
        """Convert result to PyArrow RecordBatchReader.

        Uses native Arrow fetching when available for better performance.
        Falls back to row-by-row conversion for older connector versions.
        """
        import pyarrow as pa

        # Try native Arrow fetching first (available since snowflake-connector-python 2.4.0)
        try:
            arrow_table = self.cursor.fetch_arrow_all()
            if arrow_table is None or arrow_table.num_rows == 0:
                schema = pa.schema([(desc[0], pa.string()) for desc in self._description])
                return pa.RecordBatchReader.from_batches(schema, [])
            return pa.RecordBatchReader.from_batches(arrow_table.schema, arrow_table.to_batches())
        except Exception:
            # Fallback for older connector versions or unsupported result types
            rows = self.cursor.fetchall()
            if not rows:
                schema = pa.schema([(desc[0], pa.string()) for desc in self._description])
                return pa.RecordBatchReader.from_batches(schema, [])
            columns = {desc[0]: [row[i] for row in rows] for i, desc in enumerate(self._description)}
            table = pa.table(columns)
            return pa.RecordBatchReader.from_batches(table.schema, table.to_batches())

    @property
    def description(self):
        """Get column descriptions."""
        return self._description


class SnowflakeAdapter(BaseDatabaseAdapter):
    """Snowflake database adapter.

    Example:
        >>> adapter = SnowflakeAdapter(
        ...     account="myaccount",
        ...     user="myuser",
        ...     password="mypass",
        ...     database="mydb",
        ...     schema="myschema"
        ... )
        >>> result = adapter.execute("SELECT * FROM table")
    """

    def __init__(
        self,
        account: str | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        warehouse: str | None = None,
        role: str | None = None,
        **kwargs,
    ):
        """Initialize Snowflake adapter.

        Args:
            account: Snowflake account identifier
            user: Username
            password: Password
            database: Database name
            schema: Schema name
            warehouse: Warehouse name
            role: Role name
            **kwargs: Additional arguments passed to snowflake.connector.connect
        """
        try:
            import snowflake.connector
        except ImportError as e:
            raise ImportError(
                "Snowflake support requires snowflake-connector-python. "
                "Install with: pip install sidemantic[snowflake] or pip install snowflake-connector-python"
            ) from e

        # Build connection params
        conn_params = {}
        if account:
            conn_params["account"] = account
        if user:
            conn_params["user"] = user
        if password:
            conn_params["password"] = password
        if database:
            conn_params["database"] = database
        if schema:
            conn_params["schema"] = schema
        if warehouse:
            conn_params["warehouse"] = warehouse
        if role:
            conn_params["role"] = role

        # Merge with additional kwargs
        conn_params.update(kwargs)

        self.conn = snowflake.connector.connect(**conn_params)
        self.database = database
        self.schema = schema

    def execute(self, sql: str) -> SnowflakeResult:
        """Execute SQL query."""
        cursor = self.conn.cursor()
        cursor.execute(sql)
        return SnowflakeResult(cursor)

    def executemany(self, sql: str, params: list) -> SnowflakeResult:
        """Execute SQL with multiple parameter sets."""
        cursor = self.conn.cursor()
        cursor.executemany(sql, params)
        return SnowflakeResult(cursor)

    def fetchone(self, result: SnowflakeResult) -> tuple | None:
        """Fetch one row from result."""
        return result.fetchone()

    def fetch_record_batch(self, result: SnowflakeResult) -> Any:
        """Fetch result as PyArrow RecordBatchReader."""
        return result.fetch_record_batch()

    def _query_info_schema_columns(self, table_name: str, schema: str | None, db_prefix: str = "") -> list[dict]:
        """Query information_schema.columns for a table.

        Args:
            table_name: Table name (already validated)
            schema: Schema name or None (already validated)
            db_prefix: Optional database prefix for cross-database lookups (e.g., "MYDB.")
        """
        schema_filter = f"AND table_schema = '{schema}'" if schema else ""
        sql = f"""
            SELECT column_name, data_type
            FROM {db_prefix}information_schema.columns
            WHERE table_name = '{table_name}' {schema_filter}
        """
        result = self.execute(sql)
        rows = result.fetchall()
        return [{"column_name": row[0], "data_type": row[1]} for row in rows]

    def get_tables(self) -> list[dict]:
        """List all tables in the database/schema.

        Tries uppercase identifiers first (Snowflake default for unquoted identifiers),
        then falls back to original case for quoted identifiers.
        """
        if self.schema:
            validate_identifier(self.schema, "schema")
            # Try uppercase first (unquoted identifiers), fall back to original case (quoted)
            schema_upper = self.schema.upper()
            sql = f"""
                SELECT table_name, table_schema as schema
                FROM information_schema.tables
                WHERE table_schema = '{schema_upper}'
                    AND table_type = 'BASE TABLE'
            """
            result = self.execute(sql)
            rows = result.fetchall()
            if not rows and schema_upper != self.schema:
                sql = f"""
                    SELECT table_name, table_schema as schema
                    FROM information_schema.tables
                    WHERE table_schema = '{self.schema}'
                        AND table_type = 'BASE TABLE'
                """
                result = self.execute(sql)
                rows = result.fetchall()
        else:
            sql = """
                SELECT table_name, table_schema as schema
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
            """
            result = self.execute(sql)
            rows = result.fetchall()

        return [{"table_name": row[0], "schema": row[1]} for row in rows]

    def get_columns(self, table_name: str, schema: str | None = None) -> list[dict]:
        """Get column information for a table.

        Handles cross-database lookups: if table_name contains a database qualifier
        (e.g., "other_db.my_schema.my_table"), queries that database's information_schema.

        Tries uppercase identifiers first (Snowflake default for unquoted identifiers),
        then falls back to original case for quoted identifiers.
        """
        # Parse potential database.schema.table or schema.table references
        parts = table_name.split(".")
        db_prefix = ""
        if len(parts) == 3:
            validate_identifier(parts[0], "database")
            validate_identifier(parts[1], "schema")
            validate_identifier(parts[2], "table name")
            db_prefix = f"{parts[0].upper()}."
            schema = parts[1]
            table_name = parts[2]
        elif len(parts) == 2:
            validate_identifier(parts[0], "schema")
            validate_identifier(parts[1], "table name")
            schema = parts[0]
            table_name = parts[1]
        else:
            validate_identifier(table_name, "table name")
            schema = schema or self.schema

        if schema:
            validate_identifier(schema, "schema")

        # Try uppercase first (unquoted identifiers), fall back to original case (quoted)
        table_upper = table_name.upper()
        schema_upper = schema.upper() if schema else None
        rows = self._query_info_schema_columns(table_upper, schema_upper, db_prefix)
        if not rows and (table_upper != table_name or schema_upper != schema):
            rows = self._query_info_schema_columns(table_name, schema, db_prefix)
        return rows

    def get_query_history(self, days_back: int = 7, limit: int = 1000) -> list[str]:
        """Fetch query history from Snowflake.

        Queries INFORMATION_SCHEMA.QUERY_HISTORY to find queries with sidemantic instrumentation.

        Args:
            days_back: Number of days of history to fetch (default: 7, max: 7 for INFORMATION_SCHEMA)
            limit: Maximum number of queries to return (default: 1000)

        Returns:
            List of SQL query strings containing '-- sidemantic:' comments
        """
        sql = f"""
        SELECT query_text
        FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
            END_TIME_RANGE_START => DATEADD('day', -{days_back}, CURRENT_TIMESTAMP())
        ))
        WHERE query_text LIKE '%-- sidemantic:%'
          AND execution_status = 'SUCCESS'
        ORDER BY start_time DESC
        LIMIT {limit}
        """

        result = self.execute(sql)
        rows = result.fetchall()
        return [row[0] for row in rows if row[0]]

    def close(self) -> None:
        """Close the Snowflake connection."""
        self.conn.close()

    @property
    def dialect(self) -> str:
        """Return SQL dialect."""
        return "snowflake"

    @property
    def raw_connection(self) -> Any:
        """Return raw Snowflake connection."""
        return self.conn

    @classmethod
    def from_url(cls, url: str) -> "SnowflakeAdapter":
        """Create adapter from connection URL.

        URL format: snowflake://user:password@account/database/schema?warehouse=wh&role=myrole
        Minimal: snowflake://user:password@account

        Args:
            url: Connection URL

        Returns:
            SnowflakeAdapter instance
        """
        if not url.startswith("snowflake://"):
            raise ValueError(f"Invalid Snowflake URL: {url}")

        parsed = urlparse(url)

        # Parse path: /database/schema
        path_parts = [p for p in parsed.path.split("/") if p]
        database = path_parts[0] if len(path_parts) > 0 else None
        schema = path_parts[1] if len(path_parts) > 1 else None

        # Parse query parameters
        params = {}
        if parsed.query:
            params = {k: v[0] if len(v) == 1 else v for k, v in parse_qs(parsed.query).items()}

        return cls(
            account=parsed.hostname,
            user=unquote(parsed.username) if parsed.username else None,
            password=unquote(parsed.password) if parsed.password else None,
            database=database,
            schema=schema,
            warehouse=params.pop("warehouse", None),
            role=params.pop("role", None),
            **params,
        )
