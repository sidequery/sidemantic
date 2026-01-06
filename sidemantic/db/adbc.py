"""ADBC (Arrow Database Connectivity) adapter.

This adapter uses the ADBC driver manager to connect to databases using
ADBC drivers installed via the `dbc` CLI tool (https://docs.columnar.tech/dbc/).

Supported drivers include: BigQuery, DuckDB, Flight SQL, SQL Server, MySQL,
PostgreSQL, Redshift, Snowflake, SQLite, and Trino.

Example usage:
    # Connect using a DBC-installed driver
    adapter = ADBCAdapter(driver="postgresql", uri="postgresql://localhost/mydb")

    # Or with explicit connection parameters
    adapter = ADBCAdapter(
        driver="postgresql",
        db_kwargs={"host": "localhost", "database": "mydb"}
    )
"""

from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from sidemantic.db.base import BaseDatabaseAdapter, validate_identifier


class ADBCResult:
    """Wrapper for ADBC cursor to match the DuckDB result API."""

    def __init__(self, cursor):
        """Initialize result wrapper.

        Args:
            cursor: ADBC DB-API cursor object
        """
        self.cursor = cursor
        self._description = cursor.description

    def fetchone(self) -> tuple | None:
        """Fetch one row and close cursor.

        Note: Unlike typical DB-API behavior, this closes the cursor after
        fetching to prevent resource leaks with ADBC connections.
        """
        row = self.cursor.fetchone()
        self.close()
        return row

    def fetchall(self) -> list[tuple]:
        """Fetch all rows."""
        rows = self.cursor.fetchall()
        self.close()
        return rows

    @property
    def description(self):
        """Get column descriptions."""
        return self._description

    def close(self) -> None:
        """Close the cursor."""
        try:
            self.cursor.close()
        except Exception:
            pass

    def fetch_record_batch(self) -> Any:
        """Fetch result as Arrow RecordBatch.

        Uses ADBC's native Arrow support for efficient data transfer.
        """
        # ADBC cursors have native Arrow support
        table = self.cursor.fetch_arrow_table()
        self.close()
        import pyarrow as pa

        return pa.RecordBatchReader.from_batches(table.schema, table.to_batches())


# Map driver names to SQLGlot dialect names
DRIVER_DIALECT_MAP = {
    "bigquery": "bigquery",
    "duckdb": "duckdb",
    "flightsql": None,  # Generic SQL, no specific dialect
    "mssql": "tsql",
    "mysql": "mysql",
    "postgresql": "postgres",
    "postgres": "postgres",
    "redshift": "redshift",
    "snowflake": "snowflake",
    "sqlite": "sqlite",
    "trino": "trino",
}


class ADBCAdapter(BaseDatabaseAdapter):
    """ADBC (Arrow Database Connectivity) adapter.

    Uses the adbc_driver_manager to load and connect to databases using ADBC
    drivers. Drivers can be installed via the `dbc` CLI tool or as Python
    packages (e.g., adbc_driver_postgresql).

    This adapter provides a unified interface for databases that have ADBC
    drivers, leveraging Arrow's columnar format for efficient data transfer.
    """

    def __init__(
        self,
        driver: str | Path,
        uri: str | None = None,
        *,
        entrypoint: str | None = None,
        db_kwargs: dict[str, Any] | None = None,
        conn_kwargs: dict[str, Any] | None = None,
        autocommit: bool = True,
    ):
        """Initialize ADBC adapter.

        Args:
            driver: Driver name (e.g., "postgresql", "snowflake"), manifest name
                (e.g., "adbc_driver_postgresql"), or path to a shared library.
            uri: Database connection URI. If provided, takes precedence over
                values in db_kwargs.
            entrypoint: Driver-specific entry point function name (optional).
            db_kwargs: Key-value options for database initialization.
            conn_kwargs: Key-value options for connection initialization.
            autocommit: Whether to enable autocommit mode (default: True).

        Example:
            # Using a connection URI
            adapter = ADBCAdapter("postgresql", uri="postgresql://localhost/mydb")

            # Using explicit parameters
            adapter = ADBCAdapter(
                "snowflake",
                db_kwargs={
                    "account": "myaccount",
                    "warehouse": "mywarehouse",
                    "database": "mydb",
                }
            )
        """
        try:
            import adbc_driver_manager.dbapi as adbc
        except ImportError:
            raise ImportError(
                "adbc_driver_manager is required for ADBC support. "
                "Install with: pip install 'sidemantic[adbc]' or pip install adbc-driver-manager"
            )

        self._driver_name = str(driver).lower()
        self.conn = adbc.connect(
            driver=driver,
            uri=uri,
            entrypoint=entrypoint,
            db_kwargs=db_kwargs,
            conn_kwargs=conn_kwargs,
            autocommit=autocommit,
        )

    def execute(self, sql: str) -> ADBCResult:
        """Execute SQL and return wrapped cursor."""
        cursor = self.conn.cursor()
        cursor.execute(sql)
        return ADBCResult(cursor)

    def executemany(self, sql: str, params: list) -> ADBCResult:
        """Execute SQL with multiple parameter sets."""
        cursor = self.conn.cursor()
        cursor.executemany(sql, params)
        return ADBCResult(cursor)

    def fetchone(self, result: ADBCResult) -> tuple | None:
        """Fetch one row from result."""
        return result.fetchone()

    def fetch_record_batch(self, result: ADBCResult) -> Any:
        """Fetch result as Arrow RecordBatch."""
        return result.fetch_record_batch()

    def get_tables(self) -> list[dict]:
        """Get list of tables in database.

        Uses ADBC's metadata retrieval capabilities when available,
        falling back to information_schema queries.
        """
        try:
            # Try to use ADBC's native metadata retrieval
            reader = self.conn.adbc_get_objects()
            arrow_table = reader.read_all()
            data = arrow_table.to_pydict()

            tables = []
            catalog_schemas = data.get("catalog_db_schemas", [])
            for db_schemas in catalog_schemas:
                if db_schemas is None:
                    continue
                for schema in db_schemas:
                    schema_name = schema.get("db_schema_name", "")
                    db_tables = schema.get("db_schema_tables") or []
                    for table in db_tables:
                        tables.append(
                            {
                                "table_name": table.get("table_name"),
                                "schema": schema_name or None,
                            }
                        )
            if tables:
                return tables
        except Exception:
            pass  # Fall back to SQL query

        # Fall back to information_schema query
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

        try:
            # Try to use ADBC's native table schema retrieval
            arrow_schema = self.conn.adbc_get_table_schema(
                table_name=table_name,
                db_schema=schema,
            )
            return [{"column_name": field.name, "data_type": str(field.type)} for field in arrow_schema]
        except Exception:
            pass  # Fall back to get_objects approach

        try:
            # Try using adbc_get_objects filtered by table
            reader = self.conn.adbc_get_objects(
                db_schema_filter=schema,
                table_name_filter=table_name,
            )
            arrow_table = reader.read_all()
            data = arrow_table.to_pydict()

            for db_schemas in data.get("catalog_db_schemas", []):
                if db_schemas is None:
                    continue
                for db_schema in db_schemas:
                    for table in db_schema.get("db_schema_tables") or []:
                        if table.get("table_name") == table_name:
                            columns = []
                            for col in table.get("table_columns") or []:
                                columns.append(
                                    {
                                        "column_name": col.get("column_name"),
                                        "data_type": col.get("xdbc_type_name", "unknown"),
                                    }
                                )
                            if columns:
                                return columns
        except Exception:
            pass  # Fall back to SQL query

        # Fall back to information_schema query (works for PostgreSQL, etc.)
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
        """Get SQLGlot dialect name.

        Maps the ADBC driver name to the corresponding SQLGlot dialect.
        Returns None for drivers without a specific dialect mapping.
        """
        # Normalize driver name by removing common prefixes
        driver = self._driver_name
        if driver.startswith("adbc_driver_"):
            driver = driver[len("adbc_driver_") :]

        return DRIVER_DIALECT_MAP.get(driver, "")

    @property
    def raw_connection(self) -> Any:
        """Get underlying ADBC connection."""
        return self.conn

    @classmethod
    def from_url(cls, url: str) -> "ADBCAdapter":
        """Create adapter from connection URL.

        Supports two URL formats:

        1. adbc:// scheme (recommended for YAML configs):
           - adbc://driver_name - for drivers that don't need a URI
           - adbc://driver_name/connection_uri - with URI as path
           - adbc://driver_name?uri=<uri> - with URI as query param
           - adbc://driver_name?param1=value1&param2=value2 - with db_kwargs

           Examples:
           - adbc://sqlite
           - adbc://postgresql/postgresql://localhost/mydb
           - adbc://postgresql?uri=postgresql://localhost/mydb
           - adbc://snowflake?account=myaccount&database=mydb

        2. Standard database URL schemes (auto-detected):
           - postgresql://localhost/mydb -> uses postgresql driver
           - snowflake://account/mydb -> uses snowflake driver

        Args:
            url: Connection URL (adbc:// or database-specific scheme)

        Returns:
            ADBCAdapter instance
        """
        from urllib.parse import parse_qs

        parsed = urlparse(url)
        scheme = parsed.scheme.lower()

        # Handle adbc:// scheme
        if scheme == "adbc":
            # Driver name is in the netloc (host) part
            driver = parsed.netloc or parsed.path.lstrip("/").split("/")[0]
            if not driver:
                raise ValueError(
                    "adbc:// URL must specify a driver name. Example: adbc://postgresql?uri=postgresql://localhost/mydb"
                )

            # Parse query parameters
            params = parse_qs(parsed.query)

            # Extract uri: prefer query param, then fall back to path
            # Supports both: adbc://driver?uri=... and adbc://driver/uri
            uri = params.pop("uri", [None])[0]
            if uri is None and parsed.path:
                # Path after driver name becomes URI (e.g., adbc://postgresql/postgresql://host/db)
                path_uri = parsed.path.lstrip("/")
                if path_uri:
                    uri = path_uri

            # SQLite defaults to :memory: if no uri specified
            if driver == "sqlite" and uri is None:
                uri = ":memory:"

            # Remaining params become db_kwargs (flatten single-value lists)
            db_kwargs = {k: v[0] if len(v) == 1 else v for k, v in params.items()} if params else None

            return cls(driver=driver, uri=uri, db_kwargs=db_kwargs)

        # Handle standard database URL schemes
        scheme_to_driver = {
            "postgresql": "postgresql",
            "postgres": "postgresql",
            "mysql": "mysql",
            "sqlite": "sqlite",
            "snowflake": "snowflake",
            "bigquery": "bigquery",
            "mssql": "mssql",
            "trino": "trino",
            "redshift": "redshift",
        }

        driver = scheme_to_driver.get(scheme)
        if not driver:
            raise ValueError(f"Unknown URL scheme: {scheme}. Supported: adbc://, {', '.join(scheme_to_driver.keys())}")

        # SQLite driver expects just the path, not a full URL
        if driver == "sqlite":
            # Extract path from URL (e.g., sqlite:///:memory: -> :memory:)
            uri = parsed.path.lstrip("/") or ":memory:"
            if uri == "":
                uri = ":memory:"
            return cls(driver=driver, uri=uri)

        return cls(driver=driver, uri=url)
