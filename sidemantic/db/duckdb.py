"""DuckDB database adapter."""

from typing import Any
from urllib.parse import parse_qs, urlparse

import duckdb

from sidemantic.db.base import BaseDatabaseAdapter, validate_identifier


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


class _SessionCursor:
    """Independent handle with lazy session setup covered by transport deadlines."""

    def __init__(self, cursor, initialize):
        self._cursor = cursor
        self._initialize = initialize

    def __getattr__(self, name):
        return getattr(self._cursor, name)

    def __enter__(self):
        self._cursor.__enter__()
        return self

    def __exit__(self, *args):
        return self._cursor.__exit__(*args)

    def _ensure_initialized(self):
        if self._initialize is not None:
            self._initialize(self._cursor)
            self._initialize = None

    def execute(self, *args, **kwargs):
        self._ensure_initialized()
        return self._cursor.execute(*args, **kwargs)

    def executemany(self, *args, **kwargs):
        self._ensure_initialized()
        return self._cursor.executemany(*args, **kwargs)


class DuckDBAdapter(BaseDatabaseAdapter):
    """DuckDB database adapter.

    Wraps DuckDB connection to provide unified adapter interface.
    """

    def __init__(
        self,
        path: str = ":memory:",
        read_only: bool = False,
        config: dict[str, Any] | None = None,
        init_sql: list[str] | None = None,
    ):
        """Initialize DuckDB adapter.

        Args:
            path: Database file path or ":memory:" for in-memory database
            read_only: Open database in read-only mode
            config: DuckDB configuration options
            init_sql: SQL statements to run immediately after connecting
                (e.g., loading extensions, attaching catalogs, creating secrets)
        """
        if not read_only and config is None:
            self.conn = duckdb.connect(path)
        elif config is None:
            self.conn = duckdb.connect(path, read_only=read_only)
        else:
            self.conn = duckdb.connect(path, read_only=read_only, config=config)

        self._session_settings = []
        self._session_tables = []
        self._session_views = []
        self._session_database = None
        if init_sql:
            for stmt in init_sql:
                self.conn.execute(stmt)

        if init_sql or config:
            self._capture_session()

    def _capture_session(self) -> None:
        """Snapshot configured settings, temporary tables and views.

        DuckDB cursors share attached catalogs and persistent objects, but not
        settings or temporary objects. Capture initialization once, before any
        request can use the parent connection. Temporary data stays in memory
        and requires no optional Arrow dependency. This captures initialization,
        not later mutations or other session objects such as temporary macros.
        """
        with self.conn.cursor() as baseline:
            defaults = dict(baseline.execute("SELECT name, value FROM duckdb_settings()").fetchall())
        self._session_settings = [
            (name, value)
            for name, value in self.conn.execute("SELECT name, value FROM duckdb_settings()").fetchall()
            if value != defaults.get(name) and name != "schema"
        ]
        self._session_database = self.conn.execute("SELECT current_database()").fetchone()[0]
        tables = self.conn.execute(
            "SELECT table_name, sql FROM duckdb_tables() WHERE temporary ORDER BY table_oid"
        ).fetchall()
        for name, ddl in tables:
            import sqlglot
            from sqlglot import exp

            table = f"temp.main.{_quote_identifier(name)}"
            # DuckDB's column catalogs expose generated expressions as defaults,
            # without a generated-column flag. Inspect its canonical CREATE SQL
            # to distinguish those columns from ordinary DEFAULT expressions.
            definition = sqlglot.parse_one(ddl, dialect="duckdb")
            columns = [
                _quote_identifier(column.name)
                for column in definition.this.expressions
                if isinstance(column, exp.ColumnDef)
                and not any(
                    isinstance(constraint.kind, (exp.GeneratedAsIdentityColumnConstraint, exp.ComputedColumnConstraint))
                    for constraint in column.constraints
                )
            ]
            column_sql = ", ".join(columns)
            rows = self.conn.execute(f"SELECT {column_sql} FROM {table}").fetchall()
            self._session_tables.append((table, ddl, column_sql, rows))
        self._session_views = [
            row[0]
            for row in self.conn.execute(
                "SELECT sql FROM duckdb_views() WHERE temporary AND NOT internal ORDER BY view_oid"
            ).fetchall()
        ]

    def _initialize_cursor(self, cursor) -> None:
        if self._session_database is not None:
            cursor.execute(f"USE {_quote_identifier(self._session_database)}")
        for name, value in self._session_settings:
            cursor.execute(f"SET {_quote_identifier(name)} = ?", [value])
        for table, ddl, column_sql, rows in self._session_tables:
            cursor.execute(ddl)
            if rows:
                placeholders = ", ".join("?" for _ in rows[0])
                cursor.executemany(f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders})", rows)
        for ddl in self._session_views:
            cursor.execute(ddl)

    def cursor(self) -> Any:
        """Return an independent DuckDB cursor for concurrent reads.

        In duckdb-python, ``conn.cursor()`` creates a new connection that shares
        the same underlying database. This is the sanctioned pattern for
        multithreaded reads: each thread gets its own handle, so concurrent
        ``execute`` calls do not serialize on a single connection. The returned
        cursor restores configured session settings and temporary data lazily,
        so transport deadlines also cover setup. It exposes ``execute`` returning
        a relation with ``fetch_record_batch``
        and ``fetchone``, matching the result API used across the codebase.
        """
        cursor = self.conn.cursor()
        if self._session_database is not None:
            return _SessionCursor(cursor, self._initialize_cursor)
        return cursor

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
        from sidemantic.db._duckdb_compat import fetch_arrow_reader

        return fetch_arrow_reader(result)

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
    def from_url(cls, url: str, init_sql: list[str] | None = None) -> "DuckDBAdapter":
        """Create adapter from connection URL.

        Args:
            url: Connection URL (e.g., "duckdb:///:memory:" or "duckdb:///path/to/db.duckdb")
            init_sql: SQL statements to run after connecting (overrides any init_sql in URL params)

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
        url_init_sql: list[str] | None = None

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
            if key == "init_sql":
                url_init_sql = values
                continue
            value = values[-1]
            if key == "read_only":
                read_only = bool(parse_value(value))
            else:
                config[key] = parse_value(value)

        # Parameter init_sql overrides any init_sql found in URL query params
        effective_init_sql = init_sql if init_sql is not None else url_init_sql
        return cls(db_path, read_only=read_only, config=config or None, init_sql=effective_init_sql)
