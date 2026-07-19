"""Base database adapter interface."""

import re
import threading
from abc import ABC, abstractmethod
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

# Pattern for valid SQL identifiers: starts with letter or underscore,
# followed by letters, digits, or underscores. Also allows dots for
# qualified names (schema.table).
_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$")

# Guards lazy creation of each adapter's shared-connection lock so two threads
# calling cursor() concurrently cannot end up with two different locks.
_SHARED_LOCK_INIT_GUARD = threading.Lock()


@dataclass(frozen=True, slots=True)
class CancellationOutcome:
    """Best-effort database cancellation result."""

    supported: bool
    cancelled: bool
    diagnostic: str


def validate_identifier(value: str, name: str = "identifier") -> str:
    """Validate that a value is a safe SQL identifier.

    Prevents SQL injection by ensuring identifiers only contain safe characters.
    Allows: letters, digits, underscores, and dots (for qualified names).
    Must start with a letter or underscore.

    Args:
        value: The identifier value to validate
        name: Human-readable name for error messages (e.g., "table name", "schema")

    Returns:
        The validated identifier (unchanged if valid)

    Raises:
        ValueError: If the identifier contains invalid characters
    """
    if not value:
        raise ValueError(f"Invalid {name}: cannot be empty")

    if not _IDENTIFIER_PATTERN.match(value):
        raise ValueError(
            f"Invalid {name}: '{value}'. "
            f"Identifiers must start with a letter or underscore and contain only "
            f"letters, digits, underscores, and dots."
        )

    return value


def _coerce_positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a positive integer")

    if isinstance(value, int):
        coerced = value
    elif isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            raise ValueError(f"{name} must be a positive integer")
        try:
            coerced = int(stripped, 10)
        except ValueError:
            raise ValueError(f"{name} must be a positive integer")
    else:
        raise ValueError(f"{name} must be a positive integer")

    if coerced < 1:
        raise ValueError(f"{name} must be a positive integer")
    return coerced


def validate_query_history_params(
    days_back: int,
    limit: int,
    *,
    max_days_back: int = 365,
    max_limit: int = 10_000,
) -> tuple[int, int]:
    """Validate query-history lookback and row limit values for SQL interpolation."""
    days_back_int = _coerce_positive_int(days_back, "days_back")
    limit_int = _coerce_positive_int(limit, "limit")

    if days_back_int > max_days_back:
        raise ValueError(f"days_back must be <= {max_days_back}")
    if limit_int > max_limit:
        raise ValueError(f"limit must be <= {max_limit}")

    return days_back_int, limit_int


class _MaterializedResult:
    """A fully-materialized query result served entirely from memory.

    Produced by :class:`_SerializedCursor` so that ALL shared-connection access (execute
    plus the fetch/drain) happens while the lock is held; downstream consumers then read
    from the in-memory Arrow table and never touch the shared connection concurrently.
    """

    __slots__ = ("_table", "_rows", "_pos")

    def __init__(self, table: Any):
        self._table = table
        self._rows: list | None = None
        self._pos = 0

    def fetch_record_batch(self) -> Any:
        """Return a RecordBatchReader over the materialized table (no connection access).

        ``result_to_record_batch_reader`` prefers this no-arg method over the adapter path.
        """
        return self._table.to_reader()

    def fetchone(self) -> tuple | None:
        if self._rows is None:
            self._rows = self._table.to_pylist()
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return tuple(row.values())


class _SerializedCursor:
    """Fallback cursor that guards the shared adapter connection with a lock.

    Adapters that do not expose an independent concurrent handle fall back to this
    wrapper. It preserves the pre-existing serialized single-connection behavior: the
    query and result consumption run under a per-adapter lock, so callers that switch
    to ``adapter.cursor()`` for concurrency cannot interleave fetches on a driver that
    does not support concurrent handles. Server query paths use ``execute_stream`` to
    enforce result limits batch-by-batch before materializing a complete Arrow table.
    """

    defer_execution_registration = True

    def __init__(self, adapter: "BaseDatabaseAdapter", lock: threading.Lock):
        self._adapter = adapter
        self._lock = lock

    def execute(self, sql: str) -> Any:
        """Execute SQL and fully materialize the result under the shared-connection lock."""
        with self._lock:
            result = self._adapter.execute(sql)
            # Drain to an in-memory Arrow table WHILE the lock is held so a concurrent
            # request's execute cannot interleave with this result's fetch on the shared
            # connection (which would corrupt driver state / stale the cursor).
            reader = self._adapter.fetch_record_batch(result)
            table = reader.read_all()
        return _MaterializedResult(table)

    @contextmanager
    def execute_stream(self, sql: str, *, on_acquired: Callable[[], None] | None = None):
        """Yield a reader while retaining exclusive access to the shared connection."""
        with self._lock:
            if on_acquired is not None:
                on_acquired()
            result = self._adapter.execute(sql)
            reader = self._adapter.fetch_record_batch(result)
            try:
                yield reader
            finally:
                reader_close = getattr(reader, "close", None)
                if callable(reader_close):
                    reader_close()
                result_cursor = getattr(result, "cursor", None)
                result_close = getattr(result_cursor or result, "close", None)
                if callable(result_close):
                    result_close()

    def execute_control(self, sql: str) -> None:
        """Execute a session control command without trying to fetch rows."""
        with self._lock:
            result = self._adapter.execute(sql)
            result_cursor = getattr(result, "cursor", None)
            close = getattr(result_cursor or result, "close", None)
            if callable(close):
                close()

    def fetch_record_batch(self, result: Any) -> Any:
        """Return a RecordBatchReader; results from ``execute`` are already materialized."""
        if isinstance(result, _MaterializedResult):
            return result.fetch_record_batch()
        return self._adapter.fetch_record_batch(result)

    def fetchone(self, result: Any) -> tuple | None:
        if isinstance(result, _MaterializedResult):
            return result.fetchone()
        return self._adapter.fetchone(result)

    def close(self) -> None:
        """No-op: the shared connection is owned by the adapter, not the cursor."""
        return None


class BaseDatabaseAdapter(ABC):
    """Abstract base class for database adapters.

    Adapters provide a unified interface for different database backends,
    allowing Sidemantic to work with DuckDB, PostgreSQL, and other databases.
    """

    def cursor(self) -> Any:
        """Return a per-call handle for executing a query.

        The returned object exposes ``execute(sql)`` returning a result object
        compatible with :meth:`fetch_record_batch` / :meth:`fetchone`.

        The default implementation returns a wrapper that serializes execution
        on the shared connection behind a per-adapter lock, matching the
        historical single-connection behavior. Adapters whose driver supports
        independent concurrent handles (e.g. DuckDB via ``conn.cursor()``)
        override this to return a truly independent cursor so concurrent reads
        do not serialize.
        """
        lock = getattr(self, "_shared_connection_lock", None)
        if lock is None:
            # Double-checked under a global init guard so concurrent first calls
            # settle on a single lock instance for this adapter.
            with _SHARED_LOCK_INIT_GUARD:
                lock = getattr(self, "_shared_connection_lock", None)
                if lock is None:
                    lock = threading.Lock()
                    # Store so all fallback cursors for this adapter share one lock.
                    self._shared_connection_lock = lock
        return _SerializedCursor(self, lock)

    def configure_statement_timeout(self, cursor: Any, timeout_seconds: float) -> str | None:
        """Apply a warehouse-side statement timeout when this dialect supports one.

        Returns ``None`` when applied, otherwise a diagnostic explaining that
        Sidemantic's local deadline/cancellation is the only available guard.
        """
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        execute_control = getattr(cursor, "execute_control", cursor.execute)
        if self.dialect == "postgres":
            execute_control(f"SET statement_timeout = {max(1, int(timeout_seconds * 1000))}")
            return None
        if self.dialect == "snowflake":
            import math

            execute_control(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {max(1, math.ceil(timeout_seconds))}")
            return None
        if self.dialect == "clickhouse":
            execute_control(f"SET max_execution_time = {timeout_seconds:g}")
            return None
        return (
            f"{self.dialect} adapter does not expose a portable warehouse statement timeout; "
            "the local execution deadline will request best-effort cancellation"
        )

    def cancel(self, handle: Any) -> CancellationOutcome:
        """Best-effort cancellation for an executing cursor/connection."""
        candidates = [handle, getattr(handle, "cursor", None)]
        try:
            candidates.append(self.raw_connection)
        except Exception:
            pass
        for candidate in candidates:
            if candidate is None:
                continue
            for method_name in ("cancel_safe", "cancel", "interrupt", "adbc_cancel"):
                method = getattr(candidate, method_name, None)
                if not callable(method):
                    continue
                try:
                    method()
                except Exception as exc:
                    return CancellationOutcome(
                        supported=True,
                        cancelled=False,
                        diagnostic=f"{self.dialect} cancellation request failed: {type(exc).__name__}: {exc}",
                    )
                return CancellationOutcome(
                    supported=True,
                    cancelled=True,
                    diagnostic=f"{self.dialect} cancellation requested via {method_name}()",
                )
        return CancellationOutcome(
            supported=False,
            cancelled=False,
            diagnostic=f"{self.dialect} adapter cannot cancel an in-flight query; warehouse work may continue",
        )

    @abstractmethod
    def execute(self, sql: str) -> Any:
        """Execute SQL and return result object.

        Args:
            sql: SQL query to execute

        Returns:
            Database-specific result object
        """
        raise NotImplementedError

    @abstractmethod
    def executemany(self, sql: str, params: list) -> Any:
        """Execute SQL with multiple parameter sets.

        Args:
            sql: SQL query with placeholders
            params: List of parameter tuples

        Returns:
            Database-specific result object
        """
        raise NotImplementedError

    @abstractmethod
    def fetchone(self, result: Any) -> tuple | None:
        """Fetch one row from result.

        Args:
            result: Result object from execute()

        Returns:
            Single row tuple or None
        """
        raise NotImplementedError

    @abstractmethod
    def fetch_record_batch(self, result: Any) -> Any:
        """Fetch result as Arrow RecordBatch for server.

        Args:
            result: Result object from execute()

        Returns:
            PyArrow RecordBatchReader or similar
        """
        raise NotImplementedError

    @abstractmethod
    def get_tables(self) -> list[dict]:
        """Get list of tables in database.

        Returns:
            List of dicts with 'table_name' and 'schema' keys
        """
        raise NotImplementedError

    @abstractmethod
    def get_columns(self, table_name: str, schema: str | None = None) -> list[dict]:
        """Get columns for a table.

        Args:
            table_name: Name of table
            schema: Schema name (optional)

        Returns:
            List of dicts with 'column_name' and 'data_type' keys
        """
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        """Close database connection."""
        raise NotImplementedError

    @property
    @abstractmethod
    def dialect(self) -> str:
        """Get SQLGlot dialect name.

        Returns:
            Dialect name (e.g., 'duckdb', 'postgres')
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def raw_connection(self) -> Any:
        """Get underlying database connection object.

        Returns:
            Raw connection (DuckDBPyConnection, psycopg.Connection, etc.)
        """
        raise NotImplementedError
