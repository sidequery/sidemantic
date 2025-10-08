"""Base database adapter interface."""

from abc import ABC, abstractmethod
from typing import Any


class BaseDatabaseAdapter(ABC):
    """Abstract base class for database adapters.

    Adapters provide a unified interface for different database backends,
    allowing Sidemantic to work with DuckDB, PostgreSQL, and other databases.
    """

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
