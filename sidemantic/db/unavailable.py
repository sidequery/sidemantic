"""Database adapter used when a runtime dependency is unavailable."""

from __future__ import annotations

from typing import Any

from sidemantic.db.base import BaseDatabaseAdapter


class UnavailableDatabaseAdapter(BaseDatabaseAdapter):
    """Adapter placeholder for compile-only environments."""

    def __init__(self, *, dialect: str, package: str, install_hint: str):
        self._dialect = dialect
        self._package = package
        self._install_hint = install_hint

    def _raise_unavailable(self) -> None:
        raise ModuleNotFoundError(
            f"Database runtime '{self._package}' is not installed. {self._install_hint}",
            name=self._package,
        )

    def execute(self, sql: str) -> Any:
        """Raise because SQL execution needs the missing runtime."""
        self._raise_unavailable()

    def executemany(self, sql: str, params: list) -> Any:
        """Raise because SQL execution needs the missing runtime."""
        self._raise_unavailable()

    def fetchone(self, result: Any) -> tuple | None:
        """Raise because result fetching needs the missing runtime."""
        self._raise_unavailable()

    def fetch_record_batch(self, result: Any) -> Any:
        """Raise because Arrow fetching needs the missing runtime."""
        self._raise_unavailable()

    def get_tables(self) -> list[dict]:
        """Raise because schema introspection needs the missing runtime."""
        self._raise_unavailable()

    def get_columns(self, table_name: str, schema: str | None = None) -> list[dict]:
        """Raise because schema introspection needs the missing runtime."""
        self._raise_unavailable()

    def close(self) -> None:
        """No-op: there is no underlying connection to close."""

    @property
    def dialect(self) -> str:
        """Return the SQL dialect used for compile-only operations."""
        return self._dialect

    @property
    def raw_connection(self) -> Any:
        """Raise because direct connection access needs the missing runtime."""
        self._raise_unavailable()
