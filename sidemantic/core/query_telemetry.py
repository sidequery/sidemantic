"""Process-local query telemetry and bounded history hooks."""

from __future__ import annotations

import hashlib
import logging
import threading
from collections import deque
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class QueryEvent:
    """A sanitized, process-local record of one query execution."""

    query_id: str
    request_id: str | None
    duration_ms: float
    dialect: str
    row_count: int | None = None
    response_bytes: int | None = None
    cache_hit: bool = False
    used_preaggregation: bool = False
    cancelled: bool = False
    timed_out: bool = False
    error: str | None = None
    sql: str | None = None
    sql_fingerprint: str | None = None
    plan_metadata: dict[str, Any] = field(default_factory=dict)
    cancellation_diagnostic: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return asdict(self)


class QueryTelemetry:
    """Thread-safe bounded query history with failure-isolated listeners.

    This intentionally remains an in-process library primitive. It does not
    persist events, expose users, or implement a monitoring backend.
    """

    def __init__(self, history_size: int = 1000):
        if history_size < 0:
            raise ValueError("history_size must be >= 0")
        self._history: deque[QueryEvent] = deque(maxlen=history_size or None)
        self._history_enabled = history_size > 0
        self._listeners: list[Callable[[QueryEvent], None]] = []
        self._lock = threading.Lock()

    def add_listener(self, listener: Callable[[QueryEvent], None]) -> Callable[[], None]:
        """Register a listener and return a function that unregisters it."""
        with self._lock:
            self._listeners.append(listener)

        def remove() -> None:
            with self._lock:
                if listener in self._listeners:
                    self._listeners.remove(listener)

        return remove

    def record(self, event: QueryEvent) -> None:
        """Record an event and notify listeners without affecting query success."""
        with self._lock:
            if self._history_enabled:
                self._history.append(event)
            listeners = tuple(self._listeners)
        for listener in listeners:
            try:
                listener(event)
            except Exception:
                logging.getLogger(__name__).exception("Query telemetry listener failed")

    def history(self, limit: int | None = None) -> list[QueryEvent]:
        """Return newest-first history, optionally capped to ``limit`` events."""
        if limit is not None and limit < 0:
            raise ValueError("limit must be >= 0")
        with self._lock:
            events = list(reversed(self._history)) if self._history_enabled else []
        return events if limit is None else events[:limit]

    def clear(self) -> None:
        """Clear retained history without removing listeners."""
        with self._lock:
            self._history.clear()

    def resize(self, history_size: int) -> None:
        """Change bounded retention while preserving listeners and newest events."""
        if history_size < 0:
            raise ValueError("history_size must be >= 0")
        with self._lock:
            existing = list(self._history)
            self._history = deque(existing[-history_size:] if history_size else [], maxlen=history_size or None)
            self._history_enabled = history_size > 0


def sanitize_sql(sql: str, dialect: str | None = None) -> tuple[str | None, str]:
    """Return literal-free SQL plus a fingerprint of the original statement.

    Parsing failures deliberately return no SQL text: an unknown statement may
    contain credentials or other secrets that a regex cannot safely identify.
    """
    fingerprint = hashlib.sha256(sql.encode("utf-8", errors="replace")).hexdigest()
    try:
        import sqlglot
        from sqlglot import exp

        parsed = sqlglot.parse_one(sql, read=dialect)

        def redact(node):
            if isinstance(node, exp.Literal):
                return exp.Placeholder()
            return node

        sanitized = parsed.transform(redact).sql(dialect=dialect, comments=False)
    except Exception:
        return None, fingerprint
    return sanitized, fingerprint
