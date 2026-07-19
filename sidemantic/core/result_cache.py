"""Content-keyed Arrow result cache with singleflight deduplication.

This module intentionally imports pyarrow lazily (only inside methods that need
``table.nbytes``) so it stays importable in Pyodide/WASM environments where
pyarrow is not installed. The public surface never requires pyarrow at import
time; callers that actually cache Arrow tables must have pyarrow available.
"""

from __future__ import annotations

import hashlib
import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa


def build_result_key(
    *,
    compiled_sql: str,
    dialect: str,
    connection_fingerprint: str,
    generation: int,
    user_attributes: dict | None,
) -> str:
    """Build a stable content-addressed cache key.

    The key hashes, in order:
    - the compiled SQL (already routed through pre-aggregation selection),
    - the adapter dialect,
    - a connection fingerprint (so results from different databases never collide),
    - the layer generation counter (bumped on any model/metric/config mutation),
    - the user's security attributes (sorted), so security-scoped results never
      collide across users. ``user_attributes`` may be None today; A2 will
      populate it.

    Returns a hex sha256 digest.
    """
    hasher = hashlib.sha256()

    def _feed(part: str) -> None:
        # Length-prefix each field so concatenation is unambiguous and two
        # different field splits can never produce the same digest.
        encoded = part.encode("utf-8")
        hasher.update(len(encoded).to_bytes(8, "big"))
        hasher.update(encoded)

    _feed(compiled_sql)
    _feed(dialect)
    _feed(connection_fingerprint)
    _feed(str(generation))

    if user_attributes:
        # Sort by key for a stable, order-independent representation.
        for attr_key in sorted(user_attributes.keys()):
            _feed(str(attr_key))
            _feed(repr(user_attributes[attr_key]))
    else:
        _feed("<no-user-attributes>")

    return hasher.hexdigest()


class _CacheEntry:
    """A cached Arrow table plus its insertion time and byte size."""

    __slots__ = ("table", "nbytes", "created_at")

    def __init__(self, table: pa.Table, nbytes: int, created_at: float):
        self.table = table
        self.nbytes = nbytes
        self.created_at = created_at


class _InFlight:
    """Coordination record for a single in-flight computation generation."""

    __slots__ = ("event", "result", "error")

    def __init__(self):
        self.event = threading.Event()
        self.result: pa.Table | None = None
        self.error: BaseException | None = None


class ResultCache:
    """LRU-by-bytes Arrow result cache with TTL and singleflight dedup.

    - Entries are evicted in least-recently-used order once the total cached
      bytes (sum of ``table.nbytes``) exceeds ``max_bytes``.
    - Entries older than ``ttl_seconds`` (per the injectable ``clock``) are
      treated as misses and purged.
    - Concurrent ``get_or_compute`` calls for the same key run ``compute``
      exactly once; the rest wait for and share the result. If the in-flight
      ``compute`` raises, that failure propagates to every waiter of the same
      generation (they do not retry within that generation).
    """

    def __init__(
        self,
        max_bytes: int,
        ttl_seconds: float | None = None,
        clock: Callable[[], float] | None = None,
    ):
        self._max_bytes = max_bytes
        self._ttl_seconds = ttl_seconds
        self._clock = clock if clock is not None else time.monotonic

        # Global lock guards the LRU store, byte accounting, stats, and the
        # in-flight map. It is only ever held for O(1) bookkeeping, never across
        # a compute() call.
        self._lock = threading.Lock()
        self._store: OrderedDict[str, _CacheEntry] = OrderedDict()
        self._total_bytes = 0
        self._inflight: dict[str, _InFlight] = {}

        self._hits = 0
        self._misses = 0

    def get_or_compute(self, key: str, compute: Callable[[], pa.Table]) -> pa.Table:
        """Return the cached table for ``key`` or compute (once) and cache it."""
        table, _cache_hit = self.get_or_compute_with_status(key, compute)
        return table

    def get_or_compute_with_status(self, key: str, compute: Callable[[], pa.Table]) -> tuple[pa.Table, bool]:
        """Return ``(table, cache_hit)`` while preserving singleflight behavior."""
        with self._lock:
            entry = self._get_live_entry_locked(key)
            if entry is not None:
                self._hits += 1
                return entry.table, True

            # Miss. Either join an in-flight compute or become the leader.
            inflight = self._inflight.get(key)
            if inflight is not None:
                self._misses += 1
                leader = False
            else:
                inflight = _InFlight()
                self._inflight[key] = inflight
                self._misses += 1
                leader = True

        if not leader:
            # Wait for the leader of this generation, then share its outcome.
            inflight.event.wait()
            if inflight.error is not None:
                raise inflight.error
            return inflight.result, False

        # Leader path: run compute outside the lock so other keys are unblocked.
        try:
            table = compute()
        except BaseException as exc:  # noqa: BLE001 - propagate to all waiters
            inflight.error = exc
            with self._lock:
                # Only remove if still the current in-flight record for this key.
                if self._inflight.get(key) is inflight:
                    del self._inflight[key]
            inflight.event.set()
            raise

        inflight.result = table
        with self._lock:
            self._insert_locked(key, table)
            if self._inflight.get(key) is inflight:
                del self._inflight[key]
        inflight.event.set()
        return table, False

    def invalidate_all(self) -> None:
        """Drop every cached entry (does not disturb in-flight computations)."""
        with self._lock:
            self._store.clear()
            self._total_bytes = 0

    def stats(self) -> dict:
        """Return counters: hits, misses, entries, bytes."""
        with self._lock:
            return {
                "hits": self._hits,
                "misses": self._misses,
                "entries": len(self._store),
                "bytes": self._total_bytes,
            }

    def _get_live_entry_locked(self, key: str) -> _CacheEntry | None:
        """Return a non-expired entry, refreshing LRU order; purge if expired."""
        entry = self._store.get(key)
        if entry is None:
            return None
        if self._is_expired_locked(entry):
            del self._store[key]
            self._total_bytes -= entry.nbytes
            return None
        # Mark as most-recently-used.
        self._store.move_to_end(key)
        return entry

    def _is_expired_locked(self, entry: _CacheEntry) -> bool:
        if self._ttl_seconds is None:
            return False
        return (self._clock() - entry.created_at) >= self._ttl_seconds

    def _insert_locked(self, key: str, table: pa.Table) -> None:
        nbytes = int(table.nbytes)

        # Replace any existing entry (e.g. re-inserted after expiry).
        existing = self._store.pop(key, None)
        if existing is not None:
            self._total_bytes -= existing.nbytes

        entry = _CacheEntry(table=table, nbytes=nbytes, created_at=self._clock())
        self._store[key] = entry
        self._total_bytes += nbytes

        self._evict_locked()

    def _evict_locked(self) -> None:
        # Evict least-recently-used entries until within budget. A single entry
        # larger than max_bytes is kept (evicting it would leave nothing served
        # for the compute we just paid for); everything else is dropped.
        while self._total_bytes > self._max_bytes and len(self._store) > 1:
            _evicted_key, evicted = self._store.popitem(last=False)
            self._total_bytes -= evicted.nbytes
