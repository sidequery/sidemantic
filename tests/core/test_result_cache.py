"""Tests for the content-keyed Arrow result cache with singleflight dedup."""

# ruff: noqa: E402

from __future__ import annotations

import threading
import time

import pytest

pa = pytest.importorskip("pyarrow")

from sidemantic import Metric, Model, SemanticLayer
from sidemantic.core.result_cache import ResultCache, ResultCacheWaitCancelledError, build_result_key


class _Clock:
    """Manually advanceable monotonic clock for TTL tests (no real sleeps)."""

    def __init__(self, start: float = 0.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _table(nrows: int = 1) -> pa.Table:
    return pa.table({"a": list(range(nrows))})


def test_hit_and_miss_counts():
    cache = ResultCache(max_bytes=10 * 1024 * 1024)
    calls = {"n": 0}

    def compute():
        calls["n"] += 1
        return _table()

    first = cache.get_or_compute("k", compute)
    second = cache.get_or_compute("k", compute)

    assert first is second
    assert calls["n"] == 1
    stats = cache.stats()
    assert stats["hits"] == 1
    assert stats["misses"] == 1
    assert stats["entries"] == 1
    assert stats["bytes"] == first.nbytes


def test_lru_eviction_by_bytes():
    one = _table(1)
    entry_bytes = one.nbytes
    # Budget for exactly two entries; the third insert must evict the LRU one.
    cache = ResultCache(max_bytes=entry_bytes * 2)

    cache.get_or_compute("a", lambda: _table(1))
    cache.get_or_compute("b", lambda: _table(1))
    # Touch "a" so "b" becomes least-recently-used.
    cache.get_or_compute("a", lambda: _table(1))
    cache.get_or_compute("c", lambda: _table(1))

    stats = cache.stats()
    assert stats["entries"] == 2
    assert stats["bytes"] <= entry_bytes * 2

    # "b" was evicted (LRU); recomputing it is a miss.
    misses_before = cache.stats()["misses"]
    recomputed = {"n": 0}

    def compute_b():
        recomputed["n"] += 1
        return _table(1)

    cache.get_or_compute("b", compute_b)
    assert recomputed["n"] == 1
    assert cache.stats()["misses"] == misses_before + 1


def test_ttl_expiry_via_injected_clock():
    clock = _Clock()
    cache = ResultCache(max_bytes=10 * 1024 * 1024, ttl_seconds=5.0, clock=clock)
    calls = {"n": 0}

    def compute():
        calls["n"] += 1
        return _table()

    cache.get_or_compute("k", compute)
    # Within TTL: hit.
    clock.advance(4.0)
    cache.get_or_compute("k", compute)
    assert calls["n"] == 1

    # Past TTL: expired -> miss -> recompute.
    clock.advance(2.0)
    cache.get_or_compute("k", compute)
    assert calls["n"] == 2


def test_invalidate_all():
    cache = ResultCache(max_bytes=10 * 1024 * 1024)
    calls = {"n": 0}

    def compute():
        calls["n"] += 1
        return _table()

    cache.get_or_compute("k", compute)
    cache.invalidate_all()
    assert cache.stats()["entries"] == 0
    assert cache.stats()["bytes"] == 0

    cache.get_or_compute("k", compute)
    assert calls["n"] == 2


def test_singleflight_runs_compute_once():
    cache = ResultCache(max_bytes=10 * 1024 * 1024)

    hold = threading.Event()
    entered = threading.Event()
    calls = {"n": 0}

    def compute():
        calls["n"] += 1
        entered.set()
        # Block so the second thread joins as a waiter before we finish.
        hold.wait(timeout=5.0)
        return _table()

    results: dict[str, pa.Table] = {}

    def worker(name: str):
        results[name] = cache.get_or_compute("k", compute)

    t1 = threading.Thread(target=worker, args=("t1",))
    t1.start()
    assert entered.wait(timeout=5.0)  # leader is inside compute()

    t2 = threading.Thread(target=worker, args=("t2",))
    t2.start()
    # Give t2 a moment to register as a waiter, then release the leader.
    hold.set()

    t1.join(timeout=5.0)
    t2.join(timeout=5.0)

    assert calls["n"] == 1
    assert results["t1"] is results["t2"]


def test_singleflight_follower_can_cancel_without_waiting_for_leader():
    cache = ResultCache(max_bytes=10 * 1024 * 1024)
    leader_entered = threading.Event()
    release_leader = threading.Event()
    cancel_waiter = threading.Event()

    def compute():
        leader_entered.set()
        assert release_leader.wait(timeout=5.0)
        return _table()

    leader = threading.Thread(target=lambda: cache.get_or_compute("k", compute))
    leader.start()
    assert leader_entered.wait(timeout=1.0)

    errors = []

    def wait_for_leader():
        try:
            cache.get_or_compute_with_status("k", compute, cancelled=cancel_waiter.is_set)
        except BaseException as exc:  # noqa: BLE001 - capture for assertion
            errors.append(exc)

    follower = threading.Thread(target=wait_for_leader)
    follower.start()
    deadline = time.monotonic() + 1.0
    while cache.stats()["misses"] < 2 and time.monotonic() < deadline:
        time.sleep(0.001)

    cancel_waiter.set()
    follower.join(timeout=1.0)

    assert not follower.is_alive()
    assert isinstance(errors[0], ResultCacheWaitCancelledError)
    assert leader.is_alive()

    release_leader.set()
    leader.join(timeout=1.0)


def test_singleflight_leader_does_not_cache_result_after_cancellation():
    cache = ResultCache(max_bytes=10 * 1024 * 1024)
    cancelled = threading.Event()

    def compute():
        cancelled.set()
        return _table()

    with pytest.raises(ResultCacheWaitCancelledError, match="computation was cancelled"):
        cache.get_or_compute_with_status("k", compute, cancelled=cancelled.is_set)

    assert cache.stats()["entries"] == 0
    table, cache_hit = cache.get_or_compute_with_status("k", _table)
    assert table.num_rows == 1
    assert cache_hit is False


def test_singleflight_compute_raises_propagates_without_deadlock():
    cache = ResultCache(max_bytes=10 * 1024 * 1024)

    hold = threading.Event()
    entered = threading.Event()
    calls = {"n": 0}

    class BoomError(RuntimeError):
        pass

    def compute():
        calls["n"] += 1
        entered.set()
        hold.wait(timeout=5.0)
        raise BoomError("compute failed")

    errors: dict[str, BaseException] = {}

    def worker(name: str):
        try:
            cache.get_or_compute("k", compute)
        except BaseException as exc:  # noqa: BLE001 - capture for assertion
            errors[name] = exc

    t1 = threading.Thread(target=worker, args=("t1",))
    t1.start()
    assert entered.wait(timeout=5.0)

    t2 = threading.Thread(target=worker, args=("t2",))
    t2.start()
    hold.set()

    t1.join(timeout=5.0)
    t2.join(timeout=5.0)

    # The in-flight failure propagates to all waiters of that generation.
    assert calls["n"] == 1
    assert isinstance(errors.get("t1"), BoomError)
    assert isinstance(errors.get("t2"), BoomError)

    # No deadlock and no poisoned state: a subsequent call recomputes cleanly.
    ok = cache.get_or_compute("k", lambda: _table())
    assert ok.num_rows == 1


def test_build_result_key_user_attributes_change_key():
    base = dict(
        compiled_sql="select 1",
        dialect="duckdb",
        connection_fingerprint="duckdb|mem",
        generation=0,
    )
    key_none = build_result_key(user_attributes=None, **base)
    key_a = build_result_key(user_attributes={"tenant": "a"}, **base)
    key_b = build_result_key(user_attributes={"tenant": "b"}, **base)

    assert key_none != key_a
    assert key_a != key_b

    # Order-independent for the same attributes.
    key_a2 = build_result_key(
        user_attributes={"tenant": "a", "role": "x"},
        **base,
    )
    key_a3 = build_result_key(
        user_attributes={"role": "x", "tenant": "a"},
        **base,
    )
    assert key_a2 == key_a3


def test_build_result_key_generation_changes_key():
    base = dict(
        compiled_sql="select 1",
        dialect="duckdb",
        connection_fingerprint="duckdb|mem",
        user_attributes=None,
    )
    assert build_result_key(generation=0, **base) != build_result_key(generation=1, **base)


def test_semantic_layer_generation_bump_changes_key():
    layer = SemanticLayer(auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            metrics=[Metric(name="order_count", agg="count")],
        )
    )
    sql = "SELECT 1"
    key_before = layer.build_result_key(sql)

    # Adding a metric bumps the generation counter, invalidating cached results.
    layer.add_metric(Metric(name="global_count", agg="count", sql="orders.id"))
    key_after = layer.build_result_key(sql)

    assert key_before != key_after


def test_semantic_layer_user_attributes_change_key():
    layer = SemanticLayer(auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            metrics=[Metric(name="order_count", agg="count")],
        )
    )
    sql = "SELECT 1"
    key_user_a = layer.build_result_key(sql, user_attributes={"tenant": "a"})
    key_user_b = layer.build_result_key(sql, user_attributes={"tenant": "b"})
    assert key_user_a != key_user_b
