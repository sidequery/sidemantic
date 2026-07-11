"""Concurrency proof for the HTTP API server.

Verifies that read-only query handlers no longer serialize on a single lock /
connection: multiple slow queries fired concurrently complete in far less than
the sum of their individual runtimes because each request executes on its own
``adapter.cursor()`` (an independent DuckDB handle over the same database).
"""

# ruff: noqa: E402

from __future__ import annotations

import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("pyarrow")
pytest.importorskip("httpx")

import duckdb
from fastapi.testclient import TestClient

from sidemantic import SemanticLayer, load_from_directory
from sidemantic.api_server import create_app

# A single-threaded DuckDB scan over this range takes ~150-250ms, which is slow
# enough to expose serialization but small enough to stay well under any test
# timeout. threads=1 keeps each query single-threaded so 4 concurrent queries
# only win when they run on independent handles (not one serialized connection).
_SLOW_RANGE = 20_000_000
_SLOW_SQL = f"select count(*) as c, sum(x % 7) as s from range({_SLOW_RANGE}) t(x)"

_AUTH = {"Authorization": "Bearer secret"}


def _write_models(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "models.yml").write_text(
        """
models:
  - name: orders
    table: orders
    primary_key: id
    dimensions:
      - name: status
        sql: status
        type: categorical
    metrics:
      - name: order_count
        agg: count
"""
    )


def _build_client(tmp_path: Path) -> TestClient:
    models_dir = tmp_path / "models"
    _write_models(models_dir)

    db_path = tmp_path / "warehouse.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("create table orders (id integer, status varchar)")
    conn.executemany(
        "insert into orders values (?, ?)",
        [(1, "completed"), (2, "completed"), (3, "pending")],
    )
    conn.close()

    # threads=1: keep each query single-threaded so the wall-time difference
    # between serialized and concurrent execution is unambiguous.
    layer = SemanticLayer(connection=f"duckdb:///{db_path}?threads=1", auto_register=False)
    load_from_directory(layer, str(models_dir))
    app = create_app(layer, auth_token="secret")
    return TestClient(app)


def _post_slow_sql(client: TestClient) -> float:
    start = time.perf_counter()
    response = client.post("/raw", json={"query": _SLOW_SQL}, headers=_AUTH)
    elapsed = time.perf_counter() - start
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["rows"][0]["c"] == _SLOW_RANGE
    return elapsed


def test_concurrent_reads_do_not_serialize(tmp_path):
    client = _build_client(tmp_path)

    # Warm up (first query pays DuckDB init cost) and measure single-request time.
    _post_slow_sql(client)
    single_times = [_post_slow_sql(client) for _ in range(2)]
    single = statistics.median(single_times)

    concurrency = 4
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(_post_slow_sql, client) for _ in range(concurrency)]
        per_request = [f.result() for f in futures]
    concurrent_total = time.perf_counter() - start

    print(
        f"\nconcurrency proof: single={single * 1000:.0f}ms "
        f"concurrent_total({concurrency})={concurrent_total * 1000:.0f}ms "
        f"per_request={[f'{t * 1000:.0f}ms' for t in per_request]}"
    )

    # If reads serialized, concurrent_total would approach concurrency * single.
    # With per-request cursors it should stay well under 2.5x single. The bound
    # is generous to absorb CI runners with fewer cores than requests.
    assert concurrent_total < 2.5 * single, (
        f"concurrent total {concurrent_total * 1000:.0f}ms not < 2.5x single "
        f"{single * 1000:.0f}ms -- reads appear serialized"
    )


def test_mutation_style_endpoint_works_while_queries_run(tmp_path):
    """A metadata/read endpoint stays responsive while slow queries are in flight.

    /models reads the in-memory graph (representative of the non-query control
    plane) and must not be blocked behind long-running query execution.
    """
    client = _build_client(tmp_path)
    _post_slow_sql(client)  # warm up

    stop = threading.Event()
    errors: list[str] = []

    def hammer_queries() -> None:
        while not stop.is_set():
            try:
                resp = client.post("/raw", json={"query": _SLOW_SQL}, headers=_AUTH)
                if resp.status_code != 200:
                    errors.append(resp.text)
            except Exception as exc:  # pragma: no cover - defensive
                errors.append(str(exc))

    workers = [threading.Thread(target=hammer_queries) for _ in range(3)]
    for w in workers:
        w.start()

    try:
        # While queries are hammering, the control-plane read must succeed
        # promptly rather than queueing behind query execution.
        start = time.perf_counter()
        resp = client.get("/models", headers=_AUTH)
        elapsed = time.perf_counter() - start
        assert resp.status_code == 200, resp.text
        assert resp.json()[0]["name"] == "orders"
        # The metadata read takes no query lock, so it returns promptly rather
        # than queueing behind the in-flight slow queries. Bound is generous to
        # tolerate CI scheduling jitter.
        assert elapsed < 2.0, f"control-plane read blocked {elapsed * 1000:.0f}ms behind queries"
    finally:
        stop.set()
        for w in workers:
            w.join()

    assert not errors, f"query errors during concurrency: {errors[:3]}"
