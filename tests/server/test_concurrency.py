"""Concurrency proof for the HTTP API server.

Verifies that read-only query handlers no longer serialize on a single lock /
connection: multiple queries fired concurrently are inside execute at the same
time because each request executes on its own ``adapter.cursor()`` (an
independent DuckDB handle over the same database).
"""

# ruff: noqa: E402

from __future__ import annotations

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
    adapter = client.app.state.layer.adapter

    # Each query calls a UDF that blocks inside DuckDB execution until every
    # concurrent query is executing too. Reads serialized anywhere (an app lock,
    # a lock-wrapped cursor, or one shared connection) can never all arrive, so
    # the barrier times out; unlike a wall-clock ratio, this ignores runner load.
    concurrency = 4
    barrier = threading.Barrier(concurrency, timeout=30)

    def rendezvous(value: int) -> int:
        barrier.wait()
        return value

    # Python UDFs registered on the connection are visible to its cursors.
    adapter.conn.create_function("rendezvous", rendezvous)

    def post_rendezvous() -> None:
        response = client.post("/raw", json={"query": "select rendezvous(1) as c"}, headers=_AUTH)
        assert response.status_code == 200, response.text
        assert response.json()["rows"][0]["c"] == 1

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(post_rendezvous) for _ in range(concurrency)]
        for future in futures:
            future.result()

    assert not barrier.broken, "reads did not execute concurrently"


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
