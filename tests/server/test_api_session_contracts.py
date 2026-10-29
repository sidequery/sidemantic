"""Regression coverage for configured sessions and HTTP SQL contracts."""

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from sidemantic import Dimension, Metric, Model, SemanticLayer
from sidemantic.api_server import create_app
from sidemantic.server import common


@pytest.fixture
def configured_layer():
    layer = SemanticLayer(
        auto_register=False,
        init_sql=[
            "CREATE TABLE source AS SELECT 1 id UNION ALL SELECT 2",
            "CREATE SCHEMA sales",
            "CREATE TABLE sales.source AS SELECT 1 id",
            "SET schema=sales",
            "SET TimeZone='America/New_York'",
            "CREATE TEMP TABLE scratch AS SELECT 42 id",
            "CREATE TEMP VIEW scratch_view AS SELECT * FROM scratch",
        ],
    )
    layer.add_model(Model(name="orders", table="source", metrics=[Metric(name="count", agg="count")]))
    yield layer
    layer.adapter.close()


def test_http_and_mcp_preserve_configured_session(configured_layer, monkeypatch):
    from tests.optional_dep_stubs import ensure_fake_mcp

    ensure_fake_mcp()
    from sidemantic import mcp_server

    monkeypatch.setattr(mcp_server, "_layer", configured_layer)
    monkeypatch.setattr(mcp_server, "_user_attributes", None)
    assert configured_layer.query(metrics=["orders.count"]).fetchall() == [(1,)]
    with TestClient(create_app(configured_layer)) as client:
        for _ in range(2):
            response = client.post("/query", json={"metrics": ["orders.count"]})
            assert response.status_code == 200, response.text
            assert response.json()["rows"] == [{"count": 1}]
            response = client.post(
                "/raw", json={"query": "SELECT id, current_setting('TimeZone') AS tz FROM scratch_view"}
            )
            assert response.status_code == 200, response.text
            assert response.json()["rows"] == [{"id": 42, "tz": "America/New_York"}]
            assert mcp_server.run_query(metrics=["orders.count"])["rows"] == [{"count": 1}]
            assert mcp_server.run_sql("SELECT * FROM scratch_view")["rows"] == [{"id": 42}]
    # Persistent initialization is never replayed on worker connections.
    assert configured_layer.adapter.execute("SELECT count(*) FROM main.source").fetchone() == (2,)


def test_configured_sessions_still_execute_concurrently(configured_layer):
    barrier = threading.Barrier(2, timeout=10)

    def rendezvous(value: int) -> int:
        barrier.wait()
        return value

    configured_layer.adapter.conn.create_function("session_rendezvous", rendezvous)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(common.execute_bounded, configured_layer, "SELECT session_rendezvous(id) AS id FROM scratch")
            for _ in range(2)
        ]
        assert [future.result() for future in futures] == [[{"id": 42}], [{"id": 42}]]


def test_configured_session_deadline_cancels_and_recovers(configured_layer, monkeypatch):
    monkeypatch.setattr(common, "_default_limits", common.ServerLimits(timeout_seconds=0.01))
    with pytest.raises(ValueError, match="deadline"):
        common.execute_bounded(
            configured_layer, "SELECT sum(a.range * b.range) FROM range(1000000) a CROSS JOIN range(1000000) b"
        )
    monkeypatch.setattr(common, "_default_limits", common.ServerLimits())
    assert common.execute_bounded(configured_layer, "SELECT * FROM scratch") == [{"id": 42}]


@pytest.mark.parametrize("field", ["internal__secret", "created__day"])
@pytest.mark.parametrize("reference", ["orders.{field}", '"orders"."{field}"', "{field}"])
def test_http_rejects_exact_private_names_in_filters(field, reference):
    layer = SemanticLayer(auto_register=False, enforce_visibility=True)
    layer.add_model(
        Model(
            name="orders",
            sql="SELECT 'private' AS secret, DATE '2024-01-01' AS created",
            dimensions=[
                Dimension(name=field, type="categorical", sql="secret", public=False),
                Dimension(name="created", type="time", granularity="day"),
            ],
            metrics=[Metric(name="count", agg="count")],
        )
    )
    with TestClient(create_app(layer)) as client:
        for value in ("private", "other"):
            response = client.post(
                "/query",
                json={"metrics": ["orders.count"], "filters": [f"{reference.format(field=field)} = '{value}'"]},
            )
            assert response.status_code == 403, response.text
    layer.adapter.close()


@pytest.mark.parametrize("endpoint", ["/sql/compile", "/sql", "/raw"])
def test_http_sql_parsing_uses_backend_dialect(endpoint, monkeypatch):
    import pyarrow as pa

    import sidemantic.api_server as api

    layer = SemanticLayer(dialect="bigquery", auto_register=False)
    layer.add_model(
        Model(name="orders", table="dataset.orders", metrics=[Metric(name="revenue", agg="sum", sql="amount")])
    )
    executed = []

    def query_table(app, layer, query, **kwargs):
        executed.append(query)
        return pa.table({"revenue": [10]})

    monkeypatch.setattr(api, "_query_table", query_table)
    with TestClient(create_app(layer)) as client:
        query = "SELECT `orders`.`revenue` FROM `orders`"
        response = client.post(endpoint, json={"query": query})
        assert response.status_code == 200, response.text
        if endpoint == "/sql/compile":
            assert "SUM" in response.json()["sql"]
        else:
            assert executed
        assert client.post(endpoint, json={"query": query + "; SELECT 1"}).status_code == 400
        if endpoint == "/raw":
            assert client.post(endpoint, json={"query": "DELETE FROM `orders`"}).status_code == 400
    layer.adapter.close()


def test_session_initialization_is_covered_by_execution_deadline(configured_layer, monkeypatch):
    def slow_initialize(cursor):
        cursor.execute("SELECT sum(a.range * b.range) FROM range(1000000) a CROSS JOIN range(1000000) b")

    monkeypatch.setattr(configured_layer.adapter, "_initialize_cursor", slow_initialize)
    monkeypatch.setattr(common, "_default_limits", common.ServerLimits(timeout_seconds=0.01))
    with pytest.raises(ValueError, match="deadline"):
        common.execute_bounded(configured_layer, "SELECT 1")
