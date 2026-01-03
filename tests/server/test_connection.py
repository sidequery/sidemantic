"""Tests for PostgreSQL wire protocol connection handling."""

import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer


def test_handle_auth():
    pytest.importorskip("riffq")
    pytest.importorskip("pyarrow")
    from sidemantic.server.connection import SemanticLayerConnection

    layer = SemanticLayer(connection="duckdb:///:memory:")

    conn = SemanticLayerConnection(connection_id=1, executor=None, layer=layer, username="user", password="pass")

    auth_results = []

    def callback(result):
        auth_results.append(result)

    conn.handle_auth("user", "pass", "localhost", callback=callback)
    conn.handle_auth("user", "wrong", "localhost", callback=callback)

    assert auth_results == [True, False]


def test_handle_system_queries():
    pytest.importorskip("riffq")
    pa = pytest.importorskip("pyarrow")
    from sidemantic.server.connection import SemanticLayerConnection

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="status", sql="status", type="categorical")],
            metrics=[Metric(name="order_count", agg="count")],
        )
    )

    layer.conn.execute("CREATE TABLE orders (id INTEGER, status VARCHAR)")

    captured = {}

    def send_reader(reader, callback):
        captured["reader"] = reader
        callback(True)

    conn = SemanticLayerConnection(connection_id=1, executor=None, layer=layer)
    conn.send_reader = send_reader

    assert (
        conn._try_handle_system_query(
            "SELECT * FROM information_schema.tables", "select * from information_schema.tables", lambda *_: None
        )
        is True
    )

    table = captured["reader"].read_all()
    rows = table.to_pylist()
    assert any(r["table_schema"] == "semantic_layer" for r in rows)

    captured.clear()
    assert (
        conn._try_handle_system_query(
            "SELECT * FROM pg_catalog.pg_namespace", "select * from pg_catalog.pg_namespace", lambda *_: None
        )
        is True
    )
    assert isinstance(captured["reader"], pa.RecordBatchReader)


def test_dml_passthrough():
    pytest.importorskip("riffq")
    pytest.importorskip("pyarrow")
    from sidemantic.server.connection import SemanticLayerConnection

    layer = SemanticLayer(connection="duckdb:///:memory:")

    captured = {}

    def send_reader(reader, callback):
        captured["reader"] = reader
        callback(True)

    conn = SemanticLayerConnection(connection_id=1, executor=None, layer=layer)
    conn.send_reader = send_reader

    conn._handle_query("SET search_path TO public", lambda *_: None)

    assert "reader" in captured
