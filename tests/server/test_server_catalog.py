"""Tests for server catalog registration."""

import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer


def test_map_type():
    pytest.importorskip("riffq")
    from sidemantic.server.server import map_type

    assert map_type("INTEGER") == "integer"
    assert map_type("BIGINT") == "bigint"
    assert map_type("SMALLINT") == "smallint"
    assert map_type("VARCHAR") == "text"
    assert map_type("TEXT") == "text"
    assert map_type("DECIMAL") == "numeric"
    assert map_type("DOUBLE") == "double precision"
    assert map_type("DATE") == "date"
    assert map_type("TIMESTAMP") == "timestamp"
    assert map_type("BOOLEAN") == "boolean"


def test_start_server_registers_tables(monkeypatch):
    pytest.importorskip("riffq")

    from sidemantic.server.server import start_server

    calls = {"schemas": set(), "tables": []}

    class FakeInnerServer:
        def register_database(self, name):
            self.database = name

        def register_schema(self, db, schema):
            calls["schemas"].add((db, schema))

        def register_table(self, db, schema, table, columns):
            calls["tables"].append((db, schema, table, columns))

    class FakeServer:
        def __init__(self, *args, **kwargs):
            self._server = FakeInnerServer()

        def start(self, *args, **kwargs):
            self.started = True

    monkeypatch.setattr("riffq.RiffqServer", FakeServer)

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE source_table (id INTEGER, status VARCHAR)")
    layer.add_model(
        Model(
            name="orders",
            table="source_table",
            primary_key="id",
            dimensions=[Dimension(name="status", sql="status", type="categorical")],
            metrics=[Metric(name="order_count", agg="count")],
        )
    )

    start_server(layer, port=5444)

    assert ("sidemantic", "semantic_layer") in calls["schemas"]
    assert any(t[2] == "source_table" for t in calls["tables"])
    assert any(t[2] == "orders" and t[1] == "semantic_layer" for t in calls["tables"])
