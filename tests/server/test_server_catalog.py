"""Tests for server catalog registration."""

from sidemantic import Dimension, Metric, Model, SemanticLayer
from tests.optional_dep_stubs import ensure_fake_riffq

ensure_fake_riffq()


def test_map_type():
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
    assert map_type("JSON") == "text"


def test_start_server_registers_tables(monkeypatch):
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


def test_start_server_registers_metrics_table_and_binds_connection(monkeypatch):
    from sidemantic.server.server import start_server

    calls = {"tables": [], "start": None, "bound": None}

    class FakeSemanticLayerConnection:
        def __init__(self, connection_id, executor, layer, username, password):
            calls["bound"] = {
                "connection_id": connection_id,
                "executor": executor,
                "layer": layer,
                "username": username,
                "password": password,
            }

    class FakeInnerServer:
        def register_database(self, name):
            self.database = name

        def register_schema(self, db, schema):
            pass

        def register_table(self, db, schema, table, columns):
            calls["tables"].append((db, schema, table, columns))

    class FakeServer:
        def __init__(self, address, connection_cls):
            self.address = address
            self._server = FakeInnerServer()
            connection_cls(9, "executor")

        def start(self, **kwargs):
            calls["start"] = kwargs

    monkeypatch.setattr("sidemantic.server.server.SemanticLayerConnection", FakeSemanticLayerConnection)
    monkeypatch.setattr("riffq.RiffqServer", FakeServer)

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("CREATE TABLE source_table (id INTEGER, created_at TIMESTAMP, is_active BOOLEAN)")
    layer.add_model(
        Model(
            name="orders",
            table="source_table",
            primary_key="id",
            dimensions=[
                Dimension(name="created_at", sql="created_at", type="time", granularity="day"),
                Dimension(name="is_active", sql="is_active", type="boolean"),
            ],
            metrics=[Metric(name="order_count", agg="count")],
        )
    )
    layer.add_metric(Metric(name="global_ratio", type="derived", sql="1"))

    start_server(layer, host="0.0.0.0", port=5546, username="api-user", password="api-pass")

    metrics_table = next(t for t in calls["tables"] if t[2] == "metrics")
    metric_columns = metrics_table[3]
    assert any("global_ratio" in column for column in metric_columns)
    assert any("created_at" in column and column["created_at"]["type"] == "timestamp" for column in metric_columns)
    assert any("is_active" in column and column["is_active"]["type"] == "boolean" for column in metric_columns)
    assert calls["bound"]["layer"] is layer
    assert calls["bound"]["username"] == "api-user"
    assert calls["bound"]["password"] == "api-pass"
    assert calls["start"] == {"catalog_emulation": False}
