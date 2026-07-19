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


def test_handle_auth_partial_config_fails_closed():
    pytest.importorskip("riffq")
    pytest.importorskip("pyarrow")
    from sidemantic.server.connection import SemanticLayerConnection

    layer = SemanticLayer(connection="duckdb:///:memory:")
    conn = SemanticLayerConnection(connection_id=1, executor=None, layer=layer, username="user", password=None)

    auth_results = []

    def callback(result):
        auth_results.append(result)

    conn.handle_auth("user", "anything", "localhost", callback=callback)

    assert auth_results == [False]


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

    cursor = layer.adapter.cursor()

    assert (
        conn._try_handle_system_query(
            "SELECT * FROM information_schema.tables",
            "select * from information_schema.tables",
            lambda *_: None,
            cursor,
        )
        is True
    )

    table = captured["reader"].read_all()
    rows = table.to_pylist()
    assert any(r["table_schema"] == "semantic_layer" for r in rows)

    captured.clear()
    assert (
        conn._try_handle_system_query(
            "SELECT * FROM pg_catalog.pg_namespace",
            "select * from pg_catalog.pg_namespace",
            lambda *_: None,
            cursor,
        )
        is True
    )
    assert isinstance(captured["reader"], pa.RecordBatchReader)


def test_secure_information_schema_columns_uses_visible_semantic_catalog():
    pytest.importorskip("riffq")
    pytest.importorskip("pyarrow")
    from sidemantic.server.connection import SemanticLayerConnection

    layer = SemanticLayer(connection="duckdb:///:memory:", enforce_visibility=True)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            dimensions=[
                Dimension(name="status", sql="status", type="categorical"),
                Dimension(name="secret_note", sql="secret_note", type="categorical", public=False),
            ],
            metrics=[Metric(name="order_count", agg="count")],
        )
    )
    captured = {}

    def send_reader(reader, callback):
        captured["reader"] = reader
        callback(True)

    conn = SemanticLayerConnection(connection_id=1, executor=None, layer=layer)
    conn.send_reader = send_reader
    cursor = layer.adapter.cursor()
    sql = (
        "SELECT table_schema, table_name, column_name, data_type "
        "FROM information_schema.columns "
        "WHERE table_schema = 'semantic_layer' AND table_name = 'orders' "
        "ORDER BY ordinal_position"
    )

    assert conn._try_handle_system_query(sql, sql.lower(), lambda *_: None, cursor) is True
    rows = captured["reader"].read_all().to_pylist()
    assert [row["column_name"] for row in rows] == ["status", "order_count"]
    assert all(row["table_schema"] == "semantic_layer" for row in rows)

    captured.clear()
    conn._handle_query(sql + ";", lambda *_: None)
    assert [row["column_name"] for row in captured["reader"].read_all().to_pylist()] == ["status", "order_count"]

    mixed = "SELECT * FROM information_schema.columns JOIN orders ON true"
    assert conn._try_handle_system_query(mixed, mixed.lower(), lambda *_: None, cursor) is False


def test_secure_information_schema_tables_preserves_postgres_column_names():
    pytest.importorskip("riffq")
    pytest.importorskip("pyarrow")
    from sidemantic.server.connection import SemanticLayerConnection

    layer = SemanticLayer(connection="duckdb:///:memory:", enforce_visibility=True)
    layer.add_model(Model(name="orders", table="orders", dimensions=[Dimension(name="id", type="numeric")]))
    captured = {}

    def send_reader(reader, callback):
        captured["reader"] = reader
        callback(True)

    conn = SemanticLayerConnection(connection_id=1, executor=None, layer=layer)
    conn.send_reader = send_reader
    cursor = layer.adapter.cursor()
    sql = "SELECT * FROM information_schema.tables"

    assert conn._try_handle_system_query(sql, sql.lower(), lambda *_: None, cursor) is True
    table = captured["reader"].read_all()
    assert table.column_names == ["table_schema", "table_name", "table_type"]
    assert table.to_pylist() == [{"table_schema": "semantic_layer", "table_name": "orders", "table_type": "BASE TABLE"}]


def test_secure_pg_class_uses_semantic_table_catalog():
    pytest.importorskip("riffq")
    pytest.importorskip("pyarrow")
    from sidemantic.server.connection import SemanticLayerConnection

    layer = SemanticLayer(connection="duckdb:///:memory:", enforce_visibility=True)
    layer.adapter.execute("CREATE TABLE source_table (id INTEGER)")
    layer.adapter.execute("CREATE TABLE audit_log (message VARCHAR)")
    layer.add_model(Model(name="orders", table="source_table", dimensions=[Dimension(name="id", type="numeric")]))
    captured = {}

    def send_reader(reader, callback):
        captured["reader"] = reader
        callback(True)

    conn = SemanticLayerConnection(connection_id=1, executor=None, layer=layer)
    conn.send_reader = send_reader
    cursor = layer.adapter.cursor()
    sql = "SELECT relname, relnamespace FROM pg_catalog.pg_class"

    assert conn._try_handle_system_query(sql, sql.lower(), lambda *_: None, cursor) is True
    assert captured["reader"].read_all().to_pylist() == [{"relname": "orders", "relnamespace": "semantic_layer"}]


def test_obj_description_does_not_short_circuit_mixed_user_sql():
    pytest.importorskip("riffq")
    pytest.importorskip("pyarrow")
    from sidemantic.core.semantic_layer import SecurityError
    from sidemantic.server.connection import SemanticLayerConnection

    layer = SemanticLayer(connection="duckdb:///:memory:", enforce_visibility=True)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            dimensions=[Dimension(name="secret_note", type="categorical", public=False)],
        )
    )
    conn = SemanticLayerConnection(connection_id=1, executor=None, layer=layer)
    cursor = layer.adapter.cursor()
    mixed = "SELECT secret_note, obj_description(oid, 'pg_namespace') FROM orders"

    assert conn._try_handle_system_query(mixed, mixed.lower(), lambda *_: None, cursor) is False
    with pytest.raises(SecurityError, match="not public"):
        conn._rewrite_query(mixed, None)


def test_session_set_is_acknowledged_but_dml_is_denied():
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
    with pytest.raises(ValueError, match="read-only"):
        conn._handle_query("DELETE FROM orders", lambda *_: None)
    with pytest.raises(ValueError, match="read-only"):
        conn._handle_query("CREATE TABLE bypass (secret varchar)", lambda *_: None)
    with pytest.raises(ValueError, match="exactly one"):
        conn._handle_query("SELECT 1; DELETE FROM orders", lambda *_: None)


def test_query_error_raises_exception():
    """_handle_query should raise on errors, not return error as data rows."""
    pytest.importorskip("riffq")
    pytest.importorskip("pyarrow")
    from unittest.mock import MagicMock

    from sidemantic.core.semantic_graph import SemanticGraph
    from sidemantic.server.connection import SemanticLayerConnection

    mock_layer = MagicMock()
    # Query work now runs on a per-request cursor obtained from the adapter.
    mock_layer.adapter.cursor.return_value.execute.side_effect = Exception("test error")
    mock_layer.graph = SemanticGraph()
    mock_layer.dialect = "duckdb"

    conn = SemanticLayerConnection.__new__(SemanticLayerConnection)
    conn.layer = mock_layer

    callback = MagicMock()

    with pytest.raises(Exception, match="test error"):
        conn._handle_query("SELECT invalid_col FROM nonexistent", callback)

    callback.assert_not_called()


def test_user_attributes_lookup_by_session_user():
    """The connecting username is mapped to its security user attributes."""
    pytest.importorskip("riffq")
    pytest.importorskip("pyarrow")
    from sidemantic.server.connection import SemanticLayerConnection

    layer = SemanticLayer(connection="duckdb:///:memory:")
    attrs_map = {"alice": {"tenant_id": 1}, "bob": {"tenant_id": 2}}
    conn = SemanticLayerConnection(connection_id=1, executor=None, layer=layer, user_attrs_map=attrs_map)

    # Before auth, no session user -> None.
    assert conn._user_attributes() is None

    conn.handle_auth("alice", "", "localhost", callback=lambda _r: None)
    assert conn._user_attributes() == {"tenant_id": 1}

    conn.handle_auth("bob", "", "localhost", callback=lambda _r: None)
    assert conn._user_attributes() == {"tenant_id": 2}

    # Unknown user with a configured map -> None (deny-by-default upstream).
    conn.handle_auth("mallory", "", "localhost", callback=lambda _r: None)
    assert conn._user_attributes() is None


def test_pg_rewrite_enforces_access_and_preserves_safe_queries():
    """The PostgreSQL rewrite applies access gates and deny-by-default."""
    pytest.importorskip("riffq")
    pytest.importorskip("pyarrow")

    from sidemantic import SecurityPolicy
    from sidemantic.core.semantic_layer import SecurityError
    from sidemantic.server.connection import SemanticLayerConnection

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="status", sql="status", type="categorical")],
            metrics=[Metric(name="order_count", agg="count")],
            security=SecurityPolicy(access="{{ user.role == 'admin' }}"),
        )
    )
    conn = SemanticLayerConnection(
        connection_id=1, executor=None, layer=layer, user_attrs_map={"alice": {"role": "admin"}}
    )

    # No attributes for a secured model -> denied.
    with pytest.raises(SecurityError):
        conn._rewrite_query("SELECT * FROM orders", None)

    # Non-admin -> access gate falsy -> denied.
    with pytest.raises(SecurityError):
        conn._rewrite_query("SELECT * FROM orders", {"role": "viewer"})

    # Admin -> semantically rewritten.
    rewritten = conn._rewrite_query("SELECT * FROM orders", {"role": "admin"})
    assert "-- sidemantic:" in rewritten

    # A query not touching the secured model is unaffected.
    assert conn._rewrite_query("SELECT 1", None) == "SELECT 1"
