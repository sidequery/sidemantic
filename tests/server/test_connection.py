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
    mock_layer.enforce_visibility = False
    mock_layer.use_preaggregations = False

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


def test_enforce_pg_access_denies_secured_model_without_attrs():
    """A secured model touched with no user attributes is denied."""
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
        conn._enforce_pg_access("SELECT * FROM orders", None)

    # Non-admin -> access gate falsy -> denied.
    with pytest.raises(SecurityError):
        conn._enforce_pg_access("SELECT * FROM orders", {"role": "viewer"})

    # Admin -> allowed (no raise).
    conn._enforce_pg_access("SELECT * FROM orders", {"role": "admin"})

    # A query not touching the secured model is unaffected.
    conn._enforce_pg_access("SELECT 1", None)


@pytest.fixture
def secured_pg_connection():
    pytest.importorskip("riffq")
    pytest.importorskip("pyarrow")
    from sidemantic import SecurityPolicy
    from sidemantic.server.connection import SemanticLayerConnection

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("create table private_orders (id integer, tenant integer)")
    layer.adapter.execute("insert into private_orders values (1, 1), (2, 2)")
    layer.add_model(
        Model(
            name="orders",
            sql="select * from private_orders",
            primary_key="id",
            dimensions=[Dimension(name="tenant", type="numeric")],
            metrics=[Metric(name="order_count", agg="count")],
            security=SecurityPolicy(access="{{ user.role == 'admin' }}", row_filters=["tenant = {{ user.tenant }}"]),
        )
    )
    layer.add_model(
        Model(name="public_model", table="public_source", dimensions=[Dimension(name="id", type="numeric")])
    )
    conn = SemanticLayerConnection(
        1, None, layer, user_attrs_map={"alice": {"role": "admin", "tenant": 1}, "bob": {"role": "viewer"}}
    )
    captured = []
    conn.send_reader = lambda reader, callback: captured.extend(reader.read_all().to_pylist())
    return conn, captured


@pytest.mark.parametrize(
    "query",
    [
        "select * from private_orders -- obj_description",
        "select * from private_orders where 'obj_description' = 'obj_description'",
        'select * from "main" . "private_orders"',
        "select obj_description(oid, 'pg_namespace') from private_orders",
        "with source as (select * from private_orders) select * from source",
        "select 1; select * from private_orders",
        "table orders",
        "show tables",
        "pragma show_tables",
        "table private_orders",
        "pragma table_info('private_orders')",
        "show all tables",
        "pragma database_list",
    ],
)
def test_pg_rejects_physical_source_bypasses(secured_pg_connection, query):
    from sidemantic.core.semantic_layer import SecurityError

    conn, captured = secured_pg_connection
    with pytest.raises(SecurityError):
        conn._handle_query(query, lambda *_: None)
    assert captured == []


@pytest.mark.parametrize("session", [None, "bob", "alice"])
@pytest.mark.parametrize(
    "catalog",
    ["information_schema.tables", '"information_schema"."tables"', "information_schema.tables;", "pg_catalog.pg_class"],
)
def test_pg_catalog_filters_session_models_and_physical_sources(secured_pg_connection, session, catalog):
    conn, captured = secured_pg_connection
    conn.session_user = session
    conn._handle_query(f"select * from {catalog}", lambda *_: None)
    names = {row.get("table_name", row.get("relname")) for row in captured}
    assert "public_model" in names
    assert "private_orders" not in names
    assert ("orders" in names) == (session == "alice")


def test_pg_namespace_omits_physical_schemas_under_controls(secured_pg_connection):
    conn, captured = secured_pg_connection
    conn.layer.adapter.execute("create schema private_tenant")
    conn._handle_query("select * from pg_catalog.pg_namespace", lambda *_: None)
    assert {row["nspname"] for row in captured} == {"semantic_layer"}


def test_pg_semantic_query_applies_row_filters(secured_pg_connection):
    conn, captured = secured_pg_connection
    conn.session_user = "alice"
    conn._handle_query("select orders.order_count from orders", lambda *_: None)
    assert captured == [{"order_count": 1}]


def test_pg_description_probe_is_compatible(secured_pg_connection):
    conn, captured = secured_pg_connection
    conn._handle_query("select obj_description(1, 'pg_namespace')", lambda *_: None)
    assert captured == [{"obj_description": None}]


def test_empty_pg_catalog_is_valid():
    pytest.importorskip("riffq")
    pytest.importorskip("pyarrow")
    from sidemantic.server.connection import SemanticLayerConnection

    layer = SemanticLayer(connection="duckdb:///:memory:")
    conn = SemanticLayerConnection(1, None, layer)
    captured = []
    conn.send_reader = lambda reader, callback: captured.extend(reader.read_all().to_pylist())
    conn._handle_query("select * from information_schema.tables", lambda *_: None)
    assert captured == []


@pytest.mark.parametrize("source", ['"main"."orders"', "main.orders", "semantic_layer.orders"])
def test_pg_qualified_model_names_still_enforce_policy(secured_pg_connection, source):
    from sidemantic.core.semantic_layer import SecurityError

    conn, captured = secured_pg_connection
    conn.layer.adapter.execute("create table orders as select * from private_orders")
    with pytest.raises(SecurityError):
        conn._handle_query(f"select order_count from {source}", lambda *_: None)
    assert captured == []
    conn.session_user = "alice"
    conn._handle_query(f"select order_count from {source}", lambda *_: None)
    assert captured == [{"order_count": 1}]


def test_pg_session_setting_remains_safe_with_controls(secured_pg_connection):
    conn, captured = secured_pg_connection
    conn._handle_query("SET client_encoding TO 'UTF8'", lambda *_: None)
    assert captured == []
    conn._handle_query("select 1 as ok", lambda *_: None)
    assert captured == [{"ok": 1}]
