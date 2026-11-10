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
        "begin; select * from private_orders; commit",
        "set search_path to main; select * from private_orders",
        "discard all; select order_count from orders",
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
    # "main" is the catalog database's own empty default schema, not a physical one.
    assert {row["nspname"] for row in captured} == {"main", "semantic_layer"}
    captured.clear()
    conn._handle_query("select table_schema from information_schema.tables", lambda *_: None)
    assert {row["table_schema"] for row in captured} == {"semantic_layer"}


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


def _plain_pg_connection():
    """An unsecured connection over an in-memory database, capturing sent rows."""
    pytest.importorskip("riffq")
    pytest.importorskip("pyarrow")
    from sidemantic.server.connection import SemanticLayerConnection

    layer = SemanticLayer(connection="duckdb:///:memory:")
    layer.adapter.execute("create table orders (id integer, status varchar, amount decimal(10,2))")
    layer.adapter.execute("insert into orders values (1, 'completed', 10.50), (2, 'pending', 4.25)")
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[
                Dimension(name="status", type="categorical"),
                Dimension(name="internal_note", sql="status", type="categorical", public=False),
            ],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )
    conn = SemanticLayerConnection(1, None, layer)
    captured = []
    conn.send_reader = lambda reader, callback: captured.extend(reader.read_all().to_pylist())
    return conn, captured


@pytest.mark.parametrize(
    "statement",
    [
        "create table pwn (x integer)",
        "drop table orders",
        "alter table orders add column y integer",
        "insert into orders values (3, 'x', 1)",
        "update orders set status = 'x'",
        "delete from orders",
        "truncate orders",
        "copy orders to 'orders.csv'",
        "attach 'other.db' as other",
        "install httpfs",
        "pragma threads=1",
        "call pragma_version()",
        "with gone as (delete from orders returning *) select * from gone",
        "select * into copied from orders",
        "select 1; drop table orders",
        "explain analyze delete from orders",
    ],
)
def test_pg_rejects_statements_that_change_the_database(statement):
    from sidemantic.server.connection import ReadOnlyError

    conn, captured = _plain_pg_connection()
    with pytest.raises(ReadOnlyError):
        conn._handle_query(statement, lambda *_: None)
    assert captured == []
    assert conn.layer.adapter.execute("select count(*) from orders").fetchall() == [(2,)]


@pytest.mark.parametrize(
    "statement",
    [
        "select 1 as ok",
        "select 1 as ok union all select 2",
        "with a as (select 1 as ok) select * from a",
        "show tables",
    ],
)
def test_pg_runs_read_statements(statement):
    conn, captured = _plain_pg_connection()
    conn._handle_query(statement, lambda *_: None)
    assert captured


def test_pg_transaction_statements_are_acknowledged():
    conn, captured = _plain_pg_connection()
    tags = []
    for statement in ("BEGIN", "start transaction", "COMMIT", "ROLLBACK", "DISCARD ALL"):
        conn._handle_query(statement, lambda tag, is_tag=False: tags.append((tag, is_tag)))
    assert tags == [
        ("BEGIN", True),
        ("START TRANSACTION", True),
        ("COMMIT", True),
        ("ROLLBACK", True),
        ("DISCARD ALL", True),
    ]
    assert captured == []


@pytest.mark.parametrize(
    "batch",
    [
        "BEGIN; SELECT 1 AS ok; COMMIT",
        "start transaction; select 1 as ok",
        "SET search_path TO main; SELECT 1 AS ok",
        "SHOW server_version; SELECT 1 AS ok",
        "DISCARD ALL; SELECT 1 AS ok",
        "DEALLOCATE ALL; SELECT 1 AS ok",
        "RESET ALL; SELECT 1 AS ok",
        "SELECT 1 AS ok; SELECT 2 AS ok",
        "SELECT 1 AS ok; BEGIN",
        "select table_name from information_schema.tables; select 1 as ok",
    ],
)
def test_pg_batch_in_one_message_is_rejected_not_partially_acknowledged(batch):
    """A handler call returns one result, so no statement of a batch is answered or run."""
    from sidemantic.server.connection import UnsupportedStatementError

    conn, captured = _plain_pg_connection()
    responses = []
    with pytest.raises(UnsupportedStatementError, match="multiple statements"):
        conn._handle_query(batch, lambda *args, **kwargs: responses.append((args, kwargs)))
    assert captured == []
    assert responses == []


@pytest.mark.parametrize(
    "batch",
    [
        "BEGIN; DROP TABLE orders; COMMIT",
        "SET search_path TO main; DELETE FROM orders",
        "DISCARD ALL; CREATE TABLE pwn (x INTEGER)",
        "SELECT 1; COMMIT; UPDATE orders SET status = 'x'",
    ],
)
def test_pg_batch_hiding_a_write_is_rejected_as_a_write(batch):
    from sidemantic.server.connection import ReadOnlyError

    conn, captured = _plain_pg_connection()
    responses = []
    with pytest.raises(ReadOnlyError):
        conn._handle_query(batch, lambda *args, **kwargs: responses.append((args, kwargs)))
    assert captured == []
    assert responses == []
    assert conn.layer.adapter.execute("select count(*) from orders").fetchall() == [(2,)]
    assert conn.layer.adapter.execute("select count(*) from information_schema.tables").fetchall() == [(1,)]


def test_pg_single_statement_with_separator_text_is_not_a_batch():
    pytest.importorskip("riffq")
    from sidemantic.server.connection import split_statements

    assert split_statements("BEGIN; SELECT 1; COMMIT", "duckdb") == ["BEGIN", " SELECT 1", " COMMIT"]
    assert split_statements("BEGIN;", "duckdb") == ["BEGIN"]
    assert split_statements("SET x = 1; -- done", "duckdb") == ["SET x = 1"]
    for single in ("SELECT 'a;b' AS ok", "SELECT 1 /* ; */ AS ok", "SELECT $$;$$ AS ok", 'SELECT 1 AS "a;b"', ";;"):
        assert len(split_statements(single, "duckdb")) == 1, single

    conn, captured = _plain_pg_connection()
    tags = []
    conn._handle_query("BEGIN;", lambda tag, is_tag=False: tags.append(tag))
    conn._handle_query("SET search_path TO main; -- psql", lambda *_: None)
    conn._handle_query("SELECT 'a;b' AS ok;", lambda *_: None)
    assert tags == ["BEGIN"]
    assert captured == [{"ok": "a;b"}]


def test_pg_bound_parameters_reach_semantic_queries():
    from decimal import Decimal

    conn, captured = _plain_pg_connection()
    query = "select orders.status, orders.revenue from orders where orders.status = $1"
    conn._handle_query(query, lambda *_: None, query_args=["completed"])
    assert captured == [{"status": "completed", "revenue": Decimal("10.50")}]

    # A Describe arrives before values are bound and must still report the columns.
    described = []
    conn.send_reader = lambda reader, callback: described.append(reader.schema.names)
    conn._handle_query(query, lambda *_: None, do_describe=True)
    assert described == [["status", "revenue"]]


def test_pg_inlines_parameters_as_escaped_literals():
    import datetime

    pytest.importorskip("riffq")
    from sidemantic.server.connection import ParameterError, inline_parameters

    sql = "select $1, '$2', $3 -- $4\n, x-$5, $2 from t where d = $6"
    values = ["it's", None, True, "unused", -5, datetime.date(2026, 1, 2)]
    assert inline_parameters(sql, values, dialect="duckdb") == (
        "select 'it''s', '$2', TRUE -- $4\n, x-(-5), NULL from t where d = CAST('2026-01-02' AS DATE)"
    )
    assert inline_parameters("select $1", ["a\\'b"], dialect="bigquery") == "select 'a\\\\\\'b'"
    assert inline_parameters("select $1", None, dialect="duckdb", describe=True) == "select NULL"
    with pytest.raises(ParameterError):
        inline_parameters("select $1", [], dialect="duckdb")
    with pytest.raises(ParameterError):
        # riffq passes raw bytes through for binary values it cannot decode.
        inline_parameters("select $1", ["\x00\x00%8"], dialect="duckdb")


def test_pg_errors_map_to_sqlstates():
    pytest.importorskip("riffq")
    import duckdb

    from sidemantic.core.semantic_layer import SecurityError
    from sidemantic.server.connection import ReadOnlyError, error_response

    assert error_response(SecurityError("denied")) == ("ERROR", "42501", "denied")
    assert error_response(ReadOnlyError("read-only"))[1] == "25006"
    assert error_response(ValueError("Field 'orders.x' not found"))[1] == "42703"
    assert error_response(duckdb.ParserException("syntax error"))[1] == "42601"
    assert error_response(RuntimeError("boom")) == ("ERROR", "XX000", "boom")


def test_pg_query_errors_reach_the_client_as_errors():
    """Exceptions on the executor thread must be reported through the callback."""
    conn, captured = _plain_pg_connection()
    responses = []
    conn._run_query(
        "select orders.nope from orders", lambda result, is_error=False: responses.append((result, is_error))
    )
    assert captured == []
    assert len(responses) == 1
    (severity, sqlstate, message), is_error = responses[0]
    assert (severity, sqlstate, is_error) == ("ERROR", "42703", True)
    assert "orders.nope" in message


def test_pg_catalog_lists_model_columns_and_hides_non_public_fields():
    conn, captured = _plain_pg_connection()
    query = (
        "select column_name from information_schema.columns "
        "where table_schema = 'semantic_layer' and table_name = 'orders' order by ordinal_position"
    )
    conn._handle_query(query, lambda *_: None)
    assert [row["column_name"] for row in captured] == ["status", "internal_note", "revenue"]

    conn, captured = _plain_pg_connection()
    conn.layer.enforce_visibility = True
    conn._handle_query(query, lambda *_: None)
    assert [row["column_name"] for row in captured] == ["status", "revenue"]
    # Enforced visibility is a security control: physical tables are not listed either.
    captured.clear()
    conn._handle_query("select distinct table_schema from information_schema.tables", lambda *_: None)
    assert captured == [{"table_schema": "semantic_layer"}]


def test_pg_catalog_query_honors_requested_columns_and_parameters():
    conn, captured = _plain_pg_connection()
    conn._handle_query(
        "select n.nspname, c.relname from pg_catalog.pg_class c "
        "join pg_catalog.pg_namespace n on n.oid = c.relnamespace where n.nspname = $1 order by 2",
        lambda *_: None,
        query_args=["semantic_layer"],
    )
    assert captured == [{"nspname": "semantic_layer", "relname": "orders"}]
    captured.clear()
    conn._handle_query("select pg_catalog.version(), current_database()", lambda *_: None)
    assert captured[0]["current_database"] == "sidemantic"
    assert captured[0]["version"].startswith("PostgreSQL ")

    # Column types resolve through pg_type by PostgreSQL OID, as JDBC metadata queries expect.
    captured.clear()
    conn._handle_query(
        "select a.attname, t.typname from pg_catalog.pg_attribute a "
        "join pg_catalog.pg_class c on c.oid = a.attrelid "
        "join pg_catalog.pg_namespace n on n.oid = c.relnamespace "
        "join pg_catalog.pg_type t on t.oid = a.atttypid "
        "where n.nspname = 'semantic_layer' and c.relname = 'orders' order by a.attnum",
        lambda *_: None,
    )
    assert captured == [
        {"attname": "status", "typname": "varchar"},
        {"attname": "internal_note", "typname": "varchar"},
        {"attname": "revenue", "typname": "float8"},
    ]


def test_pg_catalog_sql_is_pointed_at_compatibility_objects():
    from sidemantic.server.pg_catalog import use_compatibility_objects

    assert (
        use_compatibility_objects("select pg_catalog.version(), 'pg_catalog.x()' from pg_catalog.pg_class")
        == "select version(), 'pg_catalog.x()' from pg_catalog.pg_class"
    )
    assert use_compatibility_objects(
        "select pg_catalog.pg_attribute.attname from pg_catalog.pg_attribute where attname <> 'pg_attribute'"
    ) == ("select pg_attribute.attname from pg_attribute_compat() AS pg_attribute where attname <> 'pg_attribute'")
    assert (
        use_compatibility_objects("select a.attname from pg_attribute a join pg_class c on true")
        == "select a.attname from pg_attribute_compat() a join pg_class c on true"
    )


def test_pg_values_without_a_wire_encoding_are_sent_as_text():
    from decimal import Decimal

    conn, captured = _plain_pg_connection()
    conn._handle_query(
        "select cast('12:34:56' as time) as t, cast('x' as enum('x', 'y')) as e, {'a': 1} as s, "
        "cast(1.5 as decimal(10,2)) as d, cast(null as time) as n",
        lambda *_: None,
    )
    assert captured == [{"t": "12:34:56", "e": "x", "s": '{"a": 1}', "d": Decimal("1.50"), "n": None}]
