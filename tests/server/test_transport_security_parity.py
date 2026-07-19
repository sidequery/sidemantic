"""Security guarantees remain identical across every query transport."""

# ruff: noqa: E402

from __future__ import annotations

import json
import sys
import types
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
pytest.importorskip("pyarrow")

from fastapi.testclient import TestClient

from tests.optional_dep_stubs import ensure_fake_mcp

_mcp_modules_before = {name for name in sys.modules if name == "mcp" or name.startswith("mcp.")}
ensure_fake_mcp()

# The PostgreSQL connection's policy rewrite has no riffq runtime dependency,
# so keep it testable when the optional wire-server package is not installed.
_stubbed_riffq = False
try:
    import riffq  # noqa: F401
except ImportError:
    _stubbed_riffq = True
    riffq_stub = types.ModuleType("riffq")

    class _BaseConnection:
        def __init__(self, *_args, **_kwargs):
            pass

    riffq_stub.BaseConnection = _BaseConnection
    sys.modules["riffq"] = riffq_stub

from sidemantic import Dimension, Metric, Model, SecurityPolicy, SemanticLayer
from sidemantic.api_server import create_app
from sidemantic.core.semantic_layer import SecurityError
from sidemantic.mcp_server import get_models, get_semantic_graph, initialize_layer
from sidemantic.mcp_server import run_query as mcp_run_query
from sidemantic.mcp_server import run_sql as mcp_run_sql
from sidemantic.server.connection import SemanticLayerConnection

if _stubbed_riffq:
    sys.modules.pop("riffq", None)
for _module_name in list(sys.modules):
    if (_module_name == "mcp" or _module_name.startswith("mcp.")) and _module_name not in _mcp_modules_before:
        sys.modules.pop(_module_name, None)


def _model() -> Model:
    return Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[
            Dimension(name="tenant_id", sql="tenant_id", type="numeric"),
            Dimension(name="secret_note", sql="secret_note", type="categorical", public=False),
        ],
        metrics=[Metric(name="total_amount", agg="sum", sql="amount")],
        security=SecurityPolicy(
            access="user.role == 'analyst'",
            row_filters=["tenant_id = {{ user.tenant_id }}"],
        ),
    )


def _layer(*, enforce_visibility: bool = False, yardstick: bool = False) -> SemanticLayer:
    layer = SemanticLayer(enforce_visibility=enforce_visibility)
    layer.adapter.execute("create table orders (id integer, tenant_id integer, amount double, secret_note varchar)")
    layer.adapter.executemany(
        "insert into orders values (?, ?, ?, ?)",
        [(1, 1, 10.0, "a"), (2, 1, 20.0, "b"), (3, 2, 5.0, "c"), (4, 2, 7.0, "d")],
    )
    model = _model()
    if yardstick:
        model.metadata = {"yardstick": {}}
    layer.add_model(model)
    return layer


def _write_model(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    directory.joinpath("models.yml").write_text(
        """
models:
  - name: orders
    table: orders
    primary_key: id
    dimensions:
      - name: tenant_id
        sql: tenant_id
        type: numeric
      - name: secret_note
        sql: secret_note
        type: categorical
        public: false
    metrics:
      - name: total_amount
        agg: sum
        sql: amount
    security:
      access: "user.role == 'analyst'"
      row_filters:
        - "tenant_id = {{ user.tenant_id }}"
"""
    )


def _mcp_layer(
    directory: Path,
    attrs: dict,
    *,
    enforce_visibility: bool = False,
    yardstick: bool = False,
) -> SemanticLayer:
    _write_model(directory)
    layer = initialize_layer(
        str(directory),
        db_path=":memory:",
        user_attributes=attrs,
        enforce_visibility=enforce_visibility,
    )
    layer.adapter.execute("create table orders (id integer, tenant_id integer, amount double, secret_note varchar)")
    layer.adapter.executemany(
        "insert into orders values (?, ?, ?, ?)",
        [(1, 1, 10.0, "a"), (2, 1, 20.0, "b"), (3, 2, 5.0, "c"), (4, 2, 7.0, "d")],
    )
    if yardstick:
        layer.graph.models["orders"].metadata = {"yardstick": {}}
    return layer


def _headers(attrs: dict) -> dict[str, str]:
    return {
        "Authorization": "Bearer secret",
        "X-Sidemantic-User": json.dumps(attrs),
    }


def _pg_rewrite(layer: SemanticLayer, sql: str, attrs: dict | None) -> str:
    connection = SemanticLayerConnection.__new__(SemanticLayerConnection)
    connection.layer = layer
    return connection._rewrite_query(sql, attrs)


def test_allowed_identity_gets_identical_rows_across_transports(tmp_path):
    attrs = {"role": "analyst", "tenant_id": 2}
    http_layer = _layer()
    client = TestClient(create_app(http_layer, auth_token="secret"))

    structured = client.post(
        "/query",
        json={"dimensions": ["orders.tenant_id"], "metrics": ["orders.total_amount"]},
        headers=_headers(attrs),
    )
    semantic_sql = client.post(
        "/sql",
        json={"query": "SELECT tenant_id, total_amount FROM orders"},
        headers=_headers(attrs),
    )
    assert structured.status_code == semantic_sql.status_code == 200

    _mcp_layer(tmp_path / "mcp", attrs)
    mcp_structured = mcp_run_query(
        dimensions=["orders.tenant_id"],
        metrics=["orders.total_amount"],
    )
    mcp_sql = mcp_run_sql("SELECT tenant_id, total_amount FROM orders")

    pg_layer = _layer()
    pg_sql = _pg_rewrite(pg_layer, "SELECT tenant_id, total_amount FROM orders", attrs)
    pg_rows = pg_layer.adapter.execute(pg_sql).fetchall()

    expected = [{"tenant_id": 2, "total_amount": 12.0}]
    assert structured.json()["rows"] == expected
    assert semantic_sql.json()["rows"] == expected
    assert mcp_structured["rows"] == expected
    assert mcp_sql["rows"] == expected
    assert pg_rows == [(2, 12.0)]


def test_denied_identity_is_rejected_by_every_transport(tmp_path):
    attrs = {"role": "viewer", "tenant_id": 2}
    http_layer = _layer()
    client = TestClient(create_app(http_layer, auth_token="secret"))

    for path, payload in [
        ("/query", {"metrics": ["orders.total_amount"]}),
        ("/sql", {"query": "SELECT total_amount FROM orders"}),
    ]:
        response = client.post(path, json=payload, headers=_headers(attrs))
        assert response.status_code == 403, response.text

    _mcp_layer(tmp_path / "mcp-denied", attrs)
    with pytest.raises(SecurityError, match="denied"):
        mcp_run_query(metrics=["orders.total_amount"])
    with pytest.raises(SecurityError, match="denied"):
        mcp_run_sql("SELECT total_amount FROM orders")

    with pytest.raises(SecurityError, match="denied"):
        _pg_rewrite(_layer(), "SELECT total_amount FROM orders", attrs)


def test_mutating_sql_is_denied_across_sql_transports(tmp_path):
    attrs = {"role": "analyst", "tenant_id": 1}
    client = TestClient(create_app(_layer(), auth_token="secret"))

    response = client.post(
        "/sql",
        json={"query": "DELETE FROM orders"},
        headers=_headers(attrs),
    )
    assert response.status_code == 400

    _mcp_layer(tmp_path / "mcp-mutation", attrs)
    with pytest.raises(ValueError, match="Only SELECT"):
        mcp_run_sql("DELETE FROM orders")

    pg_connection = SemanticLayerConnection.__new__(SemanticLayerConnection)
    pg_connection.layer = _layer()
    pg_connection.send_reader = lambda *_args: None
    with pytest.raises(ValueError, match="read-only"):
        pg_connection._handle_query("DELETE FROM orders", lambda *_args: None)


def test_raw_and_unproven_sql_fail_closed_across_sql_transports(tmp_path):
    attrs = {"role": "analyst", "tenant_id": 1}
    http_layer = _layer()
    http_layer.adapter.execute("create table audit_log (message varchar)")
    client = TestClient(create_app(http_layer, auth_token="secret"))

    raw = client.post("/raw", json={"query": "SELECT * FROM orders"}, headers=_headers(attrs))
    passthrough = client.post(
        "/sql",
        json={"query": "SELECT message FROM audit_log"},
        headers=_headers(attrs),
    )
    assert raw.status_code == 403
    assert passthrough.status_code == 403

    mcp_layer = _mcp_layer(tmp_path / "mcp-raw", attrs)
    mcp_layer.adapter.execute("create table audit_log (message varchar)")
    with pytest.raises(SecurityError, match="non-semantic"):
        mcp_run_sql("SELECT message FROM audit_log")

    pg_layer = _layer()
    pg_layer.adapter.execute("create table audit_log (message varchar)")
    with pytest.raises(SecurityError, match="non-semantic"):
        _pg_rewrite(pg_layer, "SELECT message FROM audit_log", attrs)

    mixed = "WITH raw AS (SELECT message FROM audit_log) SELECT total_amount FROM orders"
    with pytest.raises(SecurityError, match="audit_log"):
        _pg_rewrite(pg_layer, mixed, attrs)


def test_out_of_scope_nested_cte_alias_fails_closed_across_sql_transports(tmp_path):
    attrs = {"role": "analyst", "tenant_id": 1}
    query = """
        WITH scoped AS (
            WITH audit_log AS (SELECT 'masked' AS secret_note)
            SELECT secret_note FROM audit_log
        ),
        leaked AS (SELECT secret_note FROM audit_log),
        sem AS (SELECT total_amount FROM orders)
        SELECT sem.total_amount FROM sem CROSS JOIN leaked
    """

    http_layer = _layer(enforce_visibility=True)
    http_layer.adapter.execute("CREATE TABLE audit_log (secret_note VARCHAR)")
    client = TestClient(create_app(http_layer, auth_token="secret"))
    response = client.post("/sql", json={"query": query}, headers=_headers(attrs))
    assert response.status_code == 403
    assert "audit_log" in response.json()["error"]

    mcp_layer = _mcp_layer(tmp_path / "mcp-nested-cte-scope", attrs, enforce_visibility=True)
    mcp_layer.adapter.execute("CREATE TABLE audit_log (secret_note VARCHAR)")
    with pytest.raises(SecurityError, match="audit_log"):
        mcp_run_sql(query)

    pg_layer = _layer(enforce_visibility=True)
    pg_layer.adapter.execute("CREATE TABLE audit_log (secret_note VARCHAR)")
    with pytest.raises(SecurityError, match="audit_log"):
        _pg_rewrite(pg_layer, query, attrs)


def test_normalized_passthrough_fails_closed_across_sql_transports(tmp_path):
    attrs = {"role": "analyst", "tenant_id": 1}
    query = "(select secret_note from orders order by secret_note) union (select secret_note from orders)"
    client = TestClient(create_app(_layer(enforce_visibility=True), auth_token="secret"))

    response = client.post("/sql", json={"query": query}, headers=_headers(attrs))
    assert response.status_code == 403
    assert "could not be proven" in response.json()["error"]

    _mcp_layer(tmp_path / "mcp-normalized-passthrough", attrs, enforce_visibility=True)
    with pytest.raises(SecurityError, match="could not be proven"):
        mcp_run_sql(query)

    with pytest.raises(SecurityError, match="could not be proven"):
        _pg_rewrite(_layer(enforce_visibility=True), query, attrs)


def test_predicate_subqueries_fail_closed_across_sql_transports(tmp_path):
    attrs = {"role": "analyst", "tenant_id": 1}
    query = "SELECT total_amount FROM orders WHERE EXISTS (SELECT 1 FROM orders WHERE secret_note = 'secret')"
    client = TestClient(create_app(_layer(enforce_visibility=True), auth_token="secret"))

    response = client.post("/sql", json={"query": query}, headers=_headers(attrs))
    assert response.status_code == 403
    assert "predicate subquery" in response.json()["error"]

    _mcp_layer(tmp_path / "mcp-predicate-subquery", attrs, enforce_visibility=True)
    with pytest.raises(SecurityError, match="predicate subquery"):
        mcp_run_sql(query)

    with pytest.raises(SecurityError, match="predicate subquery"):
        _pg_rewrite(_layer(enforce_visibility=True), query, attrs)


def test_projection_subqueries_fail_closed_across_sql_transports(tmp_path):
    attrs = {"role": "analyst", "tenant_id": 999}
    query = "SELECT (SELECT 1 FROM orders LIMIT 1) AS leaked, total_amount FROM orders"
    client = TestClient(create_app(_layer(), auth_token="secret"))

    response = client.post("/sql", json={"query": query}, headers=_headers(attrs))
    assert response.status_code == 403
    assert "projection subquery" in response.json()["error"]

    _mcp_layer(tmp_path / "mcp-projection-subquery", attrs)
    with pytest.raises(SecurityError, match="projection subquery"):
        mcp_run_sql(query)

    with pytest.raises(SecurityError, match="projection subquery"):
        _pg_rewrite(_layer(), query, attrs)


def test_order_by_subqueries_fail_closed_across_sql_transports(tmp_path):
    attrs = {"role": "analyst", "tenant_id": 999}
    query = "SELECT tenant_id, total_amount FROM orders ORDER BY (SELECT COUNT(*) FROM orders)"
    client = TestClient(create_app(_layer(), auth_token="secret"))

    response = client.post("/sql", json={"query": query}, headers=_headers(attrs))
    assert response.status_code == 403
    assert "expression subquery" in response.json()["error"]

    _mcp_layer(tmp_path / "mcp-order-subquery", attrs)
    with pytest.raises(SecurityError, match="expression subquery"):
        mcp_run_sql(query)

    with pytest.raises(SecurityError, match="expression subquery"):
        _pg_rewrite(_layer(), query, attrs)


def test_rust_rewriter_is_disabled_across_secured_sql_transports(tmp_path, monkeypatch):
    from sidemantic.sql.query_rewriter import QueryRewriter

    attrs = {"role": "analyst", "tenant_id": 2}
    query = "SELECT tenant_id, total_amount FROM orders"
    monkeypatch.setenv("SIDEMANTIC_RS_REWRITER", "1")

    def insecure_rust_rewrite(*_args, **_kwargs):
        return "SELECT tenant_id, SUM(amount) AS total_amount FROM orders GROUP BY tenant_id"

    monkeypatch.setattr(QueryRewriter, "_rewrite_with_rust", insecure_rust_rewrite)

    client = TestClient(create_app(_layer(), auth_token="secret"))
    response = client.post("/sql", json={"query": query}, headers=_headers(attrs))
    assert response.status_code == 200
    assert response.json()["rows"] == [{"tenant_id": 2, "total_amount": 12.0}]

    _mcp_layer(tmp_path / "mcp-rust-disabled", attrs)
    assert mcp_run_sql(query)["rows"] == [{"tenant_id": 2, "total_amount": 12.0}]

    pg_layer = _layer()
    rendered = _pg_rewrite(pg_layer, query, attrs)
    assert pg_layer.adapter.execute(rendered).fetchall() == [(2, 12.0)]


def test_secure_pg_catalog_accepts_terminators_and_preserves_schema_name():
    connection = SemanticLayerConnection.__new__(SemanticLayerConnection)
    connection.layer = _layer(enforce_visibility=True)
    captured = {}

    def send_reader(reader, callback):
        captured["table"] = reader.read_all()
        callback(True)

    connection.send_reader = send_reader

    connection._handle_query("SELECT * FROM information_schema.tables;", lambda *_: None)
    assert captured["table"].column_names == ["table_schema", "table_name", "table_type"]
    assert captured["table"].to_pylist() == [
        {"table_schema": "semantic_layer", "table_name": "orders", "table_type": "BASE TABLE"}
    ]

    connection._handle_query(
        "SELECT table_schema, table_name, column_name, udt_name, "
        "character_octet_length, datetime_precision FROM information_schema.columns "
        "WHERE table_schema = 'semantic_layer' AND table_name = 'orders' ORDER BY ordinal_position;",
        lambda *_: None,
    )
    rows = captured["table"].to_pylist()
    assert [row["column_name"] for row in rows] == ["tenant_id", "total_amount"]
    assert rows[0]["udt_name"] == "numeric"
    assert "character_octet_length" in rows[0]
    assert "datetime_precision" in rows[0]

    connection._handle_query("SELECT nspname FROM pg_catalog.pg_namespace;", lambda *_: None)
    assert [row["nspname"] for row in captured["table"].to_pylist()] == ["semantic_layer"]

    plain_layer = SemanticLayer()
    plain_layer.add_model(Model(name="events", table="events", dimensions=[Dimension(name="id", type="numeric")]))
    connection.layer = plain_layer
    connection._handle_query(
        "SELECT udt_name, numeric_precision_radix FROM information_schema.columns "
        "WHERE table_schema = 'semantic_layer' AND table_name = 'events';",
        lambda *_: None,
    )
    assert captured["table"].to_pylist() == [{"udt_name": "numeric", "numeric_precision_radix": 10}]


def test_mapped_pg_users_authenticate_with_shared_secret_and_keep_attributes():
    attrs_map = {"alice": {"tenant_id": 1}, "bob": {"tenant_id": 2}}
    connection = SemanticLayerConnection(
        connection_id=1,
        executor=None,
        layer=_layer(),
        username="mapped-users",
        password="shared-secret",
        user_attrs_map=attrs_map,
    )
    results = []

    connection.handle_auth("alice", "shared-secret", "localhost", callback=results.append)
    assert connection._user_attributes() == {"tenant_id": 1}
    connection.handle_auth("bob", "shared-secret", "localhost", callback=results.append)
    assert connection._user_attributes() == {"tenant_id": 2}
    connection.handle_auth("mallory", "shared-secret", "localhost", callback=results.append)
    assert connection._user_attributes() is None
    connection.handle_auth("alice", "wrong", "localhost", callback=results.append)
    assert connection._user_attributes() is None
    assert results == [True, True, False, False]


def test_sql_rewrite_cache_isolated_by_visibility_state():
    layer = SemanticLayer()
    layer.adapter.execute("create table records (id integer, secret_note varchar)")
    layer.adapter.execute("insert into records values (1, 'private')")
    layer.add_model(
        Model(
            name="records",
            table="records",
            primary_key="id",
            dimensions=[
                Dimension(name="id", sql="id", type="numeric"),
                Dimension(name="secret_note", sql="secret_note", type="categorical", public=False),
            ],
        )
    )

    assert layer.sql("SELECT secret_note FROM records").fetchall() == [("private",)]

    layer.enforce_visibility = True
    with pytest.raises(SecurityError, match="not public"):
        layer.sql("SELECT secret_note FROM records")


def test_yardstick_sql_fails_closed_across_sql_transports(tmp_path):
    attrs = {"role": "analyst", "tenant_id": 1}
    queries = [
        "SELECT AGGREGATE(total_amount) FROM orders",
        "SELECT total_amount FROM orders",
    ]
    client = TestClient(create_app(_layer(yardstick=True), auth_token="secret"))

    for query in queries:
        response = client.post("/sql", json={"query": query}, headers=_headers(attrs))
        assert response.status_code == 403
        assert "Yardstick semantic SQL" in response.json()["error"]

    _mcp_layer(tmp_path / "mcp-yardstick", attrs, yardstick=True)
    for query in queries:
        with pytest.raises(SecurityError, match="Yardstick semantic SQL"):
            mcp_run_sql(query)

    pg_layer = _layer(yardstick=True)
    for query in queries:
        with pytest.raises(SecurityError, match="Yardstick semantic SQL"):
            _pg_rewrite(pg_layer, query, attrs)


def test_hidden_column_is_rejected_and_omitted_across_transports(tmp_path):
    attrs = {"role": "analyst", "tenant_id": 1}
    http_layer = _layer(enforce_visibility=True)
    client = TestClient(create_app(http_layer, auth_token="secret"))

    for path, payload in [
        ("/query", {"dimensions": ["orders.secret_note"], "metrics": ["orders.total_amount"]}),
        ("/sql", {"query": "SELECT secret_note, total_amount FROM orders"}),
        ("/sql", {"query": "SELECT MIN(secret_note) FROM orders"}),
    ]:
        response = client.post(path, json=payload, headers=_headers(attrs))
        assert response.status_code == 403, response.text
        assert "not public" in response.json()["error"]

    mcp_layer = _mcp_layer(tmp_path / "mcp-hidden", attrs, enforce_visibility=True)
    mcp_model = mcp_layer.graph.models["orders"]
    mcp_model.dimensions.append(
        Dimension(name="hidden_time", sql="tenant_id", type="time", granularity="day", public=False)
    )
    mcp_model.default_time_dimension = "hidden_time"
    graph = get_semantic_graph()
    assert "secret_note" not in graph["models"][0]["dimensions"]
    assert "default_time_dimension" not in graph["models"][0]
    assert "default_time_dimension" not in get_models(["orders"])["models"][0]
    with pytest.raises(SecurityError, match="not public"):
        mcp_run_query(dimensions=["orders.secret_note"], metrics=["orders.total_amount"])
    with pytest.raises(SecurityError, match="not public"):
        mcp_run_sql("SELECT secret_note, total_amount FROM orders")
    with pytest.raises(SecurityError, match="not public"):
        mcp_run_sql("SELECT MIN(secret_note) FROM orders")

    with pytest.raises(SecurityError, match="not public"):
        _pg_rewrite(
            _layer(enforce_visibility=True),
            "SELECT secret_note, total_amount FROM orders",
            attrs,
        )
    with pytest.raises(SecurityError, match="not public"):
        _pg_rewrite(
            _layer(enforce_visibility=True),
            "SELECT MIN(secret_note) FROM orders",
            attrs,
        )
