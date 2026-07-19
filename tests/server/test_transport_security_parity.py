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
from sidemantic.mcp_server import get_semantic_graph, initialize_layer
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


def _layer(*, enforce_visibility: bool = False) -> SemanticLayer:
    layer = SemanticLayer(enforce_visibility=enforce_visibility)
    layer.adapter.execute("create table orders (id integer, tenant_id integer, amount double, secret_note varchar)")
    layer.adapter.executemany(
        "insert into orders values (?, ?, ?, ?)",
        [(1, 1, 10.0, "a"), (2, 1, 20.0, "b"), (3, 2, 5.0, "c"), (4, 2, 7.0, "d")],
    )
    layer.add_model(_model())
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


def _mcp_layer(directory: Path, attrs: dict, *, enforce_visibility: bool = False) -> SemanticLayer:
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


def test_yardstick_sql_fails_closed_across_sql_transports(tmp_path):
    attrs = {"role": "analyst", "tenant_id": 1}
    query = "SELECT AGGREGATE(total_amount) FROM orders"
    client = TestClient(create_app(_layer(), auth_token="secret"))

    response = client.post("/sql", json={"query": query}, headers=_headers(attrs))
    assert response.status_code == 403
    assert "Yardstick semantic SQL" in response.json()["error"]

    _mcp_layer(tmp_path / "mcp-yardstick", attrs)
    with pytest.raises(SecurityError, match="Yardstick semantic SQL"):
        mcp_run_sql(query)

    with pytest.raises(SecurityError, match="Yardstick semantic SQL"):
        _pg_rewrite(_layer(), query, attrs)


def test_hidden_column_is_rejected_and_omitted_across_transports(tmp_path):
    attrs = {"role": "analyst", "tenant_id": 1}
    http_layer = _layer(enforce_visibility=True)
    client = TestClient(create_app(http_layer, auth_token="secret"))

    for path, payload in [
        ("/query", {"dimensions": ["orders.secret_note"], "metrics": ["orders.total_amount"]}),
        ("/sql", {"query": "SELECT secret_note, total_amount FROM orders"}),
    ]:
        response = client.post(path, json=payload, headers=_headers(attrs))
        assert response.status_code == 403, response.text
        assert "not public" in response.json()["error"]

    _mcp_layer(tmp_path / "mcp-hidden", attrs, enforce_visibility=True)
    graph = get_semantic_graph()
    assert "secret_note" not in graph["models"][0]["dimensions"]
    with pytest.raises(SecurityError, match="not public"):
        mcp_run_query(dimensions=["orders.secret_note"], metrics=["orders.total_amount"])
    with pytest.raises(SecurityError, match="not public"):
        mcp_run_sql("SELECT secret_note, total_amount FROM orders")

    with pytest.raises(SecurityError, match="not public"):
        _pg_rewrite(
            _layer(enforce_visibility=True),
            "SELECT secret_note, total_amount FROM orders",
            attrs,
        )
