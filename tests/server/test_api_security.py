"""Tests for HTTP API security integration (work item A3).

Covers per-request user attributes resolved from the trusted user header and
threaded into the layer compile/query path plus the result-cache key:
- header present -> attributes reach the query and scope rows;
- header absent + require_user_attrs -> 400;
- malformed JSON header -> 400;
- SecurityError from a secured model (no attrs) -> 403;
- non-public field with enforce_visibility -> error;
- result-cache key differs across two different X-Sidemantic-User values.
"""

# ruff: noqa: E402

from __future__ import annotations

import json

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("pyarrow")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient

from sidemantic import Dimension, Metric, Model, SecurityPolicy, SemanticLayer
from sidemantic.api_server import create_app


def _make_layer() -> SemanticLayer:
    """Build an in-memory layer whose ``orders`` model has a row-level filter."""
    layer = SemanticLayer()
    # Populate the layer's own adapter DB so the app queries the same tables.
    layer.adapter.execute(
        """
        create table orders (
            id integer,
            tenant_id integer,
            status varchar,
            amount double
        )
        """
    )
    layer.adapter.executemany(
        "insert into orders values (?, ?, ?, ?)",
        [
            (1, 1, "completed", 10.0),
            (2, 1, "completed", 20.0),
            (3, 2, "completed", 5.0),
            (4, 2, "pending", 7.0),
        ],
    )

    model = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[
            Dimension(name="tenant_id", sql="tenant_id", type="numeric"),
            Dimension(name="status", sql="status", type="categorical"),
            Dimension(name="secret_note", sql="status", type="categorical", public=False),
        ],
        metrics=[
            Metric(name="order_count", agg="count"),
            Metric(name="total_amount", agg="sum", sql="amount"),
        ],
        security=SecurityPolicy(row_filters=["tenant_id = {{ user.tenant_id }}"]),
    )
    layer.add_model(model)
    return layer


def _headers(token: str = "secret", user_attrs: dict | None = None, header: str = "X-Sidemantic-User") -> dict:
    out = {"Authorization": f"Bearer {token}"}
    if user_attrs is not None:
        out[header] = json.dumps(user_attrs)
    return out


def test_user_header_scopes_rows():
    """Header present -> attributes reach the query and scope rows to the tenant."""
    layer = _make_layer()
    app = create_app(layer, auth_token="secret")
    client = TestClient(app)

    resp = client.post(
        "/query",
        json={"metrics": ["orders.total_amount"], "dimensions": ["orders.tenant_id"]},
        headers=_headers(user_attrs={"tenant_id": 1}),
    )
    assert resp.status_code == 200, resp.text
    rows = resp.json()["rows"]
    # Only tenant 1 rows should be visible.
    tenants = {row["tenant_id"] for row in rows}
    assert tenants == {1}
    total = sum(row["total_amount"] for row in rows)
    assert total == 30.0


def test_require_user_attrs_missing_header_returns_400():
    layer = _make_layer()
    app = create_app(layer, auth_token="secret", require_user_attrs=True)
    client = TestClient(app)

    resp = client.post(
        "/query",
        json={"metrics": ["orders.total_amount"]},
        headers=_headers(),  # no user header
    )
    assert resp.status_code == 400
    assert "user-attributes header" in resp.json()["detail"].lower()


def test_malformed_json_header_returns_400():
    layer = _make_layer()
    app = create_app(layer, auth_token="secret")
    client = TestClient(app)

    resp = client.post(
        "/query",
        json={"metrics": ["orders.total_amount"]},
        headers={"Authorization": "Bearer secret", "X-Sidemantic-User": "{not valid json"},
    )
    assert resp.status_code == 400
    assert "malformed json" in resp.json()["detail"].lower()


def test_non_object_json_header_returns_400():
    layer = _make_layer()
    app = create_app(layer, auth_token="secret")
    client = TestClient(app)

    resp = client.post(
        "/query",
        json={"metrics": ["orders.total_amount"]},
        headers={"Authorization": "Bearer secret", "X-Sidemantic-User": "[1, 2, 3]"},
    )
    assert resp.status_code == 400
    assert "json object" in resp.json()["detail"].lower()


def test_secured_model_without_attrs_returns_403():
    """A secured model queried with no user attributes -> SecurityError -> 403."""
    layer = _make_layer()
    app = create_app(layer, auth_token="secret")
    client = TestClient(app)

    resp = client.post(
        "/query",
        json={"metrics": ["orders.total_amount"]},
        headers=_headers(),  # no user header -> None attrs -> deny-by-default
    )
    assert resp.status_code == 403
    assert "error" in resp.json()


def test_enforce_visibility_rejects_non_public_field():
    layer = _make_layer()
    app = create_app(layer, auth_token="secret", enforce_visibility=True)
    client = TestClient(app)

    resp = client.post(
        "/query",
        json={"dimensions": ["orders.secret_note"], "metrics": ["orders.order_count"]},
        headers=_headers(user_attrs={"tenant_id": 1}),
    )
    assert resp.status_code == 403
    assert "not public" in resp.json()["error"].lower()


def test_custom_user_header_name():
    layer = _make_layer()
    app = create_app(layer, auth_token="secret", user_header="X-My-User")
    client = TestClient(app)

    resp = client.post(
        "/query",
        json={"metrics": ["orders.total_amount"], "dimensions": ["orders.tenant_id"]},
        headers=_headers(user_attrs={"tenant_id": 2}, header="X-My-User"),
    )
    assert resp.status_code == 200, resp.text
    tenants = {row["tenant_id"] for row in resp.json()["rows"]}
    assert tenants == {2}


def test_result_cache_key_differs_across_users():
    """The result-cache key must differ for different X-Sidemantic-User values.

    Two users with the same compiled query (differing only by user attributes)
    must not share a cached result. We assert directly on build_result_key so the
    test does not depend on cache internals.
    """
    layer = _make_layer()
    # Same compiled SQL string, different user attributes -> different keys.
    sql = "select 1"
    key_user_a = layer.build_result_key(sql, user_attributes={"tenant_id": 1})
    key_user_b = layer.build_result_key(sql, user_attributes={"tenant_id": 2})
    key_none = layer.build_result_key(sql, user_attributes=None)
    assert key_user_a != key_user_b
    assert key_user_a != key_none
    assert key_user_b != key_none


def test_result_cache_no_cross_user_leak_end_to_end():
    """End-to-end: caching enabled, two different users get their own scoped rows."""
    layer = _make_layer()
    app = create_app(layer, auth_token="secret", result_cache_mb=16)
    client = TestClient(app)

    resp_a = client.post(
        "/query",
        json={"metrics": ["orders.total_amount"], "dimensions": ["orders.tenant_id"]},
        headers=_headers(user_attrs={"tenant_id": 1}),
    )
    resp_b = client.post(
        "/query",
        json={"metrics": ["orders.total_amount"], "dimensions": ["orders.tenant_id"]},
        headers=_headers(user_attrs={"tenant_id": 2}),
    )
    assert resp_a.status_code == 200, resp_a.text
    assert resp_b.status_code == 200, resp_b.text
    tenants_a = {row["tenant_id"] for row in resp_a.json()["rows"]}
    tenants_b = {row["tenant_id"] for row in resp_b.json()["rows"]}
    assert tenants_a == {1}
    assert tenants_b == {2}
