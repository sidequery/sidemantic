"""Tests for MCP server static user-attributes plumbing (work item A3).

The MCP server has no per-session identity here, so security ``user_attributes``
are a server-level, process-wide value provided at ``initialize_layer``. They are
threaded into the structured ``run_query`` compile path (and ``create_chart``).
When unset (None), a query touching a secured model is denied by the layer.
"""

# ruff: noqa: E402

import tempfile
from pathlib import Path

import pytest

from tests.optional_dep_stubs import ensure_fake_mcp

ensure_fake_mcp()

from sidemantic.core.semantic_layer import SecurityError
from sidemantic.mcp_server import get_user_attributes, initialize_layer, run_query


def _write_secured_models(directory: Path) -> None:
    (directory / "models.yml").write_text(
        """
models:
  - name: orders
    table: orders
    primary_key: id
    dimensions:
      - name: tenant_id
        sql: tenant_id
        type: numeric
    metrics:
      - name: order_count
        agg: count
    security:
      row_filters:
        - "tenant_id = {{ user.tenant_id }}"
"""
    )


def _seed(layer) -> None:
    layer.adapter.execute("create table orders (id integer, tenant_id integer)")
    layer.adapter.executemany(
        "insert into orders values (?, ?)",
        [(1, 1), (2, 1), (3, 2)],
    )


def test_user_attributes_default_none_denies_secured_model():
    tmpdir = Path(tempfile.mkdtemp())
    _write_secured_models(tmpdir)
    layer = initialize_layer(str(tmpdir), db_path=":memory:")
    _seed(layer)

    assert get_user_attributes() is None
    with pytest.raises(SecurityError):
        run_query(dimensions=["orders.tenant_id"], metrics=["orders.order_count"])


def test_static_user_attributes_scope_rows():
    tmpdir = Path(tempfile.mkdtemp())
    _write_secured_models(tmpdir)
    layer = initialize_layer(str(tmpdir), db_path=":memory:", user_attributes={"tenant_id": 2})
    _seed(layer)

    assert get_user_attributes() == {"tenant_id": 2}
    result = run_query(dimensions=["orders.tenant_id"], metrics=["orders.order_count"])
    tenants = {row["tenant_id"] for row in result["rows"]}
    assert tenants == {2}
