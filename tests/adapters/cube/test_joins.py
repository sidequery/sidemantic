"""Tests for Cube adapter - join parsing, SQL generation, and round-trip.

Cube expresses joins as a SQL equality condition (``${CUBE}.fk = ${target.pk}``),
which can be composite, non-equality, or use the single-brace ``{cube.col}`` form.
These tests cover that the adapter:

- extracts structured foreign/primary keys for plain single-column equality joins,
- preserves composite / non-equality / one_to_one conditions into Relationship.sql,
- warns (instead of silently faking) when a join cannot be parsed,
- generates correct, executable SQL, and round-trips through export.
"""

import tempfile
from pathlib import Path

import pytest
import yaml

from sidemantic import SemanticLayer
from sidemantic.adapters.cube import CubeAdapter


def _parse(yaml_text: str):
    adapter = CubeAdapter()
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as f:
        f.write(yaml_text)
        path = Path(f.name)
    try:
        return adapter.parse(path)
    finally:
        path.unlink()


def test_simple_many_to_one_extracts_structured_keys():
    graph = _parse(
        """
cubes:
  - name: orders
    sql_table: orders
    joins:
      - name: customers
        relationship: many_to_one
        sql: "${CUBE}.customer_id = ${customers.id}"
  - name: customers
    sql_table: customers
"""
    )
    rel = graph.get_model("orders").relationships[0]
    assert rel.name == "customers"
    assert rel.type == "many_to_one"
    assert rel.foreign_key == "customer_id"
    assert rel.primary_key == "id"
    assert rel.sql is None


def test_simple_one_to_many_extracts_structured_keys():
    graph = _parse(
        """
cubes:
  - name: orders
    sql_table: orders
    joins:
      - name: line_items
        relationship: one_to_many
        sql: "${CUBE}.id = ${line_items.order_id}"
  - name: line_items
    sql_table: line_items
"""
    )
    rel = graph.get_model("orders").relationships[0]
    assert rel.type == "one_to_many"
    # FK lives on the related model; local key on this one.
    assert rel.foreign_key == "order_id"
    assert rel.primary_key == "id"
    assert rel.sql is None


def test_composite_key_join_preserves_full_condition():
    graph = _parse(
        """
cubes:
  - name: line_items
    sql_table: line_items
    joins:
      - name: orders
        relationship: many_to_one
        sql: "${CUBE}.order_id = ${orders.id} AND ${CUBE}.tenant_id = ${orders.tenant_id}"
  - name: orders
    sql_table: orders
"""
    )
    rel = graph.get_model("line_items").relationships[0]
    assert rel.type == "many_to_one"
    assert rel.sql == "{from}.order_id = {to}.id AND {from}.tenant_id = {to}.tenant_id"


def test_one_to_one_preserves_condition():
    # Single-brace diamond form, with the local cube referenced by name.
    graph = _parse(
        """
cubes:
  - name: a
    sql_table: a
    joins:
      - name: b
        relationship: one_to_one
        sql: "{a.id} = {b.id}"
  - name: b
    sql_table: b
"""
    )
    rel = graph.get_model("a").relationships[0]
    assert rel.type == "one_to_one"
    assert rel.sql == "{from}.id = {to}.id"


def test_unparseable_join_warns_and_keeps_key_unknown():
    adapter = CubeAdapter()
    yaml_text = """
cubes:
  - name: x
    sql_table: x
    joins:
      - name: y
        relationship: many_to_one
        sql: "${z.a} = ${w.b}"
  - name: y
    sql_table: y
"""
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as f:
        f.write(yaml_text)
        path = Path(f.name)
    try:
        with pytest.warns(UserWarning, match="Could not parse Cube join"):
            graph = adapter.parse(path)
    finally:
        path.unlink()
    rel = graph.get_model("x").relationships[0]
    assert rel.foreign_key is None
    assert rel.sql is None


def test_composite_key_join_generates_correct_sql_and_results():
    graph = _parse(
        """
cubes:
  - name: line_items
    sql_table: line_items
    dimensions:
      - {name: id, sql: id, type: number, primary_key: true}
      - {name: order_id, sql: order_id, type: number}
      - {name: tenant_id, sql: tenant_id, type: number}
    measures:
      - {name: count, type: count}
    joins:
      - name: orders
        relationship: many_to_one
        sql: "${CUBE}.order_id = ${orders.id} AND ${CUBE}.tenant_id = ${orders.tenant_id}"
  - name: orders
    sql_table: orders
    dimensions:
      - {name: id, sql: id, type: number, primary_key: true}
      - {name: tenant_id, sql: tenant_id, type: number}
      - {name: status, sql: status, type: string}
    measures:
      - {name: count, type: count}
"""
    )
    layer = SemanticLayer()
    layer.graph = graph
    layer.adapter.execute("CREATE TABLE line_items (id INT, order_id INT, tenant_id INT)")
    layer.adapter.execute("INSERT INTO line_items VALUES (100, 1, 10), (101, 1, 10), (102, 2, 20)")
    layer.adapter.execute("CREATE TABLE orders (id INT, tenant_id INT, status VARCHAR)")
    layer.adapter.execute("INSERT INTO orders VALUES (1, 10, 'done'), (2, 20, 'open')")

    sql = layer.compile(metrics=["line_items.count"], dimensions=["orders.status"])
    # Both keys in the ON clause; no phantom "orders_id" column projected.
    assert "order_id" in sql and "tenant_id" in sql
    assert "orders_id" not in sql

    rows = dict(layer.adapter.execute(sql).fetchall())
    assert rows == {"done": 2, "open": 1}


def test_composite_key_join_round_trips_through_export():
    graph = _parse(
        """
cubes:
  - name: line_items
    sql_table: line_items
    joins:
      - name: orders
        relationship: many_to_one
        sql: "${CUBE}.order_id = ${orders.id} AND ${CUBE}.tenant_id = ${orders.tenant_id}"
  - name: orders
    sql_table: orders
"""
    )
    adapter = CubeAdapter()
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as f:
        out = Path(f.name)
    try:
        adapter.export(graph, out)
        exported = yaml.safe_load(out.read_text())
        join_sql = next(c for c in exported["cubes"] if c["name"] == "line_items")["joins"][0]["sql"]
        # Exported back to Cube placeholders.
        assert join_sql == "${CUBE}.order_id = ${orders.id} AND ${CUBE}.tenant_id = ${orders.tenant_id}"

        reimported = adapter.parse(out)
        rel = reimported.get_model("line_items").relationships[0]
        assert rel.sql == "{from}.order_id = {to}.id AND {from}.tenant_id = {to}.tenant_id"
    finally:
        out.unlink()
