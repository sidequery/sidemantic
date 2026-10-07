"""Execute imported logical fields and aggregates against adversarial row grains."""

import json

import pytest

from sidemantic import SemanticLayer
from sidemantic.interchange.ossie import OssieParseOptions, lower_ossie_document, parse_ossie_document


def field(name, expression=None):
    return {"name": name, "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": expression or name}]}}


def commerce(*, source_key="customer_id", target_key="id", key_kind="primary_key", budgets=(100, 200)):
    customers = {
        "name": "customers",
        "source": "customers",
        "fields": [field("id", target_key), field("name"), field("budget", "budget * 1")],
        key_kind: ["id"] if key_kind == "primary_key" else [["id"]],
    }
    document = {
        "version": "0.2.0.dev0",
        "semantic_model": [
            {
                "name": "commerce",
                "datasets": [
                    {
                        "name": "orders",
                        "source": "orders",
                        "primary_key": ["id"],
                        "fields": [field("id"), field("customer_ref", source_key), field("amount", "amount * 1")],
                    },
                    customers,
                ],
                "relationships": [
                    {
                        "name": "customer",
                        "from": "orders",
                        "to": "customers",
                        "from_columns": ["customer_ref"],
                        "to_columns": ["id"],
                    }
                ],
                "metrics": [
                    field(name, sql)
                    for name, sql in [
                        ("revenue", "SUM(orders.amount)"),
                        ("budget", "SUM(customers.budget)"),
                        ("customer_count", "COUNT(customers.id)"),
                        ("mean_budget", "AVG(customers.budget)"),
                        ("ratio", "SUM(orders.amount) / SUM(customers.budget)"),
                        ("double_revenue", "revenue * 2"),
                        ("weighted", "SUM(orders.amount * customers.budget)"),
                        ("combined", "weighted + budget"),
                        ("filtered_budget", "SUM(CASE WHEN customers.id = 1 THEN customers.budget END)"),
                        ("average_order", "SUM(orders.amount) / COUNT(*)"),
                    ]
                ],
            }
        ],
    }
    parsed = parse_ossie_document(json.dumps(document).encode(), options=OssieParseOptions(target_dialect="duckdb"))
    lowered = lower_ossie_document(parsed)
    assert lowered.valid, lowered.diagnostics
    layer = SemanticLayer.from_catalog(lowered.catalog, fallback=False, auto_register=False)
    layer.adapter.conn.execute("create table orders(id int, customer_id int, amount int)")
    layer.adapter.conn.execute("insert into orders values (1,1,10),(2,1,20),(3,2,30)")
    layer.adapter.conn.execute("create table customers(id int, name varchar, budget int)")
    layer.adapter.conn.executemany("insert into customers values (?,?,?)", [(1, "A", budgets[0]), (2, "B", budgets[1])])
    return layer


@pytest.mark.parametrize("key_kind", ["primary_key", "unique_keys"])
def test_independent_aggregate_grains_and_ratio(key_kind):
    layer = commerce(key_kind=key_kind)
    assert layer.query(metrics=["budget", "customer_count", "mean_budget"]).fetchall() == [(300, 2, 150)]
    assert layer.query(metrics=["revenue", "budget", "customer_count", "mean_budget", "ratio"]).fetchall() == [
        (60, 300, 2, 150, 0.2)
    ]
    assert layer.query(
        metrics=["budget"], dimensions=["orders.customer_ref"], order_by=["orders.customer_ref"]
    ).fetchall() == [(1, 100), (2, 200)]
    assert layer.query(
        metrics=["revenue", "budget"], dimensions=["customers.name"], order_by=["customers.name"]
    ).fetchall() == [("A", 30, 100), ("B", 30, 200)]


def test_equal_values_are_distinct_entities_and_filters_keep_row_grain():
    layer = commerce(budgets=(100, 100))
    assert layer.query(
        metrics=["budget", "customer_count"], dimensions=["orders.customer_ref"], order_by=["orders.customer_ref"]
    ).fetchall() == [(1, 100, 1), (2, 100, 1)]
    assert layer.query(metrics=["revenue", "budget", "customer_count"]).fetchall() == [(60, 200, 2)]
    assert layer.query(metrics=["revenue", "budget"], filters=["customers.name = 'A'"]).fetchall() == [(30, 100)]
    assert layer.query(metrics=["budget"], filters=["orders.amount >= 20"]).fetchall() == [(200,)]


@pytest.mark.parametrize("side", ["source", "target"])
def test_computed_join_fields_with_complete_foreign_keys(side):
    layer = commerce(
        source_key="customer_id + 1" if side == "source" else "customer_id",
        target_key="id + 1" if side == "target" else "id",
    )
    layer.adapter.conn.execute("delete from orders")
    values = [(1, 0, 10), (2, 1, 20)] if side == "source" else [(1, 2, 10), (2, 3, 20)]
    layer.adapter.conn.executemany("insert into orders values (?,?,?)", values)
    assert layer.query(metrics=["revenue"], dimensions=["customers.name"], order_by=["customers.name"]).fetchall() == [
        ("A", 10),
        ("B", 20),
    ]


def test_metric_dependencies_and_cross_dataset_row_expressions():
    layer = commerce()
    assert layer.query(metrics=["double_revenue"]).fetchall() == [(120,)]
    assert layer.query(metrics=["double_revenue", "budget"]).fetchall() == [(120, 300)]
    assert layer.query(metrics=["weighted"]).fetchall() == [(9000,)]
    assert layer.query(metrics=["weighted", "revenue", "budget"]).fetchall() == [(9000, 60, 300)]
    assert layer.query(metrics=["weighted", "revenue", "budget", "combined"]).fetchall() == [(9000, 60, 300, 9300)]
    assert layer.query(metrics=["combined"]).fetchall() == [(9300,)]
    assert layer.query(metrics=["combined"], dimensions=["customers.name"], order_by=["customers.name"]).fetchall() == [
        ("A", 3100),
        ("B", 6200),
    ]
    assert layer.query(metrics=["combined"], filters=["orders.amount >= 20"]).fetchall() == [(8300,)]
    assert layer.query(metrics=["weighted", "budget"]).fetchall() == [(9000, 300)]
    assert layer.query(
        metrics=["weighted", "budget"], dimensions=["customers.name"], order_by=["customers.name"]
    ).fetchall() == [("A", 3000, 100), ("B", 6000, 200)]
    assert layer.query(metrics=["weighted", "budget"], filters=["orders.amount >= 20"]).fetchall() == [(8000, 300)]
    # Query-local lowering must not mutate retained runtime or compilation state.
    assert layer.graph.metrics["revenue"].sql_is_complete
    assert layer.graph.models["orders"].metrics == []


def test_filtered_aggregate_and_columnless_leaf_keep_their_population():
    layer = commerce()
    assert layer.query(metrics=["revenue", "filtered_budget", "average_order"]).fetchall() == [(60, 100, 20)]
    assert layer.query(
        metrics=["filtered_budget"], dimensions=["orders.customer_ref"], order_by=["orders.customer_ref"]
    ).fetchall() == [(1, 100), (2, None)]


@pytest.mark.parametrize("source", ["raw_orders", "SELECT id, amount FROM raw_orders"])
def test_qualified_fields_bind_to_the_dataset_source(source):
    document = {
        "version": "0.2.0.dev0",
        "name": "commerce",
        "datasets": [
            {
                "name": "orders",
                "source": source,
                "primary_key": ["id"],
                "fields": [field("id", "orders.id"), field("amount", "orders.amount * 2")],
            }
        ],
        "metrics": [field("revenue", "SUM(orders.amount)")],
    }
    parsed = parse_ossie_document(json.dumps(document).encode(), options=OssieParseOptions(target_dialect="duckdb"))
    lowered = lower_ossie_document(parsed)
    assert lowered.valid, lowered.diagnostics
    layer = SemanticLayer.from_catalog(lowered.catalog, fallback=False, auto_register=False)
    layer.adapter.conn.execute("create table raw_orders(id int, amount int)")
    layer.adapter.conn.execute("insert into raw_orders values (1, 10), (2, 20)")

    assert layer.query(metrics=["revenue"]).fetchall() == [(60,)]
    assert layer.query(metrics=["revenue"], filters=["orders.amount > 20"]).fetchall() == [(40,)]
    assert layer.query(dimensions=["orders.id", "orders.amount"], order_by=["orders.id"]).fetchall() == [
        (1, 20),
        (2, 40),
    ]
