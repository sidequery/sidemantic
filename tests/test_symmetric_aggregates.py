"""Tests for symmetric aggregates (fan-out join handling)."""

import pytest
import duckdb

from sidemantic.core.model import Model, Dimension, Metric, Relationship
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.core.symmetric_aggregate import build_symmetric_aggregate_sql
from sidemantic.sql.generator_v2 import SQLGenerator


def test_build_symmetric_aggregate_sum():
    """Test building symmetric aggregate SQL for SUM."""
    sql = build_symmetric_aggregate_sql(
        measure_expr="amount",
        primary_key="order_id",
        agg_type="sum",
    )

    # Check key components are present
    assert "SUM(DISTINCT" in sql
    assert "HASH(order_id)" in sql
    assert "HUGEINT" in sql
    assert "+ amount)" in sql


def test_build_symmetric_aggregate_sum_with_alias():
    """Test symmetric aggregate with table alias."""
    sql = build_symmetric_aggregate_sql(
        measure_expr="amount",
        primary_key="order_id",
        agg_type="sum",
        model_alias="orders_cte",
    )

    # Check key components with alias are present
    assert "HASH(orders_cte.order_id)" in sql
    assert "orders_cte.amount)" in sql


def test_build_symmetric_aggregate_avg():
    """Test building symmetric aggregate SQL for AVG."""
    sql = build_symmetric_aggregate_sql(
        measure_expr="amount",
        primary_key="order_id",
        agg_type="avg",
    )

    # Average is sum divided by distinct count
    assert "SUM(DISTINCT" in sql
    assert "HASH(order_id)" in sql
    assert "COUNT(DISTINCT order_id)" in sql
    assert "NULLIF" in sql


def test_build_symmetric_aggregate_count():
    """Test building symmetric aggregate SQL for COUNT."""
    sql = build_symmetric_aggregate_sql(
        measure_expr="amount",
        primary_key="order_id",
        agg_type="count",
    )

    assert sql == "COUNT(DISTINCT order_id)"


def test_build_symmetric_aggregate_count_distinct():
    """Test building symmetric aggregate SQL for COUNT DISTINCT."""
    sql = build_symmetric_aggregate_sql(
        measure_expr="customer_id",
        primary_key="order_id",
        agg_type="count_distinct",
    )

    # COUNT DISTINCT doesn't use symmetric aggregates - just counts distinct values
    assert sql == "COUNT(DISTINCT customer_id)"


def test_fanout_join_detection_single_join():
    """Test that single one-to-many join doesn't trigger symmetric aggregates."""
    graph = SemanticGraph()

    # Orders (base)
    orders = Model(
        name="orders",
        table="raw_orders",
        primary_key="id",
        dimensions=[Dimension(name="order_date", type="time", sql="order_date")],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        relationships=[Relationship(name="order_items", type="one_to_many", sql="id", foreign_key="order_id")],
    )

    # Order items (many)
    order_items = Model(
        name="order_items",
        table="raw_order_items",
        primary_key="id",
        dimensions=[],
        metrics=[Metric(name="quantity", agg="sum", sql="quantity")],
        relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id", primary_key="id")],
    )

    graph.add_model(orders)
    graph.add_model(order_items)

    generator = SQLGenerator(graph)

    # Single one-to-many join should NOT trigger symmetric aggregates
    needs_symmetric = generator._has_fanout_joins("orders", ["order_items"])

    assert needs_symmetric["orders"] is False


def test_fanout_join_detection_multiple_joins():
    """Test that multiple one-to-many joins trigger symmetric aggregates."""
    graph = SemanticGraph()

    # Orders (base) - has two one-to-many relationships
    orders = Model(
        name="orders",
        table="raw_orders",
        primary_key="id",
        dimensions=[Dimension(name="order_date", type="time", sql="order_date")],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        relationships=[
            Relationship(name="order_items", type="one_to_many", sql="id", foreign_key="order_id"),
            Relationship(name="shipments", type="one_to_many", sql="id", foreign_key="order_id"),
        ],
    )

    # Order items (many)
    order_items = Model(
        name="order_items",
        table="raw_order_items",
        primary_key="id",
        dimensions=[],
        metrics=[Metric(name="quantity", agg="sum", sql="quantity")],
        relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id", primary_key="id")],
    )

    # Shipments (many)
    shipments = Model(
        name="shipments",
        table="raw_shipments",
        primary_key="id",
        dimensions=[],
        metrics=[Metric(name="shipment_count", agg="count", sql="*")],
        relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id", primary_key="id")],
    )

    graph.add_model(orders)
    graph.add_model(order_items)
    graph.add_model(shipments)

    generator = SQLGenerator(graph)

    # Multiple one-to-many joins SHOULD trigger symmetric aggregates for base model
    needs_symmetric = generator._has_fanout_joins("orders", ["order_items", "shipments"])

    assert needs_symmetric["orders"] is True
    assert needs_symmetric["order_items"] is False
    assert needs_symmetric["shipments"] is False


def test_symmetric_aggregates_in_sql_generation():
    """Test that SQL generation uses symmetric aggregates for fan-out joins."""
    graph = SemanticGraph()

    # Orders (base) - has two one-to-many relationships
    orders = Model(
        name="orders",
        table="raw_orders",
        primary_key="id",
        dimensions=[Dimension(name="order_date", type="time", sql="order_date")],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        relationships=[
            Relationship(name="order_items", type="one_to_many", sql="id", foreign_key="order_id"),
            Relationship(name="shipments", type="one_to_many", sql="id", foreign_key="order_id"),
        ],
    )

    # Order items
    order_items = Model(
        name="order_items",
        table="raw_order_items",
        primary_key="id",
        dimensions=[],
        metrics=[Metric(name="quantity", agg="sum", sql="quantity")],
        relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id", primary_key="id")],
    )

    # Shipments
    shipments = Model(
        name="shipments",
        table="raw_shipments",
        primary_key="id",
        dimensions=[],
        metrics=[Metric(name="shipment_count", agg="count", sql="*")],
        relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id", primary_key="id")],
    )

    graph.add_model(orders)
    graph.add_model(order_items)
    graph.add_model(shipments)

    generator = SQLGenerator(graph)

    # Query with measures from all three models
    sql = generator.generate(
        metrics=["orders.revenue", "order_items.quantity", "shipments.shipment_count"],
        dimensions=["orders.order_date"],
    )

    # Orders.revenue should use symmetric aggregates (HASH function)
    assert "HASH(orders_cte.id)" in sql
    assert "SUM(" in sql and "DISTINCT" in sql


def test_symmetric_aggregates_with_data():
    """Test symmetric aggregates prevent double-counting with actual data."""
    # Create in-memory DuckDB
    conn = duckdb.connect(":memory:")

    # Create test data with fan-out
    # Order 1 has 2 items and 2 shipments (2x2 = 4 rows after join)
    # Order 2 has 1 item and 1 shipment (1x1 = 1 row after join)
    conn.execute("""
        CREATE TABLE raw_orders AS
        SELECT * FROM (VALUES
            (1, '2024-01-01'::DATE, 100),
            (2, '2024-01-02'::DATE, 200)
        ) AS t(id, order_date, amount)
    """)

    conn.execute("""
        CREATE TABLE raw_order_items AS
        SELECT * FROM (VALUES
            (1, 1, 5),
            (2, 1, 3),
            (3, 2, 10)
        ) AS t(id, order_id, quantity)
    """)

    conn.execute("""
        CREATE TABLE raw_shipments AS
        SELECT * FROM (VALUES
            (1, 1),
            (2, 1),
            (3, 2)
        ) AS t(id, order_id)
    """)

    # Create graph
    graph = SemanticGraph()

    orders = Model(
        name="orders",
        table="raw_orders",
        primary_key="id",
        dimensions=[Dimension(name="order_date", type="time", sql="order_date")],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        relationships=[
            Relationship(name="order_items", type="one_to_many", sql="id", foreign_key="order_id"),
            Relationship(name="shipments", type="one_to_many", sql="id", foreign_key="order_id"),
        ],
    )

    order_items = Model(
        name="order_items",
        table="raw_order_items",
        primary_key="id",
        dimensions=[],
        metrics=[Metric(name="quantity", agg="sum", sql="quantity")],
        relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id", primary_key="id")],
    )

    shipments = Model(
        name="shipments",
        table="raw_shipments",
        primary_key="id",
        dimensions=[],
        metrics=[Metric(name="shipment_count", agg="count", sql="*")],
        relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id", primary_key="id")],
    )

    graph.add_model(orders)
    graph.add_model(order_items)
    graph.add_model(shipments)

    # Generate SQL - query all three models to create fan-out
    generator = SQLGenerator(graph)
    sql = generator.generate(
        metrics=["orders.revenue", "order_items.quantity", "shipments.shipment_count"],
        dimensions=["orders.order_date"],
    )

    # Execute query
    result = conn.execute(sql).fetchall()

    # Without symmetric aggregates:
    # Order 1: revenue would be 100*2*2 = 400 (wrong!)
    # Order 2: revenue would be 200*1*1 = 200 (correct by luck)

    # With symmetric aggregates:
    # Order 1: revenue = 100 (correct)
    # Order 2: revenue = 200 (correct)

    assert len(result) == 2
    revenues = sorted([row[1] for row in result])
    assert revenues == [100, 200]  # Correct totals, not inflated

    conn.close()
