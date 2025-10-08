"""Test that foreign keys can be queried as dimensions."""

import duckdb

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.generator import SQLGenerator


def test_foreign_key_as_dimension_no_join():
    """Test querying foreign key as dimension without joining to related model."""
    graph = SemanticGraph()

    # Create orders model with customer_id as FK but NOT as a dimension
    orders = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
    )

    graph.add_model(orders)

    # Query customer_id as a dimension WITHOUT joining to customers model
    gen = SQLGenerator(graph, dialect="duckdb")
    sql = gen.generate(
        metrics=["orders.revenue"],
        dimensions=["orders.customer_id"],
    )

    # Verify customer_id is in the CTE
    assert "customer_id AS customer_id" in sql, "customer_id should be in CTE"
    assert "orders_cte.customer_id AS customer_id" in sql, "customer_id should be in SELECT"

    # Test with actual data
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE orders AS SELECT * FROM (VALUES
            (1, 101, 100.0, 'completed'),
            (2, 101, 200.0, 'completed'),
            (3, 102, 150.0, 'completed'),
            (4, 102, 175.0, 'returned')
        ) AS t(id, customer_id, amount, status)
    """)

    result = conn.execute(sql).fetchall()

    # Should group by customer_id
    assert len(result) == 2, "Should have 2 customer groups"
    # Customer 101: 100 + 200 = 300
    # Customer 102: 150 + 175 = 325
    customer_revenues = {row[0]: float(row[1]) for row in result}
    assert customer_revenues[101] == 300.0
    assert customer_revenues[102] == 325.0


def test_foreign_key_dimension_with_join():
    """Test that FK still works when joining to related model."""
    graph = SemanticGraph()

    orders = Model(
        name="orders",
        table="orders",
        primary_key="id",
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
    )

    customers = Model(
        name="customers",
        table="customers",
        primary_key="id",
        dimensions=[
            Dimension(name="tier", type="categorical", sql="tier"),
        ],
    )

    graph.add_model(orders)
    graph.add_model(customers)

    # Query with both FK and dimension from related model
    gen = SQLGenerator(graph, dialect="duckdb")
    sql = gen.generate(
        metrics=["orders.revenue"],
        dimensions=["orders.customer_id", "customers.tier"],
    )

    # Verify both are in SQL
    assert "customer_id" in sql
    assert "tier" in sql

    # Test with actual data
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE orders AS SELECT * FROM (VALUES
            (1, 101, 100.0),
            (2, 101, 200.0),
            (3, 102, 150.0)
        ) AS t(id, customer_id, amount)
    """)
    conn.execute("""
        CREATE TABLE customers AS SELECT * FROM (VALUES
            (101, 'gold'),
            (102, 'silver')
        ) AS t(id, tier)
    """)

    result = conn.execute(sql).fetchall()
    assert len(result) == 2
