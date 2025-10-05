"""Test semantic layer with real DuckDB data.

This test verifies end-to-end functionality with actual database queries.
"""

import duckdb
import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer


@pytest.fixture
def test_db():
    """Create test database with orders and customers."""
    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE orders (
            order_id INTEGER,
            customer_id INTEGER,
            status VARCHAR,
            order_amount DECIMAL(10, 2),
            created_at DATE
        )
    """)

    conn.execute("""
        CREATE TABLE customers (
            customer_id INTEGER,
            customer_name VARCHAR,
            region VARCHAR,
            tier VARCHAR
        )
    """)

    conn.execute("""
        INSERT INTO orders VALUES
            (1, 101, 'completed', 150.00, '2024-01-15'),
            (2, 102, 'completed', 200.00, '2024-01-20'),
            (3, 101, 'pending', 75.00, '2024-02-01'),
            (4, 103, 'completed', 300.00, '2024-02-10'),
            (5, 102, 'cancelled', 50.00, '2024-02-15')
    """)

    conn.execute("""
        INSERT INTO customers VALUES
            (101, 'Alice', 'US', 'premium'),
            (102, 'Bob', 'EU', 'standard'),
            (103, 'Charlie', 'US', 'premium')
    """)

    return conn


@pytest.fixture
def semantic_layer(test_db):
    """Create semantic layer with orders and customers models."""
    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[
            Dimension(name="order_id", type="numeric"),
            Dimension(name="status", type="categorical"),
            Dimension(name="created_at", type="time", granularity="day"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="order_amount"),
            Metric(name="order_count", agg="count"),
            Metric(name="avg_order_value", agg="avg", sql="order_amount"),
        ],
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
    )

    customers = Model(
        name="customers",
        table="customers",
        primary_key="customer_id",
        dimensions=[
            Dimension(name="customer_id", type="numeric"),
            Dimension(name="customer_name", type="categorical"),
            Dimension(name="region", type="categorical"),
            Dimension(name="tier", type="categorical"),
        ],
    )

    layer = SemanticLayer()
    layer.conn = test_db  # Use the existing test database connection
    layer.add_model(orders)
    layer.add_model(customers)
    return layer


def test_simple_aggregation(test_db, semantic_layer):
    """Test simple aggregation by status."""
    results = semantic_layer.query(metrics=["orders.revenue"], dimensions=["orders.status"]).fetchall()

    # Convert to dict for easier assertions
    results_dict = {row[0]: row[1] for row in results}

    assert results_dict["completed"] == 650.00  # 150 + 200 + 300
    assert results_dict["pending"] == 75.00
    assert results_dict["cancelled"] == 50.00


def test_time_granularity(test_db, semantic_layer):
    """Test aggregation with time dimension granularity."""
    results = semantic_layer.query(metrics=["orders.revenue"], dimensions=["orders.created_at__month"]).fetchall()

    # Should have 2 months: Jan 2024 and Feb 2024
    assert len(results) == 2

    # Convert to dict (date -> revenue)
    results_dict = {str(row[0])[:7]: row[1] for row in results}  # Extract YYYY-MM

    assert results_dict["2024-01"] == 350.00  # 150 + 200
    assert results_dict["2024-02"] == 425.00  # 75 + 300 + 50


def test_cross_model_join(test_db, semantic_layer):
    """Test query across models with automatic join."""
    results = semantic_layer.query(metrics=["orders.revenue"], dimensions=["customers.region"]).fetchall()

    # Convert to dict
    results_dict = {row[0]: row[1] for row in results}

    # US: orders from customers 101 (150 + 75) and 103 (300) = 525
    # EU: orders from customer 102 (200 + 50) = 250
    assert results_dict["US"] == 525.00
    assert results_dict["EU"] == 250.00


def test_filters_with_join(test_db, semantic_layer):
    """Test filters on joined models."""
    results = semantic_layer.query(
        metrics=["orders.revenue"], dimensions=["customers.tier"], filters=["customers.tier = 'premium'"]
    ).fetchall()

    # Should only have premium tier
    assert len(results) == 1
    assert results[0][0] == "premium"
    # Premium customers: 101 (150 + 75) + 103 (300) = 525
    assert results[0][1] == 525.00


def test_multiple_metrics(test_db, semantic_layer):
    """Test querying multiple metrics together."""
    results = semantic_layer.query(
        metrics=["orders.revenue", "orders.order_count", "orders.avg_order_value"], dimensions=["orders.status"]
    ).fetchall()

    # Convert to dict: status -> (revenue, count, avg)
    results_dict = {row[0]: (row[1], row[2], row[3]) for row in results}

    # Completed: 650 total, 3 orders, avg 216.67
    assert results_dict["completed"][0] == 650.00
    assert results_dict["completed"][1] == 3
    assert abs(results_dict["completed"][2] - 216.67) < 0.01

    # Pending: 75 total, 1 order, avg 75
    assert results_dict["pending"][0] == 75.00
    assert results_dict["pending"][1] == 1
    assert results_dict["pending"][2] == 75.00


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
