"""Test semantic layer with real DuckDB data.

This test verifies end-to-end functionality with actual database queries.
"""

import duckdb
import pytest

from sidemantic import Dimension, Entity, Measure, Metric, Model, SemanticLayer


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


def test_simple_aggregation(test_db):
    """Test simple aggregation by status."""
    # TODO: Implement test for simple aggregation
    # Expected: Group revenue by order status
    pass


def test_time_granularity(test_db):
    """Test aggregation with time dimension granularity."""
    # TODO: Implement test for monthly aggregation
    # Expected: Revenue by month with DATE_TRUNC
    pass


def test_cross_model_join(test_db):
    """Test query across models with automatic join."""
    # TODO: Implement test for orders + customers join
    # Expected: Revenue by customer region
    pass


def test_filters_with_join(test_db):
    """Test filters on joined models."""
    # TODO: Implement test with filter on customers.tier
    # Expected: Only premium customers
    pass


def test_multiple_metrics(test_db):
    """Test querying multiple metrics together."""
    # TODO: Implement test with revenue + order_count + avg
    # Expected: All three metrics in same query
    pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
