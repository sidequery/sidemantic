"""Test derived metrics with formula parsing.

Derived metrics combine other metrics using formulas like:
revenue_per_order = total_revenue / total_orders
"""

import duckdb
import pytest


@pytest.fixture
def orders_db():
    """Create test database with orders."""
    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE orders (
            order_id INTEGER,
            customer_id INTEGER,
            order_amount DECIMAL(10, 2),
            created_at DATE
        )
    """)

    conn.execute("""
        INSERT INTO orders VALUES
            (1, 101, 100.00, '2024-01-15'),
            (2, 102, 200.00, '2024-01-20'),
            (3, 101, 150.00, '2024-02-01'),
            (4, 103, 300.00, '2024-02-10')
    """)

    return conn


def test_simple_derived_metric(orders_db):
    """Test basic derived metric formula."""
    # TODO: Implement test for revenue_per_order = revenue / order_count
    # Expected: Correct division with formula replacement
    pass


def test_derived_metric_by_dimension(orders_db):
    """Test derived metric grouped by dimension."""
    # TODO: Implement test for revenue_per_order by month
    # Expected: Monthly averages calculated correctly
    pass


def test_nested_derived_metrics(orders_db):
    """Test derived metrics referencing other derived metrics."""
    # TODO: Implement test for metric using another derived metric
    # Expected: Recursive formula expansion works
    pass


def test_all_metrics_together(orders_db):
    """Test querying base + derived metrics together."""
    # TODO: Implement test with revenue, orders, and revenue_per_order
    # Expected: All metrics in same result set
    pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
