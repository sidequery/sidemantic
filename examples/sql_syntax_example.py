#!/usr/bin/env python
# /// script
# dependencies = ["sidemantic", "duckdb", "pandas"]
# ///
"""Complete example using SQL syntax to query the semantic layer.

This demonstrates:
- Defining semantic models in Python
- Using familiar SQL syntax to query metrics
- Running queries against actual DuckDB data
- Automatic metric aggregation and joins

Run with: uv run examples/sql_syntax_example.py
"""

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_layer import SemanticLayer
from sidemantic.sql.query_rewriter import QueryRewriter


def setup_data(layer):
    """Create sample tables with data."""
    layer.conn.execute("""
        CREATE TABLE orders (
            order_id INTEGER,
            customer_id INTEGER,
            status VARCHAR,
            created_at DATE,
            amount DECIMAL(10, 2)
        )
    """)

    layer.conn.execute("""
        INSERT INTO orders VALUES
            (1, 101, 'completed', '2024-01-15', 250.00),
            (2, 101, 'completed', '2024-01-20', 150.00),
            (3, 102, 'pending', '2024-01-25', 300.00),
            (4, 102, 'completed', '2024-02-05', 400.00),
            (5, 103, 'completed', '2024-02-10', 200.00),
            (6, 103, 'cancelled', '2024-02-15', 75.00)
    """)

    layer.conn.execute("""
        CREATE TABLE customers (
            customer_id INTEGER,
            name VARCHAR,
            region VARCHAR,
            tier VARCHAR
        )
    """)

    layer.conn.execute("""
        INSERT INTO customers VALUES
            (101, 'Acme Corp', 'US-West', 'enterprise'),
            (102, 'TechStart Inc', 'US-East', 'growth'),
            (103, 'Global Ltd', 'EU', 'enterprise')
    """)


def main():
    # Create semantic layer
    layer = SemanticLayer()

    # Define orders model
    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="order_date", type="time", sql="created_at", granularity="day"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="order_count", agg="count"),
        ],
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
    )

    # Define customers model
    customers = Model(
        name="customers",
        table="customers",
        primary_key="customer_id",
        dimensions=[
            Dimension(name="region", type="categorical", sql="region"),
            Dimension(name="tier", type="categorical", sql="tier"),
        ],
    )

    layer.add_model(orders)
    layer.add_model(customers)

    # Setup sample data
    setup_data(layer)

    # Create query rewriter for SQL interface
    rewriter = QueryRewriter(layer.graph)

    print("=" * 80)
    print("SQL Syntax Example - Query semantic models using familiar SQL")
    print("=" * 80)
    print()

    # Example 1: Simple metric query
    print("Example 1: Total revenue")
    print("-" * 40)
    user_sql = """
        SELECT
            orders.revenue,
            orders.order_count
        FROM orders
    """
    print(f"User SQL:\n{user_sql}")
    generated_sql = rewriter.rewrite(user_sql)
    print(f"Generated SQL:\n{generated_sql}\n")
    result = layer.conn.execute(generated_sql).fetchdf()
    print(result)
    print()

    # Example 2: Revenue by status (metric + dimension)
    print("Example 2: Revenue by order status")
    print("-" * 40)
    user_sql = """
        SELECT
            orders.revenue,
            orders.order_count,
            orders.status
        FROM orders
    """
    print(f"User SQL:\n{user_sql}")
    generated_sql = rewriter.rewrite(user_sql)
    print(f"Generated SQL:\n{generated_sql}\n")
    result = layer.conn.execute(generated_sql).fetchdf()
    print(result)
    print()

    # Example 3: Revenue with filter
    print("Example 3: Completed orders only")
    print("-" * 40)
    user_sql = """
        SELECT
            orders.revenue,
            orders.order_count
        FROM orders
        WHERE orders.status = 'completed'
    """
    print(f"User SQL:\n{user_sql}")
    generated_sql = rewriter.rewrite(user_sql)
    print(f"Generated SQL:\n{generated_sql}\n")
    result = layer.conn.execute(generated_sql).fetchdf()
    print(result)
    print()

    # Example 4: Cross-model query (automatic join)
    print("Example 4: Revenue by customer region (automatic join)")
    print("-" * 40)
    user_sql = """
        SELECT
            orders.revenue,
            orders.order_count,
            customers.region
        FROM orders
    """
    print(f"User SQL:\n{user_sql}")
    generated_sql = rewriter.rewrite(user_sql)
    print(f"Generated SQL:\n{generated_sql}\n")
    result = layer.conn.execute(generated_sql).fetchdf()
    print(result)
    print()

    # Example 5: Multiple dimensions and filters
    print("Example 5: Enterprise customers by status")
    print("-" * 40)
    user_sql = """
        SELECT
            orders.revenue,
            orders.status,
            customers.tier
        FROM orders
        WHERE customers.tier = 'enterprise'
        ORDER BY orders.revenue DESC
    """
    print(f"User SQL:\n{user_sql}")
    generated_sql = rewriter.rewrite(user_sql)
    print(f"Generated SQL:\n{generated_sql}\n")
    result = layer.conn.execute(generated_sql).fetchdf()
    print(result)
    print()

    print("=" * 80)
    print("Key Takeaways:")
    print("-" * 80)
    print("✓ Write familiar SQL - SELECT metrics FROM model_name")
    print("✓ Metrics are automatically aggregated correctly")
    print("✓ Joins happen automatically when referencing multiple models")
    print("✓ All standard SQL features: WHERE, ORDER BY, etc.")
    print("✓ QueryRewriter translates to optimized semantic layer SQL")
    print("=" * 80)


if __name__ == "__main__":
    main()
