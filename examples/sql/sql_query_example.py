"""Example demonstrating SQL query interface for the semantic layer.

This shows how users can write familiar SQL queries that get automatically
rewritten to use the semantic layer's metrics and dimensions.
"""

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_layer import SemanticLayer


def setup_data(conn):
    """Create sample tables with data."""
    # Create orders table
    conn.execute("""
        CREATE TABLE orders (
            id INTEGER,
            customer_id INTEGER,
            status VARCHAR,
            order_date DATE,
            amount DECIMAL(10, 2)
        )
    """)

    conn.execute("""
        INSERT INTO orders VALUES
            (1, 1, 'completed', '2024-01-01', 100.00),
            (2, 1, 'completed', '2024-01-15', 150.00),
            (3, 2, 'pending', '2024-01-20', 200.00),
            (4, 2, 'completed', '2024-02-01', 300.00),
            (5, 3, 'completed', '2024-02-10', 250.00),
            (6, 3, 'cancelled', '2024-02-15', 50.00)
    """)

    # Create customers table
    conn.execute("""
        CREATE TABLE customers (
            id INTEGER,
            name VARCHAR,
            region VARCHAR,
            tier VARCHAR
        )
    """)

    conn.execute("""
        INSERT INTO customers VALUES
            (1, 'Alice', 'US', 'premium'),
            (2, 'Bob', 'EU', 'standard'),
            (3, 'Charlie', 'US', 'premium')
    """)


def main():
    # Initialize semantic layer
    layer = SemanticLayer()

    # Define orders model
    orders = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="order_date", type="time", sql="order_date", granularity="day"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="order_count", agg="count"),
            Metric(name="avg_order_value", agg="avg", sql="amount"),
        ],
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
    )

    # Define customers model
    customers = Model(
        name="customers",
        table="customers",
        primary_key="id",
        dimensions=[
            Dimension(name="name", type="categorical", sql="name"),
            Dimension(name="region", type="categorical", sql="region"),
            Dimension(name="tier", type="categorical", sql="tier"),
        ],
        metrics=[Metric(name="customer_count", agg="count")],
        relationships=[Relationship(name="orders", type="one_to_many", foreign_key="customer_id")],
    )

    layer.add_model(orders)
    layer.add_model(customers)

    # Setup sample data
    setup_data(layer.conn)

    print("=" * 80)
    print("SQL Query Interface Example")
    print("=" * 80)
    print()

    # Example 1: Simple metric query
    print("1. Total revenue across all orders:")
    print("-" * 40)
    sql1 = "SELECT orders.revenue FROM orders"
    print(f"SQL: {sql1}")
    result1 = layer.sql(sql1)
    print(result1.fetchdf())
    print()

    # Example 2: Metric with dimension
    print("2. Revenue by order status:")
    print("-" * 40)
    sql2 = "SELECT orders.revenue, orders.status FROM orders"
    print(f"SQL: {sql2}")
    result2 = layer.sql(sql2)
    print(result2.fetchdf())
    print()

    # Example 3: Multiple metrics with dimension
    print("3. Multiple metrics by status:")
    print("-" * 40)
    sql3 = "SELECT orders.revenue, orders.order_count, orders.avg_order_value, orders.status FROM orders"
    print(f"SQL: {sql3}")
    result3 = layer.sql(sql3)
    print(result3.fetchdf())
    print()

    # Example 4: Query with WHERE filter
    print("4. Revenue for completed orders only:")
    print("-" * 40)
    sql4 = "SELECT orders.revenue FROM orders WHERE orders.status = 'completed'"
    print(f"SQL: {sql4}")
    result4 = layer.sql(sql4)
    print(result4.fetchdf())
    print()

    # Example 5: Multiple filters with AND
    print("5. Completed orders in 2024-01:")
    print("-" * 40)
    sql5 = """
        SELECT orders.revenue, orders.order_count
        FROM orders
        WHERE orders.status = 'completed'
        AND orders.order_date >= '2024-01-01'
        AND orders.order_date < '2024-02-01'
    """
    print(f"SQL: {sql5.strip()}")
    result5 = layer.sql(sql5)
    print(result5.fetchdf())
    print()

    # Example 6: Cross-model query (automatic join)
    print("6. Revenue by customer region (automatic join):")
    print("-" * 40)
    sql6 = "SELECT orders.revenue, customers.region FROM orders"
    print(f"SQL: {sql6}")
    result6 = layer.sql(sql6)
    print(result6.fetchdf())
    print()

    # Example 7: Filter on joined table
    print("7. Revenue for US customers only:")
    print("-" * 40)
    sql7 = """
        SELECT orders.revenue, customers.region
        FROM orders
        WHERE customers.region = 'US'
    """
    print(f"SQL: {sql7.strip()}")
    result7 = layer.sql(sql7)
    print(result7.fetchdf())
    print()

    # Example 8: Multiple dimensions from different models
    print("8. Revenue by status and customer tier:")
    print("-" * 40)
    sql8 = "SELECT orders.revenue, orders.status, customers.tier FROM orders"
    print(f"SQL: {sql8}")
    result8 = layer.sql(sql8)
    print(result8.fetchdf())
    print()

    # Example 9: ORDER BY
    print("9. Top regions by revenue (with ORDER BY):")
    print("-" * 40)
    sql9 = "SELECT orders.revenue, customers.region FROM orders ORDER BY orders.revenue DESC"
    print(f"SQL: {sql9}")
    result9 = layer.sql(sql9)
    print(result9.fetchdf())
    print()

    # Example 10: LIMIT
    print("10. Top 2 regions by revenue (with LIMIT):")
    print("-" * 40)
    sql10 = "SELECT orders.revenue, customers.region FROM orders ORDER BY orders.revenue DESC LIMIT 2"
    print(f"SQL: {sql10}")
    result10 = layer.sql(sql10)
    print(result10.fetchdf())
    print()

    # Example 11: Dimension-only query
    print("11. List unique order statuses:")
    print("-" * 40)
    sql11 = "SELECT orders.status FROM orders"
    print(f"SQL: {sql11}")
    result11 = layer.sql(sql11)
    print(result11.fetchdf())
    print()

    print("=" * 80)
    print("Key Benefits:")
    print("-" * 80)
    print("Write familiar SQL - no need to learn a new query language")
    print("Metrics are automatically aggregated correctly")
    print("Joins happen automatically when you reference multiple models")
    print("All semantic layer features (symmetric aggregates, etc.) work transparently")
    print("SQL is rewritten to use optimized semantic layer queries")
    print("=" * 80)


if __name__ == "__main__":
    main()
