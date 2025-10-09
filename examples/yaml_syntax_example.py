#!/usr/bin/env python
# /// script
# dependencies = ["sidemantic", "duckdb", "pandas"]
# ///
"""Complete example using YAML syntax for semantic models.

This demonstrates:
- Defining semantic models in YAML
- Loading YAML files with SemanticLayer.from_yaml()
- Querying the semantic layer with compile()
- Running compiled SQL against DuckDB

Run with: uv run examples/yaml_syntax_example.py
"""

from pathlib import Path

from sidemantic import SemanticLayer


def setup_data(conn):
    """Create sample tables with data."""
    conn.execute("""
        CREATE TABLE sales_data (
            sale_id INTEGER,
            store_id INTEGER,
            category VARCHAR,
            sale_date DATE,
            amount DECIMAL(10, 2)
        )
    """)

    conn.execute("""
        INSERT INTO sales_data VALUES
            (1, 1, 'Electronics', '2024-01-10', 899.99),
            (2, 1, 'Electronics', '2024-01-15', 1299.99),
            (3, 1, 'Home', '2024-01-20', 249.99),
            (4, 2, 'Electronics', '2024-02-05', 599.99),
            (5, 2, 'Home', '2024-02-10', 399.99),
            (6, 2, 'Home', '2024-02-15', 149.99),
            (7, 3, 'Electronics', '2024-02-20', 799.99),
            (8, 3, 'Clothing', '2024-02-25', 89.99)
    """)

    conn.execute("""
        CREATE TABLE store_data (
            store_id INTEGER,
            name VARCHAR,
            city VARCHAR,
            state VARCHAR
        )
    """)

    conn.execute("""
        INSERT INTO store_data VALUES
            (1, 'Downtown Store', 'Seattle', 'WA'),
            (2, 'Westside Store', 'Portland', 'OR'),
            (3, 'Central Store', 'San Francisco', 'CA')
    """)


def main():
    # Load semantic models from YAML
    yaml_file = Path(__file__).parent / "yaml_syntax_example.yml"
    layer = SemanticLayer.from_yaml(yaml_file)

    # Setup sample data
    setup_data(layer.conn)

    print("=" * 80)
    print("YAML Syntax Example - Define semantic models in YAML")
    print("=" * 80)
    print()

    # Example 1: Simple metric query
    print("Example 1: Total revenue")
    print("-" * 40)
    sql = layer.compile(metrics=["revenue"])
    print(f"Compiled SQL:\n{sql}\n")
    result = layer.conn.execute(sql).fetchdf()
    print(result)
    print()

    # Example 2: Metric with dimension
    print("Example 2: Sales by product category")
    print("-" * 40)
    sql = layer.compile(metrics=["sales.total_sales", "sales.sale_count"], dimensions=["sales.product_category"])
    print(f"Compiled SQL:\n{sql}\n")
    result = layer.conn.execute(sql).fetchdf()
    print(result)
    print()

    # Example 3: Time-based query
    print("Example 3: Daily sales trend")
    print("-" * 40)
    sql = layer.compile(metrics=["sales.total_sales"], dimensions=["sales.sale_date"], order_by=["sales.sale_date"])
    print(f"Compiled SQL:\n{sql}\n")
    result = layer.conn.execute(sql).fetchdf()
    print(result)
    print()

    # Example 4: Cross-model query (automatic join)
    print("Example 4: Sales by store location (with join)")
    print("-" * 40)
    sql = layer.compile(
        metrics=["sales.total_sales", "sales.sale_count"],
        dimensions=["stores.city", "stores.state"],
        order_by=["sales.total_sales DESC"],
    )
    print(f"Compiled SQL:\n{sql}\n")
    result = layer.conn.execute(sql).fetchdf()
    print(result)
    print()

    # Example 5: Query with filter
    print("Example 5: Electronics sales in CA stores")
    print("-" * 40)
    sql = layer.compile(
        metrics=["sales.total_sales", "sales.sale_count"],
        dimensions=["stores.state"],
        filters=["sales.product_category = 'Electronics'", "stores.state = 'CA'"],
    )
    print(f"Compiled SQL:\n{sql}\n")
    result = layer.conn.execute(sql).fetchdf()
    print(result)
    print()

    # Example 6: Multiple dimensions and metrics
    print("Example 6: Sales breakdown by category and location")
    print("-" * 40)
    sql = layer.compile(
        metrics=["revenue", "sales.sale_count"],
        dimensions=["sales.product_category", "stores.state"],
        order_by=["revenue DESC"],
    )
    print(f"Compiled SQL:\n{sql}\n")
    result = layer.conn.execute(sql).fetchdf()
    print(result)
    print()

    print("=" * 80)
    print("Key Takeaways:")
    print("-" * 80)
    print("✓ Define semantic models in clean, readable YAML")
    print("✓ Load with SemanticLayer.from_yaml()")
    print("✓ Use compile() to generate SQL from metrics and dimensions")
    print("✓ Automatic joins when querying across models")
    print("✓ Support for simple, derived, and ratio metrics")
    print("✓ Time dimensions with granularity (day, week, month, etc.)")
    print("=" * 80)


if __name__ == "__main__":
    main()
