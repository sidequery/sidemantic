#!/usr/bin/env python
# /// script
# dependencies = ["sidemantic[serve]", "duckdb"]
# ///
"""Example demonstrating chart generation from semantic layer queries.

Run with: uv run examples/integrations/chart_example.py
"""

from pathlib import Path

from sidemantic import SemanticLayer, load_from_directory
from sidemantic.charts import chart_to_vega, create_chart


def setup_demo_data(layer):
    """Create sample sales data."""
    layer.conn.execute("""
        CREATE TABLE sales (
            sale_id INTEGER,
            sale_date DATE,
            product_category VARCHAR,
            region VARCHAR,
            amount DECIMAL(10, 2)
        )
    """)

    layer.conn.execute("""
        INSERT INTO sales VALUES
            (1, '2024-01-15', 'Electronics', 'North', 1200.00),
            (2, '2024-01-20', 'Electronics', 'South', 800.00),
            (3, '2024-02-05', 'Clothing', 'North', 450.00),
            (4, '2024-02-10', 'Electronics', 'West', 1500.00),
            (5, '2024-02-15', 'Clothing', 'South', 600.00),
            (6, '2024-03-01', 'Electronics', 'North', 900.00),
            (7, '2024-03-10', 'Clothing', 'West', 750.00),
            (8, '2024-03-15', 'Electronics', 'South', 1100.00),
            (9, '2024-04-05', 'Clothing', 'North', 550.00),
            (10, '2024-04-20', 'Electronics', 'West', 1300.00)
    """)


def main():
    # Use the ecommerce example models
    example_dir = Path(__file__).parent.parent / "ecommerce_sql_yml" / "models"

    if not example_dir.exists():
        print(f"Error: Example models not found at {example_dir}")
        print("This example requires the ecommerce_sql_yml models")
        return

    # Load semantic layer
    layer = SemanticLayer()
    load_from_directory(layer, str(example_dir))

    # Setup demo data in the ecommerce DB
    db_path = Path(__file__).parent.parent / "ecommerce_sql_yml" / "data" / "ecommerce.db"
    if not db_path.exists():
        print(f"Error: Database not found. Run: uv run {db_path.parent}/create_db.py")
        return

    # Use the existing database
    layer = SemanticLayer(connection=f"duckdb:///{db_path.absolute()}")
    load_from_directory(layer, str(example_dir))

    print("=" * 80)
    print("Chart Generation Examples")
    print("=" * 80)
    print()

    # Example 1: Revenue trend over time
    print("Example 1: Monthly Revenue Trend (Area Chart)")
    print("-" * 40)

    sql = layer.compile(
        dimensions=["orders.created_at__month"],
        metrics=["orders.revenue"],
        order_by=["orders.created_at__month"],
    )

    result = layer.conn.execute(sql)
    data = [dict(zip([d[0] for d in result.description], row)) for row in result.fetchall()]

    chart = create_chart(
        data=data,
        title="Monthly Revenue Trend",
        chart_type="area",
    )

    vega_spec = chart_to_vega(chart)
    print("Chart type: Area")
    print(f"Data points: {len(data)}")
    print(f"Vega spec keys: {list(vega_spec.keys())}")
    print()

    # Example 2: Revenue by product category
    print("Example 2: Revenue by Category (Bar Chart)")
    print("-" * 40)

    sql = layer.compile(
        dimensions=["products.category"],
        metrics=["order_items.net_revenue"],
        order_by=["order_items.net_revenue desc"],
    )

    result = layer.conn.execute(sql)
    data = [dict(zip([d[0] for d in result.description], row)) for row in result.fetchall()]

    chart = create_chart(
        data=data,
        title="Revenue by Product Category",
        chart_type="bar",
    )

    vega_spec = chart_to_vega(chart)
    print("Chart type: Bar")
    print(f"Data points: {len(data)}")
    print(f"Vega spec keys: {list(vega_spec.keys())}")
    print()

    # Example 3: Multiple metrics over time
    print("Example 3: Revenue & Order Count Trend (Multi-Line)")
    print("-" * 40)

    sql = layer.compile(
        dimensions=["orders.created_at__month"],
        metrics=["orders.revenue", "orders.order_count"],
        order_by=["orders.created_at__month"],
    )

    result = layer.conn.execute(sql)
    data = [dict(zip([d[0] for d in result.description], row)) for row in result.fetchall()]

    chart = create_chart(
        data=data,
        title="Revenue & Orders Over Time",
        chart_type="line",
        width=700,
        height=400,
    )

    vega_spec = chart_to_vega(chart)
    print("Chart type: Line (multiple metrics)")
    print(f"Data points: {len(data)}")
    print("Metrics: revenue, order_count")
    print(f"Vega spec keys: {list(vega_spec.keys())}")
    print()

    print("=" * 80)
    print("✓ All charts generated successfully!")
    print()
    print("Key Features:")
    print("  - Smart chart type selection (area for trends, bar for categories)")
    print("  - Beautiful color palette (modern, accessible)")
    print("  - Clean typography and minimal design")
    print("  - Vega-Lite specs ready for client-side rendering")
    print("  - Can also export to PNG with chart_to_png()")
    print("=" * 80)


if __name__ == "__main__":
    main()
