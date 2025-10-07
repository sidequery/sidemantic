#!/usr/bin/env python3
"""
Multi-Format Semantic Layer Demo

This demo shows how sidemantic can unify metrics defined in different formats:
- Customers: Defined in Cube format
- Products: Defined in Hex format
- Orders: Defined in LookML format (with relationships to both)

The demo queries across all three formats seamlessly.
"""

from pathlib import Path

from sidemantic import SemanticLayer, load_from_directory

# Database path
BASE_DIR = Path(__file__).parent
DB_PATH = f"duckdb:///{BASE_DIR / 'data' / 'ecommerce.db'}"


def print_section(title):
    """Print a formatted section header."""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80 + "\n")


def main():
    print_section("Multi-Format Semantic Layer Demo")

    # Load semantic layer from different formats
    print("Loading semantic models from different formats...")
    print("  • Customers (Cube format)")
    print("  • Products (Hex format)")
    print("  • Orders (LookML format)")

    # Create semantic layer and load all formats from directory
    layer = SemanticLayer(connection=DB_PATH)
    load_from_directory(layer, BASE_DIR)  # That's it!

    print("\n✓ All formats loaded successfully!")
    print("✓ Cross-format relationships established!\n")

    # Query 1: Order metrics from LookML
    print_section("Query 1: Basic Order Metrics (LookML)")

    print("SQL Query:")
    print("""
    SELECT
        orders.order_count,
        orders.total_revenue,
        orders.avg_order_value
    FROM orders
    """)

    result = layer.sql("""
        SELECT
            orders.order_count,
            orders.total_revenue,
            orders.avg_order_value
        FROM orders
    """)

    print("\nResult:")
    print(result.fetchdf())

    # Query 2: Revenue metrics with customer region (Cube + LookML)
    print_section("Query 2: Revenue by Customer Region")
    print("Joining LookML orders with Cube customers using programmatic API")

    print("\nProgrammatic query:")
    print("""
    layer.query(
        metrics=["orders.total_revenue", "orders.order_count"],
        dimensions=["customers.region"]
    )
    """)

    result = layer.query(metrics=["orders.total_revenue", "orders.order_count"], dimensions=["customers.region"])

    print("\nResult:")
    df = result.fetchdf()
    print(df)

    # Query 3: Product metrics with order data (Hex + LookML)
    print_section("Query 3: Product Performance Analysis")
    print("Joining LookML orders with Hex products")

    print("\nSQL Query:")
    print("""
    SELECT
        products.category,
        products.avg_price,
        orders.total_quantity,
        orders.total_revenue
    FROM orders
    """)

    result = layer.sql("""
        SELECT
            products.category,
            products.avg_price,
            orders.total_quantity,
            orders.total_revenue
        FROM orders
    """)

    print("\nResult:")
    df = result.fetchdf()
    print(df)

    # Query 4: Complex cross-format query (all three formats)
    print_section("Query 4: Complete Cross-Format Analysis")
    print("Combining metrics from Cube, Hex, and LookML")

    print("\nSQL Query:")
    print("""
    SELECT
        customers.region,
        products.category,
        orders.total_revenue,
        orders.order_count,
        orders.avg_order_value
    FROM orders
    WHERE orders.status = 'completed'
    """)

    result = layer.sql("""
        SELECT
            customers.region,
            products.category,
            orders.total_revenue,
            orders.order_count,
            orders.avg_order_value
        FROM orders
        WHERE orders.status = 'completed'
    """)

    print("\nResult:")
    df = result.fetchdf()
    print(df)

    # Query 5: Using Cube segment
    print_section("Query 5: Using Cube Segment Filter")
    print("Filtering to North region customers (segment from Cube)")

    result = layer.query(
        metrics=["orders.total_revenue", "orders.order_count"],
        dimensions=["customers.region"],
        segments=["customers.north_region"],
    )

    print("\nResult:")
    df = result.fetchdf()
    print(df)

    # Show the models loaded
    print_section("Loaded Models Summary")
    print(f"Total models loaded: {len(layer.graph.models)}")
    for model_name, model in layer.graph.models.items():
        format_origin = "Cube" if model_name == "customers" else "Hex" if model_name == "products" else "LookML"
        print(f"\n  {model_name} ({format_origin})")
        print(f"    Dimensions: {len(model.dimensions)}")
        print(f"    Metrics: {len(model.metrics)}")
        print(f"    Relationships: {len(model.relationships)}")

    print_section("Demo Complete!")
    print("This demo showed:")
    print("  ✓ Loading metrics from Cube, Hex, and LookML formats")
    print("  ✓ Querying across different semantic layer formats")
    print("  ✓ Automatic joins between models from different formats")
    print("  ✓ Using segments defined in one format with metrics from another")
    print()


if __name__ == "__main__":
    main()
