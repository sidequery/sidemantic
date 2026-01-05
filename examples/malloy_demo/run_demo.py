#!/usr/bin/env python3
# /// script
# dependencies = ["sidemantic", "duckdb", "antlr4-python3-runtime"]
# ///
"""Malloy Demo: Convert LookML to Malloy format.

This demo:
1. Loads LookML files from Looker's thelook e-commerce sample
2. Converts to Malloy format using sidemantic
3. Generates sample Parquet data

To explore the generated Malloy files:
- Install the Malloy VS Code extension
- Open examples/malloy_demo/malloy_output/thelook.malloy

Usage:
    git clone https://github.com/sidequery/sidemantic && cd sidemantic
    uv run examples/malloy_demo/run_demo.py
"""

import shutil
import sys
from pathlib import Path

# Add project root to path for local development
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Sample data SQL - simple e-commerce schema
SAMPLE_DATA_SQL = """
-- Create customers table (200 customers)
CREATE TABLE customers AS
WITH first_names AS (
    SELECT unnest(['James','Mary','John','Patricia','Robert','Jennifer','Michael','Linda',
        'William','Elizabeth','David','Barbara','Richard','Susan','Joseph','Jessica',
        'Thomas','Sarah','Charles','Karen']) as first_name
),
last_names AS (
    SELECT unnest(['Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis',
        'Rodriguez','Martinez']) as last_name
),
regions AS (SELECT unnest(['Northeast','Southeast','Midwest','Southwest','West']) as region),
tiers AS (SELECT unnest(['Enterprise','Business','Professional','Starter']) as tier),
name_combos AS (
    SELECT first_name, last_name, row_number() OVER () as rn
    FROM first_names CROSS JOIN last_names
    ORDER BY random()
    LIMIT 200
)
SELECT
    rn as id,
    first_name || ' ' || last_name as name,
    lower(first_name) || '.' || lower(last_name) || rn || '@company.com' as email,
    (SELECT region FROM regions ORDER BY random() LIMIT 1) as region,
    (SELECT tier FROM tiers ORDER BY random() LIMIT 1) as tier
FROM name_combos;

-- Create products table (50 products)
CREATE TABLE products AS
SELECT
    row_number() OVER () as id,
    name,
    category,
    subcategory,
    price
FROM (VALUES
    ('MacBook Pro 16"', 'Electronics', 'Laptops', 2499.99),
    ('MacBook Pro 14"', 'Electronics', 'Laptops', 1999.99),
    ('Dell XPS 15', 'Electronics', 'Laptops', 1799.99),
    ('ThinkPad X1 Carbon', 'Electronics', 'Laptops', 1649.99),
    ('Surface Laptop 5', 'Electronics', 'Laptops', 1299.99),
    ('LG UltraFine 5K', 'Electronics', 'Monitors', 1299.99),
    ('Dell U2723QE 4K', 'Electronics', 'Monitors', 799.99),
    ('Samsung Odyssey G7', 'Electronics', 'Monitors', 649.99),
    ('ASUS ProArt 27"', 'Electronics', 'Monitors', 549.99),
    ('BenQ PD2700U', 'Electronics', 'Monitors', 449.99),
    ('Logitech MX Master 3S', 'Electronics', 'Peripherals', 99.99),
    ('Apple Magic Keyboard', 'Electronics', 'Peripherals', 199.99),
    ('Keychron Q1 Pro', 'Electronics', 'Peripherals', 189.99),
    ('CalDigit TS4 Dock', 'Electronics', 'Peripherals', 399.99),
    ('Elgato Stream Deck', 'Electronics', 'Peripherals', 149.99),
    ('Sony WH-1000XM5', 'Electronics', 'Audio', 399.99),
    ('AirPods Pro 2', 'Electronics', 'Audio', 249.99),
    ('Bose 700', 'Electronics', 'Audio', 379.99),
    ('Shure MV7', 'Electronics', 'Audio', 249.99),
    ('Blue Yeti X', 'Electronics', 'Audio', 169.99),
    ('Herman Miller Aeron', 'Furniture', 'Chairs', 1395.99),
    ('Steelcase Leap V2', 'Furniture', 'Chairs', 1199.99),
    ('Secretlab Titan', 'Furniture', 'Chairs', 449.99),
    ('Humanscale Freedom', 'Furniture', 'Chairs', 1299.99),
    ('Branch Ergonomic', 'Furniture', 'Chairs', 349.99),
    ('Uplift V2 Standing Desk', 'Furniture', 'Desks', 599.99),
    ('Jarvis Bamboo Desk', 'Furniture', 'Desks', 699.99),
    ('IKEA BEKANT', 'Furniture', 'Desks', 349.99),
    ('Fully Remi Desk', 'Furniture', 'Desks', 445.99),
    ('Vari Electric Desk', 'Furniture', 'Desks', 695.99),
    ('Notion Team Plan', 'Software', 'Productivity', 96.00),
    ('Figma Professional', 'Software', 'Design', 144.00),
    ('GitHub Enterprise', 'Software', 'Development', 252.00),
    ('Slack Business+', 'Software', 'Communication', 150.00),
    ('Zoom Business', 'Software', 'Communication', 199.99),
    ('1Password Teams', 'Software', 'Security', 95.88),
    ('Adobe Creative Cloud', 'Software', 'Design', 659.88),
    ('JetBrains All Products', 'Software', 'Development', 289.00),
    ('Linear Standard', 'Software', 'Productivity', 96.00),
    ('Loom Business', 'Software', 'Communication', 150.00)
) AS t(name, category, subcategory, price);

-- Create orders table (5000 orders)
CREATE TABLE orders AS
SELECT
    row_number() OVER () as id,
    1 + (random() * 199)::int as customer_id,
    1 + (random() * 39)::int as product_id,
    1 + (random() * 5)::int as quantity,
    round((50 + random() * 2000)::numeric, 2) as amount,
    (ARRAY['completed','shipped','processing','pending','cancelled'])[1 + (random() * 4)::int] as status,
    DATE '2023-01-01' + (random() * 730)::int * INTERVAL '1 day' as created_at
FROM generate_series(1, 5000);
"""


def main():
    demo_dir = Path(__file__).parent
    lookml_input = demo_dir / "lookml_input"
    malloy_output = demo_dir / "malloy_output"

    print("=" * 60)
    print("  Sidemantic Malloy Demo: LookML to Malloy")
    print("=" * 60)

    # Step 1: Load LookML files
    print("\n[1/4] Loading LookML files...")
    from sidemantic.adapters.lookml import LookMLAdapter

    adapter = LookMLAdapter()
    graph = adapter.parse(lookml_input)
    print(f"      Loaded {len(graph.models)} model(s)")
    for model_name, model in graph.models.items():
        dims = len(model.dimensions)
        metrics = len(model.metrics)
        print(f"      - {model_name}: {dims} dimensions, {metrics} metrics")

    # Step 2: Update table references to Parquet paths
    print("\n[2/4] Updating table references for Parquet...")
    for model in graph.models.values():
        model.table = f"data/{model.name}.parquet"
        print(f"      - {model.name} -> data/{model.name}.parquet")

    # Step 3: Export to Malloy
    print("\n[3/4] Exporting to Malloy format...")
    from sidemantic.adapters.malloy import MalloyAdapter

    # Clean previous output
    if malloy_output.exists():
        shutil.rmtree(malloy_output)
    malloy_output.mkdir(parents=True)
    (malloy_output / "data").mkdir()

    malloy_adapter = MalloyAdapter()
    malloy_file = malloy_output / "thelook.malloy"
    malloy_adapter.export(graph, malloy_file)

    # Add sample queries and dashboard
    sample_queries = """
// ============================================================
// Sample Queries - Click "Run" above any query to execute
// ============================================================

// Orders by status
run: orders -> {
  group_by: status
  aggregate: order_count
}

// Revenue by month
# bar_chart
run: orders -> {
  group_by: created_month
  aggregate: total_revenue
}

// Top 10 orders by revenue
run: orders -> {
  group_by: id, status
  aggregate: total_revenue
  order_by: total_revenue desc
  limit: 10
}

// Customers by region
# bar_chart
run: customers -> {
  group_by: region
  aggregate: customer_count
}

// Products by category
# bar_chart
run: products -> {
  group_by: category
  aggregate: product_count
}

// ============================================================
// Dashboard Example - Shows multiple visualizations together
// ============================================================

# dashboard
run: orders -> {
  aggregate:
    order_count
    total_revenue

  # bar_chart
  nest: by_status is {
    group_by: status
    aggregate: order_count
  }

  # bar_chart
  nest: by_month is {
    group_by: created_month
    aggregate: total_revenue
  }
}
"""
    with open(malloy_file, "a") as f:
        f.write(sample_queries)

    print(f"      Generated {malloy_file.name} (with sample queries)")

    # Step 4: Generate sample Parquet data
    print("\n[4/4] Generating sample Parquet data...")
    import duckdb

    conn = duckdb.connect(":memory:")
    conn.execute(SAMPLE_DATA_SQL)

    for table in ["customers", "products", "orders"]:
        parquet_path = malloy_output / "data" / f"{table}.parquet"
        conn.execute(f"COPY {table} TO '{parquet_path}' (FORMAT PARQUET)")
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"      - {table}.parquet ({row_count} rows)")

    conn.close()

    print()
    print("=" * 60)
    print("  Done! Generated files in: malloy_output/")
    print()
    print("  To explore interactively:")
    print("  1. Install the Malloy VS Code extension")
    print("  2. Open malloy_output/thelook.malloy")
    print("  3. Click 'Run' on any query")
    print("=" * 60)


if __name__ == "__main__":
    main()
