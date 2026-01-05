#!/usr/bin/env python3
# /// script
# dependencies = ["sidemantic"]
# ///
"""Cube Demo: Export sidemantic models to Cube and run Playground in Docker.

This demo:
1. Loads a sidemantic YAML definition (e-commerce model with joins)
2. Exports to Cube YAML format
3. Generates a complete Cube project with DuckDB
4. Builds and runs Cube Playground in Docker

Prerequisites:
- Docker installed and running

Usage:
    git clone https://github.com/sidequery/sidemantic && cd sidemantic
    uv run examples/cube_demo/run_demo.py
"""

import shutil
import subprocess
import sys
from pathlib import Path

# Add project root to path for local development
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

IMAGE_NAME = "sidemantic-cube-demo"

# Sample data for the e-commerce demo - generated with DuckDB
SAMPLE_DATA_SQL = """
-- Create customers table (200 customers across regions and tiers)
CREATE TABLE customers AS
WITH first_names AS (
    SELECT unnest(['James','Mary','John','Patricia','Robert','Jennifer','Michael','Linda',
        'William','Elizabeth','David','Barbara','Richard','Susan','Joseph','Jessica',
        'Thomas','Sarah','Charles','Karen','Christopher','Nancy','Daniel','Lisa',
        'Matthew','Betty','Anthony','Margaret','Mark','Sandra','Donald','Ashley',
        'Steven','Kimberly','Paul','Emily','Andrew','Donna','Joshua','Michelle']) as first_name
),
last_names AS (
    SELECT unnest(['Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis',
        'Rodriguez','Martinez','Hernandez','Lopez','Gonzalez','Wilson','Anderson','Thomas',
        'Taylor','Moore','Jackson','Martin','Lee','Perez','Thompson','White']) as last_name
),
regions AS (SELECT unnest(['Northeast','Southeast','Midwest','Southwest','West Coast','Mountain','Pacific Northwest','Great Plains']) as region),
tiers AS (SELECT unnest(['Enterprise','Business','Professional','Starter']) as tier),
name_combos AS (
    SELECT first_name, last_name, row_number() OVER () as rn
    FROM first_names CROSS JOIN last_names
    ORDER BY random()
    LIMIT 200
)
SELECT
    rn as customer_id,
    first_name || ' ' || last_name as name,
    lower(first_name) || '.' || lower(last_name) || rn || '@company.com' as email,
    (SELECT region FROM regions ORDER BY random() LIMIT 1) as region,
    (SELECT tier FROM tiers ORDER BY random() LIMIT 1) as tier
FROM name_combos;

-- Create products table (50 products across categories with subcategories)
CREATE TABLE products AS
SELECT
    100 + row_number() OVER () as product_id,
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
    ('BenQ ScreenBar Plus', 'Furniture', 'Accessories', 129.99),
    ('Rain mStand', 'Furniture', 'Accessories', 49.99),
    ('Twelve South BookArc', 'Furniture', 'Accessories', 59.99),
    ('Grovemade Desk Shelf', 'Furniture', 'Accessories', 199.99),
    ('Ugmonk Gather', 'Furniture', 'Accessories', 249.99),
    ('Notion Team Plan', 'Software', 'Productivity', 96.00),
    ('Figma Professional', 'Software', 'Design', 144.00),
    ('GitHub Enterprise', 'Software', 'Development', 252.00),
    ('Slack Business+', 'Software', 'Communication', 150.00),
    ('Zoom Business', 'Software', 'Communication', 199.99),
    ('1Password Teams', 'Software', 'Security', 95.88),
    ('Adobe Creative Cloud', 'Software', 'Design', 659.88),
    ('JetBrains All Products', 'Software', 'Development', 289.00),
    ('Linear Standard', 'Software', 'Productivity', 96.00),
    ('Loom Business', 'Software', 'Communication', 150.00),
    ('Moleskine Pro XL', 'Office Supplies', 'Notebooks', 32.99),
    ('Leuchtturm1917 A5', 'Office Supplies', 'Notebooks', 24.99),
    ('Baron Fig Confidant', 'Office Supplies', 'Notebooks', 18.99),
    ('Pilot G2 12-Pack', 'Office Supplies', 'Writing', 14.99),
    ('LAMY Safari', 'Office Supplies', 'Writing', 29.99)
) AS t(name, category, subcategory, price);

-- Create orders table (5000+ orders over 18 months with realistic patterns)
CREATE TABLE orders AS
WITH date_range AS (
    SELECT unnest(generate_series(DATE '2023-01-01', DATE '2024-06-30', INTERVAL '1 day'))::DATE as order_date
),
daily_volume AS (
    SELECT
        order_date,
        -- More orders on weekdays, seasonal patterns
        CASE
            WHEN extract(dow from order_date) IN (0, 6) THEN 2  -- weekends
            WHEN extract(month from order_date) IN (11, 12) THEN 12  -- holiday season
            WHEN extract(month from order_date) IN (1, 7) THEN 5  -- slow months
            ELSE 8  -- normal
        END as base_orders,
        -- Growth trend over time
        1.0 + (extract(epoch from order_date) - extract(epoch from DATE '2023-01-01')) / (365 * 24 * 60 * 60) * 0.5 as growth_factor
    FROM date_range
),
order_base AS (
    SELECT
        row_number() OVER () as order_id,
        order_date,
        1 + (random() * 199)::int as customer_id,
        101 + (random() * 49)::int as product_id,
        CASE
            WHEN random() < 0.6 THEN 1
            WHEN random() < 0.85 THEN 2
            WHEN random() < 0.95 THEN 3
            ELSE 4 + (random() * 6)::int
        END as quantity,
        CASE
            WHEN random() < 0.65 THEN 'completed'
            WHEN random() < 0.80 THEN 'shipped'
            WHEN random() < 0.90 THEN 'processing'
            WHEN random() < 0.96 THEN 'pending'
            ELSE 'cancelled'
        END as status,
        CASE
            WHEN random() < 0.4 THEN 'credit_card'
            WHEN random() < 0.7 THEN 'paypal'
            WHEN random() < 0.85 THEN 'bank_transfer'
            ELSE 'invoice'
        END as payment_method
    FROM daily_volume
    CROSS JOIN generate_series(1, base_orders::int) as orders_per_day(n)
    WHERE random() < (0.85 + random() * 0.15) * growth_factor
)
SELECT
    o.order_id,
    o.order_date,
    o.customer_id,
    o.product_id,
    o.quantity,
    round(p.price * o.quantity * (0.85 + random() * 0.25), 2) as amount,
    o.status,
    o.payment_method
FROM order_base o
JOIN products p ON o.product_id = p.product_id;
"""

# Cube.js configuration template
CUBE_CONFIG = """module.exports = {
  dbType: 'duckdb',
  apiSecret: 'sidemantic-cube-demo-secret',
  devServer: true,
  scheduledRefreshTimer: false,
  driverFactory: () => {
    return {
      type: 'duckdb',
      database: ':memory:',
      initSql: `%s`
    };
  }
};
"""


def main():
    demo_dir = Path(__file__).parent
    sidemantic_yaml = demo_dir / "sidemantic.yaml"
    cube_project_dir = demo_dir / "cube_project"

    print("=" * 60)
    print("  Sidemantic to Cube Demo")
    print("=" * 60)

    # Step 1: Load sidemantic YAML
    print("\n[1/5] Loading sidemantic.yaml...")
    from sidemantic.adapters.sidemantic import SidemanticAdapter

    adapter = SidemanticAdapter()
    graph = adapter.parse(sidemantic_yaml)
    print(f"      Loaded {len(graph.models)} model(s)")
    for model_name, model in graph.models.items():
        dims = len(model.dimensions)
        metrics = len(model.metrics)
        rels = len(model.relationships) if model.relationships else 0
        print(f"      - {model_name}: {dims} dimensions, {metrics} metrics, {rels} relationships")

    # Step 2: Export to Cube
    print("\n[2/5] Exporting to Cube project...")
    from sidemantic.adapters.cube import CubeAdapter

    # Clean previous output
    if cube_project_dir.exists():
        shutil.rmtree(cube_project_dir)
    cube_project_dir.mkdir(parents=True)

    # Create model directory
    model_dir = cube_project_dir / "model"
    model_dir.mkdir()

    # Export Cube YAML to model directory
    cube_adapter = CubeAdapter()
    cube_adapter.export(graph, model_dir / "ecommerce.yaml")

    # Generate cube.js config with sample data
    config_content = CUBE_CONFIG % SAMPLE_DATA_SQL.replace("`", "\\`")
    (cube_project_dir / "cube.js").write_text(config_content)

    # List generated files
    print(f"      Generated Cube project at: {cube_project_dir}")
    for file in sorted(cube_project_dir.rglob("*")):
        if file.is_file():
            rel_path = file.relative_to(cube_project_dir)
            print(f"      - {rel_path}")

    # Step 3: Check Docker
    print("\n[3/5] Checking Docker...")
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0:
            print("      ERROR: Docker is not running. Please start Docker and try again.")
            sys.exit(1)
        print("      Docker is running")
    except FileNotFoundError:
        print("      ERROR: Docker is not installed. Please install Docker and try again.")
        sys.exit(1)
    except subprocess.TimeoutExpired:
        print("      ERROR: Docker timed out. Please ensure Docker is running.")
        sys.exit(1)

    # Step 4: Build Docker image
    print(f"\n[4/5] Building Docker image '{IMAGE_NAME}'...")
    result = subprocess.run(
        ["docker", "build", "-t", IMAGE_NAME, str(demo_dir)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"      ERROR: Failed to build Docker image:\n{result.stderr}")
        sys.exit(1)
    print("      Image built successfully")

    # Step 5: Run Cube in Docker
    print("\n[5/5] Starting Cube Playground in Docker...")
    print("      Port: http://localhost:4789")
    print()
    print("=" * 60)
    print("  Open http://localhost:4789 in your browser")
    print("  Press Ctrl+C to stop")
    print("=" * 60)
    print()

    try:
        subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "-p",
                "4789:4000",
                "-p",
                "15789:15432",
                "-v",
                f"{cube_project_dir.absolute()}:/cube/conf",
                "-e",
                "CUBEJS_DEV_MODE=true",
                IMAGE_NAME,
            ],
        )
    except KeyboardInterrupt:
        print("\n\nStopping Cube...")


if __name__ == "__main__":
    main()
