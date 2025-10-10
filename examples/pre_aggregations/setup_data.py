#!/usr/bin/env -S uv run
# /// script
# dependencies = ["duckdb"]
# ///
"""
Setup sample data for pre-aggregations example.

This creates a DuckDB database with sample orders data spanning multiple months.
"""

from pathlib import Path

import duckdb


def create_sample_data():
    """Create sample orders data for testing pre-aggregations."""

    # Create data directory
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)

    db_path = data_dir / "warehouse.db"
    conn = duckdb.connect(str(db_path))

    # Create orders table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id VARCHAR PRIMARY KEY,
            customer_id VARCHAR,
            status VARCHAR,
            amount DECIMAL(10,2),
            order_date DATE,
            region VARCHAR
        )
    """)

    # Generate sample data - 10,000 orders over 90 days
    print("Generating 10,000 sample orders...")

    conn.execute("""
        INSERT OR REPLACE INTO orders
        SELECT
            'ORD-' || LPAD(CAST(seq AS VARCHAR), 6, '0') as order_id,
            'CUST-' || LPAD(CAST((seq % 1000) AS VARCHAR), 4, '0') as customer_id,
            status,
            ROUND((random() * 500 + 10)::DECIMAL, 2) as amount,
            (CURRENT_DATE - INTERVAL (seq % 90) DAY)::DATE as order_date,
            region
        FROM (
            SELECT
                generate_series as seq,
                ['pending', 'completed', 'cancelled'][1 + (CAST(random() * 2.9 AS INTEGER))] as status,
                ['North', 'South', 'East', 'West', 'Central'][1 + (CAST(random() * 4.9 AS INTEGER))] as region
            FROM generate_series(1, 10000)
        )
    """)

    # Verify data
    result = conn.execute("SELECT COUNT(*), MIN(order_date), MAX(order_date) FROM orders").fetchone()
    print(f"Created {result[0]} orders from {result[1]} to {result[2]}")

    # Show sample stats
    print("\nSample statistics:")
    stats = conn.execute("""
        SELECT
            status,
            COUNT(*) as orders,
            COALESCE(SUM(amount), 0) as revenue
        FROM orders
        GROUP BY status
        ORDER BY status
    """).fetchall()

    for row in stats:
        status = row[0] if row[0] else "unknown"
        orders = row[1] if row[1] else 0
        revenue = row[2] if row[2] else 0
        print(f"  {status:12} {orders:6,} orders  ${revenue:,.2f} revenue")

    conn.close()
    print(f"\nDatabase created at: {db_path}")
    print("Ready to use with sidemantic!")


if __name__ == "__main__":
    create_sample_data()
