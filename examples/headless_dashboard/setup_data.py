#!/usr/bin/env python
"""Create demo data for the declarative headless dashboard example."""

from __future__ import annotations

from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DB_PATH = DATA_DIR / "orders.db"


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(DB_PATH))
    try:
        connection.execute("DROP TABLE IF EXISTS orders")
        connection.execute("""
            CREATE TABLE orders AS
            WITH source AS (
                SELECT i::BIGINT AS id
                FROM range(50000) AS rows(i)
            ),
            shaped AS (
                SELECT
                    id + 1 AS id,
                    DATE '2024-01-01' + CAST(id % 365 AS INTEGER) AS created_at,
                    CASE id % 5
                        WHEN 0 THEN 'North'
                        WHEN 1 THEN 'South'
                        WHEN 2 THEN 'West'
                        WHEN 3 THEN 'Central'
                        ELSE 'East'
                    END AS region,
                    CASE id % 4
                        WHEN 0 THEN 'Web'
                        WHEN 1 THEN 'Sales'
                        WHEN 2 THEN 'Partner'
                        ELSE 'Marketplace'
                    END AS channel,
                    CASE id % 4
                        WHEN 0 THEN 'Enterprise'
                        WHEN 1 THEN 'Strategic'
                        WHEN 2 THEN 'Mid-Market'
                        ELSE 'SMB'
                    END AS customer_tier,
                    CASE id % 5
                        WHEN 0 THEN 'Platform'
                        WHEN 1 THEN 'Analytics'
                        WHEN 2 THEN 'Services'
                        WHEN 3 THEN 'Data Cloud'
                        ELSE 'AI Apps'
                    END AS product_line,
                    ROUND(
                        (80 + (id % 900) * 1.7)
                        * (1 + (id % 5) * 0.06)
                        * (1 + (id % 4) * 0.08)
                        * (1 + (id % 6) * 0.03),
                        2
                    ) AS amount
                FROM source
            )
            SELECT
                id,
                created_at,
                region,
                channel,
                customer_tier,
                product_line,
                ROUND(amount * (0.28 + (id % 7) * 0.015), 2) AS gross_margin,
                amount
            FROM shaped
        """)
        count = connection.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    finally:
        connection.close()
    print(f"Wrote {count:,} orders to {DB_PATH}")


if __name__ == "__main__":
    main()
