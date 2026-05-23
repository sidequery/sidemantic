# Basic Model Native Fixture

Purpose:

- Proves a versioned native YAML project loads.
- Proves the same fixture can be consumed by Python and Rust tests.
- Proves a simple grouped metric query compiles.
- Provides DuckDB seed data and expected result rows for execution checks.

Query:

- Metric: `orders.total_revenue`
- Dimension: `orders.status`
- Order: `orders.status`
