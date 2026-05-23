# Relationships Native Fixture

Purpose:

- Proves native relationships load.
- Proves cross-model dimensions compile.
- Proves Python execution returns expected joined rows.
- Gives Rust a shared compile fixture for join path resolution.

Query:

- Metric: `orders.total_revenue`
- Dimension: `customers.country`
- Order: `customers.country`
