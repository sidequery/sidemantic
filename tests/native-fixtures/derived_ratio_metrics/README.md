# Derived And Ratio Metrics Native Fixture

Purpose:

- Proves model-local derived metrics compile.
- Proves ratio metrics compile.
- Proves Python execution returns expected rows.
- Gives Rust a shared compile fixture for derived and ratio metric dependencies.

Query:

- Metrics: `orders.net_revenue`, `orders.average_order_value`
- Dimension: `orders.status`
- Order: `orders.status`
