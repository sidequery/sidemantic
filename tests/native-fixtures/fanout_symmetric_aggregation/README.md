# Fanout Symmetric Aggregation

Valid native fixture that proves Rust uses symmetric aggregation when a metric from the one side of a relationship is grouped by a field from the many side.

The expected result verifies each customer credit limit is counted once per order status, even when a customer has multiple orders with the same status.
