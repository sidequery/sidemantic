"""Example showing the Metric API."""

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_layer import SemanticLayer

# Create semantic layer
layer = SemanticLayer()

# Define model with metrics
orders = Model(
    name="orders",
    table="orders",
    primary_key="id",
    dimensions=[
        Dimension(name="status", type="categorical", sql="status"),
        Dimension(name="order_date", type="time", sql="order_date", granularity="day"),
    ],
    metrics=[
        # Simple aggregations - just use agg
        Metric(name="revenue", agg="sum", sql="amount"),
        Metric(name="order_count", agg="count"),
        Metric(name="avg_order_value", agg="avg", sql="amount"),
        # Complex metrics - use type
        Metric(name="margin_pct", type="ratio", numerator="revenue", denominator="cost"),
        Metric(name="profit", type="derived", sql="revenue - cost"),
        Metric(name="running_total", type="cumulative", sql="revenue", window="7 days"),
    ],
)

layer.add_model(orders)

# Setup sample data
layer.conn.execute("""
    CREATE TABLE orders (
        id INTEGER,
        status VARCHAR,
        order_date DATE,
        amount DECIMAL,
        cost DECIMAL
    )
""")

layer.conn.execute("""
    INSERT INTO orders VALUES
        (1, 'completed', '2024-01-01', 100, 70),
        (2, 'completed', '2024-01-02', 200, 140),
        (3, 'pending', '2024-01-03', 150, 100)
""")

# Query using measures
print("Simple aggregation:")
result = layer.query(metrics=["orders.revenue", "orders.order_count"])
print(result.fetchdf())
print()

print("By dimension:")
result = layer.query(metrics=["orders.revenue"], dimensions=["orders.status"])
print(result.fetchdf())
print()

print("SQL query interface (auto-rewrites):")
result = layer.sql("SELECT revenue, status FROM orders")
print(result.fetchdf())
