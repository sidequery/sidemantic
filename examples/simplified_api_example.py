"""Example showing the simplified Measure API after consolidation.

No more separate Metric class - everything is just Measure!
"""

from sidemantic.core.measure import Measure

from sidemantic.core.dimension import Dimension
from sidemantic.core.model import Model
from sidemantic.core.semantic_layer import SemanticLayer

# Create semantic layer
layer = SemanticLayer()

# Define model with measures
orders = Model(
    name="orders",
    table="orders",
    primary_key="id",
    dimensions=[
        Dimension(name="status", type="categorical", sql="status"),
        Dimension(name="order_date", type="time", sql="order_date", granularity="day"),
    ],
    measures=[
        # Simple aggregations - just use agg
        Measure(name="revenue", agg="sum", expr="amount"),
        Measure(name="order_count", agg="count"),
        Measure(name="avg_order_value", agg="avg", expr="amount"),
        # Complex measures - use type
        Measure(name="margin_pct", type="ratio", numerator="revenue", denominator="cost"),
        Measure(name="profit", type="derived", expr="revenue - cost"),
        Measure(name="running_total", type="cumulative", expr="revenue", window="7 days"),
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
print()

print("One class (Measure) for everything - simple and powerful!")
