"""Basic example of using Sidemantic."""

from sidemantic import Dimension, Entity, Measure, Model, SemanticLayer

# Create semantic layer
sl = SemanticLayer()

# Define orders model
orders = Model(
    name="orders",
    table="public.orders",
    entities=[
        Entity(name="order", type="primary", expr="order_id"),
        Entity(name="customer", type="foreign", expr="customer_id"),
    ],
    dimensions=[
        Dimension(name="status", type="categorical"),
        Dimension(name="order_date", type="time", granularity="day", expr="created_at"),
        Dimension(name="is_high_value", type="boolean", expr="order_amount > 100"),
    ],
    measures=[
        Measure(name="order_count", agg="count"),
        Measure(name="revenue", agg="sum", expr="order_amount"),
        Measure(
            name="completed_revenue",
            agg="sum",
            expr="order_amount",
            filters=["status = 'completed'"],
        ),
    ],
)

# Define customers model
customers = Model(
    name="customers",
    table="public.customers",
    entities=[
        Entity(name="customer", type="primary", expr="customer_id"),
    ],
    dimensions=[
        Dimension(name="region", type="categorical"),
        Dimension(name="tier", type="categorical"),
    ],
)

# Add models to semantic layer
sl.add_model(orders)
sl.add_model(customers)

# Define metrics
sl.add_metric(
    Measure(
        name="total_revenue",
        type="simple",
        expr="orders.revenue",
        description="Total revenue from all orders",
    )
)

sl.add_metric(
    Measure(
        name="conversion_rate",
        type="ratio",
        numerator="orders.completed_revenue",
        denominator="orders.revenue",
        description="Percentage of revenue from completed orders",
    )
)

# Example 1: Query single model
print("=" * 80)
print("Example 1: Query single model")
print("=" * 80)
sql = sl.compile(
    metrics=["orders.revenue", "orders.order_count"],
    dimensions=["orders.status"],
)
print(sql)
print()

# Example 2: Query with time granularity
print("=" * 80)
print("Example 2: Query with time granularity")
print("=" * 80)
sql = sl.compile(
    metrics=["orders.revenue"],
    dimensions=["orders.order_date__month"],
    order_by=["orders.order_date__month"],
)
print(sql)
print()

# Example 3: Query across models (join)
print("=" * 80)
print("Example 3: Query across models (with join)")
print("=" * 80)
sql = sl.compile(
    metrics=["orders.revenue", "orders.order_count"],
    dimensions=["customers.region", "orders.status"],
    filters=["customers.tier = 'premium'"],
)
print(sql)
print()

# Example 4: Query with metric
print("=" * 80)
print("Example 4: Query with metric")
print("=" * 80)
sql = sl.compile(
    metrics=["total_revenue"],
    dimensions=["customers.region"],
)
print(sql)
print()

# Example 5: Transpile to different dialects
print("=" * 80)
print("Example 5: Transpile to Snowflake")
print("=" * 80)
sql = sl.compile(
    metrics=["orders.revenue"],
    dimensions=["orders.order_date__month"],
    dialect="snowflake",
)
print(sql)
print()
