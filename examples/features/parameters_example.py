#!/usr/bin/env python3
"""Example demonstrating parameters (dynamic user input)."""

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.parameter import Parameter
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.generator import SQLGenerator

# Create semantic graph
graph = SemanticGraph()

# Define parameters for user input
status_param = Parameter(
    name="order_status",
    type="string",
    default_value="completed",
    allowed_values=["pending", "completed", "cancelled"],
    description="Filter orders by status",
)

start_date_param = Parameter(
    name="start_date",
    type="date",
    default_value="2024-01-01",
    description="Start date for analysis",
)

min_amount_param = Parameter(name="min_amount", type="number", default_value=0, description="Minimum order amount")

graph.add_parameter(status_param)
graph.add_parameter(start_date_param)
graph.add_parameter(min_amount_param)

# Define orders model
orders = Model(
    name="orders",
    table="orders",
    primary_key="id",
    dimensions=[
        Dimension(name="order_date", type="time", sql="order_date"),
        Dimension(name="status", type="categorical", sql="status"),
    ],
    metrics=[
        Metric(name="revenue", agg="sum", sql="amount"),
        Metric(name="order_count", agg="count", sql="*"),
    ],
)

graph.add_model(orders)

# Create SQL generator
generator = SQLGenerator(graph)

print("=" * 80)
print("Parameters Example")
print("=" * 80)
print()

# Example 1: Using default parameter values
print("Example 1: Default parameter values")
print("-" * 40)

sql = generator.generate(
    metrics=["orders.revenue", "orders.order_count"],
    dimensions=["orders.order_date"],
    filters=["orders.status = {{ order_status }}"],
    parameters={},  # Empty - will use defaults
    order_by=["orders.order_date"],
)

print("Filter: orders.status = {{ order_status }}")
print("Parameters: {} (using defaults)")
print()
print("Generated SQL:")
print(sql)
print()

# Example 2: Overriding parameter values
print("Example 2: Custom parameter values")
print("-" * 40)

sql = generator.generate(
    metrics=["orders.revenue", "orders.order_count"],
    dimensions=["orders.order_date"],
    filters=["orders.status = {{ order_status }}"],
    parameters={"order_status": "pending"},  # Override default
    order_by=["orders.order_date"],
)

print("Filter: orders.status = {{ order_status }}")
print("Parameters: {'order_status': 'pending'}")
print()
print("Generated SQL:")
print(sql)
print()

# Example 3: Multiple parameters
print("Example 3: Multiple parameters")
print("-" * 40)

sql = generator.generate(
    metrics=["orders.revenue"],
    dimensions=["orders.order_date"],
    filters=[
        "orders.status = {{ order_status }}",
        "orders.order_date >= {{ start_date }}",
    ],
    parameters={
        "order_status": "completed",
        "start_date": "2024-02-01",
    },
    order_by=["orders.order_date"],
)

print("Filters:")
print("  - orders.status = {{ order_status }}")
print("  - orders.order_date >= {{ start_date }}")
print()
print("Parameters:")
print("  - order_status: 'completed'")
print("  - start_date: '2024-02-01'")
print()
print("Generated SQL:")
print(sql)
print()

print("=" * 80)
print("Parameters provide type-safe user input with:")
print("  Default values")
print("  Allowed values (for dropdowns)")
print("  Type safety (string, number, date, etc.)")
print("  SQL-safe interpolation with {{ }} syntax")
print("=" * 80)
