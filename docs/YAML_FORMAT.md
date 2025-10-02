# Sidemantic Native YAML Format

## Overview

Sidemantic's native YAML format is designed for simplicity and clarity while supporting all core features.

## File Structure

```yaml
# semantic_layer.yml
models:
  - name: string               # Required
    table: string              # Optional (use table or sql, not both)
    sql: string                # Optional (use table or sql, not both)
    description: string        # Optional

    entities:                  # Required
      - name: string
        type: primary|foreign|unique
        expr: string           # Optional, defaults to name

    dimensions:                # Optional
      - name: string
        type: categorical|time|boolean|numeric
        expr: string           # Optional, defaults to name
        granularity: hour|day|week|month|quarter|year  # For time dimensions
        description: string
        label: string

    measures:                  # Optional
      - name: string
        agg: sum|count|count_distinct|avg|min|max|median
        expr: string           # Optional, defaults to * for count
        filters: [string]      # Optional WHERE clauses
        description: string
        label: string

metrics:                       # Optional
  - name: string
    type: simple|ratio|derived|cumulative
    description: string
    label: string

    # Simple metric
    measure: string            # model.measure reference

    # Ratio metric
    numerator: string          # model.measure or metric reference
    denominator: string        # model.measure or metric reference

    # Derived metric
    expr: string               # Formula (e.g., "revenue / orders")
    metrics: [string]          # Referenced metrics

    # Cumulative metric
    window: string             # e.g., "7 days", "3 months"

    # Common
    filters: [string]          # Optional WHERE clauses
```

## Example

```yaml
models:
  - name: orders
    table: public.orders
    description: "Order transactions"

    entities:
      - name: order
        type: primary
        expr: order_id
      - name: customer
        type: foreign
        expr: customer_id

    dimensions:
      - name: status
        type: categorical
        description: "Order status"

      - name: order_date
        type: time
        granularity: day
        expr: created_at
        description: "Order creation date"

      - name: is_high_value
        type: boolean
        expr: "order_amount > 100"

    measures:
      - name: order_count
        agg: count
        description: "Total number of orders"

      - name: revenue
        agg: sum
        expr: order_amount
        description: "Total revenue"

      - name: completed_revenue
        agg: sum
        expr: order_amount
        filters:
          - "status = 'completed'"
        description: "Revenue from completed orders"

  - name: customers
    table: public.customers
    description: "Customer dimension"

    entities:
      - name: customer
        type: primary
        expr: customer_id

    dimensions:
      - name: region
        type: categorical
      - name: tier
        type: categorical

metrics:
  - name: total_revenue
    type: simple
    measure: orders.revenue
    description: "Total revenue from all orders"

  - name: conversion_rate
    type: ratio
    numerator: orders.completed_revenue
    denominator: orders.revenue
    description: "Percentage of revenue from completed orders"

  - name: revenue_per_order
    type: derived
    expr: "total_revenue / order_count"
    metrics:
      - total_revenue
      - order_count
    description: "Average revenue per order"
```

## Key Design Decisions

1. **Simple structure**: Flat hierarchy, no nested complexity
2. **Familiar syntax**: Similar to MetricFlow/Cube for easy adoption
3. **Entity-first**: Entities are explicit and central to join discovery
4. **Optional SQL**: Can use `table` for simple cases or `sql` for complex queries
5. **Measure vs Metric**: Clear separation between aggregations (measures) and business logic (metrics)
6. **Formula-based derived metrics**: Simple string formulas for derived metrics
7. **Type safety**: Explicit types for dimensions and metrics

## Loading Example

```python
from sidemantic.adapters import SidemanticAdapter

adapter = SidemanticAdapter()
sl = adapter.parse("path/to/semantic_layer.yml")

# Or load directly
sl = SemanticLayer.from_yaml("path/to/semantic_layer.yml")
```

## Export Example

```python
from sidemantic.adapters import SidemanticAdapter

adapter = SidemanticAdapter()
adapter.export(sl, "output/semantic_layer.yml")

# Or export directly
sl.to_yaml("output/semantic_layer.yml")
```
