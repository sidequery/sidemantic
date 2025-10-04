# YAML Format Reference

Complete specification for Sidemantic YAML files.

## File Structure

```yaml
# yaml-language-server: $schema=./sidemantic-schema.json

models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions: [...]
    metrics: [...]
    relationships: [...]

metrics:
  - name: total_revenue
    sql: orders.revenue

parameters:
  - name: start_date
    type: date
    default_value: "2024-01-01"
```

## Models

```yaml
models:
  - name: string              # Required - unique identifier
    table: string             # Physical table (or use sql)
    sql: string               # SQL query (or use table)
    primary_key: string       # Required - primary key column
    description: string       # Optional

    dimensions: [...]         # Optional
    metrics: [...]            # Optional (model-level aggregations)
    relationships: [...]      # Optional
```

## Dimensions

```yaml
dimensions:
  - name: string              # Required
    type: categorical|time|boolean|numeric  # Required
    sql: string               # SQL expression (defaults to name)
    description: string       # Optional
    label: string             # Optional

    # For time dimensions only
    granularity: hour|day|week|month|quarter|year
```

### Dimension Types

- **categorical**: Text/enum values (status, region, product_name)
- **time**: Dates/timestamps (order_date, created_at)
- **boolean**: True/false (is_active, is_deleted)
- **numeric**: Numbers (price_tier, quantity_bucket)

### Examples

```yaml
dimensions:
  # Categorical
  - name: status
    type: categorical
    sql: status

  # Time with granularity
  - name: order_date
    type: time
    sql: created_at
    granularity: day

  # Boolean
  - name: is_active
    type: boolean
    sql: active

  # SQL expression
  - name: customer_tier
    type: categorical
    sql: |
      CASE
        WHEN amount > 1000 THEN 'premium'
        WHEN amount > 100 THEN 'standard'
        ELSE 'basic'
      END
```

## Metrics (Model-Level)

Model-level metrics are **aggregations** defined on a single model. These become the building blocks for graph-level metrics.

### Simple Aggregations

```yaml
metrics:
  - name: string              # Required
    agg: sum|count|count_distinct|avg|min|max|median  # Required
    sql: string               # SQL expression (defaults to * for count)
    filters: [string]         # Optional WHERE conditions
    description: string       # Optional
    fill_nulls_with: value    # Optional default for NULL
```

### Examples

```yaml
metrics:
  # Simple sum
  - name: revenue
    agg: sum
    sql: amount

  # Count
  - name: order_count
    agg: count

  # Average
  - name: avg_order_value
    agg: avg
    sql: amount

  # With filter
  - name: completed_revenue
    agg: sum
    sql: amount
    filters: ["status = 'completed'"]

  # SQL expression
  - name: total_value
    agg: sum
    sql: "quantity * price * (1 - discount)"

  # Multiple filters
  - name: us_revenue
    agg: sum
    sql: amount
    filters:
      - "country = 'US'"
      - "amount > 0"
```

## Metrics (Graph-Level)

Graph-level metrics are defined at the top level and can reference model-level metrics or other graph-level metrics. Dependencies are **auto-detected** from SQL expressions.

### Metric References (Untyped)

The simplest graph-level metric just references a model-level metric:

```yaml
metrics:
  # Reference a model-level metric
  - name: total_revenue
    sql: orders.revenue
    description: "Total revenue from all orders"
```

No `type` needed! Dependencies are automatically detected from the `sql` expression.

### Ratio Metrics

```yaml
metrics:
  - name: conversion_rate
    type: ratio
    numerator: orders.completed_revenue
    denominator: orders.revenue
```

### Derived Metrics

Derived metrics use formulas and **automatically detect dependencies**:

```yaml
metrics:
  # Simple formula - dependencies auto-detected
  - name: profit
    type: derived
    sql: "revenue - cost"

  # References other metrics - no manual dependency list needed!
  - name: revenue_per_customer
    type: derived
    sql: "total_revenue / total_customers"
```

### Cumulative Metrics

```yaml
metrics:
  # Rolling window
  - name: rolling_7day_revenue
    type: cumulative
    sql: orders.revenue
    window: "7 days"

  # Period-to-date (MTD, YTD, etc.)
  - name: mtd_revenue
    type: cumulative
    sql: orders.revenue
    grain_to_date: month
```

### Time Comparison Metrics

```yaml
metrics:
  - name: yoy_revenue_growth
    type: time_comparison
    base_metric: total_revenue
    comparison_type: yoy          # yoy, mom, wow, qoq
    calculation: percent_change   # percent_change, difference, ratio
```

### Conversion Funnel Metrics

```yaml
metrics:
  - name: signup_to_purchase_rate
    type: conversion
    entity: user_id
    base_event: signup
    conversion_event: purchase
    conversion_window: "7 days"
```

## Relationships

Relationships define how models join together. Use explicit relationship types instead of traditional join terminology.

```yaml
relationships:
  - name: string              # Required - name of related model
    type: many_to_one|one_to_many|one_to_one  # Required
    foreign_key: string       # Required - FK column name
    primary_key: string       # Optional - PK in related table (defaults to related model's primary_key)
```

### Relationship Types

- **many_to_one**: Many records in THIS table → one record in OTHER table (e.g., orders → customer)
- **one_to_many**: One record in THIS table → many records in OTHER table (e.g., customer → orders)
- **one_to_one**: One record in THIS table → one record in OTHER table (e.g., order → invoice)

### Examples

```yaml
models:
  # Orders: many orders belong to one customer
  - name: orders
    table: orders
    primary_key: order_id
    relationships:
      - name: customer
        type: many_to_one
        foreign_key: customer_id  # Column in orders table

  # Customers: one customer has many orders
  - name: customers
    table: customers
    primary_key: customer_id
    relationships:
      - name: orders
        type: one_to_many
        foreign_key: customer_id  # Column in orders table (the OTHER table)

  # Order has one invoice
  - name: orders
    relationships:
      - name: invoice
        type: one_to_one
        foreign_key: order_id     # Column in invoice table
```

## Parameters

```yaml
parameters:
  - name: string              # Required
    type: string|number|date|unquoted|yesno  # Required
    default_value: any        # Required
    allowed_values: [any]     # Optional - restrict to specific values
    description: string       # Optional
```

### Parameter Types

- **string**: Text values (quoted in SQL)
- **number**: Numeric values (no quotes)
- **date**: Date values (quoted as strings)
- **unquoted**: Raw SQL (table names, column names)
- **yesno**: Boolean mapped to yes/no

### Examples

```yaml
parameters:
  # Date
  - name: start_date
    type: date
    default_value: "2024-01-01"

  # Number
  - name: min_amount
    type: number
    default_value: 100

  # String with allowed values
  - name: region
    type: string
    default_value: "US"
    allowed_values: ["US", "EU", "APAC"]

  # Boolean
  - name: include_cancelled
    type: yesno
    default_value: false
```

## Complete Example

```yaml
# yaml-language-server: $schema=./sidemantic-schema.json

models:
  - name: orders
    table: public.orders
    primary_key: order_id
    description: "Customer orders"

    relationships:
      - name: customer
        type: many_to_one
        foreign_key: customer_id

    dimensions:
      - name: status
        type: categorical
        sql: status

      - name: order_date
        type: time
        sql: created_at
        granularity: day

    metrics:
      - name: revenue
        agg: sum
        sql: amount

      - name: order_count
        agg: count

      - name: completed_revenue
        agg: sum
        sql: amount
        filters: ["status = 'completed'"]

  - name: customers
    table: public.customers
    primary_key: customer_id

    relationships:
      - name: orders
        type: one_to_many
        foreign_key: customer_id

    dimensions:
      - name: region
        type: categorical
        sql: region

    metrics:
      - name: customer_count
        agg: count_distinct
        sql: customer_id

# Graph-level metrics
metrics:
  # Simple reference (dependencies auto-detected)
  - name: total_revenue
    sql: orders.revenue
    description: "Total revenue from all orders"

  # Ratio metric
  - name: conversion_rate
    type: ratio
    numerator: orders.completed_revenue
    denominator: orders.revenue
    description: "Percentage of revenue from completed orders"

  # Derived metric (dependencies auto-detected from formula)
  - name: revenue_per_customer
    type: derived
    sql: "total_revenue / customers.customer_count"
    description: "Average revenue per customer"

parameters:
  - name: start_date
    type: date
    default_value: "2024-01-01"

  - name: min_amount
    type: number
    default_value: 100
```

## JSON Schema

Generate JSON Schema for editor autocomplete:

```bash
uv run python -m sidemantic.schema
```

Add to your YAML file:

```yaml
# yaml-language-server: $schema=./sidemantic-schema.json
```

This enables autocomplete in VS Code, IntelliJ, and other editors with YAML Language Server support.
