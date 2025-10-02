# YAML Format Reference

Complete specification for Sidemantic YAML files.

## File Structure

```yaml
# yaml-language-server: $schema=./sidemantic-schema.json

models:
  - name: orders
    table: orders
    primary_key: id
    dimensions: [...]
    measures: [...]
    joins: [...]

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
    measures: [...]           # Optional
    joins: [...]              # Optional
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

## Measures

### Simple Aggregations

```yaml
measures:
  - name: string              # Required
    agg: sum|count|count_distinct|avg|min|max|median  # Required
    expr: string              # SQL expression (defaults to * for count)
    filters: [string]         # Optional WHERE conditions
    description: string       # Optional
    fill_nulls_with: value    # Optional default for NULL
```

### Complex Measures

```yaml
measures:
  # Ratio
  - name: conversion_rate
    type: ratio
    numerator: completed_orders
    denominator: total_orders

  # Derived/Formula
  - name: profit
    type: derived
    expr: "revenue - cost"

  # Cumulative
  - name: running_total
    type: cumulative
    expr: revenue
    window: "7 days"              # Rolling window
    # OR
    grain_to_date: month          # MTD/YTD

  # Time comparison
  - name: yoy_growth
    type: time_comparison
    base_metric: revenue
    comparison_type: yoy          # yoy, mom, wow, qoq
    calculation: percent_change   # percent_change, difference, ratio

  # Conversion funnel
  - name: signup_to_purchase
    type: conversion
    entity: user_id
    base_event: signup
    conversion_event: purchase
    conversion_window: "7 days"
```

### Examples

```yaml
measures:
  # Simple sum
  - name: revenue
    agg: sum
    expr: amount

  # Count
  - name: order_count
    agg: count

  # Average
  - name: avg_order_value
    agg: avg
    expr: amount

  # With filter
  - name: completed_revenue
    agg: sum
    expr: amount
    filters: ["status = 'completed'"]

  # SQL expression
  - name: total_value
    agg: sum
    expr: "quantity * price * (1 - discount)"

  # Multiple filters
  - name: us_revenue
    agg: sum
    expr: amount
    filters:
      - "country = 'US'"
      - "amount > 0"
```

## Joins

```yaml
joins:
  - name: string              # Required - name of related model
    type: belongs_to|has_many|has_one  # Required
    foreign_key: string       # Required - FK column name
```

### Join Types

- **belongs_to**: Foreign key is in THIS table
- **has_many**: Foreign key is in OTHER table
- **has_one**: Foreign key is in OTHER table (expects one record)

### Examples

```yaml
models:
  # Orders belong to customers
  - name: orders
    joins:
      - name: customers
        type: belongs_to
        foreign_key: customer_id  # Column in orders table

  # Customers have many orders
  - name: customers
    joins:
      - name: orders
        type: has_many
        foreign_key: customer_id  # Column in orders table

  # Order has one invoice
  - name: orders
    joins:
      - name: invoice
        type: has_one
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
    table: orders
    primary_key: id
    description: "Customer orders"

    dimensions:
      - name: status
        type: categorical
        sql: status

      - name: order_date
        type: time
        sql: created_at
        granularity: day

    measures:
      - name: revenue
        agg: sum
        expr: amount

      - name: order_count
        agg: count

      - name: completed_revenue
        agg: sum
        expr: amount
        filters: ["status = 'completed'"]

      - name: conversion_rate
        type: ratio
        numerator: completed_revenue
        denominator: revenue

    joins:
      - name: customers
        type: belongs_to
        foreign_key: customer_id

  - name: customers
    table: customers
    primary_key: id

    dimensions:
      - name: region
        type: categorical
        sql: region

    measures:
      - name: customer_count
        agg: count

    joins:
      - name: orders
        type: has_many
        foreign_key: customer_id

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
