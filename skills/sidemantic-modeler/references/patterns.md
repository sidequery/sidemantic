# Sidemantic Pattern Library

Ready-to-adapt YAML templates for common semantic model patterns.

## 1. Single Table Quick Start

### Minimal model

```yaml
models:
  - name: events
    table: analytics.events
    primary_key: event_id
    dimensions:
      - name: event_type
        type: categorical
    metrics:
      - name: event_count
        agg: count
```

### With time, multiple aggregations, segments

```yaml
models:
  - name: events
    table: analytics.events
    primary_key: event_id
    default_time_dimension: event_date
    default_grain: day
    dimensions:
      - name: event_type
        type: categorical
        description: "Type of event (click, view, purchase)"
      - name: event_date
        type: time
        sql: created_at
        granularity: day
      - name: user_id
        type: categorical
      - name: is_mobile
        type: boolean
        sql: "platform IN ('ios', 'android')"
    metrics:
      - name: event_count
        agg: count
      - name: unique_users
        agg: count_distinct
        sql: user_id
      - name: total_value
        agg: sum
        sql: event_value
      - name: avg_value
        agg: avg
        sql: event_value
    segments:
      - name: mobile_only
        sql: "platform IN ('ios', 'android')"
      - name: high_value
        sql: "event_value > 100"

metrics:
  - name: value_per_event
    type: ratio
    numerator: events.total_value
    denominator: events.event_count
  - name: value_per_user
    type: derived
    sql: "total_value / unique_users"
```

## 2. E-Commerce (Star Schema)

Orders fact table with customers, products, and line items.

```yaml
models:
  - name: orders
    table: public.orders
    primary_key: order_id
    default_time_dimension: order_date
    default_grain: day
    relationships:
      - name: customers
        type: many_to_one
        foreign_key: customer_id
      - name: products
        type: many_to_many
        through: line_items
        through_foreign_key: order_id
        related_foreign_key: product_id
    dimensions:
      - name: order_date
        type: time
        sql: created_at
        granularity: day
      - name: status
        type: categorical
      - name: channel
        type: categorical
      - name: is_first_order
        type: boolean
        sql: "order_number = 1"
    metrics:
      - name: order_count
        agg: count
      - name: revenue
        agg: sum
        sql: order_total
        format: "$#,##0.00"
      - name: completed_revenue
        agg: sum
        sql: order_total
        filters:
          - "{model}.status = 'completed'"
      - name: refund_amount
        agg: sum
        sql: refund_total
      - name: unique_customers
        agg: count_distinct
        sql: customer_id
    segments:
      - name: completed
        sql: "status = 'completed'"

  - name: customers
    table: public.customers
    primary_key: customer_id
    dimensions:
      - name: customer_name
        type: categorical
      - name: signup_date
        type: time
        granularity: day
      - name: region
        type: categorical
      - name: tier
        type: categorical
      - name: country
        type: categorical
      - name: state
        type: categorical
        parent: country
      - name: city
        type: categorical
        parent: state
    metrics:
      - name: customer_count
        agg: count

  - name: line_items
    table: public.line_items
    primary_key: line_item_id
    relationships:
      - name: orders
        type: many_to_one
        foreign_key: order_id
      - name: products
        type: many_to_one
        foreign_key: product_id
    dimensions:
      - name: quantity
        type: numeric
    metrics:
      - name: total_quantity
        agg: sum
        sql: quantity
      - name: line_item_revenue
        agg: sum
        sql: "quantity * unit_price"

  - name: products
    table: public.products
    primary_key: product_id
    dimensions:
      - name: product_name
        type: categorical
      - name: category
        type: categorical
      - name: subcategory
        type: categorical
        parent: category
      - name: brand
        type: categorical
      - name: price
        type: numeric
        sql: list_price
    metrics:
      - name: product_count
        agg: count_distinct
        sql: product_id
      - name: avg_price
        agg: avg
        sql: list_price

metrics:
  - name: average_order_value
    type: ratio
    numerator: orders.revenue
    denominator: orders.order_count
  - name: completion_rate
    type: ratio
    numerator: orders.completed_revenue
    denominator: orders.revenue
  - name: net_revenue
    type: derived
    sql: "revenue - refund_amount"
  - name: cumulative_revenue
    type: cumulative
    sql: orders.revenue
  - name: mtd_revenue
    type: cumulative
    sql: orders.revenue
    grain_to_date: month
  - name: customer_ltv
    type: ratio
    numerator: orders.revenue
    denominator: orders.unique_customers
```

## 3. SaaS Metrics

Users, subscriptions, product events. MRR, churn, DAU/MAU.

```yaml
models:
  - name: users
    table: saas.users
    primary_key: user_id
    relationships:
      - name: subscriptions
        type: one_to_many
        foreign_key: user_id
    dimensions:
      - name: signup_date
        type: time
        sql: created_at
        granularity: day
      - name: plan
        type: categorical
      - name: status
        type: categorical
      - name: acquisition_source
        type: categorical
    metrics:
      - name: user_count
        agg: count
      - name: active_users
        agg: count_distinct
        sql: user_id
        filters:
          - "{model}.status = 'active'"

  - name: subscriptions
    table: saas.subscriptions
    primary_key: subscription_id
    relationships:
      - name: users
        type: many_to_one
        foreign_key: user_id
    dimensions:
      - name: start_date
        type: time
        granularity: day
      - name: end_date
        type: time
        granularity: day
      - name: plan
        type: categorical
      - name: billing_interval
        type: categorical
      - name: is_active
        type: boolean
        sql: "end_date IS NULL OR end_date > CURRENT_DATE"
    metrics:
      - name: mrr
        agg: sum
        sql: monthly_amount
        format: "$#,##0.00"
      - name: subscription_count
        agg: count
      - name: active_subscriptions
        agg: count
        filters:
          - "{model}.end_date IS NULL OR {model}.end_date > CURRENT_DATE"
      - name: churned_subscriptions
        agg: count
        filters:
          - "{model}.end_date IS NOT NULL"
          - "{model}.end_date >= DATE_TRUNC('month', CURRENT_DATE)"

  - name: events
    table: saas.product_events
    primary_key: event_id
    default_time_dimension: event_date
    default_grain: day
    relationships:
      - name: users
        type: many_to_one
        foreign_key: user_id
    dimensions:
      - name: event_date
        type: time
        sql: created_at
        granularity: day
      - name: event_type
        type: categorical
      - name: feature
        type: categorical
    metrics:
      - name: event_count
        agg: count
      - name: dau
        agg: count_distinct
        sql: user_id

metrics:
  - name: arpu
    type: ratio
    numerator: subscriptions.mrr
    denominator: subscriptions.active_subscriptions
  - name: churn_rate
    type: ratio
    numerator: subscriptions.churned_subscriptions
    denominator: subscriptions.subscription_count
  - name: mrr_mom_growth
    type: time_comparison
    base_metric: subscriptions.mrr
    comparison_type: mom
    calculation: percent_change
  - name: mrr_yoy_growth
    type: time_comparison
    base_metric: subscriptions.mrr
    comparison_type: yoy
    calculation: percent_change
  - name: rolling_7day_dau
    type: cumulative
    agg: avg
    sql: events.dau
    window: "6 days"
  - name: trial_conversion
    type: conversion
    entity: user_id
    base_event: signup
    conversion_event: subscription_created
    conversion_window: "14 days"
```

## 4. Marketing Analytics

Campaigns, ad spend, conversions. ROAS, CPA, CTR.

```yaml
models:
  - name: campaigns
    table: marketing.campaigns
    primary_key: campaign_id
    dimensions:
      - name: campaign_name
        type: categorical
      - name: channel
        type: categorical
      - name: medium
        type: categorical
      - name: launch_date
        type: time
        granularity: day
      - name: is_active
        type: boolean
        sql: "end_date IS NULL OR end_date > CURRENT_DATE"
    metrics:
      - name: campaign_count
        agg: count

  - name: ad_spend
    table: marketing.ad_spend
    primary_key: spend_id
    default_time_dimension: spend_date
    default_grain: day
    relationships:
      - name: campaigns
        type: many_to_one
        foreign_key: campaign_id
    dimensions:
      - name: spend_date
        type: time
        granularity: day
      - name: platform
        type: categorical
    metrics:
      - name: total_spend
        agg: sum
        sql: amount
        format: "$#,##0.00"
      - name: impressions
        agg: sum
        sql: impressions
      - name: clicks
        agg: sum
        sql: clicks

  - name: conversions
    table: marketing.conversions
    primary_key: conversion_id
    default_time_dimension: conversion_date
    default_grain: day
    relationships:
      - name: campaigns
        type: many_to_one
        foreign_key: campaign_id
    dimensions:
      - name: conversion_date
        type: time
        granularity: day
      - name: conversion_type
        type: categorical
      - name: attribution_model
        type: categorical
    metrics:
      - name: conversion_count
        agg: count
      - name: conversion_value
        agg: sum
        sql: value
        format: "$#,##0.00"
      - name: purchase_count
        agg: count
        filters:
          - "{model}.conversion_type = 'purchase'"
      - name: purchase_value
        agg: sum
        sql: value
        filters:
          - "{model}.conversion_type = 'purchase'"

metrics:
  - name: ctr
    type: ratio
    numerator: ad_spend.clicks
    denominator: ad_spend.impressions
  - name: cpa
    type: ratio
    numerator: ad_spend.total_spend
    denominator: conversions.conversion_count
  - name: roas
    type: ratio
    numerator: conversions.conversion_value
    denominator: ad_spend.total_spend
  - name: cpc
    type: ratio
    numerator: ad_spend.total_spend
    denominator: ad_spend.clicks
  - name: spend_wow
    type: time_comparison
    base_metric: ad_spend.total_spend
    comparison_type: wow
    calculation: percent_change
  - name: click_to_purchase
    type: conversion
    entity: user_id
    base_event: click
    conversion_event: purchase
    conversion_window: "7 days"
```

## 5. Time-Series / IoT

Sensor readings, devices, rolling averages.

```yaml
models:
  - name: devices
    table: iot.devices
    primary_key: device_id
    dimensions:
      - name: device_name
        type: categorical
      - name: device_type
        type: categorical
      - name: status
        type: categorical
      - name: is_active
        type: boolean
        sql: "status = 'online'"
      - name: facility
        type: categorical
      - name: zone
        type: categorical
        parent: facility
      - name: location
        type: categorical
        parent: zone
      - name: install_date
        type: time
        granularity: day
    metrics:
      - name: device_count
        agg: count
      - name: active_devices
        agg: count
        filters:
          - "{model}.status = 'online'"

  - name: readings
    table: iot.sensor_readings
    primary_key: reading_id
    default_time_dimension: reading_time
    default_grain: hour
    relationships:
      - name: devices
        type: many_to_one
        foreign_key: device_id
    dimensions:
      - name: reading_time
        type: time
        sql: recorded_at
        granularity: second
      - name: metric_name
        type: categorical
      - name: unit
        type: categorical
    metrics:
      - name: reading_count
        agg: count
      - name: avg_value
        agg: avg
        sql: value
      - name: max_value
        agg: max
        sql: value
      - name: min_value
        agg: min
        sql: value
      - name: total_value
        agg: sum
        sql: value
      - name: stddev_value
        agg: stddev
        sql: value
      - name: median_value
        agg: median
        sql: value
    segments:
      - name: temperature_readings
        sql: "metric_name = 'temperature'"
      - name: critical_values
        sql: "value > 100 OR value < -10"

metrics:
  - name: rolling_24h_avg
    type: cumulative
    agg: avg
    sql: readings.avg_value
    window: "23 hours"
  - name: rolling_7day_avg
    type: cumulative
    agg: avg
    sql: readings.avg_value
    window: "6 days"
  - name: avg_value_dod
    type: time_comparison
    base_metric: readings.avg_value
    comparison_type: dod
    calculation: difference
  - name: ytd_total_value
    type: cumulative
    sql: readings.total_value
    grain_to_date: year
  - name: mtd_total_value
    type: cumulative
    sql: readings.total_value
    grain_to_date: month
```

## 6. Classic Dimensional Warehouse

Fact table with multiple dimension tables, role-playing dimensions, deep hierarchies.

```yaml
models:
  - name: fact_sales
    table: warehouse.fact_sales
    primary_key: sale_id
    default_time_dimension: sale_date
    default_grain: day
    relationships:
      - name: dim_date
        type: many_to_one
        foreign_key: sale_date_key
        primary_key: date_key
      - name: dim_ship_date
        type: many_to_one
        foreign_key: ship_date_key
        primary_key: date_key
      - name: dim_product
        type: many_to_one
        foreign_key: product_key
      - name: dim_store
        type: many_to_one
        foreign_key: store_key
      - name: dim_customer
        type: many_to_one
        foreign_key: customer_key
    dimensions:
      - name: sale_date
        type: time
        granularity: day
      - name: ship_date
        type: time
        granularity: day
      - name: quantity
        type: numeric
    metrics:
      - name: total_sales
        agg: sum
        sql: sale_amount
        format: "$#,##0.00"
      - name: total_cost
        agg: sum
        sql: cost_amount
      - name: units_sold
        agg: sum
        sql: quantity
      - name: transaction_count
        agg: count
      - name: unique_customers
        agg: count_distinct
        sql: customer_key
      - name: online_sales
        agg: sum
        sql: sale_amount
        filters:
          - "{model}.channel = 'online'"

  - name: dim_date
    table: warehouse.dim_date
    primary_key: date_key
    dimensions:
      - name: full_date
        type: time
        granularity: day
      - name: day_of_week
        type: categorical
      - name: month_name
        type: categorical
      - name: quarter
        type: categorical
      - name: fiscal_year
        type: categorical
      - name: is_weekend
        type: boolean
        sql: "day_of_week IN ('Saturday', 'Sunday')"

  # Role-playing dimension: same physical table, different model name
  - name: dim_ship_date
    table: warehouse.dim_date
    primary_key: date_key
    dimensions:
      - name: ship_full_date
        type: time
        sql: full_date
        granularity: day
      - name: ship_day_of_week
        type: categorical
        sql: day_of_week

  - name: dim_product
    table: warehouse.dim_product
    primary_key: product_key
    dimensions:
      - name: department
        type: categorical
      - name: category
        type: categorical
        parent: department
      - name: subcategory
        type: categorical
        parent: category
      - name: product_name
        type: categorical
        parent: subcategory
      - name: brand
        type: categorical
      - name: unit_price
        type: numeric
        sql: list_price
    metrics:
      - name: distinct_products
        agg: count_distinct
        sql: product_key

  - name: dim_store
    table: warehouse.dim_store
    primary_key: store_key
    dimensions:
      - name: region
        type: categorical
      - name: district
        type: categorical
        parent: region
      - name: store_name
        type: categorical
        parent: district
      - name: store_type
        type: categorical
      - name: city
        type: categorical
      - name: state
        type: categorical

  - name: dim_customer
    table: warehouse.dim_customer
    primary_key: customer_key
    dimensions:
      - name: customer_name
        type: categorical
      - name: segment
        type: categorical
      - name: loyalty_tier
        type: categorical
      - name: first_purchase_date
        type: time
        granularity: day

metrics:
  - name: gross_margin
    type: derived
    sql: "(total_sales - total_cost) / NULLIF(total_sales, 0)"
  - name: gross_profit
    type: derived
    sql: "total_sales - total_cost"
  - name: avg_basket_size
    type: ratio
    numerator: fact_sales.units_sold
    denominator: fact_sales.transaction_count
  - name: sales_per_customer
    type: ratio
    numerator: fact_sales.total_sales
    denominator: fact_sales.unique_customers
  - name: sales_yoy
    type: time_comparison
    base_metric: fact_sales.total_sales
    comparison_type: yoy
    calculation: percent_change
  - name: ytd_sales
    type: cumulative
    sql: fact_sales.total_sales
    grain_to_date: year
  - name: mtd_sales
    type: cumulative
    sql: fact_sales.total_sales
    grain_to_date: month
```

## Building Blocks Reference

### Derived table model (SQL instead of table)

```yaml
- name: daily_summary
  sql: |
    SELECT
      DATE_TRUNC('day', created_at) AS day,
      COUNT(*) AS total_events,
      SUM(value) AS total_value
    FROM raw_events
    GROUP BY 1
  primary_key: day
  dimensions:
    - name: day
      type: time
      granularity: day
  metrics:
    - name: events
      agg: sum
      sql: total_events
```

### Metric-level filters (CASE WHEN, not WHERE)

```yaml
metrics:
  - name: completed_revenue
    agg: sum
    sql: amount
    filters:
      - "{model}.status = 'completed'"
      - "{model}.amount > 0"
```

### Composite primary keys

```yaml
- name: line_items
  table: order_line_items
  primary_key: [order_id, line_number]
```

### Default time dimension

```yaml
- name: orders
  table: orders
  primary_key: order_id
  default_time_dimension: order_date
  default_grain: month
```

### Custom window function passthrough

```yaml
metrics:
  - name: custom_window
    type: cumulative
    window_expression: "AVG(base.daily_revenue)"
    window_frame: "RANGE BETWEEN INTERVAL 2 DAY PRECEDING AND CURRENT ROW"
    window_order: order_date
```
