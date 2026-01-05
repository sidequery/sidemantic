# Edge Cases: Complex Explore Patterns
# Tests various join types, sql_always_where, access filters, and other explore features

# Base views for explore testing
view: fact_orders {
  sql_table_name: analytics.orders ;;

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: customer_id {
    type: number
    sql: ${TABLE}.customer_id ;;
  }

  dimension: product_id {
    type: number
    sql: ${TABLE}.product_id ;;
  }

  dimension: store_id {
    type: number
    sql: ${TABLE}.store_id ;;
  }

  dimension: amount {
    type: number
    sql: ${TABLE}.amount ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: channel {
    type: string
    sql: ${TABLE}.channel ;;
  }

  dimension_group: created {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.created_at ;;
  }

  measure: count {
    type: count
  }

  measure: total_amount {
    type: sum
    sql: ${amount} ;;
  }

  measure: avg_amount {
    type: average
    sql: ${amount} ;;
  }

  measure: unique_customers {
    type: count_distinct
    sql: ${customer_id} ;;
  }
}

view: dim_customers {
  sql_table_name: analytics.customers ;;

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: tier {
    type: string
    sql: ${TABLE}.tier ;;
  }

  dimension: region_id {
    type: number
    sql: ${TABLE}.region_id ;;
  }

  dimension: account_manager_id {
    type: number
    sql: ${TABLE}.account_manager_id ;;
  }

  dimension_group: registered {
    type: time
    timeframes: [date, week, month, year]
    sql: ${TABLE}.registered_at ;;
  }

  measure: count {
    type: count
  }
}

view: dim_products {
  sql_table_name: analytics.products ;;

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: category {
    type: string
    sql: ${TABLE}.category ;;
  }

  dimension: brand {
    type: string
    sql: ${TABLE}.brand ;;
  }

  dimension: price {
    type: number
    sql: ${TABLE}.price ;;
  }

  measure: count {
    type: count
  }
}

view: dim_stores {
  sql_table_name: analytics.stores ;;

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: city {
    type: string
    sql: ${TABLE}.city ;;
  }

  dimension: region_id {
    type: number
    sql: ${TABLE}.region_id ;;
  }

  measure: count {
    type: count
  }
}

view: dim_regions {
  sql_table_name: analytics.regions ;;

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: country {
    type: string
    sql: ${TABLE}.country ;;
  }

  measure: count {
    type: count
  }
}

view: dim_account_managers {
  sql_table_name: analytics.account_managers ;;

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: team {
    type: string
    sql: ${TABLE}.team ;;
  }

  measure: count {
    type: count
  }
}

# Complex explore with multiple join types
explore: orders {
  from: fact_orders
  label: "Orders Analysis"
  description: "Main orders explore with all dimensions"
  group_label: "Sales"

  # Always filter to active orders
  sql_always_where: ${fact_orders.status} != 'deleted' ;;

  # Required filter
  always_filter: {
    filters: [fact_orders.created_date: "last 365 days"]
  }

  # Conditional filter
  conditionally_filter: {
    filters: [fact_orders.channel: "web"]
    unless: [fact_orders.store_id]
  }

  # Access filter for row-level security
  access_filter: {
    field: dim_regions.name
    user_attribute: allowed_regions
  }

  # Many-to-one join
  join: dim_customers {
    type: left_outer
    relationship: many_to_one
    sql_on: ${fact_orders.customer_id} = ${dim_customers.id} ;;
  }

  # Another many-to-one
  join: dim_products {
    type: left_outer
    relationship: many_to_one
    sql_on: ${fact_orders.product_id} = ${dim_products.id} ;;
  }

  # Join with foreign_key shorthand
  join: dim_stores {
    type: left_outer
    relationship: many_to_one
    foreign_key: fact_orders.store_id
  }

  # Chained join (through customers)
  join: customer_region {
    from: dim_regions
    type: left_outer
    relationship: many_to_one
    sql_on: ${dim_customers.region_id} = ${customer_region.id} ;;
  }

  # Another chained join (through stores)
  join: store_region {
    from: dim_regions
    type: left_outer
    relationship: many_to_one
    sql_on: ${dim_stores.region_id} = ${store_region.id} ;;
  }

  # Join with account managers
  join: dim_account_managers {
    type: left_outer
    relationship: many_to_one
    sql_on: ${dim_customers.account_manager_id} = ${dim_account_managers.id} ;;
  }
}

# Explore with one-to-many join
explore: customers {
  from: dim_customers
  label: "Customer Analysis"
  group_label: "Sales"

  # One-to-many join
  join: customer_orders {
    from: fact_orders
    type: left_outer
    relationship: one_to_many
    sql_on: ${dim_customers.id} = ${customer_orders.customer_id} ;;
  }

  join: dim_regions {
    type: left_outer
    relationship: many_to_one
    sql_on: ${dim_customers.region_id} = ${dim_regions.id} ;;
  }
}

# Explore with inner join
explore: completed_orders {
  from: fact_orders
  label: "Completed Orders Only"
  group_label: "Sales"

  # Pre-filter to completed only
  sql_always_where: ${fact_orders.status} = 'completed' ;;

  # Inner join - only orders with customers
  join: dim_customers {
    type: inner
    relationship: many_to_one
    sql_on: ${fact_orders.customer_id} = ${dim_customers.id} ;;
  }

  # Inner join - only orders with products
  join: dim_products {
    type: inner
    relationship: many_to_one
    sql_on: ${fact_orders.product_id} = ${dim_products.id} ;;
  }
}

# Explore with full outer join
explore: all_customers_orders {
  from: dim_customers
  label: "All Customers and Orders"
  group_label: "Sales"

  join: fact_orders {
    type: full_outer
    relationship: one_to_many
    sql_on: ${dim_customers.id} = ${fact_orders.customer_id} ;;
  }
}

# Explore with cross join (cartesian)
explore: date_product_matrix {
  from: dim_products
  label: "Date-Product Matrix"
  group_label: "Planning"

  # Note: cross joins create cartesian product, use carefully
  join: date_spine {
    type: cross
    relationship: many_to_many
  }
}

# Date spine view for cross join example
view: date_spine {
  derived_table: {
    sql:
      SELECT date
      FROM UNNEST(GENERATE_DATE_ARRAY(
        DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY),
        CURRENT_DATE()
      )) AS date
    ;;
  }

  dimension: date {
    type: date
    primary_key: yes
    sql: ${TABLE}.date ;;
  }

  dimension_group: report {
    type: time
    timeframes: [date, week, month, quarter, year]
    sql: ${date} ;;
  }
}

# Explore with sql_always_having
explore: high_volume_products {
  from: dim_products
  label: "High Volume Products"
  group_label: "Products"

  sql_always_having: ${product_orders.count} > 10 ;;

  join: product_orders {
    from: fact_orders
    type: left_outer
    relationship: one_to_many
    sql_on: ${dim_products.id} = ${product_orders.product_id} ;;
  }
}

# Explore with required access grants
explore: sensitive_orders {
  from: fact_orders
  label: "Sensitive Order Data"
  group_label: "Admin"

  required_access_grants: [can_view_sensitive_data]

  join: dim_customers {
    type: left_outer
    relationship: many_to_one
    sql_on: ${fact_orders.customer_id} = ${dim_customers.id} ;;
  }
}

# Explore with persisted derived table base
explore: order_metrics {
  from: order_daily_metrics
  label: "Order Metrics"
  group_label: "Metrics"
  persist_with: daily_datagroup
}

view: order_daily_metrics {
  derived_table: {
    sql:
      SELECT
        DATE(created_at) AS order_date,
        COUNT(*) AS order_count,
        SUM(amount) AS total_revenue,
        COUNT(DISTINCT customer_id) AS unique_customers
      FROM analytics.orders
      WHERE status != 'deleted'
      GROUP BY 1
    ;;
    datagroup_trigger: daily_datagroup
    indexes: ["order_date"]
  }

  dimension: order_date {
    type: date
    primary_key: yes
    sql: ${TABLE}.order_date ;;
  }

  dimension: order_count {
    type: number
    sql: ${TABLE}.order_count ;;
  }

  dimension: total_revenue {
    type: number
    sql: ${TABLE}.total_revenue ;;
  }

  dimension: unique_customers {
    type: number
    sql: ${TABLE}.unique_customers ;;
  }

  measure: sum_orders {
    type: sum
    sql: ${order_count} ;;
  }

  measure: sum_revenue {
    type: sum
    sql: ${total_revenue} ;;
  }

  measure: avg_revenue_per_day {
    type: average
    sql: ${total_revenue} ;;
  }
}
