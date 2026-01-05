# Edge Cases: Complex SQL Patterns
# Tests nested SQL, subqueries, window functions, and cross-view references

view: complex_sql_view {
  sql_table_name: analytics.orders ;;
  description: "View with complex SQL patterns"

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: customer_id {
    type: number
    sql: ${TABLE}.customer_id ;;
  }

  dimension: amount {
    type: number
    sql: ${TABLE}.amount ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  # Subquery in dimension
  dimension: customer_order_rank {
    type: number
    sql: (
      SELECT COUNT(*) + 1
      FROM analytics.orders o2
      WHERE o2.customer_id = ${TABLE}.customer_id
        AND o2.created_at < ${TABLE}.created_at
    ) ;;
    description: "Order sequence number for this customer"
  }

  # Correlated subquery for aggregation
  dimension: customer_total_orders {
    type: number
    sql: (
      SELECT COUNT(*)
      FROM analytics.orders o2
      WHERE o2.customer_id = ${TABLE}.customer_id
    ) ;;
    description: "Total orders for this customer"
  }

  # Subquery with SUM
  dimension: customer_lifetime_value {
    type: number
    sql: (
      SELECT COALESCE(SUM(o2.amount), 0)
      FROM analytics.orders o2
      WHERE o2.customer_id = ${TABLE}.customer_id
        AND o2.status = 'completed'
    ) ;;
    value_format_name: usd
    description: "Customer's total completed order value"
  }

  # Complex CASE expression
  dimension: order_size_bucket {
    type: string
    sql: CASE
      WHEN ${amount} IS NULL THEN 'Unknown'
      WHEN ${amount} < 50 THEN 'Small'
      WHEN ${amount} < 200 THEN 'Medium'
      WHEN ${amount} < 500 THEN 'Large'
      WHEN ${amount} < 1000 THEN 'XL'
      ELSE 'Enterprise'
    END ;;
    description: "Order size bucket"
  }

  # Nested CASE with multiple conditions
  dimension: customer_segment {
    type: string
    sql: CASE
      WHEN ${customer_lifetime_value} >= 10000 THEN
        CASE
          WHEN ${customer_total_orders} >= 50 THEN 'VIP Frequent'
          ELSE 'VIP Occasional'
        END
      WHEN ${customer_lifetime_value} >= 1000 THEN
        CASE
          WHEN ${customer_total_orders} >= 10 THEN 'Regular Frequent'
          ELSE 'Regular Occasional'
        END
      ELSE 'New'
    END ;;
    description: "Customer segment based on value and frequency"
  }

  # Window function in dimension (though typically these go in derived tables)
  dimension: pct_of_customer_total {
    type: number
    sql: SAFE_DIVIDE(${amount}, ${customer_lifetime_value}) * 100 ;;
    value_format: "0.0\%"
    description: "This order as percentage of customer's total spend"
  }

  # JSON extraction (BigQuery style)
  dimension: metadata_source {
    type: string
    sql: JSON_EXTRACT_SCALAR(${TABLE}.metadata, '$.source') ;;
  }

  dimension: metadata_campaign {
    type: string
    sql: JSON_EXTRACT_SCALAR(${TABLE}.metadata, '$.campaign_id') ;;
  }

  # Date arithmetic
  dimension_group: created {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.created_at ;;
  }

  dimension: days_since_created {
    type: number
    sql: DATE_DIFF(CURRENT_DATE(), DATE(${created_raw}), DAY) ;;
  }

  dimension: is_recent {
    type: yesno
    sql: ${days_since_created} <= 30 ;;
  }

  dimension: order_age_bucket {
    type: string
    sql: CASE
      WHEN ${days_since_created} <= 7 THEN '0-7 days'
      WHEN ${days_since_created} <= 30 THEN '8-30 days'
      WHEN ${days_since_created} <= 90 THEN '31-90 days'
      WHEN ${days_since_created} <= 365 THEN '91-365 days'
      ELSE '365+ days'
    END ;;
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

  # Measure with complex SQL
  measure: median_amount {
    type: number
    sql: APPROX_QUANTILES(${amount}, 100)[OFFSET(50)] ;;
    description: "Approximate median order amount"
  }

  measure: unique_customers {
    type: count_distinct
    sql: ${customer_id} ;;
  }
}

# Derived table with complex SQL
view: customer_cohorts {
  derived_table: {
    sql:
      WITH first_orders AS (
        SELECT
          customer_id,
          MIN(created_at) AS first_order_date,
          MIN(amount) AS first_order_amount
        FROM analytics.orders
        WHERE status = 'completed'
        GROUP BY customer_id
      ),
      order_metrics AS (
        SELECT
          customer_id,
          COUNT(*) AS total_orders,
          SUM(amount) AS total_revenue,
          AVG(amount) AS avg_order_value,
          MAX(created_at) AS last_order_date
        FROM analytics.orders
        WHERE status = 'completed'
        GROUP BY customer_id
      )
      SELECT
        f.customer_id,
        f.first_order_date,
        f.first_order_amount,
        DATE_TRUNC(f.first_order_date, MONTH) AS cohort_month,
        m.total_orders,
        m.total_revenue,
        m.avg_order_value,
        m.last_order_date,
        DATE_DIFF(m.last_order_date, f.first_order_date, DAY) AS customer_lifespan_days
      FROM first_orders f
      JOIN order_metrics m ON f.customer_id = m.customer_id
    ;;
    datagroup_trigger: daily_etl
    indexes: ["customer_id", "cohort_month"]
  }

  dimension: customer_id {
    type: number
    primary_key: yes
    sql: ${TABLE}.customer_id ;;
  }

  dimension: first_order_amount {
    type: number
    sql: ${TABLE}.first_order_amount ;;
    value_format_name: usd
  }

  dimension_group: first_order {
    type: time
    timeframes: [date, week, month, quarter, year]
    sql: ${TABLE}.first_order_date ;;
  }

  dimension_group: cohort {
    type: time
    timeframes: [month, quarter, year]
    sql: ${TABLE}.cohort_month ;;
  }

  dimension_group: last_order {
    type: time
    timeframes: [date, week, month]
    sql: ${TABLE}.last_order_date ;;
  }

  dimension: total_orders {
    type: number
    sql: ${TABLE}.total_orders ;;
  }

  dimension: total_revenue {
    type: number
    sql: ${TABLE}.total_revenue ;;
    value_format_name: usd
  }

  dimension: avg_order_value {
    type: number
    sql: ${TABLE}.avg_order_value ;;
    value_format_name: usd
  }

  dimension: customer_lifespan_days {
    type: number
    sql: ${TABLE}.customer_lifespan_days ;;
  }

  dimension: customer_lifespan_tier {
    type: tier
    tiers: [0, 30, 90, 180, 365]
    style: integer
    sql: ${customer_lifespan_days} ;;
  }

  measure: count {
    type: count
    description: "Number of customers"
  }

  measure: avg_total_orders {
    type: average
    sql: ${total_orders} ;;
  }

  measure: avg_total_revenue {
    type: average
    sql: ${total_revenue} ;;
    value_format_name: usd
  }

  measure: avg_customer_lifespan {
    type: average
    sql: ${customer_lifespan_days} ;;
    value_format: "0.0"
  }

  measure: total_cohort_revenue {
    type: sum
    sql: ${total_revenue} ;;
    value_format_name: usd
  }
}

# View referencing another view's SQL_TABLE_NAME
view: order_facts {
  derived_table: {
    sql:
      SELECT
        o.id AS order_id,
        o.customer_id,
        o.amount,
        o.created_at,
        c.cohort_month,
        c.total_orders AS customer_total_orders,
        c.total_revenue AS customer_total_revenue,
        o.amount / NULLIF(c.total_revenue, 0) AS pct_of_customer_revenue
      FROM analytics.orders o
      LEFT JOIN ${customer_cohorts.SQL_TABLE_NAME} c
        ON o.customer_id = c.customer_id
    ;;
  }

  dimension: order_id {
    type: number
    primary_key: yes
    sql: ${TABLE}.order_id ;;
  }

  dimension: customer_id {
    type: number
    sql: ${TABLE}.customer_id ;;
  }

  dimension: amount {
    type: number
    sql: ${TABLE}.amount ;;
  }

  dimension_group: created {
    type: time
    timeframes: [date, week, month]
    sql: ${TABLE}.created_at ;;
  }

  dimension_group: cohort {
    type: time
    timeframes: [month, quarter, year]
    sql: ${TABLE}.cohort_month ;;
  }

  dimension: customer_total_orders {
    type: number
    sql: ${TABLE}.customer_total_orders ;;
  }

  dimension: customer_total_revenue {
    type: number
    sql: ${TABLE}.customer_total_revenue ;;
    value_format_name: usd
  }

  dimension: pct_of_customer_revenue {
    type: number
    sql: ${TABLE}.pct_of_customer_revenue ;;
    value_format: "0.0\%"
  }

  measure: count {
    type: count
  }

  measure: total_amount {
    type: sum
    sql: ${amount} ;;
  }
}
