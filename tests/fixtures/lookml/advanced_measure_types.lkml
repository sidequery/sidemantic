# Advanced LookML measure types and dimension_group timeframes.
# Demonstrates:
#   - Distinct aggregate measures (sum_distinct, average_distinct,
#     median_distinct, percentile_distinct) honoring sql_distinct_key.
#   - Post-SQL / table-calculation measures (running_total, percent_of_total,
#     percent_of_previous).
#   - Non-standard / fiscal dimension_group timeframes (fiscal_quarter,
#     fiscal_month_num, day_of_week, day_of_week_index, month_name, month_num,
#     week, week_of_year, quarter_of_year, hour_of_day, day_of_month, etc.).

view: order_lines {
  sql_table_name: analytics.order_lines ;;
  description: "Denormalized order lines (joins fan out order_id)"

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: order_id {
    type: number
    sql: ${TABLE}.order_id ;;
  }

  dimension: order_amount {
    type: number
    sql: ${TABLE}.order_amount ;;
  }

  dimension: line_amount {
    type: number
    sql: ${TABLE}.line_amount ;;
  }

  # Distinct aggregate measures. sql_distinct_key dedupes the fanned-out rows.
  measure: total_order_amount {
    type: sum_distinct
    sql_distinct_key: ${order_id} ;;
    sql: ${order_amount} ;;
    description: "Sum of order amounts without double counting joined rows"
    value_format_name: usd
  }

  measure: avg_order_amount {
    type: average_distinct
    sql_distinct_key: ${order_id} ;;
    sql: ${order_amount} ;;
    description: "Average order amount across distinct orders"
  }

  measure: median_order_amount {
    type: median_distinct
    sql_distinct_key: ${order_id} ;;
    sql: ${order_amount} ;;
    description: "Median order amount across distinct orders"
  }

  measure: p90_order_amount {
    type: percentile_distinct
    percentile: 90
    sql_distinct_key: ${order_id} ;;
    sql: ${order_amount} ;;
    description: "90th percentile order amount across distinct orders"
  }

  # Distinct measure without an explicit sql_distinct_key.
  measure: sum_distinct_line_amount {
    type: sum_distinct
    sql: ${line_amount} ;;
  }

  # Base measure used by the post-SQL measures below.
  measure: total_line_amount {
    type: sum
    sql: ${line_amount} ;;
    value_format_name: usd
  }

  # Post-SQL / table-calculation measures referencing a base measure.
  measure: running_line_amount {
    type: running_total
    sql: ${total_line_amount} ;;
    description: "Cumulative line amount"
    value_format_name: usd
  }

  measure: pct_of_total_line_amount {
    type: percent_of_total
    sql: ${total_line_amount} ;;
    description: "Each row's share of the total line amount"
    value_format_name: percent_1
  }

  measure: pct_of_previous_line_amount {
    type: percent_of_previous
    sql: ${total_line_amount} ;;
    description: "Change vs the previous row"
    value_format_name: percent_1
  }
}

view: events_calendar {
  sql_table_name: analytics.events ;;
  description: "Event timestamps with fiscal and non-standard timeframes"

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  # Mix of time-truncation and extracted-part timeframes, including fiscal ones.
  dimension_group: occurred {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year,
      fiscal_quarter,
      fiscal_year,
      fiscal_month_num,
      fiscal_quarter_of_year,
      day_of_week,
      day_of_week_index,
      month_name,
      month_num,
      week_of_year,
      quarter_of_year,
      hour_of_day,
      day_of_month,
      day_of_year
    ]
    sql: ${TABLE}.occurred_at ;;
  }

  measure: count {
    type: count
  }
}
