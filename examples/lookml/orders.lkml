view: orders {
  sql_table_name: public.orders ;;
  description: "Customer orders with revenue and status tracking"

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
    description: "Order status (pending, completed, cancelled)"
  }

  dimension: customer_id {
    type: number
    sql: ${TABLE}.customer_id ;;
  }

  dimension_group: created {
    type: time
    timeframes: [date, week, month, year]
    sql: ${TABLE}.created_at ;;
  }

  measure: count {
    type: count
    description: "Total number of orders"
  }

  measure: revenue {
    type: sum
    sql: ${TABLE}.amount ;;
    description: "Total revenue from orders"
    value_format_name: usd
  }

  measure: completed_revenue {
    type: sum
    sql: ${TABLE}.amount ;;
    filters: [status: "completed"]
    description: "Revenue from completed orders only"
  }

  measure: avg_order_value {
    type: average
    sql: ${TABLE}.amount ;;
    description: "Average order value"
  }

  measure: conversion_rate {
    type: number
    sql: 1.0 * ${completed_revenue} / NULLIF(${revenue}, 0) ;;
    description: "Percentage of revenue that is completed"
  }

  filter: high_value {
    sql: ${TABLE}.amount >= 500 ;;
    description: "Orders with amount >= $500"
  }

  filter: completed {
    sql: ${TABLE}.status = 'completed' ;;
    description: "Completed orders"
  }
}

view: customers {
  sql_table_name: public.customers ;;

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

  measure: count {
    type: count
  }
}
