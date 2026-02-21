view: orders {
  sql_table_name: orders ;;
  description: "Customer orders with status and revenue tracking (LookML format)"

  dimension: id {
    type: number
    primary_key: yes
    sql: id ;;
  }

  dimension: customer_id {
    type: number
    sql: customer_id ;;
  }

  dimension: product_id {
    type: number
    sql: product_id ;;
  }

  dimension: quantity {
    type: number
    sql: quantity ;;
  }

  dimension: amount {
    type: number
    sql: amount ;;
    value_format_name: usd
  }

  dimension: status {
    type: string
    sql: status ;;
  }

  dimension_group: created {
    type: time
    timeframes: [time, date, week, month, year]
    sql: created_at ;;
  }

  measure: order_count {
    type: count
    description: "Total number of orders"
  }

  measure: total_revenue {
    type: sum
    sql: amount ;;
    description: "Total revenue from all orders"
    value_format_name: usd
  }

  measure: completed_revenue {
    type: sum
    sql: amount ;;
    filters: [status: "completed"]
    description: "Revenue from completed orders only"
  }

  measure: avg_order_value {
    type: average
    sql: amount ;;
    description: "Average order value"
  }

  measure: total_quantity {
    type: sum
    sql: quantity ;;
  }
}
