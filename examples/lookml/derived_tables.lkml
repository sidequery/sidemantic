view: customer_summary {
  derived_table: {
    sql: SELECT
            customer_id,
            COUNT(*) as order_count,
            SUM(amount) as total_revenue,
            MAX(created_at) as last_order_date
         FROM public.orders
         GROUP BY customer_id ;;
  }

  dimension: customer_id {
    type: number
    primary_key: yes
  }

  dimension: order_count {
    type: number
    sql: ${TABLE}.order_count ;;
  }

  dimension: total_revenue {
    type: number
    sql: ${TABLE}.total_revenue ;;
    value_format_name: usd
  }

  dimension_group: last_order {
    type: time
    timeframes: [date, month, year]
    sql: ${TABLE}.last_order_date ;;
  }

  measure: total_customers {
    type: count
  }

  measure: avg_orders_per_customer {
    type: average
    sql: ${order_count} ;;
  }

  measure: avg_customer_ltv {
    type: average
    sql: ${total_revenue} ;;
  }
}
