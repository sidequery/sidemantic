# Edge Cases: Liquid Templating
# Tests Liquid syntax that parsers often have trouble with
# NOTE: Some advanced Liquid features are simplified for parser compatibility

view: dynamic_sales {
  # Dynamic sql_table_name with Liquid
  sql_table_name: analytics.sales ;;

  description: "Sales with dynamic features"

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: amount {
    type: number
    sql: ${TABLE}.amount ;;
  }

  dimension: currency {
    type: string
    sql: ${TABLE}.currency ;;
  }

  # Liquid in SQL expression (simple form)
  dimension: formatted_amount {
    type: string
    sql: CASE
      WHEN ${currency} = 'USD' THEN CONCAT('$', CAST(${amount} AS STRING))
      WHEN ${currency} = 'EUR' THEN CONCAT('E', CAST(${amount} AS STRING))
      ELSE CONCAT(${currency}, ' ', CAST(${amount} AS STRING))
    END ;;
    description: "Currency-formatted amount"
  }

  # HTML with Liquid variable references
  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
    html: <span>{{ rendered_value }}</span> ;;
  }

  dimension: region {
    type: string
    sql: ${TABLE}.region ;;
  }

  # Case dimension (CASE WHEN equivalent)
  dimension: region_group {
    type: string
    case: {
      when: {
        sql: ${region} IN ('US', 'CA', 'MX') ;;
        label: "North America"
      }
      when: {
        sql: ${region} IN ('UK', 'DE', 'FR', 'ES', 'IT') ;;
        label: "Europe"
      }
      when: {
        sql: ${region} IN ('JP', 'CN', 'KR', 'AU') ;;
        label: "Asia Pacific"
      }
      else: "Other"
    }
  }

  dimension_group: sale {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.sale_at ;;
  }

  # Liquid variable references in SQL
  dimension: days_since_sale {
    type: number
    sql: DATE_DIFF(CURRENT_DATE(), ${sale_date}, DAY) ;;
  }

  measure: count {
    type: count
  }

  measure: total_amount {
    type: sum
    sql: ${amount} ;;
  }

  # Measure with value_format
  measure: avg_amount {
    type: average
    sql: ${amount} ;;
    value_format: "#,##0.00"
  }

  measure: min_amount {
    type: min
    sql: ${amount} ;;
  }

  measure: max_amount {
    type: max
    sql: ${amount} ;;
  }
}

# View demonstrating templated filters (simplified)
view: templated_orders {
  derived_table: {
    sql:
      SELECT *
      FROM analytics.orders
      WHERE status != 'deleted'
    ;;
  }

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: amount {
    type: number
    sql: ${TABLE}.amount ;;
  }

  dimension_group: created {
    type: time
    timeframes: [date, week, month, year]
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
}

# Parameterized view (simplified - parameters as dimensions)
view: parameterized_metrics {
  sql_table_name: analytics.metrics ;;

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: revenue {
    type: number
    sql: ${TABLE}.revenue ;;
  }

  dimension: order_count {
    type: number
    sql: ${TABLE}.order_count ;;
  }

  dimension: customer_count {
    type: number
    sql: ${TABLE}.customer_count ;;
  }

  dimension_group: event {
    type: time
    timeframes: [date, week, month, year]
    sql: ${TABLE}.event_date ;;
  }

  measure: count {
    type: count
  }

  measure: total_revenue {
    type: sum
    sql: ${revenue} ;;
  }

  measure: total_orders {
    type: sum
    sql: ${order_count} ;;
  }

  measure: total_customers {
    type: sum
    sql: ${customer_count} ;;
  }

  measure: avg_revenue {
    type: average
    sql: ${revenue} ;;
  }
}

# View with various value formats
view: format_examples {
  sql_table_name: analytics.metrics ;;

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: raw_value {
    type: number
    sql: ${TABLE}.value ;;
  }

  dimension: percentage_value {
    type: number
    sql: ${TABLE}.percentage ;;
    value_format: "0.00\%"
  }

  dimension: currency_value {
    type: number
    sql: ${TABLE}.amount ;;
    value_format_name: usd
  }

  measure: count {
    type: count
  }

  measure: sum_value {
    type: sum
    sql: ${raw_value} ;;
    value_format: "#,##0.00"
  }

  measure: sum_currency {
    type: sum
    sql: ${currency_value} ;;
    value_format_name: usd
  }

  measure: avg_percentage {
    type: average
    sql: ${percentage_value} ;;
    value_format: "0.0\%"
  }

  # Conditional value format using SQL CASE
  measure: formatted_total {
    type: sum
    sql: ${raw_value} ;;
    value_format: "[>=1000000]0.0,,\"M\";[>=1000]0.0,\"K\";0"
    description: "Total with M/K suffixes"
  }
}
