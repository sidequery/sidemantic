# Edge Cases: Complex Filter Syntax
# Tests various filter patterns that are common in real LookML

view: filter_edge_cases {
  sql_table_name: analytics.transactions ;;
  description: "Tests complex filter syntax patterns"

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: amount {
    type: number
    sql: ${TABLE}.amount ;;
  }

  dimension: quantity {
    type: number
    sql: ${TABLE}.quantity ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: category {
    type: string
    sql: ${TABLE}.category ;;
  }

  dimension: region {
    type: string
    sql: ${TABLE}.region ;;
  }

  dimension: is_premium {
    type: yesno
    sql: ${TABLE}.is_premium ;;
  }

  dimension: discount_pct {
    type: number
    sql: ${TABLE}.discount_pct ;;
  }

  dimension_group: created {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.created_at ;;
  }

  # Basic measures
  measure: count {
    type: count
  }

  measure: total_amount {
    type: sum
    sql: ${amount} ;;
  }

  measure: total_quantity {
    type: sum
    sql: ${quantity} ;;
  }

  # Numeric comparison filters
  measure: high_value_count {
    type: count
    filters: [amount: ">1000"]
    description: "Count where amount > 1000"
  }

  measure: low_value_count {
    type: count
    filters: [amount: "<100"]
    description: "Count where amount < 100"
  }

  measure: mid_range_count {
    type: count
    filters: [amount: ">=100", amount: "<=1000"]
    description: "Count where 100 <= amount <= 1000"
  }

  measure: non_zero_count {
    type: count
    filters: [amount: "!=0"]
    description: "Count where amount is not zero"
  }

  measure: not_null_count {
    type: count
    filters: [amount: "-NULL"]
    description: "Count where amount is not null"
  }

  # String filters
  measure: completed_count {
    type: count
    filters: [status: "completed"]
  }

  measure: not_cancelled_count {
    type: count
    filters: [status: "-cancelled"]
    description: "Count where status is not cancelled"
  }

  measure: pending_or_processing_count {
    type: count
    filters: [status: "pending,processing"]
    description: "Count where status is pending OR processing"
  }

  measure: specific_categories_count {
    type: count
    filters: [category: "electronics,clothing,home"]
    description: "Count for specific categories"
  }

  # Wildcard/pattern filters
  measure: a_region_count {
    type: count
    filters: [region: "A%"]
    description: "Count where region starts with A"
  }

  measure: contains_west_count {
    type: count
    filters: [region: "%west%"]
    description: "Count where region contains 'west'"
  }

  # Boolean filters
  measure: premium_count {
    type: count
    filters: [is_premium: "yes"]
  }

  measure: non_premium_count {
    type: count
    filters: [is_premium: "no"]
  }

  # Multiple filters (AND condition)
  measure: high_value_premium {
    type: sum
    sql: ${amount} ;;
    filters: [amount: ">500", is_premium: "yes"]
    description: "Premium transactions over $500"
  }

  measure: completed_electronics {
    type: count
    filters: [status: "completed", category: "electronics"]
  }

  measure: q1_large_orders {
    type: count
    filters: [created_quarter: "Q1", amount: ">1000"]
  }

  # Numeric range patterns
  measure: discount_applied {
    type: count
    filters: [discount_pct: ">0"]
    description: "Transactions with discount"
  }

  measure: full_price {
    type: count
    filters: [discount_pct: "0"]
    description: "Full price transactions"
  }

  measure: heavy_discount {
    type: count
    filters: [discount_pct: ">=20"]
    description: "20%+ discount transactions"
  }

  # Complex derived measures with filter logic
  measure: premium_conversion_rate {
    type: number
    sql: 1.0 * ${premium_count} / NULLIF(${count}, 0) ;;
    description: "Percentage of premium transactions"
  }

  measure: completion_rate {
    type: number
    sql: 1.0 * ${completed_count} / NULLIF(${count}, 0) ;;
    description: "Order completion rate"
  }

  measure: high_value_share {
    type: number
    sql: 1.0 * ${high_value_count} / NULLIF(${count}, 0) ;;
    description: "Share of high-value transactions"
  }

  # Filters with negative operators
  measure: excluding_cancelled_amount {
    type: sum
    sql: ${amount} ;;
    filters: [status: "-cancelled,-refunded"]
    description: "Amount excluding cancelled and refunded"
  }

  # Time-based filters (using dimension values)
  measure: recent_transactions {
    type: count
    filters: [created_date: "last 30 days"]
    description: "Transactions in last 30 days"
  }

  measure: ytd_transactions {
    type: count
    filters: [created_date: "this year"]
    description: "Year to date transactions"
  }

  # Segments
  filter: high_value {
    sql: ${TABLE}.amount >= 1000 ;;
  }

  filter: premium_segment {
    sql: ${TABLE}.is_premium = TRUE ;;
  }

  filter: active_period {
    sql: ${TABLE}.created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) ;;
  }

  filter: successful_transactions {
    sql: ${TABLE}.status IN ('completed', 'shipped', 'delivered') ;;
  }
}

# View testing special filter edge cases
view: special_filter_cases {
  sql_table_name: analytics.edge_data ;;

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: value {
    type: number
    sql: ${TABLE}.value ;;
  }

  dimension: text_field {
    type: string
    sql: ${TABLE}.text_field ;;
  }

  dimension: nullable_field {
    type: string
    sql: ${TABLE}.nullable_field ;;
  }

  # Null handling
  measure: null_values {
    type: count
    filters: [nullable_field: "NULL"]
    description: "Count of null values"
  }

  measure: not_null_values {
    type: count
    filters: [nullable_field: "-NULL"]
    description: "Count of non-null values"
  }

  # Empty string handling
  measure: empty_string_count {
    type: count
    filters: [text_field: "EMPTY"]
    description: "Count of empty strings"
  }

  measure: not_empty_count {
    type: count
    filters: [text_field: "-EMPTY"]
    description: "Count of non-empty strings"
  }

  # Special characters in filter values
  measure: contains_special {
    type: count
    filters: [text_field: "%@%"]
    description: "Contains @ symbol"
  }

  measure: count {
    type: count
  }

  measure: total_value {
    type: sum
    sql: ${value} ;;
  }
}
