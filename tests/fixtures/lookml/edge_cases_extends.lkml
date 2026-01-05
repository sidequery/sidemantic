# Edge Cases: View Extends and Refinements
# Tests LookML inheritance patterns that parsers commonly struggle with

# Base view for inheritance testing
view: base_entity {
  sql_table_name: analytics.entities ;;
  description: "Base entity with common fields"

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: is_active {
    type: yesno
    sql: ${TABLE}.is_active ;;
  }

  dimension_group: created {
    type: time
    timeframes: [date, week, month, year]
    sql: ${TABLE}.created_at ;;
  }

  measure: count {
    type: count
    description: "Total count"
  }
}

# View that extends base_entity
view: customers_extended {
  extends: [base_entity]
  sql_table_name: analytics.customers ;;
  description: "Customer entity extending base"

  # Override the name dimension from base
  dimension: name {
    label: "Customer Name"
    sql: CONCAT(${TABLE}.first_name, ' ', ${TABLE}.last_name) ;;
  }

  # Add new customer-specific dimensions
  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: tier {
    type: string
    sql: ${TABLE}.tier ;;
    suggestions: ["bronze", "silver", "gold", "platinum"]
  }

  dimension: lifetime_value {
    type: number
    sql: ${TABLE}.ltv ;;
    value_format_name: usd
  }

  # Add new measures
  measure: total_ltv {
    type: sum
    sql: ${lifetime_value} ;;
    value_format_name: usd
  }

  measure: avg_ltv {
    type: average
    sql: ${lifetime_value} ;;
    value_format_name: usd
  }
}

# Refinement syntax (LookML plus notation)
view: +base_entity {
  # Refinements add to the base view without extending
  dimension: refined_field {
    type: string
    sql: ${TABLE}.refined_field ;;
    description: "Added via refinement"
  }
}

# View with multiple extends
view: multi_extend_view {
  extends: [base_entity]
  sql_table_name: analytics.multi_entities ;;

  dimension: extra_id {
    type: string
    sql: ${TABLE}.external_id ;;
    description: "External system identifier"
  }

  dimension: metadata {
    type: string
    sql: ${TABLE}.metadata ;;
  }

  measure: unique_external_ids {
    type: count_distinct
    sql: ${extra_id} ;;
  }
}

# Abstract view (meant only to be extended, not used directly)
view: abstract_metrics {
  extension: required
  description: "Abstract view containing reusable metric definitions"

  measure: record_count {
    type: count
  }

  measure: sum_amount {
    type: sum
    sql: ${TABLE}.amount ;;
  }

  measure: avg_amount {
    type: average
    sql: ${TABLE}.amount ;;
  }

  measure: min_amount {
    type: min
    sql: ${TABLE}.amount ;;
  }

  measure: max_amount {
    type: max
    sql: ${TABLE}.amount ;;
  }
}

# Concrete view extending abstract
view: transactions {
  extends: [abstract_metrics]
  sql_table_name: analytics.transactions ;;

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: amount {
    type: number
    sql: ${TABLE}.amount ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension_group: transaction {
    type: time
    timeframes: [time, date, week, month, quarter, year]
    sql: ${TABLE}.transaction_at ;;
  }
}
