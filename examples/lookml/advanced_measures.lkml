# Advanced Measures LookML Example
# Demonstrates: Ratio metrics, derived measures, multiple filters, count_distinct

view: sales_analytics {
  sql_table_name: analytics.sales ;;
  description: "Sales data with advanced metric calculations"

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

  dimension: sale_amount {
    type: number
    sql: ${TABLE}.sale_amount ;;
  }

  dimension: cost_amount {
    type: number
    sql: ${TABLE}.cost_amount ;;
  }

  dimension: region {
    type: string
    sql: ${TABLE}.region ;;
  }

  dimension: channel {
    type: string
    sql: ${TABLE}.channel ;;
    description: "Sales channel: online, retail, wholesale"
  }

  dimension_group: sale {
    type: time
    timeframes: [time, date, week, month, quarter, year]
    sql: ${TABLE}.sale_date ;;
  }

  # Basic measures
  measure: count {
    type: count
    description: "Total number of sales"
  }

  measure: unique_customers {
    type: count_distinct
    sql: ${customer_id} ;;
    description: "Number of unique customers"
  }

  measure: unique_products {
    type: count_distinct
    sql: ${product_id} ;;
    description: "Number of unique products sold"
  }

  measure: total_sales {
    type: sum
    sql: ${sale_amount} ;;
    description: "Total sales amount"
    value_format_name: usd
  }

  measure: total_cost {
    type: sum
    sql: ${cost_amount} ;;
    description: "Total cost"
    value_format_name: usd
  }

  measure: avg_sale_amount {
    type: average
    sql: ${sale_amount} ;;
    description: "Average sale amount"
    value_format_name: usd
  }

  measure: min_sale_amount {
    type: min
    sql: ${sale_amount} ;;
    description: "Minimum sale amount"
  }

  measure: max_sale_amount {
    type: max
    sql: ${sale_amount} ;;
    description: "Maximum sale amount"
  }

  # Filtered measures
  measure: online_sales {
    type: sum
    sql: ${sale_amount} ;;
    filters: [channel: "online"]
    description: "Sales from online channel"
  }

  measure: retail_sales {
    type: sum
    sql: ${sale_amount} ;;
    filters: [channel: "retail"]
    description: "Sales from retail channel"
  }

  measure: wholesale_sales {
    type: sum
    sql: ${sale_amount} ;;
    filters: [channel: "wholesale"]
    description: "Sales from wholesale channel"
  }

  measure: online_count {
    type: count
    filters: [channel: "online"]
    description: "Number of online sales"
  }

  measure: large_sales_count {
    type: count
    filters: [sale_amount: ">1000"]
    description: "Number of sales over $1000"
  }

  measure: small_sales_count {
    type: count
    filters: [sale_amount: "<=100"]
    description: "Number of sales $100 or less"
  }

  # Multiple filter measures
  measure: online_large_sales {
    type: sum
    sql: ${sale_amount} ;;
    filters: [channel: "online", sale_amount: ">1000"]
    description: "Large sales from online channel"
  }

  # Derived/calculated measures (ratio metrics)
  measure: gross_profit {
    type: number
    sql: ${total_sales} - ${total_cost} ;;
    description: "Total gross profit"
    value_format_name: usd
  }

  measure: profit_margin {
    type: number
    sql: 100.0 * (${total_sales} - ${total_cost}) / NULLIF(${total_sales}, 0) ;;
    description: "Profit margin percentage"
    value_format_name: percent_1
  }

  measure: avg_profit_per_sale {
    type: number
    sql: (${total_sales} - ${total_cost}) / NULLIF(${count}, 0) ;;
    description: "Average profit per sale"
    value_format_name: usd
  }

  measure: customer_acquisition_efficiency {
    type: number
    sql: ${total_sales} / NULLIF(${unique_customers}, 0) ;;
    description: "Average sales per customer"
    value_format_name: usd
  }

  measure: online_channel_mix {
    type: number
    sql: 100.0 * ${online_sales} / NULLIF(${total_sales}, 0) ;;
    description: "Percentage of sales from online channel"
    value_format_name: percent_1
  }

  measure: avg_transaction_size {
    type: number
    sql: ${total_sales} / NULLIF(${count}, 0) ;;
    description: "Average transaction size"
    value_format_name: usd
  }

  # Segments
  filter: high_value {
    sql: ${TABLE}.sale_amount >= 1000 ;;
    description: "High value sales"
  }

  filter: online_channel {
    sql: ${TABLE}.channel = 'online' ;;
    description: "Online channel sales"
  }

  filter: profitable {
    sql: ${TABLE}.sale_amount > ${TABLE}.cost_amount ;;
    description: "Profitable sales"
  }
}

view: marketing_campaigns {
  sql_table_name: analytics.campaigns ;;
  description: "Marketing campaign performance"

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: campaign_name {
    type: string
    sql: ${TABLE}.campaign_name ;;
  }

  dimension: spend {
    type: number
    sql: ${TABLE}.spend ;;
  }

  dimension: impressions {
    type: number
    sql: ${TABLE}.impressions ;;
  }

  dimension: clicks {
    type: number
    sql: ${TABLE}.clicks ;;
  }

  dimension: conversions {
    type: number
    sql: ${TABLE}.conversions ;;
  }

  dimension_group: campaign {
    type: time
    timeframes: [date, week, month, year]
    sql: ${TABLE}.campaign_date ;;
  }

  measure: total_spend {
    type: sum
    sql: ${spend} ;;
    description: "Total marketing spend"
    value_format_name: usd
  }

  measure: total_impressions {
    type: sum
    sql: ${impressions} ;;
    description: "Total impressions"
  }

  measure: total_clicks {
    type: sum
    sql: ${clicks} ;;
    description: "Total clicks"
  }

  measure: total_conversions {
    type: sum
    sql: ${conversions} ;;
    description: "Total conversions"
  }

  measure: click_through_rate {
    type: number
    sql: 100.0 * ${total_clicks} / NULLIF(${total_impressions}, 0) ;;
    description: "Click-through rate percentage"
    value_format_name: percent_2
  }

  measure: conversion_rate {
    type: number
    sql: 100.0 * ${total_conversions} / NULLIF(${total_clicks}, 0) ;;
    description: "Conversion rate percentage"
    value_format_name: percent_2
  }

  measure: cost_per_click {
    type: number
    sql: ${total_spend} / NULLIF(${total_clicks}, 0) ;;
    description: "Average cost per click"
    value_format_name: usd
  }

  measure: cost_per_conversion {
    type: number
    sql: ${total_spend} / NULLIF(${total_conversions}, 0) ;;
    description: "Average cost per conversion"
    value_format_name: usd
  }

  measure: campaign_count {
    type: count
    description: "Number of campaigns"
  }

  measure: unique_campaigns {
    type: count_distinct
    sql: ${campaign_name} ;;
    description: "Number of unique campaign names"
  }
}
