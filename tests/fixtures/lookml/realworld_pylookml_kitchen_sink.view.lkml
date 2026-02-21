# Source: https://github.com/looker-open-source/pylookml/blob/main/lookml/tests/files/kitchenSink/kitchenSink.model.lkml
# Real-world LookML from PyLookML's canonical kitchen sink test
# Features: action blocks with form_param, filter: fields with suggestions,
#   dimension type:tier with tiers/style, type:yesno, measure type:median,
#   measure type:count_distinct, derived measures (type:number),
#   drill_fields with set refs, link blocks, set blocks,
#   value_format_name (usd, percent_2), html parameter with Liquid,
#   multiple dimension_groups, cross-view references, tags, required_access_grants

view: order_items {
  sql_table_name: ecomm.order_items ;;

  filter: cohort_by {
    type: string
    hidden: yes
    suggestions: ["Week","Month","Quarter","Year"]
  }

  filter: metric {
    type: string
    hidden: yes
    suggestions: ["Order Count","Gross Margin","Total Sales","Unique Users"]
  }

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
    tags: ["a","b","c"]
    action: {
      label: "Send this to slack channel"
      url: "https://hooks.zapier.com/hooks/catch/1662138/tvc3zj/"
      form_param: {
        name: "Message"
        type: textarea
        default: "Hey, check out order #{{value}}."
      }
      form_param: {
        name: "Recipient"
        type: select
        default: "zevl"
        option: { name: "zevl" label: "Zev" }
        option: { name: "slackdemo" label: "Slack Demo User" }
      }
    }
  }

  dimension: status {
    sql: ${TABLE}.status ;;
  }

  dimension: days_to_process {
    type: number
    sql: CASE
      WHEN ${status} = 'Processing' THEN DATEDIFF('day',${created_raw},current_date)*1.0
      WHEN ${status} IN ('Shipped', 'Complete', 'Returned') THEN DATEDIFF('day',${created_raw},${shipped_raw})*1.0
      WHEN ${status} = 'Cancelled' THEN NULL
      END ;;
  }

  dimension: sale_price {
    type: number
    value_format_name: usd
    sql: ${TABLE}.sale_price ;;
  }

  dimension: gross_margin {
    type: number
    value_format_name: usd
    sql: ${sale_price} - ${TABLE}.cost ;;
    html: {{sale_price._value}} ;;
  }

  dimension: item_gross_margin_percentage {
    type: number
    sql: ${gross_margin} / NULLIF(${sale_price}, 0) ;;
  }

  dimension: item_gross_margin_percentage_tier {
    type: tier
    sql: 100*${item_gross_margin_percentage} ;;
    tiers: [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
    style: interval
  }

  dimension: is_returned {
    type: yesno
    sql: ${TABLE}.returned_at IS NOT NULL ;;
  }

  dimension_group: created {
    type: time
    timeframes: [time, hour, date, week, month, year, hour_of_day, day_of_week, month_num, raw, week_of_year]
    sql: ${TABLE}.created_at ;;
  }

  dimension_group: shipped {
    type: time
    timeframes: [date, week, month, raw]
    sql: ${TABLE}.shipped_at ;;
  }

  measure: count {
    type: count_distinct
    sql: ${id} ;;
    drill_fields: [detail*]
  }

  measure: order_count {
    type: count_distinct
    sql: ${TABLE}.order_id ;;
  }

  measure: total_sale_price {
    type: sum
    sql: ${sale_price} ;;
    value_format_name: usd
  }

  measure: median_sale_price {
    type: median
    value_format_name: usd
    sql: ${sale_price} ;;
    drill_fields: [detail*]
  }

  measure: total_gross_margin {
    type: sum
    sql: ${gross_margin} ;;
    value_format_name: usd
  }

  measure: total_gross_margin_percentage {
    type: number
    value_format_name: percent_2
    sql: 1.0 * ${total_gross_margin}/ NULLIF(${total_sale_price},0) ;;
  }

  measure: average_spend_per_user {
    type: number
    value_format_name: usd
    sql: 1.0 * ${total_sale_price} / NULLIF(${order_count},0) ;;
    drill_fields: [detail*]
  }

  measure: first_purchase_count {
    view_label: "Orders"
    type: count_distinct
    sql: ${TABLE}.order_id ;;
    drill_fields: [id, created_date]
    link: {
      label: "New User's Behavior by Traffic Source"
      url: "/dashboards/abc"
    }
  }

  set: detail {
    fields: [id, status, created_date, sale_price]
  }
}
