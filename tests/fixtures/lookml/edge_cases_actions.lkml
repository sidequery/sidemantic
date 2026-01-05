# Edge Cases: Links, Actions, and Drill Fields
# Tests interactive features that are common in real Looker deployments
# NOTE: Some action/form features simplified for parser compatibility

view: interactive_orders {
  sql_table_name: analytics.orders ;;
  description: "Orders with links, actions, and drill fields"

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
    # Link to external system
    link: {
      label: "View in Admin"
      url: "https://admin.example.com/orders/{{ value }}"
      icon_url: "https://example.com/favicon.ico"
    }
  }

  dimension: customer_id {
    type: number
    sql: ${TABLE}.customer_id ;;
    link: {
      label: "View Customer Profile"
      url: "/dashboards/123?customer_id={{ value }}"
    }
  }

  dimension: payment_id {
    type: string
    sql: ${TABLE}.payment_id ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: amount {
    type: number
    sql: ${TABLE}.amount ;;
    value_format_name: usd
    # HTML formatting
    html:
      <span style="color: green;">{{ rendered_value }}</span>
    ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
    html:
      <span style="padding: 2px 8px;">{{ value }}</span>
    ;;
  }

  dimension: region {
    type: string
    sql: ${TABLE}.region ;;
    # Drill to regional dashboard
    link: {
      label: "View Regional Dashboard"
      url: "/dashboards/456?region={{ value }}"
    }
  }

  dimension: channel {
    type: string
    sql: ${TABLE}.channel ;;
  }

  dimension_group: created {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.created_at ;;
  }

  # Define sets for drill fields
  set: order_details {
    fields: [id, customer_id, email, amount, status, created_date]
  }

  set: customer_info {
    fields: [customer_id, email, region]
  }

  set: revenue_fields {
    fields: [amount, total_revenue, avg_order_value]
  }

  measure: count {
    type: count
    description: "Total orders"
    # Drill into specific fields on click
    drill_fields: [order_details*]
  }

  measure: total_revenue {
    type: sum
    sql: ${amount} ;;
    value_format_name: usd
    description: "Total revenue"
    drill_fields: [order_details*, region, channel]
    # Link to revenue analysis
    link: {
      label: "Revenue Breakdown"
      url: "/dashboards/revenue_analysis?total={{ value }}"
    }
  }

  measure: avg_order_value {
    type: average
    sql: ${amount} ;;
    value_format_name: usd
    drill_fields: [order_details*]
  }

  measure: unique_customers {
    type: count_distinct
    sql: ${customer_id} ;;
    drill_fields: [customer_info*]
  }

  measure: completed_orders {
    type: count
    filters: [status: "completed"]
    drill_fields: [order_details*]
  }

  measure: completion_rate {
    type: number
    sql: 1.0 * ${completed_orders} / NULLIF(${count}, 0) ;;
    value_format_name: percent_1
    drill_fields: [status, count, completed_orders]
  }

  # Filtered measures for different statuses
  measure: pending_orders {
    type: count
    filters: [status: "pending"]
  }

  measure: cancelled_orders {
    type: count
    filters: [status: "cancelled"]
  }

  measure: pending_revenue {
    type: sum
    sql: ${amount} ;;
    filters: [status: "pending"]
  }

  # Measures by channel
  measure: web_orders {
    type: count
    filters: [channel: "web"]
  }

  measure: mobile_orders {
    type: count
    filters: [channel: "mobile"]
  }

  measure: store_orders {
    type: count
    filters: [channel: "store"]
  }

  # Derived measures
  measure: web_share {
    type: number
    sql: 1.0 * ${web_orders} / NULLIF(${count}, 0) ;;
    value_format_name: percent_1
  }

  measure: mobile_share {
    type: number
    sql: 1.0 * ${mobile_orders} / NULLIF(${count}, 0) ;;
    value_format_name: percent_1
  }

  # Segments
  filter: high_value {
    sql: ${TABLE}.amount >= 500 ;;
    description: "High value orders"
  }

  filter: recent_orders {
    sql: ${TABLE}.created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) ;;
    description: "Orders in last 30 days"
  }
}

# View demonstrating links on multiple dimensions
view: linked_products {
  sql_table_name: analytics.products ;;

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
    link: {
      label: "Product Detail"
      url: "/products/{{ value }}"
    }
    link: {
      label: "Edit Product"
      url: "/admin/products/{{ value }}/edit"
    }
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
    link: {
      label: "Search Google"
      url: "https://www.google.com/search?q={{ value }}"
    }
  }

  dimension: category {
    type: string
    sql: ${TABLE}.category ;;
    link: {
      label: "Category Dashboard"
      url: "/dashboards/category?name={{ value }}"
    }
  }

  dimension: brand {
    type: string
    sql: ${TABLE}.brand ;;
  }

  dimension: price {
    type: number
    sql: ${TABLE}.price ;;
    value_format_name: usd
  }

  dimension: cost {
    type: number
    sql: ${TABLE}.cost ;;
    value_format_name: usd
  }

  dimension: sku {
    type: string
    sql: ${TABLE}.sku ;;
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

  set: product_detail {
    fields: [id, name, category, brand, price, sku]
  }

  measure: count {
    type: count
    drill_fields: [product_detail*]
  }

  measure: active_products {
    type: count
    filters: [is_active: "yes"]
    drill_fields: [product_detail*]
  }

  measure: avg_price {
    type: average
    sql: ${price} ;;
    value_format_name: usd
  }

  measure: total_value {
    type: sum
    sql: ${price} ;;
    value_format_name: usd
  }

  measure: avg_margin {
    type: number
    sql: AVG(${price} - ${cost}) ;;
    value_format_name: usd
  }

  measure: margin_pct {
    type: number
    sql: 100.0 * (SUM(${price}) - SUM(${cost})) / NULLIF(SUM(${price}), 0) ;;
    value_format: "0.0\%"
  }
}

# View with extensive drill fields
view: drill_heavy_view {
  sql_table_name: analytics.events ;;

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: event_type {
    type: string
    sql: ${TABLE}.event_type ;;
  }

  dimension: user_id {
    type: number
    sql: ${TABLE}.user_id ;;
  }

  dimension: session_id {
    type: string
    sql: ${TABLE}.session_id ;;
  }

  dimension: page_url {
    type: string
    sql: ${TABLE}.page_url ;;
  }

  dimension: referrer {
    type: string
    sql: ${TABLE}.referrer ;;
  }

  dimension: browser {
    type: string
    sql: ${TABLE}.browser ;;
  }

  dimension: device {
    type: string
    sql: ${TABLE}.device ;;
  }

  dimension: country {
    type: string
    sql: ${TABLE}.country ;;
  }

  dimension_group: created {
    type: time
    timeframes: [raw, time, date, hour, week, month]
    sql: ${TABLE}.created_at ;;
  }

  set: event_details {
    fields: [id, event_type, user_id, session_id, created_time]
  }

  set: user_context {
    fields: [user_id, browser, device, country]
  }

  set: page_context {
    fields: [page_url, referrer]
  }

  measure: count {
    type: count
    drill_fields: [event_details*]
  }

  measure: unique_users {
    type: count_distinct
    sql: ${user_id} ;;
    drill_fields: [user_context*]
  }

  measure: unique_sessions {
    type: count_distinct
    sql: ${session_id} ;;
    drill_fields: [event_details*]
  }

  measure: pageviews {
    type: count
    filters: [event_type: "pageview"]
    drill_fields: [page_context*, created_time]
  }

  measure: clicks {
    type: count
    filters: [event_type: "click"]
  }

  measure: conversions {
    type: count
    filters: [event_type: "conversion"]
  }

  measure: bounce_rate {
    type: number
    sql: 100.0 * COUNT(CASE WHEN ${event_type} = 'bounce' THEN 1 END) / NULLIF(${unique_sessions}, 0) ;;
    value_format: "0.0\%"
  }
}
