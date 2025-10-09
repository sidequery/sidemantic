# E-commerce LookML Example
# Demonstrates: Multiple views, relationships, various measure types, dimension groups, filters

view: orders {
  sql_table_name: ecommerce.orders ;;
  description: "Customer orders with complete lifecycle tracking"

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
    description: "Unique order identifier"
  }

  dimension: customer_id {
    type: number
    sql: ${TABLE}.customer_id ;;
    description: "Foreign key to customers table"
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
    description: "Order status: pending, processing, shipped, delivered, cancelled"
  }

  dimension: amount {
    type: number
    sql: ${TABLE}.amount ;;
    description: "Total order amount in USD"
  }

  dimension: discount_amount {
    type: number
    sql: ${TABLE}.discount_amount ;;
    description: "Discount applied to order"
  }

  dimension: shipping_country {
    type: string
    sql: ${TABLE}.shipping_country ;;
    description: "Country code for shipping destination"
  }

  dimension_group: created {
    type: time
    timeframes: [time, date, week, month, quarter, year]
    sql: ${TABLE}.created_at ;;
    description: "When the order was created"
  }

  dimension_group: shipped {
    type: time
    timeframes: [date, week, month, year]
    sql: ${TABLE}.shipped_at ;;
    description: "When the order was shipped"
  }

  # Basic aggregations
  measure: count {
    type: count
    description: "Total number of orders"
  }

  measure: total_revenue {
    type: sum
    sql: ${amount} ;;
    description: "Total revenue from all orders"
    value_format_name: usd
  }

  measure: total_discounts {
    type: sum
    sql: ${discount_amount} ;;
    description: "Total discounts given"
    value_format_name: usd
  }

  measure: avg_order_value {
    type: average
    sql: ${amount} ;;
    description: "Average order value"
    value_format_name: usd
  }

  measure: min_order_value {
    type: min
    sql: ${amount} ;;
    description: "Minimum order value"
    value_format_name: usd
  }

  measure: max_order_value {
    type: max
    sql: ${amount} ;;
    description: "Maximum order value"
    value_format_name: usd
  }

  # Filtered measures
  measure: delivered_orders {
    type: count
    filters: [status: "delivered"]
    description: "Number of delivered orders"
  }

  measure: cancelled_orders {
    type: count
    filters: [status: "cancelled"]
    description: "Number of cancelled orders"
  }

  measure: delivered_revenue {
    type: sum
    sql: ${amount} ;;
    filters: [status: "delivered"]
    description: "Revenue from delivered orders only"
  }

  measure: high_value_orders {
    type: count
    filters: [amount: ">500"]
    description: "Number of orders over $500"
  }

  # Derived measure
  measure: avg_discount_percentage {
    type: number
    sql: 100.0 * ${total_discounts} / NULLIF(${total_revenue}, 0) ;;
    description: "Average discount as percentage of revenue"
    value_format_name: percent_1
  }

  # Segments
  filter: delivered {
    sql: ${TABLE}.status = 'delivered' ;;
    description: "Orders that have been delivered"
  }

  filter: high_value {
    sql: ${TABLE}.amount >= 500 ;;
    description: "High value orders (>= $500)"
  }

  filter: international {
    sql: ${TABLE}.shipping_country != 'US' ;;
    description: "International orders (non-US)"
  }
}

view: customers {
  sql_table_name: ecommerce.customers ;;
  description: "Customer master data"

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
    description: "Unique customer identifier"
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
    description: "Customer email address"
  }

  dimension: first_name {
    type: string
    sql: ${TABLE}.first_name ;;
  }

  dimension: last_name {
    type: string
    sql: ${TABLE}.last_name ;;
  }

  dimension: full_name {
    type: string
    sql: ${first_name} || ' ' || ${last_name} ;;
    description: "Customer full name"
  }

  dimension: country {
    type: string
    sql: ${TABLE}.country ;;
    description: "Customer's country"
  }

  dimension: city {
    type: string
    sql: ${TABLE}.city ;;
    description: "Customer's city"
  }

  dimension_group: registered {
    type: time
    timeframes: [date, month, year]
    sql: ${TABLE}.registered_at ;;
    description: "When customer registered"
  }

  measure: count {
    type: count
    description: "Total number of customers"
  }

  measure: count_active {
    type: count_distinct
    sql: ${id} ;;
    filters: [registered_date: "90 days"]
    description: "Customers who registered in last 90 days"
  }

  filter: us_customers {
    sql: ${TABLE}.country = 'US' ;;
    description: "US based customers"
  }
}

view: products {
  sql_table_name: ecommerce.products ;;
  description: "Product catalog"

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
    description: "Product name"
  }

  dimension: category {
    type: string
    sql: ${TABLE}.category ;;
    description: "Product category"
  }

  dimension: brand {
    type: string
    sql: ${TABLE}.brand ;;
    description: "Product brand"
  }

  dimension: price {
    type: number
    sql: ${TABLE}.price ;;
    description: "Product price"
  }

  dimension: cost {
    type: number
    sql: ${TABLE}.cost ;;
    description: "Product cost"
  }

  measure: count {
    type: count
    description: "Number of products"
  }

  measure: avg_price {
    type: average
    sql: ${price} ;;
    description: "Average product price"
    value_format_name: usd
  }

  measure: total_inventory_value {
    type: sum
    sql: ${price} ;;
    description: "Total inventory value at retail price"
  }
}

view: order_items {
  sql_table_name: ecommerce.order_items ;;
  description: "Individual line items within orders"

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: order_id {
    type: number
    sql: ${TABLE}.order_id ;;
    description: "Foreign key to orders"
  }

  dimension: product_id {
    type: number
    sql: ${TABLE}.product_id ;;
    description: "Foreign key to products"
  }

  dimension: quantity {
    type: number
    sql: ${TABLE}.quantity ;;
    description: "Quantity ordered"
  }

  dimension: unit_price {
    type: number
    sql: ${TABLE}.unit_price ;;
    description: "Price per unit"
  }

  dimension: line_total {
    type: number
    sql: ${quantity} * ${unit_price} ;;
    description: "Total for this line item"
  }

  measure: count {
    type: count
    description: "Number of line items"
  }

  measure: total_quantity {
    type: sum
    sql: ${quantity} ;;
    description: "Total units sold"
  }

  measure: total_line_revenue {
    type: sum
    sql: ${line_total} ;;
    description: "Total revenue from line items"
    value_format_name: usd
  }

  measure: avg_quantity_per_item {
    type: average
    sql: ${quantity} ;;
    description: "Average quantity per line item"
  }

  measure: distinct_products_sold {
    type: count_distinct
    sql: ${product_id} ;;
    description: "Number of unique products sold"
  }
}
