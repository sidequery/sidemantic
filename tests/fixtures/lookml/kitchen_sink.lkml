# Kitchen Sink LookML Example
# Comprehensive multi-entity data model for integration testing
# Uses authentic LookML syntax with dimension references (${dim_name})
# This tests sidemantic's ability to resolve LookML's internal references

view: regions {
  sql_table_name: analytics.regions ;;
  description: "Geographic regions for sales territories"

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
    description: "Region name"
  }

  dimension: country {
    type: string
    sql: ${TABLE}.country ;;
  }

  measure: count {
    type: count
    description: "Number of regions"
  }
}

view: categories {
  sql_table_name: analytics.categories ;;
  description: "Product categories hierarchy"

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
    description: "Category name"
  }

  dimension: parent_id {
    type: number
    sql: ${TABLE}.parent_id ;;
    description: "Parent category for hierarchical structure"
  }

  measure: count {
    type: count
    description: "Number of categories"
  }
}

view: customers {
  sql_table_name: analytics.customers ;;
  description: "Customer master data with regional assignment"

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: region_id {
    type: number
    sql: ${TABLE}.region_id ;;
    description: "Foreign key to regions"
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: tier {
    type: string
    sql: ${TABLE}.tier ;;
    description: "Customer tier: bronze, silver, gold, platinum"
  }

  dimension: lifetime_value {
    type: number
    sql: ${TABLE}.lifetime_value ;;
    description: "Customer lifetime value"
  }

  dimension_group: registered {
    type: time
    timeframes: [date, week, month, quarter, year]
    sql: ${TABLE}.registered_at ;;
    description: "Customer registration date"
  }

  measure: count {
    type: count
    description: "Total number of customers"
  }

  # Uses ${id} reference - tests dimension reference resolution
  measure: unique_count {
    type: count_distinct
    sql: ${id} ;;
    description: "Distinct customer count"
  }

  # Uses ${lifetime_value} reference
  measure: avg_lifetime_value {
    type: average
    sql: ${lifetime_value} ;;
    description: "Average customer lifetime value"
  }

  measure: total_lifetime_value {
    type: sum
    sql: ${lifetime_value} ;;
    description: "Sum of all customer lifetime values"
  }

  measure: gold_customers {
    type: count
    filters: [tier: "gold"]
    description: "Number of gold tier customers"
  }

  measure: platinum_customers {
    type: count
    filters: [tier: "platinum"]
    description: "Number of platinum tier customers"
  }

  filter: premium_tier {
    sql: ${TABLE}.tier IN ('gold', 'platinum') ;;
    description: "Premium tier customers only"
  }

  filter: high_value {
    sql: ${TABLE}.lifetime_value >= 1000 ;;
    description: "High value customers with LTV >= 1000"
  }
}

view: products {
  sql_table_name: analytics.products ;;
  description: "Product catalog with pricing and categorization"

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: category_id {
    type: number
    sql: ${TABLE}.category_id ;;
    description: "Foreign key to categories"
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: sku {
    type: string
    sql: ${TABLE}.sku ;;
  }

  dimension: price {
    type: number
    sql: ${TABLE}.price ;;
  }

  dimension: cost {
    type: number
    sql: ${TABLE}.cost ;;
  }

  dimension: is_active {
    type: yesno
    sql: ${TABLE}.is_active ;;
    description: "Whether product is currently active"
  }

  dimension_group: created {
    type: time
    timeframes: [date, month, year]
    sql: ${TABLE}.created_at ;;
  }

  measure: count {
    type: count
    description: "Number of products"
  }

  measure: active_products {
    type: count
    filters: [is_active: "yes"]
    description: "Number of active products"
  }

  # Uses ${price} reference
  measure: avg_price {
    type: average
    sql: ${price} ;;
  }

  measure: min_price {
    type: min
    sql: ${price} ;;
  }

  measure: max_price {
    type: max
    sql: ${price} ;;
  }

  measure: total_inventory_value {
    type: sum
    sql: ${price} ;;
    description: "Total value at retail price"
  }

  measure: total_cost {
    type: sum
    sql: ${cost} ;;
    description: "Total cost value"
  }

  # Derived measure referencing other measures - tests measure-to-measure references
  measure: avg_margin {
    type: number
    sql: (${total_inventory_value} - ${total_cost}) / NULLIF(${total_inventory_value}, 0) * 100 ;;
    description: "Average margin percentage"
  }

  filter: expensive {
    sql: ${TABLE}.price >= 100 ;;
    description: "Products priced at $100 or more"
  }
}

view: orders {
  sql_table_name: analytics.orders ;;
  description: "Customer orders with full lifecycle tracking"

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: customer_id {
    type: number
    sql: ${TABLE}.customer_id ;;
    description: "Foreign key to customers"
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
    description: "Order status: pending, processing, shipped, delivered, cancelled, refunded"
  }

  dimension: channel {
    type: string
    sql: ${TABLE}.channel ;;
    description: "Sales channel: web, mobile, store, phone"
  }

  dimension: subtotal {
    type: number
    sql: ${TABLE}.subtotal ;;
  }

  dimension: tax {
    type: number
    sql: ${TABLE}.tax ;;
  }

  dimension: shipping {
    type: number
    sql: ${TABLE}.shipping ;;
  }

  dimension: discount {
    type: number
    sql: ${TABLE}.discount ;;
  }

  dimension: total {
    type: number
    sql: ${TABLE}.total ;;
  }

  dimension: is_first_order {
    type: yesno
    sql: ${TABLE}.is_first_order ;;
    description: "Whether this is the customer's first order"
  }

  dimension_group: created {
    type: time
    timeframes: [time, date, week, month, quarter, year]
    sql: ${TABLE}.created_at ;;
    description: "Order creation timestamp"
  }

  dimension_group: shipped {
    type: time
    timeframes: [date, week, month]
    sql: ${TABLE}.shipped_at ;;
    description: "Order ship date"
  }

  dimension_group: delivered {
    type: time
    timeframes: [date, week, month]
    sql: ${TABLE}.delivered_at ;;
    description: "Order delivery date"
  }

  # Basic aggregations
  measure: count {
    type: count
    description: "Total number of orders"
  }

  # Uses ${total} dimension reference
  measure: total_revenue {
    type: sum
    sql: ${total} ;;
    description: "Total revenue from orders"
    value_format_name: usd
  }

  measure: total_subtotal {
    type: sum
    sql: ${subtotal} ;;
  }

  measure: total_tax {
    type: sum
    sql: ${tax} ;;
  }

  measure: total_shipping {
    type: sum
    sql: ${shipping} ;;
  }

  measure: total_discount {
    type: sum
    sql: ${discount} ;;
  }

  measure: avg_order_value {
    type: average
    sql: ${total} ;;
    description: "Average order value"
  }

  measure: min_order_value {
    type: min
    sql: ${total} ;;
  }

  measure: max_order_value {
    type: max
    sql: ${total} ;;
  }

  # Filtered measures by status
  measure: delivered_orders {
    type: count
    filters: [status: "delivered"]
    description: "Number of delivered orders"
  }

  measure: delivered_revenue {
    type: sum
    sql: ${total} ;;
    filters: [status: "delivered"]
    description: "Revenue from delivered orders"
  }

  measure: cancelled_orders {
    type: count
    filters: [status: "cancelled"]
  }

  measure: refunded_orders {
    type: count
    filters: [status: "refunded"]
  }

  measure: pending_orders {
    type: count
    filters: [status: "pending"]
  }

  # Filtered measures by channel
  measure: web_orders {
    type: count
    filters: [channel: "web"]
  }

  measure: mobile_orders {
    type: count
    filters: [channel: "mobile"]
  }

  measure: web_revenue {
    type: sum
    sql: ${total} ;;
    filters: [channel: "web"]
    description: "Revenue from web channel"
  }

  # Multi-filter measures
  measure: delivered_web_revenue {
    type: sum
    sql: ${total} ;;
    filters: [status: "delivered", channel: "web"]
    description: "Revenue from delivered web orders"
  }

  measure: first_orders {
    type: count
    filters: [is_first_order: "yes"]
    description: "Number of first-time orders"
  }

  measure: first_order_revenue {
    type: sum
    sql: ${total} ;;
    filters: [is_first_order: "yes"]
    description: "Revenue from first-time orders"
  }

  # Count distinct using dimension reference
  measure: unique_customers {
    type: count_distinct
    sql: ${customer_id} ;;
    description: "Number of unique customers who ordered"
  }

  measure: unique_channels {
    type: count_distinct
    sql: ${channel} ;;
    description: "Number of distinct sales channels used"
  }

  # Derived measures referencing other measures - tests measure-to-measure resolution
  measure: delivery_rate {
    type: number
    sql: 100.0 * ${delivered_orders} / NULLIF(${count}, 0) ;;
    description: "Percentage of orders that were delivered"
  }

  measure: cancellation_rate {
    type: number
    sql: 100.0 * ${cancelled_orders} / NULLIF(${count}, 0) ;;
    description: "Percentage of orders that were cancelled"
  }

  measure: avg_discount_pct {
    type: number
    sql: 100.0 * ${total_discount} / NULLIF(${total_subtotal}, 0) ;;
    description: "Average discount as percentage of subtotal"
  }

  measure: repeat_customer_rate {
    type: number
    sql: 100.0 * (${count} - ${first_orders}) / NULLIF(${count}, 0) ;;
    description: "Percentage of orders from repeat customers"
  }

  # Segments
  filter: completed {
    sql: ${TABLE}.status IN ('shipped', 'delivered') ;;
    description: "Orders that are shipped or delivered"
  }

  filter: high_value {
    sql: ${TABLE}.total >= 500 ;;
    description: "High value orders ($500+)"
  }

  filter: discounted {
    sql: ${TABLE}.discount > 0 ;;
    description: "Orders with discount applied"
  }
}

view: order_items {
  sql_table_name: analytics.order_items ;;
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
  }

  dimension: unit_price {
    type: number
    sql: ${TABLE}.unit_price ;;
  }

  dimension: line_discount {
    type: number
    sql: ${TABLE}.line_discount ;;
  }

  # Calculated dimension referencing other dimensions
  dimension: line_total {
    type: number
    sql: ${quantity} * ${unit_price} - ${line_discount} ;;
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

  # References calculated dimension
  measure: total_line_revenue {
    type: sum
    sql: ${line_total} ;;
    description: "Total line item revenue"
  }

  measure: total_line_discounts {
    type: sum
    sql: ${line_discount} ;;
  }

  measure: avg_quantity_per_line {
    type: average
    sql: ${quantity} ;;
  }

  measure: avg_unit_price {
    type: average
    sql: ${unit_price} ;;
  }

  measure: distinct_products_sold {
    type: count_distinct
    sql: ${product_id} ;;
    description: "Number of unique products sold"
  }

  measure: distinct_orders {
    type: count_distinct
    sql: ${order_id} ;;
    description: "Number of unique orders with items"
  }

  # Derived measure referencing other measures
  measure: avg_items_per_order {
    type: number
    sql: 1.0 * ${count} / NULLIF(${distinct_orders}, 0) ;;
    description: "Average line items per order"
  }
}

view: shipments {
  sql_table_name: analytics.shipments ;;
  description: "Shipment tracking for orders"

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

  dimension: carrier {
    type: string
    sql: ${TABLE}.carrier ;;
    description: "Shipping carrier: ups, fedex, usps, dhl"
  }

  dimension: tracking_number {
    type: string
    sql: ${TABLE}.tracking_number ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
    description: "Shipment status: pending, in_transit, delivered, returned"
  }

  dimension: weight {
    type: number
    sql: ${TABLE}.weight ;;
    description: "Package weight in pounds"
  }

  dimension: cost {
    type: number
    sql: ${TABLE}.cost ;;
    description: "Shipping cost"
  }

  dimension_group: shipped {
    type: time
    timeframes: [date, week, month]
    sql: ${TABLE}.shipped_at ;;
  }

  dimension_group: delivered {
    type: time
    timeframes: [date, week, month]
    sql: ${TABLE}.delivered_at ;;
  }

  measure: count {
    type: count
    description: "Number of shipments"
  }

  measure: total_shipping_cost {
    type: sum
    sql: ${cost} ;;
    description: "Total shipping costs"
  }

  measure: avg_shipping_cost {
    type: average
    sql: ${cost} ;;
  }

  measure: total_weight {
    type: sum
    sql: ${weight} ;;
    description: "Total weight shipped"
  }

  measure: avg_weight {
    type: average
    sql: ${weight} ;;
  }

  measure: delivered_shipments {
    type: count
    filters: [status: "delivered"]
  }

  measure: returned_shipments {
    type: count
    filters: [status: "returned"]
  }

  measure: distinct_orders_shipped {
    type: count_distinct
    sql: ${order_id} ;;
    description: "Number of unique orders with shipments"
  }

  measure: distinct_carriers_used {
    type: count_distinct
    sql: ${carrier} ;;
    description: "Number of distinct carriers used"
  }

  # Derived measure referencing other measures
  measure: delivery_rate {
    type: number
    sql: 100.0 * ${delivered_shipments} / NULLIF(${count}, 0) ;;
    description: "Shipment delivery rate"
  }

  filter: expedited {
    sql: ${TABLE}.carrier IN ('fedex', 'ups') ;;
    description: "Expedited shipping carriers"
  }
}

view: reviews {
  sql_table_name: analytics.reviews ;;
  description: "Product reviews from customers"

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: product_id {
    type: number
    sql: ${TABLE}.product_id ;;
    description: "Foreign key to products"
  }

  dimension: customer_id {
    type: number
    sql: ${TABLE}.customer_id ;;
    description: "Foreign key to customers"
  }

  dimension: order_id {
    type: number
    sql: ${TABLE}.order_id ;;
    description: "Foreign key to orders"
  }

  dimension: rating {
    type: number
    sql: ${TABLE}.rating ;;
    description: "Rating 1-5"
  }

  dimension: is_verified {
    type: yesno
    sql: ${TABLE}.is_verified ;;
    description: "Whether the reviewer made a verified purchase"
  }

  dimension_group: created {
    type: time
    timeframes: [date, week, month, year]
    sql: ${TABLE}.created_at ;;
  }

  measure: count {
    type: count
    description: "Number of reviews"
  }

  measure: avg_rating {
    type: average
    sql: ${rating} ;;
    description: "Average rating"
  }

  measure: min_rating {
    type: min
    sql: ${rating} ;;
  }

  measure: max_rating {
    type: max
    sql: ${rating} ;;
  }

  measure: verified_reviews {
    type: count
    filters: [is_verified: "yes"]
    description: "Number of verified purchase reviews"
  }

  measure: verified_avg_rating {
    type: average
    sql: ${rating} ;;
    filters: [is_verified: "yes"]
    description: "Average rating from verified purchases"
  }

  measure: five_star_reviews {
    type: count
    filters: [rating: "5"]
    description: "Number of 5-star reviews"
  }

  measure: one_star_reviews {
    type: count
    filters: [rating: "1"]
    description: "Number of 1-star reviews"
  }

  measure: distinct_products_reviewed {
    type: count_distinct
    sql: ${product_id} ;;
  }

  measure: distinct_reviewers {
    type: count_distinct
    sql: ${customer_id} ;;
  }

  # Derived measure referencing other measures
  measure: five_star_rate {
    type: number
    sql: 100.0 * ${five_star_reviews} / NULLIF(${count}, 0) ;;
    description: "Percentage of 5-star reviews"
  }

  filter: positive {
    sql: ${TABLE}.rating >= 4 ;;
    description: "Positive reviews (4+ stars)"
  }

  filter: negative {
    sql: ${TABLE}.rating <= 2 ;;
    description: "Negative reviews (2 or fewer stars)"
  }
}
