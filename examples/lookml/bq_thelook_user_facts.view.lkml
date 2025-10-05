# Source: https://github.com/looker/bq_thelook
# Real-world LookML example demonstrating derived tables for user aggregations
# Features: derived_table with explore_source, lifetime value calculations

include: "order_items.explore"

view: user_order_facts {
  derived_table: {
    explore_source: order_items {
      column: user_id {field:order_items.user_id}
      column: lifetime_revenue {field:order_items.total_revenue}
      column: lifetime_number_of_orders {field:order_items.order_count}
      column: lifetime_product_categories {field:products.category_list}
      column: lifetime_brands {field:products.brand_list}
    }
  }
  dimension: user_id {hidden:yes}
  dimension: lifetime_revenue {type:number}
  dimension: lifetime_number_of_orders {type:number}
  dimension: lifetime_product_categories {}
  dimension: lifetime_brands {}
}
