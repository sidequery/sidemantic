# Source: https://github.com/looker/bq_thelook
# Real-world LookML example with compact syntax (no sql: ${TABLE} when field matches)
# Features: compact dimension definitions, drill_fields

view: inventory_items {
  sql_table_name: thelook_web_analytics.inventory_items ;;

  dimension: id {primary_key:yes type:number}
  dimension: cost {type:number}
  dimension: created_at {}
  dimension: product_brand {}
  dimension: product_category {}
  dimension: product_department {}
  dimension: product_distribution_center_id {type:number}
  dimension: product_id {type:number}
  dimension: product_name {}
  dimension: product_retail_price {type:number}
  dimension: product_sku {}
  dimension: sold_at {}

  measure: count {type:count
    drill_fields:[id, product_name, products.name, products.id, order_items.count]}
}
