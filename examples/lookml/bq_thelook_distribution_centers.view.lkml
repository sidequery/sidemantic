# Source: https://github.com/looker/bq_thelook
# Real-world LookML example with geographic dimensions
# Features: latitude/longitude, drill_fields

view: distribution_centers {
  sql_table_name: thelook_web_analytics.distribution_centers ;;

  dimension: id {primary_key:yes  type:number}
  dimension: latitude {type:number}
  dimension: longitude {type:number}
  dimension: name {}
  measure: count {type:count  drill_fields:[id, name, products.count]}
}
