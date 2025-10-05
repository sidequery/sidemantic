# Source: https://github.com/looker/bq_thelook
# Real-world LookML example from Looker's BigQuery thelook dataset with web analytics
# Features: aggregated measures, array operations, complex SQL expressions

view: events {
  sql_table_name: thelook_web_analytics.events ;;

  dimension: id {primary_key:yes  type:number}
  dimension: browser {}
  dimension: city {}
  dimension: country {}
  dimension_group: created {type:time  sql: ${TABLE}.created_at ;;}
  dimension: event_type {}
  dimension: ip_address {}
  dimension: latitude {type:number}
  dimension: longitude {type:number}
  dimension: os {}
  dimension: sequence_number {type:number}
  dimension: session_id {}
  dimension: state {}
  dimension: traffic_source {}
  dimension: uri {}
  dimension: user_id {type:number  sql: CAST(REGEXP_EXTRACT(${TABLE}.user_id, r'\d+') AS INT64) ;;}
  dimension: zip {}

  measure: count {type:count  drill_fields:[id, users.last_name, users.id, users.first_name]}

  measure: minimum_time {sql: MIN(${created_raw}) ;;}
  measure: max_time {sql: MAX(${created_raw}) ;;}
  measure: ip_addresses {sql: ARRAY_TO_STRING(ARRAY_AGG(DISTINCT ${ip_address}),'|') ;;}
  measure: event_types {sql: ARRAY_TO_STRING(ARRAY_AGG(DISTINCT ${event_type}),'|') ;;}

  measure: first_user_id {type:min sql: ${user_id} ;;}

  measure: product_ids_visited {
    sql: ARRAY_AGG(DISTINCT
            CAST(REGEXP_EXTRACT(${uri}, r'/product/(\d+)') AS INT64)
          IGNORE NULLS) ;;
  }
}
