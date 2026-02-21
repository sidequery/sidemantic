# Source: https://github.com/looker/bq_thelook
# Real-world LookML from BigQuery TheLook's event_sessions and user_order_facts
# Features: native derived tables with explore_source, column mappings,
#   derived_column with window functions, dimension type:tier with tiers,
#   set blocks, filters on measures (old-style block syntax),
#   compact inline dimension syntax

view: event_sessions {
  derived_table: {
    explore_source: events {
      column: session_id { field: events.session_id }
      column: event_types { field: events.event_types }
      column: session_time { field: events.minimum_time }
      column: session_end_time {field: events.max_time}
      column: ip_addresses {field: events.ip_addresses }
      column: user_id {field: events.first_user_id }
      column: product_ids_visited {field: events.product_ids_visited}
      derived_column: session_sequence {
        sql: ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY session_time) ;;
      }
    }
  }

  dimension: session_id {primary_key:yes}
  dimension: event_types {}
  dimension_group: session {type:time sql: ${TABLE}.session_time ;;}
  dimension: session_end_time {hidden:yes}
  dimension: user_id {}
  dimension: session_sequence {type:number}
  dimension: session_length {type:number
    sql: TIMESTAMP_DIFF(${session_end_time},${TABLE}.session_time, SECOND) ;;}
  dimension: session_length_tiered {type:tier tiers: [0,60,120] sql: ${session_length} ;;}

  measure: count_sessions {type:count drill_fields:[session*]}
  measure: count_sessions_with_cart {type:count drill_fields:[session*]
    filters: {
      field: event_types
      value: "%Cart%"
    }
  }
  measure: count_sessions_with_purchases {type:count drill_fields: [session*]
    filters: {
      field: event_types
      value: "%Purchase%"
    }
  }

  set: session {
    fields: [session_time, session_id, user_id, event_types]
  }
}

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

view: bq_thelook_users {
  sql_table_name: thelook_web_analytics.users ;;

  dimension: id {primary_key:yes}
  dimension: age {type:number}
  dimension: city {}
  dimension: country {}
  dimension_group: created {type:time sql:${TABLE}.created_at ;;}
  dimension: email {}
  dimension: first_name {}
  dimension: gender {}
  dimension: last_name {}
  dimension: state {}
  dimension: zip {type: zipcode}

  measure: count {type:count
    drill_fields: [id, last_name, first_name]}
}

view: bq_thelook_order_items {
  sql_table_name: thelook_web_analytics.order_items ;;

  dimension: id {primary_key:yes type:number}
  dimension_group: created {type:time sql: TIMESTAMP(${TABLE}.created_at) ;;}
  dimension: inventory_item_id {type:number}
  dimension: order_id {type:number}
  dimension: sale_price {type: number}
  dimension: status {}
  dimension: user_id {type: number}

  measure: count {type:count drill_fields: [id, created_date, user_id, sale_price]}
  measure: total_revenue {type:sum sql: ${sale_price} ;;}
  measure: order_count {type:count_distinct sql: ${order_id} ;; drill_fields: [order_id, count]}
}
