# Source: cube-js/lkml2cube (MIT License)
# File: tests/samples/lkml/constants_test.lkml
# Tests: constant interpolation with @{constant_name} syntax

constant: city {
  value: "Okayama"
}

constant: country {
  value: "Japan"
}

explore: users {
  label: "@{city} Users"
  description: "Users from @{city}, @{country}"
  
  join: orders {
    relationship: one_to_many
    sql_on: ${users.id} = ${orders.user_id} ;;
    type: left_outer
  }
}

view: users {
  label: "@{city} Users Data"
  sql_table_name: users ;;
  
  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }
  
  dimension: name {
    type: string
    label: "User Name in @{city}"
    sql: ${TABLE}.name ;;
  }
  
  measure: count {
    type: count
    label: "Count of @{city} Users"
  }
}