explore: orders {
  join: customers {
    sql_on: ${orders.customer_id} = ${customers.id} ;;
    relationship: many_to_one
    type: left_outer
  }
}
