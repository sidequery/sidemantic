# Ecommerce Explores
# Demonstrates: Multiple explores with various join types and relationships

explore: orders {
  description: "Explore orders with customer and order item data"

  join: customers {
    sql_on: ${orders.customer_id} = ${customers.id} ;;
    relationship: many_to_one
    type: left_outer
  }

  join: order_items {
    sql_on: ${orders.id} = ${order_items.order_id} ;;
    relationship: one_to_many
    type: left_outer
  }

  join: products {
    sql_on: ${order_items.product_id} = ${products.id} ;;
    relationship: many_to_one
    type: left_outer
  }
}

explore: customers {
  description: "Explore customers with their order history"

  join: orders {
    sql_on: ${customers.id} = ${orders.customer_id} ;;
    relationship: one_to_many
    type: left_outer
  }
}

explore: products {
  description: "Explore products with sales data"

  join: order_items {
    sql_on: ${products.id} = ${order_items.product_id} ;;
    relationship: one_to_many
    type: left_outer
  }

  join: orders {
    sql_on: ${order_items.order_id} = ${orders.id} ;;
    relationship: many_to_one
    type: left_outer
  }
}
