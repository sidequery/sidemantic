# Kitchen Sink Explores
# Defines relationships between entities for multi-hop join testing

explore: orders {
  description: "Comprehensive orders explore with all related entities"

  # Many-to-one: orders -> customers
  join: customers {
    sql_on: ${orders.customer_id} = ${customers.id} ;;
    relationship: many_to_one
    type: left_outer
  }

  # Many-to-one (2-hop): orders -> customers -> regions
  join: regions {
    sql_on: ${customers.region_id} = ${regions.id} ;;
    relationship: many_to_one
    type: left_outer
  }

  # One-to-many: orders -> order_items
  join: order_items {
    sql_on: ${orders.id} = ${order_items.order_id} ;;
    relationship: one_to_many
    type: left_outer
  }

  # Many-to-one (through order_items): order_items -> products
  join: products {
    sql_on: ${order_items.product_id} = ${products.id} ;;
    relationship: many_to_one
    type: left_outer
  }

  # Many-to-one (through products): products -> categories
  join: categories {
    sql_on: ${products.category_id} = ${categories.id} ;;
    relationship: many_to_one
    type: left_outer
  }

  # One-to-many: orders -> shipments
  join: shipments {
    sql_on: ${orders.id} = ${shipments.order_id} ;;
    relationship: one_to_many
    type: left_outer
  }

  # One-to-many: orders -> reviews
  join: reviews {
    sql_on: ${orders.id} = ${reviews.order_id} ;;
    relationship: one_to_many
    type: left_outer
  }
}

explore: customers {
  description: "Customer-centric explore"

  # Many-to-one: customers -> regions
  join: regions {
    sql_on: ${customers.region_id} = ${regions.id} ;;
    relationship: many_to_one
    type: left_outer
  }

  # One-to-many: customers -> orders
  join: orders {
    sql_on: ${customers.id} = ${orders.customer_id} ;;
    relationship: one_to_many
    type: left_outer
  }

  # One-to-many: customers -> reviews
  join: reviews {
    sql_on: ${customers.id} = ${reviews.customer_id} ;;
    relationship: one_to_many
    type: left_outer
  }
}

explore: products {
  description: "Product-centric explore"

  # Many-to-one: products -> categories
  join: categories {
    sql_on: ${products.category_id} = ${categories.id} ;;
    relationship: many_to_one
    type: left_outer
  }

  # One-to-many: products -> order_items
  join: order_items {
    sql_on: ${products.id} = ${order_items.product_id} ;;
    relationship: one_to_many
    type: left_outer
  }

  # Many-to-one (through order_items): order_items -> orders
  join: orders {
    sql_on: ${order_items.order_id} = ${orders.id} ;;
    relationship: many_to_one
    type: left_outer
  }

  # One-to-many: products -> reviews
  join: reviews {
    sql_on: ${products.id} = ${reviews.product_id} ;;
    relationship: one_to_many
    type: left_outer
  }
}

explore: regions {
  description: "Region-centric explore for geographic analysis"

  # One-to-many: regions -> customers
  join: customers {
    sql_on: ${regions.id} = ${customers.region_id} ;;
    relationship: one_to_many
    type: left_outer
  }
}
