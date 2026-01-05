view: products {
  sql_table_name: products ;;
  description: "Product catalog"

  dimension: id {
    type: number
    primary_key: yes
    sql: id ;;
  }

  dimension: name {
    type: string
    sql: name ;;
  }

  dimension: category {
    type: string
    sql: category ;;
  }

  dimension: subcategory {
    type: string
    sql: subcategory ;;
  }

  dimension: price {
    type: number
    sql: price ;;
  }

  measure: product_count {
    type: count
    description: "Number of products"
  }

  measure: avg_price {
    type: average
    sql: price ;;
    description: "Average product price"
  }
}
