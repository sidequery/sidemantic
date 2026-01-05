view: customers {
  sql_table_name: customers ;;
  description: "Customer master data"

  dimension: id {
    type: number
    primary_key: yes
    sql: id ;;
  }

  dimension: name {
    type: string
    sql: name ;;
  }

  dimension: email {
    type: string
    sql: email ;;
  }

  dimension: region {
    type: string
    sql: region ;;
  }

  dimension: tier {
    type: string
    sql: tier ;;
  }

  measure: customer_count {
    type: count
    description: "Total number of customers"
  }
}
