# Source: looker-open-source/pylookml (MIT License)
# File: lookml/tests/files/basic_parsing/agg.model.lkml
# Tests: aggregate tables in explores with materialization config

connection: "foo"
include: "*.view.lkml"
explore: foo {
    join: test1 {
        type: left_outer
        sql_on: ${foo.id} = ${test1.id};;
        relationship: many_to_one
    }
  aggregate_table: foo {
    query: {
      dimensions: [created_date, users.state]
      measures: [total_sales]
      pivots: [users.age_tier]
      limit:  500
      timezone: Africa/Bamako
    }
    materialization: {
      datagroup_trigger: foo
    }
  }
  aggregate_table: bar {
    query: {
      dimensions: [created_date, users.state]
      measures: [total_sales]
      pivots: [users.age_tier]
      limit:  500
      timezone: Africa/Bamako
    }
    materialization: {
      datagroup_trigger: foo
    }
  }
}