# Source: https://github.com/looker/bq_thelook
# Real-world LookML example demonstrating derived tables with explore_source
# Features: derived_table with explore_source, bind_filters

include: "users.explore.lkml"

explore: filtered_lookml_dt {}

view: filtered_lookml_dt {
  derived_table: {
    explore_source: users {
      column: age {field: users.age}
      column: people {field: users.count}
      bind_filters: {
        to_field: users.created_date
        from_field: filtered_lookml_dt.filter_date
      }
    }
  }

  filter: filter_date {
    type: date
  }
  dimension: age {type: number}
  dimension: people {type: number}
}
