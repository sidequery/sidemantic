# Source: https://github.com/teamdatatonic/looker-date-comparison/blob/master/_date_comparison.view.lkml
# Real-world LookML from Datatonic's date comparison library
# Features: extension:required, dimension_group type:duration with intervals,
#   filter: fields (filter-only, not true dimensions), parameter: with allowed_value blocks,
#   heavy Liquid templating, convert_tz, order_by_field, BigQuery SQL functions

view: _date_comparison {
  extension: required

  filter: current_date_range {
    view_label: "Timeline Comparison Fields"
    label: "1. Date Range"
    type: date
    convert_tz: yes
  }

  filter: previous_date_range {
    view_label: "Timeline Comparison Fields"
    label: "2b. Compare To (Custom):"
    group_label: "Compare to:"
    type: date
    convert_tz: yes
  }

  dimension_group: in_period {
    type: duration
    intervals: [day]
    sql_start: ${TABLE}.period_start ;;
    sql_end: ${TABLE}.period_end ;;
    hidden: yes
  }

  dimension: period_2_start {
    type: date_raw
    sql: ${TABLE}.period_2_start ;;
    hidden: yes
  }

  dimension: period_2_end {
    type: date_raw
    sql: ${TABLE}.period_2_end ;;
    hidden: yes
  }

  parameter: compare_to {
    label: "2a. Compare To (Templated):"
    type: unquoted
    allowed_value: {
      label: "Previous Period"
      value: "Period"
    }
    allowed_value: {
      label: "Previous Week"
      value: "Week"
    }
    allowed_value: {
      label: "Previous Month"
      value: "Month"
    }
    allowed_value: {
      label: "Previous Quarter"
      value: "Quarter"
    }
    allowed_value: {
      label: "Previous Year"
      value: "Year"
    }
    default_value: "Period"
    view_label: "Timeline Comparison Fields"
  }

  parameter: comparison_periods {
    label: "3. Number of Periods"
    type: unquoted
    allowed_value: {
      label: "2"
      value: "2"
    }
    allowed_value: {
      label: "3"
      value: "3"
    }
    allowed_value: {
      label: "4"
      value: "4"
    }
    default_value: "2"
    view_label: "Timeline Comparison Fields"
  }

  dimension: period {
    view_label: "Timeline Comparison Fields"
    label: "Period"
    type: string
    order_by_field: order_for_period
    sql: ${TABLE}.period_label ;;
  }

  dimension: order_for_period {
    hidden: yes
    type: number
    sql: ${TABLE}.period_order ;;
  }

  dimension_group: date_in_period {
    label: "Current Period"
    type: time
    sql: ${TABLE}.date_in_period ;;
    view_label: "Timeline Comparison Fields"
    timeframes: [date, week, month, quarter, year]
  }

  dimension: day_in_period {
    view_label: "Timeline Comparison Fields"
    type: number
    sql: ${TABLE}.day_in_period ;;
  }
}
