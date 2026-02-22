# Source: EffinAmazing/segment_attribution (MIT License)
# File: views/conversion_attribution.view.lkml
# Tests: parameter-driven attribution, complex derived table, window functions

view: conversion_attribution {
  derived_table: {
    persist_for: "12 hours"
    sql:
      with attribution_sessions as (
        select
          c.id as conversion_id,
          s.id as session_id,
          conversion_value,
          row_number() over (partition by c.id order by s.received_at asc) as index_asc,
          row_number() over (partition by c.id order by s.received_at desc) as index_desc,
          count(1) over (partition by c.id) as num_sessions,
          pow(2,(-timestamp_diff(c.received_at, s.received_at, day)/7.00)) as time_decay_value
        from ${sessions.SQL_TABLE_NAME} s
          left join ${conversion_event.SQL_TABLE_NAME} c
            on s.user_id = c.user_id
            and s.received_at <= c.received_at
        /* this logic limits the attribution window to 30 days */
        /* change 30 to your desired attribution window or remove this for all-time attribution */
        where s.received_at >= timestamp_add(c.received_at, interval -30 day)
      )
      select *,
        sum(time_decay_value) over (partition by conversion_id) as total_time_decay_value
      from attribution_sessions
    ;;
  }

  dimension: primary_key {
    primary_key: yes
    hidden: yes
    type: string
    sql: ${conversion_id}||${session_id} ;;
  }

  dimension: conversion_id {
    type: string
    hidden: yes
    sql: ${TABLE}.conversion_id ;;
  }

  dimension: session_id {
    type: string
    hidden: yes
    sql: ${TABLE}.session_id ;;
  }

  dimension: index_asc {
    type: number
    sql: ${TABLE}.index_asc ;;
  }

  dimension: index_desc {
    type: number
    sql: ${TABLE}.index_desc ;;
  }

  dimension: num_sessions {
    type: number
    sql: ${TABLE}.num_sessions ;;
  }

  dimension: time_decay_value {
    type: number
    sql: ${TABLE}.time_decay_value ;;
  }

  dimension: total_time_decay_value {
    type: number
    sql: ${TABLE}.total_time_decay_value ;;
  }

  parameter: attribution_model {
    type: string
    allowed_value: {
      label: "First Touch"
      value: "first"
    }
    allowed_value: {
      label: "Last Touch"
      value: "last"
    }
    allowed_value: {
      label: "40-20-40"
      value: "402040"
    }
    allowed_value: {
      label: "Linear"
      value: "linear"
    }
    allowed_value: {
      label: "Time Decay"
      value: "decay"
    }
  }

  dimension: attribution_credit {
    type: number
    sql:
      case
        when {% parameter attribution_model %} = 'first'
          then case when ${index_asc} = 1 then 1 else 0 end
        when {% parameter attribution_model %} = 'last'
          then case when ${index_desc} = 1 then 1 else 0 end
        when {% parameter attribution_model %} = 'linear'
          then 1.00/${num_sessions}
        when {% parameter attribution_model %} = '402040'
          then
            case
              when ${num_sessions} = 1 then 1
              when ${num_sessions} = 2 then .5
              when ${num_sessions} > 2 then
                case
                  when ${index_asc} = 1 or ${index_desc} = 1 then .4
                  else 0.2/(${num_sessions} - 2)
                end
            end
        when {% parameter attribution_model %} = 'decay'
          then (${time_decay_value}/${total_time_decay_value})
      end
    ;;
  }

  dimension: conversion_value {
    type: number
    hidden: yes
    sql: ${TABLE}.conversion_value ;;
  }

  dimension: attributed_conversion_value {
    type: number
    value_format_name: usd
    sql: ${conversion_value}*${attribution_credit} ;;
  }

  measure: share_of_conversions {
    type: sum
    value_format_name: decimal_1
    sql: ${attribution_credit} ;;
  }

  measure: share_of_revenue {
    type: sum
    value_format_name: usd_0
    sql: ${attributed_conversion_value} ;;
  }

}
