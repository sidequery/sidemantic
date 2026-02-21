# Source: https://github.com/looker-open-source/blocks_redshift_admin/blob/master/redshift_views.view.lkml
# Real-world LookML from Looker's Redshift Admin block
# Features: multiple views in one file, derived tables with complex CTEs,
#   dimension_group type:time, Liquid HTML in html: parameter,
#   datagroup_trigger, distribution/sortkeys (Redshift-specific),
#   cross-view references, group_label, link, alias, drill_fields,
#   value_format_name, set, type:yesno, measure types: count/count_distinct/sum/number/max/average

view: redshift_db_space {
  derived_table: {
    sql: select name as table
      , trim(pgn.nspname) as schema
      , sum(b.mbytes) as megabytes
      , sum(a.rows) as rows
      from (select db_id
        , id
        , name
        , sum(rows) as rows
        from stv_tbl_perm a
        group by 1,2,3) as a
      join pg_class as pgc
        on pgc.oid = a.id
      join pg_namespace as pgn
        on pgn.oid = pgc.relnamespace
      join pg_database as pgdb
        on pgdb.oid = a.db_id
      join (select tbl
        , count(*) as mbytes
        from stv_blocklist
        group by 1) as b
        on a.id = b.tbl
      group by 1,2
      ;;
  }

  dimension: table {
    type: string
    sql: ${TABLE}.table ;;
  }

  dimension: schema {
    type: string
    sql: ${TABLE}.schema ;;
  }

  dimension: megabytes {
    type: number
    sql: ${TABLE}.megabytes ;;
  }

  dimension: rows {
    type: number
    sql: ${TABLE}.rows ;;
  }

  dimension: table_stem {
    sql: case
      when (${table} ~ E'(lr|lc)\\$[a-zA-Z0-9]+_.*')
      then ltrim(regexp_substr(${table}, '_.*'), '_') || ' - Looker PDT'
      else ${table}
      end
      ;;
  }

  measure: total_megabytes {
    type: sum
    sql: ${megabytes} ;;
  }

  measure: total_rows {
    type: sum
    sql: ${rows} ;;
  }

  measure: total_tables {
    type: count_distinct
    sql: ${table} ;;
  }
}

view: redshift_etl_errors {
  derived_table: {
    sql: select starttime as error_time
      , filename as file_name
      , colname as column_name
      , type as column_data_type
      , position as error_position
      , raw_field_value as error_field_value
      , err_reason as error_reason
      , raw_line
      from stl_load_errors
      ;;
  }

  dimension_group: error {
    type: time
    timeframes: [time, date]
    sql: ${TABLE}.error_time ;;
  }

  dimension: file_name {
    type: string
    sql: ${TABLE}.file_name ;;
  }

  dimension: column_name {
    type: string
    sql: ${TABLE}.column_name ;;
  }

  dimension: column_data_type {
    type: string
    sql: ${TABLE}.column_data_type ;;
  }

  dimension: error_position {
    type: string
    sql: ${TABLE}.error_position ;;
  }

  dimension: error_field_value {
    type: string
    sql: ${TABLE}.error_field_value ;;
  }

  dimension: error_reason {
    type: string
    sql: ${TABLE}.error_reason ;;
  }

  dimension: raw_line {
    type: string
    sql: ${TABLE}.raw_line ;;
  }
}

view: redshift_data_loads {
  derived_table: {
    sql: select replace(regexp_substr(filename, '//[a-zA-Z0-9\-]+/'), '/', '') as root_bucket
      , replace(filename, split_part(filename, '/', regexp_count(filename, '/') + 1), '') as s3_path
      , split_part(filename, '/', regexp_count(filename, '/') + 1) as file_name
      , curtime as load_time
      from stl_load_commits
      ;;
  }

  dimension: root_bucket {
    type: string
    sql: ${TABLE}.root_bucket ;;
  }

  dimension: s3_path {
    type: string
    sql: ${TABLE}.s3_path ;;
  }

  dimension: file_name {
    type: string
    sql: ${TABLE}.file_name ;;
  }

  dimension_group: load {
    type: time
    timeframes: [raw, time, date]
    sql: ${TABLE}.load_time ;;
  }

  measure: most_recent_load {
    type: string
    sql: max(${load_raw}) ;;
  }

  measure: hours_since_last_load {
    type: number
    value_format_name: id
    sql: datediff('hour', ${most_recent_load}, getdate()) ;;
    html: {% if value < 24 %}
      <div style="color:green">{{ rendered_value }}</div>
      {% elsif value >= 24 and value < 48 %}
      <div style="color:orange">{{ rendered_value }}</div>
      {% elsif value >= 48 %}
      <div style="color:red">{{ rendered_value }}</div>
      {% endif %}
      ;;
  }
}

view: redshift_queries {
  derived_table: {
    datagroup_trigger: nightly
    distribution: "query"
    sortkeys: ["query"]
    sql: SELECT
      wlm.query,
      sc.name as service_class,
      wlm.service_class_start_time as start_time,
      wlm.total_queue_time,
      wlm.total_exec_time,
      q.elapsed
      FROM STL_WLM_QUERY wlm
      LEFT JOIN STV_WLM_SERVICE_CLASS_CONFIG sc ON sc.service_class=wlm.service_class
      LEFT JOIN SVL_QLOG q on q.query=wlm.query
      WHERE wlm.service_class_start_time >= dateadd(day,-1,GETDATE())
      AND wlm.service_class_start_time <= GETDATE()
      ;;
  }

  dimension: query {
    type: number
    primary_key: yes
    link: {
      label: "Inspect"
      url: "/dashboards/redshift_model::redshift_query_inspection?query={{value}}"
    }
  }

  dimension: text {
    alias: [querytxt]
  }

  dimension: snippet {
    alias: [substring]
  }

  dimension: pdt {
    label: "Is PDT?"
    group_label: "Looker Query Context"
  }

  dimension_group: start {
    type: time
    timeframes: [raw, minute, second, minute15, hour, hour_of_day, day_of_week, date]
    sql: ${TABLE}.start_time ;;
  }

  dimension: time_in_queue {
    type: number
    sql: ${TABLE}.total_queue_time /1000000;;
  }

  dimension: time_executing {
    type: number
    sql: ${TABLE}.total_exec_time /1000000;;
  }

  dimension: time_executing_roundup5 {
    group_label: "Time Executing Buckets"
    label: "05 seconds"
    type: number
    sql: CEILING(${TABLE}.total_exec_time /1000000 / 5)*5 ;;
    value_format_name: decimal_0
  }

  dimension: was_queued {
    type: yesno
    sql: ${TABLE}.total_queue_time > 0;;
  }

  measure: count {
    type: count
    drill_fields: [query, start_date, time_executing, pdt, snippet]
  }

  measure: count_of_queued {
    type: sum
    sql: ${TABLE}.total_queue_time ;;
  }

  measure: percent_queued {
    type: number
    value_format: "0.## \%"
    sql: 100 * ${count_of_queued} / NULLIF(${count}, 0) ;;
  }

  measure: total_time_in_queue {
    type: sum
    sql: ${time_in_queue};;
  }

  measure: total_time_executing {
    type: sum
    sql: ${time_executing};;
  }

  measure: time_executing_per_query {
    type: number
    sql: CASE WHEN ${count}<>0 THEN ${total_time_executing} / ${count} ELSE NULL END ;;
    value_format_name: decimal_1
  }
}

view: redshift_tables {
  derived_table: {
    datagroup_trigger: nightly
    distribution_style: all
    indexes: ["table_id","table"]
    sql: select
      "database",
      "schema",
      "Table_id",
      "table",
      "encoded",
      "diststyle",
      "size",
      "pct_used",
      "unsorted",
      "stats_off",
      "tbl_rows",
      "skew_sortkey1",
      "skew_rows"
      from svv_table_info
      ;;
  }

  dimension: table_id {
    group_label: " Identifiers"
    type: number
    sql: ${TABLE}.table_id ;;
  }

  dimension: path {
    group_label: " Identifiers"
    sql: ${TABLE}.path ;;
    primary_key: yes
  }

  dimension: encoded {
    group_label: "Size (Columns)"
    type: yesno
    sql: case ${TABLE}.encoded
      when 'Y' then true
      when 'N' then false
      else null end ;;
  }

  dimension: distribution_style {
    group_label: "Distribution"
    type: string
    sql: ${TABLE}.diststyle ;;
    html:
      {% if value == 'EVEN' %}
      <span style="color: darkorange">{{ rendered_value }}</span>
      {% elsif value == 'ALL' %}
      <span style="color: dimgray">{{ rendered_value }}</span>
      {% else %}
      {{ rendered_value }}
      {% endif %}
      ;;
  }

  dimension: unsorted {
    group_label: "Sorting"
    type: number
    sql: ${TABLE}.unsorted ;;
  }

  measure: count {
    type: count
  }

  measure: total_rows {
    type: sum
    sql: ${TABLE}.tbl_rows;;
  }

  measure: total_size {
    type: sum
    sql: ${TABLE}.size ;;
  }
}
