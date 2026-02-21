# Source: https://github.com/llooker/datablocks-acs/blob/master/snowflake.geo_map.view.lkml
# Real-world LookML from Snowflake ACS geo mapping block
# Features: derived table with persist_for, dimension type:location (sql_latitude/sql_longitude),
#   map_layer_name, link blocks with dynamic URLs, suggest_persist_for,
#   drill_fields on dimensions, group_label, value_format_name: decimal_2,
#   complex derived table SQL with JOINs and GROUP BY

view: sf_logrecno_bg_map {
  label: "Geography"
  derived_table: {
    sql:
      SELECT
        UPPER(stusab) as stusab,
        logrecno,
        CONCAT(UPPER(stusab), CAST(logrecno AS STRING)) as row_id,
        sumlevel,
        state as state_fips_code,
        county as county_fips_code,
        tract,
        blkgrp,
        SUBSTR(geo.geoid, 8, 11) as geoid11,
        geo.geoid,
        trim(CASE
          WHEN sumlevel = '140'
          THEN SPLIT_PART(name, ',', 3)
          WHEN sumlevel = '150'
          THEN SPLIT_PART(name, ',', 4)
        END) as state_name,
        trim(CASE
          WHEN sumlevel = '140'
          THEN SPLIT_PART(name, ',', 2)
          WHEN sumlevel = '150'
          THEN SPLIT_PART(name, ',', 3)
        END) as county_name,
        CASE WHEN geo.SUMLEVEL = '150' THEN bg.INTPTLAT END as latitude,
        CASE WHEN geo.SUMLEVEL = '150' THEN bg.INTPTLON END as longitude,
        SUM(COALESCE(bg.ALAND, tr.ALAND) * 0.000000386102159) AS square_miles_land,
        SUM(COALESCE(bg.AWATER, tr.AWATER) * .000000386102159) AS square_miles_water
      FROM
        ACS.GEO2015 as geo
        LEFT JOIN ACS.BLOCK_GROUP_ATTRIBS as bg on (SUBSTR(geo.GEOID, 8, 12) = bg.geoid AND geo.SUMLEVEL = '150')
        LEFT JOIN ACS.BLOCK_GROUP_ATTRIBS as tr on (SUBSTR(geo.GEOID, 8, 11) = SUBSTR(tr.geoid, 1, 11) AND geo.SUMLEVEL = '140')
      WHERE
        sumlevel in ('140', '150')
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 ;;
    persist_for: "10000 hours"
  }

  dimension: row_id {
    sql: ${TABLE}.row_id;;
    primary_key: yes
    hidden: yes
  }

  dimension: stusab {
    label: "State Abbreviation"
    group_label: "State"
    link: {
      url: "https://maps.google.com?q={{value}}"
      label: "Google Maps"
    }
    suggest_persist_for: "120 hours"
  }

  dimension: state_name {
    group_label: "State"
    map_layer_name: us_states
    sql: ${TABLE}.state_name;;
    link: {
      url: "https://maps.google.com?q={{value}}"
      label: "Google Maps"
    }
    suggest_persist_for: "120 hours"
    drill_fields: [county_name, tract]
  }

  dimension: county_name {
    group_label: "County"
    label: "County Name"
    sql: ${TABLE}.county_name ;;
    drill_fields: [tract, blkgrp]
    suggest_persist_for: "120 hours"
  }

  dimension: county_fips_code {
    group_label: "County"
    label: "County FIPS Code"
    sql: CONCAT(${state_fips_code}, ${TABLE}.county_fips_code);;
    map_layer_name: us_counties_fips
    drill_fields: [tract, blkgrp]
  }

  dimension: state_fips_code {
    group_label: "State"
    sql: ${TABLE}.state_fips_code ;;
  }

  dimension: tract {
    sql: ${TABLE}.tract ;;
  }

  dimension: blkgrp {
    label: "Block Group"
    sql: ${TABLE}.blkgrp ;;
  }

  dimension: geoid11 {
    hidden: yes
    sql: ${TABLE}.geoid11 ;;
  }

  dimension: latitude {
    type: number
    hidden: yes
    sql: ${TABLE}.latitude ;;
  }

  dimension: longitude {
    type: number
    hidden: yes
    sql: ${TABLE}.longitude ;;
  }

  dimension: block_group_centroid {
    type: location
    sql_latitude: ${TABLE}.latitude ;;
    sql_longitude: ${TABLE}.longitude ;;
    group_label: "Block Group"
  }

  dimension: sumlevel {
    hidden: yes
    sql: ${TABLE}.sumlevel ;;
  }

  measure: sq_miles_land {
    sql: ${TABLE}.square_miles_land ;;
    label: "Square Miles of Land"
    type: sum
    value_format_name: decimal_2
  }

  measure: sq_miles_water {
    sql: ${TABLE}.square_miles_water ;;
    label: "Square Miles of Water"
    type: sum
    value_format_name: decimal_2
  }

  measure: count {
    type: count
  }
}
