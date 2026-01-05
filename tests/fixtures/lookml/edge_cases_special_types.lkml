# Edge Cases: Special Dimension Types
# Tests tier, case, location, zipcode, and other special types

view: special_types {
  sql_table_name: analytics.user_data ;;
  description: "Tests special dimension types"

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: age {
    type: number
    sql: ${TABLE}.age ;;
  }

  # Tier dimension - automatic bucketing
  dimension: age_tier {
    type: tier
    tiers: [0, 18, 25, 35, 45, 55, 65]
    style: integer
    sql: ${age} ;;
    description: "Age groups"
  }

  dimension: income {
    type: number
    sql: ${TABLE}.income ;;
  }

  # Tier with different style
  dimension: income_tier {
    type: tier
    tiers: [0, 25000, 50000, 75000, 100000, 150000, 250000]
    style: classic
    sql: ${income} ;;
    value_format_name: usd_0
    description: "Income brackets"
  }

  # Relational tier
  dimension: income_tier_relational {
    type: tier
    tiers: [0, 25000, 50000, 75000, 100000]
    style: relational
    sql: ${income} ;;
  }

  dimension: score {
    type: number
    sql: ${TABLE}.score ;;
  }

  # Tier with interval style
  dimension: score_tier {
    type: tier
    tiers: [0, 20, 40, 60, 80, 100]
    style: interval
    sql: ${score} ;;
    description: "Score ranges"
  }

  # Case dimension (similar to CASE WHEN)
  dimension: customer_value_segment {
    type: string
    case: {
      when: {
        sql: ${income} >= 150000 AND ${score} >= 80 ;;
        label: "Premium"
      }
      when: {
        sql: ${income} >= 75000 AND ${score} >= 60 ;;
        label: "Standard"
      }
      when: {
        sql: ${income} >= 25000 OR ${score} >= 40 ;;
        label: "Basic"
      }
      else: "New"
    }
    description: "Customer value segment based on income and score"
  }

  # Case with alpha sorting
  dimension: priority_segment {
    type: string
    alpha_sort: yes
    case: {
      when: {
        sql: ${score} >= 90 ;;
        label: "A - Critical"
      }
      when: {
        sql: ${score} >= 70 ;;
        label: "B - High"
      }
      when: {
        sql: ${score} >= 50 ;;
        label: "C - Medium"
      }
      else: "D - Low"
    }
  }

  # Geographic dimensions
  dimension: latitude {
    type: number
    sql: ${TABLE}.latitude ;;
    hidden: yes
  }

  dimension: longitude {
    type: number
    sql: ${TABLE}.longitude ;;
    hidden: yes
  }

  # Location type combines lat/long
  dimension: location {
    type: location
    sql_latitude: ${latitude} ;;
    sql_longitude: ${longitude} ;;
    description: "User location"
  }

  # Zipcode type
  dimension: zipcode {
    type: zipcode
    sql: ${TABLE}.zipcode ;;
    description: "US ZIP code"
  }

  # Bin dimension (for histograms)
  dimension: age_bin {
    type: bin
    bins: [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    style: classic
    sql: ${age} ;;
  }

  # Distance dimension
  dimension: store_latitude {
    type: number
    sql: ${TABLE}.store_latitude ;;
    hidden: yes
  }

  dimension: store_longitude {
    type: number
    sql: ${TABLE}.store_longitude ;;
    hidden: yes
  }

  dimension: distance_to_store {
    type: distance
    start_location_field: location
    end_location_field: store_location
    units: miles
    description: "Distance from user to nearest store"
  }

  dimension: store_location {
    type: location
    sql_latitude: ${store_latitude} ;;
    sql_longitude: ${store_longitude} ;;
    hidden: yes
  }

  # Duration dimension
  dimension: session_seconds {
    type: number
    sql: ${TABLE}.session_duration_seconds ;;
    hidden: yes
  }

  dimension: session_duration {
    type: duration_second
    sql: ${session_seconds} ;;
    description: "Session duration"
  }

  # YesNo with custom labels
  dimension: is_active {
    type: yesno
    sql: ${TABLE}.is_active ;;
  }

  dimension: is_verified {
    type: yesno
    sql: ${TABLE}.verified_at IS NOT NULL ;;
    description: "Whether user has been verified"
  }

  dimension: has_purchases {
    type: yesno
    sql: ${TABLE}.purchase_count > 0 ;;
  }

  # String with suggestions
  dimension: country {
    type: string
    sql: ${TABLE}.country ;;
    suggest_persist_for: "24 hours"
    suggestions: ["US", "CA", "UK", "DE", "FR", "AU", "JP"]
  }

  # String with suggest dimension
  dimension: city {
    type: string
    sql: ${TABLE}.city ;;
    suggest_dimension: country
  }

  # String with suggest explore
  dimension: product_category {
    type: string
    sql: ${TABLE}.product_category ;;
    suggest_explore: products
    suggest_dimension: products.category
  }

  dimension_group: created {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.created_at ;;
  }

  # Duration dimension (time between events)
  dimension: time_to_first_purchase {
    type: duration_day
    sql_start: ${created_raw} ;;
    sql_end: ${TABLE}.first_purchase_at ;;
    description: "Days from signup to first purchase"
  }

  measure: count {
    type: count
  }

  measure: avg_age {
    type: average
    sql: ${age} ;;
  }

  measure: avg_income {
    type: average
    sql: ${income} ;;
    value_format_name: usd_0
  }

  measure: avg_score {
    type: average
    sql: ${score} ;;
    value_format: "0.00"
  }

  measure: active_users {
    type: count
    filters: [is_active: "yes"]
  }

  measure: verified_users {
    type: count
    filters: [is_verified: "yes"]
  }

  measure: verified_rate {
    type: number
    sql: 1.0 * ${verified_users} / NULLIF(${count}, 0) ;;
    value_format_name: percent_1
  }
}

# View with array and JSON types (BigQuery specific)
view: json_array_types {
  sql_table_name: analytics.events ;;

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: event_type {
    type: string
    sql: ${TABLE}.event_type ;;
  }

  # JSON field access
  dimension: properties {
    type: string
    sql: ${TABLE}.properties ;;
    hidden: yes
  }

  dimension: property_source {
    type: string
    sql: JSON_EXTRACT_SCALAR(${properties}, '$.source') ;;
  }

  dimension: property_campaign {
    type: string
    sql: JSON_EXTRACT_SCALAR(${properties}, '$.campaign') ;;
  }

  dimension: property_value {
    type: number
    sql: CAST(JSON_EXTRACT_SCALAR(${properties}, '$.value') AS FLOAT64) ;;
  }

  # Nested JSON access
  dimension: user_agent_browser {
    type: string
    sql: JSON_EXTRACT_SCALAR(${properties}, '$.user_agent.browser') ;;
  }

  dimension: user_agent_os {
    type: string
    sql: JSON_EXTRACT_SCALAR(${properties}, '$.user_agent.os') ;;
  }

  # Array field (BigQuery ARRAY type)
  dimension: tags {
    type: string
    sql: ${TABLE}.tags ;;
    hidden: yes
  }

  dimension: tag_count {
    type: number
    sql: ARRAY_LENGTH(${TABLE}.tags) ;;
  }

  dimension: first_tag {
    type: string
    sql: ${TABLE}.tags[SAFE_OFFSET(0)] ;;
  }

  dimension_group: created {
    type: time
    timeframes: [raw, time, date, hour, week, month]
    sql: ${TABLE}.created_at ;;
  }

  measure: count {
    type: count
  }

  measure: total_value {
    type: sum
    sql: ${property_value} ;;
  }

  measure: avg_tag_count {
    type: average
    sql: ${tag_count} ;;
  }

  # Array aggregation
  measure: all_event_types {
    type: string
    sql: ARRAY_TO_STRING(ARRAY_AGG(DISTINCT ${event_type} ORDER BY ${event_type}), ', ') ;;
    description: "Comma-separated list of event types"
  }
}
