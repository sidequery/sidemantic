# Source: https://github.com/looker/app-marketing-google-ads-transfer-bigquery
# Real-world LookML from Google Ads BigQuery Transfer Block
# Features: extension:required, deep extends chains, case dimensions with when blocks,
#   Liquid sql_table_name, explore definitions with from/view_name/required_joins,
#   multiple self-joins of same view with aliases, hidden dimensions

view: hour_base {
  extension: required

  dimension: hour_of_day {
    hidden: yes
    type: number
    sql: ${TABLE}.HourOfDay ;;
  }
}

view: transformations_base {
  extension: required

  dimension: ad_network_type {
    hidden: yes
    type: string
    case: {
      when: {
        sql: ${TABLE}.ad_network = 'SEARCH' ;;
        label: "Search"
      }
      when: {
        sql: ${TABLE}.ad_network = 'CONTENT' ;;
        label: "Content"
      }
      else: "Other"
    }
  }

  dimension: device_type {
    hidden: yes
    type: string
    case: {
      when: {
        sql: LOWER(${TABLE}.device) LIKE '%desktop%' ;;
        label: "Desktop"
      }
      when: {
        sql: LOWER(${TABLE}.device) LIKE '%mobile%' ;;
        label: "Mobile"
      }
      when: {
        sql: LOWER(${TABLE}.device) LIKE '%tablet%' ;;
        label: "Tablet"
      }
      else: "Other"
    }
  }
}

view: ad_impressions_adapter {
  extends: [transformations_base]
  sql_table_name: adwords.AccountBasicStats ;;

  dimension: cost {
    hidden: yes
    type: number
    sql: ${TABLE}.cost / 1000000;;
  }

  dimension: clicks {
    type: number
    sql: ${TABLE}.clicks ;;
  }

  dimension: impressions {
    type: number
    sql: ${TABLE}.impressions ;;
  }

  dimension: external_customer_id {
    type: string
    sql: ${TABLE}.ExternalCustomerId ;;
  }

  dimension_group: date {
    type: time
    timeframes: [date, week, month, quarter, year]
    sql: ${TABLE}._DATA_DATE ;;
  }

  measure: total_impressions {
    type: sum
    sql: ${impressions} ;;
  }

  measure: total_clicks {
    type: sum
    sql: ${clicks} ;;
  }

  measure: total_cost {
    type: sum
    sql: ${cost} ;;
    value_format_name: usd
  }

  measure: average_cpc {
    type: number
    sql: ${total_cost} / NULLIF(${total_clicks}, 0) ;;
    value_format_name: usd
  }

  measure: click_through_rate {
    type: number
    sql: ${total_clicks} / NULLIF(${total_impressions}, 0) ;;
    value_format_name: percent_2
  }
}

view: ad_impressions_campaign_adapter {
  extends: [ad_impressions_adapter]
  sql_table_name: adwords.CampaignBasicStats ;;

  dimension: campaign_id {
    hidden: yes
    sql: ${TABLE}.CampaignId ;;
  }

  dimension: campaign_name {
    type: string
    sql: ${TABLE}.CampaignName ;;
  }
}

view: ad_impressions_ad_group_adapter {
  extends: [ad_impressions_campaign_adapter]
  sql_table_name: adwords.AdGroupBasicStats ;;

  dimension: ad_group_id {
    hidden: yes
    sql: ${TABLE}.AdGroupId ;;
  }

  dimension: ad_group_name {
    type: string
    sql: ${TABLE}.AdGroupName ;;
  }
}

view: keyword_adapter {
  extension: required
  sql_table_name: adwords.Keyword ;;

  dimension: criterion_id {
    type: number
    sql: ${TABLE}.CriterionId ;;
    primary_key: yes
  }

  dimension: criteria {
    type: string
    sql: ${TABLE}.Criteria ;;
    link: {
      icon_url: "https://www.google.com/images/branding/product/ico/googleg_lodp.ico"
      label: "Google Search"
      url: "https://www.google.com/search?q={{ value }}"
    }
  }

  dimension: bidding_strategy_type {
    type: string
    case: {
      when: {
        sql: ${TABLE}.bidding_strategy = 'Target CPA' ;;
        label: "Target CPA"
      }
      when: {
        sql: ${TABLE}.bidding_strategy = 'Enhanced CPC';;
        label: "Enhanced CPC"
      }
      when: {
        sql: ${TABLE}.bidding_strategy = 'cpc' ;;
        label: "CPC"
      }
      else: "Other"
    }
  }

  dimension: is_negative {
    type: yesno
    sql: ${TABLE}.IsNegative ;;
  }

  dimension: quality_score {
    type: number
    sql: ${TABLE}.QualityScore ;;
  }

  dimension: criteria_destination_url {
    type: string
    sql: ${TABLE}.CriteriaDestinationUrl ;;
    group_label: "URLS"
  }

  dimension: final_url {
    type: string
    sql: ${TABLE}.FinalUrls ;;
    group_label: "URLS"
  }
}
