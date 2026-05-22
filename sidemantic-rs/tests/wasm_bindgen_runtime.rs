#![cfg(all(target_arch = "wasm32", feature = "wasm"))]

use sidemantic::{
    wasm_analyze_migrator_query, wasm_build_preaggregation_refresh_statements,
    wasm_build_symmetric_aggregate_sql, wasm_calculate_preaggregation_benefit_score,
    wasm_chart_auto_detect_columns, wasm_chart_encoding_type, wasm_chart_format_label,
    wasm_chart_select_type, wasm_compile_with_yaml_query, wasm_detect_adapter_kind,
    wasm_dimension_sql_expr_with_yaml, wasm_dimension_with_granularity_with_yaml,
    wasm_evaluate_table_calculation_expression, wasm_extract_column_references,
    wasm_extract_metric_dependencies_from_yaml, wasm_extract_preaggregation_patterns,
    wasm_find_models_for_query, wasm_find_relationship_path_with_yaml,
    wasm_format_parameter_value_with_yaml, wasm_generate_catalog_metadata_with_yaml,
    wasm_generate_preaggregation_definition,
    wasm_generate_preaggregation_materialization_sql_with_yaml, wasm_generate_preaggregation_name,
    wasm_generate_time_comparison_sql, wasm_interpolate_sql_with_parameters_with_yaml,
    wasm_is_relative_date, wasm_is_sql_template, wasm_load_graph_with_sql,
    wasm_load_graph_with_yaml, wasm_metric_is_simple_aggregation, wasm_metric_sql_expr,
    wasm_metric_to_sql, wasm_model_find_dimension_index_with_yaml,
    wasm_model_find_metric_index_with_yaml, wasm_model_find_pre_aggregation_index_with_yaml,
    wasm_model_find_segment_index_with_yaml, wasm_model_get_drill_down_with_yaml,
    wasm_model_get_drill_up_with_yaml, wasm_model_get_hierarchy_path_with_yaml,
    wasm_needs_symmetric_aggregate, wasm_parse_reference_with_yaml, wasm_parse_relative_date,
    wasm_parse_simple_metric_aggregation, wasm_parse_sql_definitions_payload,
    wasm_parse_sql_graph_definitions_payload, wasm_parse_sql_model_payload,
    wasm_parse_sql_statement_blocks_payload, wasm_recommend_preaggregation_patterns,
    wasm_relationship_foreign_key_columns_with_yaml,
    wasm_relationship_primary_key_columns_with_yaml, wasm_relationship_related_key_with_yaml,
    wasm_relationship_sql_expr_with_yaml, wasm_relative_date_to_range, wasm_render_sql_template,
    wasm_resolve_metric_inheritance, wasm_resolve_model_inheritance_with_yaml,
    wasm_rewrite_with_yaml, wasm_segment_get_sql_with_yaml, wasm_summarize_preaggregation_patterns,
    wasm_time_comparison_offset_interval, wasm_time_comparison_sql_offset,
    wasm_trailing_period_sql_interval, wasm_validate_engine_refresh_sql_compatibility,
    wasm_validate_metric_payload, wasm_validate_model_payload, wasm_validate_models_yaml,
    wasm_validate_parameter_payload, wasm_validate_query_references,
    wasm_validate_query_references_with_yaml, wasm_validate_query_with_yaml,
    wasm_validate_table_calculation_payload, wasm_validate_table_formula_expression,
};
use wasm_bindgen::JsValue;
use wasm_bindgen_test::*;

const SIMPLE_MODELS_YAML: &str = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
      - name: count
        agg: count
"#;

const PREAGG_MODELS_YAML: &str = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: order_date
        type: time
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
    pre_aggregations:
      - name: daily_revenue
        time_dimension: order_date
        granularity: day
        measures: [revenue]
"#;

const TOP_LEVEL_METRIC_YAML: &str = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
      - name: order_count
        agg: count
metrics:
  - name: revenue_per_order
    type: ratio
    numerator: revenue
    denominator: order_count
"#;

fn js_err_text(err: &JsValue) -> String {
    err.as_string().unwrap_or_else(|| format!("{err:?}"))
}

fn js_err_contains(err: &JsValue, needle: &str) -> bool {
    js_err_text(err)
        .to_ascii_lowercase()
        .contains(&needle.to_ascii_lowercase())
}

#[wasm_bindgen_test]
fn wasm_bindgen_runtime_compile_and_rewrite() {
    let query_yaml = r#"
metrics: [orders.revenue]
dimensions: [orders.status]
"#;

    let compiled = wasm_compile_with_yaml_query(SIMPLE_MODELS_YAML, query_yaml).unwrap();
    assert!(compiled.contains("SUM("));

    let rewritten =
        wasm_rewrite_with_yaml(SIMPLE_MODELS_YAML, "SELECT orders.revenue FROM orders").unwrap();
    assert!(rewritten.contains("SUM("));
    assert!(rewritten.contains("revenue"));

    let rewritten_with_order = wasm_rewrite_with_yaml(
        SIMPLE_MODELS_YAML,
        "SELECT orders.revenue AS rev, orders.status FROM orders ORDER BY rev DESC LIMIT 3",
    )
    .unwrap();
    assert!(rewritten_with_order.contains("ORDER BY"));
    assert!(rewritten_with_order.contains("LIMIT 3"));

    let rewritten_with_positional_order = wasm_rewrite_with_yaml(
        SIMPLE_MODELS_YAML,
        "SELECT orders.revenue, orders.status FROM orders ORDER BY 1 DESC LIMIT 2",
    )
    .unwrap();
    assert!(rewritten_with_positional_order.contains("ORDER BY"));
    assert!(rewritten_with_positional_order.contains("LIMIT 2"));

    let rewritten_with_aggregate = wasm_rewrite_with_yaml(
        SIMPLE_MODELS_YAML,
        "SELECT SUM(orders.amount) AS total_revenue, orders.status FROM orders ORDER BY total_revenue DESC LIMIT 2",
    )
    .unwrap();
    assert!(rewritten_with_aggregate.contains("SUM("));
    assert!(rewritten_with_aggregate.contains("ORDER BY"));
    assert!(rewritten_with_aggregate.contains("LIMIT 2"));

    let rewritten_with_expression = wasm_rewrite_with_yaml(
        SIMPLE_MODELS_YAML,
        "SELECT SUM(amount) / COUNT(*) AS aov, status FROM orders ORDER BY aov DESC LIMIT 1",
    )
    .unwrap();
    assert!(rewritten_with_expression
        .to_ascii_uppercase()
        .contains("COUNT("));
    assert!(rewritten_with_expression.contains("revenue / count AS aov"));
    assert!(rewritten_with_expression.contains("ORDER BY"));
    assert!(rewritten_with_expression.contains("LIMIT 1"));
}

#[wasm_bindgen_test]
fn wasm_bindgen_runtime_validate_and_load() {
    let query_yaml = r#"
metrics: [orders.revenue]
dimensions: [orders.status]
"#;
    let errors_json = wasm_validate_query_with_yaml(SIMPLE_MODELS_YAML, query_yaml).unwrap();
    assert_eq!(errors_json, "[]");

    let payload = wasm_load_graph_with_yaml(SIMPLE_MODELS_YAML).unwrap();
    assert!(payload.contains("\"models\""));
    assert!(payload.contains("\"orders\""));
    let sql_payload = wasm_load_graph_with_sql(
        "MODEL (name orders, table orders, primary_key order_id);\nMETRIC (name order_count, agg count);\n",
    )
    .unwrap();
    assert!(sql_payload.contains("\"models\""));
    assert!(sql_payload.contains("\"order_count\""));
}

#[wasm_bindgen_test]
fn wasm_bindgen_runtime_dependency_helpers() {
    let refs = wasm_extract_column_references("(revenue - cost) / revenue");
    assert_eq!(refs, "[\"cost\",\"revenue\"]");

    let models = wasm_find_models_for_query("[\"orders.status\"]", "[\"orders.revenue\"]").unwrap();
    assert_eq!(models, "[\"orders\"]");
    let ref_errors = wasm_validate_query_references_with_yaml(
        SIMPLE_MODELS_YAML,
        "[\"orders.revenue\"]",
        "[\"orders.status\"]",
    )
    .unwrap();
    assert_eq!(ref_errors, "[]");
    let ref_errors_alias = wasm_validate_query_references(
        SIMPLE_MODELS_YAML,
        "[\"orders.revenue\"]",
        "[\"orders.status\"]",
    )
    .unwrap();
    assert_eq!(ref_errors_alias, "[]");

    let relationship_yaml = r#"
models:
  - name: customers
    primary_key_columns: [id]
    relationships: []
  - name: orders
    primary_key_columns: [order_id]
    relationships:
      - name: customers
        type: many_to_one
        foreign_key_columns: [customer_id]
        has_foreign_key: true
"#;
    let path_json =
        wasm_find_relationship_path_with_yaml(relationship_yaml, "orders", "customers").unwrap();
    assert!(path_json.contains("orders"));
    assert!(path_json.contains("customers"));

    let rendered = wasm_render_sql_template(
        "select {{ col }} from {{ table }}",
        "col: amount\ntable: orders\n",
    )
    .unwrap();
    assert_eq!(rendered, "select amount from orders");
}

#[wasm_bindgen_test]
fn wasm_bindgen_runtime_catalog_and_preaggregation_helpers() {
    let catalog_json =
        wasm_generate_catalog_metadata_with_yaml(PREAGG_MODELS_YAML, "analytics").unwrap();
    assert!(catalog_json.contains("\"table_name\":\"orders\""));

    let preagg_sql = wasm_generate_preaggregation_materialization_sql_with_yaml(
        PREAGG_MODELS_YAML,
        "orders",
        "daily_revenue",
    )
    .unwrap();
    assert!(preagg_sql.contains("DATE_TRUNC('day', order_date)"));
    assert!(preagg_sql.contains("SUM(amount) as revenue_raw"));
}

#[wasm_bindgen_test]
fn wasm_bindgen_runtime_top_level_metric_validation_and_load() {
    let query_yaml = r#"
metrics: [revenue_per_order]
dimensions: [orders.status]
"#;
    let errors_json = wasm_validate_query_with_yaml(TOP_LEVEL_METRIC_YAML, query_yaml).unwrap();
    assert_eq!(errors_json, "[]");

    let payload = wasm_load_graph_with_yaml(TOP_LEVEL_METRIC_YAML).unwrap();
    assert!(payload.contains("\"top_level_metrics\""));
    assert!(payload.contains("\"revenue_per_order\""));

    assert!(wasm_validate_model_payload("name: orders\ntable: orders\nprimary_key: id\n").unwrap());
    assert!(wasm_validate_metric_payload("name: revenue\ntype: derived\nsql: amount\n").unwrap());
    assert!(wasm_validate_parameter_payload("name: region\ntype: string\n").unwrap());
    assert!(
        wasm_validate_table_calculation_payload("name: pct\ntype: percent_of_total\n").unwrap()
    );
}

#[wasm_bindgen_test]
fn wasm_bindgen_runtime_sql_definition_parsers() {
    let definitions_payload = wasm_parse_sql_definitions_payload(
        "METRIC (name revenue, agg sum, sql amount);\nSEGMENT (name completed, sql status = 'completed');\n",
    )
    .unwrap();
    assert!(definitions_payload.contains("\"metrics\""));
    assert!(definitions_payload.contains("\"segments\""));
    assert!(definitions_payload.contains("\"revenue\""));

    let graph_payload = wasm_parse_sql_graph_definitions_payload(
        "PARAMETER (name region, type string);\nPRE_AGGREGATION (name daily_rollup, measures [revenue], dimensions [status]);\n",
    )
    .unwrap();
    assert!(graph_payload.contains("\"parameters\""));
    assert!(graph_payload.contains("\"pre_aggregations\""));

    let model_payload = wasm_parse_sql_model_payload(
        "MODEL (name orders, table orders, primary_key order_id);\nDIMENSION (name status, type categorical);\n",
    )
    .unwrap();
    assert!(model_payload.contains("\"name\":\"orders\""));
    assert!(model_payload.contains("\"dimensions\""));

    let statement_blocks_payload = wasm_parse_sql_statement_blocks_payload(
        "MODEL (name orders, table orders);\nMETRIC (name revenue, expression SUM(amount));\n",
    )
    .unwrap();
    assert!(statement_blocks_payload.contains("\"kind\":\"model\""));
    assert!(statement_blocks_payload.contains("\"kind\":\"metric\""));
    assert!(statement_blocks_payload.contains("\"sql\":\"SUM(amount)\""));

    let migrator_payload = wasm_analyze_migrator_query(
        "\nSELECT\n    status,\n    SUM(amount) / COUNT(*) AS avg_order_value\nFROM orders\nGROUP BY status\n",
    )
    .unwrap();
    assert!(migrator_payload.contains("\"group_by_columns\""));
    assert!(migrator_payload.contains("\"avg_order_value\""));
    let chart_columns: serde_json::Value = serde_json::from_str(
        &wasm_chart_auto_detect_columns("[\"created_at\",\"revenue\",\"region\"]", "[true,false]")
            .unwrap(),
    )
    .unwrap();
    assert_eq!(chart_columns["x"], "created_at");
    assert_eq!(chart_columns["y"], serde_json::json!(["revenue"]));
    assert_eq!(wasm_chart_select_type("created_at", "string", 1), "area");
    assert_eq!(wasm_chart_encoding_type("order_date"), "temporal");
    assert_eq!(
        wasm_chart_format_label("created_at__month"),
        "Created At (Month)"
    );

    assert!(wasm_is_sql_template("select {{ col }} from orders"));
    assert!(!wasm_is_sql_template("select col from orders"));

    let formatted_param =
        wasm_format_parameter_value_with_yaml("name: status\ntype: string\n", "\"complete\"\n")
            .unwrap();
    assert_eq!(formatted_param, "'complete'");

    let interpolated = wasm_interpolate_sql_with_parameters_with_yaml(
        "status = {{ status }} and amount >= {{ min_amount }}",
        "- name: status\n  type: string\n- name: min_amount\n  type: number\n",
        "status: complete\nmin_amount: 100\n",
    )
    .unwrap();
    assert!(interpolated.contains("status = 'complete'"));
    assert!(interpolated.contains("amount >= 100"));

    let parsed_today: Option<String> =
        serde_json::from_str(&wasm_parse_relative_date("today", "duckdb").unwrap()).unwrap();
    assert_eq!(parsed_today, Some("CURRENT_DATE".to_string()));
    let range_today: Option<String> = serde_json::from_str(
        &wasm_relative_date_to_range("today", "event_date", "duckdb").unwrap(),
    )
    .unwrap();
    assert_eq!(range_today, Some("event_date = CURRENT_DATE".to_string()));
    assert!(wasm_is_relative_date("last 7 days"));
    assert!(!wasm_is_relative_date("2024-01-01"));

    let offset_json = wasm_time_comparison_offset_interval("yoy", None, None).unwrap();
    let offset_value: serde_json::Value = serde_json::from_str(&offset_json).unwrap();
    assert_eq!(offset_value["amount"], 1);
    assert_eq!(offset_value["unit"], "year");
    assert_eq!(
        wasm_time_comparison_sql_offset("mom", None, None).unwrap(),
        "INTERVAL '1 month'"
    );
    assert_eq!(
        wasm_trailing_period_sql_interval(3, "month").unwrap(),
        "INTERVAL '3 month'"
    );
    let comparison_sql = wasm_generate_time_comparison_sql(
        "mom",
        "percent_change",
        "SUM(amount)",
        "order_date",
        None,
        None,
    )
    .unwrap();
    assert!(comparison_sql.contains("LAG("));
    assert!(comparison_sql.contains("ORDER BY order_date"));
    assert!(comparison_sql.contains("/ NULLIF"));

    let adapter_kind: Option<String> =
        serde_json::from_str(&wasm_detect_adapter_kind("orders.lkml", "").unwrap()).unwrap();
    assert_eq!(adapter_kind, Some("lookml".to_string()));

    let parsed_simple_agg: serde_json::Value = serde_json::from_str(
        &wasm_parse_simple_metric_aggregation("COUNT(DISTINCT customer_id)").unwrap(),
    )
    .unwrap();
    assert_eq!(parsed_simple_agg[0], "count_distinct");
    assert_eq!(parsed_simple_agg[1], "customer_id");

    let metric_yaml = "name: revenue\nagg: sum\nsql: amount\n";
    assert_eq!(wasm_metric_to_sql(metric_yaml).unwrap(), "SUM(amount)");
    assert_eq!(wasm_metric_sql_expr(metric_yaml).unwrap(), "amount");
    assert!(wasm_metric_is_simple_aggregation(metric_yaml).unwrap());

    let queries_json = serde_json::to_string(&vec![
        "select * from orders -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status granularities=day".to_string(),
        "select * from orders -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status granularities=day".to_string(),
        "select * from orders -- sidemantic: models=orders metrics=orders.count dimensions=orders.region".to_string(),
    ])
    .unwrap();
    let patterns_json = wasm_extract_preaggregation_patterns(&queries_json).unwrap();
    let summary_json = wasm_summarize_preaggregation_patterns(&patterns_json, 2).unwrap();
    assert!(summary_json.contains("\"total_queries\":3"));
    assert!(summary_json.contains("\"patterns_above_threshold\":1"));
    let recommendations_json =
        wasm_recommend_preaggregation_patterns(&patterns_json, 1, 0.0, Some(1)).unwrap();
    assert!(recommendations_json.contains("\"query_count\":2"));
    let score = wasm_calculate_preaggregation_benefit_score(
        r#"{"model":"orders","metrics":["orders.revenue"],"dimensions":["orders.status"],"granularities":["day"],"count":2}"#,
        2,
    )
    .unwrap();
    assert!(score > 0.0);
    let name = wasm_generate_preaggregation_name(
        r#"{"model":"orders","metrics":["orders.revenue"],"dimensions":["orders.status"],"granularities":["day"],"count":2}"#,
    )
    .unwrap();
    assert_eq!(name, "day_status_revenue");
    let definition_json = wasm_generate_preaggregation_definition(
        r#"{"pattern":{"model":"orders","metrics":["orders.revenue"],"dimensions":["orders.created_at","orders.status"],"granularities":["day"],"count":2},"suggested_name":"day_created_at_status_revenue","query_count":2,"estimated_benefit_score":0.5}"#,
    )
    .unwrap();
    assert!(definition_json.contains("\"name\":\"day_created_at_status_revenue\""));
    assert!(definition_json.contains("\"time_dimension\":\"created_at\""));
    assert!(definition_json.contains("\"granularity\":\"day\""));

    assert!(wasm_validate_models_yaml(SIMPLE_MODELS_YAML).unwrap());
    let parsed_reference: (String, String, Option<String>) = serde_json::from_str(
        &wasm_parse_reference_with_yaml(SIMPLE_MODELS_YAML, "orders.revenue").unwrap(),
    )
    .unwrap();
    assert_eq!(
        parsed_reference,
        ("orders".to_string(), "revenue".to_string(), None)
    );

    let model_inheritance_yaml = r#"
models:
  - name: base
    table: orders
    primary_key: id
  - name: child
    extends: base
    table: child_orders
    primary_key: id
"#;
    let resolved_models = wasm_resolve_model_inheritance_with_yaml(model_inheritance_yaml).unwrap();
    assert!(resolved_models.contains("name: base"));
    assert!(resolved_models.contains("name: child"));

    let metric_inheritance_yaml = r#"
- name: base
  agg: sum
  sql: amount
- name: child
  extends: base
"#;
    let resolved_metrics = wasm_resolve_metric_inheritance(metric_inheritance_yaml).unwrap();
    assert!(resolved_metrics.contains("name: base"));
    assert!(resolved_metrics.contains("name: child"));

    let dimension_yaml = r#"
name: created_at
type: time
sql: created_at
supported_granularities: [day, month]
"#;
    assert_eq!(
        wasm_dimension_sql_expr_with_yaml(dimension_yaml).unwrap(),
        "created_at"
    );
    assert_eq!(
        wasm_dimension_with_granularity_with_yaml(dimension_yaml, "month").unwrap(),
        "DATE_TRUNC('month', created_at)"
    );

    let model_hierarchy_yaml = r#"
dimensions:
  - name: country
  - name: state
    parent: country
  - name: city
    parent: state
"#;
    let hierarchy: Vec<String> = serde_json::from_str(
        &wasm_model_get_hierarchy_path_with_yaml(model_hierarchy_yaml, "city").unwrap(),
    )
    .unwrap();
    assert_eq!(hierarchy, vec!["country", "state", "city"]);

    let drill_down: Option<String> = serde_json::from_str(
        &wasm_model_get_drill_down_with_yaml(model_hierarchy_yaml, "country").unwrap(),
    )
    .unwrap();
    assert_eq!(drill_down, Some("state".to_string()));
    let drill_up: Option<String> = serde_json::from_str(
        &wasm_model_get_drill_up_with_yaml(model_hierarchy_yaml, "city").unwrap(),
    )
    .unwrap();
    assert_eq!(drill_up, Some("state".to_string()));

    let lookup_yaml = r#"
dimensions:
  - name: status
  - name: region
metrics:
  - name: revenue
  - name: count
segments:
  - name: active
  - name: priority
pre_aggregations:
  - name: daily
  - name: monthly
"#;
    let dim_idx: Option<usize> = serde_json::from_str(
        &wasm_model_find_dimension_index_with_yaml(lookup_yaml, "region").unwrap(),
    )
    .unwrap();
    assert_eq!(dim_idx, Some(1));
    let metric_idx: Option<usize> = serde_json::from_str(
        &wasm_model_find_metric_index_with_yaml(lookup_yaml, "count").unwrap(),
    )
    .unwrap();
    assert_eq!(metric_idx, Some(1));
    let segment_idx: Option<usize> = serde_json::from_str(
        &wasm_model_find_segment_index_with_yaml(lookup_yaml, "priority").unwrap(),
    )
    .unwrap();
    assert_eq!(segment_idx, Some(1));
    let preagg_idx: Option<usize> = serde_json::from_str(
        &wasm_model_find_pre_aggregation_index_with_yaml(lookup_yaml, "monthly").unwrap(),
    )
    .unwrap();
    assert_eq!(preagg_idx, Some(1));

    let relationship_yaml = r#"
name: customers
type: many_to_one
foreign_key: customer_id
"#;
    assert_eq!(
        wasm_relationship_sql_expr_with_yaml(relationship_yaml).unwrap(),
        "customer_id"
    );
    assert_eq!(
        wasm_relationship_related_key_with_yaml(relationship_yaml).unwrap(),
        "id"
    );
    let fk_cols: Vec<String> = serde_json::from_str(
        &wasm_relationship_foreign_key_columns_with_yaml(relationship_yaml).unwrap(),
    )
    .unwrap();
    assert_eq!(fk_cols, vec!["customer_id"]);
    let pk_cols: Vec<String> = serde_json::from_str(
        &wasm_relationship_primary_key_columns_with_yaml(relationship_yaml).unwrap(),
    )
    .unwrap();
    assert_eq!(pk_cols, vec!["id"]);

    assert_eq!(
        wasm_segment_get_sql_with_yaml("sql: \"{model}.status = 'completed'\"\n", "orders_cte")
            .unwrap(),
        "orders_cte.status = 'completed'"
    );

    let metric_dependency_models_yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    metrics:
      - name: revenue
        agg: sum
        sql: amount
      - name: cost
        agg: sum
        sql: cost
"#;
    let metric_dependency_yaml = r#"
name: margin
type: derived
sql: revenue / cost
"#;
    let deps: Vec<String> = serde_json::from_str(
        &wasm_extract_metric_dependencies_from_yaml(
            metric_dependency_yaml,
            Some(metric_dependency_models_yaml.to_string()),
            Some("orders".to_string()),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(deps, vec!["orders.cost", "orders.revenue"]);

    assert_eq!(
        wasm_evaluate_table_calculation_expression("1 + 2 * 3").unwrap(),
        7.0
    );
    assert!(wasm_validate_table_formula_expression("${a} + ${b}").unwrap());
    let refresh_valid: serde_json::Value = serde_json::from_str(
        &wasm_validate_engine_refresh_sql_compatibility("SELECT 1", "snowflake").unwrap(),
    )
    .unwrap();
    assert_eq!(refresh_valid["is_valid"], true);
    assert_eq!(refresh_valid["error"], serde_json::Value::Null);
    let refresh_statements: Vec<String> = serde_json::from_str(
        &wasm_build_preaggregation_refresh_statements(
            "incremental",
            "orders_preagg_daily_revenue",
            "SELECT order_date, SUM(revenue) AS total_revenue FROM orders GROUP BY order_date",
            Some("order_date".to_string()),
            Some("2026-01-01".to_string()),
            None,
            None,
            None,
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(refresh_statements.len(), 2);
    assert!(refresh_statements[1].contains("INSERT INTO orders_preagg_daily_revenue"));
    let symmetric_sql = wasm_build_symmetric_aggregate_sql(
        "amount",
        "order_id",
        "sum",
        Some("orders_cte".to_string()),
        "duckdb",
    )
    .unwrap();
    assert!(symmetric_sql.contains("SUM(DISTINCT"));
    assert!(symmetric_sql.contains("orders_cte.order_id"));
    assert!(wasm_needs_symmetric_aggregate("one_to_many", true));
    assert!(!wasm_needs_symmetric_aggregate("many_to_one", true));
}

#[wasm_bindgen_test]
fn wasm_bindgen_runtime_error_paths_cross_wasm_boundary() {
    let invalid_yaml = wasm_load_graph_with_yaml("models: [").unwrap_err();
    assert!(js_err_contains(&invalid_yaml, "yaml"));

    let invalid_query_yaml =
        wasm_compile_with_yaml_query(SIMPLE_MODELS_YAML, "metrics: [").unwrap_err();
    let invalid_query_text = js_err_text(&invalid_query_yaml);
    assert!(
        invalid_query_text.to_ascii_lowercase().contains("parse")
            || invalid_query_text.to_ascii_lowercase().contains("eof")
            || invalid_query_text.to_ascii_lowercase().contains("expected"),
        "{invalid_query_text}"
    );

    let invalid_sql_definition = wasm_parse_sql_statement_blocks_payload("MODEL (").unwrap_err();
    assert!(
        js_err_text(&invalid_sql_definition).contains("Validation")
            || js_err_text(&invalid_sql_definition).contains("parse")
    );

    let invalid_json = wasm_find_models_for_query("not-json", "[]").unwrap_err();
    assert!(js_err_text(&invalid_json).contains("expected"));

    let invalid_chart_json =
        wasm_chart_auto_detect_columns("[\"status\",\"revenue\"]", "not-json").unwrap_err();
    assert!(js_err_text(&invalid_chart_json).contains("expected"));

    let mismatched_chart_flags =
        wasm_chart_auto_detect_columns("[\"status\",\"revenue\"]", "[false,true]").unwrap_err();
    assert!(js_err_text(&mismatched_chart_flags).contains("numeric flag count mismatch"));

    let missing_ref_errors_json = wasm_validate_query_references_with_yaml(
        SIMPLE_MODELS_YAML,
        "[\"orders.missing_metric\"]",
        "[]",
    )
    .unwrap();
    let missing_ref_errors: Vec<String> = serde_json::from_str(&missing_ref_errors_json).unwrap();
    assert_eq!(missing_ref_errors.len(), 1);
    assert!(missing_ref_errors[0].contains("missing_metric"));

    let missing_join_model =
        wasm_find_relationship_path_with_yaml(SIMPLE_MODELS_YAML, "orders", "missing").unwrap_err();
    assert!(js_err_text(&missing_join_model).contains("missing"));

    let incremental_without_watermark = wasm_build_preaggregation_refresh_statements(
        "incremental",
        "orders_preagg",
        "SELECT 1",
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap_err();
    assert!(js_err_text(&incremental_without_watermark).contains("watermark"));
}

#[wasm_bindgen_test]
fn wasm_bindgen_runtime_rewrite_fallback_rejects_unsupported_sql_shapes() {
    let select_star =
        wasm_rewrite_with_yaml(SIMPLE_MODELS_YAML, "SELECT * FROM orders").unwrap_err();
    assert!(js_err_text(&select_star).contains("SELECT *"));

    let explicit_join = wasm_rewrite_with_yaml(
        SIMPLE_MODELS_YAML,
        "SELECT orders.revenue FROM orders JOIN customers ON orders.customer_id = customers.id",
    )
    .unwrap_err();
    assert!(js_err_text(&explicit_join).contains("unsupported clause"));

    let cte_query = wasm_rewrite_with_yaml(
        SIMPLE_MODELS_YAML,
        "WITH base AS (SELECT * FROM orders) SELECT * FROM base",
    )
    .unwrap_err();
    assert!(js_err_text(&cte_query).contains("only supports SELECT"));

    let grouped_query = wasm_rewrite_with_yaml(
        SIMPLE_MODELS_YAML,
        "SELECT orders.revenue FROM orders GROUP BY orders.status",
    )
    .unwrap_err();
    assert!(js_err_text(&grouped_query).contains("unsupported clause"));

    let subquery_from = wasm_rewrite_with_yaml(
        SIMPLE_MODELS_YAML,
        "SELECT orders.revenue FROM (SELECT * FROM orders) orders",
    )
    .unwrap_err();
    assert!(js_err_text(&subquery_from).contains("single table"));
}
