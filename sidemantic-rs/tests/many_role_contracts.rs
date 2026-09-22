use serde_json::json;
use sidemantic::semantic_input::compile_with_semantic_input;

fn source() -> serde_json::Value {
    json!({
        "version": 1,
        "input_dialect": "duckdb",
        "models": [
            {
                "name": "orders", "table": "orders", "primary_key": "id",
                "dimensions": [{"name": "id", "type": "numeric"}],
                "metrics": [{"name": "revenue", "agg": "sum", "sql": "amount"}],
                "relationships": [
                    {"name": "primary_tags", "target_model": "tags", "type": "many_to_many",
                     "through": "links", "through_foreign_key": "order_id",
                     "related_foreign_key": "primary_tag", "edge_id": "primary_tags"},
                    {"name": "secondary_tags", "target_model": "tags", "type": "many_to_many",
                     "through": "links", "through_foreign_key": "order_id",
                     "related_foreign_key": "secondary_tag", "edge_id": "secondary_tags"}
                ]
            },
            {
                "name": "tags", "table": "tags", "primary_key": "id",
                "dimensions": [{"name": "name", "type": "categorical"}],
                "metrics": [{"name": "sum_ids", "agg": "sum", "sql": "id"}]
            },
            {"name": "links", "table": "links", "primary_key": "id"}
        ]
    })
}

#[test]
fn target_metric_inputs_keep_the_requested_role() {
    for role in ["primary_tags", "secondary_tags"] {
        let query = json!({"metrics": [format!("{role}.sum_ids")], "dimensions": ["orders.id"]});
        let sql = compile_with_semantic_input(&source().to_string(), &query.to_string()).unwrap();
        assert!(sql.contains(&format!("{role}_cte")), "{sql}");
        assert!(sql.contains(&format!("{role}$through_cte")), "{sql}");
        assert!(sql.contains("SELECT DISTINCT"), "{sql}");
    }
}

#[test]
fn junction_policies_filter_links_without_changing_source_preservation() {
    let mut source = source();
    source["models"][2]["security"] = json!({"row_filters": ["tenant = {{ user.tenant }}"]});
    source["models"][2]["invariant_filters"] = json!(["enabled"]);
    let query = json!({
        "metrics": ["orders.revenue"],
        "dimensions": ["primary_tags.name", "secondary_tags.name"],
        "user_attributes": {"tenant": "a"}
    });
    let sql = compile_with_semantic_input(&source.to_string(), &query.to_string()).unwrap();
    for role in ["primary_tags", "secondary_tags"] {
        assert!(
            sql.contains(&format!("LEFT JOIN {role}$through_cte")),
            "{sql}"
        );
        assert!(
            !sql.contains(&format!("INNER JOIN {role}$through_cte")),
            "{sql}"
        );
    }
    assert_eq!(sql.matches("tenant = 'a' AND enabled").count(), 2, "{sql}");
}
