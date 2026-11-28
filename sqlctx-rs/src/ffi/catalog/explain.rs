use super::*;

#[no_mangle]
pub extern "C" fn sqlctx_compiler_explain(
    compiler: *const compiler::SqlctxCompiler,
    sql: *const c_char,
) -> SqlctxRewriteResult {
    semantic_result(
        guard(|| {
            let compiler = unsafe { compiler.as_ref() }.ok_or("null compiler pointer")?;
            let sql = required_arg(sql, "sql")?;
            explain_prepared_graph(&compiler.graph, &compiler.views, &sql)
        })
        .map_err(semantic_error),
    )
}

/// Compile against exactly the same catalog and routing rules as snapshot
/// rewrite, while retaining observations from the actual compiler invocation.
#[no_mangle]
pub extern "C" fn sqlctx_snapshot_explain(
    snapshot: *const c_char,
    sql: *const c_char,
) -> SqlctxRewriteResult {
    semantic_result(
        guard(|| {
            let graph = Snapshot::parse(&optional_arg(snapshot, "snapshot")?)?.into_graph()?;
            let sql = required_arg(sql, "sql")?;
            explain_graph(&graph, &sql)
        })
        .map_err(semantic_error),
    )
}

pub(super) fn explain_graph(graph: &SemanticGraph, sql: &str) -> CatalogResult<String> {
    let views = views::PreparedViews::new(graph)?;
    explain_prepared_graph(graph, &views, sql)
}

fn explain_prepared_graph(
    graph: &SemanticGraph,
    views: &views::PreparedViews,
    sql: &str,
) -> CatalogResult<String> {
    crate::semantic_input::with_semantic_stack(|| {
        let ((compiled, warnings), events) = crate::sql::explain::capture(|| {
            rewrite_prepared_graph_diagnostics(graph, views, sql)
                .map_err(crate::error::SqlctxError::Validation)
        })?;
        let mut candidates = graph
            .models()
            .flat_map(|model| {
                model.pre_aggregations.iter().map(|candidate| {
                    serde_json::json!({
                        "model": model.name, "name": candidate.name,
                        "status": "declared; eligibility is reported only when evaluated",
                    })
                })
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(serde_json::Value::to_string);
        let coverage = concat!(
            "Compiler observations; absent events do not imply safety or eligibility. ",
            "Specialized routes may expose only their route and query. ",
            "Database physical planning, table availability and row counts are not evaluated."
        );
        Ok(serde_json::json!({
            "version": 1, "sql": compiled, "warnings": warnings,
            "events": events, "rollup_candidates": candidates, "coverage": coverage,
        })
        .to_string())
    })
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Dimension, Metric, Model, Relationship};
    use serde_json::Value;

    fn graph() -> SemanticGraph {
        let mut graph = SemanticGraph::new();
        graph
            .add_model(
                Model::new("orders", "id")
                    .with_table("raw_orders")
                    .with_dimension(Dimension::categorical("status"))
                    .with_metric(Metric::sum("revenue", "amount"))
                    .with_relationship(Relationship::many_to_one("customers")),
            )
            .unwrap();
        graph
            .add_model(
                Model::new("customers", "id")
                    .with_table("raw_customers")
                    .with_dimension(Dimension::categorical("country"))
                    .with_metric(Metric::count("customers_count")),
            )
            .unwrap();
        graph
    }

    fn explain(sql: &str) -> Value {
        let graph = graph();
        let snapshot =
            CString::new(serde_json::to_string(&Snapshot::from_graph(&graph)).unwrap()).unwrap();
        let query = CString::new(sql).unwrap();
        let report = sqlctx_snapshot_explain(snapshot.as_ptr(), query.as_ptr());
        assert!(
            report.error.is_null(),
            "{}",
            if report.error.is_null() {
                String::new()
            } else {
                unsafe { CStr::from_ptr(report.error) }
                    .to_string_lossy()
                    .into_owned()
            }
        );
        let value =
            serde_json::from_str(unsafe { CStr::from_ptr(report.sql) }.to_str().unwrap()).unwrap();
        super::super::super::sqlctx_free_result(report);
        value
    }

    #[test]
    fn explanation_uses_the_same_compilation_and_reports_population_changes() {
        let sql =
            "select customers.country, orders.revenue from orders where customers.country = 'US'";
        let report = explain(sql);
        assert_eq!(
            report["sql"],
            QueryRewriter::new(&graph())
                .with_qualified_tables_physical()
                .rewrite_with_dialect(sql, polyglot_sql::DialectType::DuckDB)
                .unwrap()
        );
        let events = report["events"].as_array().unwrap();
        assert!(events.iter().any(|event| event["kind"] == "grain"
            && event["dimensions"] == serde_json::json!(["customers.country"])));
        assert!(events.iter().any(|event| event["kind"] == "join"
            && event["from"] == "orders"
            && event["to"] == "customers"
            && event["join_type"] == "INNER JOIN"));
        assert!(!events.iter().any(|event| event["kind"] == "fanout_guard"));
    }

    #[test]
    fn fanout_explanation_records_symmetric_guard_and_physical_passthrough_is_empty() {
        let report = explain("select orders.status, customers.customers_count from customers");
        let events = report["events"].as_array().unwrap();
        assert!(events.iter().any(|event| event["kind"] == "fanout_guard"
            && event["model"] == "customers"
            && event["strategy"] == "count_distinct_entity"
            && event["entity_keys"] == serde_json::json!(["id"])));
        assert_eq!(
            explain("select * from main.orders")["events"],
            serde_json::json!([])
        );
    }

    #[test]
    fn named_selection_explanation_uses_filtered_ratio_expansion() {
        let (snapshot, _) = apply(
            "",
            "",
            "yaml",
            r#"
models:
  - name: orders
    table: raw_orders
    primary_key: id
    dimensions:
      - {name: status, type: categorical}
    metrics:
      - {name: paid, agg: sum, sql: amount, filters: ["status = 'paid'"]}
      - {name: rows, agg: count}
      - {name: average, type: ratio, numerator: paid, denominator: rows}
"#,
            false,
        )
        .unwrap();
        let (snapshot, _) = apply(
            &snapshot,
            "orders",
            "view",
            "sales AS SELECT status, paid, average AS aov FROM orders",
            false,
        )
        .unwrap();
        let graph = Snapshot::parse(&snapshot).unwrap().into_graph().unwrap();
        for sql in [
            "select status, aov, paid from sales",
            "select aov, paid from sales",
        ] {
            let report: Value = serde_json::from_str(&explain_graph(&graph, sql).unwrap()).unwrap();
            assert_eq!(report["sql"], rewrite_graph(&graph, sql).unwrap());
            let compiled = report["sql"].as_str().unwrap();
            assert!(
                compiled.contains("NULLIF") && compiled.contains("CASE WHEN"),
                "{compiled}"
            );
            assert!(!report["events"].as_array().unwrap().is_empty());
        }
    }

    #[test]
    fn compiler_events_follow_actual_aggregate_and_rollup_routes() {
        crate::semantic_input::with_semantic_stack(|| {
            let mut graph = graph();
            graph.set_metric_scopes(Default::default()).unwrap();
            let query = crate::sql::SemanticQuery::new()
                .with_dimensions(vec!["orders.status".into()])
                .with_metrics(vec!["customers.customers_count".into()]);
            let (sql, events) = crate::sql::explain::capture(|| {
                crate::sql::SqlGenerator::new(&graph).generate(&query)
            })?;
            assert!(
                events.iter().any(|event| event["kind"] == "aggregate_plan"),
                "{sql}\n{events:?}"
            );
            assert!(
                events
                    .iter()
                    .any(|event| event["strategy"] == "distinct_entity_rows"),
                "{events:?}"
            );

            let mut graph = self::graph();
            let mut orders = graph.get_model("orders").unwrap().clone();
            orders.pre_aggregations.push(
                serde_json::from_value(serde_json::json!({
                    "name": "by_status", "dimensions": ["status"], "measures": ["revenue"]
                }))
                .unwrap(),
            );
            graph.replace_model(orders).unwrap();
            let query = crate::sql::SemanticQuery::new()
                .with_dimensions(vec!["orders.status".into()])
                .with_metrics(vec!["orders.revenue".into()])
                .with_use_preaggregations(true);
            let (sql, events) = crate::sql::explain::capture(|| {
                crate::sql::SqlGenerator::new(&graph).generate(&query)
            })?;
            assert!(sql.contains("used_preagg=true"));
            assert!(events
                .iter()
                .any(|event| event["kind"] == "rollup_selected" && event["name"] == "by_status"));
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn declared_rollups_are_not_claimed_eligible_when_routing_is_disabled() {
        let mut graph = graph();
        let mut orders = graph.get_model("orders").unwrap().clone();
        orders.pre_aggregations.push(
            serde_json::from_value(serde_json::json!({
                "name": "by_status", "dimensions": ["status"], "measures": ["revenue"]
            }))
            .unwrap(),
        );
        graph.replace_model(orders).unwrap();
        let report: Value = serde_json::from_str(
            &explain_graph(&graph, "select status, revenue from orders").unwrap(),
        )
        .unwrap();
        assert_eq!(report["rollup_candidates"][0]["name"], "by_status");
        assert!(report["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["kind"] == "query" && event["rollup_routing_enabled"] == false));
        assert!(!report["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["kind"] == "rollup_selected"));
    }
}
