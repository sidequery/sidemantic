use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use sidemantic::{OssieConsumerProfile, OssieForwardAdapter, OssieSerialization, OssieTarget};

#[derive(Debug, Deserialize)]
struct Manifest {
    cases: Vec<FixtureCase>,
}

#[derive(Debug, Deserialize)]
struct FixtureCase {
    id: String,
    input: String,
    serialization: String,
    expected: Expected,
}

#[derive(Debug, Deserialize)]
struct Expected {
    valid: bool,
    #[serde(default)]
    diagnostics: Vec<ExpectedDiagnostic>,
}

#[derive(Debug, Deserialize)]
struct ExpectedDiagnostic {
    code: String,
    instance_path: String,
}

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("tests/ossie-fixtures")
}

#[test]
fn shared_ossie_fixture_manifest_matches_strict_rust_diagnostics() {
    let root = fixture_root();
    let manifest: Manifest =
        serde_yaml::from_str(&fs::read_to_string(root.join("manifest.yaml")).unwrap()).unwrap();

    for case in manifest.cases {
        let serialization = OssieSerialization::parse(&case.serialization).unwrap();
        let content = fs::read_to_string(root.join(&case.input)).unwrap();
        let status =
            OssieForwardAdapter.inspect(&content, serialization, OssieConsumerProfile::OssieCore);

        assert_eq!(status.valid, case.expected.valid, "fixture {}", case.id);
        for expected in case.expected.diagnostics {
            assert!(
                status.diagnostics.iter().any(|actual| {
                    actual.code == expected.code && actual.instance_path == expected.instance_path
                }),
                "fixture {} missing diagnostic {} at {}; actual: {:?}",
                case.id,
                expected.code,
                expected.instance_path,
                status.diagnostics
            );
        }
    }
}

fn reordered_key_document() -> serde_json::Value {
    serde_json::from_str(
        &fs::read_to_string(fixture_root().join("cases/logical-composite-key-reordered.json"))
            .unwrap(),
    )
    .unwrap()
}

#[test]
fn reordered_declared_keys_preserve_relationship_pair_order() {
    for unique_key in [false, true] {
        let mut document = reordered_key_document();
        if unique_key {
            let target = document["semantic_model"][0]["datasets"][1]
                .as_object_mut()
                .unwrap();
            let key = target.remove("primary_key").unwrap();
            target.insert("unique_keys".to_string(), serde_json::json!([key]));
        }
        let catalog = OssieForwardAdapter
            .parse_catalog(
                &document.to_string(),
                OssieSerialization::Json,
                OssieConsumerProfile::OssieCore,
                OssieTarget::DuckDb,
            )
            .unwrap();
        let relationship = &catalog.scopes[0].models[0].relationships[0];
        assert_eq!(
            relationship.foreign_key_columns(),
            vec!["customer_id", "tenant_id"]
        );
        assert_eq!(relationship.primary_key_columns(), vec!["id", "tenant_id"]);
    }
}

#[test]
fn unique_key_duplicates_compare_normalized_column_sets() {
    let mut document = reordered_key_document();
    document["semantic_model"][0]["datasets"][1]["unique_keys"] =
        serde_json::json!([["ID", "TENANT_ID"]]);
    let status = OssieForwardAdapter.inspect(
        &document.to_string(),
        OssieSerialization::Json,
        OssieConsumerProfile::OssieCore,
    );
    assert!(status.valid, "{:?}", status.diagnostics);

    document["semantic_model"][0]["datasets"][1]["unique_keys"] =
        serde_json::json!([["ID", "TENANT_ID"], ["tenant_id", "id"]]);
    let status = OssieForwardAdapter.inspect(
        &document.to_string(),
        OssieSerialization::Json,
        OssieConsumerProfile::OssieCore,
    );
    assert!(status.diagnostics.iter().any(|diagnostic| {
        diagnostic.code == "ossie.semantic.dataset.key_duplicate"
            && diagnostic.instance_path == "/semantic_model/0/datasets/1/unique_keys/1"
    }));

    document["semantic_model"][0]["datasets"][1]["unique_keys"] = serde_json::json!([["id", "ID"]]);
    let status = OssieForwardAdapter.inspect(
        &document.to_string(),
        OssieSerialization::Json,
        OssieConsumerProfile::OssieCore,
    );
    assert!(status
        .diagnostics
        .iter()
        .any(|diagnostic| { diagnostic.code == "ossie.semantic.dataset.key_column_duplicate" }));
}

#[test]
fn current_metadata_and_dialects_validate_without_becoming_sql_targets() {
    let content = fs::read_to_string(
        fixture_root().join("cases/logical-0.2-current-dialects-vendors/document.json"),
    )
    .unwrap();
    let status = OssieForwardAdapter.inspect(
        &content,
        OssieSerialization::Json,
        OssieConsumerProfile::OssieCore,
    );
    assert!(status.valid, "{:?}", status.diagnostics);
    let catalog = OssieForwardAdapter
        .parse_catalog(
            &content,
            OssieSerialization::Json,
            OssieConsumerProfile::OssieCore,
            OssieTarget::DuckDb,
        )
        .unwrap();
    assert_eq!(
        catalog.scopes[0].models[0].dimensions[0].sql.as_deref(),
        Some("amount")
    );
    let mut non_sql: serde_json::Value = serde_json::from_str(&content).unwrap();
    non_sql["semantic_model"][0]["datasets"][0]["fields"][0]["expression"]["dialects"]
        .as_array_mut()
        .unwrap()
        .remove(0);
    let error = OssieForwardAdapter
        .parse_catalog(
            &non_sql.to_string(),
            OssieSerialization::Json,
            OssieConsumerProfile::OssieCore,
            OssieTarget::DuckDb,
        )
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("ossie.lowering.expression_unavailable"));
    for dialect in ["SIGMA", "THOUGHTSPOT"] {
        assert!(OssieTarget::parse(dialect).is_err());
    }
    let mut document: serde_json::Value = serde_json::from_str(&content).unwrap();
    document["version"] = serde_json::json!("0.1.1");
    assert!(
        !OssieForwardAdapter
            .inspect(
                &document.to_string(),
                OssieSerialization::Json,
                OssieConsumerProfile::OssieCore,
            )
            .valid
    );
    document["version"] = serde_json::json!("0.2.0.dev0");
    document["vendors"] = serde_json::json!([42]);
    assert!(
        !OssieForwardAdapter
            .inspect(
                &document.to_string(),
                OssieSerialization::Json,
                OssieConsumerProfile::OssieCore,
            )
            .valid
    );
}

const LATEST_REVISION: &str = "172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1";
const OCTOBER_REVISION: &str = "b6c702ed1c07e91382a69e870c875cbd19570828";

fn latest_document() -> serde_json::Value {
    serde_json::json!({"version": "0.2.0.dev0", "name": "commerce", "datasets": [{
        "name": "orders", "source": "orders", "fields": [
            {"name": "amount", "expression": {"sql": "amount * 2"}}
        ], "metrics": [
            {"name": "total", "expression": {"sql": "SUM(amount)"}},
            {"name": "physical", "expression": {"sql": "SUM(source.amount)"}},
            {"name": "rows", "expression": {"sql": "COUNT(*)"}}
        ]
    }]})
}

fn latest_scope(
    document: &serde_json::Value,
) -> sidemantic::Result<sidemantic::OssieCompiledScope> {
    OssieForwardAdapter.select_scope_with_revision(
        &document.to_string(),
        OssieSerialization::Json,
        OssieConsumerProfile::OssieCore,
        OssieTarget::DuckDb,
        None,
        Some(LATEST_REVISION),
    )
}

#[test]
fn latest_features_infer_an_immutable_revision_and_preserve_historical_selection() {
    let document = latest_document();
    let status = OssieForwardAdapter.inspect(
        &document.to_string(),
        OssieSerialization::Json,
        OssieConsumerProfile::OssieCore,
    );
    assert!(status.valid, "{:?}", status.diagnostics);
    assert_eq!(
        status.profile.unwrap().schema_revision.as_deref(),
        Some(LATEST_REVISION)
    );
    let historical = OssieForwardAdapter.inspect_with_revision(
        &document.to_string(),
        OssieSerialization::Json,
        OssieConsumerProfile::OssieCore,
        Some(OCTOBER_REVISION),
    );
    assert!(!historical.valid);
    assert!(historical
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic.code == "ossie.schema.additional_properties"));
    let ambiguous = serde_json::json!({"version":"0.2.0.dev0", "name":"old", "datasets":[{"name":"t", "source":"t"}]});
    let inferred = OssieForwardAdapter.inspect(
        &ambiguous.to_string(),
        OssieSerialization::Json,
        OssieConsumerProfile::OssieCore,
    );
    assert_eq!(
        inferred.profile.unwrap().schema_revision.as_deref(),
        Some(OCTOBER_REVISION)
    );
    assert!(OssieForwardAdapter
        .inspect_with_revision(
            &ambiguous.to_string(),
            OssieSerialization::Json,
            OssieConsumerProfile::OssieCore,
            Some("unknown")
        )
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic.code == "ossie.schema.profile_unsupported"));
}

#[test]
fn latest_sql_is_authoritative_portable_form_with_exact_target_precedence() {
    let mut document = latest_document();
    document["datasets"][0]["fields"][0]["expression"] = serde_json::json!({
        "sql":"amount * 2", "dialects":[
            {"dialect":"OSSIE_SQL_2026", "expression":"amount * 999"},
            {"dialect":"UNREGISTERED_VENDOR", "expression":"opaque text"},
            {"dialect":"SNOWFLAKE", "expression":"amount * 3"}
        ]
    });
    let scope = latest_scope(&document).unwrap();
    assert_eq!(
        scope.models[0].dimensions[0].sql.as_deref(),
        Some("amount * 2")
    );
    let scope = OssieForwardAdapter
        .select_scope(
            &document.to_string(),
            OssieSerialization::Json,
            OssieConsumerProfile::OssieCore,
            OssieTarget::Snowflake,
            None,
        )
        .unwrap();
    assert_eq!(
        scope.models[0].dimensions[0].sql.as_deref(),
        Some("amount * 3")
    );
    document["datasets"][0]["fields"][0]["expression"] = serde_json::json!({"dialects":[{"dialect":"UNREGISTERED_VENDOR", "expression":"opaque text"}]});
    assert!(latest_scope(&document).is_err());
}

#[test]
fn latest_dataset_metrics_bind_fields_source_columns_and_row_counts_separately() {
    let scope = latest_scope(&latest_document()).unwrap();
    assert_eq!(
        scope.models[0].metadata.as_ref().unwrap()["ossie_scope_name"],
        "commerce"
    );
    assert_eq!(
        scope
            .metrics
            .iter()
            .map(|metric| metric.name.as_str())
            .collect::<Vec<_>>(),
        vec!["orders.total", "orders.physical", "orders.rows"]
    );
    assert_eq!(scope.metrics[0].sql.as_deref(), Some("SUM(orders.amount)"));
    let graph = scope.into_graph().unwrap();
    assert_eq!(graph.metric_owner("orders.rows"), Some("orders"));
    let sql = sidemantic::SqlGenerator::new(&graph)
        .generate(&sidemantic::SemanticQuery::new().with_metrics(vec![
            "orders.total".into(),
            "orders.physical".into(),
            "orders.rows".into(),
        ]))
        .unwrap();
    assert!(sql.contains("amount * 2"), "{sql}");
    assert!(sql.contains("amount AS"), "{sql}");
    assert!(sql.contains("1 AS"), "{sql}");
    assert!(!sql.contains("source.amount"), "{sql}");
}

#[test]
fn latest_metric_namespaces_reject_undeclared_cross_dataset_and_metric_references() {
    for sql in [
        "SUM(missing)",
        "SUM(orders.amount)",
        "SUM(other.amount)",
        "total * 2",
    ] {
        let mut document = latest_document();
        document["datasets"][0]["metrics"][0]["expression"] = serde_json::json!({"sql": sql});
        assert!(latest_scope(&document).is_err(), "dataset metric {sql}");
    }
    for sql in [
        "SUM(amount)",
        "SUM(orders.missing)",
        "SUM(orders.total)",
        "global * 2",
    ] {
        let mut document = latest_document();
        document["metrics"] = serde_json::json!([{"name":"global", "expression":{"sql":sql}}]);
        assert!(latest_scope(&document).is_err(), "model metric {sql}");
    }
    let mut document = latest_document();
    document["metrics"] =
        serde_json::json!([{"name":"global", "expression":{"sql":"SUM(orders.amount)"}}]);
    assert!(latest_scope(&document).is_ok());
}

#[test]
fn latest_dataset_field_metric_collisions_and_source_shadowing_fail_closed() {
    let mut document = latest_document();
    document["datasets"][0]["metrics"][0]["name"] = serde_json::json!("AMOUNT");
    let status = OssieForwardAdapter.inspect(
        &document.to_string(),
        OssieSerialization::Json,
        OssieConsumerProfile::OssieCore,
    );
    assert!(status
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic.code == "ossie.semantic.metric.field_name_collision"));
    let mut document = latest_document();
    document["datasets"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({"name":"source", "expression":{"sql":"amount"}}));
    assert!(latest_scope(&document)
        .unwrap_err()
        .to_string()
        .contains("shadows"));
}

#[test]
fn portable_window_order_is_explicit_in_final_duckdb_sql() {
    for (ordering, placement) in [
        ("ASC", "ASC NULLS LAST"),
        ("DESC", "DESC NULLS FIRST"),
        ("ASC NULLS FIRST", "ASC NULLS FIRST"),
        ("DESC NULLS LAST", "DESC NULLS LAST"),
    ] {
        let document = serde_json::json!({"version":"0.2.0.dev0", "name":"window", "datasets":[{
            "name":"orders", "source":"orders", "fields":[
                {"name":"amount", "expression":{"sql":"amount"}},
                {"name":"position", "expression":{"sql":format!("ROW_NUMBER() OVER (ORDER BY amount {ordering})")}}
            ]
        }]});
        let graph = latest_scope(&document).unwrap().into_graph().unwrap();
        let sql = sidemantic::SqlGenerator::new(&graph)
            .generate(
                &sidemantic::SemanticQuery::new()
                    .with_dimensions(vec!["orders.amount".into(), "orders.position".into()]),
            )
            .unwrap();
        assert!(sql.contains(&format!("amount {placement}")), "{sql}");
    }
}

#[test]
fn latest_official_scott_model_compiles_with_declared_dataset_owners() {
    let path = fixture_root().join("latest-query-172a3e5/scott/scott_model.ossie.yaml");
    let content = fs::read_to_string(path).unwrap();
    let catalog = OssieForwardAdapter
        .parse_catalog(
            &content,
            OssieSerialization::Yaml,
            OssieConsumerProfile::OssieCore,
            OssieTarget::DuckDb,
        )
        .unwrap();
    assert_eq!(
        catalog.profile.schema_revision.as_deref(),
        Some(LATEST_REVISION)
    );
    let graph = catalog
        .scopes
        .into_iter()
        .next()
        .unwrap()
        .into_graph()
        .unwrap();
    assert_eq!(graph.metric_owner("emp.sum_sal"), Some("emp"));
    assert_eq!(graph.metric_owner("dept.dept_count"), Some("dept"));
    let sql = sidemantic::SqlGenerator::new(&graph)
        .generate(
            &sidemantic::SemanticQuery::new()
                .with_metrics(vec!["emp.sum_sal".into(), "emp.emp_count".into()]),
        )
        .unwrap();
    assert!(sql.contains("FROM scott.emp"), "{sql}");
    assert!(sql.contains("SUM("), "{sql}");
    assert!(sql.contains("COUNT("), "{sql}");
}

#[test]
fn latest_filter_predicates_do_not_cross_dataset_populations() {
    let mut document = latest_document();
    document["datasets"].as_array_mut().unwrap().push(serde_json::json!({
        "name":"returns", "source":"returns", "fields":[{"name":"amount", "expression":{"sql":"amount"}}]
    }));
    for expression in [
        "SUM(orders.amount) FILTER (WHERE returns.amount > 0)",
        "ROUND(orders.amount) FILTER (WHERE orders.amount > 0)",
    ] {
        document["metrics"] = serde_json::json!([{"name":"bad", "expression":{"sql":expression}}]);
        assert!(latest_scope(&document).is_err(), "{expression}");
    }
    document["metrics"] = serde_json::json!([{"name":"good", "expression":{"sql":"SUM(orders.amount) FILTER (WHERE orders.amount > 0)"}}]);
    assert!(latest_scope(&document).is_ok());
}

#[test]
fn latest_scope_identity_is_opaque_and_model_aggregate_grain_is_query_time() {
    let mut document = latest_document();
    document["name"] = serde_json::json!("Sales semantic model");
    assert_eq!(
        latest_scope(&document).unwrap().scope_id,
        "Sales semantic model"
    );
    for expression in [
        "COUNT(*)",
        "SUM(1)",
        "COUNT(*) FILTER (WHERE orders.amount > 0)",
    ] {
        document["metrics"] =
            serde_json::json!([{"name":"ambiguous", "expression":{"sql":expression}}]);
        let graph = latest_scope(&document).unwrap().into_graph().unwrap();
        let result = sidemantic::SqlGenerator::new(&graph)
            .generate_ossie_wide(&serde_json::json!({"metrics":["ambiguous"]}));
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("E_AMBIGUOUS_MEASURE_GRAIN"),
            "{expression}"
        );
    }
}

#[test]
fn latest_portable_agg_is_reserved_without_reinterpreting_historical_or_vendor_sql() {
    let mut document = latest_document();
    for function in ["AGG", "agg", "Agg"] {
        document["datasets"][0]["fields"][0]["expression"] =
            serde_json::json!({"sql":format!("{function}(amount)")});
        assert!(latest_scope(&document)
            .unwrap_err()
            .to_string()
            .contains("reserved"));
    }
    document["datasets"][0]["fields"][0]["expression"] = serde_json::json!({"sql":"amount", "dialects":[{"dialect":"OSSIE_SQL_2026", "expression":"AGG(amount)"}]});
    assert!(
        latest_scope(&document).is_ok(),
        "authoritative sql overrides the older portable variant"
    );
    document["datasets"][0]["fields"][0]["expression"] =
        serde_json::json!({"dialects":[{"dialect":"ANSI_SQL", "expression":"AGG(amount)"}]});
    assert!(
        latest_scope(&document).is_ok(),
        "vendor expressions retain their meaning"
    );
    document["datasets"][0]
        .as_object_mut()
        .unwrap()
        .remove("metrics");
    document["datasets"][0]["fields"][0]["expression"] =
        serde_json::json!({"dialects":[{"dialect":"OSSIE_SQL_2026", "expression":"AGG(amount)"}]});
    assert!(OssieForwardAdapter
        .select_scope_with_revision(
            &document.to_string(),
            OssieSerialization::Json,
            OssieConsumerProfile::OssieCore,
            OssieTarget::DuckDb,
            None,
            Some(OCTOBER_REVISION)
        )
        .is_ok());
}

#[test]
fn dataset_count_star_filter_binds_its_predicate_before_runtime_handoff() {
    let mut document = latest_document();
    document["datasets"][0]["metrics"][2]["expression"] =
        serde_json::json!({"sql":"COUNT(*) FILTER (WHERE amount > 10)"});
    let scope = latest_scope(&document).unwrap();
    assert!(scope.metrics[2]
        .sql
        .as_ref()
        .unwrap()
        .contains("orders.amount > 10"));
    assert!(scope.into_graph().is_ok());
    document["datasets"][0]["metrics"][2]["expression"] =
        serde_json::json!({"sql":"COUNT(*) FILTER (WHERE missing > 10)"});
    assert!(latest_scope(&document).is_err());
}
