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
