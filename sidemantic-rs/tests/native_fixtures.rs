use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;
use sidemantic::{
    load_from_directory, runtime::interpolate_query_filters, QueryRewriter, SemanticQuery,
    SqlGenerator, TableCalculation,
};

#[cfg(feature = "adbc-exec")]
use {
    adbc_core::options::{OptionDatabase, OptionValue},
    chrono::{Datelike, NaiveDate},
    serde_json::Value as JsonValue,
    sidemantic::{execute_with_adbc, AdbcExecutionRequest, AdbcValue},
};

#[derive(Debug, Deserialize)]
struct FixtureManifest {
    fixtures: Vec<FixtureCase>,
}

#[derive(Debug, Deserialize)]
struct FixtureCase {
    name: String,
    #[serde(default)]
    valid: Option<bool>,
    #[serde(default)]
    #[cfg_attr(not(feature = "adbc-exec"), allow(dead_code))]
    seed: Option<String>,
    #[serde(default)]
    expected_validation: Option<String>,
    #[serde(default)]
    error_contains: Vec<String>,
    #[serde(default)]
    queries: Vec<FixtureQueryCase>,
    #[serde(default)]
    rewrite_queries: Vec<FixtureRewriteCase>,
}

#[derive(Debug, Deserialize)]
struct FixtureQueryCase {
    name: String,
    file: String,
    #[serde(default)]
    #[cfg_attr(not(feature = "adbc-exec"), allow(dead_code))]
    expected_result: Option<String>,
    #[serde(default)]
    #[cfg_attr(not(feature = "adbc-exec"), allow(dead_code))]
    rust_expected_result: Option<String>,
    #[serde(default)]
    rust_only_reason: Option<String>,
    #[serde(default)]
    #[cfg_attr(not(feature = "adbc-exec"), allow(dead_code))]
    result_columns: Vec<String>,
    #[serde(default)]
    sql_contains: Vec<String>,
    #[serde(default)]
    rust_sql_contains: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct FixtureRewriteCase {
    name: String,
    sql: String,
    #[serde(default)]
    sql_contains: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct FixtureQuery {
    #[serde(default)]
    metrics: Vec<String>,
    #[serde(default)]
    dimensions: Vec<String>,
    #[serde(default)]
    filters: Vec<String>,
    #[serde(default)]
    segments: Vec<String>,
    #[serde(default)]
    order_by: Vec<String>,
    #[serde(default)]
    ungrouped: bool,
    #[serde(default)]
    use_preaggregations: bool,
    #[serde(default)]
    skip_default_time_dimensions: bool,
    #[serde(default)]
    parameter_values: HashMap<String, serde_yaml::Value>,
    #[serde(default)]
    table_calculations: Vec<TableCalculation>,
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ExpectedValidation {
    valid: bool,
}

fn fixture_suite_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("tests")
        .join("native-fixtures")
}

fn fixture_root(name: &str) -> PathBuf {
    fixture_suite_root().join(name)
}

fn load_manifest() -> FixtureManifest {
    let manifest_text = fs::read_to_string(fixture_suite_root().join("manifest.yml")).unwrap();
    serde_yaml::from_str(&manifest_text).unwrap()
}

fn assert_expected_validation_contract(root: &Path, fixture: &FixtureCase, should_be_valid: bool) {
    let Some(expected_validation) = fixture.expected_validation.as_ref() else {
        return;
    };
    let validation_text = fs::read_to_string(root.join(expected_validation)).unwrap();
    let expected: ExpectedValidation = serde_json::from_str(&validation_text).unwrap();
    assert_eq!(
        expected.valid, should_be_valid,
        "fixture '{}' expected validation file should match manifest validity",
        fixture.name
    );
}

#[test]
fn native_fixtures_load_and_compile() {
    let manifest = load_manifest();

    for fixture in manifest.fixtures {
        let root = fixture_root(&fixture.name);
        let should_be_valid = fixture.valid.unwrap_or(true);
        assert_expected_validation_contract(&root, &fixture, should_be_valid);
        let load_result = load_from_directory(root.join("models"));
        let graph = match load_result {
            Ok(graph) if should_be_valid => graph,
            Ok(_) => panic!(
                "fixture '{}' should be invalid but loaded successfully",
                fixture.name
            ),
            Err(err) if !should_be_valid => {
                let error_text = err.to_string();
                for token in fixture.error_contains {
                    assert!(
                        error_text.contains(&token),
                        "fixture '{}' error should contain '{}'\n{}",
                        fixture.name,
                        token,
                        error_text
                    );
                }
                continue;
            }
            Err(err) => panic!("fixture '{}' should load: {err}", fixture.name),
        };

        assert!(
            graph.models().count() > 0,
            "fixture '{}' should load at least one model",
            fixture.name
        );

        for query_case in fixture.queries {
            let query_text = fs::read_to_string(root.join(&query_case.file)).unwrap();
            let fixture_query: FixtureQuery = serde_yaml::from_str(&query_text).unwrap();
            if (!fixture_query.table_calculations.is_empty()
                || query_case.rust_expected_result.is_some())
                && query_case.expected_result.is_none()
            {
                assert!(
                    query_case
                        .rust_only_reason
                        .as_ref()
                        .is_some_and(|reason| !reason.trim().is_empty()),
                    "fixture '{}::{}' must document Rust-only behavior with rust_only_reason",
                    fixture.name,
                    query_case.name
                );
            }
            let filters = interpolate_query_filters(
                &graph,
                fixture_query.filters,
                &fixture_query.parameter_values,
            )
            .unwrap_or_else(|err| {
                panic!(
                    "fixture '{}::{}' query parameters should interpolate: {err}",
                    fixture.name, query_case.name
                )
            });
            let mut query = SemanticQuery::new()
                .with_metrics(fixture_query.metrics)
                .with_dimensions(fixture_query.dimensions)
                .with_filters(filters)
                .with_segments(fixture_query.segments)
                .with_table_calculations(fixture_query.table_calculations)
                .with_order_by(fixture_query.order_by)
                .with_ungrouped(fixture_query.ungrouped)
                .with_use_preaggregations(fixture_query.use_preaggregations)
                .with_skip_default_time_dimensions(fixture_query.skip_default_time_dimensions);
            if let Some(limit) = fixture_query.limit {
                query = query.with_limit(limit);
            }
            if let Some(offset) = fixture_query.offset {
                query = query.with_offset(offset);
            }

            let sql = SqlGenerator::new(&graph)
                .generate(&query)
                .unwrap_or_else(|err| {
                    panic!(
                        "fixture '{}::{}' should compile: {err}",
                        fixture.name, query_case.name
                    )
                });

            for token in query_case.sql_contains {
                assert!(
                    sql.to_lowercase().contains(&token.to_lowercase()),
                    "fixture '{}::{}' SQL should contain '{}'\n{}",
                    fixture.name,
                    query_case.name,
                    token,
                    sql
                );
            }
            for token in query_case.rust_sql_contains {
                assert!(
                    sql.to_lowercase().contains(&token.to_lowercase()),
                    "fixture '{}::{}' Rust SQL should contain '{}'\n{}",
                    fixture.name,
                    query_case.name,
                    token,
                    sql
                );
            }
        }

        let rewriter = QueryRewriter::new(&graph);
        for rewrite_case in fixture.rewrite_queries {
            let sql = rewriter.rewrite(&rewrite_case.sql).unwrap_or_else(|err| {
                panic!(
                    "fixture '{}::{}' rewrite query should compile: {err}",
                    fixture.name, rewrite_case.name
                )
            });

            for token in rewrite_case.sql_contains {
                assert!(
                    sql.to_lowercase().contains(&token.to_lowercase()),
                    "fixture '{}::{}' rewritten SQL should contain '{}'\n{}",
                    fixture.name,
                    rewrite_case.name,
                    token,
                    sql
                );
            }
        }
    }
}

#[cfg(feature = "adbc-exec")]
const DUCKDB_ENTRYPOINT: &str = "duckdb_adbc_init";

#[cfg(feature = "adbc-exec")]
fn duckdb_driver_path() -> Option<String> {
    std::env::var("SIDEMANTIC_TEST_ADBC_DUCKDB_DRIVER")
        .ok()
        .filter(|value| !value.trim().is_empty())
}

#[cfg(feature = "adbc-exec")]
fn duckdb_database_options(db_path: &Path) -> Vec<(OptionDatabase, OptionValue)> {
    vec![(
        OptionDatabase::from("path"),
        OptionValue::String(db_path.to_string_lossy().to_string()),
    )]
}

#[cfg(feature = "adbc-exec")]
fn execute_duckdb_sql(
    driver: &str,
    db_path: &Path,
    sql: &str,
) -> sidemantic::Result<Vec<Vec<AdbcValue>>> {
    let result = execute_with_adbc(AdbcExecutionRequest {
        driver: driver.to_string(),
        sql: sql.to_string(),
        uri: None,
        entrypoint: Some(DUCKDB_ENTRYPOINT.to_string()),
        database_options: duckdb_database_options(db_path),
        connection_options: Vec::new(),
    })?;
    Ok(result.rows)
}

#[cfg(feature = "adbc-exec")]
fn split_seed_statements(seed_sql: &str) -> impl Iterator<Item = String> + '_ {
    seed_sql
        .split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
        .map(ToString::to_string)
}

#[cfg(feature = "adbc-exec")]
fn seed_fixture(driver: &str, db_path: &Path, root: &Path, seed: &str) {
    let seed_sql = fs::read_to_string(root.join(seed)).unwrap_or_else(|err| {
        panic!(
            "fixture seed '{}' should be readable: {err}",
            root.join(seed).display()
        )
    });
    for statement in split_seed_statements(&seed_sql) {
        execute_duckdb_sql(driver, db_path, &statement).unwrap_or_else(|err| {
            panic!("fixture seed statement should execute:\n{statement}\n{err}")
        });
    }
}

#[cfg(feature = "adbc-exec")]
fn fixture_duckdb_path(fixture_name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "sidemantic_native_fixture_{}_{}.duckdb",
        std::process::id(),
        fixture_name
    ))
}

#[cfg(feature = "adbc-exec")]
fn normalize_json_number(value: &JsonValue) -> Option<f64> {
    if let Some(value) = value.as_f64() {
        return Some(value);
    }
    if let Some(value) = value.as_i64() {
        return Some(value as f64);
    }
    value.as_u64().map(|value| value as f64)
}

#[cfg(feature = "adbc-exec")]
fn expected_date_integer_values(value: &str) -> Option<Vec<i64>> {
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d").ok()?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    let days = (date.num_days_from_ce() - epoch.num_days_from_ce()) as i64;
    let seconds = date
        .and_hms_opt(0, 0, 0)?
        .signed_duration_since(epoch.and_hms_opt(0, 0, 0)?)
        .num_seconds();

    Some(vec![
        days,
        seconds,
        seconds * 1_000,
        seconds * 1_000_000,
        seconds * 1_000_000_000,
    ])
}

#[cfg(feature = "adbc-exec")]
fn assert_adbc_value_matches_json(actual: &AdbcValue, expected: &JsonValue, context: &str) {
    match (actual, expected) {
        (AdbcValue::Null, JsonValue::Null) => {}
        (AdbcValue::Bool(actual), JsonValue::Bool(expected)) => {
            assert_eq!(actual, expected, "{context}")
        }
        (AdbcValue::String(actual), JsonValue::String(expected)) => {
            assert_eq!(actual, expected, "{context}")
        }
        (AdbcValue::String(actual), _) => {
            let Some(expected_number) = normalize_json_number(expected) else {
                panic!(
                    "{context}: expected value {expected:?} is not comparable to string '{actual}'"
                );
            };
            let actual_number = actual.parse::<f64>().unwrap_or_else(|err| {
                panic!("{context}: actual string '{actual}' is not numeric: {err}")
            });
            assert!(
                (actual_number - expected_number).abs() < 1e-9,
                "{context}: expected {expected_number}, got {actual_number}"
            );
        }
        (AdbcValue::I64(actual), JsonValue::String(expected)) => {
            let Some(expected_values) = expected_date_integer_values(expected) else {
                panic!(
                    "{context}: expected string '{expected}' is not comparable to integer {actual}"
                );
            };
            assert!(
                expected_values.contains(actual),
                "{context}: expected date '{expected}' as days/seconds/milliseconds/microseconds/nanoseconds since epoch, got {actual}"
            );
        }
        (AdbcValue::I64(actual), _) => {
            let Some(expected_number) = normalize_json_number(expected) else {
                panic!("{context}: expected value {expected:?} is not numeric");
            };
            assert!(
                (*actual as f64 - expected_number).abs() < f64::EPSILON,
                "{context}: expected {expected_number}, got {actual}"
            );
        }
        (AdbcValue::U64(actual), _) => {
            let Some(expected_number) = normalize_json_number(expected) else {
                panic!("{context}: expected value {expected:?} is not numeric");
            };
            assert!(
                (*actual as f64 - expected_number).abs() < f64::EPSILON,
                "{context}: expected {expected_number}, got {actual}"
            );
        }
        (AdbcValue::F64(actual), _) => {
            let Some(expected_number) = normalize_json_number(expected) else {
                panic!("{context}: expected value {expected:?} is not numeric");
            };
            assert!(
                (*actual - expected_number).abs() < 1e-9,
                "{context}: expected {expected_number}, got {actual}"
            );
        }
        (AdbcValue::Bytes(actual), JsonValue::Array(expected)) => {
            let expected_bytes = expected
                .iter()
                .map(|value| {
                    value
                        .as_u64()
                        .and_then(|value| u8::try_from(value).ok())
                        .unwrap_or_else(|| {
                            panic!("{context}: expected byte array value must be 0-255")
                        })
                })
                .collect::<Vec<_>>();
            assert_eq!(actual, &expected_bytes, "{context}");
        }
        _ => panic!("{context}: expected {expected:?}, got {actual:?}"),
    }
}

#[cfg(feature = "adbc-exec")]
fn assert_rows_match_expected(
    fixture_name: &str,
    query_name: &str,
    columns: &[String],
    rows: &[Vec<AdbcValue>],
    expected: &[JsonValue],
) {
    assert_eq!(
        rows.len(),
        expected.len(),
        "fixture '{fixture_name}::{query_name}' row count should match"
    );

    for (row_index, (actual_row, expected_row)) in rows.iter().zip(expected.iter()).enumerate() {
        let Some(expected_object) = expected_row.as_object() else {
            panic!(
                "fixture '{fixture_name}::{query_name}' expected row {row_index} must be an object"
            );
        };
        assert_eq!(
            actual_row.len(),
            columns.len(),
            "fixture '{fixture_name}::{query_name}' row {row_index} column count should match manifest"
        );
        for (column, actual_value) in columns.iter().zip(actual_row.iter()) {
            let Some(expected_value) = expected_object.get(column) else {
                panic!(
                    "fixture '{fixture_name}::{query_name}' expected row {row_index} is missing column '{column}'"
                );
            };
            assert_adbc_value_matches_json(
                actual_value,
                expected_value,
                &format!(
                    "fixture '{fixture_name}::{query_name}' row {row_index} column '{column}'"
                ),
            );
        }
    }
}

#[cfg(feature = "adbc-exec")]
#[test]
fn native_fixtures_execute_expected_results_with_duckdb_adbc() {
    let Some(driver) = duckdb_driver_path() else {
        eprintln!("skipping native fixture DuckDB ADBC parity; SIDEMANTIC_TEST_ADBC_DUCKDB_DRIVER is not set");
        return;
    };
    let manifest = load_manifest();

    for fixture in manifest.fixtures {
        if !fixture.valid.unwrap_or(true) {
            continue;
        }
        let root = fixture_root(&fixture.name);
        let executable_queries = fixture
            .queries
            .iter()
            .filter(|query_case| {
                query_case.expected_result.is_some() || query_case.rust_expected_result.is_some()
            })
            .collect::<Vec<_>>();
        if executable_queries.is_empty() {
            continue;
        }
        let seed = fixture.seed.as_deref().unwrap_or_else(|| {
            panic!(
                "fixture '{}' has expected results but no seed",
                fixture.name
            )
        });
        let db_path = fixture_duckdb_path(&fixture.name);
        if db_path.exists() {
            fs::remove_file(&db_path).unwrap_or_else(|err| {
                panic!(
                    "fixture database '{}' should be removable: {err}",
                    db_path.display()
                )
            });
        }
        seed_fixture(&driver, &db_path, &root, seed);

        let graph = load_from_directory(root.join("models"))
            .unwrap_or_else(|err| panic!("fixture '{}' should load: {err}", fixture.name));
        let generator = SqlGenerator::new(&graph);

        for query_case in executable_queries {
            let query_text = fs::read_to_string(root.join(&query_case.file)).unwrap();
            let fixture_query: FixtureQuery = serde_yaml::from_str(&query_text).unwrap();
            if query_case.rust_expected_result.is_some() && query_case.expected_result.is_none() {
                assert!(
                    query_case
                        .rust_only_reason
                        .as_ref()
                        .is_some_and(|reason| !reason.trim().is_empty()),
                    "fixture '{}::{}' must document Rust-only expected results with rust_only_reason",
                    fixture.name,
                    query_case.name
                );
            }
            let filters = interpolate_query_filters(
                &graph,
                fixture_query.filters,
                &fixture_query.parameter_values,
            )
            .unwrap_or_else(|err| {
                panic!(
                    "fixture '{}::{}' query parameters should interpolate: {err}",
                    fixture.name, query_case.name
                )
            });
            let mut query = SemanticQuery::new()
                .with_metrics(fixture_query.metrics)
                .with_dimensions(fixture_query.dimensions)
                .with_filters(filters)
                .with_segments(fixture_query.segments)
                .with_table_calculations(fixture_query.table_calculations)
                .with_order_by(fixture_query.order_by)
                .with_ungrouped(fixture_query.ungrouped)
                .with_use_preaggregations(fixture_query.use_preaggregations)
                .with_skip_default_time_dimensions(fixture_query.skip_default_time_dimensions);
            if let Some(limit) = fixture_query.limit {
                query = query.with_limit(limit);
            }
            if let Some(offset) = fixture_query.offset {
                query = query.with_offset(offset);
            }

            let sql = generator.generate(&query).unwrap_or_else(|err| {
                panic!(
                    "fixture '{}::{}' should compile: {err}",
                    fixture.name, query_case.name
                )
            });
            let rows = execute_duckdb_sql(&driver, &db_path, &sql).unwrap_or_else(|err| {
                panic!(
                    "fixture '{}::{}' SQL should execute:\n{sql}\n{err}",
                    fixture.name, query_case.name
                )
            });
            let expected_path = query_case
                .expected_result
                .as_ref()
                .or(query_case.rust_expected_result.as_ref())
                .expect("executable query should have expected result");
            let expected = serde_json::from_str::<Vec<JsonValue>>(
                &fs::read_to_string(root.join(expected_path)).unwrap(),
            )
            .unwrap();
            assert_rows_match_expected(
                &fixture.name,
                &query_case.name,
                &query_case.result_columns,
                &rows,
                &expected,
            );
        }
        if db_path.exists() {
            fs::remove_file(&db_path).unwrap_or_else(|err| {
                panic!(
                    "fixture database '{}' should be removable: {err}",
                    db_path.display()
                )
            });
        }
    }
}
