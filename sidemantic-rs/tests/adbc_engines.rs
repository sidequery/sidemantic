#![cfg(feature = "adbc-exec")]

use std::env;

use sidemantic::{AdbcExecutor, Dimension, Metric, Model, SemanticGraph, SemanticQuery};

#[test]
fn adbc_engine_semantic_query_smoke() {
    if env::var("SIDEMANTIC_ADBC_TESTS").ok().as_deref() != Some("1") {
        return;
    }

    let target = EngineTarget::from_env();
    match run_target(&target) {
        Ok(()) => {}
        Err(err) if !target.required => {
            eprintln!(
                "skipping best-effort Rust ADBC smoke for {}: {err}",
                target.db
            );
        }
        Err(err) => panic!("Rust ADBC smoke failed for {}: {err}", target.db),
    }
}

struct EngineTarget {
    db: String,
    url: String,
    table: String,
    required: bool,
}

impl EngineTarget {
    fn from_env() -> Self {
        let db = env::var("SIDEMANTIC_ADBC_DB")
            .unwrap_or_else(|_| "sqlite".to_string())
            .to_ascii_lowercase();

        let required = required_from_env(&db);
        match db.as_str() {
            "sqlite" => Self {
                db,
                url: "sqlite:///:memory:".to_string(),
                table: "sidemantic_adbc_smoke".to_string(),
                required,
            },
            "duckdb" => Self {
                db,
                url: "duckdb:///:memory:".to_string(),
                table: "sidemantic_adbc_smoke".to_string(),
                required,
            },
            "postgres" | "postgresql" => Self {
                db,
                url: env::var("POSTGRES_URL").unwrap_or_else(|_| {
                    "postgres://test:test@localhost:5432/sidemantic_test".into()
                }),
                table: "sidemantic_adbc_smoke".to_string(),
                required,
            },
            "clickhouse" => Self {
                db,
                url: env::var("CLICKHOUSE_URL")
                    .unwrap_or_else(|_| "adbc://clickhouse?uri=http://localhost:8123/".into()),
                table: "sidemantic_adbc_smoke".to_string(),
                required,
            },
            "bigquery" => {
                let project =
                    env::var("BIGQUERY_PROJECT").unwrap_or_else(|_| "test-project".into());
                let dataset =
                    env::var("BIGQUERY_DATASET").unwrap_or_else(|_| "test_dataset".into());
                env::set_var("GOOGLE_CLOUD_PROJECT", &project);
                Self {
                    db,
                    url: format!("bigquery://{project}/{dataset}"),
                    table: format!("{dataset}.sidemantic_adbc_smoke"),
                    required,
                }
            }
            "snowflake" => Self {
                db,
                url: env::var("SNOWFLAKE_URL").unwrap_or_else(|_| {
                    "snowflake://test:test@test/testdb/public?warehouse=test_warehouse".into()
                }),
                table: "sidemantic_adbc_smoke".to_string(),
                required,
            },
            other => panic!("unsupported SIDEMANTIC_ADBC_DB={other:?}"),
        }
    }

    fn drop_sql(&self) -> String {
        format!("DROP TABLE IF EXISTS {}", self.table)
    }

    fn create_sql(&self) -> String {
        match self.db.as_str() {
            "bigquery" => format!(
                "CREATE TABLE {} (order_id INT64, status STRING, amount INT64)",
                self.table
            ),
            "clickhouse" => format!(
                "CREATE TABLE {} (order_id Int32, status String, amount Int32) ENGINE = Memory",
                self.table
            ),
            "snowflake" => format!(
                "CREATE OR REPLACE TEMPORARY TABLE {} \
                 (order_id INTEGER, status VARCHAR, amount INTEGER)",
                self.table
            ),
            _ => format!(
                "CREATE TABLE {} (order_id INTEGER, status VARCHAR(16), amount INTEGER)",
                self.table
            ),
        }
    }

    fn insert_sql(&self) -> String {
        format!(
            "INSERT INTO {} VALUES (1, 'paid', 10), (2, 'paid', 20), (3, 'open', 5)",
            self.table
        )
    }
}

fn required_from_env(db: &str) -> bool {
    match env::var("SIDEMANTIC_ADBC_REQUIRED") {
        Ok(value) if matches!(value.as_str(), "1" | "true" | "yes" | "required") => true,
        Ok(value) if matches!(value.as_str(), "0" | "false" | "no" | "best-effort") => false,
        _ => matches!(
            db,
            "sqlite" | "duckdb" | "postgres" | "postgresql" | "clickhouse"
        ),
    }
}

fn run_target(target: &EngineTarget) -> Result<(), String> {
    let mut executor = AdbcExecutor::connect_url(&target.url).map_err(|err| {
        format!(
            "failed to connect with URL {:?} through dbc/ADBC: {err}",
            target.url
        )
    })?;

    let drop_sql = target.drop_sql();
    let _ = executor.execute_update(&drop_sql);

    let result = run_semantic_flow(target, &mut executor);
    let _ = executor.execute_update(&drop_sql);
    result
}

fn run_semantic_flow(target: &EngineTarget, executor: &mut AdbcExecutor) -> Result<(), String> {
    execute_update(executor, &target.create_sql())?;
    execute_update(executor, &target.insert_sql())?;

    let mut graph = SemanticGraph::new();
    graph
        .add_model(
            Model::new("orders", "order_id")
                .with_table(&target.table)
                .with_dimension(Dimension::categorical("status"))
                .with_metric(Metric::sum("revenue", "amount")),
        )
        .map_err(|err| format!("failed to build semantic graph: {err}"))?;

    let query = SemanticQuery::new()
        .with_metrics(vec!["orders.revenue".into()])
        .with_dimensions(vec!["orders.status".into()]);

    let result = executor
        .execute_semantic_query(&graph, &query)
        .map_err(|err| format!("failed to execute semantic query: {err}"))?;
    let field_names: Vec<_> = result
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    if field_names != ["status".to_string(), "revenue".to_string()] {
        return Err(format!("unexpected result schema fields: {field_names:?}"));
    }

    let batches = result
        .collect()
        .map_err(|err| format!("failed to collect semantic query results: {err}"))?;
    let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    if row_count != 2 {
        return Err(format!("expected two grouped result rows, got {row_count}"));
    }

    Ok(())
}

fn execute_update(executor: &mut AdbcExecutor, sql: &str) -> Result<(), String> {
    executor
        .execute_update(sql)
        .map(|_| ())
        .map_err(|err| format!("failed SQL {sql:?}: {err}"))
}
