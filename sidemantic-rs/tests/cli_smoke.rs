use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be valid")
        .as_nanos();
    dir.push(format!("{prefix}_{suffix}"));
    fs::create_dir_all(&dir).expect("temp dir should be created");
    dir
}

fn is_expected_workbench_unavailable_error(stderr: &str) -> bool {
    stderr.contains("workbench requires the crate feature 'workbench-tui'")
        || stderr.contains("workbench requires an interactive terminal (TTY)")
}

fn write_retail_fixture(dir: &std::path::Path) -> PathBuf {
    let models_path = dir.join("retail.yml");
    fs::write(
        &models_path,
        r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
      - name: customer_id
        type: numeric
    metrics:
      - name: revenue
        agg: sum
        sql: amount
      - name: order_count
        agg: count
    relationships:
      - name: customers
        type: many_to_one
        foreign_key: customer_id
  - name: customers
    table: customers
    primary_key: id
    dimensions:
      - name: country
        type: categorical
metrics:
  - name: revenue_per_order
    type: ratio
    numerator: revenue
    denominator: order_count
"#,
    )
    .expect("retail fixture should be written");
    models_path
}

#[test]
fn cli_help_lists_core_commands() {
    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("--help")
        .output()
        .expect("sidemantic binary should run");

    assert!(
        output.status.success(),
        "help command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("compile"));
    assert!(stdout.contains("rewrite"));
    assert!(stdout.contains("validate"));
    assert!(stdout.contains("run"));
    assert!(stdout.contains("preagg"));
    assert!(stdout.contains("workbench"));
    assert!(stdout.contains("serve"));
    assert!(stdout.contains("mcp-serve"));
    assert!(stdout.contains("lsp"));
}

#[test]
fn cli_workbench_reports_expected_unavailable_status() {
    let dir = unique_temp_dir("sidemantic_cli_workbench_unavailable");
    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("workbench")
        .arg(&dir)
        .output()
        .expect("workbench command should run");

    assert!(
        !output.status.success(),
        "workbench command should fail in non-interactive smoke test mode"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        is_expected_workbench_unavailable_error(&stderr),
        "unexpected stderr: {stderr}"
    );
    assert!(
        stderr.contains(&format!("models={}", dir.display())),
        "unexpected stderr: {stderr}"
    );

    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}

#[test]
fn cli_workbench_autodiscovers_data_db_connection() {
    let dir = unique_temp_dir("sidemantic_cli_workbench_auto_db");
    let data_dir = dir.join("data");
    fs::create_dir_all(&data_dir).expect("data dir should be created");
    let db_path = data_dir.join("warehouse.db");
    fs::write(&db_path, []).expect("placeholder db file should be created");

    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("workbench")
        .arg(&dir)
        .output()
        .expect("workbench command should run");

    assert!(
        !output.status.success(),
        "workbench command should fail in non-interactive smoke test mode"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        is_expected_workbench_unavailable_error(&stderr),
        "unexpected stderr: {stderr}"
    );
    let expected_connection = format!("connection=duckdb://{}", db_path.to_string_lossy());
    assert!(
        stderr.contains(&expected_connection),
        "unexpected stderr: {stderr}"
    );

    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}

#[test]
fn cli_workbench_demo_resolves_connection() {
    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("workbench")
        .arg("--demo")
        .output()
        .expect("workbench demo command should run");

    assert!(
        !output.status.success(),
        "workbench command should fail in non-interactive smoke test mode"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        is_expected_workbench_unavailable_error(&stderr),
        "unexpected stderr: {stderr}"
    );
    assert!(stderr.contains("demo=true"), "unexpected stderr: {stderr}");
    assert!(
        stderr.contains("connection=duckdb:///"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn cli_tree_alias_requires_directory_positional() {
    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("tree")
        .output()
        .expect("tree command should run");

    assert!(
        !output.status.success(),
        "tree command should fail without required directory"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("tree requires exactly one positional models directory"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn cli_tree_alias_forwards_to_workbench_unavailable_status() {
    let dir = unique_temp_dir("sidemantic_cli_tree_alias_unavailable");
    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("tree")
        .arg(&dir)
        .output()
        .expect("tree command should run");

    assert!(
        !output.status.success(),
        "tree command should fail in non-interactive smoke test mode"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("tree is deprecated; use 'workbench'"),
        "unexpected stderr: {stderr}"
    );
    assert!(
        is_expected_workbench_unavailable_error(&stderr),
        "unexpected stderr: {stderr}"
    );

    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}

#[test]
fn cli_serve_alias_is_recognized() {
    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("serve")
        .output()
        .expect("serve alias should run");

    assert!(
        !output.status.success(),
        "serve should fail without runtime-server feature in default test build"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("server requires the crate feature 'runtime-server'"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn cli_mcp_serve_alias_is_recognized() {
    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("mcp-serve")
        .output()
        .expect("mcp-serve alias should run");

    assert!(
        !output.status.success(),
        "mcp-serve should fail without mcp-server feature in default test build"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("mcp requires the crate feature 'mcp-server'"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn cli_compile_accepts_yaml_model_file() {
    let dir = unique_temp_dir("sidemantic_cli_smoke");
    let models_path = dir.join("models.yml");
    fs::write(
        &models_path,
        r#"
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
"#,
    )
    .expect("models file should be written");

    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("compile")
        .arg("--models")
        .arg(&models_path)
        .arg("--metric")
        .arg("orders.revenue")
        .arg("--dimension")
        .arg("orders.status")
        .output()
        .expect("compile command should run");

    assert!(
        output.status.success(),
        "compile command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("SELECT"), "unexpected SQL output: {stdout}");
    assert!(stdout.contains("SUM("), "unexpected SQL output: {stdout}");

    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}

#[test]
fn cli_info_lists_model_summary() {
    let dir = unique_temp_dir("sidemantic_cli_info");
    let models_path = dir.join("models.yml");
    fs::write(
        &models_path,
        r#"
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
"#,
    )
    .expect("models file should be written");

    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("info")
        .arg("--models")
        .arg(&models_path)
        .output()
        .expect("info command should run");

    assert!(
        output.status.success(),
        "info command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Semantic Layer:"));
    assert!(stdout.contains("- orders"));
    assert!(stdout.contains("Dimensions: 1"));
    assert!(stdout.contains("Metrics: 1"));

    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}

#[test]
fn cli_query_dry_run_rewrites_sql() {
    let dir = unique_temp_dir("sidemantic_cli_query_dry_run");
    let models_path = dir.join("models.yml");
    fs::write(
        &models_path,
        r#"
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
"#,
    )
    .expect("models file should be written");

    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("query")
        .arg("--models")
        .arg(&models_path)
        .arg("--sql")
        .arg("SELECT orders.revenue, orders.status FROM orders")
        .arg("--dry-run")
        .output()
        .expect("query command should run");

    assert!(
        output.status.success(),
        "query command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("SELECT"), "unexpected SQL output: {stdout}");
    assert!(stdout.contains("SUM("), "unexpected SQL output: {stdout}");
    assert!(
        stdout.contains("GROUP BY"),
        "unexpected SQL output: {stdout}"
    );

    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}

#[test]
fn cli_rewrite_covers_representative_relationship_query() {
    let dir = unique_temp_dir("sidemantic_cli_rewrite_retail");
    let models_path = write_retail_fixture(&dir);

    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("rewrite")
        .arg("--models")
        .arg(&models_path)
        .arg("--sql")
        .arg(
            "SELECT orders.revenue, customers.country FROM orders \
             WHERE customers.country = 'US' \
             ORDER BY orders.revenue DESC LIMIT 5",
        )
        .output()
        .expect("rewrite command should run");

    assert!(
        output.status.success(),
        "rewrite command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("SUM("), "unexpected SQL output: {stdout}");
    assert!(stdout.contains("JOIN"), "unexpected SQL output: {stdout}");
    assert!(
        stdout.contains("country"),
        "unexpected SQL output: {stdout}"
    );
    assert!(
        stdout.contains("ORDER BY"),
        "unexpected SQL output: {stdout}"
    );
    assert!(
        stdout.contains("LIMIT 5"),
        "unexpected SQL output: {stdout}"
    );

    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}

#[test]
fn cli_compile_covers_top_level_ratio_metric_fixture() {
    let dir = unique_temp_dir("sidemantic_cli_compile_ratio");
    let models_path = write_retail_fixture(&dir);

    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("compile")
        .arg("--models")
        .arg(&models_path)
        .arg("--metric")
        .arg("revenue_per_order")
        .arg("--dimension")
        .arg("customers.country")
        .arg("--order-by")
        .arg("revenue_per_order DESC")
        .arg("--limit")
        .arg("10")
        .output()
        .expect("compile command should run");

    assert!(
        output.status.success(),
        "compile command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("SUM("), "unexpected SQL output: {stdout}");
    assert!(stdout.contains("COUNT("), "unexpected SQL output: {stdout}");
    assert!(stdout.contains("JOIN"), "unexpected SQL output: {stdout}");
    assert!(
        stdout.contains("ORDER BY"),
        "unexpected SQL output: {stdout}"
    );
    assert!(
        stdout.contains("LIMIT 10"),
        "unexpected SQL output: {stdout}"
    );

    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}

#[test]
fn cli_preagg_apply_writes_recommendations_to_model_files() {
    let dir = unique_temp_dir("sidemantic_cli_preagg_apply");
    let models_dir = dir.join("models");
    fs::create_dir_all(&models_dir).expect("models dir should be created");
    let models_path = models_dir.join("orders.yml");
    fs::write(
        &models_path,
        r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: created_at
        type: time
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
"#,
    )
    .expect("models file should be written");
    let queries_path = dir.join("queries.sql");
    fs::write(
        &queries_path,
        "select * from orders -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.created_at,orders.status granularities=day\n\
select * from orders -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.created_at,orders.status granularities=day\n",
    )
    .expect("queries file should be written");

    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("preagg")
        .arg("apply")
        .arg("--models")
        .arg(&models_dir)
        .arg("--queries-file")
        .arg(&queries_path)
        .arg("--min-query-count")
        .arg("1")
        .arg("--min-benefit-score")
        .arg("0")
        .output()
        .expect("preagg apply command should run");

    assert!(
        output.status.success(),
        "preagg apply command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let updated = fs::read_to_string(&models_path).expect("updated model file should be readable");
    assert!(
        updated.contains("pre_aggregations"),
        "missing pre_aggregations in updated model file: {updated}"
    );
    assert!(
        updated.contains("day_created_at_status_revenue"),
        "missing expected generated pre-aggregation name: {updated}"
    );

    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}

#[test]
fn cli_preagg_apply_dry_run_does_not_modify_model_files() {
    let dir = unique_temp_dir("sidemantic_cli_preagg_apply_dry_run");
    let models_dir = dir.join("models");
    fs::create_dir_all(&models_dir).expect("models dir should be created");
    let models_path = models_dir.join("orders.yml");
    let original = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: created_at
        type: time
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
"#;
    fs::write(&models_path, original).expect("models file should be written");
    let queries_path = dir.join("queries.sql");
    fs::write(
        &queries_path,
        "select * from orders -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.created_at,orders.status granularities=day\n\
select * from orders -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.created_at,orders.status granularities=day\n",
    )
    .expect("queries file should be written");

    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("preagg")
        .arg("apply")
        .arg("--models")
        .arg(&models_dir)
        .arg("--queries-file")
        .arg(&queries_path)
        .arg("--min-query-count")
        .arg("1")
        .arg("--min-benefit-score")
        .arg("0")
        .arg("--dry-run")
        .output()
        .expect("preagg apply dry-run command should run");

    assert!(
        output.status.success(),
        "preagg apply dry-run command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let updated = fs::read_to_string(&models_path).expect("model file should be readable");
    assert_eq!(updated, original);

    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}

#[test]
fn cli_preagg_recommend_connection_mode_requires_adbc_feature() {
    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("preagg")
        .arg("recommend")
        .arg("--connection")
        .arg("snowflake://account/db/schema")
        .output()
        .expect("preagg recommend command should run");

    assert!(
        !output.status.success(),
        "preagg recommend connection mode should fail in smoke environment"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    if cfg!(feature = "adbc-exec") {
        assert!(
            stderr.contains("preagg recommend: failed to fetch query history via ADBC"),
            "unexpected stderr: {stderr}"
        );
    } else {
        assert!(
            stderr.contains(
                "preagg recommend: query-history mode requires the crate feature 'adbc-exec'"
            ),
            "unexpected stderr: {stderr}"
        );
    }
}

#[test]
fn cli_preagg_apply_connection_mode_requires_adbc_feature() {
    let dir = unique_temp_dir("sidemantic_cli_preagg_apply_connection_mode");
    let models_dir = dir.join("models");
    fs::create_dir_all(&models_dir).expect("models dir should be created");
    let models_path = models_dir.join("orders.yml");
    fs::write(
        &models_path,
        r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    metrics:
      - name: revenue
        agg: sum
        sql: amount
"#,
    )
    .expect("models file should be written");

    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("preagg")
        .arg("apply")
        .arg("--models")
        .arg(&models_dir)
        .arg("--connection")
        .arg("snowflake://account/db/schema")
        .output()
        .expect("preagg apply command should run");

    assert!(
        !output.status.success(),
        "preagg apply connection mode should fail in smoke environment"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    if cfg!(feature = "adbc-exec") {
        assert!(
            stderr.contains("preagg apply: failed to fetch query history via ADBC"),
            "unexpected stderr: {stderr}"
        );
    } else {
        assert!(
            stderr.contains(
                "preagg apply: query-history mode requires the crate feature 'adbc-exec'"
            ),
            "unexpected stderr: {stderr}"
        );
    }

    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}

#[test]
fn cli_migrator_generate_models_bootstrap_writes_outputs() {
    let dir = unique_temp_dir("sidemantic_cli_migrator_bootstrap");
    let queries_path = dir.join("queries.sql");
    fs::write(
        &queries_path,
        "select orders.status, sum(orders.amount) as revenue from orders group by orders.status\n",
    )
    .expect("queries file should be written");
    let output_dir = dir.join("generated");

    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("migrator")
        .arg("--queries")
        .arg(&queries_path)
        .arg("--generate-models")
        .arg(&output_dir)
        .output()
        .expect("migrator bootstrap command should run");

    assert!(
        output.status.success(),
        "migrator bootstrap command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let model_file = output_dir.join("models").join("orders.yml");
    assert!(model_file.exists(), "expected generated model file");
    let model_content = fs::read_to_string(&model_file).expect("model file should be readable");
    assert!(
        model_content.contains("model:"),
        "unexpected model YAML: {model_content}"
    );
    assert!(
        model_content.contains("name: orders"),
        "unexpected model YAML: {model_content}"
    );
    assert!(
        model_content.contains("sum_amount"),
        "missing expected inferred metric: {model_content}"
    );

    let rewritten_file = output_dir.join("rewritten_queries").join("query_1.sql");
    assert!(rewritten_file.exists(), "expected rewritten query file");

    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}

#[test]
fn cli_migrator_coverage_mode_reports_totals() {
    let dir = unique_temp_dir("sidemantic_cli_migrator_coverage");
    let models_path = dir.join("models.yml");
    fs::write(
        &models_path,
        r#"
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
"#,
    )
    .expect("models file should be written");
    let queries_path = dir.join("queries.sql");
    fs::write(
        &queries_path,
        "select orders.status, sum(orders.amount) as revenue from orders group by orders.status\n",
    )
    .expect("queries file should be written");

    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("migrator")
        .arg("--models")
        .arg(&models_path)
        .arg("--queries")
        .arg(&queries_path)
        .output()
        .expect("migrator coverage command should run");

    assert!(
        output.status.success(),
        "migrator coverage command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Total Queries: 1"),
        "unexpected output: {stdout}"
    );
    assert!(
        stdout.contains("Rewritable: 1"),
        "unexpected output: {stdout}"
    );
    assert!(
        stdout.contains("Coverage: 100.0%"),
        "unexpected output: {stdout}"
    );

    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}

#[test]
fn cli_validate_definition_mode_with_verbose_uses_positional_models_path() {
    let dir = unique_temp_dir("sidemantic_cli_validate_verbose");
    let models_path = dir.join("models.yml");
    fs::write(
        &models_path,
        r#"
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
"#,
    )
    .expect("models file should be written");

    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("validate")
        .arg(&models_path)
        .arg("--verbose")
        .output()
        .expect("validate command should run");

    assert!(
        output.status.success(),
        "validate command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Validation passed"),
        "unexpected output: {stdout}"
    );
    assert!(stdout.contains("Models: 1"), "unexpected output: {stdout}");
    assert!(stdout.contains("- orders"), "unexpected output: {stdout}");

    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}

#[test]
fn cli_validate_query_reference_mode_remains_supported() {
    let dir = unique_temp_dir("sidemantic_cli_validate_refs");
    let models_path = dir.join("models.yml");
    fs::write(
        &models_path,
        r#"
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
"#,
    )
    .expect("models file should be written");

    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("validate")
        .arg("--models")
        .arg(&models_path)
        .arg("--metric")
        .arg("orders.revenue")
        .arg("--dimension")
        .arg("orders.status")
        .output()
        .expect("validate command should run");

    assert!(
        output.status.success(),
        "validate reference mode failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("ok"), "unexpected output: {stdout}");

    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}

#[test]
fn cli_preagg_refresh_generates_sql_plan() {
    let dir = unique_temp_dir("sidemantic_cli_preagg_refresh");
    let models_path = dir.join("models.yml");
    fs::write(
        &models_path,
        r#"
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
"#,
    )
    .expect("models file should be written");

    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("preagg")
        .arg("refresh")
        .arg("--models")
        .arg(&models_path)
        .arg("--model")
        .arg("orders")
        .arg("--name")
        .arg("daily_revenue")
        .arg("--mode")
        .arg("incremental")
        .arg("--from-watermark")
        .arg("2026-01-01")
        .output()
        .expect("preagg refresh command should run");

    assert!(
        output.status.success(),
        "preagg refresh command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("INSERT INTO orders_preagg_daily_revenue"),
        "unexpected refresh SQL output: {stdout}"
    );
    assert!(
        stdout.contains("order_date_day >= '2026-01-01'"),
        "unexpected refresh SQL output: {stdout}"
    );

    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}

#[test]
fn cli_preagg_refresh_engine_mode_generates_bigquery_ddl() {
    let dir = unique_temp_dir("sidemantic_cli_preagg_engine");
    let models_path = dir.join("models.yml");
    fs::write(
        &models_path,
        r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: order_date
        type: time
    metrics:
      - name: revenue
        agg: sum
        sql: amount
    pre_aggregations:
      - name: daily_revenue
        time_dimension: order_date
        granularity: day
        measures: [revenue]
"#,
    )
    .expect("models file should be written");

    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("preagg")
        .arg("refresh")
        .arg("--models")
        .arg(&models_path)
        .arg("--model")
        .arg("orders")
        .arg("--name")
        .arg("daily_revenue")
        .arg("--mode")
        .arg("engine")
        .arg("--dialect")
        .arg("bigquery")
        .arg("--refresh-every")
        .arg("2 hours")
        .output()
        .expect("preagg refresh engine command should run");

    assert!(
        output.status.success(),
        "preagg refresh engine command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("CREATE MATERIALIZED VIEW IF NOT EXISTS orders_preagg_daily_revenue"),
        "unexpected engine refresh SQL output: {stdout}"
    );
    assert!(
        stdout.contains("refresh_interval_minutes = 120"),
        "unexpected engine refresh SQL output: {stdout}"
    );

    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}

#[test]
fn cli_preagg_refresh_engine_mode_requires_dialect() {
    let dir = unique_temp_dir("sidemantic_cli_preagg_engine_missing_dialect");
    let models_path = dir.join("models.yml");
    fs::write(
        &models_path,
        r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: order_date
        type: time
    metrics:
      - name: revenue
        agg: sum
        sql: amount
    pre_aggregations:
      - name: daily_revenue
        time_dimension: order_date
        granularity: day
        measures: [revenue]
"#,
    )
    .expect("models file should be written");

    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("preagg")
        .arg("refresh")
        .arg("--models")
        .arg(&models_path)
        .arg("--model")
        .arg("orders")
        .arg("--name")
        .arg("daily_revenue")
        .arg("--mode")
        .arg("engine")
        .output()
        .expect("preagg refresh engine command should run");

    assert!(
        !output.status.success(),
        "preagg refresh engine command should fail without dialect"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("engine refresh mode requires --dialect"),
        "unexpected stderr: {stderr}"
    );

    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}

#[test]
fn cli_preagg_refresh_defaults_to_incremental_when_refresh_key_is_incremental() {
    let dir = unique_temp_dir("sidemantic_cli_preagg_default_incremental");
    let models_path = dir.join("models.yml");
    fs::write(
        &models_path,
        r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: order_date
        type: time
    metrics:
      - name: revenue
        agg: sum
        sql: amount
    pre_aggregations:
      - name: daily_revenue
        time_dimension: order_date
        granularity: day
        measures: [revenue]
        refresh_key:
          incremental: true
"#,
    )
    .expect("models file should be written");

    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("preagg")
        .arg("refresh")
        .arg("--models")
        .arg(&models_path)
        .arg("--model")
        .arg("orders")
        .arg("--name")
        .arg("daily_revenue")
        .output()
        .expect("preagg refresh command should run");

    assert!(
        output.status.success(),
        "preagg refresh command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("mode=incremental"),
        "unexpected refresh mode output: {stdout}"
    );
    assert!(
        stdout.contains("INSERT INTO orders_preagg_daily_revenue"),
        "unexpected refresh SQL output: {stdout}"
    );

    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}

#[test]
fn cli_preagg_refresh_defaults_to_full_without_incremental_refresh_key() {
    let dir = unique_temp_dir("sidemantic_cli_preagg_default_full");
    let models_path = dir.join("models.yml");
    fs::write(
        &models_path,
        r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: order_date
        type: time
    metrics:
      - name: revenue
        agg: sum
        sql: amount
    pre_aggregations:
      - name: daily_revenue
        time_dimension: order_date
        granularity: day
        measures: [revenue]
"#,
    )
    .expect("models file should be written");

    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("preagg")
        .arg("refresh")
        .arg("--models")
        .arg(&models_path)
        .arg("--model")
        .arg("orders")
        .arg("--name")
        .arg("daily_revenue")
        .output()
        .expect("preagg refresh command should run");

    assert!(
        output.status.success(),
        "preagg refresh command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("mode=full"),
        "unexpected refresh mode output: {stdout}"
    );
    assert!(
        stdout.contains("DELETE FROM orders_preagg_daily_revenue"),
        "unexpected refresh SQL output: {stdout}"
    );

    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}
