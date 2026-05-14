use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process;
#[cfg(any(
    feature = "mcp-server",
    feature = "runtime-server",
    feature = "runtime-lsp"
))]
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(feature = "adbc-exec")]
use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use regex::Regex;
use serde::Deserialize;
#[cfg(feature = "adbc-exec")]
use serde::Serialize;
#[cfg(feature = "adbc-exec")]
use serde_json::Map as JsonMap;
#[cfg(feature = "adbc-exec")]
use serde_json::Value as JsonValue;
use sidemantic::{
    build_preaggregation_refresh_statements, extract_preaggregation_patterns,
    generate_preaggregation_definition, recommend_preaggregation_patterns,
    summarize_preaggregation_patterns, SemanticQuery, SidemanticError, SidemanticRuntime,
};
#[cfg(feature = "adbc-exec")]
use sidemantic::{execute_with_adbc, AdbcExecutionRequest, AdbcValue};

#[cfg(feature = "workbench-tui")]
mod workbench;

type CliResult<T> = std::result::Result<T, String>;
type ParsedOptions = HashMap<String, Vec<String>>;
#[cfg(feature = "adbc-exec")]
pub(crate) type AdbcConnectionUrlParts =
    (String, Option<String>, Vec<(OptionDatabase, OptionValue)>);

#[cfg(feature = "adbc-exec")]
#[derive(Debug, Default, Serialize)]
struct RunOutput {
    sql: String,
    columns: Vec<String>,
    rows: Vec<JsonValue>,
    row_count: usize,
}

#[derive(Debug, Clone)]
struct RefreshPlan {
    model_name: String,
    preagg_name: String,
    table_name: String,
    mode: String,
    statements: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct CliPreaggPattern {
    model: String,
    metrics: Vec<String>,
    dimensions: Vec<String>,
    granularities: Vec<String>,
    count: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct CliPreaggRecommendation {
    pattern: CliPreaggPattern,
    suggested_name: String,
    query_count: usize,
    estimated_benefit_score: f64,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct CliMigratorAnalysisPayload {
    #[serde(default)]
    column_references: Vec<String>,
    #[serde(default)]
    group_by_columns: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
struct CliMigratorMetric {
    name: String,
    agg: String,
    sql: String,
}

#[derive(Debug, Clone, Default)]
struct CliGeneratedModel {
    dimensions: BTreeSet<String>,
    metrics: Vec<CliMigratorMetric>,
}

#[cfg(feature = "adbc-exec")]
#[derive(Debug, Clone)]
struct AdbcCliConfig {
    driver: String,
    uri: Option<String>,
    entrypoint: Option<String>,
    database_options: Vec<(OptionDatabase, OptionValue)>,
    connection_options: Vec<(OptionConnection, OptionValue)>,
}

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err}");
        process::exit(1);
    }
}

fn run() -> CliResult<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.is_empty() {
        print_help();
        return Ok(());
    }

    let command = args[0].as_str();
    let rest = &args[1..];
    match command {
        "-h" | "--help" | "help" => {
            print_help();
            Ok(())
        }
        "compile" => compile_command(rest),
        "rewrite" => rewrite_command(rest),
        "validate" => validate_command(rest),
        "migrator" => migrator_command(rest),
        "info" => info_command(rest),
        "query" => query_command(rest),
        "run" => run_command(rest),
        "preagg" => preagg_command(rest),
        "workbench" => workbench_command(rest),
        "tree" => tree_command(rest),
        "mcp" => mcp_command(rest),
        "mcp-serve" => mcp_command(rest),
        "server" => server_command(rest),
        "serve" => server_command(rest),
        "lsp" => lsp_command(rest),
        unknown => Err(format!(
            "unknown command '{unknown}'. Use 'sidemantic --help' for usage."
        )),
    }
}

fn print_help() {
    println!(
        "sidemantic (Rust CLI)\n\
         \n\
         Commands:\n\
           compile   Compile semantic query to SQL\n\
           rewrite   Rewrite SQL using semantic graph\n\
           validate  Validate model/query references\n\
           migrator  Analyze SQL coverage and bootstrap model files\n\
           info      Show semantic layer model summary\n\
           query     Rewrite SQL and optionally execute via ADBC\n\
           run       Compile and execute query via ADBC\n\
           preagg    Pre-aggregation helpers (materialize/recommend/refresh)\n\
           workbench Launch interactive workbench (ratatui)\n\
           mcp-serve Launch sidemantic-mcp passthrough\n\
           serve     Launch sidemantic-server passthrough\n\
           lsp       Launch sidemantic-lsp passthrough\n\
         \n\
         Examples:\n\
           sidemantic compile --models ./models --metric orders.revenue --dimension orders.status\n\
           sidemantic rewrite --models ./models --sql \"select orders.revenue from orders\"\n\
           sidemantic migrator --queries ./queries --generate-models ./out\n\
           sidemantic info --models ./models\n\
           sidemantic query --models ./models --sql \"select orders.revenue from orders\" --dry-run\n\
           sidemantic run --models ./models --metric orders.revenue --driver adbc_driver_duckdb --uri :memory:\n\
           sidemantic preagg refresh --models ./models --model orders --name daily_revenue --mode full\n\
          sidemantic serve --models ./models --bind 127.0.0.1:5544\n\
         \n\
         Use '<command> --help' for command-specific usage."
    );
}

fn parse_options(args: &[String]) -> CliResult<(ParsedOptions, Vec<String>)> {
    let mut options: ParsedOptions = HashMap::new();
    let mut positionals = Vec::new();
    let mut index = 0usize;

    while index < args.len() {
        let arg = &args[index];
        if arg.starts_with("--") {
            if let Some((key, value)) = arg.split_once('=') {
                options
                    .entry(key.to_string())
                    .or_default()
                    .push(value.to_string());
                index += 1;
                continue;
            }

            let key = arg.to_string();
            if index + 1 < args.len() && !args[index + 1].starts_with("--") {
                options
                    .entry(key)
                    .or_default()
                    .push(args[index + 1].clone());
                index += 2;
            } else {
                options.entry(key).or_default().push("true".to_string());
                index += 1;
            }
        } else {
            positionals.push(arg.clone());
            index += 1;
        }
    }

    Ok((options, positionals))
}

fn option_values(options: &ParsedOptions, key: &str) -> Vec<String> {
    options.get(key).cloned().unwrap_or_default()
}

fn option_value(options: &ParsedOptions, key: &str) -> Option<String> {
    options.get(key).and_then(|values| values.last().cloned())
}

fn option_value_any(options: &ParsedOptions, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(value) = option_value(options, key) {
            return Some(value);
        }
    }
    None
}

fn require_option(options: &ParsedOptions, key: &str) -> CliResult<String> {
    option_value(options, key).ok_or_else(|| format!("missing required option '{key}'"))
}

fn option_flag(options: &ParsedOptions, key: &str) -> bool {
    options.contains_key(key)
}

fn option_usize(options: &ParsedOptions, key: &str) -> CliResult<Option<usize>> {
    match option_value(options, key) {
        Some(value) => value
            .parse::<usize>()
            .map(Some)
            .map_err(|_| format!("invalid usize value for {key}: {value}")),
        None => Ok(None),
    }
}

fn option_f64(options: &ParsedOptions, key: &str) -> CliResult<Option<f64>> {
    match option_value(options, key) {
        Some(value) => value
            .parse::<f64>()
            .map(Some)
            .map_err(|_| format!("invalid f64 value for {key}: {value}")),
        None => Ok(None),
    }
}

fn expect_no_positionals(positionals: &[String], context: &str) -> CliResult<()> {
    if positionals.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{context}: unexpected positional arguments: {}",
            positionals.join(" ")
        ))
    }
}

fn refresh_plan_error_message(err: SidemanticError) -> String {
    match err {
        SidemanticError::Validation(message) => message,
        other => other.to_string(),
    }
}

#[cfg(feature = "adbc-exec")]
fn env_non_empty(key: &str) -> Option<String> {
    match env::var(key) {
        Ok(value) if !value.trim().is_empty() => Some(value),
        _ => None,
    }
}

fn load_runtime(models_path: &str) -> CliResult<SidemanticRuntime> {
    let path = PathBuf::from(models_path);
    if path.is_dir() {
        return SidemanticRuntime::from_directory(path)
            .map_err(|e| format!("failed to load models directory '{models_path}': {e}"));
    }
    if path.is_file() {
        return SidemanticRuntime::from_file(path)
            .map_err(|e| format!("failed to load models file '{models_path}': {e}"));
    }
    Err(format!(
        "models path '{models_path}' is not a readable file or directory"
    ))
}

fn build_query_from_options(options: &ParsedOptions) -> CliResult<SemanticQuery> {
    let metrics = option_values(options, "--metric");
    let dimensions = option_values(options, "--dimension");
    if metrics.is_empty() && dimensions.is_empty() {
        return Err("query requires at least one --metric or --dimension".to_string());
    }

    let filters = option_values(options, "--filter");
    let segments = option_values(options, "--segment");
    let order_by = option_values(options, "--order-by");
    let limit = option_usize(options, "--limit")?;
    let ungrouped = option_flag(options, "--ungrouped");
    let use_preaggregations = option_flag(options, "--use-preaggregations");
    let skip_default_time_dimensions = option_flag(options, "--skip-default-time-dimensions");
    let preagg_database = option_value(options, "--preagg-database");
    let preagg_schema = option_value(options, "--preagg-schema");

    let mut query = SemanticQuery::new()
        .with_metrics(metrics)
        .with_dimensions(dimensions)
        .with_filters(filters)
        .with_segments(segments)
        .with_order_by(order_by)
        .with_ungrouped(ungrouped)
        .with_use_preaggregations(use_preaggregations)
        .with_skip_default_time_dimensions(skip_default_time_dimensions)
        .with_preaggregation_qualifiers(preagg_database, preagg_schema);

    if let Some(limit) = limit {
        query = query.with_limit(limit);
    }
    Ok(query)
}

fn compile_command(args: &[String]) -> CliResult<()> {
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!(
            "Usage: sidemantic compile --models <path> [--metric <model.metric> ...] [--dimension <model.dimension> ...] [--filter <sql> ...] [--segment <model.segment> ...] [--order-by <expr> ...] [--limit <n>] [--use-preaggregations] [--preagg-database <db>] [--preagg-schema <schema>] [--skip-default-time-dimensions]"
        );
        return Ok(());
    }

    let (options, positionals) = parse_options(args)?;
    expect_no_positionals(&positionals, "compile")?;
    let models = require_option(&options, "--models")?;
    let runtime = load_runtime(&models)?;
    let query = build_query_from_options(&options)?;
    let sql = runtime
        .compile(&query)
        .map_err(|e| format!("failed to compile query: {e}"))?;
    println!("{sql}");
    Ok(())
}

fn rewrite_command(args: &[String]) -> CliResult<()> {
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!("Usage: sidemantic rewrite --models <path> (--sql <query> | --sql-file <file>)");
        return Ok(());
    }

    let (options, positionals) = parse_options(args)?;
    expect_no_positionals(&positionals, "rewrite")?;
    let models = require_option(&options, "--models")?;
    let runtime = load_runtime(&models)?;

    let sql = if let Some(sql) = option_value(&options, "--sql") {
        sql
    } else if let Some(path) = option_value(&options, "--sql-file") {
        fs::read_to_string(&path).map_err(|e| format!("failed to read SQL file '{path}': {e}"))?
    } else {
        return Err("rewrite requires --sql or --sql-file".to_string());
    };

    let rewritten = runtime
        .rewrite(&sql)
        .map_err(|e| format!("failed to rewrite SQL: {e}"))?;
    println!("{rewritten}");
    Ok(())
}

fn validate_command(args: &[String]) -> CliResult<()> {
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!(
            "Usage: sidemantic validate [models_path] [--models <path>] [--verbose] [--metric <model.metric> ...] [--dimension <model.dimension> ...]"
        );
        return Ok(());
    }

    let (options, positionals) = parse_options(args)?;
    if positionals.len() > 1 {
        return Err(format!(
            "validate: unexpected positional arguments: {}",
            positionals[1..].join(" ")
        ));
    }

    let models = option_value(&options, "--models")
        .or_else(|| positionals.first().cloned())
        .unwrap_or_else(|| ".".to_string());
    let runtime = load_runtime(&models)?;
    let metrics = option_values(&options, "--metric");
    let dimensions = option_values(&options, "--dimension");
    let verbose = option_flag(&options, "--verbose");

    if !metrics.is_empty() || !dimensions.is_empty() {
        let errors = runtime.validate_query_references(&metrics, &dimensions);
        if errors.is_empty() {
            println!("ok");
            return Ok(());
        }

        for err in errors {
            eprintln!("{err}");
        }
        return Err("query reference validation failed".to_string());
    }

    let models_list = runtime.graph().models().collect::<Vec<_>>();
    if models_list.is_empty() {
        return Err("No models found".to_string());
    }

    println!("Validation passed");
    if verbose {
        println!("Models: {}", models_list.len());
        for model in &models_list {
            println!(
                "- {}: {} dimensions, {} metrics, {} relationships",
                model.name,
                model.dimensions.len(),
                model.metrics.len(),
                model.relationships.len()
            );
        }
    } else {
        println!("ok");
        return Ok(());
    }
    Ok(())
}

fn parse_queries_from_path(path: &str) -> CliResult<Vec<String>> {
    let source = PathBuf::from(path);
    if source.is_file() {
        let content = fs::read_to_string(&source)
            .map_err(|e| format!("failed to read queries file '{}': {e}", source.display()))?;
        let queries = split_queries(&content);
        if queries.is_empty() {
            return Err(format!("no SQL queries found in '{}'", source.display()));
        }
        return Ok(queries);
    }

    if !source.is_dir() {
        return Err(format!(
            "queries path '{}' does not exist",
            source.display()
        ));
    }

    let mut sql_files = Vec::new();
    let mut stack = vec![source.clone()];
    while let Some(dir) = stack.pop() {
        let entries = fs::read_dir(&dir)
            .map_err(|e| format!("failed to read queries directory '{}': {e}", dir.display()))?;
        for entry in entries {
            let entry = entry.map_err(|e| format!("failed to read query directory entry: {e}"))?;
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path
                .extension()
                .and_then(|value| value.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("sql"))
            {
                sql_files.push(path);
            }
        }
    }
    sql_files.sort();

    let mut queries = Vec::new();
    for file in sql_files {
        let content = fs::read_to_string(&file)
            .map_err(|e| format!("failed to read queries file '{}': {e}", file.display()))?;
        queries.extend(split_queries(&content));
    }
    if queries.is_empty() {
        return Err(format!("no SQL queries found under '{}'", source.display()));
    }
    Ok(queries)
}

fn split_column_reference(reference: &str) -> (String, String) {
    let cleaned = reference
        .trim()
        .trim_matches('`')
        .trim_matches('"')
        .trim_matches('[')
        .trim_matches(']');
    if let Some((table, column)) = cleaned.rsplit_once('.') {
        return (table.trim().to_string(), column.trim().to_string());
    }
    (String::new(), cleaned.to_string())
}

fn infer_query_models(payload: &CliMigratorAnalysisPayload) -> BTreeSet<String> {
    let mut models = BTreeSet::new();
    for reference in &payload.column_references {
        let (table, _column) = split_column_reference(reference);
        if !table.is_empty() {
            models.insert(table);
        }
    }
    for (table, _column) in &payload.group_by_columns {
        if !table.is_empty() {
            models.insert(table.clone());
        }
    }
    models
}

fn is_time_dimension_name(name: &str) -> bool {
    let lowered = name.to_ascii_lowercase();
    lowered.contains("date")
        || lowered.contains("time")
        || lowered.contains("timestamp")
        || lowered == "created_at"
        || lowered == "updated_at"
}

fn normalize_migrator_agg_name(raw_agg: &str, raw_arg: &str) -> (String, String) {
    let mut agg = raw_agg.to_ascii_lowercase();
    let mut arg = raw_arg.trim().to_string();
    if agg == "count" && arg.to_ascii_lowercase().starts_with("distinct ") {
        agg = "count_distinct".to_string();
        arg = arg[8..].trim().to_string();
    }
    if agg == "count" && arg.is_empty() {
        arg = "*".to_string();
    }
    (agg, arg)
}

fn extract_aggregations_from_query(query: &str) -> Vec<(String, String, String)> {
    let Ok(re) = Regex::new(r"(?i)\b(sum|avg|count|min|max|median)\s*\(([^)]*)\)") else {
        return Vec::new();
    };
    let mut values = Vec::new();
    for capture in re.captures_iter(query) {
        let Some(agg) = capture.get(1).map(|value| value.as_str()) else {
            continue;
        };
        let Some(arg) = capture.get(2).map(|value| value.as_str()) else {
            continue;
        };
        let (normalized_agg, normalized_arg) = normalize_migrator_agg_name(agg, arg);
        let (table, column) = split_column_reference(&normalized_arg);
        let effective_column = if column.is_empty() {
            normalized_arg
        } else {
            column
        };
        values.push((normalized_agg, effective_column, table));
    }
    values
}

fn build_metric_name(agg: &str, column: &str) -> String {
    if agg == "count" && column == "*" {
        "count".to_string()
    } else if agg == "count" || agg == "count_distinct" {
        format!("{}_count", column)
    } else {
        format!("{agg}_{column}")
    }
}

fn build_rewritten_query(
    payload: &CliMigratorAnalysisPayload,
    aggregations: &[(String, String, String)],
    default_model: Option<&str>,
) -> Option<String> {
    let default = default_model?;
    let mut dimensions = BTreeSet::new();
    for (table, column) in &payload.group_by_columns {
        let target = if table.is_empty() { default } else { table };
        if target == default && !column.is_empty() {
            dimensions.insert(format!("{default}.{column}"));
        }
    }

    let mut metrics = BTreeSet::new();
    for (agg, column, table) in aggregations {
        let target = if table.is_empty() { default } else { table };
        if target == default {
            metrics.insert(format!("{default}.{}", build_metric_name(agg, column)));
        }
    }

    if dimensions.is_empty() && metrics.is_empty() {
        return None;
    }

    let mut selections = Vec::new();
    selections.extend(dimensions);
    selections.extend(metrics);
    let body = selections
        .iter()
        .map(|value| format!("    {value}"))
        .collect::<Vec<_>>()
        .join(",\n");
    Some(format!("SELECT\n{body}\nFROM {default}"))
}

fn render_generated_model_yaml(model_name: &str, model: &CliGeneratedModel) -> CliResult<String> {
    let mut root = serde_yaml::Mapping::new();

    let mut model_meta = serde_yaml::Mapping::new();
    model_meta.insert(
        serde_yaml::Value::String("name".to_string()),
        serde_yaml::Value::String(model_name.to_string()),
    );
    model_meta.insert(
        serde_yaml::Value::String("table".to_string()),
        serde_yaml::Value::String(model_name.to_string()),
    );
    model_meta.insert(
        serde_yaml::Value::String("description".to_string()),
        serde_yaml::Value::String("Auto-generated from query analysis".to_string()),
    );
    root.insert(
        serde_yaml::Value::String("model".to_string()),
        serde_yaml::Value::Mapping(model_meta),
    );

    if !model.dimensions.is_empty() {
        let mut dimensions = Vec::new();
        for dimension in &model.dimensions {
            let mut item = serde_yaml::Mapping::new();
            item.insert(
                serde_yaml::Value::String("name".to_string()),
                serde_yaml::Value::String(dimension.to_string()),
            );
            item.insert(
                serde_yaml::Value::String("sql".to_string()),
                serde_yaml::Value::String(dimension.to_string()),
            );
            item.insert(
                serde_yaml::Value::String("type".to_string()),
                serde_yaml::Value::String(if is_time_dimension_name(dimension) {
                    "time".to_string()
                } else {
                    "categorical".to_string()
                }),
            );
            dimensions.push(serde_yaml::Value::Mapping(item));
        }
        root.insert(
            serde_yaml::Value::String("dimensions".to_string()),
            serde_yaml::Value::Sequence(dimensions),
        );
    }

    if !model.metrics.is_empty() {
        let mut metrics = model.metrics.clone();
        metrics.sort_by(|left, right| left.name.cmp(&right.name));
        let mut metric_values = Vec::new();
        for metric in metrics {
            let mut item = serde_yaml::Mapping::new();
            item.insert(
                serde_yaml::Value::String("name".to_string()),
                serde_yaml::Value::String(metric.name),
            );
            item.insert(
                serde_yaml::Value::String("agg".to_string()),
                serde_yaml::Value::String(metric.agg),
            );
            item.insert(
                serde_yaml::Value::String("sql".to_string()),
                serde_yaml::Value::String(metric.sql),
            );
            metric_values.push(serde_yaml::Value::Mapping(item));
        }
        root.insert(
            serde_yaml::Value::String("metrics".to_string()),
            serde_yaml::Value::Sequence(metric_values),
        );
    }

    serde_yaml::to_string(&serde_yaml::Value::Mapping(root))
        .map_err(|e| format!("failed to serialize generated model '{model_name}': {e}"))
}

fn migrator_command(args: &[String]) -> CliResult<()> {
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!(
            "Usage: sidemantic migrator [models_dir] --queries <file_or_dir> [--verbose] [--generate-models <out_dir>] [--models <path>]"
        );
        return Ok(());
    }

    let (options, positionals) = parse_options(args)?;
    if positionals.len() > 1 {
        return Err(format!(
            "migrator: unexpected positional arguments: {}",
            positionals[1..].join(" ")
        ));
    }

    let queries_path = option_value_any(&options, &["--queries", "--queries-file"])
        .ok_or_else(|| "migrator requires --queries <file_or_dir>".to_string())?;
    let queries = parse_queries_from_path(&queries_path)?;
    let verbose = option_flag(&options, "--verbose");
    let generate_models_dir = option_value(&options, "--generate-models");

    if let Some(output_root) = generate_models_dir {
        let models_output = PathBuf::from(&output_root).join("models");
        let rewritten_output = PathBuf::from(&output_root).join("rewritten_queries");
        fs::create_dir_all(&models_output).map_err(|e| {
            format!(
                "failed to create generated models directory '{}': {e}",
                models_output.display()
            )
        })?;
        fs::create_dir_all(&rewritten_output).map_err(|e| {
            format!(
                "failed to create rewritten queries directory '{}': {e}",
                rewritten_output.display()
            )
        })?;

        let mut models: BTreeMap<String, CliGeneratedModel> = BTreeMap::new();
        let mut rewritten_queries: Vec<String> = Vec::new();
        let mut parseable_queries = 0usize;

        for query in &queries {
            let analysis_json = sidemantic::analyze_migrator_query(query)
                .map_err(|e| format!("failed to analyze query for migrator bootstrap: {e}"))?;
            let payload: CliMigratorAnalysisPayload = serde_json::from_str(&analysis_json)
                .map_err(|e| format!("failed to decode migrator analysis payload: {e}"))?;
            parseable_queries += 1;

            let query_models = infer_query_models(&payload);
            let default_model = query_models.iter().next().map(String::as_str);
            let aggregations = extract_aggregations_from_query(query);

            for (table, column) in &payload.group_by_columns {
                let resolved_model = if table.is_empty() {
                    default_model.map(str::to_string)
                } else {
                    Some(table.to_string())
                };
                let Some(model_name) = resolved_model else {
                    continue;
                };
                if column.is_empty() {
                    continue;
                }
                models
                    .entry(model_name)
                    .or_default()
                    .dimensions
                    .insert(column.to_string());
            }

            for (agg, column, table) in &aggregations {
                let resolved_model = if table.is_empty() {
                    default_model.map(str::to_string)
                } else {
                    Some(table.to_string())
                };
                let Some(model_name) = resolved_model else {
                    continue;
                };
                let metric = CliMigratorMetric {
                    name: build_metric_name(agg, column),
                    agg: agg.to_string(),
                    sql: column.to_string(),
                };
                let model_entry = models.entry(model_name).or_default();
                if !model_entry
                    .metrics
                    .iter()
                    .any(|existing| existing.name == metric.name)
                {
                    model_entry.metrics.push(metric);
                }
            }

            rewritten_queries.push(
                build_rewritten_query(&payload, &aggregations, default_model)
                    .unwrap_or_else(|| query.clone()),
            );
        }

        for (model_name, model) in &models {
            let file_path = models_output.join(format!("{model_name}.yml"));
            let rendered = render_generated_model_yaml(model_name, model)?;
            fs::write(&file_path, rendered).map_err(|e| {
                format!(
                    "failed to write generated model file '{}': {e}",
                    file_path.display()
                )
            })?;
        }
        for (index, rewritten_sql) in rewritten_queries.iter().enumerate() {
            let file_path = rewritten_output.join(format!("query_{}.sql", index + 1));
            fs::write(&file_path, format!("{rewritten_sql}\n")).map_err(|e| {
                format!(
                    "failed to write rewritten query file '{}': {e}",
                    file_path.display()
                )
            })?;
        }

        println!(
            "Generated {} models and {} rewritten queries in {}",
            models.len(),
            rewritten_queries.len(),
            output_root
        );
        println!("Analyzed {parseable_queries} queries");
        return Ok(());
    }

    let models_path = option_value(&options, "--models")
        .or_else(|| positionals.first().cloned())
        .unwrap_or_else(|| ".".to_string());
    let runtime = load_runtime(&models_path)?;
    let available_models = runtime
        .graph()
        .models()
        .map(|model| model.name.clone())
        .collect::<BTreeSet<_>>();

    let mut parseable_queries = 0usize;
    let mut rewritable_queries = 0usize;
    let mut missing_models = BTreeSet::new();

    for (index, query) in queries.iter().enumerate() {
        let analysis_json = sidemantic::analyze_migrator_query(query)
            .map_err(|e| format!("failed to analyze query in migrator coverage mode: {e}"))?;
        let payload: CliMigratorAnalysisPayload = serde_json::from_str(&analysis_json)
            .map_err(|e| format!("failed to decode migrator analysis payload: {e}"))?;
        parseable_queries += 1;

        let query_models = infer_query_models(&payload);
        let missing_for_query = query_models
            .iter()
            .filter(|model| !available_models.contains(*model))
            .cloned()
            .collect::<BTreeSet<_>>();

        if missing_for_query.is_empty() {
            rewritable_queries += 1;
        } else {
            missing_models.extend(missing_for_query.iter().cloned());
        }

        if verbose {
            println!("Query #{}:", index + 1);
            if query_models.is_empty() {
                println!("  models: (none inferred)");
            } else {
                println!(
                    "  models: {}",
                    query_models.iter().cloned().collect::<Vec<_>>().join(", ")
                );
            }
            if missing_for_query.is_empty() {
                println!("  rewritable: yes");
            } else {
                println!(
                    "  missing models: {}",
                    missing_for_query
                        .iter()
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
        }
    }

    let coverage = if queries.is_empty() {
        0.0
    } else {
        (rewritable_queries as f64 / queries.len() as f64) * 100.0
    };

    println!("Total Queries: {}", queries.len());
    println!("Parseable: {}", parseable_queries);
    println!("Rewritable: {}", rewritable_queries);
    println!("Coverage: {:.1}%", coverage);
    if !missing_models.is_empty() {
        println!(
            "Missing Models: {}",
            missing_models.into_iter().collect::<Vec<_>>().join(", ")
        );
    }

    Ok(())
}

fn info_command(args: &[String]) -> CliResult<()> {
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!("Usage: sidemantic info [--models <path>]");
        return Ok(());
    }

    let (options, positionals) = parse_options(args)?;
    expect_no_positionals(&positionals, "info")?;
    let models = option_value(&options, "--models").unwrap_or_else(|| ".".to_string());
    let runtime = load_runtime(&models)?;

    let mut sorted_models = runtime.graph().models().collect::<Vec<_>>();
    sorted_models.sort_by(|left, right| left.name.cmp(&right.name));

    if sorted_models.is_empty() {
        println!("No models found");
        return Ok(());
    }

    println!("\nSemantic Layer: {models}\n");
    for model in sorted_models {
        println!("- {}", model.name);
        let table = match model.table.as_deref() {
            Some(value) if !value.is_empty() => value,
            _ => "N/A",
        };
        println!("  Table: {table}");
        println!("  Dimensions: {}", model.dimensions.len());
        println!("  Metrics: {}", model.metrics.len());
        println!("  Relationships: {}", model.relationships.len());
        if !model.relationships.is_empty() {
            let connected = model
                .relationships
                .iter()
                .map(|relationship| relationship.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            println!("  Connected to: {connected}");
        }
        println!();
    }

    Ok(())
}

fn query_command(args: &[String]) -> CliResult<()> {
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!(
            "Usage: sidemantic query [--models <path>] [--sql <query> | --sql-file <path> | <query>] [--dry-run] [--output <csv_file>] [--connection <url> | --db <path>] [--driver <name>] [--uri <uri>] [--entrypoint <symbol>] [--dbopt <k=v>] [--connopt <k=v>] [--username <user>] [--password <password>]"
        );
        return Ok(());
    }

    let (options, positionals) = parse_options(args)?;
    let models = option_value(&options, "--models").unwrap_or_else(|| ".".to_string());
    let runtime = load_runtime(&models)?;
    let sql = if let Some(sql) = option_value(&options, "--sql") {
        sql
    } else if let Some(path) = option_value(&options, "--sql-file") {
        fs::read_to_string(&path).map_err(|e| format!("failed to read SQL file '{path}': {e}"))?
    } else if positionals.is_empty() {
        return Err("query requires --sql, --sql-file, or a positional SQL query".to_string());
    } else {
        positionals.join(" ")
    };

    let rewritten = runtime
        .rewrite(&sql)
        .map_err(|e| format!("failed to rewrite SQL: {e}"))?;
    if option_flag(&options, "--dry-run") {
        println!("{rewritten}");
        return Ok(());
    }

    #[cfg(not(feature = "adbc-exec"))]
    {
        let _ = rewritten;
        Err("query execution requires the crate feature 'adbc-exec'".to_string())
    }

    #[cfg(feature = "adbc-exec")]
    {
        let adbc = parse_query_adbc_cli_config(&options, "query")?;
        let result = execute_with_adbc(AdbcExecutionRequest {
            driver: adbc.driver,
            sql: rewritten,
            uri: adbc.uri,
            entrypoint: adbc.entrypoint,
            database_options: adbc.database_options,
            connection_options: adbc.connection_options,
        })
        .map_err(|e| format!("failed to execute query via ADBC: {e}"))?;
        write_csv_rows(
            &result.columns,
            &result.rows,
            option_value(&options, "--output").as_deref(),
        )
    }
}

fn run_command(args: &[String]) -> CliResult<()> {
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!(
            "Usage: sidemantic run --models <path> [--driver <adbc_driver>] [--uri <adbc_uri>] [--entrypoint <driver_entrypoint>] [--username <user>] [--password <password>] [--dbopt <k=v[,k=v...]> ...] [--connopt <k=v[,k=v...]> ...] [--catalog <name>] [--schema <name>] [--autocommit <true|false>] [--read-only <true|false>] [--isolation-level <value>] [query flags]"
        );
        return Ok(());
    }

    #[cfg(not(feature = "adbc-exec"))]
    {
        let _ = args;
        Err("run requires the crate feature 'adbc-exec'".to_string())
    }

    #[cfg(feature = "adbc-exec")]
    {
        let (options, positionals) = parse_options(args)?;
        expect_no_positionals(&positionals, "run")?;

        let models = require_option(&options, "--models")?;
        let runtime = load_runtime(&models)?;
        let query = build_query_from_options(&options)?;
        let sql = runtime
            .compile(&query)
            .map_err(|e| format!("failed to compile query: {e}"))?;

        let adbc = parse_adbc_cli_config(&options, "run")?;
        let result = execute_with_adbc(AdbcExecutionRequest {
            driver: adbc.driver,
            sql: sql.clone(),
            uri: adbc.uri,
            entrypoint: adbc.entrypoint,
            database_options: adbc.database_options,
            connection_options: adbc.connection_options,
        })
        .map_err(|e| format!("failed to execute query via ADBC: {e}"))?;

        let rows = result
            .rows
            .iter()
            .map(|row| {
                let mut row_map = JsonMap::new();
                for (idx, column) in result.columns.iter().enumerate() {
                    let value = row
                        .get(idx)
                        .map(adbc_value_to_json)
                        .unwrap_or(JsonValue::Null);
                    row_map.insert(column.clone(), value);
                }
                JsonValue::Object(row_map)
            })
            .collect::<Vec<_>>();
        let payload = RunOutput {
            sql,
            columns: result.columns,
            row_count: rows.len(),
            rows,
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&payload)
                .map_err(|e| format!("failed to serialize run output: {e}"))?
        );
        Ok(())
    }
}

fn preagg_command(args: &[String]) -> CliResult<()> {
    if args.is_empty() || args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!(
            "Usage: sidemantic preagg <materialize|recommend|apply|refresh> [options]\n\
             \n\
             materialize: --models <path> --model <model_name> --name <preagg_name> [--execute ...adbc opts]\n\
             recommend: (--queries-file <path> | [--connection <url>|--db <path>] [--days <n>] [--limit <n>] [adbc opts]) [--min-query-count <n>] [--min-benefit-score <f64>] [--top-n <n>] [--json]\n\
             apply: --models <path> (--queries-file <path> | [--connection <url>|--db <path>] [--days <n>] [--limit <n>] [adbc opts]) [--min-query-count <n>] [--min-benefit-score <f64>] [--top-n <n>] [--dry-run]\n\
             refresh: --models <path> [--model <name>] [--name <preagg_name>|--preagg <preagg_name>] [--mode <full|incremental|merge|engine>] [--dialect <snowflake|clickhouse|bigquery>] [--refresh-every <interval>] [--from-watermark <value>] [--lookback <interval>] [--watermark-column <column>] [--execute ...adbc opts]\n\
             \n\
             ADBC opts: [--driver <name>] [--uri <uri>] [--entrypoint <symbol>] [--username <user>] [--password <password>] [--dbopt <k=v>] [--connopt <k=v>]"
        );
        return Ok(());
    }

    match args[0].as_str() {
        "materialize" => preagg_materialize_command(&args[1..]),
        "recommend" => preagg_recommend_command(&args[1..]),
        "apply" => preagg_apply_command(&args[1..]),
        "refresh" => preagg_refresh_command(&args[1..]),
        other => Err(format!(
            "unknown preagg subcommand '{other}'. Use 'preagg --help' for usage."
        )),
    }
}

fn preagg_materialize_command(args: &[String]) -> CliResult<()> {
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!(
            "Usage: sidemantic preagg materialize --models <path> --model <model_name> --name <preagg_name> [--preagg-database <db>] [--preagg-schema <schema>] [--execute ...adbc opts]"
        );
        return Ok(());
    }

    let (options, positionals) = parse_options(args)?;
    expect_no_positionals(&positionals, "preagg materialize")?;
    let models = require_option(&options, "--models")?;
    let model_name = require_option(&options, "--model")?;
    let preagg_name = require_option(&options, "--name")?;

    let runtime = load_runtime(&models)?;
    let sql = runtime
        .generate_preaggregation_materialization_sql(&model_name, &preagg_name)
        .map_err(|e| format!("failed to generate pre-aggregation SQL: {e}"))?;
    println!("{sql}");

    if !option_flag(&options, "--execute") {
        return Ok(());
    }

    #[cfg(not(feature = "adbc-exec"))]
    {
        Err("preagg materialize --execute requires feature 'adbc-exec'".to_string())
    }

    #[cfg(feature = "adbc-exec")]
    {
        let adbc = parse_adbc_cli_config(&options, "preagg materialize")?;
        execute_sql_statements(&adbc, &[sql])?;
        println!("materialized");
        Ok(())
    }
}

fn preagg_refresh_command(args: &[String]) -> CliResult<()> {
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!(
            "Usage: sidemantic preagg refresh --models <path> [--model <model>] [--name <preagg>|--preagg <preagg>] [--mode <full|incremental|merge|engine>] [--dialect <snowflake|clickhouse|bigquery>] [--refresh-every <interval>] [--from-watermark <value>] [--lookback <interval>] [--watermark-column <column>] [--preagg-database <db>] [--preagg-schema <schema>] [--execute ...adbc opts]"
        );
        return Ok(());
    }

    let (options, positionals) = parse_options(args)?;
    expect_no_positionals(&positionals, "preagg refresh")?;

    let models = require_option(&options, "--models")?;
    let model_filter = option_value(&options, "--model");
    let preagg_filter = option_value_any(&options, &["--name", "--preagg"]);
    let mode = option_value(&options, "--mode");
    if let Some(mode_value) = mode.as_deref() {
        let mode_is_valid = matches!(mode_value, "full" | "incremental" | "merge" | "engine");
        if !mode_is_valid {
            return Err(format!(
                "invalid preagg refresh mode '{mode_value}'. Supported modes: full, incremental, merge, engine"
            ));
        }
    }
    let dialect = option_value(&options, "--dialect");
    let refresh_every = option_value(&options, "--refresh-every");
    if mode.as_deref() == Some("engine") && dialect.is_none() {
        return Err("engine refresh mode requires --dialect".to_string());
    }

    let preagg_database = option_value(&options, "--preagg-database");
    let preagg_schema = option_value(&options, "--preagg-schema");
    let from_watermark = option_value(&options, "--from-watermark");
    let lookback = option_value(&options, "--lookback");
    let forced_watermark_column = option_value(&options, "--watermark-column");
    let execute = option_flag(&options, "--execute");

    let runtime = load_runtime(&models)?;
    let mut plans: Vec<RefreshPlan> = Vec::new();

    for model in runtime.graph().models() {
        if let Some(target_model) = model_filter.as_deref() {
            if model.name != target_model {
                continue;
            }
        }

        for preagg in &model.pre_aggregations {
            if let Some(target_preagg) = preagg_filter.as_deref() {
                if preagg.name != target_preagg {
                    continue;
                }
            }

            let table_name = preagg.table_name(
                &model.name,
                preagg_database.as_deref(),
                preagg_schema.as_deref(),
            );
            let resolved_mode = mode.clone().unwrap_or_else(|| {
                if preagg
                    .refresh_key
                    .as_ref()
                    .is_some_and(|refresh_key| refresh_key.incremental)
                {
                    "incremental".to_string()
                } else {
                    "full".to_string()
                }
            });
            let source_sql = runtime
                .generate_preaggregation_materialization_sql(&model.name, &preagg.name)
                .map_err(|e| {
                    format!(
                        "failed to generate materialization SQL for {}.{}: {e}",
                        model.name, preagg.name
                    )
                })?;

            let default_watermark = preagg
                .time_dimension
                .as_ref()
                .zip(preagg.granularity.as_ref())
                .map(|(time_dimension, granularity)| format!("{time_dimension}_{granularity}"));
            let watermark_column = forced_watermark_column
                .clone()
                .or(default_watermark)
                .unwrap_or_default();
            let effective_refresh_every = refresh_every.clone().or_else(|| {
                preagg
                    .refresh_key
                    .as_ref()
                    .and_then(|key| key.every.clone())
            });

            let statements = build_preaggregation_refresh_statements(
                &resolved_mode,
                &table_name,
                &source_sql,
                if watermark_column.is_empty() {
                    None
                } else {
                    Some(watermark_column.as_str())
                },
                from_watermark.as_deref(),
                lookback.as_deref(),
                dialect.as_deref(),
                effective_refresh_every.as_deref(),
            )
            .map_err(refresh_plan_error_message)?;

            plans.push(RefreshPlan {
                model_name: model.name.clone(),
                preagg_name: preagg.name.clone(),
                table_name,
                mode: resolved_mode,
                statements,
            });
        }
    }

    if plans.is_empty() {
        let mut scope_parts = Vec::new();
        if let Some(value) = model_filter {
            scope_parts.push(format!("model={value}"));
        }
        if let Some(value) = preagg_filter {
            scope_parts.push(format!("preagg={value}"));
        }
        let scope = if scope_parts.is_empty() {
            "requested scope".to_string()
        } else {
            scope_parts.join(", ")
        };
        return Err(format!("no pre-aggregations found for {scope}"));
    }

    for plan in &plans {
        println!(
            "-- refresh {}.{} mode={} table={}",
            plan.model_name, plan.preagg_name, plan.mode, plan.table_name
        );
        for statement in &plan.statements {
            println!("{statement};");
        }
        println!();
    }

    if !execute {
        println!("dry-run: generated refresh SQL only (add --execute to run statements)");
        return Ok(());
    }

    #[cfg(not(feature = "adbc-exec"))]
    {
        Err("preagg refresh --execute requires feature 'adbc-exec'".to_string())
    }

    #[cfg(feature = "adbc-exec")]
    {
        let adbc = parse_adbc_cli_config(&options, "preagg refresh")?;
        let mut statement_count = 0usize;
        for plan in &plans {
            execute_sql_statements(&adbc, &plan.statements)?;
            statement_count += plan.statements.len();
        }
        println!(
            "refreshed {} pre-aggregation(s) with {} SQL statement(s)",
            plans.len(),
            statement_count
        );
        Ok(())
    }
}

fn preagg_recommend_command(args: &[String]) -> CliResult<()> {
    let (options, positionals) = parse_options(args)?;
    expect_no_positionals(&positionals, "preagg recommend")?;
    let queries = load_preagg_queries(&options, "preagg recommend")?;
    let patterns_json = extract_preaggregation_patterns(queries)
        .map_err(|e| format!("failed to extract pre-aggregation patterns: {e}"))?;

    let min_query_count = option_usize(&options, "--min-query-count")?.unwrap_or(10);
    let min_benefit_score = option_f64(&options, "--min-benefit-score")?.unwrap_or(0.3);
    let top_n = option_usize(&options, "--top-n")?;
    let recommendations_json = recommend_preaggregation_patterns(
        &patterns_json,
        min_query_count,
        min_benefit_score,
        top_n,
    )
    .map_err(|e| format!("failed to build recommendations: {e}"))?;

    if option_flag(&options, "--json") {
        println!("{recommendations_json}");
        return Ok(());
    }

    let summary_json = summarize_preaggregation_patterns(&patterns_json, min_query_count)
        .map_err(|e| format!("failed to summarize recommendations: {e}"))?;
    let summary: serde_json::Value = serde_json::from_str(&summary_json)
        .map_err(|e| format!("failed to parse recommendation summary payload: {e}"))?;
    let recommendations: Vec<CliPreaggRecommendation> = serde_json::from_str(&recommendations_json)
        .map_err(|e| format!("failed to parse recommendations payload: {e}"))?;

    let total_queries = summary
        .get("total_queries")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let unique_patterns = summary
        .get("unique_patterns")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let patterns_above_threshold = summary
        .get("patterns_above_threshold")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);

    eprintln!("\n\u{2713} Analyzed {total_queries} queries");
    eprintln!(
        "  Found {unique_patterns} unique pattern{}",
        if unique_patterns == 1 { "" } else { "s" }
    );
    eprintln!("  {patterns_above_threshold} patterns above threshold");

    if let Some(models) = summary.get("models").and_then(serde_json::Value::as_object) {
        if !models.is_empty() {
            eprintln!("\n  Models:");
            let mut model_counts: Vec<_> = models.iter().collect();
            model_counts.sort_by(|left, right| left.0.cmp(right.0));
            for (model_name, count) in model_counts {
                let count = count.as_u64().unwrap_or(0);
                eprintln!("    {model_name}: {count} queries");
            }
        }
    }

    if recommendations.is_empty() {
        eprintln!("\nNo recommendations found above thresholds");
        eprintln!(
            "Try lowering --min-count (currently {min_query_count}) or --min-score (currently {min_benefit_score})"
        );
        return Ok(());
    }

    println!("\n{}", "=".repeat(80));
    println!(
        "Pre-Aggregation Recommendations (found {})",
        recommendations.len()
    );
    println!("{}\n", "=".repeat(80));

    for (index, recommendation) in recommendations.iter().enumerate() {
        println!("{}. {}", index + 1, recommendation.suggested_name);
        println!("   Model: {}", recommendation.pattern.model);
        println!("   Query Count: {}", recommendation.query_count);
        println!(
            "   Benefit Score: {:.2}",
            recommendation.estimated_benefit_score
        );
        println!("   Metrics: {}", recommendation.pattern.metrics.join(", "));
        if recommendation.pattern.dimensions.is_empty() {
            println!("   Dimensions: (none)");
        } else {
            println!(
                "   Dimensions: {}",
                recommendation.pattern.dimensions.join(", ")
            );
        }
        if !recommendation.pattern.granularities.is_empty() {
            println!(
                "   Granularities: {}",
                recommendation.pattern.granularities.join(", ")
            );
        }
        println!();
    }

    eprintln!("Run 'sidemantic preagg apply' to add these to your models");
    Ok(())
}

fn collect_yaml_files(models_path: &str) -> CliResult<Vec<PathBuf>> {
    let path = PathBuf::from(models_path);
    if path.is_file() {
        let extension = path
            .extension()
            .and_then(|value| value.to_str())
            .map(|value| value.to_ascii_lowercase())
            .unwrap_or_default();
        if extension == "yml" || extension == "yaml" {
            return Ok(vec![path]);
        }
        return Err(format!("models path '{models_path}' is not a YAML file"));
    }

    if !path.is_dir() {
        return Err(format!("models path '{models_path}' does not exist"));
    }

    let mut files: Vec<PathBuf> = Vec::new();
    let mut stack: Vec<PathBuf> = vec![path];
    while let Some(dir) = stack.pop() {
        let entries = fs::read_dir(&dir)
            .map_err(|e| format!("failed to read models directory '{}': {e}", dir.display()))?;
        for entry in entries {
            let entry = entry.map_err(|e| format!("failed to read models directory entry: {e}"))?;
            let entry_path = entry.path();
            if entry_path.is_dir() {
                stack.push(entry_path);
                continue;
            }
            if let Some(extension) = entry_path.extension().and_then(|value| value.to_str()) {
                let extension = extension.to_ascii_lowercase();
                if extension == "yml" || extension == "yaml" {
                    files.push(entry_path);
                }
            }
        }
    }
    files.sort();
    Ok(files)
}

fn find_model_in_yaml(content: &str, model_name: &str) -> CliResult<bool> {
    let yaml: serde_yaml::Value =
        serde_yaml::from_str(content).map_err(|e| format!("failed to parse YAML: {e}"))?;
    let Some(root) = yaml.as_mapping() else {
        return Ok(false);
    };

    let models_key = serde_yaml::Value::String("models".to_string());
    let Some(models_value) = root.get(&models_key) else {
        return Ok(false);
    };
    let Some(models) = models_value.as_sequence() else {
        return Ok(false);
    };

    for model in models {
        let Some(model_mapping) = model.as_mapping() else {
            continue;
        };
        let name_key = serde_yaml::Value::String("name".to_string());
        let Some(name_value) = model_mapping.get(&name_key) else {
            continue;
        };
        if name_value.as_str() == Some(model_name) {
            return Ok(true);
        }
    }

    Ok(false)
}

fn preagg_value_name(preagg_value: &serde_yaml::Value) -> Option<String> {
    let mapping = preagg_value.as_mapping()?;
    let key = serde_yaml::Value::String("name".to_string());
    mapping
        .get(&key)
        .and_then(serde_yaml::Value::as_str)
        .map(str::to_string)
}

fn parse_string_array(
    value: &serde_json::Value,
    key: &str,
) -> CliResult<Option<Vec<serde_yaml::Value>>> {
    let Some(raw) = value.get(key) else {
        return Ok(None);
    };
    let Some(items) = raw.as_array() else {
        return Err(format!(
            "pre-aggregation definition field '{key}' must be an array"
        ));
    };
    if items.is_empty() {
        return Ok(None);
    }
    let mut output = Vec::with_capacity(items.len());
    for item in items {
        let Some(item_str) = item.as_str() else {
            return Err(format!(
                "pre-aggregation definition field '{key}' must contain strings"
            ));
        };
        output.push(serde_yaml::Value::String(item_str.to_string()));
    }
    Ok(Some(output))
}

fn build_preagg_yaml_value(definition_json: &str) -> CliResult<serde_yaml::Value> {
    let definition: serde_json::Value = serde_json::from_str(definition_json)
        .map_err(|e| format!("failed to parse generated pre-aggregation definition JSON: {e}"))?;
    let Some(name) = definition
        .get("name")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
    else {
        return Err("generated pre-aggregation definition missing name".to_string());
    };
    let Some(measures) = parse_string_array(&definition, "measures")? else {
        return Err("generated pre-aggregation definition missing measures".to_string());
    };

    let mut mapping = serde_yaml::Mapping::new();
    mapping.insert(
        serde_yaml::Value::String("name".to_string()),
        serde_yaml::Value::String(name),
    );
    mapping.insert(
        serde_yaml::Value::String("measures".to_string()),
        serde_yaml::Value::Sequence(measures),
    );

    if let Some(dimensions) = parse_string_array(&definition, "dimensions")? {
        mapping.insert(
            serde_yaml::Value::String("dimensions".to_string()),
            serde_yaml::Value::Sequence(dimensions),
        );
    }
    if let Some(time_dimension) = definition
        .get("time_dimension")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
    {
        mapping.insert(
            serde_yaml::Value::String("time_dimension".to_string()),
            serde_yaml::Value::String(time_dimension),
        );
    }
    if let Some(granularity) = definition
        .get("granularity")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
    {
        mapping.insert(
            serde_yaml::Value::String("granularity".to_string()),
            serde_yaml::Value::String(granularity),
        );
    }

    Ok(serde_yaml::Value::Mapping(mapping))
}

fn preagg_apply_command(args: &[String]) -> CliResult<()> {
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!(
            "Usage: sidemantic preagg apply --models <path> (--queries-file <path> | [--connection <url>|--db <path>] [--days <n>] [--limit <n>] [adbc opts]) [--min-query-count <n>] [--min-benefit-score <f64>] [--top-n <n>] [--dry-run]"
        );
        return Ok(());
    }

    let (options, positionals) = parse_options(args)?;
    expect_no_positionals(&positionals, "preagg apply")?;
    let models = require_option(&options, "--models")?;
    let queries = load_preagg_queries(&options, "preagg apply")?;
    let patterns_json = extract_preaggregation_patterns(queries)
        .map_err(|e| format!("failed to extract pre-aggregation patterns: {e}"))?;

    let min_query_count = option_usize(&options, "--min-query-count")?.unwrap_or(10);
    let min_benefit_score = option_f64(&options, "--min-benefit-score")?.unwrap_or(0.3);
    let top_n = option_usize(&options, "--top-n")?;
    let dry_run = option_flag(&options, "--dry-run");

    let recommendations_json = recommend_preaggregation_patterns(
        &patterns_json,
        min_query_count,
        min_benefit_score,
        top_n,
    )
    .map_err(|e| format!("failed to build recommendations: {e}"))?;
    let recommendations: Vec<CliPreaggRecommendation> = serde_json::from_str(&recommendations_json)
        .map_err(|e| format!("failed to parse recommendations payload: {e}"))?;
    if recommendations.is_empty() {
        eprintln!("No recommendations found above thresholds");
        return Ok(());
    }

    let yaml_files = collect_yaml_files(&models)?;
    if yaml_files.is_empty() {
        return Err(format!("no YAML model files found under '{models}'"));
    }

    eprintln!(
        "\nFound {} recommendations to apply\n",
        recommendations.len()
    );
    let mut by_model: BTreeMap<String, Vec<CliPreaggRecommendation>> = BTreeMap::new();
    for recommendation in recommendations {
        by_model
            .entry(recommendation.pattern.model.clone())
            .or_default()
            .push(recommendation);
    }

    let mut updated_count = 0usize;
    for (model_name, model_recommendations) in by_model {
        let mut model_file: Option<PathBuf> = None;
        for yaml_file in &yaml_files {
            let file_content = fs::read_to_string(yaml_file)
                .map_err(|e| format!("failed to read model file '{}': {e}", yaml_file.display()))?;
            if find_model_in_yaml(&file_content, &model_name)? {
                model_file = Some(yaml_file.clone());
                break;
            }
        }

        let Some(model_file_path) = model_file else {
            eprintln!("warning: Could not find YAML file for model '{model_name}'");
            continue;
        };

        let mut yaml_data: serde_yaml::Value =
            serde_yaml::from_str(&fs::read_to_string(&model_file_path).map_err(|e| {
                format!(
                    "failed to read model file '{}': {e}",
                    model_file_path.display()
                )
            })?)
            .map_err(|e| {
                format!(
                    "failed to parse YAML file '{}': {e}",
                    model_file_path.display()
                )
            })?;
        let Some(root_mapping) = yaml_data.as_mapping_mut() else {
            return Err(format!(
                "YAML file '{}' must contain a mapping root",
                model_file_path.display()
            ));
        };

        let models_key = serde_yaml::Value::String("models".to_string());
        let Some(models_value) = root_mapping.get_mut(&models_key) else {
            continue;
        };
        let Some(models_seq) = models_value.as_sequence_mut() else {
            return Err(format!(
                "YAML file '{}' has non-sequence 'models' entry",
                model_file_path.display()
            ));
        };

        let mut file_modified = false;
        for model in models_seq {
            let Some(model_mapping) = model.as_mapping_mut() else {
                continue;
            };

            let name_key = serde_yaml::Value::String("name".to_string());
            if model_mapping
                .get(&name_key)
                .and_then(serde_yaml::Value::as_str)
                != Some(model_name.as_str())
            {
                continue;
            }

            let preaggs_key = serde_yaml::Value::String("pre_aggregations".to_string());
            if !model_mapping.contains_key(&preaggs_key) {
                model_mapping.insert(preaggs_key.clone(), serde_yaml::Value::Sequence(Vec::new()));
            }
            let Some(preaggs_seq) = model_mapping
                .get_mut(&preaggs_key)
                .and_then(serde_yaml::Value::as_sequence_mut)
            else {
                return Err(format!(
                    "model '{model_name}' in '{}' has non-sequence pre_aggregations",
                    model_file_path.display()
                ));
            };

            for recommendation in &model_recommendations {
                let recommendation_payload = serde_json::json!({
                    "pattern": {
                        "model": recommendation.pattern.model,
                        "metrics": recommendation.pattern.metrics,
                        "dimensions": recommendation.pattern.dimensions,
                        "granularities": recommendation.pattern.granularities,
                        "count": recommendation.pattern.count,
                    },
                    "suggested_name": recommendation.suggested_name,
                    "query_count": recommendation.query_count,
                    "estimated_benefit_score": recommendation.estimated_benefit_score,
                });
                let recommendation_json = serde_json::to_string(&recommendation_payload)
                    .map_err(|e| format!("failed to serialize recommendation payload: {e}"))?;
                let definition_json = generate_preaggregation_definition(&recommendation_json)
                    .map_err(|e| format!("failed to generate pre-aggregation definition: {e}"))?;
                let preagg_value = build_preagg_yaml_value(&definition_json)?;
                let Some(preagg_name) = preagg_value_name(&preagg_value) else {
                    return Err("generated pre-aggregation definition missing name".to_string());
                };

                if preaggs_seq.iter().any(|existing| {
                    preagg_value_name(existing).as_deref() == Some(preagg_name.as_str())
                }) {
                    continue;
                }

                preaggs_seq.push(preagg_value);
                file_modified = true;
                updated_count += 1;
                eprintln!(
                    "  + {model_name}.{preagg_name} ({} queries)",
                    recommendation.query_count
                );
            }
            break;
        }

        if file_modified && !dry_run {
            let rendered = serde_yaml::to_string(&yaml_data).map_err(|e| {
                format!(
                    "failed to serialize updated YAML for '{}': {e}",
                    model_file_path.display()
                )
            })?;
            fs::write(&model_file_path, rendered).map_err(|e| {
                format!(
                    "failed to write updated YAML file '{}': {e}",
                    model_file_path.display()
                )
            })?;
        }
    }

    if dry_run {
        eprintln!("Dry run: Would add {updated_count} pre-aggregations");
    } else {
        eprintln!("\u{2713} Added {updated_count} pre-aggregations to model files");
    }

    Ok(())
}

pub(crate) fn workbench_command(args: &[String]) -> CliResult<()> {
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!(
            "Usage: sidemantic workbench [models_dir] [--demo] [--connection <url>] [--db <path>]"
        );
        return Ok(());
    }

    let (options, positionals) = parse_options(args)?;
    if positionals.len() > 1 {
        return Err(format!(
            "workbench: unexpected positional arguments: {}",
            positionals[1..].join(" ")
        ));
    }

    let demo_mode = option_flag(&options, "--demo");
    let directory = if demo_mode {
        let candidates = [
            PathBuf::from("examples").join("multi_format_demo"),
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("..")
                .join("examples")
                .join("multi_format_demo"),
        ];
        candidates
            .iter()
            .find(|path| path.exists())
            .cloned()
            .ok_or_else(|| "Error: Demo models not found".to_string())?
    } else {
        PathBuf::from(
            positionals
                .first()
                .cloned()
                .unwrap_or_else(|| ".".to_string()),
        )
    };

    if !directory.exists() {
        return Err(format!(
            "Error: Directory {} does not exist",
            directory.display()
        ));
    }

    let connection = if let Some(value) = option_value(&options, "--connection") {
        Some(value)
    } else if let Some(db_path) = option_value(&options, "--db") {
        let db_path = PathBuf::from(db_path);
        let absolute = if db_path.is_absolute() {
            db_path
        } else {
            env::current_dir()
                .map_err(|e| format!("failed to inspect current directory: {e}"))?
                .join(db_path)
        };
        Some(render_duckdb_connection_url(&absolute))
    } else if demo_mode {
        prepare_workbench_demo_connection()?
    } else {
        discover_duckdb_connection_from_data_dir(&directory)?
    };

    let mut details = vec![format!("models={}", directory.display())];
    if demo_mode {
        details.push("demo=true".to_string());
    }
    if let Some(value) = connection.as_ref() {
        details.push(format!("connection={value}"));
    }

    launch_workbench_tui(directory.to_string_lossy().as_ref(), connection).map_err(|err| {
        format!(
            "{err} (resolved: {})",
            details
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        )
    })
}

fn discover_duckdb_connection_from_data_dir(directory: &Path) -> CliResult<Option<String>> {
    let data_dir = directory.join("data");
    if !data_dir.is_dir() {
        return Ok(None);
    }

    let entries = fs::read_dir(&data_dir).map_err(|e| {
        format!(
            "failed to read workbench data directory '{}': {e}",
            data_dir.display()
        )
    })?;

    let mut db_files = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| {
            format!(
                "failed to read workbench data directory entry in '{}': {e}",
                data_dir.display()
            )
        })?;
        let path = entry.path();
        if path.is_file()
            && path
                .extension()
                .and_then(|value| value.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("db"))
        {
            db_files.push(path);
        }
    }
    db_files.sort();

    let Some(selected) = db_files.into_iter().next() else {
        return Ok(None);
    };

    let absolute = if selected.is_absolute() {
        selected
    } else {
        env::current_dir()
            .map_err(|e| format!("failed to inspect current directory: {e}"))?
            .join(selected)
    };
    Ok(Some(render_duckdb_connection_url(&absolute)))
}

fn unique_demo_db_path() -> CliResult<PathBuf> {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("failed to compute demo database timestamp: {e}"))?
        .as_nanos();
    let mut path = env::temp_dir();
    path.push(format!("sidemantic_workbench_demo_{suffix}.db"));
    Ok(path)
}

fn render_duckdb_connection_url(path: &Path) -> String {
    let rendered = path.to_string_lossy();
    if path.is_absolute() {
        format!("duckdb://{rendered}")
    } else {
        format!("duckdb:///{rendered}")
    }
}

#[cfg(feature = "adbc-exec")]
fn prepare_workbench_demo_connection() -> CliResult<Option<String>> {
    let db_path = unique_demo_db_path()?;
    let db_uri = db_path.to_string_lossy().to_string();
    let connection_url = render_duckdb_connection_url(&db_path);

    let adbc = AdbcCliConfig {
        driver: normalize_adbc_driver_name("duckdb"),
        uri: Some(db_uri),
        entrypoint: None,
        database_options: Vec::new(),
        connection_options: Vec::new(),
    };
    if let Err(err) = execute_sql_statements(&adbc, &workbench_demo_seed_sql()) {
        eprintln!(
            "warning: failed to seed demo database via ADBC ({err}); continuing with demo connection"
        );
    }

    Ok(Some(connection_url))
}

#[cfg(not(feature = "adbc-exec"))]
fn prepare_workbench_demo_connection() -> CliResult<Option<String>> {
    let db_path = unique_demo_db_path()?;
    Ok(Some(render_duckdb_connection_url(&db_path)))
}

#[cfg(feature = "adbc-exec")]
fn workbench_demo_seed_sql() -> Vec<String> {
    vec![
        r#"
create table customers (
  id integer primary key,
  name varchar,
  email varchar,
  region varchar,
  signup_date date
)
"#
        .trim()
        .to_string(),
        r#"
insert into customers values
  (1, 'Alice Johnson', 'alice@example.com', 'North', '2023-01-15'),
  (2, 'Bob Smith', 'bob@example.com', 'South', '2023-02-20'),
  (3, 'Carol Davis', 'carol@example.com', 'East', '2023-03-10'),
  (4, 'David Wilson', 'david@example.com', 'West', '2023-04-05'),
  (5, 'Eve Martinez', 'eve@example.com', 'North', '2023-05-18')
"#
        .trim()
        .to_string(),
        r#"
create table products (
  id integer primary key,
  name varchar,
  category varchar,
  price decimal(10,2),
  cost decimal(10,2)
)
"#
        .trim()
        .to_string(),
        r#"
insert into products values
  (1, 'Laptop Pro', 'Electronics', 1299.99, 800.00),
  (2, 'Wireless Mouse', 'Electronics', 29.99, 15.00),
  (3, 'Desk Chair', 'Furniture', 249.99, 120.00),
  (4, 'Standing Desk', 'Furniture', 599.99, 350.00),
  (5, 'Notebook Set', 'Office Supplies', 12.99, 5.00)
"#
        .trim()
        .to_string(),
        r#"
create table orders (
  id integer primary key,
  customer_id integer,
  product_id integer,
  quantity integer,
  amount decimal(10,2),
  status varchar,
  created_at timestamp
)
"#
        .trim()
        .to_string(),
        r#"
insert into orders values
  (1, 1, 1, 1, 1299.99, 'completed', '2025-01-10 10:30:00'),
  (2, 2, 2, 2, 59.98, 'completed', '2025-01-11 11:45:00'),
  (3, 3, 3, 1, 249.99, 'pending', '2025-01-12 09:15:00'),
  (4, 4, 4, 1, 599.99, 'completed', '2025-01-13 14:20:00'),
  (5, 5, 5, 3, 38.97, 'cancelled', '2025-01-14 16:05:00'),
  (6, 1, 2, 1, 29.99, 'completed', '2025-01-15 13:10:00'),
  (7, 2, 3, 2, 499.98, 'completed', '2025-01-16 15:40:00')
"#
        .trim()
        .to_string(),
    ]
}

fn tree_command(args: &[String]) -> CliResult<()> {
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!("Usage: sidemantic tree <models_dir>");
        return Ok(());
    }

    let (options, positionals) = parse_options(args)?;
    if !options.is_empty() {
        return Err(
            "tree does not accept options; use 'workbench' for --demo/--connection/--db"
                .to_string(),
        );
    }
    if positionals.len() != 1 {
        return Err("tree requires exactly one positional models directory".to_string());
    }
    eprintln!("tree is deprecated; use 'workbench'.");
    workbench_command(positionals.as_slice())
}

#[cfg(feature = "workbench-tui")]
fn launch_workbench_tui(models_path: &str, connection: Option<String>) -> CliResult<()> {
    workbench::launch(models_path, connection)
}

#[cfg(not(feature = "workbench-tui"))]
fn launch_workbench_tui(_models_path: &str, _connection: Option<String>) -> CliResult<()> {
    Err("workbench requires the crate feature 'workbench-tui'".to_string())
}

fn mcp_command(args: &[String]) -> CliResult<()> {
    #[cfg(not(feature = "mcp-server"))]
    {
        let _ = args;
        Err("mcp requires the crate feature 'mcp-server'".to_string())
    }

    #[cfg(feature = "mcp-server")]
    {
        run_sibling_binary("sidemantic-mcp", args)
    }
}

fn server_command(args: &[String]) -> CliResult<()> {
    #[cfg(not(feature = "runtime-server"))]
    {
        let _ = args;
        Err("server requires the crate feature 'runtime-server'".to_string())
    }

    #[cfg(feature = "runtime-server")]
    {
        run_sibling_binary("sidemantic-server", args)
    }
}

fn lsp_command(args: &[String]) -> CliResult<()> {
    #[cfg(not(feature = "runtime-lsp"))]
    {
        let _ = args;
        Err("lsp requires the crate feature 'runtime-lsp'".to_string())
    }

    #[cfg(feature = "runtime-lsp")]
    {
        run_sibling_binary("sidemantic-lsp", args)
    }
}

#[cfg(any(
    feature = "mcp-server",
    feature = "runtime-server",
    feature = "runtime-lsp"
))]
fn run_sibling_binary(binary_name: &str, args: &[String]) -> CliResult<()> {
    let current = env::current_exe().map_err(|e| format!("failed to inspect executable: {e}"))?;
    let mut candidate = current.clone();
    candidate.set_file_name(format!("{binary_name}{}", env::consts::EXE_SUFFIX));
    if !candidate.exists() {
        return Err(format!(
            "{binary_name} binary not found next to {}. Build/install sibling runtime binaries (e.g. cargo build --manifest-path sidemantic-rs/Cargo.toml --features mcp-server,runtime-server,runtime-lsp --bins).",
            current.display()
        ));
    }

    let status = Command::new(&candidate)
        .args(args)
        .status()
        .map_err(|e| format!("failed to launch {}: {e}", candidate.display()))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{binary_name} exited with status {status}"))
    }
}

fn split_queries(content: &str) -> Vec<String> {
    let chunks: Vec<String> = content
        .split(';')
        .map(str::trim)
        .filter(|chunk| !chunk.is_empty())
        .map(str::to_string)
        .collect();
    if !chunks.is_empty() {
        return chunks;
    }
    content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect()
}

fn load_preagg_queries(options: &ParsedOptions, context: &str) -> CliResult<Vec<String>> {
    let queries_file = option_value(options, "--queries-file");
    let has_connection_mode = option_value(options, "--connection").is_some()
        || option_value(options, "--db").is_some()
        || option_value(options, "--driver").is_some()
        || option_value(options, "--uri").is_some();
    if queries_file.is_some() && has_connection_mode {
        return Err(format!(
            "{context}: provide either --queries-file or connection options (--connection/--db/--driver/--uri), not both"
        ));
    }

    if let Some(queries_file) = queries_file {
        let content = fs::read_to_string(&queries_file)
            .map_err(|e| format!("failed to read queries file '{queries_file}': {e}"))?;
        return Ok(split_queries(&content));
    }

    if !has_connection_mode {
        return Err(format!(
            "{context}: missing query source; use --queries-file or --connection/--db"
        ));
    }

    let days_back = option_usize(options, "--days")?.unwrap_or(7);
    let limit = option_usize(options, "--limit")?.unwrap_or(1000);
    if days_back == 0 {
        return Err(format!("{context}: --days must be greater than 0"));
    }
    if limit == 0 {
        return Err(format!("{context}: --limit must be greater than 0"));
    }

    #[cfg(not(feature = "adbc-exec"))]
    {
        let _ = days_back;
        let _ = limit;
        Err(format!(
            "{context}: query-history mode requires the crate feature 'adbc-exec'"
        ))
    }

    #[cfg(feature = "adbc-exec")]
    {
        let adbc = parse_query_adbc_cli_config(options, context)?;
        fetch_preagg_query_history(&adbc, days_back, limit, context)
    }
}

#[cfg(feature = "adbc-exec")]
fn parse_kv_pairs(input: &str, option_name: &str) -> CliResult<Vec<(String, String)>> {
    let mut pairs = Vec::new();
    for fragment in input
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
    {
        let (key, value) = fragment
            .split_once('=')
            .ok_or_else(|| format!("{option_name} expects key=value, got '{fragment}'"))?;
        if key.trim().is_empty() {
            return Err(format!("{option_name} key cannot be empty: '{fragment}'"));
        }
        pairs.push((key.trim().to_string(), value.to_string()));
    }
    if pairs.is_empty() {
        return Err(format!("{option_name} expects key=value pairs"));
    }
    Ok(pairs)
}

#[cfg(feature = "adbc-exec")]
fn infer_preagg_history_dialect(driver: &str, uri: Option<&str>) -> Option<&'static str> {
    let driver_family = driver
        .strip_prefix("adbc_driver_")
        .unwrap_or(driver)
        .to_ascii_lowercase();
    match driver_family.as_str() {
        "bigquery" => Some("bigquery"),
        "snowflake" => Some("snowflake"),
        "clickhouse" => Some("clickhouse"),
        "databricks" => Some("databricks"),
        "spark" => {
            if let Some(uri_value) = uri {
                if uri_value.to_ascii_lowercase().starts_with("databricks://") {
                    return Some("databricks");
                }
            }
            None
        }
        _ => None,
    }
}

#[cfg(feature = "adbc-exec")]
fn is_safe_sql_identifier_fragment(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
}

#[cfg(feature = "adbc-exec")]
fn parse_bigquery_history_target(uri: Option<&str>, context: &str) -> CliResult<(String, String)> {
    let raw = uri.ok_or_else(|| {
        format!(
            "{context}: BigQuery query-history mode requires a --connection URL or --uri with project information"
        )
    })?;
    let normalized = raw.strip_prefix("bigquery://").unwrap_or(raw);
    let (path_part, query_part) = normalized
        .split_once('?')
        .map_or((normalized, None), |(path, query)| (path, Some(query)));
    let project = path_part
        .split('/')
        .next()
        .map(str::trim)
        .unwrap_or_default();
    if !is_safe_sql_identifier_fragment(project) {
        return Err(format!(
            "{context}: BigQuery project id must contain only letters, numbers, '_' or '-'"
        ));
    }

    let mut location = "us".to_string();
    if let Some(query) = query_part {
        for (key, value) in parse_query_pairs(query) {
            if (key == "location" || key == "region") && !value.trim().is_empty() {
                location = value.trim().to_ascii_lowercase();
            }
        }
    }
    if !is_safe_sql_identifier_fragment(&location) {
        return Err(format!(
            "{context}: BigQuery location must contain only letters, numbers, '_' or '-'"
        ));
    }

    Ok((project.to_string(), location))
}

#[cfg(feature = "adbc-exec")]
fn build_preagg_query_history_sql(
    dialect: &str,
    uri: Option<&str>,
    days_back: usize,
    limit: usize,
    context: &str,
) -> CliResult<String> {
    match dialect {
        "snowflake" => Ok(format!(
            "SELECT query_text \
             FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(END_TIME_RANGE_START => DATEADD('day', -{days_back}, CURRENT_TIMESTAMP()))) \
             WHERE query_text LIKE '%-- sidemantic:%' \
               AND execution_status = 'SUCCESS' \
             ORDER BY start_time DESC \
             LIMIT {limit}"
        )),
        "bigquery" => {
            let (project, location) = parse_bigquery_history_target(uri, context)?;
            Ok(format!(
                "SELECT query \
                 FROM `{project}.region-{location}.INFORMATION_SCHEMA.JOBS_BY_PROJECT` \
                 WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY) \
                   AND job_type = 'QUERY' \
                   AND state = 'DONE' \
                   AND query LIKE '%-- sidemantic:%' \
                 ORDER BY creation_time DESC \
                 LIMIT {limit}"
            ))
        }
        "databricks" => Ok(format!(
            "SELECT statement_text \
             FROM system.query.history \
             WHERE start_time >= CURRENT_TIMESTAMP() - INTERVAL {days_back} DAYS \
               AND statement_text LIKE '%-- sidemantic:%' \
               AND status = 'FINISHED' \
             ORDER BY start_time DESC \
             LIMIT {limit}"
        )),
        "clickhouse" => Ok(format!(
            "SELECT query \
             FROM system.query_log \
             WHERE event_time >= now() - INTERVAL {days_back} DAY \
               AND query LIKE '%-- sidemantic:%' \
               AND type = 'QueryFinish' \
               AND exception = '' \
             ORDER BY event_time DESC \
             LIMIT {limit}"
        )),
        _ => Err(format!(
            "{context}: unsupported query-history dialect '{dialect}'"
        )),
    }
}

#[cfg(feature = "adbc-exec")]
fn adbc_value_text(value: &AdbcValue) -> Option<String> {
    match value {
        AdbcValue::Null => None,
        AdbcValue::Bool(v) => Some(v.to_string()),
        AdbcValue::I64(v) => Some(v.to_string()),
        AdbcValue::U64(v) => Some(v.to_string()),
        AdbcValue::F64(v) => Some(v.to_string()),
        AdbcValue::String(v) => Some(v.clone()),
        AdbcValue::Bytes(v) => Some(String::from_utf8_lossy(v).to_string()),
    }
}

#[cfg(feature = "adbc-exec")]
fn fetch_preagg_query_history(
    adbc: &AdbcCliConfig,
    days_back: usize,
    limit: usize,
    context: &str,
) -> CliResult<Vec<String>> {
    let dialect = infer_preagg_history_dialect(&adbc.driver, adbc.uri.as_deref()).ok_or_else(|| {
        format!(
            "{context}: adapter does not support get_query_history(). Supported adapters: BigQueryAdapter, SnowflakeAdapter, DatabricksAdapter, ClickHouseAdapter"
        )
    })?;
    let history_sql =
        build_preagg_query_history_sql(dialect, adbc.uri.as_deref(), days_back, limit, context)?;
    let result = execute_with_adbc(AdbcExecutionRequest {
        driver: adbc.driver.clone(),
        sql: history_sql,
        uri: adbc.uri.clone(),
        entrypoint: adbc.entrypoint.clone(),
        database_options: adbc.database_options.clone(),
        connection_options: adbc.connection_options.clone(),
    })
    .map_err(|e| format!("{context}: failed to fetch query history via ADBC: {e}"))?;

    let mut queries = Vec::new();
    for row in result.rows {
        if let Some(first_col) = row.first() {
            if let Some(text) = adbc_value_text(first_col) {
                let trimmed = text.trim();
                if !trimmed.is_empty() {
                    queries.push(trimmed.to_string());
                }
            }
        }
    }
    Ok(queries)
}

#[cfg(feature = "adbc-exec")]
fn parse_option_value(value: &str) -> OptionValue {
    if let Some(rest) = value.strip_prefix("int:") {
        if let Ok(parsed) = rest.parse::<i64>() {
            return OptionValue::Int(parsed);
        }
    }
    if let Some(rest) = value.strip_prefix("float:") {
        if let Ok(parsed) = rest.parse::<f64>() {
            return OptionValue::Double(parsed);
        }
    }
    if let Some(rest) = value.strip_prefix("str:") {
        return OptionValue::String(rest.to_string());
    }
    if let Ok(parsed) = value.parse::<i64>() {
        return OptionValue::Int(parsed);
    }
    if let Ok(parsed) = value.parse::<f64>() {
        return OptionValue::Double(parsed);
    }
    OptionValue::String(value.to_string())
}

#[cfg(feature = "adbc-exec")]
fn parse_bool_option(raw: &str, option_name: &str) -> CliResult<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(format!("{option_name} expects true/false, got '{raw}'")),
    }
}

#[cfg(feature = "adbc-exec")]
fn parse_database_options(values: &[String]) -> CliResult<Vec<(OptionDatabase, OptionValue)>> {
    let mut parsed = Vec::new();
    for value in values {
        for (key, raw_value) in parse_kv_pairs(value, "--dbopt")? {
            parsed.push((
                OptionDatabase::from(key.as_str()),
                parse_option_value(&raw_value),
            ));
        }
    }
    Ok(parsed)
}

#[cfg(feature = "adbc-exec")]
fn parse_connection_options(values: &[String]) -> CliResult<Vec<(OptionConnection, OptionValue)>> {
    let mut parsed = Vec::new();
    for value in values {
        for (key, raw_value) in parse_kv_pairs(value, "--connopt")? {
            parsed.push((
                OptionConnection::from(key.as_str()),
                parse_option_value(&raw_value),
            ));
        }
    }
    Ok(parsed)
}

#[cfg(feature = "adbc-exec")]
fn normalize_adbc_driver_name(driver: &str) -> String {
    if driver.starts_with("adbc_driver_")
        || driver.contains('/')
        || driver.contains('\\')
        || driver.ends_with(".so")
        || driver.ends_with(".dylib")
        || driver.ends_with(".dll")
    {
        driver.to_string()
    } else {
        format!("adbc_driver_{driver}")
    }
}

#[cfg(feature = "adbc-exec")]
fn parse_query_pairs(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|fragment| !fragment.trim().is_empty())
        .filter_map(|fragment| {
            let (key, value) = fragment.split_once('=').unwrap_or((fragment, ""));
            if key.trim().is_empty() {
                return None;
            }
            Some((key.trim().to_string(), value.to_string()))
        })
        .collect()
}

#[cfg(feature = "adbc-exec")]
pub(crate) fn parse_connection_url_to_adbc(connection: &str) -> CliResult<AdbcConnectionUrlParts> {
    let (scheme, remainder) = connection
        .split_once("://")
        .ok_or_else(|| format!("invalid connection URL '{connection}': expected scheme://..."))?;
    let scheme = scheme.to_ascii_lowercase();
    let (path_part, query_part) = remainder
        .split_once('?')
        .map_or((remainder, None), |(path, query)| (path, Some(query)));
    let query_pairs = query_part.map(parse_query_pairs).unwrap_or_default();

    match scheme.as_str() {
        "adbc" => {
            let (driver_raw, path_uri) = path_part
                .split_once('/')
                .map_or((path_part, None), |(driver, path)| (driver, Some(path)));
            if driver_raw.trim().is_empty() {
                return Err(
                    "adbc:// URL must include a driver, e.g. adbc://duckdb?uri=:memory:"
                        .to_string(),
                );
            }

            let mut uri = path_uri
                .map(str::to_string)
                .filter(|value| !value.trim().is_empty());
            let mut db_options: Vec<(OptionDatabase, OptionValue)> = Vec::new();
            for (key, raw_value) in query_pairs {
                if key == "uri" {
                    if !raw_value.is_empty() {
                        uri = Some(raw_value);
                    }
                    continue;
                }
                db_options.push((
                    OptionDatabase::from(key.as_str()),
                    parse_option_value(&raw_value),
                ));
            }
            if driver_raw.eq_ignore_ascii_case("sqlite") && uri.is_none() {
                uri = Some(":memory:".to_string());
            }
            Ok((normalize_adbc_driver_name(driver_raw), uri, db_options))
        }
        "duckdb" => {
            let mut uri = if matches!(path_part, "" | "/" | ":memory:" | "/:memory:") {
                ":memory:".to_string()
            } else if path_part.starts_with("md:") || path_part.starts_with('/') {
                path_part.to_string()
            } else {
                format!("//{path_part}")
            };

            if uri == "//" {
                uri = ":memory:".to_string();
            }

            let db_options = query_pairs
                .into_iter()
                .map(|(key, raw_value)| {
                    (
                        OptionDatabase::from(key.as_str()),
                        parse_option_value(&raw_value),
                    )
                })
                .collect::<Vec<_>>();
            Ok((normalize_adbc_driver_name("duckdb"), Some(uri), db_options))
        }
        "sqlite" => {
            let trimmed = path_part.trim_start_matches('/');
            let uri = if trimmed.is_empty() {
                ":memory:".to_string()
            } else {
                trimmed.to_string()
            };
            Ok((normalize_adbc_driver_name("sqlite"), Some(uri), Vec::new()))
        }
        "databricks" | "spark" | "postgresql" | "postgres" | "mysql" | "snowflake" | "bigquery"
        | "clickhouse" | "mssql" | "trino" | "redshift" => {
            let driver = match scheme.as_str() {
                "postgres" => "postgresql",
                other => other,
            };
            Ok((
                normalize_adbc_driver_name(driver),
                Some(connection.to_string()),
                Vec::new(),
            ))
        }
        _ => Err(format!(
            "unsupported connection URL scheme '{scheme}'. Use --driver/--uri for custom drivers."
        )),
    }
}

#[cfg(feature = "adbc-exec")]
fn parse_adbc_cli_config_internal(
    options: &ParsedOptions,
    context: &str,
    default_driver: Option<String>,
    default_uri: Option<String>,
    mut default_database_options: Vec<(OptionDatabase, OptionValue)>,
) -> CliResult<AdbcCliConfig> {
    let driver = option_value_any(options, &["--driver"])
        .or_else(|| env_non_empty("SIDEMANTIC_ADBC_DRIVER"))
        .or_else(|| env_non_empty("SIDEMANTIC_MCP_ADBC_DRIVER"))
        .or(default_driver)
        .ok_or_else(|| {
            format!("{context}: missing ADBC driver. Set --driver or SIDEMANTIC_ADBC_DRIVER.")
        })?;
    let uri = option_value_any(options, &["--uri", "--db-uri"])
        .or_else(|| env_non_empty("SIDEMANTIC_ADBC_URI"))
        .or_else(|| env_non_empty("SIDEMANTIC_MCP_ADBC_URI"))
        .or(default_uri);
    let entrypoint = option_value(options, "--entrypoint")
        .or_else(|| env_non_empty("SIDEMANTIC_ADBC_ENTRYPOINT"))
        .or_else(|| env_non_empty("SIDEMANTIC_MCP_ADBC_ENTRYPOINT"));

    let mut database_options = std::mem::take(&mut default_database_options);
    database_options.extend(parse_database_options(&option_values(options, "--dbopt"))?);
    let mut connection_options = parse_connection_options(&option_values(options, "--connopt"))?;

    if let Some(env_opts) = env_non_empty("SIDEMANTIC_ADBC_DBOPTS") {
        database_options.extend(parse_database_options(&[env_opts])?);
    }
    if let Some(env_opts) = env_non_empty("SIDEMANTIC_ADBC_CONNOPTS") {
        connection_options.extend(parse_connection_options(&[env_opts])?);
    }

    let username = option_value_any(options, &["--username", "--db-username"])
        .or_else(|| env_non_empty("SIDEMANTIC_ADBC_USERNAME"));
    let password = option_value_any(options, &["--password", "--db-password"])
        .or_else(|| env_non_empty("SIDEMANTIC_ADBC_PASSWORD"));

    if let Some(username) = username {
        database_options.push((OptionDatabase::Username, OptionValue::String(username)));
    }
    if let Some(password) = password {
        database_options.push((OptionDatabase::Password, OptionValue::String(password)));
    }

    if let Some(catalog) = option_value(options, "--catalog") {
        connection_options.push((
            OptionConnection::CurrentCatalog,
            OptionValue::String(catalog),
        ));
    }
    if let Some(schema) = option_value(options, "--schema") {
        connection_options.push((OptionConnection::CurrentSchema, OptionValue::String(schema)));
    }
    if let Some(raw) = option_value(options, "--autocommit") {
        let value = parse_bool_option(&raw, "--autocommit")?;
        connection_options.push((
            OptionConnection::AutoCommit,
            OptionValue::String(value.to_string()),
        ));
    }
    if let Some(raw) = option_value(options, "--read-only") {
        let value = parse_bool_option(&raw, "--read-only")?;
        connection_options.push((
            OptionConnection::ReadOnly,
            OptionValue::String(value.to_string()),
        ));
    }
    if let Some(level) = option_value(options, "--isolation-level") {
        connection_options.push((OptionConnection::IsolationLevel, OptionValue::String(level)));
    }

    Ok(AdbcCliConfig {
        driver: normalize_adbc_driver_name(&driver),
        uri,
        entrypoint,
        database_options,
        connection_options,
    })
}

#[cfg(feature = "adbc-exec")]
fn parse_adbc_cli_config(options: &ParsedOptions, context: &str) -> CliResult<AdbcCliConfig> {
    parse_adbc_cli_config_internal(options, context, None, None, Vec::new())
}

#[cfg(feature = "adbc-exec")]
fn parse_query_adbc_cli_config(options: &ParsedOptions, context: &str) -> CliResult<AdbcCliConfig> {
    let mut default_driver = None;
    let mut default_uri = None;
    let mut default_database_options = Vec::new();

    if let Some(connection_url) = option_value(options, "--connection") {
        let (driver, uri, db_options) = parse_connection_url_to_adbc(&connection_url)?;
        default_driver = Some(driver);
        default_uri = uri;
        default_database_options = db_options;
    } else if let Some(db_path) = option_value(options, "--db") {
        default_driver = Some(normalize_adbc_driver_name("duckdb"));
        default_uri = Some(db_path);
    }

    parse_adbc_cli_config_internal(
        options,
        context,
        default_driver,
        default_uri,
        default_database_options,
    )
}

#[cfg(feature = "adbc-exec")]
fn execute_sql_statements(adbc: &AdbcCliConfig, statements: &[String]) -> CliResult<()> {
    for statement in statements {
        let _ = execute_with_adbc(AdbcExecutionRequest {
            driver: adbc.driver.clone(),
            sql: statement.clone(),
            uri: adbc.uri.clone(),
            entrypoint: adbc.entrypoint.clone(),
            database_options: adbc.database_options.clone(),
            connection_options: adbc.connection_options.clone(),
        })
        .map_err(|e| format!("failed to execute statement via ADBC: {e}"))?;
    }
    Ok(())
}

#[cfg(feature = "adbc-exec")]
fn adbc_value_to_json(value: &AdbcValue) -> JsonValue {
    match value {
        AdbcValue::Null => JsonValue::Null,
        AdbcValue::Bool(v) => JsonValue::Bool(*v),
        AdbcValue::I64(v) => JsonValue::Number((*v).into()),
        AdbcValue::U64(v) => JsonValue::Number(serde_json::Number::from(*v)),
        AdbcValue::F64(v) => serde_json::Number::from_f64(*v)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        AdbcValue::String(v) => JsonValue::String(v.clone()),
        AdbcValue::Bytes(v) => JsonValue::Array(
            v.iter()
                .map(|byte| JsonValue::Number(serde_json::Number::from(*byte)))
                .collect(),
        ),
    }
}

#[cfg(feature = "adbc-exec")]
fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

#[cfg(feature = "adbc-exec")]
fn adbc_value_to_csv_field(value: &AdbcValue) -> String {
    match value {
        AdbcValue::Null => String::new(),
        AdbcValue::Bool(v) => v.to_string(),
        AdbcValue::I64(v) => v.to_string(),
        AdbcValue::U64(v) => v.to_string(),
        AdbcValue::F64(v) => v.to_string(),
        AdbcValue::String(v) => v.clone(),
        AdbcValue::Bytes(v) => v.iter().map(|byte| format!("{byte:02x}")).collect(),
    }
}

#[cfg(feature = "adbc-exec")]
fn write_csv_rows(
    columns: &[String],
    rows: &[Vec<AdbcValue>],
    output_path: Option<&str>,
) -> CliResult<()> {
    let mut csv = String::new();
    csv.push_str(
        &columns
            .iter()
            .map(|column| csv_escape(column))
            .collect::<Vec<_>>()
            .join(","),
    );
    csv.push('\n');

    for row in rows {
        let mut serialized_row = Vec::with_capacity(columns.len());
        for (index, _) in columns.iter().enumerate() {
            let raw = row
                .get(index)
                .map(adbc_value_to_csv_field)
                .unwrap_or_else(String::new);
            serialized_row.push(csv_escape(&raw));
        }
        csv.push_str(&serialized_row.join(","));
        csv.push('\n');
    }

    if let Some(path) = output_path {
        fs::write(path, csv).map_err(|e| format!("failed to write CSV output to '{path}': {e}"))?;
    } else {
        print!("{csv}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::split_queries;
    #[cfg(feature = "adbc-exec")]
    use super::{
        normalize_adbc_driver_name, parse_connection_url_to_adbc, render_duckdb_connection_url,
    };
    #[cfg(all(unix, feature = "adbc-exec"))]
    use std::path::Path;

    #[test]
    fn test_split_queries_counts_semicolon_separated_instrumented_queries() {
        let content = "\
SELECT revenue FROM orders
-- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status granularities=day;
SELECT revenue FROM orders
-- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status granularities=day;
";

        let queries = split_queries(content);

        assert_eq!(queries.len(), 2);
        assert!(queries.iter().all(|query| query.contains("-- sidemantic:")));
    }

    #[cfg(feature = "adbc-exec")]
    #[test]
    fn normalize_adbc_driver_name_keeps_explicit_library_paths() {
        assert_eq!(
            normalize_adbc_driver_name("duckdb"),
            "adbc_driver_duckdb".to_string()
        );
        assert_eq!(
            normalize_adbc_driver_name("adbc_driver_postgresql"),
            "adbc_driver_postgresql".to_string()
        );
        assert_eq!(
            normalize_adbc_driver_name("/tmp/_duckdb.so"),
            "/tmp/_duckdb.so".to_string()
        );
        assert_eq!(
            normalize_adbc_driver_name(".\\drivers\\duckdb.dll"),
            ".\\drivers\\duckdb.dll".to_string()
        );
    }

    #[cfg(all(unix, feature = "adbc-exec"))]
    #[test]
    fn duckdb_connection_url_preserves_absolute_path() {
        let path = Path::new("/tmp/sidemantic-workbench.duckdb");
        let url = render_duckdb_connection_url(path);
        assert_eq!(url, "duckdb:///tmp/sidemantic-workbench.duckdb");

        let (_, uri, _) = parse_connection_url_to_adbc(&url).unwrap();
        assert_eq!(uri.as_deref(), Some("/tmp/sidemantic-workbench.duckdb"));
    }
}
