//! Rust-native HTTP runtime server for Sidemantic.
//!
//! This server exposes semantic compile/execute operations over HTTP using axum.

use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;

#[cfg(feature = "runtime-server-adbc")]
use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use axum::body::{to_bytes, Body};
use axum::extract::{Path, Request, State};
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map as JsonMap, Value as JsonValue};
use sidemantic::runtime::interpolate_query_filters;
#[cfg(feature = "runtime-server-adbc")]
use sidemantic::{execute_with_adbc, AdbcExecutionRequest, AdbcValue};
use sidemantic::{Metric, Model, Relationship, RelationshipType, SemanticQuery, SidemanticRuntime};

#[cfg(feature = "runtime-server-adbc")]
type DatabaseOption = (OptionDatabase, OptionValue);
#[cfg(not(feature = "runtime-server-adbc"))]
type DatabaseOption = (String, String);
#[cfg(feature = "runtime-server-adbc")]
type ConnectionOption = (OptionConnection, OptionValue);
#[cfg(not(feature = "runtime-server-adbc"))]
type ConnectionOption = (String, String);

#[cfg_attr(not(feature = "runtime-server-adbc"), allow(dead_code))]
#[derive(Debug, Clone)]
struct AppState {
    runtime: Arc<SidemanticRuntime>,
    adbc_driver: Option<String>,
    adbc_uri: Option<String>,
    adbc_entrypoint: Option<String>,
    database_options: Vec<DatabaseOption>,
    connection_options: Vec<ConnectionOption>,
}

#[derive(Debug, Clone)]
struct HttpControls {
    auth_token: Option<String>,
    cors_origins: Vec<String>,
    max_request_body_bytes: usize,
}

#[derive(Debug, Default)]
struct ServerConfig {
    models_path: String,
    bind: String,
    auth_token: Option<String>,
    cors_origins: Vec<String>,
    max_request_body_bytes: usize,
    adbc_driver: Option<String>,
    adbc_uri: Option<String>,
    adbc_entrypoint: Option<String>,
    database_options: Vec<DatabaseOption>,
    connection_options: Vec<ConnectionOption>,
}

#[derive(Debug, Clone, Deserialize)]
struct QueryRequest {
    #[serde(default)]
    dimensions: Vec<String>,
    #[serde(default)]
    metrics: Vec<String>,
    #[serde(default, rename = "where")]
    where_clause: Option<String>,
    #[serde(default)]
    filters: Vec<String>,
    #[serde(default)]
    segments: Vec<String>,
    #[serde(default)]
    order_by: Vec<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    offset: Option<usize>,
    #[serde(default)]
    ungrouped: bool,
    #[serde(default)]
    use_preaggregations: bool,
    #[serde(default)]
    parameters: JsonMap<String, JsonValue>,
}

#[derive(Debug, Clone, Deserialize)]
struct SQLRequest {
    query: String,
}

#[derive(Debug, Clone, Deserialize)]
struct GetModelsRequest {
    #[serde(default)]
    model_names: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

fn json_error(status: StatusCode, message: impl Into<String>) -> (StatusCode, Json<ErrorResponse>) {
    (
        status,
        Json(ErrorResponse {
            error: message.into(),
        }),
    )
}

fn json_error_response(status: StatusCode, message: impl Into<String>) -> Response {
    (
        status,
        Json(ErrorResponse {
            error: message.into(),
        }),
    )
        .into_response()
}

fn cors_allowed_origin(controls: &HttpControls, request: &Request) -> Option<HeaderValue> {
    if controls.cors_origins.is_empty() {
        return None;
    }
    let origin = request.headers().get(header::ORIGIN)?.to_str().ok()?;
    if controls.cors_origins.iter().any(|allowed| allowed == "*") {
        return HeaderValue::from_str(origin).ok();
    }
    controls
        .cors_origins
        .iter()
        .any(|allowed| allowed == origin)
        .then(|| HeaderValue::from_str(origin).ok())
        .flatten()
}

fn apply_cors(mut response: Response, origin: Option<HeaderValue>) -> Response {
    if let Some(origin) = origin {
        let headers = response.headers_mut();
        headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
        headers.insert(
            header::ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_static("GET,POST,OPTIONS"),
        );
        headers.insert(
            header::ACCESS_CONTROL_ALLOW_HEADERS,
            HeaderValue::from_static("authorization,content-type"),
        );
        headers.insert(
            header::ACCESS_CONTROL_MAX_AGE,
            HeaderValue::from_static("600"),
        );
    }
    response
}

async fn http_controls_middleware(
    State(controls): State<Arc<HttpControls>>,
    mut request: Request,
    next: Next,
) -> Response {
    let origin = cors_allowed_origin(&controls, &request);
    if request.method() == Method::OPTIONS {
        return apply_cors(StatusCode::NO_CONTENT.into_response(), origin);
    }

    if request.uri().path() != "/readyz" {
        if let Some(expected) = &controls.auth_token {
            let authorized = request
                .headers()
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value == format!("Bearer {expected}"));
            if !authorized {
                let mut response = json_error_response(StatusCode::UNAUTHORIZED, "Unauthorized");
                response
                    .headers_mut()
                    .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
                return apply_cors(response, origin);
            }
        }
    }

    if matches!(
        request.method(),
        &Method::POST | &Method::PUT | &Method::PATCH
    ) {
        if let Some(content_length) = request
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<usize>().ok())
        {
            if content_length > controls.max_request_body_bytes {
                return apply_cors(
                    json_error_response(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        format!(
                            "Request body exceeds {} bytes",
                            controls.max_request_body_bytes
                        ),
                    ),
                    origin,
                );
            }
        }

        let (parts, body) = request.into_parts();
        let bytes = match to_bytes(body, controls.max_request_body_bytes + 1).await {
            Ok(bytes) if bytes.len() <= controls.max_request_body_bytes => bytes,
            _ => {
                return apply_cors(
                    json_error_response(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        format!(
                            "Request body exceeds {} bytes",
                            controls.max_request_body_bytes
                        ),
                    ),
                    origin,
                );
            }
        };
        request = Request::from_parts(parts, Body::from(bytes));
    }

    apply_cors(next.run(request).await, origin)
}

fn compile_request(runtime: &SidemanticRuntime, request: &QueryRequest) -> Result<String, String> {
    let mut filters = request.filters.clone();
    if let Some(where_clause) = &request.where_clause {
        if !where_clause.trim().is_empty() {
            filters.push(where_clause.clone());
        }
    }
    let parameter_values = request
        .parameters
        .iter()
        .map(|(key, value)| {
            serde_yaml::to_value(value)
                .map(|value| (key.clone(), value))
                .map_err(|e| format!("failed to parse query parameter '{key}': {e}"))
        })
        .collect::<Result<HashMap<_, _>, _>>()?;
    let filters = interpolate_query_filters(runtime.graph(), filters, &parameter_values)
        .map_err(|e| format!("failed to interpolate query parameters: {e}"))?;

    let mut query = SemanticQuery::new()
        .with_dimensions(request.dimensions.clone())
        .with_metrics(request.metrics.clone())
        .with_filters(filters)
        .with_segments(request.segments.clone())
        .with_ungrouped(request.ungrouped)
        .with_use_preaggregations(request.use_preaggregations);

    if !request.order_by.is_empty() {
        query = query.with_order_by(request.order_by.clone());
    }
    if let Some(limit) = request.limit {
        query = query.with_limit(limit);
    }
    if let Some(offset) = request.offset {
        query = query.with_offset(offset);
    }

    runtime
        .compile(&query)
        .map_err(|e| format!("failed to compile query: {e}"))
}

async fn readyz() -> Json<JsonValue> {
    Json(json!({ "status": "ok" }))
}

async fn health(State(state): State<Arc<AppState>>) -> Json<JsonValue> {
    let payload = state.runtime.loaded_graph_payload();
    Json(json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "dialect": "generic",
        "model_count": payload.models.len()
    }))
}

fn model_summary_json(model: &Model) -> JsonValue {
    let mut entry = JsonMap::new();
    entry.insert("name".to_string(), json!(model.name));
    entry.insert("table".to_string(), json!(model.table));
    entry.insert(
        "dimensions".to_string(),
        json!(model
            .dimensions
            .iter()
            .map(|dimension| dimension.name.clone())
            .collect::<Vec<_>>()),
    );
    entry.insert(
        "metrics".to_string(),
        json!(model
            .metrics
            .iter()
            .map(|metric| metric.name.clone())
            .collect::<Vec<_>>()),
    );
    entry.insert(
        "relationships".to_string(),
        json!(model.relationships.len()),
    );
    JsonValue::Object(entry)
}

fn graph_model_summary_json(model: &Model) -> JsonValue {
    let mut entry = match model_summary_json(model) {
        JsonValue::Object(entry) => entry,
        _ => JsonMap::new(),
    };
    entry.insert(
        "relationships".to_string(),
        JsonValue::Array(
            model
                .relationships
                .iter()
                .map(|relationship| {
                    json!({
                        "name": relationship.name,
                        "type": enum_json_name(&relationship.r#type)
                            .unwrap_or_else(|| "many_to_one".to_string())
                    })
                })
                .collect(),
        ),
    );
    if !model.segments.is_empty() {
        entry.insert(
            "segments".to_string(),
            json!(model
                .segments
                .iter()
                .map(|segment| segment.name.clone())
                .collect::<Vec<_>>()),
        );
    }
    if let Some(description) = &model.description {
        entry.insert("description".to_string(), json!(description));
    }
    if !model.primary_key.is_empty() {
        entry.insert("primary_key".to_string(), json!(model.primary_key));
    }
    if let Some(default_time_dimension) = &model.default_time_dimension {
        entry.insert(
            "default_time_dimension".to_string(),
            json!(default_time_dimension),
        );
    }
    JsonValue::Object(entry)
}

fn dimension_json(dimension: &sidemantic::Dimension) -> JsonValue {
    let mut entry = JsonMap::new();
    entry.insert("name".to_string(), json!(dimension.name));
    entry.insert(
        "type".to_string(),
        json!(enum_json_name(&dimension.r#type).unwrap_or_else(|| "categorical".to_string())),
    );
    entry.insert("sql".to_string(), json!(dimension.sql));
    if let Some(description) = &dimension.description {
        entry.insert("description".to_string(), json!(description));
    }
    if let Some(label) = &dimension.label {
        entry.insert("label".to_string(), json!(label));
    }
    if let Some(granularity) = &dimension.granularity {
        entry.insert("granularity".to_string(), json!(granularity));
    }
    if let Some(supported_granularities) = &dimension.supported_granularities {
        entry.insert(
            "supported_granularities".to_string(),
            json!(supported_granularities),
        );
    }
    if let Some(format) = &dimension.format {
        entry.insert("format".to_string(), json!(format));
    }
    if let Some(value_format_name) = &dimension.value_format_name {
        entry.insert("value_format_name".to_string(), json!(value_format_name));
    }
    if let Some(parent) = &dimension.parent {
        entry.insert("parent".to_string(), json!(parent));
    }
    JsonValue::Object(entry)
}

fn segment_json(segment: &sidemantic::Segment) -> JsonValue {
    let mut entry = JsonMap::new();
    entry.insert("name".to_string(), json!(segment.name));
    entry.insert("sql".to_string(), json!(segment.sql));
    if let Some(description) = &segment.description {
        entry.insert("description".to_string(), json!(description));
    }
    if !segment.public {
        entry.insert("public".to_string(), json!(false));
    }
    JsonValue::Object(entry)
}

fn model_detail_json(model: &Model, model_map: &HashMap<&str, &Model>) -> JsonValue {
    let mut entry = JsonMap::new();
    entry.insert("name".to_string(), json!(model.name));
    entry.insert("table".to_string(), json!(model.table));
    entry.insert("primary_key".to_string(), json!(model.primary_key));
    entry.insert(
        "dimensions".to_string(),
        JsonValue::Array(model.dimensions.iter().map(dimension_json).collect()),
    );
    entry.insert(
        "metrics".to_string(),
        JsonValue::Array(model.metrics.iter().map(metric_json).collect()),
    );
    entry.insert(
        "relationships".to_string(),
        JsonValue::Array(
            model
                .relationships
                .iter()
                .map(|relationship| relationship_json(model, relationship, model_map))
                .collect(),
        ),
    );
    if !model.segments.is_empty() {
        entry.insert(
            "segments".to_string(),
            JsonValue::Array(model.segments.iter().map(segment_json).collect()),
        );
    }
    if let Some(description) = &model.description {
        entry.insert("description".to_string(), json!(description));
    }
    if let Some(sql) = &model.sql {
        entry.insert("sql".to_string(), json!(sql));
    }
    if let Some(default_time_dimension) = &model.default_time_dimension {
        entry.insert(
            "default_time_dimension".to_string(),
            json!(default_time_dimension),
        );
    }
    if let Some(default_grain) = &model.default_grain {
        entry.insert("default_grain".to_string(), json!(default_grain));
    }
    JsonValue::Object(entry)
}

async fn list_models(State(state): State<Arc<AppState>>) -> Json<JsonValue> {
    let payload = state.runtime.loaded_graph_payload();
    let models = payload.models.iter().map(model_summary_json).collect();
    Json(JsonValue::Array(models))
}

async fn get_model(
    State(state): State<Arc<AppState>>,
    Path(model_name): Path<String>,
) -> Result<Json<JsonValue>, (StatusCode, Json<ErrorResponse>)> {
    let payload = state.runtime.loaded_graph_payload();
    let model_map: HashMap<&str, &Model> = payload
        .models
        .iter()
        .map(|model| (model.name.as_str(), model))
        .collect();
    let Some(model) = model_map.get(model_name.as_str()) else {
        return Err(json_error(
            StatusCode::NOT_FOUND,
            format!("model not found: {model_name}"),
        ));
    };

    Ok(Json(model_detail_json(model, &model_map)))
}

async fn get_models(
    State(state): State<Arc<AppState>>,
    Json(request): Json<GetModelsRequest>,
) -> Json<JsonValue> {
    let payload = state.runtime.loaded_graph_payload();
    let model_map: HashMap<&str, &Model> = payload
        .models
        .iter()
        .map(|model| (model.name.as_str(), model))
        .collect();

    let mut details = Vec::new();
    for model_name in &request.model_names {
        let Some(model) = model_map.get(model_name.as_str()) else {
            continue;
        };

        details.push(model_detail_json(model, &model_map));
    }

    Json(JsonValue::Array(details))
}

fn graph_payload(runtime: &SidemanticRuntime) -> JsonValue {
    let payload = runtime.loaded_graph_payload();
    let models = payload
        .models
        .iter()
        .map(graph_model_summary_json)
        .collect::<Vec<_>>();
    let graph_metrics = payload
        .top_level_metrics
        .iter()
        .map(metric_json)
        .collect::<Vec<_>>();

    let model_names = payload
        .models
        .iter()
        .map(|model| model.name.clone())
        .collect::<Vec<_>>();
    let mut joinable_pairs = Vec::new();
    for (idx, left_name) in model_names.iter().enumerate() {
        for right_name in model_names.iter().skip(idx + 1) {
            if let Ok(path) = runtime.find_join_path(left_name, right_name) {
                joinable_pairs.push(json!({
                    "from": left_name,
                    "to": right_name,
                    "hops": path.steps.len()
                }));
            }
        }
    }

    let mut result = JsonMap::new();
    result.insert("models".to_string(), JsonValue::Array(models));
    result.insert(
        "joinable_pairs".to_string(),
        JsonValue::Array(joinable_pairs),
    );
    if !graph_metrics.is_empty() {
        result.insert("graph_metrics".to_string(), JsonValue::Array(graph_metrics));
    }
    JsonValue::Object(result)
}

async fn graph(State(state): State<Arc<AppState>>) -> Json<JsonValue> {
    Json(graph_payload(&state.runtime))
}

async fn compile_query(
    State(state): State<Arc<AppState>>,
    Json(request): Json<QueryRequest>,
) -> Result<Json<JsonValue>, (StatusCode, Json<ErrorResponse>)> {
    let sql = compile_request(&state.runtime, &request)
        .map_err(|message| json_error(StatusCode::BAD_REQUEST, message))?;
    Ok(Json(json!({ "sql": sql })))
}

async fn run_query(
    State(state): State<Arc<AppState>>,
    Json(request): Json<QueryRequest>,
) -> Result<Json<JsonValue>, (StatusCode, Json<ErrorResponse>)> {
    let sql = compile_request(&state.runtime, &request)
        .map_err(|message| json_error(StatusCode::BAD_REQUEST, message))?;

    #[cfg(not(feature = "runtime-server-adbc"))]
    {
        let _ = sql;
        return Err(json_error(
            StatusCode::BAD_REQUEST,
            "ADBC execution support is not enabled. Rebuild with feature 'runtime-server-adbc' to use /query/run.",
        ));
    }

    #[cfg(feature = "runtime-server-adbc")]
    {
        let Some(driver) = state.adbc_driver.clone() else {
            return Err(json_error(
            StatusCode::BAD_REQUEST,
            "ADBC driver is not configured. Set SIDEMANTIC_SERVER_ADBC_DRIVER or pass --driver.",
        ));
        };

        let result = execute_with_adbc(AdbcExecutionRequest {
            driver,
            sql: sql.clone(),
            uri: state.adbc_uri.clone(),
            entrypoint: state.adbc_entrypoint.clone(),
            database_options: state.database_options.clone(),
            connection_options: state.connection_options.clone(),
        })
        .map_err(|e| {
            json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to execute query via ADBC: {e}"),
            )
        })?;

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

        Ok(Json(json!({
            "sql": sql,
            "rows": rows,
            "row_count": rows.len()
        })))
    }
}

async fn compile_sql(
    State(state): State<Arc<AppState>>,
    Json(request): Json<SQLRequest>,
) -> Result<Json<JsonValue>, (StatusCode, Json<ErrorResponse>)> {
    let query = normalize_sql(&request.query)
        .map_err(|message| json_error(StatusCode::BAD_REQUEST, message))?;
    let sql = state.runtime.rewrite(&query).map_err(|e| {
        json_error(
            StatusCode::BAD_REQUEST,
            format!("failed to rewrite SQL: {e}"),
        )
    })?;
    Ok(Json(json!({ "sql": sql })))
}

async fn run_sql(
    State(state): State<Arc<AppState>>,
    Json(request): Json<SQLRequest>,
) -> Result<Json<JsonValue>, (StatusCode, Json<ErrorResponse>)> {
    let original_sql = normalize_sql(&request.query)
        .map_err(|message| json_error(StatusCode::BAD_REQUEST, message))?;
    let sql = state.runtime.rewrite(&original_sql).map_err(|e| {
        json_error(
            StatusCode::BAD_REQUEST,
            format!("failed to rewrite SQL: {e}"),
        )
    })?;
    execute_sql_json(&state, sql, Some(original_sql), "/sql")
}

async fn run_raw_sql(
    State(state): State<Arc<AppState>>,
    Json(request): Json<SQLRequest>,
) -> Result<Json<JsonValue>, (StatusCode, Json<ErrorResponse>)> {
    let sql = normalize_sql(&request.query)
        .map_err(|message| json_error(StatusCode::BAD_REQUEST, message))?;
    require_select_only_sql(&sql)
        .map_err(|message| json_error(StatusCode::BAD_REQUEST, message))?;
    execute_sql_json(&state, sql, None, "/raw")
}

fn execute_sql_json(
    state: &AppState,
    sql: String,
    original_sql: Option<String>,
    route_name: &str,
) -> Result<Json<JsonValue>, (StatusCode, Json<ErrorResponse>)> {
    let _ = route_name;
    #[cfg(not(feature = "runtime-server-adbc"))]
    {
        let _ = (state, sql, original_sql);
        return Err(json_error(
            StatusCode::BAD_REQUEST,
            format!(
                "ADBC execution support is not enabled. Rebuild with feature 'runtime-server-adbc' to use {route_name}."
            ),
        ));
    }

    #[cfg(feature = "runtime-server-adbc")]
    {
        let Some(driver) = state.adbc_driver.clone() else {
            return Err(json_error(
                StatusCode::BAD_REQUEST,
                "ADBC driver is not configured. Set SIDEMANTIC_SERVER_ADBC_DRIVER or pass --driver.",
            ));
        };

        let result = execute_with_adbc(AdbcExecutionRequest {
            driver,
            sql: sql.clone(),
            uri: state.adbc_uri.clone(),
            entrypoint: state.adbc_entrypoint.clone(),
            database_options: state.database_options.clone(),
            connection_options: state.connection_options.clone(),
        })
        .map_err(|e| {
            json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to execute query via ADBC: {e}"),
            )
        })?;

        let rows = adbc_rows_to_json_rows(&result.columns, &result.rows);
        let mut response = JsonMap::new();
        response.insert("sql".to_string(), json!(sql));
        if let Some(original_sql) = original_sql {
            response.insert("original_sql".to_string(), json!(original_sql));
        }
        response.insert("row_count".to_string(), json!(rows.len()));
        response.insert("rows".to_string(), JsonValue::Array(rows));
        Ok(Json(JsonValue::Object(response)))
    }
}

#[cfg(feature = "runtime-server-adbc")]
fn adbc_rows_to_json_rows(columns: &[String], rows: &[Vec<AdbcValue>]) -> Vec<JsonValue> {
    rows.iter()
        .map(|row| {
            let mut row_map = JsonMap::new();
            for (idx, column) in columns.iter().enumerate() {
                let value = row
                    .get(idx)
                    .map(adbc_value_to_json)
                    .unwrap_or(JsonValue::Null);
                row_map.insert(column.clone(), value);
            }
            JsonValue::Object(row_map)
        })
        .collect()
}

fn normalize_sql(sql: &str) -> Result<String, String> {
    let mut normalized = sql.trim().to_string();
    if normalized.is_empty() {
        return Err("SQL query cannot be empty".to_string());
    }
    while normalized.ends_with(';') {
        normalized.pop();
        normalized = normalized.trim_end().to_string();
    }
    if has_unquoted_semicolon(&normalized) {
        return Err("Only one SQL statement is allowed".to_string());
    }
    Ok(normalized)
}

fn has_unquoted_semicolon(sql: &str) -> bool {
    let mut in_single = false;
    let mut in_double = false;
    let mut prev = '\0';
    for ch in sql.chars() {
        match ch {
            '\'' if !in_double && prev != '\\' => in_single = !in_single,
            '"' if !in_single && prev != '\\' => in_double = !in_double,
            ';' if !in_single && !in_double => return true,
            _ => {}
        }
        prev = ch;
    }
    false
}

fn require_select_only_sql(sql: &str) -> Result<(), String> {
    let scrubbed = scrub_quoted_sql(sql);
    let lower = scrubbed.to_ascii_lowercase();
    let first_word = lower
        .split_whitespace()
        .next()
        .ok_or_else(|| "SQL query cannot be empty".to_string())?;
    if first_word != "select" && first_word != "with" {
        return Err("Raw SQL execution only supports SELECT statements".to_string());
    }
    for banned in [
        "insert", "update", "delete", "drop", "create", "alter", "truncate", "merge", "copy",
        "call", "grant", "revoke",
    ] {
        if lower
            .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
            .any(|token| token == banned)
        {
            return Err(format!(
                "Raw SQL execution only supports SELECT statements; found {banned}"
            ));
        }
    }
    Ok(())
}

fn scrub_quoted_sql(sql: &str) -> String {
    let mut scrubbed = String::with_capacity(sql.len());
    let mut in_single = false;
    let mut in_double = false;
    let mut prev = '\0';
    for ch in sql.chars() {
        match ch {
            '\'' if !in_double && prev != '\\' => {
                in_single = !in_single;
                scrubbed.push(' ');
            }
            '"' if !in_single && prev != '\\' => {
                in_double = !in_double;
                scrubbed.push(' ');
            }
            _ if in_single || in_double => scrubbed.push(' '),
            _ => scrubbed.push(ch),
        }
        prev = ch;
    }
    scrubbed
}

fn parse_config() -> Result<ServerConfig, String> {
    let mut models_path: Option<String> = None;
    let mut bind = env::var("SIDEMANTIC_SERVER_BIND")
        .ok()
        .unwrap_or_else(|| "127.0.0.1:4543".to_string());
    let mut auth_token = env::var("SIDEMANTIC_SERVER_AUTH_TOKEN").ok();
    let mut cors_origins = env::var("SIDEMANTIC_SERVER_CORS_ORIGINS")
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut max_request_body_bytes = env::var("SIDEMANTIC_SERVER_MAX_REQUEST_BODY_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1024 * 1024);
    let mut adbc_driver = env::var("SIDEMANTIC_SERVER_ADBC_DRIVER")
        .ok()
        .or_else(|| env::var("SIDEMANTIC_MCP_ADBC_DRIVER").ok());
    let mut adbc_uri = env::var("SIDEMANTIC_SERVER_ADBC_URI")
        .ok()
        .or_else(|| env::var("SIDEMANTIC_MCP_ADBC_URI").ok());
    let mut adbc_entrypoint = env::var("SIDEMANTIC_SERVER_ADBC_ENTRYPOINT")
        .ok()
        .or_else(|| env::var("SIDEMANTIC_MCP_ADBC_ENTRYPOINT").ok());
    let mut database_options: Vec<DatabaseOption> = Vec::new();
    let mut connection_options: Vec<ConnectionOption> = Vec::new();
    if let Ok(env_dbopts) =
        env::var("SIDEMANTIC_SERVER_ADBC_DBOPTS").or_else(|_| env::var("SIDEMANTIC_ADBC_DBOPTS"))
    {
        database_options.extend(parse_database_options(&env_dbopts)?);
    }
    if let Ok(env_connopts) = env::var("SIDEMANTIC_SERVER_ADBC_CONNOPTS")
        .or_else(|_| env::var("SIDEMANTIC_ADBC_CONNOPTS"))
    {
        connection_options.extend(parse_connection_options(&env_connopts)?);
    }

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--models" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--models requires a path value".to_string())?;
                models_path = Some(value);
            }
            "--bind" => {
                bind = args
                    .next()
                    .ok_or_else(|| "--bind requires a value".to_string())?;
            }
            "--auth-token" => {
                auth_token = Some(
                    args.next()
                        .ok_or_else(|| "--auth-token requires a value".to_string())?,
                );
            }
            "--cors-origin" => {
                cors_origins.push(
                    args.next()
                        .ok_or_else(|| "--cors-origin requires a value".to_string())?,
                );
            }
            "--max-request-body-bytes" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--max-request-body-bytes requires a value".to_string())?;
                max_request_body_bytes = value
                    .parse::<usize>()
                    .map_err(|_| "--max-request-body-bytes must be an integer".to_string())?;
            }
            "--driver" => {
                adbc_driver = Some(
                    args.next()
                        .ok_or_else(|| "--driver requires a value".to_string())?,
                );
            }
            "--uri" => {
                adbc_uri = Some(
                    args.next()
                        .ok_or_else(|| "--uri requires a value".to_string())?,
                );
            }
            "--entrypoint" => {
                adbc_entrypoint = Some(
                    args.next()
                        .ok_or_else(|| "--entrypoint requires a value".to_string())?,
                );
            }
            "--dbopt" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--dbopt requires a value".to_string())?;
                database_options.extend(parse_database_options(&value)?);
            }
            "--connopt" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--connopt requires a value".to_string())?;
                connection_options.extend(parse_connection_options(&value)?);
            }
            "--help" | "-h" => {
                return Err(
                    "Usage: sidemantic-server [--models <path>] [--bind <host:port>] [--auth-token <token>] [--cors-origin <origin>] [--max-request-body-bytes <bytes>] [--driver <adbc_driver>] [--uri <adbc_uri>] [--entrypoint <driver_entrypoint>] [--dbopt <k=v[,k=v...]>] [--connopt <k=v[,k=v...]>]".to_string()
                );
            }
            unknown => {
                return Err(format!(
                    "unknown argument: {unknown}. Use --help for usage."
                ));
            }
        }
    }

    let models_path = models_path
        .or_else(|| env::var("SIDEMANTIC_SERVER_MODELS").ok())
        .unwrap_or_else(|| ".".to_string());

    Ok(ServerConfig {
        models_path,
        bind,
        auth_token,
        cors_origins,
        max_request_body_bytes,
        adbc_driver,
        adbc_uri,
        adbc_entrypoint,
        database_options,
        connection_options,
    })
}

fn parse_kv_pairs(input: &str, option_name: &str) -> Result<Vec<(String, String)>, String> {
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

#[cfg(feature = "runtime-server-adbc")]
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

#[cfg(not(feature = "runtime-server-adbc"))]
fn parse_option_value(value: &str) -> String {
    value.to_string()
}

fn parse_database_options(input: &str) -> Result<Vec<DatabaseOption>, String> {
    let mut parsed = Vec::new();
    for (key, raw_value) in parse_kv_pairs(input, "--dbopt")? {
        #[cfg(feature = "runtime-server-adbc")]
        parsed.push((
            OptionDatabase::from(key.as_str()),
            parse_option_value(&raw_value),
        ));
        #[cfg(not(feature = "runtime-server-adbc"))]
        parsed.push((key, parse_option_value(&raw_value)));
    }
    Ok(parsed)
}

fn parse_connection_options(input: &str) -> Result<Vec<ConnectionOption>, String> {
    let mut parsed = Vec::new();
    for (key, raw_value) in parse_kv_pairs(input, "--connopt")? {
        #[cfg(feature = "runtime-server-adbc")]
        parsed.push((
            OptionConnection::from(key.as_str()),
            parse_option_value(&raw_value),
        ));
        #[cfg(not(feature = "runtime-server-adbc"))]
        parsed.push((key, parse_option_value(&raw_value)));
    }
    Ok(parsed)
}

fn load_runtime(models_path: &str) -> Result<SidemanticRuntime, String> {
    let path = PathBuf::from(models_path);
    if path.is_dir() {
        return SidemanticRuntime::from_directory(path)
            .map_err(|e| format!("failed to load models from directory '{models_path}': {e}"));
    }
    if path.is_file() {
        return SidemanticRuntime::from_file(path)
            .map_err(|e| format!("failed to load models from file '{models_path}': {e}"));
    }
    Err(format!(
        "models path '{models_path}' is not a readable file or directory"
    ))
}

fn enum_json_name<T: serde::Serialize>(value: &T) -> Option<String> {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(ToString::to_string))
}

fn metric_json(metric: &Metric) -> JsonValue {
    let mut entry = JsonMap::new();
    entry.insert("name".to_string(), json!(metric.name));
    entry.insert("sql".to_string(), json!(metric.sql));
    if let Some(agg) = &metric.agg {
        if let Some(name) = enum_json_name(agg) {
            entry.insert("agg".to_string(), json!(name));
        }
    }
    if let Some(name) = enum_json_name(&metric.r#type) {
        entry.insert("type".to_string(), json!(name));
    }
    if let Some(description) = &metric.description {
        entry.insert("description".to_string(), json!(description));
    }
    if !metric.filters.is_empty() {
        entry.insert("filters".to_string(), json!(metric.filters));
    }
    JsonValue::Object(entry)
}

fn relationship_json(
    model: &Model,
    relationship: &Relationship,
    model_map: &HashMap<&str, &Model>,
) -> JsonValue {
    let mut entry = JsonMap::new();
    entry.insert("name".to_string(), json!(relationship.name));
    entry.insert(
        "type".to_string(),
        json!(enum_json_name(&relationship.r#type).unwrap_or_else(|| "many_to_one".to_string())),
    );
    if let Some(foreign_key) = &relationship.foreign_key {
        entry.insert("foreign_key".to_string(), json!(foreign_key));
    }
    if let Some(primary_key) = &relationship.primary_key {
        entry.insert("primary_key".to_string(), json!(primary_key));
    }
    if let Some(through) = &relationship.through {
        entry.insert("through".to_string(), json!(through));
    }
    if let Some(through_fk) = &relationship.through_foreign_key {
        entry.insert("through_foreign_key".to_string(), json!(through_fk));
    }
    if let Some(related_fk) = &relationship.related_foreign_key {
        entry.insert("related_foreign_key".to_string(), json!(related_fk));
    }

    if let Some(join_condition) = format_join_condition(model, relationship, model_map) {
        entry.insert("join_condition".to_string(), json!(join_condition));
    }

    JsonValue::Object(entry)
}

fn format_join_condition(
    model: &Model,
    relationship: &Relationship,
    model_map: &HashMap<&str, &Model>,
) -> Option<String> {
    let related_model = model_map.get(relationship.name.as_str())?;
    let related_name = relationship.name.as_str();
    let model_name = model.name.as_str();

    match relationship.r#type {
        RelationshipType::ManyToOne => {
            let fk = relationship
                .foreign_key
                .clone()
                .unwrap_or_else(|| format!("{related_name}_id"));
            let pk = relationship
                .primary_key
                .clone()
                .unwrap_or_else(|| related_model.primary_key.clone());
            Some(format!("{model_name}.{fk} = {related_name}.{pk}"))
        }
        RelationshipType::OneToMany | RelationshipType::OneToOne => {
            let fk = relationship.foreign_key.clone()?;
            let pk = model.primary_key.clone();
            Some(format!("{related_name}.{fk} = {model_name}.{pk}"))
        }
        RelationshipType::ManyToMany => {
            if let Some(through) = &relationship.through {
                let _junction_model = model_map.get(through.as_str())?;
                let (junction_self_fk, junction_related_fk) = relationship.junction_keys();
                let junction_self_fk = junction_self_fk?;
                let junction_related_fk = junction_related_fk?;
                let base_pk = model.primary_key.clone();
                let related_pk = relationship
                    .primary_key
                    .clone()
                    .unwrap_or_else(|| related_model.primary_key.clone());
                return Some(format!(
                    "{model_name}.{base_pk} = {through}.{junction_self_fk} AND {through}.{junction_related_fk} = {related_name}.{related_pk}"
                ));
            }

            relationship.foreign_key.clone().map(|foreign_key| {
                format!(
                    "{model_name}.{} = {related_name}.{foreign_key}",
                    model.primary_key
                )
            })
        }
    }
}

#[cfg(feature = "runtime-server-adbc")]
fn adbc_value_to_json(value: &AdbcValue) -> JsonValue {
    match value {
        AdbcValue::Null => JsonValue::Null,
        AdbcValue::Bool(v) => json!(v),
        AdbcValue::I64(v) => json!(v),
        AdbcValue::U64(v) => json!(v),
        AdbcValue::F64(v) => json!(v),
        AdbcValue::String(v) => json!(v),
        AdbcValue::Bytes(v) => json!(v),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = parse_config()?;
    let runtime = load_runtime(&config.models_path)?;

    let state = Arc::new(AppState {
        runtime: Arc::new(runtime),
        adbc_driver: config.adbc_driver,
        adbc_uri: config.adbc_uri,
        adbc_entrypoint: config.adbc_entrypoint,
        database_options: config.database_options,
        connection_options: config.connection_options,
    });
    let controls = Arc::new(HttpControls {
        auth_token: config.auth_token,
        cors_origins: config.cors_origins,
        max_request_body_bytes: config.max_request_body_bytes,
    });

    let app = Router::new()
        .route("/readyz", get(readyz))
        .route("/health", get(health))
        .route("/graph", get(graph))
        .route("/models", get(list_models).post(get_models))
        .route("/models/{model}", get(get_model))
        .route("/compile", post(compile_query))
        .route("/query", post(run_query))
        .route("/query/compile", post(compile_query))
        .route("/query/run", post(run_query))
        .route("/sql/compile", post(compile_sql))
        .route("/sql", post(run_sql))
        .route("/raw", post(run_raw_sql))
        .with_state(state)
        .layer(middleware::from_fn_with_state(
            controls,
            http_controls_middleware,
        ));

    eprintln!("sidemantic-server listening on {}", config.bind);
    let listener = tokio::net::TcpListener::bind(&config.bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
