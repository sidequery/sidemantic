//! Sidemantic MCP server implemented with the rmcp Rust SDK.
//!
//! This binary provides Rust-native MCP tools for semantic model introspection
//! and query compilation/execution.

use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;

#[cfg(feature = "mcp-adbc")]
use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{
        Annotated, CallToolResult, ListResourcesResult, PaginatedRequestParams, RawResource,
        ReadResourceRequestParams, ReadResourceResult, ResourceContents, ServerCapabilities,
        ServerInfo,
    },
    schemars,
    schemars::JsonSchema,
    service::{RequestContext, RoleServer},
    tool, tool_handler, tool_router,
    transport::stdio,
    ErrorData as McpError, ServerHandler, ServiceExt,
};
use serde::Deserialize;
use serde_json::{json, Map as JsonMap, Value as JsonValue};
#[cfg(feature = "mcp-adbc")]
use sidemantic::{
    chart_auto_detect_columns, chart_encoding_type, chart_format_label, chart_select_type,
};
#[cfg(feature = "mcp-adbc")]
use sidemantic::{execute_with_adbc, AdbcExecutionRequest, AdbcExecutionResult, AdbcValue};
use sidemantic::{Metric, Model, Relationship, RelationshipType, SemanticQuery, SidemanticRuntime};

const CATALOG_RESOURCE_URI: &str = "semantic://catalog";
#[cfg(feature = "mcp-adbc")]
const CHART_COLORS: [&str; 8] = [
    "#2E5EAA", "#E8702A", "#4C9A2A", "#9B59B6", "#1ABC9C", "#E74C3C", "#F39C12", "#34495E",
];

#[cfg(feature = "mcp-adbc")]
type DatabaseOption = (OptionDatabase, OptionValue);
#[cfg(not(feature = "mcp-adbc"))]
type DatabaseOption = (String, String);
#[cfg(feature = "mcp-adbc")]
type ConnectionOption = (OptionConnection, OptionValue);
#[cfg(not(feature = "mcp-adbc"))]
type ConnectionOption = (String, String);

#[cfg_attr(not(feature = "mcp-adbc"), allow(dead_code))]
#[derive(Debug, Clone)]
struct SidemanticMcpServer {
    runtime: Arc<SidemanticRuntime>,
    adbc_driver: Option<String>,
    adbc_uri: Option<String>,
    adbc_entrypoint: Option<String>,
    database_options: Vec<DatabaseOption>,
    connection_options: Vec<ConnectionOption>,
    tool_router: ToolRouter<Self>,
}

#[derive(Debug, Default)]
struct ServerConfig {
    models_path: String,
    adbc_driver: Option<String>,
    adbc_uri: Option<String>,
    adbc_entrypoint: Option<String>,
    database_options: Vec<DatabaseOption>,
    connection_options: Vec<ConnectionOption>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
struct GetModelsRequest {
    #[serde(default)]
    model_names: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
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
    dry_run: bool,
    #[serde(default)]
    use_preaggregations: bool,
}

#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
struct ValidateQueryRequest {
    #[serde(default)]
    dimensions: Vec<String>,
    #[serde(default)]
    metrics: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
struct SQLRequest {
    query: String,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
struct ChartRequest {
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
    #[serde(default = "default_chart_type")]
    chart_type: String,
    #[serde(default)]
    title: String,
    #[serde(default = "default_chart_width")]
    width: usize,
    #[serde(default = "default_chart_height")]
    height: usize,
}

fn default_chart_type() -> String {
    "auto".to_string()
}

fn default_chart_width() -> usize {
    600
}

fn default_chart_height() -> usize {
    400
}

impl SidemanticMcpServer {
    fn new(runtime: SidemanticRuntime, config: ServerConfig) -> Self {
        Self {
            runtime: Arc::new(runtime),
            adbc_driver: config.adbc_driver,
            adbc_uri: config.adbc_uri,
            adbc_entrypoint: config.adbc_entrypoint,
            database_options: config.database_options,
            connection_options: config.connection_options,
            tool_router: Self::tool_router(),
        }
    }

    fn compile_request(&self, request: &QueryRequest) -> Result<String, McpError> {
        let mut filters = request.filters.clone();
        if let Some(where_clause) = &request.where_clause {
            if !where_clause.trim().is_empty() {
                filters.push(where_clause.clone());
            }
        }

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

        self.runtime
            .compile(&query)
            .map_err(|e| McpError::invalid_params(format!("failed to compile query: {e}"), None))
    }

    fn execute_sql_tool_response(
        &self,
        sql: String,
        original_sql: Option<String>,
        tool_name: &str,
    ) -> Result<CallToolResult, McpError> {
        let _ = tool_name;
        #[cfg(not(feature = "mcp-adbc"))]
        {
            let _ = (sql, original_sql);
            return Err(McpError::invalid_params(
                format!(
                    "ADBC execution support is not enabled. Rebuild with feature 'mcp-adbc' to use {tool_name}."
                ),
                None,
            ));
        }

        #[cfg(feature = "mcp-adbc")]
        {
            let result = self.execute_sql_with_adbc(&sql)?;
            let rows = adbc_rows_to_json_rows(&result.columns, &result.rows);
            let mut response = JsonMap::new();
            response.insert("sql".to_string(), json!(sql));
            if let Some(original_sql) = original_sql {
                response.insert("original_sql".to_string(), json!(original_sql));
            }
            response.insert("rows".to_string(), JsonValue::Array(rows.clone()));
            response.insert("row_count".to_string(), json!(rows.len()));
            Ok(CallToolResult::structured(JsonValue::Object(response)))
        }
    }

    #[cfg(feature = "mcp-adbc")]
    fn execute_sql_with_adbc(&self, sql: &str) -> Result<AdbcExecutionResult, McpError> {
        let Some(driver) = self.adbc_driver.as_ref() else {
            return Err(McpError::invalid_params(
                "ADBC driver is not configured. Set SIDEMANTIC_MCP_ADBC_DRIVER or pass --driver."
                    .to_string(),
                None,
            ));
        };

        execute_with_adbc(AdbcExecutionRequest {
            driver: driver.clone(),
            sql: sql.to_string(),
            uri: self.adbc_uri.clone(),
            entrypoint: self.adbc_entrypoint.clone(),
            database_options: self.database_options.clone(),
            connection_options: self.connection_options.clone(),
        })
        .map_err(|e| {
            McpError::internal_error(format!("failed to execute query via ADBC: {e}"), None)
        })
    }
}

#[tool_router]
impl SidemanticMcpServer {
    #[tool(
        name = "list_models",
        description = "List all available semantic models with dimensions and metrics."
    )]
    async fn list_models(&self) -> Result<CallToolResult, McpError> {
        let payload = self.runtime.loaded_graph_payload();
        let models = payload
            .models
            .iter()
            .map(model_summary_json)
            .collect::<Vec<_>>();
        Ok(CallToolResult::structured(json!({ "models": models })))
    }

    #[tool(
        name = "get_models",
        description = "Get detailed metadata for one or more semantic models."
    )]
    async fn get_models(
        &self,
        Parameters(request): Parameters<GetModelsRequest>,
    ) -> Result<CallToolResult, McpError> {
        let payload = self.runtime.loaded_graph_payload();
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

        Ok(CallToolResult::structured(json!({ "models": details })))
    }

    #[tool(
        name = "get_semantic_graph",
        description = "Discover models, relationships, graph-level metrics, and joinable model pairs."
    )]
    async fn get_semantic_graph(&self) -> Result<CallToolResult, McpError> {
        Ok(CallToolResult::structured(graph_payload(&self.runtime)))
    }

    #[tool(
        name = "compile_query",
        description = "Compile semantic dimensions/metrics into SQL without execution."
    )]
    async fn compile_query(
        &self,
        Parameters(request): Parameters<QueryRequest>,
    ) -> Result<CallToolResult, McpError> {
        let sql = self.compile_request(&request)?;
        Ok(CallToolResult::structured(json!({ "sql": sql })))
    }

    #[tool(
        name = "validate_query",
        description = "Validate semantic dimensions and metrics without compiling or executing."
    )]
    async fn validate_query(
        &self,
        Parameters(request): Parameters<ValidateQueryRequest>,
    ) -> Result<CallToolResult, McpError> {
        let errors = self
            .runtime
            .validate_query_references(&request.metrics, &request.dimensions);
        Ok(CallToolResult::structured(json!({
            "valid": errors.is_empty(),
            "errors": errors
        })))
    }

    #[tool(
        name = "run_query",
        description = "Compile and execute a semantic query using ADBC driver manager."
    )]
    async fn run_query(
        &self,
        Parameters(request): Parameters<QueryRequest>,
    ) -> Result<CallToolResult, McpError> {
        let sql = self.compile_request(&request)?;
        if request.dry_run {
            return Ok(CallToolResult::structured(json!({ "sql": sql })));
        }

        #[cfg(not(feature = "mcp-adbc"))]
        {
            let _ = sql;
            return Err(McpError::invalid_params(
                "ADBC execution support is not enabled. Rebuild with feature 'mcp-adbc' to use run_query."
                    .to_string(),
                None,
            ));
        }

        #[cfg(feature = "mcp-adbc")]
        {
            let result = self.execute_sql_with_adbc(&sql)?;
            let rows = adbc_rows_to_json_rows(&result.columns, &result.rows);

            Ok(CallToolResult::structured(json!({
                "sql": sql,
                "rows": rows,
                "row_count": rows.len()
            })))
        }
    }

    #[tool(
        name = "run_sql",
        description = "Rewrite semantic SQL and execute it using ADBC driver manager."
    )]
    async fn run_sql(
        &self,
        Parameters(request): Parameters<SQLRequest>,
    ) -> Result<CallToolResult, McpError> {
        let original_sql = normalize_sql(&request.query)
            .map_err(|message| McpError::invalid_params(message, None))?;
        let sql = self
            .runtime
            .rewrite(&original_sql)
            .map_err(|e| McpError::invalid_params(format!("failed to rewrite SQL: {e}"), None))?;
        self.execute_sql_tool_response(sql, Some(original_sql), "run_sql")
    }

    #[tool(
        name = "create_chart",
        description = "Execute a semantic query and return a Vega-Lite chart spec plus a PNG preview."
    )]
    async fn create_chart(
        &self,
        Parameters(request): Parameters<ChartRequest>,
    ) -> Result<CallToolResult, McpError> {
        let query_request = QueryRequest {
            dimensions: request.dimensions.clone(),
            metrics: request.metrics.clone(),
            where_clause: request.where_clause.clone(),
            filters: request.filters.clone(),
            segments: request.segments.clone(),
            order_by: request.order_by.clone(),
            limit: request.limit,
            offset: request.offset,
            ungrouped: false,
            dry_run: false,
            use_preaggregations: false,
        };
        let sql = self.compile_request(&query_request)?;

        #[cfg(not(feature = "mcp-adbc"))]
        {
            let _ = (
                &request.chart_type,
                &request.title,
                request.width,
                request.height,
            );
            let _ = sql;
            return Err(McpError::invalid_params(
                "ADBC execution support is not enabled. Rebuild with feature 'mcp-adbc' to use create_chart."
                    .to_string(),
                None,
            ));
        }

        #[cfg(feature = "mcp-adbc")]
        {
            if request.width == 0 || request.height == 0 {
                return Err(McpError::invalid_params(
                    "chart width and height must be greater than zero".to_string(),
                    None,
                ));
            }

            let result = self.execute_sql_with_adbc(&sql)?;
            let rows = adbc_rows_to_json_rows(&result.columns, &result.rows);
            if rows.is_empty() {
                return Err(McpError::invalid_params(
                    "Query returned no data. Check filters or use run_query to inspect results first."
                        .to_string(),
                    None,
                ));
            }

            let title = if request.title.trim().is_empty() {
                generate_chart_title(&request.dimensions, &request.metrics)
            } else {
                request.title.clone()
            };
            let chart = build_chart_payload(
                rows,
                &result.columns,
                &request.chart_type,
                &title,
                request.width,
                request.height,
            )?;

            Ok(CallToolResult::structured(json!({
                "sql": sql,
                "vega_spec": chart.vega_spec,
                "png_base64": chart.png_base64,
                "row_count": chart.row_count
            })))
        }
    }
}

#[tool_handler]
impl ServerHandler for SidemanticMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            instructions: Some(
                "Rust-native Sidemantic MCP server. Tools: list_models, get_models, get_semantic_graph, validate_query, compile_query, run_query, run_sql, create_chart. Resource: semantic://catalog."
                    .to_string(),
            ),
            capabilities: ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .build(),
            ..Default::default()
        }
    }

    fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListResourcesResult, McpError>> + Send + '_ {
        std::future::ready(Ok(ListResourcesResult::with_all_items(vec![
            Annotated::new(
                RawResource {
                    uri: CATALOG_RESOURCE_URI.to_string(),
                    name: "catalog".to_string(),
                    title: Some("Sidemantic Catalog Metadata".to_string()),
                    description: Some(
                        "Postgres-compatible catalog metadata for the semantic layer.".to_string(),
                    ),
                    mime_type: Some("application/json".to_string()),
                    size: None,
                    icons: None,
                    meta: None,
                },
                None,
            ),
        ])))
    }

    fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ReadResourceResult, McpError>> + Send + '_ {
        let result = if request.uri == CATALOG_RESOURCE_URI {
            self.runtime
                .generate_catalog_metadata("public")
                .map_err(|e| {
                    McpError::internal_error(
                        format!("failed to generate catalog metadata: {e}"),
                        None,
                    )
                })
                .map(|catalog| ReadResourceResult {
                    contents: vec![ResourceContents::TextResourceContents {
                        uri: CATALOG_RESOURCE_URI.to_string(),
                        mime_type: Some("application/json".to_string()),
                        text: catalog,
                        meta: None,
                    }],
                })
        } else {
            Err(McpError::resource_not_found(
                format!("resource not found: {}", request.uri),
                None,
            ))
        };
        std::future::ready(result)
    }
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

#[cfg(feature = "mcp-adbc")]
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

#[cfg(feature = "mcp-adbc")]
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

#[cfg(feature = "mcp-adbc")]
struct ChartPayload {
    vega_spec: JsonValue,
    png_base64: String,
    row_count: usize,
}

#[cfg(feature = "mcp-adbc")]
fn build_chart_payload(
    rows: Vec<JsonValue>,
    columns: &[String],
    requested_type: &str,
    title: &str,
    width: usize,
    height: usize,
) -> Result<ChartPayload, McpError> {
    if columns.is_empty() {
        return Err(McpError::invalid_params(
            "chart query returned no columns".to_string(),
            None,
        ));
    }

    let numeric_flags = columns
        .iter()
        .skip(1)
        .map(|column| {
            rows.iter()
                .filter_map(JsonValue::as_object)
                .filter_map(|row| row.get(column))
                .any(is_json_number)
        })
        .collect::<Vec<_>>();
    let (x, y_cols) = chart_auto_detect_columns(columns, &numeric_flags)
        .map_err(|e| McpError::invalid_params(format!("failed to build chart: {e}"), None))?;
    let mut chart_rows = rows.clone();
    if x == "index" {
        for (idx, row) in chart_rows.iter_mut().enumerate() {
            if let Some(row) = row.as_object_mut() {
                row.insert("index".to_string(), json!(idx));
            }
        }
    }

    let x_value_kind = chart_rows
        .first()
        .and_then(JsonValue::as_object)
        .and_then(|row| row.get(&x))
        .map(json_value_kind)
        .unwrap_or("other");
    let chart_type = if requested_type == "auto" {
        chart_select_type(&x, x_value_kind, y_cols.len())
    } else {
        requested_type.to_string()
    };
    let allowed = ["bar", "line", "area", "scatter", "point"];
    if !allowed.contains(&chart_type.as_str()) {
        return Err(McpError::invalid_params(
            format!("Unsupported chart type: {chart_type}"),
            None,
        ));
    }

    let vega_spec = build_vega_spec(&chart_rows, &x, &y_cols, &chart_type, title, width, height);
    let png_base64 =
        render_chart_png_data_url(&chart_rows, &x, &y_cols, &chart_type, width, height);
    Ok(ChartPayload {
        vega_spec,
        png_base64,
        row_count: chart_rows.len(),
    })
}

#[cfg(feature = "mcp-adbc")]
fn build_vega_spec(
    rows: &[JsonValue],
    x: &str,
    y_cols: &[String],
    chart_type: &str,
    title: &str,
    width: usize,
    height: usize,
) -> JsonValue {
    let x_label = chart_format_label(x);
    let data = json!({ "values": rows });
    let config = json!({
        "font": "Inter, system-ui, -apple-system, sans-serif",
        "title": {
            "fontSize": 18,
            "fontWeight": 600,
            "anchor": "start",
            "color": "#1a1a1a",
            "offset": 20
        },
        "axis": {
            "labelFontSize": 12,
            "titleFontSize": 13,
            "titleFontWeight": 500,
            "titleColor": "#4a4a4a",
            "labelColor": "#6a6a6a",
            "gridColor": "#e8e8e8",
            "gridOpacity": 0.6,
            "domainColor": "#cccccc",
            "tickColor": "#cccccc",
            "titlePadding": 12,
            "labelPadding": 8
        },
        "legend": {
            "titleFontSize": 13,
            "titleFontWeight": 500,
            "labelFontSize": 12,
            "titleColor": "#4a4a4a",
            "labelColor": "#6a6a6a",
            "symbolSize": 100,
            "orient": "right",
            "offset": 10
        },
        "view": { "strokeWidth": 0 },
        "bar": { "cornerRadiusEnd": 2 },
        "line": { "strokeCap": "round" },
        "point": { "filled": true }
    });

    if chart_type == "scatter" {
        let y = y_cols.first().cloned().unwrap_or_else(|| x.to_string());
        return json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "data": data,
            "mark": { "type": "circle", "size": 80, "opacity": 0.7 },
            "encoding": {
                "x": { "field": x, "type": "quantitative", "title": x_label },
                "y": { "field": y, "type": "quantitative", "title": chart_format_label(&y) },
                "color": { "value": CHART_COLORS[0] },
                "tooltip": [
                    { "field": x, "type": "quantitative", "title": x_label, "format": ",.2f" },
                    { "field": y, "type": "quantitative", "title": chart_format_label(&y), "format": ",.2f" }
                ]
            },
            "config": config,
            "width": width,
            "height": height,
            "title": title
        });
    }

    let x_type = chart_encoding_type(x);
    if y_cols.len() > 1 {
        let mark = match chart_type {
            "line" => json!({ "type": "line", "point": true, "strokeWidth": 2.5 }),
            "area" => json!({ "type": "area", "opacity": 0.6, "line": true }),
            "point" => json!({ "type": "point", "size": 80, "filled": true }),
            _ => json!({ "type": "bar" }),
        };
        return json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "data": data,
            "transform": [{ "fold": y_cols, "as": ["metric", "value"] }],
            "mark": mark,
            "encoding": {
                "x": { "field": x, "type": x_type, "title": x_label },
                "y": { "field": "value", "type": "quantitative", "title": "Value" },
                "color": {
                    "field": "metric",
                    "type": "nominal",
                    "title": "Metric",
                    "scale": { "range": CHART_COLORS }
                },
                "tooltip": [
                    { "field": x, "type": x_type, "title": x_label },
                    { "field": "metric", "type": "nominal", "title": "Metric" },
                    { "field": "value", "type": "quantitative", "title": "Value", "format": ",.2f" }
                ]
            },
            "config": config,
            "width": width,
            "height": height,
            "title": title
        });
    }

    let y = y_cols.first().cloned().unwrap_or_else(|| x.to_string());
    let y_label = chart_format_label(&y);
    let mark = match chart_type {
        "line" => json!({ "type": "line", "point": true, "strokeWidth": 3 }),
        "area" => json!({ "type": "area", "opacity": 0.7, "line": true }),
        "point" => json!({ "type": "point", "size": 100, "filled": true }),
        _ => json!({ "type": "bar" }),
    };
    let mut encoding = JsonMap::new();
    encoding.insert(
        "x".to_string(),
        json!({ "field": x, "type": x_type, "title": x_label }),
    );
    encoding.insert(
        "y".to_string(),
        json!({ "field": y, "type": "quantitative", "title": y_label }),
    );
    encoding.insert(
        "tooltip".to_string(),
        json!([
            { "field": x, "type": x_type, "title": x_label },
            { "field": y, "type": "quantitative", "title": y_label, "format": ",.2f" }
        ]),
    );
    if chart_type == "bar" {
        encoding.insert(
            "color".to_string(),
            json!({
                "field": x,
                "type": x_type,
                "legend": null,
                "scale": { "range": CHART_COLORS }
            }),
        );
    }

    json!({
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "data": data,
        "mark": mark,
        "encoding": JsonValue::Object(encoding),
        "config": config,
        "width": width,
        "height": height,
        "title": title
    })
}

#[cfg(feature = "mcp-adbc")]
fn generate_chart_title(dimensions: &[String], metrics: &[String]) -> String {
    if metrics.is_empty() {
        return "Data Visualization".to_string();
    }

    let metric_names = metrics
        .iter()
        .map(|metric| format_chart_field_name(metric))
        .collect::<Vec<_>>();
    let mut title = if metric_names.len() == 1 {
        metric_names[0].clone()
    } else if metric_names.len() == 2 {
        format!("{} & {}", metric_names[0], metric_names[1])
    } else {
        format!("{} & {} more", metric_names[0], metric_names.len() - 1)
    };

    if let Some(dimension) = dimensions.first() {
        title = format!("{} by {}", title, format_chart_field_name(dimension));
    }
    title
}

#[cfg(feature = "mcp-adbc")]
fn format_chart_field_name(field: &str) -> String {
    let field = field.rsplit('.').next().unwrap_or(field);
    if let Some((base, granularity)) = field.rsplit_once("__") {
        return format!(
            "{} ({})",
            chart_format_label(base),
            chart_format_label(granularity)
        );
    }
    chart_format_label(field)
}

#[cfg(feature = "mcp-adbc")]
fn is_json_number(value: &JsonValue) -> bool {
    matches!(value, JsonValue::Number(_))
}

#[cfg(feature = "mcp-adbc")]
fn json_value_kind(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        _ => "other",
    }
}

#[cfg(feature = "mcp-adbc")]
fn json_number(value: &JsonValue) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|value| value as f64))
        .or_else(|| value.as_u64().map(|value| value as f64))
}

#[cfg(feature = "mcp-adbc")]
fn render_chart_png_data_url(
    rows: &[JsonValue],
    x: &str,
    y_cols: &[String],
    chart_type: &str,
    width: usize,
    height: usize,
) -> String {
    let width = width.clamp(1, 1200);
    let height = height.clamp(1, 900);
    let mut image = vec![255u8; width * height * 3];
    let left = 40usize.min(width.saturating_sub(1));
    let right = 10usize.min(width.saturating_sub(1));
    let top = 12usize.min(height.saturating_sub(1));
    let bottom = 28usize.min(height.saturating_sub(1));
    let plot_left = left;
    let plot_right = width.saturating_sub(right + 1).max(plot_left);
    let plot_top = top;
    let plot_bottom = height.saturating_sub(bottom + 1).max(plot_top);
    draw_line(
        &mut image,
        width,
        height,
        (plot_left as isize, plot_bottom as isize),
        (plot_right as isize, plot_bottom as isize),
        [204, 204, 204],
    );
    draw_line(
        &mut image,
        width,
        height,
        (plot_left as isize, plot_top as isize),
        (plot_left as isize, plot_bottom as isize),
        [204, 204, 204],
    );

    let y = y_cols.first().map(String::as_str).unwrap_or(x);
    let values = rows
        .iter()
        .filter_map(JsonValue::as_object)
        .filter_map(|row| row.get(y).and_then(json_number))
        .collect::<Vec<_>>();
    if values.is_empty() {
        let png = encode_rgb_png(width as u32, height as u32, &image);
        return format!("data:image/png;base64,{}", base64_encode(&png));
    }

    let min_value = values.iter().copied().fold(0.0_f64, f64::min);
    let mut max_value = values.iter().copied().fold(0.0_f64, f64::max);
    if (max_value - min_value).abs() < f64::EPSILON {
        max_value = min_value + 1.0;
    }
    let scale_y = |value: f64| -> isize {
        let span = max_value - min_value;
        let normalized = (value - min_value) / span;
        (plot_bottom as f64 - normalized * (plot_bottom.saturating_sub(plot_top) as f64)).round()
            as isize
    };
    let color = [46, 94, 170];

    if chart_type == "bar" {
        let slot =
            (plot_right.saturating_sub(plot_left).max(1) as f64 / values.len() as f64).max(1.0);
        let bar_width = (slot * 0.7).max(1.0) as usize;
        for (idx, value) in values.iter().enumerate() {
            let center = plot_left as f64 + slot * (idx as f64 + 0.5);
            let x0 = center.round() as isize - (bar_width as isize / 2);
            let x1 = x0 + bar_width as isize;
            let y0 = scale_y(*value);
            fill_rect(
                &mut image,
                width,
                height,
                (x0, y0.min(plot_bottom as isize)),
                (x1, plot_bottom as isize),
                color,
            );
        }
    } else {
        let denom = values.len().saturating_sub(1).max(1) as f64;
        let points = values
            .iter()
            .enumerate()
            .map(|(idx, value)| {
                let x = plot_left as f64
                    + (plot_right.saturating_sub(plot_left) as f64 * idx as f64 / denom);
                (x.round() as isize, scale_y(*value))
            })
            .collect::<Vec<_>>();

        for window in points.windows(2) {
            draw_line(&mut image, width, height, window[0], window[1], color);
        }
        for (x, y) in points {
            fill_circle(&mut image, width, height, x, y, 3, color);
        }
    }

    let png = encode_rgb_png(width as u32, height as u32, &image);
    format!("data:image/png;base64,{}", base64_encode(&png))
}

#[cfg(feature = "mcp-adbc")]
fn set_pixel(image: &mut [u8], width: usize, height: usize, x: isize, y: isize, color: [u8; 3]) {
    if x < 0 || y < 0 || x >= width as isize || y >= height as isize {
        return;
    }
    let idx = ((y as usize * width) + x as usize) * 3;
    image[idx..idx + 3].copy_from_slice(&color);
}

#[cfg(feature = "mcp-adbc")]
fn draw_line(
    image: &mut [u8],
    width: usize,
    height: usize,
    start: (isize, isize),
    end: (isize, isize),
    color: [u8; 3],
) {
    let (mut x0, mut y0) = start;
    let (x1, y1) = end;
    let dx = (x1 - x0).abs();
    let sx = if x0 < x1 { 1 } else { -1 };
    let dy = -(y1 - y0).abs();
    let sy = if y0 < y1 { 1 } else { -1 };
    let mut err = dx + dy;
    loop {
        set_pixel(image, width, height, x0, y0, color);
        if x0 == x1 && y0 == y1 {
            break;
        }
        let e2 = 2 * err;
        if e2 >= dy {
            err += dy;
            x0 += sx;
        }
        if e2 <= dx {
            err += dx;
            y0 += sy;
        }
    }
}

#[cfg(feature = "mcp-adbc")]
fn fill_rect(
    image: &mut [u8],
    width: usize,
    height: usize,
    top_left: (isize, isize),
    bottom_right: (isize, isize),
    color: [u8; 3],
) {
    let (x0, y0) = top_left;
    let (x1, y1) = bottom_right;
    for y in y0.max(0)..=y1.min(height as isize - 1) {
        for x in x0.max(0)..=x1.min(width as isize - 1) {
            set_pixel(image, width, height, x, y, color);
        }
    }
}

#[cfg(feature = "mcp-adbc")]
fn fill_circle(
    image: &mut [u8],
    width: usize,
    height: usize,
    cx: isize,
    cy: isize,
    radius: isize,
    color: [u8; 3],
) {
    for y in -radius..=radius {
        for x in -radius..=radius {
            if x * x + y * y <= radius * radius {
                set_pixel(image, width, height, cx + x, cy + y, color);
            }
        }
    }
}

#[cfg(feature = "mcp-adbc")]
fn encode_rgb_png(width: u32, height: u32, rgb: &[u8]) -> Vec<u8> {
    let mut scanlines = Vec::with_capacity((width as usize * 3 + 1) * height as usize);
    for row in 0..height as usize {
        scanlines.push(0);
        let start = row * width as usize * 3;
        let end = start + width as usize * 3;
        scanlines.extend_from_slice(&rgb[start..end]);
    }

    let mut out = Vec::new();
    out.extend_from_slice(&[137, 80, 78, 71, 13, 10, 26, 10]);
    let mut ihdr = Vec::new();
    ihdr.extend_from_slice(&width.to_be_bytes());
    ihdr.extend_from_slice(&height.to_be_bytes());
    ihdr.extend_from_slice(&[8, 2, 0, 0, 0]);
    write_png_chunk(&mut out, b"IHDR", &ihdr);
    write_png_chunk(&mut out, b"IDAT", &zlib_store(&scanlines));
    write_png_chunk(&mut out, b"IEND", &[]);
    out
}

#[cfg(feature = "mcp-adbc")]
fn write_png_chunk(out: &mut Vec<u8>, kind: &[u8; 4], data: &[u8]) {
    out.extend_from_slice(&(data.len() as u32).to_be_bytes());
    out.extend_from_slice(kind);
    out.extend_from_slice(data);
    let mut crc_input = Vec::with_capacity(kind.len() + data.len());
    crc_input.extend_from_slice(kind);
    crc_input.extend_from_slice(data);
    out.extend_from_slice(&crc32(&crc_input).to_be_bytes());
}

#[cfg(feature = "mcp-adbc")]
fn zlib_store(data: &[u8]) -> Vec<u8> {
    let mut out = vec![0x78, 0x01];
    let mut offset = 0;
    while offset < data.len() {
        let remaining = data.len() - offset;
        let chunk_len = remaining.min(65_535);
        let is_final = offset + chunk_len >= data.len();
        out.push(if is_final { 1 } else { 0 });
        let len = chunk_len as u16;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&(!len).to_le_bytes());
        out.extend_from_slice(&data[offset..offset + chunk_len]);
        offset += chunk_len;
    }
    out.extend_from_slice(&adler32(data).to_be_bytes());
    out
}

#[cfg(feature = "mcp-adbc")]
fn adler32(data: &[u8]) -> u32 {
    const MOD_ADLER: u32 = 65_521;
    let mut a = 1u32;
    let mut b = 0u32;
    for byte in data {
        a = (a + *byte as u32) % MOD_ADLER;
        b = (b + a) % MOD_ADLER;
    }
    (b << 16) | a
}

#[cfg(feature = "mcp-adbc")]
fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for byte in data {
        crc ^= *byte as u32;
        for _ in 0..8 {
            let mask = if crc & 1 == 1 { 0xedb8_8320 } else { 0 };
            crc = (crc >> 1) ^ mask;
        }
    }
    !crc
}

#[cfg(feature = "mcp-adbc")]
fn base64_encode(data: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        out.push(TABLE[(b0 >> 2) as usize] as char);
        out.push(TABLE[(((b0 & 0b0000_0011) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[(((b1 & 0b0000_1111) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(b2 & 0b0011_1111) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
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

fn parse_config() -> Result<ServerConfig, String> {
    let mut models_path: Option<String> = None;
    let mut adbc_driver: Option<String> = env::var("SIDEMANTIC_MCP_ADBC_DRIVER").ok();
    let mut adbc_uri: Option<String> = env::var("SIDEMANTIC_MCP_ADBC_URI").ok();
    let mut adbc_entrypoint: Option<String> = env::var("SIDEMANTIC_MCP_ADBC_ENTRYPOINT").ok();
    let mut database_options: Vec<DatabaseOption> = Vec::new();
    let mut connection_options: Vec<ConnectionOption> = Vec::new();
    if let Ok(env_dbopts) =
        env::var("SIDEMANTIC_MCP_ADBC_DBOPTS").or_else(|_| env::var("SIDEMANTIC_ADBC_DBOPTS"))
    {
        database_options.extend(parse_database_options(&env_dbopts)?);
    }
    if let Ok(env_connopts) =
        env::var("SIDEMANTIC_MCP_ADBC_CONNOPTS").or_else(|_| env::var("SIDEMANTIC_ADBC_CONNOPTS"))
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
            "--driver" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--driver requires a value".to_string())?;
                adbc_driver = Some(value);
            }
            "--uri" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--uri requires a value".to_string())?;
                adbc_uri = Some(value);
            }
            "--entrypoint" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--entrypoint requires a value".to_string())?;
                adbc_entrypoint = Some(value);
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
                    "Usage: sidemantic-mcp [<models_path>] [--models <path>] [--driver <adbc_driver>] [--uri <adbc_uri>] [--entrypoint <driver_entrypoint>] [--dbopt <k=v[,k=v...]>] [--connopt <k=v[,k=v...]>]".to_string()
                );
            }
            value if !value.starts_with('-') && models_path.is_none() => {
                models_path = Some(value.to_string());
            }
            unknown => {
                return Err(format!(
                    "unknown argument: {unknown}. Use --help for usage."
                ));
            }
        }
    }

    let models_path = models_path
        .or_else(|| env::var("SIDEMANTIC_MCP_MODELS").ok())
        .unwrap_or_else(|| ".".to_string());

    Ok(ServerConfig {
        models_path,
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

#[cfg(feature = "mcp-adbc")]
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

#[cfg(not(feature = "mcp-adbc"))]
fn parse_option_value(value: &str) -> String {
    value.to_string()
}

fn parse_database_options(input: &str) -> Result<Vec<DatabaseOption>, String> {
    let mut parsed = Vec::new();
    for (key, raw_value) in parse_kv_pairs(input, "--dbopt")? {
        #[cfg(feature = "mcp-adbc")]
        parsed.push((
            OptionDatabase::from(key.as_str()),
            parse_option_value(&raw_value),
        ));
        #[cfg(not(feature = "mcp-adbc"))]
        parsed.push((key, parse_option_value(&raw_value)));
    }
    Ok(parsed)
}

fn parse_connection_options(input: &str) -> Result<Vec<ConnectionOption>, String> {
    let mut parsed = Vec::new();
    for (key, raw_value) in parse_kv_pairs(input, "--connopt")? {
        #[cfg(feature = "mcp-adbc")]
        parsed.push((
            OptionConnection::from(key.as_str()),
            parse_option_value(&raw_value),
        ));
        #[cfg(not(feature = "mcp-adbc"))]
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = parse_config()?;
    let runtime = load_runtime(&config.models_path)?;
    let server = SidemanticMcpServer::new(runtime, config);

    let service = server.serve(stdio()).await?;
    service.waiting().await?;
    Ok(())
}
