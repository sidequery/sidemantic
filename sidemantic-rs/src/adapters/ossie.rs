//! Forward Apache Ossie importer.
//!
//! This is intentionally separate from [`super::osi`], whose permissive legacy
//! behavior is retained for compatibility. The forward adapter is strict,
//! scope-preserving, target-aware, and fail-closed.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, HashMap};

use polyglot_sql::{DialectType, Expression};
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

use crate::core::{
    Dimension, DimensionType, Metric, MetricType, Model, Relationship, RelationshipType, Segment,
    SemanticGraph,
};
use crate::error::{Result, SidemanticError};

const VALIDATION_MODE: &str = "closed_structural_subset";
const CURRENT_SCHEMA_REVISION: &str = "b6c702ed1c07e91382a69e870c875cbd19570828";
const CURRENT_SCHEMA_VERSION: &str = "0.2.0.dev0-current";
const MAX_SOURCE_BYTES: usize = 16 * 1024 * 1024;
const MAX_PARSE_DEPTH: usize = 256;
const MAX_PARSE_NODES: usize = 100_000;
thread_local! {
    static PARSE_BUDGET: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
}

struct ParseDepth;
impl Drop for ParseDepth {
    fn drop(&mut self) {
        PARSE_BUDGET.with(|budget| {
            let (nodes, depth) = budget.get();
            budget.set((nodes, depth.saturating_sub(1)));
        });
    }
}
const TEMPORAL_TYPES: &[&str] = &["Date", "Time", "DateTime", "DateTimeTz"];
const NUMERIC_TYPES: &[&str] = &["Integer", "Decimal", "Float"];
const DATA_TYPES: &[&str] = &[
    "String",
    "Integer",
    "Decimal",
    "Float",
    "Boolean",
    "Date",
    "Time",
    "DateTime",
    "DateTimeTz",
    "Opaque",
];
const DIALECTS_0_1_1: &[&str] = &[
    "ANSI_SQL",
    "SNOWFLAKE",
    "MDX",
    "TABLEAU",
    "DATABRICKS",
    "MAQL",
];
const DIALECTS_0_2_0: &[&str] = &[
    "ANSI_SQL",
    "SNOWFLAKE",
    "MDX",
    "TABLEAU",
    "DATABRICKS",
    "MAQL",
    "BIGQUERY",
    "SIGMA",
    "THOUGHTSPOT",
];
const DIALECTS_CURRENT: &[&str] = &[
    "ANSI_SQL",
    "SNOWFLAKE",
    "MDX",
    "TABLEAU",
    "DATABRICKS",
    "MAQL",
    "BIGQUERY",
    "SIGMA",
    "THOUGHTSPOT",
    "DAX",
    "OSSIE_SQL_2026",
];

/// Deserialize objects before converting to `Value`, which otherwise silently
/// overwrites duplicate JSON keys. The same contract applies to YAML mappings.
struct UniqueValue(Value);

impl<'de> Deserialize<'de> for UniqueValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let permitted = PARSE_BUDGET.with(|budget| {
            let (nodes, depth) = budget.get();
            budget.set((nodes + 1, depth + 1));
            nodes < MAX_PARSE_NODES && depth < MAX_PARSE_DEPTH
        });
        let _depth = ParseDepth;
        if !permitted {
            return Err(de::Error::custom("Ossie parser resource limit exceeded"));
        }
        struct UniqueVisitor;
        impl<'de> Visitor<'de> for UniqueVisitor {
            type Value = UniqueValue;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a JSON-compatible value without duplicate keys")
            }
            fn visit_bool<E: de::Error>(self, value: bool) -> std::result::Result<Self::Value, E> {
                Ok(UniqueValue(Value::Bool(value)))
            }
            fn visit_i64<E: de::Error>(self, value: i64) -> std::result::Result<Self::Value, E> {
                Ok(UniqueValue(value.into()))
            }
            fn visit_u64<E: de::Error>(self, value: u64) -> std::result::Result<Self::Value, E> {
                Ok(UniqueValue(value.into()))
            }
            fn visit_f64<E: de::Error>(self, value: f64) -> std::result::Result<Self::Value, E> {
                serde_json::Number::from_f64(value)
                    .map(|value| UniqueValue(Value::Number(value)))
                    .ok_or_else(|| E::custom("non-finite numbers are not JSON values"))
            }
            fn visit_str<E: de::Error>(self, value: &str) -> std::result::Result<Self::Value, E> {
                Ok(UniqueValue(value.into()))
            }
            fn visit_string<E: de::Error>(
                self,
                value: String,
            ) -> std::result::Result<Self::Value, E> {
                Ok(UniqueValue(value.into()))
            }
            fn visit_unit<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
                Ok(UniqueValue(Value::Null))
            }
            fn visit_none<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
                Ok(UniqueValue(Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(value) = sequence.next_element::<UniqueValue>()? {
                    values.push(value.0);
                }
                Ok(UniqueValue(Value::Array(values)))
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut mapping: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = Map::new();
                while let Some(key) = mapping.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(de::Error::custom(format!("duplicate key {key:?}")));
                    }
                    values.insert(key, mapping.next_value::<UniqueValue>()?.0);
                }
                Ok(UniqueValue(Value::Object(values)))
            }
        }
        deserializer.deserialize_any(UniqueVisitor)
    }
}

fn expand_yaml_merges(value: &mut Value) -> std::result::Result<(), String> {
    match value {
        Value::Array(values) => {
            for value in values {
                expand_yaml_merges(value)?;
            }
        }
        Value::Object(object) => {
            for value in object.values_mut() {
                expand_yaml_merges(value)?;
            }
            if let Some(merge) = object.remove("<<") {
                let mappings = match merge {
                    Value::Object(mapping) => vec![mapping],
                    Value::Array(values) => values
                        .into_iter()
                        .map(|value| match value {
                            Value::Object(mapping) => Ok(mapping),
                            _ => Err("YAML merge sequences must contain mappings".to_string()),
                        })
                        .collect::<std::result::Result<Vec<_>, _>>()?,
                    _ => {
                        return Err("YAML merge value must be a mapping or sequence of mappings"
                            .to_string())
                    }
                };
                // Explicit keys override merged defaults. In merge sequences,
                // earlier mappings take precedence over subsequent mappings.
                for mapping in mappings {
                    for (key, value) in mapping {
                        object.entry(key).or_insert(value);
                    }
                }
            }
        }
        _ => {}
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DocumentKind {
    Logical,
    Ontology,
}

impl DocumentKind {
    fn label(self) -> &'static str {
        match self {
            Self::Logical => "logical",
            Self::Ontology => "ontology",
        }
    }
}

/// Source serialization is independent from the Ossie schema version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OssieSerialization {
    Json,
    Yaml,
}

impl OssieSerialization {
    pub fn parse(value: &str) -> std::result::Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "json" => Ok(Self::Json),
            "yaml" | "yml" => Ok(Self::Yaml),
            other => Err(format!("unsupported Ossie serialization {other:?}")),
        }
    }
}

/// Consumer-specific compatibility contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OssieConsumerProfile {
    OssieCore,
    Dbt112,
}

impl OssieConsumerProfile {
    pub fn parse(value: &str) -> std::result::Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "ossie-core" => Ok(Self::OssieCore),
            "dbt-1.12" => Ok(Self::Dbt112),
            other => Err(format!("unsupported Ossie consumer profile {other:?}")),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::OssieCore => "ossie-core",
            Self::Dbt112 => "dbt-1.12",
        }
    }
}

/// Executable SQL target. Non-SQL Ossie variants are preserved but never selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OssieTarget {
    AnsiSql,
    DuckDb,
    Postgres,
    Snowflake,
    Databricks,
    BigQuery,
}

impl OssieTarget {
    pub fn parse(value: &str) -> std::result::Result<Self, String> {
        match value.trim().to_ascii_uppercase().replace('-', "_").as_str() {
            "ANSI" | "ANSI_SQL" => Ok(Self::AnsiSql),
            "DUCKDB" => Ok(Self::DuckDb),
            "POSTGRES" | "POSTGRESQL" => Ok(Self::Postgres),
            "SNOWFLAKE" => Ok(Self::Snowflake),
            "DATABRICKS" => Ok(Self::Databricks),
            "BIGQUERY" | "BIG_QUERY" => Ok(Self::BigQuery),
            other => Err(format!(
                "unsupported executable Ossie target {other:?}; supported: ANSI_SQL, DUCKDB, POSTGRES, SNOWFLAKE, DATABRICKS, BIGQUERY"
            )),
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::AnsiSql => "ANSI_SQL",
            Self::DuckDb => "DUCKDB",
            Self::Postgres => "POSTGRES",
            Self::Snowflake => "SNOWFLAKE",
            Self::Databricks => "DATABRICKS",
            Self::BigQuery => "BIGQUERY",
        }
    }

    fn parser_dialect(self) -> DialectType {
        match self {
            Self::AnsiSql | Self::DuckDb => DialectType::DuckDB,
            Self::Postgres => DialectType::PostgreSQL,
            Self::Snowflake => DialectType::Snowflake,
            Self::Databricks => DialectType::Databricks,
            Self::BigQuery => DialectType::BigQuery,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OssieProfile {
    pub identifier: String,
    pub schema_version: String,
    pub consumer_profile: String,
    pub validation_schema_version: String,
    pub compatibility_alias_for: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_revision: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OssieDiagnostic {
    pub code: String,
    pub severity: &'static str,
    pub message: String,
    pub instance_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OssieStatus {
    pub valid: bool,
    pub executable: bool,
    pub validation_mode: &'static str,
    pub profile: Option<OssieProfile>,
    pub document_kind: Option<String>,
    pub scopes: Vec<String>,
    pub diagnostics: Vec<OssieDiagnostic>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OssieCompiledScope {
    pub scope_id: String,
    pub semantic_model_index: usize,
    pub target_dialect: String,
    pub models: Vec<Model>,
    pub metrics: Vec<Metric>,
}

impl OssieCompiledScope {
    /// Build one graph without inferring dataset ownership from metric SQL.
    /// Ossie metrics belong to the semantic-model namespace, and may reference
    /// metrics declared later in the source document.
    pub fn into_graph(self) -> Result<SemanticGraph> {
        crate::semantic_input::with_semantic_stack(|| {
            let target =
                OssieTarget::parse(&self.target_dialect).map_err(SidemanticError::Validation)?;
            let mut graph = SemanticGraph::new();
            for model in self.models {
                graph.add_model(model)?;
            }
            for metric in self.metrics {
                graph.add_metric_unvalidated(metric)?;
            }
            graph.set_metric_scopes(HashMap::new())?;
            validate_scope_dependencies(&graph, target)?;
            Ok(graph)
        })
    }
}

fn validate_scope_dependencies(graph: &SemanticGraph, target: OssieTarget) -> Result<()> {
    fn collect(value: &Value, graph: &SemanticGraph, dependencies: &mut Vec<String>) -> Result<()> {
        if let Some(column) = value.get("column") {
            let field = column["name"]["name"].as_str().unwrap_or("");
            if let Some(model) = column["table"]["name"].as_str() {
                if graph.get_model(model).is_none() {
                    return Err(SidemanticError::Validation(format!(
                        "Unknown dataset '{model}'"
                    )));
                }
                // Logical fields and undeclared physical inputs were qualified
                // by the importer; neither is a metric dependency.
            } else if graph.get_metric(field).is_some() {
                dependencies.push(field.to_string());
            } else {
                return Err(SidemanticError::Validation(format!(
                    "Unresolved metric or ambiguous field '{field}'"
                )));
            }
            return Ok(());
        }
        match value {
            Value::Object(fields) => {
                for child in fields.values() {
                    collect(child, graph, dependencies)?;
                }
            }
            Value::Array(children) => {
                for child in children {
                    collect(child, graph, dependencies)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    fn visit(
        name: &str,
        edges: &HashMap<String, Vec<String>>,
        active: &mut BTreeSet<String>,
        complete: &mut BTreeSet<String>,
    ) -> Result<()> {
        if complete.contains(name) {
            return Ok(());
        }
        if !active.insert(name.to_string()) {
            return Err(SidemanticError::CircularDependency(name.to_string()));
        }
        for child in &edges[name] {
            visit(child, edges, active, complete)?;
        }
        active.remove(name);
        complete.insert(name.to_string());
        Ok(())
    }
    let mut edges = HashMap::new();
    for metric in graph.metrics() {
        let expression = parse_scalar_sql(metric.sql.as_deref().unwrap_or(""), target)
            .map_err(SidemanticError::SqlParse)?;
        let ast = serde_json::to_value(expression)
            .map_err(|error| SidemanticError::Validation(error.to_string()))?;
        let mut dependencies = Vec::new();
        collect(&ast, graph, &mut dependencies)?;
        edges.insert(metric.name.clone(), dependencies);
    }
    let mut active = BTreeSet::new();
    let mut complete = BTreeSet::new();
    for name in edges.keys() {
        visit(name, &edges, &mut active, &mut complete)?;
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize)]
pub struct OssieCatalog {
    pub profile: OssieProfile,
    pub validation_mode: &'static str,
    pub scopes: Vec<OssieCompiledScope>,
    pub diagnostics: Vec<OssieDiagnostic>,
}

#[derive(Debug, Clone)]
struct InspectedDocument {
    root: Option<Value>,
    profile: Option<OssieProfile>,
    kind: Option<DocumentKind>,
    scope_ids: Vec<String>,
    diagnostics: Vec<OssieDiagnostic>,
    flat_root: bool,
}

impl InspectedDocument {
    fn status(&self) -> OssieStatus {
        let valid = self.diagnostics.is_empty();
        OssieStatus {
            valid,
            executable: valid && self.kind == Some(DocumentKind::Logical),
            validation_mode: VALIDATION_MODE,
            profile: self.profile.clone(),
            document_kind: self.kind.map(|kind| kind.label().to_string()),
            scopes: self.scope_ids.clone(),
            diagnostics: self.diagnostics.clone(),
        }
    }
}

/// Strict forward adapter. It never dispatches through the legacy OSI adapter.
#[derive(Debug, Clone, Copy, Default)]
pub struct OssieForwardAdapter;

impl OssieForwardAdapter {
    pub fn inspect(
        &self,
        content: &str,
        serialization: OssieSerialization,
        consumer: OssieConsumerProfile,
    ) -> OssieStatus {
        crate::semantic_input::with_semantic_stack(|| {
            Ok(self
                .inspect_document(content, serialization, consumer)
                .status())
        })
        .unwrap_or_else(|error| OssieStatus {
            valid: false,
            executable: false,
            validation_mode: VALIDATION_MODE,
            profile: None,
            document_kind: None,
            scopes: Vec::new(),
            diagnostics: vec![diagnostic(
                "ossie.parse.resource_limit",
                error.to_string(),
                "",
                None,
            )],
        })
    }

    pub fn parse_catalog(
        &self,
        content: &str,
        serialization: OssieSerialization,
        consumer: OssieConsumerProfile,
        target: OssieTarget,
    ) -> Result<OssieCatalog> {
        crate::semantic_input::with_semantic_stack(|| {
            self.parse_catalog_inner(content, serialization, consumer, target)
        })
    }

    fn parse_catalog_inner(
        &self,
        content: &str,
        serialization: OssieSerialization,
        consumer: OssieConsumerProfile,
        target: OssieTarget,
    ) -> Result<OssieCatalog> {
        let mut inspected = self.inspect_document(content, serialization, consumer);
        if !inspected.diagnostics.is_empty() {
            return Err(status_error(&inspected.status()));
        }
        if inspected.kind != Some(DocumentKind::Logical) {
            inspected.diagnostics.push(diagnostic(
                "ossie.handoff.document_not_executable",
                "The validated document is not a logical semantic-model document.",
                "",
                None,
            ));
            return Err(status_error(&inspected.status()));
        }

        let root = inspected
            .root
            .as_ref()
            .and_then(Value::as_object)
            .ok_or_else(|| {
                SidemanticError::Validation("validated Ossie root disappeared".to_string())
            })?;
        let semantic_models = root
            .get("semantic_model")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                SidemanticError::Validation("validated Ossie scopes disappeared".to_string())
            })?;

        let profile = inspected.profile.clone().expect("valid logical profile");
        let mut diagnostics = Vec::new();
        let mut scopes = Vec::new();
        for (index, value) in semantic_models.iter().enumerate() {
            let scope_id = inspected.scope_ids[index].clone();
            let scope_pointer = if inspected.flat_root {
                String::new()
            } else {
                format!("/semantic_model/{index}")
            };
            if let Some(scope) = compile_scope(
                value,
                index,
                &scope_pointer,
                &scope_id,
                target,
                &mut diagnostics,
            ) {
                scopes.push(scope);
            }
        }
        sort_diagnostics(&mut diagnostics);
        if !diagnostics.is_empty() {
            let status = OssieStatus {
                valid: false,
                executable: false,
                validation_mode: VALIDATION_MODE,
                profile: Some(profile),
                document_kind: Some("logical".to_string()),
                scopes: inspected.scope_ids,
                diagnostics,
            };
            return Err(status_error(&status));
        }

        Ok(OssieCatalog {
            profile,
            validation_mode: VALIDATION_MODE,
            scopes,
            diagnostics,
        })
    }

    pub fn select_scope(
        &self,
        content: &str,
        serialization: OssieSerialization,
        consumer: OssieConsumerProfile,
        target: OssieTarget,
        scope_id: Option<&str>,
    ) -> Result<OssieCompiledScope> {
        let catalog = self.parse_catalog(content, serialization, consumer, target)?;
        match scope_id {
            Some(requested) => {
                let available = catalog
                    .scopes
                    .iter()
                    .map(|scope| scope.scope_id.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                catalog
                    .scopes
                    .into_iter()
                    .find(|scope| scope.scope_id == requested)
                    .ok_or_else(|| {
                        SidemanticError::Validation(format!(
                            "Ossie scope {requested:?} not found; available: {available}"
                        ))
                    })
            }
            None if catalog.scopes.len() == 1 => Ok(catalog.scopes.into_iter().next().unwrap()),
            None if catalog.scopes.is_empty() => Err(SidemanticError::Validation(
                "Ossie document contains no executable scopes".to_string(),
            )),
            None => Err(SidemanticError::Validation(format!(
                "Ossie scope selection is ambiguous; select one explicitly: {}",
                catalog
                    .scopes
                    .iter()
                    .map(|scope| scope.scope_id.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))),
        }
    }

    fn inspect_document(
        &self,
        content: &str,
        serialization: OssieSerialization,
        consumer: OssieConsumerProfile,
    ) -> InspectedDocument {
        PARSE_BUDGET.with(|budget| budget.set((0, 0)));
        let parsed = if content.len() > MAX_SOURCE_BYTES {
            Err("Ossie parser resource limit exceeded: source is larger than 16 MiB".to_string())
        } else {
            match serialization {
                OssieSerialization::Json => serde_json::from_str::<UniqueValue>(content)
                    .map(|value| value.0)
                    .map_err(|error| error.to_string()),
                OssieSerialization::Yaml => serde_yaml::from_str::<UniqueValue>(content)
                    .map_err(|error| error.to_string())
                    .and_then(|mut value| {
                        expand_yaml_merges(&mut value.0)?;
                        Ok(value.0)
                    }),
            }
        };
        let root: Value = match parsed {
            Ok(value) => value,
            Err(error) => {
                return InspectedDocument {
                    root: None,
                    profile: None,
                    kind: None,
                    scope_ids: Vec::new(),
                    flat_root: false,
                    diagnostics: vec![diagnostic(
                        if error.contains("resource limit") || error.contains("recursion limit") {
                            "ossie.parse.resource_limit"
                        } else if error.contains("duplicate key") {
                            "ossie.parse.duplicate_key"
                        } else {
                            "ossie.parse.invalid_syntax"
                        },
                        format!("Invalid Ossie input: {error}"),
                        "",
                        None,
                    )],
                };
            }
        };

        let Some(object) = root.as_object() else {
            return InspectedDocument {
                root: Some(root),
                profile: None,
                kind: None,
                scope_ids: Vec::new(),
                flat_root: false,
                diagnostics: vec![diagnostic(
                    "ossie.schema.type",
                    "The Ossie document root must be an object.",
                    "",
                    None,
                )],
            };
        };

        let kind = classify_document(object);
        let flat_root =
            kind == Some(DocumentKind::Logical) && !object.contains_key("semantic_model");
        let mut diagnostics = Vec::new();
        let mut profile = resolve_profile(object.get("version"), consumer, &mut diagnostics);
        if kind == Some(DocumentKind::Ontology) && object.contains_key("prefixes") {
            if let Some(profile) = profile.as_mut() {
                profile.schema_revision = Some(CURRENT_SCHEMA_REVISION.to_string());
            }
        }
        if flat_root {
            if let Some(profile) = profile.as_mut() {
                if profile.schema_version != "0.2.0.dev0" {
                    diagnostics.push(diagnostic(
                        "ossie.schema.profile_unsupported",
                        "Flat logical documents require version 0.2.0.dev0.",
                        "/version",
                        None,
                    ));
                }
                profile.schema_revision = Some(CURRENT_SCHEMA_REVISION.to_string());
            }
        }
        // Keep the internal scope traversal shared. Source pointers are mapped
        // back to the flat document before returning diagnostics.
        let root = if flat_root {
            let mut scope = object.clone();
            scope.remove("version");
            serde_json::json!({"version": object.get("version"), "semantic_model": [scope]})
        } else {
            root
        };
        let object = root.as_object().expect("checked object root");
        let mut scope_ids = Vec::new();

        match kind {
            Some(DocumentKind::Logical) => {
                validate_logical_root(object, profile.as_ref(), &mut diagnostics);
                scope_ids = logical_scope_ids(object);
                validate_semantics(object, &scope_ids, &mut diagnostics);
            }
            Some(DocumentKind::Ontology) => validate_ontology_root(object, &mut diagnostics),
            None => diagnostics.push(diagnostic(
                "ossie.schema.document_family",
                "Document must contain exactly one of semantic_model or ontology/ontology_mappings.",
                "",
                None,
            )),
        }
        if flat_root {
            for diagnostic in &mut diagnostics {
                if let Some(pointer) = diagnostic.instance_path.strip_prefix("/semantic_model/0") {
                    diagnostic.instance_path = pointer.to_string();
                }
            }
        }
        sort_diagnostics(&mut diagnostics);

        InspectedDocument {
            root: Some(root),
            profile,
            kind,
            scope_ids,
            diagnostics,
            flat_root,
        }
    }
}

fn classify_document(root: &Map<String, Value>) -> Option<DocumentKind> {
    let logical = root.contains_key("semantic_model") || root.contains_key("datasets");
    let ontology = root.contains_key("ontology") || root.contains_key("ontology_mappings");
    match (logical, ontology) {
        (true, false) => Some(DocumentKind::Logical),
        (false, true) => Some(DocumentKind::Ontology),
        _ => None,
    }
}

fn resolve_profile(
    version: Option<&Value>,
    consumer: OssieConsumerProfile,
    diagnostics: &mut Vec<OssieDiagnostic>,
) -> Option<OssieProfile> {
    let Some(version) = version.and_then(Value::as_str) else {
        diagnostics.push(diagnostic(
            "ossie.schema.required",
            "A string Ossie version is required.",
            "/version",
            None,
        ));
        return None;
    };

    let (validation_version, alias) = match (consumer, version) {
        (OssieConsumerProfile::OssieCore, "0.1.1") => ("0.1.1", None),
        (OssieConsumerProfile::OssieCore, "0.2.0.dev0") => ("0.2.0.dev0", None),
        (OssieConsumerProfile::Dbt112, "0.1.0") => ("0.1.1", Some("0.1.1")),
        (OssieConsumerProfile::Dbt112, "0.1.1") => ("0.1.1", None),
        (OssieConsumerProfile::OssieCore, "0.1.0") => {
            diagnostics.push(diagnostic(
                "ossie.schema.profile_context_required",
                "Ossie 0.1.0 is only supported as the dbt-1.12 compatibility alias.",
                "/version",
                None,
            ));
            return None;
        }
        _ => {
            diagnostics.push(diagnostic(
                "ossie.schema.profile_unsupported",
                format!(
                    "Unsupported Ossie version {version:?} for consumer {}.",
                    consumer.label()
                ),
                "/version",
                None,
            ));
            return None;
        }
    };

    Some(OssieProfile {
        identifier: format!("{}:{version}", consumer.label()),
        schema_version: version.to_string(),
        consumer_profile: consumer.label().to_string(),
        validation_schema_version: validation_version.to_string(),
        compatibility_alias_for: alias.map(str::to_string),
        schema_revision: None,
    })
}

fn validate_logical_root(
    root: &Map<String, Value>,
    profile: Option<&OssieProfile>,
    diagnostics: &mut Vec<OssieDiagnostic>,
) {
    let validation_version = profile.map(|profile| {
        if profile.schema_revision.is_some() {
            CURRENT_SCHEMA_VERSION
        } else {
            profile.validation_schema_version.as_str()
        }
    });
    reject_unknown(
        root,
        &["version", "dialects", "vendors", "semantic_model"],
        "",
        diagnostics,
        None,
    );
    let allowed_dialects = if validation_version == Some(CURRENT_SCHEMA_VERSION) {
        DIALECTS_CURRENT
    } else if validation_version == Some("0.2.0.dev0") {
        DIALECTS_0_2_0
    } else {
        DIALECTS_0_1_1
    };
    if let Some(dialects) = optional_array(root, "dialects", "", diagnostics, None) {
        for (index, dialect) in dialects.iter().enumerate() {
            if !dialect
                .as_str()
                .is_some_and(|value| allowed_dialects.contains(&value))
            {
                diagnostics.push(diagnostic(
                    "ossie.schema.enum",
                    "Unsupported document dialect for this profile.",
                    format!("/dialects/{index}"),
                    None,
                ));
            }
        }
    }
    if let Some(vendors) = optional_array(root, "vendors", "", diagnostics, None) {
        for (index, vendor) in vendors.iter().enumerate() {
            if !vendor.is_string() {
                diagnostics.push(diagnostic(
                    "ossie.schema.type",
                    "Expected a vendor string.",
                    format!("/vendors/{index}"),
                    None,
                ));
            }
        }
    }

    let Some(scopes) = required_array(root, "semantic_model", "", diagnostics, None) else {
        return;
    };
    for (scope_index, scope) in scopes.iter().enumerate() {
        let pointer = format!("/semantic_model/{scope_index}");
        let Some(scope) = require_object(scope, &pointer, diagnostics, None) else {
            continue;
        };
        reject_unknown(
            scope,
            &[
                "name",
                "description",
                "ai_context",
                "datasets",
                "relationships",
                "metrics",
                "custom_extensions",
            ],
            &pointer,
            diagnostics,
            None,
        );
        required_string(scope, "name", &pointer, diagnostics, None);
        let Some(datasets) = required_array(scope, "datasets", &pointer, diagnostics, None) else {
            continue;
        };
        if datasets.is_empty() {
            diagnostics.push(diagnostic(
                "ossie.schema.min_items",
                "Semantic-model datasets must contain at least one item.",
                format!("{pointer}/datasets"),
                None,
            ));
        }
        for (dataset_index, dataset) in datasets.iter().enumerate() {
            validate_dataset(
                dataset,
                &format!("{pointer}/datasets/{dataset_index}"),
                validation_version,
                diagnostics,
            );
        }
        if let Some(relationships) =
            optional_array(scope, "relationships", &pointer, diagnostics, None)
        {
            for (index, relationship) in relationships.iter().enumerate() {
                validate_relationship_schema(
                    relationship,
                    &format!("{pointer}/relationships/{index}"),
                    diagnostics,
                );
            }
        }
        if let Some(metrics) = optional_array(scope, "metrics", &pointer, diagnostics, None) {
            for (index, metric) in metrics.iter().enumerate() {
                validate_metric_schema(
                    metric,
                    &format!("{pointer}/metrics/{index}"),
                    validation_version,
                    diagnostics,
                );
            }
        }
    }
}

fn validate_dataset(
    value: &Value,
    pointer: &str,
    version: Option<&str>,
    diagnostics: &mut Vec<OssieDiagnostic>,
) {
    let Some(dataset) = require_object(value, pointer, diagnostics, None) else {
        return;
    };
    reject_unknown(
        dataset,
        &[
            "name",
            "source",
            "primary_key",
            "unique_keys",
            "description",
            "ai_context",
            "fields",
            "custom_extensions",
        ],
        pointer,
        diagnostics,
        None,
    );
    required_string(dataset, "name", pointer, diagnostics, None);
    if let Some(source) = required_string(dataset, "source", pointer, diagnostics, None) {
        if ![
            OssieTarget::DuckDb,
            OssieTarget::Postgres,
            OssieTarget::Snowflake,
            OssieTarget::Databricks,
            OssieTarget::BigQuery,
        ]
        .iter()
        .any(|target| classify_source(source, *target).is_some())
        {
            diagnostics.push(diagnostic(
                "ossie.lowering.source_ambiguous",
                "Dataset source is neither a table reference nor one SQL query.",
                format!("{pointer}/source"),
                None,
            ));
        }
    }
    validate_string_array(
        dataset.get("primary_key"),
        &format!("{pointer}/primary_key"),
        diagnostics,
        None,
    );
    validate_nested_string_array(
        dataset.get("unique_keys"),
        &format!("{pointer}/unique_keys"),
        diagnostics,
        None,
    );
    if let Some(fields) = optional_array(dataset, "fields", pointer, diagnostics, None) {
        for (index, field) in fields.iter().enumerate() {
            validate_field_schema(
                field,
                &format!("{pointer}/fields/{index}"),
                version,
                diagnostics,
            );
        }
    }
}

fn validate_field_schema(
    value: &Value,
    pointer: &str,
    version: Option<&str>,
    diagnostics: &mut Vec<OssieDiagnostic>,
) {
    let Some(field) = require_object(value, pointer, diagnostics, None) else {
        return;
    };
    let allowed = if matches!(version, Some("0.2.0.dev0") | Some(CURRENT_SCHEMA_VERSION)) {
        &[
            "name",
            "expression",
            "dimension",
            "label",
            "description",
            "datatype",
            "ai_context",
            "custom_extensions",
        ][..]
    } else {
        &[
            "name",
            "expression",
            "dimension",
            "label",
            "description",
            "ai_context",
            "custom_extensions",
        ][..]
    };
    reject_unknown(field, allowed, pointer, diagnostics, None);
    required_string(field, "name", pointer, diagnostics, None);
    validate_expression_schema(
        field.get("expression"),
        &format!("{pointer}/expression"),
        version,
        diagnostics,
    );
    validate_data_type(
        field.get("datatype"),
        &format!("{pointer}/datatype"),
        diagnostics,
    );
    if let Some(dimension) = field.get("dimension") {
        if let Some(dimension) = require_object(
            dimension,
            &format!("{pointer}/dimension"),
            diagnostics,
            None,
        ) {
            reject_unknown(
                dimension,
                &["is_time"],
                &format!("{pointer}/dimension"),
                diagnostics,
                None,
            );
            if let Some(is_time) = dimension.get("is_time") {
                if !is_time.is_boolean() {
                    diagnostics.push(diagnostic(
                        "ossie.schema.type",
                        "dimension.is_time must be a boolean.",
                        format!("{pointer}/dimension/is_time"),
                        None,
                    ));
                }
            }
        }
    }
}

fn validate_metric_schema(
    value: &Value,
    pointer: &str,
    version: Option<&str>,
    diagnostics: &mut Vec<OssieDiagnostic>,
) {
    let Some(metric) = require_object(value, pointer, diagnostics, None) else {
        return;
    };
    let allowed = if matches!(version, Some("0.2.0.dev0") | Some(CURRENT_SCHEMA_VERSION)) {
        &[
            "name",
            "expression",
            "description",
            "datatype",
            "ai_context",
            "custom_extensions",
        ][..]
    } else {
        &[
            "name",
            "expression",
            "description",
            "ai_context",
            "custom_extensions",
        ][..]
    };
    reject_unknown(metric, allowed, pointer, diagnostics, None);
    required_string(metric, "name", pointer, diagnostics, None);
    validate_expression_schema(
        metric.get("expression"),
        &format!("{pointer}/expression"),
        version,
        diagnostics,
    );
    validate_data_type(
        metric.get("datatype"),
        &format!("{pointer}/datatype"),
        diagnostics,
    );
}

fn validate_relationship_schema(
    value: &Value,
    pointer: &str,
    diagnostics: &mut Vec<OssieDiagnostic>,
) {
    let Some(relationship) = require_object(value, pointer, diagnostics, None) else {
        return;
    };
    reject_unknown(
        relationship,
        &[
            "name",
            "from",
            "to",
            "from_columns",
            "to_columns",
            "ai_context",
            "custom_extensions",
        ],
        pointer,
        diagnostics,
        None,
    );
    for key in ["name", "from", "to"] {
        required_string(relationship, key, pointer, diagnostics, None);
    }
    for key in ["from_columns", "to_columns"] {
        let columns = required_array(relationship, key, pointer, diagnostics, None);
        if columns.is_some_and(Vec::is_empty) {
            diagnostics.push(diagnostic(
                "ossie.schema.min_items",
                format!("Relationship {key} must not be empty."),
                format!("{pointer}/{key}"),
                None,
            ));
        }
        validate_string_array(
            relationship.get(key),
            &format!("{pointer}/{key}"),
            diagnostics,
            None,
        );
    }
}

fn validate_expression_schema(
    value: Option<&Value>,
    pointer: &str,
    version: Option<&str>,
    diagnostics: &mut Vec<OssieDiagnostic>,
) {
    let Some(value) = value else {
        diagnostics.push(diagnostic(
            "ossie.schema.required",
            "An expression is required.",
            pointer
                .rsplit_once('/')
                .map(|(parent, _)| parent)
                .unwrap_or(""),
            None,
        ));
        return;
    };
    let Some(expression) = require_object(value, pointer, diagnostics, None) else {
        return;
    };
    reject_unknown(expression, &["dialects"], pointer, diagnostics, None);
    let Some(variants) = required_array(expression, "dialects", pointer, diagnostics, None) else {
        return;
    };
    if variants.is_empty() {
        diagnostics.push(diagnostic(
            "ossie.schema.min_items",
            "Expression dialects must contain at least one item.",
            format!("{pointer}/dialects"),
            None,
        ));
    }
    let allowed_dialects = if version == Some(CURRENT_SCHEMA_VERSION) {
        DIALECTS_CURRENT
    } else if version == Some("0.2.0.dev0") {
        DIALECTS_0_2_0
    } else {
        DIALECTS_0_1_1
    };
    let mut seen = BTreeSet::new();
    for (index, variant) in variants.iter().enumerate() {
        let variant_pointer = format!("{pointer}/dialects/{index}");
        let Some(variant) = require_object(variant, &variant_pointer, diagnostics, None) else {
            continue;
        };
        reject_unknown(
            variant,
            &["dialect", "expression"],
            &variant_pointer,
            diagnostics,
            None,
        );
        let dialect = required_string(variant, "dialect", &variant_pointer, diagnostics, None);
        required_string(variant, "expression", &variant_pointer, diagnostics, None);
        if let Some(dialect) = dialect {
            let normalized = dialect.to_ascii_uppercase();
            if !allowed_dialects.contains(&dialect) {
                diagnostics.push(diagnostic(
                    "ossie.schema.enum",
                    format!("Unsupported expression dialect {dialect:?} for this profile."),
                    format!("{variant_pointer}/dialect"),
                    None,
                ));
            } else if !seen.insert(normalized.clone()) {
                diagnostics.push(diagnostic(
                    "ossie.semantic.expression.dialect_duplicate",
                    format!("Duplicate expression dialect {normalized:?}."),
                    format!("{variant_pointer}/dialect"),
                    None,
                ));
            }
        }
    }
}

fn validate_data_type(
    value: Option<&Value>,
    pointer: &str,
    diagnostics: &mut Vec<OssieDiagnostic>,
) {
    let Some(value) = value else {
        return;
    };
    match value.as_str() {
        Some(value) if DATA_TYPES.contains(&value) => {}
        Some(value) => diagnostics.push(diagnostic(
            "ossie.schema.enum",
            format!("Unsupported logical datatype {value:?}."),
            pointer,
            None,
        )),
        None => diagnostics.push(diagnostic(
            "ossie.schema.type",
            "Logical datatype must be a string.",
            pointer,
            None,
        )),
    }
}

fn validate_ontology_root(root: &Map<String, Value>, diagnostics: &mut Vec<OssieDiagnostic>) {
    reject_unknown(
        root,
        &[
            "version",
            "name",
            "description",
            "ai_context",
            "ontology",
            "ontology_mappings",
            "requires",
            "prefixes",
        ],
        "",
        diagnostics,
        None,
    );
    required_string(root, "name", "", diagnostics, None);
    if root.get("version").and_then(Value::as_str) != Some("0.2.0.dev0") {
        diagnostics.push(diagnostic(
            "ossie.schema.const",
            "Ontology documents require version 0.2.0.dev0.",
            "/version",
            None,
        ));
    }
    validate_string_array(root.get("requires"), "/requires", diagnostics, None);
    if let Some(prefixes) = root.get("prefixes") {
        if !prefixes
            .as_object()
            .is_some_and(|prefixes| prefixes.values().all(Value::is_string))
        {
            diagnostics.push(diagnostic(
                "ossie.schema.type",
                "Ontology prefixes must map strings to IRI strings.",
                "/prefixes",
                None,
            ));
        }
    }
    if !root.contains_key("ontology") {
        diagnostics.push(diagnostic(
            "ossie.schema.required",
            "An ontology array is required.",
            "",
            None,
        ));
    }
    if let Some(ontology) = root.get("ontology") {
        if ontology.as_array().is_some_and(Vec::is_empty) {
            diagnostics.push(diagnostic(
                "ossie.schema.min_items",
                "Ontology must contain at least one component.",
                "/ontology",
                None,
            ));
        }
        if !ontology.is_array() {
            diagnostics.push(diagnostic(
                "ossie.schema.type",
                "ontology must be an array.",
                "/ontology",
                None,
            ));
        }
    }
    if let Some(mappings) = root.get("ontology_mappings") {
        if !mappings.is_array() {
            diagnostics.push(diagnostic(
                "ossie.schema.type",
                "ontology_mappings must be an array.",
                "/ontology_mappings",
                None,
            ));
        }
    }
}

fn logical_scope_ids(root: &Map<String, Value>) -> Vec<String> {
    let Some(scopes) = root.get("semantic_model").and_then(Value::as_array) else {
        return Vec::new();
    };
    let names = scopes
        .iter()
        .map(|scope| {
            scope
                .as_object()
                .and_then(|scope| scope.get("name"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        })
        .collect::<Vec<_>>();
    let mut counts: HashMap<String, usize> = HashMap::new();
    for name in &names {
        *counts.entry(normalize_identifier(name)).or_insert(0) += 1;
    }
    names
        .into_iter()
        .enumerate()
        .map(|(index, name)| {
            if counts
                .get(&normalize_identifier(&name))
                .copied()
                .unwrap_or(0)
                > 1
            {
                format!("{name}@{index}")
            } else {
                name
            }
        })
        .collect()
}

fn validate_semantics(
    root: &Map<String, Value>,
    scope_ids: &[String],
    diagnostics: &mut Vec<OssieDiagnostic>,
) {
    let Some(scopes) = root.get("semantic_model").and_then(Value::as_array) else {
        return;
    };
    duplicate_names(
        scopes,
        "/semantic_model",
        "ossie.semantic.semantic_model.name_duplicate",
        None,
        diagnostics,
    );
    for (scope_index, scope_value) in scopes.iter().enumerate() {
        let Some(scope) = scope_value.as_object() else {
            continue;
        };
        let scope_id = scope_ids.get(scope_index).cloned();
        let scope_pointer = format!("/semantic_model/{scope_index}");
        let datasets = scope.get("datasets").and_then(Value::as_array);
        let metrics = scope.get("metrics").and_then(Value::as_array);
        let relationships = scope.get("relationships").and_then(Value::as_array);
        if let Some(datasets) = datasets {
            duplicate_names(
                datasets,
                &format!("{scope_pointer}/datasets"),
                "ossie.semantic.dataset.name_duplicate",
                scope_id.as_deref(),
                diagnostics,
            );
            for (dataset_index, dataset) in datasets.iter().enumerate() {
                if let Some(dataset) = dataset.as_object() {
                    validate_declared_keys(
                        dataset,
                        &format!("{scope_pointer}/datasets/{dataset_index}"),
                        scope_id.as_deref(),
                        diagnostics,
                    );
                }
                if let Some(fields) = dataset
                    .as_object()
                    .and_then(|dataset| dataset.get("fields"))
                    .and_then(Value::as_array)
                {
                    duplicate_names(
                        fields,
                        &format!("{scope_pointer}/datasets/{dataset_index}/fields"),
                        "ossie.semantic.field.name_duplicate",
                        scope_id.as_deref(),
                        diagnostics,
                    );
                }
            }
        }
        if let Some(metrics) = metrics {
            duplicate_names(
                metrics,
                &format!("{scope_pointer}/metrics"),
                "ossie.semantic.metric.name_duplicate",
                scope_id.as_deref(),
                diagnostics,
            );
        }
        if let Some(relationships) = relationships {
            duplicate_names(
                relationships,
                &format!("{scope_pointer}/relationships"),
                "ossie.semantic.relationship.name_duplicate",
                scope_id.as_deref(),
                diagnostics,
            );
            validate_relationship_references(
                datasets.map(Vec::as_slice).unwrap_or(&[]),
                relationships,
                scope_index,
                scope_id.as_deref(),
                diagnostics,
            );
        }
    }
}

fn validate_declared_keys(
    dataset: &Map<String, Value>,
    pointer: &str,
    scope: Option<&str>,
    diagnostics: &mut Vec<OssieDiagnostic>,
) {
    let fields = dataset
        .get("fields")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|field| field.get("name").and_then(Value::as_str))
        .map(normalize_identifier)
        .collect::<BTreeSet<_>>();
    let mut keys = Vec::new();
    if let Some(key) = dataset.get("primary_key").and_then(Value::as_array) {
        keys.push(("primary_key".to_string(), key));
    }
    if let Some(unique_keys) = dataset.get("unique_keys").and_then(Value::as_array) {
        for (index, key) in unique_keys.iter().enumerate() {
            if let Some(key) = key.as_array() {
                keys.push((format!("unique_keys/{index}"), key));
            }
        }
    }
    let mut seen_unique_keys = BTreeSet::new();
    for (path, columns) in keys {
        let key_pointer = format!("{pointer}/{path}");
        if columns.is_empty() {
            diagnostics.push(diagnostic(
                "ossie.semantic.dataset.key_empty",
                "Declared primary and unique keys must contain at least one field.",
                &key_pointer,
                scope,
            ));
            continue;
        }
        let mut valid = true;
        let mut seen_columns = BTreeSet::new();
        for (index, column) in columns.iter().enumerate() {
            let Some(column) = column.as_str() else {
                valid = false;
                continue;
            };
            let normalized = normalize_identifier(column);
            if !fields.contains(&normalized) {
                valid = false;
                diagnostics.push(diagnostic(
                    "ossie.semantic.dataset.key_field_unknown",
                    format!("Declared key field {column:?} is not declared in this dataset."),
                    format!("{key_pointer}/{index}"),
                    scope,
                ));
            }
            if !seen_columns.insert(normalized) {
                valid = false;
                diagnostics.push(diagnostic(
                    "ossie.semantic.dataset.key_column_duplicate",
                    format!(
                        "Declared key repeats field {column:?} after identifier normalization."
                    ),
                    format!("{key_pointer}/{index}"),
                    scope,
                ));
            }
        }
        // A primary key may also be listed as a unique key. Duplicate unique
        // declarations compare column sets, independently of declaration order.
        if valid && path.starts_with("unique_keys/") && !seen_unique_keys.insert(seen_columns) {
            diagnostics.push(diagnostic(
                "ossie.semantic.dataset.key_duplicate",
                "Declared unique key duplicates an earlier unique key.",
                key_pointer,
                scope,
            ));
        }
    }
}

fn validate_relationship_references(
    datasets: &[Value],
    relationships: &[Value],
    scope_index: usize,
    scope_id: Option<&str>,
    diagnostics: &mut Vec<OssieDiagnostic>,
) {
    let mut dataset_by_name = BTreeMap::new();
    for dataset in datasets {
        let Some(dataset) = dataset.as_object() else {
            continue;
        };
        let Some(name) = dataset.get("name").and_then(Value::as_str) else {
            continue;
        };
        dataset_by_name.insert(normalize_identifier(name), dataset);
    }
    for (relationship_index, relationship) in relationships.iter().enumerate() {
        let Some(relationship) = relationship.as_object() else {
            continue;
        };
        let pointer = format!("/semantic_model/{scope_index}/relationships/{relationship_index}");
        let from = relationship.get("from").and_then(Value::as_str);
        let to = relationship.get("to").and_then(Value::as_str);
        let from_dataset =
            from.and_then(|name| dataset_by_name.get(&normalize_identifier(name)).copied());
        let to_dataset =
            to.and_then(|name| dataset_by_name.get(&normalize_identifier(name)).copied());
        if from.is_some() && from_dataset.is_none() {
            diagnostics.push(diagnostic(
                "ossie.semantic.relationship.from_dataset_unknown",
                "Relationship source dataset does not exist uniquely in this scope.",
                format!("{pointer}/from"),
                scope_id,
            ));
        }
        if to.is_some() && to_dataset.is_none() {
            diagnostics.push(diagnostic(
                "ossie.semantic.relationship.to_dataset_unknown",
                "Relationship target dataset does not exist uniquely in this scope.",
                format!("{pointer}/to"),
                scope_id,
            ));
        }
        let from_columns = relationship.get("from_columns").and_then(Value::as_array);
        let to_columns = relationship.get("to_columns").and_then(Value::as_array);
        if let (Some(from_columns), Some(to_columns)) = (from_columns, to_columns) {
            if from_columns.len() != to_columns.len() {
                diagnostics.push(diagnostic(
                    "ossie.semantic.relationship.key_arity_mismatch",
                    "Relationship source and target key arrays must have equal length.",
                    &pointer,
                    scope_id,
                ));
            }
            validate_key_fields(
                from_dataset,
                from_columns,
                "from",
                &format!("{pointer}/from_columns"),
                scope_id,
                diagnostics,
            );
            let valid_target = validate_key_fields(
                to_dataset,
                to_columns,
                "to",
                &format!("{pointer}/to_columns"),
                scope_id,
                diagnostics,
            );
            if valid_target {
                if let Some(target) = to_dataset {
                    let target_columns = normalized_column_set(to_columns);
                    let mut declared = Vec::new();
                    if let Some(key) = target.get("primary_key").and_then(Value::as_array) {
                        declared.push(normalized_column_set(key));
                    }
                    if let Some(keys) = target.get("unique_keys").and_then(Value::as_array) {
                        for key in keys.iter().filter_map(Value::as_array) {
                            declared.push(normalized_column_set(key));
                        }
                    }
                    if !declared.contains(&target_columns) {
                        diagnostics.push(diagnostic(
                            "ossie.semantic.relationship.target_key_not_unique",
                            "Relationship target columns are not a declared primary or unique key.",
                            format!("{pointer}/to_columns"),
                            scope_id,
                        ));
                    }
                }
            }
        }
    }
}

fn validate_key_fields(
    dataset: Option<&Map<String, Value>>,
    columns: &[Value],
    side: &str,
    pointer: &str,
    scope: Option<&str>,
    diagnostics: &mut Vec<OssieDiagnostic>,
) -> bool {
    let Some(dataset) = dataset else {
        return false;
    };
    let fields = dataset
        .get("fields")
        .and_then(Value::as_array)
        .map(|fields| {
            fields
                .iter()
                .filter_map(Value::as_object)
                .filter_map(|field| field.get("name"))
                .filter_map(Value::as_str)
                .map(normalize_identifier)
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    let mut valid = true;
    for (index, column) in columns.iter().enumerate() {
        if let Some(column) = column.as_str() {
            if !fields.contains(&normalize_identifier(column)) {
                valid = false;
                diagnostics.push(diagnostic(
                    format!("ossie.semantic.relationship.{side}_key_field_unknown"),
                    format!("Relationship {side} key field {column:?} is not declared."),
                    format!("{pointer}/{index}"),
                    scope,
                ));
            }
        }
    }
    valid
}

fn compile_scope(
    value: &Value,
    scope_index: usize,
    scope_pointer: &str,
    scope_id: &str,
    target: OssieTarget,
    diagnostics: &mut Vec<OssieDiagnostic>,
) -> Option<OssieCompiledScope> {
    let scope = value.as_object()?;
    let datasets = scope.get("datasets")?.as_array()?;
    let dataset_names = runtime_names(
        &datasets
            .iter()
            .filter_map(|dataset| dataset.get("name").and_then(Value::as_str))
            .map(str::to_string)
            .collect::<Vec<_>>(),
    );
    let mut models = Vec::new();
    let mut model_index = HashMap::new();

    for (dataset_index, dataset) in datasets.iter().enumerate() {
        let dataset = dataset.as_object()?;
        let name = dataset.get("name")?.as_str()?.to_string();
        let source = dataset.get("source")?.as_str()?.to_string();
        let runtime_name = dataset_names.get(&name)?.clone();
        let Some(is_query) = classify_source(&source, target) else {
            diagnostics.push(diagnostic(
                "ossie.lowering.source_ambiguous",
                format!(
                    "Dataset source for {name:?} is neither a table reference nor one SQL query."
                ),
                format!("{scope_pointer}/datasets/{dataset_index}/source"),
                Some(scope_id),
            ));
            continue;
        };
        let source_field_names = dataset
            .get("fields")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_object)
            .filter_map(|field| field.get("name"))
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect::<Vec<_>>();
        let field_runtime_names = runtime_names(&source_field_names);
        let field_names = source_field_names
            .iter()
            .map(|name| {
                (
                    normalize_identifier(name),
                    field_runtime_names[name].clone(),
                )
            })
            .collect::<HashMap<_, _>>();
        let canonical_columns = |values: &[Value]| {
            values
                .iter()
                .filter_map(Value::as_str)
                .filter_map(|name| field_names.get(&normalize_identifier(name)).cloned())
                .collect::<Vec<_>>()
        };
        let keys = dataset
            .get("primary_key")
            .and_then(Value::as_array)
            .map(|values| canonical_columns(values))
            .unwrap_or_default();
        let unique_keys = dataset
            .get("unique_keys")
            .and_then(Value::as_array)
            .map(|keys| {
                keys.iter()
                    .filter_map(Value::as_array)
                    .map(|key| canonical_columns(key))
                    .collect::<Vec<_>>()
            })
            .filter(|keys| !keys.is_empty());
        let mut dimensions = Vec::new();
        for (field_index, field) in dataset
            .get("fields")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .enumerate()
        {
            let Some(field) = field.as_object() else {
                continue;
            };
            let Some(field_name) = field.get("name").and_then(Value::as_str) else {
                continue;
            };
            let pointer =
                format!("{scope_pointer}/datasets/{dataset_index}/fields/{field_index}/expression");
            let selected = match lower_selected_expression(field.get("expression"), target) {
                Ok(selected) => selected,
                Err(message) => {
                    diagnostics.push(diagnostic(
                        "ossie.lowering.expression_invalid",
                        message,
                        pointer,
                        Some(scope_id),
                    ));
                    continue;
                }
            };
            let Some(sql) = selected else {
                diagnostics.push(diagnostic(
                    "ossie.lowering.expression_unavailable",
                    format!(
                        "Field {name}.{field_name} has no {} or ANSI_SQL expression.",
                        target.label()
                    ),
                    pointer,
                    Some(scope_id),
                ));
                continue;
            };
            if let Err(message) = validate_row_sql(&sql, target) {
                diagnostics.push(diagnostic(
                    "ossie.lowering.expression_invalid",
                    message,
                    pointer,
                    Some(scope_id),
                ));
                continue;
            }
            let logical_data_type = field
                .get("datatype")
                .and_then(Value::as_str)
                .map(str::to_string);
            let declared_is_time = field
                .get("dimension")
                .and_then(Value::as_object)
                .and_then(|dimension| dimension.get("is_time"))
                .and_then(Value::as_bool);
            let effective_is_time = declared_is_time.unwrap_or_else(|| {
                logical_data_type
                    .as_deref()
                    .is_some_and(|value| TEMPORAL_TYPES.contains(&value))
            });
            let dimension_type = if effective_is_time {
                DimensionType::Time
            } else if logical_data_type.as_deref() == Some("Boolean") {
                DimensionType::Boolean
            } else if logical_data_type
                .as_deref()
                .is_some_and(|value| NUMERIC_TYPES.contains(&value))
            {
                DimensionType::Numeric
            } else {
                DimensionType::Categorical
            };
            dimensions.push(Dimension {
                name: field_runtime_names[field_name].clone(),
                r#type: dimension_type,
                logical_data_type,
                declared_is_time,
                sql: Some(sql),
                granularity: None,
                supported_granularities: None,
                label: optional_string(field, "label"),
                description: optional_string(field, "description"),
                metadata: Some(serde_json::json!({"ossie_source_name": field_name})),
                meta: None,
                format: None,
                value_format_name: None,
                parent: None,
                window: None,
                public: true,
            });
        }

        let model = Model {
            name: runtime_name,
            table: (!is_query).then_some(source.clone()),
            sql: is_query.then_some(source),
            source_uri: None,
            extends: None,
            primary_key: keys.first().cloned().unwrap_or_default(),
            primary_key_columns: keys,
            unique_keys,
            dimensions,
            metrics: Vec::new(),
            relationships: Vec::new(),
            segments: Vec::<Segment>::new(),
            pre_aggregations: Vec::new(),
            default_time_dimension: None,
            default_grain: None,
            label: None,
            description: optional_string(dataset, "description"),
            metadata: Some(
                serde_json::json!({"ossie_source_name": name, "ossie_source_kind": if is_query { "query" } else { "table" }}),
            ),
            meta: None,
        };
        model_index.insert(normalize_identifier(&name), models.len());
        models.push(model);
    }

    for relationship in scope
        .get("relationships")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(relationship) = relationship.as_object() else {
            continue;
        };
        let Some(edge_id) = relationship.get("name").and_then(Value::as_str) else {
            continue;
        };
        let Some(from) = relationship.get("from").and_then(Value::as_str) else {
            continue;
        };
        let Some(to) = relationship.get("to").and_then(Value::as_str) else {
            continue;
        };
        let Some(from_index) = model_index.get(&normalize_identifier(from)).copied() else {
            continue;
        };
        let Some(to_index) = model_index.get(&normalize_identifier(to)).copied() else {
            continue;
        };
        let from_field_names = models[from_index]
            .dimensions
            .iter()
            .map(|field| {
                (
                    normalize_identifier(source_name(&field.name, &field.metadata)),
                    field.name.clone(),
                )
            })
            .collect::<HashMap<_, _>>();
        let to_field_names = models[to_index]
            .dimensions
            .iter()
            .map(|field| {
                (
                    normalize_identifier(source_name(&field.name, &field.metadata)),
                    field.name.clone(),
                )
            })
            .collect::<HashMap<_, _>>();
        let from_columns = relationship
            .get("from_columns")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .filter_map(|name| from_field_names.get(&normalize_identifier(name)).cloned())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let to_columns = relationship
            .get("to_columns")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .filter_map(|name| to_field_names.get(&normalize_identifier(name)).cloned())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let target_name = models[to_index].name.clone();
        models[from_index].relationships.push(Relationship {
            name: target_name,
            target_model: None,
            active: true,
            edge_id: Some(edge_id.to_string()),
            r#type: RelationshipType::ManyToOne,
            foreign_key: from_columns.first().cloned(),
            foreign_key_columns: Some(from_columns),
            primary_key: to_columns.first().cloned(),
            primary_key_columns: Some(to_columns),
            through: None,
            through_foreign_key: None,
            through_foreign_key_columns: None,
            related_foreign_key: None,
            related_foreign_key_columns: None,
            sql: None,
            metadata: None,
        });
    }

    let mut metrics = Vec::new();
    let metric_source_names = scope
        .get("metrics")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|metric| metric.get("name").and_then(Value::as_str))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let metric_runtime_names = runtime_names(&metric_source_names);
    let metric_names = metric_source_names
        .iter()
        .map(|name| {
            (
                normalize_identifier(name),
                metric_runtime_names[name].clone(),
            )
        })
        .collect::<HashMap<_, _>>();
    for (metric_index, metric) in scope
        .get("metrics")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
    {
        let Some(metric) = metric.as_object() else {
            continue;
        };
        let Some(name) = metric.get("name").and_then(Value::as_str) else {
            continue;
        };
        let pointer = format!("{scope_pointer}/metrics/{metric_index}/expression");
        let selected = match lower_selected_expression(metric.get("expression"), target) {
            Ok(selected) => selected,
            Err(message) => {
                diagnostics.push(diagnostic(
                    "ossie.lowering.expression_invalid",
                    message,
                    pointer,
                    Some(scope_id),
                ));
                continue;
            }
        };
        let Some(sql) = selected else {
            diagnostics.push(diagnostic(
                "ossie.lowering.expression_unavailable",
                format!(
                    "Metric {name:?} has no {} or ANSI_SQL expression.",
                    target.label()
                ),
                pointer,
                Some(scope_id),
            ));
            continue;
        };
        if let Err(message) = validate_scalar_sql(&sql, target) {
            diagnostics.push(diagnostic(
                "ossie.lowering.expression_invalid",
                message,
                pointer,
                Some(scope_id),
            ));
            continue;
        }
        let expression_dialect =
            selected_expression_dialect(metric.get("expression"), target).unwrap_or_default();
        let sql = match bind_metric_sql(&sql, &models, &metric_names, target, &expression_dialect) {
            Ok(sql) => sql,
            Err(message) => {
                diagnostics.push(diagnostic(
                    "ossie.lowering.metric_unexecutable",
                    message,
                    pointer,
                    Some(scope_id),
                ));
                continue;
            }
        };
        let mut parsed = Metric::new(&metric_runtime_names[name]);
        parsed.r#type = MetricType::Derived;
        parsed.agg = None;
        parsed.sql = Some(sql);
        parsed.sql_is_complete = true;
        parsed.description = optional_string(metric, "description");
        parsed.metadata = Some(serde_json::json!({
            "ossie_expression_dialect": expression_dialect,
            "ossie_target_dialect": target.label(),
            "ossie_source_name": name,
        }));
        parsed.logical_data_type = metric
            .get("datatype")
            .and_then(Value::as_str)
            .map(str::to_string);
        metrics.push(parsed);
    }

    Some(OssieCompiledScope {
        scope_id: scope_id.to_string(),
        semantic_model_index: scope_index,
        target_dialect: target.label().to_string(),
        models,
        metrics,
    })
}

fn classify_source(source: &str, target: OssieTarget) -> Option<bool> {
    if let Ok(expressions) = polyglot_sql::parse(source, target.parser_dialect()) {
        if expressions.len() != 1 {
            return None;
        }
        if matches!(
            expressions[0],
            Expression::Select(_)
                | Expression::Union(_)
                | Expression::Intersect(_)
                | Expression::Except(_)
                | Expression::Subquery(_)
                | Expression::Values(_)
        ) {
            let ast = serde_json::to_value(&expressions[0]).ok()?;
            fn empty_select(value: &Value) -> bool {
                if value.get("select").is_some_and(|select| {
                    select
                        .get("expressions")
                        .and_then(Value::as_array)
                        .is_none_or(Vec::is_empty)
                }) {
                    return true;
                }
                match value {
                    Value::Object(object) => object.values().any(empty_select),
                    Value::Array(values) => values.iter().any(empty_select),
                    _ => false,
                }
            }
            if empty_select(&ast) {
                return None;
            }
            return Some(true);
        }
    }
    if source.split_whitespace().next().is_some_and(|word| {
        matches!(
            word.to_ascii_uppercase().as_str(),
            "SELECT"
                | "WITH"
                | "VALUES"
                | "INSERT"
                | "UPDATE"
                | "DELETE"
                | "CREATE"
                | "DROP"
                | "ALTER"
                | "TRUNCATE"
                | "CALL"
        )
    }) {
        return None;
    }
    let parsed =
        polyglot_sql::parse(&format!("SELECT * FROM {source}"), target.parser_dialect()).ok()?;
    if parsed.len() != 1 {
        return None;
    }
    let Expression::Select(select) = &parsed[0] else {
        return None;
    };
    let from = select.from.as_ref()?;
    if from.expressions.len() != 1
        || !select.joins.is_empty()
        || select.where_clause.is_some()
        || select.group_by.is_some()
        || select.having.is_some()
        || select.order_by.is_some()
        || select.limit.is_some()
        || select.offset.is_some()
        || select.qualify.is_some()
        || select.with.is_some()
    {
        return None;
    }
    let Expression::Table(table) = &from.expressions[0] else {
        return None;
    };
    if table.alias.is_some() || !table.column_aliases.is_empty() || table.name.name.is_empty() {
        return None;
    }
    Some(false)
}

#[cfg(test)]
fn source_is_query(source: &str, target: OssieTarget) -> bool {
    classify_source(source, target) == Some(true)
}

fn select_variant(value: Option<&Value>, target: OssieTarget) -> Option<(String, String)> {
    let variants = value?.as_object()?.get("dialects")?.as_array()?;
    let mut exact = None;
    let mut ansi = None;
    let mut portable = None;
    for variant in variants {
        let variant = variant.as_object()?;
        let dialect = variant.get("dialect")?.as_str()?.to_ascii_uppercase();
        let expression = variant.get("expression")?.as_str()?.to_string();
        if dialect == target.label() && dialect != "ANSI_SQL" && exact.is_none() {
            exact = Some((expression.clone(), dialect.clone()));
        }
        if dialect == "ANSI_SQL" && ansi.is_none() {
            ansi = Some((expression.clone(), dialect.clone()));
        }
        if dialect == "OSSIE_SQL_2026" && portable.is_none() {
            portable = Some((expression, dialect));
        }
    }
    exact.or(portable).or(ansi)
}

#[cfg(test)]
fn select_expression(value: Option<&Value>, target: OssieTarget) -> Option<String> {
    select_variant(value, target).map(|(sql, _)| sql)
}

fn selected_expression_dialect(value: Option<&Value>, target: OssieTarget) -> Option<String> {
    select_variant(value, target).map(|(_, dialect)| dialect)
}

fn lower_selected_expression(
    value: Option<&Value>,
    target: OssieTarget,
) -> std::result::Result<Option<String>, String> {
    let Some((sql, dialect)) = select_variant(value, target) else {
        return Ok(None);
    };
    if dialect == "OSSIE_SQL_2026" {
        super::ossie_sql::lower_ossie_sql(&sql, target.parser_dialect()).map(Some)
    } else {
        Ok(Some(sql))
    }
}

fn validate_scalar_sql(sql: &str, target: OssieTarget) -> std::result::Result<(), String> {
    parse_scalar_sql(sql, target).map(|_| ())
}

fn parse_scalar_sql(sql: &str, target: OssieTarget) -> std::result::Result<Expression, String> {
    crate::semantic_input::with_semantic_stack(|| {
        parse_scalar_sql_inner(sql, target).map_err(SidemanticError::SqlParse)
    })
    .map_err(|error| error.to_string())
}

fn parse_scalar_sql_inner(
    sql: &str,
    target: OssieTarget,
) -> std::result::Result<Expression, String> {
    let wrapped = format!("SELECT {sql}");
    let mut statements =
        crate::semantic_input::dialects::parse_many(&wrapped, target.parser_dialect())
            .map_err(|error| format!("Invalid {} SQL expression: {error}", target.label()))?;
    if statements.len() != 1 {
        return Err("Expected exactly one scalar SQL expression.".into());
    }
    let Expression::Select(mut select) = statements.remove(0) else {
        return Err(format!(
            "Expected one scalar {} SQL expression.",
            target.label()
        ));
    };
    let has_query_clauses = select.from.is_some()
        || !select.joins.is_empty()
        || select.where_clause.is_some()
        || select.group_by.is_some()
        || select.having.is_some()
        || select.qualify.is_some()
        || select.order_by.is_some()
        || select.limit.is_some()
        || select.offset.is_some()
        || select.with.is_some()
        || select.distinct;
    if select.expressions.len() != 1 || has_query_clauses {
        return Err(format!(
            "Expected one scalar {} SQL expression without query clauses.",
            target.label()
        ));
    }
    let expression = select.expressions.remove(0);
    if matches!(
        expression,
        Expression::Alias(_) | Expression::Aliases(_) | Expression::Star(_)
    ) {
        return Err("Expected a scalar value without a projection alias or wildcard.".into());
    }
    let ast = serde_json::to_value(&expression).map_err(|error| error.to_string())?;
    let contains_query = ast_contains_query(&ast);
    if contains_query {
        return Err(format!(
            "Expected one scalar {} SQL expression without a nested query.",
            target.label()
        ));
    }
    Ok(expression)
}

fn ast_contains_query(value: &Value) -> bool {
    match value {
        Value::Object(object) => {
            object.keys().any(|key| {
                matches!(
                    key.as_str(),
                    "select"
                        | "subquery"
                        | "union"
                        | "intersect"
                        | "except"
                        | "values"
                        | "insert"
                        | "update"
                        | "delete"
                        | "create"
                        | "drop"
                        | "command"
                )
            }) || object.values().any(ast_contains_query)
        }
        Value::Array(values) => values.iter().any(ast_contains_query),
        _ => false,
    }
}

fn validate_row_sql(sql: &str, target: OssieTarget) -> std::result::Result<(), String> {
    let expression = parse_scalar_sql(sql, target)?;
    let value = serde_json::to_value(expression).map_err(|error| error.to_string())?;
    fn has_aggregate(value: &Value) -> bool {
        if value
            .as_object()
            .is_some_and(|object| object.contains_key("window_function"))
        {
            return false;
        }
        if crate::core::is_aggregate_ast_node(value) {
            return true;
        }
        match value {
            Value::Object(object) => object.values().any(has_aggregate),
            Value::Array(values) => values.iter().any(has_aggregate),
            _ => false,
        }
    }
    if has_aggregate(&value) {
        return Err(
            "Ossie dataset fields are row-level expressions and cannot contain aggregates.".into(),
        );
    }
    Ok(())
}

fn bind_metric_sql(
    sql: &str,
    models: &[Model],
    metrics: &HashMap<String, String>,
    target: OssieTarget,
    expression_dialect: &str,
) -> std::result::Result<String, String> {
    let expression = parse_scalar_sql(sql, target)?;
    let mut value = serde_json::to_value(expression).map_err(|error| error.to_string())?;
    fn key(identifier: &Value, expression_dialect: &str) -> String {
        let name = identifier["name"].as_str().unwrap_or("");
        if identifier["quoted"].as_bool() == Some(true)
            && !matches!(expression_dialect, "BIGQUERY" | "DATABRICKS")
        {
            name.to_string()
        } else {
            normalize_identifier(name)
        }
    }
    fn runtime_identifier(name: &str) -> Value {
        serde_json::json!({"name": name, "quoted": name.starts_with('"')})
    }
    fn bind(
        value: &mut Value,
        models: &[Model],
        metrics: &HashMap<String, String>,
        expression_dialect: &str,
    ) -> std::result::Result<bool, String> {
        if let Some(column) = value.get_mut("column").and_then(Value::as_object_mut) {
            let field_key = key(&column["name"], expression_dialect);
            if column.get("table").is_none_or(Value::is_null) {
                if let Some(name) = metrics.get(&field_key) {
                    if column["name"]["name"].as_str() != Some(name.as_str()) {
                        column.insert("name".into(), runtime_identifier(name));
                        return Ok(true);
                    }
                    return Ok(false);
                }
            }
            let model = if column.get("table").is_some_and(|table| !table.is_null()) {
                let table_key = key(&column["table"], expression_dialect);
                Some(
                    models
                        .iter()
                        .find(|model| {
                            normalize_identifier(source_name(&model.name, &model.metadata))
                                == table_key
                        })
                        .ok_or_else(|| {
                            format!(
                                "Unknown logical dataset in column reference {:?}.",
                                column["table"]["name"]
                            )
                        })?,
                )
            } else {
                let candidates = models
                    .iter()
                    .filter(|model| {
                        model.dimensions.iter().any(|field| {
                            normalize_identifier(source_name(&field.name, &field.metadata))
                                == field_key
                        })
                    })
                    .collect::<Vec<_>>();
                if candidates.len() == 1 {
                    Some(candidates[0])
                } else if candidates.is_empty() && models.len() == 1 {
                    Some(&models[0])
                } else {
                    None
                }
            };
            if let Some(model) = model {
                let field = model.dimensions.iter().find(|field| {
                    normalize_identifier(source_name(&field.name, &field.metadata)) == field_key
                });
                let field_name = field
                    .map(|field| field.name.as_str())
                    .unwrap_or_else(|| column["name"]["name"].as_str().unwrap_or(""));
                if column["table"]["name"].as_str() != Some(model.name.as_str())
                    || column["name"]["name"].as_str() != Some(field_name)
                {
                    let field_name = field_name.to_string();
                    column.insert("table".into(), runtime_identifier(&model.name));
                    column.insert("name".into(), runtime_identifier(&field_name));
                    return Ok(true);
                }
            }
            return Ok(false);
        }
        let mut changed = false;
        match value {
            Value::Object(object) => {
                for child in object.values_mut() {
                    changed |= bind(child, models, metrics, expression_dialect)?;
                }
            }
            Value::Array(values) => {
                for child in values {
                    changed |= bind(child, models, metrics, expression_dialect)?;
                }
            }
            _ => {}
        }
        Ok(changed)
    }
    if !bind(&mut value, models, metrics, expression_dialect)? {
        return Ok(sql.to_string());
    }
    let expression: Expression =
        serde_json::from_value(value).map_err(|error| error.to_string())?;
    polyglot_sql::generate(&expression, target.parser_dialect()).map_err(|error| error.to_string())
}

fn duplicate_names(
    values: &[Value],
    pointer: &str,
    code: &str,
    scope: Option<&str>,
    diagnostics: &mut Vec<OssieDiagnostic>,
) {
    let mut first = BTreeMap::new();
    for (index, value) in values.iter().enumerate() {
        let Some(name) = value
            .as_object()
            .and_then(|value| value.get("name"))
            .and_then(Value::as_str)
        else {
            continue;
        };
        if !identifier_syntax_valid(name) {
            diagnostics.push(diagnostic("ossie.semantic.identifier.invalid", "Expected an ANSI regular identifier or a non-empty double-quoted identifier with doubled interior quotes.", format!("{pointer}/{index}/name"), scope));
        }
        if identifier_length(name) > 128 {
            diagnostics.push(diagnostic(
                "ossie.semantic.identifier.length_exceeded",
                "Ossie identifiers are limited to 128 decoded characters.",
                format!("{pointer}/{index}/name"),
                scope,
            ));
        }
        if let Some(first_index) = first.insert(normalize_identifier(name), index) {
            diagnostics.push(diagnostic(
                code,
                format!("Duplicate name {name:?}; first declared at index {first_index}."),
                format!("{pointer}/{index}/name"),
                scope,
            ));
        }
    }
}

fn normalize_identifier(identifier: &str) -> String {
    if identifier.len() >= 2 && identifier.starts_with('"') && identifier.ends_with('"') {
        identifier[1..identifier.len() - 1].replace("\"\"", "\"")
    } else {
        identifier.to_uppercase()
    }
}

fn identifier_length(identifier: &str) -> usize {
    if identifier.starts_with('"') && identifier.ends_with('"') && identifier.len() >= 2 {
        normalize_identifier(identifier).chars().count()
    } else {
        identifier.chars().count()
    }
}

fn identifier_syntax_valid(identifier: &str) -> bool {
    if identifier.starts_with('"') {
        if identifier.len() < 3 || !identifier.ends_with('"') {
            return false;
        }
        let mut body = identifier[1..identifier.len() - 1].chars();
        while let Some(character) = body.next() {
            if character == '\0' || character == '"' && body.next() != Some('"') {
                return false;
            }
        }
        return true;
    }
    let mut characters = identifier.chars();
    characters
        .next()
        .is_some_and(|character| character == '_' || character.is_alphabetic())
        && characters.all(|character| character == '_' || character.is_alphanumeric())
}

fn source_name<'a>(name: &'a str, metadata: &'a Option<Value>) -> &'a str {
    metadata
        .as_ref()
        .and_then(|metadata| metadata.get("ossie_source_name"))
        .and_then(Value::as_str)
        .unwrap_or(name)
}

fn runtime_names(names: &[String]) -> HashMap<String, String> {
    let quoted = |name: &str| name.len() >= 2 && name.starts_with('"') && name.ends_with('"');
    let candidates = names
        .iter()
        .map(|name| {
            (
                name.clone(),
                if quoted(name) {
                    normalize_identifier(name)
                } else {
                    name.clone()
                },
            )
        })
        .collect::<HashMap<_, _>>();
    let mut counts = HashMap::new();
    for candidate in candidates.values() {
        *counts
            .entry(candidate.to_ascii_lowercase())
            .or_insert(0usize) += 1;
    }
    let mut used = candidates
        .values()
        .map(|candidate| candidate.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let mut ordered = names.to_vec();
    ordered.sort();
    let mut result = HashMap::new();
    for name in ordered {
        let candidate = &candidates[&name];
        let mut characters = candidate.chars();
        let safe = characters
            .next()
            .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
            && characters.all(|character| character.is_ascii_alphanumeric() || character == '_');
        if safe && (!quoted(&name) || counts[&candidate.to_ascii_lowercase()] == 1) {
            result.insert(name, candidate.clone());
            continue;
        }
        // Stable non-security identifier fingerprint, shared with Python lowering.
        let fingerprint = name.bytes().fold(0xcbf29ce484222325u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
        });
        let base = format!("__ossie_{fingerprint:016x}");
        let mut candidate = base.clone();
        let mut suffix = 1;
        while used.contains(&candidate.to_ascii_lowercase()) {
            candidate = format!("{base}_{suffix}");
            suffix += 1;
        }
        used.insert(candidate.to_ascii_lowercase());
        result.insert(name, candidate);
    }
    result
}

fn normalized_strings(values: &[Value]) -> Vec<String> {
    values
        .iter()
        .filter_map(Value::as_str)
        .map(normalize_identifier)
        .collect()
}

fn normalized_column_set(values: &[Value]) -> BTreeSet<String> {
    normalized_strings(values).into_iter().collect()
}

fn reject_unknown(
    object: &Map<String, Value>,
    allowed: &[&str],
    pointer: &str,
    diagnostics: &mut Vec<OssieDiagnostic>,
    scope: Option<&str>,
) {
    for key in object.keys().filter(|key| !allowed.contains(&key.as_str())) {
        diagnostics.push(diagnostic(
            "ossie.schema.additional_properties",
            format!("Unexpected property {key:?}."),
            pointer,
            scope,
        ));
    }
    validate_metadata(object, pointer, diagnostics, scope);
}

fn validate_metadata(
    object: &Map<String, Value>,
    pointer: &str,
    diagnostics: &mut Vec<OssieDiagnostic>,
    scope: Option<&str>,
) {
    for name in ["description", "label"] {
        if object.get(name).is_some_and(|value| !value.is_string()) {
            diagnostics.push(diagnostic(
                "ossie.schema.type",
                format!("{name} must be a string."),
                format!("{pointer}/{name}"),
                scope,
            ));
        }
    }
    if let Some(context) = object.get("ai_context") {
        if let Some(context) = context.as_object() {
            if context
                .get("instructions")
                .is_some_and(|value| !value.is_string())
            {
                diagnostics.push(diagnostic(
                    "ossie.schema.type",
                    "AI instructions must be a string.",
                    format!("{pointer}/ai_context/instructions"),
                    scope,
                ));
            }
            for name in ["synonyms", "examples"] {
                if let Some(values) = context.get(name) {
                    if !values
                        .as_array()
                        .is_some_and(|values| values.iter().all(Value::is_string))
                    {
                        diagnostics.push(diagnostic(
                            "ossie.schema.type",
                            format!("AI {name} must be an array of strings."),
                            format!("{pointer}/ai_context/{name}"),
                            scope,
                        ));
                    }
                }
            }
        } else if !context.is_string() {
            diagnostics.push(diagnostic(
                "ossie.schema.type",
                "ai_context must be a string or object.",
                format!("{pointer}/ai_context"),
                scope,
            ));
        }
    }
    if let Some(extensions) =
        optional_array(object, "custom_extensions", pointer, diagnostics, scope)
    {
        for (index, extension) in extensions.iter().enumerate() {
            let extension_pointer = format!("{pointer}/custom_extensions/{index}");
            if let Some(extension) =
                require_object(extension, &extension_pointer, diagnostics, scope)
            {
                for key in extension
                    .keys()
                    .filter(|key| !matches!(key.as_str(), "vendor_name" | "data"))
                {
                    diagnostics.push(diagnostic(
                        "ossie.schema.additional_properties",
                        format!("Unexpected extension property {key:?}."),
                        &extension_pointer,
                        scope,
                    ));
                }
                for key in ["vendor_name", "data"] {
                    match extension.get(key) {
                        None => diagnostics.push(diagnostic(
                            "ossie.schema.required",
                            format!("Required extension property {key:?} is missing."),
                            &extension_pointer,
                            scope,
                        )),
                        Some(value) if !value.is_string() => diagnostics.push(diagnostic(
                            "ossie.schema.type",
                            format!("Extension {key} must be a string."),
                            format!("{extension_pointer}/{key}"),
                            scope,
                        )),
                        _ => {}
                    }
                }
            }
        }
    }
}

fn require_object<'a>(
    value: &'a Value,
    pointer: &str,
    diagnostics: &mut Vec<OssieDiagnostic>,
    scope: Option<&str>,
) -> Option<&'a Map<String, Value>> {
    match value.as_object() {
        Some(value) => Some(value),
        None => {
            diagnostics.push(diagnostic(
                "ossie.schema.type",
                "Expected an object.",
                pointer,
                scope,
            ));
            None
        }
    }
}

fn required_array<'a>(
    object: &'a Map<String, Value>,
    key: &str,
    pointer: &str,
    diagnostics: &mut Vec<OssieDiagnostic>,
    scope: Option<&str>,
) -> Option<&'a Vec<Value>> {
    let Some(value) = object.get(key) else {
        diagnostics.push(diagnostic(
            "ossie.schema.required",
            format!("Required property {key:?} is missing."),
            pointer,
            scope,
        ));
        return None;
    };
    match value.as_array() {
        Some(value) => Some(value),
        None => {
            diagnostics.push(diagnostic(
                "ossie.schema.type",
                format!("Property {key:?} must be an array."),
                format!("{pointer}/{key}"),
                scope,
            ));
            None
        }
    }
}

fn optional_array<'a>(
    object: &'a Map<String, Value>,
    key: &str,
    pointer: &str,
    diagnostics: &mut Vec<OssieDiagnostic>,
    scope: Option<&str>,
) -> Option<&'a Vec<Value>> {
    let value = object.get(key)?;
    match value.as_array() {
        Some(value) => Some(value),
        None => {
            diagnostics.push(diagnostic(
                "ossie.schema.type",
                format!("Property {key:?} must be an array."),
                format!("{pointer}/{key}"),
                scope,
            ));
            None
        }
    }
}

fn required_string<'a>(
    object: &'a Map<String, Value>,
    key: &str,
    pointer: &str,
    diagnostics: &mut Vec<OssieDiagnostic>,
    scope: Option<&str>,
) -> Option<&'a str> {
    let Some(value) = object.get(key) else {
        diagnostics.push(diagnostic(
            "ossie.schema.required",
            format!("Required property {key:?} is missing."),
            pointer,
            scope,
        ));
        return None;
    };
    match value.as_str().filter(|value| !value.is_empty()) {
        Some(value) => {
            if matches!(key, "from" | "to") && identifier_length(value) > 128 {
                diagnostics.push(diagnostic(
                    "ossie.semantic.identifier.length_exceeded",
                    "Ossie identifiers are limited to 128 decoded characters.",
                    format!("{pointer}/{key}"),
                    scope,
                ));
            }
            Some(value)
        }
        None => {
            diagnostics.push(diagnostic(
                "ossie.schema.type",
                format!("Property {key:?} must be a non-empty string."),
                format!("{pointer}/{key}"),
                scope,
            ));
            None
        }
    }
}

fn validate_string_array(
    value: Option<&Value>,
    pointer: &str,
    diagnostics: &mut Vec<OssieDiagnostic>,
    scope: Option<&str>,
) {
    let Some(value) = value else {
        return;
    };
    let Some(values) = value.as_array() else {
        diagnostics.push(diagnostic(
            "ossie.schema.type",
            "Expected an array of strings.",
            pointer,
            scope,
        ));
        return;
    };
    for (index, value) in values.iter().enumerate() {
        if value.as_str().filter(|value| !value.is_empty()).is_none() {
            diagnostics.push(diagnostic(
                "ossie.schema.type",
                "Expected a non-empty string.",
                format!("{pointer}/{index}"),
                scope,
            ));
        }
    }
}

fn validate_nested_string_array(
    value: Option<&Value>,
    pointer: &str,
    diagnostics: &mut Vec<OssieDiagnostic>,
    scope: Option<&str>,
) {
    let Some(value) = value else {
        return;
    };
    let Some(values) = value.as_array() else {
        diagnostics.push(diagnostic(
            "ossie.schema.type",
            "Expected an array of string arrays.",
            pointer,
            scope,
        ));
        return;
    };
    for (index, value) in values.iter().enumerate() {
        validate_string_array(
            Some(value),
            &format!("{pointer}/{index}"),
            diagnostics,
            scope,
        );
    }
}

fn optional_string(object: &Map<String, Value>, key: &str) -> Option<String> {
    object.get(key).and_then(Value::as_str).map(str::to_string)
}

fn diagnostic(
    code: impl Into<String>,
    message: impl Into<String>,
    instance_path: impl Into<String>,
    scope: Option<&str>,
) -> OssieDiagnostic {
    OssieDiagnostic {
        code: code.into(),
        severity: "error",
        message: message.into(),
        instance_path: instance_path.into(),
        scope: scope.map(str::to_string),
    }
}

fn sort_diagnostics(diagnostics: &mut [OssieDiagnostic]) {
    diagnostics.sort_by(|left, right| {
        (&left.instance_path, &left.code, &left.scope).cmp(&(
            &right.instance_path,
            &right.code,
            &right.scope,
        ))
    });
}

fn status_error(status: &OssieStatus) -> SidemanticError {
    let payload = serde_json::to_string(status).unwrap_or_else(|_| "{}".to_string());
    SidemanticError::Validation(format!("Ossie strict handoff rejected input: {payload}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flat_document() -> Value {
        serde_json::json!({
            "version": "0.2.0.dev0", "name": "commerce",
            "datasets": [{"name": "Orders", "source": "SELECT 10 AS amount UNION ALL SELECT 20 AS amount",
                "fields": [{"name": "Amount", "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": "amount * 2"}]}}]}],
            "metrics": [{"name": "total", "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": "SUM(orders.amount)"}]}}]
        })
    }

    #[test]
    fn flat_expression_import_preserves_logical_complete_sql() {
        let scope = OssieForwardAdapter
            .select_scope(
                &flat_document().to_string(),
                OssieSerialization::Json,
                OssieConsumerProfile::OssieCore,
                OssieTarget::DuckDb,
                None,
            )
            .unwrap();
        assert_eq!(scope.models[0].name, "Orders");
        assert_eq!(scope.metrics[0].sql.as_deref(), Some("SUM(Orders.Amount)"));
        assert!(scope.metrics[0].sql_is_complete);
        assert!(scope.metrics[0].agg.is_none());
        assert_eq!(
            scope.metrics[0].metadata.as_ref().unwrap()["ossie_expression_dialect"],
            "ANSI_SQL"
        );
    }

    #[test]
    fn unsafe_sources_fail_inspection_and_compilation() {
        for source in [
            "DELETE FROM orders",
            "1 + 2",
            "SELECT",
            "orders; DROP TABLE orders",
            "orders AS aliased",
        ] {
            let mut document = flat_document();
            document["datasets"][0]["source"] = source.into();
            let content = document.to_string();
            let status = OssieForwardAdapter.inspect(
                &content,
                OssieSerialization::Json,
                OssieConsumerProfile::OssieCore,
            );
            assert!(!status.valid && !status.executable, "{source}: {status:?}");
            assert!(
                OssieForwardAdapter
                    .parse_catalog(
                        &content,
                        OssieSerialization::Json,
                        OssieConsumerProfile::OssieCore,
                        OssieTarget::DuckDb
                    )
                    .is_err(),
                "{source}"
            );
        }
    }

    #[test]
    fn graph_boundary_validates_complete_expressions_and_dependency_cycles() {
        for sql in [
            "STDDEV_SAMP(orders.amount)",
            "VAR_POP(orders.amount)",
            "QUANTILE_CONT(orders.amount, 0.5 ORDER BY orders.amount DESC)",
        ] {
            let mut document = flat_document();
            document["metrics"][0]["expression"]["dialects"][0]["expression"] = sql.into();
            let scope = OssieForwardAdapter
                .select_scope(
                    &document.to_string(),
                    OssieSerialization::Json,
                    OssieConsumerProfile::OssieCore,
                    OssieTarget::DuckDb,
                    None,
                )
                .unwrap();
            assert!(scope.into_graph().is_ok(), "{sql}");
        }
        for (second, valid) in [("SUM(orders.amount)", true), ("total + 1", false)] {
            let mut document = flat_document();
            document["metrics"][0]["expression"]["dialects"][0]["expression"] = "later + 1".into();
            document["metrics"].as_array_mut().unwrap().push(serde_json::json!({"name":"later", "expression":{"dialects":[{"dialect":"ANSI_SQL", "expression":second}]}}));
            let scope = OssieForwardAdapter
                .select_scope(
                    &document.to_string(),
                    OssieSerialization::Json,
                    OssieConsumerProfile::OssieCore,
                    OssieTarget::DuckDb,
                    None,
                )
                .unwrap();
            assert_eq!(scope.into_graph().is_ok(), valid, "{second}");
        }
    }

    #[test]
    fn row_aggregates_and_projection_aliases_fail_closed() {
        for expression in [
            "SUM(amount)",
            "STDDEV(amount)",
            "COUNT(*)",
            "amount AS renamed",
            "*",
            "SUM((SELECT 1))",
        ] {
            let mut document = flat_document();
            document["datasets"][0]["fields"][0]["expression"]["dialects"][0]["expression"] =
                expression.into();
            let error = OssieForwardAdapter
                .parse_catalog(
                    &document.to_string(),
                    OssieSerialization::Json,
                    OssieConsumerProfile::OssieCore,
                    OssieTarget::DuckDb,
                )
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("ossie.lowering.expression_invalid"),
                "{expression}: {error}"
            );
        }
    }

    #[test]
    fn quoted_and_regular_source_names_have_distinct_runtime_bindings() {
        let mut document = flat_document();
        let mut quoted = document["datasets"][0].clone();
        quoted["name"] = "\"Orders\"".into();
        document["datasets"].as_array_mut().unwrap().push(quoted);
        document["metrics"].as_array_mut().unwrap().push(serde_json::json!({
            "name": "quoted", "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": "SUM(\"Orders\".amount)"}]}
        }));
        let scope = OssieForwardAdapter
            .select_scope(
                &document.to_string(),
                OssieSerialization::Json,
                OssieConsumerProfile::OssieCore,
                OssieTarget::DuckDb,
                None,
            )
            .unwrap();
        assert_ne!(
            scope.models[0].name.to_lowercase(),
            scope.models[1].name.to_lowercase()
        );
        assert_eq!(
            scope.models[1].metadata.as_ref().unwrap()["ossie_source_name"],
            "\"Orders\""
        );
        assert!(scope.metrics[1]
            .sql
            .as_ref()
            .unwrap()
            .contains(&scope.models[1].name));
    }

    #[test]
    fn duplicate_json_keys_and_invalid_metadata_are_rejected() {
        let duplicate = r#"{"version":"0.2.0.dev0","name":"first","name":"second","datasets":[{"name":"orders","source":"orders"}]}"#;
        let status = OssieForwardAdapter.inspect(
            duplicate,
            OssieSerialization::Json,
            OssieConsumerProfile::OssieCore,
        );
        assert!(!status.valid);
        assert_eq!(status.diagnostics[0].code, "ossie.parse.duplicate_key");
        for (name, value) in [
            ("description", serde_json::json!(42)),
            ("ai_context", serde_json::json!([])),
            (
                "custom_extensions",
                serde_json::json!([{"vendor_name":"example","data":{}}]),
            ),
        ] {
            let mut document = flat_document();
            document["datasets"][0][name] = value;
            assert!(
                !OssieForwardAdapter
                    .inspect(
                        &document.to_string(),
                        OssieSerialization::Json,
                        OssieConsumerProfile::OssieCore
                    )
                    .valid,
                "{name}"
            );
        }
    }

    #[test]
    fn yaml_merge_overrides_are_preserved_and_expansion_is_bounded() {
        let content = r#"
version: 0.2.0.dev0
name: commerce
datasets:
  - name: orders
    source: orders
    fields:
      - &field
        name: amount
        expression: {dialects: [{dialect: ANSI_SQL, expression: amount}]}
      - <<: *field
        name: other_amount
"#;
        let status = OssieForwardAdapter.inspect(
            content,
            OssieSerialization::Yaml,
            OssieConsumerProfile::OssieCore,
        );
        assert!(status.valid, "{status:?}");
        let oversized = format!("{}0{}", "[".repeat(300), "]".repeat(300));
        assert!(
            !OssieForwardAdapter
                .inspect(
                    &oversized,
                    OssieSerialization::Json,
                    OssieConsumerProfile::OssieCore
                )
                .valid
        );
    }

    #[test]
    fn current_ontology_prefixes_are_valid_but_not_executable() {
        let document = serde_json::json!({"version":"0.2.0.dev0", "name":"business",
            "prefixes":{"ex":"https://example.com/"}, "requires":[], "ontology":[{"concept":"Order", "type":"EntityType", "iri":"ex:Order"}]});
        let status = OssieForwardAdapter.inspect(
            &document.to_string(),
            OssieSerialization::Json,
            OssieConsumerProfile::OssieCore,
        );
        assert!(status.valid, "{status:?}");
        assert!(!status.executable);
        assert_eq!(
            status.profile.unwrap().schema_revision.as_deref(),
            Some(CURRENT_SCHEMA_REVISION)
        );
    }

    const MULTI_SCOPE: &str = r#"
version: 0.2.0.dev0
semantic_model:
  - name: commerce
    datasets:
      - name: orders
        source: analytics.orders
        primary_key: [id]
        fields:
          - name: id
            datatype: Integer
            dimension: {is_time: false}
            expression:
              dialects:
                - {dialect: ANSI_SQL, expression: id}
                - {dialect: BIGQUERY, expression: SAFE_CAST(id AS INT64)}
                - {dialect: MDX, expression: "[Orders].[Id]"}
  - name: operations
    datasets:
      - name: orders
        source: operations.orders
"#;

    #[test]
    fn profile_contracts_are_explicit() {
        let core = OssieForwardAdapter.inspect(
            "{\"version\":\"0.1.1\",\"semantic_model\":[]}",
            OssieSerialization::Json,
            OssieConsumerProfile::OssieCore,
        );
        assert!(core.valid);
        assert_eq!(core.profile.unwrap().identifier, "ossie-core:0.1.1");

        let alias = OssieForwardAdapter.inspect(
            "{\"version\":\"0.1.0\",\"semantic_model\":[]}",
            OssieSerialization::Json,
            OssieConsumerProfile::Dbt112,
        );
        assert!(alias.valid);
        let profile = alias.profile.unwrap();
        assert_eq!(profile.identifier, "dbt-1.12:0.1.0");
        assert_eq!(profile.compatibility_alias_for.as_deref(), Some("0.1.1"));

        let dbt_released = OssieForwardAdapter.inspect(
            "{\"version\":\"0.1.1\",\"semantic_model\":[]}",
            OssieSerialization::Json,
            OssieConsumerProfile::Dbt112,
        );
        assert!(dbt_released.valid, "{:?}", dbt_released.diagnostics);
        assert_eq!(dbt_released.profile.unwrap().identifier, "dbt-1.12:0.1.1");
    }

    #[test]
    fn executable_targets_cover_runtime_dialects() {
        assert_eq!(OssieTarget::parse("duckdb"), Ok(OssieTarget::DuckDb));
        assert_eq!(OssieTarget::parse("postgresql"), Ok(OssieTarget::Postgres));
        assert_eq!(OssieTarget::parse("snowflake"), Ok(OssieTarget::Snowflake));
        assert_eq!(
            OssieTarget::parse("databricks"),
            Ok(OssieTarget::Databricks)
        );
        assert_eq!(OssieTarget::parse("bigquery"), Ok(OssieTarget::BigQuery));
    }

    #[test]
    fn expression_selection_is_exact_then_ansi_for_every_runtime_target() {
        let expression = serde_json::json!({
            "dialects": [
                {"dialect": "ANSI_SQL", "expression": "ansi_value"},
                {"dialect": "SNOWFLAKE", "expression": "snowflake_value"},
                {"dialect": "DATABRICKS", "expression": "databricks_value"},
                {"dialect": "BIGQUERY", "expression": "bigquery_value"}
            ]
        });

        assert_eq!(
            select_expression(Some(&expression), OssieTarget::Snowflake).as_deref(),
            Some("snowflake_value")
        );
        assert_eq!(
            select_expression(Some(&expression), OssieTarget::Databricks).as_deref(),
            Some("databricks_value")
        );
        assert_eq!(
            select_expression(Some(&expression), OssieTarget::BigQuery).as_deref(),
            Some("bigquery_value")
        );
        assert_eq!(
            select_expression(Some(&expression), OssieTarget::DuckDb).as_deref(),
            Some("ansi_value")
        );
        assert_eq!(
            select_expression(Some(&expression), OssieTarget::Postgres).as_deref(),
            Some("ansi_value")
        );
    }

    #[test]
    fn source_classification_handles_relations_and_all_query_shapes() {
        for source in [
            "SELECT * FROM raw.orders",
            "(SELECT * FROM raw.orders)",
            "WITH recent AS (SELECT * FROM raw.orders) SELECT * FROM recent",
            "VALUES (1)",
        ] {
            assert!(source_is_query(source, OssieTarget::DuckDb), "{source}");
        }
        for source in ["analytics.orders", "\"analytics\".\"orders\""] {
            assert!(!source_is_query(source, OssieTarget::DuckDb), "{source}");
        }
    }

    #[test]
    fn regular_identifier_references_bind_to_declared_runtime_names() {
        let content = r#"
version: 0.2.0.dev0
semantic_model:
  - name: Commerce
    datasets:
      - name: Orders
        source: analytics.orders
        primary_key: [ID]
        fields:
          - name: Id
            expression: {dialects: [{dialect: ANSI_SQL, expression: id}]}
          - name: Customer_Id
            expression: {dialects: [{dialect: ANSI_SQL, expression: customer_id}]}
      - name: Customers
        source: analytics.customers
        primary_key: [id]
        fields:
          - name: ID
            expression: {dialects: [{dialect: ANSI_SQL, expression: id}]}
    relationships:
      - name: Order_Customer
        from: ORDERS
        to: customers
        from_columns: [CUSTOMER_ID]
        to_columns: [Id]
"#;
        let catalog = OssieForwardAdapter
            .parse_catalog(
                content,
                OssieSerialization::Yaml,
                OssieConsumerProfile::OssieCore,
                OssieTarget::DuckDb,
            )
            .unwrap();
        let scope = &catalog.scopes[0];
        assert_eq!(scope.models[0].primary_keys(), vec!["Id"]);
        let relationship = &scope.models[0].relationships[0];
        assert_eq!(relationship.name, "Customers");
        assert_eq!(relationship.foreign_key_columns(), vec!["Customer_Id"]);
        assert_eq!(relationship.primary_key_columns(), vec!["ID"]);
    }

    #[test]
    fn scalar_validation_rejects_query_constructs() {
        for sql in [
            "id FROM orders",
            "id WHERE active",
            "(SELECT id FROM orders)",
            "id IN (SELECT id FROM orders)",
        ] {
            assert!(
                validate_scalar_sql(sql, OssieTarget::DuckDb).is_err(),
                "{sql}"
            );
        }
        assert!(
            validate_scalar_sql("CASE WHEN active THEN id ELSE 0 END", OssieTarget::DuckDb).is_ok()
        );
    }

    #[test]
    fn scopes_remain_separate_and_selection_is_explicit() {
        let adapter = OssieForwardAdapter;
        let catalog = adapter
            .parse_catalog(
                MULTI_SCOPE,
                OssieSerialization::Yaml,
                OssieConsumerProfile::OssieCore,
                OssieTarget::BigQuery,
            )
            .unwrap();
        assert_eq!(
            catalog
                .scopes
                .iter()
                .map(|scope| scope.scope_id.as_str())
                .collect::<Vec<_>>(),
            vec!["commerce", "operations"]
        );
        assert_eq!(
            catalog.scopes[0].models[0].dimensions[0].sql.as_deref(),
            Some("SAFE_CAST(id AS INT64)")
        );
        assert_eq!(
            catalog.scopes[0].models[0].dimensions[0].declared_is_time,
            Some(false)
        );
        assert!(adapter
            .select_scope(
                MULTI_SCOPE,
                OssieSerialization::Yaml,
                OssieConsumerProfile::OssieCore,
                OssieTarget::BigQuery,
                None,
            )
            .unwrap_err()
            .to_string()
            .contains("ambiguous"));
        let operations = adapter
            .select_scope(
                MULTI_SCOPE,
                OssieSerialization::Yaml,
                OssieConsumerProfile::OssieCore,
                OssieTarget::BigQuery,
                Some("operations"),
            )
            .unwrap();
        assert_eq!(
            operations.models[0].table.as_deref(),
            Some("operations.orders")
        );
    }

    #[test]
    fn ansi_fallback_ignores_non_sql_variants() {
        let scope = OssieForwardAdapter
            .select_scope(
                MULTI_SCOPE,
                OssieSerialization::Yaml,
                OssieConsumerProfile::OssieCore,
                OssieTarget::AnsiSql,
                Some("commerce"),
            )
            .unwrap();
        assert_eq!(scope.models[0].dimensions[0].sql.as_deref(), Some("id"));
    }

    #[test]
    fn non_sql_only_expression_fails_closed() {
        let document = r#"
version: 0.2.0.dev0
semantic_model:
  - name: commerce
    datasets:
      - name: orders
        source: analytics.orders
        fields:
          - name: id
            expression:
              dialects:
                - {dialect: MDX, expression: "[Orders].[Id]"}
"#;

        let error = OssieForwardAdapter
            .select_scope(
                document,
                OssieSerialization::Yaml,
                OssieConsumerProfile::OssieCore,
                OssieTarget::BigQuery,
                Some("commerce"),
            )
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("ossie.lowering.expression_unavailable"));
    }
}
