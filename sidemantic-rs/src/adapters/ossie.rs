//! Forward Apache Ossie importer.
//!
//! This is intentionally separate from [`super::osi`], whose permissive legacy
//! behavior is retained for compatibility. The forward adapter is strict,
//! scope-preserving, target-aware, and fail-closed.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use polyglot_sql::{DialectType, Expression, ExpressionWalk};
use serde::Serialize;
use serde_json::{Map, Value};

use crate::config::schema::metric_from_sql_expression;
use crate::core::{
    Dimension, DimensionType, Metric, Model, Relationship, RelationshipType, Segment,
};
use crate::error::{Result, SidemanticError};

const VALIDATION_MODE: &str = "closed_structural_subset";
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
];

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
        self.inspect_document(content, serialization, consumer)
            .status()
    }

    pub fn parse_catalog(
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
            if let Some(scope) = compile_scope(value, index, &scope_id, target, &mut diagnostics) {
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
        let parsed = match serialization {
            OssieSerialization::Json => {
                serde_json::from_str(content).map_err(|error| error.to_string())
            }
            OssieSerialization::Yaml => {
                serde_yaml::from_str(content).map_err(|error| error.to_string())
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
                    diagnostics: vec![diagnostic(
                        "ossie.parse.invalid_syntax",
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
                diagnostics: vec![diagnostic(
                    "ossie.schema.type",
                    "The Ossie document root must be an object.",
                    "",
                    None,
                )],
            };
        };

        let kind = classify_document(object);
        let mut diagnostics = Vec::new();
        let profile = resolve_profile(object.get("version"), consumer, &mut diagnostics);
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
        sort_diagnostics(&mut diagnostics);

        InspectedDocument {
            root: Some(root),
            profile,
            kind,
            scope_ids,
            diagnostics,
        }
    }
}

fn classify_document(root: &Map<String, Value>) -> Option<DocumentKind> {
    let logical = root.contains_key("semantic_model");
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
    })
}

fn validate_logical_root(
    root: &Map<String, Value>,
    profile: Option<&OssieProfile>,
    diagnostics: &mut Vec<OssieDiagnostic>,
) {
    let validation_version = profile.map(|profile| profile.validation_schema_version.as_str());
    let allowed_root = if validation_version == Some("0.1.1") {
        &["version", "dialects", "vendors", "semantic_model"][..]
    } else {
        &["version", "semantic_model"][..]
    };
    reject_unknown(root, allowed_root, "", diagnostics, None);

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
    required_string(dataset, "source", pointer, diagnostics, None);
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
    let allowed = if version == Some("0.2.0.dev0") {
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
    let allowed = if version == Some("0.2.0.dev0") {
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
    let allowed_dialects = if version == Some("0.2.0.dev0") {
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
            if !allowed_dialects.contains(&normalized.as_str()) {
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
        ],
        "",
        diagnostics,
        None,
    );
    if let Some(ontology) = root.get("ontology") {
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
                    let target_columns = normalized_strings(to_columns);
                    let mut declared = Vec::new();
                    if let Some(key) = target.get("primary_key").and_then(Value::as_array) {
                        declared.push(normalized_strings(key));
                    }
                    if let Some(keys) = target.get("unique_keys").and_then(Value::as_array) {
                        for key in keys.iter().filter_map(Value::as_array) {
                            declared.push(normalized_strings(key));
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
    scope_id: &str,
    target: OssieTarget,
    diagnostics: &mut Vec<OssieDiagnostic>,
) -> Option<OssieCompiledScope> {
    let scope = value.as_object()?;
    let datasets = scope.get("datasets")?.as_array()?;
    let mut models = Vec::new();
    let mut model_index = HashMap::new();

    for (dataset_index, dataset) in datasets.iter().enumerate() {
        let dataset = dataset.as_object()?;
        let name = dataset.get("name")?.as_str()?.to_string();
        let source = dataset.get("source")?.as_str()?.to_string();
        let field_names = dataset
            .get("fields")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_object)
            .filter_map(|field| field.get("name"))
            .filter_map(Value::as_str)
            .map(|name| (normalize_identifier(name), name.to_string()))
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
            let pointer = format!(
                "/semantic_model/{scope_index}/datasets/{dataset_index}/fields/{field_index}/expression"
            );
            let Some(sql) = select_expression(field.get("expression"), target) else {
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
            if let Err(message) = validate_scalar_sql(&sql, target) {
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
                name: field_name.to_string(),
                r#type: dimension_type,
                logical_data_type,
                declared_is_time,
                sql: Some(sql),
                granularity: None,
                supported_granularities: None,
                label: optional_string(field, "label"),
                description: optional_string(field, "description"),
                metadata: None,
                meta: None,
                format: None,
                value_format_name: None,
                parent: None,
                window: None,
                public: true,
            });
        }

        let is_query = source_is_query(&source, target);
        let model = Model {
            name: name.clone(),
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
            metadata: None,
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
            .map(|field| (normalize_identifier(&field.name), field.name.clone()))
            .collect::<HashMap<_, _>>();
        let to_field_names = models[to_index]
            .dimensions
            .iter()
            .map(|field| (normalize_identifier(&field.name), field.name.clone()))
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
        let pointer = format!("/semantic_model/{scope_index}/metrics/{metric_index}/expression");
        let Some(sql) = select_expression(metric.get("expression"), target) else {
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
        let mut parsed = metric_from_sql_expression(
            name.to_string(),
            Some(sql),
            optional_string(metric, "description"),
            None,
        );
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

fn source_is_query(source: &str, target: OssieTarget) -> bool {
    polyglot_sql::parse_one(source, target.parser_dialect()).is_ok_and(|expression| {
        matches!(
            expression,
            Expression::Select(_)
                | Expression::Union(_)
                | Expression::Intersect(_)
                | Expression::Except(_)
                | Expression::Subquery(_)
                | Expression::Values(_)
        )
    })
}

fn select_expression(value: Option<&Value>, target: OssieTarget) -> Option<String> {
    let variants = value?.as_object()?.get("dialects")?.as_array()?;
    let mut exact = None;
    let mut ansi = None;
    for variant in variants {
        let variant = variant.as_object()?;
        let dialect = variant.get("dialect")?.as_str()?.to_ascii_uppercase();
        let expression = variant.get("expression")?.as_str()?.to_string();
        if dialect == target.label() && exact.is_none() {
            exact = Some(expression.clone());
        }
        if dialect == "ANSI_SQL" && ansi.is_none() {
            ansi = Some(expression);
        }
    }
    exact.or(ansi)
}

fn validate_scalar_sql(sql: &str, target: OssieTarget) -> std::result::Result<(), String> {
    let wrapped = format!("SELECT {sql}");
    let parsed = polyglot_sql::parse_one(&wrapped, target.parser_dialect())
        .map_err(|error| format!("Invalid {} SQL expression: {error}", target.label()))?;
    let Expression::Select(select) = parsed else {
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
        || select.with.is_some();
    if select.expressions.len() != 1 || has_query_clauses {
        return Err(format!(
            "Expected one scalar {} SQL expression without query clauses.",
            target.label()
        ));
    }
    let contains_query = select.expressions[0].contains(|expression| {
        matches!(
            expression,
            Expression::Select(_)
                | Expression::Union(_)
                | Expression::Intersect(_)
                | Expression::Except(_)
                | Expression::Subquery(_)
                | Expression::Values(_)
                | Expression::Insert(_)
                | Expression::Update(_)
                | Expression::Delete(_)
        )
    });
    if contains_query {
        return Err(format!(
            "Expected one scalar {} SQL expression without a nested query.",
            target.label()
        ));
    }
    Ok(())
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

fn normalized_strings(values: &[Value]) -> Vec<String> {
    values
        .iter()
        .filter_map(Value::as_str)
        .map(normalize_identifier)
        .collect()
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
        Some(value) => Some(value),
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
