//! Stateless semantic catalog payloads for transactional host catalogs.
//!
//! A payload stores declarations, not graph indexes or session state. Every
//! operation validates a private graph and returns a candidate for the host to
//! publish atomically. Failed operations have no observable mutation.

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::Path;
use std::ptr;

use serde::{Deserialize, Serialize};

use crate::config::{
    load_from_directory_with_metadata, load_from_file_with_metadata,
    load_from_sql_string_with_metadata, load_literal_sources_with_metadata,
    load_literal_yaml_with_metadata, parse_sql_model, LoadedGraphMetadata,
};
use crate::core::{Metric, Model, Parameter, SemanticGraph, TableCalculation};
use crate::sql::QueryRewriter;

use super::{
    active_model_for_loaded_models, semantic_error, semantic_result, sidemantic_free, FfiState,
    SidemanticRewriteResult,
};

mod api;

type CatalogResult<T> = Result<T, String>;

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    version: u32,
    #[serde(default)]
    models: Vec<Model>,
    #[serde(default)]
    metrics: Vec<Metric>,
    #[serde(default)]
    parameters: Vec<Parameter>,
    #[serde(default)]
    table_calculations: Vec<TableCalculation>,
    #[serde(default)]
    metadata: Option<serde_json::Value>,
}

impl Snapshot {
    fn parse(json: &str) -> CatalogResult<Self> {
        if json.trim().is_empty() {
            return Ok(Self {
                version: 1,
                ..Self::default()
            });
        }
        let snapshot: Self =
            serde_json::from_str(json).map_err(|e| format!("invalid catalog snapshot: {e}"))?;
        if snapshot.version != 1 {
            return Err(format!(
                "unsupported catalog snapshot version {}",
                snapshot.version
            ));
        }
        Ok(snapshot)
    }

    fn from_graph(graph: &SemanticGraph) -> Self {
        let mut snapshot = Self {
            version: 1,
            models: graph.models().cloned().collect(),
            metrics: graph.graph_metrics().cloned().collect(),
            parameters: graph.parameters().cloned().collect(),
            table_calculations: graph.table_calculations().cloned().collect(),
            metadata: graph.metadata().cloned(),
        };
        snapshot.models.sort_by(|a, b| a.name.cmp(&b.name));
        snapshot.metrics.sort_by(|a, b| a.name.cmp(&b.name));
        snapshot.parameters.sort_by(|a, b| a.name.cmp(&b.name));
        snapshot
            .table_calculations
            .sort_by(|a, b| a.name.cmp(&b.name));
        snapshot
    }

    fn into_graph(self) -> CatalogResult<SemanticGraph> {
        let mut graph = SemanticGraph::new();
        // Register globals before model indexes, then validate dependencies once
        // the full graph exists. Declaration order must not affect reconstruction.
        for metric in self.metrics {
            graph
                .add_metric_unvalidated(metric)
                .map_err(|e| e.to_string())?;
        }
        for parameter in self.parameters {
            graph.add_parameter(parameter).map_err(|e| e.to_string())?;
        }
        for calculation in self.table_calculations {
            graph
                .add_table_calculation(calculation)
                .map_err(|e| e.to_string())?;
        }
        for model in self.models {
            graph.add_model(model).map_err(|e| e.to_string())?;
        }
        for metric in graph.graph_metrics() {
            graph
                .validate_metric_dependencies(metric)
                .map_err(|e| e.to_string())?;
        }
        if let Some(metadata) = self.metadata {
            graph.set_metadata(metadata);
        }
        Ok(graph)
    }

    fn merge(&mut self, incoming: Self) {
        for model in incoming.models {
            self.models.retain(|current| current.name != model.name);
            self.models.push(model);
        }
        for metric in incoming.metrics {
            self.metrics.retain(|current| current.name != metric.name);
            self.metrics.push(metric);
        }
        for parameter in incoming.parameters {
            self.parameters
                .retain(|current| current.name != parameter.name);
            self.parameters.push(parameter);
        }
        for calculation in incoming.table_calculations {
            self.table_calculations
                .retain(|current| current.name != calculation.name);
            self.table_calculations.push(calculation);
        }
        if let Some(incoming) = incoming.metadata {
            merge_metadata(
                self.metadata.get_or_insert(serde_json::Value::Null),
                incoming,
            );
        }
    }
}

fn merge_metadata(current: &mut serde_json::Value, incoming: serde_json::Value) {
    if let (Some(current), Some(incoming)) = (current.as_object_mut(), incoming.as_object()) {
        for (name, value) in incoming {
            merge_metadata(
                current
                    .entry(name.clone())
                    .or_insert(serde_json::Value::Null),
                value.clone(),
            );
        }
    } else {
        *current = incoming;
    }
}

fn required_arg(value: *const c_char, name: &str) -> CatalogResult<String> {
    if value.is_null() {
        return Err(format!("null {name} pointer"));
    }
    unsafe { CStr::from_ptr(value) }
        .to_str()
        .map(str::to_owned)
        .map_err(|e| format!("invalid UTF-8 in {name}: {e}"))
}

fn optional_arg(value: *const c_char, name: &str) -> CatalogResult<String> {
    if value.is_null() {
        Ok(String::new())
    } else {
        required_arg(value, name)
    }
}

fn guard<T>(operation: impl FnOnce() -> CatalogResult<T>) -> CatalogResult<T> {
    catch_unwind(AssertUnwindSafe(operation))
        .map_err(|_| "semantic catalog operation panicked".to_string())?
}

#[repr(C)]
pub struct SidemanticSnapshotResult {
    pub snapshot: *mut c_char,
    pub active_model: *mut c_char,
    pub error: *mut c_char,
}

fn merge_loaded(snapshot: &str, loaded: LoadedGraphMetadata) -> CatalogResult<(String, String)> {
    let mut merged = Snapshot::parse(snapshot)?;
    merged.merge(Snapshot::from_graph(&loaded.graph));
    let graph = merged.into_graph()?;
    let snapshot =
        serde_json::to_string(&Snapshot::from_graph(&graph)).map_err(|e| e.to_string())?;
    let active = active_model_for_loaded_models(&loaded.model_order).unwrap_or_default();
    Ok((snapshot, active))
}

fn snapshot_result(
    operation: impl FnOnce() -> CatalogResult<(String, String)>,
) -> SidemanticSnapshotResult {
    let result = guard(|| {
        let (snapshot, active_model) = operation()?;
        let snapshot =
            CString::new(snapshot).map_err(|_| "snapshot contains a NUL byte".to_string())?;
        let active_model =
            CString::new(active_model).map_err(|_| "model name contains a NUL byte".to_string())?;
        Ok((snapshot, active_model))
    });
    match result {
        Ok((snapshot, active_model)) => SidemanticSnapshotResult {
            snapshot: snapshot.into_raw(),
            active_model: active_model.into_raw(),
            error: ptr::null_mut(),
        },
        Err(error) => SidemanticSnapshotResult {
            snapshot: ptr::null_mut(),
            active_model: ptr::null_mut(),
            error: semantic_error(error),
        },
    }
}

#[repr(C)]
pub struct SidemanticSource {
    pub path: *const c_char,
    pub content: *const c_char,
}

/// Import bytes already read and authorized by the embedding host.
#[no_mangle]
pub extern "C" fn sidemantic_snapshot_load_sources(
    snapshot: *const c_char,
    sources: *const SidemanticSource,
    count: usize,
    directory: bool,
) -> SidemanticSnapshotResult {
    snapshot_result(|| {
        if sources.is_null() && count != 0 {
            return Err("null sources pointer".into());
        }
        let sources = if count == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(sources, count) }
        };
        let sources = sources
            .iter()
            .map(|source| {
                Ok((
                    required_arg(source.path, "source path")?,
                    required_arg(source.content, "source content")?,
                ))
            })
            .collect::<CatalogResult<Vec<_>>>()?;
        let loaded = if directory {
            load_literal_sources_with_metadata(sources)
        } else {
            let [(path, content)] = sources.as_slice() else {
                return Err("a single-file import requires exactly one source".into());
            };
            if Path::new(path)
                .extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| extension.eq_ignore_ascii_case("sql"))
            {
                load_from_sql_string_with_metadata(content)
            } else {
                load_literal_yaml_with_metadata(content)
            }
        }
        .map_err(|error| error.to_string())?;
        merge_loaded(&optional_arg(snapshot, "snapshot")?, loaded)
    })
}

// DROP operates on declarations, then reconstructs all indexes before publishing.
// The boolean slot in snapshot_apply means IF EXISTS for drop operations.
fn drop_definition(
    original: &str,
    active: &str,
    kind: &str,
    content: &str,
    if_exists: bool,
) -> CatalogResult<(String, String)> {
    let mut snapshot = Snapshot::parse(original)?;
    let names: Vec<String> = serde_json::from_str(content).map_err(|e| e.to_string())?;
    let (model_name, field) =
        match (kind, names.as_slice()) {
            ("model", [model]) => (model.as_str(), None),
            ("metric" | "dimension" | "segment", [model, field]) => {
                (model.as_str(), Some(field.as_str()))
            }
            ("metric" | "dimension" | "segment", [field]) if !active.is_empty() => {
                (active, Some(field.as_str()))
            }
            _ => return Err(
                "DROP requires a model name or a qualified model.field name (or an active model)"
                    .into(),
            ),
        };
    let missing = || {
        if if_exists {
            Ok((original.to_owned(), active.to_owned()))
        } else {
            Err(format!("{kind} '{}' not found", names.join(".")))
        }
    };
    let Some(index) = snapshot
        .models
        .iter()
        .position(|m| m.name.eq_ignore_ascii_case(model_name))
    else {
        return missing();
    };
    let model = &snapshot.models[index];
    let canonical_field = match kind {
        "metric" => model
            .metrics
            .iter()
            .map(|f| &f.name)
            .find(|n| n.eq_ignore_ascii_case(field.unwrap())),
        "dimension" => model
            .dimensions
            .iter()
            .map(|f| &f.name)
            .find(|n| n.eq_ignore_ascii_case(field.unwrap())),
        "segment" => model
            .segments
            .iter()
            .map(|f| &f.name)
            .find(|n| n.eq_ignore_ascii_case(field.unwrap())),
        _ => None,
    };
    if field.is_some() && canonical_field.is_none() {
        return missing();
    }
    let target_model = model.clone();
    let target_field = canonical_field.cloned();
    if let Some(field) = &target_field {
        let model = &mut snapshot.models[index];
        match kind {
            "metric" => model.metrics.retain(|f| &f.name != field),
            "dimension" => model.dimensions.retain(|f| &f.name != field),
            "segment" => model.segments.retain(|f| &f.name != field),
            _ => unreachable!(),
        }
    } else {
        snapshot.models.remove(index);
    }
    restrict_drop(&snapshot, &target_model, target_field.as_deref())?;
    let graph = snapshot.into_graph()?;
    let json = serde_json::to_string(&Snapshot::from_graph(&graph)).map_err(|e| e.to_string())?;
    let active = if target_field.is_none() && active.eq_ignore_ascii_case(&target_model.name) {
        ""
    } else {
        active
    };
    Ok((json, active.to_owned()))
}

fn restrict_drop(
    snapshot: &Snapshot,
    removed_model: &Model,
    target_field: Option<&str>,
) -> CatalogResult<()> {
    let target_model = removed_model.name.as_str();
    let target = target_field.map_or_else(
        || target_model.to_owned(),
        |f| format!("{target_model}.{f}"),
    );
    let matches = |model: Option<&str>, field: &str, context: Option<&str>, bare: bool| {
        let field_matches = target_field.is_none_or(|target| target.eq_ignore_ascii_case(field));
        field_matches
            && match model {
                Some(model) => model.eq_ignore_ascii_case(target_model),
                None => {
                    bare && match context {
                        Some(model) => model.eq_ignore_ascii_case(target_model),
                        None => removed_model
                            .metrics
                            .iter()
                            .map(|f| &f.name)
                            .chain(removed_model.dimensions.iter().map(|f| &f.name))
                            .chain(removed_model.segments.iter().map(|f| &f.name))
                            .any(|name| name.eq_ignore_ascii_case(field)),
                    }
                }
            }
    };
    let reject = |owner: &str| {
        Err(format!(
            "cannot drop '{target}': dependent definition '{owner}' (RESTRICT)"
        ))
    };
    let reference = |value: &str, context: Option<&str>, owner: &str| -> CatalogResult<()> {
        let (model, field) = value
            .rsplit_once('.')
            .map_or((None, value), |(m, f)| (Some(m), f));
        if matches(model, field, context, true) {
            reject(owner)
        } else {
            Ok(())
        }
    };
    let expression =
        |sql: &str, context: Option<&str>, bare: bool, owner: &str| -> CatalogResult<()> {
            // These documented row-alias placeholders are physical references.
            // Other templates remain unprovable and are rejected explicitly.
            let sql = sql
                .replace("{model}", "__physical_row")
                .replace("${CUBE}", "__physical_row");
            let refs = match crate::core::semantic_column_references(&sql) {
                Ok(refs) => refs,
                Err(_) => {
                    if drop_sql_dependency(&sql, removed_model, target_field, context, bare, false)
                        .map_err(|e| {
                            format!(
                            "cannot drop '{target}': cannot prove dependencies of '{owner}': {e}"
                        )
                        })?
                    {
                        return reject(owner);
                    }
                    return Ok(());
                }
            };
            for column in refs {
                // Aggregate arguments and unqualified row expressions are physical
                // columns, even when a semantic field has the same spelling.
                if (target_field.is_none() || !column.aggregate_input)
                    && matches(column.model.as_deref(), &column.field, context, bare)
                {
                    return reject(owner);
                }
            }
            Ok(())
        };
    let metric = |metric: &Metric, context: Option<&str>, owner: &str| -> CatalogResult<()> {
        let semantic_expression =
            metric.agg.is_none() || metric.agg == Some(crate::core::Aggregation::Expression);
        for value in [
            &metric.base_metric,
            &metric.numerator,
            &metric.denominator,
            &metric.extends,
            &metric.non_additive_dimension,
        ]
        .into_iter()
        .flatten()
        {
            reference(value, context, owner)?;
        }
        for value in [
            &metric.entity_dimensions,
            &metric.non_additive_window_groupings,
            &metric.drill_fields,
        ]
        .into_iter()
        .flatten()
        .flatten()
        {
            reference(value, context, owner)?;
        }
        if let Some(sql) = metric
            .sql
            .as_ref()
            .filter(|_| semantic_expression || target_field.is_none())
        {
            expression(sql, context, semantic_expression, owner)?;
        }
        for sql in metric
            .filters
            .iter()
            .chain(metric.having.iter())
            .chain(metric.window_expression.iter())
            .chain(metric.window_order.iter())
            .chain(metric.base_event.iter())
            .chain(metric.conversion_event.iter())
            .chain(metric.cohort_event.iter())
            .chain(metric.activity_event.iter())
            .chain(metric.steps.iter().flatten())
        {
            expression(sql, context, false, owner)?;
        }
        Ok(())
    };
    for model in &snapshot.models {
        let context = Some(model.name.as_str());
        if target_field.is_none()
            && (model
                .extends
                .as_deref()
                .is_some_and(|v| v.eq_ignore_ascii_case(target_model))
                || model.relationships.iter().any(|r| {
                    r.related_model().eq_ignore_ascii_case(target_model)
                        || r.through
                            .as_deref()
                            .is_some_and(|v| v.eq_ignore_ascii_case(target_model))
                }))
        {
            return reject(&model.name);
        }
        if let Some(sql) = &model.sql {
            if drop_sql_dependency(sql, removed_model, target_field, None, false, true).map_err(
                |e| {
                    format!(
                        "cannot drop '{target}': cannot prove dependencies of SQL model '{}': {e}",
                        model.name
                    )
                },
            )? {
                return reject(&model.name);
            }
        }
        if let Some(time) = &model.default_time_dimension {
            reference(time, context, &model.name)?;
        }
        for dimension in &model.dimensions {
            let owner = format!("{}.{}", model.name, dimension.name);
            if let Some(parent) = &dimension.parent {
                reference(parent, context, &owner)?;
            }
            for sql in dimension.sql.iter().chain(dimension.window.iter()) {
                expression(sql, context, false, &owner)?;
            }
        }
        for item in &model.metrics {
            metric(item, context, &format!("{}.{}", model.name, item.name))?;
        }
        for segment in &model.segments {
            expression(
                &segment.sql,
                context,
                false,
                &format!("{}.{}", model.name, segment.name),
            )?;
        }
        for relationship in &model.relationships {
            if let Some(sql) = &relationship.sql {
                expression(
                    sql,
                    context,
                    false,
                    &format!("{}.{}", model.name, relationship.name),
                )?;
            }
        }
        for preagg in &model.pre_aggregations {
            let owner = format!("{}.{}", model.name, preagg.name);
            for value in preagg
                .measures
                .iter()
                .flatten()
                .chain(preagg.dimensions.iter().flatten())
                .chain(preagg.time_dimension.iter())
            {
                reference(value, context, &owner)?;
            }
            if let Some(sql) = &preagg.sql {
                if drop_sql_dependency(sql, removed_model, target_field, None, false, true)
                    .map_err(|e| format!("cannot drop '{target}': cannot prove dependencies of SQL pre-aggregation '{owner}': {e}"))? {
                    return reject(&owner);
                }
            }
        }
    }
    for item in &snapshot.metrics {
        metric(item, None, &item.name)?;
    }
    for calc in &snapshot.table_calculations {
        for value in calc
            .field
            .iter()
            .chain(calc.partition_by.iter().flatten())
            .chain(calc.order_by.iter().flatten())
        {
            reference(value, None, &calc.name)?;
        }
        if let Some(sql) = &calc.expression {
            expression(sql, None, true, &calc.name)?;
        }
    }
    Ok(())
}

// Query dependencies need SQL scope: a physical table aliased to a model name
// and a CTE with that name must not create semantic dependencies. The serialized
// AST includes typed expression children omitted by polyglot's public walker.
fn drop_sql_dependency(
    sql: &str,
    model: &Model,
    field: Option<&str>,
    context: Option<&str>,
    bare: bool,
    query: bool,
) -> CatalogResult<bool> {
    use serde_json::Value;
    use std::collections::{HashMap, HashSet};

    type Scope = HashMap<String, bool>;
    struct References<'a> {
        model: &'a Model,
        field: Option<&'a str>,
        context: Option<&'a str>,
        bare: bool,
    }
    impl References<'_> {
        fn source(&self, node: &Value, scope: &mut Scope, ctes: &HashSet<String>) -> bool {
            if let Some(table) = node.get("table") {
                let name = table["name"]["name"].as_str().unwrap_or("");
                let target = table["schema"].is_null()
                    && table["catalog"].is_null()
                    && name.eq_ignore_ascii_case(&self.model.name)
                    && !ctes.contains(&name.to_ascii_lowercase());
                let alias = table["alias"]["name"].as_str().unwrap_or(name);
                scope.insert(alias.to_ascii_lowercase(), target);
                return target && self.field.is_none();
            }
            if let Some(subquery) = node.get("subquery") {
                let found = self.visit(&subquery["this"], scope, ctes, false);
                if let Some(alias) = subquery["alias"]["name"].as_str() {
                    scope.insert(alias.to_ascii_lowercase(), false);
                }
                return found;
            }
            if let Some(alias) = node.get("alias") {
                let mut child_scope = Scope::new();
                let found = self.source(&alias["this"], &mut child_scope, ctes);
                if let Some(name) = alias["alias"]["name"].as_str() {
                    scope.insert(name.to_ascii_lowercase(), child_scope.values().any(|v| *v));
                }
                return found;
            }
            match node {
                Value::Array(values) => {
                    let mut found = false;
                    for child in values {
                        found |= self.source(child, scope, ctes);
                    }
                    found
                }
                Value::Object(values) => {
                    let mut found = false;
                    for child in values.values() {
                        found |= self.source(child, scope, ctes);
                    }
                    found
                }
                _ => false,
            }
        }

        fn visit(
            &self,
            node: &Value,
            scope: &Scope,
            ctes: &HashSet<String>,
            aggregate: bool,
        ) -> bool {
            let Value::Object(fields) = node else {
                return node.as_array().is_some_and(|children| {
                    children
                        .iter()
                        .any(|child| self.visit(child, scope, ctes, aggregate))
                });
            };
            let mut ctes = ctes.clone();
            if let Some(with) = fields.get("with").filter(|value| !value.is_null()) {
                for cte in with["ctes"].as_array().into_iter().flatten() {
                    let name = cte["alias"]["name"]
                        .as_str()
                        .unwrap_or("")
                        .to_ascii_lowercase();
                    if with["recursive"].as_bool() == Some(true) {
                        ctes.insert(name.clone());
                    }
                    if self.visit(&cte["this"], scope, &ctes, false) {
                        return true;
                    }
                    ctes.insert(name);
                }
            }
            if let Some(select) = fields.get("select") {
                // Process WITH before collecting sources so CTEs shadow model names.
                let mut select = select.clone();
                if let Some(with) = select.get_mut("with") {
                    if let Some(definitions) = with["ctes"].as_array() {
                        for cte in definitions {
                            let name = cte["alias"]["name"]
                                .as_str()
                                .unwrap_or("")
                                .to_ascii_lowercase();
                            if with["recursive"].as_bool() == Some(true) {
                                ctes.insert(name.clone());
                            }
                            if self.visit(&cte["this"], scope, &ctes, false) {
                                return true;
                            }
                            ctes.insert(name);
                        }
                    }
                    *with = Value::Null;
                }
                let mut scope = scope.clone();
                if self.source(&select["from"], &mut scope, &ctes) {
                    return true;
                }
                for join in select["joins"].as_array().into_iter().flatten() {
                    if self.source(&join["this"], &mut scope, &ctes) {
                        return true;
                    }
                }
                return self.visit(&select, &scope, &ctes, false);
            }
            let aggregate = aggregate
                || crate::core::is_aggregate_ast_node(node)
                || fields.contains_key("window")
                || fields.contains_key("window_function");
            if let Some(column) = fields.get("column") {
                let name = column["name"]["name"].as_str().unwrap_or("");
                if self
                    .field
                    .is_some_and(|field| !field.eq_ignore_ascii_case(name))
                    || (aggregate && self.field.is_some())
                {
                    return false;
                }
                if let Some(table) = column["table"]["name"].as_str() {
                    return scope
                        .get(&table.to_ascii_lowercase())
                        .copied()
                        .unwrap_or_else(|| {
                            table.eq_ignore_ascii_case(&self.model.name)
                                && !ctes.contains(&table.to_ascii_lowercase())
                        });
                }
                if !scope.is_empty() {
                    return scope.values().any(|target| *target);
                }
                return self.bare
                    && self.context.map_or_else(
                        || {
                            self.model
                                .metrics
                                .iter()
                                .any(|f| f.name.eq_ignore_ascii_case(name))
                                || self
                                    .model
                                    .dimensions
                                    .iter()
                                    .any(|f| f.name.eq_ignore_ascii_case(name))
                                || self
                                    .model
                                    .segments
                                    .iter()
                                    .any(|f| f.name.eq_ignore_ascii_case(name))
                        },
                        |context| context.eq_ignore_ascii_case(&self.model.name),
                    );
            }
            if fields.contains_key("star") && !aggregate {
                return scope.values().any(|target| *target);
            }
            fields
                .iter()
                .filter(|(key, _)| key.as_str() != "with")
                .any(|(_, child)| self.visit(child, scope, &ctes, aggregate))
        }
    }
    let input = if query {
        sql.to_owned()
    } else {
        format!("SELECT {sql}")
    };
    match crate::semantic_input::dialects::parse(&input, polyglot_sql::DialectType::DuckDB) {
        Ok(ast) => {
            let ast = serde_json::to_value(ast).map_err(|e| e.to_string())?;
            Ok(References {
                model,
                field,
                context,
                bare,
            }
            .visit(&ast, &Scope::new(), &HashSet::new(), false))
        }
        Err(error) => {
            // Unsupported syntax cannot establish a dependency, but must not
            // freeze unrelated definitions. Tokenize only to decide whether the
            // unresolved scope could mention this target; never accept it as a
            // proven dependency or scan literals/comments as identifiers.
            use polyglot_sql::tokens::TokenType;
            let tokens = polyglot_sql::Dialect::get(polyglot_sql::DialectType::DuckDB)
                .tokenize(sql)
                .map_err(|e| e.to_string())?;
            let identifier = |name: &str| {
                tokens.iter().any(|token| {
                    matches!(
                        token.token_type,
                        TokenType::Identifier | TokenType::QuotedIdentifier | TokenType::Var
                    ) && token.text.eq_ignore_ascii_case(name)
                })
            };
            let model_mentioned = identifier(&model.name);
            let in_context =
                context.is_none_or(|context| context.eq_ignore_ascii_case(&model.name));
            let possible = field.map_or(model_mentioned, |field| {
                (identifier(field) && (model_mentioned || bare && in_context))
                    || model_mentioned
                        && tokens
                            .iter()
                            .any(|token| token.token_type == TokenType::Star)
            });
            if possible {
                Err(error.to_string())
            } else {
                Ok(false)
            }
        }
    }
}

// Read a declaration identifier without splitting quoted names on dots/spaces.
fn declaration_identifier(input: &str) -> Option<(String, &str)> {
    let input = input.trim_start();
    let first = input.chars().next()?;
    if matches!(first, '\'' | '"' | '`') {
        let mut value = String::new();
        let mut chars = input.char_indices().skip(1).peekable();
        while let Some((index, character)) = chars.next() {
            if character == first {
                if chars.peek().is_some_and(|(_, next)| *next == first) {
                    chars.next();
                    value.push(first);
                } else {
                    return Some((value, &input[index + character.len_utf8()..]));
                }
            } else {
                value.push(character);
            }
        }
        None
    } else {
        let end = input
            .find(|c: char| c.is_whitespace() || matches!(c, '.' | '(' | ')' | ';'))
            .unwrap_or(input.len());
        (end > 0).then(|| (input[..end].to_owned(), &input[end..]))
    }
}

fn apply_item(state: &mut FfiState, content: &str, replace: bool) -> CatalogResult<()> {
    let (keyword, body) = declaration_identifier(content).ok_or("missing definition kind")?;
    let mut model_name = state.active_model.clone();
    let mut adjusted = content.to_owned();
    let mut declared_name = None;
    if let Some((first, remainder)) = declaration_identifier(body) {
        let (field, rest) = if let Some(remainder) = remainder.trim_start().strip_prefix('.') {
            let (field, rest) =
                declaration_identifier(remainder).ok_or("missing qualified field name")?;
            model_name = Some(first);
            (field, rest)
        } else {
            (first, remainder)
        };
        declared_name = Some(field);
        adjusted = if let Some(properties) = rest.trim_start().strip_prefix('(') {
            format!("{keyword} (name __field, {properties}")
        } else {
            format!("{keyword} __field {rest}")
        };
    }
    let model_name = model_name.ok_or("no active model; use a qualified model.field name")?;
    let mut model = state
        .graph
        .models()
        .find(|model| model.name.eq_ignore_ascii_case(&model_name))
        .cloned()
        .ok_or_else(|| format!("model '{model_name}' not found"))?;
    let parsed = parse_sql_model(&format!(
        "MODEL (name __definition, table dummy);\n{adjusted}"
    ))
    .map_err(|e| format!("parsing definition: {e}"))?;
    // Preserve canonical spelling on replacement and reject case-only duplicates,
    // matching DuckDB's identifier contract, including quoted identifiers.
    macro_rules! merge_items {
        ($items:ident, $kind:literal) => {
            for mut item in parsed.$items {
                if let Some(name) = &declared_name {
                    item.name = name.clone();
                }
                if let Some(existing) = model
                    .$items
                    .iter()
                    .position(|old| old.name.eq_ignore_ascii_case(&item.name))
                {
                    if !replace {
                        return Err(format!("duplicate {} '{}'", $kind, item.name));
                    }
                    item.name = model.$items[existing].name.clone();
                    model.$items[existing] = item;
                } else {
                    model.$items.push(item);
                }
            }
        };
    }
    merge_items!(metrics, "metric");
    merge_items!(dimensions, "dimension");
    merge_items!(segments, "segment");
    state
        .graph
        .replace_model(model)
        .map_err(|e| format!("updating model: {e}"))
}

fn apply(
    snapshot: &str,
    active_model: &str,
    operation: &str,
    content: &str,
    replace: bool,
) -> CatalogResult<(String, String)> {
    // DuckDB workers can have only 512 KiB of stack. Keep dependency parsing,
    // AST walking/serialization, and graph reconstruction on the established
    // semantic worker; nested compiler operations reuse that protected stack.
    crate::semantic_input::with_semantic_stack(|| {
        Ok(apply_on_semantic_worker(
            snapshot,
            active_model,
            operation,
            content,
            replace,
        ))
    })
    .map_err(|error| error.to_string())?
}

fn apply_on_semantic_worker(
    snapshot: &str,
    active_model: &str,
    operation: &str,
    content: &str,
    replace: bool,
) -> CatalogResult<(String, String)> {
    if let Some(kind) = operation.strip_prefix("drop_") {
        return drop_definition(snapshot, active_model, kind, content, replace);
    }
    if operation == "import" {
        let incoming = Snapshot::parse(content)?;
        let active = if incoming.models.len() == 1 {
            incoming.models[0].name.clone()
        } else {
            String::new()
        };
        let incoming = Snapshot::from_graph(&incoming.into_graph()?);
        let mut merged = Snapshot::parse(snapshot)?;
        merged.merge(incoming);
        let graph = merged.into_graph()?;
        return Ok((
            serde_json::to_string(&Snapshot::from_graph(&graph)).map_err(|e| e.to_string())?,
            active,
        ));
    }
    let mut state = FfiState {
        graph: Snapshot::parse(snapshot)?.into_graph()?,
        active_model: if active_model.is_empty() {
            None
        } else {
            Some(active_model.to_owned())
        },
    };
    match operation {
        "model" => {
            let mut model =
                parse_sql_model(content).map_err(|e| format!("parsing definition: {e}"))?;
            if let Some(existing) = state
                .graph
                .models()
                .find(|old| old.name.eq_ignore_ascii_case(&model.name))
            {
                if !replace {
                    return Err(format!("model '{}' already exists", model.name));
                }
                model.name = existing.name.clone();
            }
            let name = model.name.clone();
            if replace {
                state.graph.replace_model(model)
            } else {
                state.graph.add_model(model)
            }
            .map_err(|e| e.to_string())?;
            state.active_model = Some(name);
        }
        "item" => {
            apply_item(&mut state, content, replace)?;
        }
        "use" => {
            let name = content.trim();
            let model = state
                .graph
                .models()
                .find(|model| model.name.eq_ignore_ascii_case(name))
                .ok_or_else(|| format!("model '{name}' not found"))?;
            state.active_model = Some(model.name.clone());
        }
        "yaml" | "file" | "legacy_sql" => {
            let loaded = match operation {
                "yaml" => load_literal_yaml_with_metadata(content),
                "legacy_sql" => load_from_sql_string_with_metadata(content),
                _ if Path::new(content).is_dir() => load_from_directory_with_metadata(content),
                _ => load_from_file_with_metadata(content),
            }
            .map_err(|e| e.to_string())?;
            return merge_loaded(snapshot, loaded);
        }
        _ => return Err(format!("unknown catalog operation '{operation}'")),
    }
    for metric in state.graph.graph_metrics() {
        state
            .graph
            .validate_metric_dependencies(metric)
            .map_err(|e| e.to_string())?;
    }
    let snapshot =
        serde_json::to_string(&Snapshot::from_graph(&state.graph)).map_err(|e| e.to_string())?;
    Ok((snapshot, state.active_model.unwrap_or_default()))
}

#[no_mangle]
pub extern "C" fn sidemantic_snapshot_apply(
    snapshot: *const c_char,
    active_model: *const c_char,
    operation: *const c_char,
    content: *const c_char,
    replace: bool,
) -> SidemanticSnapshotResult {
    snapshot_result(|| {
        apply(
            &optional_arg(snapshot, "snapshot")?,
            &optional_arg(active_model, "active_model")?,
            &required_arg(operation, "operation")?,
            &required_arg(content, "content")?,
            replace,
        )
    })
}

#[no_mangle]
pub extern "C" fn sidemantic_free_snapshot_result(result: SidemanticSnapshotResult) {
    sidemantic_free(result.snapshot);
    sidemantic_free(result.active_model);
    sidemantic_free(result.error);
}

#[no_mangle]
pub extern "C" fn sidemantic_snapshot_rewrite(
    snapshot: *const c_char,
    sql: *const c_char,
) -> SidemanticRewriteResult {
    semantic_result(
        guard(|| {
            let graph = Snapshot::parse(&optional_arg(snapshot, "snapshot")?)?.into_graph()?;
            let sql = required_arg(sql, "sql")?;
            // DuckDB callers own the database; `main.orders` names the
            // physical table even when a model is also called `orders`.
            QueryRewriter::new(&graph)
                .with_qualified_tables_physical()
                .rewrite_with_dialect(&sql, polyglot_sql::DialectType::DuckDB)
                .map_err(|e| e.to_string())
        })
        .map_err(semantic_error),
    )
}

#[repr(C)]
pub struct SidemanticModelInfo {
    pub name: *mut c_char,
    pub fields: *mut *mut c_char,
    pub field_count: usize,
}

#[repr(C)]
pub struct SidemanticModelList {
    pub models: *mut SidemanticModelInfo,
    pub count: usize,
    pub error: *mut c_char,
}

#[no_mangle]
pub extern "C" fn sidemantic_snapshot_list_models(snapshot: *const c_char) -> SidemanticModelList {
    let result = guard(|| {
        let graph = Snapshot::parse(&optional_arg(snapshot, "snapshot")?)?.into_graph()?;
        let snapshot = Snapshot::from_graph(&graph);
        snapshot
            .models
            .into_iter()
            .map(|model| {
                let mut fields: Vec<_> = model
                    .dimensions
                    .into_iter()
                    .map(|field| field.name)
                    .chain(model.metrics.into_iter().map(|field| field.name))
                    .chain(model.segments.into_iter().map(|field| field.name))
                    .collect();
                fields.sort();
                fields.dedup();
                let fields: Vec<CString> = fields
                    .into_iter()
                    .map(|field| {
                        CString::new(field)
                            .map_err(|_| "field name contains a NUL byte".to_string())
                    })
                    .collect::<CatalogResult<_>>()?;
                let name = CString::new(model.name)
                    .map_err(|_| "model name contains a NUL byte".to_string())?;
                Ok((name, fields))
            })
            .collect::<CatalogResult<Vec<_>>>()
    });
    match result {
        Ok(models) => {
            // Convert ownership only after every name has passed validation.
            let models: Box<[_]> = models
                .into_iter()
                .map(|(name, fields)| {
                    let fields: Box<[_]> = fields.into_iter().map(CString::into_raw).collect();
                    SidemanticModelInfo {
                        name: name.into_raw(),
                        field_count: fields.len(),
                        fields: Box::into_raw(fields).cast(),
                    }
                })
                .collect();
            SidemanticModelList {
                count: models.len(),
                models: Box::into_raw(models).cast(),
                error: ptr::null_mut(),
            }
        }
        Err(error) => SidemanticModelList {
            models: ptr::null_mut(),
            count: 0,
            error: semantic_error(error),
        },
    }
}

#[no_mangle]
pub extern "C" fn sidemantic_free_model_list(result: SidemanticModelList) {
    if !result.models.is_null() {
        let models =
            unsafe { Box::from_raw(ptr::slice_from_raw_parts_mut(result.models, result.count)) };
        for model in models {
            sidemantic_free(model.name);
            let fields = unsafe {
                Box::from_raw(ptr::slice_from_raw_parts_mut(
                    model.fields,
                    model.field_count,
                ))
            };
            for field in fields {
                sidemantic_free(field);
            }
        }
    }
    sidemantic_free(result.error);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{ComparisonType, Dimension, ParameterType, TableCalcType};

    fn take_apply(result: SidemanticSnapshotResult) -> CatalogResult<(String, String)> {
        let value = if result.error.is_null() {
            assert!(!result.snapshot.is_null());
            assert!(!result.active_model.is_null());
            Ok(unsafe {
                (
                    CStr::from_ptr(result.snapshot).to_str().unwrap().to_owned(),
                    CStr::from_ptr(result.active_model)
                        .to_str()
                        .unwrap()
                        .to_owned(),
                )
            })
        } else {
            assert!(result.snapshot.is_null());
            assert!(result.active_model.is_null());
            Err(unsafe { CStr::from_ptr(result.error).to_str().unwrap().to_owned() })
        };
        sidemantic_free_snapshot_result(result);
        value
    }

    fn apply_ffi(
        snapshot: &str,
        active: &str,
        operation: &str,
        content: &str,
        replace: bool,
    ) -> CatalogResult<(String, String)> {
        let strings: Vec<_> = [snapshot, active, operation, content]
            .map(|value| CString::new(value).unwrap())
            .into();
        take_apply(sidemantic_snapshot_apply(
            strings[0].as_ptr(),
            strings[1].as_ptr(),
            strings[2].as_ptr(),
            strings[3].as_ptr(),
            replace,
        ))
    }

    fn rewrite(snapshot: &str, sql: &str) -> CatalogResult<String> {
        let snapshot = CString::new(snapshot).unwrap();
        let sql = CString::new(sql).unwrap();
        let result = sidemantic_snapshot_rewrite(snapshot.as_ptr(), sql.as_ptr());
        let value = if result.error.is_null() {
            assert!(result.was_rewritten);
            Ok(unsafe { CStr::from_ptr(result.sql).to_str().unwrap().to_owned() })
        } else {
            assert!(!result.was_rewritten);
            Err(unsafe { CStr::from_ptr(result.error).to_str().unwrap().to_owned() })
        };
        super::super::sidemantic_free_result(result);
        value
    }

    fn orders() -> (String, String) {
        apply_ffi(
            "",
            "",
            "legacy_sql",
            "MODEL (name orders, table raw_orders, primary_key id); METRIC revenue AS SUM(amount);",
            false,
        )
        .unwrap()
    }

    #[test]
    fn qualified_lifecycle_preserves_active_model_and_quoted_names() {
        let (snapshot, _) = orders();
        let (mut snapshot, active) = apply_ffi(
            &snapshot,
            "orders",
            "model",
            "MODEL (name other, table other_rows);",
            false,
        )
        .unwrap();
        for (kind, definition, field) in [
            (
                "metric",
                "METRIC \"ORDERS\".\"Gross amount\" AS SUM(amount)",
                "Gross amount",
            ),
            (
                "dimension",
                "DIMENSION ORDERS.\"sales.region\" AS region",
                "sales.region",
            ),
            (
                "segment",
                "SEGMENT orders.\"Open orders\" AS status = 'open'",
                "Open orders",
            ),
        ] {
            for replace in [false, true] {
                let result = apply_ffi(&snapshot, &active, "item", definition, replace).unwrap();
                snapshot = result.0;
                assert_eq!(result.1, "other");
            }
            let target = serde_json::to_string(&["ORDERS", field]).unwrap();
            let result =
                apply_ffi(&snapshot, &active, &format!("drop_{kind}"), &target, false).unwrap();
            snapshot = result.0;
            assert_eq!(result.1, "other");
            assert!(
                apply_ffi(&snapshot, &active, &format!("drop_{kind}"), &target, false)
                    .unwrap_err()
                    .contains("not found")
            );
            assert_eq!(
                apply_ffi(&snapshot, &active, &format!("drop_{kind}"), &target, true)
                    .unwrap()
                    .0,
                snapshot
            );
        }
        let (snapshot, active) =
            apply_ffi(&snapshot, &active, "drop_model", "[\"OTHER\"]", false).unwrap();
        assert!(active.is_empty());
        assert_eq!(Snapshot::parse(&snapshot).unwrap().models.len(), 1);
    }

    #[test]
    fn qualified_replacement_compiles_inline_aggregates_as_grouped_calculations() {
        let (snapshot, active) = orders();
        let (snapshot, _) = apply_ffi(
            &snapshot,
            &active,
            "item",
            "METRIC orders.\"Gross amount\" AS SUM(amount)",
            false,
        )
        .unwrap();
        let (snapshot, _) = apply_ffi(
            &snapshot,
            &active,
            "item",
            "METRIC orders.\"Gross amount\" AS SUM(amount) * 2",
            true,
        )
        .unwrap();
        let query = "SELECT orders.\"Gross amount\" FROM orders";
        let actual = rewrite(&snapshot, query).unwrap();
        let mut graph = SemanticGraph::new();
        graph
            .add_model(
                Model::new("orders", "id")
                    .with_table("raw_orders")
                    .with_metric(Metric::derived("Gross amount", "SUM(amount) * 2")),
            )
            .unwrap();
        let expected = QueryRewriter::new(&graph)
            .rewrite_with_dialect(query, polyglot_sql::DialectType::DuckDB)
            .unwrap();
        assert_eq!(actual, expected);
        assert!(actual.contains("SUM("), "{actual}");
        assert!(!actual.contains("SELECT id,"), "{actual}");
    }

    #[test]
    fn lifecycle_dependency_parsing_runs_off_small_host_stacks() {
        std::thread::Builder::new()
            .stack_size(512 * 1024)
            .spawn(|| {
                let (snapshot, active) = orders();
                let calculated = "METRIC orders.calculated AS
                    SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) * 2";
                let (snapshot, _) =
                    apply_ffi(&snapshot, &active, "item", calculated, false).unwrap();
                let (snapshot, _) = apply_ffi(
                    &snapshot,
                    &active,
                    "item",
                    "METRIC orders.doubled AS revenue * 2",
                    false,
                )
                .unwrap();
                // Scalar dependency analysis must reject safely on a DuckDB-sized
                // stack, and the worker must remain usable after that error.
                let revenue = r#"["orders","revenue"]"#;
                let doubled = r#"["orders","doubled"]"#;
                let calculated = r#"["orders","calculated"]"#;
                let error =
                    apply_ffi(&snapshot, &active, "drop_metric", revenue, false).unwrap_err();
                assert!(error.contains("dependent definition"));
                let (snapshot, _) =
                    apply_ffi(&snapshot, &active, "drop_metric", doubled, false).unwrap();
                // The SQL-source scope walker and unsupported-expression fallback
                // use the same protected boundary, including their AST traversal.
                let source = r#"
models:
  - name: source
    sql: SELECT o.revenue FROM orders o
    primary_key: id
"#;
                let (snapshot, _) = apply_ffi(&snapshot, &active, "yaml", source, false).unwrap();
                let (snapshot, _) =
                    apply_ffi(&snapshot, &active, "drop_metric", calculated, false).unwrap();
                let error =
                    apply_ffi(&snapshot, &active, "drop_metric", revenue, false).unwrap_err();
                assert!(error.contains("source"));
                let malformed = r#"
models:
  - name: unsupported
    table: raw_other
    primary_key: id
    metrics:
      - name: invalid_sql
        type: derived
        sql: 'unrelated_value + ('
"#;
                let (snapshot, _) =
                    apply_ffi(&snapshot, &active, "yaml", malformed, false).unwrap();
                assert!(
                    apply_ffi(&snapshot, &active, "drop_model", r#"["source"]"#, false).is_ok()
                );
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn drop_restrict_checks_dependencies_and_preserves_original_snapshot() {
        let (original, active) = orders();
        for definition in [
            "METRIC doubled AS revenue * 2;",
            "METRIC (name yoy, type time_comparison, base_metric revenue, comparison_type yoy);",
            "SEGMENT valuable AS orders.revenue > 10;",
        ] {
            let (snapshot, _) = apply_ffi(&original, &active, "item", definition, false).unwrap();
            let error = apply_ffi(
                &snapshot,
                &active,
                "drop_metric",
                "[\"orders\",\"revenue\"]",
                false,
            )
            .unwrap_err();
            assert!(error.contains("dependent definition"), "{error}");
            assert!(rewrite(&snapshot, "SELECT orders.revenue FROM orders")
                .unwrap()
                .contains("SUM("));
        }
        let (snapshot, _) = apply_ffi(
            &original,
            &active,
            "item",
            "DIMENSION amount AS amount",
            false,
        )
        .unwrap();
        let (snapshot, _) = apply_ffi(
            &snapshot,
            &active,
            "drop_dimension",
            "[\"orders\",\"amount\"]",
            false,
        )
        .unwrap();
        assert!(rewrite(&snapshot, "SELECT orders.revenue FROM orders")
            .unwrap()
            .contains("SUM("));
        let mut declared = Snapshot::parse(&original).unwrap();
        declared
            .metrics
            .push(Metric::derived("global_total", "orders.revenue"));
        let snapshot = serde_json::to_string(&declared).unwrap();
        for (operation, target) in [
            ("drop_metric", "[\"orders\",\"revenue\"]"),
            ("drop_model", "[\"orders\"]"),
        ] {
            assert!(apply_ffi(&snapshot, &active, operation, target, false)
                .unwrap_err()
                .contains("global_total"));
        }
        declared.metrics.clear();
        declared.models.push(
            Model::new("customers", "id")
                .with_table("raw_customers")
                .with_relationship(crate::core::Relationship::many_to_one("orders")),
        );
        let snapshot = serde_json::to_string(&declared).unwrap();
        assert!(
            apply_ffi(&snapshot, &active, "drop_model", "[\"orders\"]", false)
                .unwrap_err()
                .contains("customers")
        );
    }

    #[test]
    fn snapshot_import_is_lossless_and_validates_before_merge() {
        let (snapshot, _) = orders();
        let (restored, active) = apply_ffi("", "", "import", &snapshot, false).unwrap();
        assert_eq!(snapshot, restored);
        assert_eq!(active, "orders");
        assert!(apply_ffi(&snapshot, &active, "import", "{\"version\":2}", false).is_err());
    }

    #[test]
    fn drop_checks_dimension_calculation_and_dynamic_sql_dependencies() {
        let (snapshot, active) = orders();
        let mut declaration = Snapshot::parse(&snapshot).unwrap();
        declaration.models[0]
            .dimensions
            .push(Dimension::time("created_at"));
        declaration.models[0].default_time_dimension = Some("created_at".into());
        let with_time = serde_json::to_string(&declaration).unwrap();
        assert!(apply_ffi(
            &with_time,
            &active,
            "drop_dimension",
            "[\"orders\",\"created_at\"]",
            false
        )
        .unwrap_err()
        .contains("dependent definition"));
        declaration.models[0].default_time_dimension = None;
        declaration.table_calculations.push(
            TableCalculation::new("running_revenue", TableCalcType::RunningTotal)
                .with_field("revenue"),
        );
        let with_calculation = serde_json::to_string(&declaration).unwrap();
        assert!(apply_ffi(
            &with_calculation,
            &active,
            "drop_metric",
            "[\"orders\",\"revenue\"]",
            false
        )
        .unwrap_err()
        .contains("running_revenue"));
        declaration.table_calculations.clear();
        declaration
            .models
            .push(Model::new("dynamic_source", "id").with_sql("SELECT * FROM orders"));
        let with_sql = serde_json::to_string(&declaration).unwrap();
        assert!(
            apply_ffi(&with_sql, &active, "drop_model", "[\"orders\"]", false)
                .unwrap_err()
                .contains("dependent definition")
        );
    }

    #[test]
    fn drop_sql_sources_respect_aliases_ctes_and_physical_inputs() {
        let model = Model::new("orders", "id")
            .with_metric(Metric::sum("revenue", "amount"))
            .with_dimension(Dimension::categorical("amount"));
        for (sql, model_dependency, revenue_dependency) in [
            ("SELECT r.revenue FROM raw_orders r", false, false),
            ("SELECT orders.revenue FROM raw_orders orders", false, false),
            (
                "WITH orders AS (SELECT revenue FROM raw_orders) SELECT orders.revenue FROM orders",
                false,
                false,
            ),
            ("SELECT o.revenue FROM orders o", true, true),
            ("SELECT revenue FROM orders", true, true),
            ("SELECT * FROM orders", true, true),
            (
                "WITH totals AS (SELECT o.revenue FROM orders o) SELECT * FROM totals",
                true,
                true,
            ),
            (
                "SELECT * FROM (SELECT o.revenue FROM orders o) totals",
                true,
                true,
            ),
            (
                "SELECT * FROM (SELECT revenue FROM raw_orders) orders",
                false,
                false,
            ),
            ("SELECT sum(o.amount) FROM orders o", true, false),
            ("SELECT 'orders.revenue' FROM raw_orders", false, false),
            ("SELECT o.revenue FROM main.orders o", false, false),
        ] {
            assert_eq!(
                drop_sql_dependency(sql, &model, None, None, false, true).unwrap(),
                model_dependency,
                "{sql}"
            );
            assert_eq!(
                drop_sql_dependency(sql, &model, Some("revenue"), None, false, true).unwrap(),
                revenue_dependency,
                "{sql}"
            );
        }
        assert!(!drop_sql_dependency(
            "SELECT sum(o.amount) FROM orders o",
            &model,
            Some("amount"),
            None,
            false,
            true
        )
        .unwrap());
        let quoted =
            Model::new("Orders Archive", "id").with_metric(Metric::sum("Gross Amount", "amount"));
        assert!(drop_sql_dependency(
            "SELECT o.\"Gross Amount\" FROM \"Orders Archive\" o",
            &quoted,
            Some("Gross Amount"),
            None,
            false,
            true
        )
        .unwrap());
    }

    #[test]
    fn unrelated_sql_definitions_do_not_block_drop() {
        let (original, active) = orders();
        let mut declaration = Snapshot::parse(&original).unwrap();
        declaration.models[0].metrics.push(Metric::count("unused"));
        declaration
            .models
            .push(Model::new("sql_source", "id").with_sql("SELECT id, amount FROM raw_source"));
        declaration
            .models
            .push(Model::new("unrelated", "id").with_table("raw_unrelated"));
        declaration.models[2].pre_aggregations.push(serde_json::from_value(serde_json::json!({
            "name": "physical_rollup", "type": "original_sql", "sql": "SELECT sum(amount) FROM raw_rollup"
        })).unwrap());
        let snapshot = serde_json::to_string(&declaration).unwrap();
        assert!(apply_ffi(
            &snapshot,
            &active,
            "drop_metric",
            "[\"orders\",\"unused\"]",
            false
        )
        .is_ok());
        assert!(apply_ffi(&snapshot, &active, "drop_model", "[\"unrelated\"]", false).is_ok());
        declaration.models[2].pre_aggregations[0].sql =
            Some("SELECT o.unused FROM orders o".into());
        let with_dependency = serde_json::to_string(&declaration).unwrap();
        assert!(apply_ffi(
            &with_dependency,
            &active,
            "drop_metric",
            "[\"orders\",\"unused\"]",
            false
        )
        .unwrap_err()
        .contains("physical_rollup"));
        declaration.models[2].pre_aggregations.clear();
        declaration.models[1].sql = Some("SELECT o.revenue FROM orders o".into());
        let snapshot = serde_json::to_string(&declaration).unwrap();
        assert!(apply_ffi(
            &snapshot,
            &active,
            "drop_metric",
            "[\"orders\",\"unused\"]",
            false
        )
        .is_ok());
        assert!(apply_ffi(
            &snapshot,
            &active,
            "drop_metric",
            "[\"orders\",\"revenue\"]",
            false
        )
        .unwrap_err()
        .contains("sql_source"));
        // An unrelated unsupported expression must not freeze catalog changes;
        // an unsupported expression mentioning this metric still fails closed.
        declaration.models[1].sql = None;
        declaration.models[1].table = Some("raw_source".into());
        declaration.models[1]
            .metrics
            .push(Metric::derived("unsupported", "unrelated_value + ("));
        let snapshot = serde_json::to_string(&declaration).unwrap();
        assert!(apply_ffi(
            &snapshot,
            &active,
            "drop_metric",
            "[\"orders\",\"unused\"]",
            false
        )
        .is_ok());
        declaration.models[1].metrics[0].sql = Some("orders.unused + (".into());
        let snapshot = serde_json::to_string(&declaration).unwrap();
        let error = apply_ffi(
            &snapshot,
            &active,
            "drop_metric",
            "[\"orders\",\"unused\"]",
            false,
        )
        .unwrap_err();
        assert!(
            error.contains("cannot prove dependencies") || error.contains("dependent definition"),
            "{error}"
        );
    }

    fn load_sources(
        snapshot: &str,
        sources: &[(&str, &str)],
        directory: bool,
    ) -> CatalogResult<(String, String)> {
        let snapshot = CString::new(snapshot).unwrap();
        let strings: Vec<_> = sources
            .iter()
            .map(|(path, content)| {
                (
                    CString::new(*path).unwrap(),
                    CString::new(*content).unwrap(),
                )
            })
            .collect();
        let inputs: Vec<_> = strings
            .iter()
            .map(|(path, content)| SidemanticSource {
                path: path.as_ptr(),
                content: content.as_ptr(),
            })
            .collect();
        take_apply(sidemantic_snapshot_load_sources(
            snapshot.as_ptr(),
            inputs.as_ptr(),
            inputs.len(),
            directory,
        ))
    }

    #[test]
    fn host_sources_preserve_directory_semantics_without_files_or_environment() {
        let sources = [
            ("child.yml", "models:\n  - name: child\n    extends: parent\n    table: child_rows\n"),
            ("parent.yaml", "models:\n  - name: parent\n    table: '${SIDEMANTIC_HOST_IMPORT_TEST:-not_expanded}'\n    primary_key: id\n    metrics:\n      - name: revenue\n        agg: sum\n        sql: amount\n"),
            ("events.SQL", "MODEL (name events, table raw_events, primary_key id); METRIC rows AS COUNT(*);"),
        ];
        let (snapshot, active) = load_sources("", &sources, true).unwrap();
        assert!(active.is_empty());
        let graph = Snapshot::parse(&snapshot).unwrap().into_graph().unwrap();
        assert_eq!(graph.models().count(), 3);
        assert!(graph
            .get_model("child")
            .unwrap()
            .get_metric("revenue")
            .is_some());
        assert_eq!(
            graph.get_model("parent").unwrap().table_name(),
            "${SIDEMANTIC_HOST_IMPORT_TEST:-not_expanded}"
        );
        assert!(rewrite(&snapshot, "SELECT events.rows FROM events")
            .unwrap()
            .contains("COUNT("));
        let (single, active) = load_sources("", &sources[2..], false).unwrap();
        assert_eq!(active, "events");
        assert_eq!(Snapshot::parse(&single).unwrap().models.len(), 1);
        assert!(load_sources(&snapshot, &[("broken.yml", "models: [")], true).is_err());
        assert_eq!(Snapshot::parse(&snapshot).unwrap().models.len(), 3);
    }

    #[test]
    fn host_source_boundary_rejects_missing_inputs() {
        assert!(take_apply(sidemantic_snapshot_load_sources(
            ptr::null(),
            ptr::null(),
            1,
            true
        ))
        .is_err());
        assert!(take_apply(sidemantic_snapshot_load_sources(
            ptr::null(),
            ptr::null(),
            0,
            false
        ))
        .is_err());
        let (snapshot, active) = take_apply(sidemantic_snapshot_load_sources(
            ptr::null(),
            ptr::null(),
            0,
            true,
        ))
        .unwrap();
        assert!(active.is_empty());
        assert!(Snapshot::parse(&snapshot).unwrap().models.is_empty());
        let invalid = SidemanticSource {
            path: ptr::null(),
            content: ptr::null(),
        };
        assert!(take_apply(sidemantic_snapshot_load_sources(
            ptr::null(),
            &invalid,
            1,
            true
        ))
        .is_err());
    }

    #[test]
    fn snapshot_mutations_are_private_and_errors_preserve_original() {
        let (snapshot, active) = orders();
        assert_eq!(active, "orders");
        let duplicate = apply_ffi(
            &snapshot,
            &active,
            "item",
            "METRIC revenue AS SUM(other_amount);",
            false,
        )
        .unwrap_err();
        assert!(duplicate.contains("duplicate metric"), "{duplicate}");
        let (replacement, _) = apply_ffi(
            &snapshot,
            &active,
            "item",
            "METRIC revenue AS SUM(other_amount);",
            true,
        )
        .unwrap();
        let sql = "SELECT \"o\".\"revenue\" FROM \"orders\" AS \"o\"";
        let original_sql = rewrite(&snapshot, sql).unwrap();
        let replaced_sql = rewrite(&replacement, sql).unwrap();
        assert!(
            original_sql.contains("amount AS revenue_raw")
                && !original_sql.contains("other_amount"),
            "{original_sql}"
        );
        assert!(
            replaced_sql.contains("other_amount AS revenue_raw"),
            "{replaced_sql}"
        );
        assert!(rewrite(
            &snapshot,
            "SELECT orders.revenue, orders.missing FROM orders"
        )
        .is_err());
        assert!(apply_ffi(
            &snapshot,
            &active,
            "model",
            "MODEL (name orders, table other_orders, primary_key id);",
            false
        )
        .unwrap_err()
        .contains("already exists"));
    }

    #[test]
    fn imports_preserve_globals_and_require_selection_for_multiple_models() {
        let yaml = r#"
models:
  - name: orders
    table: raw_orders
    primary_key: id
    metrics:
      - name: revenue
        agg: sum
        sql: amount
  - name: customers
    table: raw_customers
    primary_key: id
parameters:
  - name: currency
    type: string
    default_value: USD
metadata:
  owner: finance
sql_metrics: |
  METRIC total AS orders.revenue;
"#;
        let (snapshot, active) = apply_ffi("", "orders", "yaml", yaml, false).unwrap();
        assert!(active.is_empty());
        let error = apply_ffi(
            &snapshot,
            &active,
            "item",
            "DIMENSION (name region, type categorical);",
            false,
        )
        .unwrap_err();
        assert!(error.contains("no active model"), "{error}");
        let (snapshot, active) = apply_ffi(
            &snapshot,
            &active,
            "item",
            "DIMENSION customers.region (type categorical);",
            false,
        )
        .unwrap();
        assert!(active.is_empty());
        let (snapshot, active) = apply_ffi(&snapshot, &active, "use", "orders", false).unwrap();
        assert_eq!(active, "orders");
        let (snapshot, _) = apply_ffi(
            &snapshot,
            &active,
            "model",
            "MODEL (name events, table raw_events, primary_key id);",
            false,
        )
        .unwrap();
        let graph = Snapshot::parse(&snapshot).unwrap().into_graph().unwrap();
        assert_eq!(graph.models().count(), 3);
        assert!(graph.get_metric("total").is_some());
        assert_eq!(
            graph.get_parameter("currency").unwrap().default_value,
            Some(serde_json::json!("USD"))
        );
        assert_eq!(graph.metadata().unwrap()["owner"], "finance");
        assert!(graph
            .get_model("customers")
            .unwrap()
            .get_dimension("region")
            .is_some());
        let (merged, _) = apply_ffi(
            &snapshot,
            "",
            "yaml",
            "metadata:\n  department: analytics\nmodels: []\n",
            false,
        )
        .unwrap();
        let graph = Snapshot::parse(&merged).unwrap().into_graph().unwrap();
        assert_eq!(graph.metadata().unwrap()["owner"], "finance");
        assert_eq!(graph.metadata().unwrap()["department"], "analytics");
    }

    #[test]
    fn snapshot_roundtrip_rebuilds_indexes_without_promoting_model_metrics() {
        let mut graph = SemanticGraph::new();
        graph
            .add_model(
                Model::new("orders", "id")
                    .with_table("raw_orders")
                    .with_dimension(Dimension::time("created_at"))
                    .with_metric(Metric::sum("revenue", "amount"))
                    .with_metric(Metric::time_comparison(
                        "revenue_yoy",
                        "revenue",
                        ComparisonType::Yoy,
                    )),
            )
            .unwrap();
        graph
            .add_metric(Metric::derived("total", "orders.revenue"))
            .unwrap();
        graph
            .add_parameter(Parameter {
                name: "currency".into(),
                parameter_type: ParameterType::String,
                description: None,
                label: None,
                default_value: Some(serde_json::json!("USD")),
                allowed_values: None,
                default_to_today: false,
            })
            .unwrap();
        graph
            .add_table_calculation(
                TableCalculation::new("running_revenue", TableCalcType::RunningTotal)
                    .with_field("revenue"),
            )
            .unwrap();
        graph.set_metadata(serde_json::json!({"owner":"finance"}));
        assert_eq!(graph.metrics().count(), 2);
        let snapshot = serde_json::to_string(&Snapshot::from_graph(&graph)).unwrap();
        let rebuilt = Snapshot::parse(&snapshot).unwrap().into_graph().unwrap();
        assert_eq!(rebuilt.graph_metrics().count(), 1);
        assert_eq!(rebuilt.metrics().count(), 2);
        assert!(rebuilt.get_parameter("currency").is_some());
        assert!(rebuilt.get_table_calculation("running_revenue").is_some());
        assert_eq!(
            serde_json::to_string(&Snapshot::from_graph(&rebuilt)).unwrap(),
            snapshot
        );
        let (changed, _) = apply_ffi(
            &snapshot,
            "orders",
            "item",
            "DIMENSION (name status, type categorical);",
            false,
        )
        .unwrap();
        let changed = Snapshot::parse(&changed).unwrap().into_graph().unwrap();
        assert!(changed.get_table_calculation("running_revenue").is_some());
        assert!(changed.get_parameter("currency").is_some());
    }

    #[test]
    fn file_import_captures_contents_and_supports_independent_candidates() {
        let path = std::env::temp_dir().join(format!(
            "sidemantic-snapshot-{}-{}.sql",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, "MODEL (name orders, table captured_orders, primary_key id); METRIC revenue AS SUM(amount);").unwrap();
        let (snapshot, _) = apply_ffi("", "", "file", path.to_str().unwrap(), false).unwrap();
        std::fs::remove_file(&path).unwrap();
        let candidates: Vec<_> = ["first", "second"].into_iter().map(|table| {
            let snapshot = snapshot.clone();
            std::thread::spawn(move || {
                let (snapshot, _) = apply_ffi(&snapshot, "orders", "model", &format!("MODEL (name orders, table {table}, primary_key id); METRIC revenue AS SUM(amount);"), true).unwrap();
                rewrite(&snapshot, "SELECT orders.revenue FROM orders").unwrap()
            })
        }).collect();
        let sql = rewrite(&snapshot, "SELECT orders.revenue FROM orders").unwrap();
        assert!(sql.contains("captured_orders"), "{sql}");
        for (candidate, table) in candidates.into_iter().zip(["first", "second"]) {
            let sql = candidate.join().unwrap();
            assert!(sql.contains(table), "{sql}");
            assert!(!sql.contains("captured_orders"), "{sql}");
        }
    }

    #[test]
    fn model_list_preserves_punctuation_and_reports_semantic_fields() {
        let yaml = "models:\n  - name: 'orders, archive'\n    table: raw_orders\n    primary_key: id\n    dimensions:\n      - name: status\n        type: categorical\n    metrics:\n      - name: revenue\n        agg: sum\n        sql: amount\n    segments:\n      - name: active\n        sql: status = 'active'\n";
        let (snapshot, active) = apply_ffi("", "", "yaml", yaml, false).unwrap();
        let (snapshot, _) = apply_ffi(
            &snapshot,
            &active,
            "item",
            "DIMENSION (name channel, type categorical);",
            false,
        )
        .unwrap();
        let snapshot = CString::new(snapshot).unwrap();
        let result = sidemantic_snapshot_list_models(snapshot.as_ptr());
        assert!(result.error.is_null());
        assert_eq!(result.count, 1);
        unsafe {
            let model = &*result.models;
            assert_eq!(
                CStr::from_ptr(model.name).to_str().unwrap(),
                "orders, archive"
            );
            let fields: Vec<_> = std::slice::from_raw_parts(model.fields, model.field_count)
                .iter()
                .map(|field| CStr::from_ptr(*field).to_str().unwrap())
                .collect();
            assert_eq!(fields, ["active", "channel", "revenue", "status"]);
        }
        sidemantic_free_model_list(result);
        let empty = sidemantic_snapshot_list_models(ptr::null());
        assert!(empty.error.is_null());
        assert_eq!(empty.count, 0);
        sidemantic_free_model_list(empty);
    }

    #[test]
    fn snapshot_boundary_rejects_invalid_inputs_without_partial_results() {
        for snapshot in ["{", "{\"version\":2}", "{\"version\":1,\"surprise\":true}"] {
            assert!(apply_ffi(
                snapshot,
                "",
                "model",
                "MODEL (name orders, table orders);",
                false
            )
            .is_err());
            let snapshot = CString::new(snapshot).unwrap();
            let list = sidemantic_snapshot_list_models(snapshot.as_ptr());
            assert!(!list.error.is_null());
            assert!(list.models.is_null());
            sidemantic_free_model_list(list);
        }
        let operation = CString::new("model").unwrap();
        let content = CString::new("MODEL (name orders, table orders);").unwrap();
        let invalid_utf8 = [0xff_u8, 0];
        assert!(take_apply(sidemantic_snapshot_apply(
            ptr::null(),
            ptr::null(),
            ptr::null(),
            content.as_ptr(),
            false
        ))
        .is_err());
        assert!(take_apply(sidemantic_snapshot_apply(
            ptr::null(),
            ptr::null(),
            operation.as_ptr(),
            ptr::null(),
            false
        ))
        .is_err());
        assert!(take_apply(sidemantic_snapshot_apply(
            invalid_utf8.as_ptr().cast(),
            ptr::null(),
            operation.as_ptr(),
            content.as_ptr(),
            false
        ))
        .is_err());
        assert!(apply_ffi("", "", "unknown", "", false).is_err());
        let created = take_apply(sidemantic_snapshot_apply(
            ptr::null(),
            ptr::null(),
            operation.as_ptr(),
            content.as_ptr(),
            false,
        ))
        .unwrap();
        assert_eq!(created.1, "orders");
    }
}
