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
    active_model_for_loaded_models, add_item_definition, semantic_error, semantic_result,
    sidemantic_free, FfiState, SidemanticRewriteResult,
};

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

fn apply(
    snapshot: &str,
    active_model: &str,
    operation: &str,
    content: &str,
    replace: bool,
) -> CatalogResult<(String, String)> {
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
            let model = parse_sql_model(content).map_err(|e| format!("parsing definition: {e}"))?;
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
            add_item_definition(&mut state, content, replace)?;
        }
        "use" => {
            let name = content.trim();
            if state.graph.get_model(name).is_none() {
                return Err(format!("model '{name}' not found"));
            }
            state.active_model = Some(name.to_owned());
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
            QueryRewriter::new(&graph)
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
