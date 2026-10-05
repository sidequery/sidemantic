//! Read-only catalog discovery for native semantic SQL hosts.

use serde_json::{json, Value};

use super::*;

fn canonical_snapshot(snapshot: &str) -> CatalogResult<Snapshot> {
    Ok(Snapshot::from_graph(
        &Snapshot::parse(snapshot)?.into_graph()?,
    ))
}

fn text_result(operation: impl FnOnce() -> CatalogResult<String>) -> SidemanticRewriteResult {
    semantic_result(guard(operation).map_err(semantic_error))
}

/// Return the complete, versioned catalog, including non-model declarations.
#[no_mangle]
pub extern "C" fn sidemantic_snapshot_export(snapshot: *const c_char) -> SidemanticRewriteResult {
    text_result(|| {
        serde_json::to_string(&canonical_snapshot(&optional_arg(snapshot, "snapshot")?)?)
            .map_err(|error| error.to_string())
    })
}

#[derive(Debug)]
struct CatalogEntry {
    kind: &'static str,
    model_name: Option<String>,
    name: String,
    qualified_name: String,
    definition: Value,
}

impl CatalogEntry {
    fn new<T: Serialize>(
        kind: &'static str,
        model: Option<&str>,
        name: &str,
        definition: &T,
    ) -> CatalogResult<Self> {
        Ok(Self {
            kind,
            model_name: model.map(str::to_owned),
            name: name.to_owned(),
            qualified_name: model
                .map_or_else(|| name.to_owned(), |model| format!("{model}.{name}")),
            definition: serde_json::to_value(definition).map_err(|error| error.to_string())?,
        })
    }
}

fn entries(snapshot: &Snapshot, kind: &str, model: &str) -> CatalogResult<Vec<CatalogEntry>> {
    if ![
        "",
        "model",
        "metric",
        "dimension",
        "segment",
        "relationship",
        "parameter",
        "table_calculation",
    ]
    .contains(&kind)
    {
        return Err(format!("unknown catalog kind '{kind}'"));
    }
    let selected = if model.is_empty() {
        None
    } else {
        Some(
            snapshot
                .models
                .iter()
                .find(|candidate| candidate.name.eq_ignore_ascii_case(model))
                .ok_or_else(|| format!("model '{model}' not found"))?
                .name
                .as_str(),
        )
    };
    let mut entries = Vec::new();
    for model in &snapshot.models {
        if selected.is_some_and(|selected| selected != model.name) {
            continue;
        }
        entries.push(CatalogEntry::new("model", None, &model.name, model)?);
        for dimension in &model.dimensions {
            entries.push(CatalogEntry::new(
                "dimension",
                Some(&model.name),
                &dimension.name,
                dimension,
            )?);
        }
        for metric in &model.metrics {
            entries.push(CatalogEntry::new(
                "metric",
                Some(&model.name),
                &metric.name,
                metric,
            )?);
        }
        for segment in &model.segments {
            entries.push(CatalogEntry::new(
                "segment",
                Some(&model.name),
                &segment.name,
                segment,
            )?);
        }
        for relationship in &model.relationships {
            let mut entry = CatalogEntry::new(
                "relationship",
                Some(&model.name),
                &relationship.name,
                relationship,
            )?;
            entry.definition["target_model"] = json!(relationship.related_model());
            entries.push(entry);
        }
    }
    if selected.is_none() {
        for metric in &snapshot.metrics {
            entries.push(CatalogEntry::new("metric", None, &metric.name, metric)?);
        }
        for parameter in &snapshot.parameters {
            entries.push(CatalogEntry::new(
                "parameter",
                None,
                &parameter.name,
                parameter,
            )?);
        }
        for calculation in &snapshot.table_calculations {
            entries.push(CatalogEntry::new(
                "table_calculation",
                None,
                &calculation.name,
                calculation,
            )?);
        }
    }
    entries.retain(|entry| kind.is_empty() || entry.kind == kind);
    entries.sort_by(|a, b| (&a.kind, &a.qualified_name).cmp(&(&b.kind, &b.qualified_name)));
    Ok(entries)
}

#[repr(C)]
pub struct SidemanticCatalogEntry {
    pub kind: *mut c_char,
    pub model_name: *mut c_char,
    pub name: *mut c_char,
    pub qualified_name: *mut c_char,
    pub label: *mut c_char,
    pub description: *mut c_char,
    pub semantic_type: *mut c_char,
    pub data_type: *mut c_char,
    pub sql: *mut c_char,
    pub aggregation: *mut c_char,
    pub target_model: *mut c_char,
    pub relationship_type: *mut c_char,
    pub granularity: *mut c_char,
    pub is_public: bool,
    pub definition: *mut c_char,
}

impl SidemanticCatalogEntry {
    fn from_entry(entry: CatalogEntry) -> CatalogResult<Self> {
        // Validate every allocation before transferring ownership to C. Null
        // means absent metadata; it is distinct from an authored empty string.
        let field = |value: Option<&str>| -> CatalogResult<Option<CString>> {
            value
                .map(CString::new)
                .transpose()
                .map_err(|_| "catalog metadata contains a NUL byte".into())
        };
        let property = |name: &str| field(entry.definition.get(name).and_then(Value::as_str));
        let kind = field(Some(entry.kind))?;
        let model_name = field(entry.model_name.as_deref())?;
        let name = field(Some(&entry.name))?;
        let qualified_name = field(Some(&entry.qualified_name))?;
        let label = property("label")?;
        let description = property("description")?;
        let semantic_type = property("type")?;
        let data_type = property("logical_data_type")?;
        let sql = property("sql")?;
        let aggregation = property("agg")?;
        let target_model = property("target_model")?;
        let relationship_type = if entry.kind == "relationship" {
            property("type")?
        } else {
            None
        };
        let granularity = property("granularity")?;
        let definition = field(Some(&entry.definition.to_string()))?;
        let raw = |value: Option<CString>| value.map_or(ptr::null_mut(), CString::into_raw);
        Ok(Self {
            kind: raw(kind),
            model_name: raw(model_name),
            name: raw(name),
            qualified_name: raw(qualified_name),
            label: raw(label),
            description: raw(description),
            semantic_type: raw(semantic_type),
            data_type: raw(data_type),
            sql: raw(sql),
            aggregation: raw(aggregation),
            target_model: raw(target_model),
            relationship_type: raw(relationship_type),
            granularity: raw(granularity),
            is_public: entry
                .definition
                .get("public")
                .and_then(Value::as_bool)
                .unwrap_or(true),
            definition: raw(definition),
        })
    }
}

impl Drop for SidemanticCatalogEntry {
    fn drop(&mut self) {
        for value in [
            self.kind,
            self.model_name,
            self.name,
            self.qualified_name,
            self.label,
            self.description,
            self.semantic_type,
            self.data_type,
            self.sql,
            self.aggregation,
            self.target_model,
            self.relationship_type,
            self.granularity,
            self.definition,
        ] {
            sidemantic_free(value);
        }
    }
}

#[repr(C)]
pub struct SidemanticCatalogEntries {
    pub entries: *mut SidemanticCatalogEntry,
    pub count: usize,
    pub error: *mut c_char,
}

fn compatible_dimensions(snapshot: &str, metric: &str) -> CatalogResult<Vec<CatalogEntry>> {
    let snapshot = canonical_snapshot(snapshot)?;
    let names: Vec<String> = serde_json::from_str(metric).map_err(|e| e.to_string())?;
    let [model_name, metric_name] = names.as_slice() else {
        return Err("SHOW DIMENSIONS FOR requires model.metric".into());
    };
    let metric = entries(&snapshot, "metric", model_name)?
        .into_iter()
        .find(|entry| entry.name.eq_ignore_ascii_case(metric_name))
        .ok_or_else(|| format!("metric '{model_name}.{metric_name}' not found"))?;
    let candidates = entries(&snapshot, "dimension", "")?;
    let graph = snapshot.into_graph()?;
    let rewriter = QueryRewriter::new(&graph);
    let quote = |name: &str| format!("\"{}\"", name.replace('"', "\"\""));
    let owner = quote(metric.model_name.as_deref().unwrap());
    let projection = format!("{owner}.{}", quote(&metric.name));
    let compile = |selection: &str| {
        rewriter.rewrite_with_dialect(
            &format!("select {selection} from {owner}"),
            polyglot_sql::DialectType::DuckDB,
        )
    };
    // Invalid metrics must surface their error rather than look like an empty
    // dimension list. Each candidate then uses the same compiler as SELECT.
    compile(&projection).map_err(|error| error.to_string())?;
    Ok(candidates
        .into_iter()
        .filter(|entry| {
            let dimension = format!(
                "{}.{}",
                quote(entry.model_name.as_deref().unwrap()),
                quote(&entry.name)
            );
            compile(&format!("{projection}, {dimension}")).is_ok()
        })
        .collect())
}

/// A non-null metric holds the parsed model and metric identifiers as JSON.
/// Return dimensions whose SELECT combination compiles, not just graph neighbors.
#[no_mangle]
pub extern "C" fn sidemantic_snapshot_catalog(
    snapshot: *const c_char,
    kind: *const c_char,
    model: *const c_char,
    metric: *const c_char,
) -> SidemanticCatalogEntries {
    let result = guard(|| {
        let source = optional_arg(snapshot, "snapshot")?;
        let snapshot = canonical_snapshot(&source)?;
        let kind = optional_arg(kind, "kind")?;
        let model = optional_arg(model, "model")?;
        let rows = if metric.is_null() {
            entries(&snapshot, &kind, &model)?
        } else {
            compatible_dimensions(&source, &required_arg(metric, "metric")?)?
        };
        rows.into_iter()
            .map(SidemanticCatalogEntry::from_entry)
            .collect::<CatalogResult<Vec<_>>>()
    });
    match result {
        Ok(entries) => {
            let entries = entries.into_boxed_slice();
            SidemanticCatalogEntries {
                count: entries.len(),
                entries: Box::into_raw(entries).cast(),
                error: ptr::null_mut(),
            }
        }
        Err(error) => SidemanticCatalogEntries {
            entries: ptr::null_mut(),
            count: 0,
            error: semantic_error(error),
        },
    }
}

#[no_mangle]
pub extern "C" fn sidemantic_free_catalog_entries(result: SidemanticCatalogEntries) {
    if !result.entries.is_null() {
        unsafe {
            drop(Box::from_raw(ptr::slice_from_raw_parts_mut(
                result.entries,
                result.count,
            )));
        }
    }
    sidemantic_free(result.error);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> Snapshot {
        let (snapshot, _) = apply(
            "",
            "",
            "yaml",
            r#"
models:
  - name: orders
    table: raw_orders
    primary_key: id
    label: Orders
    description: Order facts
    dimensions:
      - name: status
        type: categorical
        description: Fulfilment status
    metrics:
      - name: revenue
        agg: sum
        sql: amount
        label: Revenue
        description: Gross revenue
    relationships:
      - name: customers
        type: many_to_one
        foreign_key: customer_id
  - name: customers
    table: raw_customers
    primary_key: id
    dimensions:
      - name: country
        type: categorical
  - name: unrelated
    table: unrelated
    primary_key: id
    dimensions:
      - name: name
        type: categorical
metrics:
  - name: total
    type: derived
    sql: orders.revenue
parameters:
  - name: region
    type: string
    default_value: US
metadata:
  owner: finance
"#,
            false,
        )
        .unwrap();
        canonical_snapshot(&snapshot).unwrap()
    }

    #[test]
    fn discovery_checks_select_combinations_with_the_query_compiler() {
        let snapshot = snapshot();
        let source = serde_json::to_string(&snapshot).unwrap();
        let rows = compatible_dimensions(&source, r#"["ORDERS","REVENUE"]"#).unwrap();
        let names: Vec<_> = rows.iter().map(|row| row.qualified_name.as_str()).collect();
        assert_eq!(names, ["customers.country", "orders.status"]);
        assert!(compatible_dimensions(&source, r#"["orders","missing"]"#).is_err());
        assert!(compatible_dimensions(&source, r#"["missing","revenue"]"#).is_err());
    }

    #[test]
    fn discovery_preserves_definitions_and_scopes() {
        let snapshot = snapshot();
        let rows = entries(&snapshot, "", "ORDERS").unwrap();
        assert!(rows
            .iter()
            .any(|row| row.kind == "model" && row.definition["description"] == "Order facts"));
        assert!(rows
            .iter()
            .any(|row| row.qualified_name == "orders.revenue"
                && row.definition["label"] == "Revenue"));
        assert!(
            rows.iter()
                .any(|row| row.kind == "relationship"
                    && row.definition["target_model"] == "customers")
        );
        // The loader exposes the global derived metric in its owning model too.
        assert_eq!(entries(&snapshot, "metric", "").unwrap().len(), 3);
        assert!(entries(&snapshot, "metric", "missing").is_err());
        assert!(entries(&snapshot, "unknown", "").is_err());
    }
}
