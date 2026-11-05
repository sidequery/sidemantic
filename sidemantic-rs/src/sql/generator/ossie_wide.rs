//! Ossie Layer 3: independent home-row populations and a NULL-equal key spine.
use super::*;
use crate::core::replace_semantic_columns;
use crate::semantic_input::dialects::{self, Fragment};
use polyglot_sql::expressions::Column;
use serde_json::Value;
use std::collections::BTreeMap;

const REVISION: &str = "172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1";
fn fail(code: &str, message: impl std::fmt::Display) -> SidemanticError {
    SidemanticError::YardstickBinding(format!("{code}: {message}"))
}
fn source_name<'a>(name: &'a str, metadata: &'a Option<Value>) -> &'a str {
    metadata
        .as_ref()
        .and_then(|m| m["ossie_source_name"].as_str())
        .unwrap_or(name)
}
fn identifier(text: &str) -> Option<(String, String)> {
    if text.starts_with('"') && text.ends_with('"') && text.len() > 2 {
        let inner = &text[1..text.len() - 1];
        let decoded = inner.replace("\"\"", "\"");
        if decoded.replace('"', "\"\"") != inner || decoded.chars().count() > 128 {
            return None;
        }
        Some((decoded.clone(), decoded))
    } else if !text.is_empty()
        && text.chars().count() <= 128
        && text
            .chars()
            .enumerate()
            .all(|(i, c)| c == '_' || c.is_alphabetic() || (i > 0 && c.is_alphanumeric()))
    {
        Some((text.to_uppercase(), text.to_owned()))
    } else {
        None
    }
}
fn stored_dialect(metadata: &Option<Value>, fallback: DialectType) -> Result<DialectType> {
    metadata
        .as_ref()
        .and_then(|m| m["ossie_target_dialect"].as_str())
        .map(dialects::parse_dialect)
        .transpose()
        .map(|dialect| dialect.unwrap_or(fallback))
}
fn normal(text: &str) -> String {
    identifier(text)
        .map(|x| x.0)
        .unwrap_or_else(|| text.to_uppercase())
}
fn ast_kind(value: &Value) -> Option<&str> {
    value
        .as_object()
        .filter(|o| o.len() == 1)
        .and_then(|o| o.keys().next())
        .map(String::as_str)
}
fn contains_kind(value: &Value, predicate: fn(&Value) -> bool) -> bool {
    predicate(value)
        || match value {
            Value::Object(o) => o.values().any(|v| contains_kind(v, predicate)),
            Value::Array(a) => a.iter().any(|v| contains_kind(v, predicate)),
            _ => false,
        }
}
fn is_window(value: &Value) -> bool {
    matches!(ast_kind(value), Some("window" | "window_function"))
}
fn unaggregated_field(value: &Value) -> bool {
    if crate::core::is_aggregate_ast_node(value)
        || matches!(ast_kind(value), Some("filter" | "within_group"))
    {
        return false;
    }
    if ast_kind(value) == Some("column") {
        return true;
    }
    match value {
        Value::Object(fields) => fields.values().any(unaggregated_field),
        Value::Array(children) => children.iter().any(unaggregated_field),
        _ => false,
    }
}
fn nested_aggregate(value: &Value, inside_aggregate: bool) -> bool {
    let aggregate = crate::core::is_aggregate_ast_node(value);
    if aggregate && inside_aggregate {
        return true;
    }
    let nested = inside_aggregate || aggregate;
    match value {
        Value::Object(fields) => fields.values().any(|child| nested_aggregate(child, nested)),
        Value::Array(children) => children.iter().any(|child| nested_aggregate(child, nested)),
        _ => false,
    }
}
fn non_boolean_predicate(expression: &Expression) -> bool {
    match expression {
        Expression::Paren(paren) => non_boolean_predicate(&paren.this),
        Expression::Cast(cast) => !matches!(
            cast.to,
            polyglot_sql::expressions::DataType::Boolean
                | polyglot_sql::expressions::DataType::Unknown
        ),
        Expression::Literal(_)
        | Expression::Add(_)
        | Expression::Sub(_)
        | Expression::Mul(_)
        | Expression::Div(_)
        | Expression::Mod(_)
        | Expression::Concat(_)
        | Expression::Interval(_)
        | Expression::Date(_) => true,
        _ => false,
    }
}
fn ast(expression: &Expression) -> Result<Value> {
    serde_json::to_value(expression).map_err(|e| fail("E_INVALID_QUERY", e))
}
fn expression(value: Value) -> Result<Expression> {
    serde_json::from_value(value).map_err(|e| fail("E_INVALID_QUERY", e))
}
fn parse_reference(text: &str) -> Option<Column> {
    match parse_semantic_expression(text).ok()? {
        Expression::Column(c)
            if c.name.name.chars().count() <= 128
                && c.table
                    .as_ref()
                    .is_none_or(|t| t.name.chars().count() <= 128) =>
        {
            Some(*c)
        }
        _ => None,
    }
}
fn id_normal(id: &Identifier) -> String {
    if id.quoted {
        id.name.clone()
    } else {
        id.name.to_uppercase()
    }
}
#[derive(Clone)]
enum Reference {
    Field(String, String),
    Metric(Option<String>, Box<Metric>),
}
#[derive(Clone)]
struct Entry {
    reference: String,
    output: String,
    normalized_output: String,
    resolved: Reference,
}
#[derive(Clone)]
struct Component {
    home: String,
    sql: String,
}
#[derive(Clone)]
struct Edge {
    from: String,
    to: String,
    index: usize,
}
struct Planner<'a, 'g> {
    generator: &'a SqlGenerator<'g>,
    components: Vec<Component>,
    paths: HashMap<(String, String), Vec<Vec<Edge>>>,
}
impl SqlGenerator<'_> {
    /// Strict JSON binding for Apache Ossie Layer 3 (wide-table semantics §3.7).
    pub fn generate_ossie_wide(&self, query: &Value) -> Result<String> {
        if !self.uses_latest_ossie() {
            return Err(fail(
                "E_INVALID_QUERY",
                "wide-table queries require an exclusively latest Ossie model scope",
            ));
        }
        crate::semantic_input::with_semantic_stack(|| {
            Planner {
                generator: self,
                components: vec![],
                paths: HashMap::new(),
            }
            .compile(query)
        })
    }
    pub(super) fn uses_latest_ossie(&self) -> bool {
        self.graph.models().next().is_some()
            && self.graph.models().all(|m| {
                m.metadata
                    .as_ref()
                    .and_then(|v| v["ossie_schema_revision"].as_str())
                    == Some(REVISION)
            })
    }
    pub(super) fn generate_latest_ossie(&self, query: &SemanticQuery) -> Result<String> {
        // Sorting/pagination are explicit native API extensions. Population
        // changes still follow the draft's single-filter contract.
        if !query.segments.is_empty()
            || !query.table_calculations.is_empty()
            || query.ungrouped
            || query.with_totals
            || query.consumption_base_model.is_some()
            || !query.required_population_models.is_empty()
            || query.parameter_values.is_some()
            || query.prepared_policies.model_names().next().is_some()
        {
            return Err(fail(
                "E_INVALID_QUERY",
                "unsupported Ossie wide-table query extension",
            ));
        }
        let output_name = |name: &str| {
            query.aliases.get(name).cloned().unwrap_or_else(|| {
                if query.metrics.iter().any(|metric| metric == name) {
                    name.to_owned()
                } else {
                    parse_reference(name)
                        .map(|column| column.name.name)
                        .unwrap_or_else(|| name.to_owned())
                }
            })
        };
        let entries = |names: &[String], key: &str| {
            names
                .iter()
                .map(|name| {
                    // Structured aliases are literal output names, whereas the
                    // strict binding accepts SQL identifiers. Quote the whole
                    // name so qualified metric references remain one output.
                    let alias = format!("\"{}\"", output_name(name).replace('"', "\"\""));
                    serde_json::json!({key: name, "alias": alias})
                })
                .collect::<Vec<_>>()
        };
        let mut request = serde_json::json!({"dimensions": entries(&query.dimensions, "field"), "metrics": entries(&query.metrics, "metric")});
        if !query.filters.is_empty() {
            request["filter"] = query
                .filters
                .iter()
                .map(|filter| format!("({filter})"))
                .collect::<Vec<_>>()
                .join(" AND ")
                .into();
        }
        let mut sql = self.generate_ossie_wide(&request)?;
        if !query.order_by.is_empty() {
            let mut orders = vec![];
            for order in &query.order_by {
                let (name, direction) = order
                    .strip_suffix(" DESC")
                    .map(|n| (n, " DESC"))
                    .or_else(|| order.strip_suffix(" ASC").map(|n| (n, " ASC")))
                    .unwrap_or((order.as_str(), ""));
                let selected =
                    query
                        .metrics
                        .iter()
                        .chain(&query.dimensions)
                        .find(|reference| {
                            match (parse_reference(reference), parse_reference(name)) {
                                (Some(selected), Some(ordered)) => {
                                    id_normal(&selected.name) == id_normal(&ordered.name)
                                        && selected.table.as_ref().map(id_normal)
                                            == ordered.table.as_ref().map(id_normal)
                                }
                                _ => false,
                            }
                        });
                let output = output_name(selected.map(String::as_str).unwrap_or(name));
                orders.push(format!("{}{direction}", self.quote_identifier(&output)));
            }
            sql.push_str(&format!(" ORDER BY {}", orders.join(", ")));
        }
        if let Some(limit) = query.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }
        if let Some(offset) = query.offset {
            sql.push_str(&format!(" OFFSET {offset}"));
        }
        Ok(sql)
    }
}
impl<'a, 'g> Planner<'a, 'g> {
    fn q(&self, name: &str) -> String {
        self.generator.quote_identifier(name)
    }
    fn emit(&self, value: &Value) -> Result<String> {
        self.emit_final(expression(value.clone())?)
    }
    fn emit_final(&self, expression: Expression) -> Result<String> {
        let expression =
            dialects::ossie_index_offsets(expression, DialectType::DuckDB, self.generator.dialect)?;
        self.generator.emit_expression(&expression)
    }
    fn source_expression(&self, sql: &str, source: DialectType) -> Result<Expression> {
        let canonical = dialects::fragment(sql, source, Fragment::Scalar)?;
        dialects::ossie_index_offsets(
            parse_semantic_expression(&canonical)?,
            source,
            DialectType::DuckDB,
        )
    }
    fn canonical(&self, value: &Value) -> Result<String> {
        dialects::generate_preserving_null_order(
            &expression(value.clone())?,
            DialectType::DuckDB,
            DialectType::DuckDB,
        )
    }
    fn resolve(&self, column: &Column) -> Option<Reference> {
        let field = id_normal(&column.name);
        if let Some(table) = &column.table {
            let owner = self
                .generator
                .graph
                .models()
                .find(|m| normal(source_name(&m.name, &m.metadata)) == id_normal(table))?;
            if let Some(dimension) = owner.dimensions.iter().find(|d| {
                d.public
                    && !d.metadata.as_ref().is_some_and(|m| {
                        m.get("ossie_private_input").is_some()
                            || m.get("ossie_internal_expression").is_some()
                    })
                    && normal(source_name(&d.name, &d.metadata)) == field
            }) {
                return Some(Reference::Field(owner.name.clone(), dimension.name.clone()));
            }
            for metric in self.generator.graph.metrics().chain(owner.metrics.iter()) {
                let metric_owner = self
                    .generator
                    .graph
                    .metric_owner(&metric.name)
                    .or_else(|| {
                        metric
                            .metadata
                            .as_ref()
                            .and_then(|m| m["ossie_dataset_owner"].as_str())
                    })
                    .or_else(|| {
                        owner
                            .metrics
                            .iter()
                            .any(|m| std::ptr::eq(m, metric))
                            .then_some(owner.name.as_str())
                    });
                if metric_owner == Some(owner.name.as_str())
                    && normal(source_name(
                        metric.name.rsplit('.').next().unwrap_or(&metric.name),
                        &metric.metadata,
                    )) == field
                {
                    return Some(Reference::Metric(
                        Some(owner.name.clone()),
                        Box::new(metric.clone()),
                    ));
                }
            }
            None
        } else {
            self.generator
                .graph
                .metrics()
                .find(|m| {
                    self.generator.graph.metric_owner(&m.name).is_none()
                        && !m
                            .metadata
                            .as_ref()
                            .is_some_and(|v| v["ossie_dataset_metric"].as_bool() == Some(true))
                        && normal(source_name(&m.name, &m.metadata)) == field
                })
                .map(|m| Reference::Metric(None, Box::new(m.clone())))
        }
    }
    fn entry_shapes(
        &self,
        query: &Value,
        part: &str,
        key: &str,
    ) -> Result<Vec<(String, Option<String>)>> {
        let Some(value) = query.get(part) else {
            return Ok(vec![]);
        };
        let list = value
            .as_array()
            .ok_or_else(|| fail("E_INVALID_QUERY", format!("{part} must be an array")))?;
        list.iter()
            .map(|value| {
                let (name, alias) = if let Some(name) = value.as_str() {
                    (name, None)
                } else {
                    let obj = value
                        .as_object()
                        .ok_or_else(|| fail("E_INVALID_QUERY", "invalid reference entry"))?;
                    if obj.keys().any(|k| k != key && k != "alias") {
                        return Err(fail("E_INVALID_QUERY", "unknown entry property"));
                    }
                    let name = obj
                        .get(key)
                        .and_then(Value::as_str)
                        .ok_or_else(|| fail("E_INVALID_QUERY", "missing reference"))?;
                    let alias = obj
                        .get("alias")
                        .map(|v| {
                            v.as_str().ok_or_else(|| {
                                fail("E_INVALID_QUERY", "alias must be an identifier")
                            })
                        })
                        .transpose()?;
                    (name, alias)
                };
                if parse_reference(name).is_none() || alias.is_some_and(|a| identifier(a).is_none())
                {
                    return Err(fail(
                        "E_INVALID_QUERY",
                        "invalid identifier reference or alias",
                    ));
                }
                Ok((name.to_owned(), alias.map(str::to_owned)))
            })
            .collect()
    }
    fn entries(&self, shapes: Vec<(String, Option<String>)>) -> Result<Vec<Entry>> {
        shapes
            .into_iter()
            .map(|(name, alias)| {
                let column = parse_reference(&name).unwrap();
                let resolved = self
                    .resolve(&column)
                    .ok_or_else(|| fail("E_NAME_NOT_FOUND", &name))?;
                let (normalized_output, output) = alias
                    .as_deref()
                    .and_then(identifier)
                    .unwrap_or_else(|| (id_normal(&column.name), column.name.name.clone()));
                Ok(Entry {
                    reference: name,
                    output,
                    normalized_output,
                    resolved,
                })
            })
            .collect()
    }
    fn compile(&mut self, query: &Value) -> Result<String> {
        let object = query
            .as_object()
            .ok_or_else(|| fail("E_INVALID_QUERY", "query must be an object"))?;
        if object
            .keys()
            .any(|k| !["dimensions", "metrics", "filter"].contains(&k.as_str()))
        {
            return Err(fail("E_INVALID_QUERY", "unknown query property"));
        }
        let dimension_shapes = self.entry_shapes(query, "dimensions", "field")?;
        let metric_shapes = self.entry_shapes(query, "metrics", "metric")?;
        let filter = object
            .get("filter")
            .map(|value| {
                let sql = value
                    .as_str()
                    .filter(|s| !s.trim().is_empty())
                    .ok_or_else(|| {
                        fail("E_INVALID_QUERY", "filter must be a nonempty expression")
                    })?;
                crate::adapters::ossie_sql::lower_ossie_sql(sql, DialectType::DuckDB)
                    .map_err(|error| fail("E_INVALID_QUERY", format!("malformed filter: {error}")))
                    .and_then(|lowered| {
                        parse_semantic_expression(&lowered).map_err(|error| {
                            fail("E_INVALID_QUERY", format!("malformed filter: {error}"))
                        })
                    })
            })
            .transpose()?;
        if filter.as_ref().is_some_and(non_boolean_predicate) {
            return Err(fail(
                "E_INVALID_QUERY",
                "filter must be a Boolean expression",
            ));
        }
        if dimension_shapes.is_empty() && metric_shapes.is_empty() {
            return Err(fail(
                "E_EMPTY_AGGREGATION_QUERY",
                "at least one dimension or metric is required",
            ));
        }
        let dimensions = self.entries(dimension_shapes)?;
        let metrics = self.entries(metric_shapes)?;
        let mut filter_ast = filter.as_ref().map(ast).transpose()?;
        let mut filter_owners = HashSet::new();
        let mut filter_metric = false;
        if let Some(value) = &mut filter_ast {
            self.bind_filter(value, &mut filter_owners, &mut filter_metric)?;
        }
        if dimensions
            .iter()
            .any(|e| matches!(e.resolved, Reference::Metric(..)))
        {
            return Err(fail(
                "E_METRIC_IN_DIMENSIONS",
                "dimensions must name fields",
            ));
        }
        if metrics
            .iter()
            .any(|e| matches!(e.resolved, Reference::Field(..)))
        {
            return Err(fail("E_FIELD_IN_METRICS", "metrics must name metrics"));
        }
        if filter_metric
            || filter_ast
                .as_ref()
                .is_some_and(|v| contains_kind(v, crate::core::is_aggregate_ast_node))
        {
            return Err(fail(
                "E_AGGREGATE_IN_WHERE",
                "filter cannot aggregate or reference metrics",
            ));
        }
        if filter_ast
            .as_ref()
            .is_some_and(|v| contains_kind(v, is_window))
        {
            return Err(fail("E_WINDOW_IN_WHERE", "filter cannot contain windows"));
        }
        if filter_owners.len() > 1 {
            return Err(fail(
                "E_CROSS_DATASET_FILTER",
                "filter must reference one dataset",
            ));
        }
        let mut outputs = HashSet::new();
        if dimensions
            .iter()
            .chain(metrics.iter())
            .any(|e| !outputs.insert(&e.normalized_output))
        {
            return Err(fail(
                "E_NAME_COLLISION",
                "output names must be unique after normalization",
            ));
        }
        let mut metric_expressions = vec![];
        let mut metric_homes = vec![];
        let mut metric_errors = vec![];
        for entry in &metrics {
            let Reference::Metric(owner, metric) = &entry.resolved else {
                unreachable!()
            };
            let metric_sql = metric
                .sql
                .as_deref()
                .ok_or_else(|| fail("E_INVALID_QUERY", "metric has no expression"))?;
            let mut value = ast(&self.source_expression(
                metric_sql,
                stored_dialect(&metric.metadata, DialectType::DuckDB)?,
            )?)?;
            let begin = self.components.len();
            if nested_aggregate(&value, false) {
                metric_errors.push((
                    1,
                    "E_INVALID_QUERY",
                    format!("metric {} contains nested aggregate calls", entry.reference),
                ));
            }
            if let Some(owner) = owner {
                if contains_kind(&value, is_window)
                    || unaggregated_field(&value)
                    || nested_aggregate(&value, false)
                    || !contains_kind(&value, crate::core::is_aggregate_ast_node)
                {
                    metric_errors.push((1, "E_INVALID_QUERY", format!("dataset metric {} must be an aggregate measure without windows or unaggregated fields", entry.reference)));
                }
                let sql = self.canonical(&value)?;
                let index = self.components.len();
                self.components.push(Component {
                    home: owner.clone(),
                    sql,
                });
                value = ast(&parse_semantic_expression(&format!(
                    "__ossie_values.__ossie_c{index}"
                ))?)?;
            } else if let Err((rank, code)) = self.decompose(&mut value) {
                metric_errors.push((rank, code, entry.reference.clone()));
            }
            metric_homes.push(
                self.components[begin..]
                    .iter()
                    .map(|c| c.home.clone())
                    .collect::<HashSet<_>>(),
            );
            metric_expressions.push(value);
        }
        if let Some((_, code, reference)) = metric_errors.into_iter().min_by_key(|e| e.0) {
            return Err(fail(code, reference));
        }
        let dimension_owners = dimensions
            .iter()
            .map(|d| match &d.resolved {
                Reference::Field(m, _) => m.clone(),
                _ => unreachable!(),
            })
            .collect::<Vec<_>>();
        let filter_owner = filter_owners.into_iter().next();
        let mut homes = self
            .components
            .iter()
            .map(|c| c.home.clone())
            .collect::<Vec<_>>();
        homes.sort();
        homes.dedup();
        let mut path_errors = vec![];
        if metrics.is_empty() {
            let mut candidates = dimension_owners.clone();
            candidates.sort();
            candidates.dedup();
            candidates.retain(|home| {
                dimension_owners
                    .iter()
                    .all(|target| !self.all_paths(home, target).is_empty())
            });
            if candidates.is_empty() {
                path_errors.push((
                    15,
                    "E_NO_ANCHOR",
                    "dimensions have no common anchor".to_owned(),
                ));
            }
            if candidates.len() > 1 {
                path_errors.push((
                    17,
                    "E_AMBIGUOUS_PATH",
                    "more than one dimension anchor".to_owned(),
                ));
            }
            homes = candidates.into_iter().take(1).collect();
        } else {
            for (dimension, target) in dimensions.iter().zip(&dimension_owners) {
                let reached = metric_homes
                    .iter()
                    .map(|owners| {
                        owners
                            .iter()
                            .all(|home| !self.all_paths(home, target).is_empty())
                    })
                    .collect::<Vec<_>>();
                let missing = metrics
                    .iter()
                    .zip(&reached)
                    .filter(|(_, reached)| !**reached)
                    .map(|(entry, _)| entry.reference.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                if reached.iter().all(|r| !r) {
                    path_errors.push((
                        13,
                        "E_NO_PATH",
                        format!(
                            "dimension {} is unreachable from metrics {missing}",
                            dimension.reference,
                        ),
                    ));
                } else if reached.iter().any(|r| !r) {
                    path_errors.push((
                        14,
                        "E_PARTIAL_DIMENSION_REACHABILITY",
                        format!(
                            "dimension {} is unreachable from metrics {missing}",
                            dimension.reference,
                        ),
                    ));
                }
            }
        }
        for home in &homes {
            for target in &dimension_owners {
                if self.all_paths(home, target).len() > 1 {
                    path_errors.push((17, "E_AMBIGUOUS_PATH", format!("{home} -> {target}")));
                }
            }
            if let Some(target) = &filter_owner {
                let affected = if metrics.is_empty() {
                    format!("anchor {home}")
                } else {
                    metrics
                        .iter()
                        .zip(&metric_homes)
                        .filter(|(_, owners)| owners.contains(home))
                        .map(|(entry, _)| entry.reference.clone())
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                match self.all_paths(home, target).len() {
                    0 => path_errors.push((
                        16,
                        "E_UNSUPPORTED_FILTER_PATH",
                        format!(
                            "filter dataset {target} is unreachable from {affected} (home {home})"
                        ),
                    )),
                    1 => (),
                    _ => path_errors.push((
                        17,
                        "E_AMBIGUOUS_PATH",
                        format!("{home} -> filter dataset {target}"),
                    )),
                }
            }
        }
        if let Some((_, code, message)) = path_errors.into_iter().min_by_key(|e| e.0) {
            return Err(fail(code, message));
        }
        self.render(
            &dimensions,
            &metrics,
            &metric_expressions,
            &homes,
            filter_ast.as_ref(),
            filter_owner.as_deref(),
        )
    }
    fn bind_filter(
        &self,
        node: &mut Value,
        owners: &mut HashSet<String>,
        metric: &mut bool,
    ) -> Result<()> {
        if ast_kind(node) == Some("column") {
            let column: Column = serde_json::from_value(node["column"].clone())
                .map_err(|e| fail("E_INVALID_QUERY", e))?;
            match self
                .resolve(&column)
                .ok_or_else(|| fail("E_NAME_NOT_FOUND", self.emit(node).unwrap_or_default()))?
            {
                Reference::Field(owner, field) => {
                    owners.insert(owner.clone());
                    *node = ast(&parse_semantic_expression(&format!(
                        "{}.{}",
                        self.q(&owner),
                        self.q(&field)
                    ))?)?;
                }
                Reference::Metric(..) => *metric = true,
            }
            return Ok(());
        }
        match node {
            Value::Object(o) => {
                for v in o.values_mut() {
                    self.bind_filter(v, owners, metric)?
                }
            }
            Value::Array(a) => {
                for v in a {
                    self.bind_filter(v, owners, metric)?
                }
            }
            _ => (),
        }
        Ok(())
    }
    fn decompose(&mut self, node: &mut Value) -> std::result::Result<(), (usize, &'static str)> {
        if is_window(node) {
            return Err((11, "E_DEFERRED_WINDOWED_METRIC"));
        }
        if crate::core::is_aggregate_ast_node(node)
            || matches!(ast_kind(node), Some("filter" | "within_group"))
        {
            if contains_kind(node, is_window) {
                return Err((11, "E_DEFERRED_WINDOWED_METRIC"));
            }
            let sql = self
                .canonical(node)
                .map_err(|_| (12, "E_AMBIGUOUS_MEASURE_GRAIN"))?;
            // FILTER restricts the population but never establishes its home.
            // COUNT stores FILTER in its payload; others use an outer wrapper.
            fn arguments(node: &mut Value) {
                if ast_kind(node) == Some("filter") {
                    *node = node["filter"]["this"].clone();
                }
                match node {
                    Value::Object(fields) => {
                        if let Some(payload) =
                            fields.values_mut().next().and_then(Value::as_object_mut)
                        {
                            payload.remove("filter");
                        }
                        for child in fields.values_mut() {
                            arguments(child);
                        }
                    }
                    Value::Array(children) => {
                        for child in children {
                            arguments(child);
                        }
                    }
                    _ => (),
                }
            }
            let mut argument_ast = node.clone();
            arguments(&mut argument_ast);
            let argument_sql = self
                .canonical(&argument_ast)
                .map_err(|_| (12, "E_AMBIGUOUS_MEASURE_GRAIN"))?;
            let refs = semantic_column_references(&argument_sql)
                .map_err(|_| (12, "E_AMBIGUOUS_MEASURE_GRAIN"))?;
            let owners = refs
                .into_iter()
                .filter_map(|r| r.model)
                .collect::<HashSet<_>>();
            if owners.len() != 1 {
                return Err((12, "E_AMBIGUOUS_MEASURE_GRAIN"));
            }
            let home = owners.into_iter().next().unwrap();
            if semantic_column_references(&sql)
                .map_err(|_| (12, "E_AMBIGUOUS_MEASURE_GRAIN"))?
                .iter()
                .any(|r| r.model.as_deref() != Some(home.as_str()))
            {
                return Err((12, "E_AMBIGUOUS_MEASURE_GRAIN"));
            }
            if self.generator.graph.get_model(&home).is_none() {
                return Err((12, "E_AMBIGUOUS_MEASURE_GRAIN"));
            }
            let index = self.components.len();
            self.components.push(Component { home, sql });
            *node = ast(
                &parse_semantic_expression(&format!("__ossie_values.__ossie_c{index}")).unwrap(),
            )
            .unwrap();
            return Ok(());
        }
        if ast_kind(node) == Some("column") {
            return Err((10, "E_UNAGGREGATED_FINER_GRAIN_REFERENCE"));
        }
        let mut errors = vec![];
        match node {
            Value::Object(o) => {
                for v in o.values_mut() {
                    if let Err(e) = self.decompose(v) {
                        errors.push(e)
                    }
                }
            }
            Value::Array(a) => {
                for v in a {
                    if let Err(e) = self.decompose(v) {
                        errors.push(e)
                    }
                }
            }
            _ => (),
        }
        errors.into_iter().min_by_key(|e| e.0).map_or(Ok(()), Err)
    }
    fn all_paths(&mut self, from: &str, to: &str) -> Vec<Vec<Edge>> {
        let key = (from.to_owned(), to.to_owned());
        if let Some(paths) = self.paths.get(&key) {
            return paths.clone();
        }
        fn visit(
            graph: &SemanticGraph,
            current: &str,
            target: &str,
            seen: &mut HashSet<String>,
            path: &mut Vec<Edge>,
            paths: &mut Vec<Vec<Edge>>,
        ) {
            if current == target {
                paths.push(path.clone());
                return;
            }
            if paths.len() >= 2 {
                return;
            }
            seen.insert(current.to_owned());
            if let Some(model) = graph.get_model(current) {
                for (index, relationship) in model.relationships.iter().enumerate() {
                    let next = relationship.related_model();
                    if !relationship.active
                        || relationship.r#type != RelationshipType::ManyToOne
                        || seen.contains(next)
                        || next == current
                    {
                        continue;
                    }
                    path.push(Edge {
                        from: current.to_owned(),
                        to: next.to_owned(),
                        index,
                    });
                    visit(graph, next, target, seen, path, paths);
                    path.pop();
                }
            }
            seen.remove(current);
        }
        let mut paths = vec![];
        visit(
            self.generator.graph,
            from,
            to,
            &mut HashSet::new(),
            &mut vec![],
            &mut paths,
        );
        self.paths.insert(key, paths.clone());
        paths
    }
    fn rebind(&self, sql: &str, alias: &str) -> Result<String> {
        let mut replacements = HashMap::new();
        for c in semantic_column_references(sql)? {
            replacements.insert(
                (c.model, c.field.clone()),
                format!("{}.{}", self.q(alias), self.q(&c.field)),
            );
        }
        self.emit_final(replace_semantic_columns(
            parse_semantic_expression(sql)?,
            &replacements,
        )?)
    }
    fn render(
        &mut self,
        dimensions: &[Entry],
        metrics: &[Entry],
        expressions: &[Value],
        homes: &[String],
        filter: Option<&Value>,
        filter_owner: Option<&str>,
    ) -> Result<String> {
        let mut needed = HashSet::new();
        let mut trees = BTreeMap::new();
        for home in homes {
            needed.insert(home.clone());
            let mut edges = vec![];
            let mut added = HashSet::new();
            let targets = dimensions
                .iter()
                .map(|d| match &d.resolved {
                    Reference::Field(m, _) => m.as_str(),
                    _ => unreachable!(),
                })
                .chain(filter_owner);
            for target in targets {
                for edge in self
                    .all_paths(home, target)
                    .into_iter()
                    .next()
                    .unwrap_or_default()
                {
                    needed.insert(edge.to.clone());
                    if added.insert((edge.from.clone(), edge.index)) {
                        edges.push(edge);
                    }
                }
            }
            trees.insert(home.clone(), edges);
        }
        // With no aggregate components there are no branches. A dimensioned
        // query therefore has an empty key union; source projections are needed
        // only to give those empty output columns their declared field types.
        if homes.is_empty() && !dimensions.is_empty() {
            for dimension in dimensions {
                if let Reference::Field(owner, _) = &dimension.resolved {
                    needed.insert(owner.clone());
                }
            }
        }
        let mut names = needed.into_iter().collect::<Vec<_>>();
        names.sort();
        let source_names = names
            .iter()
            .enumerate()
            .map(|(i, n)| (n.clone(), format!("__ossie_source{i}")))
            .collect::<HashMap<_, _>>();
        // Unused datasets and unused field expressions must not add physical
        // dependencies (or evaluate volatile expressions) to this query.
        let mut inputs: HashMap<String, HashSet<String>> = HashMap::new();
        for dimension in dimensions {
            if let Reference::Field(owner, field) = &dimension.resolved {
                inputs
                    .entry(owner.clone())
                    .or_default()
                    .insert(field.clone());
            }
        }
        for sql in self
            .components
            .iter()
            .map(|c| Ok(c.sql.clone()))
            .chain(filter.into_iter().map(|f| self.canonical(f)))
        {
            for column in semantic_column_references(&sql?)? {
                if let Some(owner) = column.model {
                    inputs.entry(owner).or_default().insert(column.field);
                }
            }
        }
        for edges in trees.values() {
            for edge in edges {
                let rel = &self
                    .generator
                    .graph
                    .get_model(&edge.from)
                    .unwrap()
                    .relationships[edge.index];
                inputs.entry(edge.from.clone()).or_default().extend(
                    rel.foreign_key_columns.clone().unwrap_or_else(|| {
                        vec![rel
                            .foreign_key
                            .clone()
                            .unwrap_or_else(|| format!("{}_id", edge.to))]
                    }),
                );
                inputs.entry(edge.to.clone()).or_default().extend(
                    rel.primary_key_columns.clone().unwrap_or_else(|| {
                        vec![rel.primary_key.clone().unwrap_or_else(|| "id".into())]
                    }),
                );
            }
        }
        let mut row_ids = HashMap::new();
        let mut ctes = vec![];
        for name in &names {
            let model = self.generator.graph.get_model(name).unwrap();
            let source_dialect = stored_dialect(&model.metadata, DialectType::DuckDB)?;
            let source = if let Some(sql) = &model.sql {
                let canonical = dialects::query(sql, source_dialect)?;
                let canonical = dialects::ossie_index_offsets(
                    dialects::parse(&canonical, DialectType::DuckDB)?,
                    source_dialect,
                    DialectType::DuckDB,
                )?;
                let sql = self.emit_final(canonical)?;
                format!("({sql}) AS __ossie_physical")
            } else {
                let canonical = dialects::fragment(
                    model.table.as_deref().unwrap_or(name),
                    source_dialect,
                    Fragment::Table,
                )?;
                let Expression::Select(mut select) =
                    dialects::parse(&format!("SELECT * FROM {canonical}"), DialectType::DuckDB)?
                else {
                    unreachable!()
                };
                let table = select.from.take().unwrap().expressions.remove(0);
                self.emit_final(table)?
            };
            let mut row_id = "__ossie_row_id".to_owned();
            while model
                .dimensions
                .iter()
                .any(|d| d.name.eq_ignore_ascii_case(&row_id))
            {
                row_id.push('_');
            }
            let mut fields = vec![format!("ROW_NUMBER() OVER () AS {row_id}")];
            row_ids.insert(name.clone(), row_id);
            for dim in model.dimensions.iter().filter(|d| {
                inputs
                    .get(name)
                    .is_some_and(|fields| fields.contains(&d.name))
            }) {
                let canonical = self.source_expression(
                    dim.window_sql_expr(),
                    stored_dialect(&dim.metadata, source_dialect)?,
                )?;
                let sql = self.emit_final(canonical)?;
                fields.push(format!("{sql} AS {}", self.q(&dim.name)));
            }
            ctes.push(format!(
                "{} AS (SELECT {} FROM {source})",
                source_names[name],
                fields.join(", ")
            ));
        }
        if homes.is_empty() && !dimensions.is_empty() {
            let mut outputs = dimensions
                .iter()
                .map(|dimension| match &dimension.resolved {
                    Reference::Field(owner, field) => format!(
                        "{}.{} AS {}",
                        self.q(owner),
                        self.q(field),
                        self.q(&dimension.output)
                    ),
                    _ => unreachable!(),
                })
                .collect::<Vec<_>>();
            for (entry, value) in metrics.iter().zip(expressions) {
                outputs.push(format!(
                    "{} AS {}",
                    self.emit(value)?,
                    self.q(&entry.output)
                ));
            }
            let sources = names
                .iter()
                .map(|name| format!("{} AS {}", source_names[name], self.q(name)))
                .collect::<Vec<_>>();
            return Ok(format!(
                "WITH {} SELECT {} FROM {} WHERE FALSE",
                ctes.join(", "),
                outputs.join(", "),
                sources.join(" CROSS JOIN ")
            ));
        }
        let mut branches = vec![];
        for (branch, home) in homes.iter().enumerate() {
            let mut relation = format!("{} AS {}", source_names[home], self.q(home));
            for edge in &trees[home] {
                let rel = &self
                    .generator
                    .graph
                    .get_model(&edge.from)
                    .unwrap()
                    .relationships[edge.index];
                let from_keys = rel.foreign_key_columns.clone().unwrap_or_else(|| {
                    vec![rel
                        .foreign_key
                        .clone()
                        .unwrap_or_else(|| format!("{}_id", edge.to))]
                });
                let to_keys = rel.primary_key_columns.clone().unwrap_or_else(|| {
                    vec![rel.primary_key.clone().unwrap_or_else(|| "id".into())]
                });
                let condition = if let Some(sql) = &rel.sql {
                    sql.replace("{from}", &self.q(&edge.from))
                        .replace("{to}", &self.q(&edge.to))
                } else {
                    from_keys
                        .iter()
                        .zip(&to_keys)
                        .map(|(a, b)| {
                            format!(
                                "{}.{} = {}.{}",
                                self.q(&edge.from),
                                self.q(a),
                                self.q(&edge.to),
                                self.q(b)
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(" AND ")
                };
                relation.push_str(&format!(
                    " LEFT JOIN {} AS {} ON {condition}",
                    source_names[&edge.to],
                    self.q(&edge.to)
                ));
            }
            let mut fields = dimensions
                .iter()
                .enumerate()
                .map(|(i, d)| match &d.resolved {
                    Reference::Field(m, f) => {
                        format!("{}.{} AS __ossie_d{i}", self.q(m), self.q(f))
                    }
                    _ => unreachable!(),
                })
                .collect::<Vec<_>>();
            let where_sql = filter
                .map(|f| self.emit(f).map(|s| format!(" WHERE {s}")))
                .transpose()?
                .unwrap_or_default();
            if metrics.is_empty() {
                return Ok(format!(
                    "WITH {} SELECT DISTINCT {} FROM {relation}{where_sql}",
                    ctes.join(", "),
                    dimensions
                        .iter()
                        .map(|d| match &d.resolved {
                            Reference::Field(m, f) =>
                                format!("{}.{} AS {}", self.q(m), self.q(f), self.q(&d.output)),
                            _ => unreachable!(),
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            fields.push(format!(
                "{}.{} AS __ossie_row_id",
                self.q(home),
                row_ids[home]
            ));
            ctes.push(format!(
                "__ossie_population{branch} AS (SELECT DISTINCT {} FROM {relation}{where_sql})",
                fields.join(", ")
            ));
            let keys = (0..dimensions.len())
                .map(|i| format!("p.__ossie_d{i}"))
                .collect::<Vec<_>>();
            let mut selections = keys.clone();
            selections.push("1 AS __ossie_present".into());
            for (index, component) in self
                .components
                .iter()
                .enumerate()
                .filter(|(_, c)| &c.home == home)
            {
                selections.push(format!(
                    "{} AS __ossie_c{index}",
                    self.rebind(&component.sql, "h")?
                ));
            }
            let group = if keys.is_empty() {
                String::new()
            } else {
                format!(" GROUP BY {}", keys.join(", "))
            };
            ctes.push(format!("__ossie_branch{branch} AS (SELECT {} FROM __ossie_population{branch} p JOIN {} h ON p.__ossie_row_id = h.{}{group})",selections.join(", "),source_names[home], row_ids[home]));
            branches.push(format!("__ossie_branch{branch}"));
        }
        let keys = (0..dimensions.len())
            .map(|i| format!("__ossie_d{i}"))
            .collect::<Vec<_>>();
        if keys.is_empty() {
            ctes.push("__ossie_keys AS (SELECT 1 AS __ossie_singleton)".into());
        } else {
            ctes.push(format!(
                "__ossie_keys AS ({})",
                branches
                    .iter()
                    .map(|b| format!("SELECT {} FROM {b}", keys.join(", ")))
                    .collect::<Vec<_>>()
                    .join(" UNION ")
            ));
        }
        let mut combined = keys.iter().map(|k| format!("k.{k}")).collect::<Vec<_>>();
        let mut relation = "__ossie_keys k".to_owned();
        for (branch, home) in homes.iter().enumerate() {
            let condition = if keys.is_empty() {
                "TRUE".into()
            } else {
                keys.iter()
                    .map(|k| format!("k.{k} IS NOT DISTINCT FROM b{branch}.{k}"))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            };
            relation.push_str(&format!(
                " LEFT JOIN __ossie_branch{branch} b{branch} ON {condition}"
            ));
            for (index, component) in self
                .components
                .iter()
                .enumerate()
                .filter(|(_, c)| &c.home == home)
            {
                let empty = format!(
                    "(SELECT {} FROM {} h WHERE FALSE)",
                    self.rebind(&component.sql, "h")?,
                    source_names[home]
                );
                combined.push(format!("CASE WHEN b{branch}.__ossie_present IS NULL THEN {empty} ELSE b{branch}.__ossie_c{index} END AS __ossie_c{index}"));
            }
        }
        if combined.is_empty() {
            combined.push("1 AS __ossie_singleton".into());
        }
        ctes.push(format!(
            "__ossie_values AS (SELECT {} FROM {relation})",
            combined.join(", ")
        ));
        let mut output = dimensions
            .iter()
            .enumerate()
            .map(|(i, d)| format!("__ossie_d{i} AS {}", self.q(&d.output)))
            .collect::<Vec<_>>();
        for (entry, value) in metrics.iter().zip(expressions) {
            output.push(format!(
                "{} AS {}",
                self.emit(value)?,
                self.q(&entry.output)
            ));
        }
        Ok(format!(
            "WITH {} SELECT {} FROM __ossie_values",
            ctes.join(", "),
            output.join(", ")
        ))
    }
}
