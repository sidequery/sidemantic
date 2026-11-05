//! Relational Ossie measures retain source-row identity through SQL operators.
//!
//! The joined row domain is materialized exactly as authored. Each measure is
//! evaluated over its own distinct identity/input tuples inside each group;
//! ordinary aggregates still consume the joined multiset. This implements the
//! reference semantics rather than selecting joins from a metric dependency graph.

use super::*;
use crate::core::Model;
use polyglot_sql::expressions::{Column, Literal};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

fn error(message: impl Into<String>) -> SidemanticError {
    SidemanticError::Validation(format!(
        "Ossie AGG relational interface: {}",
        message.into()
    ))
}
fn q(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
fn key(name: &Identifier) -> String {
    if name.quoted {
        name.name.clone()
    } else {
        name.name.to_uppercase()
    }
}
fn declared(name: &str) -> Identifier {
    if name.starts_with('"') && name.ends_with('"') {
        Identifier::quoted(name[1..name.len() - 1].replace("\"\"", "\""))
    } else {
        Identifier::new(name)
    }
}
fn sql(expr: &Expression) -> Result<String> {
    crate::semantic_input::dialects::emit(expr.clone(), DialectType::DuckDB, DialectType::DuckDB)
}
fn parse(sql: &str) -> Result<Expression> {
    parse_sql_with_dialect(sql, DialectType::DuckDB)?
        .into_iter()
        .next()
        .ok_or_else(|| error("empty query"))
}
fn expr(sql: &str) -> Result<Expression> {
    let Expression::Select(mut select) = parse(&format!("SELECT {sql}"))? else {
        return Err(error("invalid scalar SQL"));
    };
    if select.expressions.len() != 1 {
        return Err(error("expected one expression"));
    }
    Ok(select.expressions.remove(0))
}
fn ossie_input(source: &str, dialect: DialectType) -> Result<Vec<Expression>> {
    // A target generator may quote a keyword (rows -> "rows") even when the
    // author did not. Preserve identifier identity until Ossie name binding.
    let mut originals = HashMap::<String, Value>::new();
    fn protect(value: &mut Value, originals: &mut HashMap<String, Value>) {
        match value {
            Value::Object(fields) => {
                if fields.get("name").is_some_and(Value::is_string)
                    && fields.get("quoted").is_some_and(Value::is_boolean)
                    && fields.keys().all(|name| {
                        matches!(
                            name.as_str(),
                            "name" | "quoted" | "span" | "trailing_comments"
                        )
                    })
                {
                    let existing = originals
                        .iter()
                        .find(|(_, original)| {
                            original.get("name") == fields.get("name")
                                && original.get("quoted") == fields.get("quoted")
                        })
                        .map(|(token, _)| token.clone());
                    let token = existing.unwrap_or_else(|| {
                        format!("__ossie_authored_identifier_{}", originals.len())
                    });
                    originals
                        .entry(token.clone())
                        .or_insert_with(|| Value::Object(fields.clone()));
                    *value = serde_json::to_value(Identifier::new(token)).unwrap();
                } else {
                    for child in fields.values_mut() {
                        protect(child, originals);
                    }
                }
            }
            Value::Array(values) => {
                for value in values {
                    protect(value, originals);
                }
            }
            _ => {}
        }
    }
    fn restore(value: &mut Value, originals: &HashMap<String, Value>) {
        match value {
            Value::Object(fields) => {
                if let Some(original) = fields
                    .get("name")
                    .and_then(Value::as_str)
                    .and_then(|name| originals.get(name))
                {
                    if fields.get("quoted").is_some() {
                        *value = original.clone();
                        return;
                    }
                }
                for child in fields.values_mut() {
                    restore(child, originals);
                }
            }
            Value::Array(values) => {
                for value in values {
                    restore(value, originals);
                }
            }
            _ => {}
        }
    }
    let normalized = if dialect == DialectType::DuckDB {
        parse_rewrite_input(source, dialect)?
    } else {
        let authored = source.to_owned();
        let parsed = run_parser(move || {
            Ok(vec![crate::semantic_input::dialects::parse(
                &authored, dialect,
            )?])
        })?
        .remove(0);
        let mut value = serde_json::to_value(parsed).map_err(|err| error(err.to_string()))?;
        protect(&mut value, &mut originals);
        let parsed = serde_json::from_value(value).map_err(|err| error(err.to_string()))?;
        let protected = polyglot_sql::Dialect::get(dialect)
            .generate(&parsed)
            .map_err(|err| error(err.to_string()))?;
        parse_rewrite_input(&protected, dialect)?
    };
    normalized
        .into_iter()
        .map(|expression| {
            let mut value =
                serde_json::to_value(expression).map_err(|err| error(err.to_string()))?;
            restore(&mut value, &originals);
            let expression = serde_json::from_value(value).map_err(|err| error(err.to_string()))?;
            crate::semantic_input::dialects::ossie_index_offsets(
                expression,
                dialect,
                DialectType::DuckDB,
            )
        })
        .collect()
}
fn source_scalar(source: &str, dialect: DialectType) -> Result<Expression> {
    let normalized = crate::semantic_input::dialects::fragment(
        source,
        dialect,
        crate::semantic_input::dialects::Fragment::Scalar,
    )?;
    crate::semantic_input::dialects::ossie_index_offsets(
        expr(&normalized)?,
        dialect,
        DialectType::DuckDB,
    )
}
fn column(slot: &str) -> Expression {
    Expression::Column(Box::new(Column {
        name: Identifier::quoted(slot),
        table: None,
        join_mark: false,
        trailing_comments: vec![],
        span: None,
        inferred_type: None,
    }))
}
fn qualified(alias: &str, slot: &str) -> Result<Expression> {
    expr(&format!("{}.{}", q(alias), q(slot)))
}
fn is_call(node: &Expression, name: &str) -> bool {
    match node {
        Expression::Function(function) => function.name.eq_ignore_ascii_case(name),
        Expression::AggregateFunction(function) => function.name.eq_ignore_ascii_case(name),
        _ => false,
    }
}
fn aggregate(node: &Expression) -> bool {
    if is_call(node, "AGG") {
        return true;
    }
    match node {
        Expression::WithinGroup(group) => aggregate(&group.this),
        Expression::Filter(filter) => aggregate(&filter.this),
        _ => serde_json::to_value(node)
            .ok()
            .is_some_and(|value| crate::core::is_aggregate_ast_node(&value)),
    }
}
fn has_aggregate(expression: &Expression) -> Result<bool> {
    let mut found = false;
    binding::transform_nodes(expression.clone(), &mut |node| {
        if matches!(
            node,
            Expression::Subquery(_) | Expression::Select(_) | Expression::WindowFunction(_)
        ) || is_call(node, "TO_MEASURE")
        {
            return Ok(Some(node.clone()));
        }
        if aggregate(node) {
            found = true;
            return Ok(Some(node.clone()));
        }
        Ok(None)
    })?;
    Ok(found)
}
fn refs(expression: &Expression) -> Result<BTreeSet<String>> {
    let mut result = BTreeSet::new();
    binding::transform_nodes(expression.clone(), &mut |node| {
        if matches!(node, Expression::Subquery(_) | Expression::Select(_)) {
            return Ok(Some(node.clone()));
        }
        if let Expression::Column(column) = node {
            result.insert(column.name.name.clone());
        }
        Ok(None)
    })?;
    Ok(result)
}
fn rename(expression: &Expression, mapping: &HashMap<String, String>) -> Result<Expression> {
    binding::transform_nodes(expression.clone(), &mut |node| {
        if let Expression::Column(value) = node {
            if let Some(new) = mapping.get(&value.name.name) {
                return Ok(Some(column(new)));
            }
        }
        Ok(None)
    })
}

#[derive(Clone)]
enum Recipe {
    Filtered {
        recipe: Box<Recipe>,
        predicate: Expression,
    },
    Base {
        identity: String,
        inputs: BTreeSet<String>,
        formula: Expression,
    },
    Derived {
        formula: Expression,
        dependencies: Vec<(String, Recipe)>,
    },
}
impl Recipe {
    fn refs(&self) -> BTreeSet<String> {
        match self {
            Self::Filtered { recipe, predicate } => recipe
                .refs()
                .into_iter()
                .chain(refs(predicate).unwrap_or_default())
                .collect(),
            Self::Base {
                identity, inputs, ..
            } => inputs.iter().cloned().chain([identity.clone()]).collect(),
            Self::Derived { dependencies, .. } => dependencies
                .iter()
                .flat_map(|(_, recipe)| recipe.refs())
                .collect(),
        }
    }
    fn rename(&self, mapping: &HashMap<String, String>) -> Result<Self> {
        Ok(match self {
            Self::Filtered { recipe, predicate } => Self::Filtered {
                recipe: Box::new(recipe.rename(mapping)?),
                predicate: rename(predicate, mapping)?,
            },
            Self::Base {
                identity,
                inputs,
                formula,
            } => Self::Base {
                identity: mapping
                    .get(identity)
                    .cloned()
                    .unwrap_or_else(|| identity.clone()),
                inputs: inputs
                    .iter()
                    .map(|name| mapping.get(name).cloned().unwrap_or_else(|| name.clone()))
                    .collect(),
                formula: rename(formula, mapping)?,
            },
            Self::Derived {
                formula,
                dependencies,
            } => Self::Derived {
                formula: formula.clone(),
                dependencies: dependencies
                    .iter()
                    .map(|(name, recipe)| Ok((name.clone(), recipe.rename(mapping)?)))
                    .collect::<Result<_>>()?,
            },
        })
    }
    fn evaluate(&self, joined: &str, predicate: &str) -> Result<Expression> {
        match self {
            Self::Filtered {
                recipe,
                predicate: filter,
            } => recipe.evaluate(joined, &format!("({predicate}) AND ({})", sql(filter)?)),
            Self::Base {
                identity,
                inputs,
                formula,
            } => {
                let fields = inputs
                    .iter()
                    .cloned()
                    .chain([identity.clone()])
                    .collect::<BTreeSet<_>>()
                    .iter()
                    .map(|name| q(name))
                    .collect::<Vec<_>>()
                    .join(", ");
                expr(&format!("(SELECT {} FROM (SELECT DISTINCT {fields} FROM {} WHERE {} IS NOT NULL AND ({predicate})) AS __ossie_inputs)",sql(formula)?,q(joined),q(identity)))
            }
            Self::Derived {
                formula,
                dependencies,
            } => {
                let replacements = dependencies
                    .iter()
                    .map(|(name, recipe)| Ok((name.clone(), recipe.evaluate(joined, predicate)?)))
                    .collect::<Result<HashMap<_, _>>>()?;
                binding::transform_nodes(formula.clone(), &mut |node| {
                    if let Expression::Column(column) = node {
                        return Ok(replacements.get(&column.name.name).cloned());
                    }
                    Ok(None)
                })
            }
        }
    }
}
#[derive(Clone)]
struct Output {
    data_type: String,
    name: Identifier,
    slot: String,
    measure: Option<Recipe>,
}
#[derive(Clone)]
struct Relation {
    sql: String,
    outputs: Vec<Output>,
    hidden: BTreeSet<String>,
}
impl Relation {
    fn slots(&self) -> BTreeSet<String> {
        self.outputs
            .iter()
            .filter(|out| out.measure.is_none())
            .map(|out| out.slot.clone())
            .chain(self.hidden.iter().cloned())
            .collect()
    }
    fn has_measures(&self) -> bool {
        self.outputs.iter().any(|out| out.measure.is_some())
    }
    fn visible_sql(&self) -> String {
        format!(
            "SELECT {} FROM ({}) AS __ossie_result",
            self.outputs
                .iter()
                .map(|out| format!("{} AS {}", q(&out.slot), q(&out.name.name)))
                .collect::<Vec<_>>()
                .join(", "),
            self.sql
        )
    }
}
#[derive(Clone)]
struct Bound {
    data_type: String,
    unqualified: bool,
    name: Identifier,
    qualifier: String,
    value: Expression,
    measure: Option<Recipe>,
}
#[derive(Clone, Default)]
struct Context {
    columns: Vec<Bound>,
    outer: Option<Box<Context>>,
}
impl Context {
    fn resolve(&self, column: &Column) -> Result<Bound> {
        let candidates = self
            .columns
            .iter()
            .filter(|bound| {
                key(&bound.name) == key(&column.name)
                    && (column.table.is_some() || bound.unqualified)
                    && column
                        .table
                        .as_ref()
                        .is_none_or(|table| key(table) == bound.qualifier)
            })
            .collect::<Vec<_>>();
        match candidates.as_slice() {
            [bound] => Ok((*bound).clone()),
            [] => self
                .outer
                .as_ref()
                .map(|outer| outer.resolve(column))
                .unwrap_or_else(|| {
                    Err(error(format!(
                        "unknown field or measure {}",
                        column.name.name
                    )))
                }),
            _ => Err(error(format!("ambiguous column {}", column.name.name))),
        }
    }
}
#[derive(Clone)]
struct Source {
    alias: Identifier,
    relation: Relation,
}
struct Compiler<'a> {
    input_dialect: DialectType,
    graph: &'a SemanticGraph,
    sequence: usize,
    view_stack: HashSet<String>,
}
impl<'a> Compiler<'a> {
    fn fresh(&mut self, kind: &str) -> String {
        self.sequence += 1;
        format!("__ossie_{kind}_{}", self.sequence)
    }
    fn view_definition(&self, table: &TableRef) -> Result<Option<(String, String)>> {
        fn parts(table: &TableRef) -> Vec<String> {
            table
                .catalog
                .iter()
                .chain(table.schema.iter())
                .chain([&table.name])
                .map(key)
                .collect()
        }
        let requested = parts(table);
        if let Some(views) = self
            .graph
            .metadata()
            .and_then(|meta| meta.get("ossie_views"))
            .and_then(Value::as_object)
        {
            for (name, definition) in views {
                let Expression::Select(select) = parse(&format!("SELECT * FROM {name}"))? else {
                    continue;
                };
                let Some(Expression::Table(candidate)) = select
                    .from
                    .as_ref()
                    .and_then(|from| from.expressions.first())
                else {
                    continue;
                };
                if parts(candidate) == requested {
                    if let Some(definition) = definition.as_str() {
                        return Ok(Some((name.clone(), definition.into())));
                    }
                }
            }
        }
        Ok(None)
    }
    fn model(&self, table: &TableRef) -> Result<Option<&'a Model>> {
        let models = self
            .graph
            .models()
            .filter(|model| {
                model
                    .metadata
                    .as_ref()
                    .and_then(|meta| meta.get("ossie_scope_name"))
                    .is_some()
            })
            .collect::<Vec<_>>();
        let candidates = models
            .into_iter()
            .filter(|model| {
                let source = model
                    .metadata
                    .as_ref()
                    .and_then(|meta| meta.get("ossie_source_name"))
                    .and_then(Value::as_str)
                    .unwrap_or(&model.name);
                key(&declared(source)) == key(&table.name)
            })
            .collect::<Vec<_>>();
        let filtered = candidates
            .iter()
            .filter(|model| {
                table.schema.as_ref().is_none_or(|schema| {
                    let scope = model
                        .metadata
                        .as_ref()
                        .and_then(|meta| meta.get("ossie_scope_name"))
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    key(&declared(scope)) == key(schema)
                })
            })
            .collect::<Vec<_>>();
        if let [model] = filtered.as_slice() {
            return Ok(Some(**model));
        }
        if !candidates.is_empty() && table.schema.is_none() {
            return Err(error("ambiguous model namespace"));
        }
        Ok(None)
    }
    fn model_relation(
        &mut self,
        model: &Model,
        ctes: &HashMap<String, Relation>,
    ) -> Result<Relation> {
        if let Some(definition) = model
            .metadata
            .as_ref()
            .and_then(|meta| meta.get("ossie_view_definition"))
            .and_then(Value::as_str)
        {
            if !self.view_stack.insert(model.name.clone()) {
                return Err(error("recursive measure view definition"));
            }
            let result = self.query(
                ossie_input(&preprocess_measure_syntax(definition)?, self.input_dialect)?.remove(0),
                ctes,
                None,
            );
            self.view_stack.remove(&model.name);
            return result;
        }
        use crate::semantic_input::dialects::{fragment, query as normalize_query, Fragment};
        let dialect = model
            .metadata
            .as_ref()
            .and_then(|meta| meta.get("ossie_target_dialect"))
            .and_then(Value::as_str)
            .unwrap_or("duckdb");
        let dialect = if dialect.eq_ignore_ascii_case("ANSI_SQL") {
            DialectType::DuckDB
        } else {
            crate::semantic_input::dialects::parse_dialect(dialect)?
        };
        let physical_dialect = model
            .metadata
            .as_ref()
            .and_then(|meta| meta.get("ossie_source_dialect"))
            .and_then(Value::as_str)
            .map(crate::semantic_input::dialects::parse_dialect)
            .transpose()?
            .unwrap_or(dialect);
        let source = if let Some(query) = &model.sql {
            let expression = parse(&normalize_query(query, physical_dialect)?)?;
            let expression = crate::semantic_input::dialects::ossie_index_offsets(
                expression,
                physical_dialect,
                DialectType::DuckDB,
            )?;
            format!("({})", sql(&expression)?)
        } else {
            fragment(
                model
                    .table
                    .as_deref()
                    .ok_or_else(|| error("dataset has no source"))?,
                physical_dialect,
                Fragment::Table,
            )?
        };
        let source_alias = self.fresh("physical");
        let identity = self.fresh("identity");
        let mut selections = vec![format!("ROW_NUMBER() OVER () AS {}", q(&identity))];
        let mut outputs = Vec::new();
        let mut hidden = BTreeSet::from([identity.clone()]);
        let mut mapping = HashMap::new();
        for field in &model.dimensions {
            let slot = self.fresh("input");
            mapping.insert(field.name.clone(), slot.clone());
            hidden.insert(slot.clone());
            let raw = source_scalar(field.sql.as_deref().unwrap_or(&field.name), dialect)?;
            let raw = binding::transform_nodes(raw, &mut |node| {
                if let Expression::Column(value) = node {
                    let mut value = value.clone();
                    value.table = Some(Identifier::quoted(&source_alias));
                    return Ok(Some(Expression::Column(value)));
                }
                Ok(None)
            })?;
            selections.push(format!("{} AS {}", sql(&raw)?, q(&slot)));
            if field.public
                && !field
                    .metadata
                    .as_ref()
                    .is_some_and(|metadata| metadata.get("ossie_internal_expression").is_some())
            {
                let name = field
                    .metadata
                    .as_ref()
                    .and_then(|meta| meta.get("ossie_source_name"))
                    .and_then(Value::as_str)
                    .unwrap_or(&field.name);
                outputs.push(Output {
                    data_type: field
                        .logical_data_type
                        .clone()
                        .unwrap_or_else(|| "UNKNOWN".into()),
                    name: declared(name),
                    slot,
                    measure: None,
                });
            }
        }
        let mut metrics = self
            .graph
            .metrics()
            .filter(|metric| {
                metric
                    .metadata
                    .as_ref()
                    .and_then(|meta| meta.get("ossie_dataset_metric"))
                    .and_then(Value::as_bool)
                    == Some(true)
                    && self.graph.metric_owner(&metric.name) == Some(model.name.as_str())
            })
            .collect::<Vec<_>>();
        metrics.sort_by_key(|metric| {
            metric
                .metadata
                .as_ref()
                .and_then(|meta| meta.get("ossie_dataset_metric_index"))
                .and_then(Value::as_u64)
                .unwrap_or(u64::MAX)
        });
        for metric in metrics {
            let formula = source_scalar(
                metric
                    .sql
                    .as_deref()
                    .ok_or_else(|| error("measure has no expression"))?,
                dialect,
            )?;
            let formula = binding::transform_nodes(formula, &mut |node| {
                if let Expression::Column(value) = node {
                    let slot = mapping.get(&value.name.name).ok_or_else(|| {
                        error(format!(
                            "measure input {} is not a declared logical field",
                            value.name.name
                        ))
                    })?;
                    return Ok(Some(column(slot)));
                }
                Ok(None)
            })?;
            let name = metric
                .metadata
                .as_ref()
                .and_then(|meta| meta.get("ossie_source_name"))
                .and_then(Value::as_str)
                .unwrap_or(&metric.name);
            outputs.push(Output {
                data_type: metric
                    .logical_data_type
                    .clone()
                    .unwrap_or_else(|| "UNKNOWN".into()),
                name: declared(name),
                slot: self.fresh("measure"),
                measure: Some(Recipe::Base {
                    identity: identity.clone(),
                    inputs: refs(&formula)?,
                    formula,
                }),
            });
        }
        Ok(Relation {
            sql: format!(
                "SELECT {} FROM {source} AS {}",
                selections.join(", "),
                q(&source_alias)
            ),
            outputs,
            hidden,
        })
    }
    fn source(
        &mut self,
        node: Expression,
        ctes: &HashMap<String, Relation>,
        outer: Option<Context>,
        hints: &[Column],
    ) -> Result<Source> {
        match node {
            Expression::Table(table) => {
                let alias = table.alias.clone().unwrap_or_else(|| table.name.clone());
                let relation = if table.schema.is_none() && ctes.contains_key(&key(&table.name)) {
                    ctes[&key(&table.name)].clone()
                } else if let Some((view_key, definition)) = self.view_definition(&table)? {
                    if !self.view_stack.insert(view_key.clone()) {
                        return Err(error("recursive measure view definition"));
                    }
                    let relation = self.query(
                        ossie_input(&preprocess_measure_syntax(&definition)?, self.input_dialect)?
                            .remove(0),
                        ctes,
                        None,
                    )?;
                    self.view_stack.remove(&view_key);
                    relation
                } else if let Some(model) = self.model(&table)? {
                    self.model_relation(model, ctes)?
                } else {
                    let mut names = hints
                        .iter()
                        .filter(|col| {
                            col.table
                                .as_ref()
                                .is_none_or(|qualifier| key(qualifier) == key(&alias))
                        })
                        .map(|col| (key(&col.name), col.name.clone()))
                        .collect::<BTreeMap<_, _>>();
                    let mut physical = table.clone();
                    physical.alias = None;
                    physical.column_aliases.clear();
                    let physical_name = sql(&Expression::Table(physical.clone()))?;
                    // Source metadata supplies the schema of physical tables behind datasets.
                    // Raw expressions still use physical fields, without logical rewrites.
                    let mut ordered_names = Vec::new();
                    let mut seen_names = HashSet::new();
                    for model in self.graph.models().filter(|model| {
                        model
                            .table
                            .as_ref()
                            .is_some_and(|name| name.eq_ignore_ascii_case(&physical_name))
                    }) {
                        for field in &model.dimensions {
                            let expression = expr(field.sql.as_deref().unwrap_or(&field.name))?;
                            binding::transform_nodes(expression, &mut |node| {
                                if let Expression::Column(column) = node {
                                    if seen_names.insert(key(&column.name)) {
                                        ordered_names.push(column.name.clone());
                                    }
                                    names.remove(&key(&column.name));
                                }
                                Ok(None)
                            })?;
                        }
                    }
                    ordered_names.extend(names.into_values());
                    let outputs = ordered_names
                        .iter()
                        .map(|name| Output {
                            name: name.clone(),
                            slot: name.name.clone(),
                            measure: None,
                            data_type: "UNKNOWN".into(),
                        })
                        .collect::<Vec<_>>();
                    let columns = if outputs.is_empty() {
                        "1 AS __ossie_dummy".to_string()
                    } else {
                        outputs
                            .iter()
                            .map(|out| q(&out.slot))
                            .collect::<Vec<_>>()
                            .join(", ")
                    };
                    Relation {
                        sql: format!(
                            "SELECT {columns} FROM {}",
                            sql(&Expression::Table(physical))?
                        ),
                        outputs,
                        hidden: BTreeSet::new(),
                    }
                };
                let mut source = Source { alias, relation };
                if !table.column_aliases.is_empty() {
                    Self::alias_columns(&mut source.relation, &table.column_aliases)?;
                }
                Ok(source)
            }
            Expression::Subquery(mut subquery) => {
                // A scalar physical SELECT * can be narrowed to the explicitly
                // referenced outer fields when no catalog schema is available.
                if let Expression::Select(select) = &mut subquery.this {
                    let unknown_source=select.from.as_ref().is_some_and(|from|from.expressions.len()==1&&matches!(&from.expressions[0],Expression::Table(table) if self.model(table).ok().flatten().is_none()&&!ctes.contains_key(&key(&table.name))));
                    if unknown_source
                        && subquery.column_aliases.is_empty()
                        && select.expressions.len() == 1
                        && matches!(&select.expressions[0], Expression::Star(_))
                    {
                        let names = hints
                            .iter()
                            .filter(|column| {
                                column.table.as_ref().is_none_or(|table| {
                                    subquery
                                        .alias
                                        .as_ref()
                                        .is_none_or(|alias| key(table) == key(alias))
                                })
                            })
                            .map(|column| (key(&column.name), column.name.clone()))
                            .collect::<BTreeMap<_, _>>();
                        if !names.is_empty() {
                            select.expressions = names
                                .values()
                                .map(|name| {
                                    Expression::Column(Box::new(Column {
                                        name: name.clone(),
                                        table: None,
                                        join_mark: false,
                                        trailing_comments: vec![],
                                        span: None,
                                        inferred_type: None,
                                    }))
                                })
                                .collect();
                        }
                    }
                }
                let mut relation = self.query(subquery.this, ctes, outer)?;
                Self::alias_columns(&mut relation, &subquery.column_aliases)?;
                Ok(Source {
                    alias: subquery
                        .alias
                        .unwrap_or_else(|| Identifier::new(self.fresh("relation"))),
                    relation,
                })
            }
            Expression::Alias(alias) => {
                let mut source = self.source(alias.this, ctes, outer, hints)?;
                source.alias = alias.alias;
                Ok(source)
            }
            Expression::Paren(paren) => self.source(paren.this, ctes, outer, hints),
            Expression::Values(mut values) => {
                let aliases = std::mem::take(&mut values.column_aliases);
                let alias = values
                    .alias
                    .take()
                    .unwrap_or_else(|| Identifier::new(self.fresh("values")));
                let mut relation = self.query(Expression::Values(values), ctes, outer)?;
                Self::alias_columns(&mut relation, &aliases)?;
                Ok(Source { alias, relation })
            }
            _ => Err(error("unsupported relation expression")),
        }
    }
    fn alias_columns(relation: &mut Relation, aliases: &[Identifier]) -> Result<()> {
        if aliases.is_empty() {
            return Ok(());
        }
        if aliases.len() != relation.outputs.len() {
            return Err(error(
                "relation column alias count does not match projection",
            ));
        }
        for (output, name) in relation.outputs.iter_mut().zip(aliases) {
            output.name = name.clone();
        }
        Ok(())
    }
    fn scalar(
        &mut self,
        node: Expression,
        context: &Context,
        ctes: &HashMap<String, Relation>,
        allow_aggregate: bool,
    ) -> Result<Expression> {
        binding::transform_nodes(node, &mut |node| {
            if let Expression::Exists(exists) = node {
                let mut exists = exists.clone();
                let relation = self.query(exists.this.clone(), ctes, Some(context.clone()))?;
                exists.this = parse(&relation.visible_sql())?;
                return Ok(Some(Expression::Exists(exists)));
            }
            if let Expression::WindowFunction(window) = node {
                let mut window = window.clone();
                window.this = self.scalar(window.this.clone(), context, ctes, true)?;
                window.over.partition_by = window
                    .over
                    .partition_by
                    .into_iter()
                    .map(|value| self.scalar(value, context, ctes, false))
                    .collect::<Result<_>>()?;
                for order in &mut window.over.order_by {
                    order.this = self.scalar(order.this.clone(), context, ctes, false)?;
                }
                return Ok(Some(Expression::WindowFunction(window)));
            }
            if let Expression::Select(_) = node {
                let relation = self.query(node.clone(), ctes, Some(context.clone()))?;
                if relation.has_measures() || relation.outputs.len() != 1 {
                    return Err(error("scalar subquery must return one evaluated field"));
                }
                return Ok(Some(parse(&relation.visible_sql())?));
            }
            if let Expression::Subquery(subquery) = node {
                let relation = self.query(subquery.this.clone(), ctes, Some(context.clone()))?;
                if relation.has_measures() || relation.outputs.len() != 1 {
                    return Err(error("scalar subquery must return one evaluated field"));
                }
                return Ok(Some(expr(&format!("({})", relation.visible_sql()))?));
            }
            if let Expression::Column(column) = node {
                let bound = context.resolve(column)?;
                if bound.measure.is_some() {
                    return Err(error(
                        "measure values are only valid inside AGG or as an unaggregated projection",
                    ));
                }
                return Ok(Some(bound.value));
            }
            if is_call(node, "AGG")
                || is_call(node, "TO_MEASURE")
                || (!allow_aggregate && aggregate(node))
            {
                return Err(error("aggregate is not allowed in this scalar context"));
            }
            Ok(None)
        })
    }
    fn resolve_measure(&self, node: &Expression, context: &Context) -> Result<Recipe> {
        let (args, distinct) = match node {
            Expression::Function(f) => (f.args.as_slice(), f.distinct),
            Expression::AggregateFunction(f) => (f.args.as_slice(), f.distinct),
            _ => return Err(error("invalid AGG invocation")),
        };
        let [Expression::Column(column)] = args else {
            return Err(error("AGG requires one measure-column reference"));
        };
        if distinct {
            return Err(error("AGG DISTINCT is not defined"));
        }
        context
            .resolve(column)?
            .measure
            .ok_or_else(|| error("AGG argument is a field, not a measure"))
    }
    fn definition(
        &mut self,
        node: Expression,
        context: &Context,
        ctes: &HashMap<String, Relation>,
        identity: &str,
    ) -> Result<Recipe> {
        let mut dependencies = Vec::new();
        let formula = binding::transform_nodes(node, &mut |node| {
            if is_call(node, "AGG") {
                let name = self.fresh("dependency");
                let recipe = self.resolve_measure(node, context)?;
                let recipe = if let Expression::AggregateFunction(function) = node {
                    if let Some(filter) = &function.filter {
                        Recipe::Filtered {
                            recipe: Box::new(recipe),
                            predicate: self.scalar(filter.clone(), context, ctes, false)?,
                        }
                    } else {
                        recipe
                    }
                } else {
                    recipe
                };
                dependencies.push((name.clone(), recipe));
                return Ok(Some(column(&name)));
            }
            if aggregate(node) {
                let formula = self.scalar(node.clone(), context, ctes, true)?;
                let name = self.fresh("dependency");
                dependencies.push((
                    name.clone(),
                    Recipe::Base {
                        identity: identity.to_string(),
                        inputs: refs(&formula)?,
                        formula,
                    },
                ));
                return Ok(Some(column(&name)));
            }
            if matches!(node, Expression::Column(_)) {
                return Err(error(
                    "measure definitions require aggregate expressions, not raw fields",
                ));
            }
            Ok(None)
        })?;
        if dependencies.is_empty() {
            return Err(error("TO_MEASURE requires an aggregate expression"));
        }
        Ok(Recipe::Derived {
            formula,
            dependencies,
        })
    }
    fn query(
        &mut self,
        node: Expression,
        inherited: &HashMap<String, Relation>,
        outer: Option<Context>,
    ) -> Result<Relation> {
        match node {
            Expression::Select(select) => self.select(*select, inherited, outer),
            Expression::Subquery(subquery) => self.query(subquery.this, inherited, outer),
            Expression::Paren(paren) => self.query(paren.this, inherited, outer),
            Expression::Union(union) => self.set(
                union.left,
                union.right,
                if union.all { "UNION ALL" } else { "UNION" },
                union.with,
                union.order_by,
                union.limit,
                union.offset,
                inherited,
                outer,
            ),
            Expression::Intersect(set) => self.set(
                set.left,
                set.right,
                if set.all {
                    "INTERSECT ALL"
                } else {
                    "INTERSECT"
                },
                set.with,
                set.order_by,
                set.limit,
                set.offset,
                inherited,
                outer,
            ),
            Expression::Except(set) => self.set(
                set.left,
                set.right,
                if set.all { "EXCEPT ALL" } else { "EXCEPT" },
                set.with,
                set.order_by,
                set.limit,
                set.offset,
                inherited,
                outer,
            ),
            Expression::Values(values) => {
                let width = values
                    .expressions
                    .first()
                    .map(|row| row.expressions.len())
                    .unwrap_or(0);
                let outputs = (0..width)
                    .map(|index| Output {
                        data_type: "UNKNOWN".into(),
                        name: Identifier::new(format!("col{index}")),
                        slot: format!("col{index}"),
                        measure: None,
                    })
                    .collect();
                Ok(Relation {
                    sql: format!("SELECT * FROM ({})", sql(&Expression::Values(values))?),
                    outputs,
                    hidden: BTreeSet::new(),
                })
            }
            _ => Err(error("expected a SELECT or set query")),
        }
    }
    fn ctes(
        &mut self,
        with: Option<With>,
        inherited: &HashMap<String, Relation>,
        outer: Option<Context>,
    ) -> Result<HashMap<String, Relation>> {
        let mut ctes = inherited.clone();
        if let Some(with) = with {
            for cte in with.ctes {
                let mut self_reference = false;
                if with.recursive {
                    binding::transform_nodes(cte.this.clone(), &mut |node| {
                        if let Expression::Table(table) = node {
                            if table.schema.is_none() && key(&table.name) == key(&cte.alias) {
                                self_reference = true;
                            }
                        }
                        Ok(None)
                    })?;
                }
                if self_reference {
                    let anchor = match &cte.this {
                        Expression::Union(union) => union.left.clone(),
                        _ => return Err(error("recursive CTE requires a UNION anchor")),
                    };
                    let mut anchor = self.query(anchor, &ctes, outer.clone())?;
                    if anchor.has_measures() {
                        return Err(error(
                            "recursive set operands cannot carry unevaluated measures",
                        ));
                    }
                    Self::alias_columns(&mut anchor, &cte.columns)?;
                    let names = anchor
                        .outputs
                        .iter()
                        .map(|out| out.name.clone())
                        .collect::<Vec<_>>();
                    let recursive_source = Relation {
                        sql: format!(
                            "SELECT {} FROM {}",
                            names
                                .iter()
                                .map(|name| q(&name.name))
                                .collect::<Vec<_>>()
                                .join(", "),
                            q(&cte.alias.name)
                        ),
                        outputs: anchor
                            .outputs
                            .iter()
                            .map(|out| Output {
                                name: out.name.clone(),
                                slot: out.name.name.clone(),
                                measure: None,
                                data_type: out.data_type.clone(),
                            })
                            .collect(),
                        hidden: BTreeSet::new(),
                    };
                    let mut recursive_scope = ctes.clone();
                    recursive_scope.insert(key(&cte.alias), recursive_source);
                    let Expression::Union(union) = cte.this else {
                        unreachable!()
                    };
                    let step = self.query(union.right, &recursive_scope, outer.clone())?;
                    if step.has_measures() {
                        return Err(error(
                            "recursive set operands cannot carry unevaluated measures",
                        ));
                    }
                    if step.outputs.len() != anchor.outputs.len() {
                        return Err(error("recursive operands must have equal width"));
                    }
                    let body = format!(
                        "({}) UNION{} ({})",
                        anchor.visible_sql(),
                        if union.all { " ALL" } else { "" },
                        step.visible_sql()
                    );
                    let outputs = anchor.outputs.clone();
                    let sql = format!(
                        "WITH RECURSIVE {} ({}) AS ({}) SELECT {} FROM {}",
                        q(&cte.alias.name),
                        names
                            .iter()
                            .map(|name| q(&name.name))
                            .collect::<Vec<_>>()
                            .join(", "),
                        body,
                        outputs
                            .iter()
                            .map(|out| format!("{} AS {}", q(&out.name.name), q(&out.slot)))
                            .collect::<Vec<_>>()
                            .join(", "),
                        q(&cte.alias.name)
                    );
                    ctes.insert(
                        key(&cte.alias),
                        Relation {
                            sql,
                            outputs,
                            hidden: BTreeSet::new(),
                        },
                    );
                    continue;
                }

                let mut relation = self.query(cte.this, &ctes, outer.clone())?;
                Self::alias_columns(&mut relation, &cte.columns)?;
                ctes.insert(key(&cte.alias), relation);
            }
        }
        Ok(ctes)
    }
    fn set(
        &mut self,
        left: Expression,
        mut right: Expression,
        operator: &str,
        with: Option<With>,
        mut order_by: Option<polyglot_sql::expressions::OrderBy>,
        mut limit: Option<Box<Expression>>,
        mut offset: Option<Box<Expression>>,
        inherited: &HashMap<String, Relation>,
        outer: Option<Context>,
    ) -> Result<Relation> {
        // Polyglot attaches unparenthesized compound-query tails to the right SELECT.
        // A parenthesized branch remains a Subquery and keeps its own local tail.
        if let Expression::Select(select) = &mut right {
            if order_by.is_none() {
                order_by = select.order_by.take();
            }
            if limit.is_none() {
                limit = select.limit.take().map(|value| Box::new(value.this));
            }
            if offset.is_none() {
                offset = select.offset.take().map(|value| Box::new(value.this));
            }
        }
        let ctes = self.ctes(with, inherited, outer.clone())?;
        let left = self.query(left, &ctes, outer.clone())?;
        let right = self.query(right, &ctes, outer)?;
        if left.has_measures() || right.has_measures() {
            return Err(error("set operators cannot carry unevaluated measures"));
        }
        if left.outputs.len() != right.outputs.len() {
            return Err(error("set operands must have the same column count"));
        }
        let right_sql = format!(
            "SELECT {} FROM ({}) AS __ossie_set",
            right
                .outputs
                .iter()
                .zip(&left.outputs)
                .map(|(r, l)| format!("{} AS {}", q(&r.slot), q(&l.slot)))
                .collect::<Vec<_>>()
                .join(", "),
            right.sql
        );
        let mut body = format!(
            "SELECT * FROM (({}) {operator} ({right_sql})) AS __ossie_set_result",
            left.sql
        );
        if let Some(order) = order_by {
            let mapping = left
                .outputs
                .iter()
                .map(|out| (key(&out.name), out.slot.clone()))
                .collect::<HashMap<_, _>>();
            let order = order
                .expressions
                .into_iter()
                .map(|mut order| {
                    order.this = binding::transform_nodes(order.this, &mut |node| {
                        if let Expression::Column(value) = node {
                            return mapping
                                .get(&key(&value.name))
                                .map(|name| Ok(Some(column(name))))
                                .unwrap_or_else(|| Err(error("unknown set ORDER BY output")));
                        }
                        Ok(None)
                    })?;
                    sql(&Expression::Ordered(Box::new(order)))
                })
                .collect::<Result<Vec<_>>>()?;
            body.push_str(&format!(" ORDER BY {}", order.join(", ")));
        }
        if let Some(limit) = limit {
            let value = if let Expression::Limit(limit) = *limit {
                limit.this
            } else {
                *limit
            };
            body.push_str(&format!(" LIMIT {}", sql(&value)?));
        }
        if let Some(offset) = offset {
            let value = if let Expression::Offset(offset) = *offset {
                offset.this
            } else {
                *offset
            };
            body.push_str(&format!(" OFFSET {}", sql(&value)?));
        }
        Ok(Relation {
            sql: body,
            outputs: left.outputs,
            hidden: BTreeSet::new(),
        })
    }
    fn select(
        &mut self,
        select: Select,
        inherited: &HashMap<String, Relation>,
        outer: Option<Context>,
    ) -> Result<Relation> {
        let ctes = self.ctes(select.with.clone(), inherited, outer.clone())?;
        let mut hints = Vec::new();
        binding::transform_nodes(Expression::Select(Box::new(select.clone())), &mut |node| {
            if matches!(node, Expression::Subquery(_)) {
                return Ok(Some(node.clone()));
            }
            if let Expression::Column(column) = node {
                hints.push(*column.clone());
            }
            Ok(None)
        })?;
        let mut sources = Vec::new();
        for source in select
            .from
            .as_ref()
            .into_iter()
            .flat_map(|from| &from.expressions)
        {
            sources.push(self.source(source.clone(), &ctes, outer.clone(), &hints)?);
        }
        let from_count = sources.len();
        for join in &select.joins {
            sources.push(self.source(join.this.clone(), &ctes, outer.clone(), &hints)?);
        }
        let mut raw_context = Context {
            columns: Vec::new(),
            outer: outer.clone().map(Box::new),
        };
        let mut context = Context {
            columns: Vec::new(),
            outer: outer.map(Box::new),
        };
        let mut selections = Vec::new();
        let mut from = String::new();
        for source in &sources {
            let mut mapping = HashMap::new();
            for slot in source.relation.slots() {
                let new = self.fresh("row");
                selections.push(format!(
                    "{}.{} AS {}",
                    q(&source.alias.name),
                    q(&slot),
                    q(&new)
                ));
                mapping.insert(slot, new);
            }
            for output in &source.relation.outputs {
                let measure = output
                    .measure
                    .as_ref()
                    .map(|recipe| recipe.rename(&mapping))
                    .transpose()?;
                raw_context.columns.push(Bound {
                    data_type: output.data_type.clone(),
                    unqualified: true,
                    name: output.name.clone(),
                    qualifier: key(&source.alias),
                    value: qualified(&source.alias.name, &output.slot)?,
                    measure: measure.clone(),
                });
                context.columns.push(Bound {
                    data_type: output.data_type.clone(),
                    unqualified: true,
                    name: output.name.clone(),
                    qualifier: key(&source.alias),
                    value: column(mapping.get(&output.slot).unwrap_or(&output.slot)),
                    measure,
                });
            }
        }
        for (index, source) in sources.iter().take(from_count).enumerate() {
            if index > 0 {
                from.push_str(", ");
            }
            from.push_str(&format!(
                "({}) AS {}",
                source.relation.sql,
                q(&source.alias.name)
            ));
        }
        for (index, join) in select.joins.iter().enumerate() {
            let source = &sources[from_count + index];
            let mut rendered = join.clone();
            rendered.this = parse(&format!(
                "SELECT * FROM ({}) AS {}",
                source.relation.sql,
                q(&source.alias.name)
            ))?;
            if let Expression::Select(query) = rendered.this {
                rendered.this = query.from.unwrap().expressions.into_iter().next().unwrap();
            }
            if let Some(on) = &join.on {
                rendered.on = Some(self.scalar(on.clone(), &raw_context, &ctes, false)?);
            }
            use polyglot_sql::expressions::JoinKind;
            let natural = matches!(
                join.kind,
                JoinKind::Natural
                    | JoinKind::NaturalLeft
                    | JoinKind::NaturalRight
                    | JoinKind::NaturalFull
            );
            let left_qualifiers = sources[..from_count + index]
                .iter()
                .map(|source| key(&source.alias))
                .collect::<HashSet<_>>();
            let using = if natural {
                source
                    .relation
                    .outputs
                    .iter()
                    .filter(|right| {
                        raw_context.columns.iter().any(|left| {
                            left_qualifiers.contains(&left.qualifier)
                                && key(&left.name) == key(&right.name)
                        })
                    })
                    .map(|out| out.name.clone())
                    .collect::<Vec<_>>()
            } else {
                join.using.clone()
            };
            if natural {
                rendered.kind = match join.kind {
                    JoinKind::NaturalLeft => JoinKind::Left,
                    JoinKind::NaturalRight => JoinKind::Right,
                    JoinKind::NaturalFull => JoinKind::Full,
                    _ => JoinKind::Inner,
                };
            }
            if !using.is_empty() {
                let mut conditions = Vec::new();
                for name in &using {
                    let right_index = raw_context
                        .columns
                        .iter()
                        .position(|bound| {
                            bound.qualifier == key(&source.alias) && key(&bound.name) == key(name)
                        })
                        .ok_or_else(|| error("USING field not found"))?;
                    let left_index = raw_context
                        .columns
                        .iter()
                        .position(|bound| {
                            (left_qualifiers.contains(&bound.qualifier)
                                || bound.qualifier.is_empty())
                                && bound.unqualified
                                && key(&bound.name) == key(name)
                        })
                        .ok_or_else(|| error("USING field not found"))?;
                    if raw_context.columns[right_index].measure.is_some()
                        || raw_context.columns[left_index].measure.is_some()
                    {
                        return Err(error("JOIN USING cannot interpret a measure"));
                    }
                    conditions.push(format!(
                        "{} = {}",
                        sql(&raw_context.columns[left_index].value)?,
                        sql(&raw_context.columns[right_index].value)?
                    ));
                    for scope in [&mut raw_context, &mut context] {
                        let left = scope.columns[left_index].value.clone();
                        let right = scope.columns[right_index].value.clone();
                        scope.columns[left_index].unqualified = false;
                        scope.columns[right_index].unqualified = false;
                        let value = match rendered.kind {
                            JoinKind::Full | JoinKind::Outer => {
                                expr(&format!("COALESCE({}, {})", sql(&left)?, sql(&right)?))?
                            }
                            JoinKind::Right => right,
                            _ => left,
                        };
                        scope.columns.push(Bound {
                            data_type: scope.columns[left_index].data_type.clone(),
                            unqualified: true,
                            name: name.clone(),
                            qualifier: String::new(),
                            value,
                            measure: None,
                        });
                    }
                }
                rendered.using.clear();
                rendered.on = Some(expr(&conditions.join(" AND "))?);
            } else if natural {
                rendered.on = Some(expr("TRUE")?);
            }
            let Expression::Select(mut join_query) = parse("SELECT * FROM __ossie_join_left")?
            else {
                unreachable!()
            };
            join_query.joins = vec![rendered];
            let rendered = sql(&Expression::Select(join_query))?;
            let suffix = rendered
                .strip_prefix("SELECT * FROM __ossie_join_left")
                .ok_or_else(|| error("join rendering failed"))?;
            from.push_str(suffix);
        }
        let identity = self.fresh("defined_identity");
        selections.push(format!("ROW_NUMBER() OVER () AS {}", q(&identity)));
        let mut joined_sql = format!("SELECT {}", selections.join(", "));
        if !from.is_empty() {
            joined_sql.push_str(&format!(" FROM {from}"));
        }
        if let Some(filter) = &select.where_clause {
            joined_sql.push_str(&format!(
                " WHERE {}",
                sql(&self.scalar(filter.this.clone(), &raw_context, &ctes, false)?)?
            ));
        }
        let joined = self.fresh("joined");
        let projections = self.expand_projections(&select.expressions, &context)?;
        let aliases = projections
            .iter()
            .map(|(name, expression)| (key(name), expression.clone()))
            .collect::<HashMap<_, _>>();
        let aggregated = select.group_by.is_some()
            || select.having.is_some()
            || projections.iter().try_fold(false, |found, (_, expr)| {
                Ok::<_, SidemanticError>(found || has_aggregate(expr)?)
            })?
            || select.order_by.as_ref().is_some_and(|order| {
                order
                    .expressions
                    .iter()
                    .any(|order| has_aggregate(&order.this).unwrap_or(false))
            });
        if aggregated {
            return self.aggregated(
                select,
                projections,
                aliases,
                context,
                ctes,
                joined,
                joined_sql,
            );
        }
        let mut outputs = Vec::new();
        let mut hidden = BTreeSet::new();
        let mut projected = Vec::new();
        for (name, projection) in projections {
            let slot = self.fresh("output");
            let data_type = if let Expression::Column(column) = &projection {
                context.resolve(column)?.data_type
            } else {
                "UNKNOWN".into()
            };
            let measure = if let Expression::Column(column) = &projection {
                context.resolve(column)?.measure
            } else if let Expression::Function(function) = &projection {
                if function.name.eq_ignore_ascii_case("TO_MEASURE") {
                    if function.distinct || function.args.len() != 1 {
                        return Err(error("TO_MEASURE requires one aggregate expression"));
                    }
                    Some(self.definition(function.args[0].clone(), &context, &ctes, &identity)?)
                } else {
                    None
                }
            } else {
                None
            };
            if let Some(recipe) = &measure {
                if select.distinct {
                    return Err(error("DISTINCT cannot carry an unevaluated measure"));
                }
                hidden.extend(recipe.refs());
                projected.push(format!("NULL AS {}", q(&slot)));
            } else {
                projected.push(format!(
                    "{} AS {}",
                    sql(&self.scalar(projection, &context, &ctes, false)?)?,
                    q(&slot)
                ));
            }
            outputs.push(Output {
                data_type,
                name,
                slot,
                measure,
            });
        }
        for name in &hidden {
            projected.push(q(name));
        }
        let distinct = if select.distinct { "DISTINCT " } else { "" };
        let mut body = format!(
            "SELECT {distinct}{} FROM {}",
            projected.join(", "),
            q(&joined)
        );
        if let Some(order) = &select.order_by {
            let mut ordering = Vec::new();
            for value in &order.expressions {
                let mut ordered = value.clone();
                ordered.this = expand_alias(ordered.this, &aliases)?;
                ordered.this = self.scalar(ordered.this, &context, &ctes, false)?;
                ordering.push(sql(&Expression::Ordered(Box::new(ordered)))?);
            }
            body.push_str(&format!(" ORDER BY {}", ordering.join(", ")));
        }
        append_limit(&mut body, &select)?;
        Ok(Relation {
            sql: format!("WITH {} AS MATERIALIZED ({joined_sql}) {body}", q(&joined)),
            outputs,
            hidden,
        })
    }
    fn expand_projections(
        &self,
        projections: &[Expression],
        context: &Context,
    ) -> Result<Vec<(Identifier, Expression)>> {
        let mut result = Vec::new();
        for projection in projections {
            if let Expression::Star(star) = projection {
                for bound in &context.columns {
                    if star.table.is_none() && !bound.unqualified {
                        continue;
                    }
                    if star
                        .table
                        .as_ref()
                        .is_some_and(|table| key(table) != bound.qualifier)
                    {
                        continue;
                    }
                    if star.except.as_ref().is_some_and(|except| {
                        except.iter().any(|name| key(name) == key(&bound.name))
                    }) {
                        continue;
                    }
                    let mut reference = Column {
                        name: bound.name.clone(),
                        table: None,
                        join_mark: false,
                        trailing_comments: vec![],
                        span: None,
                        inferred_type: None,
                    };
                    reference.table = if bound.qualifier.is_empty() {
                        None
                    } else {
                        Some(Identifier::quoted(&bound.qualifier))
                    };
                    let mut name = bound.name.clone();
                    if let Some(renamed) = star
                        .rename
                        .as_ref()
                        .and_then(|renames| renames.iter().find(|(old, _)| key(old) == key(&name)))
                    {
                        name = renamed.1.clone();
                    }
                    let expression = star
                        .replace
                        .as_ref()
                        .and_then(|replacements| {
                            replacements
                                .iter()
                                .find(|replace| key(&replace.alias) == key(&bound.name))
                        })
                        .map(|replace| replace.this.clone())
                        .unwrap_or(Expression::Column(Box::new(reference)));
                    result.push((name, expression));
                }
                continue;
            }
            match projection {
                Expression::Alias(alias) => result.push((alias.alias.clone(), alias.this.clone())),
                Expression::Column(column) => {
                    result.push((column.name.clone(), projection.clone()))
                }
                _ => result.push((Identifier::quoted(sql(projection)?), projection.clone())),
            }
        }
        Ok(result)
    }
    fn aggregated(
        &mut self,
        select: Select,
        projections: Vec<(Identifier, Expression)>,
        aliases: HashMap<String, Expression>,
        context: Context,
        ctes: HashMap<String, Relation>,
        joined: String,
        joined_sql: String,
    ) -> Result<Relation> {
        let mut group_sets = vec![Vec::new()];
        if let Some(group) = &select.group_by {
            let expressions = if group.all == Some(true) {
                projections
                    .iter()
                    .filter_map(|(_, projection)| match has_aggregate(projection) {
                        Ok(false) => Some(Ok(projection.clone())),
                        Ok(true) => None,
                        Err(error) => Some(Err(error)),
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                group.expressions.clone()
            };
            for expression in expressions {
                let sets = group_options(expression);
                let mut product = Vec::new();
                for previous in &group_sets {
                    for options in &sets {
                        let mut values = previous.clone();
                        for value in options {
                            let value = if let Expression::Literal(Literal::Number(number)) = value
                            {
                                number
                                    .parse::<usize>()
                                    .ok()
                                    .filter(|n| *n > 0)
                                    .and_then(|n| projections.get(n - 1))
                                    .map(|(_, expr)| expr.clone())
                                    .unwrap_or_else(|| value.clone())
                            } else {
                                expand_alias(value.clone(), &aliases)?
                            };
                            values.push(self.scalar(value, &context, &ctes, false)?);
                        }
                        product.push(values);
                    }
                }
                group_sets = product;
            }
        }
        let mut all_groups = BTreeMap::new();
        for set in &group_sets {
            for value in set {
                all_groups
                    .entry(sql(value)?)
                    .or_insert_with(|| value.clone());
            }
        }
        let output_slots = projections
            .iter()
            .map(|_| self.fresh("output"))
            .collect::<Vec<_>>();
        let order_slots = select
            .order_by
            .as_ref()
            .map(|order| {
                order
                    .expressions
                    .iter()
                    .map(|_| self.fresh("sort"))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut branches = Vec::new();
        for groups in &group_sets {
            let spine = self.fresh("spine");
            let group_slots = groups
                .iter()
                .map(|_| self.fresh("group"))
                .collect::<Vec<_>>();
            let group_map = groups
                .iter()
                .zip(&group_slots)
                .map(|(expression, slot)| Ok((sql(expression)?, slot.clone())))
                .collect::<Result<HashMap<_, _>>>()?;
            let predicate = groups
                .iter()
                .zip(&group_slots)
                .map(|(value, slot)| {
                    Ok(format!(
                        "{} IS NOT DISTINCT FROM {}.{}",
                        sql(value)?,
                        q(&spine),
                        q(slot)
                    ))
                })
                .collect::<Result<Vec<_>>>()?
                .join(" AND ");
            let predicate = if predicate.is_empty() {
                "TRUE".to_string()
            } else {
                predicate
            };
            let mut raw = Vec::new();
            let mut selected = Vec::new();
            for ((_, projection), slot) in projections.iter().zip(&output_slots) {
                let value = self.evaluated(
                    projection.clone(),
                    &context,
                    &ctes,
                    &HashMap::new(),
                    &all_groups,
                    &group_map,
                    &joined,
                    &spine,
                    &predicate,
                    &mut raw,
                )?;
                selected.push(format!("{} AS {}", sql(&value)?, q(slot)));
            }
            let having = select
                .having
                .as_ref()
                .map(|having| {
                    self.evaluated(
                        expand_alias(having.this.clone(), &aliases)?,
                        &context,
                        &ctes,
                        &aliases,
                        &all_groups,
                        &group_map,
                        &joined,
                        &spine,
                        &predicate,
                        &mut raw,
                    )
                })
                .transpose()?;
            if let Some(order) = &select.order_by {
                for (value, slot) in order.expressions.iter().zip(&order_slots) {
                    let value = if let Expression::Literal(Literal::Number(number)) = &value.this {
                        number
                            .parse::<usize>()
                            .ok()
                            .filter(|n| *n > 0)
                            .and_then(|n| projections.get(n - 1))
                            .map(|(_, expression)| expression.clone())
                            .unwrap_or_else(|| value.this.clone())
                    } else {
                        expand_alias(value.this.clone(), &aliases)?
                    };
                    let value = self.evaluated(
                        value,
                        &context,
                        &ctes,
                        &aliases,
                        &all_groups,
                        &group_map,
                        &joined,
                        &spine,
                        &predicate,
                        &mut raw,
                    )?;
                    selected.push(format!("{} AS {}", sql(&value)?, q(slot)));
                }
            }
            let mut spine_select = groups
                .iter()
                .zip(&group_slots)
                .map(|(value, slot)| Ok(format!("{} AS {}", sql(value)?, q(slot))))
                .collect::<Result<Vec<_>>>()?;
            spine_select.extend(
                raw.iter()
                    .map(|(slot, value)| Ok(format!("{} AS {}", sql(value)?, q(slot))))
                    .collect::<Result<Vec<_>>>()?,
            );
            if groups.is_empty() && raw.is_empty() {
                spine_select.push("COUNT(*) AS __ossie_domain".into());
            }
            let mut spine_sql = format!("SELECT {} FROM {}", spine_select.join(", "), q(&joined));
            if !groups.is_empty() {
                spine_sql.push_str(&format!(
                    " GROUP BY {}",
                    groups
                        .iter()
                        .map(sql)
                        .collect::<Result<Vec<_>>>()?
                        .join(", ")
                ));
            }
            let mut body = format!(
                "WITH {} AS ({spine_sql}) SELECT {} FROM {}",
                q(&spine),
                selected.join(", "),
                q(&spine)
            );
            if let Some(having) = having {
                body.push_str(&format!(" WHERE {}", sql(&having)?));
            }
            branches.push(format!("({body})"));
        }
        let combined = self.fresh("groups");
        let distinct = if select.distinct { "DISTINCT " } else { "" };
        let mut body = format!(
            "SELECT {distinct}{} FROM ({}) AS {}",
            output_slots
                .iter()
                .map(|slot| q(slot))
                .collect::<Vec<_>>()
                .join(", "),
            branches.join(" UNION ALL "),
            q(&combined)
        );
        if let Some(order) = &select.order_by {
            let rendered = order
                .expressions
                .iter()
                .zip(&order_slots)
                .map(|(value, slot)| {
                    let mut value = value.clone();
                    value.this = column(slot);
                    sql(&Expression::Ordered(Box::new(value)))
                })
                .collect::<Result<Vec<_>>>()?;
            body.push_str(&format!(" ORDER BY {}", rendered.join(", ")));
        }
        append_limit(&mut body, &select)?;
        Ok(Relation {
            sql: format!("WITH {} AS MATERIALIZED ({joined_sql}) {body}", q(&joined)),
            outputs: projections
                .into_iter()
                .zip(output_slots)
                .map(|((name, _), slot)| Output {
                    data_type: "UNKNOWN".into(),
                    name,
                    slot,
                    measure: None,
                })
                .collect(),
            hidden: BTreeSet::new(),
        })
    }
    #[allow(clippy::too_many_arguments)]
    fn evaluated(
        &mut self,
        node: Expression,
        context: &Context,
        ctes: &HashMap<String, Relation>,
        aliases: &HashMap<String, Expression>,
        all_groups: &BTreeMap<String, Expression>,
        groups: &HashMap<String, String>,
        joined: &str,
        spine: &str,
        predicate: &str,
        raw: &mut Vec<(String, Expression)>,
    ) -> Result<Expression> {
        binding::transform_nodes(node, &mut |node| {
            if is_call(node, "AGG") {
                return Ok(Some({
                    let predicate = if let Expression::AggregateFunction(function) = node {
                        if let Some(filter) = &function.filter {
                            format!(
                                "({predicate}) AND ({})",
                                sql(&self.scalar(filter.clone(), context, ctes, false)?)?
                            )
                        } else {
                            predicate.to_string()
                        }
                    } else {
                        predicate.to_string()
                    };
                    self.resolve_measure(node, context)?
                        .evaluate(joined, &predicate)?
                }));
            }
            if is_call(node, "TO_MEASURE") {
                return Err(error(
                    "measure definitions belong in unaggregated projections",
                ));
            }
            let subquery = match node {
                Expression::Subquery(subquery) => Some((subquery.this.clone(), false, true)),
                Expression::Exists(exists) => Some((exists.this.clone(), true, false)),
                Expression::Select(_) => Some((node.clone(), false, false)),
                _ => None,
            };
            if let Some((query, exists, wrapped)) = subquery {
                let relation = self.query(query, ctes, Some(context.clone()))?;
                if !exists && (relation.has_measures() || relation.outputs.len() != 1) {
                    return Err(error("scalar subquery requires one scalar column"));
                }
                let outer_refs = context
                    .columns
                    .iter()
                    .filter(|bound| bound.measure.is_none())
                    .flat_map(|bound| refs(&bound.value).unwrap_or_default())
                    .collect::<HashSet<_>>();
                let query =
                    binding::transform_nodes(parse(&relation.visible_sql())?, &mut |value| {
                        let canonical = sql(value)?;
                        if let Some(slot) = groups.get(&canonical) {
                            return Ok(Some(qualified(spine, slot)?));
                        }
                        if all_groups.contains_key(&canonical) {
                            return Ok(Some(expr("NULL")?));
                        }
                        if let Expression::Column(column) = value {
                            if outer_refs.contains(&column.name.name) {
                                return Err(error(
                                    "correlated outer field must be grouped or aggregated",
                                ));
                            }
                        }
                        Ok(None)
                    })?;
                return Ok(Some(if exists {
                    let Expression::Exists(exists) = node else {
                        unreachable!()
                    };
                    let mut exists = exists.clone();
                    exists.this = query;
                    Expression::Exists(exists)
                } else if wrapped {
                    expr(&format!("({})", sql(&query)?))?
                } else {
                    query
                }));
            }
            let grouping_args = match node {
                Expression::Grouping(group) => Some(group.expressions.as_slice()),
                Expression::GroupingId(group) => Some(group.expressions.as_slice()),
                Expression::Function(function)
                    if function.name.eq_ignore_ascii_case("GROUPING")
                        || function.name.eq_ignore_ascii_case("GROUPING_ID") =>
                {
                    Some(function.args.as_slice())
                }
                _ => None,
            };
            if let Some(arguments) = grouping_args {
                let mut bits = 0u64;
                for argument in arguments {
                    let argument = self.scalar(argument.clone(), context, ctes, false)?;
                    let key = sql(&argument)?;
                    if !all_groups.contains_key(&key) {
                        return Err(error("GROUPING argument is not a grouping expression"));
                    }
                    bits = bits * 2 + u64::from(!groups.contains_key(&key));
                }
                return Ok(Some(expr(&bits.to_string())?));
            }
            if aggregate(node) {
                let value = self.scalar(node.clone(), context, ctes, true)?;
                let slot = self.fresh("aggregate");
                raw.push((slot.clone(), value));
                return Ok(Some(qualified(spine, &slot)?));
            }
            if let Ok(value) = self.scalar(node.clone(), context, ctes, false) {
                let key = sql(&value)?;
                if let Some(slot) = groups.get(&key) {
                    return Ok(Some(qualified(spine, slot)?));
                }
                if all_groups.contains_key(&key) {
                    return Ok(Some(expr("NULL")?));
                }
            }
            if let Expression::Column(column) = node {
                if column.table.is_none() {
                    if let Some(value) = aliases.get(&key(&column.name)) {
                        if value != node {
                            return Ok(Some(self.evaluated(
                                value.clone(),
                                context,
                                ctes,
                                &HashMap::new(),
                                all_groups,
                                groups,
                                joined,
                                spine,
                                predicate,
                                raw,
                            )?));
                        }
                    }
                }
                let bound = context.resolve(column)?;
                if bound.measure.is_some() {
                    return Err(error("unevaluated measure in aggregated projection"));
                }
                return Err(error(format!(
                    "field {} must be grouped or aggregated",
                    column.name.name
                )));
            }
            Ok(None)
        })
    }
}

fn expand_alias(node: Expression, aliases: &HashMap<String, Expression>) -> Result<Expression> {
    binding::transform_nodes(node, &mut |node| {
        if aggregate(node)
            || is_call(node, "TO_MEASURE")
            || matches!(node, Expression::Subquery(_) | Expression::Select(_))
        {
            return Ok(Some(node.clone()));
        }
        if let Expression::Column(column) = node {
            if column.table.is_none() {
                if let Some(value) = aliases.get(&key(&column.name)) {
                    if value != node {
                        return Ok(Some(value.clone()));
                    }
                }
            }
        }
        Ok(None)
    })
}
fn group_options(node: Expression) -> Vec<Vec<Expression>> {
    match node {
        Expression::Function(function) if function.name.eq_ignore_ascii_case("ROLLUP") => (0
            ..=function.args.len())
            .rev()
            .map(|n| function.args[..n].to_vec())
            .collect(),
        Expression::Function(function) if function.name.eq_ignore_ascii_case("CUBE") => (0
            ..(1usize << function.args.len()))
            .map(|mask| {
                function
                    .args
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| mask & (1 << i) != 0)
                    .map(|(_, value)| value.clone())
                    .collect()
            })
            .collect(),
        Expression::Function(function) if function.name.eq_ignore_ascii_case("GROUPING SETS") => {
            function
                .args
                .into_iter()
                .map(|value| match value {
                    Expression::Tuple(tuple) => tuple.expressions,
                    Expression::Paren(paren) => vec![paren.this],
                    other => vec![other],
                })
                .collect()
        }
        Expression::Rollup(rollup) => (0..=rollup.expressions.len())
            .rev()
            .map(|n| rollup.expressions[..n].to_vec())
            .collect(),
        Expression::Cube(cube) => (0..(1usize << cube.expressions.len()))
            .map(|mask| {
                cube.expressions
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| mask & (1 << i) != 0)
                    .map(|(_, expr)| expr.clone())
                    .collect()
            })
            .collect(),
        Expression::GroupingSets(sets) => sets
            .expressions
            .into_iter()
            .map(|expr| match expr {
                Expression::Tuple(tuple) => tuple.expressions,
                Expression::Paren(paren) => vec![paren.this],
                other => vec![other],
            })
            .collect(),
        other => vec![vec![other]],
    }
}
fn append_limit(body: &mut String, select: &Select) -> Result<()> {
    if let Some(limit) = &select.limit {
        body.push_str(&format!(" LIMIT {}", sql(&limit.this)?));
    }
    if let Some(offset) = &select.offset {
        body.push_str(&format!(" OFFSET {}", sql(&offset.this)?));
    }
    Ok(())
}

/// Translate the draft's illustrative marker using lexer spans, never textual
/// replacements in string literals or quoted identifiers.
fn preprocess_measure_syntax(source: &str) -> Result<String> {
    let tokens = polyglot_sql::Dialect::get(DialectType::DuckDB)
        .tokenize(source)
        .map_err(|err| error(err.to_string()))?;
    let offsets = source
        .char_indices()
        .map(|(offset, _)| offset)
        .chain([source.len()])
        .collect::<Vec<_>>();
    let mut depth = 0i32;
    let mut boundaries = HashMap::new();
    let mut edits = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        use polyglot_sql::tokens::TokenType;
        if token.token_type == TokenType::LParen {
            depth += 1;
        } else if token.token_type == TokenType::RParen {
            depth -= 1;
        }
        if token.token_type == TokenType::Select || token.token_type == TokenType::Comma {
            boundaries.insert(depth, token.span.end);
        }
        if token.token_type == TokenType::To
            && tokens.get(index + 1).is_some_and(|next| {
                next.token_type == TokenType::Var && next.text.eq_ignore_ascii_case("MEASURE")
            })
        {
            let start = *boundaries
                .get(&depth)
                .ok_or_else(|| error("TO MEASURE must follow a select expression"))?;
            edits.push((offsets[start], offsets[start], " TO_MEASURE(".to_string()));
            edits.push((
                offsets[token.span.start],
                offsets[tokens[index + 1].span.end],
                ")".to_string(),
            ));
        }
    }
    edits.sort_by_key(|edit| std::cmp::Reverse(edit.0));
    let mut result = source.to_string();
    for (start, end, replacement) in edits {
        result.replace_range(start..end, &replacement);
    }
    Ok(result)
}

impl QueryRewriter<'_> {
    fn has_ossie_relational_models(&self) -> bool {
        self.graph.models().any(|model| {
            model
                .metadata
                .as_ref()
                .and_then(|meta| meta.get("ossie_schema_revision"))
                .and_then(Value::as_str)
                == Some(crate::adapters::ossie::LATEST_SCHEMA_REVISION)
        }) || self.graph.metrics().any(|metric| {
            metric
                .metadata
                .as_ref()
                .and_then(|meta| meta.get("ossie_dataset_metric"))
                .and_then(Value::as_bool)
                == Some(true)
        }) || self
            .graph
            .metadata()
            .and_then(|metadata| metadata.get("ossie_views"))
            .is_some()
    }
    pub(super) fn rewrite_ossie_relational(
        &self,
        source: &str,
        input: DialectType,
        output: DialectType,
    ) -> Result<Option<String>> {
        if !self.has_ossie_relational_models()
            && !source.to_ascii_uppercase().contains("TO_MEASURE")
            && !source.to_ascii_uppercase().contains("TO MEASURE")
        {
            return Ok(None);
        }
        if self.security_controls {
            return Err(error(
                "security-controlled relational queries require explicit policy preparation",
            ));
        }
        let source = preprocess_measure_syntax(source)?;
        let mut statements = ossie_input(&source, input)?;
        if statements.len() != 1 {
            return Err(error("one query is required"));
        }
        if !matches!(
            statements[0],
            Expression::Select(_)
                | Expression::Union(_)
                | Expression::Intersect(_)
                | Expression::Except(_)
                | Expression::Subquery(_)
                | Expression::Paren(_)
        ) {
            return Ok(None);
        }
        let mut compiler = Compiler {
            input_dialect: input,
            graph: self.graph,
            sequence: 0,
            view_stack: HashSet::new(),
        };
        let mut relevant = false;
        binding::transform_nodes(statements[0].clone(), &mut |node| {
            if is_call(node, "AGG") || is_call(node, "TO_MEASURE") {
                relevant = true;
            }
            if let Expression::Table(table) = node {
                if compiler.model(table)?.is_some() {
                    relevant = true;
                }
                if compiler.view_definition(table)?.is_some() {
                    relevant = true;
                }
            }
            Ok(None)
        })?;
        if !relevant {
            return Ok(None);
        }
        let zero_rows = matches!(&statements[0],Expression::Select(select) if select.limit.as_ref().is_some_and(|limit|matches!(&limit.this,Expression::Literal(Literal::Number(number)) if number=="0"))||select.where_clause.as_ref().is_some_and(|filter|sql(&filter.this).is_ok_and(|value|value.eq_ignore_ascii_case("FALSE"))));
        let relation = compiler.query(statements.remove(0), &HashMap::new(), None)?;
        if relation.has_measures() && !zero_rows {
            return Err(error("top-level unevaluated measures require AGG"));
        }
        let visible = if relation.has_measures() {
            format!(
                "SELECT {} FROM ({}) AS __ossie_metadata",
                relation
                    .outputs
                    .iter()
                    .map(|out| format!(
                        "{} AS {}",
                        if out.measure.is_some() {
                            "CAST(NULL AS STRUCT(MEASURE VARCHAR))".to_string()
                        } else {
                            q(&out.slot)
                        },
                        q(&out.name.name)
                    ))
                    .collect::<Vec<_>>()
                    .join(", "),
                relation.sql
            )
        } else {
            relation.visible_sql()
        };
        let statement = crate::semantic_input::dialects::ossie_index_offsets(
            parse(&visible)?,
            DialectType::DuckDB,
            output,
        )?;
        Ok(Some(crate::semantic_input::dialects::emit(
            statement,
            DialectType::DuckDB,
            output,
        )?))
    }
    pub fn compile_ossie_view_definition(
        &self,
        source: &str,
        input: DialectType,
        output: DialectType,
    ) -> Result<(String, Value)> {
        if self.security_controls {
            return Err(error("measure views require explicit policy preparation"));
        }
        let source = preprocess_measure_syntax(source)?;
        let mut statements = ossie_input(&source, input)?;
        if statements.len() != 1 {
            return Err(error("one view query is required"));
        }
        let mut compiler = Compiler {
            input_dialect: input,
            graph: self.graph,
            sequence: 0,
            view_stack: HashSet::new(),
        };
        let relation = compiler.query(statements.remove(0), &HashMap::new(), None)?;
        let columns=relation.outputs.iter().map(|out|serde_json::json!({"name":out.name.name,"is_measure":out.measure.is_some(),"data_type":if out.measure.is_some(){format!("{} MEASURE",out.data_type)}else{out.data_type.clone()}})).collect::<Vec<_>>();
        let physical = if relation.has_measures() {
            format!(
                "SELECT {} FROM ({}) AS __ossie_view",
                relation
                    .outputs
                    .iter()
                    .map(|out| format!(
                        "{} AS {}",
                        if out.measure.is_some() {
                            "NULL".to_string()
                        } else {
                            q(&out.slot)
                        },
                        q(&out.name.name)
                    ))
                    .collect::<Vec<_>>()
                    .join(", "),
                relation.sql
            )
        } else {
            relation.visible_sql()
        };
        Ok((
            crate::semantic_input::dialects::emit(
                crate::semantic_input::dialects::ossie_index_offsets(
                    parse(&physical)?,
                    DialectType::DuckDB,
                    output,
                )?,
                DialectType::DuckDB,
                output,
            )?,
            Value::Array(columns),
        ))
    }
}
