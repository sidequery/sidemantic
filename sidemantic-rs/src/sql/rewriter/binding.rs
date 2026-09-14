//! Bind semantic fields before evaluating the user's scalar SQL expressions.
//!
//! The structured planner owns aggregate populations, fanout and policies. SQL
//! expressions operate on its result; aggregating the physical join here would
//! give a different answer when a relationship multiplies rows.

use super::*;
use crate::core::Metric;
use crate::sql::SqlGenerator;
use polyglot_sql::expressions::Column;

/// Visit the serialized polyglot AST because the pinned public visitor omits
/// typed function children. This transforms AST nodes, never SQL source text.
/// Returning a replacement stops descent into that node, preserving SQL scopes
/// and preventing already-bound expressions from being bound a second time.
pub(super) fn transform_nodes(
    expression: Expression,
    replace: &mut impl FnMut(&Expression) -> Result<Option<Expression>>,
) -> Result<Expression> {
    fn visit(
        value: &mut serde_json::Value,
        replace: &mut impl FnMut(&Expression) -> Result<Option<Expression>>,
    ) -> Result<()> {
        if value.as_object().is_some_and(|fields| fields.len() == 1) {
            if let Ok(expression) = serde_json::from_value::<Expression>(value.clone()) {
                if let Some(replacement) = replace(&expression)? {
                    *value = serde_json::to_value(replacement)
                        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
                    return Ok(());
                }
            }
        }
        match value {
            serde_json::Value::Object(fields) => {
                for child in fields.values_mut() {
                    visit(child, replace)?;
                }
            }
            serde_json::Value::Array(children) => {
                for child in children {
                    visit(child, replace)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    let mut value = serde_json::to_value(expression)
        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
    visit(&mut value, replace)?;
    serde_json::from_value(value).map_err(|error| SidemanticError::SqlGeneration(error.to_string()))
}

fn expression_sql(expression: &Expression) -> Result<String> {
    polyglot_generate(expression, DialectType::DuckDB)
        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))
}

fn aggregate_name(expression: &Expression) -> Option<String> {
    match expression {
        Expression::AggregateFunction(function) => return Some(function.name.to_ascii_lowercase()),
        Expression::WithinGroup(within) => return aggregate_name(&within.this),
        Expression::Filter(filter) => return aggregate_name(&filter.this),
        _ => {}
    }
    let value = serde_json::to_value(expression).ok()?;
    let name = value.as_object()?.keys().next()?.as_str();
    matches!(
        name,
        "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "median"
            | "mode"
            | "stddev"
            | "stddev_pop"
            | "stddev_samp"
            | "variance"
            | "var_pop"
            | "var_samp"
            | "aggregate_function"
            | "group_concat"
            | "string_agg"
            | "list_agg"
            | "array_agg"
            | "count_if"
            | "sum_if"
            | "first"
            | "last"
            | "any_value"
            | "approx_distinct"
            | "approx_count_distinct"
            | "approx_percentile"
            | "percentile"
            | "logical_and"
            | "logical_or"
            | "skewness"
            | "array_concat_agg"
            | "array_unique_agg"
            | "bool_xor_agg"
            | "bitwise_and_agg"
            | "bitwise_or_agg"
            | "bitwise_xor_agg"
            | "percentile_cont"
            | "percentile_disc"
            | "quantile"
            | "approx_quantile"
            | "approx_quantiles"
            | "json_array_agg"
            | "json_object_agg"
    )
    .then(|| name.to_string())
}

struct Bindings {
    graph: SemanticGraph,
    query: SemanticQuery,
    sources: Vec<(String, String)>,
    base: Option<String>,
    references: HashMap<String, String>,
    aggregate_inputs: Vec<String>,
    next_metric: usize,
    strict_fields: bool,
}

impl Bindings {
    fn resolve(&self, column: &Column) -> Result<String> {
        let name = &column.name.name;
        if let Some(table) = &column.table {
            let model = resolve_model_ref(&table.name, &self.sources)
                .map_or(table.name.as_str(), |(model, _)| model);
            return Ok(format!("{model}.{name}"));
        }
        if let Some(base) = &self.base {
            return Ok(format!("{base}.{name}"));
        }
        if self.graph.get_metric(name).is_some() {
            return Ok(name.clone());
        }
        Err(SidemanticError::Validation(format!(
            "Column '{name}' must be fully qualified when using FROM metrics"
        )))
    }

    fn add_reference(&mut self, reference: String) -> Result<Expression> {
        if let Some(alias) = self.references.get(&reference) {
            return Ok(Expression::qualified_column(
                "__semantic_query",
                alias.clone(),
            ));
        }
        let metric = if let Some((owner, field)) = reference.split_once('.') {
            let model = self
                .graph
                .get_model(owner)
                .ok_or_else(|| SidemanticError::Validation(format!("Model '{owner}' not found")))?;
            if model.get_metric(field).is_some() || self.graph.get_metric(&reference).is_some() {
                true
            } else if model.get_dimension(split_granularity(field).0).is_some() {
                false
            } else {
                return Err(SidemanticError::Validation(format!(
                    "Field '{reference}' not found"
                )));
            }
        } else if self.graph.get_metric(&reference).is_some() {
            true
        } else {
            return Err(SidemanticError::Validation(format!(
                "Graph metric '{reference}' not found"
            )));
        };
        let alias = format!("__sd_field_{}", self.references.len());
        if metric {
            self.query.metrics.push(reference.clone());
        } else {
            self.query.dimensions.push(reference.clone());
        }
        self.query.aliases.insert(reference.clone(), alias.clone());
        self.references.insert(reference, alias.clone());
        Ok(Expression::qualified_column("__semantic_query", alias))
    }

    fn add_aggregate(&mut self, expression: &Expression) -> Result<Expression> {
        let base = self.base.clone().ok_or_else(|| {
            SidemanticError::Validation(
                "Ad hoc aggregate expressions require a single semantic model in FROM".into(),
            )
        })?;
        let normalized = transform_nodes(expression.clone(), &mut |node| {
            if matches!(node, Expression::Select(_) | Expression::Subquery(_)) {
                self.validate_subquery_correlations(node.clone(), &HashSet::new())?;
                return Ok(Some(node.clone()));
            }
            let Expression::Column(column) = node else {
                return Ok(None);
            };
            let reference = self.resolve(column)?;
            let (owner, field) = reference.split_once('.').ok_or_else(|| {
                SidemanticError::Validation("Ad hoc aggregate needs a model column".into())
            })?;
            if owner != base {
                return Err(SidemanticError::Validation(
                    "Ad hoc aggregate expressions can only reference columns from the base semantic model".into(),
                ));
            }
            // Validate original public/private fields, not the synthetic metric
            // name. The SQL itself keeps physical aggregate-input semantics.
            self.aggregate_inputs.push(reference.clone());
            let mut column = column.clone();
            column.table = None;
            column.name.name = field.to_owned();
            Ok(Some(Expression::Column(column)))
        })?;
        let mut model = self
            .graph
            .get_model(&base)
            .expect("validated base model")
            .clone();
        let name = loop {
            // Double underscores delimit time granularities in field references.
            let name = format!("sd_adhoc_metric_{}", self.next_metric);
            self.next_metric += 1;
            if model.get_metric(&name).is_none() && model.get_dimension(&name).is_none() {
                break name;
            }
        };
        // A complete aggregate belongs in the grouped SELECT, never in the
        // raw-row CTE beside primary keys and other nonaggregated columns.
        let mut metric = Metric::derived(&name, expression_sql(&normalized)?);
        metric.sql_is_complete = true;
        model.metrics.push(metric);
        self.graph.replace_model(model)?;
        self.add_reference(format!("{base}.{name}"))
    }

    fn expression(&mut self, expression: Expression) -> Result<Expression> {
        transform_nodes(expression, &mut |node| {
            if matches!(node, Expression::Select(_) | Expression::Subquery(_)) {
                self.validate_subquery_correlations(node.clone(), &HashSet::new())?;
                return Ok(Some(node.clone()));
            }
            if aggregate_name(node).is_some() {
                return self.add_aggregate(node).map(Some);
            }
            if let Expression::Column(column) = node {
                return self.add_reference(self.resolve(column)?).map(Some);
            }
            Ok(None)
        })
    }

    fn filter(&self, expression: Expression) -> Result<String> {
        let expression = transform_nodes(expression, &mut |node| {
            if matches!(node, Expression::Select(_) | Expression::Subquery(_)) {
                self.validate_subquery_correlations(node.clone(), &HashSet::new())?;
                return Ok(Some(node.clone()));
            }
            let Expression::Column(column) = node else {
                return Ok(None);
            };
            if column.table.is_none() && self.base.is_none() {
                if self.graph.get_metric(&column.name.name).is_some() {
                    return Ok(None);
                }
                return Err(SidemanticError::Validation(format!(
                    "Column '{}' must be fully qualified when using FROM metrics",
                    column.name.name
                )));
            }
            let reference = self.resolve(column)?;
            let (owner, field) = reference.split_once('.').expect("qualified filter");
            if self.graph.get_model(owner).is_none() {
                return Err(SidemanticError::Validation(format!(
                    "Model '{owner}' not found"
                )));
            }
            if self.strict_fields {
                let model = self.graph.get_model(owner).expect("validated filter model");
                if model.get_metric(field).is_none()
                    && model.get_dimension(split_granularity(field).0).is_none()
                {
                    return Err(SidemanticError::Validation(format!(
                        "Field '{reference}' not found"
                    )));
                }
            }
            Ok(Some(Expression::qualified_column(owner, field)))
        })?;
        expression_sql(&expression)
    }

    fn validate_subquery_correlations(
        &self,
        expression: Expression,
        inherited_sources: &HashSet<String>,
    ) -> Result<()> {
        // The wrapper removes semantic source aliases. Reject references that
        // would be left dangling, while allowing local aliases to shadow them.
        let mut root = true;
        let mut local_sources = inherited_sources.clone();
        if let Expression::Select(select) = &expression {
            for source in select
                .from
                .iter()
                .flat_map(|from| &from.expressions)
                .chain(select.joins.iter().map(|join| &join.this))
            {
                let name = match source {
                    Expression::Table(table) => {
                        Some(table.alias.as_ref().unwrap_or(&table.name).name.clone())
                    }
                    Expression::Alias(alias) => Some(alias.alias.name.clone()),
                    Expression::Subquery(query) => {
                        query.alias.as_ref().map(|name| name.name.clone())
                    }
                    _ => None,
                };
                if let Some(name) = name {
                    local_sources.insert(name);
                }
            }
        }
        transform_nodes(expression, &mut |node| {
            if root {
                root = false;
                return Ok(None);
            }
            if matches!(node, Expression::Select(_) | Expression::Subquery(_)) {
                self.validate_subquery_correlations(node.clone(), &local_sources)?;
                return Ok(Some(node.clone()));
            }
            if let Expression::Column(column) = node {
                if let Some(table) = &column.table {
                    if !local_sources.contains(&table.name)
                        && resolve_model_ref(&table.name, &self.sources).is_some()
                    {
                        return Err(SidemanticError::Validation(format!(
                            "Correlated subquery references semantic source '{}'; correlated semantic subqueries are not supported",
                            table.name
                        )));
                    }
                }
            }
            Ok(None)
        })?;
        Ok(())
    }

    fn validate_group_by(
        &self,
        group: &polyglot_sql::expressions::GroupBy,
        projected_references: &HashMap<String, Identifier>,
    ) -> Result<()> {
        let dimensions: HashSet<_> = self.query.dimensions.iter().cloned().collect();
        let mut names: HashMap<String, HashSet<String>> = HashMap::new();
        for reference in &dimensions {
            let (_, field) = reference.split_once('.').expect("dimension reference");
            names
                .entry(field.into())
                .or_default()
                .insert(reference.clone());
            if let Some(alias) = projected_references.get(reference) {
                names
                    .entry(alias.name.clone())
                    .or_default()
                    .insert(reference.clone());
            }
        }
        let mut grouped = HashSet::new();
        if group.all.is_some() || group.totals {
            return Err(SidemanticError::Validation(
                "GROUP BY modifiers are not supported for semantic queries".into(),
            ));
        }
        for expression in &group.expressions {
            let Expression::Column(column) = expression else {
                return Err(SidemanticError::Validation("GROUP BY is only supported when it repeats selected semantic dimensions exactly".into()));
            };
            let reference = if column.table.is_some() {
                self.resolve(column)?
            } else {
                let matches = names.get(&column.name.name);
                if matches.is_some_and(|matches| matches.len() > 1) {
                    return Err(SidemanticError::Validation(format!(
                        "GROUP BY field '{}' is ambiguous; use a qualified semantic field",
                        column.name.name
                    )));
                }
                matches
                    .and_then(|matches| matches.iter().next())
                    .cloned()
                    .unwrap_or_default()
            };
            if !dimensions.contains(&reference) {
                return Err(SidemanticError::Validation("GROUP BY is only supported when it repeats selected semantic dimensions exactly".into()));
            }
            grouped.insert(reference);
        }
        if grouped != dimensions {
            return Err(SidemanticError::Validation(
                "GROUP BY must include exactly the selected semantic dimensions".into(),
            ));
        }
        Ok(())
    }
}

impl QueryRewriter<'_> {
    pub(super) fn compile_semantic_select(&self, mut select: Select) -> Result<Select> {
        let mut remainder = select.clone();
        remainder.expressions.clear();
        remainder.from = None;
        remainder.joins.clear();
        remainder.where_clause = None;
        remainder.having = None;
        remainder.group_by = None;
        remainder.order_by = None;
        remainder.limit = None;
        remainder.offset = None;
        remainder.distinct = false;
        remainder.leading_comments.clear();
        remainder.post_select_comments.clear();
        if remainder != Select::new() {
            return Err(SidemanticError::UnsupportedSemanticFeatures {
                capabilities: vec!["rewrite.scoped_select_shape".into()],
            });
        }
        let from = select.from.as_ref().ok_or_else(|| {
            SidemanticError::Validation("Semantic query requires a FROM clause".into())
        })?;
        if from.expressions.len() != 1 {
            return Err(SidemanticError::Validation(
                "Semantic query requires one base model".into(),
            ));
        }
        let (source, alias) = table_name_and_alias(&from.expressions[0]).ok_or_else(|| {
            SidemanticError::Validation("Semantic query requires a model table".into())
        })?;
        let base = (!source.eq_ignore_ascii_case("metrics")).then_some(source.clone());
        let mut sources = base.as_ref().map_or_else(Vec::new, |base| {
            vec![(base.clone(), alias.unwrap_or_else(|| base.clone()))]
        });
        let join_filters = self.bind_explicit_joins(&select.joins, &mut sources)?;
        let mut bindings = Bindings {
            graph: self.graph.clone(),
            query: SemanticQuery::new(),
            sources,
            base,
            references: HashMap::new(),
            aggregate_inputs: Vec::new(),
            next_metric: 0,
            strict_fields: self.security_controls,
        };
        let mut projections = Vec::new();
        let mut aliases = HashSet::new();
        let mut projected_references = HashMap::new();
        for projection in select.expressions {
            if matches!(projection, Expression::Star(_)) {
                let Some(base) = &bindings.base else {
                    return Err(SidemanticError::Validation(
                        "SELECT * is not supported with FROM metrics".into(),
                    ));
                };
                if bindings.sources.len() != 1 {
                    return Err(SidemanticError::Validation(
                        "SELECT * requires a FROM clause with a single table".into(),
                    ));
                }
                let model = bindings.graph.get_model(base).expect("semantic source");
                let fields: Vec<_> = model
                    .dimensions
                    .iter()
                    .map(|field| &field.name)
                    .chain(model.metrics.iter().map(|field| &field.name))
                    .map(|name| (format!("{base}.{name}"), name.clone()))
                    .collect();
                for (reference, name) in fields {
                    projections.push(bindings.add_reference(reference)?.alias(name.clone()));
                    aliases.insert(name);
                }
                continue;
            }
            let (expression, alias) = match projection {
                Expression::Alias(alias) => (alias.this, Some(alias.alias)),
                Expression::Column(ref column) => (projection.clone(), Some(column.name.clone())),
                expression => {
                    let alias = aggregate_name(&expression).map(Identifier::new);
                    (expression, alias)
                }
            };
            if let (Expression::Column(column), Some(alias)) = (&expression, &alias) {
                projected_references.insert(bindings.resolve(column)?, alias.clone());
            }
            let mut expression = bindings.expression(expression)?;
            if let Some(alias) = alias {
                aliases.insert(alias.name.clone());
                expression = expression.alias(alias.name.clone());
                if let Expression::Alias(node) = &mut expression {
                    node.alias = alias;
                }
            }
            projections.push(expression);
        }
        if let Some(group) = &select.group_by {
            bindings.validate_group_by(group, &projected_references)?;
        }
        if let Some(order) = &mut select.order_by {
            for item in &mut order.expressions {
                if matches!(&item.this, Expression::Column(column)
                    if column.table.is_none() && aliases.contains(&column.name.name))
                {
                    continue;
                }
                if let Expression::Column(column) = &item.this {
                    if let Some(alias) = projected_references.get(&bindings.resolve(column)?) {
                        let mut output = column.clone();
                        output.table = None;
                        output.name = alias.clone();
                        item.this = Expression::Column(output);
                        continue;
                    }
                }
                item.this = bindings.expression(item.this.clone())?;
            }
        }
        if let Some(filter) = select.where_clause {
            let filter = bindings.filter(filter.this)?;
            bindings.query.filters.push(filter);
        }
        if let Some(filter) = select.having {
            let filter = bindings.filter(filter.this)?;
            bindings.query.filters.push(filter);
        }
        bindings.query.filters.extend(join_filters);
        if bindings.query.metrics.is_empty() && bindings.query.dimensions.is_empty() {
            return Err(SidemanticError::Validation(
                "Query must select at least one metric or dimension".into(),
            ));
        }
        if let Some(prepare) = self.query_preparer {
            // Policy preparation also checks ad hoc aggregate input visibility;
            // these references must not change grouping or output columns.
            bindings.query.order_by = bindings.aggregate_inputs;
            prepare(&bindings.graph, &mut bindings.query)?;
            bindings.query.order_by.clear();
        }
        let sql = SqlGenerator::new(&bindings.graph).generate(&bindings.query)?;
        let mut wrapper = parse_sql_with_dialect(
            &format!("SELECT * FROM ({sql}) AS __semantic_query"),
            DialectType::DuckDB,
        )?;
        let Expression::Select(mut outer) = wrapper.remove(0) else {
            return Err(SidemanticError::SqlGeneration(
                "Expected semantic query wrapper".into(),
            ));
        };
        outer.expressions = projections;
        outer.order_by = select.order_by;
        outer.limit = select.limit;
        outer.offset = select.offset;
        outer.distinct = select.distinct;
        Ok(*outer)
    }

    fn bind_explicit_joins(
        &self,
        joins: &[Join],
        sources: &mut Vec<(String, String)>,
    ) -> Result<Vec<String>> {
        let mut filters = Vec::new();
        for join in joins {
            if !matches!(join.kind, JoinKind::Inner | JoinKind::Left) {
                return Err(SidemanticError::Validation(
                    "Explicit semantic JOINs support INNER and LEFT joins only".into(),
                ));
            }
            let (target, alias) = table_name_and_alias(&join.this).ok_or_else(|| {
                SidemanticError::Validation(
                    "Explicit JOINs from semantic models only support direct model tables".into(),
                )
            })?;
            let model = self.graph.get_model(&target).ok_or_else(|| {
                SidemanticError::Validation(
                    "Explicit JOINs from semantic models only support modeled semantic tables"
                        .into(),
                )
            })?;
            let alias = alias.unwrap_or_else(|| target.clone());
            let mut all_sources = sources.clone();
            all_sources.push((target.clone(), alias.clone()));
            if let Some(on) = &join.on {
                let pairs = equality_pairs(on).ok_or_else(|| relationship_error(&target))?;
                let mut actual = HashSet::new();
                let mut other_model = None;
                for (left, right) in pairs {
                    let (left_model, _) = left
                        .table
                        .as_ref()
                        .and_then(|table| resolve_model_ref(&table.name, &all_sources))
                        .ok_or_else(|| relationship_error(&target))?;
                    let (right_model, _) = right
                        .table
                        .as_ref()
                        .and_then(|table| resolve_model_ref(&table.name, &all_sources))
                        .ok_or_else(|| relationship_error(&target))?;
                    let (other, from, to) = if right_model == target && left_model != target {
                        (left_model, &left.name.name, &right.name.name)
                    } else if left_model == target && right_model != target {
                        (right_model, &right.name.name, &left.name.name)
                    } else {
                        return Err(relationship_error(&target));
                    };
                    if !sources.iter().any(|(model, _)| model == other)
                        || other_model.is_some_and(|previous| previous != other)
                    {
                        return Err(relationship_error(&target));
                    }
                    other_model = Some(other);
                    actual.insert((from.clone(), to.clone()));
                }
                let path = self
                    .graph
                    .find_join_path(
                        other_model.ok_or_else(|| relationship_error(&target))?,
                        &target,
                    )
                    .map_err(|_| relationship_error(&target))?;
                if path.steps.len() != 1 {
                    return Err(relationship_error(&target));
                }
                let step = &path.steps[0];
                let expected: HashSet<_> = step
                    .from_keys
                    .iter()
                    .cloned()
                    .zip(step.to_keys.iter().cloned())
                    .collect();
                if actual != expected {
                    return Err(relationship_error(&target));
                }
            } else {
                let base = sources.first().ok_or_else(|| relationship_error(&target))?;
                self.graph
                    .find_join_path(&base.0, &target)
                    .map_err(|_| relationship_error(&target))?;
            }
            if join.kind == JoinKind::Inner {
                for key in model.primary_keys() {
                    let column = expression_sql(&Expression::qualified_column(&target, key))?;
                    filters.push(format!("{column} IS NOT NULL"));
                }
            }
            sources.push((target, alias));
        }
        Ok(filters)
    }
}

fn relationship_error(target: &str) -> SidemanticError {
    SidemanticError::Validation(format!(
        "Explicit JOIN to '{target}' does not match a declared relationship"
    ))
}

fn equality_pairs(expression: &Expression) -> Option<Vec<(&Column, &Column)>> {
    match expression {
        Expression::Paren(paren) => equality_pairs(&paren.this),
        Expression::And(binary) => {
            let mut pairs = equality_pairs(&binary.left)?;
            pairs.extend(equality_pairs(&binary.right)?);
            Some(pairs)
        }
        Expression::Eq(binary) => match (&binary.left, &binary.right) {
            (Expression::Column(left), Expression::Column(right)) => Some(vec![(left, right)]),
            _ => None,
        },
        _ => None,
    }
}
