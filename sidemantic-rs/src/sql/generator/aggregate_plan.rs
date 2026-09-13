//! Scoped calculations over independently aggregated source populations.
//!
//! This path never gives a graph calculation a synthetic model owner. Only
//! aggregate leaves enter child queries; scalar calculations bind their outputs.

use super::*;
use crate::core::replace_semantic_columns;

#[derive(Clone)]
struct ResolvedMetric {
    reference: String,
    context: Option<String>,
    metric: Metric,
}

#[derive(Clone)]
struct Leaf {
    reference: String,
    model: String,
    metric: Metric,
    alias: String,
}

struct Plan<'a, 'g> {
    generator: &'a SqlGenerator<'g>,
    leaves: Vec<Leaf>,
    models: Vec<String>,
    expressions: HashMap<String, String>,
    active: HashSet<String>,
    cross_source_calculation: bool,
}

fn unsupported(capability: &str) -> SidemanticError {
    SidemanticError::UnsupportedSemanticFeatures {
        capabilities: vec![format!("aggregation.{capability}")],
    }
}

impl<'a, 'g> Plan<'a, 'g> {
    fn resolve(&self, reference: &str, context: Option<&str>) -> Result<Option<ResolvedMetric>> {
        let graph = self.generator.graph;
        if let Some((model_name, name)) = reference.split_once('.') {
            return Ok(graph.get_model(model_name).and_then(|model| {
                model.get_metric(name).map(|metric| ResolvedMetric {
                    reference: reference.to_string(),
                    context: Some(model_name.to_string()),
                    metric: metric.clone(),
                })
            }));
        }
        if let Some(model_name) = context {
            if let Some(metric) = graph
                .get_model(model_name)
                .and_then(|m| m.get_metric(reference))
            {
                return Ok(Some(ResolvedMetric {
                    reference: format!("{model_name}.{reference}"),
                    context: Some(model_name.to_string()),
                    metric: metric.clone(),
                }));
            }
        }
        if let Some(metric) = graph.get_metric(reference) {
            return Ok(Some(ResolvedMetric {
                reference: reference.to_string(),
                context: graph.metric_owner(reference).map(str::to_string),
                metric: metric.clone(),
            }));
        }
        let owners: Vec<_> = graph
            .models()
            .filter(|m| m.get_metric(reference).is_some())
            .collect();
        if owners.len() > 1 {
            return Err(SidemanticError::AmbiguousReference {
                field: reference.to_string(),
                models: owners
                    .iter()
                    .map(|m| m.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
            });
        }
        Ok(owners.first().map(|model| ResolvedMetric {
            reference: format!("{}.{}", model.name, reference),
            context: Some(model.name.clone()),
            metric: model.get_metric(reference).unwrap().clone(),
        }))
    }

    fn expand(&mut self, reference: &str, context: Option<&str>) -> Result<String> {
        let resolved = self.resolve(reference, context)?.ok_or_else(|| {
            SidemanticError::Validation(format!("Metric not found: '{reference}'"))
        })?;
        if let Some(expression) = self.expressions.get(&resolved.reference) {
            return Ok(expression.clone());
        }
        if !self.active.insert(resolved.reference.clone()) {
            return Err(SidemanticError::CircularDependency(resolved.reference));
        }
        let metric = &resolved.metric;
        if metric.sql_is_complete
            || metric.non_additive_dimension.is_some()
            || metric.offset_window.is_some()
        {
            return Err(unsupported("calculation_shape"));
        }
        if metric.r#type != MetricType::Simple && !metric.filters.is_empty() {
            return Err(unsupported("calculation_filters"));
        }
        let expression = match metric.r#type {
            MetricType::Simple => {
                let model = resolved
                    .context
                    .clone()
                    .ok_or_else(|| unsupported("unscoped_leaf"))?;
                if metric.agg.is_none() || metric.agg == Some(Aggregation::Expression) {
                    return Err(unsupported("inline_aggregate"));
                }
                // A leaf is source-local. Qualified raw inputs from another
                // source need a separate row-grain plan, not this aggregate split.
                if let Some(sql) = &metric.sql {
                    if sql != "*"
                        && semantic_column_references(sql)?.iter().any(|column| {
                            column.model.as_deref().is_some_and(|owner| owner != model)
                        })
                    {
                        return Err(unsupported("cross_source_raw_input"));
                    }
                }
                if !self.models.contains(&model) {
                    self.models.push(model.clone());
                }
                let alias = format!("__sidemantic_metric_{}", self.leaves.len());
                self.leaves.push(Leaf {
                    reference: resolved.reference.clone(),
                    model: model.clone(),
                    metric: metric.clone(),
                    alias: alias.clone(),
                });
                format!(
                    "{}.{}",
                    self.generator.quote_identifier(&format!("{model}_preagg")),
                    self.generator.quote_identifier(&alias)
                )
            }
            MetricType::Ratio => {
                let numerator = metric.numerator.as_deref().ok_or_else(|| {
                    SidemanticError::Validation(format!("Ratio {} requires numerator", metric.name))
                })?;
                let denominator = metric.denominator.as_deref().ok_or_else(|| {
                    SidemanticError::Validation(format!(
                        "Ratio {} requires denominator",
                        metric.name
                    ))
                })?;
                let numerator = self.expand(numerator, resolved.context.as_deref())?;
                let denominator = self.expand(denominator, resolved.context.as_deref())?;
                format!("({numerator}) / NULLIF(({denominator}), 0)")
            }
            MetricType::Derived => {
                if !metric.filters.is_empty() {
                    return Err(unsupported("calculation_filters"));
                }
                let sql = metric.sql.as_deref().ok_or_else(|| {
                    SidemanticError::Validation(format!(
                        "Derived metric {} requires sql",
                        metric.name
                    ))
                })?;
                let mut replacements = HashMap::new();
                for column in semantic_column_references(sql)? {
                    if column.aggregate_input {
                        return Err(unsupported("inline_aggregate"));
                    }
                    let expanded = self.expand(&column.name(), resolved.context.as_deref())?;
                    replacements.insert((column.model, column.field), format!("({expanded})"));
                }
                let expression =
                    replace_semantic_columns(parse_semantic_expression(sql)?, &replacements)?;
                self.generator.emit_expression(&expression)?
            }
            _ => return Err(unsupported("calculation_shape")),
        };
        let expression = self.generator.fill_metric_expression(metric, expression)?;
        self.active.remove(&resolved.reference);
        if metric.r#type != MetricType::Simple
            && resolved
                .context
                .as_ref()
                .is_some_and(|owner| self.models.iter().any(|model| model != owner))
        {
            self.cross_source_calculation = true;
        }
        self.expressions
            .insert(resolved.reference, expression.clone());
        Ok(expression)
    }
}

fn conjuncts(expression: Expression, output: &mut Vec<Expression>) {
    match expression {
        Expression::And(binary) => {
            conjuncts(binary.left, output);
            conjuncts(binary.right, output);
        }
        Expression::Paren(paren) => conjuncts(paren.this, output),
        other => output.push(other),
    }
}

fn dimension_alias(index: usize) -> String {
    format!("__sidemantic_dimension_{index}")
}

/// Normalize the ordinary compiler's public outputs at the child boundary.
/// Each child has its own collision set because it selects different measures.
fn child_projection(
    generator: &SqlGenerator<'_>,
    dimensions: &[DimensionRef],
    leaves: &[&Leaf],
) -> Result<Vec<String>> {
    let references: Vec<_> = leaves.iter().map(|leaf| leaf.reference.clone()).collect();
    let metrics = generator.parse_metric_refs(&references)?;
    let mut collisions = HashMap::new();
    for alias in dimensions
        .iter()
        .map(|dimension| &dimension.alias)
        .chain(metrics.iter().map(|metric| &metric.alias))
    {
        *collisions.entry(alias.clone()).or_insert(0) += 1;
    }
    let mut names = HashSet::new();
    let mut projection = Vec::new();
    for (index, dimension) in dimensions.iter().enumerate() {
        let source = generator.output_alias(&dimension.model, &dimension.alias, &collisions);
        if !names.insert(source.clone()) {
            return Err(unsupported("child_output_alias_collision"));
        }
        projection.push(format!(
            "__sidemantic_source.{} AS {}",
            generator.quote_identifier(&source),
            dimension_alias(index)
        ));
    }
    for (metric, leaf) in metrics.iter().zip(leaves) {
        let source = generator.output_alias(&metric.model, &metric.alias, &collisions);
        if !names.insert(source.clone()) {
            return Err(unsupported("child_output_alias_collision"));
        }
        projection.push(format!(
            "__sidemantic_source.{} AS {}",
            generator.quote_identifier(&source),
            leaf.alias
        ));
    }
    Ok(projection)
}

/// Return None for the existing single-source/special-metric paths. Only this
/// module's fully planned multi-source calculations bypass graph-owner parsing.
pub(super) fn try_generate(
    generator: &SqlGenerator<'_>,
    query: &SemanticQuery,
) -> Result<Option<String>> {
    if !generator.graph.has_strict_metric_scope() || query.metrics.is_empty() {
        return Ok(None);
    }
    let mut plan = Plan {
        generator,
        leaves: Vec::new(),
        models: Vec::new(),
        expressions: HashMap::new(),
        active: HashSet::new(),
        cross_source_calculation: false,
    };
    let mut outputs = Vec::new();
    for reference in &query.metrics {
        let expression = match plan.expand(reference, None) {
            Ok(expression) => expression,
            // Existing routes own special shapes; their scope gate stays intact.
            Err(SidemanticError::UnsupportedSemanticFeatures { .. }) => return Ok(None),
            Err(error) => return Err(error),
        };
        let metric = plan.resolve(reference, None)?.unwrap();
        outputs.push((reference.clone(), metric, expression));
    }
    let all_filters: Vec<_> = query
        .filters
        .iter()
        .cloned()
        .chain(generator.resolve_segments(&query.segments)?)
        .collect();
    let mut filters = Vec::new();
    for filter in &all_filters {
        conjuncts(parse_semantic_expression(filter)?, &mut filters);
        for column in semantic_column_references(filter)? {
            if plan.resolve(&column.name(), None)?.is_some() {
                plan.expand(&column.name(), None)?;
            }
        }
    }
    if plan.models.len() < 2 && !plan.cross_source_calculation {
        return Ok(None);
    }
    if query.ungrouped || !query.table_calculations.is_empty() {
        return Err(unsupported("cross_grain_query_shape"));
    }
    if !query.skip_default_time_dimensions
        && plan.models.iter().any(|model| {
            generator
                .graph
                .get_model(model)
                .is_some_and(|model| model.default_time_dimension.is_some())
        })
    {
        return Err(unsupported("cross_grain_default_time_dimension"));
    }
    // Independent populations still require a declared, supported model graph.
    for model in plan.models.iter().skip(1) {
        generator.graph.find_join_path(&plan.models[0], model)?;
    }
    let dimensions = generator.parse_dimension_refs(&query.dimensions)?;
    for dimension in &dimensions {
        if generator
            .graph
            .get_model(&dimension.model)
            .and_then(|model| model.get_dimension(&dimension.name))
            .is_some_and(|dimension| dimension.window.is_some())
        {
            return Err(unsupported("window_dimension"));
        }
    }
    let dimension_expressions: HashMap<_, _> = query
        .dimensions
        .iter()
        .zip(&dimensions)
        .enumerate()
        .map(|(index, (reference, _))| {
            let columns = plan
                .models
                .iter()
                .map(|model| {
                    format!(
                        "{}.{}",
                        generator.quote_identifier(&format!("{model}_preagg")),
                        dimension_alias(index)
                    )
                })
                .collect::<Vec<_>>();
            let expression = if columns.len() == 1 {
                columns[0].clone()
            } else {
                format!("COALESCE({})", columns.join(", "))
            };
            (reference.clone(), expression)
        })
        .collect();
    let mut row_filters = Vec::new();
    let mut aggregate_filters = Vec::new();
    for filter in filters {
        let sql = generator.emit_expression(&filter)?;
        let columns = semantic_column_references(&sql)?;
        let mut replacements = HashMap::new();
        let mut has_metric = false;
        let mut has_raw = false;
        for column in columns {
            let reference = column.name();
            if let Some(metric) = plan.resolve(&reference, None)? {
                has_metric = true;
                let expression = plan.expressions.get(&metric.reference).unwrap();
                replacements.insert((column.model, column.field), format!("({expression})"));
            } else if let Some(expression) = dimension_expressions.get(&reference) {
                replacements.insert((column.model, column.field), format!("({expression})"));
            } else {
                has_raw = true;
            }
        }
        if has_metric && has_raw {
            return Err(unsupported("mixed_row_aggregate_filter"));
        }
        if has_metric {
            aggregate_filters.push(
                generator.emit_expression(&replace_semantic_columns(filter, &replacements)?)?,
            );
        } else {
            row_filters.push(format!("({sql})"));
        }
    }
    let mut ctes = Vec::new();
    for model in &plan.models {
        let leaves: Vec<_> = plan
            .leaves
            .iter()
            .filter(|leaf| &leaf.model == model)
            .collect();
        let projection = child_projection(generator, &dimensions, &leaves)?;
        // Reuse the ordinary source-local compiler only where its fanout contract
        // is proven: single declared keys and aggregates with symmetric support.
        let mut required = HashSet::from([model.clone()]);
        required.extend(dimensions.iter().map(|dimension| dimension.model.clone()));
        required.extend(query.prepared_policies.model_names().cloned());
        required.extend(generator.find_filter_models(&row_filters));
        let paths = generator.build_join_paths(model, &required)?;
        if generator.detect_fan_out_risk(model, &paths).contains(model) {
            let source = generator.graph.get_model(model).unwrap();
            if source.primary_keys().is_empty() {
                return Err(SidemanticError::Validation(format!(
                    "Model '{model}' has no primary key; cannot safely aggregate across a fanout join"
                )));
            }
            if source.primary_keys().len() != 1 {
                return Err(unsupported("requires_single_primary_key"));
            }
            if leaves.iter().any(|leaf| {
                !matches!(
                    leaf.metric.agg,
                    Some(
                        Aggregation::Sum
                            | Aggregation::Count
                            | Aggregation::CountDistinct
                            | Aggregation::Avg
                            | Aggregation::Min
                            | Aggregation::Max
                    )
                )
            }) {
                return Err(unsupported("fanout_aggregate_kind"));
            }
        }
        let mut child = query.clone();
        child.metrics = leaves.iter().map(|leaf| leaf.reference.clone()).collect();
        child.filters = row_filters.clone();
        child.segments.clear();
        child.order_by.clear();
        child.limit = None;
        child.offset = None;
        child.skip_default_time_dimensions = true;
        // Materialized routing is qualified for whole single-source queries.
        // Cross-source child populations need separate grain/domain acceptance.
        child.use_preaggregations = false;
        let child_sql = generator.generate_from_model(&child, Some(model))?;
        ctes.push(format!(
            "{} AS (\nSELECT {}\nFROM (\n{child_sql}\n) AS __sidemantic_source\n)",
            generator.quote_identifier(&format!("{model}_preagg")),
            projection.join(", ")
        ));
    }
    let mut names = HashMap::new();
    for dimension in &dimensions {
        *names.entry(dimension.alias.clone()).or_insert(0usize) += 1;
    }
    for (_, resolved, _) in &outputs {
        *names.entry(resolved.metric.name.clone()).or_insert(0usize) += 1;
    }
    let mut public_names = HashSet::new();
    let mut order_names = HashMap::new();
    let mut selections = Vec::new();
    for (reference, dimension) in query.dimensions.iter().zip(&dimensions) {
        let alias = generator.output_alias(&dimension.model, &dimension.alias, &names);
        if !public_names.insert(alias.clone()) {
            return Err(unsupported("ambiguous_output_alias"));
        }
        order_names.insert(reference.clone(), alias.clone());
        if names[&dimension.alias] == 1 {
            order_names.insert(dimension.alias.clone(), alias.clone());
        }
        selections.push(format!(
            "{} AS {}",
            dimension_expressions[reference],
            generator.quote_identifier(&alias)
        ));
    }
    for (reference, resolved, expression) in outputs {
        let name = &resolved.metric.name;
        let alias = if names[name] > 1 {
            let owner = resolved
                .context
                .as_deref()
                .ok_or_else(|| unsupported("ambiguous_output_alias"))?;
            generator.output_alias(owner, name, &names)
        } else {
            name.clone()
        };
        if !public_names.insert(alias.clone()) {
            return Err(unsupported("ambiguous_output_alias"));
        }
        order_names.insert(reference, alias.clone());
        if names[name] == 1 {
            order_names.insert(name.clone(), alias.clone());
        }
        selections.push(format!(
            "{expression} AS {}",
            generator.quote_identifier(&alias)
        ));
    }
    let mut sql = format!(
        "WITH {}\nSELECT {}\nFROM {}",
        ctes.join(",\n"),
        selections.join(",\n"),
        generator.quote_identifier(&format!("{}_preagg", plan.models[0]))
    );
    for (index, model) in plan.models.iter().enumerate().skip(1) {
        let table = generator.quote_identifier(&format!("{model}_preagg"));
        if dimensions.is_empty() {
            sql.push_str(&format!("\nCROSS JOIN {table}"));
        } else {
            let conditions = dimensions
                .iter()
                .enumerate()
                .map(|(dimension_index, _)| {
                    let name = dimension_alias(dimension_index);
                    let previous: Vec<_> = plan.models[..index]
                        .iter()
                        .map(|model| {
                            format!(
                                "{}.{name}",
                                generator.quote_identifier(&format!("{model}_preagg"))
                            )
                        })
                        .collect();
                    let previous = if previous.len() == 1 {
                        previous[0].clone()
                    } else {
                        format!("COALESCE({})", previous.join(", "))
                    };
                    format!("{previous} IS NOT DISTINCT FROM {table}.{name}")
                })
                .collect::<Vec<_>>();
            sql.push_str(&format!(
                "\nFULL OUTER JOIN {table} ON {}",
                conditions.join(" AND ")
            ));
        }
    }
    if !aggregate_filters.is_empty() {
        sql.push_str(&format!(
            "\nWHERE {}",
            aggregate_filters
                .iter()
                .map(|filter| format!("({filter})"))
                .collect::<Vec<_>>()
                .join(" AND ")
        ));
    }
    if !query.order_by.is_empty() {
        let mut order = Vec::new();
        for item in &query.order_by {
            let (reference, direction) = item
                .rsplit_once(' ')
                .filter(|(_, direction)| {
                    direction.eq_ignore_ascii_case("asc") || direction.eq_ignore_ascii_case("desc")
                })
                .map_or((item.as_str(), ""), |(reference, direction)| {
                    (reference, direction)
                });
            let alias = order_names
                .get(reference)
                .ok_or_else(|| unsupported("unprojected_order_by"))?;
            order.push(format!("{} {direction}", generator.quote_identifier(alias)));
        }
        sql.push_str(&format!("\nORDER BY {}", order.join(", ")));
    }
    if let Some(limit) = query.limit {
        sql.push_str(&format!("\nLIMIT {limit}"));
    }
    if let Some(offset) = query.offset {
        sql.push_str(&format!("\nOFFSET {offset}"));
    }
    Ok(Some(sql))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Dimension, Relationship};

    fn try_generate(generator: &SqlGenerator<'_>, query: &SemanticQuery) -> Result<Option<String>> {
        // Match the public handoff's stack for the large typed SQL AST.
        crate::semantic_input::with_semantic_stack(|| super::try_generate(generator, query))
    }

    fn assert_valid_sql(sql: &str) {
        crate::semantic_input::with_semantic_stack(|| {
            polyglot_sql::parse_one(sql, DialectType::DuckDB)
                .map(|_| ())
                .map_err(|error| SidemanticError::SqlParse(error.to_string()))
        })
        .unwrap();
    }

    fn graph() -> SemanticGraph {
        let mut graph = SemanticGraph::new();
        let mut customers = Relationship::many_to_one("customers");
        customers.foreign_key = Some("customer_id".into());
        graph
            .add_model(
                Model::new("orders", "id")
                    .with_table("orders")
                    .with_dimension(Dimension::categorical("region"))
                    .with_dimension(Dimension::categorical("revenue").with_sql("region"))
                    .with_metric(Metric::sum("revenue", "amount"))
                    .with_metric(Metric::derived("proxy", "customers.customer_count"))
                    .with_metric(Metric::derived(
                        "local_sum",
                        "revenue + customers.customer_count",
                    ))
                    .with_relationship(customers),
            )
            .unwrap();
        graph
            .add_model(
                Model::new("customers", "id")
                    .with_table("customers")
                    .with_dimension(Dimension::categorical("region"))
                    .with_dimension(Dimension::categorical("revenue").with_sql("region"))
                    .with_dimension(Dimension::categorical("orders_region").with_sql("region"))
                    .with_metric(Metric::count("customer_count")),
            )
            .unwrap();
        for metric in [
            Metric::ratio("ratio", "orders.revenue", "customers.customer_count"),
            Metric::ratio("proxy_ratio", "orders.revenue", "orders.proxy"),
            Metric::derived("added", "orders.revenue + customers.customer_count"),
            Metric::derived("double_added", "added * 2"),
        ] {
            graph.add_metric_unvalidated(metric).unwrap();
        }
        graph.set_metric_scopes(HashMap::new()).unwrap();
        graph
    }

    fn compile(graph: &SemanticGraph, metrics: &[&str], filters: &[&str]) -> Result<String> {
        let query = SemanticQuery::new()
            .with_metrics(metrics.iter().map(|s| s.to_string()).collect())
            .with_filters(filters.iter().map(|s| s.to_string()).collect());
        try_generate(&SqlGenerator::new(graph), &query)?
            .ok_or_else(|| unsupported("test_not_planned"))
    }

    #[test]
    fn graph_ratio_has_independent_source_aggregates() {
        let sql = compile(&graph(), &["ratio"], &[]).unwrap();
        assert!(sql.contains("orders_preagg AS"));
        assert!(sql.contains("customers_preagg AS"));
        assert!(sql.contains("CROSS JOIN customers_preagg"));
        assert!(sql.contains("NULLIF((customers_preagg.__sidemantic_metric_1), 0)"));
        assert!(!sql.contains("LEFT JOIN customers_cte"));
        assert_valid_sql(&sql);
    }

    #[test]
    fn proxies_and_local_names_expand_to_aggregate_owners() {
        let graph = graph();
        for metric in ["proxy_ratio", "orders.local_sum"] {
            let sql = compile(&graph, &[metric], &[]).unwrap();
            assert!(sql.contains("customers_preagg.__sidemantic_metric_"));
            assert!(!sql.contains("proxy_raw"));
            assert!(!sql.contains("local_sum_raw"));
        }
    }

    #[test]
    fn nested_arithmetic_keeps_parentheses() {
        let sql = compile(&graph(), &["double_added"], &[]).unwrap();
        assert!(sql.contains("((orders_preagg.__sidemantic_metric_"));
        assert!(sql.contains(") + (customers_preagg.__sidemantic_metric_"));
        assert!(sql.contains(")) * 2"));
    }

    #[test]
    fn filters_project_unselected_dependencies_and_bind_outer_columns() {
        let sql = compile(
            &graph(),
            &["double_added"],
            &["ratio > 90 AND orders.proxy > 2"],
        )
        .unwrap();
        assert!(sql.contains("WHERE"));
        assert!(!sql.contains("WHERE ratio"));
        assert!(!sql.contains("proxy_raw"));
        assert!(sql.contains("customers_preagg.__sidemantic_metric_"));
    }

    #[test]
    fn mixed_raw_and_aggregate_or_is_typed_unsupported() {
        assert!(
            matches!(compile(&graph(), &["ratio"], &["ratio > 90 OR orders.amount > 60"]),
            Err(SidemanticError::UnsupportedSemanticFeatures { capabilities })
                if capabilities == vec!["aggregation.mixed_row_aggregate_filter"])
        );
    }

    #[test]
    fn grouped_filters_and_order_use_outer_dimension() {
        let graph = graph();
        let query = SemanticQuery::new()
            .with_metrics(vec!["ratio".into()])
            .with_dimensions(vec!["customers.region".into()])
            .with_filters(vec!["ratio > 90 OR customers.region = 'west'".into()])
            .with_order_by(vec!["customers.region DESC".into()]);
        let sql = try_generate(&SqlGenerator::new(&graph), &query)
            .unwrap()
            .unwrap();
        assert!(sql.contains("IS NOT DISTINCT FROM"));
        assert!(sql.contains("COALESCE(orders_preagg.__sidemantic_dimension_0, customers_preagg.__sidemantic_dimension_0)"));
        assert!(sql.contains("ORDER BY region DESC"));
    }

    #[test]
    fn single_source_stays_on_existing_path() {
        let graph = graph();
        let query = SemanticQuery::new().with_metrics(vec!["orders.revenue".into()]);
        assert!(try_generate(&SqlGenerator::new(&graph), &query)
            .unwrap()
            .is_none());
    }

    #[test]
    fn ungrouped_cross_grain_query_is_rejected() {
        let graph = graph();
        let query = SemanticQuery::new()
            .with_metrics(vec!["ratio".into()])
            .with_ungrouped(true);
        assert!(matches!(
            try_generate(&SqlGenerator::new(&graph), &query),
            Err(SidemanticError::UnsupportedSemanticFeatures { .. })
        ));
    }

    #[test]
    fn dimension_leaf_alias_collision_is_normalized_per_child() {
        let graph = graph();
        let query = SemanticQuery::new()
            .with_metrics(vec!["ratio".into()])
            .with_dimensions(vec!["customers.revenue".into()]);
        let sql = try_generate(&SqlGenerator::new(&graph), &query)
            .unwrap()
            .unwrap();
        assert!(
            sql.contains("__sidemantic_source.customers_revenue AS __sidemantic_dimension_0"),
            "{sql}"
        );
        assert!(
            sql.contains("__sidemantic_source.orders_revenue AS __sidemantic_metric_0"),
            "{sql}"
        );
        assert!(
            sql.contains("__sidemantic_source.revenue AS __sidemantic_dimension_0"),
            "{sql}"
        );
        assert!(sql.contains(") AS revenue"), "{sql}");
        assert_valid_sql(&sql);
    }

    #[test]
    fn duplicate_dimension_names_have_distinct_grouping_and_order_names() {
        let graph = graph();
        let query = SemanticQuery::new()
            .with_metrics(vec!["ratio".into()])
            .with_dimensions(vec!["orders.region".into(), "customers.region".into()])
            .with_filters(vec!["ratio > 1 OR customers.region IS NULL".into()])
            .with_order_by(vec![
                "orders.region ASC".into(),
                "customers.region DESC".into(),
            ]);
        let sql = try_generate(&SqlGenerator::new(&graph), &query)
            .unwrap()
            .unwrap();
        assert!(
            sql.contains("__sidemantic_source.orders_region AS __sidemantic_dimension_0"),
            "{sql}"
        );
        assert!(
            sql.contains("__sidemantic_source.customers_region AS __sidemantic_dimension_1"),
            "{sql}"
        );
        assert!(sql.contains(") AS orders_region"), "{sql}");
        assert!(sql.contains(") AS customers_region"), "{sql}");
        assert!(sql.contains("__sidemantic_dimension_0 IS NOT DISTINCT FROM customers_preagg.__sidemantic_dimension_0"), "{sql}");
        assert!(sql.contains("__sidemantic_dimension_1 IS NOT DISTINCT FROM customers_preagg.__sidemantic_dimension_1"), "{sql}");
        assert!(
            sql.contains("ORDER BY orders_region ASC, customers_region DESC"),
            "{sql}"
        );
        assert_valid_sql(&sql);
    }

    #[test]
    fn selected_metric_and_dimension_collisions_use_qualified_public_names() {
        let graph = graph();
        let query = SemanticQuery::new()
            .with_metrics(vec![
                "orders.revenue".into(),
                "customers.customer_count".into(),
            ])
            .with_dimensions(vec!["customers.revenue".into()])
            .with_filters(vec!["orders.revenue > 100".into()])
            .with_order_by(vec![
                "orders.revenue DESC".into(),
                "customers.revenue ASC".into(),
            ]);
        let sql = try_generate(&SqlGenerator::new(&graph), &query)
            .unwrap()
            .unwrap();
        assert!(sql.contains(") AS customers_revenue"), "{sql}");
        assert!(
            sql.contains("orders_preagg.__sidemantic_metric_0 AS orders_revenue"),
            "{sql}"
        );
        assert!(
            sql.contains("ORDER BY orders_revenue DESC, customers_revenue ASC"),
            "{sql}"
        );
        assert_valid_sql(&sql);
    }

    #[test]
    fn genuinely_duplicate_child_outputs_remain_unsupported() {
        let graph = graph();
        for dimensions in [
            vec!["orders.revenue"],
            vec![
                "orders.region",
                "customers.region",
                "customers.orders_region",
            ],
        ] {
            let query = SemanticQuery::new()
                .with_metrics(vec!["ratio".into()])
                .with_dimensions(dimensions.into_iter().map(str::to_string).collect());
            assert!(matches!(try_generate(&SqlGenerator::new(&graph), &query),
                Err(SidemanticError::UnsupportedSemanticFeatures { capabilities })
                if capabilities == vec!["aggregation.child_output_alias_collision"]));
        }
    }

    #[test]
    fn single_source_proxy_keeps_the_aggregate_owner() {
        let sql = compile(&graph(), &["orders.proxy"], &["orders.amount > 60"]).unwrap();
        assert!(sql.contains("customers_preagg"));
        assert!(!sql.contains("orders_preagg AS"));
        assert_valid_sql(&sql);
    }
}
