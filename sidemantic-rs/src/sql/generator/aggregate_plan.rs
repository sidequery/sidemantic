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
    inline_aggregates: bool,
}

fn unsupported(capability: &str) -> SidemanticError {
    SidemanticError::UnsupportedSemanticFeatures {
        capabilities: vec![format!("aggregation.{capability}")],
    }
}

fn count_aggregate(expression: &Expression) -> bool {
    match expression {
        Expression::Count(_)
        | Expression::CountIf(_)
        | Expression::ApproxDistinct(_)
        | Expression::ApproxCountDistinct(_) => true,
        Expression::Filter(filter) => count_aggregate(&filter.this),
        Expression::WithinGroup(group) => count_aggregate(&group.this),
        _ => false,
    }
}

impl<'a, 'g> Plan<'a, 'g> {
    /// Split authored aggregate calls before expanding scalar metric references.
    /// Each call must read one source; arithmetic combines its grouped output.
    fn expand_calculation(
        &mut self,
        node: &mut serde_json::Value,
        context: Option<&str>,
    ) -> Result<()> {
        let aggregate_node = crate::core::is_aggregate_ast_node(node);
        if let serde_json::Value::Object(fields) = node {
            let kind = (fields.len() == 1).then(|| fields.keys().next().unwrap().as_str());
            if matches!(
                kind,
                Some("window" | "window_function" | "select" | "subquery" | "raw")
            ) {
                return Err(unsupported("calculation_shape"));
            }
            let aggregate = aggregate_node || matches!(kind, Some("filter" | "within_group"));
            if aggregate || kind == Some("column") {
                let expression: Expression = serde_json::from_value(node.clone())
                    .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
                let sql = self.generator.emit_expression(&expression)?;
                let replacement = if aggregate {
                    self.inline_aggregates = true;
                    let columns = semantic_column_references(&sql)?;
                    let mut owners = HashSet::new();
                    for column in columns {
                        let owner = column
                            .model
                            .as_deref()
                            .or(context)
                            .ok_or_else(|| unsupported("unscoped_leaf"))?;
                        if self.generator.graph.get_model(owner).is_none() {
                            return Err(unsupported("cross_source_raw_input"));
                        }
                        owners.insert(owner.to_string());
                    }
                    if owners.is_empty() {
                        owners.extend(context.map(str::to_string));
                    }
                    if owners.len() != 1 {
                        return Err(unsupported("cross_source_raw_input"));
                    }
                    let model = owners.into_iter().next().unwrap();
                    if !self.models.contains(&model) {
                        self.models.push(model.clone());
                    }
                    let alias = format!("__sidemantic_metric_{}", self.leaves.len());
                    let mut metric = Metric::derived(&alias, sql);
                    metric.sql_is_complete = true;
                    self.leaves.push(Leaf {
                        reference: format!("{model}.{alias}"),
                        model: model.clone(),
                        metric,
                        alias: alias.clone(),
                    });
                    let output = format!(
                        "{}.{}",
                        self.generator.quote_identifier(&format!("{model}_preagg")),
                        self.generator.quote_identifier(&alias),
                    );
                    if count_aggregate(&expression) {
                        format!("COALESCE({output}, 0)")
                    } else {
                        output
                    }
                } else {
                    let column = semantic_column_references(&sql)?.remove(0);
                    self.expand(&column.name(), context)?
                };
                *node =
                    serde_json::to_value(parse_semantic_expression(&format!("({replacement})"))?)
                        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
                return Ok(());
            }
        }
        match node {
            serde_json::Value::Object(fields) => {
                for child in fields.values_mut() {
                    self.expand_calculation(child, context)?;
                }
            }
            serde_json::Value::Array(children) => {
                for child in children {
                    self.expand_calculation(child, context)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn graph_metric_context(&self, reference: &str, metric: &Metric) -> Result<Option<String>> {
        if let Some(owner) = self.generator.graph.metric_owner(reference) {
            return Ok(Some(owner.to_string()));
        }
        // Imported graph aggregates can bind their source in qualified SQL
        // without an explicit owner annotation. Scalar calculations stay unowned.
        if metric.agg.is_some() && !metric.sql_is_complete {
            let owners = self
                .generator
                .graph_metric_owner_models(reference, metric)?;
            if owners.len() == 1 {
                return Ok(owners.into_iter().next());
            }
        }
        Ok(None)
    }

    fn resolve(&self, reference: &str, context: Option<&str>) -> Result<Option<ResolvedMetric>> {
        let graph = self.generator.graph;
        if let Some((model_name, name)) = reference.split_once('.') {
            if let Some(metric) = graph.get_metric(reference) {
                return Ok(Some(ResolvedMetric {
                    reference: reference.to_string(),
                    context: self.graph_metric_context(reference, metric)?,
                    metric: metric.clone(),
                }));
            }
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
                context: self.graph_metric_context(reference, metric)?,
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
        if metric.non_additive_dimension.is_some() || metric.offset_window.is_some() {
            return Err(unsupported("calculation_shape"));
        }
        if metric.r#type != MetricType::Simple
            && !metric.sql_is_complete
            && !metric.filters.is_empty()
        {
            return Err(unsupported("calculation_filters"));
        }
        let leaf_type = if metric.sql_is_complete && resolved.context.is_some() {
            MetricType::Simple
        } else if metric.sql_is_complete {
            MetricType::Derived
        } else {
            metric.r#type.clone()
        };
        let expression = match leaf_type {
            MetricType::Simple => {
                let model = resolved
                    .context
                    .clone()
                    .ok_or_else(|| unsupported("unscoped_leaf"))?;
                if !metric.sql_is_complete
                    && (metric.agg.is_none() || metric.agg == Some(Aggregation::Expression))
                {
                    return Err(unsupported("inline_aggregate"));
                }
                // A leaf is source-local. Qualified raw inputs from another
                // source need a separate row-grain plan, not this aggregate split.
                if let Some(sql) = &metric.sql {
                    if sql != "*"
                        && semantic_column_references(&sql.replace("{model}", &model))?
                            .iter()
                            .any(|column| {
                                column.model.as_deref().is_some_and(|owner| {
                                    owner != model
                                        && !(metric.sql_is_complete
                                            && owner == format!("{model}_cte"))
                                })
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
                let output = format!(
                    "{}.{}",
                    self.generator.quote_identifier(&format!("{model}_preagg")),
                    self.generator.quote_identifier(&alias)
                );
                // A restored outer-join group has no source rows: COUNT is
                // zero there, before metric defaults or downstream arithmetic.
                // SUM/MIN and other nullable aggregates retain their NULL value.
                if matches!(
                    metric.agg,
                    Some(
                        Aggregation::Count
                            | Aggregation::CountDistinct
                            | Aggregation::ApproxCountDistinct
                    )
                ) {
                    format!("COALESCE({output}, 0)")
                } else {
                    output
                }
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
                let sql = crate::core::replace_model_placeholder(sql, resolved.context.as_deref())?;
                let mut ast = serde_json::to_value(parse_semantic_expression(&sql)?)
                    .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
                self.expand_calculation(&mut ast, resolved.context.as_deref())?;
                let expression = serde_json::from_value(ast)
                    .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
                self.generator.emit_expression(&expression)?
            }
            _ => return Err(unsupported("calculation_shape")),
        };
        // Complete SQL is opaque inside another formula. Python applies its
        // default only when that metric itself is selected, not at each use.
        let expression = if metric.sql_is_complete {
            expression
        } else {
            self.generator.fill_metric_expression(metric, expression)?
        };
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

fn independent_source_path(
    graph: &SemanticGraph,
    from: &str,
    to: &str,
    dimensions: &[DimensionRef],
) -> Result<Option<JoinPath>> {
    match graph.find_join_path(from, to) {
        Ok(path) => Ok(Some(path)),
        Err(SidemanticError::AmbiguousJoinPath { .. })
            if dimensions.iter().any(|dimension| {
                [from, to].into_iter().all(|source| {
                    graph
                        .find_join_path(source, &dimension.model)
                        .is_ok_and(|path| {
                            path.steps
                                .iter()
                                .all(|step| step.relationship_type != RelationshipType::Cross)
                        })
                })
            }) =>
        {
            // Both children have a unique keyed route to a requested grouping
            // model. Their aggregate outputs meet there; no source-to-source
            // row join needs to choose among the alternate graph routes.
            Ok(None)
        }
        Err(error) => Err(error),
    }
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
        inline_aggregates: false,
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
        let expression = if metric.metric.sql_is_complete {
            generator.fill_metric_expression(&metric.metric, expression)?
        } else {
            expression
        };
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
        for column in crate::core::outer_semantic_column_references(filter)? {
            if plan.resolve(&column.name(), None)?.is_some() {
                plan.expand(&column.name(), None)?;
            }
        }
    }
    let mut effective_query;
    let query = if plan.models.len() == 1 && !query.skip_default_time_dimensions {
        effective_query = query.clone();
        effective_query.dimensions =
            generator.apply_default_time_dimensions(&query.metrics, &query.dimensions)?;
        effective_query.skip_default_time_dimensions = true;
        &effective_query
    } else {
        query
    };
    let dimensions = generator.parse_dimension_refs(&query.dimensions)?;
    // Inline splitting exists to combine independent sources. The ordinary
    // single-source compiler binds semantic dimension inputs and owns authored
    // aggregate/window expressions without manufacturing physical columns.
    if plan.inline_aggregates && plan.models.len() < 2 {
        return Ok(None);
    }
    // Cartesian products require every participating source even in a child
    // selecting only one source's measure: an empty sibling annihilates rows.
    // Keyed independent aggregates keep their existing separate populations.
    let mut population_models = query.required_population_models.clone();
    for (index, from) in plan.models.iter().enumerate() {
        for to in plan.models.iter().skip(index + 1) {
            let Some(path) = independent_source_path(generator.graph, from, to, &dimensions)?
            else {
                continue;
            };
            if path
                .steps
                .iter()
                .any(|step| step.relationship_type == RelationshipType::Cross)
            {
                for step in path.steps {
                    population_models.insert(step.from_model);
                    population_models.insert(step.to_model);
                }
            }
        }
    }
    let mut fanout_models = HashSet::new();
    for model in &plan.models {
        let mut required = HashSet::from([model.clone()]);
        required.extend(dimensions.iter().map(|dimension| dimension.model.clone()));
        required.extend(query.prepared_policies.model_names().cloned());
        required.extend(generator.find_filter_models(&all_filters));
        required.extend(query.consumption_base_model.iter().cloned());
        required.extend(population_models.iter().cloned());
        let anchor = query.consumption_base_model.as_deref().unwrap_or(model);
        let paths = generator.build_join_paths(anchor, &required)?;
        if !query.ungrouped
            && generator
                .detect_fan_out_risk(anchor, &paths)
                .contains(model)
        {
            fanout_models.insert(model.clone());
        }
    }
    if plan.models.len() < 2
        && !plan.cross_source_calculation
        && fanout_models.is_empty()
        && !plan.leaves.iter().any(|leaf| leaf.metric.sql_is_complete)
    {
        return Ok(None);
    }
    if query.use_preaggregations {
        generator.reject_totals_route(query, "preaggregation")?;
    }
    if query.with_totals {
        for (index, source) in plan.models.iter().enumerate() {
            for target in plan.models.iter().skip(index + 1) {
                for (from, to) in [(source, target), (target, source)] {
                    if generator
                        .graph
                        .find_join_path(from, to)?
                        .steps
                        .iter()
                        .any(|step| step.relationship_type == RelationshipType::ManyToOne)
                    {
                        generator.reject_totals_route(query, "preaggregation")?;
                    }
                }
            }
        }
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
        independent_source_path(generator.graph, &plan.models[0], model, &dimensions)?;
    }
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
    let mut window_filters: HashMap<String, Vec<String>> = HashMap::new();
    let mut aggregate_filters = Vec::new();
    for filter in filters {
        let sql = generator.emit_expression(&filter)?;
        let columns = crate::core::outer_semantic_column_references(&sql)?;
        let window_owners: HashSet<_> = columns
            .iter()
            .filter_map(|column| {
                let owner = column.model.as_deref()?;
                generator
                    .graph
                    .get_model(owner)?
                    .get_dimension(&column.field)?
                    .window
                    .as_ref()
                    .map(|_| owner.to_string())
            })
            .collect();
        if !window_owners.is_empty() {
            // Window predicates filter source rows after window evaluation and
            // before that source's aggregate. Metric references in the same
            // predicate therefore refer to their row inputs, not outer totals.
            let mut replacements = HashMap::new();
            for column in &columns {
                if let Some(metric) = plan.resolve(&column.name(), None)? {
                    let owner = metric
                        .context
                        .as_deref()
                        .ok_or_else(|| unsupported("mixed_row_aggregate_filter"))?;
                    if !window_owners.contains(owner)
                        || metric.metric.r#type != MetricType::Simple
                        || metric.metric.sql_is_complete
                    {
                        return Err(unsupported("mixed_row_aggregate_filter"));
                    }
                    let source_model = generator.graph.get_model(owner).unwrap();
                    let mut raw = generator.metric_raw_expression(&metric.metric, source_model)?;
                    if !metric.metric.filters.is_empty() {
                        let predicate = generator.normalize_metric_filters(
                            &metric.metric.filters,
                            owner,
                            &generator.model_alias(owner),
                        )?;
                        raw = format!("CASE WHEN {predicate} THEN {raw} END");
                    }
                    raw = raw.replace("{model}", owner);
                    let mut inputs = HashMap::new();
                    for input in semantic_column_references(&raw)? {
                        if input.model.as_deref().is_some_and(|qualifier| {
                            qualifier != owner
                                && qualifier != generator.model_alias(owner)
                                && qualifier != source_model.table_name()
                        }) {
                            return Err(unsupported("mixed_row_aggregate_filter"));
                        }
                        // Bind physical inputs directly to the CTE. Going back
                        // through semantic names could expand a same-named
                        // computed dimension instead of the metric's row input.
                        let qualified = format!(
                            "{}.{}",
                            generator.model_alias(owner),
                            generator.quote_identifier(&input.field)
                        );
                        inputs.insert((input.model, input.field), qualified);
                    }
                    let raw = generator.emit_expression(&replace_semantic_columns(
                        parse_semantic_expression(&raw)?,
                        &inputs,
                    )?)?;
                    replacements.insert(
                        (column.model.clone(), column.field.clone()),
                        format!("({raw})"),
                    );
                }
            }
            let predicate = generator.emit_expression(
                &crate::core::replace_outer_semantic_columns(filter, &replacements)?,
            )?;
            for owner in window_owners {
                window_filters
                    .entry(owner)
                    .or_default()
                    .push(predicate.clone());
            }
            continue;
        }
        let mut replacements = HashMap::new();
        let mut has_metric = false;
        let mut has_raw = false;
        for column in columns {
            let reference = column.name();
            if let Some(metric) = plan.resolve(&reference, None)? {
                has_metric = true;
                let expression = outputs
                    .iter()
                    .find(|(_, output, _)| output.reference == metric.reference)
                    .map(|(_, _, expression)| expression)
                    .unwrap_or_else(|| plan.expressions.get(&metric.reference).unwrap());
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
            aggregate_filters.push(generator.emit_expression(
                &crate::core::replace_outer_semantic_columns(filter, &replacements)?,
            )?);
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
        let mut child = query.clone();
        child.required_population_models = population_models.clone();
        child.metrics = leaves.iter().map(|leaf| leaf.reference.clone()).collect();
        child.filters = row_filters.clone();
        child
            .filters
            .extend(window_filters.get(model).into_iter().flatten().cloned());
        child.segments.clear();
        child.order_by.clear();
        child.limit = None;
        child.offset = None;
        child.skip_default_time_dimensions = true;
        // Materialized routing is qualified for whole single-source queries.
        // Cross-source child populations need separate grain/domain acceptance.
        child.use_preaggregations = false;
        let entity_rows =
            fanout_models.contains(model) || leaves.iter().any(|leaf| leaf.metric.sql_is_complete);
        let child_sql = if entity_rows {
            let metrics: Vec<_> = leaves
                .iter()
                .map(|leaf| (&leaf.metric, leaf.alias.as_str()))
                .collect();
            super::fanout_complete::generate_entity_aggregates(
                generator,
                &child,
                model,
                &dimensions,
                &metrics,
                fanout_models.contains(model),
                plan.models.len() > 1,
            )?
        } else {
            let mut projection = child_projection(generator, &dimensions, &leaves)?;
            if query.with_totals && !dimensions.is_empty() {
                projection.push("__sidemantic_source._is_total AS _is_total".into());
            }
            // Independent sources retain unmatched rows. Grouping order must
            // not choose which population contributes to the calculation.
            let anchor = if plan.models.len() > 1 {
                Some(model.clone())
            } else {
                generator
                    .query_base_model(&dimensions, &generator.parse_metric_refs(&child.metrics)?)
            };
            let sql = generator.generate_from_model(&child, anchor.as_deref())?;
            format!(
                "SELECT {}\nFROM (\n{sql}\n) AS __sidemantic_source",
                projection.join(", ")
            )
        };
        ctes.push(format!(
            "{} AS (\n{child_sql}\n)",
            generator.quote_identifier(&format!("{model}_preagg")),
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
    if query.with_totals && !dimensions.is_empty() {
        let markers: Vec<_> = plan
            .models
            .iter()
            .map(|model| {
                format!(
                    "{}._is_total",
                    generator.quote_identifier(&format!("{model}_preagg"))
                )
            })
            .collect();
        let marker = if markers.len() == 1 {
            markers[0].clone()
        } else {
            format!("COALESCE({})", markers.join(", "))
        };
        selections.push(format!("{marker} AS _is_total"));
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
            let mut conditions = dimensions
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
            if query.with_totals {
                let previous: Vec<_> = plan.models[..index]
                    .iter()
                    .map(|model| {
                        format!(
                            "{}._is_total",
                            generator.quote_identifier(&format!("{model}_preagg"))
                        )
                    })
                    .collect();
                let previous = if previous.len() == 1 {
                    previous[0].clone()
                } else {
                    format!("COALESCE({})", previous.join(", "))
                };
                // A real all-NULL group and the grand-total group are different
                // rows even though their dimension values are identical.
                conditions.push(format!("{previous} = {table}._is_total"));
            }
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
        assert!(sql.contains("NULLIF((COALESCE(customers_preagg.__sidemantic_metric_1, 0)), 0)"));
        assert!(!sql.contains("LEFT JOIN customers_cte"));
        assert_valid_sql(&sql);
    }

    #[test]
    fn cross_source_graph_alias_does_not_require_a_single_owner() {
        let graph = graph();
        let generator = SqlGenerator::new(&graph);
        let mut query = SemanticQuery::new().with_metrics(vec!["ratio".into()]);
        query.aliases.insert("ratio".into(), "value".into());
        let sql = generator.generate(&query).unwrap();
        assert!(sql.contains("orders_preagg AS"), "{sql}");
        assert!(sql.contains("customers_preagg AS"), "{sql}");
        assert!(sql.contains("AS \"value\""), "{sql}");
        assert_valid_sql(&sql);
    }

    #[test]
    fn dotted_graph_calculation_is_not_reinterpreted_as_a_model_path() {
        let mut graph = graph();
        graph
            .add_metric_unvalidated(Metric::derived("business.ratio", "ratio * 2"))
            .unwrap();
        let sql = compile(&graph, &["business.ratio"], &[]).unwrap();
        assert!(sql.contains("orders_preagg AS"), "{sql}");
        assert!(sql.contains("customers_preagg AS"), "{sql}");
        assert!(sql.contains("AS \"business.ratio\""), "{sql}");
        assert_valid_sql(&sql);
    }

    #[test]
    fn grouped_children_preserve_each_source_domain() {
        let graph = graph();
        for dimension in ["customers.region", "orders.region"] {
            let query = SemanticQuery::new()
                .with_metrics(vec!["ratio".into()])
                .with_dimensions(vec![dimension.into()]);
            let sql = SqlGenerator::new(&graph).generate(&query).unwrap();
            for owner in ["orders", "customers"] {
                assert!(
                    sql.contains(&format!("FROM {owner}_cte AS {owner}_cte")),
                    "{sql}"
                );
            }
            assert_valid_sql(&sql);
        }
    }

    #[test]
    fn inline_cross_source_aggregates_are_split_before_scalar_arithmetic() {
        let mut graph = graph();
        for (name, expression) in [
            (
                "inline_ratio",
                "COUNT(orders.id) * 1.0 / NULLIF(COUNT(customers.id), 0)",
            ),
            (
                "inline_sum",
                "SUM(orders.amount) + SUM(CASE WHEN customers.id > 1 THEN customers.id ELSE 0 END)",
            ),
        ] {
            let mut metric = Metric::derived(name, expression);
            metric.sql_is_complete = true;
            graph.add_metric_unvalidated(metric).unwrap();
            let sql = compile(&graph, &[name], &[]).unwrap();
            assert!(sql.contains("orders_preagg AS"), "{sql}");
            assert!(sql.contains("customers_preagg AS"), "{sql}");
            assert!(sql.contains("CROSS JOIN customers_preagg"), "{sql}");
            assert_valid_sql(&sql);
        }
    }

    #[test]
    fn inline_filtered_distinct_count_restores_absent_sources_to_zero() {
        let mut graph = graph();
        let mut metric = Metric::derived(
            "filtered_count_ratio",
            "COUNT(DISTINCT orders.id) FILTER (WHERE orders.amount > 10) / NULLIF(COUNT(customers.id), 0)",
        );
        metric.sql_is_complete = true;
        graph.add_metric_unvalidated(metric).unwrap();
        let sql = compile(&graph, &["filtered_count_ratio"], &[]).unwrap();
        assert!(
            sql.contains("COALESCE(orders_preagg.__sidemantic_metric_"),
            "{sql}"
        );
        assert!(
            sql.contains("FILTER(WHERE") || sql.contains("FILTER (WHERE"),
            "{sql}"
        );
        assert_valid_sql(&sql);
    }

    #[test]
    fn inline_cross_source_metric_filters_are_not_discarded() {
        let mut graph = graph();
        let mut metric = Metric::derived(
            "filtered_inline",
            "SUM(orders.amount) + COUNT(customers.id)",
        );
        metric.sql_is_complete = true;
        metric.filters.push("orders.amount > 10".into());
        graph.add_metric_unvalidated(metric).unwrap();
        let generator = SqlGenerator::new(&graph);
        let mut plan = Plan {
            generator: &generator,
            leaves: Vec::new(),
            models: Vec::new(),
            expressions: HashMap::new(),
            active: HashSet::new(),
            cross_source_calculation: false,
            inline_aggregates: false,
        };
        assert!(matches!(plan.expand("filtered_inline", None),
            Err(SidemanticError::UnsupportedSemanticFeatures { capabilities })
            if capabilities == vec!["aggregation.calculation_filters"]));
    }

    #[test]
    fn inline_single_source_and_window_aggregates_keep_existing_compiler() {
        for sql in [
            "COUNT(orders.virtual_row)",
            "SUM(orders.amount) / NULLIF(SUM(SUM(orders.amount)) OVER (), 0)",
        ] {
            let mut graph = graph();
            let mut orders = graph.get_model("orders").unwrap().clone();
            orders
                .dimensions
                .push(Dimension::categorical("virtual_row").with_sql("1"));
            graph.replace_model(orders).unwrap();
            let mut metric = Metric::derived("inline", sql);
            metric.sql_is_complete = true;
            graph.add_metric_unvalidated(metric).unwrap();
            let query = SemanticQuery::new().with_metrics(vec!["inline".into()]);
            assert!(try_generate(&SqlGenerator::new(&graph), &query)
                .unwrap()
                .is_none());
        }
    }

    #[test]
    fn imported_aggregate_leaves_use_qualified_inputs_and_requested_group_routes() {
        let mut graph = SemanticGraph::new();
        for name in ["clicks", "impressions"] {
            let mut model = Model::new(name, "id").with_table(name);
            for target in ["campaigns", "publishers"] {
                let mut relationship = Relationship::many_to_one(target);
                relationship.foreign_key = Some(format!("{target}_id"));
                model.relationships.push(relationship);
            }
            graph.add_model(model).unwrap();
        }
        for name in ["campaigns", "publishers"] {
            graph
                .add_model(
                    Model::new(name, "id")
                        .with_table(name)
                        .with_dimension(Dimension::categorical("name")),
                )
                .unwrap();
        }
        let mut click_count = Metric::count("click_count");
        click_count.sql = Some("clicks.id".into());
        graph.add_metric_unvalidated(click_count).unwrap();
        let mut ratio = Metric::derived(
            "ctr",
            "COUNT(clicks.id) * 1.0 / NULLIF(COUNT(impressions.id), 0)",
        );
        ratio.sql_is_complete = true;
        graph.add_metric_unvalidated(ratio).unwrap();
        graph.set_metric_scopes(HashMap::new()).unwrap();
        let query = SemanticQuery::new()
            .with_metrics(vec!["click_count".into(), "ctr".into()])
            .with_dimensions(vec!["campaigns.name".into()]);
        let sql = SqlGenerator::new(&graph).generate(&query).unwrap();
        assert!(sql.contains("clicks_preagg AS"), "{sql}");
        assert!(sql.contains("impressions_preagg AS"), "{sql}");
        assert!(!sql.contains("publishers_cte"), "{sql}");
        assert_valid_sql(&sql);
        let mut ungrouped = query;
        ungrouped.dimensions.clear();
        assert!(matches!(
            SqlGenerator::new(&graph).generate(&ungrouped),
            Err(SidemanticError::AmbiguousJoinPath { .. })
        ));
    }

    #[test]
    fn legacy_window_metric_predicate_is_applied_before_aggregation() {
        let mut legacy = SemanticGraph::new();
        for model in graph().models() {
            legacy.add_model(model.clone()).unwrap();
        }
        let mut orders = legacy.get_model("orders").unwrap().clone();
        let mut next_status = Dimension::categorical("next_status").with_sql("status");
        next_status.window = Some("LEAD(status) OVER (ORDER BY id)".into());
        orders.dimensions.push(next_status);
        legacy.replace_model(orders).unwrap();
        let query = SemanticQuery::new()
            .with_metrics(vec![
                "orders.revenue".into(),
                "customers.customer_count".into(),
            ])
            .with_dimensions(vec!["orders.region".into()])
            .with_filters(vec![
                "orders.next_status = 'complete' OR orders.revenue > 100".into(),
            ]);
        let sql = SqlGenerator::new(&legacy).generate(&query).unwrap();
        let (orders_sql, customers_sql) = sql.split_once("customers_preagg AS (").unwrap();
        assert!(orders_sql.contains("'complete'"), "{sql}");
        assert!(!orders_sql.contains("HAVING"), "{sql}");
        assert!(!customers_sql.contains("'complete'"), "{sql}");
        assert_valid_sql(&sql);
    }

    #[test]
    fn complete_count_without_inputs_retains_owner_join_and_dimension_domain() {
        let mut graph = graph();
        let mut orders = graph.get_model("orders").unwrap().clone();
        let mut count = Metric::derived("opaque_count", "COUNT(*)");
        count.sql_is_complete = true;
        orders.metrics.push(count);
        graph.replace_model(orders).unwrap();
        let query = SemanticQuery::new()
            .with_metrics(vec!["orders.opaque_count".into()])
            .with_dimensions(vec!["customers.region".into()]);
        let sql = SqlGenerator::new(&graph).generate(&query).unwrap();
        assert!(sql.contains("COUNT(*) AS __sidemantic_metric_0"), "{sql}");
        assert!(sql.contains("FROM customers_cte AS customers_cte"), "{sql}");
        assert!(sql.contains("LEFT JOIN orders_cte AS orders_cte"), "{sql}");
        assert_valid_sql(&sql);
    }

    #[test]
    fn mixed_window_predicate_filters_only_its_source_before_aggregation() {
        let mut graph = graph();
        let mut orders = graph.get_model("orders").unwrap().clone();
        let mut next_status = crate::core::Dimension::categorical("next_status").with_sql("status");
        next_status.window = Some("LEAD(status) OVER (ORDER BY id)".into());
        orders.dimensions.push(next_status);
        graph.replace_model(orders).unwrap();
        let query = SemanticQuery::new()
            .with_metrics(vec![
                "orders.revenue".into(),
                "customers.customer_count".into(),
            ])
            .with_dimensions(vec!["orders.region".into()])
            .with_filters(vec![
                "orders.next_status = 'complete' OR orders.revenue > 100".into(),
            ]);
        let sql = SqlGenerator::new(&graph).generate(&query).unwrap();
        let (orders_sql, rest) = sql.split_once("customers_preagg AS (").unwrap();
        assert!(orders_sql.contains("'complete'"), "{sql}");
        assert!(
            orders_sql.contains("amount) > 100") || orders_sql.contains("amount > 100"),
            "{sql}"
        );
        assert!(!rest.contains("'complete'"), "{sql}");
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
        assert!(sql.contains(") + (COALESCE(customers_preagg.__sidemantic_metric_"));
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
