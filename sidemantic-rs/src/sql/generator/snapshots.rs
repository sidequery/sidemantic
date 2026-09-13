//! Per-leaf snapshot selection before aggregation; sibling measures retain all rows.
use super::*;

fn unsupported(shape: &str) -> SidemanticError {
    SidemanticError::UnsupportedSemanticFeatures {
        capabilities: vec![format!("metric.non_additive_{shape}")],
    }
}

pub(super) fn try_generate(
    generator: &SqlGenerator<'_>,
    query: &SemanticQuery,
) -> Result<Option<String>> {
    if !generator
        .graph
        .metrics()
        .chain(generator.graph.models().flat_map(|model| &model.metrics))
        .any(|metric| metric.non_additive_dimension.is_some())
    {
        return Ok(None);
    }
    let metrics = generator.parse_metric_refs(&query.metrics)?;
    let mut leaves = HashSet::new();
    for reference in &metrics {
        generator.collect_simple_metric_dependencies(
            reference,
            &mut leaves,
            &mut HashSet::new(),
        )?;
    }
    let filters: Vec<_> = query
        .filters
        .iter()
        .cloned()
        .chain(generator.resolve_segments(&query.segments)?)
        .collect();
    for filter in &filters {
        for column in semantic_column_references(filter)? {
            let context = metrics.first().map_or("", |metric| metric.model.as_str());
            if let Some((model, name, graph_metric)) =
                generator.resolve_metric_reference_location(&column.name(), context)?
            {
                generator.collect_simple_metric_dependencies(
                    &MetricRef {
                        model,
                        alias: name.clone(),
                        name,
                        graph_metric,
                    },
                    &mut leaves,
                    &mut HashSet::new(),
                )?;
            }
        }
    }
    let has_snapshot = leaves.iter().any(|(model, name, graph_metric)| {
        generator
            .metric_for_ref(&MetricRef {
                model: model.clone(),
                name: name.clone(),
                alias: name.clone(),
                graph_metric: *graph_metric,
            })
            .is_ok_and(|metric| metric.non_additive_dimension.is_some())
    });
    if !has_snapshot {
        return Ok(None);
    }
    if metrics.is_empty()
        || query.ungrouped
        || !query.table_calculations.is_empty()
        || query.use_preaggregations
    {
        return Err(unsupported("query_shape"));
    }
    let owner = &metrics[0].model;
    for reference in &metrics {
        let metric = generator.metric_for_ref(reference)?;
        if reference.model != *owner
            || reference.graph_metric
            || metric.r#type != MetricType::Simple
            || metric.sql_is_complete
            || metric.agg == Some(Aggregation::Expression)
            || metric.fill_nulls_with.is_some()
        {
            return Err(unsupported("metric_shape"));
        }
    }
    let dimensions = if query.skip_default_time_dimensions {
        query.dimensions.clone()
    } else {
        generator.apply_default_time_dimensions(&query.metrics, &query.dimensions)?
    };
    let output_dimensions = generator.parse_dimension_refs(&dimensions)?;
    let mut raw_dimensions = dimensions.clone();
    for reference in &metrics {
        let metric = generator.metric_for_ref(reference)?;
        if let Some(dimension) = &metric.non_additive_dimension {
            if !matches!(
                metric.non_additive_window.as_deref(),
                None | Some("min" | "max")
            ) {
                return Err(SidemanticError::Validation(
                    "non_additive_window must be min or max".into(),
                ));
            }
            for field in std::iter::once(dimension)
                .chain(metric.non_additive_window_groupings.iter().flatten())
            {
                if generator
                    .graph
                    .get_model(owner)
                    .and_then(|model| model.get_dimension(field))
                    .is_none()
                {
                    return Err(SidemanticError::Validation(format!(
                        "Unknown snapshot dimension '{owner}.{field}'"
                    )));
                }
                let qualified = format!("{owner}.{field}");
                if !raw_dimensions.contains(&qualified) {
                    raw_dimensions.push(qualified);
                }
            }
        }
    }
    let raw_refs = generator.parse_dimension_refs(&raw_dimensions)?;
    let mut aliases = HashSet::new();
    for alias in raw_refs
        .iter()
        .map(|dimension| &dimension.alias)
        .chain(metrics.iter().map(|metric| &metric.alias))
    {
        if !aliases.insert(alias) {
            return Err(unsupported("alias_collision"));
        }
    }
    // Aggregate predicates need a separate outer binding, never a raw-row WHERE.
    for filter in &filters {
        for column in semantic_column_references(filter)? {
            let model = column.model.as_deref().unwrap_or(owner);
            if generator
                .graph
                .get_model(model)
                .and_then(|source| source.get_metric(&column.field))
                .is_some()
                || generator.graph.get_metric(&column.field).is_some()
            {
                return Err(unsupported("aggregate_filter"));
            }
        }
    }
    let mut required = generator.find_required_models(&raw_refs, &metrics)?;
    required.extend(generator.find_filter_models(&filters));
    for reference in &metrics {
        generator.collect_metric_referenced_models(
            reference,
            &mut required,
            &mut HashSet::new(),
        )?;
    }
    required.extend(query.prepared_policies.model_names().cloned());
    let paths = generator.build_join_paths(owner, &required)?;
    if generator.detect_fan_out_risk(owner, &paths).contains(owner) {
        return Err(unsupported("fanout"));
    }

    // Reuse source projection, joins, row filters, and prepared mandatory policies.
    // Only remove snapshot annotations from this private graph to prevent recursion.
    let mut graph = generator.graph.clone();
    let mut model = graph.get_model(owner).unwrap().clone();
    for metric in &mut model.metrics {
        metric.non_additive_dimension = None;
    }
    graph.replace_model(model)?;
    let mut child = query.clone();
    child.dimensions = raw_dimensions.clone();
    child.ungrouped = true;
    child.skip_default_time_dimensions = true;
    child.order_by.clear();
    child.limit = None;
    child.offset = None;
    let raw = SqlGenerator::new(&graph)
        .with_dialect(generator.dialect)
        .generate(&child)?;
    let quote = |name: &str| generator.quote_identifier(name);
    let mut marked: Vec<String> = output_dimensions
        .iter()
        .map(|dimension| quote(&dimension.alias))
        .collect();
    for reference in &metrics {
        let metric = generator.metric_for_ref(reference)?;
        let mut value = quote(&reference.alias);
        if let Some(dimension) = &metric.non_additive_dimension {
            let qualified = format!("{owner}.{dimension}");
            if !dimensions.contains(&qualified) {
                let time = &raw_refs[raw_dimensions
                    .iter()
                    .position(|field| field == &qualified)
                    .unwrap()]
                .alias;
                let mut partitions = Vec::new();
                if let Some(groupings) = metric
                    .non_additive_window_groupings
                    .as_ref()
                    .filter(|fields| !fields.is_empty())
                {
                    for grouping in groupings {
                        let field = format!("{owner}.{grouping}");
                        let alias = &raw_refs[raw_dimensions
                            .iter()
                            .position(|candidate| candidate == &field)
                            .unwrap()]
                        .alias;
                        if !partitions.contains(alias) {
                            partitions.push(alias.clone());
                        }
                    }
                    for (field, parsed) in dimensions.iter().zip(&output_dimensions) {
                        if field.starts_with(&format!("{qualified}__"))
                            && !partitions.contains(&parsed.alias)
                        {
                            partitions.push(parsed.alias.clone());
                        }
                    }
                } else {
                    partitions.extend(
                        output_dimensions
                            .iter()
                            .map(|dimension| dimension.alias.clone()),
                    );
                }
                let partition = if partitions.is_empty() {
                    String::new()
                } else {
                    format!(
                        "PARTITION BY {}",
                        partitions
                            .iter()
                            .map(|field| quote(field))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                };
                let window = metric
                    .non_additive_window
                    .as_deref()
                    .unwrap_or("max")
                    .to_uppercase();
                value = format!(
                    "CASE WHEN {} = {window}({}) OVER ({partition}) THEN {value} END",
                    quote(time),
                    quote(time)
                );
            }
        }
        marked.push(format!("{value} AS {}", quote(&reference.alias)));
    }
    let mut selections: Vec<String> = output_dimensions
        .iter()
        .map(|dimension| quote(&dimension.alias))
        .collect();
    for reference in &metrics {
        let metric = generator.metric_for_ref(reference)?;
        let value = quote(&reference.alias);
        let aggregate = match metric.agg.as_ref() {
            Some(Aggregation::CountDistinct) => format!("COUNT(DISTINCT {value})"),
            Some(aggregation) => format!("{}({value})", aggregation.as_sql()),
            None => return Err(unsupported("aggregation")),
        };
        selections.push(format!("{aggregate} AS {value}"));
    }
    let mut sql = format!(
        "SELECT {}\nFROM (SELECT {}\nFROM ({raw}) AS __snapshot_rows) AS __snapshot_values",
        selections.join(", "),
        marked.join(", ")
    );
    if !output_dimensions.is_empty() {
        sql.push_str(&format!(
            "\nGROUP BY {}",
            (1..=output_dimensions.len())
                .map(|index| index.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    let mut ordering = Vec::new();
    for item in &query.order_by {
        let (field, direction) = item
            .rsplit_once(' ')
            .filter(|(_, dir)| dir.eq_ignore_ascii_case("asc") || dir.eq_ignore_ascii_case("desc"))
            .unwrap_or((item, ""));
        let alias = query
            .metrics
            .iter()
            .zip(&metrics)
            .find(|(name, parsed)| name.as_str() == field || parsed.alias == field)
            .map(|(_, parsed)| &parsed.alias)
            .or_else(|| {
                dimensions
                    .iter()
                    .zip(&output_dimensions)
                    .find(|(name, parsed)| name.as_str() == field || parsed.alias == field)
                    .map(|(_, parsed)| &parsed.alias)
            })
            .ok_or_else(|| unsupported("order_by"))?;
        ordering.push(format!("{} {direction}", quote(alias)));
    }
    if !ordering.is_empty() {
        sql.push_str(&format!("\nORDER BY {}", ordering.join(", ")));
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
    use crate::core::Dimension;

    fn graph() -> SemanticGraph {
        let mut balance = Metric::sum("balance", "amount");
        balance.non_additive_dimension = Some("day".into());
        let mut graph = SemanticGraph::new();
        graph
            .add_model(
                Model::new("snapshots", "id")
                    .with_table("snapshots")
                    .with_dimension(Dimension::categorical("day"))
                    .with_metric(balance)
                    .with_metric(Metric::sum("activity", "amount")),
            )
            .unwrap();
        graph
    }

    #[test]
    fn simple_model_snapshot_is_not_bypassed_by_graph_metric_fast_path() {
        let graph = graph();
        assert_eq!(graph.metrics().count(), 0);
        let query = SemanticQuery::new().with_metrics(vec!["snapshots.balance".into()]);
        let sql = try_generate(&SqlGenerator::new(&graph), &query)
            .unwrap()
            .expect("model-local snapshot must select the snapshot route");
        assert!(sql.contains("CASE WHEN day = MAX(day) OVER () THEN balance END AS balance"));
    }

    #[test]
    fn snapshot_marker_does_not_filter_additive_sibling() {
        crate::semantic_input::with_semantic_stack(|| {
            let graph = graph();
            let query = SemanticQuery::new().with_metrics(vec![
                "snapshots.balance".into(),
                "snapshots.activity".into(),
            ]);
            let sql = SqlGenerator::new(&graph).generate(&query)?;
            assert!(sql.contains("CASE WHEN day = MAX(day) OVER () THEN balance END AS balance"));
            assert!(sql.contains("SUM(activity) AS activity"));
            polyglot_sql::parse_one(&sql, DialectType::DuckDB)
                .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn snapshot_predicate_is_not_applied_to_raw_rows() {
        let graph = graph();
        let query = SemanticQuery::new()
            .with_metrics(vec!["snapshots.activity".into()])
            .with_filters(vec!["snapshots.balance > 10".into()]);
        assert!(matches!(
            SqlGenerator::new(&graph).generate(&query),
            Err(SidemanticError::UnsupportedSemanticFeatures { .. })
        ));
    }
}
