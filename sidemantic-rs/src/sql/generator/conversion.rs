//! Qualification boundary for the existing two-event conversion algorithm.
use super::*;

fn unsupported(shape: &str) -> SidemanticError {
    SidemanticError::UnsupportedSemanticFeatures {
        capabilities: vec![format!("metric.conversion_{shape}")],
    }
}

impl SqlGenerator<'_> {
    pub(super) fn generate_scoped_conversion(
        &self,
        query: &SemanticQuery,
        reference: &MetricRef,
        dimensions: &[DimensionRef],
    ) -> Result<String> {
        if self.dialect != DialectType::DuckDB
            || reference.graph_metric
            || query.ungrouped
            || !query.table_calculations.is_empty()
            || query.use_preaggregations
        {
            return Err(unsupported("query_shape"));
        }
        let model = self
            .graph
            .get_model(&reference.model)
            .ok_or_else(|| unsupported("owner"))?;
        let metric = self.metric_for_ref(reference)?;
        if metric.steps.is_some() || metric.fill_nulls_with.is_some() {
            return Err(unsupported("metric_shape"));
        }
        // The legacy algorithm addresses these source columns by their declared names.
        // Do not silently reinterpret dimension SQL aliases as physical columns.
        let entity = metric
            .entity
            .as_deref()
            .ok_or_else(|| SidemanticError::Validation("conversion requires entity".into()))?;
        if !Self::is_simple_identifier(entity) {
            return Err(unsupported("entity_expression"));
        }
        let mut names = vec![entity];
        for dimension in &model.dimensions {
            if dimension.r#type == crate::core::DimensionType::Time
                || (dimension.name.to_lowercase().contains("event")
                    && dimension.name.to_lowercase().contains("type"))
            {
                names.push(&dimension.name);
            }
        }
        for name in names {
            if !Self::is_simple_identifier(name) {
                return Err(unsupported("source_identifier"));
            }
            if model
                .get_dimension(name)
                .is_some_and(|dimension| dimension.sql_expr() != name)
            {
                return Err(unsupported("source_alias"));
            }
        }
        let mut aliases = HashSet::new();
        for dimension in dimensions {
            if dimension.model != model.name || model.get_dimension(&dimension.name).is_none() {
                return Err(unsupported("joined_dimension"));
            }
            if !Self::is_simple_identifier(&dimension.alias) || !aliases.insert(&dimension.alias) {
                return Err(unsupported("output_alias"));
            }
        }
        if !Self::is_simple_identifier(&metric.name) || aliases.contains(&metric.name) {
            return Err(unsupported("output_alias"));
        }
        if query
            .prepared_policies
            .model_names()
            .any(|name| name != &model.name)
        {
            return Err(unsupported("joined_policy"));
        }
        let filters: Vec<_> = query
            .filters
            .iter()
            .cloned()
            .chain(self.resolve_segments(&query.segments)?)
            .collect();
        for filter in filters.iter().chain(&metric.filters) {
            for column in semantic_column_references(filter)? {
                if column
                    .model
                    .as_ref()
                    .is_some_and(|owner| owner != &model.name)
                {
                    return Err(unsupported("joined_filter"));
                }
                if model.get_metric(&column.field).is_some()
                    || self.graph.get_metric(&column.field).is_some()
                {
                    return Err(unsupported("aggregate_filter"));
                }
            }
        }
        let mut ordering = Vec::new();
        for item in &query.order_by {
            let (field, direction) = item
                .rsplit_once(' ')
                .filter(|(_, direction)| {
                    direction.eq_ignore_ascii_case("asc") || direction.eq_ignore_ascii_case("desc")
                })
                .unwrap_or((item, ""));
            let alias =
                if field == metric.name || field == format!("{}.{}", model.name, metric.name) {
                    &metric.name
                } else {
                    dimensions
                        .iter()
                        .find(|dimension| {
                            field == dimension.alias
                                || field == format!("{}.{}", model.name, dimension.alias)
                        })
                        .map(|dimension| &dimension.alias)
                        .ok_or_else(|| unsupported("order_by"))?
                };
            ordering.push(format!("{alias} {direction}"));
        }
        let window = metric.conversion_window.as_deref().unwrap_or("7 days");
        let parts: Vec<_> = window.split_whitespace().collect();
        if parts.len() != 2 {
            return Err(SidemanticError::Validation(
                "conversion_window requires a number and unit".into(),
            ));
        }
        self.validate_interval_parts(parts[0], parts[1])?;

        // Prepared policies already contain physical source expressions and escaped
        // caller literals. Apply them once before either event population is formed.
        let predicates: Vec<_> = query
            .prepared_policies
            .filters_for_model(&model.name)
            .map(|predicate| format!("({predicate})"))
            .collect();
        let mut graph = self.graph.clone();
        if !predicates.is_empty() {
            let mut secured = model.clone();
            secured.sql = Some(format!(
                "SELECT * FROM {} WHERE {}",
                self.model_from_clause(model, Some("t")),
                predicates.join(" AND ")
            ));
            secured.table = None;
            graph.replace_model(secured)?;
        }
        SqlGenerator::new(&graph)
            .with_dialect(self.dialect)
            .generate_conversion_query(
                reference,
                dimensions,
                &filters,
                &ordering,
                query.limit,
                query.offset,
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multistep_is_not_promoted_by_conversion_decoder() {
        let mut metric = Metric::new("funnel");
        metric.r#type = MetricType::Conversion;
        metric.entity = Some("user_id".into());
        metric.steps = Some(vec![
            "event_type = 'signup'".into(),
            "event_type = 'buy'".into(),
        ]);
        let mut graph = SemanticGraph::new();
        graph
            .add_model(
                Model::new("events", "id")
                    .with_table("events")
                    .with_metric(metric),
            )
            .unwrap();
        let reference = MetricRef {
            model: "events".into(),
            name: "funnel".into(),
            alias: "funnel".into(),
            graph_metric: false,
        };
        assert!(
            matches!(SqlGenerator::new(&graph).generate_scoped_conversion(&SemanticQuery::new(), &reference, &[]), Err(SidemanticError::UnsupportedSemanticFeatures { capabilities }) if capabilities == vec!["metric.conversion_metric_shape"])
        );
    }
}
