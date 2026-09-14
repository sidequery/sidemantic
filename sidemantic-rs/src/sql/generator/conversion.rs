//! Qualification boundary for the existing two-event conversion algorithm.
use super::*;
use crate::core::{replace_semantic_columns, validate_row_expression};

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn unsupported(shape: &str) -> SidemanticError {
    SidemanticError::UnsupportedSemanticFeatures {
        capabilities: vec![format!("metric.conversion_{shape}")],
    }
}

impl SqlGenerator<'_> {
    fn conversion_source_expression(&self, model: &Model, expression: &str) -> Result<String> {
        if let Some(dimension) = model.get_dimension(expression.trim()) {
            if dimension.window.is_some() {
                return Err(unsupported("non_row_expression"));
            }
            if dimension.sql_expr() == dimension.name {
                return Ok(quote(&dimension.name));
            }
            let source = self.raw_dimension_sql(model, dimension.sql_expr());
            let parsed = parse_semantic_expression(&source)?;
            validate_row_expression(&parsed, "metric.conversion_non_row_expression")?;
            for column in semantic_column_references(&source)? {
                if column
                    .model
                    .as_ref()
                    .is_some_and(|owner| owner != &model.name && owner != "t")
                {
                    return Err(unsupported("joined_expression"));
                }
            }
            return self.emit_expression(&parsed);
        }
        let expression = expression.replace("{model}", &model.name);
        let parsed = parse_semantic_expression(&expression)?;
        validate_row_expression(&parsed, "metric.conversion_non_row_expression")?;
        let mut replacements = HashMap::new();
        for column in semantic_column_references(&expression)? {
            if column
                .model
                .as_ref()
                .is_some_and(|owner| owner != &model.name && owner != "t")
            {
                return Err(unsupported("joined_expression"));
            }
            if model
                .get_dimension(&column.field)
                .is_some_and(|dimension| dimension.window.is_some())
            {
                return Err(unsupported("non_row_expression"));
            }
            let source = model.get_dimension(&column.field).map_or_else(
                || quote(&column.field),
                |dimension| {
                    if dimension.sql_expr() == dimension.name {
                        quote(&dimension.name)
                    } else {
                        self.raw_dimension_sql(model, dimension.sql_expr())
                    }
                },
            );
            validate_row_expression(
                &parse_semantic_expression(&source)?,
                "metric.conversion_non_row_expression",
            )?;
            for source_column in semantic_column_references(&source)? {
                if source_column
                    .model
                    .as_ref()
                    .is_some_and(|owner| owner != &model.name && owner != "t")
                {
                    return Err(unsupported("joined_expression"));
                }
            }
            replacements.insert((column.model, column.field), format!("({source})"));
        }
        self.emit_expression(&replace_semantic_columns(
            parse_semantic_expression(&expression)?,
            &replacements,
        )?)
    }

    fn generate_scoped_multistep_conversion(
        &self,
        query: &SemanticQuery,
        reference: &MetricRef,
        dimensions: &[DimensionRef],
        model: &Model,
        metric: &Metric,
    ) -> Result<String> {
        let steps = metric.steps.as_ref().unwrap();
        if steps.len() < 2 || metric.conversion_window.is_some() {
            return Err(SidemanticError::Validation(
                "multi-step conversion requires at least two steps and no conversion_window".into(),
            ));
        }
        if metric.fill_nulls_with.is_some() {
            return Err(unsupported("metric_shape"));
        }
        if query
            .prepared_policies
            .model_names()
            .any(|name| name != &model.name)
        {
            return Err(unsupported("joined_policy"));
        }
        let entity = metric
            .entity
            .as_deref()
            .ok_or_else(|| unsupported("entity"))?;
        let time = self
            .default_time_dimension(model)
            .ok_or_else(|| unsupported("time_dimension"))?;
        let mut projection = vec![
            format!(
                "{} AS __funnel_entity",
                self.conversion_source_expression(model, entity)?
            ),
            format!(
                "{} AS __funnel_time",
                self.conversion_source_expression(model, &time.name)?
            ),
        ];
        let mut secured = model.clone();
        // The shared sequential generator sees only generated column names;
        // its legacy filter normalization never rewrites caller expressions.
        secured.dimensions = vec![crate::core::Dimension::time("__funnel_time")];
        secured.default_time_dimension = Some("__funnel_time".into());
        let mut output_names = HashSet::from(["total_entities".to_string()]);
        for index in 1..=steps.len() {
            output_names.insert(format!("step_{index}_count"));
        }
        if !output_names.insert(metric.name.to_ascii_lowercase()) {
            return Err(unsupported("output_alias"));
        }
        let mut output = Vec::new();
        let mut inner_dimensions = Vec::new();
        for (index, dimension) in dimensions.iter().enumerate() {
            if dimension.model != model.name || model.get_dimension(&dimension.name).is_none() {
                return Err(unsupported("joined_dimension"));
            }
            if dimension.alias.eq_ignore_ascii_case("entity")
                || (1..=steps.len()).any(|index| {
                    dimension
                        .alias
                        .eq_ignore_ascii_case(&format!("step_{index}_ts"))
                })
                || !output_names.insert(dimension.alias.to_ascii_lowercase())
            {
                return Err(unsupported("output_alias"));
            }
            let mut expression = self.conversion_source_expression(model, &dimension.name)?;
            if let Some(grain) = &dimension.granularity {
                expression = self.date_trunc_sql(grain, &expression)?;
            }
            let internal = format!("__funnel_group_{index}");
            projection.push(format!("{expression} AS {internal}"));
            secured
                .dimensions
                .push(crate::core::Dimension::categorical(&internal));
            inner_dimensions.push(DimensionRef {
                model: model.name.clone(),
                name: internal.clone(),
                alias: internal.clone(),
                granularity: None,
            });
            output.push(format!("{internal} AS {}", quote(&dimension.alias)));
        }
        // Step predicates refer to physical source columns in the Python contract.
        // Resolve only owner qualifiers, preserving literals and guarding row scope.
        let mut physical = model.clone();
        physical.dimensions.clear();
        let mut inner_metric = metric.clone();
        inner_metric.entity = Some("__funnel_entity".into());
        inner_metric.name = "__funnel_result".into();
        inner_metric.filters.clear();
        let mut inner_steps = Vec::new();
        for (index, step) in steps.iter().enumerate() {
            let expression = self.conversion_source_expression(&physical, step)?;
            let internal = format!("__funnel_step_{index}");
            projection.push(format!("({expression}) AS {internal}"));
            inner_steps.push(internal);
        }
        inner_metric.steps = Some(inner_steps);
        let filters: Vec<_> = query
            .filters
            .iter()
            .cloned()
            .chain(self.resolve_segments(&query.segments)?)
            .chain(metric.filters.iter().cloned())
            .collect();
        let mut predicates = Vec::new();
        for filter in &filters {
            for column in semantic_column_references(filter)? {
                if model.get_metric(&column.field).is_some()
                    || self.graph.get_metric(&column.field).is_some()
                {
                    return Err(unsupported("aggregate_filter"));
                }
            }
            predicates.push(format!(
                "({})",
                self.conversion_source_expression(model, filter)?
            ));
        }
        predicates.extend(
            query
                .prepared_policies
                .filters_for_model(&model.name)
                .map(|predicate| format!("({predicate})")),
        );
        let restriction = if predicates.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", predicates.join(" AND "))
        };
        secured.sql = Some(format!(
            "SELECT {} FROM {}{restriction}",
            projection.join(", "),
            self.model_from_clause(model, Some("t"))
        ));
        secured.table = None;
        let inner = self.generate_multistep_conversion_query(
            &secured,
            &inner_metric,
            reference,
            &inner_dimensions,
            &[],
            &[],
            None,
            None,
        )?;
        output.push("total_entities".into());
        output.extend((1..=steps.len()).map(|index| format!("step_{index}_count")));
        output.push(format!("__funnel_result AS {}", quote(&metric.name)));
        let mut sql = format!(
            "SELECT {} FROM ({inner}) AS funnel_result",
            output.join(", ")
        );
        let mut order = Vec::new();
        for item in &query.order_by {
            let (field, direction) = item
                .rsplit_once(' ')
                .filter(|(_, direction)| {
                    direction.eq_ignore_ascii_case("asc") || direction.eq_ignore_ascii_case("desc")
                })
                .unwrap_or((item, ""));
            let name = field
                .strip_prefix(&format!("{}.", model.name))
                .unwrap_or(field);
            if !output_names.contains(&name.to_ascii_lowercase()) {
                return Err(unsupported("order_by"));
            }
            order.push(format!("{} {direction}", quote(name)));
        }
        if !order.is_empty() {
            sql.push_str(&format!(" ORDER BY {}", order.join(", ")));
        }
        if let Some(limit) = query.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }
        if let Some(offset) = query.offset {
            sql.push_str(&format!(" OFFSET {offset}"));
        }
        Ok(sql)
    }

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
        if metric.steps.is_some() {
            return self
                .generate_scoped_multistep_conversion(query, reference, dimensions, model, metric);
        }
        if metric.fill_nulls_with.is_some() {
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
            if !Self::is_simple_identifier(&dimension.alias)
                || !aliases.insert(dimension.alias.to_ascii_lowercase())
            {
                return Err(unsupported("output_alias"));
            }
        }
        if !Self::is_simple_identifier(&metric.name)
            || aliases.contains(&metric.name.to_ascii_lowercase())
        {
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

        // Resolve caller/metric filters once, preserving SQL literals. The legacy
        // filter helper rewrites text, so it must never see these physical predicates.
        let mut predicates = Vec::new();
        for filter in filters.iter().chain(&metric.filters) {
            predicates.push(format!(
                "({})",
                self.conversion_source_expression(model, filter)?
            ));
        }
        predicates.extend(
            query
                .prepared_policies
                .filters_for_model(&model.name)
                .map(|predicate| format!("({predicate})")),
        );
        let mut secured = model.clone();
        for dimension in dimensions {
            let expression = self.conversion_source_expression(model, &dimension.name)?;
            secured
                .dimensions
                .iter_mut()
                .find(|field| field.name == dimension.name)
                .unwrap()
                .sql = Some(expression);
        }
        if !predicates.is_empty() {
            secured.sql = Some(format!(
                "SELECT * FROM {} WHERE {}",
                self.model_from_clause(model, Some("t")),
                predicates.join(" AND ")
            ));
            secured.table = None;
        }
        secured
            .metrics
            .iter_mut()
            .find(|candidate| candidate.name == metric.name)
            .unwrap()
            .filters
            .clear();
        let mut graph = self.graph.clone();
        graph.replace_model(secured)?;
        SqlGenerator::new(&graph)
            .with_dialect(self.dialect)
            .with_timezone(self.timezone.clone())
            .generate_conversion_query(
                reference,
                dimensions,
                &[],
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
    fn multistep_requires_a_time_dimension() {
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
            matches!(SqlGenerator::new(&graph).generate_scoped_conversion(&SemanticQuery::new(), &reference, &[]), Err(SidemanticError::UnsupportedSemanticFeatures { capabilities }) if capabilities == vec!["metric.conversion_time_dimension"])
        );
    }
}
