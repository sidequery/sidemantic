//! Source-local cohort populations with separate source and inner-result bindings.
use super::*;
use crate::core::{replace_semantic_columns, validate_row_expression};

fn unsupported(shape: &str) -> SqlctxError {
    SqlctxError::UnsupportedSemanticFeatures {
        capabilities: vec![format!("metric.cohort_{shape}")],
    }
}

fn aggregate(generator: &SqlGenerator<'_>, kind: &Aggregation, expression: &str) -> Result<String> {
    match kind {
        Aggregation::CountDistinct => Ok(format!("COUNT(DISTINCT {expression})")),
        Aggregation::Expression => Err(unsupported("complete_aggregate")),
        kind => generator.aggregate_sql(kind, expression),
    }
}

impl SqlGenerator<'_> {
    fn cohort_source_expression(&self, model: &Model, expression: &str) -> Result<String> {
        if let Some(dimension) = model.get_dimension(expression.trim()) {
            if dimension.window.is_some() {
                return Err(unsupported("non_row_expression"));
            }
            if dimension.sql_expr() == dimension.name {
                return Ok(self.population_identifier(&dimension.name));
            }
            return self.population_physical_expression(model, dimension.sql_expr(), "cohort");
        }
        let expression = bind_population_owner(expression, model)?;
        let parsed = parse_semantic_expression(&expression)?;
        validate_row_expression(&parsed, "metric.cohort_non_row_expression")?;
        let mut replacements = HashMap::new();
        for column in semantic_column_references(&expression)? {
            if column
                .model
                .as_ref()
                .is_some_and(|owner| owner != &model.name && owner != "t")
            {
                return Err(unsupported("joined_expression"));
            }
            let source = if let Some(dimension) = model.get_dimension(&column.field) {
                if dimension.window.is_some() {
                    return Err(unsupported("non_row_expression"));
                }
                self.population_physical_expression(model, dimension.sql_expr(), "cohort")?
            } else {
                self.population_identifier(&column.field)
            };
            replacements.insert((column.model, column.field), format!("({source})"));
        }
        self.emit_expression(&replace_semantic_columns(parsed, &replacements)?)
    }

    fn cohort_result_expression(
        &self,
        expression: &str,
        model: &str,
        fields: &HashMap<String, String>,
    ) -> Result<String> {
        let expression = crate::core::replace_model_placeholder(expression, Some("cohort_sub"))?;
        let parsed = parse_semantic_expression(&expression)?;
        validate_row_expression(&parsed, "metric.cohort_result_non_row_expression")?;
        let mut replacements = HashMap::new();
        for column in semantic_column_references(&expression)? {
            if column
                .model
                .as_ref()
                .is_some_and(|owner| owner != "cohort_sub" && owner != model)
            {
                return Err(unsupported("result_reference"));
            }
            let name = fields
                .get(&column.field.to_ascii_lowercase())
                .ok_or_else(|| unsupported("result_reference"))?;
            let field = self.population_identifier(name);
            replacements.insert((column.model, column.field), format!("cohort_sub.{field}"));
        }
        self.emit_expression(&replace_semantic_columns(parsed, &replacements)?)
    }

    pub(super) fn generate_scoped_cohort(
        &self,
        query: &SemanticQuery,
        reference: &MetricRef,
        dimensions: &[DimensionRef],
    ) -> Result<String> {
        if query.ungrouped || query.use_preaggregations || !query.table_calculations.is_empty() {
            return Err(unsupported("query_shape"));
        }
        // Direct query identifiers use target syntax. The source-expression
        // binders above retain canonical quotes until AST emission.
        let quote = |name: &str| self.population_identifier(name);
        // Metric reference resolution already enforces a declared owner or a
        // unique entity source for graph-level cohorts.
        let model = self
            .graph
            .get_model(&reference.model)
            .ok_or_else(|| unsupported("owner"))?;
        let metric = self.metric_for_ref(reference)?;
        let entity = metric
            .entity
            .as_deref()
            .ok_or_else(|| SqlctxError::Validation("cohort requires entity".into()))?;
        if !Self::is_simple_identifier(entity) {
            return Err(unsupported("entity_expression"));
        }
        let entity_sql = self.cohort_source_expression(model, entity)?;
        let mut fields = HashMap::from([(entity.to_ascii_lowercase(), entity.to_string())]);
        let mut inner_select = vec![format!("{entity_sql} AS {}", quote(entity))];
        let mut inner_group = vec![entity_sql];
        let mut output_dimensions = Vec::new();
        for name in metric.entity_dimensions.iter().flatten() {
            if model.get_dimension(name).is_none() {
                return Err(unsupported("entity_dimension"));
            }
            output_dimensions.push(DimensionRef {
                model: model.name.clone(),
                name: name.clone(),
                granularity: None,
                alias: name.clone(),
            });
        }
        for dimension in dimensions {
            if !output_dimensions.iter().any(|existing| {
                existing.model == dimension.model
                    && existing.name == dimension.name
                    && existing.granularity == dimension.granularity
            }) {
                output_dimensions.push(dimension.clone());
            }
        }
        let mut output_names = HashSet::new();
        for dimension in &output_dimensions {
            if dimension.model != model.name || model.get_dimension(&dimension.name).is_none() {
                return Err(unsupported("joined_dimension"));
            }
            if !output_names.insert(dimension.alias.to_ascii_lowercase())
                || dimension.alias.eq_ignore_ascii_case(&metric.name)
            {
                return Err(unsupported("output_alias_collision"));
            }
            let mut sql = self.cohort_source_expression(model, &dimension.name)?;
            if let Some(grain) = &dimension.granularity {
                sql = self.date_trunc_sql(grain, &sql)?;
            }
            if fields
                .insert(
                    dimension.alias.to_ascii_lowercase(),
                    dimension.alias.clone(),
                )
                .is_none()
            {
                inner_select.push(format!("{sql} AS {}", quote(&dimension.alias)));
                inner_group.push(sql);
            } else if dimension.name != entity || dimension.granularity.is_some() {
                return Err(unsupported("inner_alias_collision"));
            }
        }
        let inner_metrics = metric
            .inner_metrics
            .as_ref()
            .filter(|metrics| !metrics.is_empty())
            .ok_or_else(|| SqlctxError::Validation("cohort requires inner_metrics".into()))?;
        for inner in inner_metrics {
            if fields
                .insert(inner.name.to_ascii_lowercase(), inner.name.clone())
                .is_some()
            {
                return Err(unsupported("inner_alias_collision"));
            }
            let kind = inner.agg.as_ref().unwrap_or(&Aggregation::Count);
            let expression = match inner.sql.as_deref() {
                Some(sql) => self.cohort_source_expression(model, sql)?,
                None if *kind == Aggregation::Count => "*".into(),
                None => {
                    return Err(SqlctxError::Validation(format!(
                        "Cohort inner metric '{}' requires sql",
                        inner.name
                    )))
                }
            };
            let aggregate = aggregate(self, kind, &expression)?;
            inner_select.push(format!("{aggregate} AS {}", quote(&inner.name)));
        }
        let having = metric
            .having
            .as_deref()
            .ok_or_else(|| SqlctxError::Validation("cohort requires having".into()))?;
        let having = self.cohort_result_expression(having, &model.name, &fields)?;
        let mut filters = query.filters.clone();
        filters.extend(self.resolve_segments(&query.segments, query.parameter_values.as_ref())?);
        filters.extend(metric.filters.clone());
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
                self.cohort_source_expression(model, filter)?
            ));
        }
        if query
            .prepared_policies
            .model_names()
            .any(|owner| owner != &model.name)
        {
            return Err(unsupported("joined_policy"));
        }
        predicates.extend(
            query
                .prepared_policies
                .filters_for_model(&model.name)
                .map(|filter| format!("({filter})")),
        );
        let where_clause = if predicates.is_empty() {
            String::new()
        } else {
            format!("\nWHERE {}", predicates.join(" AND "))
        };
        let outer_kind = metric.agg.as_ref().unwrap_or(&Aggregation::Count);
        let outer_expression = match metric.sql.as_deref() {
            Some(sql) => self.cohort_result_expression(sql, &model.name, &fields)?,
            None if *outer_kind == Aggregation::Count => "*".into(),
            None if *outer_kind == Aggregation::CountDistinct => {
                format!("cohort_sub.{}", quote(entity))
            }
            None => {
                return Err(SqlctxError::Validation(
                    "Cohort outer aggregation requires sql".into(),
                ))
            }
        };
        let mut outer_select: Vec<_> = output_dimensions
            .iter()
            .map(|dimension| quote(&dimension.alias))
            .collect();
        outer_select.push(format!(
            "{} AS {}",
            self.fill_metric_expression(metric, aggregate(self, outer_kind, &outer_expression)?)?,
            quote(&metric.name)
        ));
        let mut sql = format!("SELECT {}\nFROM (SELECT {}\nFROM {}{where_clause}\nGROUP BY {}) AS cohort_sub\nWHERE {having}", outer_select.join(", "), inner_select.join(", "), self.model_from_clause(model, Some("t")), inner_group.join(", "));
        if !output_dimensions.is_empty() {
            sql.push_str(&format!(
                "\nGROUP BY {}",
                output_dimensions
                    .iter()
                    .map(|dimension| quote(&dimension.alias))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        let mut order = Vec::new();
        let names: Vec<_> = output_dimensions
            .iter()
            .map(|dimension| dimension.alias.as_str())
            .chain(std::iter::once(metric.name.as_str()))
            .collect();
        for item in &query.order_by {
            let (field, direction) = crate::sql::split_order_field(item, &names);
            let name = field
                .strip_prefix(&format!("{}.", model.name))
                .unwrap_or(field);
            if !name.eq_ignore_ascii_case(&metric.name)
                && !output_names.contains(&name.to_ascii_lowercase())
            {
                return Err(unsupported("order_by"));
            }
            order.push(format!("{} {direction}", quote(name)));
        }
        if !order.is_empty() {
            sql.push_str(&format!("\nORDER BY {}", order.join(", ")));
        }
        if let Some(limit) = query.limit {
            sql.push_str(&format!("\nLIMIT {limit}"));
        }
        if let Some(offset) = query.offset {
            sql.push_str(&format!("\nOFFSET {offset}"));
        }
        Ok(sql)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::semantic_input::compile_with_semantic_input;
    use serde_json::json;

    #[test]
    fn bigquery_cohort_bindings_keep_identifiers_quoted_as_identifiers() {
        let graph = SemanticGraph::new();
        let generator = SqlGenerator::new(&graph).with_dialect(DialectType::BigQuery);
        let model = Model::new("events", "id").with_table("events");
        let source = generator
            .cohort_source_expression(&model, "platform")
            .unwrap();
        assert!(source.contains("`platform`"), "{source}");
        polyglot_sql::parse_one(&format!("SELECT {source}"), DialectType::BigQuery).unwrap();
        let fields = HashMap::from([
            ("platforms".into(), "platforms".into()),
            ("group".into(), "group".into()),
        ]);
        let outer = generator
            .cohort_result_expression("COALESCE(platforms, 0)", "events", &fields)
            .unwrap();
        assert!(outer.contains("cohort_sub.`platforms`"), "{outer}");
        let aggregate = generator
            .aggregate_sql(&Aggregation::ApproxCountDistinct, &outer)
            .unwrap();
        polyglot_sql::parse_one(&format!("SELECT {aggregate}"), DialectType::BigQuery).unwrap();
        let reserved = generator
            .cohort_result_expression("\"group\"", "events", &fields)
            .unwrap();
        assert_eq!(reserved, "cohort_sub.`group`");
    }

    #[test]
    fn postgres_cohort_predicate_binds_grouped_outputs() {
        let graph = SemanticGraph::new();
        let generator = SqlGenerator::new(&graph).with_dialect(DialectType::PostgreSQL);
        let fields = HashMap::from([
            ("platforms".into(), "Platforms".into()),
            ("person".into(), "person".into()),
        ]);
        let sql = generator
            .cohort_result_expression("platforms >= 2 AND person > 0", "events", &fields)
            .unwrap();
        assert!(sql.contains(r#"cohort_sub."Platforms""#), "{sql}");
        assert!(sql.contains(r#"cohort_sub."person""#), "{sql}");
    }

    #[test]
    fn population_identifiers_and_approximate_aggregates_compile() {
        for (dialect, dialect_type) in [
            ("duckdb", DialectType::DuckDB),
            ("postgres", DialectType::PostgreSQL),
            ("bigquery", DialectType::BigQuery),
            ("snowflake", DialectType::Snowflake),
            ("trino", DialectType::Trino),
            ("clickhouse", DialectType::ClickHouse),
        ] {
            for outer_approximate in [false, true] {
                let input = json!({
                    "version": 1, "input_dialect": "duckdb",
                    "models": [{
                        "name": "events", "table": "events", "primary_key": "id",
                        "dimensions": [
                            {"name": "person", "sql": "user_id", "type": "categorical"},
                            {"name": "group", "sql": "region", "type": "categorical"}
                        ],
                        "metrics": [{
                            "name": "qualified", "type": "cohort", "entity": "person",
                            "agg": if outer_approximate { "approx_count_distinct" } else { "count" },
                            "sql": if outer_approximate { json!("platforms") } else { json!(null) },
                            "inner_metrics": [{
                                "name": "platforms", "agg": "approx_count_distinct", "sql": "platform"
                            }],
                            "having": "platforms >= 2"
                        }]
                    }]
                });
                let query = json!({
                    "metrics": ["events.qualified"], "dimensions": ["events.group"],
                    "order_by": ["events.group"], "dialect": dialect,
                });
                let result = compile_with_semantic_input(&input.to_string(), &query.to_string());
                if dialect_type == DialectType::PostgreSQL {
                    assert!(matches!(
                        result,
                        Err(SqlctxError::UnsupportedSemanticFeatures { .. })
                    ));
                    continue;
                }
                let sql = result.unwrap();
                polyglot_sql::parse_one(&sql, dialect_type).unwrap();
                let quoted = if dialect_type == DialectType::BigQuery {
                    "`group`"
                } else {
                    "\"group\""
                };
                assert!(sql.contains(quoted), "{dialect}: {sql}");
                assert!(sql.contains("user_id"), "{dialect}: {sql}");
                assert!(sql.contains("WHERE cohort_sub."), "{dialect}: {sql}");
            }
        }
    }

    #[test]
    fn owned_graph_cohort_approximate_aggregate_uses_qualified_inner_results() {
        let input = json!({
            "version": 1, "input_dialect": "duckdb",
            "models": [{
                "name": "events", "table": "events", "primary_key": "id",
                "dimensions": [{"name": "person", "type": "categorical"}]
            }],
            "metrics": [{
                "name": "qualified", "type": "cohort", "entity": "person",
                "agg": "approx_count_distinct", "sql": "amount",
                "inner_metrics": [{"name": "amount", "agg": "sum", "sql": "raw_amount"}],
                "having": "amount > 10"
            }],
            "metric_owners": {"qualified": "events"}
        });
        let sql = compile_with_semantic_input(
            &input.to_string(),
            &json!({"metrics": ["qualified"]}).to_string(),
        )
        .unwrap();
        // The graph metric's SQL names an inner result, not a source column.
        assert!(
            sql.contains("APPROX_COUNT_DISTINCT(cohort_sub.\"amount\")"),
            "{sql}"
        );
        assert!(sql.contains("SUM((\"raw_amount\"))"), "{sql}");
        assert!(sql.contains("WHERE cohort_sub."), "{sql}");
        polyglot_sql::parse_one(&sql, DialectType::DuckDB).unwrap();
    }
}
