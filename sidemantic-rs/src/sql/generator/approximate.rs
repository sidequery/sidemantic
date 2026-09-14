//! Validate approximate distinct inputs without restricting ordinary join plans.
use super::*;
use crate::core::validate_row_expression;

fn unsupported(shape: &str) -> SidemanticError {
    SidemanticError::UnsupportedSemanticFeatures {
        capabilities: vec![format!("metric.approx_count_distinct_{shape}")],
    }
}

impl SqlGenerator<'_> {
    /// Render an aggregate over an already target-dialect input expression.
    /// Approximate function names use polyglot's target AST handlers; ordinary
    /// aggregate construction retains its existing SQL and count semantics.
    pub(super) fn aggregate_sql(&self, kind: &Aggregation, input: &str) -> Result<String> {
        if *kind == Aggregation::CountDistinct {
            return Ok(format!("COUNT(DISTINCT {input})"));
        }
        if *kind != Aggregation::ApproxCountDistinct {
            return Ok(format!("{}({input})", kind.as_sql()));
        }
        let statement = crate::semantic_input::dialects::parse(&format!("SELECT {input}"), self.dialect)
            .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
        let Expression::Select(mut select) = statement else {
            return Err(SidemanticError::SqlParse("Expected aggregate input".into()));
        };
        if select.expressions.len() != 1 {
            return Err(SidemanticError::SqlParse(
                "Expected one aggregate input".into(),
            ));
        }
        let aggregate = polyglot_sql::expressions::AggFunc {
            this: select.expressions.remove(0),
            distinct: false,
            filter: None,
            order_by: Vec::new(),
            name: None,
            ignore_nulls: None,
            having_max: None,
            limit: None,
            inferred_type: None,
        };
        let expression = self
            .lower_approximate_aggregates(Expression::ApproxCountDistinct(Box::new(aggregate)))?;
        // The input was parsed in the target dialect; do not round-trip this
        // target aggregate through the canonical DuckDB source emitter.
        polyglot_sql::generate(&expression, self.dialect)
            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))
    }

    /// Use the same target AST lowering for aggregates inside complete SQL and
    /// window expressions as for directly constructed semantic aggregates.
    pub(super) fn lower_approximate_aggregates(
        &self,
        expression: Expression,
    ) -> Result<Expression> {
        fn lower(value: &mut serde_json::Value, generator: &SqlGenerator<'_>) -> Result<()> {
            match value {
                serde_json::Value::Object(fields) => {
                    for child in fields.values_mut() {
                        lower(child, generator)?;
                    }
                    if fields.len() == 1
                        && (fields.contains_key("approx_count_distinct")
                            || fields.contains_key("approx_distinct"))
                    {
                        let node: Expression = serde_json::from_value(value.clone())
                            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
                        let aggregate = match node {
                            Expression::ApproxCountDistinct(aggregate)
                            | Expression::ApproxDistinct(aggregate) => aggregate,
                            _ => unreachable!("matched serialized approximate aggregate"),
                        };
                        *value = serde_json::to_value(
                            generator.lower_approximate_aggregate(*aggregate)?,
                        )
                        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
                    }
                }
                serde_json::Value::Array(values) => {
                    for child in values {
                        lower(child, generator)?;
                    }
                }
                _ => {}
            }
            Ok(())
        }
        // The pinned polyglot visitor skips some typed-function children. Walk
        // complete serialized AST nodes so nesting cannot hide an aggregate.
        let mut value = serde_json::to_value(expression)
            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
        lower(&mut value, self)?;
        serde_json::from_value(value)
            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))
    }

    fn lower_approximate_aggregate(
        &self,
        mut aggregate: polyglot_sql::expressions::AggFunc,
    ) -> Result<Expression> {
        aggregate.name = None;
        // Approximate distinct ignores NULL inputs. Express FILTER as a
        // nullable input before target lowering so function-name transforms
        // cannot discard the predicate (notably ClickHouse's uniq mapping).
        if let Some(predicate) = aggregate.filter.take() {
            aggregate.this = Expression::Case(Box::new(polyglot_sql::expressions::Case {
                operand: None,
                whens: vec![(predicate, aggregate.this)],
                else_: None,
                comments: Vec::new(),
                inferred_type: None,
            }));
        }
        let expression = match self.dialect {
            DialectType::DuckDB
            | DialectType::Snowflake
            | DialectType::BigQuery
            | DialectType::Spark
            | DialectType::Databricks
            | DialectType::Hive => Expression::ApproxCountDistinct(Box::new(aggregate)),
            DialectType::ClickHouse => Expression::AggregateFunction(Box::new(
                polyglot_sql::expressions::AggregateFunction {
                    name: "APPROX_COUNT_DISTINCT".into(),
                    args: vec![aggregate.this],
                    distinct: aggregate.distinct,
                    filter: aggregate.filter,
                    order_by: aggregate.order_by,
                    limit: aggregate.limit,
                    ignore_nulls: aggregate.ignore_nulls,
                    inferred_type: aggregate.inferred_type,
                },
            )),
            _ => Expression::ApproxDistinct(Box::new(aggregate)),
        };
        polyglot_sql::Dialect::get(self.dialect)
            .transform(expression)
            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))
    }

    // Resolve without requiring a graph calculation to have a single owner.
    fn approximate_dependency<'a>(
        &'a self,
        reference: &str,
        context: &str,
    ) -> Option<(&'a Metric, String)> {
        if let Some((owner, name)) = reference.split_once('.') {
            return self
                .graph
                .get_model(owner)?
                .get_metric(name)
                .map(|metric| (metric, owner.to_string()));
        }
        if let Some(metric) = self
            .graph
            .get_model(context)
            .and_then(|model| model.get_metric(reference))
        {
            return Some((metric, context.to_string()));
        }
        if let Some(metric) = self.graph.get_metric(reference) {
            return Some((
                metric,
                self.graph.metric_owner(reference).unwrap_or("").to_string(),
            ));
        }
        let mut owners = self.graph.models().filter_map(|model| {
            model
                .get_metric(reference)
                .map(|metric| (metric, model.name.clone()))
        });
        let metric = owners.next()?;
        owners.next().is_none().then_some(metric)
    }

    fn contains_approximate_metric(
        &self,
        reference: &str,
        context: &str,
        visiting: &mut HashSet<(String, String)>,
    ) -> Result<bool> {
        if !visiting.insert((reference.to_string(), context.to_string())) {
            return Ok(false);
        }
        let Some((metric, owner)) = self.approximate_dependency(reference, context) else {
            return Ok(false);
        };
        if metric.agg == Some(Aggregation::ApproxCountDistinct)
            || metric
                .inner_metrics
                .iter()
                .flatten()
                .any(|inner| inner.agg == Some(Aggregation::ApproxCountDistinct))
        {
            return Ok(true);
        }
        if metric.sql_is_complete {
            return Ok(false);
        }
        for fragment in self.graph_metric_dependency_fragments(metric) {
            for column in semantic_column_references(fragment)? {
                if self.contains_approximate_metric(&column.name(), &owner, visiting)? {
                    return Ok(true);
                }
            }
        }
        if let Some(window) = &metric.window_expression {
            for dependency in self.metric_refs_from_window_expression(window, &owner)? {
                if self.contains_approximate_metric(&dependency, &owner, visiting)? {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn validate_approximate_dependency(
        &self,
        reference: &str,
        context: &str,
        visiting: &mut HashSet<(String, String)>,
    ) -> Result<()> {
        if !visiting.insert((reference.to_string(), context.to_string())) {
            return Ok(());
        }
        let Some((metric, owner)) = self.approximate_dependency(reference, context) else {
            return Ok(());
        };
        if !self.contains_approximate_metric(reference, context, &mut HashSet::new())? {
            return Ok(());
        }
        if metric.agg == Some(Aggregation::ApproxCountDistinct) {
            if metric.sql.as_deref().is_some_and(|sql| sql.trim() == "*") {
                return Err(unsupported("explicit_expression"));
            }
            for expression in metric
                .sql
                .as_deref()
                .filter(|sql| !sql.is_empty())
                .into_iter()
                .chain(metric.filters.iter().map(String::as_str))
            {
                validate_row_expression(
                    &parse_semantic_expression(&expression.replace("{model}", &owner))?,
                    "metric.approx_count_distinct_non_row_expression",
                )?;
            }
        } else {
            for fragment in self.graph_metric_dependency_fragments(metric) {
                for column in semantic_column_references(fragment)? {
                    self.validate_approximate_dependency(&column.name(), &owner, visiting)?;
                }
            }
        }
        if let Some(window) = &metric.window_expression {
            for dependency in self.metric_refs_from_window_expression(window, &owner)? {
                self.validate_approximate_dependency(&dependency, &owner, visiting)?;
            }
        }
        Ok(())
    }

    pub(super) fn validate_approximate_query(&self, query: &SemanticQuery) -> Result<()> {
        if !self
            .graph
            .models()
            .flat_map(|model| &model.metrics)
            .chain(self.graph.metrics())
            .any(|metric| {
                metric.agg == Some(Aggregation::ApproxCountDistinct)
                    || metric
                        .inner_metrics
                        .iter()
                        .flatten()
                        .any(|inner| inner.agg == Some(Aggregation::ApproxCountDistinct))
            })
        {
            return Ok(());
        }
        let mut references = query.metrics.clone();
        for filter in query
            .filters
            .iter()
            .cloned()
            .chain(self.resolve_segments(&query.segments)?)
        {
            references.extend(
                semantic_column_references(&filter)?
                    .iter()
                    .map(|column| column.name()),
            );
        }
        for reference in references {
            if self.contains_approximate_metric(&reference, "", &mut HashSet::new())? {
                self.validate_approximate_dependency(&reference, "", &mut HashSet::new())?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn output_dialects() -> [(DialectType, &'static str); 11] {
        [
            (DialectType::DuckDB, "APPROX_COUNT_DISTINCT"),
            (DialectType::PostgreSQL, "APPROX_DISTINCT"),
            (DialectType::BigQuery, "APPROX_COUNT_DISTINCT"),
            (DialectType::Snowflake, "APPROX_COUNT_DISTINCT"),
            (DialectType::Trino, "APPROX_DISTINCT"),
            (DialectType::Spark, "APPROX_COUNT_DISTINCT"),
            (DialectType::Databricks, "APPROX_COUNT_DISTINCT"),
            (DialectType::Redshift, "APPROXIMATE COUNT"),
            (DialectType::ClickHouse, "UNIQ"),
            (DialectType::MySQL, "APPROX_DISTINCT"),
            (DialectType::SQLite, "APPROX_DISTINCT"),
        ]
    }

    #[test]
    fn approximate_target_names_cover_direct_inline_and_cohort_rendering() {
        let graph = graph_with(json!({"name":"total", "agg":"sum", "sql":"amount"}));
        let inner: CohortInnerMetric = serde_json::from_value(json!({
            "name":"inner", "agg":"approx_count_distinct", "sql":"user_id"
        }))
        .unwrap();
        let outer: Metric = serde_json::from_value(json!({
            "name":"outer", "type":"cohort", "agg":"approx_count_distinct", "sql":"inner"
        }))
        .unwrap();
        for (dialect, name) in output_dialects() {
            let generator = SqlGenerator::new(&graph).with_dialect(dialect);
            for sql in [
                generator
                    .aggregate_sql(
                        &Aggregation::ApproxCountDistinct,
                        "CASE WHEN paid THEN user_id END",
                    )
                    .unwrap(),
                generator
                    .emit_expression(
                        &parse_semantic_expression("APPROX_COUNT_DISTINCT(user_id) + 1").unwrap(),
                    )
                    .unwrap(),
                generator
                    .cohort_inner_metric_sql(graph.get_model("events").unwrap(), &inner)
                    .unwrap(),
                generator.cohort_outer_metric_sql(&outer).unwrap(),
            ] {
                assert!(sql.to_uppercase().contains(name), "{dialect}: {sql}");
                polyglot_sql::parse_one(&format!("SELECT {sql}"), dialect).unwrap();
            }
            let query = SemanticQuery {
                metrics: vec!["events.users".into()],
                ..Default::default()
            };
            let sql = generator.generate(&query).unwrap();
            assert!(sql.to_uppercase().contains(name), "{dialect}: {sql}");
            polyglot_sql::parse_one(&sql, dialect).unwrap();
            let filtered = generator
                .emit_expression(
                    &parse_semantic_expression(
                        "APPROX_COUNT_DISTINCT(user_id) FILTER (WHERE paid)",
                    )
                    .unwrap(),
                )
                .unwrap();
            assert!(filtered.contains("paid"), "{dialect}: {filtered}");
            assert!(
                filtered.to_uppercase().contains(name),
                "{dialect}: {filtered}"
            );
            polyglot_sql::parse_one(&format!("SELECT {filtered}"), dialect).unwrap();
        }
    }

    #[test]
    fn typed_function_nesting_cannot_hide_approximate_aggregates() {
        let graph = graph_with(json!({"name":"total", "agg":"sum", "sql":"amount"}));
        for (dialect, name) in output_dialects() {
            let generator = SqlGenerator::new(&graph).with_dialect(dialect);
            // Construct the typed node explicitly: parser versions may retain
            // some functions as generic nodes that the old visitor did handle.
            let typed_round = Expression::Round(Box::new(polyglot_sql::expressions::RoundFunc {
                this: parse_semantic_expression("COALESCE(APPROX_COUNT_DISTINCT(user_id), 0)")
                    .unwrap(),
                decimals: None,
            }));
            let rounded = generator.emit_expression(&typed_round).unwrap();
            assert!(
                rounded.to_uppercase().contains(name),
                "{dialect}: {rounded}"
            );
            for source in [
                "ROUND(COALESCE(APPROX_COUNT_DISTINCT(user_id), 0), 0)",
                "GREATEST(APPROX_COUNT_DISTINCT(user_id), 0)",
                "NULLIF(APPROX_COUNT_DISTINCT(user_id), 0)",
                "ROUND(APPROX_COUNT_DISTINCT(user_id) OVER (PARTITION BY region), 0)",
            ] {
                let sql = generator
                    .emit_expression(&parse_semantic_expression(source).unwrap())
                    .unwrap();
                assert!(sql.to_uppercase().contains(name), "{dialect}: {sql}");
                if name != "APPROX_COUNT_DISTINCT" {
                    assert!(
                        !sql.to_uppercase().contains("APPROX_COUNT_DISTINCT"),
                        "{dialect}: {sql}"
                    );
                }
                polyglot_sql::parse_one(&format!("SELECT {sql}"), dialect).unwrap();
            }
        }
    }

    fn graph_with(metric: serde_json::Value) -> SemanticGraph {
        let approximate: Metric = serde_json::from_value(json!({
            "name":"users", "agg":"approx_count_distinct", "sql":"user_id"
        }))
        .unwrap();
        let metric: Metric = serde_json::from_value(metric).unwrap();
        let mut graph = SemanticGraph::new();
        graph
            .add_model(
                Model::new("events", "id")
                    .with_table("events")
                    .with_metric(approximate)
                    .with_metric(metric),
            )
            .unwrap();
        graph.set_metric_scopes(HashMap::new()).unwrap();
        graph
    }

    #[test]
    fn approximate_leaves_do_not_block_special_route_validation() {
        for metric in [
            json!({"name":"special", "type":"cumulative", "agg":"approx_count_distinct", "sql":"user_id"}),
            json!({"name":"special", "type":"cumulative", "sql":"users"}),
            json!({"name":"special", "type":"cumulative", "window_expression":"SUM(base.users)"}),
            json!({"name":"special", "type":"time_comparison", "base_metric":"users", "comparison_type":"yoy"}),
            json!({"name":"special", "type":"cohort", "agg":"approx_count_distinct", "sql":"user_id"}),
            json!({"name":"special", "type":"cohort", "agg":"count", "inner_metrics":[{"name":"inner", "agg":"approx_count_distinct", "sql":"user_id"}]}),
            json!({"name":"special", "type":"simple", "agg":"approx_count_distinct", "sql":"user_id", "non_additive_dimension":"created_at"}),
        ] {
            let graph = graph_with(metric.clone());
            let query = SemanticQuery {
                metrics: vec!["events.special".into()],
                ..Default::default()
            };
            SqlGenerator::new(&graph)
                .validate_approximate_query(&query)
                .unwrap();
        }
    }

    #[test]
    fn unused_approximate_metric_does_not_claim_graph_calculations() {
        let mut graph = graph_with(json!({"name":"total", "agg":"sum", "sql":"amount"}));
        graph
            .add_model(
                Model::new("other", "id")
                    .with_table("other")
                    .with_relationship(
                        crate::core::Relationship::many_to_one("events")
                            .with_keys("event_id", "id"),
                    )
                    .with_metric(Metric::sum("total", "amount")),
            )
            .unwrap();
        graph.add_metric_unvalidated(serde_json::from_value(json!({
            "name":"ratio", "type":"ratio", "numerator":"events.total", "denominator":"other.total"
        })).unwrap()).unwrap();
        graph.set_metric_scopes(HashMap::new()).unwrap();
        let query = SemanticQuery {
            metrics: vec!["ratio".into()],
            ..Default::default()
        };
        let sql = SqlGenerator::new(&graph).generate(&query).unwrap();
        assert!(!sql.contains("APPROX_COUNT_DISTINCT"));
    }

    #[test]
    fn graph_calculation_can_use_approximate_leaf() {
        let mut graph = graph_with(json!({"name":"total", "agg":"sum", "sql":"amount"}));
        graph
            .add_metric_unvalidated(
                serde_json::from_value(json!({
                    "name":"derived", "type":"derived", "sql":"events.users + 1"
                }))
                .unwrap(),
            )
            .unwrap();
        graph.set_metric_scopes(HashMap::new()).unwrap();
        let query = SemanticQuery {
            metrics: vec!["derived".into()],
            ..Default::default()
        };
        let sql = SqlGenerator::new(&graph).generate(&query).unwrap();
        assert!(sql.contains("APPROX_COUNT_DISTINCT"), "{sql}");
    }

    #[test]
    fn physical_columns_and_cohort_outputs_are_not_metric_dependencies() {
        for metric in [
            json!({"name":"unrelated", "agg":"sum", "sql":"users"}),
            json!({"name":"unrelated", "type":"derived", "sql_is_complete":true, "sql":"SUM(users)"}),
            json!({"name":"unrelated", "type":"cohort", "agg":"sum", "sql":"users", "entity":"user_id", "inner_metrics":[{"name":"users", "agg":"sum", "sql":"amount"}]}),
        ] {
            let graph = graph_with(metric);
            let query = SemanticQuery {
                metrics: vec!["events.unrelated".into()],
                ..Default::default()
            };
            // These expressions belong to raw rows or cohort result rows even
            // though a model metric happens to share their identifier.
            SqlGenerator::new(&graph)
                .validate_approximate_query(&query)
                .unwrap();
        }
    }
}
