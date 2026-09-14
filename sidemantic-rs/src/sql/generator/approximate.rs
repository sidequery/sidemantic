//! Validate approximate distinct inputs without restricting ordinary join plans.
use super::*;
use crate::core::validate_row_expression;

fn unsupported(shape: &str) -> SidemanticError {
    SidemanticError::UnsupportedSemanticFeatures {
        capabilities: vec![format!("metric.approx_count_distinct_{shape}")],
    }
}

impl SqlGenerator<'_> {
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
                if self.dialect != DialectType::DuckDB {
                    return Err(unsupported("dialect"));
                }
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
