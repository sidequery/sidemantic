//! Query options apply at the semantic input and final result boundaries.
use super::*;
use crate::core::replace_semantic_columns;

impl SqlGenerator<'_> {
    pub(super) fn reject_totals_route(&self, query: &SemanticQuery, route: &str) -> Result<()> {
        if query.with_totals {
            return Err(SidemanticError::UnsupportedSemanticFeatures {
                capabilities: vec![format!("query.totals.{route}")],
            });
        }
        Ok(())
    }

    /// Resolve the public output names before renaming. Internal CTE, metric,
    /// snapshot and window references must retain their original names.
    pub(super) fn selected_aliases(
        &self,
        query: &SemanticQuery,
    ) -> Result<HashMap<String, String>> {
        let dimensions = if query.skip_default_time_dimensions {
            query.dimensions.clone()
        } else {
            // Cross-source graph calculations have no single default-time
            // owner. Their planner owns that eligibility check; resolving a
            // presentation alias must not force them through owner inference.
            let mut default_metrics = Vec::new();
            for reference in &query.metrics {
                if let Some(metric) = self.graph.get_metric(reference) {
                    if self.graph_metric_owner_models(reference, metric)?.len() > 1 {
                        continue;
                    }
                }
                default_metrics.push(reference.clone());
            }
            self.apply_default_time_dimensions(&default_metrics, &query.dimensions)?
        };
        let mut fields = Vec::new();
        for (reference, dimension) in dimensions
            .iter()
            .zip(self.parse_dimension_refs(&dimensions)?)
        {
            fields.push((reference.clone(), dimension.model, dimension.alias));
        }
        for reference in &query.metrics {
            // Graph metrics can deliberately have no model owner and their names
            // may contain dots. Do not reinterpret those names as model paths.
            let (model, name) = if let Some(metric) = self.graph.get_metric(reference) {
                let owners = self.graph_metric_owner_models(reference, metric)?;
                (
                    self.graph
                        .metric_owner(reference)
                        .map(str::to_owned)
                        .or_else(|| (owners.len() == 1).then(|| owners[0].clone()))
                        .unwrap_or_default(),
                    metric.name.clone(),
                )
            } else if let Some((model, name)) = reference.split_once('.') {
                (model.to_owned(), name.to_owned())
            } else {
                let metric = self
                    .parse_metric_refs(std::slice::from_ref(reference))?
                    .remove(0);
                (metric.model, metric.name)
            };
            fields.push((reference.clone(), model, name));
        }
        let mut collisions = HashMap::new();
        for (_, _, name) in &fields {
            *collisions.entry(name.clone()).or_insert(0) += 1;
        }
        Ok(fields
            .into_iter()
            .filter_map(|(reference, model, name)| {
                query
                    .aliases
                    .get(&reference)
                    .map(|alias| (self.output_alias(&model, &name, &collisions), alias.clone()))
            })
            .collect())
    }

    pub(super) fn generate_with_options(&self, query: &SemanticQuery) -> Result<String> {
        if query.aliases.is_empty()
            && !query.with_totals
            && query.timezone.is_none()
            && self.timezone.is_none()
        {
            return self.generate_from_model(query, None);
        }
        timezone::validate_query_timezone(query.timezone.as_deref().or(self.timezone.as_deref()))?;
        if query.with_totals && (query.limit.is_some() || query.offset.is_some()) {
            return Err(SidemanticError::Validation(
                "with_totals cannot be combined with limit/offset".into(),
            ));
        }
        if query.with_totals && query.ungrouped {
            return Err(SidemanticError::Validation(
                "with_totals cannot be combined with ungrouped".into(),
            ));
        }
        let generator = SqlGenerator {
            graph: self.graph,
            dialect: self.dialect,
            timezone: query.timezone.clone().or_else(|| self.timezone.clone()),
        };
        let mut inner = query.clone();
        inner.timezone = generator.timezone.clone();
        if generator
            .timezone
            .as_deref()
            .is_some_and(|timezone| !timezone.is_empty())
        {
            // Rollups store UTC buckets; local wall-clock buckets need raw rows.
            inner.use_preaggregations = false;
        }
        if query.aliases.is_empty() {
            return generator.generate_from_model(&inner, None);
        }
        let aliases = generator.selected_aliases(query)?;
        // A caller may order by a custom output name. Resolve it back before
        // compiling so specialized planners see the same semantic field refs.
        let names: Vec<_> = query.aliases.values().map(String::as_str).collect();
        for item in &mut inner.order_by {
            let (field, suffix) = crate::sql::split_order_field(item, &names);
            if let Some((reference, _)) = query
                .aliases
                .iter()
                .find(|(_, alias)| alias.as_str() == field)
            {
                *item = format!("{reference} {suffix}").trim_end().to_owned();
            }
        }
        inner.aliases.clear();
        let sql = generator.generate_from_model(&inner, None)?;
        generator.alias_result(sql, &aliases)
    }

    fn alias_result(&self, sql: String, aliases: &HashMap<String, String>) -> Result<String> {
        if aliases.is_empty() {
            return Ok(sql);
        }
        #[cfg(target_arch = "wasm32")]
        crate::wasm_sql_guard::check(&sql, self.dialect)?;
        let Expression::Select(select) = crate::semantic_input::dialects::parse(&sql, self.dialect)
            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?
        else {
            return Err(SidemanticError::UnsupportedSemanticFeatures {
                capabilities: vec!["query.aliases.result_projection".into()],
            });
        };
        let mut projections = Vec::new();
        let mut replacements = HashMap::new();
        let mut renamed = false;
        let quote =
            |name: &str| self.emit_expression(&Expression::Identifier(Identifier::quoted(name)));
        for expression in &select.expressions {
            let name = match expression {
                Expression::Alias(alias) => &alias.alias.name,
                Expression::Column(column) => &column.name.name,
                _ => {
                    return Err(SidemanticError::UnsupportedSemanticFeatures {
                        capabilities: vec!["query.aliases.result_projection".into()],
                    })
                }
            };
            let alias = aliases.get(name).unwrap_or(name);
            renamed |= alias != name;
            projections.push(format!(
                "__sidemantic_result.{} AS {}",
                quote(name)?,
                quote(alias)?
            ));
            replacements.insert((None, name.clone()), quote(alias)?);
            let source = match expression {
                Expression::Alias(alias) => &alias.this,
                other => other,
            };
            if let Expression::Column(column) = source {
                replacements.insert(
                    (
                        column.table.as_ref().map(|table| table.name.clone()),
                        column.name.name.clone(),
                    ),
                    quote(alias)?,
                );
            }
        }
        if !renamed {
            return Ok(sql);
        }
        let wrapper = format!(
            "SELECT {} FROM ({sql}) AS __sidemantic_result",
            projections.join(", ")
        );
        #[cfg(target_arch = "wasm32")]
        crate::wasm_sql_guard::check(&wrapper, self.dialect)?;
        let Expression::Select(mut outer) =
            crate::semantic_input::dialects::parse(&wrapper, self.dialect)
                .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?
        else {
            unreachable!()
        };
        outer.order_by = select.order_by;
        if let Some(order) = &mut outer.order_by {
            for item in &mut order.expressions {
                item.this = replace_semantic_columns(item.this.clone(), &replacements)?;
            }
        }
        self.emit_expression(&Expression::Select(outer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::Dimension;
    use crate::semantic_input::{compile_with_semantic_input, validate_with_semantic_input};
    use serde_json::json;

    fn graph() -> SemanticGraph {
        let mut graph = SemanticGraph::new();
        graph
            .add_model(
                Model::new("orders", "id")
                    .with_table("orders")
                    .with_dimension(Dimension::categorical("category"))
                    .with_metric(Metric::sum("revenue", "amount")),
            )
            .unwrap();
        graph
    }

    #[test]
    fn aliases_and_totals_match_result_schema() {
        let graph = graph();
        let generator = SqlGenerator::new(&graph);
        let mut query = SemanticQuery::new()
            .with_metrics(vec!["orders.revenue".into()])
            .with_dimensions(vec!["orders.category".into()])
            .with_order_by(vec!["total DESC".into()]);
        query.aliases = HashMap::from([
            ("orders.revenue".into(), "total".into()),
            ("orders.category".into(), "Category label".into()),
        ]);
        query.with_totals = true;
        let sql = generator.generate(&query).unwrap();
        assert!(sql.contains("GROUPING SETS"), "{sql}");
        assert!(sql.contains("GROUPING("), "{sql}");
        assert!(sql.contains("ORDER BY \"total\" DESC"), "{sql}");
        assert_eq!(
            generator
                .result_schema(&query)
                .unwrap()
                .into_iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>(),
            ["Category label", "total", "_is_total"]
        );
    }

    #[test]
    fn semantic_boundary_binds_spaced_order_aliases_before_policy_parsing() {
        let source = json!({"version": 1, "input_dialect": "duckdb", "models": [{"name": "orders", "table": "orders", "primary_key": "id", "dimensions": [{"name": "category", "type": "categorical"}], "metrics": [{"name": "revenue", "agg": "sum", "sql": "amount"}]}]}).to_string();
        for alias in ["Category label", "Category DESC", "Category NULLS FIRST"] {
            for suffix in ["", " DESC", " ASC NULLS FIRST", "\tDESC\tNULLS\tLAST"] {
                for dialect in ["duckdb", "postgres"] {
                    let query = json!({
                        "metrics": ["orders.revenue"], "dimensions": ["orders.category"],
                        "aliases": {"orders.category": alias},
                        "order_by": [format!("{alias}{suffix}")], "limit": 2,
                        "query_dialect": dialect, "dialect": dialect
                    });
                    let sql = compile_with_semantic_input(&source, &query.to_string()).unwrap();
                    assert!(sql.contains(&format!("ORDER BY \"{alias}\"")), "{sql}");
                    assert!(sql.contains("LIMIT 2"), "{sql}");
                }
            }
        }
    }

    #[test]
    fn malformed_options_and_totals_controls_are_invalid_on_both_boundaries() {
        let source = json!({"version": 1, "input_dialect": "duckdb", "models": [{"name": "orders", "table": "orders", "primary_key": "id", "dimensions": [{"name": "category", "type": "categorical"}], "metrics": [{"name": "revenue", "agg": "sum", "sql": "amount"}]}]}).to_string();
        for extra in [
            json!({"aliases": []}),
            json!({"aliases": {"orders.revenue": 4}}),
            json!({"timezone": 5}),
            json!({"timezone": "UTC'; select 1"}),
            json!({"with_totals": "yes"}),
            json!({"with_totals": true, "limit": 1}),
            json!({"with_totals": true, "ungrouped": true}),
        ] {
            let mut query =
                json!({"metrics": ["orders.revenue"], "dimensions": ["orders.category"]});
            query
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            for result in [
                compile_with_semantic_input(&source, &query.to_string()).map(|_| ()),
                validate_with_semantic_input(&source, &query.to_string()).map(|_| ()),
            ] {
                assert!(result.is_err(), "{query}");
                assert!(
                    !matches!(
                        result,
                        Err(SidemanticError::UnsupportedSemanticFeatures { .. })
                    ),
                    "{query}"
                );
            }
        }
    }
}
