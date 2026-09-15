//! Imported calculations retain their source-measure and population semantics.
use super::*;

impl SqlGenerator<'_> {
    /// LookML post-SQL calculations encode base measures as aggregate inputs.
    /// Resolve only this explicit import contract; ordinary complete aggregates
    /// continue to address physical columns even when a metric shadows a name.
    pub(super) fn imported_calculation_expression(
        &self,
        metric: &Metric,
        model_name: &str,
    ) -> Result<String> {
        let calculation = metric
            .meta
            .as_ref()
            .and_then(|meta| meta.get("table_calculation"))
            .and_then(serde_json::Value::as_str);
        if !matches!(
            calculation,
            Some("percent_of_total" | "percent_of_previous")
        ) {
            return Ok(metric.sql_expr().to_string());
        }
        let sql = crate::core::replace_model_placeholder(metric.sql_expr(), Some(model_name))?;
        let mut replacements = HashMap::new();
        for column in semantic_column_references(&sql)? {
            if !column.aggregate_input {
                continue;
            }
            let owner = column.model.as_deref().unwrap_or(model_name);
            let Some(model) = self.graph.get_model(owner) else {
                continue;
            };
            let Some(base) = model.get_metric(&column.field) else {
                continue;
            };
            if base.r#type != MetricType::Simple {
                continue;
            }
            let mut input = self.metric_raw_expression(base, model)?;
            if !base.filters.is_empty() {
                let filters = base
                    .filters
                    .iter()
                    .map(|filter| format!("({filter})"))
                    .collect::<Vec<_>>();
                input = format!("CASE WHEN {} THEN {input} END", filters.join(" AND "));
            }
            let input = crate::core::replace_model_placeholder(&input, Some(owner))?;
            let qualified = semantic_column_references(&input)?
                .into_iter()
                .map(|reference| {
                    let name = format!(
                        "{}.{}",
                        self.quote_identifier(reference.model.as_deref().unwrap_or(owner)),
                        self.quote_identifier(&reference.field)
                    );
                    ((reference.model, reference.field), name)
                })
                .collect();
            let input = crate::core::replace_semantic_columns(
                parse_semantic_expression(&input)?,
                &qualified,
            )?;
            replacements.insert(
                (column.model, column.field),
                format!("({})", self.emit_expression(&input)?),
            );
        }
        self.emit_expression(&crate::core::replace_semantic_columns(
            parse_semantic_expression(&sql)?,
            &replacements,
        )?)
    }

    /// Reaggregate the expanded measure over the containing query's exact source
    /// before GROUP BY/HAVING/pagination. CTE-level policies and metric filters
    /// are retained, as are join predicates and residual WHERE filters.
    pub(super) fn expand_imported_totals(&self, projection: &str, source: &str) -> Result<String> {
        fn rewrite(
            generator: &SqlGenerator<'_>,
            value: &mut serde_json::Value,
            source: &str,
        ) -> Result<()> {
            if let Some(function) = value.get("function") {
                if function
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|name| name.eq_ignore_ascii_case("__bsl_all"))
                {
                    let expression: Expression = serde_json::from_value(value.clone())
                        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
                    let Expression::Function(function) = expression else {
                        unreachable!()
                    };
                    if function.args.len() != 1 {
                        return Err(SidemanticError::Validation(
                            "BSL all() requires one measure".into(),
                        ));
                    }
                    let aggregate = generator.emit_expression(&function.args[0])?;
                    *value = serde_json::to_value(Expression::Raw(Raw {
                        sql: format!("(SELECT {aggregate} {source})"),
                    }))
                    .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
                    return Ok(());
                }
            }
            match value {
                serde_json::Value::Object(fields) => {
                    for child in fields.values_mut() {
                        rewrite(generator, child, source)?;
                    }
                }
                serde_json::Value::Array(children) => {
                    for child in children {
                        rewrite(generator, child, source)?;
                    }
                }
                _ => {}
            }
            Ok(())
        }
        let statement =
            crate::semantic_input::dialects::parse(&format!("SELECT {projection}"), self.dialect)?;
        let Expression::Select(mut select) = statement else {
            unreachable!()
        };
        let expression = select.expressions.remove(0);
        let mut value = serde_json::to_value(expression)
            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
        rewrite(self, &mut value, source)?;
        let expression = serde_json::from_value(value)
            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
        self.emit_expression(&expression)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Dimension, Relationship};

    #[test]
    fn bsl_distinct_total_reaggregates_joined_filtered_population() {
        let mut graph = SemanticGraph::new();
        graph
            .add_model(
                Model::new("orders", "id")
                    .with_table("orders")
                    .with_dimension(Dimension::categorical("customer_id"))
                    .with_metric(Metric::count_distinct("users", "user_id"))
                    .with_metric(Metric::derived("share", "users / __bsl_all(users)"))
                    .with_relationship(
                        Relationship::many_to_one("customers").with_keys("customer_id", "id"),
                    ),
            )
            .unwrap();
        graph
            .add_model(
                Model::new("customers", "id")
                    .with_table("customers")
                    .with_dimension(Dimension::categorical("region")),
            )
            .unwrap();
        graph.set_metric_scopes(HashMap::new()).unwrap();
        let query = SemanticQuery::new()
            .with_metrics(vec!["orders.share".into()])
            .with_dimensions(vec!["customers.region".into()])
            .with_filters(vec!["customers.region = 'EU'".into()]);
        let sql = SqlGenerator::new(&graph).generate(&query).unwrap();
        assert!(!sql.to_ascii_lowercase().contains("__bsl_all"), "{sql}");
        assert!(
            sql.contains("SELECT (COUNT(DISTINCT orders_cte.users_raw))"),
            "{sql}"
        );
        // Both aggregates read the same CTEs and join edge. The region filter
        // may be pushed into the shared customers CTE, so it need not repeat.
        assert_eq!(sql.matches("JOIN orders_cte").count(), 2, "{sql}");
        assert!(sql.contains("'EU'"), "{sql}");
        polyglot_sql::parse_one(&sql, DialectType::DuckDB).unwrap();
    }

    #[test]
    fn lookml_post_sql_count_distinct_binds_base_measure_input() {
        let mut graph = SemanticGraph::new();
        let mut percentage = Metric::derived(
            "percentage",
            "COUNT(DISTINCT {model}.users) / NULLIF(SUM(COUNT(DISTINCT {model}.users)) OVER (), 0)",
        );
        percentage.meta = Some(serde_json::json!({"table_calculation":"percent_of_total"}));
        graph
            .add_model(
                Model::new("visits", "id")
                    .with_table("visits")
                    .with_dimension(Dimension::categorical("country"))
                    .with_metric(Metric::count_distinct("users", "user_id"))
                    .with_metric(percentage),
            )
            .unwrap();
        graph.set_metric_scopes(HashMap::new()).unwrap();
        let sql = SqlGenerator::new(&graph)
            .generate(
                &SemanticQuery::new()
                    .with_metrics(vec!["visits.percentage".into()])
                    .with_dimensions(vec!["visits.country".into()]),
            )
            .unwrap();
        assert!(
            sql.contains("COUNT(DISTINCT (visits_cte.user_id))"),
            "{sql}"
        );
        assert!(!sql.contains("visits_cte.users"), "{sql}");
        assert!(!sql.contains("users AS users"), "{sql}");
        polyglot_sql::parse_one(&sql, DialectType::DuckDB).unwrap();
    }

    #[test]
    fn lookml_post_sql_base_filters_preserve_conjunction_grouping() {
        let mut graph = SemanticGraph::new();
        let mut users = Metric::count_distinct("users", "user_id");
        users.filters = vec![
            "country = 'US' OR country = 'CA'".into(),
            "active = 1".into(),
        ];
        graph
            .add_model(
                Model::new("visits", "id")
                    .with_table("visits")
                    .with_metric(users),
            )
            .unwrap();
        let mut percentage = Metric::derived("percentage", "COUNT(DISTINCT {model}.users)");
        percentage.meta = Some(serde_json::json!({"table_calculation":"percent_of_total"}));
        let expression = SqlGenerator::new(&graph)
            .imported_calculation_expression(&percentage, "visits")
            .unwrap();
        assert!(
            expression.contains(
                "(visits.country = 'US' OR visits.country = 'CA') AND (visits.active = 1)"
            ),
            "{expression}"
        );
        // The same unmarked expression remains a physical aggregate input.
        percentage.meta = None;
        assert_eq!(
            SqlGenerator::new(&graph)
                .imported_calculation_expression(&percentage, "visits")
                .unwrap(),
            "COUNT(DISTINCT {model}.users)"
        );
    }
}
