//! Retention over one authorized source population with independently sized cohorts.

use super::*;
use crate::core::{replace_semantic_columns, validate_row_expression};

pub(super) fn unsupported(shape: &str) -> SidemanticError {
    SidemanticError::UnsupportedSemanticFeatures {
        capabilities: vec![format!("metric.retention_{shape}")],
    }
}

impl SqlGenerator<'_> {
    pub(crate) fn validate_retention_metric(metric: &Metric) -> Result<()> {
        if metric.r#type != MetricType::Retention {
            return Ok(());
        }
        if metric.entity.as_deref().is_none_or(str::is_empty)
            || metric.cohort_event.as_deref().is_none_or(str::is_empty)
        {
            return Err(SidemanticError::Validation(format!(
                "Retention metric {} requires entity and cohort_event",
                metric.name
            )));
        }
        if metric.periods == Some(0) {
            return Err(SidemanticError::Validation(
                "Invalid periods value: 0".into(),
            ));
        }
        if !matches!(
            metric.retention_granularity.as_deref(),
            None | Some("day" | "week" | "month")
        ) {
            return Err(SidemanticError::Validation(
                "Retention granularity must be day, week, or month".into(),
            ));
        }
        if metric.agg.is_some()
            || metric.sql.is_some()
            || metric.sql_is_complete
            || metric.numerator.is_some()
            || metric.denominator.is_some()
            || metric.offset_window.is_some()
            || metric.window.is_some()
            || metric.grain_to_date.is_some()
            || metric.window_expression.is_some()
            || metric.window_frame.is_some()
            || metric.window_order.is_some()
            || metric.base_metric.is_some()
            || metric.comparison_type.is_some()
            || metric.time_offset.is_some()
            || metric.calculation.is_some()
            || metric.base_event.is_some()
            || metric.conversion_event.is_some()
            || metric.conversion_window.is_some()
            || metric.steps.is_some()
            || metric.inner_metrics.is_some()
            || metric.entity_dimensions.is_some()
            || metric.having.is_some()
            || metric.non_additive_dimension.is_some()
        {
            return Err(unsupported("metric_shape"));
        }
        Ok(())
    }

    /// Resolve scalar source fields without editing SQL literals or reinterpreting
    /// already-expanded physical policy columns as semantic dimensions.
    fn retention_source_expression(
        &self,
        model: &Model,
        sql: &str,
        dimensions: bool,
    ) -> Result<String> {
        let sql = sql.replace("{model}", &model.name);
        let expression = parse_semantic_expression(&sql)?;
        validate_row_expression(&expression, "metric.retention_non_row_expression")?;
        let mut replacements = HashMap::new();
        for column in semantic_column_references(&sql)? {
            if column.aggregate_input {
                return Err(unsupported("aggregate_expression"));
            }
            if column
                .model
                .as_deref()
                .is_some_and(|owner| owner != model.name)
            {
                return Err(unsupported("cross_model_expression"));
            }
            let value = if dimensions {
                if let Some(dimension) = model.get_dimension(&column.field) {
                    if dimension.window.is_some() {
                        return Err(unsupported("window_dimension"));
                    }
                    self.retention_source_expression(model, dimension.sql_expr(), false)?
                } else if model.get_metric(&column.field).is_some()
                    || self.graph.get_metric(&column.field).is_some()
                {
                    return Err(unsupported("aggregate_filter"));
                } else {
                    self.quote_identifier(&column.field)
                }
            } else {
                self.quote_identifier(&column.field)
            };
            replacements.insert((column.model, column.field), format!("({value})"));
        }
        self.emit_expression(&replace_semantic_columns(expression, &replacements)?)
    }

    pub(super) fn generate_retention_query(
        &self,
        reference: &MetricRef,
        query: &SemanticQuery,
    ) -> Result<String> {
        if reference.graph_metric {
            return Err(unsupported("graph_scope"));
        }
        if !query.dimensions.is_empty() || query.ungrouped || !query.table_calculations.is_empty() {
            return Err(unsupported("query_shape"));
        }
        let model = self.graph.get_model(&reference.model).ok_or_else(|| {
            SidemanticError::Validation(format!("Unknown retention source '{}'", reference.model))
        })?;
        let metric = self.metric_for_ref(reference)?;
        Self::validate_retention_metric(metric)?;
        if query
            .prepared_policies
            .model_names()
            .any(|owner| owner != &reference.model)
        {
            return Err(unsupported("joined_population"));
        }
        self.ensure_queryable_sources(&HashSet::from([reference.model.clone()]))?;
        let timestamp = self.default_time_dimension(model).ok_or_else(|| {
            SidemanticError::Validation(
                "Retention metrics require a time dimension on the model".into(),
            )
        })?;
        if timestamp.window.is_some() {
            return Err(unsupported("window_dimension"));
        }
        let entity = metric.entity.as_deref().unwrap();
        // The entity names a dimension or physical column, not an arbitrary SQL
        // expression. A declared dimension may itself carry a scalar expression.
        if entity.contains('.') {
            return Err(unsupported("entity_reference"));
        }
        self.validate_identifier(entity, "entity")?;
        let entity_sql = self.retention_source_expression(model, entity, true)?;
        let timestamp_sql = self.retention_source_expression(model, timestamp.sql_expr(), false)?;
        let granularity = metric.retention_granularity.as_deref().unwrap_or("day");
        let periods = metric.periods.unwrap_or(28);
        // Both operands are DATE values projected below. Preserve Python's
        // explicit date-difference syntax and operand order for these targets.
        let day_difference = match self.dialect {
            DialectType::BigQuery => "DATE_DIFF(a.active_date, c.cohort_date, DAY)",
            DialectType::Snowflake => "DATEDIFF('day', c.cohort_date, a.active_date)",
            _ => "(a.active_date - c.cohort_date)",
        };
        let (date_sql, difference, label) = match granularity {
            "day" => (
                format!("CAST({timestamp_sql} AS DATE)"),
                day_difference.to_string(),
                "days_since",
            ),
            "week" => (
                format!(
                    "CAST({} AS DATE)",
                    self.date_trunc_sql("week", &timestamp_sql)?
                ),
                format!("({day_difference}) / 7"),
                "weeks_since",
            ),
            "month" => (
                format!(
                    "CAST({} AS DATE)",
                    self.date_trunc_sql("month", &timestamp_sql)?
                ),
                concat!(
                    "(EXTRACT(YEAR FROM a.active_date) - EXTRACT(YEAR FROM c.cohort_date)) * 12",
                    " + EXTRACT(MONTH FROM a.active_date) - EXTRACT(MONTH FROM c.cohort_date)",
                )
                .to_string(),
                "months_since",
            ),
            _ => unreachable!("validated retention granularity"),
        };
        let cohort = self.retention_source_expression(
            model,
            metric.cohort_event.as_deref().unwrap(),
            false,
        )?;
        let activity = self.retention_source_expression(
            model,
            metric
                .activity_event
                .as_deref()
                .filter(|sql| !sql.is_empty())
                .unwrap_or("TRUE"),
            false,
        )?;
        let mut predicates = Vec::new();
        for filter in query.filters.iter().chain(&metric.filters) {
            predicates.push(self.retention_source_expression(model, filter, true)?);
        }
        for filter in self.resolve_segments(&query.segments)? {
            predicates.push(self.retention_source_expression(model, &filter, true)?);
        }
        // Prepared policies are physical predicates. Expanding them as semantic
        // fields again can change their meaning when names collide.
        for predicate in query.prepared_policies.filters_for_model(&reference.model) {
            predicates.push(self.retention_source_expression(model, predicate, false)?);
        }
        let source_filter = if predicates.is_empty() {
            String::new()
        } else {
            format!(
                "\n  WHERE {}",
                predicates
                    .iter()
                    .map(|p| format!("({p})"))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            )
        };
        let source = self.model_from_clause(model, Some("t"));
        let mut sql = format!(
            r#"WITH retention_source AS (
  SELECT {entity_sql} AS __entity, {date_sql} AS __date,
    ({cohort}) AS __cohort, ({activity}) AS __activity
  FROM {source}{source_filter}
),
cohorts AS (
  SELECT __entity, MIN(__date) AS cohort_date
  FROM retention_source
  WHERE __cohort
  GROUP BY __entity
),
activity AS (
  SELECT DISTINCT __entity, __date AS active_date
  FROM retention_source
  WHERE __activity
),
retention AS (
  SELECT c.cohort_date, CAST({difference} AS INTEGER) AS periods_since,
    COUNT(DISTINCT c.__entity) AS active_users
  FROM cohorts c
  JOIN activity a ON c.__entity = a.__entity AND a.active_date >= c.cohort_date
  WHERE CAST({difference} AS INTEGER) <= {periods}
  GROUP BY 1, 2
),
cohort_sizes AS (
  SELECT cohort_date, COUNT(DISTINCT __entity) AS cohort_size
  FROM cohorts
  GROUP BY 1
)
SELECT r.cohort_date, r.periods_since AS {label}, r.active_users, c.cohort_size,
  ROUND(r.active_users * 100.0 / c.cohort_size, 1) AS retention_pct
FROM retention r
JOIN cohort_sizes c USING (cohort_date)"#
        );
        let outputs = [
            "cohort_date",
            label,
            "active_users",
            "cohort_size",
            "retention_pct",
        ];
        let mut ordering = Vec::new();
        for item in &query.order_by {
            let (field, direction) = crate::sql::split_order_field(item, &outputs);
            if !outputs.contains(&field) {
                return Err(unsupported("order_by"));
            }
            ordering.push(format!("{} {direction}", self.quote_identifier(field)));
        }
        if ordering.is_empty() {
            sql.push_str("\nORDER BY r.cohort_date, r.periods_since");
        } else {
            sql.push_str(&format!("\nORDER BY {}", ordering.join(", ")));
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
    use serde_json::{json, Value};

    fn input() -> Value {
        json!({
            "version": 1,
            "input_dialect": "duckdb",
            "models": [{
                "name": "events", "table": "events", "primary_key": "id",
                "security": {"row_filters": ["tenant = {{ user.tenant }}"]},
                "invariant_filters": ["deleted = false"],
                "dimensions": [
                    {"name": "person", "type": "categorical", "sql": "user_id"},
                    {"name": "time", "type": "time", "sql": "event_time"}
                ],
                "metrics": [{"name": "retained", "type": "retention", "entity": "person",
                    "cohort_event": "event = 'signup'", "activity_event": "event = 'active'",
                    "periods": 3}]
            }]
        })
    }

    fn query() -> Value {
        json!({"metrics": ["events.retained"], "user_attributes": {"tenant": 1}})
    }

    #[test]
    fn retention_source_applies_policies_before_both_populations() {
        let sql = compile_with_semantic_input(&input().to_string(), &query().to_string()).unwrap();
        assert!(sql.contains("retention_source AS"), "{sql}");
        assert!(sql.contains("tenant"), "{sql}");
        assert!(sql.contains("deleted"), "{sql}");
        assert!(sql.contains("user_id"), "{sql}");
        assert_eq!(sql.matches("FROM retention_source").count(), 2);
        polyglot_sql::parse_one(&sql, DialectType::DuckDB).unwrap();
    }

    #[test]
    fn source_field_literals_are_not_rewritten_as_dimensions() {
        let mut query = query();
        query["filters"] = json!(["person = 'person' OR person = 'events.person'"]);
        let sql = compile_with_semantic_input(&input().to_string(), &query.to_string()).unwrap();
        assert!(sql.contains("'person'"), "{sql}");
        assert!(sql.contains("'events.person'"), "{sql}");
        assert!(sql.contains("user_id"), "{sql}");
    }

    #[test]
    fn prepared_physical_policy_is_not_expanded_again() {
        let mut input = input();
        input["models"][0]["dimensions"]
            .as_array_mut()
            .unwrap()
            .push(json!({"name": "tenant", "type": "numeric", "sql": "tenant + 1"}));
        let sql = compile_with_semantic_input(&input.to_string(), &query().to_string()).unwrap();
        // Policy preparation already expands tenant to tenant+1 exactly once.
        assert_eq!(sql.matches("+ 1").count(), 1, "{sql}");
    }

    #[test]
    fn unsupported_dimensions_and_aggregate_predicates_fail_closed() {
        for (field, value) in [
            ("dimensions", json!(["events.person"])),
            ("filters", json!(["COUNT(*) > 1"])),
            ("filters", json!(["other.id = 1"])),
        ] {
            let mut query = query();
            query[field] = value;
            assert!(matches!(
                compile_with_semantic_input(&input().to_string(), &query.to_string()),
                Err(SidemanticError::UnsupportedSemanticFeatures { .. })
            ));
        }
    }

    #[test]
    fn retention_window_filter_is_rejected_at_query_boundary() {
        let mut query = query();
        query["filters"] = json!(["ROW_NUMBER() OVER () > 1"]);
        let error =
            compile_with_semantic_input(&input().to_string(), &query.to_string()).unwrap_err();
        assert!(
            matches!(
                error,
                SidemanticError::ValidationIssue { ref code, ref field, .. }
                    if code == "invalid_semantic_input" && field == "query.filters"
            ),
            "{error}"
        );
    }

    #[test]
    fn retention_requires_valid_period_and_granularity() {
        for (field, value) in [
            ("periods", json!(0)),
            ("retention_granularity", json!("hour")),
        ] {
            let mut input = input();
            input["models"][0]["metrics"][0][field] = value;
            assert!(matches!(
                compile_with_semantic_input(&input.to_string(), &query().to_string()),
                Err(SidemanticError::Validation(_))
            ));
        }
    }
    #[test]
    fn retention_compiles_calendar_periods_for_supported_output_dialects() {
        for (dialect, dialect_type) in [
            ("duckdb", DialectType::DuckDB),
            ("postgres", DialectType::PostgreSQL),
            ("bigquery", DialectType::BigQuery),
            ("snowflake", DialectType::Snowflake),
        ] {
            for grain in ["day", "week", "month"] {
                let mut input = input();
                input["models"][0]["metrics"][0]["retention_granularity"] = json!(grain);
                let mut query = query();
                query["dialect"] = json!(dialect);
                let sql =
                    compile_with_semantic_input(&input.to_string(), &query.to_string()).unwrap();
                polyglot_sql::parse_one(&sql, dialect_type).unwrap();
                assert!(sql.contains(&format!("{grain}s_since")), "{sql}");
                if grain != "month" {
                    let difference = match dialect_type {
                        DialectType::BigQuery => "DATE_DIFF(a.active_date, c.cohort_date, DAY)",
                        DialectType::Snowflake => "DATEDIFF('day', c.cohort_date, a.active_date)",
                        _ => "(a.active_date - c.cohort_date)",
                    };
                    assert!(sql.contains(difference), "{sql}");
                }
            }
        }
    }
}
