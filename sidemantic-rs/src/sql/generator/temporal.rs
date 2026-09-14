//! Calendar-based windows used by the existing temporal query generator.

use super::*;
use crate::core::{ComparisonType, TimeGrain};

fn unsupported(feature: &str) -> SidemanticError {
    SidemanticError::UnsupportedSemanticFeatures {
        capabilities: vec![format!("metric.{feature}")],
    }
}

pub(crate) fn validate_metric(metric: &Metric) -> Result<()> {
    if metric.r#type != MetricType::Cumulative
        && (metric.window_expression.is_some()
            || metric.window_frame.is_some()
            || metric.window_order.is_some())
    {
        return Err(unsupported("window_fields_without_cumulative"));
    }
    if metric.r#type == MetricType::Cumulative {
        if !matches!(
            metric.agg,
            None | Some(Aggregation::Sum | Aggregation::Avg | Aggregation::Min | Aggregation::Max)
        ) {
            return Err(unsupported("cumulative_aggregation"));
        }
        if let Some(expression) = &metric.window_expression {
            if metric.window.is_some() || metric.grain_to_date.is_some() {
                return Err(unsupported("window_expression_controls"));
            }
            window_output_reference(expression)?;
        }
        if let Some(frame) = &metric.window_frame {
            if metric.window.is_some() || metric.grain_to_date.is_some() {
                return Err(unsupported("window_frame_controls"));
            }
            validate_output_frame(frame)?;
        }
        if let Some(window) = &metric.window {
            period_interval(window)?;
        }
    }
    if let Some(offset) = &metric.time_offset {
        period_interval(offset)?;
    }
    if let Some(offset) = &metric.offset_window {
        period_interval(offset)?;
    }
    Ok(())
}

/// The promoted contract consumes one grouped output, never a physical row column.
pub(super) fn window_output_reference(expression: &str) -> Result<String> {
    let pattern = regex::Regex::new(
        r#"(?i)^\s*(?:SUM|AVG|MIN|MAX|COUNT)\s*\(\s*base\.(?:([A-Za-z_][A-Za-z0-9_]*)|"([A-Za-z_][A-Za-z0-9_]*)")\s*\)\s*$"#,
    ).expect("valid period output expression pattern");
    let captures = pattern
        .captures(expression)
        .ok_or_else(|| unsupported("window_expression"))?;
    Ok(captures
        .get(1)
        .or_else(|| captures.get(2))
        .unwrap()
        .as_str()
        .replace("\"\"", "\""))
}

fn validate_output_frame(frame: &str) -> Result<()> {
    let pattern = regex::Regex::new(
        r"(?i)^\s*(?:ROWS\s+BETWEEN\s+(?:UNBOUNDED|[0-9]+)\s+PRECEDING|RANGE\s+BETWEEN\s+(?:UNBOUNDED|INTERVAL\s+(?:'[0-9]+\s+(?:DAY|WEEK|MONTH|YEAR)S?'|[0-9]+\s+(?:DAY|WEEK|MONTH|YEAR)S?))\s+PRECEDING)\s+AND\s+CURRENT\s+ROW\s*$",
    ).expect("valid period output frame pattern");
    if pattern.is_match(frame) {
        Ok(())
    } else {
        Err(unsupported("window_frame"))
    }
}

fn period_interval(value: &str) -> Result<(u32, String)> {
    let parts: Vec<_> = value.split_whitespace().collect();
    let invalid = || SidemanticError::Validation(format!("Invalid temporal interval '{value}'"));
    if parts.len() != 2 {
        return Err(invalid());
    }
    let amount = parts[0].parse::<u32>().map_err(|_| invalid())?;
    if amount == 0 {
        return Err(invalid());
    }
    let unit = parts[1].to_lowercase();
    let unit = unit.trim_end_matches('s');
    match unit {
        "day" | "week" | "month" | "year" => Ok((amount, unit.to_string())),
        "quarter" => Ok((amount.checked_mul(3).ok_or_else(invalid)?, "month".into())),
        _ => Err(invalid()),
    }
}

impl SqlGenerator<'_> {
    pub(crate) fn window_output_dependency(metric: &Metric) -> Result<Option<String>> {
        metric
            .window_expression
            .as_deref()
            .map(|expression| {
                let name = window_output_reference(expression)?;
                polyglot_sql::generate(
                    &Expression::Identifier(Identifier::quoted(name)),
                    DialectType::DuckDB,
                )
                .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))
            })
            .transpose()
    }

    pub(crate) fn validate_temporal_metric(metric: &Metric) -> Result<()> {
        validate_metric(metric)
    }

    pub(super) fn temporal_partition_columns(
        &self,
        dimensions: &[DimensionRef],
        time_column: &str,
    ) -> Vec<String> {
        let mut columns = Vec::new();
        for dimension in dimensions {
            let is_time = self
                .graph
                .get_model(&dimension.model)
                .and_then(|model| model.get_dimension(&dimension.name))
                .is_some_and(|dimension| dimension.r#type == crate::core::DimensionType::Time);
            let column = format!("base.{}", self.quote_identifier(&dimension.alias));
            if !is_time && column != time_column && !columns.contains(&column) {
                columns.push(column);
            }
        }
        columns
    }

    pub(super) fn cumulative_window_sql(
        &self,
        metric: &Metric,
        dimensions: &[DimensionRef],
        time_column: &str,
    ) -> Result<String> {
        if let Some(frame) = &metric.window_frame {
            if metric.window.is_some() || metric.grain_to_date.is_some() {
                return Err(unsupported("window_frame_controls"));
            }
            validate_output_frame(frame)?;
        }
        let mut partitions = self.temporal_partition_columns(dimensions, time_column);
        if let Some(grain) = &metric.grain_to_date {
            let grain = match grain {
                TimeGrain::Day => "day",
                TimeGrain::Week => "week",
                TimeGrain::Month => "month",
                TimeGrain::Quarter => "quarter",
                TimeGrain::Year => "year",
            };
            partitions.push(format!("DATE_TRUNC('{grain}', {time_column})"));
        }
        let partition = if partitions.is_empty() {
            String::new()
        } else {
            format!("PARTITION BY {} ", partitions.join(", "))
        };
        let frame = if let Some(frame) = &metric.window_frame {
            frame.clone()
        } else if metric.grain_to_date.is_none() {
            if let Some(window) = &metric.window {
                let (amount, unit) = period_interval(window)?;
                format!("RANGE BETWEEN INTERVAL '{amount} {unit}' PRECEDING AND CURRENT ROW")
            } else {
                "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW".into()
            }
        } else {
            "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW".into()
        };
        Ok(format!("{partition}ORDER BY {time_column} {frame}"))
    }

    pub(super) fn calendar_prior_value(
        &self,
        metric: &Metric,
        dimensions: &[DimensionRef],
        time_column: &str,
        granularity: Option<&str>,
        value: &str,
    ) -> Result<Option<String>> {
        let interval = if let Some(offset) = metric
            .offset_window
            .as_deref()
            .or(metric.time_offset.as_deref())
        {
            Some(period_interval(offset)?)
        } else {
            match metric
                .comparison_type
                .as_ref()
                .unwrap_or(&ComparisonType::PriorPeriod)
            {
                ComparisonType::Dod => Some((1, "day".into())),
                ComparisonType::Wow => Some((1, "week".into())),
                ComparisonType::Mom => Some((1, "month".into())),
                ComparisonType::Qoq => Some((3, "month".into())),
                ComparisonType::Yoy => Some((1, "year".into())),
                ComparisonType::PriorPeriod => granularity
                    .map(|grain| period_interval(&format!("1 {grain}")))
                    .transpose()?,
            }
        };
        let Some((amount, unit)) = interval else {
            return Ok(None);
        };
        if !matches!(self.dialect, DialectType::DuckDB | DialectType::PostgreSQL) {
            return Err(unsupported("calendar_dialect"));
        }
        let partition_columns = self.temporal_partition_columns(dimensions, time_column);
        let partition = if partition_columns.is_empty() {
            String::new()
        } else {
            format!("PARTITION BY {} ", partition_columns.join(", "))
        };
        Ok(Some(format!(
            "MAX({value}) OVER ({partition}ORDER BY {time_column} RANGE BETWEEN INTERVAL '{amount} {unit}' PRECEDING AND INTERVAL '{amount} {unit}' PRECEDING)"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filled_temporal_metrics_reject_controls_their_generator_does_not_use() {
        for (kind, field, value) in [
            ("cumulative", "offset_window", "1 day"),
            ("cumulative", "time_offset", "1 day"),
            ("time_comparison", "window", "1 day"),
            ("time_comparison", "grain_to_date", "month"),
        ] {
            let mut metric = serde_json::json!({"name":"filled", "type":kind,
                "sql":"sales.revenue", "base_metric":"sales.revenue", "fill_nulls_with":0});
            metric[field] = serde_json::json!(value);
            let input = serde_json::json!({"version":1,"input_dialect":"duckdb",
                "models":[],"metrics":[metric.clone()],"metric_owners":{}});
            assert!(
                matches!(
                    crate::semantic_input::SemanticInput::from_json(&input.to_string()),
                    Err(SidemanticError::UnsupportedSemanticFeatures { .. })
                ),
                "{kind}.{field}"
            );
            let mut direct: Metric = serde_json::from_value(metric).unwrap();
            assert!(
                SqlGenerator::validate_metric_fill(&direct).is_err(),
                "{kind}.{field}"
            );
            direct.fill_nulls_with = None;
            assert!(
                SqlGenerator::validate_metric_fill(&direct).is_ok(),
                "{kind}.{field}"
            );
        }
    }

    #[test]
    fn temporal_fills_wrap_final_values_and_keep_other_shapes_unsupported() {
        let (graph, _) = grouped_graph();
        let generator = SqlGenerator::new(&graph);
        let mut cumulative = Metric::cumulative("running", "sales.revenue");
        cumulative.window = Some("2 months".into());
        cumulative.fill_nulls_with = Some(serde_json::json!(-9));
        assert_eq!(
            generator
                .fill_metric_expression(&cumulative, "SUM(base.revenue) OVER ()".into())
                .unwrap(),
            "COALESCE(SUM(base.revenue) OVER (), -9)"
        );
        cumulative.window = Some("0 days".into());
        assert!(validate_metric(&cumulative).is_err());
        let mut comparison =
            Metric::time_comparison("change", "sales.revenue", ComparisonType::Mom);
        comparison.fill_nulls_with = Some(serde_json::json!("missing's value"));
        assert_eq!(
            generator
                .fill_metric_expression(&comparison, "value / NULLIF(prior, 0)".into())
                .unwrap(),
            "COALESCE(value / NULLIF(prior, 0), 'missing''s value')"
        );
        for kind in [MetricType::Conversion, MetricType::Retention] {
            comparison.r#type = kind;
            assert!(SqlGenerator::validate_metric_fill(&comparison).is_err());
        }
        let mut simple = Metric::sum("snapshot", "amount");
        simple.fill_nulls_with = Some(serde_json::json!(0));
        simple.non_additive_dimension = Some("day".into());
        assert!(SqlGenerator::validate_metric_fill(&simple).is_ok());
        simple.non_additive_dimension = None;
        simple.r#type = MetricType::Ratio;
        simple.offset_window = Some("1 day".into());
        assert!(SqlGenerator::validate_metric_fill(&simple).is_ok());
    }

    #[test]
    fn graph_temporal_metrics_compile_through_handoff_and_preserve_mixed_outputs() {
        let input = serde_json::json!({
            "version": 1,
            "input_dialect": "duckdb",
            "models": [{"name": "sales", "table": "sales", "primary_key": "id",
                "dimensions": [{"name": "day", "type": "time", "granularity": "day"},
                    {"name": "category", "type": "categorical"}],
                "metrics": [{"name": "revenue", "agg": "sum", "sql": "amount"}]}],
            "metrics": [
                {"name": "month_change", "type": "time_comparison", "base_metric": "sales.revenue", "comparison_type": "mom", "calculation": "difference", "fill_nulls_with": -9},
                {"name": "rolling_revenue", "type": "cumulative", "sql": "sales.revenue", "window": "2 months", "fill_nulls_with": 0}],
            "metric_owners": {}
        });
        let sql = crate::semantic_input::compile_with_semantic_input(
            &input.to_string(),
            r#"{"metrics":["rolling_revenue","month_change"],"dimensions":["sales.day__month","sales.category"]}"#,
        ).unwrap();
        assert!(
            sql.contains("MAX(base.revenue) OVER (PARTITION BY base.category"),
            "{sql}"
        );
        assert!(
            sql.contains("INTERVAL '1 month' PRECEDING AND INTERVAL '1 month' PRECEDING"),
            "{sql}"
        );
        assert!(
            sql.contains("SUM(base.revenue) OVER (PARTITION BY base.category"),
            "{sql}"
        );
        let final_select = sql.rsplit("FROM lag_cte").nth(1).unwrap();
        assert!(final_select.contains("rolling_revenue"), "{sql}");
        assert!(final_select.contains("month_change"), "{sql}");
        assert!(sql.contains("COALESCE(SUM(base.revenue) OVER"), "{sql}");
        assert!(
            sql.contains("COALESCE((revenue - month_change_prev_value), -9)"),
            "{sql}"
        );
    }

    fn grouped_graph() -> (SemanticGraph, Vec<DimensionRef>) {
        let mut graph = SemanticGraph::new();
        graph
            .add_model(
                Model::new("sales", "id")
                    .with_table("sales")
                    .with_dimension(crate::core::Dimension::time("day"))
                    .with_dimension(crate::core::Dimension::categorical("category"))
                    .with_metric(Metric::sum("revenue", "amount")),
            )
            .unwrap();
        let dimensions = vec![
            DimensionRef {
                model: "sales".into(),
                name: "day".into(),
                granularity: Some("month".into()),
                alias: "day__month".into(),
            },
            DimensionRef {
                model: "sales".into(),
                name: "category".into(),
                granularity: None,
                alias: "category".into(),
            },
        ];
        (graph, dimensions)
    }

    #[test]
    fn calendar_comparison_uses_exact_period_and_group_partition() {
        let (graph, dimensions) = grouped_graph();
        let generator = SqlGenerator::new(&graph);
        let mut metric = Metric::time_comparison("change", "sales.revenue", ComparisonType::Mom);
        let prior = generator
            .calendar_prior_value(
                &metric,
                &dimensions,
                "base.day__month",
                Some("month"),
                "base.revenue",
            )
            .unwrap()
            .unwrap();
        assert_eq!(prior, "MAX(base.revenue) OVER (PARTITION BY base.category ORDER BY base.day__month RANGE BETWEEN INTERVAL '1 month' PRECEDING AND INTERVAL '1 month' PRECEDING)");
        metric.time_offset = Some("3 months".into());
        assert!(generator
            .calendar_prior_value(
                &metric,
                &dimensions,
                "base.day__month",
                Some("month"),
                "base.revenue"
            )
            .unwrap()
            .unwrap()
            .contains("INTERVAL '3 month'"));
    }

    #[test]
    fn named_comparisons_have_calendar_intervals_without_declared_granularity() {
        let (graph, dimensions) = grouped_graph();
        let generator = SqlGenerator::new(&graph);
        for (comparison, interval) in [
            (ComparisonType::Dod, "1 day"),
            (ComparisonType::Wow, "1 week"),
            (ComparisonType::Mom, "1 month"),
            (ComparisonType::Qoq, "3 month"),
            (ComparisonType::Yoy, "1 year"),
        ] {
            let metric = Metric::time_comparison("change", "sales.revenue", comparison);
            let sql = generator
                .calendar_prior_value(&metric, &dimensions, "base.day", None, "base.revenue")
                .unwrap()
                .unwrap();
            assert!(
                sql.contains(&format!("INTERVAL '{interval}' PRECEDING")),
                "{sql}"
            );
        }
        let mut metric =
            Metric::time_comparison("change", "sales.revenue", ComparisonType::PriorPeriod);
        assert!(generator
            .calendar_prior_value(&metric, &dimensions, "base.day", None, "base.revenue")
            .unwrap()
            .is_none());
        metric.time_offset = Some("2 months".into());
        assert!(generator
            .calendar_prior_value(&metric, &dimensions, "base.day", None, "base.revenue")
            .unwrap()
            .unwrap()
            .contains("INTERVAL '2 month'"));
    }

    #[test]
    fn cumulative_frames_preserve_groups_and_calendar_reset() {
        let (graph, dimensions) = grouped_graph();
        let generator = SqlGenerator::new(&graph);
        let mut metric = Metric::cumulative("running", "sales.revenue");
        assert_eq!(generator.cumulative_window_sql(&metric, &dimensions, "base.day__month").unwrap(), "PARTITION BY base.category ORDER BY base.day__month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
        metric.window = Some("2 months".into());
        assert_eq!(generator.cumulative_window_sql(&metric, &dimensions, "base.day__month").unwrap(), "PARTITION BY base.category ORDER BY base.day__month RANGE BETWEEN INTERVAL '2 month' PRECEDING AND CURRENT ROW");
        metric.grain_to_date = Some(TimeGrain::Year);
        assert_eq!(generator.cumulative_window_sql(&metric, &dimensions, "base.day__month").unwrap(), "PARTITION BY base.category, DATE_TRUNC('year', base.day__month) ORDER BY base.day__month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
    }

    #[test]
    fn intervals_validate_amounts_and_normalize_quarters() {
        assert_eq!(period_interval("2 quarters").unwrap(), (6, "month".into()));
        assert_eq!(period_interval("1 YEAR").unwrap(), (1, "year".into()));
        for value in [
            "0 days",
            "-1 day",
            "days",
            "1; drop table x",
            "2 fortnights",
            "4294967295 quarters",
        ] {
            assert!(period_interval(value).is_err(), "{value}");
        }
    }

    #[test]
    fn cumulative_ambiguous_aggregations_are_explicitly_unsupported() {
        for aggregation in [Aggregation::Count, Aggregation::CountDistinct] {
            let mut metric = Metric::cumulative("running", "sales.revenue");
            metric.agg = Some(aggregation);
            assert!(matches!(
                validate_metric(&metric),
                Err(SidemanticError::UnsupportedSemanticFeatures { .. })
            ));
        }
        let mut metric = Metric::cumulative("running", "sales.revenue");
        metric.window_expression = Some("SUM(base.revenue)".into());
        assert!(validate_metric(&metric).is_ok());
        assert_eq!(
            window_output_reference(r#"AVG(base."daily_revenue")"#).unwrap(),
            "daily_revenue"
        );
        for expression in [
            "SUM(amount)",
            "SUM(base.amount) + 1",
            "SUM(base.amount); SELECT 1",
        ] {
            metric.window_expression = Some(expression.into());
            assert!(validate_metric(&metric).is_err());
        }
        metric.window_expression = Some("SUM(base.revenue)".into());
        for frame in [
            "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW",
            "RANGE BETWEEN INTERVAL 2 DAY PRECEDING AND CURRENT ROW",
        ] {
            metric.window_frame = Some(frame.into());
            assert!(validate_metric(&metric).is_ok());
        }
        metric.window_frame = Some("ROWS BETWEEN -1 PRECEDING AND CURRENT ROW".into());
        assert!(validate_metric(&metric).is_err());
    }
}
