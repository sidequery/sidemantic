//! Calendar-based windows used by the existing temporal query generator.

use super::*;
use crate::core::{ComparisonType, TimeGrain};

fn unsupported(feature: &str) -> SidemanticError {
    SidemanticError::UnsupportedSemanticFeatures {
        capabilities: vec![format!("metric.{feature}")],
    }
}

pub(crate) fn validate_metric(metric: &Metric) -> Result<()> {
    if metric.r#type == MetricType::Cumulative {
        if !matches!(
            metric.agg,
            None | Some(Aggregation::Sum | Aggregation::Avg | Aggregation::Min | Aggregation::Max)
        ) {
            return Err(unsupported("cumulative_aggregation"));
        }
        if metric.window_expression.is_some()
            || metric.window_frame.is_some()
            || metric.window_order.is_some()
        {
            return Err(unsupported("cumulative_raw_window"));
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
        let frame = if metric.grain_to_date.is_none() {
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
    fn graph_temporal_metrics_compile_through_handoff_and_preserve_mixed_outputs() {
        let input = serde_json::json!({
            "version": 1,
            "input_dialect": "duckdb",
            "models": [{"name": "sales", "table": "sales", "primary_key": "id",
                "dimensions": [{"name": "day", "type": "time", "granularity": "day"},
                    {"name": "category", "type": "categorical"}],
                "metrics": [{"name": "revenue", "agg": "sum", "sql": "amount"}]}],
            "metrics": [
                {"name": "month_change", "type": "time_comparison", "base_metric": "sales.revenue", "comparison_type": "mom", "calculation": "difference"},
                {"name": "rolling_revenue", "type": "cumulative", "sql": "sales.revenue", "window": "2 months"}],
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
    fn cumulative_ambiguous_aggregations_and_raw_frames_are_explicitly_unsupported() {
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
        assert!(matches!(
            validate_metric(&metric),
            Err(SidemanticError::UnsupportedSemanticFeatures { .. })
        ));
    }
}
