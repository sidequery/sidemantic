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
        if let Some(expression) = &metric.window_expression {
            validate_window_function(&parse_semantic_expression(expression)?)?;
            // Python gives an explicit function precedence over window/grain_to_date.
            window_output_references(expression)?;
        }
        if let Some(frame) = &metric.window_frame {
            if metric.window.is_some() || metric.grain_to_date.is_some() {
                return Err(unsupported("window_frame_controls"));
            }
            parse_output_frame(frame)?;
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

// OVER applies to a root aggregate/window function, not scalar arithmetic or
// a column that merely contains an aggregate somewhere below it.
fn validate_window_function(expression: &Expression) -> Result<()> {
    match expression {
        Expression::Count(_)
        | Expression::Sum(_)
        | Expression::Avg(_)
        | Expression::Min(_)
        | Expression::Max(_)
        | Expression::GroupConcat(_)
        | Expression::StringAgg(_)
        | Expression::ListAgg(_)
        | Expression::ArrayAgg(_)
        | Expression::CountIf(_)
        | Expression::SumIf(_)
        | Expression::Stddev(_)
        | Expression::StddevPop(_)
        | Expression::StddevSamp(_)
        | Expression::Variance(_)
        | Expression::VarPop(_)
        | Expression::VarSamp(_)
        | Expression::Median(_)
        | Expression::Mode(_)
        | Expression::First(_)
        | Expression::Last(_)
        | Expression::AnyValue(_)
        | Expression::ApproxDistinct(_)
        | Expression::ApproxCountDistinct(_)
        | Expression::ApproxPercentile(_)
        | Expression::Percentile(_)
        | Expression::LogicalAnd(_)
        | Expression::LogicalOr(_)
        | Expression::Skewness(_)
        | Expression::ArrayConcatAgg(_)
        | Expression::ArrayUniqueAgg(_)
        | Expression::BoolXorAgg(_)
        | Expression::RowNumber(_)
        | Expression::Rank(_)
        | Expression::DenseRank(_)
        | Expression::NTile(_)
        | Expression::Lead(_)
        | Expression::Lag(_)
        | Expression::FirstValue(_)
        | Expression::LastValue(_)
        | Expression::NthValue(_)
        | Expression::PercentRank(_)
        | Expression::CumeDist(_)
        | Expression::PercentileCont(_)
        | Expression::PercentileDisc(_)
        | Expression::AggregateFunction(_) => Ok(()),
        Expression::Filter(filter) => validate_window_function(&filter.this),
        _ => Err(unsupported("window_expression")),
    }
}

/// Extract grouped-output dependencies from the parsed expression, including
/// FILTER predicates and arithmetic arguments without mistaking literals for names.
pub(super) fn window_output_references(expression: &str) -> Result<Vec<String>> {
    let mut names = Vec::new();
    for column in semantic_column_references(expression)? {
        if column
            .model
            .as_deref()
            .is_some_and(|model| model.eq_ignore_ascii_case("base"))
            && !names.contains(&column.field)
        {
            names.push(column.field);
        }
    }
    Ok(names)
}

fn parse_output_frame(frame: &str) -> Result<polyglot_sql::expressions::WindowFrame> {
    let sql = format!("SELECT SUM(x) OVER (ORDER BY y {frame})");
    let statements = polyglot_sql::parse(&sql, DialectType::DuckDB)
        .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
    if statements.len() != 1 {
        return Err(unsupported("window_frame"));
    }
    let mut statement = statements.into_iter().next().unwrap();
    let Expression::Select(select) = &mut statement else {
        return Err(unsupported("window_frame"));
    };
    if select.expressions.len() != 1 {
        return Err(unsupported("window_frame"));
    }
    let Expression::WindowFunction(window) = &mut select.expressions[0] else {
        return Err(unsupported("window_frame"));
    };
    let parsed_frame = window
        .over
        .frame
        .take()
        .ok_or_else(|| unsupported("window_frame"))?;
    let baseline = polyglot_sql::parse_one("SELECT SUM(x) OVER (ORDER BY y)", DialectType::DuckDB)
        .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
    if statement != baseline {
        return Err(unsupported("window_frame"));
    }
    Ok(parsed_frame)
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
    pub(super) fn offset_window_lag_rows(
        offset: Option<&str>,
        granularity: Option<&str>,
    ) -> Result<u64> {
        let Some(offset) = offset else {
            return Ok(1);
        };
        let (amount, unit) = period_interval(offset)?;
        let days = |unit: &str| match unit {
            "day" => 1_u64,
            "week" => 7,
            "quarter" => 90,
            "year" => 365,
            _ => 30,
        };
        let total_days = u64::from(amount) * days(&unit);
        let grain_days = days(granularity.unwrap_or("month"));
        let rows = total_days / grain_days;
        let remainder = total_days % grain_days;
        // Python round() breaks halfway ties toward the nearest even integer.
        let round_up = remainder * 2 > grain_days || (remainder * 2 == grain_days && rows % 2 == 1);
        Ok((rows + u64::from(round_up)).max(1))
    }

    pub(crate) fn window_output_dependency(metric: &Metric) -> Result<Option<String>> {
        metric
            .window_expression
            .as_deref()
            .map(|expression| {
                let names = window_output_references(expression)?;
                names
                    .into_iter()
                    .map(|name| {
                        polyglot_sql::generate(
                            &Expression::Identifier(Identifier::quoted(name)),
                            DialectType::DuckDB,
                        )
                        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))
                    })
                    .collect::<Result<Vec<_>>>()
                    .map(|names| names.join(" + "))
            })
            .transpose()
            .map(|dependency| dependency.filter(|sql| !sql.is_empty()))
    }

    /// Parse authored function/frame in the input dialect and generated output
    /// identifiers in the target dialect, then emit one composed window AST.
    pub(super) fn output_window_expression(
        &self,
        function: &str,
        partition: &str,
        order: &str,
        frame: &str,
    ) -> Result<String> {
        let frame = parse_output_frame(frame)?;
        let statement = polyglot_sql::parse_one(
            &format!("SELECT SUM(1) OVER ({partition}ORDER BY {order})"),
            self.dialect,
        )
        .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
        let Expression::Select(mut select) = statement else {
            return Err(unsupported("window_expression"));
        };
        let mut expression = select.expressions.remove(0);
        let Expression::WindowFunction(window) = &mut expression else {
            return Err(unsupported("window_expression"));
        };
        window.this = parse_semantic_expression(function)?;
        validate_window_function(&window.this)?;
        window.over.frame = Some(frame);
        self.emit_expression(&expression)
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
            parse_output_frame(frame)?;
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
            partitions.push(self.date_trunc_sql(grain, time_column)?);
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
        if !matches!(self.dialect, DialectType::DuckDB | DialectType::PostgreSQL)
            || (metric.r#type == MetricType::Ratio && granularity.is_none())
        {
            return Ok(None);
        }
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
    fn explicit_window_requires_a_root_window_capable_function() {
        for sql in [
            "SUM(base.x + 1)",
            "AVG(base.x)",
            "LAG(base.x)",
            "ROW_NUMBER()",
            "SUM(base.x) FILTER (WHERE base.x > 0)",
        ] {
            validate_window_function(&parse_semantic_expression(sql).unwrap()).unwrap();
        }
        for sql in [
            "base.x",
            "1",
            "base.x + 1",
            "SUM(base.x) + 1",
            "ABS(base.x)",
            "SUM(base.x) OVER ()",
        ] {
            assert!(
                matches!(
                    validate_window_function(&parse_semantic_expression(sql).unwrap()),
                    Err(SidemanticError::UnsupportedSemanticFeatures { .. })
                ),
                "{sql}"
            );
        }
    }

    #[test]
    fn filled_temporal_metrics_accept_python_control_combinations() {
        for kind in [
            MetricType::Cumulative,
            MetricType::TimeComparison,
            MetricType::Conversion,
            MetricType::Retention,
        ] {
            let mut metric = Metric::new("filled");
            metric.r#type = kind;
            metric.fill_nulls_with = Some(serde_json::json!(0));
            metric.time_offset = Some("1 day".into());
            assert!(SqlGenerator::validate_metric_fill(&metric).is_ok());
        }
    }

    #[test]
    fn temporal_fills_wrap_final_values() {
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
            assert!(SqlGenerator::validate_metric_fill(&comparison).is_ok());
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
    fn cumulative_expression_dependencies_and_frames_use_the_sql_ast() {
        for aggregation in [
            Aggregation::Count,
            Aggregation::CountDistinct,
            Aggregation::Median,
        ] {
            let mut metric = Metric::cumulative("running", "sales.revenue");
            metric.agg = Some(aggregation);
            assert!(validate_metric(&metric).is_ok());
        }
        assert_eq!(
            window_output_references("SUM(base.revenue + base.tax) FILTER (WHERE base.tax > 0)")
                .unwrap()
                .into_iter()
                .collect::<HashSet<_>>(),
            HashSet::from(["revenue".to_string(), "tax".to_string()])
        );
        assert_eq!(
            window_output_references(r#"AVG(base."daily_revenue")"#).unwrap(),
            vec!["daily_revenue"]
        );
        for frame in [
            "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW",
            "RANGE BETWEEN INTERVAL 2 DAY PRECEDING AND CURRENT ROW",
            "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
            "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW",
        ] {
            parse_output_frame(frame).unwrap();
        }
        for frame in [
            "garbage",
            "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW); SELECT 1; --",
            "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM sales --",
        ] {
            assert!(parse_output_frame(frame).is_err());
        }
    }
}
