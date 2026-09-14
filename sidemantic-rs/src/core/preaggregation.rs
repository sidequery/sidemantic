//! Shared additive state SQL for rollup builds and lambda fresh-source legs.

use super::{Aggregation, MetricType, Model, PreAggregation, PreAggregationType};
use crate::error::{Result, SidemanticError};

pub(crate) fn materialization_sql(
    model: &Model,
    preagg: &PreAggregation,
    partition_filter: Option<&str>,
) -> Result<String> {
    let unsupported = |feature: &str| SidemanticError::UnsupportedSemanticFeatures {
        capabilities: vec![format!("preaggregation.materialization.{feature}")],
    };
    if !preagg.has_unique_output_names() {
        return Err(unsupported("output_alias_collision"));
    }
    if !matches!(
        preagg.preagg_type,
        PreAggregationType::Rollup | PreAggregationType::Lambda
    ) || preagg.sql.is_some()
    {
        return Err(unsupported("type_or_custom_sql"));
    }
    if preagg.time_dimension.is_some() != preagg.granularity.is_some() {
        return Err(unsupported("incomplete_time_grain"));
    }

    let source_expression =
        |sql: &str| self::source_expression(model, sql, None, polyglot_sql::DialectType::DuckDB);
    let mut select_exprs = Vec::new();
    let mut group_by_positions = Vec::new();
    if let (Some(time_dimension), Some(granularity)) =
        (preagg.time_dimension.as_ref(), preagg.granularity.as_ref())
    {
        if !matches!(
            granularity.as_str(),
            "year" | "quarter" | "month" | "week" | "day" | "hour" | "minute" | "second"
        ) {
            return Err(unsupported("time_grain"));
        }
        let time_dim = model.get_dimension(time_dimension).ok_or_else(|| {
            SidemanticError::Validation(format!("Unknown rollup time dimension '{time_dimension}'"))
        })?;
        if time_dim.window.is_some() {
            return Err(unsupported("window_dimension"));
        }
        let expression = source_expression(time_dim.sql_expr())?;
        select_exprs.push(format!(
            "DATE_TRUNC('{granularity}', {expression}) as {time_dimension}_{granularity}"
        ));
        group_by_positions.push(select_exprs.len().to_string());
    }
    for dim_name in preagg.dimensions.iter().flatten() {
        let dim = model.get_dimension(dim_name).ok_or_else(|| {
            SidemanticError::Validation(format!("Unknown rollup dimension '{dim_name}'"))
        })?;
        if dim.window.is_some() {
            return Err(unsupported("window_dimension"));
        }
        select_exprs.push(format!(
            "{} as {dim_name}",
            source_expression(dim.sql_expr())?
        ));
        group_by_positions.push(select_exprs.len().to_string());
    }
    for measure_name in preagg.measures.iter().flatten() {
        let measure = model.get_metric(measure_name).ok_or_else(|| {
            SidemanticError::Validation(format!("Unknown rollup measure '{measure_name}'"))
        })?;
        // Derived declarations carry no stored column; their leaves are rebuilt
        // by the query router, matching the Python materialization contract.
        if measure.agg.is_none()
            && matches!(measure.r#type, MetricType::Derived | MetricType::Ratio)
        {
            continue;
        }
        if measure.r#type != MetricType::Simple
            || measure.sql_is_complete
            || measure.non_additive_dimension.is_some()
            || measure.window.is_some()
            || measure.window_expression.is_some()
            || measure.window_frame.is_some()
            || measure.window_order.is_some()
            || measure.grain_to_date.is_some()
            || measure.offset_window.is_some()
        {
            return Err(unsupported("measure_state"));
        }
        let aggregate = match measure.agg.as_ref() {
            Some(Aggregation::Sum | Aggregation::Avg) => "SUM",
            Some(Aggregation::Count) => "COUNT",
            Some(Aggregation::CountDistinct)
                if preagg.preagg_type == PreAggregationType::Rollup =>
            {
                "COUNT"
            }
            Some(Aggregation::Min) => "MIN",
            Some(Aggregation::Max) => "MAX",
            // Lambda and distribution states need mergeable state, not distinct scalars.
            _ => return Err(unsupported("measure_aggregation")),
        };
        let count_rows = measure.agg == Some(Aggregation::Count)
            && measure
                .sql
                .as_deref()
                .is_none_or(|sql| sql.trim().is_empty() || sql.trim() == "*");
        let mut expression = if count_rows {
            "*".to_owned()
        } else {
            source_expression(measure.sql_expr())?
        };
        if !measure.filters.is_empty() {
            let predicates = measure
                .filters
                .iter()
                .map(|filter| {
                    let predicate =
                        source_expression(filter).or_else(|error| -> Result<String> {
                            // Public builds historically accept physical subquery filters.
                            // Routing still requires strict row-scope proof before reuse.
                            let physical =
                                replace_model_placeholder(filter, None).map_err(|_| error)?;
                            super::parse_semantic_expression(&physical)?;
                            Ok(physical)
                        })?;
                    Ok(format!("({predicate})"))
                })
                .collect::<Result<Vec<_>>>()?;
            let input = if count_rows { "1" } else { &expression };
            expression = format!(
                "CASE WHEN {} THEN {input} ELSE NULL END",
                predicates.join(" AND ")
            );
        }
        if measure.agg == Some(Aggregation::CountDistinct) {
            expression = format!("DISTINCT {expression}");
        }
        select_exprs.push(format!("{aggregate}({expression}) as {measure_name}_raw"));
    }
    if select_exprs.is_empty() {
        return Err(unsupported("empty_rollup"));
    }
    let from_clause = if let Some(model_sql) = model.sql.as_ref() {
        format!("({model_sql}) AS t")
    } else {
        model
            .table
            .clone()
            .ok_or_else(|| unsupported("missing_source"))?
    };
    let mut sql = format!(
        "SELECT\n  {}\nFROM {from_clause}",
        select_exprs.join(",\n  ")
    );
    if let Some(predicate) = partition_filter {
        sql.push_str(&format!("\nWHERE ({predicate})"));
    }
    if !group_by_positions.is_empty() {
        sql.push_str(&format!("\nGROUP BY {}", group_by_positions.join(", ")));
    }
    Ok(sql)
}

/// Rewrite only actual placeholder tokens, leaving literal and subquery text intact.
fn replace_model_placeholder(sql: &str, owner: Option<&str>) -> Result<String> {
    use polyglot_sql::{dialects::Dialect, DialectType, TokenType};
    let tokens = Dialect::get(DialectType::DuckDB)
        .tokenize(sql)
        .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
    // Token spans are character offsets, including for Unicode input.
    let mut characters: Vec<char> = sql.chars().collect();
    for index in (0..tokens.len().saturating_sub(2)).rev() {
        let window = &tokens[index..index + 3];
        if window[0].token_type != TokenType::LBrace
            || window[1].text != "model"
            || window[2].token_type != TokenType::RBrace
            || characters[window[0].span.start..window[2].span.end]
                .iter()
                .collect::<String>()
                != "{model}"
        {
            continue;
        }
        let mut end = window[2].span.end;
        if owner.is_none() {
            if let Some(dot) = tokens
                .get(index + 3)
                .filter(|token| token.token_type == TokenType::Dot)
            {
                end = dot.span.end;
            } else {
                return Err(SidemanticError::SqlParse(
                    "Expected column after model placeholder".into(),
                ));
            }
        }
        characters.splice(window[0].span.start..end, owner.unwrap_or("").chars());
    }
    Ok(characters.into_iter().collect())
}

/// Bind only physical columns; tokenized placeholders never touch literal text.
pub(crate) fn source_expression(
    model: &Model,
    sql: &str,
    alias: Option<&str>,
    dialect: polyglot_sql::DialectType,
) -> Result<String> {
    use polyglot_sql::{expressions::Identifier, Expression};
    let placeholder_owner = format!("\"{}\"", model.name.replace('"', "\"\""));
    let source = replace_model_placeholder(sql, Some(&placeholder_owner))?;
    let expression = super::parse_semantic_expression(&source)?;
    super::validate_row_expression(&expression, "preaggregation.source_scope")?;
    fn bind(value: &mut serde_json::Value, model: &Model, alias: Option<&str>) -> Result<()> {
        match value {
            serde_json::Value::Object(fields)
                if fields.len() == 1 && fields.contains_key("column") =>
            {
                let mut column: polyglot_sql::expressions::Column =
                    serde_json::from_value(fields["column"].clone())
                        .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
                if column.join_mark
                    || column
                        .table
                        .as_ref()
                        .is_some_and(|table| table.name != model.name && table.name != "t")
                {
                    return Err(SidemanticError::UnsupportedSemanticFeatures {
                        capabilities: vec!["preaggregation.foreign_source".into()],
                    });
                }
                column.table = alias.map(|alias| Identifier::quoted(alias.to_owned()));
                fields.insert(
                    "column".into(),
                    serde_json::to_value(column)
                        .map_err(|error| SidemanticError::SqlParse(error.to_string()))?,
                );
            }
            serde_json::Value::Object(fields) => {
                for value in fields.values_mut() {
                    bind(value, model, alias)?;
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    bind(value, model, alias)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    let mut value = serde_json::to_value(expression)
        .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
    bind(&mut value, model, alias)?;
    let expression: Expression = serde_json::from_value(value)
        .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
    polyglot_sql::generate(&expression, dialect)
        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use polyglot_sql::DialectType;

    #[test]
    fn source_binding_keeps_unicode_literals_and_quoted_columns() {
        let model = Model::new("orders", "id");
        let expression = source_expression(
            &model,
            "CASE WHEN {model}.\"Amount\" > 0 THEN 'é{model}.Amount' ELSE orders.\"Label\" END",
            Some("source"),
            DialectType::PostgreSQL,
        )
        .unwrap();
        assert!(expression.contains("'é{model}.Amount'"), "{expression}");
        assert!(expression.contains("\"source\".\"Amount\""), "{expression}");
        assert!(expression.contains("\"source\".\"Label\""), "{expression}");
        let expression =
            source_expression(&model, "'λ' || {model}.name", None, DialectType::DuckDB).unwrap();
        assert!(expression.contains("'λ'"), "{expression}");
        assert!(!expression.contains("{model}"), "{expression}");
    }

    #[test]
    fn public_materializer_preserves_physical_subquery_filters() {
        let model: Model = serde_json::from_value(serde_json::json!({
            "name":"orders", "table":"orders", "primary_key":"id", "metrics":[
                {"name":"revenue", "agg":"sum", "sql":"amount",
                 "filters":["{model}.id IN (SELECT order_id FROM paid_orders WHERE paid_orders.label = 'é{model}.x')"]}
            ]
        }))
        .unwrap();
        let preagg: PreAggregation = serde_json::from_value(serde_json::json!({
            "name":"state", "measures":["revenue"]
        }))
        .unwrap();
        let sql = materialization_sql(&model, &preagg, None).unwrap();
        assert!(
            sql.contains(
                "id IN (SELECT order_id FROM paid_orders WHERE paid_orders.label = 'é{model}.x')"
            ),
            "{sql}"
        );
        assert!(source_expression(
            &model,
            &model.metrics[0].filters[0],
            None,
            DialectType::DuckDB
        )
        .is_err());
    }

    #[test]
    fn distinct_materialization_filters_input_and_declines_lambda() {
        let model: Model = serde_json::from_value(serde_json::json!({
            "name":"orders", "table":"orders", "primary_key":"id", "metrics":[
                {"name":"people", "agg":"count_distinct", "sql":"person", "filters":["paid"]}
            ]
        }))
        .unwrap();
        let mut preagg: PreAggregation = serde_json::from_value(serde_json::json!({
            "name":"state", "measures":["people"]
        }))
        .unwrap();
        let sql = materialization_sql(&model, &preagg, None).unwrap();
        assert!(
            sql.contains(
                "COUNT(DISTINCT CASE WHEN (paid) THEN person ELSE NULL END) as people_raw"
            ),
            "{sql}"
        );
        preagg.preagg_type = PreAggregationType::Lambda;
        assert!(materialization_sql(&model, &preagg, None).is_err());
    }

    #[test]
    fn average_materialization_stores_sum_and_exact_count_input() {
        let model: Model = serde_json::from_value(serde_json::json!({
            "name":"orders", "table":"orders", "primary_key":"id", "metrics":[
                {"name":"average", "agg":"avg", "sql":"amount", "filters":["paid"]},
                {"name":"values", "agg":"count", "sql":"amount", "filters":["paid"]}
            ]
        }))
        .unwrap();
        let preagg: PreAggregation = serde_json::from_value(
            serde_json::json!({"name":"state", "type":"lambda", "measures":["average","values"]}),
        )
        .unwrap();
        let sql = materialization_sql(&model, &preagg, Some("id >= 2")).unwrap();
        assert!(
            sql.contains("SUM(CASE WHEN (paid) THEN amount ELSE NULL END) as average_raw"),
            "{sql}"
        );
        assert!(
            sql.contains("COUNT(CASE WHEN (paid) THEN amount ELSE NULL END) as values_raw"),
            "{sql}"
        );
        assert!(sql.contains("WHERE (id >= 2)"), "{sql}");
    }
}
