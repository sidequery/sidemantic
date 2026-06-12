use std::io::{self, Read};

use polyglot_sql::DialectType;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sidemantic::{
    build_symmetric_aggregate_sql, config::SidemanticConfig, load_from_string, Aggregation,
    DimensionType, Metric, Model, QueryRewriter, RelationshipType, SemanticGraph, SemanticQuery,
    SqlDialect, SqlGenerator, SymmetricAggType, TableCalculation,
};

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum Request {
    Validate {
        models_yaml: String,
    },
    Compile {
        models_yaml: String,
        #[serde(default)]
        metrics: Vec<String>,
        #[serde(default)]
        dimensions: Vec<String>,
        #[serde(default)]
        filters: Vec<String>,
        #[serde(default)]
        segments: Vec<String>,
        #[serde(default)]
        order_by: Vec<String>,
        limit: Option<usize>,
        offset: Option<usize>,
        #[serde(default)]
        ungrouped: bool,
        #[serde(default)]
        skip_default_time_dimensions: bool,
        dialect: Option<String>,
    },
    JoinPath {
        models_yaml: String,
        from_model: String,
        to_model: String,
    },
    RewriteSql {
        models_yaml: String,
        sql: String,
    },
    CatalogMetadata {
        models_yaml: String,
        schema: Option<String>,
    },
    GraphAddModel {
        models_yaml: String,
        model_yaml: String,
    },
    GraphAddMetric {
        models_yaml: String,
        metric_yaml: String,
    },
    GraphAddTableCalculation {
        models_yaml: String,
        #[serde(default)]
        table_calculations_json: Vec<Value>,
        table_calculation_json: Value,
    },
    GraphGetModel {
        models_yaml: String,
        name: String,
    },
    GraphGetMetric {
        models_yaml: String,
        name: String,
    },
    GraphGetTableCalculation {
        models_yaml: String,
        #[serde(default)]
        table_calculations_json: Vec<Value>,
        name: String,
    },
    GraphBuildAdjacency {
        models_yaml: String,
    },
    SymmetricAggregateSql {
        measure_expr: String,
        primary_key: String,
        agg_type: String,
        model_alias: Option<String>,
        dialect: Option<String>,
    },
    NeedsSymmetricAggregate {
        relationship: String,
        is_base_model: bool,
    },
}

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum Response {
    Ok {
        #[serde(skip_serializing_if = "Option::is_none")]
        sql: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        path: Option<Vec<PathStep>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        catalog: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        value: Option<Value>,
    },
    Error {
        error: String,
    },
}

#[derive(Debug, Serialize)]
struct PathStep {
    from_model: String,
    to_model: String,
    from_columns: Vec<String>,
    to_columns: Vec<String>,
    relationship: &'static str,
}

fn handle(request: Request) -> sidemantic::Result<Response> {
    match request {
        Request::Validate { models_yaml } => {
            load_from_string(&models_yaml)?;
            Ok(Response::Ok {
                sql: None,
                path: None,
                catalog: None,
                value: None,
            })
        }
        Request::Compile {
            models_yaml,
            metrics,
            dimensions,
            filters,
            segments,
            order_by,
            limit,
            offset,
            ungrouped,
            skip_default_time_dimensions,
            dialect,
        } => {
            let graph = load_from_string(&models_yaml)?;
            let mut query = SemanticQuery::new()
                .with_metrics(metrics)
                .with_dimensions(dimensions)
                .with_filters(filters)
                .with_segments(segments)
                .with_order_by(order_by)
                .with_ungrouped(ungrouped)
                .with_skip_default_time_dimensions(skip_default_time_dimensions);
            if let Some(limit) = limit {
                query = query.with_limit(limit);
            }
            if let Some(offset) = offset {
                query = query.with_offset(offset);
            }
            let mut generator = SqlGenerator::new(&graph);
            if let Some(dialect) = dialect {
                generator = generator.with_dialect(parse_dialect(&dialect)?);
            }
            let sql = generator.generate(&query)?;
            Ok(Response::Ok {
                sql: Some(sql),
                path: None,
                catalog: None,
                value: None,
            })
        }
        Request::JoinPath {
            models_yaml,
            from_model,
            to_model,
        } => {
            let graph = load_from_string(&models_yaml)?;
            let path = graph.find_join_path(&from_model, &to_model)?;
            let steps = path
                .steps
                .into_iter()
                .map(|step| PathStep {
                    from_model: step.from_model,
                    to_model: step.to_model,
                    from_columns: step.from_keys,
                    to_columns: step.to_keys,
                    relationship: relationship_type_name(&step.relationship_type),
                })
                .collect();
            Ok(Response::Ok {
                sql: None,
                path: Some(steps),
                catalog: None,
                value: None,
            })
        }
        Request::RewriteSql { models_yaml, sql } => {
            let graph = load_from_string(&models_yaml)?;
            let rewritten = QueryRewriter::new(&graph).rewrite(&sql)?;
            Ok(Response::Ok {
                sql: Some(rewritten),
                path: None,
                catalog: None,
                value: None,
            })
        }
        Request::CatalogMetadata {
            models_yaml,
            schema,
        } => {
            let graph = load_from_string(&models_yaml)?;
            Ok(Response::Ok {
                sql: None,
                path: None,
                catalog: Some(catalog_metadata(
                    &graph,
                    schema.as_deref().unwrap_or("public"),
                )),
                value: None,
            })
        }
        Request::GraphAddModel {
            models_yaml,
            model_yaml,
        } => {
            let mut graph = load_from_string(&models_yaml)?;
            graph.add_model(parse_single_model(&model_yaml)?)?;
            Ok(Response::Ok {
                sql: None,
                path: None,
                catalog: None,
                value: None,
            })
        }
        Request::GraphAddMetric {
            models_yaml,
            metric_yaml,
        } => {
            let mut graph = load_from_string(&models_yaml)?;
            graph.add_metric(parse_single_metric(&metric_yaml)?)?;
            Ok(Response::Ok {
                sql: None,
                path: None,
                catalog: None,
                value: None,
            })
        }
        Request::GraphAddTableCalculation {
            models_yaml,
            table_calculations_json,
            table_calculation_json,
        } => {
            let mut graph =
                load_graph_with_table_calculations(&models_yaml, table_calculations_json)?;
            graph.add_table_calculation(parse_table_calculation_value(table_calculation_json)?)?;
            Ok(Response::Ok {
                sql: None,
                path: None,
                catalog: None,
                value: None,
            })
        }
        Request::GraphGetModel { models_yaml, name } => {
            let graph = load_from_string(&models_yaml)?;
            if graph.get_model(&name).is_none() {
                return Err(sidemantic::SidemanticError::Validation(format!(
                    "Model {name} not found"
                )));
            }
            Ok(Response::Ok {
                sql: None,
                path: None,
                catalog: None,
                value: None,
            })
        }
        Request::GraphGetMetric { models_yaml, name } => {
            let graph = load_from_string(&models_yaml)?;
            if graph.get_metric(&name).is_none() {
                return Err(sidemantic::SidemanticError::Validation(format!(
                    "Measure {name} not found"
                )));
            }
            Ok(Response::Ok {
                sql: None,
                path: None,
                catalog: None,
                value: None,
            })
        }
        Request::GraphGetTableCalculation {
            models_yaml,
            table_calculations_json,
            name,
        } => {
            let graph = load_graph_with_table_calculations(&models_yaml, table_calculations_json)?;
            if graph.get_table_calculation(&name).is_none() {
                return Err(sidemantic::SidemanticError::Validation(format!(
                    "Table calculation {name} not found"
                )));
            }
            Ok(Response::Ok {
                sql: None,
                path: None,
                catalog: None,
                value: None,
            })
        }
        Request::GraphBuildAdjacency { models_yaml } => {
            load_from_string(&models_yaml)?;
            Ok(Response::Ok {
                sql: None,
                path: None,
                catalog: None,
                value: None,
            })
        }
        Request::SymmetricAggregateSql {
            measure_expr,
            primary_key,
            agg_type,
            model_alias,
            dialect,
        } => {
            let agg_type = parse_symmetric_agg_type(&agg_type)?;
            let dialect = dialect
                .as_deref()
                .and_then(SqlDialect::parse)
                .unwrap_or(SqlDialect::DuckDB);
            let sql = build_symmetric_aggregate_sql(
                &measure_expr,
                &primary_key,
                agg_type,
                model_alias.as_deref(),
                dialect,
            );
            Ok(Response::Ok {
                sql: Some(sql),
                path: None,
                catalog: None,
                value: None,
            })
        }
        Request::NeedsSymmetricAggregate {
            relationship,
            is_base_model,
        } => Ok(Response::Ok {
            sql: None,
            path: None,
            catalog: None,
            value: Some(json!(
                sidemantic::core::symmetric_agg::needs_symmetric_aggregate(
                    &relationship,
                    is_base_model
                )
            )),
        }),
    }
}

fn parse_symmetric_agg_type(agg_type: &str) -> sidemantic::Result<SymmetricAggType> {
    match agg_type {
        "sum" => Ok(SymmetricAggType::Sum),
        "avg" => Ok(SymmetricAggType::Avg),
        "count" => Ok(SymmetricAggType::Count),
        "count_distinct" => Ok(SymmetricAggType::CountDistinct),
        "min" => Ok(SymmetricAggType::Min),
        "max" => Ok(SymmetricAggType::Max),
        "median" => Err(sidemantic::SidemanticError::Validation(
            "Symmetric aggregates do not support MEDIAN. Use pre-aggregation or restructure the query to avoid fan-out joins.".to_string(),
        )),
        value => Err(sidemantic::SidemanticError::Validation(format!(
            "Unsupported aggregation type for symmetric aggregates: {value}"
        ))),
    }
}

fn parse_dialect(dialect: &str) -> sidemantic::Result<DialectType> {
    match dialect.to_ascii_lowercase().as_str() {
        "duckdb" => Ok(DialectType::DuckDB),
        "bigquery" => Ok(DialectType::BigQuery),
        "postgres" | "postgresql" => Ok(DialectType::PostgreSQL),
        value => Err(sidemantic::SidemanticError::Validation(format!(
            "Unsupported SQL dialect for pure Rust Python test parity adapter: {value}"
        ))),
    }
}

fn parse_single_model(model_yaml: &str) -> sidemantic::Result<Model> {
    let config: SidemanticConfig = serde_yaml::from_str(model_yaml)?;
    let (models, _, _) = config.into_parts()?;
    let mut models = models.into_iter();
    let Some(model) = models.next() else {
        return Err(sidemantic::SidemanticError::Validation(
            "Expected exactly one model".to_string(),
        ));
    };
    if models.next().is_some() {
        return Err(sidemantic::SidemanticError::Validation(
            "Expected exactly one model".to_string(),
        ));
    }
    Ok(model)
}

fn parse_single_metric(metric_yaml: &str) -> sidemantic::Result<Metric> {
    let config: SidemanticConfig = serde_yaml::from_str(metric_yaml)?;
    let (_, metrics, _) = config.into_parts()?;
    let mut metrics = metrics.into_iter();
    let Some(metric) = metrics.next() else {
        return Err(sidemantic::SidemanticError::Validation(
            "Expected exactly one metric".to_string(),
        ));
    };
    if metrics.next().is_some() {
        return Err(sidemantic::SidemanticError::Validation(
            "Expected exactly one metric".to_string(),
        ));
    }
    Ok(metric)
}

fn load_graph_with_table_calculations(
    models_yaml: &str,
    table_calculations_json: Vec<Value>,
) -> sidemantic::Result<SemanticGraph> {
    let mut graph = load_from_string(models_yaml)?;
    for value in table_calculations_json {
        graph.add_table_calculation(parse_table_calculation_value(value)?)?;
    }
    Ok(graph)
}

fn parse_table_calculation_value(value: Value) -> sidemantic::Result<TableCalculation> {
    serde_json::from_value(value).map_err(|err| {
        sidemantic::SidemanticError::Validation(format!("Invalid table calculation: {err}"))
    })
}

fn catalog_metadata(graph: &SemanticGraph, schema: &str) -> Value {
    let mut tables = Vec::new();
    let mut columns = Vec::new();
    let mut constraints = Vec::new();
    let mut key_column_usage = Vec::new();

    for model in graph.models() {
        tables.push(json!({
            "table_catalog": "sidemantic",
            "table_schema": schema,
            "table_name": model.name,
            "table_type": "BASE TABLE",
            "is_insertable_into": "NO",
            "is_typed": "NO",
        }));

        let mut ordinal = 1;
        for primary_key in model.primary_keys() {
            columns.push(json!({
                "table_catalog": "sidemantic",
                "table_schema": schema,
                "table_name": model.name,
                "column_name": primary_key,
                "ordinal_position": ordinal,
                "column_default": null,
                "is_nullable": "NO",
                "data_type": "BIGINT",
                "character_maximum_length": null,
                "numeric_precision": 64,
                "numeric_scale": 0,
                "is_primary_key": true,
                "is_foreign_key": false,
                "is_metric": false,
            }));
            ordinal += 1;
        }

        if !model.primary_keys().is_empty() {
            constraints.push(json!({
                "constraint_catalog": "sidemantic",
                "constraint_schema": schema,
                "constraint_name": format!("{}_pkey", model.name),
                "table_catalog": "sidemantic",
                "table_schema": schema,
                "table_name": model.name,
                "constraint_type": "PRIMARY KEY",
                "is_deferrable": "NO",
                "initially_deferred": "NO",
            }));

            for (key_index, primary_key) in model.primary_keys().into_iter().enumerate() {
                key_column_usage.push(json!({
                    "constraint_catalog": "sidemantic",
                    "constraint_schema": schema,
                    "constraint_name": format!("{}_pkey", model.name),
                    "table_catalog": "sidemantic",
                    "table_schema": schema,
                    "table_name": model.name,
                    "column_name": primary_key,
                    "ordinal_position": key_index + 1,
                }));
            }
        }

        for dimension in &model.dimensions {
            if model.primary_keys().contains(&dimension.name) {
                continue;
            }

            let data_type =
                postgres_type_for_dimension(&dimension.r#type, dimension.granularity.as_deref());
            let mut column = json!({
                "table_catalog": "sidemantic",
                "table_schema": schema,
                "table_name": model.name,
                "column_name": dimension.name,
                "ordinal_position": ordinal,
                "column_default": null,
                "is_nullable": "YES",
                "data_type": data_type,
                "character_maximum_length": if data_type == "VARCHAR" { Some(255) } else { None },
                "numeric_precision": if data_type == "NUMERIC" { Some(38) } else { None },
                "numeric_scale": if data_type == "NUMERIC" { Some(10) } else { None },
                "is_primary_key": false,
                "is_foreign_key": false,
                "is_metric": false,
            });
            if let Some(description) = &dimension.description {
                column["description"] = json!(description);
            }
            if let Some(label) = &dimension.label {
                column["label"] = json!(label);
            }
            columns.push(column);
            ordinal += 1;
        }

        for metric in &model.metrics {
            let data_type = postgres_type_for_metric(metric.agg.as_ref());
            let mut column = json!({
                "table_catalog": "sidemantic",
                "table_schema": schema,
                "table_name": model.name,
                "column_name": metric.name,
                "ordinal_position": ordinal,
                "column_default": null,
                "is_nullable": "YES",
                "data_type": data_type,
                "character_maximum_length": null,
                "numeric_precision": if data_type == "NUMERIC" { 38 } else { 64 },
                "numeric_scale": if data_type == "NUMERIC" { 10 } else { 0 },
                "is_primary_key": false,
                "is_foreign_key": false,
                "is_metric": true,
                "aggregation": metric_aggregation_name(metric.agg.as_ref()),
            });
            if let Some(description) = &metric.description {
                column["description"] = json!(description);
            }
            if let Some(label) = &metric.label {
                column["label"] = json!(label);
            }
            columns.push(column);
            ordinal += 1;
        }

        for relationship in &model.relationships {
            if matches!(
                relationship.r#type,
                RelationshipType::ManyToOne | RelationshipType::OneToOne
            ) {
                let fk_column = relationship.fk();
                let referenced_table = &relationship.name;
                let referenced_column = graph
                    .get_model(referenced_table)
                    .map(|target| {
                        target
                            .primary_keys()
                            .into_iter()
                            .next()
                            .unwrap_or_else(|| "id".to_string())
                    })
                    .unwrap_or_else(|| relationship.pk());
                let constraint_name = format!("{}_{}_fkey", model.name, fk_column);

                constraints.push(json!({
                    "constraint_catalog": "sidemantic",
                    "constraint_schema": schema,
                    "constraint_name": constraint_name,
                    "table_catalog": "sidemantic",
                    "table_schema": schema,
                    "table_name": model.name,
                    "constraint_type": "FOREIGN KEY",
                    "is_deferrable": "NO",
                    "initially_deferred": "NO",
                }));

                key_column_usage.push(json!({
                    "constraint_catalog": "sidemantic",
                    "constraint_schema": schema,
                    "constraint_name": constraint_name,
                    "table_catalog": "sidemantic",
                    "table_schema": schema,
                    "table_name": model.name,
                    "column_name": fk_column,
                    "ordinal_position": 1,
                    "position_in_unique_constraint": 1,
                    "referenced_table_schema": schema,
                    "referenced_table_name": referenced_table,
                    "referenced_column_name": referenced_column,
                }));

                for column in &mut columns {
                    if column["table_name"] == model.name && column["column_name"] == fk_column {
                        column["is_foreign_key"] = json!(true);
                        break;
                    }
                }
            }
        }
    }

    json!({
        "tables": tables,
        "columns": columns,
        "constraints": constraints,
        "key_column_usage": key_column_usage,
    })
}

fn postgres_type_for_dimension(
    dimension_type: &DimensionType,
    granularity: Option<&str>,
) -> &'static str {
    match dimension_type {
        DimensionType::Categorical => "VARCHAR",
        DimensionType::Numeric => "NUMERIC",
        DimensionType::Boolean => "BOOLEAN",
        DimensionType::Time => match granularity {
            Some("day" | "week" | "month" | "quarter" | "year") => "DATE",
            _ => "TIMESTAMP",
        },
    }
}

fn postgres_type_for_metric(aggregation: Option<&Aggregation>) -> &'static str {
    match aggregation {
        Some(Aggregation::Count | Aggregation::CountDistinct) => "BIGINT",
        _ => "NUMERIC",
    }
}

fn metric_aggregation_name(aggregation: Option<&Aggregation>) -> &'static str {
    match aggregation {
        Some(Aggregation::Sum) => "sum",
        Some(Aggregation::Count) => "count",
        Some(Aggregation::CountDistinct) => "count_distinct",
        Some(Aggregation::Avg) => "avg",
        Some(Aggregation::Min) => "min",
        Some(Aggregation::Max) => "max",
        Some(Aggregation::Median) => "median",
        Some(Aggregation::Stddev) => "stddev",
        Some(Aggregation::StddevPop) => "stddev_pop",
        Some(Aggregation::Variance) => "variance",
        Some(Aggregation::VariancePop) => "variance_pop",
        Some(Aggregation::Expression) | None => "sum",
    }
}

fn relationship_type_name(relationship_type: &RelationshipType) -> &'static str {
    match relationship_type {
        RelationshipType::ManyToOne => "many_to_one",
        RelationshipType::OneToOne => "one_to_one",
        RelationshipType::OneToMany => "one_to_many",
        RelationshipType::ManyToMany => "many_to_many",
    }
}

fn main() {
    let mut input = String::new();
    if let Err(err) = io::stdin().read_to_string(&mut input) {
        println!(
            "{}",
            serde_json::to_string(&Response::Error {
                error: format!("failed to read stdin: {err}")
            })
            .unwrap()
        );
        std::process::exit(1);
    }

    let response = serde_json::from_str::<Request>(&input)
        .map_err(|err| err.to_string())
        .and_then(|request| handle(request).map_err(|err| err.to_string()))
        .unwrap_or_else(|error| Response::Error { error });

    println!("{}", serde_json::to_string(&response).unwrap());
}
