//! Standard SQL metadata for Ossie virtual relations and measure columns.
use super::*;
use serde_json::Value;

fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
fn identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
fn display(value: &str) -> String {
    if value.starts_with('"') && value.ends_with('"') {
        value[1..value.len() - 1].replace("\"\"", "\"")
    } else {
        value.into()
    }
}
fn name_key(value: &str) -> String {
    if value.starts_with('"') {
        display(value)
    } else {
        value.to_uppercase()
    }
}
fn metadata<'a>(value: &'a Option<Value>, key: &str) -> Option<&'a str> {
    value.as_ref()?.get(key)?.as_str()
}
fn rows(names: &[&str], rows: &[Vec<String>]) -> String {
    rows.iter()
        .map(|row| {
            format!(
                "SELECT {}",
                names
                    .iter()
                    .zip(row)
                    .map(|(name, value)| format!("{value} AS {}", identifier(name)))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ")
}

impl QueryRewriter<'_> {
    pub(super) fn rewrite_ossie_discovery(
        &self,
        sql: &str,
        input: DialectType,
        output: DialectType,
    ) -> Result<Option<String>> {
        let mut relations = Vec::new();
        for model in self.graph.models() {
            let Some(scope) = metadata(&model.metadata, "ossie_scope_name") else {
                continue;
            };
            let relation = metadata(&model.metadata, "ossie_source_name").unwrap_or(&model.name);
            let mut columns = Vec::new();
            for dimension in &model.dimensions {
                if let Some(source) = metadata(&dimension.metadata, "ossie_source_name") {
                    columns.push((
                        display(source),
                        dimension
                            .logical_data_type
                            .as_deref()
                            .unwrap_or("UNKNOWN")
                            .to_uppercase(),
                    ));
                }
            }
            let mut metrics: Vec<_> = self.graph.metrics().collect();
            metrics.sort_by_key(|metric| {
                (
                    metric
                        .metadata
                        .as_ref()
                        .and_then(|meta| meta.get("ossie_dataset_metric_index"))
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX),
                    metric.name.clone(),
                )
            });
            for metric in metrics {
                if metric
                    .metadata
                    .as_ref()
                    .and_then(|v| v.get("ossie_dataset_metric"))
                    .and_then(Value::as_bool)
                    == Some(true)
                    && self.graph.metric_owner(&metric.name) == Some(model.name.as_str())
                {
                    columns.push((
                        display(
                            metadata(&metric.metadata, "ossie_source_name").unwrap_or(&metric.name),
                        ),
                        format!(
                            "{} MEASURE",
                            metric
                                .logical_data_type
                                .as_deref()
                                .unwrap_or("UNKNOWN")
                                .to_uppercase()
                        ),
                    ));
                }
            }
            relations.push((None, scope.to_owned(), relation.to_owned(), columns, None));
        }
        if let Some(views) = self
            .graph
            .metadata()
            .and_then(|m| m.get("ossie_view_columns"))
            .and_then(Value::as_object)
        {
            for (registered_name, descriptors) in views {
                let parsed =
                    parse_rewrite_input(&format!("SELECT * FROM {registered_name}"), input)?;
                let Expression::Select(select) = &parsed[0] else {
                    continue;
                };
                let Some(from) = &select.from else { continue };
                let Some(Expression::Table(table)) = from.expressions.first() else {
                    continue;
                };
                let mut columns = Vec::new();
                for column in descriptors.as_array().into_iter().flatten() {
                    let Some(name) = column.get("name").and_then(Value::as_str) else {
                        continue;
                    };
                    let mut dtype = column
                        .get("data_type")
                        .and_then(Value::as_str)
                        .unwrap_or("UNKNOWN")
                        .to_uppercase();
                    if column.get("is_measure").and_then(Value::as_bool) == Some(true)
                        && !dtype.contains("MEASURE")
                    {
                        dtype.push_str(" MEASURE");
                    }
                    columns.push((name.to_owned(), dtype));
                }
                let scope = table
                    .schema
                    .as_ref()
                    .map(|id| {
                        if id.quoted {
                            identifier(&id.name)
                        } else {
                            id.name.clone()
                        }
                    })
                    .unwrap_or("main".into());
                let name = if table.name.quoted {
                    identifier(&table.name.name)
                } else {
                    table.name.name.clone()
                };
                relations.push((
                    table.catalog.as_ref().map(|id| id.name.clone()),
                    scope,
                    name,
                    columns,
                    Some(registered_name.clone()),
                ));
            }
        }
        if relations.is_empty() {
            return Ok(None);
        }
        // Avoid changing the parser behavior of unrelated extension syntax.
        let upper = sql.to_uppercase();
        if !upper.contains("DESCRIBE")
            && !upper.trim_start().starts_with("DESC ")
            && !upper.contains("INFORMATION_SCHEMA")
        {
            return Ok(None);
        }
        let mut statements = parse_rewrite_input(sql, input)?;
        if statements.len() != 1 {
            return Ok(None);
        }
        let statement = statements.remove(0);
        if let Expression::Describe(describe) = &statement {
            if let Expression::Table(table) = &describe.target {
                let key = if table.name.quoted {
                    table.name.name.clone()
                } else {
                    table.name.name.to_uppercase()
                };
                let bound = super::ossie_relational::bound_view_name(self.graph, table, input);
                let matches: Vec<_> = relations
                    .iter()
                    .filter(|(_, scope, name, _, registered_name)| {
                        if let Some(registered_name) = registered_name {
                            if self
                                .graph
                                .metadata()
                                .is_some_and(|m| m.get("ossie_view_bindings").is_some())
                            {
                                return bound == Some(registered_name.as_str());
                            }
                        }
                        bound.is_none()
                            && table.catalog.is_none()
                            && name_key(name) == key
                            && table.schema.as_ref().is_none_or(|schema| {
                                name_key(scope)
                                    == if schema.quoted {
                                        schema.name.clone()
                                    } else {
                                        schema.name.to_uppercase()
                                    }
                            })
                    })
                    .collect();
                if let [(_, _, _, columns, _)] = matches.as_slice() {
                    if self.security_controls {
                        return Err(policy::unsupported());
                    }
                    let data: Vec<_> = columns
                        .iter()
                        .map(|(name, dtype)| {
                            vec![
                                literal(name),
                                literal(dtype),
                                literal("YES"),
                                "NULL".into(),
                                "NULL".into(),
                                "NULL".into(),
                            ]
                        })
                        .collect();
                    let sql = rows(
                        &[
                            "column_name",
                            "column_type",
                            "null",
                            "key",
                            "default",
                            "extra",
                        ],
                        &data,
                    );
                    let parsed = parse_rewrite_input(&sql, DialectType::DuckDB)?.remove(0);
                    return Ok(Some(crate::semantic_input::dialects::emit(
                        parsed,
                        DialectType::DuckDB,
                        output,
                    )?));
                }
            }
            if matches!(
                &describe.target,
                Expression::Select(_) | Expression::Union(_)
            ) {
                if !self.ossie_relational_query_relevant(&describe.target, input)? {
                    return Ok(Some(sql.to_owned()));
                }
                if self.security_controls {
                    return Err(policy::unsupported());
                }
                let query = crate::semantic_input::dialects::emit(
                    describe.target.clone(),
                    DialectType::DuckDB,
                    input,
                )?;
                let (_, columns) = self.compile_ossie_view_definition(&query, input, output)?;
                let data: Vec<_> = columns
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|column| {
                        let mut dtype = column
                            .get("data_type")
                            .and_then(Value::as_str)
                            .unwrap_or("UNKNOWN")
                            .to_uppercase();
                        if column.get("is_measure").and_then(Value::as_bool) == Some(true)
                            && !dtype.contains("MEASURE")
                        {
                            dtype.push_str(" MEASURE");
                        }
                        vec![
                            literal(column.get("name").and_then(Value::as_str).unwrap_or("")),
                            literal(&dtype),
                            literal("YES"),
                            "NULL".into(),
                            "NULL".into(),
                            "NULL".into(),
                        ]
                    })
                    .collect();
                let sql = rows(
                    &[
                        "column_name",
                        "column_type",
                        "null",
                        "key",
                        "default",
                        "extra",
                    ],
                    &data,
                );
                let parsed = parse_rewrite_input(&sql, DialectType::DuckDB)?.remove(0);
                return Ok(Some(crate::semantic_input::dialects::emit(
                    parsed,
                    DialectType::DuckDB,
                    output,
                )?));
            }
            return Ok(Some(sql.to_owned()));
        }
        let mut changed = false;
        let statement = binding::transform_nodes(statement, &mut |node| {
            let Expression::Table(table) = node else {
                return Ok(None);
            };
            if !table
                .schema
                .as_ref()
                .is_some_and(|s| s.name.eq_ignore_ascii_case("information_schema"))
            {
                return Ok(None);
            }
            let kind = table.name.name.to_lowercase();
            if kind != "columns" && kind != "tables" {
                return Ok(None);
            }
            if self.security_controls {
                return Err(policy::unsupported());
            }
            let names: &[&str] = if kind == "columns" {
                &[
                    "table_catalog",
                    "table_schema",
                    "table_name",
                    "column_name",
                    "ordinal_position",
                    "column_default",
                    "is_nullable",
                    "data_type",
                ]
            } else {
                &["table_catalog", "table_schema", "table_name", "table_type"]
            };
            let mut data = Vec::new();
            for (catalog, scope, name, columns, _) in &relations {
                let catalog = catalog
                    .as_deref()
                    .map(literal)
                    .unwrap_or("CURRENT_CATALOG".into());
                if kind == "tables" {
                    data.push(vec![
                        catalog.clone(),
                        literal(&display(scope)),
                        literal(&display(name)),
                        literal("VIEW"),
                    ]);
                } else {
                    for (index, (column, dtype)) in columns.iter().enumerate() {
                        data.push(vec![
                            catalog.clone(),
                            literal(&display(scope)),
                            literal(&display(name)),
                            literal(column),
                            (index + 1).to_string(),
                            "NULL".into(),
                            literal("YES"),
                            literal(dtype),
                        ]);
                    }
                }
            }
            if data.is_empty() {
                return Ok(None);
            }
            let physical = if output == DialectType::DuckDB {
                "*".into()
            } else {
                names.join(", ")
            };
            let by_name = if output == DialectType::DuckDB {
                " BY NAME"
            } else {
                ""
            };
            let alias = table
                .alias
                .as_ref()
                .map(|a| a.name.as_str())
                .unwrap_or(&table.name.name);
            let exclusion = relations
                .iter()
                .map(|(catalog, scope, name, _, _)| {
                    format!(
                        "(table_catalog={} AND table_schema={} AND table_name={})",
                        catalog
                            .as_deref()
                            .map(literal)
                            .unwrap_or("CURRENT_CATALOG".into()),
                        literal(&display(scope)),
                        literal(&display(name))
                    )
                })
                .collect::<Vec<_>>()
                .join(" OR ");
            let sql=format!("SELECT * FROM (SELECT {physical} FROM information_schema.{kind} WHERE NOT ({exclusion}) UNION ALL{by_name} ({})) AS {}",rows(names,&data),identifier(alias));
            let Expression::Select(select) =
                parse_rewrite_input(&sql, DialectType::DuckDB)?.remove(0)
            else {
                unreachable!()
            };
            changed = true;
            Ok(Some(select.from.unwrap().expressions.remove(0)))
        })?;
        if !changed {
            return Ok(None);
        }
        Ok(Some(crate::semantic_input::dialects::emit(
            statement,
            DialectType::DuckDB,
            output,
        )?))
    }
}
