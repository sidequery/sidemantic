//! Bounded Layer 2 binding for grand-total queries over one Ossie relation.

use super::*;
use polyglot_sql::expressions::{Alias, From};
use serde_json::Value;

fn key(identifier: &Identifier) -> String {
    if identifier.quoted {
        identifier.name.clone()
    } else {
        identifier.name.to_uppercase()
    }
}

fn source_key(name: &str) -> String {
    if name.starts_with('"') && name.ends_with('"') {
        name[1..name.len() - 1].replace("\"\"", "\"")
    } else {
        name.to_uppercase()
    }
}

fn unsupported() -> SidemanticError {
    SidemanticError::Validation(
        "Ossie AGG currently supports only direct metric projections over one relation".into(),
    )
}

fn empty(value: &Value) -> bool {
    value.is_null() || value == &Value::Bool(false) || value.as_array().is_some_and(Vec::is_empty)
}

impl QueryRewriter<'_> {
    pub(super) fn bind_ossie_aggregate_query(
        &self,
        statement: Expression,
        statement_count: usize,
    ) -> Result<Expression> {
        let models: Vec<_> = self
            .graph
            .models()
            .filter(|model| {
                model
                    .metadata
                    .as_ref()
                    .and_then(|meta| meta.get("ossie_scope_name"))
                    .and_then(Value::as_str)
                    .is_some()
            })
            .collect();
        if models.is_empty() {
            return Ok(statement);
        }
        let mut contains_agg = false;
        let statement = binding::transform_nodes(statement, &mut |node| {
            if matches!(node, Expression::Function(function) if function.name.eq_ignore_ascii_case("AGG"))
            {
                contains_agg = true;
            }
            Ok(None)
        })?;
        if !contains_agg {
            return Ok(statement);
        }
        if statement_count != 1 {
            return Err(unsupported());
        }
        let Expression::Select(mut select) = statement else {
            return Err(unsupported());
        };
        let value = serde_json::to_value(&select)
            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
        if value.as_object().unwrap().iter().any(|(name, value)| {
            !matches!(
                name.as_str(),
                "expressions" | "from" | "leading_comments" | "post_select_comments"
            ) && !empty(value)
        }) {
            return Err(unsupported());
        }
        let sources = select.from.as_ref().ok_or_else(unsupported)?;
        let [Expression::Table(table)] = sources.expressions.as_slice() else {
            return Err(unsupported());
        };
        let value = serde_json::to_value(table)
            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
        if value.as_object().unwrap().iter().any(|(name, value)| {
            !matches!(
                name.as_str(),
                "name" | "schema" | "alias" | "alias_explicit_as" | "trailing_comments" | "span"
            ) && !empty(value)
        }) {
            return Err(unsupported());
        }
        let model = models
            .into_iter()
            .find(|model| {
                let source = model
                    .metadata
                    .as_ref()
                    .and_then(|meta| meta.get("ossie_source_name"))
                    .and_then(Value::as_str)
                    .unwrap_or(&model.name);
                source_key(source) == key(&table.name)
            })
            .ok_or_else(|| SidemanticError::Validation("Ossie AGG relation not found".into()))?;
        if table.schema.as_ref().is_some_and(|schema| {
            key(schema)
                != source_key(
                    model.metadata.as_ref().unwrap()["ossie_scope_name"]
                        .as_str()
                        .unwrap(),
                )
        }) {
            return Err(SidemanticError::Validation(
                "Ossie AGG model namespace not found".into(),
            ));
        }
        let qualifier = table.alias.as_ref().unwrap_or(&table.name);
        let mut projections = Vec::new();
        for projection in &select.expressions {
            let (call, alias) = match projection {
                Expression::Alias(alias) => (&alias.this, Some(alias.alias.clone())),
                expression => (expression, None),
            };
            let Expression::Function(function) = call else {
                return Err(unsupported());
            };
            if !function.name.eq_ignore_ascii_case("AGG") || function.distinct {
                return Err(unsupported());
            }
            let [Expression::Column(argument)] = function.args.as_slice() else {
                return Err(SidemanticError::Validation(
                    "Ossie AGG requires one dataset metric reference".into(),
                ));
            };
            if argument
                .table
                .as_ref()
                .is_some_and(|table| key(table) != key(qualifier))
            {
                return Err(SidemanticError::Validation(
                    "Ossie AGG relation qualifier not found".into(),
                ));
            }
            let candidates: Vec<_> = self
                .graph
                .metrics()
                .filter(|metric| {
                    let Some(meta) = metric.metadata.as_ref() else {
                        return false;
                    };
                    meta.get("ossie_dataset_metric").and_then(Value::as_bool) == Some(true)
                        && self.graph.metric_owner(&metric.name) == Some(model.name.as_str())
                        && source_key(
                            meta.get("ossie_source_name")
                                .and_then(Value::as_str)
                                .unwrap_or_else(|| metric.name.rsplit('.').next().unwrap()),
                        ) == key(&argument.name)
                })
                .collect();
            let [metric] = candidates.as_slice() else {
                return Err(SidemanticError::Validation(
                    "Ossie AGG dataset metric not found".into(),
                ));
            };
            let mut bound =
                Expression::qualified_column(&model.name, metric.name.rsplit('.').next().unwrap());
            if let Expression::Column(column) = &mut bound {
                column.name.quoted = true;
                column.table.as_mut().unwrap().quoted = true;
            }
            projections.push(Expression::Alias(Box::new(Alias::new(
                bound,
                alias.unwrap_or_else(|| argument.name.clone()),
            ))));
        }
        select.expressions = projections;
        let mut source = TableRef::new(&model.name);
        source.name.quoted = true;
        select.from = Some(From {
            expressions: vec![Expression::Table(Box::new(source))],
        });
        Ok(Expression::Select(select))
    }
}
