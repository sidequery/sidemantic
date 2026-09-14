//! Preserve native SUM/AVG values while deduplicating the existing joined population.
use super::*;
use serde_json::Value;

fn unsupported() -> SidemanticError {
    SidemanticError::UnsupportedSemanticFeatures {
        capabilities: vec!["aggregation.fanout_expression".into()],
    }
}

fn expression(sql: &str, dialect: DialectType) -> Result<Expression> {
    #[cfg(target_arch = "wasm32")]
    crate::wasm_sql_guard::check(sql, dialect)?;
    let Expression::Select(mut select) = polyglot_sql::parse_one(&format!("SELECT {sql}"), dialect)
        .map_err(|error| SidemanticError::SqlParse(error.to_string()))?
    else {
        return Err(unsupported());
    };
    if select.expressions.len() != 1 || select.from.is_some() || select.where_clause.is_some() {
        return Err(unsupported());
    }
    Ok(select.expressions.remove(0))
}

fn json_error(error: serde_json::Error) -> SidemanticError {
    SidemanticError::SqlGeneration(error.to_string())
}

fn expand_having(value: &mut Value, outputs: &HashMap<String, Value>) -> Result<()> {
    match value {
        Value::Object(fields) if fields.len() == 1 && fields.contains_key("column") => {
            let column: polyglot_sql::expressions::Column =
                serde_json::from_value(fields["column"].clone()).map_err(json_error)?;
            if column.table.is_none() {
                *value = outputs
                    .get(&column.name.name.to_ascii_lowercase())
                    .cloned()
                    .ok_or_else(unsupported)?;
            }
        }
        Value::Object(fields) => {
            for child in fields.values_mut() {
                expand_having(child, outputs)?;
            }
        }
        Value::Array(children) => {
            for child in children {
                expand_having(child, outputs)?;
            }
        }
        _ => {}
    }
    Ok(())
}

struct Projection {
    dialect: DialectType,
    columns: HashMap<String, Value>,
    select: Vec<String>,
    outer_names: HashSet<String>,
    next_column: usize,
}

impl Projection {
    fn rewrite(&mut self, value: &mut Value) -> Result<()> {
        match value {
            Value::Object(fields) if fields.len() == 1 && fields.contains_key("column") => {
                let column: polyglot_sql::expressions::Column =
                    serde_json::from_value(fields["column"].clone()).map_err(json_error)?;
                if column.table.is_none()
                    && self
                        .outer_names
                        .contains(&column.name.name.to_ascii_lowercase())
                {
                    return Ok(());
                }
                // Unqualified source columns cannot be rebound safely after projection.
                if column.table.is_none() {
                    return Err(unsupported());
                }
                let sql =
                    polyglot_sql::generate(&Expression::Column(Box::new(column)), self.dialect)
                        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
                if let Some(replacement) = self.columns.get(&sql) {
                    *value = replacement.clone();
                } else {
                    let name = loop {
                        let candidate = format!("__fanout_column_{}", self.next_column);
                        self.next_column += 1;
                        if !self.outer_names.contains(&candidate) {
                            break candidate;
                        }
                    };
                    let replacement = serde_json::to_value(expression(&name, self.dialect)?)
                        .map_err(json_error)?;
                    self.select.push(format!("{sql} AS {name}"));
                    self.columns.insert(sql, replacement.clone());
                    *value = replacement;
                }
            }
            Value::Object(fields) => {
                if fields.contains_key("select")
                    || fields.contains_key("subquery")
                    || fields.contains_key("raw")
                {
                    return Err(unsupported());
                }
                for child in fields.values_mut() {
                    self.rewrite(child)?;
                }
            }
            Value::Array(children) => {
                for child in children {
                    self.rewrite(child)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn emit(&mut self, mut value: Value) -> Result<String> {
        self.rewrite(&mut value)?;
        let parsed: Expression = serde_json::from_value(value).map_err(json_error)?;
        polyglot_sql::generate(&parsed, self.dialect)
            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))
    }
}

impl SqlGenerator<'_> {
    pub(super) fn ranked_aggregate_source(
        &self,
        select: &[String],
        having: &[String],
        ranks: &[(String, String)],
        source: &str,
    ) -> Result<(String, Vec<String>)> {
        let expressions = select
            .iter()
            .map(|sql| expression(sql, self.dialect))
            .collect::<Result<Vec<_>>>()?;
        let mut outputs = HashMap::new();
        for parsed in &expressions {
            if let Expression::Alias(alias) = parsed {
                if outputs
                    .insert(
                        alias.alias.name.to_ascii_lowercase(),
                        serde_json::to_value(&alias.this).map_err(json_error)?,
                    )
                    .is_some()
                {
                    return Err(unsupported());
                }
            }
        }
        let mut projection = Projection {
            dialect: self.dialect,
            columns: HashMap::new(),
            select: Vec::new(),
            next_column: 0,
            outer_names: outputs
                .keys()
                .cloned()
                .chain(ranks.iter().map(|(name, _)| name.clone()))
                .collect(),
        };
        let mut outer = Vec::new();
        for parsed in expressions {
            outer.push(projection.emit(serde_json::to_value(parsed).map_err(json_error)?)?);
        }
        let mut outer_having = Vec::new();
        for predicate in having {
            let mut parsed =
                serde_json::to_value(expression(predicate, self.dialect)?).map_err(json_error)?;
            expand_having(&mut parsed, &outputs)?;
            outer_having.push(projection.emit(parsed)?);
        }
        projection
            .select
            .extend(ranks.iter().map(|(_, sql)| sql.clone()));
        Ok((
            format!(
                "SELECT\n{}\nFROM (SELECT\n{}\n{source}) AS __fanout_rows\n",
                outer.join(",\n"),
                projection.select.join(",\n")
            ),
            outer_having,
        ))
    }
}
