//! Collect physical inputs without allowing nested SQL aliases to capture them.
use super::*;

type Bindings = HashMap<String, Option<String>>;

fn bind_source(
    source: &Expression,
    bindings: &mut Bindings,
    ctes: &HashSet<String>,
    sources: &HashSet<String>,
) {
    match source {
        Expression::Table(table) => {
            let source = (table.schema.is_none()
                && table.catalog.is_none()
                && sources.contains(&table.name.name)
                && !ctes.contains(&table.name.name))
            .then(|| table.name.name.clone());
            bindings.insert(
                table.alias.as_ref().unwrap_or(&table.name).name.clone(),
                source,
            );
        }
        Expression::Alias(alias) => {
            let mut local = Bindings::new();
            bind_source(&alias.this, &mut local, ctes, sources);
            let source = local.into_values().next().flatten();
            bindings.insert(alias.alias.name.clone(), source);
        }
        Expression::Subquery(query) => {
            if let Some(alias) = &query.alias {
                bindings.insert(alias.name.clone(), None);
            }
        }
        Expression::JoinedTable(joined) => {
            if let Some(alias) = &joined.alias {
                bindings.insert(alias.name.clone(), None);
            } else {
                bind_source(&joined.left, bindings, ctes, sources);
                for join in &joined.joins {
                    bind_source(&join.this, bindings, ctes, sources);
                }
            }
        }
        _ => {}
    }
}

fn collect(
    value: &serde_json::Value,
    bindings: &Bindings,
    ctes: &HashSet<String>,
    sources: &HashSet<String>,
    columns: &mut HashMap<String, HashSet<String>>,
) -> Result<()> {
    match value {
        serde_json::Value::Object(fields) => {
            if let Some(select) = fields.get("select").filter(|_| fields.len() == 1) {
                let parsed: polyglot_sql::expressions::Select =
                    serde_json::from_value(select.clone())
                        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
                let mut ctes = ctes.clone();
                if let Some(with) = &parsed.with {
                    if with.recursive {
                        ctes.extend(with.ctes.iter().map(|cte| cte.alias.name.clone()));
                    }
                    for cte in &with.ctes {
                        let body = serde_json::to_value(&cte.this)
                            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
                        collect(&body, bindings, &ctes, sources, columns)?;
                        ctes.insert(cte.alias.name.clone());
                    }
                }
                let mut local = bindings.clone();
                for source in parsed
                    .from
                    .iter()
                    .flat_map(|from| &from.expressions)
                    .chain(parsed.joins.iter().map(|join| &join.this))
                {
                    bind_source(source, &mut local, &ctes, sources);
                }
                for (key, child) in select.as_object().expect("serialized SELECT") {
                    if key != "with" {
                        collect(child, &local, &ctes, sources, columns)?;
                    }
                }
            } else if let Some(lambda) = fields.get("lambda").filter(|_| fields.len() == 1) {
                let mut local = bindings.clone();
                for parameter in lambda["parameters"].as_array().into_iter().flatten() {
                    if let Some(name) = parameter["name"].as_str() {
                        local.insert(name.to_string(), None);
                    }
                }
                collect(&lambda["body"], &local, ctes, sources, columns)?;
            } else if let Some(column) = fields.get("column").filter(|_| fields.len() == 1) {
                let column: polyglot_sql::expressions::Column =
                    serde_json::from_value(column.clone())
                        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
                if let Some(source) = column
                    .table
                    .as_ref()
                    .and_then(|table| bindings.get(&table.name))
                    .and_then(Option::as_ref)
                {
                    columns
                        .entry(source.clone())
                        .or_default()
                        .insert(column.name.name);
                }
            } else {
                for child in fields.values() {
                    collect(child, bindings, ctes, sources, columns)?;
                }
            }
        }
        serde_json::Value::Array(items) => {
            for child in items {
                collect(child, bindings, ctes, sources, columns)?;
            }
        }
        _ => {}
    }
    Ok(())
}

impl SqlGenerator<'_> {
    /// Walk the complete AST, including typed aggregate children omitted by the
    /// parser's visitor, while preserving relation and lambda scopes.
    pub(super) fn query_source_columns(
        &self,
        sql: &str,
    ) -> Result<HashMap<String, HashSet<String>>> {
        let expression = polyglot_sql::parse_one(sql, self.dialect)
            .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
        let value = serde_json::to_value(expression)
            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
        let sources = self
            .graph
            .model_instances()
            .map(|name| self.model_alias(name))
            .collect();
        let mut columns = HashMap::new();
        collect(
            &value,
            &Bindings::new(),
            &HashSet::new(),
            &sources,
            &mut columns,
        )?;
        Ok(columns)
    }
}
