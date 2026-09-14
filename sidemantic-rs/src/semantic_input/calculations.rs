//! Result calculations wrap finalized (including paginated) semantic SQL.
use std::collections::{HashMap, HashSet};

use polyglot_sql::{DialectType, Expression};
use serde::Deserialize;
use serde_json::Value;

use super::{invalid, unsupported};
use crate::error::Result;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Calculation {
    #[serde(rename = "description")]
    _description: Option<String>,
    name: String,
    #[serde(rename = "type")]
    kind: String,
    expression: Option<String>,
    field: Option<String>,
    partition_by: Option<Vec<String>>,
    order_by: Option<Vec<String>>,
    window_size: Option<usize>,
    percentile: Option<f64>,
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
fn reference(name: &str, columns: &[String]) -> Result<String> {
    if !columns.iter().any(|column| column == name) {
        return Err(invalid(
            "query.table_calculations",
            format!("reference is not an available result column: {name}"),
        ));
    }
    Ok(quote(name))
}

/// A deliberately bounded arithmetic grammar; every identifier comes from a
/// selected ${column}. Division by zero follows the row processor's NULL result.
struct Formula<'a> {
    source: &'a str,
    position: usize,
    columns: &'a [String],
    depth: usize,
}
impl Formula<'_> {
    fn whitespace(&mut self) {
        while self
            .source
            .as_bytes()
            .get(self.position)
            .is_some_and(u8::is_ascii_whitespace)
        {
            self.position += 1;
        }
    }
    fn expression(&mut self, minimum: u8) -> Result<String> {
        self.depth += 1;
        if self.depth > 64 {
            return Err(unsupported("table_calculation.formula_depth"));
        }
        self.whitespace();
        let mut left = match self.source.as_bytes().get(self.position).copied() {
            Some(b'+' | b'-') => {
                let sign = self.source.as_bytes()[self.position] as char;
                self.position += 1;
                format!("({sign}{})", self.expression(3)?)
            }
            Some(b'(') => {
                self.position += 1;
                let value = self.expression(0)?;
                self.whitespace();
                if self.source.as_bytes().get(self.position) != Some(&b')') {
                    return Err(invalid(
                        "query.table_calculations",
                        "formula missing closing parenthesis",
                    ));
                }
                self.position += 1;
                value
            }
            Some(b'$') if self.source[self.position..].starts_with("${") => {
                self.position += 2;
                let end = self.source[self.position..].find('}').ok_or_else(|| {
                    invalid("query.table_calculations", "unclosed formula reference")
                })? + self.position;
                let field = reference(&self.source[self.position..end], self.columns)?;
                self.position = end + 1;
                format!("COALESCE({field}, 0)")
            }
            Some(b'0'..=b'9' | b'.') => {
                let start = self.position;
                while self
                    .source
                    .as_bytes()
                    .get(self.position)
                    .is_some_and(|c| c.is_ascii_digit() || *c == b'.')
                {
                    self.position += 1;
                }
                if matches!(self.source.as_bytes().get(self.position), Some(b'e' | b'E')) {
                    self.position += 1;
                    if matches!(self.source.as_bytes().get(self.position), Some(b'+' | b'-')) {
                        self.position += 1;
                    }
                    while self
                        .source
                        .as_bytes()
                        .get(self.position)
                        .is_some_and(u8::is_ascii_digit)
                    {
                        self.position += 1;
                    }
                }
                let token = &self.source[start..self.position];
                if !token.parse::<f64>().is_ok_and(f64::is_finite)
                    || (token.len() > 1
                        && token.starts_with('0')
                        && token.as_bytes()[1].is_ascii_digit()
                        && !token.contains(['.', 'e', 'E']))
                {
                    return Err(invalid(
                        "query.table_calculations",
                        "invalid numeric formula constant",
                    ));
                }
                token.to_owned()
            }
            _ => return Err(unsupported("table_calculation.formula_expression")),
        };
        loop {
            self.whitespace();
            let Some(operator) = self.source.as_bytes().get(self.position).copied() else {
                break;
            };
            let precedence = match operator {
                b'+' | b'-' => 1,
                b'*' | b'/' => 2,
                _ => break,
            };
            if precedence < minimum {
                break;
            }
            self.position += 1;
            let right = self.expression(precedence + 1)?;
            left = if operator == b'/' {
                format!("(CAST({left} AS DOUBLE PRECISION) / NULLIF({right}, 0))")
            } else {
                format!("({left} {} {right})", operator as char)
            };
        }
        self.depth -= 1;
        Ok(left)
    }
}

/// Special generators (retention and cohort entity dimensions) can expand the
/// result beyond the requested metric/dimension lists. The finalized projection
/// is authoritative for a post-result calculation's columns and their order.
fn output_columns(sql: &str, dialect: DialectType) -> Result<Vec<String>> {
    #[cfg(target_arch = "wasm32")]
    crate::wasm_sql_guard::check(sql, dialect)?;
    let expression = polyglot_sql::parse_one(sql, dialect)
        .map_err(|error| invalid("query.table_calculations", error))?;
    let Expression::Select(select) = expression else {
        return Err(unsupported("table_calculation.result_projection"));
    };
    if select.expressions.is_empty() {
        return Err(unsupported("table_calculation.result_projection"));
    }
    select
        .expressions
        .iter()
        .map(|projection| match projection {
            Expression::Alias(alias) => Ok(alias.alias.name.clone()),
            Expression::Column(column) if column.name.name != "*" => Ok(column.name.name.clone()),
            _ => Err(unsupported("table_calculation.result_projection")),
        })
        .collect()
}

pub(super) fn wrap(
    sql: String,
    catalog: &Value,
    names: &[String],
    order_by: &[String],
    dialect: DialectType,
    aliases: &HashMap<String, String>,
) -> Result<String> {
    if names.is_empty() {
        return Ok(sql);
    }
    if !matches!(dialect, DialectType::DuckDB | DialectType::PostgreSQL) {
        return Err(unsupported("table_calculation.output_dialect"));
    }
    let mut columns = output_columns(&sql, dialect)?;
    let mut prefix = "__sidemantic_calc_".to_owned();
    while sql.to_ascii_lowercase().contains(&prefix)
        || columns
            .iter()
            .chain(names)
            .any(|name| name.to_ascii_lowercase().starts_with(&prefix))
    {
        prefix.push('_');
    }
    let mut seen = HashSet::new();
    for name in columns.iter().chain(names) {
        if !seen.insert(name.to_ascii_lowercase()) {
            return Err(invalid(
                "query.table_calculations",
                "duplicate or colliding result/calculation name",
            ));
        }
    }
    let mut ordering = Vec::new();
    let order_names: Vec<_> = columns
        .iter()
        .chain(aliases.keys())
        .chain(aliases.values())
        .map(String::as_str)
        .collect();
    for item in order_by {
        let (field, suffix) = crate::sql::split_order_field(item, &order_names);
        if field.is_empty() {
            return Err(invalid("query.order_by", "empty ordering"));
        }
        let output_alias = aliases.get(field).or_else(|| {
            if field.contains('.') || columns.iter().any(|column| column == field) {
                return None;
            }
            let matches: HashSet<_> = aliases
                .iter()
                .filter(|(name, _)| name.rsplit('.').next() == Some(field))
                .map(|(_, alias)| alias)
                .collect();
            if matches.len() == 1 {
                matches.into_iter().next()
            } else {
                None
            }
        });
        let field = output_alias.map_or(field, String::as_str);
        let collided = field.replace('.', "_");
        let field = if columns.iter().any(|c| c.as_str() == field) {
            field
        } else if columns.contains(&collided) {
            &collided
        } else {
            field.rsplit('.').next().unwrap_or(field)
        };
        ordering.push(format!("{} {suffix}", reference(field, &columns)?));
    }
    let ordinal = quote(&format!("{prefix}ordinal"));
    let window_order = if ordering.is_empty() {
        String::new()
    } else {
        format!("ORDER BY {}", ordering.join(", "))
    };
    let mut ctes = vec![format!("{prefix}base AS (\n{}\n)", sql.trim_end().trim_end_matches(';')), format!("{prefix}0 AS (SELECT *, ROW_NUMBER() OVER ({window_order}) AS {ordinal} FROM {prefix}base)")];
    let mut previous = format!("{prefix}0");
    for (index, name) in names.iter().enumerate() {
        let index = index + 1;
        let declaration = catalog
            .as_array()
            .and_then(|items| {
                items
                    .iter()
                    .find(|c| c.get("name").and_then(Value::as_str) == Some(name.as_str()))
            })
            .ok_or_else(|| {
                invalid(
                    "query.table_calculations",
                    format!("unknown calculation: {name}"),
                )
            })?;
        let calc: Calculation = serde_json::from_value(declaration.clone())
            .map_err(|e| invalid("query.table_calculations", e))?;
        if calc.order_by.as_ref().is_some_and(|v| !v.is_empty())
            || (calc.kind != "percent_of_column_total"
                && calc.partition_by.as_ref().is_some_and(|v| !v.is_empty()))
        {
            return Err(unsupported("table_calculation.order_partition_controls"));
        }
        if matches!(
            calc.kind.as_str(),
            "running_total" | "percent_of_previous" | "row_number" | "moving_average" | "rank"
        ) && ordering.is_empty()
        {
            return Err(invalid(
                "query.table_calculations",
                format!("{} requires explicit selected-column order_by", calc.name),
            ));
        }
        let field = if matches!(calc.kind.as_str(), "formula" | "row_number") {
            String::new()
        } else {
            reference(calc.field.as_deref().unwrap_or(""), &columns)?
        };
        let value = format!("COALESCE({field}, 0)");
        let result = match calc.kind.as_str() {
            "formula" => {
                let source = calc.expression.as_deref().unwrap_or("");
                if source.len() > 8192 { return Err(unsupported("table_calculation.formula_length")); }
                let mut formula = Formula { source, position: 0, columns: &columns, depth: 0 };
                let result = formula.expression(0)?;
                formula.whitespace();
                if formula.position != source.len() { return Err(unsupported("table_calculation.formula_expression")); }
                result
            }
            "percent_of_total" | "percent_of_column_total" => {
                let partition: Vec<_> = calc.partition_by.as_deref().unwrap_or_default().iter().map(|p| reference(p, &columns)).collect::<Result<_>>()?;
                let partition = if partition.is_empty() { String::new() } else { format!("PARTITION BY {}", partition.join(", ")) };
                format!("COALESCE(CAST({value} AS DOUBLE PRECISION) / NULLIF(SUM({value}) OVER ({partition}), 0) * 100, 0)")
            }
            "running_total" => format!("SUM({value}) OVER (ORDER BY {ordinal} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"),
            "percent_of_previous" => {
                let prior = format!("LAG({field}) OVER (ORDER BY {ordinal})");
                format!("CAST(({field} - {prior}) AS DOUBLE PRECISION) / NULLIF({prior}, 0) * 100")
            }
            "row_number" => ordinal.clone(),
            "moving_average" => {
                let window = calc.window_size.filter(|w| *w > 0).ok_or_else(|| invalid("query.table_calculations", "moving_average requires positive window_size"))? - 1;
                format!("AVG(CAST({value} AS DOUBLE PRECISION)) OVER (ORDER BY {ordinal} ROWS BETWEEN {window} PRECEDING AND CURRENT ROW)")
            }
            "percentile" => {
                let p = calc.percentile.filter(|p| (0.0..=1.0).contains(p)).ok_or_else(|| invalid("query.table_calculations", "percentile requires value between 0 and 1"))?;
                format!("(SELECT PERCENTILE_CONT({p}) WITHIN GROUP (ORDER BY {field}) FROM {previous})")
            }
            "rank" => {
                let position = quote(&format!("{prefix}position"));
                let boundary = quote(&format!("{prefix}boundary"));
                let rank_order = format!("ORDER BY {value} DESC, {ordinal}");
                let rank_cte = format!("{prefix}rank{index}");
                ctes.push(format!("{rank_cte} AS (SELECT *, ROW_NUMBER() OVER ({rank_order}) AS {position}, CASE WHEN {field} IS DISTINCT FROM LAG({field}) OVER ({rank_order}) THEN 1 ELSE 0 END AS {boundary} FROM {previous})"));
                previous = rank_cte;
                format!("COALESCE(MAX(CASE WHEN {boundary} = 1 THEN {position} END) OVER (ORDER BY {position} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 1)")
            }
            _ => return Err(unsupported(format!("table_calculation.{}", calc.kind))),
        };
        let current = format!("{prefix}{index}");
        let projection = columns
            .iter()
            .map(|c| quote(c))
            .collect::<Vec<_>>()
            .join(", ");
        ctes.push(format!(
            "{current} AS (SELECT {projection}, {ordinal}, {result} AS {} FROM {previous})",
            quote(name)
        ));
        previous = current;
        columns.push(name.clone());
    }
    Ok(format!(
        "WITH {}\nSELECT {} FROM {previous} ORDER BY {ordinal}",
        ctes.join(",\n"),
        columns
            .iter()
            .map(|c| quote(c))
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::semantic_input::compile_with_semantic_input;

    #[test]
    fn actual_projection_keeps_expanded_columns_and_quoted_aliases() {
        for dialect in [DialectType::DuckDB, DialectType::PostgreSQL] {
            assert_eq!(
                output_columns("WITH inner_rows AS (SELECT 1 AS hidden) SELECT cohort_date, days_since, active_users, cohort_size, retention_pct FROM inner_rows", dialect).unwrap(),
                ["cohort_date", "days_since", "active_users", "cohort_size", "retention_pct"]
            );
            assert_eq!(
                output_columns("WITH inner_rows AS (SELECT 1 AS hidden) SELECT region AS \"Region Name\", COUNT(*) AS qualified FROM inner_rows GROUP BY region", dialect).unwrap(),
                ["Region Name", "qualified"]
            );
            for sql in [
                "SELECT * FROM inner_rows",
                "SELECT inner_rows.* FROM inner_rows",
                "SELECT 1 + 2 FROM inner_rows",
            ] {
                assert!(output_columns(sql, dialect).is_err());
            }
        }
    }

    #[test]
    fn selected_kinds_compile_on_both_qualified_dialects() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/selected_calculations.json"
        ))
        .unwrap();
        for dialect in ["duckdb", "postgres"] {
            let mut query = fixture["query"].clone();
            query["dialect"] = dialect.into();
            let sql =
                compile_with_semantic_input(&fixture["source"].to_string(), &query.to_string())
                    .unwrap();
            assert!(sql.contains("PERCENTILE_CONT"));
            assert!(sql.contains("IS DISTINCT FROM LAG"));
            assert!(sql.ends_with("ORDER BY \"__sidemantic_calc_ordinal\""));
        }
    }

    #[test]
    fn formula_grammar_and_selected_reference_gates() {
        let columns = vec!["value".to_owned()];
        for source in ["${value} / (2 - 2)", "-(${value} + 2.5e-2) * 3"] {
            let mut parser = Formula {
                source,
                position: 0,
                columns: &columns,
                depth: 0,
            };
            assert!(parser.expression(0).is_ok());
            assert_eq!(parser.position, source.len());
        }
        for source in [
            "${absent}",
            "_ref0",
            "true",
            "1e309",
            "${value} ** 2",
            "${value} // 2",
        ] {
            let mut parser = Formula {
                source,
                position: 0,
                columns: &columns,
                depth: 0,
            };
            assert!(parser.expression(0).is_err());
        }
    }
}
