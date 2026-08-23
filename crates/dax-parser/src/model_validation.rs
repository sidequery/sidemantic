//! Offline, caller-supplied model name resolution for DAX syntax trees.
//!
//! Parsing deliberately does not require a semantic model.  This module is the
//! optional layer for callers that do have model metadata and want diagnostics
//! for unresolved or ambiguous table, column, and measure references.

use std::collections::{HashMap, HashSet};

use serde::Serialize;

use crate::{Definition, Expr, Query, TableName, VisualShape};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
pub struct ModelMetadata {
    pub tables: Vec<ModelTable>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ModelTable {
    pub name: String,
    pub columns: Vec<String>,
    pub measures: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ModelValidationCode {
    UnknownTable,
    UnknownMember,
    UnknownIdentifier,
    AmbiguousReference,
    ConflictingVariableName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ModelValidationIssue {
    pub code: ModelValidationCode,
    pub message: String,
    /// Stable structural location that callers can correlate with lossless node spans.
    pub path: String,
}

#[derive(Debug, Clone)]
struct NamedSymbol {
    display: String,
}

#[derive(Debug, Clone)]
struct MeasureSymbol {
    table_key: Option<String>,
}

#[derive(Debug, Default)]
struct ModelIndex {
    model_tables: HashMap<String, Vec<NamedSymbol>>,
    query_tables: HashMap<String, Vec<NamedSymbol>>,
    query_tables_with_known_schema: HashSet<String>,
    model_columns: HashMap<String, HashMap<String, Vec<NamedSymbol>>>,
    query_columns: HashMap<String, HashMap<String, Vec<NamedSymbol>>>,
    model_measures: HashMap<String, Vec<MeasureSymbol>>,
    query_measures: HashMap<String, Vec<MeasureSymbol>>,
}

fn key(name: &str) -> String {
    // DAX identifiers are case-insensitive. Unicode lowercasing is closer to
    // Tabular's invariant comparison than ASCII-only folding and avoids making
    // non-ASCII identifiers unexpectedly case-sensitive.
    name.to_lowercase()
}

fn push_named(map: &mut HashMap<String, Vec<NamedSymbol>>, name: &str) {
    map.entry(key(name)).or_default().push(NamedSymbol {
        display: name.to_string(),
    });
}

fn push_column(
    map: &mut HashMap<String, HashMap<String, Vec<NamedSymbol>>>,
    table: &str,
    column: &str,
) {
    push_named(map.entry(key(table)).or_default(), column);
}

fn push_measure(map: &mut HashMap<String, Vec<MeasureSymbol>>, table: Option<&str>, measure: &str) {
    map.entry(key(measure)).or_default().push(MeasureSymbol {
        table_key: table.map(key),
    });
}

impl ModelIndex {
    fn from_model(model: &ModelMetadata) -> Self {
        let mut index = Self::default();
        for table in &model.tables {
            push_named(&mut index.model_tables, &table.name);
            // Preserve the distinction between a known-empty schema and a
            // table expression whose output schema cannot be inferred.
            index.model_columns.entry(key(&table.name)).or_default();
            for column in &table.columns {
                push_column(&mut index.model_columns, &table.name, column);
            }
            for measure in &table.measures {
                push_measure(&mut index.model_measures, Some(&table.name), measure);
            }
        }
        index
    }

    fn add_query_definitions(&mut self, query: &Query) {
        let Some(define) = &query.define else {
            return;
        };

        // Register objects before walking expressions so forward references in
        // a DEFINE block resolve consistently with query-scoped definitions.
        for definition in &define.defs {
            match definition {
                Definition::Table { name, expr, .. } => {
                    push_named(&mut self.query_tables, name);
                    // DATATABLE exposes an exact output schema. Other table
                    // expressions do not yet carry inferred output columns, so
                    // avoid guessing their shape.
                    if let Expr::DataTable { columns, .. } = expr {
                        self.query_tables_with_known_schema.insert(key(name));
                        self.query_columns.entry(key(name)).or_default();
                        for column in columns {
                            push_column(&mut self.query_columns, name, &column.name);
                        }
                    }
                }
                Definition::Column {
                    table: Some(table),
                    name,
                    ..
                } => push_column(&mut self.query_columns, &table.name, name),
                Definition::Measure { table, name, .. } => push_measure(
                    &mut self.query_measures,
                    table.as_ref().map(|table| table.name.as_str()),
                    name,
                ),
                Definition::Column { table: None, .. }
                | Definition::Var { .. }
                | Definition::Function { .. } => {}
            }
        }
    }

    fn tables(&self, name: &str) -> &[NamedSymbol] {
        let normalized = key(name);
        self.query_tables
            .get(&normalized)
            .or_else(|| self.model_tables.get(&normalized))
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    fn columns(&self, table: &str, column: &str) -> &[NamedSymbol] {
        let table_key = key(table);
        let column_key = key(column);
        let columns = if self.query_tables.contains_key(&table_key) {
            self.query_columns
                .get(&table_key)
                .and_then(|columns| columns.get(&column_key))
        } else {
            self.query_columns
                .get(&table_key)
                .and_then(|columns| columns.get(&column_key))
                .or_else(|| {
                    self.model_columns
                        .get(&table_key)
                        .and_then(|columns| columns.get(&column_key))
                })
        };
        columns.map(Vec::as_slice).unwrap_or_default()
    }

    fn measures(&self, name: &str) -> &[MeasureSymbol] {
        let normalized = key(name);
        self.query_measures
            .get(&normalized)
            .or_else(|| self.model_measures.get(&normalized))
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    fn qualified_measures(&self, table: &str, name: &str) -> Vec<&MeasureSymbol> {
        let table_key = key(table);
        let name_key = key(name);
        let query_matches = self
            .query_measures
            .get(&name_key)
            .map_or_else(Vec::new, |symbols| {
                symbols
                    .iter()
                    .filter(|measure| measure.table_key.as_deref() == Some(table_key.as_str()))
                    .collect()
            });
        if query_matches.is_empty() {
            self.model_measures
                .get(&name_key)
                .map_or_else(Vec::new, |symbols| {
                    symbols
                        .iter()
                        .filter(|measure| measure.table_key.as_deref() == Some(table_key.as_str()))
                        .collect()
                })
        } else {
            query_matches
        }
    }

    fn table_schema_is_known(&self, table: &str) -> bool {
        let table = key(table);
        if self.query_tables.contains_key(&table) {
            self.query_tables_with_known_schema.contains(&table)
        } else {
            self.model_tables.contains_key(&table)
        }
    }

    fn conflicts_with_model_table(&self, name: &str) -> bool {
        self.model_tables.contains_key(&key(name))
    }
}

fn issue(
    issues: &mut Vec<ModelValidationIssue>,
    code: ModelValidationCode,
    path: &str,
    message: impl Into<String>,
) {
    issues.push(ModelValidationIssue {
        code,
        message: message.into(),
        path: path.to_string(),
    });
}

fn describe_named(symbols: &[NamedSymbol]) -> String {
    symbols
        .iter()
        .map(|symbol| format!("`{}`", symbol.display))
        .collect::<Vec<_>>()
        .join(", ")
}

fn resolve_table(
    table: &TableName,
    path: &str,
    index: &ModelIndex,
    issues: &mut Vec<ModelValidationIssue>,
) -> bool {
    match index.tables(&table.name) {
        [] => {
            issue(
                issues,
                ModelValidationCode::UnknownTable,
                path,
                format!("unknown table `{}`", table.name),
            );
            false
        }
        [_] => true,
        candidates => {
            issue(
                issues,
                ModelValidationCode::AmbiguousReference,
                path,
                format!(
                    "table reference `{}` is ambiguous between {}",
                    table.name,
                    describe_named(candidates)
                ),
            );
            false
        }
    }
}

fn resolve_qualified_member(
    table: &TableName,
    member: &str,
    path: &str,
    index: &ModelIndex,
    issues: &mut Vec<ModelValidationIssue>,
) {
    if !resolve_table(table, &format!("{path}.table"), index, issues) {
        return;
    }

    let columns = index.columns(&table.name, member);
    let measures = index.qualified_measures(&table.name, member);
    let count = columns.len() + measures.len();
    match count {
        0 if index.table_schema_is_known(&table.name) => issue(
            issues,
            ModelValidationCode::UnknownMember,
            path,
            format!("unknown column or measure `{}[{}]`", table.name, member),
        ),
        0 => {}
        1 => {}
        _ => issue(
            issues,
            ModelValidationCode::AmbiguousReference,
            path,
            format!(
                "reference `{}[{}]` matches {count} columns or measures",
                table.name, member
            ),
        ),
    }
}

fn resolve_bracket_ref(
    name: &str,
    current_table: Option<&str>,
    path: &str,
    index: &ModelIndex,
    issues: &mut Vec<ModelValidationIssue>,
) {
    let mut match_count = index.measures(name).len();
    if let Some(table) = current_table {
        match_count += index.columns(table, name).len();
    } else {
        let mut table_keys = HashSet::new();
        table_keys.extend(index.model_columns.keys().cloned());
        table_keys.extend(index.query_columns.keys().cloned());
        for table in table_keys {
            match_count += index.columns(&table, name).len();
        }
    }

    match match_count {
        0 => issue(
            issues,
            ModelValidationCode::UnknownMember,
            path,
            format!("unknown unqualified column or measure `[{name}]`"),
        ),
        1 => {}
        _ => issue(
            issues,
            ModelValidationCode::AmbiguousReference,
            path,
            format!("unqualified reference `[{name}]` matches {match_count} columns or measures"),
        ),
    }
}

fn resolve_column(
    table: &str,
    column: &str,
    path: &str,
    index: &ModelIndex,
    issues: &mut Vec<ModelValidationIssue>,
) {
    match index.columns(table, column) {
        [] => issue(
            issues,
            ModelValidationCode::UnknownMember,
            path,
            format!("unknown column `{table}[{column}]`"),
        ),
        [_] => {}
        columns => issue(
            issues,
            ModelValidationCode::AmbiguousReference,
            path,
            format!(
                "column reference `{table}[{column}]` is ambiguous between {}",
                describe_named(columns)
            ),
        ),
    }
}

fn validate_variable_name(
    name: &str,
    path: &str,
    index: &ModelIndex,
    issues: &mut Vec<ModelValidationIssue>,
) {
    if index.conflicts_with_model_table(name) {
        issue(
            issues,
            ModelValidationCode::ConflictingVariableName,
            path,
            format!("variable `{name}` conflicts with a model table name"),
        );
    }
}

fn validate_expr(
    expr: &Expr,
    path: &str,
    index: &ModelIndex,
    lexical_scope: &HashSet<String>,
    current_table: Option<&str>,
    issues: &mut Vec<ModelValidationIssue>,
) {
    match expr {
        Expr::Number(_)
        | Expr::String(_)
        | Expr::DateTime(_)
        | Expr::Boolean(_)
        | Expr::Blank
        | Expr::Omitted
        | Expr::Parameter(_) => {}
        Expr::Identifier(name) => {
            if !lexical_scope.contains(&key(name)) {
                match index.tables(name) {
                    [] => issue(
                        issues,
                        ModelValidationCode::UnknownIdentifier,
                        path,
                        format!("unknown variable, parameter, or table `{name}`"),
                    ),
                    [_] => {}
                    tables => issue(
                        issues,
                        ModelValidationCode::AmbiguousReference,
                        path,
                        format!(
                            "identifier `{name}` is an ambiguous table reference between {}",
                            describe_named(tables)
                        ),
                    ),
                }
            }
        }
        Expr::TableRef(table) => {
            resolve_table(table, path, index, issues);
        }
        Expr::BracketRef(name) => {
            resolve_bracket_ref(name, current_table, path, index, issues);
        }
        Expr::TableColumnRef { table, column } => {
            resolve_qualified_member(table, column, path, index, issues);
        }
        Expr::HierarchyRef { table, column, .. } => {
            // The supplied metadata deliberately models tables, columns, and
            // measures only. Validate the hierarchy's table/base-column pair;
            // level resolution needs hierarchy metadata from a future API.
            resolve_qualified_member(table, column, path, index, issues);
        }
        Expr::FunctionCall { args, .. } => {
            for (arg_index, arg) in args.iter().enumerate() {
                validate_expr(
                    arg,
                    &format!("{path}.args[{arg_index}]"),
                    index,
                    lexical_scope,
                    current_table,
                    issues,
                );
            }
        }
        Expr::DataTable { rows, .. } | Expr::TableConstructor(rows) => {
            for (row_index, row) in rows.iter().enumerate() {
                for (column_index, value) in row.iter().enumerate() {
                    validate_expr(
                        value,
                        &format!("{path}.rows[{row_index}][{column_index}]"),
                        index,
                        lexical_scope,
                        current_table,
                        issues,
                    );
                }
            }
        }
        Expr::Unary { expr, .. } | Expr::Paren(expr) => validate_expr(
            expr,
            &format!("{path}.expr"),
            index,
            lexical_scope,
            current_table,
            issues,
        ),
        Expr::Binary { left, right, .. } => {
            validate_expr(
                left,
                &format!("{path}.left"),
                index,
                lexical_scope,
                current_table,
                issues,
            );
            validate_expr(
                right,
                &format!("{path}.right"),
                index,
                lexical_scope,
                current_table,
                issues,
            );
        }
        Expr::VarBlock { decls, body } => {
            let mut scope = lexical_scope.clone();
            for (decl_index, decl) in decls.iter().enumerate() {
                validate_variable_name(
                    &decl.name,
                    &format!("{path}.decls[{decl_index}].name"),
                    index,
                    issues,
                );
                // A declaration is visible to subsequent declarations and the
                // RETURN body, but not in its own initializer.
                validate_expr(
                    &decl.expr,
                    &format!("{path}.decls[{decl_index}].expr"),
                    index,
                    &scope,
                    current_table,
                    issues,
                );
                scope.insert(key(&decl.name));
            }
            validate_expr(
                body,
                &format!("{path}.body"),
                index,
                &scope,
                current_table,
                issues,
            );
        }
        Expr::Tuple(elements) => {
            for (element_index, element) in elements.iter().enumerate() {
                validate_expr(
                    element,
                    &format!("{path}.elements[{element_index}]"),
                    index,
                    lexical_scope,
                    current_table,
                    issues,
                );
            }
        }
    }
}

fn validate_visual_shape(
    shape: &VisualShape,
    table: &str,
    path: &str,
    index: &ModelIndex,
    issues: &mut Vec<ModelValidationIssue>,
) {
    // Only validate shape columns when the output schema is known (currently a
    // direct DATATABLE or explicit DEFINE COLUMN). Arbitrary table expressions
    // require type/schema inference and must not produce false unknown errors.
    if !index.table_schema_is_known(table) {
        return;
    }
    for (axis_index, axis) in shape.axes.iter().enumerate() {
        for (group_index, group) in axis.groups.iter().enumerate() {
            for (column_index, column) in group.columns.iter().enumerate() {
                resolve_column(
                    table,
                    &column.name,
                    &format!(
                        "{path}.axes[{axis_index}].groups[{group_index}].columns[{column_index}]"
                    ),
                    index,
                    issues,
                );
            }
            resolve_column(
                table,
                &group.total.name,
                &format!("{path}.axes[{axis_index}].groups[{group_index}].total"),
                index,
                issues,
            );
        }
        for (column_index, column) in axis.order_by.iter().enumerate() {
            resolve_column(
                table,
                &column.name,
                &format!("{path}.axes[{axis_index}].order_by[{column_index}]"),
                index,
                issues,
            );
        }
    }
}

pub fn validate_expression_against_model(
    expr: &Expr,
    model: &ModelMetadata,
) -> Vec<ModelValidationIssue> {
    let index = ModelIndex::from_model(model);
    let mut issues = Vec::new();
    validate_expr(expr, "$", &index, &HashSet::new(), None, &mut issues);
    issues
}

pub fn validate_query_against_model(
    query: &Query,
    model: &ModelMetadata,
) -> Vec<ModelValidationIssue> {
    let mut index = ModelIndex::from_model(model);
    index.add_query_definitions(query);
    let mut issues = Vec::new();

    let mut query_scope = HashSet::new();
    if let Some(define) = &query.define {
        for definition in &define.defs {
            if let Definition::Var { name, .. } = definition {
                query_scope.insert(key(name));
            }
        }

        let mut definition_scope = HashSet::new();
        for (definition_index, definition) in define.defs.iter().enumerate() {
            let path = format!("$.define.defs[{definition_index}]");
            match definition {
                Definition::Measure { table, expr, .. }
                | Definition::Column { table, expr, .. } => {
                    let current_table = table.as_ref().map(|table| table.name.as_str());
                    if let Some(table) = table {
                        resolve_table(table, &format!("{path}.table"), &index, &mut issues);
                    }
                    validate_expr(
                        expr,
                        &format!("{path}.expr"),
                        &index,
                        &definition_scope,
                        current_table,
                        &mut issues,
                    );
                }
                Definition::Var { name, expr, .. } => {
                    validate_variable_name(name, &format!("{path}.name"), &index, &mut issues);
                    validate_expr(
                        expr,
                        &format!("{path}.expr"),
                        &index,
                        &definition_scope,
                        None,
                        &mut issues,
                    );
                    definition_scope.insert(key(name));
                }
                Definition::Table {
                    name,
                    expr,
                    visual_shape,
                    ..
                } => {
                    validate_expr(
                        expr,
                        &format!("{path}.expr"),
                        &index,
                        &definition_scope,
                        None,
                        &mut issues,
                    );
                    if let Some(shape) = visual_shape {
                        validate_visual_shape(
                            shape,
                            name,
                            &format!("{path}.visual_shape"),
                            &index,
                            &mut issues,
                        );
                    }
                }
                Definition::Function { params, body, .. } => {
                    let mut parameter_scope = definition_scope.clone();
                    for (parameter_index, parameter) in params.iter().enumerate() {
                        if let Some(default) = &parameter.default {
                            validate_expr(
                                default,
                                &format!("{path}.params[{parameter_index}].default"),
                                &index,
                                &parameter_scope,
                                None,
                                &mut issues,
                            );
                        }
                        parameter_scope.insert(key(&parameter.name));
                    }
                    validate_expr(
                        body,
                        &format!("{path}.body"),
                        &index,
                        &parameter_scope,
                        None,
                        &mut issues,
                    );
                }
            }
        }
    }

    for (evaluate_index, evaluate) in query.evaluates.iter().enumerate() {
        let path = format!("$.evaluates[{evaluate_index}]");
        validate_expr(
            &evaluate.expr,
            &format!("{path}.expr"),
            &index,
            &query_scope,
            None,
            &mut issues,
        );
        for (key_index, order_key) in evaluate.order_by.iter().enumerate() {
            validate_expr(
                &order_key.expr,
                &format!("{path}.order_by[{key_index}].expr"),
                &index,
                &query_scope,
                None,
                &mut issues,
            );
        }
        if let Some(start_at) = &evaluate.start_at {
            for (value_index, value) in start_at.iter().enumerate() {
                validate_expr(
                    value,
                    &format!("{path}.start_at[{value_index}]"),
                    &index,
                    &query_scope,
                    None,
                    &mut issues,
                );
            }
        }
    }

    issues
}

#[cfg(test)]
mod tests {
    use crate::{parse_expression, parse_query};

    use super::*;

    fn model() -> ModelMetadata {
        ModelMetadata {
            tables: vec![
                ModelTable {
                    name: "Sales".into(),
                    columns: vec!["Amount".into(), "ProductKey".into()],
                    measures: vec!["Revenue".into()],
                },
                ModelTable {
                    name: "Product".into(),
                    columns: vec!["ProductKey".into(), "Color".into()],
                    measures: vec![],
                },
            ],
        }
    }

    #[test]
    fn resolves_model_names_case_insensitively() {
        let expr =
            parse_expression("SUM(sAlEs[aMoUnT]) + [rEvEnUe] + COUNTROWS('pRoDuCt')").unwrap();
        assert!(validate_expression_against_model(&expr, &model()).is_empty());
    }

    #[test]
    fn reports_unknown_and_ambiguous_references() {
        let expr =
            parse_expression("Missing[Value] + Sales[Missing] + [ProductKey] + UnknownVariable")
                .unwrap();
        let issues = validate_expression_against_model(&expr, &model());
        assert!(issues
            .iter()
            .any(|issue| issue.code == ModelValidationCode::UnknownTable));
        assert!(issues
            .iter()
            .any(|issue| issue.code == ModelValidationCode::UnknownMember));
        assert!(issues
            .iter()
            .any(|issue| issue.code == ModelValidationCode::UnknownIdentifier));
        assert!(issues
            .iter()
            .any(|issue| issue.code == ModelValidationCode::AmbiguousReference));
    }

    #[test]
    fn query_definitions_variables_and_udf_parameters_are_scoped() {
        let query = parse_query(
            "define
             var GlobalRate = 2
             table Local = DATATABLE(\"Value\", INTEGER, {{1}})
             measure Sales[Adjusted] = [Amount] * GlobalRate
             function Scale = (x: numeric, factor: numeric = GlobalRate) => x * factor
             evaluate { Scale(Local[Value]) }
             order by GlobalRate
             start at @position",
        )
        .unwrap();
        assert!(validate_query_against_model(&query, &model()).is_empty());
    }

    #[test]
    fn local_var_initializers_are_sequentially_scoped() {
        let expr = parse_expression("VAR a = 1 VAR b = A VAR c = missing RETURN b + C").unwrap();
        let issues = validate_expression_against_model(&expr, &model());
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].code, ModelValidationCode::UnknownIdentifier);
        assert!(issues[0].path.ends_with("decls[2].expr"));
    }

    #[test]
    fn query_scoped_objects_override_model_objects() {
        let query = parse_query(
            "define
             measure Sales[Revenue] = Sales[Amount]
             table Product = DATATABLE(\"Name\", STRING, {{\"Widget\"}})
             evaluate { [Revenue], Product[Name] }",
        )
        .unwrap();
        assert!(validate_query_against_model(&query, &model()).is_empty());
    }

    #[test]
    fn query_table_shadow_does_not_fall_through_to_model_columns() {
        let query = parse_query(
            "define
             table Product = DATATABLE(\"Name\", STRING, {{\"Widget\"}})
             evaluate { Product[Color] }",
        )
        .unwrap();

        let issues = validate_query_against_model(&query, &model());
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].code, ModelValidationCode::UnknownMember);
        assert!(issues[0].message.contains("[Color]"));
    }

    #[test]
    fn query_table_with_uninferred_schema_does_not_report_unknown_members() {
        let query =
            parse_query("define table Local = FILTER(Sales, TRUE()) evaluate { Local[Amount] }")
                .unwrap();
        assert!(validate_query_against_model(&query, &model()).is_empty());

        let typed = parse_query(
            "define table Local = DATATABLE(\"Value\", INTEGER, {{1}}) evaluate { Local[Missing] }",
        )
        .unwrap();
        let issues = validate_query_against_model(&typed, &model());
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].code, ModelValidationCode::UnknownMember);
    }

    #[test]
    fn variable_names_cannot_conflict_with_model_tables_case_insensitively() {
        let expr = parse_expression("VAR sAlEs = 1 RETURN Sales").unwrap();
        let issues = validate_expression_against_model(&expr, &model());
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].code, ModelValidationCode::ConflictingVariableName);
        assert!(issues[0].path.ends_with("decls[0].name"));

        let query = parse_query("define var PRODUCT = 1 evaluate { Product }").unwrap();
        let issues = validate_query_against_model(&query, &model());
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].code, ModelValidationCode::ConflictingVariableName);
        assert!(issues[0].path.ends_with("define.defs[0].name"));
    }

    #[test]
    fn unqualified_column_and_measure_matches_are_ambiguous() {
        let mut metadata = model();
        metadata.tables[0].measures.push("Amount".into());
        let expr = parse_expression("[Amount]").unwrap();
        let issues = validate_expression_against_model(&expr, &metadata);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].code, ModelValidationCode::AmbiguousReference);
        assert!(issues[0].message.contains("columns or measures"));
    }
}
