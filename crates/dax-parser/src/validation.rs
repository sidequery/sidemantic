use std::collections::{HashMap, HashSet};

use serde::Serialize;

use crate::{
    lookup, ArgumentCategory, BinaryOp, DataTableType, Definition, Expr, FuncParam,
    FunctionSignature, Query, ResultKind,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ValidationCode {
    UnrecognizedFunction,
    FunctionArity,
    ExpectedTable,
    ExpectedScalar,
    MissingDefinitionTable,
    InvalidFunctionName,
    InvalidParameterName,
    DuplicateParameter,
    TooManyParameters,
    InvalidTypeHint,
    InvalidDefaultExpression,
    InvalidDataTableValue,
    InvalidVariableName,
    DuplicateVariable,
    InvalidArgumentType,
    MissingRequiredArgument,
    DuplicateFunction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ValidationIssue {
    pub code: ValidationCode,
    pub message: String,
    /// Stable structural location that can be correlated with the node spans
    /// returned by the separate `LosslessParse` APIs.
    pub path: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ValidationOptions {
    /// Report calls absent from the bundled function-name catalog and query-scoped UDFs.
    /// This is a compatibility diagnostic, not proof that a future engine rejects the name.
    pub report_unrecognized_functions: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExprKind {
    Scalar,
    Table,
    Unknown,
}

fn known_builtin_name(name: &str) -> bool {
    lookup(name).is_some()
}

fn reserved_word(name: &str) -> bool {
    matches!(
        name.to_ascii_uppercase().as_str(),
        "DEFINE"
            | "EVALUATE"
            | "ORDER"
            | "BY"
            | "START"
            | "AT"
            | "RETURN"
            | "VAR"
            | "IN"
            | "ASC"
            | "DESC"
            | "MEASURE"
            | "COLUMN"
            | "TABLE"
            | "FUNCTION"
            | "WITH"
            | "VISUAL"
            | "SHAPE"
            | "AXIS"
            | "GROUP"
            | "TOTAL"
            | "DENSIFY"
            | "TRUE"
            | "FALSE"
            | "NOT"
    )
}

fn issue(
    issues: &mut Vec<ValidationIssue>,
    code: ValidationCode,
    path: &str,
    message: impl Into<String>,
) {
    issues.push(ValidationIssue {
        code,
        message: message.into(),
        path: path.to_string(),
    });
}

fn valid_function_name(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('.')
        && !name.ends_with('.')
        && !name.contains("..")
        && name.split('.').all(valid_simple_name)
}

fn valid_simple_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(first) if first == '_' || first.is_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_alphanumeric())
}

/// VAR has a deliberately narrower identifier grammar than UDF parameters.
/// Microsoft documents ASCII alphanumerics with a non-digit first character,
/// plus a double-underscore prefix. Model table-name conflicts require model
/// resolution and are therefore outside this validator.
fn valid_variable_name(name: &str) -> bool {
    let rest = name.strip_prefix("__").unwrap_or(name);
    let mut chars = rest.chars();
    matches!(chars.next(), Some(first) if first.is_ascii_alphabetic())
        && chars.all(|ch| ch.is_ascii_alphanumeric())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParamCategory {
    AnyVal,
    Scalar,
    Table,
    AnyRef,
    CalendarRef,
    ColumnRef,
    MeasureRef,
    TableRef,
}

fn parse_param_hints(hints: &[String]) -> Option<ParamCategory> {
    let mut category = None;
    let mut has_subtype = false;
    let mut has_mode = false;
    let mut val_mode = false;
    let mut last_position = 0;

    for hint in hints {
        let upper = hint.to_ascii_uppercase();
        let (position, item_category) = match upper.as_str() {
            "ANYVAL" => (1, Some(ParamCategory::AnyVal)),
            "SCALAR" => (1, Some(ParamCategory::Scalar)),
            "TABLE" => (1, Some(ParamCategory::Table)),
            "ANYREF" => (1, Some(ParamCategory::AnyRef)),
            "CALENDARREF" => (1, Some(ParamCategory::CalendarRef)),
            "COLUMNREF" => (1, Some(ParamCategory::ColumnRef)),
            "MEASUREREF" => (1, Some(ParamCategory::MeasureRef)),
            "TABLEREF" => (1, Some(ParamCategory::TableRef)),
            "BOOLEAN" | "DATETIME" | "DECIMAL" | "DOUBLE" | "INT64" | "NUMERIC" | "STRING"
            | "VARIANT" => (2, None),
            "VAL" | "EXPR" => (3, None),
            _ => return None,
        };

        // Documented order: [type] [subtype] [passing mode]. Each category is
        // optional, but it cannot be repeated or reordered.
        if position < last_position {
            return None;
        }
        last_position = position;
        match position {
            1 => {
                if category.is_some() {
                    return None;
                }
                category = item_category;
            }
            2 => {
                if has_subtype {
                    return None;
                }
                has_subtype = true;
            }
            3 => {
                if has_mode {
                    return None;
                }
                has_mode = true;
                val_mode = upper == "VAL";
            }
            _ => unreachable!(),
        }
    }

    // A subtype by itself implies SCALAR. Explicit non-scalar types cannot
    // carry one because subtypes apply only to SCALAR.
    let category = category.unwrap_or(if has_subtype {
        ParamCategory::Scalar
    } else {
        ParamCategory::AnyVal
    });
    if has_subtype && category != ParamCategory::Scalar {
        return None;
    }
    if val_mode
        && matches!(
            category,
            ParamCategory::AnyRef
                | ParamCategory::CalendarRef
                | ParamCategory::ColumnRef
                | ParamCategory::MeasureRef
                | ParamCategory::TableRef
        )
    {
        return None;
    }
    Some(category)
}

fn validate_params(params: &[FuncParam], path: &str, issues: &mut Vec<ValidationIssue>) {
    if params.len() > 256 {
        issue(
            issues,
            ValidationCode::TooManyParameters,
            path,
            format!(
                "DAX UDFs support at most 256 parameters; found {}",
                params.len()
            ),
        );
    }

    let mut names = HashSet::new();
    for (index, param) in params.iter().enumerate() {
        let param_path = format!("{path}.params[{index}]");
        if !valid_simple_name(&param.name) || reserved_word(&param.name) {
            issue(
                issues,
                ValidationCode::InvalidParameterName,
                &param_path,
                format!(
                    "invalid or reserved DAX UDF parameter name `{}`",
                    param.name
                ),
            );
        }
        if !names.insert(param.name.to_ascii_lowercase()) {
            issue(
                issues,
                ValidationCode::DuplicateParameter,
                &param_path,
                format!("duplicate DAX UDF parameter `{}`", param.name),
            );
        }
        if parse_param_hints(&param.type_hints).is_none() {
            issue(
                issues,
                ValidationCode::InvalidTypeHint,
                &param_path,
                format!(
                    "invalid DAX UDF type hints `{}`; expected [type] [subtype] [passing mode]",
                    param.type_hints.join(" ")
                ),
            );
        }
    }

    let optional_names: HashSet<String> = params
        .iter()
        .filter(|param| param.default.is_some())
        .map(|param| param.name.to_ascii_lowercase())
        .collect();
    for (index, param) in params.iter().enumerate() {
        let Some(default) = &param.default else {
            continue;
        };
        let mut identifiers = HashSet::new();
        collect_free_identifiers(default, &HashSet::new(), &mut identifiers);
        for referenced in identifiers.intersection(&optional_names) {
            issue(
                issues,
                ValidationCode::InvalidDefaultExpression,
                &format!("{path}.params[{index}].default"),
                format!(
                    "default for `{}` cannot reference optional parameter `{referenced}`",
                    param.name
                ),
            );
        }
    }
}

fn collect_free_identifiers(
    expr: &Expr,
    bound: &HashSet<String>,
    identifiers: &mut HashSet<String>,
) {
    match expr {
        Expr::Identifier(name) => {
            let normalized = name.to_ascii_lowercase();
            if !bound.contains(&normalized) {
                identifiers.insert(normalized);
            }
        }
        Expr::FunctionCall { args, .. } | Expr::Tuple(args) => {
            for arg in args {
                collect_free_identifiers(arg, bound, identifiers);
            }
        }
        Expr::DataTable { rows, .. } | Expr::TableConstructor(rows) => {
            for row in rows {
                for value in row {
                    collect_free_identifiers(value, bound, identifiers);
                }
            }
        }
        Expr::Unary { expr, .. } | Expr::Paren(expr) => {
            collect_free_identifiers(expr, bound, identifiers)
        }
        Expr::Binary { left, right, .. } => {
            collect_free_identifiers(left, bound, identifiers);
            collect_free_identifiers(right, bound, identifiers);
        }
        Expr::VarBlock { decls, body } => {
            let mut local_bound = bound.clone();
            for decl in decls {
                collect_free_identifiers(&decl.expr, &local_bound, identifiers);
                local_bound.insert(decl.name.to_ascii_lowercase());
            }
            collect_free_identifiers(body, &local_bound, identifiers);
        }
        Expr::Number(_)
        | Expr::String(_)
        | Expr::DateTime(_)
        | Expr::Boolean(_)
        | Expr::Blank
        | Expr::Omitted
        | Expr::Parameter(_)
        | Expr::TableRef(_)
        | Expr::BracketRef(_)
        | Expr::TableColumnRef { .. }
        | Expr::HierarchyRef { .. } => {}
    }
}

fn check_expected_kind(
    actual: ExprKind,
    expected: ExprKind,
    path: &str,
    context: &str,
    issues: &mut Vec<ValidationIssue>,
) {
    match (actual, expected) {
        (ExprKind::Table, ExprKind::Scalar) => issue(
            issues,
            ValidationCode::ExpectedScalar,
            path,
            format!("{context} requires a scalar expression"),
        ),
        (ExprKind::Scalar, ExprKind::Table) => issue(
            issues,
            ValidationCode::ExpectedTable,
            path,
            format!("{context} requires a table expression"),
        ),
        _ => {}
    }
}

fn datatable_value_compatible(value: &Expr, data_type: &DataTableType) -> bool {
    let value = match value {
        Expr::Paren(inner) => return datatable_value_compatible(inner, data_type),
        Expr::Unary { expr, .. } => expr.as_ref(),
        Expr::Blank | Expr::Omitted => return true,
        Expr::FunctionCall { name, args }
            if name.eq_ignore_ascii_case("blank") && args.is_empty() =>
        {
            return true;
        }
        other => other,
    };

    match data_type {
        // Text is the broadest declared DATATABLE type; constants have a
        // documented implicit-conversion path to text.
        DataTableType::String => true,
        DataTableType::Boolean => match value {
            Expr::Boolean(_) => true,
            Expr::Number(raw) => raw.parse::<f64>().is_ok(),
            Expr::String(raw) => {
                raw.eq_ignore_ascii_case("true") || raw.eq_ignore_ascii_case("false")
            }
            _ => false,
        },
        DataTableType::Currency | DataTableType::Double | DataTableType::Integer => match value {
            Expr::Number(raw) => raw.parse::<f64>().is_ok(),
            Expr::Boolean(_) | Expr::DateTime(_) => true,
            Expr::String(raw) => raw.parse::<f64>().is_ok(),
            Expr::FunctionCall { name, .. }
                if name.eq_ignore_ascii_case("date") || name.eq_ignore_ascii_case("time") =>
            {
                true
            }
            Expr::Binary { .. } => true,
            _ => false,
        },
        DataTableType::DateTime => match value {
            Expr::DateTime(_) | Expr::Number(_) => true,
            // Microsoft explicitly documents a text datetime as a valid
            // DATATABLE constant. Its accepted forms are locale-sensitive, so
            // the offline validator deliberately does not guess at the format.
            Expr::String(_) => true,
            Expr::FunctionCall { name, .. }
                if name.eq_ignore_ascii_case("date") || name.eq_ignore_ascii_case("time") =>
            {
                true
            }
            Expr::Binary { .. } => true,
            _ => false,
        },
    }
}

fn signature_argument(
    signature: &FunctionSignature,
    index: usize,
    arg_count: usize,
) -> Option<ArgumentCategory> {
    if (signature.name == "MAX" || signature.name == "MIN") && arg_count == 2 {
        return Some(ArgumentCategory::Scalar);
    }
    if signature.name == "LOOKUPVALUE" {
        return match index {
            0 => Some(ArgumentCategory::Column),
            last if arg_count.is_multiple_of(2) && last + 1 == arg_count => {
                Some(ArgumentCategory::Scalar)
            }
            position if position % 2 == 1 => Some(ArgumentCategory::Column),
            _ => Some(ArgumentCategory::Scalar),
        };
    }
    if signature.name == "TOPN" && index >= 2 {
        return Some(if index.is_multiple_of(2) {
            ArgumentCategory::OrderBy
        } else {
            ArgumentCategory::Enum
        });
    }
    if let Some(category) = signature.arguments.get(index) {
        if *category != ArgumentCategory::Unknown {
            return Some(*category);
        }
    }
    let repeat = signature.repeat?;
    let start = usize::from(repeat.start);
    let width = usize::from(repeat.width);
    if index < start || width == 0 {
        return None;
    }
    signature
        .arguments
        .get(start + (index - start) % width)
        .copied()
        .filter(|category| *category != ArgumentCategory::Unknown)
}

fn unparenthesized(expr: &Expr) -> &Expr {
    match expr {
        Expr::Paren(inner) => unparenthesized(inner),
        other => other,
    }
}

fn column_reference_shape(expr: &Expr) -> bool {
    matches!(
        unparenthesized(expr),
        Expr::Identifier(_)
            | Expr::BracketRef(_)
            | Expr::TableColumnRef { .. }
            | Expr::HierarchyRef { .. }
    )
}

fn measure_reference_shape(expr: &Expr) -> bool {
    matches!(
        unparenthesized(expr),
        Expr::Identifier(_) | Expr::BracketRef(_) | Expr::TableColumnRef { .. }
    )
}

fn table_reference_shape(expr: &Expr) -> bool {
    matches!(
        unparenthesized(expr),
        Expr::Identifier(_) | Expr::TableRef(_)
    )
}

fn any_reference_shape(expr: &Expr) -> bool {
    column_reference_shape(expr) || measure_reference_shape(expr) || table_reference_shape(expr)
}

fn valid_type_name_shape(expr: &Expr) -> bool {
    let Expr::Identifier(name) = unparenthesized(expr) else {
        return false;
    };
    matches!(
        name.to_ascii_uppercase().as_str(),
        "BOOLEAN"
            | "LOGICAL"
            | "CURRENCY"
            | "DECIMAL"
            | "DATETIME"
            | "DOUBLE"
            | "INTEGER"
            | "INT64"
            | "STRING"
            | "TEXT"
    )
}

fn validate_catalog_argument(
    expr: &Expr,
    actual: ExprKind,
    category: ArgumentCategory,
    path: &str,
    context: &str,
    issues: &mut Vec<ValidationIssue>,
) {
    if matches!(expr, Expr::Omitted) {
        return;
    }
    match category {
        ArgumentCategory::Scalar | ArgumentCategory::OrderBy | ArgumentCategory::Enum => {
            check_expected_kind(actual, ExprKind::Scalar, path, context, issues);
            return;
        }
        ArgumentCategory::Table => {
            check_expected_kind(actual, ExprKind::Table, path, context, issues);
            return;
        }
        _ => {}
    }
    let valid = match category {
        ArgumentCategory::Column => column_reference_shape(expr),
        ArgumentCategory::Measure => measure_reference_shape(expr),
        ArgumentCategory::ColumnOrTable => {
            column_reference_shape(expr) || table_reference_shape(expr)
        }
        ArgumentCategory::TypeName => valid_type_name_shape(expr),
        // A filter can be a Boolean scalar or a table expression. The helper
        // categories are contextual function expressions and remain unknown
        // without interpreting individual function contracts.
        ArgumentCategory::Filter
        | ArgumentCategory::PartitionBy
        | ArgumentCategory::MatchBy
        | ArgumentCategory::Unknown => true,
        ArgumentCategory::Scalar
        | ArgumentCategory::Table
        | ArgumentCategory::OrderBy
        | ArgumentCategory::Enum => unreachable!(),
    };
    if !valid {
        issue(
            issues,
            ValidationCode::InvalidArgumentType,
            path,
            format!("{context} expects {category:?}"),
        );
    }
}

fn validate_param_argument(
    expr: &Expr,
    actual: ExprKind,
    category: ParamCategory,
    path: &str,
    context: &str,
    issues: &mut Vec<ValidationIssue>,
) {
    if matches!(expr, Expr::Omitted) {
        return;
    }
    match category {
        ParamCategory::AnyVal => return,
        ParamCategory::Scalar => {
            check_expected_kind(actual, ExprKind::Scalar, path, context, issues);
            return;
        }
        ParamCategory::Table => {
            check_expected_kind(actual, ExprKind::Table, path, context, issues);
            return;
        }
        _ => {}
    }
    let valid = match category {
        ParamCategory::AnyRef => any_reference_shape(expr),
        ParamCategory::CalendarRef | ParamCategory::TableRef => table_reference_shape(expr),
        ParamCategory::ColumnRef => column_reference_shape(expr),
        ParamCategory::MeasureRef => measure_reference_shape(expr),
        ParamCategory::AnyVal | ParamCategory::Scalar | ParamCategory::Table => unreachable!(),
    };
    if !valid {
        issue(
            issues,
            ValidationCode::InvalidArgumentType,
            path,
            format!("{context} does not accept this expression shape"),
        );
    }
}

fn validate_call_arity(
    name: &str,
    args: &[Expr],
    arg_kinds: &[ExprKind],
    path: &str,
    udfs: &HashMap<String, &[FuncParam]>,
    options: ValidationOptions,
    issues: &mut Vec<ValidationIssue>,
) -> ExprKind {
    if let Some(params) = udfs.get(&name.to_ascii_lowercase()) {
        if args.len() > params.len() {
            issue(
                issues,
                ValidationCode::FunctionArity,
                path,
                format!(
                    "UDF `{name}` accepts at most {} arguments; found {}",
                    params.len(),
                    args.len()
                ),
            );
        }
        for (index, param) in params.iter().enumerate() {
            let missing = args
                .get(index)
                .is_none_or(|arg| matches!(arg, Expr::Omitted));
            if missing && param.default.is_none() {
                issue(
                    issues,
                    ValidationCode::MissingRequiredArgument,
                    &format!("{path}.args[{index}]"),
                    format!(
                        "required argument `{}` is missing in call to `{name}`",
                        param.name
                    ),
                );
            }
            if let (Some(arg), Some(actual), Some(category)) = (
                args.get(index),
                arg_kinds.get(index),
                parse_param_hints(&param.type_hints),
            ) {
                validate_param_argument(
                    arg,
                    *actual,
                    category,
                    &format!("{path}.args[{index}]"),
                    &format!("parameter `{}`", param.name),
                    issues,
                );
            }
        }
        return ExprKind::Unknown;
    }

    let Some(signature) = lookup(name) else {
        if options.report_unrecognized_functions && !known_builtin_name(name) {
            issue(
                issues,
                ValidationCode::UnrecognizedFunction,
                path,
                format!("function `{name}` is not in the bundled DAX function catalog"),
            );
        }
        return ExprKind::Unknown;
    };

    let too_few = signature
        .min_args
        .is_some_and(|min| args.len() < usize::from(min));
    let too_many = signature
        .max_args
        .is_some_and(|max| args.len() > usize::from(max));
    let invalid_repeat = signature.repeat.is_some_and(|repeat| {
        let start = usize::from(repeat.start);
        let width = usize::from(repeat.width);
        width > 0 && args.len() > start && !(args.len() - start).is_multiple_of(width)
    });
    if too_few || too_many || invalid_repeat {
        let expected = match (signature.min_args, signature.max_args) {
            (Some(min), Some(max)) if min == max => min.to_string(),
            (Some(min), Some(max)) => format!("{min}..={max}"),
            (Some(min), None) => format!("at least {min}"),
            (None, Some(max)) => format!("at most {max}"),
            (None, None) => "a documented number of".to_string(),
        };
        issue(
            issues,
            ValidationCode::FunctionArity,
            path,
            format!(
                "function `{name}` expects {expected} arguments; found {}",
                args.len()
            ),
        );
    }
    for (index, arg) in args.iter().enumerate() {
        if matches!(arg, Expr::Omitted)
            && !signature
                .omittable_positions
                .contains(&u8::try_from(index).unwrap_or(u8::MAX))
        {
            issue(
                issues,
                ValidationCode::MissingRequiredArgument,
                &format!("{path}.args[{index}]"),
                format!(
                    "required argument {} is missing in call to `{name}`",
                    index + 1
                ),
            );
        }
        if let (Some(actual), Some(category)) = (
            arg_kinds.get(index),
            signature_argument(signature, index, args.len()),
        ) {
            validate_catalog_argument(
                arg,
                *actual,
                category,
                &format!("{path}.args[{index}]"),
                &format!("argument {} of `{name}`", index + 1),
                issues,
            );
        }
    }
    match signature.result {
        ResultKind::Scalar => ExprKind::Scalar,
        ResultKind::Table => ExprKind::Table,
        ResultKind::Special | ResultKind::Unknown => ExprKind::Unknown,
    }
}

fn validate_expr(
    expr: &Expr,
    path: &str,
    udfs: &HashMap<String, &[FuncParam]>,
    options: ValidationOptions,
    issues: &mut Vec<ValidationIssue>,
) -> ExprKind {
    match expr {
        Expr::Number(_)
        | Expr::String(_)
        | Expr::DateTime(_)
        | Expr::Boolean(_)
        | Expr::Blank
        | Expr::BracketRef(_)
        | Expr::TableColumnRef { .. }
        | Expr::HierarchyRef { .. } => ExprKind::Scalar,
        Expr::Tuple(elements) => {
            for (index, element) in elements.iter().enumerate() {
                let element_path = format!("{path}.elements[{index}]");
                let kind = validate_expr(element, &element_path, udfs, options, issues);
                check_expected_kind(
                    kind,
                    ExprKind::Scalar,
                    &element_path,
                    "tuple element",
                    issues,
                );
            }
            ExprKind::Scalar
        }
        Expr::TableRef(_) => ExprKind::Table,
        Expr::TableConstructor(rows) => {
            for (row_index, row) in rows.iter().enumerate() {
                for (column_index, value) in row.iter().enumerate() {
                    let value_path = format!("{path}.rows[{row_index}][{column_index}]");
                    let kind = validate_expr(value, &value_path, udfs, options, issues);
                    check_expected_kind(
                        kind,
                        ExprKind::Scalar,
                        &value_path,
                        "table-constructor cell",
                        issues,
                    );
                }
            }
            ExprKind::Table
        }
        Expr::DataTable { columns, rows } => {
            for (row_index, row) in rows.iter().enumerate() {
                for (column_index, value) in row.iter().enumerate() {
                    let value_path = format!("{path}.rows[{row_index}][{column_index}]");
                    let kind = validate_expr(value, &value_path, udfs, options, issues);
                    check_expected_kind(
                        kind,
                        ExprKind::Scalar,
                        &value_path,
                        "DATATABLE cell",
                        issues,
                    );
                    if let Some(column) = columns.get(column_index) {
                        if !datatable_value_compatible(value, &column.data_type) {
                            issue(
                                issues,
                                ValidationCode::InvalidDataTableValue,
                                &value_path,
                                format!(
                                    "value is not statically convertible to DATATABLE column `{}` ({:?})",
                                    column.name, column.data_type
                                ),
                            );
                        }
                    }
                }
            }
            ExprKind::Table
        }
        Expr::Identifier(_) | Expr::Parameter(_) | Expr::Omitted => ExprKind::Unknown,
        Expr::Paren(inner) => validate_expr(inner, path, udfs, options, issues),
        Expr::Unary { expr, .. } => {
            let operand_path = format!("{path}.expr");
            let kind = validate_expr(expr, &operand_path, udfs, options, issues);
            check_expected_kind(
                kind,
                ExprKind::Scalar,
                &operand_path,
                "unary operator",
                issues,
            );
            ExprKind::Scalar
        }
        Expr::Binary { op, left, right } => {
            let left_path = format!("{path}.left");
            let right_path = format!("{path}.right");
            let left_kind = validate_expr(left, &left_path, udfs, options, issues);
            let right_kind = validate_expr(right, &right_path, udfs, options, issues);
            check_expected_kind(
                left_kind,
                ExprKind::Scalar,
                &left_path,
                "binary operator operand",
                issues,
            );
            check_expected_kind(
                right_kind,
                if matches!(op, BinaryOp::In) {
                    ExprKind::Table
                } else {
                    ExprKind::Scalar
                },
                &right_path,
                if matches!(op, BinaryOp::In) {
                    "IN right operand"
                } else {
                    "binary operator operand"
                },
                issues,
            );
            ExprKind::Scalar
        }
        Expr::FunctionCall { name, args } => {
            let arg_kinds: Vec<_> = args
                .iter()
                .enumerate()
                .map(|(index, arg)| {
                    validate_expr(arg, &format!("{path}.args[{index}]"), udfs, options, issues)
                })
                .collect();
            validate_call_arity(name, args, &arg_kinds, path, udfs, options, issues)
        }
        Expr::VarBlock { decls, body } => {
            let mut names = HashSet::new();
            for (index, decl) in decls.iter().enumerate() {
                let decl_path = format!("{path}.decls[{index}]");
                if !valid_variable_name(&decl.name) || reserved_word(&decl.name) {
                    issue(
                        issues,
                        ValidationCode::InvalidVariableName,
                        &decl_path,
                        format!("invalid or reserved DAX variable name `{}`", decl.name),
                    );
                }
                if !names.insert(decl.name.to_ascii_lowercase()) {
                    issue(
                        issues,
                        ValidationCode::DuplicateVariable,
                        &decl_path,
                        format!(
                            "duplicate DAX variable `{}` in the same VAR block",
                            decl.name
                        ),
                    );
                }
                validate_expr(
                    &decl.expr,
                    &format!("{decl_path}.expr"),
                    udfs,
                    options,
                    issues,
                );
            }
            validate_expr(body, &format!("{path}.body"), udfs, options, issues)
        }
    }
}

pub fn validate_expression(expr: &Expr, options: ValidationOptions) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    validate_expr(expr, "$", &HashMap::new(), options, &mut issues);
    issues
}

pub fn validate_query(query: &Query, options: ValidationOptions) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    let mut udfs: HashMap<String, &[FuncParam]> = HashMap::new();

    if let Some(define) = &query.define {
        for (index, definition) in define.defs.iter().enumerate() {
            if let Definition::Function { name, params, .. } = definition {
                if udfs.insert(name.to_ascii_lowercase(), params).is_some() {
                    issue(
                        &mut issues,
                        ValidationCode::DuplicateFunction,
                        &format!("$.define.defs[{index}]"),
                        format!("duplicate query-scoped DAX UDF `{name}`"),
                    );
                }
            }
        }
        for (index, definition) in define.defs.iter().enumerate() {
            let path = format!("$.define.defs[{index}]");
            match definition {
                Definition::Measure { table, expr, .. }
                | Definition::Column { table, expr, .. } => {
                    if table.is_none() {
                        issue(
                            &mut issues,
                            ValidationCode::MissingDefinitionTable,
                            &path,
                            "query-scoped MEASURE and COLUMN definitions require a table target",
                        );
                    }
                    if validate_expr(expr, &format!("{path}.expr"), &udfs, options, &mut issues)
                        == ExprKind::Table
                    {
                        issue(
                            &mut issues,
                            ValidationCode::ExpectedScalar,
                            &format!("{path}.expr"),
                            "MEASURE and COLUMN definitions require a scalar expression",
                        );
                    }
                }
                Definition::Table { expr, .. } => {
                    if validate_expr(expr, &format!("{path}.expr"), &udfs, options, &mut issues)
                        == ExprKind::Scalar
                    {
                        issue(
                            &mut issues,
                            ValidationCode::ExpectedTable,
                            &format!("{path}.expr"),
                            "TABLE definitions require a table expression",
                        );
                    }
                }
                Definition::Var { expr, .. } => {
                    let Definition::Var { name, .. } = definition else {
                        unreachable!()
                    };
                    if !valid_variable_name(name) || reserved_word(name) {
                        issue(
                            &mut issues,
                            ValidationCode::InvalidVariableName,
                            &path,
                            format!("invalid or reserved DAX variable name `{name}`"),
                        );
                    }
                    validate_expr(expr, &format!("{path}.expr"), &udfs, options, &mut issues);
                }
                Definition::Function {
                    name, params, body, ..
                } => {
                    if !valid_function_name(name) || reserved_word(name) || known_builtin_name(name)
                    {
                        issue(
                            &mut issues,
                            ValidationCode::InvalidFunctionName,
                            &path,
                            format!("invalid or reserved DAX UDF name `{name}`"),
                        );
                    }
                    validate_params(params, &path, &mut issues);
                    for (param_index, param) in params.iter().enumerate() {
                        let Some(default) = &param.default else {
                            continue;
                        };
                        let default_path = format!("{path}.params[{param_index}].default");
                        let kind =
                            validate_expr(default, &default_path, &udfs, options, &mut issues);
                        if let Some(category) = parse_param_hints(&param.type_hints) {
                            validate_param_argument(
                                default,
                                kind,
                                category,
                                &default_path,
                                &format!("default for parameter `{}`", param.name),
                                &mut issues,
                            );
                        }
                    }
                    validate_expr(body, &format!("{path}.body"), &udfs, options, &mut issues);
                }
            }
        }
    }

    for (index, evaluate) in query.evaluates.iter().enumerate() {
        let path = format!("$.evaluates[{index}].expr");
        if validate_expr(&evaluate.expr, &path, &udfs, options, &mut issues) == ExprKind::Scalar {
            issue(
                &mut issues,
                ValidationCode::ExpectedTable,
                &path,
                "EVALUATE requires a table expression",
            );
        }
        for (key_index, key) in evaluate.order_by.iter().enumerate() {
            let key_path = format!("$.evaluates[{index}].order_by[{key_index}].expr");
            let kind = validate_expr(&key.expr, &key_path, &udfs, options, &mut issues);
            check_expected_kind(
                kind,
                ExprKind::Scalar,
                &key_path,
                "ORDER BY key",
                &mut issues,
            );
        }
    }

    issues
}

#[cfg(test)]
mod tests {
    use crate::{parse_expression, parse_query};

    use super::*;

    #[test]
    fn validates_builtin_arity_without_rejecting_future_functions_by_default() {
        let expr = parse_expression("SUM() + FUTURE_DAX(1)").unwrap();
        let issues = validate_expression(&expr, ValidationOptions::default());
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].code, ValidationCode::FunctionArity);

        let strict = validate_expression(
            &expr,
            ValidationOptions {
                report_unrecognized_functions: true,
            },
        );
        assert!(strict
            .iter()
            .any(|issue| issue.code == ValidationCode::UnrecognizedFunction));

        let standard = parse_expression("AVERAGE([x]) + COUNTROWS(CALCULATETABLE({1}))").unwrap();
        assert!(validate_expression(
            &standard,
            ValidationOptions {
                report_unrecognized_functions: true,
            },
        )
        .iter()
        .all(|issue| issue.code != ValidationCode::UnrecognizedFunction));

        for source in ["DATE(, 2, 3)", "ROW(\"a\", 1, \"b\")"] {
            let expr = parse_expression(source).unwrap();
            assert!(!validate_expression(&expr, ValidationOptions::default()).is_empty());
        }

        let too_short_topn = parse_expression("TOPN(1, {1})").unwrap();
        assert!(
            validate_expression(&too_short_topn, ValidationOptions::default())
                .iter()
                .any(|issue| issue.code == ValidationCode::FunctionArity)
        );

        for source in [
            "TOPN(1, {1}, 1)",
            "LOOKUPVALUE([Result], T[Key], 1, 0)",
            "LOOKUPVALUE([Result], T[K1], 1, T[K2], 2, 0)",
        ] {
            let expr = parse_expression(source).unwrap();
            assert!(
                validate_expression(&expr, ValidationOptions::default())
                    .iter()
                    .all(|issue| !matches!(
                        issue.code,
                        ValidationCode::FunctionArity | ValidationCode::InvalidArgumentType
                    )),
                "source: {source}"
            );
        }
    }

    #[test]
    fn validates_query_expression_contexts_and_definition_targets() {
        let query = parse_query("define measure [m] = {1} table T = 1 evaluate 1").unwrap();
        let issues = validate_query(&query, ValidationOptions::default());
        assert!(issues
            .iter()
            .any(|issue| issue.code == ValidationCode::MissingDefinitionTable));
        assert!(issues
            .iter()
            .any(|issue| issue.code == ValidationCode::ExpectedScalar));
        assert_eq!(
            issues
                .iter()
                .filter(|issue| issue.code == ValidationCode::ExpectedTable)
                .count(),
            2
        );
    }

    #[test]
    fn validates_udf_names_parameters_and_calls() {
        let query = parse_query(
            "define
             function bad..name = (x: nope, x: numeric, y: numeric = 2) => x
             evaluate { bad..name(, 3, 4, 5) }",
        )
        .unwrap();
        let issues = validate_query(&query, ValidationOptions::default());
        for code in [
            ValidationCode::InvalidFunctionName,
            ValidationCode::InvalidTypeHint,
            ValidationCode::DuplicateParameter,
            ValidationCode::FunctionArity,
            ValidationCode::MissingRequiredArgument,
        ] {
            assert!(
                issues.iter().any(|issue| issue.code == code),
                "missing {code:?}"
            );
        }
    }

    #[test]
    fn validates_reserved_builtin_and_duplicate_udf_names() {
        let query = parse_query(
            "define
             function measure = () => 1
             function average = () => 1
             function custom = () => 1
             function CUSTOM = () => 2
             evaluate { custom() }",
        )
        .unwrap();
        let issues = validate_query(&query, ValidationOptions::default());
        assert_eq!(
            issues
                .iter()
                .filter(|issue| issue.code == ValidationCode::InvalidFunctionName)
                .count(),
            2
        );
        assert!(issues
            .iter()
            .any(|issue| issue.code == ValidationCode::DuplicateFunction));
    }

    #[test]
    fn validates_datatable_value_compatibility_conservatively() {
        let query = parse_query(
            r#"evaluate DATATABLE(
                "Whole", INTEGER,
                "Flag", BOOLEAN,
                "When", DATETIME,
                {{"not a number", "not a boolean", TRUE}}
            )"#,
        )
        .unwrap();
        let issues = validate_query(&query, ValidationOptions::default());
        assert_eq!(
            issues
                .iter()
                .filter(|issue| issue.code == ValidationCode::InvalidDataTableValue)
                .count(),
            3
        );

        let compatible = parse_query(
            r#"evaluate DATATABLE(
                "Whole", INTEGER,
                "Flag", BOOLEAN,
                "When", DATETIME,
                "Text", STRING,
                {{"12", 1, "2009-04-15 02:45:21", TRUE}, {,,,}}
            )"#,
        )
        .unwrap();
        assert!(validate_query(&compatible, ValidationOptions::default())
            .iter()
            .all(|issue| issue.code != ValidationCode::InvalidDataTableValue));
    }

    #[test]
    fn requires_scalar_table_constructor_cells_and_operator_operands() {
        for source in [
            "{ FILTER({1}, TRUE()) }",
            "{1} + 2",
            "-FILTER({1}, TRUE())",
            "(FILTER({1}, TRUE()), 2)",
        ] {
            let expr = parse_expression(source).unwrap();
            assert!(
                validate_expression(&expr, ValidationOptions::default())
                    .iter()
                    .any(|issue| issue.code == ValidationCode::ExpectedScalar),
                "source: {source}"
            );
        }
    }

    #[test]
    fn validates_var_names_and_duplicates_within_a_var_block() {
        let expr =
            parse_expression("VAR _bad = 1 VAR __good = 2 VAR x = 3 VAR X = 4 RETURN __good + x")
                .unwrap();
        let issues = validate_expression(&expr, ValidationOptions::default());
        assert_eq!(
            issues
                .iter()
                .filter(|issue| issue.code == ValidationCode::InvalidVariableName)
                .count(),
            1
        );
        assert_eq!(
            issues
                .iter()
                .filter(|issue| issue.code == ValidationCode::DuplicateVariable)
                .count(),
            1
        );

        let query = parse_query("define var return = 1 evaluate { return }").unwrap();
        assert!(validate_query(&query, ValidationOptions::default())
            .iter()
            .any(|issue| issue.code == ValidationCode::InvalidVariableName));
    }

    #[test]
    fn validates_udf_type_hint_grammar_and_reserved_parameters() {
        let query = parse_query(
            "define
             function valid = (
               a,
               b: scalar numeric expr,
               c: numeric,
               d: expr,
               e: table expr,
               f: columnref
             ) => a
             function invalid = (
               measure: scalar,
               x: table numeric,
               y: val numeric,
               z: scalar scalar,
               bad_ref_mode: columnref val
             ) => 1
             evaluate { valid(1, 2, 3, 4, {5}, [c]) }",
        )
        .unwrap();
        let issues = validate_query(&query, ValidationOptions::default());
        assert_eq!(
            issues
                .iter()
                .filter(|issue| issue.code == ValidationCode::InvalidParameterName)
                .count(),
            1
        );
        assert_eq!(
            issues
                .iter()
                .filter(|issue| issue.code == ValidationCode::InvalidTypeHint)
                .count(),
            4
        );
    }

    #[test]
    fn validates_udf_default_scope_and_expression_kind() {
        let query = parse_query(
            "define
             function defaults = (
               optional: scalar = 1,
               bad_scope: scalar = optional + 1,
               self_ref: scalar = self_ref,
               bad_scalar: scalar = {1},
               bad_table: table = 1,
               good_table: table = {1},
               shadowed: scalar = VAR optional = 2 RETURN optional
             ) => 1
             evaluate { defaults() }",
        )
        .unwrap();
        let issues = validate_query(&query, ValidationOptions::default());
        assert_eq!(
            issues
                .iter()
                .filter(|issue| issue.code == ValidationCode::InvalidDefaultExpression)
                .count(),
            2
        );
        assert_eq!(
            issues
                .iter()
                .filter(|issue| issue.code == ValidationCode::ExpectedScalar)
                .count(),
            1
        );
        assert_eq!(
            issues
                .iter()
                .filter(|issue| issue.code == ValidationCode::ExpectedTable)
                .count(),
            1
        );
    }

    #[test]
    fn validates_udf_argument_kinds_and_order_by_keys() {
        let query = parse_query(
            "define function typed = (s: scalar, t: table) => s
             evaluate { typed({1}, 2) }
             order by FILTER({1}, TRUE())",
        )
        .unwrap();
        let issues = validate_query(&query, ValidationOptions::default());
        assert_eq!(
            issues
                .iter()
                .filter(|issue| issue.code == ValidationCode::ExpectedScalar)
                .count(),
            2
        );
        assert_eq!(
            issues
                .iter()
                .filter(|issue| issue.code == ValidationCode::ExpectedTable)
                .count(),
            1
        );
    }

    #[test]
    fn does_not_reject_repeated_non_function_define_names() {
        // Microsoft's DEFINE contract explicitly says entity names do not
        // have to be unique. UDFs are the documented exception and are tested
        // separately above.
        let query = parse_query(
            "define measure T[m] = 1 measure T[m] = 2 var x = 1 var x = 2 evaluate { [m], x }",
        )
        .unwrap();
        let issues = validate_query(&query, ValidationOptions::default());
        assert!(issues.iter().all(|issue| !matches!(
            issue.code,
            ValidationCode::DuplicateFunction | ValidationCode::DuplicateVariable
        )));
    }

    #[test]
    fn validates_catalog_reference_and_type_name_shapes() {
        let invalid = parse_expression("SUM(1) + CONVERT(1, 123) + COUNTROWS(1)").unwrap();
        let issues = validate_expression(&invalid, ValidationOptions::default());
        assert_eq!(
            issues
                .iter()
                .filter(|issue| issue.code == ValidationCode::InvalidArgumentType)
                .count(),
            2
        );
        assert!(issues
            .iter()
            .any(|issue| issue.code == ValidationCode::ExpectedTable));

        for source in [
            "SUM([Amount])",
            "SUM('Sales'[Amount])",
            "CONVERT(1, INTEGER)",
            "DISTINCT('Sales')",
            "TOPN(1, {1}, 1 + 2)",
            "MAX(1, 2)",
            "MIN(1, 2)",
        ] {
            let expr = parse_expression(source).unwrap();
            assert!(
                validate_expression(&expr, ValidationOptions::default())
                    .iter()
                    .all(|issue| issue.code != ValidationCode::InvalidArgumentType),
                "source: {source}"
            );
        }

        for source in ["MAX(1)", "MIN(1)"] {
            let expr = parse_expression(source).unwrap();
            assert!(validate_expression(&expr, ValidationOptions::default())
                .iter()
                .any(|issue| issue.code == ValidationCode::InvalidArgumentType));
        }
    }

    #[test]
    fn validates_udf_reference_parameter_shapes() {
        let query = parse_query(
            "define
             function refs = (
               c: columnref,
               m: measureref,
               t: tableref,
               cal: calendarref,
               a: anyref
             ) => 1
             evaluate {
               refs('T'[C], [M], 'T', 'Calendar', [Anything]),
               refs(1, 2, {1}, DATE(2020, 1, 1), 3)
             }",
        )
        .unwrap();
        let issues = validate_query(&query, ValidationOptions::default());
        assert_eq!(
            issues
                .iter()
                .filter(|issue| issue.code == ValidationCode::InvalidArgumentType)
                .count(),
            5
        );
    }

    #[test]
    fn validates_in_operand_contexts_without_rejecting_valid_tables() {
        for source in ["1 IN {1, 2}", "('T'[A], 'T'[B]) IN {(1, 2), (3, 4)}"] {
            let expr = parse_expression(source).unwrap();
            assert!(
                validate_expression(&expr, ValidationOptions::default())
                    .iter()
                    .all(|issue| !matches!(
                        issue.code,
                        ValidationCode::ExpectedScalar | ValidationCode::ExpectedTable
                    )),
                "source: {source}"
            );
        }

        let scalar_rhs = parse_expression("1 IN 2").unwrap();
        assert!(
            validate_expression(&scalar_rhs, ValidationOptions::default())
                .iter()
                .any(|issue| issue.code == ValidationCode::ExpectedTable)
        );

        let table_lhs = parse_expression("{1} IN {1}").unwrap();
        assert!(
            validate_expression(&table_lhs, ValidationOptions::default())
                .iter()
                .any(|issue| issue.code == ValidationCode::ExpectedScalar)
        );
    }
}
