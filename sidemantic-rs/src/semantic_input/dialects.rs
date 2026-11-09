//! Normalize declared SQL syntax with polyglot before semantic binding.
//!
//! The executable graph uses DuckDB AST syntax internally; source declarations
//! remain unchanged in `SemanticInput::source`. This is syntax translation, not
//! a second resolver for metric names, policies, or query populations.

use std::collections::HashMap;

use polyglot_sql::expressions::Select;
use polyglot_sql::{Dialect, DialectType, Expression};
use serde_json::Value;

use crate::error::{Result, SidemanticError};

pub(crate) fn parse_dialect(name: &str) -> Result<DialectType> {
    name.parse().map_err(|error| {
        SidemanticError::Validation(format!("Invalid SQL dialect '{name}': {error}"))
    })
}

/// The pinned transpiler preserves bracket text instead of translating array
/// positions. Apply this correction only at Ossie's declared-dialect boundaries;
/// ordinary native graph compilation retains its existing behavior.
pub(crate) fn ossie_index_offsets(
    expression: Expression,
    source: DialectType,
    target: DialectType,
) -> Result<Expression> {
    use polyglot_sql::expressions::{BinaryOp, Case, Literal, UnaryFunc};
    if !matches!(
        (source, target),
        (DialectType::Snowflake, DialectType::DuckDB)
            | (DialectType::DuckDB, DialectType::Snowflake)
    ) {
        return Ok(expression);
    }
    fn number(value: i64) -> Expression {
        Expression::Literal(Literal::Number(value.to_string()))
    }
    fn adjust_node(node: Expression, source: DialectType) -> Expression {
        let Expression::Subscript(mut subscript) = node else {
            return node;
        };
        // String subscripts select object/map keys, not array positions.
        if matches!(&subscript.index, Expression::Literal(Literal::String(_))) {
            return Expression::Subscript(subscript);
        }
        let literal = match &subscript.index {
            Expression::Literal(Literal::Number(value)) => value.parse::<i64>().ok(),
            _ => None,
        };
        if let Some(index) = literal.filter(|index| *index >= 0) {
            subscript.index = number(if source == DialectType::Snowflake {
                index.saturating_add(1)
            } else {
                index - 1
            });
        } else {
            let index = subscript.index.clone();
            let (condition, yes, no) = if source == DialectType::Snowflake {
                // Negative Snowflake positions are absent; DuckDB negative
                // positions count from the end, so use its absent zero slot.
                (
                    Expression::Gte(Box::new(BinaryOp::new(index.clone(), number(0)))),
                    Expression::Add(Box::new(BinaryOp::new(index, number(1)))),
                    number(0),
                )
            } else {
                let size = Expression::ArraySize(Box::new(UnaryFunc {
                    this: subscript.this.clone(),
                    original_name: None,
                    inferred_type: None,
                }));
                (
                    Expression::Lt(Box::new(BinaryOp::new(index.clone(), number(0)))),
                    Expression::Add(Box::new(BinaryOp::new(size, index.clone()))),
                    Expression::Sub(Box::new(BinaryOp::new(index, number(1)))),
                )
            };
            subscript.index = Expression::Case(Box::new(Case {
                operand: None,
                whens: vec![(condition, yes)],
                else_: Some(no),
                comments: vec![],
                inferred_type: None,
            }));
        }
        Expression::Subscript(subscript)
    }
    fn visit(value: &mut Value, source: DialectType) -> Result<()> {
        match value {
            Value::Object(fields) => {
                for child in fields.values_mut() {
                    visit(child, source)?;
                }
                if fields.contains_key("subscript") {
                    let expression = serde_json::from_value(value.clone())
                        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
                    *value = serde_json::to_value(adjust_node(expression, source))
                        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
                }
            }
            Value::Array(children) => {
                for child in children {
                    visit(child, source)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    // Walk every typed function payload; the dependency's visitor skips some
    // aggregate/scalar arguments, including SUM and ABS subscripts.
    let mut value = serde_json::to_value(expression)
        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
    visit(&mut value, source)?;
    serde_json::from_value(value).map_err(|error| SidemanticError::SqlGeneration(error.to_string()))
}

fn ossie_adjust_sql(sql: &str, source: DialectType, kind: Option<Fragment>) -> Result<String> {
    if source != DialectType::Snowflake || !sql.contains('[') {
        return Ok(sql.to_owned());
    }
    let expression = match kind {
        Some(kind) => {
            let wrapper = match kind {
                Fragment::Scalar => format!("SELECT {sql}"),
                Fragment::Order => format!("SELECT 1 ORDER BY {sql}"),
                Fragment::Table => format!("SELECT * FROM {sql}"),
            };
            extract_fragment(&wrapper, source, kind)?
        }
        None => parse(sql, source)?,
    };
    let adjusted = ossie_index_offsets(expression, source, DialectType::DuckDB)?;
    let sql = Dialect::get(source)
        .generate(&adjusted)
        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
    Ok(if matches!(kind, Some(Fragment::Order)) {
        sql.strip_prefix("ORDER BY ").unwrap_or(&sql).to_owned()
    } else {
        sql
    })
}

fn ossie_adjust_definition(definition: &mut Value, source: DialectType, query: bool) -> Result<()> {
    for name in [
        "sql",
        "entity",
        "window_expression",
        "having",
        "cohort_event",
        "activity_event",
        "window_order",
    ] {
        if let Some(Value::String(sql)) = definition.get_mut(name) {
            *sql = ossie_adjust_sql(
                sql,
                source,
                if name == "sql" && query {
                    None
                } else if name == "window_order" {
                    Some(Fragment::Order)
                } else {
                    Some(Fragment::Scalar)
                },
            )?;
        }
    }
    Ok(())
}

pub(crate) fn emit(
    expression: Expression,
    source: DialectType,
    target: DialectType,
) -> Result<String> {
    if source == target {
        return generate_preserving_null_order(&expression, target, source);
    }

    // The pinned dialect transformer has large recursive frames even for the
    // few nested SELECTs in an ordinary generated query. Run final emission on
    // the same stack budget used by the SQL parser, not the caller's test or
    // application thread. This boundary also covers Yardstick and policy SQL.
    #[cfg(not(target_arch = "wasm32"))]
    {
        std::thread::Builder::new()
            .stack_size(16 * 1024 * 1024)
            .spawn(move || emit_transformed(expression, source, target))
            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?
            .join()
            .map_err(|_| {
                SidemanticError::SqlGeneration("Polyglot emission thread panicked".into())
            })?
    }
    #[cfg(target_arch = "wasm32")]
    {
        emit_transformed(expression, source, target)
    }
}

fn emit_transformed(
    expression: Expression,
    source: DialectType,
    target: DialectType,
) -> Result<String> {
    let expression = super::literals::target_expression(expression, target)?;
    let expression = if target == DialectType::PostgreSQL {
        postgres_full_join_keys(expression)?
    } else {
        expression
    };
    // Generated ASTs can contain target SQL leaves. Keep them as ASTs instead
    // of reparsing target-quoted identifiers as source SQL. Authored input uses
    // the complete transpile pipeline below before reaching this boundary.
    let dialect = Dialect::get(target);
    let expression = dialect
        .transform(expression)
        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
    generate_preserving_null_order(&expression, target, source)
}

/// PostgreSQL only plans FULL JOIN on hash- or merge-joinable operators, which
/// excludes IS NOT DISTINCT FROM. Array equality is both, keeps the column
/// type's own comparison, and treats NULL elements as equal, so the NULL-safe
/// key match of independently aggregated sources survives unchanged.
fn postgres_full_join_keys(expression: Expression) -> Result<Expression> {
    use polyglot_sql::expressions::{ArrayConstructor, BinaryOp};

    fn array(element: Expression) -> Expression {
        Expression::ArrayFunc(Box::new(ArrayConstructor {
            expressions: vec![element],
            bracket_notation: true,
            use_list_keyword: false,
        }))
    }
    // Only top-level conjuncts are join keys; anything else keeps its meaning.
    fn lower(condition: Expression) -> Expression {
        match condition {
            Expression::And(operands) => {
                let mut operands = *operands;
                operands.left = lower(operands.left);
                operands.right = lower(operands.right);
                Expression::And(Box::new(operands))
            }
            Expression::Paren(inner) => {
                let mut inner = *inner;
                inner.this = lower(inner.this);
                Expression::Paren(Box::new(inner))
            }
            Expression::NullSafeEq(operands) => Expression::Eq(Box::new(BinaryOp::new(
                array(operands.left),
                array(operands.right),
            ))),
            other => other,
        }
    }
    fn visit(value: &mut Value) -> Result<()> {
        match value {
            Value::Object(fields) => {
                for child in fields.values_mut() {
                    visit(child)?;
                }
                // Joins are plain structs in the serialized AST, not tagged nodes.
                let full_join = fields.get("kind").and_then(Value::as_str) == Some("Full")
                    && fields.contains_key("using");
                if let Some(on) = fields.get_mut("on").filter(|on| full_join && !on.is_null()) {
                    let condition: Expression = serde_json::from_value(on.take())
                        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
                    *on = serde_json::to_value(lower(condition))
                        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
                }
            }
            Value::Array(values) => {
                for child in values {
                    visit(child)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    let mut value = serde_json::to_value(expression)
        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
    visit(&mut value)?;
    serde_json::from_value(value).map_err(|error| SidemanticError::SqlGeneration(error.to_string()))
}

/// Preserve explicit NULL placement even when the warehouse's mutable session
/// default currently matches it. Polyglot otherwise omits that clause. Render
/// only the Ordered leaf as target SQL after transformation, keeping every other
/// expression in the target dialect's normal generator.
pub(crate) fn generate_preserving_null_order(
    expression: &Expression,
    target: DialectType,
    source: DialectType,
) -> Result<String> {
    fn preserve(value: &mut Value, target: DialectType, source: DialectType) -> Result<()> {
        match value {
            Value::Object(fields) => {
                for child in fields.values_mut() {
                    preserve(child, target, source)?;
                }
                if fields.contains_key("this")
                    && fields.contains_key("desc")
                    && fields.contains_key("nulls_first")
                {
                    if let Some(nulls_first) = fields.get("nulls_first").and_then(Value::as_bool) {
                        let expression: Expression = serde_json::from_value(fields["this"].clone())
                            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
                        let sql = Dialect::get(target)
                            .generate_with_source(&expression, source)
                            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
                        let direction = if fields["desc"].as_bool() == Some(true) {
                            "DESC"
                        } else {
                            "ASC"
                        };
                        let nulls = if nulls_first { "FIRST" } else { "LAST" };
                        let raw = Expression::Var(Box::new(polyglot_sql::expressions::Var {
                            this: format!("{sql} {direction} NULLS {nulls}"),
                        }));
                        fields.insert(
                            "this".into(),
                            serde_json::to_value(raw).map_err(|error| {
                                SidemanticError::SqlGeneration(error.to_string())
                            })?,
                        );
                        fields.insert("desc".into(), Value::Bool(false));
                        fields.insert("explicit_asc".into(), Value::Bool(false));
                        fields.insert("nulls_first".into(), Value::Null);
                    }
                }
            }
            Value::Array(values) => {
                for child in values {
                    preserve(child, target, source)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    // These targets have mutable NULL defaults. Other target generators retain
    // their existing support/emulation rules.
    if !matches!(target, DialectType::DuckDB | DialectType::Snowflake) {
        return Dialect::get(target)
            .generate_with_source(expression, source)
            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()));
    }
    let mut value = serde_json::to_value(expression)
        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
    preserve(&mut value, target, source)?;
    let expression = serde_json::from_value(value)
        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
    Dialect::get(target)
        .generate_with_source(&expression, source)
        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))
}

fn transpile(sql: &str, source: DialectType, target: DialectType) -> Result<String> {
    let sql = super::literals::source_sql(sql, source)?;
    let sql = explicit_seed_aliases(&sql, source)?;
    #[cfg(target_arch = "wasm32")]
    crate::wasm_sql_guard::check(&sql, source)?;
    let mut statements = Dialect::get(source)
        .transpile_to(&sql, target)
        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
    if statements.len() != 1 {
        return Err(SidemanticError::SqlParse(
            "Expected one SQL statement".into(),
        ));
    }
    super::dates::normalize_transpiled(&statements.remove(0), source, target)
}

pub(crate) fn parse(sql: &str, source: DialectType) -> Result<Expression> {
    let sql = super::literals::source_sql(sql, source)?;
    let sql = explicit_seed_aliases(&sql, source)?;
    #[cfg(target_arch = "wasm32")]
    crate::wasm_sql_guard::check(&sql, source)?;
    let mut statements = parse_many(&sql, source)?;
    if statements.len() != 1 {
        return Err(SidemanticError::SqlParse(
            "Expected one SQL statement".into(),
        ));
    }
    Ok(statements.remove(0))
}

/// The pinned parser treats SEED after a derived table as a sampling clause,
/// even where the warehouse accepts it as an implicit alias. Insert only the
/// optional AS token in that exact lexer-confirmed relation context. Strings,
/// quoted identifiers, comments, and actual sampling clauses remain untouched.
fn explicit_seed_aliases(sql: &str, dialect: DialectType) -> Result<String> {
    use polyglot_sql::tokens::TokenType;
    if !sql.to_ascii_uppercase().contains("SEED") {
        return Ok(sql.to_owned());
    }
    let tokens = Dialect::get(dialect)
        .tokenize(sql)
        .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
    let offsets = sql
        .char_indices()
        .map(|(index, _)| index)
        .chain([sql.len()])
        .collect::<Vec<_>>();
    let mut opens = Vec::new();
    let mut closes = HashMap::new();
    let mut insertions = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        match token.token_type {
            TokenType::LParen => opens.push(index),
            TokenType::RParen => {
                if let Some(open) = opens.pop() {
                    closes.insert(index, open);
                }
            }
            TokenType::Seed if index > 0 => {
                let Some(&open) = closes.get(&(index - 1)) else {
                    continue;
                };
                if open == 0 {
                    continue;
                }
                let relation = matches!(
                    tokens[open - 1].token_type,
                    TokenType::From | TokenType::Join | TokenType::Comma | TokenType::Lateral
                );
                let query = matches!(
                    tokens[open + 1].token_type,
                    TokenType::Select | TokenType::With | TokenType::Values
                );
                if relation && query {
                    insertions.push(offsets[token.span.start]);
                }
            }
            _ => {}
        }
    }
    let mut output = sql.to_owned();
    for offset in insertions.into_iter().rev() {
        output.insert_str(offset, "AS ");
    }
    Ok(output)
}

/// QUANTILE_CONT/DISC are absent from the pinned parser's aggregate registry.
/// Parse their argument lists with its generic aggregate grammar, then restore
/// the actual name in the AST. ORDER BY, DISTINCT, FILTER and window clauses
/// must survive; deleting the aggregate ordering changes percentile semantics.
pub(crate) fn parse_many(sql: &str, dialect: DialectType) -> Result<Vec<Expression>> {
    use polyglot_sql::expressions::AggregateFunction;
    use polyglot_sql::tokens::TokenType;

    let mut replacements: HashMap<String, AggregateFunction> = HashMap::new();
    let mut prepared = sql.to_owned();
    if sql.to_ascii_uppercase().contains("QUANTILE_") {
        let stream = Dialect::get(dialect)
            .tokenize(sql)
            .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
        let offsets = sql
            .char_indices()
            .map(|(index, _)| index)
            .chain([sql.len()])
            .collect::<Vec<_>>();
        let mut prefix = "__sidemantic_quantile_".to_owned();
        while sql.to_ascii_lowercase().contains(&prefix) {
            prefix.push('_');
        }
        prepared.clear();
        let mut cursor = 0;
        let mut index = 0;
        while index + 1 < stream.len() {
            let token = &stream[index];
            if token.token_type == TokenType::Identifier
                || !["QUANTILE_CONT", "QUANTILE_DISC"]
                    .contains(&token.text.to_ascii_uppercase().as_str())
                || stream[index + 1].token_type != TokenType::LParen
                || index > 0 && stream[index - 1].token_type == TokenType::Dot
            {
                index += 1;
                continue;
            }
            let open = index + 1;
            let mut end = open + 1;
            let mut depth = 1;
            while end < stream.len() {
                match stream[end].token_type {
                    TokenType::LParen => depth += 1,
                    TokenType::RParen => depth -= 1,
                    _ => {}
                }
                if depth == 0 {
                    break;
                }
                end += 1;
            }
            if end == stream.len() {
                return Err(SidemanticError::SqlParse(
                    "Unclosed quantile aggregate".into(),
                ));
            }
            let body = &sql[offsets[stream[open].span.end]..offsets[stream[end].span.start]];
            // RESERVOIR_SAMPLE uses the generic aggregate grammar without a
            // specialized argument parser in the pinned registry. Only
            // this root node is renamed; nested calls keep their own identity.
            let mut parsed = parse_many(&format!("SELECT RESERVOIR_SAMPLE({body})"), dialect)?;
            let Expression::Select(select) = parsed.remove(0) else {
                unreachable!()
            };
            let Expression::AggregateFunction(mut aggregate) = select.expressions[0].clone() else {
                return Err(SidemanticError::SqlParse(
                    "Expected quantile aggregate arguments".into(),
                ));
            };
            aggregate.name = token.text.to_ascii_uppercase();
            let placeholder = format!("{prefix}{}", replacements.len());
            replacements.insert(placeholder.clone(), *aggregate);
            prepared.push_str(&sql[cursor..offsets[token.span.start]]);
            prepared.push_str(&format!("{placeholder}()"));
            cursor = offsets[stream[end].span.end];
            index = end + 1;
        }
        prepared.push_str(&sql[cursor..]);
    }
    let prepared = explicit_seed_aliases(&prepared, dialect)?;
    let statements = polyglot_sql::parse(&prepared, dialect)
        .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
    let complete_conditionals =
        dialect == DialectType::DuckDB && prepared.to_ascii_uppercase().contains("IF");
    if replacements.is_empty() && !complete_conditionals {
        return Ok(statements);
    }
    fn restore(
        value: &mut Value,
        replacements: &HashMap<String, AggregateFunction>,
        complete_conditionals: bool,
    ) -> Result<()> {
        match value {
            Value::Object(fields) => {
                for child in fields.values_mut() {
                    restore(child, replacements, complete_conditionals)?;
                }
                if complete_conditionals {
                    if let Some(function) = fields.get_mut("if_func").and_then(Value::as_object_mut)
                    {
                        if function.get("false_value").is_none_or(Value::is_null) {
                            // The parser accepts IF(condition, value), but the
                            // DuckDB emitter preserves that invalid two-argument
                            // call. Its omitted false branch has NULL semantics.
                            function.insert(
                                "false_value".into(),
                                serde_json::to_value(Expression::null()).map_err(|error| {
                                    SidemanticError::SqlParse(error.to_string())
                                })?,
                            );
                        }
                    }
                }
                for kind in ["function", "aggregate_function"] {
                    let Some(function) = fields.get(kind) else {
                        continue;
                    };
                    let Some(name) = function.get("name").and_then(Value::as_str) else {
                        continue;
                    };
                    let Some(saved) = replacements.get(&name.to_ascii_lowercase()) else {
                        continue;
                    };
                    let mut aggregate = saved.clone();
                    if kind == "aggregate_function" {
                        let wrapper: AggregateFunction =
                            serde_json::from_value(function.clone())
                                .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
                        aggregate.filter = wrapper.filter;
                        aggregate.ignore_nulls = wrapper.ignore_nulls.or(aggregate.ignore_nulls);
                    }
                    *value =
                        serde_json::to_value(Expression::AggregateFunction(Box::new(aggregate)))
                            .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
                    break;
                }
            }
            Value::Array(children) => {
                for child in children {
                    restore(child, replacements, complete_conditionals)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    let mut value = serde_json::to_value(statements)
        .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
    restore(&mut value, &replacements, complete_conditionals)?;
    serde_json::from_value(value).map_err(|error| SidemanticError::SqlParse(error.to_string()))
}

pub(crate) fn query(sql: &str, source: DialectType) -> Result<String> {
    if source == DialectType::DuckDB {
        return Ok(sql.to_owned());
    }
    parse(sql, source)?;
    transpile(sql, source, DialectType::DuckDB)
}

/// The native rewriter accepts ordinary statement batches without request
/// policies. Fragment/model validation continues to require exactly one query.
pub(crate) fn query_batch(sql: &str, source: DialectType) -> Result<String> {
    if source == DialectType::DuckDB {
        return Ok(sql.to_owned());
    }
    let sql = super::literals::source_sql(sql, source)?;
    let sql = explicit_seed_aliases(&sql, source)?;
    #[cfg(target_arch = "wasm32")]
    crate::wasm_sql_guard::check(&sql, source)?;
    Dialect::get(source)
        .transpile_to(&sql, DialectType::DuckDB)
        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?
        .iter()
        .map(|statement| super::dates::normalize_transpiled(statement, source, DialectType::DuckDB))
        .collect::<Result<Vec<_>>>()
        .map(|statements| statements.join(";\n"))
}

#[derive(Clone, Copy)]
pub(crate) enum Fragment {
    Scalar,
    Order,
    Table,
}

/// Wrappers let the SQL parser validate the complete fragment. A trailing
/// clause cannot disappear while extracting just its first expression.
pub(crate) fn fragment(sql: &str, source: DialectType, kind: Fragment) -> Result<String> {
    if source == DialectType::DuckDB || sql.trim().is_empty() {
        return Ok(sql.to_owned());
    }
    let wrapper = match kind {
        Fragment::Scalar => format!("SELECT {sql}"),
        Fragment::Order => format!("SELECT 1 ORDER BY {sql}"),
        Fragment::Table => format!("SELECT * FROM {sql}"),
    };
    extract_fragment(&wrapper, source, kind)?;
    let normalized = query(&wrapper, source)?;
    let result = extract_fragment(&normalized, DialectType::DuckDB, kind)?;
    let sql = polyglot_sql::generate(&result, DialectType::DuckDB)
        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
    Ok(match kind {
        Fragment::Order => sql.strip_prefix("ORDER BY ").unwrap_or(&sql).to_owned(),
        _ => sql,
    })
}

fn extract_fragment(wrapper: &str, source: DialectType, kind: Fragment) -> Result<Expression> {
    let Expression::Select(mut select) = parse(wrapper, source)? else {
        return Err(SidemanticError::SqlParse("Expected a SQL fragment".into()));
    };
    let expressions = std::mem::take(&mut select.expressions);
    let result = match kind {
        Fragment::Scalar if expressions.len() == 1 => expressions.into_iter().next().unwrap(),
        Fragment::Order => {
            Expression::OrderBy(Box::new(select.order_by.take().ok_or_else(|| {
                SidemanticError::SqlParse("Expected an ordering expression".into())
            })?))
        }
        Fragment::Table => {
            let mut from = select
                .from
                .take()
                .ok_or_else(|| SidemanticError::SqlParse("Expected a table expression".into()))?;
            if from.expressions.len() != 1 {
                return Err(SidemanticError::SqlParse(
                    "Expected one table expression".into(),
                ));
            }
            from.expressions.remove(0)
        }
        _ => {
            return Err(SidemanticError::SqlParse(
                "Expected one scalar expression".into(),
            ))
        }
    };
    select.leading_comments.clear();
    select.post_select_comments.clear();
    if *select != Select::new() {
        return Err(SidemanticError::SqlParse(
            "SQL fragment contains extra clauses".into(),
        ));
    }
    Ok(result)
}

fn field(definition: &mut Value, name: &str, dialect: DialectType, kind: Fragment) -> Result<()> {
    if let Some(Value::String(sql)) = definition.get_mut(name) {
        *sql = template_sql(sql, dialect, Some(kind))?;
    }
    Ok(())
}

fn fields(definition: &mut Value, name: &str, dialect: DialectType) -> Result<()> {
    if let Some(Value::Array(values)) = definition.get_mut(name) {
        for value in values {
            if let Value::String(sql) = value {
                *sql = template_sql(sql, dialect, Some(Fragment::Scalar))?;
            }
        }
    }
    Ok(())
}

/// Protect interpolation tokens while the library translates surrounding SQL.
/// Runtime interpolation still owns values and escaping. A collision-free name
/// prevents ordinary source identifiers or literals from being substituted.
pub(crate) fn template_sql(
    sql: &str,
    dialect: DialectType,
    kind: Option<Fragment>,
) -> Result<String> {
    if dialect == DialectType::DuckDB {
        return Ok(sql.to_owned());
    }
    let token = regex::Regex::new(r"\{\{[^{}]*\}\}|\$\{[^{}]*\}|\{(?:model|from|to)\}").unwrap();
    let mut prefix = "__sidemantic_dialect_token_".to_owned();
    while sql.to_ascii_lowercase().contains(&prefix) {
        prefix.push('_');
    }
    let mut replacements = Vec::new();
    let protected = token.replace_all(sql, |captures: &regex::Captures<'_>| {
        let name = format!("{prefix}{}__", replacements.len());
        replacements.push((name.clone(), captures[0].to_owned()));
        name
    });
    let mut result = match kind {
        Some(kind) => fragment(&protected, dialect, kind)?,
        None => query(&protected, dialect)?,
    };
    for (name, original) in replacements {
        result = result.replace(&name, &original);
    }
    Ok(result)
}

fn definition_dialect(definition: &mut Value, fallback: DialectType) -> Result<DialectType> {
    let Some(metadata) = definition
        .get_mut("metadata")
        .and_then(Value::as_object_mut)
    else {
        return Ok(fallback);
    };
    let field = if metadata.contains_key("ossie_target_dialect") {
        "ossie_target_dialect"
    } else {
        "ossie_expression_dialect"
    };
    let Some(value) = metadata.get_mut(field) else {
        return Ok(fallback);
    };
    let name = value
        .as_str()
        .ok_or_else(|| SidemanticError::Validation("Expression dialect must be a string".into()))?;
    let dialect = parse_dialect(name)?;
    *value = Value::String("duckdb".into());
    Ok(dialect)
}

fn metric(definition: &mut Value, source: DialectType, ossie_latest: bool) -> Result<()> {
    let source = definition_dialect(definition, source)?;
    if ossie_latest {
        ossie_adjust_definition(definition, source, false)?;
    }
    for name in [
        "sql",
        "entity",
        "window_expression",
        "having",
        "cohort_event",
        "activity_event",
    ] {
        field(definition, name, source, Fragment::Scalar)?;
    }
    for name in ["base_event", "conversion_event"] {
        // Bare names are event values, not expressions. Predicate-shaped
        // events are SQL and need the same translation as funnel steps.
        if definition
            .get(name)
            .and_then(Value::as_str)
            .is_some_and(|sql| sql.contains(['=', '<', '>', '(', ')']))
        {
            field(definition, name, source, Fragment::Scalar)?;
        }
    }
    field(definition, "window_order", source, Fragment::Order)?;
    for name in ["filters", "steps"] {
        fields(definition, name, source)?;
    }
    if let Some(Value::Array(inner)) = definition.get_mut("inner_metrics") {
        for item in inner {
            field(item, "sql", source, Fragment::Scalar)?;
        }
    }
    Ok(())
}

pub(super) type SegmentDialects = HashMap<(String, String), DialectType>;

pub(super) fn normalize(
    envelope: &mut super::Envelope,
    source: DialectType,
) -> Result<SegmentDialects> {
    let mut deferred_segments = HashMap::new();
    let latest_models: std::collections::HashSet<String> = envelope
        .models
        .iter()
        .filter(|model| {
            model
                .pointer("/metadata/ossie_schema_revision")
                .and_then(Value::as_str)
                == Some(crate::adapters::ossie::LATEST_SCHEMA_REVISION)
        })
        .filter_map(|model| model.get("name").and_then(Value::as_str).map(str::to_owned))
        .collect();
    let exclusively_latest =
        !envelope.models.is_empty() && latest_models.len() == envelope.models.len();
    for model in &mut envelope.models {
        let source = definition_dialect(model, source)?;
        let model_name = model
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let latest = latest_models.contains(&model_name);
        // Physical source SQL can be authored in a different dialect from the
        // selected/lowered field and metric expressions. Canonicalize each
        // against its own declaration, then mark both as executable DuckDB SQL.
        let physical_source = if latest {
            model
                .pointer("/metadata/ossie_source_dialect")
                .and_then(Value::as_str)
                .map(parse_dialect)
                .transpose()?
                .unwrap_or(source)
        } else {
            source
        };
        if let Some(Value::String(sql)) = model.get_mut("sql") {
            if latest {
                *sql = ossie_adjust_sql(sql, physical_source, None)?;
            }
            *sql = template_sql(sql, physical_source, None)?;
        }
        field(model, "table", physical_source, Fragment::Table)?;
        if latest {
            if let Some(marker) = model.pointer_mut("/metadata/ossie_source_dialect") {
                *marker = Value::String("duckdb".into());
            }
        }
        for collection in ["dimensions", "relationships", "segments"] {
            if let Some(Value::Array(values)) = model.get_mut(collection) {
                for value in values {
                    let dialect = definition_dialect(value, source)?;
                    if latest {
                        ossie_adjust_definition(value, dialect, false)?;
                    }
                    if collection == "segments"
                        && value
                            .get("sql")
                            .and_then(Value::as_str)
                            .is_some_and(crate::runtime::is_sql_template)
                    {
                        // Selected segments are rendered with request parameters
                        // before syntax normalization. Unselected templates stay
                        // inert, including Jinja control blocks and unused names.
                        let name = value
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        deferred_segments.insert((model_name.clone(), name.to_owned()), dialect);
                        continue;
                    }
                    field(value, "sql", dialect, Fragment::Scalar)?;
                }
            }
        }
        if let Some(Value::Array(metrics)) = model.get_mut("metrics") {
            for value in metrics {
                metric(value, source, latest)?;
            }
        }
        if let Some(Value::Array(preaggregations)) = model.get_mut("pre_aggregations") {
            for preaggregation in preaggregations {
                if let Some(Value::String(sql)) = preaggregation.get_mut("sql") {
                    *sql = template_sql(sql, source, None)?;
                }
            }
        }
    }
    for value in &mut envelope.metrics {
        let latest = exclusively_latest
            || value
                .get("name")
                .and_then(Value::as_str)
                .and_then(|name| envelope.metric_owners.get(name))
                .is_some_and(|owner| latest_models.contains(owner));
        metric(value, source, latest)?;
    }
    Ok(deferred_segments)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_full_join_uses_hash_joinable_null_safe_keys() {
        let source = "SELECT * FROM a FULL OUTER JOIN b \
                      ON a.x IS NOT DISTINCT FROM b.x AND a.total = b.total \
                      FULL OUTER JOIN c ON COALESCE(a.x, b.x) IS NOT DISTINCT FROM c.x \
                      LEFT JOIN d ON c.x IS NOT DISTINCT FROM d.x";
        let emitted = |target| {
            let expression = parse(source, DialectType::DuckDB).unwrap();
            emit(expression, DialectType::DuckDB, target).unwrap()
        };
        let postgres = emitted(DialectType::PostgreSQL);
        assert!(
            postgres.contains("ON ARRAY[a.x] = ARRAY[b.x] AND a.total = b.total"),
            "{postgres}"
        );
        assert!(
            postgres.contains("ON ARRAY[COALESCE(a.x, b.x)] = ARRAY[c.x]"),
            "{postgres}"
        );
        // Other join kinds accept the operator as written.
        assert!(
            postgres.contains("LEFT JOIN d ON c.x IS NOT DISTINCT FROM d.x"),
            "{postgres}"
        );
        for target in [DialectType::DuckDB, DialectType::Snowflake] {
            let sql = emitted(target);
            assert!(!sql.contains("ARRAY["), "{target:?}: {sql}");
            assert_eq!(
                sql.matches("IS NOT DISTINCT FROM").count(),
                3,
                "{target:?}: {sql}"
            );
        }
    }

    #[test]
    fn source_transpilation_keeps_seed_alias_and_empty_predicate() {
        let source = "SELECT amount FROM (SELECT 1 AS amount) seed WHERE FALSE";
        for dialect in [
            DialectType::DuckDB,
            DialectType::PostgreSQL,
            DialectType::Snowflake,
            DialectType::ClickHouse,
            DialectType::BigQuery,
        ] {
            for sql in [
                query(source, dialect).unwrap(),
                query_batch(source, dialect).unwrap(),
            ] {
                let Expression::Select(select) = parse(&sql, DialectType::DuckDB).unwrap() else {
                    panic!("expected SELECT");
                };
                assert!(select.where_clause.is_some(), "{dialect:?}: {sql}");
                assert!(sql.contains("FALSE"), "{dialect:?}: {sql}");
            }
        }
    }

    #[test]
    fn derived_seed_aliases_do_not_rewrite_sampling_or_quoted_text() {
        for sql in [
            "SELECT amount FROM (SELECT 1 AS amount) seed WHERE FALSE",
            "SELECT seed.amount FROM (SELECT 1 AS amount) /* é seed */ seed",
            "WITH x AS (SELECT 1 AS amount) SELECT amount FROM (SELECT * FROM x) seed",
            "SELECT seed.amount FROM (VALUES (1)) seed(amount)",
        ] {
            let rewritten = explicit_seed_aliases(sql, DialectType::DuckDB).unwrap();
            assert!(rewritten.contains("AS seed"), "{rewritten}");
            assert!(parse_many(sql, DialectType::DuckDB).is_ok(), "{sql}");
        }
        for sql in [
            "SELECT 'seed' FROM (SELECT 1) AS seed",
            "SELECT * FROM (SELECT 1) \"seed\"",
            "SELECT * FROM t USING SAMPLE 10 PERCENT (bernoulli, 42)",
            "SELECT * FROM t TABLESAMPLE BERNOULLI (10) SEED (42)",
        ] {
            assert_eq!(
                explicit_seed_aliases(sql, DialectType::DuckDB).unwrap(),
                sql
            );
        }
    }

    #[test]
    fn ossie_physical_and_expression_dialects_normalize_independently_once() {
        for (physical, expression, query, field_sql) in [
            (
                "snowflake",
                "duckdb",
                "SELECT ARRAY_CONSTRUCT(10, 20)[0] AS first, ARRAY_CONSTRUCT(30, 40) AS xs",
                "xs[1]",
            ),
            (
                "duckdb",
                "snowflake",
                "SELECT [10, 20][1] AS first, [30, 40] AS xs",
                "xs[0]",
            ),
            (
                "snowflake",
                "snowflake",
                "SELECT ARRAY_CONSTRUCT(10, 20)[0] AS first, ARRAY_CONSTRUCT(30, 40) AS xs",
                "xs[0]",
            ),
        ] {
            let mut envelope: crate::semantic_input::Envelope = serde_json::from_value(serde_json::json!({
                "version": 1, "input_dialect": "duckdb", "models": [{
                    "name": "items", "sql": query,
                    "metadata": {"ossie_schema_revision": crate::adapters::ossie::LATEST_SCHEMA_REVISION,
                        "ossie_source_dialect": physical, "ossie_target_dialect": expression},
                    "dimensions": [{"name": "value", "sql": field_sql}],
                    "metrics": [{"name": "total", "sql": format!("SUM({field_sql})")}]
                }]
            })).unwrap();
            normalize(&mut envelope, DialectType::DuckDB).unwrap();
            let canonical = envelope.models.clone();
            let model = &canonical[0];
            assert_eq!(model["metadata"]["ossie_source_dialect"], "duckdb");
            assert_eq!(model["metadata"]["ossie_target_dialect"], "duckdb");
            let source_sql = model["sql"].as_str().unwrap();
            assert!(source_sql.contains("[1]"), "{source_sql}");
            assert!(!source_sql.contains("[2]"), "{source_sql}");
            assert_eq!(model["dimensions"][0]["sql"], "xs[1]");
            assert_eq!(model["metrics"][0]["sql"], "SUM(xs[1])");
            normalize(&mut envelope, DialectType::DuckDB).unwrap();
            assert_eq!(
                envelope.models, canonical,
                "canonicalization must be idempotent"
            );
        }
    }

    #[test]
    fn ossie_array_offsets_reach_typed_functions_and_keep_object_keys() {
        let expression = parse(
            "SELECT SUM(ABS(xs[0])), object['key'], xs[position], xs[-2] FROM source",
            DialectType::Snowflake,
        )
        .unwrap();
        let adjusted =
            ossie_index_offsets(expression, DialectType::Snowflake, DialectType::DuckDB).unwrap();
        let sql = Dialect::get(DialectType::DuckDB)
            .generate(&adjusted)
            .unwrap();
        assert!(sql.contains("SUM(ABS(xs[1]))"), "{sql}");
        assert!(sql.contains("object['key']"), "{sql}");
        assert!(sql.contains("position >= 0"), "{sql}");
        assert!(sql.contains("ELSE 0 END"), "{sql}");
        // Native, unmarked declarations continue through the original path.
        assert_eq!(
            fragment("xs[0]", DialectType::Snowflake, Fragment::Scalar).unwrap(),
            "xs[0]"
        );
    }

    #[test]
    fn ordered_quantiles_preserve_arguments_order_filter_and_window() {
        for name in ["quantile_cont", "quantile_disc"] {
            let sql = format!("SELECT {name}(DISTINCT value, 0.25 ORDER BY sort_key DESC NULLS FIRST) FILTER (WHERE included) OVER (PARTITION BY category), 'é QUANTILE_CONT(x ORDER BY y)' AS label");
            let parsed = parse(&sql, DialectType::DuckDB).unwrap();
            let generated = polyglot_sql::generate(&parsed, DialectType::DuckDB).unwrap();
            assert!(
                generated.contains(&format!(
                    "{}(DISTINCT value, 0.25 ORDER BY sort_key DESC NULLS FIRST)",
                    name.to_ascii_uppercase()
                )),
                "{generated}"
            );
            assert!(
                generated.contains("FILTER(WHERE included)")
                    || generated.contains("FILTER (WHERE included)"),
                "{generated}"
            );
            assert!(generated.contains("PARTITION BY category"), "{generated}");
            assert!(
                generated.contains("'é QUANTILE_CONT(x ORDER BY y)'"),
                "{generated}"
            );
            let reparsed = parse(&generated, DialectType::DuckDB).unwrap();
            assert_eq!(
                polyglot_sql::generate(&reparsed, DialectType::DuckDB).unwrap(),
                generated
            );
        }
    }

    #[test]
    fn omitted_conditional_false_branches_are_explicit_nulls_for_duckdb() {
        for (input, expected) in [
            ("IF(flag, 1)", "IF(flag, 1, NULL)"),
            ("COUNT(IF(flag, 1))", "COUNT(IF(flag, 1, NULL))"),
            (
                "IF(flag, IF(other, 1), 0)",
                "IF(flag, IF(other, 1, NULL), 0)",
            ),
            ("IF(flag, 1, 0)", "IF(flag, 1, 0)"),
        ] {
            let parsed = parse(&format!("SELECT {input}"), DialectType::DuckDB).unwrap();
            let generated = polyglot_sql::generate(&parsed, DialectType::DuckDB).unwrap();
            assert_eq!(generated, format!("SELECT {expected}"));
        }
        let sql = "SELECT 'IF(flag, 1)' AS label";
        let parsed = parse(sql, DialectType::DuckDB).unwrap();
        assert_eq!(
            polyglot_sql::generate(&parsed, DialectType::DuckDB).unwrap(),
            sql
        );
    }

    #[test]
    fn nested_quantiles_keep_each_function_identity() {
        let parsed = parse("SELECT quantile_cont((SELECT quantile_disc(value, 0.5 ORDER BY value) FROM raw), 0.25 ORDER BY rank)", DialectType::DuckDB).unwrap();
        let generated = polyglot_sql::generate(&parsed, DialectType::DuckDB).unwrap();
        assert!(generated.contains("QUANTILE_CONT("), "{generated}");
        assert!(
            generated.contains("QUANTILE_DISC(value, 0.5 ORDER BY value)"),
            "{generated}"
        );
        assert!(!generated.contains("RESERVOIR_SAMPLE"), "{generated}");
        assert!(!generated.contains("__sidemantic_quantile_"), "{generated}");
    }

    #[test]
    fn nested_query_emission_uses_production_stack_on_standard_test_thread() {
        // Construct the AST directly to isolate final emission from parsing.
        let mut expression = Expression::Select(Box::new(
            Select::new().column(Expression::number(7).alias("value")),
        ));
        for index in 0..5 {
            let source = Expression::Paren(Box::new(polyglot_sql::expressions::Paren {
                this: expression,
                trailing_comments: Vec::new(),
            }))
            .alias(format!("q{index}"));
            expression = Expression::Select(Box::new(
                Select::new()
                    .column(Expression::column("value"))
                    .from(source),
            ));
        }
        for target in [DialectType::Generic, DialectType::PostgreSQL] {
            let sql = emit(expression.clone(), DialectType::DuckDB, target).unwrap();
            assert_eq!(sql.matches("SELECT").count(), 6, "{sql}");
            assert!(sql.contains("7 AS value"), "{sql}");
        }
    }

    #[test]
    fn library_normalizes_source_functions_and_identifiers() {
        for (dialect, sql) in [
            (DialectType::BigQuery, "IFNULL(`amount`, 0)"),
            (DialectType::MySQL, "IFNULL(`amount`, 0)"),
            (DialectType::TSQL, "ISNULL([amount], 0)"),
        ] {
            let result = fragment(sql, dialect, Fragment::Scalar).unwrap();
            assert!(result.to_uppercase().contains("COALESCE"), "{result}");
            let expression = crate::core::parse_semantic_expression(&result).unwrap();
            assert!(matches!(expression, Expression::Coalesce(_)), "{result}");
        }
    }

    #[test]
    fn source_order_defaults_survive_the_intermediate_dialect() {
        let result = fragment("amount DESC", DialectType::PostgreSQL, Fragment::Order).unwrap();
        assert!(result.contains("NULLS FIRST"), "{result}");
        let result = fragment("amount ASC", DialectType::BigQuery, Fragment::Order).unwrap();
        assert!(result.contains("NULLS FIRST"), "{result}");
    }

    #[test]
    fn fragments_cannot_hide_trailing_clauses_or_statements() {
        for sql in ["amount LIMIT 1", "amount FROM orders", "amount; SELECT 2"] {
            assert!(
                fragment(sql, DialectType::PostgreSQL, Fragment::Scalar).is_err(),
                "{sql}"
            );
        }
        assert!(fragment(
            "amount DESC LIMIT 1",
            DialectType::BigQuery,
            Fragment::Order
        )
        .is_err());
    }

    #[test]
    fn interpolation_tokens_and_matching_literal_text_are_preserved() {
        let sql =
            "IFNULL({model}.`amount`, {{ minimum }}) + LENGTH('__sidemantic_dialect_token_0__')";
        let result = template_sql(sql, DialectType::BigQuery, Some(Fragment::Scalar)).unwrap();
        assert!(result.contains("{model}."), "{result}");
        assert!(result.contains("{{ minimum }}"), "{result}");
        assert!(
            result.contains("'__sidemantic_dialect_token_0__'"),
            "{result}"
        );
        let relationship = template_sql(
            "{from}.`customer_id` = {to}.`id`",
            DialectType::BigQuery,
            Some(Fragment::Scalar),
        )
        .unwrap();
        assert!(relationship.contains("{from}."), "{relationship}");
        assert!(relationship.contains("{to}."), "{relationship}");
    }
}
