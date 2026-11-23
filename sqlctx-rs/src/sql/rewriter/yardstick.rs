//! Yardstick evaluation contexts lowered into ordinary correlated SQL.
//!
//! Polyglot does not parse SEMANTIC, curly measure references, or AT chains.
//! Only those extension tokens are normalized here. Every measure reference,
//! modifier expression, query, and generated subquery uses Polyglot's AST.

use std::collections::{HashMap, HashSet};

use super::{parse_sql_with_dialect, table_name_and_alias, QueryRewriter};
use crate::core::MetricType;
use crate::error::{Result, SqlctxError};
use crate::semantic_input::dialects;
use polyglot_sql::dialects::{DialectImpl, SnowflakeDialect};
use polyglot_sql::expressions::{GroupBy, Identifier, Select, Where};
use polyglot_sql::tokens::{Token, TokenType, Tokenizer};
use polyglot_sql::{generate as polyglot_generate, Dialect, DialectType, Expression};
use serde_json::Value;

#[derive(Clone)]
struct Call {
    argument: Expression,
    modifiers: Vec<String>,
    visible: bool,
}

struct Lowerer<'a, 'g> {
    rewriter: &'a QueryRewriter<'g>,
    source_dialect: DialectType,
    calls: HashMap<String, Call>,
    reserved: HashSet<String>,
    changed: bool,
}

fn invalid(message: impl Into<String>) -> SqlctxError {
    SqlctxError::Validation(message.into())
}

fn encode<T: serde::Serialize>(value: T) -> Result<Value> {
    serde_json::to_value(value).map_err(|error| invalid(error.to_string()))
}

fn decode<T: serde::de::DeserializeOwned>(value: Value) -> Result<T> {
    serde_json::from_value(value).map_err(|error| invalid(error.to_string()))
}

fn tokens(sql: &str, dialect: DialectType) -> Result<Vec<Token>> {
    let mut tokens = if dialect == DialectType::Snowflake {
        // Lex the original escaped string spans. Literal decoding is performed
        // once later by dialect normalization, not by this extension scanner.
        let mut config = SnowflakeDialect.tokenizer_config();
        config.string_escapes.push('\\');
        Tokenizer::new(config).tokenize(sql)
    } else {
        Dialect::get(dialect).tokenize(sql)
    }
    .map_err(|error| SqlctxError::SqlParse(error.to_string()))?;
    // Polyglot 0.1.x spans count Unicode scalars; Rust string slices use bytes.
    let offsets = sql
        .char_indices()
        .map(|(offset, _)| offset)
        .chain([sql.len()])
        .collect::<Vec<_>>();
    for token in &mut tokens {
        token.span.start = *offsets
            .get(token.span.start)
            .ok_or_else(|| invalid("Invalid Yardstick token span"))?;
        token.span.end = *offsets
            .get(token.span.end)
            .ok_or_else(|| invalid("Invalid Yardstick token span"))?;
    }
    Ok(tokens)
}

fn matching(tokens: &[Token], open: usize) -> Result<usize> {
    let mut depth = 0;
    for (index, token) in tokens.iter().enumerate().skip(open) {
        match token.token_type {
            TokenType::LParen | TokenType::LBrace => depth += 1,
            TokenType::RParen | TokenType::RBrace => {
                depth -= 1;
                if depth == 0 {
                    return Ok(index);
                }
            }
            _ => {}
        }
    }
    Err(invalid("Unclosed Yardstick expression"))
}

fn reference(expression: &Expression) -> Option<(Option<String>, String)> {
    match expression {
        Expression::Column(column) => Some((
            column.table.as_ref().map(|table| table.name.clone()),
            column.name.name.clone(),
        )),
        Expression::Identifier(identifier) => Some((None, identifier.name.clone())),
        _ => None,
    }
}

/// Walk the complete serialized AST: the pinned Polyglot public walker omits
/// some typed aggregate children. SELECT boundaries must remain scope-local.
fn map_columns(
    value: &mut Value,
    callback: &mut impl FnMut(Expression) -> Result<Expression>,
) -> Result<()> {
    match value {
        Value::Object(fields) => {
            if fields.len() == 1 && fields.contains_key("select") {
                return Ok(());
            }
            if fields.len() == 1 && fields.contains_key("column") {
                *value = encode(callback(decode(value.clone())?)?)?;
                return Ok(());
            }
            for child in fields.values_mut() {
                map_columns(child, callback)?;
            }
        }
        Value::Array(children) => {
            for child in children {
                map_columns(child, callback)?;
            }
        }
        _ => {}
    }
    Ok(())
}

impl Lowerer<'_, '_> {
    fn expression(&self, sql: &str) -> Result<Expression> {
        let statements = parse_sql_with_dialect(&format!("SELECT {sql}"), DialectType::DuckDB)?;
        match statements.as_slice() {
            [Expression::Select(select)] if select.expressions.len() == 1 => {
                let mut remainder = (**select).clone();
                remainder.expressions.clear();
                remainder.leading_comments.clear();
                remainder.post_select_comments.clear();
                if remainder != Select::new() {
                    return Err(invalid(format!("Expected one Yardstick expression: {sql}")));
                }
                Ok(select.expressions[0].clone())
            }
            _ => Err(invalid(format!("Expected one Yardstick expression: {sql}"))),
        }
    }

    fn sql(&self, expression: &Expression) -> Result<String> {
        polyglot_generate(expression, DialectType::DuckDB)
            .map_err(|error| SqlctxError::SqlGeneration(error.to_string()))
    }

    fn authored_expression(&self, sql: &str) -> Result<Expression> {
        self.expression(&dialects::fragment(
            sql,
            self.source_dialect,
            dialects::Fragment::Scalar,
        )?)
    }

    fn register(&mut self, call: Call) -> String {
        let mut index = self.calls.len();
        loop {
            let name = format!("__sqlctx_yardstick_{index}");
            index += 1;
            if self.reserved.insert(name.clone()) {
                self.calls.insert(name.clone(), call);
                return name;
            }
        }
    }

    fn normalize(&mut self, sql: &str) -> Result<(String, bool)> {
        let stream = tokens(sql, self.source_dialect)?;
        self.reserved
            .extend(stream.iter().map(|token| token.text.to_lowercase()));
        let semantic = stream
            .first()
            .is_some_and(|token| token.text.eq_ignore_ascii_case("SEMANTIC"));
        let mut cursor = if semantic { stream[0].span.end } else { 0 };
        let mut index = usize::from(semantic);
        let mut output = String::new();
        while index < stream.len() {
            let mut start = index;
            let mut end = index;
            let mut argument = None;
            let mut visible = false;
            if stream[index].text.eq_ignore_ascii_case("AGGREGATE")
                && stream
                    .get(index + 1)
                    .is_some_and(|token| token.token_type == TokenType::LParen)
            {
                end = matching(&stream, index + 1)?;
                let candidate = &sql[stream[index + 1].span.end..stream[end].span.start];
                argument = self
                    .authored_expression(candidate)
                    .ok()
                    .filter(|expr| reference(expr).is_some());
                visible = true;
                if index >= 2 && stream[index - 1].token_type == TokenType::Dot {
                    start = index - 2;
                }
            } else if stream[index].token_type == TokenType::LBrace {
                end = matching(&stream, index)?;
                let candidate = &sql[stream[index].span.end..stream[end].span.start];
                argument = self
                    .authored_expression(candidate)
                    .ok()
                    .filter(|expr| reference(expr).is_some());
                // Curly references have the plain measure evaluation context;
                // only AGGREGATE(...) inherits the visible row predicate.
            } else {
                while stream
                    .get(end + 1)
                    .is_some_and(|token| token.token_type == TokenType::Dot)
                    && end + 2 < stream.len()
                {
                    end += 2;
                }
                if stream
                    .get(end + 1)
                    .is_some_and(|token| token.text.eq_ignore_ascii_case("AT"))
                    && stream
                        .get(end + 2)
                        .is_some_and(|token| token.token_type == TokenType::LParen)
                {
                    argument = self
                        .authored_expression(&sql[stream[index].span.start..stream[end].span.end])
                        .ok()
                        .filter(|expr| reference(expr).is_some());
                }
            }
            let Some(argument) = argument else {
                index += 1;
                continue;
            };
            let mut clauses = Vec::new();
            while stream
                .get(end + 1)
                .is_some_and(|token| token.text.eq_ignore_ascii_case("AT"))
                && stream
                    .get(end + 2)
                    .is_some_and(|token| token.token_type == TokenType::LParen)
            {
                let open = end + 2;
                end = matching(&stream, open)?;
                clauses.push(split_modifiers(
                    &sql[stream[open].span.end..stream[end].span.start],
                    self.source_dialect,
                )?);
            }
            output.push_str(&sql[cursor..stream[start].span.start]);
            output.push_str(&self.register(Call {
                argument,
                modifiers: clauses.into_iter().rev().flatten().collect(),
                visible,
            }));
            cursor = stream[end].span.end;
            index = end + 1;
        }
        output.push_str(&sql[cursor..]);
        Ok((output, semantic))
    }

    fn remap(
        &self,
        expression: Expression,
        aliases: &[&str],
        target: &str,
        unqualified: bool,
    ) -> Result<Expression> {
        let mut value = encode(expression)?;
        map_columns(&mut value, &mut |node| {
            if let Expression::Column(mut column) = node {
                let table = column.table.as_ref().map(|table| table.name.as_str());
                if table.is_some_and(|table| aliases.contains(&table))
                    || (table.is_none() && unqualified)
                {
                    column.table = (!target.is_empty()).then(|| Identifier::new(target));
                }
                return Ok(Expression::Column(column));
            }
            Ok(node)
        })?;
        decode(value)
    }

    fn signature(&self, expression: Expression) -> Result<String> {
        let mut value = encode(expression)?;
        map_columns(&mut value, &mut |node| {
            if let Expression::Column(mut column) = node {
                column.table = None;
                // Quoting is syntax, not a different context dimension. The
                // declared dimension expansion may have removed source quotes.
                column.name.quoted = false;
                return Ok(Expression::Column(column));
            }
            Ok(node)
        })?;
        self.sql(&decode(value)?).map(|sql| sql.to_lowercase())
    }

    fn resolve(
        &self,
        expression: &Expression,
        sources: &[(String, String)],
        implicit: bool,
    ) -> Result<Option<(String, String, String)>> {
        let Some((qualifier, name)) = reference(expression) else {
            return Ok(None);
        };
        let matches = sources
            .iter()
            .filter(|(model, alias)| {
                qualifier
                    .as_ref()
                    .is_none_or(|qualifier| qualifier == alias || (!implicit && qualifier == model))
                    && self.rewriter.graph.get_model(model).is_some_and(|model| {
                        (!implicit
                            || model
                                .metadata
                                .as_ref()
                                .is_some_and(|metadata| metadata.get("yardstick").is_some()))
                            && model.get_metric(&name).is_some()
                    })
            })
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [(model, alias)] => Ok(Some((model.clone(), alias.clone(), name))),
            [] => Ok(None),
            _ if implicit => Ok(None),
            _ => Err(SqlctxError::YardstickBinding(format!(
                "Ambiguous Yardstick measure '{name}'"
            ))),
        }
    }

    fn scopes(&mut self, value: &mut Value) -> Result<()> {
        match value {
            Value::Object(fields) => {
                for child in fields.values_mut() {
                    self.scopes(child)?;
                }
                if fields.len() == 1 && fields.contains_key("select") {
                    let Expression::Select(select) = decode(value.clone())? else {
                        unreachable!()
                    };
                    *value = encode(Expression::Select(Box::new(self.select(*select)?)))?;
                }
            }
            Value::Array(children) => {
                for child in children {
                    self.scopes(child)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn select(&mut self, mut select: Select) -> Result<Select> {
        if let Some(from) = &mut select.from {
            for source in &mut from.expressions {
                self.table_function(source)?;
            }
        }
        for join in &mut select.joins {
            self.table_function(&mut join.this)?;
        }
        let mut sources = select
            .from
            .as_ref()
            .map(|from| {
                from.expressions
                    .iter()
                    .filter_map(table_name_and_alias)
                    .filter(|(name, _)| self.rewriter.graph.get_model(name).is_some())
                    .map(|(name, alias)| (name.clone(), alias.unwrap_or(name)))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for join in &select.joins {
            if let Some((name, alias)) = table_name_and_alias(&join.this) {
                if self.rewriter.graph.get_model(&name).is_some() {
                    sources.push((name.clone(), alias.unwrap_or(name)));
                }
            }
        }
        let single = sources.len() == 1
            && select.joins.is_empty()
            && select
                .from
                .as_ref()
                .is_some_and(|from| from.expressions.len() == 1);
        // Implicit measures apply to projection/HAVING/ORDER only. WHERE columns
        // remain row predicates, unless the caller used an explicit AT call.
        for projection in &mut select.expressions {
            let alias = self
                .resolve(projection, &sources, true)?
                .map(|(_, _, name)| name);
            let mut value = encode(&*projection)?;
            map_columns(&mut value, &mut |node| {
                if self.resolve(&node, &sources, true)?.is_some() {
                    let name = self.register(Call {
                        argument: node,
                        modifiers: vec![],
                        visible: false,
                    });
                    self.expression(&name)
                } else {
                    Ok(node)
                }
            })?;
            *projection = decode(value)?;
            if let Some(alias) = alias {
                *projection = Expression::alias(projection.clone(), alias);
            }
        }
        for mut value in [encode(&select.having)?, encode(&select.order_by)?]
            .into_iter()
            .enumerate()
        {
            map_columns(&mut value.1, &mut |node| {
                if self.resolve(&node, &sources, true)?.is_some() {
                    let name = self.register(Call {
                        argument: node,
                        modifiers: vec![],
                        visible: false,
                    });
                    self.expression(&name)
                } else {
                    Ok(node)
                }
            })?;
            if value.0 == 0 {
                select.having = decode(value.1)?;
            } else {
                select.order_by = decode(value.1)?;
            }
        }
        let mut scope_calls = HashSet::new();
        let mut value = encode(&select)?;
        map_columns(&mut value, &mut |node| {
            if let Some((None, name)) = reference(&node) {
                if self.calls.contains_key(&name) {
                    scope_calls.insert(name);
                }
            }
            Ok(node)
        })?;
        if scope_calls.is_empty() {
            return Ok(select);
        }
        if sources.is_empty() {
            return Err(SqlctxError::YardstickBinding(
                "Yardstick query must reference a known semantic model in FROM/JOIN".into(),
            ));
        }
        // Match transport_security: Yardstick's independent evaluation contexts
        // cannot enter a request with semantic access or row-security controls.
        if self.rewriter.security_controls {
            return Err(SqlctxError::Security(
                "Yardstick SQL is not supported while semantic security controls are active".into(),
            ));
        }
        self.changed = true;
        let output_aliases = select
            .expressions
            .iter()
            .filter_map(|projection| {
                if let Expression::Alias(alias) = projection {
                    Some(alias.alias.name.clone())
                } else {
                    None
                }
            })
            .collect::<HashSet<_>>();
        let original_names = select
            .expressions
            .iter()
            .map(|projection| reference(projection).map(|(_, name)| name))
            .collect::<Vec<_>>();
        // Expand declared dimension expressions before computing context keys.
        map_columns(&mut value, &mut |node| {
            let Some((table, name)) = reference(&node) else {
                return Ok(node);
            };
            if scope_calls.contains(&name) {
                return Ok(node);
            }
            let matches = sources
                .iter()
                .filter(|(model, alias)| {
                    table
                        .as_ref()
                        .map_or(single, |table| table == alias || table == model)
                })
                .collect::<Vec<_>>();
            if let [(model_name, alias)] = matches.as_slice() {
                let model = self.rewriter.graph.get_model(model_name).unwrap();
                if let Some(dimension) = model.get_dimension(&name) {
                    let expression =
                        self.expression(&dimension.sql_expr().replace("{model}", alias))?;
                    return self.remap(expression, &[model_name, alias], alias, true);
                }
                if table.is_none() && single {
                    if output_aliases.contains(&name) {
                        return Ok(node);
                    }
                    return self.remap(node, &[], alias, true);
                }
            }
            Ok(node)
        })?;
        select = decode(value)?;
        for (projection, original_name) in select.expressions.iter_mut().zip(original_names) {
            if let Some(name) = original_name {
                if !scope_calls.contains(&name) {
                    *projection = projection.clone().alias(name);
                }
            }
        }
        let contains_call = |expression: &Expression| -> Result<bool> {
            let mut found = false;
            map_columns(&mut encode(expression)?, &mut |node| {
                found |= reference(&node).is_some_and(|(_, name)| scope_calls.contains(&name));
                Ok(node)
            })?;
            Ok(found)
        };
        let mut scalar_aggregate = false;
        if select.group_by.is_none()
            && select
                .expressions
                .iter()
                .any(|expr| contains_call(expr).unwrap_or(false))
        {
            let mut groups = Vec::new();
            for expression in &select.expressions {
                let expression = match expression {
                    Expression::Alias(alias) => &alias.this,
                    other => other,
                };
                if !contains_call(expression)?
                    && !matches!(
                        expression,
                        Expression::Literal(_) | Expression::Null(_) | Expression::Boolean(_)
                    )
                {
                    groups.push(expression.clone());
                }
            }
            if groups.is_empty() {
                scalar_aggregate = true;
            } else {
                select.group_by = Some(GroupBy {
                    expressions: groups,
                    all: None,
                    totals: false,
                    comments: vec![],
                });
            }
        }
        if select
            .group_by
            .as_ref()
            .is_some_and(|group| group.all == Some(true))
            && !select
                .expressions
                .iter()
                .any(|projection| matches!(projection, Expression::Star(_)))
        {
            let mut groups = Vec::new();
            for projection in &select.expressions {
                let expression = match projection {
                    Expression::Alias(alias) => &alias.this,
                    other => other,
                };
                if !contains_call(expression)?
                    && !has_aggregate_semantics(&encode(expression)?)
                    && !matches!(
                        expression,
                        Expression::Literal(_) | Expression::Null(_) | Expression::Boolean(_)
                    )
                {
                    groups.push(expression.clone());
                }
            }
            if !groups.is_empty() {
                select.group_by = Some(GroupBy {
                    expressions: groups,
                    all: None,
                    totals: false,
                    comments: vec![],
                });
            }
        }
        if let Some(group) = &mut select.group_by {
            for expression in &mut group.expressions {
                if let Ok(index) = self.sql(expression)?.parse::<usize>() {
                    if let Some(projection) = index
                        .checked_sub(1)
                        .and_then(|index| select.expressions.get(index))
                    {
                        *expression = match projection {
                            Expression::Alias(alias) => alias.this.clone(),
                            other => other.clone(),
                        };
                    }
                } else if let Some((None, name)) = reference(expression) {
                    if let Some(Expression::Alias(alias)) = select.expressions.iter().find(|projection| matches!(projection, Expression::Alias(alias) if alias.alias.name == name)) {
                        *expression = alias.this.clone();
                    }
                }
            }
        }
        let groups = select
            .group_by
            .as_ref()
            .map(|group| group.expressions.clone())
            .unwrap_or_default();
        let mut context_aliases = HashMap::new();
        for projection in &mut select.expressions {
            if !matches!(
                projection,
                Expression::Alias(_)
                    | Expression::Column(_)
                    | Expression::Literal(_)
                    | Expression::Null(_)
                    | Expression::Boolean(_)
            ) && !contains_call(projection)?
            {
                let mut columns = false;
                map_columns(&mut encode(&*projection)?, &mut |node| {
                    columns = true;
                    Ok(node)
                })?;
                if columns {
                    let alias = format!("__ysdim_{}", context_aliases.len());
                    *projection = projection.clone().alias(alias);
                }
            }
            if let Expression::Alias(alias) = projection {
                if !contains_call(&alias.this)? {
                    context_aliases.insert(
                        self.signature(alias.this.clone())?,
                        alias.alias.name.clone(),
                    );
                }
            }
        }
        let mut having_calls = HashSet::new();
        if let Some(having) = &select.having {
            map_columns(&mut encode(&having.this)?, &mut |node| {
                if let Some((None, name)) = reference(&node) {
                    if scope_calls.contains(&name) {
                        having_calls.insert(name);
                    }
                }
                Ok(node)
            })?;
        }
        let no_aliases = HashMap::new();
        let mut replacements = HashMap::new();
        for name in &scope_calls {
            let call = &self.calls[name];
            let (model, alias, measure) = self
                .resolve(&call.argument, &sources, false)?
                .ok_or_else(|| {
                    SqlctxError::YardstickBinding(format!(
                        "Unknown Yardstick measure {}",
                        self.sql(&call.argument).unwrap_or_default()
                    ))
                })?;
            let mut correlated = false;
            let mut expression = self.measure(
                &model,
                &alias,
                &measure,
                call,
                &groups,
                select.where_clause.as_ref(),
                single,
                if having_calls.contains(name) {
                    &no_aliases
                } else {
                    &context_aliases
                },
                &mut HashSet::new(),
                &mut correlated,
            )?;
            if scalar_aggregate && correlated {
                // Preserve Yardstick's maximum across correlated contexts,
                // including LIST values that MAX(subquery) cannot bind in 2.0.
                expression =
                    self.expression(&format!("LIST_MAX(LIST({}))", self.sql(&expression)?))?;
            }
            replacements.insert(name.clone(), expression);
        }
        let mut value = encode(&select)?;
        map_columns(&mut value, &mut |node| {
            if let Some((None, name)) = reference(&node) {
                if let Some(replacement) = replacements.get(&name) {
                    return Ok(replacement.clone());
                }
            }
            Ok(node)
        })?;
        select = decode(value)?;
        if scalar_aggregate {
            // DISTINCT drops the global aggregate row on an empty input.
            let parsed = parse_sql_with_dialect("SELECT 1 GROUP BY ()", DialectType::DuckDB)?;
            let Expression::Select(global) = &parsed[0] else {
                unreachable!()
            };
            select.group_by = global.group_by.clone();
        }
        let aliases = select
            .expressions
            .iter()
            .filter_map(|projection| {
                if let Expression::Alias(alias) = projection {
                    Some((alias.alias.name.clone(), alias.this.clone()))
                } else {
                    None
                }
            })
            .collect::<HashMap<_, _>>();
        if let Some(order) = &mut select.order_by {
            for ordered in &mut order.expressions {
                if reference(&ordered.this).is_some_and(|(_, name)| aliases.contains_key(&name)) {
                    continue;
                }
                let mut value = encode(&ordered.this)?;
                map_columns(&mut value, &mut |node| {
                    if let Some((None, name)) = reference(&node) {
                        if let Some(expression) = aliases.get(&name) {
                            return Ok(expression.clone());
                        }
                    }
                    Ok(node)
                })?;
                ordered.this = decode(value)?;
            }
        }
        // Rewrite only current source relations; already-lowered nested queries
        // retain their independently bound contexts.
        if let Some(from) = &mut select.from {
            for source in &mut from.expressions {
                self.source(source)?;
            }
        }
        for join in &mut select.joins {
            self.source(&mut join.this)?;
        }
        Ok(select)
    }

    fn source(&self, source: &mut Expression) -> Result<()> {
        let Some((name, alias)) = table_name_and_alias(source) else {
            return Ok(());
        };
        let Some(model) = self.rewriter.graph.get_model(&name) else {
            return Ok(());
        };
        let source_sql = model
            .sql
            .as_ref()
            .map(|sql| format!("({sql})"))
            .unwrap_or_else(|| model.table_name().to_owned());
        let alias = Identifier::new(alias.unwrap_or(name));
        let alias_sql = self.sql(&Expression::Identifier(alias))?;
        let parsed = parse_sql_with_dialect(
            &format!("SELECT * FROM {source_sql} AS {alias_sql}"),
            DialectType::DuckDB,
        )?;
        let Expression::Select(select) = &parsed[0] else {
            unreachable!()
        };
        *source = select.from.as_ref().unwrap().expressions[0].clone();
        Ok(())
    }

    fn table_function(&mut self, source: &mut Expression) -> Result<()> {
        if let Expression::Alias(alias) = source {
            return self.table_function(&mut alias.this);
        }
        let Expression::Function(function) = source else {
            return Ok(());
        };
        if !function.name.eq_ignore_ascii_case("yardstick") {
            return Ok(());
        }
        if self.rewriter.security_controls {
            return Err(SqlctxError::Security(
                "yardstick() is not supported while semantic security controls are active".into(),
            ));
        }
        let [Expression::Literal(polyglot_sql::expressions::Literal::String(sql))] =
            function.args.as_slice()
        else {
            return Err(invalid("yardstick() requires one literal SQL query"));
        };
        let mut validator = Lowerer {
            rewriter: self.rewriter,
            source_dialect: self.source_dialect,
            calls: HashMap::new(),
            reserved: HashSet::new(),
            changed: false,
        };
        let (normalized, _) = validator.normalize(sql)?;
        let normalized = dialects::query(&normalized, self.source_dialect)?;
        let statements = parse_sql_with_dialect(&normalized, DialectType::DuckDB)?;
        if statements.len() != 1
            || !matches!(
                statements[0],
                Expression::Select(_)
                    | Expression::Union(_)
                    | Expression::Intersect(_)
                    | Expression::Except(_)
            )
        {
            return Err(invalid(
                "yardstick() requires a single read-only SELECT query",
            ));
        }
        let value = encode(&statements[0])?;
        if [
            "insert",
            "update",
            "delete",
            "create",
            "select_into",
            "command",
        ]
        .iter()
        .any(|kind| contains_kind(&value, kind))
        {
            return Err(invalid(
                "yardstick() requires a single read-only SELECT query",
            ));
        }
        let rewritten = self.rewriter.rewrite_with_output_dialect(
            sql,
            self.source_dialect,
            DialectType::DuckDB,
        )?;
        *source = self.expression(&format!("({rewritten})"))?;
        self.changed = true;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn measure(
        &self,
        model_name: &str,
        alias: &str,
        name: &str,
        call: &Call,
        groups: &[Expression],
        outer_where: Option<&Where>,
        single: bool,
        context_aliases: &HashMap<String, String>,
        visiting: &mut HashSet<String>,
        correlated: &mut bool,
    ) -> Result<Expression> {
        let model = self.rewriter.graph.get_model(model_name).unwrap();
        let metric = model
            .get_metric(name)
            .ok_or_else(|| invalid(format!("Unknown measure '{name}'")))?;
        if !visiting.insert(name.to_owned()) {
            return Err(invalid(format!(
                "Circular derived measure reference '{model_name}.{name}'"
            )));
        }
        let raw = metric.sql_expr().replace("{model}", alias);
        let mut expression = self.expression(&raw)?;
        // SemanticInput normalizes a metric without an explicit aggregation to
        // Derived. Preserve the distinction between a formula of measures and
        // complete aggregate/window SQL supplied by the Yardstick adapter.
        if metric.r#type == MetricType::Derived && !has_aggregate_semantics(&encode(&expression)?) {
            let mut value = encode(expression)?;
            map_columns(&mut value, &mut |node| {
                if let Some((table, dependency)) = reference(&node) {
                    if table
                        .as_ref()
                        .is_none_or(|table| table == model_name || table == alias)
                        && model.get_metric(&dependency).is_some()
                    {
                        return self.measure(
                            model_name,
                            alias,
                            &dependency,
                            call,
                            groups,
                            outer_where,
                            single,
                            context_aliases,
                            visiting,
                            correlated,
                        );
                    }
                }
                Ok(node)
            })?;
            visiting.remove(name);
            return decode(value);
        }
        expression = self.remap(
            expression,
            &[model_name, alias, &format!("{model_name}_cte")],
            "_inner",
            true,
        )?;
        let expression_sql = self.sql(&expression)?;
        let aggregate = match &metric.agg {
            Some(crate::core::Aggregation::Expression) | None => expression_sql,
            Some(crate::core::Aggregation::Count)
                if metric.sql.is_none() || metric.sql.as_deref() == Some("*") =>
            {
                "COUNT(*)".into()
            }
            Some(crate::core::Aggregation::CountDistinct) => {
                format!("COUNT(DISTINCT {expression_sql})")
            }
            Some(aggregation) => format!("{}({expression_sql})", aggregation.as_sql()),
        };
        let mut context = Vec::new();
        for group in groups {
            let mut expanded = Vec::new();
            expand_groups(group, false, &mut expanded)?;
            for (group, optional) in expanded {
                let mut columns = Vec::new();
                map_columns(&mut encode(&group)?, &mut |node| {
                    if let Some(reference) = reference(&node) {
                        columns.push(reference);
                    }
                    Ok(node)
                })?;
                if columns.is_empty() {
                    continue;
                }
                let foreign = columns.iter().any(|(table, _)| {
                    table
                        .as_ref()
                        .is_some_and(|table| table != alias && table != model_name)
                });
                if foreign
                    && columns
                        .iter()
                        .any(|(_, name)| model.get_dimension(name).is_none())
                {
                    continue;
                }
                if !single && columns.iter().all(|(table, _)| table.is_none()) {
                    continue;
                }
                let signature = self.signature(group.clone())?;
                let aliases = columns
                    .iter()
                    .filter_map(|(table, _)| table.as_deref())
                    .collect::<Vec<_>>();
                let inner = self.remap(group.clone(), &aliases, "_inner", single)?;
                let mut outer = self.remap(group, &[model_name], alias, single)?;
                // A subtotal omits this expression, rather than grouping its NULL values.
                // GROUPING must refer to the group expression, not its output alias.
                let grouping = optional.then(|| self.sql(&outer)).transpose()?;
                if context.iter().any(|(key, _, _, existing_grouping)| {
                    key == &signature && existing_grouping == &grouping
                }) {
                    continue;
                }
                if let Some(output_alias) = context_aliases.get(&signature).filter(|_| !optional) {
                    // SELECT * imports can expose physical grouping columns
                    // without declaring dimensions. An unqualified output
                    // alias with that same name would bind to the inner source
                    // and turn correlation into a tautology.
                    let unsafe_alias = columns
                        .iter()
                        .any(|(_, name)| name.eq_ignore_ascii_case(output_alias))
                        || model.dimensions.iter().any(|dimension| {
                            dimension.name.eq_ignore_ascii_case(output_alias)
                                && self
                                    .expression(dimension.sql_expr())
                                    .ok()
                                    .and_then(|expression| reference(&expression))
                                    .is_some_and(|(_, name)| {
                                        name.eq_ignore_ascii_case(output_alias)
                                    })
                        });
                    if !unsafe_alias {
                        outer = Expression::Identifier(Identifier::new(output_alias));
                    }
                }
                context.push((signature, self.sql(&inner)?, self.sql(&outer)?, grouping));
            }
        }
        let mut active = context.clone();
        let implicit_visible = call.visible && call.modifiers.is_empty();
        let mut ambient = HashMap::new();
        if let Some(predicate) = outer_where {
            self.fixed_context(&predicate.this, &mut ambient)?;
        }
        let mut filters: Vec<(Expression, HashSet<String>)> = Vec::new();
        let mut set_predicates = std::collections::BTreeMap::new();
        let mut set_bindings = HashMap::new();
        let mut visible = implicit_visible;
        let mut has_all = false;
        let mut has_global_all = false;
        let mut has_set = false;
        if implicit_visible {
            if let Some(predicate) = outer_where {
                filters.push((
                    self.remap(
                        predicate.this.clone(),
                        &[model_name, alias],
                        "_inner",
                        single,
                    )?,
                    HashSet::new(),
                ));
            }
        }
        for modifier in &call.modifiers {
            let stream = tokens(modifier, self.source_dialect)?;
            let Some(head) = stream.first() else { continue };
            let body = modifier[head.span.end..].trim();

            let current = || -> Result<HashMap<String, Expression>> {
                let mut effective = ambient.clone();
                for (filter, removed) in &filters {
                    let mut fixed = HashMap::new();
                    self.fixed_context(filter, &mut fixed)?;
                    for (key, expr) in fixed {
                        if !removed.contains(&key) {
                            effective.insert(key, expr);
                        }
                    }
                }
                for (key, _, outer, _) in &active {
                    effective.insert(key.clone(), self.expression(outer)?);
                }
                effective.extend(set_bindings.clone());
                Ok(effective)
            };

            match head.text.to_ascii_uppercase().as_str() {
                "ALL" if body.is_empty() => {
                    has_all = true;
                    has_global_all = true;
                    active.clear();
                    set_predicates.clear();
                    set_bindings.clear();
                    filters.clear();
                    ambient.clear();
                    visible = false;
                }
                "ALL" => {
                    has_all = true;
                    for target in self.all_targets(body)? {
                        let key = self.signature(target)?;
                        active.retain(|(signature, _, _, _)| signature != &key);
                        set_predicates.remove(&key);
                        set_bindings.remove(&key);
                        ambient.remove(&key);
                        for (_, removed) in &mut filters {
                            removed.insert(key.clone());
                        }
                    }
                }
                "VISIBLE" => {
                    visible = true;
                    if let Some(predicate) = outer_where {
                        filters.push((
                            self.remap(
                                predicate.this.clone(),
                                &[model_name, alias],
                                "_inner",
                                single,
                            )?,
                            HashSet::new(),
                        ));
                    }
                }
                "WHERE" => {
                    let (expression, bindings) = self.current(body, &current()?)?;
                    active.clear();
                    set_predicates.clear();
                    set_bindings.clear();
                    filters.clear();
                    ambient.clear();
                    visible = false;
                    // Apply outer bindings only after mapping the authored filter
                    // columns to the measure's source.
                    let predicate =
                        self.remap(expression, &[model_name, alias], "_inner", single)?;
                    filters.push((self.replace_current(predicate, &bindings)?, HashSet::new()));
                }
                "SET" => {
                    has_set = true;
                    let (direct, empty) = self.current(body, &HashMap::new())?;
                    let direct = self.replace_current(direct, &empty)?;
                    if let Expression::Eq(eq) = direct {
                        let key = self.signature(eq.left.clone())?;
                        if let Some((_, dimension)) = reference(&eq.left) {
                            let mut self_reference = false;
                            map_columns(&mut encode(eq.right)?, &mut |node| {
                                self_reference |= self.signature(node.clone())? == key;
                                Ok(node)
                            })?;
                            if self_reference
                                && !context.iter().any(|(signature, _, _, _)| signature == &key)
                            {
                                return Err(SqlctxError::YardstickBinding(format!(
                                    "AT ({modifier}) references {dimension}, but the query does not group by {dimension}. Add {dimension} to SELECT/GROUP BY or use a constant SET value."
                                )));
                            }
                        }
                    }

                    let (expression, bindings) = self.current(body, &current()?)?;
                    let key = match &expression {
                        Expression::Eq(eq) => self.signature(eq.left.clone())?,
                        Expression::In(set) => self.signature(set.this.clone())?,
                        _ => return Err(invalid(format!("Unsupported SET modifier: {modifier}"))),
                    };
                    active.retain(|(signature, _, _, _)| signature != &key);
                    set_bindings.remove(&key);
                    ambient.remove(&key);
                    for (_, removed) in &mut filters {
                        removed.insert(key.clone());
                    }
                    match expression {
                        Expression::Eq(eq) => {
                            let left =
                                self.remap(eq.left, &[model_name, alias], "_inner", single)?;
                            let mut right = encode(eq.right)?;
                            map_columns(&mut right, &mut |node| {
                                if let Some((table, name)) = reference(&node) {
                                    if table.as_deref().map_or(single, |table| {
                                        table == alias || table == model_name
                                    }) {
                                        if let Some(dimension) = model.get_dimension(&name) {
                                            let expression = self.expression(
                                                &dimension.sql_expr().replace("{model}", alias),
                                            )?;
                                            return self.remap(
                                                expression,
                                                &[model_name, alias],
                                                alias,
                                                true,
                                            );
                                        }
                                    }
                                }
                                Ok(node)
                            })?;
                            let right = self.remap(decode(right)?, &[model_name], alias, single)?;
                            let right = self.replace_current(right, &bindings)?;
                            set_bindings.insert(key.clone(), right.clone());
                            set_predicates.insert(
                                key,
                                format!(
                                    "({}) IS NOT DISTINCT FROM ({})",
                                    self.sql(&left)?,
                                    self.sql(&right)?
                                ),
                            );
                        }
                        Expression::In(in_expr) => {
                            let expression = self.remap(
                                Expression::In(in_expr),
                                &[model_name, alias],
                                "_inner",
                                single,
                            )?;
                            let expression = self.replace_current(expression, &bindings)?;
                            set_predicates.insert(key, self.sql(&expression)?);
                        }
                        _ => unreachable!(),
                    }
                }
                _ => return Err(invalid(format!("Unsupported AT modifier: {modifier}"))),
            }
        }
        let mut base = Vec::new();
        for (filter, removed) in filters {
            if let Some(predicate) = self.project_at_filter(filter, &removed)? {
                base.push(self.sql(&predicate)?);
            }
        }
        let no_replacements = std::collections::BTreeMap::new();
        // Like upstream, a global ALL is a deliberate reset of the visible
        // filters, even if a later clause adds an explicit replacement.
        let warning_modifier = if visible {
            None
        } else if has_all {
            Some("ALL")
        } else if has_set {
            Some("SET")
        } else {
            None
        };
        self.warn_dropped_filters(
            model,
            alias,
            name,
            outer_where,
            &active,
            if has_global_all { &[] } else { &base },
            if has_global_all {
                &no_replacements
            } else {
                &set_predicates
            },
            warning_modifier,
        )?;
        for filter in &metric.filters {
            let expression = self.expression(&filter.replace("{model}", "_inner"))?;
            let expression = self.remap(expression, &[model_name, alias], "_inner", single)?;
            base.push(self.sql(&expression)?);
        }
        let mut post = set_predicates.into_values().collect::<Vec<_>>();
        post.extend(active.into_iter().map(|(_, inner, outer, grouping)| {
            let equality = format!("({inner}) IS NOT DISTINCT FROM ({outer})");
            match grouping {
                Some(group) => format!("GROUPING({group}) = 1 OR ({equality})"),
                None => equality,
            }
        }));
        let source = model
            .sql
            .as_ref()
            .map(|sql| format!("({sql})"))
            .unwrap_or_else(|| model.table_name().to_owned());
        let mut predicates = base.clone();
        predicates.extend(post.clone());
        for predicate in &predicates {
            *correlated |= crate::core::outer_semantic_column_references(predicate)?
                .iter()
                .any(|column| {
                    column
                        .model
                        .as_deref()
                        .is_some_and(|table| table != "_inner")
                });
        }
        let sql = if contains_kind(&encode(&expression)?, "window")
            || contains_kind(&encode(&expression)?, "window_function")
        {
            let message = name.replace('\'', "''");
            format!(
                "(SELECT CASE WHEN COUNT(*) = 0 THEN NULL WHEN COUNT(DISTINCT __ys_window_value) = 1 THEN MIN(__ys_window_value) ELSE error('Window measure {message} returned multiple values for the evaluation context') END FROM (SELECT _inner.*, {aggregate} AS __ys_window_value FROM {source} AS _inner{}) AS _inner{})",
                predicate_clause(&base),
                predicate_clause(&post)
            )
        } else {
            format!(
                "(SELECT {aggregate} FROM {source} AS _inner{})",
                predicate_clause(&predicates)
            )
        };
        visiting.remove(name);
        self.expression(&sql)
    }

    #[allow(clippy::too_many_arguments)]
    fn warn_dropped_filters(
        &self,
        model: &crate::core::Model,
        alias: &str,
        name: &str,
        outer_where: Option<&Where>,
        active: &[(String, String, String, Option<String>)],
        where_predicates: &[String],
        set_predicates: &std::collections::BTreeMap<String, String>,
        warning_modifier: Option<&str>,
    ) -> Result<()> {
        let (Some(outer_where), Some(warning_modifier)) = (outer_where, warning_modifier) else {
            return Ok(());
        };
        let mut source_names = model
            .dimensions
            .iter()
            .map(|dimension| dimension.name.to_lowercase())
            .collect::<HashSet<_>>();
        for dimension in &model.dimensions {
            let expression = self.expression(&dimension.sql_expr().replace("{model}", alias))?;
            map_columns(&mut encode(expression)?, &mut |node| {
                if let Some((_, name)) = reference(&node) {
                    source_names.insert(name.to_lowercase());
                }
                Ok(node)
            })?;
        }
        let source_columns = |expression: &Expression| -> Result<HashSet<String>> {
            let mut columns = HashSet::new();
            map_columns(&mut encode(expression)?, &mut |node| {
                if let Some((table, name)) = reference(&node) {
                    if source_names.contains(&name.to_lowercase())
                        && table.as_ref().is_none_or(|table| {
                            table.eq_ignore_ascii_case(alias)
                                || table.eq_ignore_ascii_case(&model.name)
                                || table.eq_ignore_ascii_case("_inner")
                        })
                    {
                        columns.insert(name.to_lowercase());
                    }
                }
                Ok(node)
            })?;
            Ok(columns)
        };
        let outer_columns = source_columns(&outer_where.this)?;
        if outer_columns.is_empty() {
            return Ok(());
        }
        let mut encoded = HashSet::new();
        for (_, inner, _, _) in active {
            let expression = self.expression(inner)?;
            if reference(&expression).is_some() {
                encoded.extend(source_columns(&expression)?);
            }
        }
        for predicate in where_predicates {
            encoded.extend(source_columns(&self.expression(predicate)?)?);
        }
        let mut outer_signatures = HashSet::new();
        self.collect_signatures(&encode(&outer_where.this)?, &mut outer_signatures)?;
        for predicate in set_predicates.values() {
            let expression = self.expression(predicate)?;
            let mut target = match expression {
                Expression::NullSafeEq(eq) | Expression::Eq(eq) => eq.left,
                Expression::In(in_expr) => in_expr.this,
                _ => continue,
            };
            while let Expression::Paren(paren) = target {
                target = paren.this;
            }
            if reference(&target).is_some()
                || outer_signatures.contains(&self.signature(target.clone())?)
            {
                encoded.extend(source_columns(&target)?);
            }
        }
        let mut dropped = outer_columns
            .difference(&encoded)
            .cloned()
            .collect::<Vec<_>>();
        dropped.sort();
        if !dropped.is_empty() {
            self.rewriter.warnings.borrow_mut().push(format!(
                "AT ({warning_modifier} ...) on AGGREGATE({name}) does not preserve outer WHERE filter(s) on ungrouped dimension(s): {}. Add the filter dimension(s) to SELECT/GROUP BY or use an explicit AT modifier that encodes the intended denominator.", dropped.join(", ")
            ));
        }
        Ok(())
    }

    fn collect_signatures(&self, value: &Value, signatures: &mut HashSet<String>) -> Result<()> {
        match value {
            Value::Object(fields) => {
                if fields.len() == 1
                    && (fields.contains_key("select") || fields.contains_key("subquery"))
                {
                    return Ok(());
                }
                if fields.len() == 1 {
                    if let Ok(expression) = decode::<Expression>(value.clone()) {
                        signatures.insert(self.signature(expression)?);
                    }
                }
                for child in fields.values() {
                    self.collect_signatures(child, signatures)?;
                }
            }
            Value::Array(children) => {
                for child in children {
                    self.collect_signatures(child, signatures)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn fixed_context(
        &self,
        expression: &Expression,
        fixed: &mut HashMap<String, Expression>,
    ) -> Result<()> {
        match expression {
            Expression::Paren(paren) => self.fixed_context(&paren.this, fixed)?,
            Expression::And(and) => {
                self.fixed_context(&and.left, fixed)?;
                self.fixed_context(&and.right, fixed)?;
            }
            Expression::Eq(eq) => {
                if matches!(eq.right, Expression::Literal(_)) {
                    fixed.insert(self.signature(eq.left.clone())?, eq.right.clone());
                }
                if matches!(eq.left, Expression::Literal(_)) {
                    fixed.insert(self.signature(eq.right.clone())?, eq.left.clone());
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn current(
        &self,
        sql: &str,
        context: &HashMap<String, Expression>,
    ) -> Result<(Expression, HashMap<String, Expression>)> {
        let stream = tokens(sql, self.source_dialect)?;
        let mut output = String::new();
        let mut bindings = HashMap::new();
        let mut cursor = 0;
        let mut index = 0;
        while index + 1 < stream.len() {
            if !matches!(
                stream[index].token_type,
                TokenType::Current | TokenType::Var
            ) || !stream[index].text.eq_ignore_ascii_case("CURRENT")
            {
                index += 1;
                continue;
            }
            let start = index + 1;
            let mut end = start;
            let parenthesized = stream[start].token_type == TokenType::LParen;
            if parenthesized {
                end = matching(&stream, start)?;
            } else if stream
                .get(start + 1)
                .is_some_and(|token| token.token_type == TokenType::LParen)
            {
                end = matching(&stream, start + 1)?;
            } else {
                while stream
                    .get(end + 1)
                    .is_some_and(|token| token.token_type == TokenType::Dot)
                    && end + 2 < stream.len()
                {
                    end += 2;
                }
            }
            let target = if parenthesized {
                &sql[stream[start].span.end..stream[end].span.start]
            } else {
                &sql[stream[start].span.start..stream[end].span.end]
            };
            output.push_str(&sql[cursor..stream[index].span.start]);
            let key = self.signature(self.authored_expression(target)?)?;
            let placeholder = (bindings.len()..)
                .map(|n| format!("__ys_at_current_{n}"))
                .find(|name| {
                    !bindings.contains_key(name)
                        && !stream
                            .iter()
                            .any(|token| token.text.eq_ignore_ascii_case(name))
                })
                .expect("unbounded placeholder iterator");
            output.push_str(&placeholder);
            let value = context
                .get(&key)
                .cloned()
                .map(Ok)
                .unwrap_or_else(|| self.expression("NULL"))?;
            bindings.insert(placeholder, value);
            cursor = stream[end].span.end;
            index = end + 1;
        }
        output.push_str(&sql[cursor..]);
        Ok((self.authored_expression(&output)?, bindings))
    }

    fn replace_current(
        &self,
        expression: Expression,
        bindings: &HashMap<String, Expression>,
    ) -> Result<Expression> {
        let mut value = encode(&expression)?;
        map_columns(&mut value, &mut |node| {
            if let Some((_, name)) = reference(&node) {
                if let Some(bound) = bindings.get(&name) {
                    return Ok(bound.clone());
                }
            }
            Ok(node)
        })?;
        decode(value)
    }

    fn repeatable_at_filter(expression: &Expression) -> bool {
        // The offline semantic graph cannot prove the stability of unknown
        // functions or subqueries. Normalize only known repeatable operators.
        match expression {
            Expression::Column(_)
            | Expression::Literal(_)
            | Expression::Null(_)
            | Expression::Boolean(_) => true,
            Expression::And(binary)
            | Expression::Or(binary)
            | Expression::Eq(binary)
            | Expression::Neq(binary)
            | Expression::Lt(binary)
            | Expression::Lte(binary)
            | Expression::Gt(binary)
            | Expression::Gte(binary)
            | Expression::NullSafeEq(binary)
            | Expression::NullSafeNeq(binary) => {
                Self::repeatable_at_filter(&binary.left)
                    && Self::repeatable_at_filter(&binary.right)
            }
            Expression::Paren(paren) => Self::repeatable_at_filter(&paren.this),
            Expression::Not(unary) => Self::repeatable_at_filter(&unary.this),
            Expression::IsNull(is_null) => Self::repeatable_at_filter(&is_null.this),
            _ => false,
        }
    }

    fn normalize_at_filter(expression: Expression) -> Result<Expression> {
        use polyglot_sql::expressions::{BinaryOp, UnaryOp};

        fn and(left: Expression, right: Expression) -> Expression {
            Expression::And(Box::new(BinaryOp::new(left, right)))
        }
        fn or(left: Expression, right: Expression) -> Expression {
            Expression::Or(Box::new(BinaryOp::new(left, right)))
        }
        fn not(inner: Expression) -> Expression {
            Expression::Not(Box::new(UnaryOp::new(inner)))
        }
        fn conjuncts(expression: Expression, output: &mut Vec<Expression>) {
            match expression {
                Expression::And(binary) => {
                    conjuncts(binary.left, output);
                    conjuncts(binary.right, output);
                }
                Expression::Paren(paren) => conjuncts(paren.this, output),
                expression => output.push(expression),
            }
        }
        Ok(match expression {
            Expression::Paren(paren) => Self::normalize_at_filter(paren.this)?,
            Expression::Not(unary) => match Self::normalize_at_filter(unary.this)? {
                Expression::Or(binary) => and(
                    Self::normalize_at_filter(not(binary.left))?,
                    Self::normalize_at_filter(not(binary.right))?,
                ),
                Expression::And(binary) => or(
                    Self::normalize_at_filter(not(binary.left))?,
                    Self::normalize_at_filter(not(binary.right))?,
                ),
                Expression::Not(inner) => inner.this,
                inner => not(inner),
            },
            Expression::And(binary) => and(
                Self::normalize_at_filter(binary.left)?,
                Self::normalize_at_filter(binary.right)?,
            ),
            Expression::Or(binary) => {
                let left = Self::normalize_at_filter(binary.left)?;
                let right = Self::normalize_at_filter(binary.right)?;
                let mut left_terms = Vec::new();
                let mut right_terms = Vec::new();
                conjuncts(left.clone(), &mut left_terms);
                conjuncts(right.clone(), &mut right_terms);
                let mut common = Vec::new();
                left_terms.retain(|candidate| {
                    if Self::repeatable_at_filter(candidate) {
                        if let Some(index) = right_terms.iter().position(|term| term == candidate) {
                            common.push(candidate.clone());
                            right_terms.remove(index);
                            return false;
                        }
                    }
                    true
                });
                let Some(shared) = common.into_iter().reduce(and) else {
                    return Ok(or(left, right));
                };
                match (
                    left_terms.into_iter().reduce(and),
                    right_terms.into_iter().reduce(and),
                ) {
                    (Some(left), Some(right)) => and(shared, or(left, right)),
                    // (C AND P) OR C = C in SQL's three-valued logic.
                    _ => shared,
                }
            }
            expression => expression,
        })
    }

    fn project_at_filter(
        &self,
        expression: Expression,
        removed: &HashSet<String>,
    ) -> Result<Option<Expression>> {
        if removed.is_empty() {
            return Ok(Some(expression));
        }
        let dependencies = |expression: &Expression| -> Result<(bool, bool)> {
            let mut retained = false;
            let mut dropped = false;
            map_columns(&mut encode(expression)?, &mut |node| {
                if let Some((table, _)) = reference(&node) {
                    let own = table.as_deref().is_none_or(|table| table == "_inner");
                    if own && removed.contains(&self.signature(node.clone())?) {
                        dropped = true;
                    } else {
                        retained = true;
                    }
                }
                Ok(node)
            })?;
            Ok((dropped, retained))
        };
        let (dropped, _) = dependencies(&expression)?;
        if !dropped {
            return Ok(Some(expression)); // Preserve unrelated predicate shape and evaluation.
        }
        let expression = match expression {
            Expression::Paren(paren) => return self.project_at_filter(paren.this, removed),
            Expression::And(mut and) => {
                return match (
                    self.project_at_filter(and.left.clone(), removed)?,
                    self.project_at_filter(and.right.clone(), removed)?,
                ) {
                    (Some(left), Some(right)) => {
                        and.left = left;
                        and.right = right;
                        Ok(Some(Expression::And(and)))
                    }
                    (left, right) => Ok(left.or(right)),
                };
            }
            expression => expression,
        };
        let value = encode(&expression)?;
        let boolean_complex = contains_kind(&value, "and") || contains_kind(&value, "or");
        if boolean_complex && !Self::repeatable_at_filter(&expression) {
            return Err(invalid(
                "AT dimension replacement cannot safely normalize a volatile or unproven predicate; use an explicit AT WHERE predicate",
            ));
        }
        let normalized = Self::normalize_at_filter(expression)?;
        if matches!(normalized, Expression::And(_)) {
            return self.project_at_filter(normalized, removed);
        }
        match dependencies(&normalized)? {
            (true, true) => Err(invalid(
                "mixed VISIBLE predicate: use an explicit AT context",
            )),
            (true, false) => Ok(None),
            _ => Ok(Some(normalized)),
        }
    }

    fn all_targets(&self, sql: &str) -> Result<Vec<Expression>> {
        let stream = tokens(sql, self.source_dialect)?;
        // Space-separated targets are an extension. Arithmetic remains a single
        // ordinary SQL expression and is parsed without splitting it.
        if stream.iter().any(|token| {
            ["+", "-", "*", "/", "=", "AND", "OR"]
                .contains(&token.text.to_ascii_uppercase().as_str())
        }) {
            return Ok(vec![self.authored_expression(sql)?]);
        }
        let mut targets = Vec::new();
        let mut index = 0;
        while index < stream.len() {
            if stream[index].token_type == TokenType::Comma {
                index += 1;
                continue;
            }
            let start = index;
            if stream[index].token_type == TokenType::LParen {
                index = matching(&stream, index)?;
            } else if stream
                .get(index + 1)
                .is_some_and(|token| token.token_type == TokenType::LParen)
            {
                index = matching(&stream, index + 1)?;
            } else {
                while stream
                    .get(index + 1)
                    .is_some_and(|token| token.token_type == TokenType::Dot)
                    && index + 2 < stream.len()
                {
                    index += 2;
                }
            }
            targets.push(
                self.authored_expression(&sql[stream[start].span.start..stream[index].span.end])?,
            );
            index += 1;
        }
        Ok(targets)
    }
}

fn predicate_clause(predicates: &[String]) -> String {
    if predicates.is_empty() {
        String::new()
    } else {
        format!(
            " WHERE {}",
            predicates
                .iter()
                .map(|predicate| format!("({predicate})"))
                .collect::<Vec<_>>()
                .join(" AND ")
        )
    }
}

fn contains_kind(value: &Value, kind: &str) -> bool {
    match value {
        Value::Object(fields) => {
            fields.contains_key(kind) || fields.values().any(|child| contains_kind(child, kind))
        }
        Value::Array(children) => children.iter().any(|child| contains_kind(child, kind)),
        _ => false,
    }
}

fn has_aggregate_semantics(value: &Value) -> bool {
    // These are Polyglot AST variants, not text matches: an aggregate name in a
    // string, comment, or column cannot change the measure's evaluation grain.
    const AGGREGATES: &[&str] = &[
        "aggregate_function",
        "count",
        "sum",
        "avg",
        "min",
        "max",
        "median",
        "mode",
        "stddev",
        "stddev_pop",
        "stddev_samp",
        "variance",
        "var_pop",
        "var_samp",
        "approx_distinct",
        "array_agg",
        "group_concat",
        "string_agg",
        "list_agg",
        "percentile",
        "percentile_cont",
        "percentile_disc",
        "approx_percentile",
        "quantile",
        "approx_quantile",
        "approx_quantiles",
        "within_group",
        "window",
        "window_function",
    ];
    match value {
        Value::Object(fields) => {
            (fields.len() == 1 && fields.keys().any(|key| AGGREGATES.contains(&key.as_str())))
                || fields.values().any(has_aggregate_semantics)
        }
        Value::Array(children) => children.iter().any(has_aggregate_semantics),
        _ => false,
    }
}

fn expand_groups(
    expression: &Expression,
    optional: bool,
    output: &mut Vec<(Expression, bool)>,
) -> Result<()> {
    // The pinned parser represents inline ROLLUP/CUBE as ordinary functions,
    // but WITH ROLLUP/CUBE as the typed variants handled below.
    if let Expression::Function(function) = expression {
        if ["ROLLUP", "CUBE", "GROUPING SETS"]
            .iter()
            .any(|name| function.name.eq_ignore_ascii_case(name))
        {
            for child in &function.args {
                expand_groups(child, true, output)?;
            }
            return Ok(());
        }
    }
    let value = encode(expression)?;
    let Some(fields) = value.as_object() else {
        return Ok(());
    };
    if let Some((kind, group)) = ["rollup", "cube", "grouping_sets", "tuple"]
        .into_iter()
        .find_map(|kind| fields.get(kind).map(|group| (kind, group)))
    {
        if let Some(children) = group.get("expressions").and_then(Value::as_array) {
            for child in children {
                expand_groups(&decode(child.clone())?, optional || kind != "tuple", output)?;
            }
            return Ok(());
        }
    }
    output.push((expression.clone(), optional));
    Ok(())
}

fn split_modifiers(sql: &str, dialect: DialectType) -> Result<Vec<String>> {
    let stream = tokens(sql, dialect)?;
    let mut starts = vec![0];
    let mut depth = 0;
    let head = |token: &Token| {
        matches!(
            token.token_type,
            TokenType::All | TokenType::Set | TokenType::Where
        ) || (token.token_type == TokenType::Var && token.text.eq_ignore_ascii_case("VISIBLE"))
    };
    for (index, token) in stream.iter().enumerate() {
        match token.token_type {
            TokenType::LParen => depth += 1,
            TokenType::RParen => depth -= 1,
            _ => {}
        }
        if index == 0 || depth != 0 || !head(token) {
            continue;
        }
        // VISIBLE is also a valid unquoted column name; it can only start a
        // context modifier at the end or just before the next modifier.
        if token.text.eq_ignore_ascii_case("VISIBLE") {
            let preceding = &stream[index - 1];
            if [
                ".", "=", "!=", "<>", "<", ">", "<=", ">=", "+", "-", "*", "/", "%", "AND", "OR",
                "NOT", "IS", "LIKE", "ILIKE", "WHERE", "SET",
            ]
            .iter()
            .any(|word| preceding.text.eq_ignore_ascii_case(word))
                || !stream
                    .get(index + 1)
                    .is_none_or(|next| head(next) || next.token_type == TokenType::Eof)
            {
                continue;
            }
        }
        starts.push(token.span.start);
    }
    starts.push(sql.len());
    Ok(starts
        .windows(2)
        .map(|range| sql[range[0]..range[1]].trim().to_owned())
        .filter(|part| !part.is_empty())
        .collect())
}

impl QueryRewriter<'_> {
    pub(super) fn rewrite_yardstick(
        &self,
        sql: &str,
        input: DialectType,
        output: DialectType,
    ) -> Result<Option<String>> {
        let mut lowerer = Lowerer {
            rewriter: self,
            source_dialect: input,
            calls: HashMap::new(),
            reserved: HashSet::new(),
            changed: false,
        };
        let (normalized, semantic) = lowerer.normalize(sql)?;
        if lowerer.calls.is_empty()
            && !tokens(sql, input)?
                .iter()
                .any(|token| token.text.eq_ignore_ascii_case("yardstick"))
            && !self.graph.models().any(|model| {
                model
                    .metadata
                    .as_ref()
                    .is_some_and(|metadata| metadata.get("yardstick").is_some())
            })
        {
            if semantic {
                return self
                    .rewrite_with_output_dialect(&normalized, input, output)
                    .map(Some);
            }
            return Ok(None);
        }
        let normalized = dialects::query(&normalized, input)?;
        let mut statements = parse_sql_with_dialect(&normalized, DialectType::DuckDB)?;
        if statements.len() != 1 {
            return Err(invalid("Yardstick requires a single statement"));
        }
        let statement = self.rename_user_ctes(statements.remove(0))?;
        let mut value = encode(statement)?;
        lowerer.scopes(&mut value)?;
        if !lowerer.changed {
            if semantic {
                return self
                    .rewrite_with_output_dialect(&normalized, DialectType::DuckDB, output)
                    .map(Some);
            }
            return Ok(None);
        }
        dialects::emit(decode(value)?, DialectType::DuckDB, output).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Metric, Model, SemanticGraph};

    #[test]
    fn wildcard_import_grouping_retains_outer_column_qualification() {
        let mut model = Model::new("orders_v", "id")
            .with_table("raw_orders")
            .with_metric(Metric::sum("revenue", "amount"));
        model.metadata = Some(serde_json::json!({"yardstick": {}}));
        let mut graph = SemanticGraph::new();
        graph.add_model(model).unwrap();
        for grouping in [
            "o.product",
            "ROLLUP(o.product)",
            "CUBE(o.product)",
            "GROUPING SETS ((o.product), ())",
        ] {
            let sql = QueryRewriter::new(&graph).rewrite(&format!("SELECT o.product, AGGREGATE(o.revenue) AS total FROM orders_v AS o GROUP BY {grouping}")).unwrap();
            assert!(
                sql.contains("(_inner.product) IS NOT DISTINCT FROM (o.product)"),
                "{sql}"
            );
            assert!(!sql.contains("IS NOT DISTINCT FROM (product)"), "{sql}");
            assert_eq!(
                sql.contains("GROUPING(o.product) = 1"),
                grouping != "o.product",
                "{sql}"
            );
        }
    }

    #[test]
    fn grouping_sets_keep_visible_filters_outside_grouping_guard() {
        let mut model = Model::new("orders_v", "id")
            .with_table("raw_orders")
            .with_metric(Metric::sum("revenue", "amount"));
        model.metadata = Some(serde_json::json!({"yardstick": {}}));
        let mut graph = SemanticGraph::new();
        graph.add_model(model).unwrap();
        let rewriter = QueryRewriter::new(&graph);
        for grouping in [
            "ROLLUP(o.product, o.year)",
            "CUBE(o.product, o.year)",
            "GROUPING SETS ((o.product), (o.year), ())",
        ] {
            let sql = rewriter.rewrite(&format!(
                "SELECT o.product, o.year, COUNT(*) AS c, AGGREGATE(o.revenue) AS vis, o.revenue AS plain FROM orders_v o WHERE o.customer <> 'Bob' GROUP BY {grouping}"
            )).unwrap();
            assert_eq!(sql.matches("GROUPING(o.product) = 1").count(), 2, "{sql}");
            assert_eq!(sql.matches("GROUPING(o.year) = 1").count(), 2, "{sql}");
            assert_eq!(sql.matches("_inner.customer <> 'Bob'").count(), 1, "{sql}");
            assert!(
                sql.contains("(_inner.product) IS NOT DISTINCT FROM (o.product)"),
                "{sql}"
            );
        }
    }

    #[test]
    fn optional_grouping_retains_relation_identity_and_ignores_output_aliases() {
        let mut model = Model::new("orders_v", "id")
            .with_table("raw_orders")
            .with_dimension(crate::core::Dimension::categorical("year"))
            .with_metric(Metric::count("orders"));
        model.metadata = Some(serde_json::json!({"yardstick": {}}));
        let mut graph = SemanticGraph::new();
        graph.add_model(model).unwrap();
        let rewriter = QueryRewriter::new(&graph);
        let sql = rewriter.rewrite(
            "SELECT o.year AS order_year, y.year AS joined_year, AGGREGATE(o.orders) FROM orders_v o JOIN (SELECT DISTINCT year FROM raw_orders) y ON o.year = y.year GROUP BY GROUPING SETS ((o.year), (y.year), ())"
        ).unwrap();
        for qualifier in ["o", "y"] {
            assert!(sql.contains(&format!("GROUPING({qualifier}.year) = 1 OR ((_inner.year) IS NOT DISTINCT FROM ({qualifier}.year))")), "{sql}");
        }
    }
    fn at_graph() -> SemanticGraph {
        let mut model = Model::new("orders_v", "id")
            .with_table("raw_orders")
            .with_dimension(crate::core::Dimension::categorical("year"))
            .with_dimension(crate::core::Dimension::categorical("region"))
            .with_metric(Metric::sum("revenue", "amount"));
        model.metadata = Some(serde_json::json!({"yardstick": {}}));
        let mut graph = SemanticGraph::new();
        graph.add_model(model).unwrap();
        graph
    }

    #[test]
    fn at_clauses_apply_outer_inward_but_parts_apply_in_written_order() {
        let graph = at_graph();
        let rewriter = QueryRewriter::new(&graph);
        let query = |modifier| {
            rewriter
                .rewrite(&format!(
                    "SELECT AGGREGATE(revenue) {modifier} FROM orders_v WHERE year=2023"
                ))
                .unwrap()
        };
        let compound = query("AT (WHERE region='N' VISIBLE)");
        assert!(compound.contains("_inner.region = 'N'"), "{compound}");
        assert!(compound.contains("_inner.year = 2023"), "{compound}");
        let chained = query("AT (WHERE region='N') AT (VISIBLE)");
        assert!(chained.contains("_inner.region = 'N'"), "{chained}");
        assert!(!chained.contains("_inner.year = 2023"), "{chained}");
        let reset_then_set = query("AT (ALL SET year=2024)");
        assert!(
            reset_then_set.contains("IS NOT DISTINCT FROM (2024)"),
            "{reset_then_set}"
        );
        let set_then_reset = query("AT (SET year=2024 ALL)");
        assert!(!set_then_reset.contains("2024"), "{set_then_reset}");
    }

    #[test]
    fn at_current_uses_prior_binding_and_filter_projection_is_ordered() {
        let graph = at_graph();
        let rewriter = QueryRewriter::new(&graph);
        let sql = rewriter.rewrite(
            "SELECT AGGREGATE(revenue) AT (WHERE year=2024 AND region='N' SET year=CURRENT year-1) FROM orders_v",
        ).unwrap();
        assert!(sql.contains("2024 - 1"), "{sql}");
        assert!(sql.contains("_inner.region = 'N'"), "{sql}");
        assert!(!sql.contains("_inner.year = 2024"), "{sql}");
        let mixed = rewriter.rewrite(
            "SELECT AGGREGATE(revenue) AT (VISIBLE SET year=2024) FROM orders_v WHERE year=2023 OR region='N'",
        ).unwrap_err();
        assert!(
            mixed.to_string().contains("mixed VISIBLE predicate"),
            "{mixed}"
        );
        for word in ["ALL", "SET", "WHERE", "VISIBLE", "CURRENT"] {
            let sql = rewriter
                .rewrite(&format!(
                    "SELECT AGGREGATE(revenue) AT (WHERE region='{word}') FROM orders_v"
                ))
                .unwrap();
            assert!(sql.contains(&format!("_inner.region = '{word}'")), "{sql}");
        }
    }
    #[test]
    fn at_projection_separates_deterministic_predicates_but_not_volatile_disjunctions() {
        let graph = at_graph();
        let rewriter = QueryRewriter::new(&graph);
        let query = |predicate| {
            rewriter.rewrite(&format!(
                "SELECT year, AGGREGATE(revenue) AT (VISIBLE SET year=year-1) FROM orders_v WHERE {predicate} GROUP BY year"
            ))
        };
        for predicate in [
            "NOT (year<>2024 OR region<>'N')",
            "(year=2024 AND region='N') OR (year=2025 AND region='N')",
            "NOT (year<>2024 OR region IS DISTINCT FROM NULL)",
        ] {
            let sql = query(predicate).unwrap();
            assert!(sql.contains("_inner.region"), "{sql}");
            assert!(!sql.contains("_inner.year = 2024"), "{sql}");
        }
        let volatile = query(
            "(year=2024 AND random() > 0.5 AND region='N') OR (year=2025 AND random() > 0.5 AND region='N')",
        ).unwrap_err();
        assert!(
            volatile
                .to_string()
                .contains("volatile or unproven predicate"),
            "{volatile}"
        );
    }
}
