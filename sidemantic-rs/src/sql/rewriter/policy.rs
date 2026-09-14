//! Bind SQL scopes, then secure and compile semantic leaves independently.

use super::*;

pub(super) fn unsupported() -> SidemanticError {
    SidemanticError::UnsupportedSemanticFeatures {
        capabilities: vec!["rewrite.policy_select_shape".into()],
    }
}

/// User CTE names must not capture physical reads introduced by the compiler.
/// Allocate names outside both the input namespace and model source namespace.
struct CteNames {
    reserved: HashSet<String>,
    next: usize,
}

impl CteNames {
    fn new(
        statement: &Expression,
        graph: &SemanticGraph,
        policy_definitions: &str,
    ) -> Result<Self> {
        let mut reserved = HashSet::new();
        for definition in graph
            .models()
            .map(serde_json::to_string)
            .chain(graph.metrics().map(serde_json::to_string))
            .chain(std::iter::once(serde_json::to_string(statement)))
            .chain(std::iter::once(Ok(policy_definitions.to_owned())))
        {
            // Source reads can occur inside trusted SQL definitions as well as
            // model.table. Reserve those tokens and the complete input AST,
            // including children omitted by polyglot's public traversal.
            let definition =
                definition.map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
            reserved.extend(
                definition
                    .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
                    .filter(|token| !token.is_empty())
                    .map(str::to_ascii_lowercase),
            );
        }
        Ok(Self { reserved, next: 0 })
    }

    fn allocate(&mut self) -> Identifier {
        loop {
            let name = format!("__sidemantic_input_cte_{}", self.next);
            self.next += 1;
            if self.reserved.insert(name.clone()) {
                return Identifier::new(name);
            }
        }
    }
}

impl QueryRewriter<'_> {
    /// Reserve trusted source names before a syntax extension lowers its own
    /// semantics. The empty graph makes the shared scope walker rename only.
    pub(super) fn rename_user_ctes(&self, statement: Expression) -> Result<Expression> {
        let definitions = self
            .graph
            .models()
            .map(serde_json::to_string)
            .chain(self.graph.metrics().map(serde_json::to_string))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
        let reserved = format!("{} {}", self.policy_definitions, definitions.join(" "));
        let graph = SemanticGraph::new();
        let rewriter = QueryRewriter {
            graph: &graph,
            query_preparer: None,
            policy_definitions: &reserved,
            rename_only: true,
            security_controls: false,
            warnings: std::cell::RefCell::new(Vec::new()),
        };
        rewriter.rewrite_policy_statement(statement)
    }

    pub(super) fn rewrite_policy_statement(&self, statement: Expression) -> Result<Expression> {
        let mut names = CteNames::new(&statement, self.graph, self.policy_definitions)?;
        self.rewrite_policy_query(statement, &HashMap::new(), &mut names)
    }

    fn rewrite_policy_query(
        &self,
        statement: Expression,
        inherited_ctes: &HashMap<String, Identifier>,
        names: &mut CteNames,
    ) -> Result<Expression> {
        // Set operations own their WITH/ORDER/LIMIT clauses. Rewrite their
        // operands without moving those clauses onto an individual SELECT.
        match statement {
            Expression::Union(mut set) => {
                let ctes = self.rewrite_ctes(&mut set.with, inherited_ctes, names)?;
                set.left = self.rewrite_policy_query(set.left.clone(), &ctes, names)?;
                set.right = self.rewrite_policy_query(set.right.clone(), &ctes, names)?;
                return self.rewrite_set_clauses(Expression::Union(set), &ctes, names);
            }
            Expression::Intersect(mut set) => {
                let ctes = self.rewrite_ctes(&mut set.with, inherited_ctes, names)?;
                set.left = self.rewrite_policy_query(set.left.clone(), &ctes, names)?;
                set.right = self.rewrite_policy_query(set.right.clone(), &ctes, names)?;
                return self.rewrite_set_clauses(Expression::Intersect(set), &ctes, names);
            }
            Expression::Except(mut set) => {
                let ctes = self.rewrite_ctes(&mut set.with, inherited_ctes, names)?;
                set.left = self.rewrite_policy_query(set.left.clone(), &ctes, names)?;
                set.right = self.rewrite_policy_query(set.right.clone(), &ctes, names)?;
                return self.rewrite_set_clauses(Expression::Except(set), &ctes, names);
            }
            Expression::Subquery(mut query) => {
                query.this = self.rewrite_policy_query(query.this, inherited_ctes, names)?;
                return Ok(Expression::Subquery(query));
            }
            Expression::Paren(mut paren) => {
                paren.this = self.rewrite_policy_query(paren.this, inherited_ctes, names)?;
                return Ok(Expression::Paren(paren));
            }
            Expression::Select(_) => {}
            other if self.query_preparer.is_none() => return Ok(other),
            Expression::Values(values) => {
                return self.rewrite_scalar_scopes(
                    Expression::Values(values),
                    inherited_ctes,
                    names,
                )
            }
            _ => return Err(unsupported()),
        }
        let Expression::Select(mut select) = statement else {
            unreachable!()
        };
        let mut with = select.with.take();
        let ctes = self.rewrite_ctes(&mut with, inherited_ctes, names)?;
        if select.into.is_some() || !select.locks.is_empty() {
            return Err(unsupported());
        }
        let semantic_leaf = !self.rename_only
            && select.from.as_ref().is_some_and(|from| {
                from.expressions.first().is_some_and(|source| {
                    matches!(source, Expression::Table(table)
                    if table.schema.is_none() && table.catalog.is_none()
                        && !ctes.contains_key(&table.name.name.to_ascii_lowercase())
                        && (table.name.name.eq_ignore_ascii_case("metrics")
                            || self.graph.get_model(&table.name.name).is_some()))
                })
            });
        // Scalar subqueries form independent scopes, including when hidden in
        // typed function arguments. Do not revisit already compiled sources.
        let from = select.from.take();
        let joins = std::mem::take(&mut select.joins);
        let mut scalars = serde_json::to_value(&select)
            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
        self.rewrite_scalar_values(&mut scalars, &ctes, names)?;
        select = serde_json::from_value(scalars)
            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
        select.from = from;
        select.joins = joins;
        if semantic_leaf {
            if let Some(from) = &select.from {
                if let Some(Expression::Table(table)) = from.expressions.first() {
                    validate_table(table)?;
                    if !table.column_aliases.is_empty() {
                        return Err(unsupported());
                    }
                }
            }
            let mut compiled = self.compile_semantic_select(*select)?;
            compiled.with = with;
            return Ok(Expression::Select(Box::new(compiled)));
        }
        if select.from.is_none() && has_star_projection(&select.expressions) {
            return Err(SidemanticError::Validation(
                "SELECT * requires a FROM clause with a single table".into(),
            ));
        }
        if let Some(from) = &mut select.from {
            for source in &mut from.expressions {
                *source = self.rewrite_policy_source(source.clone(), &ctes, names)?;
            }
        }
        for join in &mut select.joins {
            join.this = self.rewrite_policy_source(join.this.clone(), &ctes, names)?;
            if let Some(on) = join.on.take() {
                join.on = Some(self.rewrite_scalar_scopes(on, &ctes, names)?);
            }
        }
        select.with = with;
        Ok(Expression::Select(select))
    }

    fn rewrite_ctes(
        &self,
        with: &mut Option<With>,
        inherited: &HashMap<String, Identifier>,
        names: &mut CteNames,
    ) -> Result<HashMap<String, Identifier>> {
        let mut ctes = inherited.clone();
        if let Some(with) = with {
            let mut local = HashSet::new();
            for cte in &mut with.ctes {
                let original = cte.alias.name.to_ascii_lowercase();
                if !local.insert(original.clone()) {
                    return Err(unsupported());
                }
                let alias = names.allocate();
                // Recursive bodies see their own new binding; ordinary bodies
                // see the preceding/inherited binding until the body finishes.
                if with.recursive {
                    ctes.insert(original.clone(), alias.clone());
                }
                cte.this = self.rewrite_policy_query(cte.this.clone(), &ctes, names)?;
                cte.alias = alias.clone();
                ctes.insert(original, alias);
            }
        }
        Ok(ctes)
    }

    fn rewrite_scalar_scopes(
        &self,
        expression: Expression,
        ctes: &HashMap<String, Identifier>,
        names: &mut CteNames,
    ) -> Result<Expression> {
        binding::transform_nodes(expression, &mut |node| {
            if matches!(
                node,
                Expression::Select(_)
                    | Expression::Subquery(_)
                    | Expression::Union(_)
                    | Expression::Intersect(_)
                    | Expression::Except(_)
            ) {
                return self
                    .rewrite_policy_query(node.clone(), ctes, names)
                    .map(Some);
            }
            if self.query_preparer.is_some()
                && matches!(
                    node,
                    Expression::Table(_) | Expression::Raw(_) | Expression::Command(_)
                )
            {
                return Err(unsupported());
            }
            Ok(None)
        })
    }

    fn rewrite_scalar_values(
        &self,
        value: &mut serde_json::Value,
        ctes: &HashMap<String, Identifier>,
        names: &mut CteNames,
    ) -> Result<()> {
        if value.as_object().is_some_and(|fields| fields.len() == 1) {
            if let Ok(expression) = serde_json::from_value::<Expression>(value.clone()) {
                *value = serde_json::to_value(self.rewrite_scalar_scopes(expression, ctes, names)?)
                    .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
                return Ok(());
            }
        }
        match value {
            serde_json::Value::Object(fields) => {
                for child in fields.values_mut() {
                    self.rewrite_scalar_values(child, ctes, names)?;
                }
            }
            serde_json::Value::Array(children) => {
                for child in children {
                    self.rewrite_scalar_values(child, ctes, names)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn rewrite_set_clauses(
        &self,
        expression: Expression,
        ctes: &HashMap<String, Identifier>,
        names: &mut CteNames,
    ) -> Result<Expression> {
        let mut value = serde_json::to_value(expression)
            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
        let fields = value
            .as_object_mut()
            .expect("expression node")
            .values_mut()
            .next()
            .and_then(serde_json::Value::as_object_mut)
            .expect("set node");
        for (name, child) in fields {
            if !matches!(name.as_str(), "left" | "right" | "with") {
                self.rewrite_scalar_values(child, ctes, names)?;
            }
        }
        serde_json::from_value(value)
            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))
    }

    fn rewrite_policy_source(
        &self,
        source: Expression,
        ctes: &HashMap<String, Identifier>,
        names: &mut CteNames,
    ) -> Result<Expression> {
        match source {
            Expression::Table(mut table) => {
                let binding = (table.schema.is_none() && table.catalog.is_none())
                    .then(|| ctes.get(&table.name.name.to_ascii_lowercase()))
                    .flatten();
                let Some(binding) = binding else {
                    if self.query_preparer.is_some() {
                        return Err(unsupported());
                    }
                    return Ok(Expression::Table(table));
                };
                validate_table(&table)?;
                // Retain the user's qualifier, including quoted aliases.
                if table.alias.is_none() {
                    table.alias = Some(table.name.clone());
                }
                table.name = binding.clone();
                Ok(Expression::Table(table))
            }
            Expression::Subquery(mut subquery) => {
                subquery.this = self.rewrite_policy_query(subquery.this, ctes, names)?;
                Ok(Expression::Subquery(subquery))
            }
            Expression::Alias(mut alias) => {
                alias.this = self.rewrite_policy_source(alias.this, ctes, names)?;
                if let Expression::Table(table) = &mut alias.this {
                    table.alias = None;
                }
                Ok(Expression::Alias(alias))
            }
            Expression::Paren(mut paren) => {
                paren.this = self.rewrite_policy_source(paren.this, ctes, names)?;
                Ok(Expression::Paren(paren))
            }
            Expression::JoinedTable(mut joined) => {
                joined.left = self.rewrite_policy_source(joined.left, ctes, names)?;
                for join in &mut joined.joins {
                    join.this = self.rewrite_policy_source(join.this.clone(), ctes, names)?;
                    if let Some(on) = join.on.take() {
                        join.on = Some(self.rewrite_scalar_scopes(on, ctes, names)?);
                    }
                }
                Ok(Expression::JoinedTable(joined))
            }
            other if self.query_preparer.is_none() => {
                self.rewrite_scalar_scopes(other, ctes, names)
            }
            _ => Err(unsupported()),
        }
    }
}

fn validate_table(table: &TableRef) -> Result<()> {
    let mut remainder = table.clone();
    remainder.name = Identifier::new("");
    remainder.alias = None;
    remainder.alias_explicit_as = false;
    remainder.column_aliases.clear();
    remainder.trailing_comments.clear();
    remainder.span = None;
    if remainder != TableRef::new("") {
        return Err(unsupported());
    }
    Ok(())
}
