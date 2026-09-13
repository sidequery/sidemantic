//! Secure semantic leaves before retaining their relational SQL wrappers.

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
        let Expression::Select(mut select) = statement else {
            return Err(unsupported());
        };
        let mut ctes = inherited_ctes.clone();
        let mut with = select.with.take();
        if let Some(with) = &mut with {
            if with.recursive || with.search.is_some() {
                return Err(unsupported());
            }
            let mut local_names = HashSet::new();
            for cte in &mut with.ctes {
                let original = cte.alias.name.to_ascii_lowercase();
                if !local_names.insert(original.clone()) || !cte.key_expressions.is_empty() {
                    return Err(unsupported());
                }
                // A nonrecursive CTE sees preceding and inherited bindings,
                // not its own new binding. Shadowing starts after its body.
                cte.this = self.rewrite_policy_query(cte.this.clone(), &ctes, names)?;
                cte.alias = names.allocate();
                ctes.insert(original, cte.alias.clone());
            }
        }

        let mut scalar_clauses = select.clone();
        scalar_clauses.from = None;
        // The pinned polyglot walker misses typed function children (Sum.this
        // among them). Traverse the serialized AST, as dependency analysis does,
        // so a function cannot conceal a source read from the policy boundary.
        let ast = serde_json::to_value(scalar_clauses)
            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
        let mut nodes = vec![&ast];
        while let Some(node) = nodes.pop() {
            match node {
                serde_json::Value::Object(fields) => {
                    let kind = (fields.len() == 1).then(|| fields.keys().next().unwrap().as_str());
                    if matches!(
                        kind,
                        Some("select" | "subquery" | "table" | "raw" | "command")
                    ) {
                        return Err(unsupported());
                    }
                    nodes.extend(fields.values());
                }
                serde_json::Value::Array(children) => nodes.extend(children),
                _ => {}
            }
        }

        let source = select
            .from
            .as_ref()
            .and_then(|from| (from.expressions.len() == 1).then(|| &from.expressions[0]));
        let Some(source) = source else {
            return Err(unsupported());
        };
        let semantic_leaf = matches!(source, Expression::Table(table)
            if table.name.name.eq_ignore_ascii_case("metrics")
                && table.schema.is_none() && table.catalog.is_none()
                && !ctes.contains_key("metrics"));
        if semantic_leaf {
            let Expression::Table(table) = source else {
                unreachable!()
            };
            validate_table(table)?;
            if !table.column_aliases.is_empty() {
                return Err(unsupported());
            }
            let mut compiled = self.rewrite_scoped_metrics_select(*select)?;
            compiled.with = with;
            return Ok(Expression::Select(Box::new(compiled)));
        }

        // Preserve exactly this bounded set of outer relational clauses.
        // In particular, SELECT INTO and dialect controls cannot pass through.
        let mut remainder = (*select).clone();
        remainder.expressions.clear();
        remainder.from = None;
        remainder.where_clause = None;
        remainder.group_by = None;
        remainder.having = None;
        remainder.order_by = None;
        remainder.limit = None;
        remainder.offset = None;
        remainder.distinct = false;
        remainder.leading_comments.clear();
        remainder.post_select_comments.clear();
        if remainder != Select::new() {
            return Err(unsupported());
        }
        let source = self.rewrite_policy_source(source.clone(), &ctes, names)?;
        select.from = Some(From {
            expressions: vec![source],
        });
        select.with = with;
        Ok(Expression::Select(select))
    }

    fn rewrite_policy_source(
        &self,
        source: Expression,
        ctes: &HashMap<String, Identifier>,
        names: &mut CteNames,
    ) -> Result<Expression> {
        match source {
            Expression::Table(mut table) => {
                validate_table(&table)?;
                let Some(binding) = ctes.get(&table.name.name.to_ascii_lowercase()) else {
                    return Err(unsupported());
                };
                // Retain the user's qualifier, including quoted aliases.
                if table.alias.is_none() {
                    table.alias = Some(table.name.clone());
                }
                table.name = binding.clone();
                Ok(Expression::Table(table))
            }
            Expression::Subquery(mut subquery) => {
                if subquery.lateral
                    || subquery.order_by.is_some()
                    || subquery.limit.is_some()
                    || subquery.offset.is_some()
                    || subquery.distribute_by.is_some()
                    || subquery.sort_by.is_some()
                    || subquery.cluster_by.is_some()
                {
                    return Err(unsupported());
                }
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
