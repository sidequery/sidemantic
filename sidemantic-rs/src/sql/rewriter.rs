//! Query rewriter: rewrites SQL using semantic layer definitions

use std::collections::HashSet;

#[cfg(not(target_arch = "wasm32"))]
use polyglot_sql::parse as polyglot_parse;
use polyglot_sql::{
    expressions::{
        BinaryOp, Cte, From, GroupBy, Having, Identifier, Join, JoinKind, Select, TableRef, Where,
        With,
    },
    generate as polyglot_generate, traversal, DialectType, Expression,
};

use crate::core::{DimensionType, MetricType, SemanticGraph};
use crate::error::{Result, SidemanticError};

/// SQL query rewriter using semantic definitions
pub struct QueryRewriter<'a> {
    graph: &'a SemanticGraph,
}

impl<'a> QueryRewriter<'a> {
    pub fn new(graph: &'a SemanticGraph) -> Self {
        Self { graph }
    }

    /// Rewrite a SQL query using semantic layer definitions
    pub fn rewrite(&self, sql: &str) -> Result<String> {
        let statements = parse_sql_with_large_stack(sql)?;

        if statements.is_empty() {
            return Err(SidemanticError::SqlParse("Empty SQL".into()));
        }

        let mut rewritten_statements = Vec::new();
        for statement in statements {
            let rewritten = self.rewrite_statement(statement)?;
            rewritten_statements.push(expr_to_sql(&rewritten)?);
        }

        Ok(rewritten_statements.join(";\n"))
    }

    fn rewrite_statement(&self, statement: Expression) -> Result<Expression> {
        match statement {
            Expression::Select(select) => {
                let rewritten_select = self.rewrite_select(*select)?;
                Ok(Expression::Select(Box::new(rewritten_select)))
            }
            other => Ok(other),
        }
    }

    fn rewrite_select(&self, mut select: Select) -> Result<Select> {
        if let Some(mut with_clause) = select.with.take() {
            for cte in &mut with_clause.ctes {
                cte.this = self.rewrite_nested_query_expr(cte.this.clone())?;
            }
            select.with = Some(with_clause);
        }

        if let Some(from_clause) = &mut select.from {
            for source in &mut from_clause.expressions {
                *source = self.rewrite_nested_query_expr(source.clone())?;
            }
        }

        // Find semantic model references in FROM clause
        let mut model_refs = self.find_model_references(select.from.as_ref());
        let from_metrics = is_from_metrics(select.from.as_ref());

        if model_refs.is_empty() {
            if from_metrics {
                if has_star_projection(&select.expressions) {
                    return Err(SidemanticError::Validation(
                        "SELECT * is not supported with FROM metrics".into(),
                    ));
                }

                let mut referenced_models = self.find_referenced_models(&select.expressions);
                if let Some(selection) = &select.where_clause {
                    self.collect_model_refs_from_expr(&selection.this, &mut referenced_models);
                }
                if let Some(having) = &select.having {
                    self.collect_model_refs_from_expr(&having.this, &mut referenced_models);
                }

                if referenced_models.is_empty() {
                    return Err(SidemanticError::Validation(
                        "Column must be fully qualified when using FROM metrics".into(),
                    ));
                }

                let mut ordered_models: Vec<String> = referenced_models.into_iter().collect();
                ordered_models.sort();

                let base_model = ordered_models[0].clone();
                let base_alias = base_model.clone();
                model_refs.push((base_model.clone(), base_alias.clone()));

                let base_table = self.graph.get_model(&base_model).ok_or_else(|| {
                    SidemanticError::Validation(format!("Model '{base_model}' not found"))
                })?;
                select.from = Some(From {
                    expressions: vec![Expression::Table(table_ref_for(
                        base_table.table_name(),
                        Some(&base_alias),
                    ))],
                });
            }

            if select.from.is_none() && has_star_projection(&select.expressions) {
                return Err(SidemanticError::Validation(
                    "SELECT * requires a FROM clause with a single table".into(),
                ));
            }

            if model_refs.is_empty() {
                // No semantic models, return as-is
                return Ok(select);
            }
        }

        if !select.joins.is_empty() {
            return Err(SidemanticError::Validation(
                "Explicit JOIN syntax is not supported. Joins are automatic based on model relationships."
                    .into(),
            ));
        }

        // Find all models referenced in projection AND WHERE clause
        let mut referenced_models = self.find_referenced_models(&select.expressions);
        if let Some(selection) = &select.where_clause {
            self.collect_model_refs_from_expr(&selection.this, &mut referenced_models);
        }
        if let Some(having) = &select.having {
            self.collect_model_refs_from_expr(&having.this, &mut referenced_models);
        }

        // Find models that need to be joined (referenced but not in FROM)
        let base_model = model_refs.first().map(|(m, _)| m.clone());
        let models_in_from: HashSet<_> = model_refs.iter().map(|(m, _)| m.clone()).collect();
        let models_to_join: Vec<_> = referenced_models
            .iter()
            .filter(|m| !models_in_from.contains(*m))
            .cloned()
            .collect();

        // Add aliases for joined models
        for model_name in &models_to_join {
            let existing_aliases: HashSet<String> =
                model_refs.iter().map(|(_, alias)| alias.clone()).collect();
            let alias = unique_alias(model_name, &existing_aliases);
            model_refs.push((model_name.clone(), alias));
        }

        // Rewrite SELECT items
        select.expressions = self.rewrite_projection(&select.expressions, &model_refs)?;

        // Rewrite FROM clause with JOINs
        self.rewrite_from_with_joins(
            &mut select,
            &model_refs,
            base_model.as_deref(),
            &models_to_join,
        )?;

        // Rewrite WHERE clause
        if let Some(selection) = select.where_clause.take() {
            select.where_clause = Some(Where {
                this: self.rewrite_expr(selection.this, &model_refs)?,
            });
        }

        // Rewrite HAVING clause
        if let Some(having) = select.having.take() {
            select.having = Some(Having {
                this: self.rewrite_expr(having.this, &model_refs)?,
            });
        }

        if select.expressions.is_empty() {
            return Err(SidemanticError::Validation(
                "Query must select at least one metric or dimension".into(),
            ));
        }

        // Add GROUP BY whenever we select non-aggregated dimensions.
        let has_dimensions = self.has_non_aggregated_columns(&select.expressions);

        if has_dimensions {
            select.group_by = Some(self.build_group_by(&select.expressions));
        }

        select = self.wrap_simple_select_with_cte(select, &model_refs)?;

        Ok(select)
    }

    fn wrap_simple_select_with_cte(
        &self,
        select: Select,
        model_refs: &[(String, String)],
    ) -> Result<Select> {
        if model_refs.len() != 1
            || select.with.is_some()
            || !select.joins.is_empty()
            || select.having.is_some()
            || select.order_by.is_some()
            || select.limit.is_some()
            || select.offset.is_some()
            || select.distinct
            || select.group_by.is_none()
        {
            return Ok(select);
        }

        let (model_name, model_alias) = (&model_refs[0].0, &model_refs[0].1);
        let model = self.graph.get_model(model_name).ok_or_else(|| {
            SidemanticError::Validation(format!("Model '{model_name}' not found"))
        })?;

        let mut dimensions: Vec<(Expression, String)> = Vec::new();
        let mut metrics: Vec<(Expression, String)> = Vec::new();

        for projection in &select.expressions {
            let Expression::Alias(alias) = projection else {
                return Ok(select);
            };

            if self.is_aggregation(&alias.this) {
                metrics.push((alias.this.clone(), alias.alias.name.clone()));
            } else {
                dimensions.push((alias.this.clone(), alias.alias.name.clone()));
            }
        }

        if dimensions.is_empty() || metrics.is_empty() {
            return Ok(select);
        }

        let mut cte_select = Select::new();
        cte_select.from = select.from.clone();
        cte_select.where_clause = select.where_clause.clone();
        for primary_key in model.primary_keys() {
            cte_select.expressions.push(
                Expression::qualified_column(model_alias.clone(), primary_key.clone())
                    .alias(primary_key),
            );
        }

        for (dimension_expr, alias_name) in &dimensions {
            cte_select
                .expressions
                .push(dimension_expr.clone().alias(alias_name.clone()));
        }

        for (metric_expr, alias_name) in &metrics {
            let Some(raw_expr) = extract_aggregate_input(metric_expr) else {
                return Ok(select);
            };
            cte_select
                .expressions
                .push(raw_expr.alias(format!("{alias_name}_raw")));
        }

        let cte_name = format!("{model_name}_cte");
        let cte_alias = cte_name.clone();

        let mut outer_select = Select::new();
        outer_select.with = Some(With {
            ctes: vec![Cte {
                alias: Identifier::new(cte_name),
                this: Expression::Select(Box::new(cte_select)),
                columns: vec![],
                materialized: None,
                key_expressions: vec![],
                alias_first: false,
            }],
            recursive: false,
            leading_comments: vec![],
            search: None,
        });
        outer_select.from = Some(From {
            expressions: vec![Expression::Table(table_ref_for(&cte_alias, None))],
        });

        for (_, alias_name) in &dimensions {
            outer_select.expressions.push(
                Expression::qualified_column(cte_alias.clone(), alias_name.clone())
                    .alias(alias_name.clone()),
            );
        }

        for (metric_expr, alias_name) in &metrics {
            let raw_col =
                Expression::qualified_column(cte_alias.clone(), format!("{alias_name}_raw"));
            let Some(outer_metric_expr) = rebuild_aggregate_with_input(metric_expr, raw_col) else {
                return Ok(select);
            };
            outer_select
                .expressions
                .push(outer_metric_expr.alias(alias_name.clone()));
        }

        outer_select.group_by = Some(GroupBy {
            expressions: (1..=dimensions.len())
                .map(|i| Expression::number(i as i64))
                .collect(),
            all: None,
            totals: false,
        });

        Ok(outer_select)
    }

    fn rewrite_nested_query_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            Expression::Select(select) => {
                let rewritten = self.rewrite_select(*select)?;
                Ok(Expression::Select(Box::new(rewritten)))
            }
            Expression::Subquery(mut subquery) => {
                subquery.this = self.rewrite_nested_query_expr(subquery.this)?;
                Ok(Expression::Subquery(subquery))
            }
            Expression::Alias(mut alias) => {
                alias.this = self.rewrite_nested_query_expr(alias.this)?;
                Ok(Expression::Alias(alias))
            }
            Expression::Paren(mut paren) => {
                paren.this = self.rewrite_nested_query_expr(paren.this)?;
                Ok(Expression::Paren(paren))
            }
            Expression::JoinedTable(mut joined) => {
                joined.left = self.rewrite_nested_query_expr(joined.left)?;
                for join in &mut joined.joins {
                    join.this = self.rewrite_nested_query_expr(join.this.clone())?;
                }
                Ok(Expression::JoinedTable(joined))
            }
            other => Ok(other),
        }
    }

    /// Find semantic model references in FROM clause
    fn find_model_references(
        &self,
        from: Option<&polyglot_sql::expressions::From>,
    ) -> Vec<(String, String)> {
        let mut refs = Vec::new();

        if let Some(from_clause) = from {
            for source in &from_clause.expressions {
                if let Some((table_name, alias)) = table_name_and_alias(source) {
                    if self.graph.get_model(&table_name).is_some() {
                        refs.push((table_name.clone(), alias.unwrap_or(table_name)));
                    }
                }
            }
        }

        refs
    }

    /// Find all models referenced in the SELECT projection
    fn find_referenced_models(&self, projection: &[Expression]) -> HashSet<String> {
        let mut models = HashSet::new();

        for item in projection {
            self.collect_model_refs_from_expr(item, &mut models);
        }

        models
    }

    /// Collect model references from an expression
    fn collect_model_refs_from_expr(&self, expr: &Expression, models: &mut HashSet<String>) {
        for column_ref in traversal::get_columns(expr) {
            if let Expression::Column(column) = column_ref {
                if let Some(table) = &column.table {
                    if self.graph.get_model(&table.name).is_some() {
                        models.insert(table.name.clone());
                    }
                }
            }
        }
    }

    /// Rewrite SELECT projection items
    fn rewrite_projection(
        &self,
        projection: &[Expression],
        model_refs: &[(String, String)],
    ) -> Result<Vec<Expression>> {
        let mut result = Vec::new();

        for item in projection {
            match item {
                Expression::Star(_) => {
                    if model_refs.len() != 1 {
                        return Err(SidemanticError::Validation(
                            "SELECT * requires a FROM clause with a single table".into(),
                        ));
                    }

                    let (model_name, alias) = (&model_refs[0].0, &model_refs[0].1);
                    let model = self.graph.get_model(model_name).ok_or_else(|| {
                        SidemanticError::Validation(format!("Model '{model_name}' not found"))
                    })?;

                    for dimension in &model.dimensions {
                        result.push(
                            Expression::qualified_column(
                                alias.clone(),
                                dimension.sql_expr().to_string(),
                            )
                            .alias(dimension.name.clone()),
                        );
                    }
                    for metric in &model.metrics {
                        result.push(
                            self.metric_to_expr(metric, alias)
                                .alias(metric.name.clone()),
                        );
                    }
                }
                Expression::Alias(alias) => {
                    let mut new_alias = alias.as_ref().clone();
                    new_alias.this = self.rewrite_select_expr(new_alias.this, model_refs)?;
                    result.push(Expression::Alias(Box::new(new_alias)));
                }
                Expression::Column(column) => {
                    let rewritten = self.rewrite_select_expr(item.clone(), model_refs)?;
                    let alias_name = column_alias_name(column, model_refs)
                        .unwrap_or_else(|| column.name.name.clone());
                    result.push(rewritten.alias(alias_name));
                }
                Expression::Count(_)
                | Expression::Sum(_)
                | Expression::Avg(_)
                | Expression::Min(_)
                | Expression::Max(_)
                | Expression::Median(_)
                | Expression::AggregateFunction(_) => {
                    return Err(SidemanticError::Validation(
                        "Aggregate functions must be defined as a metric".into(),
                    ));
                }
                Expression::Function(_) => {
                    return Err(SidemanticError::Validation(
                        "Aggregate functions must be defined as a metric".into(),
                    ));
                }
                _ => {
                    let rewritten = self.rewrite_select_expr(item.clone(), model_refs)?;
                    if matches!(rewritten, Expression::Identifier(_)) {
                        return Err(SidemanticError::Validation(
                            "Query must select at least one metric or dimension".into(),
                        ));
                    }
                    result.push(rewritten);
                }
            }
        }

        Ok(result)
    }

    /// Rewrite a SELECT expression (could be metric or dimension)
    fn rewrite_select_expr(
        &self,
        expr: Expression,
        model_refs: &[(String, String)],
    ) -> Result<Expression> {
        if let Expression::Column(column) = &expr {
            if let Some((model_name, alias_name, base_field, granularity)) =
                resolve_model_field(column, model_refs)
            {
                let model = self.graph.get_model(model_name).ok_or_else(|| {
                    SidemanticError::Validation(format!("Model '{model_name}' not found"))
                })?;

                if let Some(metric) = model.get_metric(base_field) {
                    return Ok(self.metric_to_expr(metric, alias_name));
                }

                if let Some(dimension) = model.get_dimension(base_field) {
                    return Ok(dimension_to_expr(alias_name, dimension, granularity));
                }

                return Err(SidemanticError::Validation(format!(
                    "Field '{model_name}.{base_field}' not found"
                )));
            }

            return Err(SidemanticError::Validation(format!(
                "Cannot resolve column: {}",
                column.name.name
            )));
        }

        if matches!(
            expr,
            Expression::Count(_)
                | Expression::Sum(_)
                | Expression::Avg(_)
                | Expression::Min(_)
                | Expression::Max(_)
                | Expression::Median(_)
                | Expression::AggregateFunction(_)
                | Expression::Function(_)
        ) {
            return Err(SidemanticError::Validation(
                "Aggregate functions must be defined as a metric".into(),
            ));
        }

        self.rewrite_expr(expr, model_refs)
    }

    /// Convert a metric to an expression
    fn metric_to_expr(&self, metric: &crate::core::Metric, alias: &str) -> Expression {
        match metric.r#type {
            MetricType::Simple => {
                // Handle Expression type: sql field contains the full expression
                if let Some(crate::core::Aggregation::Expression) = &metric.agg {
                    if let Some(expr) = parse_select_expr(metric.sql_expr()) {
                        return expr;
                    }
                    // Fallback: return as identifier
                    return Expression::identifier(metric.name.clone());
                }

                if let Some(expr) = parse_select_expr(&metric.to_sql(Some(alias))) {
                    return expr;
                }

                // Fallback: return as identifier
                Expression::identifier(metric.name.clone())
            }
            MetricType::Derived
            | MetricType::Ratio
            | MetricType::Cumulative
            | MetricType::TimeComparison
            | MetricType::Conversion => {
                if let Some(expr) = parse_select_expr(&metric.to_sql(Some(alias))) {
                    return expr;
                }
                // Fallback: return as identifier
                Expression::identifier(metric.name.clone())
            }
        }
    }

    /// Rewrite FROM clause with JOINs for cross-model references
    fn rewrite_from_with_joins(
        &self,
        select: &mut Select,
        model_refs: &[(String, String)],
        base_model: Option<&str>,
        models_to_join: &[String],
    ) -> Result<()> {
        // Rewrite base FROM semantic model table names
        if let Some(from_clause) = &mut select.from {
            for source in &mut from_clause.expressions {
                self.rewrite_from_source(source);
            }
        }

        // Add auto-joins for referenced models
        if let Some(base) = base_model {
            for target_model_name in models_to_join {
                if let Ok(join_path) = self.graph.find_join_path(base, target_model_name) {
                    for step in &join_path.steps {
                        let target_model = self.graph.get_model(&step.to_model).unwrap();

                        // Find aliases for this join step
                        let to_alias = model_refs
                            .iter()
                            .find(|(m, _)| m == &step.to_model)
                            .map(|(_, a)| a.clone())
                            .unwrap_or_else(|| step.to_model.clone());

                        let from_alias = model_refs
                            .iter()
                            .find(|(m, _)| m == &step.from_model)
                            .map(|(_, a)| a.clone())
                            .unwrap_or_else(|| step.from_model.clone());

                        let join_condition = if let Some(custom) = &step.custom_condition {
                            let condition_sql = custom
                                .replace("{from}", &from_alias)
                                .replace("{to}", &to_alias);
                            parse_where_expr(&condition_sql).unwrap_or_else(|| {
                                self.build_default_join_condition(
                                    &from_alias,
                                    &step.from_keys,
                                    &to_alias,
                                    &step.to_keys,
                                )
                                .expect("join path keys already validated")
                            })
                        } else {
                            self.build_default_join_condition(
                                &from_alias,
                                &step.from_keys,
                                &to_alias,
                                &step.to_keys,
                            )?
                        };

                        select.joins.push(Join {
                            this: Expression::Table(table_ref_for(
                                target_model.table_name(),
                                Some(&to_alias),
                            )),
                            on: Some(join_condition),
                            using: vec![],
                            kind: JoinKind::Left,
                            use_inner_keyword: false,
                            use_outer_keyword: false,
                            deferred_condition: false,
                            join_hint: None,
                            match_condition: None,
                            pivots: vec![],
                        });
                    }
                }
            }
        }

        Ok(())
    }

    fn rewrite_from_source(&self, source: &mut Expression) {
        match source {
            Expression::Table(table) => {
                let model_name = table.name.name.clone();
                if let Some(model) = self.graph.get_model(&model_name) {
                    rewrite_table_ref_name(table, model.table_name());
                    if table.alias.is_none() && model.table_name() != model_name {
                        table.alias = Some(Identifier::new(model_name));
                    }
                }
            }
            Expression::Alias(alias) => {
                self.rewrite_from_source(&mut alias.this);
            }
            Expression::Paren(paren) => {
                self.rewrite_from_source(&mut paren.this);
            }
            _ => {}
        }
    }

    /// Build default JOIN condition (from.fk = to.pk)
    fn build_default_join_condition(
        &self,
        from_alias: &str,
        from_keys: &[String],
        to_alias: &str,
        to_keys: &[String],
    ) -> Result<Expression> {
        if from_keys.is_empty() || to_keys.is_empty() {
            return Err(SidemanticError::Validation(
                "Join path is missing join key columns".to_string(),
            ));
        }
        if from_keys.len() != to_keys.len() {
            return Err(SidemanticError::Validation(format!(
                "Join key column count mismatch: {} vs {}",
                from_keys.len(),
                to_keys.len()
            )));
        }

        let mut conditions = from_keys
            .iter()
            .zip(to_keys.iter())
            .map(|(from_key, to_key)| {
                Expression::Eq(Box::new(BinaryOp::new(
                    Expression::qualified_column(from_alias.to_string(), from_key.to_string()),
                    Expression::qualified_column(to_alias.to_string(), to_key.to_string()),
                )))
            });

        let first = conditions.next().expect("checked non-empty key lists");
        Ok(conditions.fold(first, |left, right| {
            Expression::And(Box::new(BinaryOp::new(left, right)))
        }))
    }

    /// Rewrite general expressions
    fn rewrite_expr(
        &self,
        expr: Expression,
        model_refs: &[(String, String)],
    ) -> Result<Expression> {
        match expr {
            Expression::Column(column) => {
                if let Some((model_name, alias_name, base_field, granularity)) =
                    resolve_model_field(&column, model_refs)
                {
                    let model = self.graph.get_model(model_name).ok_or_else(|| {
                        SidemanticError::Validation(format!("Model '{model_name}' not found"))
                    })?;

                    if let Some(metric) = model.get_metric(base_field) {
                        return Ok(self.metric_to_expr(metric, alias_name));
                    }

                    if let Some(dimension) = model.get_dimension(base_field) {
                        return Ok(dimension_to_expr(alias_name, dimension, granularity));
                    }
                }

                Ok(Expression::Column(column))
            }
            Expression::Alias(mut alias) => {
                alias.this = self.rewrite_expr(alias.this, model_refs)?;
                Ok(Expression::Alias(alias))
            }
            Expression::Paren(mut paren) => {
                paren.this = self.rewrite_expr(paren.this, model_refs)?;
                Ok(Expression::Paren(paren))
            }

            Expression::And(binary) => Ok(Expression::And(Box::new(
                self.rewrite_binary_op(*binary, model_refs)?,
            ))),
            Expression::Or(binary) => Ok(Expression::Or(Box::new(
                self.rewrite_binary_op(*binary, model_refs)?,
            ))),
            Expression::Add(binary) => Ok(Expression::Add(Box::new(
                self.rewrite_binary_op(*binary, model_refs)?,
            ))),
            Expression::Sub(binary) => Ok(Expression::Sub(Box::new(
                self.rewrite_binary_op(*binary, model_refs)?,
            ))),
            Expression::Mul(binary) => Ok(Expression::Mul(Box::new(
                self.rewrite_binary_op(*binary, model_refs)?,
            ))),
            Expression::Div(binary) => Ok(Expression::Div(Box::new(
                self.rewrite_binary_op(*binary, model_refs)?,
            ))),
            Expression::Mod(binary) => Ok(Expression::Mod(Box::new(
                self.rewrite_binary_op(*binary, model_refs)?,
            ))),
            Expression::Eq(binary) => Ok(Expression::Eq(Box::new(
                self.rewrite_binary_op(*binary, model_refs)?,
            ))),
            Expression::Neq(binary) => Ok(Expression::Neq(Box::new(
                self.rewrite_binary_op(*binary, model_refs)?,
            ))),
            Expression::Lt(binary) => Ok(Expression::Lt(Box::new(
                self.rewrite_binary_op(*binary, model_refs)?,
            ))),
            Expression::Lte(binary) => Ok(Expression::Lte(Box::new(
                self.rewrite_binary_op(*binary, model_refs)?,
            ))),
            Expression::Gt(binary) => Ok(Expression::Gt(Box::new(
                self.rewrite_binary_op(*binary, model_refs)?,
            ))),
            Expression::Gte(binary) => Ok(Expression::Gte(Box::new(
                self.rewrite_binary_op(*binary, model_refs)?,
            ))),
            Expression::Like(mut like_op) => {
                like_op.left = self.rewrite_expr(like_op.left, model_refs)?;
                like_op.right = self.rewrite_expr(like_op.right, model_refs)?;
                if let Some(escape) = like_op.escape.take() {
                    like_op.escape = Some(self.rewrite_expr(escape, model_refs)?);
                }
                Ok(Expression::Like(like_op))
            }
            Expression::ILike(mut like_op) => {
                like_op.left = self.rewrite_expr(like_op.left, model_refs)?;
                like_op.right = self.rewrite_expr(like_op.right, model_refs)?;
                if let Some(escape) = like_op.escape.take() {
                    like_op.escape = Some(self.rewrite_expr(escape, model_refs)?);
                }
                Ok(Expression::ILike(like_op))
            }
            Expression::Not(mut unary) => {
                unary.this = self.rewrite_expr(unary.this, model_refs)?;
                Ok(Expression::Not(unary))
            }
            Expression::Neg(mut unary) => {
                unary.this = self.rewrite_expr(unary.this, model_refs)?;
                Ok(Expression::Neg(unary))
            }
            Expression::BitwiseNot(mut unary) => {
                unary.this = self.rewrite_expr(unary.this, model_refs)?;
                Ok(Expression::BitwiseNot(unary))
            }
            Expression::In(mut in_expr) => {
                in_expr.this = self.rewrite_expr(in_expr.this, model_refs)?;
                in_expr.expressions = in_expr
                    .expressions
                    .into_iter()
                    .map(|e| self.rewrite_expr(e, model_refs))
                    .collect::<Result<Vec<_>>>()?;
                if let Some(query_expr) = in_expr.query.take() {
                    in_expr.query = Some(self.rewrite_expr(query_expr, model_refs)?);
                }
                if let Some(unnest_expr) = in_expr.unnest.take() {
                    in_expr.unnest = Some(Box::new(self.rewrite_expr(*unnest_expr, model_refs)?));
                }
                Ok(Expression::In(in_expr))
            }
            Expression::Between(mut between) => {
                between.this = self.rewrite_expr(between.this, model_refs)?;
                between.low = self.rewrite_expr(between.low, model_refs)?;
                between.high = self.rewrite_expr(between.high, model_refs)?;
                Ok(Expression::Between(between))
            }
            Expression::IsNull(mut is_null) => {
                is_null.this = self.rewrite_expr(is_null.this, model_refs)?;
                Ok(Expression::IsNull(is_null))
            }
            other => Ok(other),
        }
    }

    fn rewrite_binary_op(
        &self,
        mut binary: BinaryOp,
        model_refs: &[(String, String)],
    ) -> Result<BinaryOp> {
        binary.left = self.rewrite_expr(binary.left, model_refs)?;
        binary.right = self.rewrite_expr(binary.right, model_refs)?;
        Ok(binary)
    }

    /// Check if expression is an aggregation function
    fn is_aggregation(&self, expr: &Expression) -> bool {
        match expr {
            Expression::Alias(alias) => self.is_aggregation(&alias.this),
            Expression::Count(_)
            | Expression::Sum(_)
            | Expression::Avg(_)
            | Expression::Min(_)
            | Expression::Max(_)
            | Expression::Median(_)
            | Expression::AggregateFunction(_) => true,
            _ => false,
        }
    }

    /// Check if projection has non-aggregated columns
    fn has_non_aggregated_columns(&self, projection: &[Expression]) -> bool {
        projection.iter().any(|expr| !self.is_aggregation(expr))
    }

    /// Build GROUP BY clause from non-aggregated columns
    fn build_group_by(&self, projection: &[Expression]) -> GroupBy {
        let mut group_by_exprs = Vec::new();

        for (i, expr) in projection.iter().enumerate() {
            if !self.is_aggregation(expr) {
                // Use positional reference
                group_by_exprs.push(Expression::number((i + 1) as i64));
            }
        }

        GroupBy {
            expressions: group_by_exprs,
            all: None,
            totals: false,
        }
    }
}

fn parse_sql_with_large_stack(sql: &str) -> Result<Vec<Expression>> {
    #[cfg(target_arch = "wasm32")]
    {
        let _ = sql;
        return Err(SidemanticError::SqlParse(
            "operation not supported on this platform".to_string(),
        ));
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        let sql_owned = sql.to_string();
        let handle = std::thread::Builder::new()
            .stack_size(16 * 1024 * 1024)
            .spawn(move || {
                polyglot_parse(&sql_owned, DialectType::Generic).map_err(|e| e.to_string())
            })
            .map_err(|e| SidemanticError::SqlParse(e.to_string()))?;

        let parse_result = handle
            .join()
            .map_err(|_| SidemanticError::SqlParse("Polyglot parser thread panicked".into()))?;

        parse_result.map_err(SidemanticError::SqlParse)
    }
}

fn parse_select_expr(expr_sql: &str) -> Option<Expression> {
    let sql = format!("SELECT {expr_sql}");
    let statements = parse_sql_with_large_stack(&sql).ok()?;
    let statement = statements.first()?;
    let select = statement.as_select()?;
    select.expressions.first().cloned()
}

fn parse_where_expr(condition_sql: &str) -> Option<Expression> {
    let sql = format!("SELECT 1 WHERE {condition_sql}");
    let statements = parse_sql_with_large_stack(&sql).ok()?;
    let statement = statements.first()?;
    let select = statement.as_select()?;
    select.where_clause.as_ref().map(|w| w.this.clone())
}

fn expr_to_sql(expr: &Expression) -> Result<String> {
    polyglot_generate(expr, DialectType::Generic)
        .map_err(|e| SidemanticError::SqlGeneration(e.to_string()))
}

fn resolve_model_ref<'a>(
    table_or_alias: &str,
    model_refs: &'a [(String, String)],
) -> Option<(&'a str, &'a str)> {
    model_refs
        .iter()
        .find(|(model, alias)| model == table_or_alias || alias == table_or_alias)
        .map(|(model, alias)| (model.as_str(), alias.as_str()))
}

fn resolve_model_field<'a>(
    column: &'a polyglot_sql::expressions::Column,
    model_refs: &'a [(String, String)],
) -> Option<(&'a str, &'a str, &'a str, Option<&'a str>)> {
    let (base_field, granularity) = split_granularity(&column.name.name);

    if let Some(table) = &column.table {
        if let Some((model, alias)) = resolve_model_ref(&table.name, model_refs) {
            return Some((model, alias, base_field, granularity));
        }
        return None;
    }

    if model_refs.len() == 1 {
        let (model, alias) = (&model_refs[0].0, &model_refs[0].1);
        return Some((model.as_str(), alias.as_str(), base_field, granularity));
    }

    None
}

fn split_granularity(field: &str) -> (&str, Option<&str>) {
    const VALID_GRANULARITIES: [&str; 8] = [
        "year", "quarter", "month", "week", "day", "hour", "minute", "second",
    ];

    if let Some((base, gran)) = field.rsplit_once("__") {
        if VALID_GRANULARITIES.contains(&gran) {
            return (base, Some(gran));
        }
    }

    (field, None)
}

fn column_alias_name(
    column: &polyglot_sql::expressions::Column,
    model_refs: &[(String, String)],
) -> Option<String> {
    resolve_model_field(column, model_refs).map(|(_, _, _, _)| column.name.name.clone())
}

fn has_star_projection(projection: &[Expression]) -> bool {
    projection
        .iter()
        .any(|expr| matches!(expr, Expression::Star(_)))
}

fn extract_aggregate_input(expr: &Expression) -> Option<Expression> {
    match expr {
        Expression::Sum(agg)
        | Expression::Avg(agg)
        | Expression::Min(agg)
        | Expression::Max(agg)
        | Expression::Median(agg) => Some(agg.this.clone()),
        Expression::AggregateFunction(func) => {
            let func_name = func.name.to_uppercase();
            match func_name.as_str() {
                "SUM" | "AVG" | "MIN" | "MAX" | "MEDIAN" => func.args.first().cloned(),
                "COUNT" => {
                    if let Some(arg) = func.args.first() {
                        if matches!(arg, Expression::Star(_)) {
                            Some(Expression::number(1))
                        } else {
                            Some(arg.clone())
                        }
                    } else {
                        Some(Expression::number(1))
                    }
                }
                _ => None,
            }
        }
        Expression::Count(count) => {
            if count.star || count.this.is_none() {
                Some(Expression::number(1))
            } else {
                count.this.clone()
            }
        }
        _ => None,
    }
}

fn rebuild_aggregate_with_input(expr: &Expression, input: Expression) -> Option<Expression> {
    match expr.clone() {
        Expression::Sum(mut agg) => {
            agg.this = input;
            Some(Expression::Sum(agg))
        }
        Expression::Avg(mut agg) => {
            agg.this = input;
            Some(Expression::Avg(agg))
        }
        Expression::Min(mut agg) => {
            agg.this = input;
            Some(Expression::Min(agg))
        }
        Expression::Max(mut agg) => {
            agg.this = input;
            Some(Expression::Max(agg))
        }
        Expression::Median(mut agg) => {
            agg.this = input;
            Some(Expression::Median(agg))
        }
        Expression::Count(mut count) => {
            count.this = Some(input);
            count.star = false;
            Some(Expression::Count(count))
        }
        Expression::AggregateFunction(mut func) => {
            let func_name = func.name.to_uppercase();
            match func_name.as_str() {
                "SUM" | "AVG" | "MIN" | "MAX" | "MEDIAN" | "COUNT" => {
                    func.args = vec![input];
                    Some(Expression::AggregateFunction(func))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn is_from_metrics(from: Option<&From>) -> bool {
    let Some(from_clause) = from else {
        return false;
    };
    if from_clause.expressions.len() != 1 {
        return false;
    }
    matches!(
        &from_clause.expressions[0],
        Expression::Table(table) if table.name.name == "metrics"
    )
}

fn unique_alias(model_name: &str, existing: &HashSet<String>) -> String {
    let base = model_name.chars().next().unwrap_or('t').to_string();
    if !existing.contains(&base) {
        return base;
    }

    let mut i = 2;
    loop {
        let candidate = format!("{base}{i}");
        if !existing.contains(&candidate) {
            return candidate;
        }
        i += 1;
    }
}

fn dimension_to_expr(
    alias_name: &str,
    dimension: &crate::core::Dimension,
    granularity: Option<&str>,
) -> Expression {
    if let Some(gran) = granularity.filter(|_| dimension.r#type == DimensionType::Time) {
        let sql = format!(
            "DATE_TRUNC('{gran}', {}.{})",
            alias_name,
            dimension.sql_expr()
        );
        if let Some(expr) = parse_select_expr(&sql) {
            return expr;
        }
    }

    Expression::qualified_column(alias_name.to_string(), dimension.sql_expr().to_string())
}

fn table_name_and_alias(source: &Expression) -> Option<(String, Option<String>)> {
    match source {
        Expression::Table(table) => Some((
            table.name.name.clone(),
            table.alias.as_ref().map(|a| a.name.clone()),
        )),
        Expression::Alias(alias) => {
            if let Expression::Table(table) = &alias.this {
                Some((table.name.name.clone(), Some(alias.alias.name.clone())))
            } else {
                None
            }
        }
        Expression::Paren(paren) => table_name_and_alias(&paren.this),
        _ => None,
    }
}

fn table_ref_for(table_name: &str, alias: Option<&str>) -> TableRef {
    let mut table_ref = TableRef::new("");
    rewrite_table_ref_name(&mut table_ref, table_name);
    table_ref.alias = alias.map(Identifier::new);
    table_ref
}

fn rewrite_table_ref_name(table_ref: &mut TableRef, table_name: &str) {
    let parts: Vec<&str> = table_name.split('.').collect();
    match parts.len() {
        0 => {}
        1 => {
            table_ref.catalog = None;
            table_ref.schema = None;
            table_ref.name = Identifier::new(parts[0]);
        }
        2 => {
            table_ref.catalog = None;
            table_ref.schema = Some(Identifier::new(parts[0]));
            table_ref.name = Identifier::new(parts[1]);
        }
        _ => {
            table_ref.catalog = Some(Identifier::new(parts[parts.len() - 3]));
            table_ref.schema = Some(Identifier::new(parts[parts.len() - 2]));
            table_ref.name = Identifier::new(parts[parts.len() - 1]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Dimension, Metric, Model, Relationship};

    fn create_test_graph() -> SemanticGraph {
        let mut graph = SemanticGraph::new();

        let orders = Model::new("orders", "order_id")
            .with_table("public.orders")
            .with_dimension(Dimension::categorical("status"))
            .with_dimension(Dimension::time("order_date").with_sql("created_at"))
            .with_metric(Metric::sum("revenue", "amount"))
            .with_metric(Metric::count("order_count"))
            .with_relationship(Relationship::many_to_one("customers"));

        let customers = Model::new("customers", "id")
            .with_table("public.customers")
            .with_dimension(Dimension::categorical("name"))
            .with_dimension(Dimension::categorical("country"));

        graph.add_model(orders).unwrap();
        graph.add_model(customers).unwrap();

        graph
    }

    #[test]
    fn test_simple_rewrite() {
        let graph = create_test_graph();
        let rewriter = QueryRewriter::new(&graph);

        let sql = "SELECT orders.revenue, orders.status FROM orders";
        let rewritten = rewriter.rewrite(sql).unwrap();

        assert!(rewritten.contains("public.orders"));
        assert!(rewritten.contains("SUM("));
        assert!(rewritten.contains("GROUP BY"));
    }

    #[test]
    fn test_rewrite_with_alias() {
        let graph = create_test_graph();
        let rewriter = QueryRewriter::new(&graph);

        let sql = "SELECT o.revenue, o.status FROM orders AS o";
        let rewritten = rewriter.rewrite(sql).unwrap();

        assert!(rewritten.contains("public.orders"));
    }

    #[test]
    fn test_rewrite_with_filter() {
        let graph = create_test_graph();
        let rewriter = QueryRewriter::new(&graph);

        let sql = "SELECT orders.revenue FROM orders WHERE orders.status = 'completed'";
        let rewritten = rewriter.rewrite(sql).unwrap();

        assert!(rewritten.contains("WHERE"));
        assert!(rewritten.contains("status"));
    }

    #[test]
    fn test_cross_model_join() {
        let graph = create_test_graph();
        let rewriter = QueryRewriter::new(&graph);

        // Query orders metric with customers dimension - should auto-join
        let sql = "SELECT orders.revenue, customers.country FROM orders";
        let rewritten = rewriter.rewrite(sql).unwrap();

        // Should have JOIN clause
        assert!(
            rewritten.to_uppercase().contains("JOIN"),
            "Expected JOIN in: {rewritten}"
        );
        assert!(
            rewritten.contains("customers"),
            "Expected customers table in: {rewritten}"
        );
    }

    #[test]
    fn test_cross_model_composite_join() {
        let mut graph = SemanticGraph::new();

        let shipments = Model::new("shipments", "shipment_id")
            .with_table("public.shipments")
            .with_metric(Metric::count("shipment_count"))
            .with_relationship(Relationship::many_to_one("order_items").with_key_columns(
                vec!["order_id".to_string(), "item_id".to_string()],
                vec!["order_id".to_string(), "item_id".to_string()],
            ));
        let order_items = Model::new("order_items", "order_id")
            .with_primary_key_columns(vec!["order_id".to_string(), "item_id".to_string()])
            .with_table("public.order_items")
            .with_dimension(Dimension::categorical("sku"));

        graph.add_model(shipments).unwrap();
        graph.add_model(order_items).unwrap();

        let rewriter = QueryRewriter::new(&graph);
        let sql = "SELECT shipments.shipment_count, order_items.sku FROM shipments";
        let rewritten = rewriter.rewrite(sql).unwrap();

        assert!(rewritten.contains("shipments.order_id = o.order_id"));
        assert!(rewritten.contains("shipments.item_id = o.item_id"));
        assert!(rewritten.to_uppercase().contains(" AND "));
    }

    #[test]
    fn test_cross_model_join_in_where() {
        let graph = create_test_graph();
        let rewriter = QueryRewriter::new(&graph);

        // Model referenced only in WHERE should still trigger JOIN
        let sql = "SELECT orders.revenue FROM orders WHERE customers.country = 'US'";
        let rewritten = rewriter.rewrite(sql).unwrap();

        // Should have JOIN clause even though customers only in WHERE
        assert!(
            rewritten.to_uppercase().contains("JOIN"),
            "Expected JOIN in: {rewritten}"
        );
        assert!(
            rewritten.contains("customers"),
            "Expected customers table in: {rewritten}"
        );
    }

    #[test]
    fn test_count_without_sql() {
        // Test COUNT metric without explicit sql (simulates parsed definition)
        let mut graph = SemanticGraph::new();

        // Create metric with sql: None (like what SQL parser produces)
        let mut count_metric = Metric::new("order_count");
        count_metric.agg = Some(crate::core::Aggregation::Count);
        count_metric.sql = None; // Explicit None to simulate parsed metric

        let orders = Model::new("orders", "order_id")
            .with_table("orders")
            .with_dimension(Dimension::categorical("status"))
            .with_metric(count_metric);

        graph.add_model(orders).unwrap();
        let rewriter = QueryRewriter::new(&graph);

        let sql = "SELECT orders.order_count FROM orders";
        let rewritten = rewriter.rewrite(sql).unwrap();

        // Should be COUNT(*) not COUNT(order_count)
        assert!(
            rewritten.contains("COUNT(*)"),
            "Expected COUNT(*) but got: {rewritten}"
        );
        assert!(
            !rewritten.contains("COUNT(order_count)"),
            "Should not count order_count column directly: {rewritten}"
        );
    }

    #[test]
    fn test_wrap_simple_select_with_cte_preserves_composite_primary_keys() {
        let mut graph = SemanticGraph::new();

        let order_items = Model::new("order_items", "order_id")
            .with_primary_key_columns(vec!["order_id".to_string(), "item_id".to_string()])
            .with_table("public.order_items")
            .with_dimension(Dimension::categorical("sku"))
            .with_metric(Metric::sum("item_revenue", "amount"));

        graph.add_model(order_items).unwrap();
        let rewriter = QueryRewriter::new(&graph);

        let sql = "SELECT order_items.sku, order_items.item_revenue FROM order_items";
        let rewritten = rewriter.rewrite(sql).unwrap();

        assert!(rewritten.contains("order_items.order_id AS order_id"));
        assert!(rewritten.contains("order_items.item_id AS item_id"));
        assert!(rewritten.contains("GROUP BY 1"));
    }
}
