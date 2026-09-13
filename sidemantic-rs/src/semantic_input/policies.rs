//! Request-scoped policy preparation over the complete semantic population.
//!
//! Prepare once before an aggregate plan splits a request into source queries.
//! Every child must retain these predicates so a policy on a denominator model
//! also constrains the numerator's joined population.

use std::collections::{BTreeSet, HashMap, HashSet};

use polyglot_sql::expressions::{Column, Expression, Identifier};
use polyglot_sql::DialectType;
use serde_json::{Map, Value};

use crate::core::{
    parse_semantic_expression, Metric, MetricType, Model, SecurityPolicy, SemanticGraph,
};
use crate::error::{Result, SidemanticError};
use crate::sql::SemanticQuery;

#[derive(Debug, Clone, Default)]
pub(crate) struct ModelPolicies {
    pub security: Option<SecurityPolicy>,
    pub invariant_filters: Vec<String>,
}

use crate::core::PreparedPolicies;

/// Decode only policy declarations; the ordinary decoder still validates every
/// other model field. Keep this map beside the graph, keyed by canonical model.
pub(crate) fn decode(models: &[Value]) -> Result<HashMap<String, ModelPolicies>> {
    let mut policies = HashMap::new();
    for model in models {
        let name = model
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| SidemanticError::Validation("Policy model requires a name".into()))?;
        let security = match model.get("security") {
            Some(value) if !value.is_null() => {
                Some(serde_json::from_value(value.clone()).map_err(|error| {
                    SidemanticError::Validation(format!("Model '{name}' security: {error}"))
                })?)
            }
            _ => None,
        };
        let invariant_filters = match model.get("invariant_filters") {
            Some(value) => serde_json::from_value(value.clone()).map_err(|error| {
                SidemanticError::Validation(format!("Model '{name}' invariant filters: {error}"))
            })?,
            None => Vec::new(),
        };
        policies.insert(
            name.to_owned(),
            ModelPolicies {
                security,
                invariant_filters,
            },
        );
    }
    Ok(policies)
}

pub(crate) fn prepare(
    graph: &SemanticGraph,
    policies: &HashMap<String, ModelPolicies>,
    query: &SemanticQuery,
    user_attributes: Option<&Map<String, Value>>,
    enforce_visibility: bool,
    output_dialect: DialectType,
) -> Result<PreparedPolicies> {
    let mut population = Population::new(graph);
    for reference in &query.metrics {
        population.metric(reference, None)?;
    }
    for reference in &query.dimensions {
        population.field(reference)?;
    }
    for reference in &query.segments {
        if let Some((model, name)) = reference.split_once('.') {
            population.add_model(model);
            if let Some(segment) = graph
                .get_model(model)
                .and_then(|model| model.get_segment(name))
            {
                population.expression(&segment.get_sql(model), Some(model))?;
            }
        }
    }
    for filter in &query.filters {
        population.expression(filter, None)?;
    }
    for order in &query.order_by {
        for column in order_columns(order)? {
            population.column(&column, None)?;
        }
    }

    // Include intermediate models on all required paths. Graph aggregate leaves
    // may belong to several models and must be checked together, not per child.
    let required: Vec<_> = population.models.iter().cloned().collect();
    let query_models = required.iter().cloned().collect();
    for (index, from) in required.iter().enumerate() {
        for to in required.iter().skip(index + 1) {
            let path = graph.find_join_path_with_context(from, to, Some(&query_models))?;
            for step in path.steps {
                population.models.insert(step.from_model);
                population.models.insert(step.to_model);
            }
        }
    }

    if enforce_visibility {
        check_visibility(graph, query, &population.models, &population.metric_models)?;
    }
    let mut prepared = PreparedPolicies::default();
    for instance in &population.models {
        let Some(model) = graph.get_model(instance) else {
            continue;
        };
        // A role instance points at the canonical definition while retaining its
        // own SQL identity. Apply canonical policies to every used role instance.
        let Some(policy) = policies.get(&model.name) else {
            continue;
        };
        if let Some(security) = &policy.security {
            let rendered = security
                .render_for_model(instance, user_attributes)
                .map_err(|error| SidemanticError::Security(error.to_string()))?;
            if !rendered.is_empty() {
                let qualified = rendered
                    .iter()
                    .map(|filter| {
                        source_predicate(&filter.replace("{model}", instance), instance, model)
                    })
                    .collect::<Result<Vec<_>>>()?;
                prepared.row_filters.insert(instance.clone(), qualified);
            }
        }
        if !policy.invariant_filters.is_empty() {
            let qualified = policy
                .invariant_filters
                .iter()
                .map(|filter| {
                    source_predicate(&filter.replace("{model}", instance), instance, model)
                })
                .collect::<Result<Vec<_>>>()?;
            prepared
                .invariant_filters
                .insert(instance.clone(), qualified);
        }
    }
    if !matches!(
        output_dialect,
        DialectType::DuckDB | DialectType::PostgreSQL
    ) && prepared.model_names().next().is_some()
    {
        return Err(SidemanticError::UnsupportedSemanticFeatures {
            capabilities: vec![format!("policy.output_dialect.{output_dialect}")],
        });
    }
    Ok(prepared)
}

struct Population<'a> {
    graph: &'a SemanticGraph,
    models: BTreeSet<String>,
    metric_models: BTreeSet<String>,
    visited: HashSet<(Option<String>, String)>,
}

impl<'a> Population<'a> {
    fn new(graph: &'a SemanticGraph) -> Self {
        Self {
            graph,
            models: BTreeSet::new(),
            metric_models: BTreeSet::new(),
            visited: HashSet::new(),
        }
    }

    fn add_model(&mut self, model: &str) {
        if self.graph.get_model(model).is_some() {
            self.models.insert(model.to_owned());
            if let Some(root) = self.graph.role_root_owner(model) {
                self.models.insert(root.to_owned());
            }
        }
    }

    fn resolve_metric(
        &self,
        reference: &str,
        context: Option<&str>,
    ) -> Result<Option<(Option<String>, Metric)>> {
        if let Some((model, name)) = reference.split_once('.') {
            return Ok(self.graph.get_model(model).and_then(|definition| {
                definition
                    .get_metric(name)
                    .map(|metric| (Some(model.to_owned()), metric.clone()))
            }));
        }
        if let Some(model) = context {
            if let Some(metric) = self
                .graph
                .get_model(model)
                .and_then(|definition| definition.get_metric(reference))
            {
                return Ok(Some((Some(model.to_owned()), metric.clone())));
            }
        }
        if let Some(metric) = self.graph.get_metric(reference) {
            return Ok(Some((
                self.graph.metric_owner(reference).map(str::to_owned),
                metric.clone(),
            )));
        }
        let owners: Vec<_> = self
            .graph
            .models()
            .filter(|model| model.get_metric(reference).is_some())
            .collect();
        if owners.len() > 1 {
            return Err(SidemanticError::AmbiguousReference {
                field: reference.to_owned(),
                models: owners
                    .iter()
                    .map(|model| model.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
            });
        }
        Ok(owners.first().map(|model| {
            (
                Some(model.name.clone()),
                model.get_metric(reference).unwrap().clone(),
            )
        }))
    }

    fn metric(&mut self, reference: &str, context: Option<&str>) -> Result<()> {
        let Some((owner, metric)) = self.resolve_metric(reference, context)? else {
            return Ok(());
        };
        if !self.visited.insert((owner.clone(), metric.name.clone())) {
            return Ok(());
        }
        if let Some(owner) = &owner {
            self.add_model(owner);
            self.metric_models.insert(owner.clone());
        }
        match metric.r#type {
            MetricType::Ratio => {
                for reference in [metric.numerator.as_deref(), metric.denominator.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    self.metric(reference, owner.as_deref())?;
                }
            }
            MetricType::TimeComparison => {
                if let Some(reference) = metric.base_metric.as_deref() {
                    self.metric(reference, owner.as_deref())?;
                }
            }
            _ => {
                if let Some(sql) = metric.sql.as_deref().filter(|sql| *sql != "*") {
                    if metric.agg.is_some() || metric.sql_is_complete {
                        for column in contextual_columns(sql, owner.as_deref())? {
                            if let Some(model) = column.table.as_ref() {
                                self.add_model(&model.name);
                            }
                        }
                    } else {
                        self.expression(sql, owner.as_deref())?;
                    }
                }
            }
        }
        for filter in &metric.filters {
            self.expression(filter, owner.as_deref())?;
        }
        Ok(())
    }

    fn field(&mut self, reference: &str) -> Result<()> {
        if let Some((model, name)) = reference.split_once('.') {
            self.add_model(model);
            let name = base_field(name);
            if let Some(sql) = self
                .graph
                .get_model(model)
                .and_then(|definition| definition.get_dimension(name))
                .and_then(|dimension| dimension.sql.clone())
            {
                self.expression(&sql, Some(model))?;
            }
        }
        Ok(())
    }

    fn expression(&mut self, sql: &str, context: Option<&str>) -> Result<()> {
        for column in contextual_columns(sql, context)? {
            self.column(&column, context)?;
        }
        Ok(())
    }

    fn column(&mut self, column: &Column, context: Option<&str>) -> Result<()> {
        let model = column
            .table
            .as_ref()
            .map(|table| table.name.as_str())
            .or(context);
        if let Some(model) = model {
            self.add_model(model);
        }
        let reference = column.table.as_ref().map_or_else(
            || column.name.name.clone(),
            |table| format!("{}.{}", table.name, column.name.name),
        );
        // Bare physical filter columns must not bind to unrelated model metrics.
        if model.is_some() || self.graph.get_metric(&reference).is_some() {
            self.metric(&reference, context)?;
        }
        Ok(())
    }
}

fn contextual_columns(sql: &str, context: Option<&str>) -> Result<Vec<Column>> {
    let scoped_sql =
        context.map(|model| sql.replace("{model}", &format!("\"{}\"", model.replace('"', "\"\""))));
    outer_columns(parse_semantic_expression(
        scoped_sql.as_deref().unwrap_or(sql),
    )?)
}

fn base_field(field: &str) -> &str {
    field.rsplit_once("__").map_or(field, |(field, _)| field)
}

fn check_visibility(
    graph: &SemanticGraph,
    query: &SemanticQuery,
    candidates: &BTreeSet<String>,
    metric_models: &BTreeSet<String>,
) -> Result<()> {
    let check = |model: &str, name: &str| -> Result<()> {
        let Some(model_definition) = graph.get_model(model) else {
            return Ok(());
        };
        let name = base_field(name);
        let public = model_definition
            .get_dimension(name)
            .map(|dimension| dimension.public)
            .or_else(|| {
                model_definition
                    .get_metric(name)
                    .map(|metric| metric.public)
            })
            .unwrap_or(true);
        if !public {
            return Err(SidemanticError::Security(format!(
                "Field '{model}.{name}' is not public"
            )));
        }
        Ok(())
    };
    for reference in query.metrics.iter().chain(&query.dimensions) {
        if let Some((model, name)) = reference.split_once('.') {
            check(model, name)?;
        } else if graph
            .get_metric(reference)
            .is_some_and(|metric| !metric.public)
        {
            return Err(SidemanticError::Security(format!(
                "Field '{reference}' is not public"
            )));
        } else if graph.get_metric(reference).is_none() {
            for model in candidates {
                check(model, reference)?;
            }
        }
    }
    for reference in &query.segments {
        if let Some((model, name)) = reference.split_once('.') {
            if graph
                .get_model(model)
                .and_then(|model| model.get_segment(name))
                .is_some_and(|segment| !segment.public)
            {
                return Err(SidemanticError::Security(format!(
                    "Segment '{reference}' is not public"
                )));
            }
        }
    }
    if !query.skip_default_time_dimensions {
        for model in metric_models {
            if let Some(default_time) = graph
                .get_model(model)
                .and_then(|model| model.default_time_dimension.as_deref())
            {
                let has_time = query
                    .dimensions
                    .iter()
                    .filter_map(|reference| reference.split_once('.'))
                    .any(|(owner, field)| {
                        owner == model
                            && graph
                                .get_model(owner)
                                .and_then(|model| model.get_dimension(base_field(field)))
                                .is_some_and(|dimension| {
                                    dimension.r#type == crate::core::DimensionType::Time
                                })
                    });
                if !has_time {
                    check(model, default_time)?;
                }
            }
        }
    }
    let mut columns = Vec::new();
    for filter in &query.filters {
        columns.extend(outer_columns(parse_semantic_expression(filter)?)?);
    }
    for order in &query.order_by {
        columns.extend(order_columns(order)?);
    }
    for column in columns {
        if let Some(model) = column
            .table
            .as_ref()
            .filter(|model| graph.get_model(&model.name).is_some())
        {
            check(&model.name, &column.name.name)?;
        } else {
            if graph
                .get_metric(&column.name.name)
                .is_some_and(|metric| !metric.public)
            {
                return Err(SidemanticError::Security(format!(
                    "Field '{}' is not public",
                    column.name.name
                )));
            }
            for model in candidates {
                check(model, &column.name.name)?;
            }
        }
    }
    Ok(())
}

fn order_columns(order: &str) -> Result<Vec<Column>> {
    let expression =
        polyglot_sql::parse_one(&format!("SELECT 1 ORDER BY {order}"), DialectType::DuckDB)
            .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
    outer_columns(expression)
}

fn outer_columns(expression: Expression) -> Result<Vec<Column>> {
    fn visit(value: &Value, root: bool, columns: &mut Vec<Column>) -> Result<()> {
        match value {
            Value::Object(fields) => {
                let kind = (fields.len() == 1).then(|| fields.keys().next().unwrap().as_str());
                if !root && matches!(kind, Some("select" | "subquery")) {
                    return Ok(());
                }
                if kind == Some("column") {
                    columns.push(
                        serde_json::from_value(fields["column"].clone())
                            .map_err(|error| SidemanticError::SqlParse(error.to_string()))?,
                    );
                } else {
                    for child in fields.values() {
                        visit(child, false, columns)?;
                    }
                }
            }
            Value::Array(values) => {
                for child in values {
                    visit(child, false, columns)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    let ast = serde_json::to_value(expression)
        .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
    let mut columns = Vec::new();
    visit(&ast, true, &mut columns)?;
    Ok(columns)
}

/// Resolve policy fields against the source CTE without rewriting SQL text.
/// Subquery scopes retain their own columns and literals remain byte-exact.
fn source_predicate(predicate: &str, instance: &str, model: &Model) -> Result<String> {
    fn expand(value: &mut Value, instance: &str, model: &Model, dimensions: bool) -> Result<()> {
        match value {
            Value::Object(fields) => {
                let kind = (fields.len() == 1).then(|| fields.keys().next().unwrap().as_str());
                if matches!(kind, Some("select" | "subquery")) {
                    return Ok(());
                }
                if kind == Some("column") {
                    let mut column: Column = serde_json::from_value(fields["column"].clone())
                        .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
                    if column
                        .table
                        .as_ref()
                        .is_some_and(|table| table.name != instance && table.name != model.name)
                    {
                        return Err(SidemanticError::UnsupportedSemanticFeatures {
                            capabilities: vec!["policy.cross_model_predicate".into()],
                        });
                    }
                    if dimensions {
                        if let Some(dimension) = model.get_dimension(&column.name.name) {
                            if let Some(sql) = &dimension.sql {
                                let expression =
                                    parse_semantic_expression(&sql.replace("{model}", instance))?;
                                let mut replacement =
                                    serde_json::to_value(expression).map_err(|error| {
                                        SidemanticError::SqlParse(error.to_string())
                                    })?;
                                expand(&mut replacement, instance, model, false)?;
                                *value = replacement;
                                return Ok(());
                            }
                        }
                    }
                    column.table = None;
                    fields.insert(
                        "column".into(),
                        serde_json::to_value(column)
                            .map_err(|error| SidemanticError::SqlParse(error.to_string()))?,
                    );
                } else {
                    for child in fields.values_mut() {
                        expand(child, instance, model, dimensions)?;
                    }
                }
            }
            Value::Array(values) => {
                for child in values {
                    expand(child, instance, model, dimensions)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    let predicate = qualify_predicate(predicate, instance, &model.name)?;
    let mut ast = serde_json::to_value(parse_semantic_expression(&predicate)?)
        .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
    expand(&mut ast, instance, model, true)?;
    let expression: Expression = serde_json::from_value(ast)
        .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
    polyglot_sql::generate(&expression, DialectType::DuckDB)
        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))
}

fn qualify_predicate(predicate: &str, model: &str, canonical_model: &str) -> Result<String> {
    fn qualify(value: &mut Value, model: &str, canonical_model: &str) -> Result<()> {
        match value {
            Value::Object(fields) => {
                let kind = (fields.len() == 1).then(|| fields.keys().next().unwrap().as_str());
                if matches!(kind, Some("select" | "subquery")) {
                    return Ok(());
                }
                if kind == Some("column") {
                    let mut column: Column = serde_json::from_value(fields["column"].clone())
                        .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
                    if column
                        .table
                        .as_ref()
                        .is_none_or(|table| table.name == canonical_model)
                    {
                        column.table = Some(Identifier::quoted(model));
                    }
                    fields.insert(
                        "column".into(),
                        serde_json::to_value(column)
                            .map_err(|error| SidemanticError::SqlParse(error.to_string()))?,
                    );
                } else {
                    for child in fields.values_mut() {
                        qualify(child, model, canonical_model)?;
                    }
                }
            }
            Value::Array(values) => {
                for child in values {
                    qualify(child, model, canonical_model)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    let expression = parse_semantic_expression(predicate).map_err(|error| {
        SidemanticError::Security(format!(
            "Policy predicate for model '{model}' failed to parse: {error}"
        ))
    })?;
    let mut ast = serde_json::to_value(expression)
        .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
    qualify(&mut ast, model, canonical_model)?;
    let expression: Expression = serde_json::from_value(ast)
        .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
    polyglot_sql::generate(&expression, DialectType::DuckDB)
        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Dimension, Model, Relationship, Segment};
    use serde_json::json;

    fn graph() -> SemanticGraph {
        let mut graph = SemanticGraph::new();
        graph
            .add_model(
                Model::new("orders", "id")
                    .with_table("orders")
                    .with_metric(Metric::sum("revenue", "amount"))
                    .with_segment(Segment::new("enterprise", "accounts.tier = 'enterprise'"))
                    .with_relationship(
                        Relationship::many_to_one("accounts").with_keys("account_id", "id"),
                    ),
            )
            .unwrap();
        graph
            .add_model(
                Model::new("accounts", "id")
                    .with_table("accounts")
                    .with_metric(Metric::sum("quota", "quota"))
                    .with_dimension(Dimension::categorical("tier")),
            )
            .unwrap();
        graph
            .add_metric(Metric::ratio("ratio", "orders.revenue", "accounts.quota"))
            .unwrap();
        graph
    }

    #[test]
    fn cube_measure_placeholders_preserve_raw_and_semantic_population() {
        let mut graph = SemanticGraph::new();
        graph
            .add_model(Model::new("orders", "id").with_table("orders").with_metric(
                Metric::sum("amount", "{model}.amount").with_filter("{model}.amount >= 100"),
            ))
            .unwrap();
        let mut population = Population::new(&graph);
        population.metric("orders.amount", None).unwrap();
        assert_eq!(population.models, BTreeSet::from(["orders".into()]));
        assert_eq!(population.visited.len(), 1);
        let columns = contextual_columns("{model}.amount >= 100", Some("orders")).unwrap();
        assert_eq!(columns[0].table.as_ref().unwrap().name, "orders");
        assert_eq!(columns[0].name.name, "amount");
    }

    #[test]
    fn whole_ratio_population_receives_both_models_policies() {
        let graph = graph();
        let policies = decode(&[
            json!({"name":"orders", "invariant_filters":["not deleted"]}),
            json!({"name":"accounts", "security":{"row_filters":["tenant = {{ user.tenant }}"]}, "invariant_filters":["active"]}),
        ]).unwrap();
        let attrs = json!({"tenant":1});
        let prepared = prepare(
            &graph,
            &policies,
            &SemanticQuery::new().with_metrics(vec!["ratio".into()]),
            attrs.as_object(),
            false,
            DialectType::DuckDB,
        )
        .unwrap();
        assert!(prepared.has_row_filters());
        assert!(prepared.invariant_filters["orders"][0].contains("deleted"));
        assert!(prepared.invariant_filters["accounts"][0].contains("active"));
        assert!(prepared.row_filters["accounts"][0].contains("tenant = 1"));
        assert!(prepare(
            &graph,
            &policies,
            &SemanticQuery::new().with_metrics(vec!["ratio".into()]),
            None,
            false,
            DialectType::DuckDB
        )
        .is_err());
    }

    #[test]
    fn postgres_policy_output_preserves_predicates_and_other_dialects_stay_gated() {
        let graph = graph();
        let policies = decode(&[json!({
            "name":"orders", "security":{"row_filters":["tenant = {{ user.tenant }}"]},
            "invariant_filters":["not deleted"]
        })])
        .unwrap();
        let query = SemanticQuery::new().with_metrics(vec!["orders.revenue".into()]);
        let attributes = json!({"tenant": 1});
        let prepared = prepare(
            &graph,
            &policies,
            &query,
            attributes.as_object(),
            false,
            DialectType::PostgreSQL,
        )
        .unwrap();
        assert!(prepared.row_filters["orders"][0].contains("tenant = 1"));
        assert!(prepared.invariant_filters["orders"][0].contains("deleted"));
        assert!(matches!(
            prepare(
                &graph,
                &policies,
                &query,
                attributes.as_object(),
                false,
                DialectType::BigQuery
            ),
            Err(SidemanticError::UnsupportedSemanticFeatures { .. })
        ));
    }

    #[test]
    fn policy_qualification_keeps_subquery_local_columns() {
        let filter =
            qualify_predicate("id IN (SELECT id FROM allowed)", "orders", "orders").unwrap();
        assert!(filter.contains("\"orders\".id"), "{filter}");
        assert!(filter.contains("SELECT id FROM allowed"), "{filter}");
        assert!(!filter.contains("SELECT \"orders\".id"), "{filter}");
    }

    #[test]
    fn canonical_policy_qualifiers_follow_role_instances() {
        let filter = qualify_predicate(
            "accounts.tenant = 1 AND label = 'accounts.tenant'",
            "buyer",
            "accounts",
        )
        .unwrap();
        assert!(filter.contains("\"buyer\".tenant = 1"), "{filter}");
        assert!(
            filter.contains("\"buyer\".label = 'accounts.tenant'"),
            "{filter}"
        );
    }

    #[test]
    fn source_policy_expands_dimensions_without_touching_subqueries_or_literals() {
        let model = Model::new("accounts", "id")
            .with_dimension(Dimension::categorical("tenant").with_sql("tenant_id"));
        let filter = source_predicate(
            "accounts.tenant = 1 AND label = 'accounts.tenant' AND id IN (SELECT id FROM allowed)",
            "buyer",
            &model,
        )
        .unwrap();
        assert!(filter.contains("tenant_id = 1"), "{filter}");
        assert!(filter.contains("label = 'accounts.tenant'"), "{filter}");
        assert!(filter.contains("SELECT id FROM allowed"), "{filter}");
        assert!(!filter.contains("buyer"), "{filter}");
    }

    #[test]
    fn segment_dependencies_require_joined_model_policy() {
        let graph = graph();
        let policies = decode(&[json!({"name":"accounts", "security":{}})]).unwrap();
        let mut query = SemanticQuery::new().with_metrics(vec!["orders.revenue".into()]);
        query.segments = vec!["orders.enterprise".into()];
        assert!(matches!(
            prepare(&graph, &policies, &query, None, false, DialectType::DuckDB),
            Err(SidemanticError::Security(_))
        ));
    }

    #[test]
    fn unrelated_policy_does_not_require_attributes() {
        let graph = graph();
        let policies = decode(&[json!({"name":"accounts", "security":{}})]).unwrap();
        assert!(prepare(
            &graph,
            &policies,
            &SemanticQuery::new().with_metrics(vec!["orders.revenue".into()]),
            None,
            false,
            DialectType::DuckDB
        )
        .is_ok());
    }
}
