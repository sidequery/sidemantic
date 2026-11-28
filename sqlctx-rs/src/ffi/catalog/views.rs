//! Named selections retain semantic fields; they never persist an aggregated SQL result.
use std::collections::{HashMap, HashSet};

use polyglot_sql::{expressions::Identifier, DialectType, Expression};
use serde_json::{json, Value};

use super::*;

const VIEW_KEY: &str = "sqlctx_semantic_view";

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ViewDefinition {
    pub source: String,
    pub fields: Vec<ViewField>,
    pub sql: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ViewField {
    pub name: String,
    pub source: String,
    pub kind: String,
}

pub(super) fn definition(model: &Model) -> CatalogResult<Option<ViewDefinition>> {
    model
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get(VIEW_KEY))
        .map(|value| {
            serde_json::from_value(value.clone())
                .map_err(|error| format!("invalid semantic view '{}': {error}", model.name))
        })
        .transpose()
}

fn source_field(source: &Model, name: &str) -> CatalogResult<ViewField> {
    if let Some(field) = source
        .dimensions
        .iter()
        .find(|field| field.name.eq_ignore_ascii_case(name))
    {
        return Ok(ViewField {
            name: field.name.clone(),
            source: field.name.clone(),
            kind: "dimension".into(),
        });
    }
    if let Some(field) = source
        .metrics
        .iter()
        .find(|field| field.name.eq_ignore_ascii_case(name))
    {
        return Ok(ViewField {
            name: field.name.clone(),
            source: field.name.clone(),
            kind: "metric".into(),
        });
    }
    Err(format!(
        "semantic view source '{}' has no dimension or metric '{name}'",
        source.name
    ))
}

pub(super) fn create(snapshot: &mut Snapshot, content: &str, replace: bool) -> CatalogResult<()> {
    let (name, rest) = declaration_identifier(content).ok_or("semantic view requires a name")?;
    let (keyword, sql) = declaration_identifier(rest).ok_or("semantic view requires AS SELECT")?;
    if !keyword.eq_ignore_ascii_case("AS") {
        return Err("semantic view requires AS SELECT".into());
    }
    let ast = crate::semantic_input::dialects::parse(sql, DialectType::DuckDB)
        .map_err(|error| error.to_string())?;
    let Expression::Select(select) = ast else {
        return Err("semantic view requires a SELECT projection".into());
    };
    // A semantic selection has no fixed grain, ordering or row limit. Reject
    // SQL shapes whose ordinary relational meaning would freeze that grain.
    let mut shape = (*select).clone();
    shape.expressions.clear();
    shape.from = None;
    shape.leading_comments.clear();
    shape.post_select_comments.clear();
    if shape != polyglot_sql::expressions::Select::new() {
        return Err("semantic view expects dimensions and metrics from one source, without WHERE, GROUP BY, ORDER BY or LIMIT".into());
    }
    let sources = select
        .from
        .as_ref()
        .map(|from| from.expressions.as_slice())
        .unwrap_or_default();
    let [Expression::Table(table)] = sources else {
        return Err("semantic view requires exactly one model or semantic view source".into());
    };
    if table.catalog.is_some()
        || table
            .schema
            .as_ref()
            .is_some_and(|schema| !schema.name.eq_ignore_ascii_case("semantic"))
        || !table.column_aliases.is_empty()
    {
        return Err(
            "semantic view source must be an unqualified model/view or semantic.<name>".into(),
        );
    }
    let source = snapshot
        .models
        .iter()
        .find(|model| model.name.eq_ignore_ascii_case(&table.name.name))
        .ok_or_else(|| format!("semantic view source '{}' not found", table.name.name))?;
    let qualifier = table.alias.as_ref().unwrap_or(&table.name);
    let mut fields = Vec::new();
    for expression in &select.expressions {
        let (expression, alias) = match expression {
            Expression::Alias(alias) => (&alias.this, Some(&alias.alias.name)),
            other => (other, None),
        };
        match expression {
            Expression::Column(column) if column.table.as_ref().is_none_or(|table| table.name.eq_ignore_ascii_case(&qualifier.name)) => {
                let mut field = source_field(source, &column.name.name)?;
                if let Some(alias) = alias { field.name = alias.clone(); }
                fields.push(field);
            }
            Expression::Star(star) if alias.is_none()
                && star.table.as_ref().is_none_or(|table| table.name.eq_ignore_ascii_case(&qualifier.name))
                && star.except.is_none() && star.replace.is_none() && star.rename.is_none() => {
                if let Some(view) = self::definition(source)? {
                    for field in view.fields { fields.push(source_field(source, &field.name)?); }
                } else {
                    for name in source.dimensions.iter().map(|field| &field.name).chain(source.metrics.iter().map(|field| &field.name)) {
                        fields.push(source_field(source, name)?);
                    }
                }
            }
            _ => return Err("semantic view selections must be dimension or metric references, optionally renamed with AS".into()),
        }
    }
    let mut names = HashSet::new();
    if fields.is_empty()
        || fields
            .iter()
            .any(|field| !names.insert(field.name.to_ascii_lowercase()))
    {
        return Err("semantic view requires nonempty, unique output names".into());
    }
    let definition = ViewDefinition {
        source: source.name.clone(),
        fields,
        sql: sql.trim().to_owned(),
    };
    let existing = snapshot
        .models
        .iter()
        .position(|model| model.name.eq_ignore_ascii_case(&name));
    let canonical_name = if let Some(index) = existing {
        if self::definition(&snapshot.models[index])?.is_none() {
            return Err(format!("'{name}' is a model, not a semantic view"));
        }
        if !replace {
            return Err(format!("semantic view '{name}' already exists"));
        }
        snapshot.models[index].name.clone()
    } else {
        name
    };
    // Discovery describes the authored selection. A semantic view has no
    // physical table or fixed primary key because its grain belongs to a query.
    let mut model = Model::new(canonical_name, "").with_sql(definition.sql.clone());
    model.metadata = Some(json!({VIEW_KEY: definition}));
    if let Some(index) = existing {
        snapshot.models[index] = model;
    } else {
        snapshot.models.push(model);
    }
    refresh(&mut snapshot.models)
}

/// Rebind selections on every reconstruction. Compatible source replacements
/// flow through existing views; removed fields, changed field kinds and cycles
/// fail before the host can publish a candidate catalog.
pub(super) fn refresh(models: &mut [Model]) -> CatalogResult<()> {
    fn resolve(
        index: usize,
        models: &mut [Model],
        resolved: &mut HashSet<usize>,
        visiting: &mut HashSet<usize>,
    ) -> CatalogResult<()> {
        if resolved.contains(&index) {
            return Ok(());
        }
        let Some(view) = definition(&models[index])? else {
            resolved.insert(index);
            return Ok(());
        };
        if !visiting.insert(index) {
            return Err(format!(
                "cyclic semantic view dependency involving '{}'",
                models[index].name
            ));
        }
        let source_index = models
            .iter()
            .position(|model| model.name.eq_ignore_ascii_case(&view.source))
            .ok_or_else(|| {
                format!(
                    "semantic view '{}' depends on missing source '{}' (RESTRICT)",
                    models[index].name, view.source
                )
            })?;
        resolve(source_index, models, resolved, visiting)?;
        let source = models[source_index].clone();
        let model = &mut models[index];
        model.dimensions.clear();
        model.metrics.clear();
        for field in &view.fields {
            let current = source_field(&source, &field.source).map_err(|error| {
                format!(
                    "semantic view '{}' dependency: {error} (RESTRICT)",
                    model.name
                )
            })?;
            if current.kind != field.kind {
                return Err(format!(
                    "semantic view '{}' field '{}' changed semantic kind (RESTRICT)",
                    model.name, field.source
                ));
            }
            if field.kind == "dimension" {
                let mut dimension = source
                    .dimensions
                    .iter()
                    .find(|item| item.name == current.source)
                    .unwrap()
                    .clone();
                dimension.name = field.name.clone();
                model.dimensions.push(dimension);
            } else {
                let mut metric = source
                    .metrics
                    .iter()
                    .find(|item| item.name == current.source)
                    .unwrap()
                    .clone();
                metric.name = field.name.clone();
                model.metrics.push(metric);
            }
        }
        visiting.remove(&index);
        resolved.insert(index);
        Ok(())
    }
    let mut resolved = HashSet::new();
    for index in 0..models.len() {
        resolve(index, models, &mut resolved, &mut HashSet::new())?;
    }
    Ok(())
}

pub(super) fn restrict(
    snapshot: &Snapshot,
    source: &str,
    field: Option<&str>,
) -> CatalogResult<()> {
    for model in &snapshot.models {
        if let Some(view) = definition(model)? {
            if view.source.eq_ignore_ascii_case(source)
                && field.is_none_or(|field| {
                    view.fields
                        .iter()
                        .any(|item| item.source.eq_ignore_ascii_case(field))
                })
            {
                return Err(format!(
                    "cannot drop '{source}': dependent semantic view '{}' (RESTRICT)",
                    model.name
                ));
            }
        }
    }
    Ok(())
}

#[derive(Clone)]
struct Binding {
    source: String,
    fields: Vec<ViewField>,
}

fn bindings(graph: &SemanticGraph) -> CatalogResult<HashMap<String, Binding>> {
    let mut result = HashMap::new();
    for model in graph.models() {
        let Some(view) = definition(model)? else {
            continue;
        };
        let mut binding = Binding {
            source: view.source,
            fields: view.fields,
        };
        while let Some(parent) = definition(
            graph
                .get_model(&binding.source)
                .ok_or("missing semantic view source")?,
        )? {
            for field in &mut binding.fields {
                field.source = parent
                    .fields
                    .iter()
                    .find(|parent| parent.name.eq_ignore_ascii_case(&field.source))
                    .ok_or("missing nested semantic view field")?
                    .source
                    .clone();
            }
            binding.source = parent.source;
        }
        result.insert(model.name.to_ascii_lowercase(), binding);
    }
    Ok(result)
}

/// Resolve immutable catalog state once, independently of each query. View
/// descriptors belong to discovery, not source metric resolution; catalogs
/// without views reuse their original graph without allocating another copy.
pub(super) struct PreparedViews {
    bindings: HashMap<String, Binding>,
    source_graph: Option<SemanticGraph>,
}

impl PreparedViews {
    pub(super) fn new(graph: &SemanticGraph) -> CatalogResult<Self> {
        let bindings = bindings(graph)?;
        let source_graph = if bindings.is_empty() {
            None
        } else {
            let mut snapshot = Snapshot::from_graph(graph);
            snapshot
                .models
                .retain(|model| !bindings.contains_key(&model.name.to_ascii_lowercase()));
            Some(snapshot.into_graph()?)
        };
        Ok(Self {
            bindings,
            source_graph,
        })
    }

    pub(super) fn graph<'a>(&'a self, original: &'a SemanticGraph) -> &'a SemanticGraph {
        self.source_graph.as_ref().unwrap_or(original)
    }

    pub(super) fn expand(&self, sql: &str) -> CatalogResult<String> {
        expand_bindings(&self.bindings, sql)
    }
}

/// Expand only column bindings, never an aggregate result subquery. The normal
/// compiler then selects source measures at the caller's requested grain.
fn expand_bindings(views: &HashMap<String, Binding>, sql: &str) -> CatalogResult<String> {
    if views.is_empty() {
        return Ok(sql.to_owned());
    }
    #[derive(Default)]
    struct Scope<'a> {
        local: HashMap<String, Option<Binding>>,
        parent: Option<&'a Scope<'a>>,
    }

    impl Scope<'_> {
        fn relation(&self, alias: &str) -> Option<(&String, &Option<Binding>)> {
            self.local
                .get_key_value(alias)
                .or_else(|| self.parent?.relation(alias))
        }

        fn single_source(&self) -> Option<(&String, &Option<Binding>)> {
            match self.local.len() {
                0 => self.parent?.single_source(),
                1 => self.local.iter().next(),
                _ => None,
            }
        }
    }
    fn source(
        node: &mut Value,
        views: &HashMap<String, Binding>,
        scope: &mut Scope,
        ctes: &HashSet<String>,
    ) -> CatalogResult<()> {
        if let Some(table) = node.get_mut("table") {
            let name = table["name"]["name"].as_str().unwrap_or("").to_owned();
            let alias = table["alias"]["name"].as_str().unwrap_or(&name).to_owned();
            let semantic_schema = table["schema"]["name"]
                .as_str()
                .is_some_and(|schema| schema.eq_ignore_ascii_case("semantic"));
            let binding = if table["catalog"].is_null()
                && (semantic_schema
                    || table["schema"].is_null() && !ctes.contains(&name.to_ascii_lowercase()))
            {
                views.get(&name.to_ascii_lowercase()).cloned()
            } else {
                None
            };
            if let Some(binding) = &binding {
                table["name"] = serde_json::to_value(Identifier::quoted(&binding.source)).unwrap();
                table["alias"] = serde_json::to_value(Identifier::quoted(&alias)).unwrap();
                table["schema"] = Value::Null;
            }
            scope.local.insert(alias.to_ascii_lowercase(), binding);
            return Ok(());
        }
        if let Some(subquery) = node.get_mut("subquery") {
            visit(&mut subquery["this"], views, scope, ctes, &HashSet::new())?;
            if let Some(alias) = subquery["alias"]["name"].as_str() {
                scope.local.insert(alias.to_ascii_lowercase(), None);
            }
            return Ok(());
        }
        match node {
            Value::Object(fields) => {
                for child in fields.values_mut() {
                    source(child, views, scope, ctes)?;
                }
            }
            Value::Array(children) => {
                for child in children {
                    source(child, views, scope, ctes)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    fn visit(
        node: &mut Value,
        views: &HashMap<String, Binding>,
        outer: &Scope,
        inherited_ctes: &HashSet<String>,
        output_aliases: &HashSet<String>,
    ) -> CatalogResult<()> {
        if let Some(select) = node.get_mut("select") {
            let mut ctes = inherited_ctes.clone();
            if let Some(with) = select.get_mut("with").filter(|value| !value.is_null()) {
                let recursive = with["recursive"].as_bool() == Some(true);
                for cte in with["ctes"].as_array_mut().into_iter().flatten() {
                    let name = cte["alias"]["name"]
                        .as_str()
                        .unwrap_or("")
                        .to_ascii_lowercase();
                    if recursive {
                        ctes.insert(name.clone());
                    }
                    visit(&mut cte["this"], views, outer, &ctes, &HashSet::new())?;
                    ctes.insert(name);
                }
            }
            let mut scope = Scope {
                local: HashMap::new(),
                parent: Some(outer),
            };
            source(&mut select["from"], views, &mut scope, &ctes)?;
            for join in select["joins"].as_array_mut().into_iter().flatten() {
                source(&mut join["this"], views, &mut scope, &ctes)?;
            }
            let mut projections = Vec::new();
            for projection in select["expressions"].as_array().into_iter().flatten() {
                if let Some(star) = projection.get("star") {
                    let target = star["table"]["name"].as_str().map(str::to_ascii_lowercase);
                    let binding = match target {
                        Some(alias) => scope.relation(&alias),
                        None => scope.single_source(),
                    };
                    if let Some((alias, Some(binding))) = binding {
                        if !star["except"].is_null()
                            || !star["replace"].is_null()
                            || !star["rename"].is_null()
                        {
                            return Err(
                                "semantic view wildcard modifiers must be resolved by the host"
                                    .into(),
                            );
                        }
                        for field in &binding.fields {
                            let mut column = Expression::qualified_column(alias, &field.name);
                            if let Expression::Column(column) = &mut column {
                                column.name = Identifier::quoted(&field.name);
                                column.table = Some(Identifier::quoted(alias));
                            }
                            projections.push(
                                serde_json::to_value(column).map_err(|error| error.to_string())?,
                            );
                        }
                        continue;
                    }
                }
                projections.push(projection.clone());
            }
            select["expressions"] = Value::Array(projections);
            let output_aliases: HashSet<_> = select["expressions"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|projection| projection["alias"]["alias"]["name"].as_str())
                .map(str::to_ascii_lowercase)
                .collect();
            // Preserve the authored column name when its source name differs.
            for projection in select["expressions"].as_array_mut().into_iter().flatten() {
                let original_name = projection
                    .get("column")
                    .and_then(|column| column["name"]["name"].as_str())
                    .map(str::to_owned);
                visit(projection, views, &scope, &ctes, &HashSet::new())?;
                if let Some(name) = original_name {
                    if projection["column"]["name"]["name"].as_str() != Some(&name) {
                        let expression: Expression = serde_json::from_value(projection.clone())
                            .map_err(|error| error.to_string())?;
                        let mut aliased = expression.alias(&name);
                        if let Expression::Alias(alias) = &mut aliased {
                            alias.alias = Identifier::quoted(name);
                        }
                        *projection =
                            serde_json::to_value(aliased).map_err(|error| error.to_string())?;
                    }
                }
            }
            for (key, child) in select.as_object_mut().unwrap() {
                if !matches!(key.as_str(), "from" | "with" | "expressions") {
                    if key == "joins" {
                        for join in child.as_array_mut().into_iter().flatten() {
                            for (key, child) in join.as_object_mut().unwrap() {
                                if key != "this" {
                                    visit(child, views, &scope, &ctes, &output_aliases)?;
                                }
                            }
                        }
                    } else if key == "order_by" && !child.is_null() {
                        for item in child["expressions"].as_array_mut().into_iter().flatten() {
                            // DuckDB resolves a bare ORDER BY name against the output
                            // first. Inside a sort expression, input columns win.
                            let column = &item["this"]["column"];
                            if column["table"].is_null()
                                && column["name"]["name"].as_str().is_some_and(|name| {
                                    output_aliases.contains(&name.to_ascii_lowercase())
                                })
                            {
                                continue;
                            }
                            visit(item, views, &scope, &ctes, &output_aliases)?;
                        }
                    } else {
                        visit(child, views, &scope, &ctes, &output_aliases)?;
                    }
                }
            }
            return Ok(());
        }
        if let Some(column) = node.get_mut("column") {
            let field = column["name"]["name"].as_str().unwrap_or("").to_owned();
            let binding = if let Some(table) = column["table"]["name"].as_str() {
                outer.relation(&table.to_ascii_lowercase())
            } else {
                outer.single_source()
            }
            .and_then(|(_, binding)| binding.as_ref());
            if let Some(binding) = binding {
                let selected = binding
                    .fields
                    .iter()
                    .find(|item| item.name.eq_ignore_ascii_case(&field));
                if selected.is_none()
                    && column["table"].is_null()
                    && output_aliases.contains(&field.to_ascii_lowercase())
                {
                    return Ok(());
                }
                let source = if let Some(selected) = selected {
                    selected.source.clone()
                } else if let Some((base, granularity)) = field.rsplit_once("__") {
                    let selected = binding
                        .fields
                        .iter()
                        .find(|item| {
                            item.kind == "dimension" && item.name.eq_ignore_ascii_case(base)
                        })
                        .ok_or_else(|| format!("semantic view has no selected field '{field}'"))?;
                    format!("{}__{granularity}", selected.source)
                } else {
                    return Err(format!("semantic view has no selected field '{field}'"));
                };
                column["name"] = serde_json::to_value(Identifier::quoted(source)).unwrap();
            }
            return Ok(());
        }
        match node {
            Value::Object(fields) => {
                for child in fields.values_mut() {
                    visit(child, views, outer, inherited_ctes, output_aliases)?;
                }
            }
            Value::Array(children) => {
                for child in children {
                    visit(child, views, outer, inherited_ctes, output_aliases)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    crate::semantic_input::with_semantic_stack(|| {
        let ast = crate::semantic_input::dialects::parse(sql, DialectType::DuckDB)?;
        let mut ast = serde_json::to_value(ast)
            .map_err(|error| crate::error::SqlctxError::SqlGeneration(error.to_string()))?;
        visit(
            &mut ast,
            views,
            &Scope::default(),
            &HashSet::new(),
            &HashSet::new(),
        )
        .map_err(crate::error::SqlctxError::SqlGeneration)?;
        let ast = serde_json::from_value(ast)
            .map_err(|error| crate::error::SqlctxError::SqlGeneration(error.to_string()))?;
        polyglot_sql::generate(&ast, DialectType::DuckDB)
            .map_err(|error| crate::error::SqlctxError::SqlGeneration(error.to_string()))
    })
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expand(graph: &SemanticGraph, sql: &str) -> CatalogResult<String> {
        PreparedViews::new(graph)?.expand(sql)
    }

    fn catalog() -> String {
        apply(
            "",
            "",
            "yaml",
            r#"
models:
  - name: orders
    table: raw_orders
    primary_key: id
    dimensions:
      - {name: status, type: categorical}
      - {name: country, type: categorical}
    metrics:
      - {name: revenue, agg: sum, sql: amount}
      - {name: paid, agg: sum, sql: amount, filters: ["status = 'paid'"]}
      - {name: rows, agg: count}
      - {name: average, type: ratio, numerator: revenue, denominator: rows}
"#,
            false,
        )
        .unwrap()
        .0
    }

    fn view(snapshot: &str, content: &str, replace: bool) -> CatalogResult<String> {
        apply(snapshot, "orders", "view", content, replace).map(|result| result.0)
    }

    fn compile(snapshot: &str, sql: &str) -> String {
        let graph = Snapshot::parse(snapshot).unwrap().into_graph().unwrap();
        rewrite_graph(&graph, sql).unwrap()
    }

    #[test]
    fn selections_regrain_filtered_measures_and_ratios() {
        let snapshot = view(
            &catalog(),
            "sales AS SELECT status, paid, average AS aov FROM orders",
            false,
        )
        .unwrap();
        let total = compile(&snapshot, "SELECT aov, paid FROM sales");
        assert!(
            total.contains("NULLIF") && total.contains("CASE WHEN"),
            "{total}"
        );
        assert!(!total.contains("GROUP BY"), "{total}");
        let grouped = compile(&snapshot, "SELECT status, aov, paid FROM sales");
        assert!(grouped.contains("GROUP BY"), "{grouped}");
        assert!(grouped.contains("raw_orders"), "{grouped}");
        assert!(!grouped.contains("__semantic_view"), "{grouped}");
        let graph = Snapshot::parse(&snapshot).unwrap().into_graph().unwrap();
        let star = expand(&graph, "SELECT * FROM sales").unwrap();
        assert!(
            !star.contains("country") && !star.contains("revenue"),
            "{star}"
        );
        assert!(expand(&graph, "SELECT country FROM sales").is_err());
    }

    #[test]
    fn nested_views_preserve_names_and_bind_replacements_dynamically() {
        let base = view(
            &catalog(),
            "sales AS SELECT status, revenue AS gross FROM orders",
            false,
        )
        .unwrap();
        let nested = view(
            &base,
            "report AS SELECT status AS state, gross AS total FROM sales",
            false,
        )
        .unwrap();
        let sql = compile(&nested, "SELECT state, total FROM report");
        assert!(
            sql.contains("amount") && sql.contains("total") && sql.contains("state"),
            "{sql}"
        );
        let replaced = apply(
            &nested,
            "orders",
            "item",
            "METRIC revenue AS SUM(replacement_amount)",
            true,
        )
        .unwrap()
        .0;
        assert!(compile(&replaced, "SELECT total FROM report").contains("replacement_amount"));
        assert!(!compile(&nested, "SELECT total FROM report").contains("replacement_amount"));
        assert!(view(&nested, "sales AS SELECT status FROM orders", true)
            .unwrap_err()
            .contains("gross"));
        assert!(compile(&nested, "SELECT total FROM report").contains("amount"));
        let exported = serde_json::to_string(&Snapshot::from_graph(
            &Snapshot::parse(&nested).unwrap().into_graph().unwrap(),
        ))
        .unwrap();
        assert_eq!(
            compile(&nested, "SELECT total FROM report"),
            compile(&exported, "SELECT total FROM report")
        );
    }

    #[test]
    fn view_dependencies_restrict_drop_and_incompatible_replacement() {
        let snapshot = view(
            &catalog(),
            "sales AS SELECT status, revenue FROM orders",
            false,
        )
        .unwrap();
        for (operation, content) in [
            ("drop_model", r#"["orders"]"#),
            ("drop_metric", r#"["orders","revenue"]"#),
            ("drop_dimension", r#"["orders","status"]"#),
        ] {
            let error = apply(&snapshot, "orders", operation, content, false).unwrap_err();
            assert!(
                error.contains("sales") && error.contains("RESTRICT"),
                "{error}"
            );
        }
        assert!(apply(
            &snapshot,
            "orders",
            "model",
            "MODEL (name orders, table replacement)",
            true
        )
        .unwrap_err()
        .contains("sales"));
        let nested = view(&snapshot, "report AS SELECT revenue FROM sales", false).unwrap();
        assert!(apply(&nested, "orders", "drop_view", r#"["sales"]"#, false)
            .unwrap_err()
            .contains("report"));
        assert!(view(&snapshot, "sales AS SELECT revenue FROM sales", true)
            .unwrap_err()
            .contains("cyclic"));
        let dropped = apply(&snapshot, "orders", "drop_view", r#"["sales"]"#, false)
            .unwrap()
            .0;
        assert_eq!(Snapshot::parse(&dropped).unwrap().models.len(), 1);
        assert!(apply(&snapshot, "orders", "drop_model", r#"["sales"]"#, false).is_err());
        assert!(apply(&snapshot, "orders", "drop_view", r#"["orders"]"#, false).is_err());
    }

    #[test]
    fn order_by_output_alias_takes_precedence_over_view_dimension() {
        let snapshot = view(
            &catalog(),
            "sales AS SELECT country AS region, revenue AS gross FROM orders",
            false,
        )
        .unwrap();
        let sql = compile(
            &snapshot,
            "SELECT gross AS region FROM sales ORDER BY region DESC",
        );
        assert!(!sql.contains("GROUP BY"), "{sql}");
        let graph = Snapshot::parse(&snapshot).unwrap().into_graph().unwrap();
        for (order, expected) in [
            ("region DESC", "ORDER BY region DESC"),
            ("sales.region", "sales.\"country\""),
            ("region + 1", "\"country\" + 1"),
        ] {
            let sql = expand(
                &graph,
                &format!("SELECT gross AS region FROM sales ORDER BY {order}"),
            )
            .unwrap();
            assert!(sql.contains(expected), "{sql}");
        }
    }

    #[test]
    fn scalar_subqueries_resolve_local_view_fields_before_outer_sources() {
        let snapshot = view(
            &catalog(),
            "sales AS SELECT revenue AS gross FROM orders",
            false,
        )
        .unwrap();
        let graph = Snapshot::parse(&snapshot).unwrap().into_graph().unwrap();
        let sql = expand(
            &graph,
            "SELECT (SELECT gross FROM sales) AS total FROM main.raw_orders",
        )
        .unwrap();
        assert!(sql.contains("\"revenue\" AS \"gross\""), "{sql}");
        let sql = expand(
            &graph,
            "SELECT (SELECT s.gross FROM main.raw_orders) FROM sales s",
        )
        .unwrap();
        assert!(sql.contains("s.\"revenue\" AS \"gross\""), "{sql}");
        let sql = expand(
            &graph,
            "SELECT (SELECT gross FROM main.raw_orders s) FROM sales s",
        )
        .unwrap();
        assert!(sql.contains("SELECT gross FROM main.raw_orders"), "{sql}");
    }

    #[test]
    fn view_expansion_respects_cte_and_physical_scope() {
        let snapshot = view(
            &catalog(),
            "sales AS SELECT revenue AS gross FROM orders",
            false,
        )
        .unwrap();
        let graph = Snapshot::parse(&snapshot).unwrap().into_graph().unwrap();
        for sql in [
            "SELECT gross FROM main.sales",
            "WITH sales AS (SELECT 7 AS gross) SELECT gross FROM sales",
        ] {
            assert!(!expand(&graph, sql).unwrap().contains("orders"));
        }
        let sql = expand(&graph, "SELECT q.gross FROM (SELECT gross FROM sales) q").unwrap();
        assert!(
            sql.contains("AS \"gross\"") && sql.contains("q.gross"),
            "{sql}"
        );
        let aliased = expand(&graph, "SELECT gross AS total FROM sales ORDER BY total").unwrap();
        assert!(aliased.contains("ORDER BY total"), "{aliased}");
        let quoted = view(
            &catalog(),
            "\"Sales Report\" AS SELECT revenue AS \"Gross Amount\" FROM orders",
            false,
        )
        .unwrap();
        let graph = Snapshot::parse(&quoted).unwrap().into_graph().unwrap();
        let star = expand(&graph, "SELECT * FROM \"Sales Report\"").unwrap();
        assert!(star.contains("AS \"Gross Amount\""), "{star}");
        assert!(
            polyglot_sql::parse_one(&star, DialectType::DuckDB).is_ok(),
            "{star}"
        );
        for sql in [
            "sales AS SELECT SUM(revenue) FROM orders",
            "sales AS SELECT revenue FROM orders GROUP BY ALL",
            "sales AS SELECT revenue FROM missing",
            "sales AS SELECT revenue, revenue FROM orders",
        ] {
            assert!(view(&catalog(), sql, false).is_err(), "{sql}");
        }
    }
}
