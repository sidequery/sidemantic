//! Aggregate projected entity rows, including opaque aggregate SQL.
//!
//! The ordinary compiler still owns joins, policies and row filters. Its
//! ungrouped projection supplies typed keys and inputs; DISTINCT restores the
//! source entity grain before any aggregate or complete expression runs.
use super::*;
use crate::core::replace_semantic_columns;

fn unsupported(shape: &str) -> SidemanticError {
    SidemanticError::UnsupportedSemanticFeatures {
        capabilities: vec![format!("aggregation.{shape}")],
    }
}

struct Inputs<'a> {
    generator: &'a SqlGenerator<'a>,
    model: Model,
    names: HashSet<String>,
    references: Vec<String>,
    next: usize,
}

impl Inputs<'_> {
    fn add(&mut self, sql: String, filters: &[String]) -> String {
        let name = loop {
            let candidate = format!("__sidemantic_input_{}", self.next);
            self.next += 1;
            if self.names.insert(candidate.clone()) {
                break candidate;
            }
        };
        let mut metric = Metric::sum(&name, sql);
        metric.filters = filters.to_vec();
        self.model.metrics.push(metric);
        self.references
            .push(format!("{}.{}", self.model.name, name));
        self.generator.quote_identifier(&name)
    }
}

pub(super) fn generate_entity_aggregates(
    generator: &SqlGenerator<'_>,
    query: &SemanticQuery,
    owner: &str,
    dimensions: &[DimensionRef],
    metrics: &[(&Metric, &str)],
    deduplicate: bool,
) -> Result<String> {
    let model = generator.graph.get_model(owner).unwrap();
    if deduplicate && model.primary_keys().is_empty() {
        return Err(SidemanticError::Validation(format!(
            "Model '{owner}' has no primary key; cannot safely aggregate across a fanout join"
        )));
    }
    let names = generator
        .graph
        .models()
        .flat_map(|model| {
            model
                .metrics
                .iter()
                .map(|metric| metric.name.clone())
                .chain(
                    model
                        .dimensions
                        .iter()
                        .map(|dimension| dimension.name.clone()),
                )
        })
        .chain(dimensions.iter().map(|dimension| dimension.alias.clone()))
        .collect();
    let mut inputs = Inputs {
        generator,
        model: model.clone(),
        names,
        references: Vec::new(),
        next: 0,
    };
    if deduplicate {
        for key in model.primary_keys() {
            inputs.add(generator.key_sql(model, &key, None)?, &[]);
        }
    }
    let mut selections = Vec::new();
    for (metric, alias) in metrics {
        let sql = if metric.sql_is_complete {
            let sql = metric
                .sql
                .as_deref()
                .ok_or_else(|| unsupported("complete_sql_missing"))?
                .replace("{model}", owner);
            let columns = semantic_column_references(&sql)?;
            if columns.is_empty() && !metric.filters.is_empty() {
                return Err(unsupported("filtered_constant_complete_sql"));
            }
            let mut replacements = HashMap::new();
            for column in columns {
                if column
                    .model
                    .as_deref()
                    .is_some_and(|name| name != owner && name != format!("{owner}_cte"))
                {
                    return Err(unsupported("cross_source_raw_input"));
                }
                let key = (column.model, column.field.clone());
                if !replacements.contains_key(&key) {
                    // Complete SQL names physical source columns. Do not expand
                    // a coincidentally named semantic metric as a dependency.
                    let input =
                        inputs.add(generator.quote_identifier(&column.field), &metric.filters);
                    replacements.insert(key, input);
                }
            }
            let parsed = replace_semantic_columns(parse_semantic_expression(&sql)?, &replacements)?;
            let sql = generator.emit_expression(&parsed)?;
            if !dimensions.is_empty() && !SqlGenerator::is_inline_aggregate_expression(&sql) {
                format!("ANY_VALUE({sql})")
            } else {
                sql
            }
        } else {
            let implicit_distinct = deduplicate
                && matches!(
                    metric.agg,
                    Some(Aggregation::CountDistinct | Aggregation::ApproxCountDistinct)
                )
                && metric.sql.as_deref().is_none_or(str::is_empty);
            let raw = if implicit_distinct {
                "1".to_string()
            } else {
                generator.metric_raw_expression(metric, model)?
            };
            let raw = inputs.add(raw, &metric.filters);
            if implicit_distinct {
                format!("COUNT({raw})")
            } else {
                match metric
                    .agg
                    .as_ref()
                    .ok_or_else(|| unsupported("inline_aggregate"))?
                {
                    Aggregation::CountDistinct => format!("COUNT(DISTINCT {raw})"),
                    Aggregation::Expression => return Err(unsupported("inline_aggregate")),
                    kind => format!("{}({raw})", kind.as_sql()),
                }
            }
        };
        selections.push(format!("{sql} AS {}", generator.quote_identifier(alias)));
    }
    // COUNT(*) can be the only output, but the row compiler still needs one
    // projected input when no keys or dimensions were required.
    if inputs.references.is_empty() && dimensions.is_empty() {
        inputs.add("1".into(), &[]);
    }
    let mut graph = generator.graph.clone();
    graph.replace_model(inputs.model)?;
    let row_generator = SqlGenerator::new(&graph)
        .with_dialect(generator.dialect)
        .with_timezone(generator.timezone.clone());
    let input_columns = inputs
        .references
        .iter()
        .map(|reference| generator.quote_identifier(reference.rsplit('.').next().unwrap()))
        .collect::<Vec<_>>();
    let mut rows = query.clone();
    rows.metrics = inputs.references;
    rows.ungrouped = true;
    rows.with_totals = false;
    rows.use_preaggregations = false;
    let row_sql = row_generator.generate_from_model(&rows, Some(owner))?;
    let mut collisions = HashMap::new();
    for dimension in dimensions {
        *collisions.entry(dimension.alias.clone()).or_insert(0usize) += 1;
    }
    let mut outer = Vec::new();
    for (index, dimension) in dimensions.iter().enumerate() {
        let source = generator.output_alias(&dimension.model, &dimension.alias, &collisions);
        outer.push(format!(
            "{} AS __sidemantic_dimension_{index}",
            generator.quote_identifier(&source)
        ));
    }
    outer.extend(selections.iter().cloned());
    let grouped_totals = query.with_totals && !dimensions.is_empty();
    if grouped_totals {
        outer.push("0 AS _is_total".into());
    }
    let source = if deduplicate {
        format!("SELECT DISTINCT * FROM (\n{row_sql}\n) AS __sidemantic_joined")
    } else {
        row_sql.clone()
    };
    let mut sql = format!(
        "SELECT {}\nFROM (\n{source}\n) AS __sidemantic_entities",
        outer.join(", ")
    );
    if !dimensions.is_empty() {
        let groups = (1..=dimensions.len())
            .map(|i| i.to_string())
            .collect::<Vec<_>>();
        sql.push_str(&format!("\nGROUP BY {}", groups.join(", ")));
    }
    if grouped_totals {
        // An entity may belong to several dimension groups. Deduplicate the
        // total's keys and inputs independently, without those dimensions.
        let total_source = if deduplicate {
            format!(
                "SELECT DISTINCT {} FROM (\n{row_sql}\n) AS __sidemantic_joined",
                input_columns.join(", ")
            )
        } else {
            row_sql
        };
        let mut total = (0..dimensions.len())
            .map(|index| format!("NULL AS __sidemantic_dimension_{index}"))
            .collect::<Vec<_>>();
        total.extend(selections);
        total.push("1 AS _is_total".into());
        sql.push_str(&format!(
            "\nUNION ALL\nSELECT {}\nFROM (\n{total_source}\n) AS __sidemantic_entities",
            total.join(", ")
        ));
    }
    Ok(sql)
}
