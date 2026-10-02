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
    models: HashMap<String, Model>,
    names: HashSet<String>,
    references: Vec<String>,
    next: usize,
}

impl Inputs<'_> {
    fn add(&mut self, owner: &str, sql: String, filters: &[String]) -> String {
        let name = loop {
            let candidate = format!("sidemantic_input_{}", self.next);
            self.next += 1;
            if self.names.insert(candidate.clone()) {
                break candidate;
            }
        };
        let mut metric = Metric::sum(&name, sql);
        metric.filters = filters.to_vec();
        self.models.get_mut(owner).unwrap().metrics.push(metric);
        self.references.push(format!("{owner}.{name}"));
        self.generator.quote_identifier(&name)
    }
}

pub(super) fn generate_entity_aggregates(
    generator: &SqlGenerator<'_>,
    query: &SemanticQuery,
    dimensions: &[DimensionRef],
    metrics: &[(&Metric, &str)],
    deduplicate: bool,
    independent_source: bool,
    sources: &[String],
) -> Result<String> {
    let owner = sources[0].as_str();
    let model = generator.graph.get_model(owner).unwrap();
    // Joined expressions own a tuple of source rows. Include every source key
    // when another relationship multiplies that tuple, rather than restoring
    // only one source's grain or deduplicating equal measure values.
    let mut required = query.required_population_models.clone();
    required.extend(sources.iter().cloned());
    required.extend(dimensions.iter().map(|dimension| dimension.model.clone()));
    required.extend(generator.find_filter_models(&query.filters));
    required.extend(query.prepared_policies.model_names().cloned());
    let anchor = query.consumption_base_model.as_deref().unwrap_or(owner);
    let paths = generator.build_join_paths(anchor, &required)?;
    let source_paths = generator.build_join_paths(anchor, &sources.iter().cloned().collect())?;
    let population_edges: HashSet<_> = source_paths
        .values()
        .flat_map(|path| &path.steps)
        .map(|step| (&step.from_model, &step.to_model))
        .collect();
    let extra_fanout = paths.values().flat_map(|path| &path.steps).any(|step| {
        step.causes_fan_out() && !population_edges.contains(&(&step.from_model, &step.to_model))
    });
    let deduplicate = deduplicate || (sources.len() > 1 && extra_fanout);
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
        models: sources
            .iter()
            .map(|source| {
                (
                    source.clone(),
                    generator.graph.get_model(source).unwrap().clone(),
                )
            })
            .collect(),
        names,
        references: Vec::new(),
        next: 0,
    };
    if deduplicate {
        for source in sources {
            let source_model = generator.graph.get_model(source).unwrap();
            if source_model.primary_keys().is_empty() {
                return Err(SidemanticError::Validation(format!(
                    "Model '{source}' has no primary key; cannot safely aggregate across a fanout join"
                )));
            }
            for key in source_model.primary_keys() {
                inputs.add(source, generator.key_sql(source_model, &key, None)?, &[]);
            }
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
                let source = column.model.as_deref().unwrap_or(owner);
                let source = source
                    .strip_suffix("_cte")
                    .filter(|source| sources.iter().any(|item| item == source))
                    .unwrap_or(source);
                if !sources.iter().any(|item| item == source) {
                    return Err(unsupported("cross_source_raw_input"));
                }
                let key = (column.model.clone(), column.field.clone());
                if let std::collections::hash_map::Entry::Vacant(entry) = replacements.entry(key) {
                    // Ossie aggregate inputs prefer declared logical fields and
                    // preserve undeclared physical references. Native complete
                    // SQL retains its physical source-column contract. Project
                    // either expression under a fresh internal metric name, never
                    // beside SELECT * under a colliding source-column name.
                    let source_model = generator.graph.get_model(source).unwrap();
                    let raw = if super::aggregate_plan::has_logical_inputs(metric) {
                        source_model
                            .get_dimension(&column.field)
                            .map(|dimension| dimension.sql_expr().to_string())
                            .unwrap_or_else(|| generator.quote_identifier(&column.field))
                    } else {
                        generator.quote_identifier(&column.field)
                    };
                    let input = inputs.add(source, raw, &metric.filters);
                    entry.insert(input);
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
            let raw = inputs.add(owner, raw, &metric.filters);
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
                    kind => generator.aggregate_sql(kind, &raw)?,
                }
            }
        };
        selections.push(format!("{sql} AS {}", generator.quote_identifier(alias)));
    }
    // COUNT(*) can be the only output. Retain its source in the row query even
    // when every grouping dimension belongs to a different model.
    if inputs.references.is_empty() {
        inputs.add(owner, "1".into(), &[]);
    }
    let mut graph = generator.graph.clone();
    for model in inputs.models.into_values() {
        graph.replace_model(model)?;
    }
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
    // Independent children retain their own population before their grouped
    // outputs are joined. A single complete expression retains the ordinary
    // dimension-domain contract, including COUNT(*) on null-extended rows.
    let source = if independent_source {
        Some(owner.to_string())
    } else {
        row_generator.query_base_model(dimensions, &row_generator.parse_metric_refs(&rows.metrics)?)
    };
    let row_sql =
        row_generator.generate_from_model_with_aggregation(&rows, source.as_deref(), false)?;
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
