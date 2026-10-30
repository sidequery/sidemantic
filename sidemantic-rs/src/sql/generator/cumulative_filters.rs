//! Query-local inputs for cumulative measures with their own row filters.
use super::*;

impl SqlGenerator<'_> {
    pub(super) fn filtered_window_input(
        &self,
        reference: &str,
        owner: &str,
        filters: &[String],
        graph: &mut SemanticGraph,
        visiting: &mut HashSet<(String, String, bool)>,
    ) -> Result<String> {
        let (model_name, name, graph_metric) = self
            .resolve_metric_reference_location(reference, owner)?
            .ok_or_else(|| {
                SidemanticError::Validation(format!("Unknown cumulative base metric '{reference}'"))
            })?;
        let key = (model_name.clone(), name.clone(), graph_metric);
        if !visiting.insert(key.clone()) {
            return Err(SidemanticError::Validation(format!(
                "Circular cumulative base metric '{reference}'"
            )));
        }
        let source = self.metric_for_model_with_source(&model_name, &name, graph_metric)?;
        let mut metric = if graph_metric && source.r#type == MetricType::Simple {
            self.graph_metric_source_inputs(source, Some(&model_name))?
        } else {
            source.clone()
        };
        match metric.r#type {
            MetricType::Simple => metric.filters.extend_from_slice(filters),
            MetricType::Derived => {
                let sql = metric.sql.as_deref().unwrap_or("0");
                let mut replacements = HashMap::new();
                let mut combined_filters = metric.filters.clone();
                combined_filters.extend_from_slice(filters);
                for column in semantic_column_references(sql)? {
                    let reference = column.name();
                    if !column.aggregate_input
                        && self
                            .resolve_metric_reference_location(&reference, &model_name)?
                            .is_some()
                    {
                        let scoped = self.filtered_window_input(
                            &reference,
                            &model_name,
                            &combined_filters,
                            graph,
                            visiting,
                        )?;
                        let (owner, input) =
                            scoped.rsplit_once('.').expect("qualified private input");
                        let replacement = if Self::is_simple_identifier(owner)
                            && Self::is_simple_identifier(input)
                        {
                            scoped
                        } else {
                            format!(
                                "\"{}\".\"{}\"",
                                owner.replace('"', "\"\""),
                                input.replace('"', "\"\"")
                            )
                        };
                        replacements.insert((column.model, column.field), replacement);
                    }
                }
                if replacements.is_empty() {
                    metric.filters = combined_filters;
                } else {
                    metric.sql = Some(
                        polyglot_sql::generate(
                            &crate::core::replace_semantic_columns(
                                parse_semantic_expression(sql)?,
                                &replacements,
                            )?,
                            SOURCE_DIALECT,
                        )
                        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?,
                    );
                    metric.filters.clear();
                }
            }
            MetricType::Ratio if metric.offset_window.is_none() => {
                let mut combined_filters = metric.filters.clone();
                combined_filters.extend_from_slice(filters);
                for reference in [&mut metric.numerator, &mut metric.denominator] {
                    if let Some(value) = reference {
                        *value = self.filtered_window_input(
                            value,
                            &model_name,
                            &combined_filters,
                            graph,
                            visiting,
                        )?;
                    }
                }
                metric.filters.clear();
            }
            _ => {
                return Err(SidemanticError::UnsupportedSemanticFeatures {
                    capabilities: vec!["metric.cumulative.filtered_base_shape".into()],
                })
            }
        }
        let mut model = graph.get_model(&model_name).cloned().ok_or_else(|| {
            SidemanticError::Validation(format!("Unknown cumulative input model '{model_name}'"))
        })?;
        let mut index = model.metrics.len();
        metric.name = loop {
            let name = format!("__sidemantic_filtered_{index}");
            index += 1;
            if graph.get_metric(&name).is_none()
                && graph.models().all(|model| {
                    model.get_metric(&name).is_none() && model.get_dimension(&name).is_none()
                })
            {
                break name;
            }
        };
        let reference = format!("{model_name}.{}", metric.name);
        model.metrics.push(metric);
        graph.replace_model(model)?;
        visiting.remove(&key);
        Ok(reference)
    }
}

pub(super) struct GroupedWindowInput {
    reference: String,
    base_reference: String,
    filters: Vec<String>,
}

impl SqlGenerator<'_> {
    pub(super) fn prepare_filtered_window_input(
        &self,
        reference: &str,
        owner: &str,
        filters: &[String],
        graph: &mut SemanticGraph,
        grouped: &mut Vec<GroupedWindowInput>,
    ) -> Result<String> {
        let (source, _, _) = self
            .resolve_metric_reference_location(reference, owner)?
            .ok_or_else(|| {
                SidemanticError::Validation(format!("Unknown cumulative base metric '{reference}'"))
            })?;
        let mut cross_source = false;
        let filters = filters
            .iter()
            .map(|filter| {
                let filter = crate::core::replace_model_placeholder(filter, Some(owner))?;
                let mut replacements = HashMap::new();
                for column in crate::core::outer_semantic_column_references(&filter)? {
                    let model = column.model.as_deref().unwrap_or(owner);
                    let model = if self.graph.get_model(model).is_some() {
                        model
                    } else {
                        model.strip_suffix("_cte").unwrap_or(model)
                    };
                    cross_source |= model != source;
                    replacements.insert(
                        (column.model.clone(), column.field.clone()),
                        format!(
                            "\"{}\".\"{}\"",
                            model.replace('"', "\"\""),
                            column.field.replace('"', "\"\"")
                        ),
                    );
                }
                polyglot_sql::generate(
                    &crate::core::replace_outer_semantic_columns(
                        parse_semantic_expression(&filter)?,
                        &replacements,
                    )?,
                    SOURCE_DIALECT,
                )
                .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        let scoped = self.filtered_window_input(
            reference,
            owner,
            if cross_source { &[] } else { &filters },
            graph,
            &mut HashSet::new(),
        )?;
        if cross_source {
            grouped.push(GroupedWindowInput {
                reference: scoped.clone(),
                base_reference: reference.to_string(),
                filters,
            });
        }
        Ok(scoped)
    }

    pub(super) fn merge_grouped_window_inputs(
        &self,
        inner: &SemanticQuery,
        inner_sql: String,
        outputs: &HashMap<String, String>,
        grouped: &[GroupedWindowInput],
    ) -> Result<String> {
        if grouped.is_empty() {
            return Ok(inner_sql);
        }
        let domain = "__sidemantic_window_domain";
        let mut ctes = vec![format!("{domain} AS ({inner_sql})")];
        let mut joins = Vec::new();
        let mut substitutions = HashMap::new();
        for (index, scoped) in grouped.iter().enumerate() {
            let alias = format!("__sidemantic_window_scope_{index}");
            let mut child = inner.clone();
            child.metrics = vec![scoped.base_reference.clone()];
            child.filters.extend(scoped.filters.clone());
            let mut sql = self.generate(&child)?;
            let child_outputs = self.selected_output_names(&child)?;
            let input = format!(
                "{alias}.{}",
                self.quote_identifier(&child_outputs[&scoped.base_reference])
            );
            let metric_ref = self.parse_metric_refs(&child.metrics)?.remove(0);
            let metric = self.metric_for_ref(&metric_ref)?;
            let input = if matches!(metric.r#type, MetricType::Derived | MetricType::Ratio) {
                // Reconstruct the expression's true empty-group value (COUNT +
                // 1 is 1, for example), without replacing genuine NULL results.
                let empty_alias = format!("{alias}_empty");
                let mut empty = child.clone();
                empty.dimensions.clear();
                empty.skip_default_time_dimensions = true;
                empty.filters.push("FALSE".into());
                let empty_sql = self.generate(&empty)?;
                let empty_outputs = self.selected_output_names(&empty)?;
                let empty_column = self.quote_identifier(&empty_outputs[&scoped.base_reference]);
                ctes.push(format!("{empty_alias} AS ({empty_sql})"));
                let mut marker = "__sidemantic_present".to_string();
                while child_outputs.values().any(|output| output == &marker) {
                    marker.push('_');
                }
                let marker = self.quote_identifier(&marker);
                sql = format!(
                    "SELECT __scope_values.*, 1 AS {marker} FROM ({sql}) AS __scope_values"
                );
                format!("CASE WHEN {alias}.{marker} IS NULL THEN (SELECT {empty_column} FROM {empty_alias}) ELSE {input} END")
            } else if matches!(
                metric.agg,
                Some(
                    Aggregation::Count
                        | Aggregation::CountDistinct
                        | Aggregation::ApproxCountDistinct
                )
            ) {
                format!("COALESCE({input}, 0)")
            } else {
                input
            };
            substitutions.insert(
                scoped.reference.clone(),
                self.fill_metric_expression(metric, input)?,
            );
            ctes.push(format!("{alias} AS ({sql})"));
            let conditions = inner
                .dimensions
                .iter()
                .map(|reference| {
                    // Parse in the handoff dialect, then emit the target dialect's
                    // null-safe equality (e.g. <=> on MySQL).
                    let left = outputs[reference].replace('"', "\"\"");
                    let right = child_outputs[reference].replace('"', "\"\"");
                    self.emit_expression(&parse_semantic_expression(&format!(
                        "{domain}.\"{left}\" IS NOT DISTINCT FROM {alias}.\"{right}\""
                    ))?)
                })
                .collect::<Result<Vec<_>>>()?;
            joins.push(if conditions.is_empty() {
                format!("CROSS JOIN {alias}")
            } else {
                format!("LEFT JOIN {alias} ON {}", conditions.join(" AND "))
            });
        }
        let mut seen = HashSet::new();
        let projections = inner
            .dimensions
            .iter()
            .chain(&inner.metrics)
            .filter_map(|reference| {
                let alias = &outputs[reference];
                if !seen.insert(alias) {
                    return None;
                }
                let input = substitutions
                    .get(reference)
                    .cloned()
                    .unwrap_or_else(|| format!("{domain}.{}", self.quote_identifier(alias)));
                Some(format!("{input} AS {}", self.quote_identifier(alias)))
            })
            .collect::<Vec<_>>();
        Ok(format!(
            "WITH {}\nSELECT {}\nFROM {domain}\n{}",
            ctes.join(",\n"),
            projections.join(", "),
            joins.join("\n")
        ))
    }
}
