//! Cube.js adapter: imports Cube YAML (`cubes:`) into the semantic graph.

use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::core::{
    Aggregation, Dimension, DimensionType, Metric, MetricType, Model, Relationship,
    RelationshipType, Segment,
};
use crate::error::{Result, SidemanticError};

use super::{Adapter, ParsedDocument};

/// Adapter for importing Cube.js semantic definitions.
#[derive(Debug, Default, Clone, Copy)]
pub struct CubeAdapter;

impl CubeAdapter {
    pub fn new() -> Self {
        Self
    }

    /// Parse Cube YAML content into core models.
    pub fn parse_models(&self, content: &str) -> Result<Vec<Model>> {
        let config: CubeConfig = serde_yaml::from_str(content)
            .map_err(|e| SidemanticError::Validation(format!("YAML parse error: {e}")))?;
        Ok(config.into_models())
    }
}

impl Adapter for CubeAdapter {
    fn parse_document(&self, content: &str) -> Result<ParsedDocument> {
        Ok(ParsedDocument {
            models: self.parse_models(content)?,
            ..Default::default()
        })
    }
}

// =============================================================================
// Cube.js Format Schema
// =============================================================================

/// Root schema for Cube.js YAML files
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CubeConfig {
    #[serde(default)]
    pub cubes: Vec<CubeDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CubeDefinition {
    pub name: String,
    pub sql_table: Option<String>,
    pub sql: Option<String>,
    pub description: Option<String>,
    #[serde(default)]
    pub dimensions: Vec<CubeDimension>,
    #[serde(default)]
    pub measures: Vec<CubeMeasure>,
    #[serde(default)]
    pub segments: Vec<CubeSegment>,
    #[serde(default)]
    pub joins: Vec<CubeJoin>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CubeJoin {
    pub name: String,
    pub sql: Option<String>,
    pub relationship: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CubeDimension {
    pub name: String,
    #[serde(rename = "type")]
    pub dim_type: Option<String>,
    pub sql: Option<String>,
    pub description: Option<String>,
    pub title: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CubeMeasure {
    pub name: String,
    #[serde(rename = "type")]
    pub measure_type: Option<String>,
    pub sql: Option<String>,
    pub description: Option<String>,
    pub title: Option<String>,
    #[serde(default)]
    pub filters: Vec<CubeFilter>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CubeFilter {
    pub sql: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CubeSegment {
    pub name: String,
    pub sql: String,
    pub description: Option<String>,
}

// =============================================================================
// Conversion to Core Types
// =============================================================================

impl CubeConfig {
    /// Convert to list of core Model types
    pub fn into_models(self) -> Vec<Model> {
        self.cubes.into_iter().map(|c| c.into_model()).collect()
    }
}

impl CubeDefinition {
    fn into_model(self) -> Model {
        // Infer primary key from name (cube_name -> cube_name_id or id)
        let primary_key = "id".to_string();

        let self_name = self.name.clone();
        let relationships = self
            .joins
            .into_iter()
            .filter_map(|join| relationship_from_cube_join(&self_name, join))
            .collect();

        Model {
            name: self.name,
            table: self.sql_table,
            sql: self.sql,
            source_uri: None,
            extends: None,
            primary_key: primary_key.clone(),
            primary_key_columns: vec![primary_key],
            unique_keys: None,
            dimensions: self
                .dimensions
                .into_iter()
                .map(|d| d.into_dimension())
                .collect(),
            metrics: self.measures.into_iter().map(|m| m.into_metric()).collect(),
            relationships,
            segments: self
                .segments
                .into_iter()
                .map(|s| s.into_segment())
                .collect(),
            pre_aggregations: Vec::new(),
            default_time_dimension: None,
            default_grain: None,
            label: None,
            description: self.description,
            metadata: None,
            meta: None,
        }
    }
}

impl CubeDimension {
    fn into_dimension(self) -> Dimension {
        let dim_type = match self.dim_type.as_deref() {
            Some("time") => DimensionType::Time,
            Some("boolean") => DimensionType::Boolean,
            Some("number") => DimensionType::Numeric,
            _ => DimensionType::Categorical, // string, etc.
        };

        // Strip ${CUBE}. prefix from SQL
        let sql = self.sql.map(|s| strip_cube_placeholder(&s));

        Dimension {
            name: self.name,
            r#type: dim_type,
            sql,
            granularity: None,
            supported_granularities: None,
            label: self.title,
            description: self.description,
            metadata: None,
            meta: None,
            format: None,
            value_format_name: None,
            parent: None,
            window: None,
            public: true,
        }
    }
}

impl CubeMeasure {
    fn into_metric(self) -> Metric {
        // Map Cube.js measure types to aggregations
        let (metric_type, agg) = match self.measure_type.as_deref() {
            Some("count") => (MetricType::Simple, Some(Aggregation::Count)),
            Some("countDistinct") | Some("count_distinct") => {
                (MetricType::Simple, Some(Aggregation::CountDistinct))
            }
            Some("sum") => (MetricType::Simple, Some(Aggregation::Sum)),
            Some("avg") => (MetricType::Simple, Some(Aggregation::Avg)),
            Some("min") => (MetricType::Simple, Some(Aggregation::Min)),
            Some("max") => (MetricType::Simple, Some(Aggregation::Max)),
            Some("stddev") => (MetricType::Simple, Some(Aggregation::Stddev)),
            Some("stddev_pop") => (MetricType::Simple, Some(Aggregation::StddevPop)),
            Some("variance") => (MetricType::Simple, Some(Aggregation::Variance)),
            Some("variance_pop") => (MetricType::Simple, Some(Aggregation::VariancePop)),
            Some("number") => (MetricType::Derived, None), // derived/calculated
            _ => (MetricType::Simple, Some(Aggregation::Sum)),
        };

        // Strip ${CUBE}. prefix from SQL
        let sql = self.sql.map(|s| strip_cube_placeholder(&s));

        // Convert filters
        let filters = self
            .filters
            .into_iter()
            .map(|f| strip_cube_placeholder(&f.sql))
            .collect();

        Metric {
            name: self.name,
            extends: None,
            r#type: metric_type,
            agg,
            sql,
            numerator: None,
            denominator: None,
            offset_window: None,
            filters,
            label: self.title,
            description: self.description,
            metadata: None,
            meta: None,
            window: None,
            grain_to_date: None,
            window_expression: None,
            window_frame: None,
            window_order: None,
            base_metric: None,
            comparison_type: None,
            time_offset: None,
            calculation: None,
            entity: None,
            base_event: None,
            conversion_event: None,
            conversion_window: None,
            steps: None,
            cohort_event: None,
            activity_event: None,
            periods: None,
            retention_granularity: None,
            inner_metrics: None,
            entity_dimensions: None,
            having: None,
            fill_nulls_with: None,
            format: None,
            value_format_name: None,
            drill_fields: None,
            non_additive_dimension: None,
            public: true,
        }
    }
}

impl CubeSegment {
    fn into_segment(self) -> Segment {
        // Convert ${CUBE} to {model} for our segment format
        let sql = self.sql.replace("${CUBE}", "{model}");

        Segment {
            name: self.name,
            sql,
            description: self.description,
            public: true,
        }
    }
}

/// Strip ${CUBE}. prefix from SQL expressions
fn strip_cube_placeholder(sql: &str) -> String {
    sql.replace("${CUBE}.", "").replace("${CUBE}", "")
}

/// Split a Cube member reference into `(cube_name, column)`.
///
/// Handles both `${cube.col}` / `{cube.col}` (column inside the braces) and
/// `${cube}.col` / `${CUBE}.col` (column trailing the braces).
fn split_cube_ref(inner: &str, trailing: Option<&str>) -> (String, Option<String>) {
    if let Some(col) = trailing {
        return (inner.to_string(), Some(col.to_string()));
    }
    if let Some((head, col)) = inner.split_once('.') {
        return (head.to_string(), Some(col.to_string()));
    }
    (inner.to_string(), None)
}

/// Convert a Cube join condition into Sidemantic form.
///
/// Cube expresses joins as a SQL equality referencing members with `${CUBE}.col`
/// (this cube), `${target.col}` (the joined cube), and the single-brace
/// `{cube.col}` variants. Sidemantic uses `{from}` / `{to}` placeholders.
///
/// Returns `(native_sql, from_col, to_col)` where `native_sql` is the condition
/// rewritten with `{from}` / `{to}`, or `None` if it references a third cube and
/// cannot be represented faithfully. `from_col` / `to_col` are populated only when
/// the condition is a plain `a = b` equality.
fn convert_cube_join(
    join_sql: &str,
    self_name: &str,
    join_name: &str,
) -> (Option<String>, Option<String>, Option<String>) {
    let re = Regex::new(r"\$?\{([^}]+)\}(?:\.(\w+))?").expect("valid cube member regex");

    let mut untranslatable = false;
    let mut refs: Vec<(&'static str, Option<String>)> = Vec::new();

    let native = re
        .replace_all(join_sql, |caps: &regex::Captures| {
            let inner = caps.get(1).map(|m| m.as_str()).unwrap_or("");
            let trailing = caps.get(2).map(|m| m.as_str());
            let (head, col) = split_cube_ref(inner, trailing);
            let side = if head == "CUBE" || head == self_name {
                "from"
            } else if head == join_name {
                "to"
            } else {
                // References a third cube: cannot be expressed with {from}/{to}.
                untranslatable = true;
                return caps.get(0).map(|m| m.as_str()).unwrap_or("").to_string();
            };
            refs.push((side, col.clone()));
            match col {
                Some(c) => format!("{{{side}}}.{c}"),
                None => format!("{{{side}}}"),
            }
        })
        .to_string();

    if untranslatable || refs.is_empty() {
        return (None, None, None);
    }

    // Detect a plain single-column equality: exactly two members (one per side)
    // joined by a single '=' with nothing else around them.
    let residual: String = re
        .replace_all(join_sql, "@")
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    let has_from = refs.iter().any(|(side, _)| *side == "from");
    let has_to = refs.iter().any(|(side, _)| *side == "to");
    let is_simple = residual == "@=@"
        && refs.len() == 2
        && has_from
        && has_to
        && refs.iter().all(|(_, col)| col.is_some());

    if is_simple {
        let from_col = refs
            .iter()
            .find(|(side, _)| *side == "from")
            .and_then(|(_, col)| col.clone());
        let to_col = refs
            .iter()
            .find(|(side, _)| *side == "to")
            .and_then(|(_, col)| col.clone());
        return (Some(native), from_col, to_col);
    }

    (Some(native), None, None)
}

/// Build a [`Relationship`] from a Cube `joins` entry.
fn relationship_from_cube_join(self_name: &str, join: CubeJoin) -> Option<Relationship> {
    let join_name = join.name;
    if join_name.is_empty() {
        return None;
    }

    let rel_type = match join
        .relationship
        .as_deref()
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("one_to_one" | "onetoone") => RelationshipType::OneToOne,
        Some("one_to_many" | "onetomany") => RelationshipType::OneToMany,
        Some("many_to_many" | "manytomany") => RelationshipType::ManyToMany,
        _ => RelationshipType::ManyToOne,
    };

    let join_sql = join.sql.unwrap_or_default();
    let (native_sql, from_col, to_col) = convert_cube_join(&join_sql, self_name, &join_name);

    let mut rel = Relationship::new(join_name.clone());
    rel.r#type = rel_type.clone();

    // Plain single-column equality on many_to_one / one_to_many -> structured keys.
    // Both engines agree on the join direction for these, so structured keys round-trip
    // cleanly. one_to_one is deliberately excluded: the engines interpret its FK/PK
    // direction differently, so the explicit condition is preserved instead.
    if let (Some(from_col), Some(to_col)) = (from_col, to_col) {
        match rel_type {
            RelationshipType::ManyToOne => {
                return Some(rel.with_keys(from_col, to_col));
            }
            RelationshipType::OneToMany => {
                // FK lives on the related model, local key on this one.
                return Some(rel.with_keys(to_col, from_col));
            }
            _ => {}
        }
    }

    // Composite / non-equality / one_to_one condition -> preserve the full predicate.
    if let Some(sql) = native_sql {
        return Some(rel.with_condition(sql));
    }

    // Unparseable (references another cube, or no recognizable members): fall back to
    // Cube's naming convention, but warn instead of silently faking a join.
    eprintln!(
        "warning: could not parse Cube join '{self_name}' -> '{join_name}' from SQL {join_sql:?}; \
         falling back to foreign key '{join_name}_id'."
    );
    rel.foreign_key = Some(format!("{join_name}_id"));
    rel.foreign_key_columns = Some(vec![format!("{join_name}_id")]);
    Some(rel)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cube_yaml() {
        let yaml = r#"
cubes:
  - name: orders
    sql_table: orders

    dimensions:
      - name: status
        sql: "${CUBE}.status"
        type: string
      - name: created_at
        sql: "${CUBE}.created_at"
        type: time

    measures:
      - name: count
        type: count
      - name: revenue
        sql: "${CUBE}.amount"
        type: sum

    segments:
      - name: completed
        sql: "${CUBE}.status = 'completed'"
"#;

        let config: CubeConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.cubes.len(), 1);

        let models = config.into_models();
        let orders = &models[0];
        assert_eq!(orders.name, "orders");
        assert_eq!(orders.dimensions.len(), 2);
        assert_eq!(orders.metrics.len(), 2);
        assert_eq!(orders.segments.len(), 1);

        // Check ${CUBE} was stripped from dimension SQL
        assert_eq!(orders.dimensions[0].sql, Some("status".to_string()));

        // Check ${CUBE} was converted to {model} in segment
        assert_eq!(orders.segments[0].sql, "{model}.status = 'completed'");
    }

    #[test]
    fn test_strip_cube_placeholder() {
        assert_eq!(strip_cube_placeholder("${CUBE}.status"), "status");
        assert_eq!(
            strip_cube_placeholder("${CUBE}.amount > 100"),
            "amount > 100"
        );
    }

    #[test]
    fn test_cube_adapter_parse_document() {
        let yaml = r#"
cubes:
  - name: orders
    sql_table: orders
    measures:
      - name: revenue
        sql: "${CUBE}.amount"
        type: sum
"#;
        let parsed = CubeAdapter::new().parse_document(yaml).unwrap();
        assert_eq!(parsed.models.len(), 1);
        assert_eq!(parsed.models[0].name, "orders");
        assert!(parsed.graph_metrics.is_empty());
        assert!(!parsed.explicit_relationships);
    }

    #[test]
    fn test_cube_simple_many_to_one_join() {
        let yaml = r#"
cubes:
  - name: orders
    sql_table: orders
    joins:
      - name: customers
        sql: "${CUBE}.customer_id = ${customers.id}"
        relationship: many_to_one
  - name: customers
    sql_table: customers
"#;
        let config: CubeConfig = serde_yaml::from_str(yaml).unwrap();
        let models = config.into_models();
        let orders = models.iter().find(|m| m.name == "orders").unwrap();
        assert_eq!(orders.relationships.len(), 1);
        let rel = &orders.relationships[0];
        assert_eq!(rel.name, "customers");
        assert_eq!(rel.r#type, RelationshipType::ManyToOne);
        assert_eq!(rel.foreign_key.as_deref(), Some("customer_id"));
        assert_eq!(rel.primary_key.as_deref(), Some("id"));
        assert!(rel.sql.is_none());
    }

    #[test]
    fn test_cube_simple_one_to_many_join() {
        let yaml = r#"
cubes:
  - name: orders
    sql_table: orders
    joins:
      - name: line_items
        sql: "${CUBE}.id = ${line_items.order_id}"
        relationship: one_to_many
  - name: line_items
    sql_table: line_items
"#;
        let config: CubeConfig = serde_yaml::from_str(yaml).unwrap();
        let models = config.into_models();
        let orders = models.iter().find(|m| m.name == "orders").unwrap();
        let rel = &orders.relationships[0];
        assert_eq!(rel.r#type, RelationshipType::OneToMany);
        // FK lives on the related model, local key on this one.
        assert_eq!(rel.foreign_key.as_deref(), Some("order_id"));
        assert_eq!(rel.primary_key.as_deref(), Some("id"));
        assert!(rel.sql.is_none());
    }

    #[test]
    fn test_cube_composite_join_preserves_condition() {
        let yaml = r#"
cubes:
  - name: line_items
    sql_table: line_items
    joins:
      - name: orders
        sql: "${CUBE}.order_id = ${orders.id} AND ${CUBE}.tenant_id = ${orders.tenant_id}"
        relationship: many_to_one
  - name: orders
    sql_table: orders
"#;
        let config: CubeConfig = serde_yaml::from_str(yaml).unwrap();
        let models = config.into_models();
        let line_items = models.iter().find(|m| m.name == "line_items").unwrap();
        let rel = &line_items.relationships[0];
        assert_eq!(rel.r#type, RelationshipType::ManyToOne);
        assert_eq!(
            rel.sql.as_deref(),
            Some("{from}.order_id = {to}.id AND {from}.tenant_id = {to}.tenant_id")
        );
    }

    #[test]
    fn test_cube_one_to_one_single_brace_uses_condition() {
        // Diamond-style single-brace references with the local cube named explicitly.
        let yaml = r#"
cubes:
  - name: a
    sql_table: a
    joins:
      - name: b
        sql: "{a.id} = {b.id}"
        relationship: one_to_one
  - name: b
    sql_table: b
"#;
        let config: CubeConfig = serde_yaml::from_str(yaml).unwrap();
        let models = config.into_models();
        let a = models.iter().find(|m| m.name == "a").unwrap();
        let rel = &a.relationships[0];
        assert_eq!(rel.r#type, RelationshipType::OneToOne);
        // one_to_one is preserved as an explicit condition (engines diverge on FK/PK).
        assert_eq!(rel.sql.as_deref(), Some("{from}.id = {to}.id"));
    }

    #[test]
    fn test_cube_unparseable_join_falls_back_to_convention() {
        let yaml = r#"
cubes:
  - name: x
    sql_table: x
    joins:
      - name: y
        sql: "${z.a} = ${w.b}"
        relationship: many_to_one
  - name: y
    sql_table: y
"#;
        let config: CubeConfig = serde_yaml::from_str(yaml).unwrap();
        let models = config.into_models();
        let x = models.iter().find(|m| m.name == "x").unwrap();
        let rel = &x.relationships[0];
        assert_eq!(rel.foreign_key.as_deref(), Some("y_id"));
        assert!(rel.sql.is_none());
    }
}
