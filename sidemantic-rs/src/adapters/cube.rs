//! Cube.js adapter: imports Cube YAML (`cubes:`) into the semantic graph.

use serde::{Deserialize, Serialize};

use crate::core::{
    Aggregation, Dimension, DimensionType, Metric, MetricType, Model, Relationship, Segment,
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
            relationships: Vec::<Relationship>::new(), // Cube.js uses joins differently
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
}
