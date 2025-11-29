//! YAML schema definitions for config loading
//!
//! Supports both native Sidemantic format and Cube.js format.

use serde::{Deserialize, Serialize};

use crate::core::{
    Aggregation, Dimension, DimensionType, Metric, MetricType, Model, Relationship,
    RelationshipType, Segment,
};

// =============================================================================
// Native Sidemantic Format
// =============================================================================

/// Root schema for native sidemantic YAML files
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SidemanticConfig {
    #[serde(default)]
    pub models: Vec<ModelConfig>,
    /// Graph-level metrics (can reference model metrics)
    #[serde(default)]
    pub metrics: Vec<MetricConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelConfig {
    pub name: String,
    /// Parent model to inherit from
    pub extends: Option<String>,
    pub table: Option<String>,
    pub sql: Option<String>,
    #[serde(default = "default_primary_key")]
    pub primary_key: String,
    pub description: Option<String>,
    #[serde(default)]
    pub dimensions: Vec<DimensionConfig>,
    #[serde(default)]
    pub metrics: Vec<MetricConfig>,
    #[serde(default)]
    pub relationships: Vec<RelationshipConfig>,
    #[serde(default)]
    pub segments: Vec<SegmentConfig>,
}

fn default_primary_key() -> String {
    "id".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DimensionConfig {
    pub name: String,
    #[serde(default, rename = "type")]
    pub dim_type: Option<String>,
    pub sql: Option<String>,
    pub granularity: Option<String>,
    pub description: Option<String>,
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricConfig {
    pub name: String,
    #[serde(default, rename = "type")]
    pub metric_type: Option<String>,
    pub agg: Option<String>,
    pub sql: Option<String>,
    pub numerator: Option<String>,
    pub denominator: Option<String>,
    #[serde(default)]
    pub filters: Vec<String>,
    pub description: Option<String>,
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationshipConfig {
    pub name: String,
    #[serde(default, rename = "type")]
    pub rel_type: Option<String>,
    pub foreign_key: Option<String>,
    pub primary_key: Option<String>,
    /// Custom SQL join condition using {from} and {to} placeholders
    pub sql: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentConfig {
    pub name: String,
    pub sql: String,
    pub description: Option<String>,
}

// =============================================================================
// Cube.js Format
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

impl SidemanticConfig {
    /// Convert to list of core Model types
    pub fn into_models(self) -> Vec<Model> {
        self.models.into_iter().map(|m| m.into_model()).collect()
    }
}

impl ModelConfig {
    /// Convert to core Model type
    pub fn into_model(self) -> Model {
        Model {
            name: self.name,
            table: self.table,
            sql: self.sql,
            primary_key: self.primary_key,
            dimensions: self
                .dimensions
                .into_iter()
                .map(|d| d.into_dimension())
                .collect(),
            metrics: self.metrics.into_iter().map(|m| m.into_metric()).collect(),
            relationships: self
                .relationships
                .into_iter()
                .map(|r| r.into_relationship())
                .collect(),
            segments: self
                .segments
                .into_iter()
                .map(|s| s.into_segment())
                .collect(),
            label: None,
            description: self.description,
        }
    }
}

impl DimensionConfig {
    fn into_dimension(self) -> Dimension {
        let dim_type = match self.dim_type.as_deref() {
            Some("time") => DimensionType::Time,
            Some("boolean") => DimensionType::Boolean,
            Some("numeric") => DimensionType::Numeric,
            _ => DimensionType::Categorical,
        };

        Dimension {
            name: self.name,
            r#type: dim_type,
            sql: self.sql,
            granularity: self.granularity,
            label: self.label,
            description: self.description,
        }
    }
}

impl MetricConfig {
    fn into_metric(self) -> Metric {
        let metric_type = match self.metric_type.as_deref() {
            Some("derived") => MetricType::Derived,
            Some("ratio") => MetricType::Ratio,
            _ => MetricType::Simple,
        };

        let agg = self.agg.as_deref().map(parse_aggregation);

        Metric {
            name: self.name,
            r#type: metric_type,
            agg,
            sql: self.sql,
            numerator: self.numerator,
            denominator: self.denominator,
            filters: self.filters,
            label: self.label,
            description: self.description,
            window: None,
            grain_to_date: None,
            base_metric: None,
            comparison_type: None,
            time_offset: None,
            calculation: None,
            fill_nulls_with: None,
            format: None,
        }
    }
}

impl RelationshipConfig {
    fn into_relationship(self) -> Relationship {
        let rel_type = match self.rel_type.as_deref() {
            Some("one_to_one") => RelationshipType::OneToOne,
            Some("one_to_many") => RelationshipType::OneToMany,
            Some("many_to_many") => RelationshipType::ManyToMany,
            _ => RelationshipType::ManyToOne,
        };

        Relationship {
            name: self.name,
            r#type: rel_type,
            foreign_key: self.foreign_key,
            primary_key: self.primary_key,
            sql: self.sql,
        }
    }
}

impl SegmentConfig {
    fn into_segment(self) -> Segment {
        Segment {
            name: self.name,
            sql: self.sql,
            description: self.description,
            public: true,
        }
    }
}

// Cube.js conversions

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
            primary_key,
            dimensions: self
                .dimensions
                .into_iter()
                .map(|d| d.into_dimension())
                .collect(),
            metrics: self.measures.into_iter().map(|m| m.into_metric()).collect(),
            relationships: Vec::new(), // Cube.js uses joins differently
            segments: self
                .segments
                .into_iter()
                .map(|s| s.into_segment())
                .collect(),
            label: None,
            description: self.description,
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
            label: self.title,
            description: self.description,
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
            r#type: metric_type,
            agg,
            sql,
            numerator: None,
            denominator: None,
            filters,
            label: self.title,
            description: self.description,
            window: None,
            grain_to_date: None,
            base_metric: None,
            comparison_type: None,
            time_offset: None,
            calculation: None,
            fill_nulls_with: None,
            format: None,
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

// =============================================================================
// Helpers
// =============================================================================

fn parse_aggregation(s: &str) -> Aggregation {
    match s.to_lowercase().as_str() {
        "count" => Aggregation::Count,
        "count_distinct" | "countdistinct" => Aggregation::CountDistinct,
        "sum" => Aggregation::Sum,
        "avg" | "average" => Aggregation::Avg,
        "min" => Aggregation::Min,
        "max" => Aggregation::Max,
        "median" => Aggregation::Median,
        _ => Aggregation::Sum,
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
    fn test_parse_native_yaml() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
      - name: order_date
        type: time
        sql: created_at
    metrics:
      - name: revenue
        agg: sum
        sql: amount
    segments:
      - name: completed
        sql: "{model}.status = 'completed'"
"#;

        let config: SidemanticConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.models.len(), 1);

        let models = config.into_models();
        let orders = &models[0];
        assert_eq!(orders.name, "orders");
        assert_eq!(orders.dimensions.len(), 2);
        assert_eq!(orders.metrics.len(), 1);
        assert_eq!(orders.segments.len(), 1);
    }

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
}
