//! YAML schema definitions for config loading
//!
//! Supports both native Sidemantic format and Cube.js format.

use serde::{Deserialize, Serialize};

use crate::core::{
    Aggregation, ComparisonCalculation, ComparisonType, Dimension, DimensionType, Metric,
    MetricType, Model, Parameter, ParameterType, PreAggregation, PreAggregationType, RefreshKey,
    Relationship, RelationshipType, Segment, TimeGrain,
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
    #[serde(default)]
    pub parameters: Vec<ParameterConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelConfig {
    pub name: String,
    /// Parent model to inherit from
    pub extends: Option<String>,
    pub table: Option<String>,
    pub sql: Option<String>,
    pub source_uri: Option<String>,
    #[serde(default = "default_primary_key_config")]
    pub primary_key: KeyConfig,
    #[serde(default)]
    pub primary_key_columns: Option<Vec<String>>,
    #[serde(default)]
    pub unique_keys: Option<Vec<Vec<String>>>,
    pub description: Option<String>,
    #[serde(default)]
    pub dimensions: Vec<DimensionConfig>,
    #[serde(default)]
    pub metrics: Vec<MetricConfig>,
    #[serde(default)]
    pub relationships: Vec<RelationshipConfig>,
    #[serde(default)]
    pub segments: Vec<SegmentConfig>,
    #[serde(default)]
    pub pre_aggregations: Vec<PreAggregationConfig>,
    pub default_time_dimension: Option<String>,
    pub default_grain: Option<String>,
}

fn default_primary_key() -> String {
    "id".to_string()
}

fn default_primary_key_config() -> KeyConfig {
    KeyConfig::Single(default_primary_key())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum KeyConfig {
    Single(String),
    Multiple(Vec<String>),
}

impl KeyConfig {
    fn into_columns(self) -> Vec<String> {
        match self {
            Self::Single(value) => vec![value],
            Self::Multiple(values) => values,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DimensionConfig {
    pub name: String,
    #[serde(default, rename = "type")]
    pub dim_type: Option<String>,
    pub sql: Option<String>,
    pub granularity: Option<String>,
    pub supported_granularities: Option<Vec<String>>,
    pub description: Option<String>,
    pub label: Option<String>,
    pub format: Option<String>,
    pub value_format_name: Option<String>,
    pub parent: Option<String>,
    pub window: Option<String>,
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
    pub offset_window: Option<String>,
    pub window: Option<String>,
    pub grain_to_date: Option<String>,
    pub window_expression: Option<String>,
    pub window_frame: Option<String>,
    pub window_order: Option<String>,
    pub base_metric: Option<String>,
    pub comparison_type: Option<String>,
    pub time_offset: Option<String>,
    pub calculation: Option<String>,
    pub entity: Option<String>,
    pub base_event: Option<String>,
    pub conversion_event: Option<String>,
    pub conversion_window: Option<String>,
    pub fill_nulls_with: Option<serde_json::Value>,
    pub format: Option<String>,
    pub value_format_name: Option<String>,
    pub drill_fields: Option<Vec<String>>,
    pub non_additive_dimension: Option<String>,
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
    pub foreign_key: Option<KeyConfig>,
    #[serde(default)]
    pub foreign_key_columns: Option<Vec<String>>,
    pub primary_key: Option<KeyConfig>,
    #[serde(default)]
    pub primary_key_columns: Option<Vec<String>>,
    pub through: Option<String>,
    pub through_foreign_key: Option<String>,
    pub related_foreign_key: Option<String>,
    /// Custom SQL join condition using {from} and {to} placeholders
    pub sql: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentConfig {
    pub name: String,
    pub sql: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreAggregationConfig {
    pub name: String,
    #[serde(default, rename = "type")]
    pub preagg_type: Option<String>,
    #[serde(default)]
    pub measures: Option<Vec<String>>,
    #[serde(default)]
    pub dimensions: Option<Vec<String>>,
    #[serde(default)]
    pub time_dimension: Option<String>,
    #[serde(default)]
    pub granularity: Option<String>,
    #[serde(default)]
    pub partition_granularity: Option<String>,
    #[serde(default)]
    pub build_range_start: Option<String>,
    #[serde(default)]
    pub build_range_end: Option<String>,
    #[serde(default = "default_scheduled_refresh")]
    pub scheduled_refresh: bool,
    #[serde(default)]
    pub refresh_key: Option<RefreshKeyConfig>,
    #[serde(default)]
    pub indexes: Option<Vec<IndexConfig>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshKeyConfig {
    #[serde(default)]
    pub every: Option<String>,
    #[serde(default)]
    pub sql: Option<String>,
    #[serde(default)]
    pub incremental: bool,
    #[serde(default)]
    pub update_window: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfig {
    pub name: String,
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(default = "default_index_type", rename = "type")]
    pub index_type: String,
}

fn default_index_type() -> String {
    "regular".to_string()
}

fn default_scheduled_refresh() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParameterConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub parameter_type: ParameterType,
    pub description: Option<String>,
    pub label: Option<String>,
    pub default_value: Option<serde_json::Value>,
    pub allowed_values: Option<Vec<serde_json::Value>>,
    #[serde(default)]
    pub default_to_today: bool,
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
    /// Convert to core models, top-level metrics, and top-level parameters.
    pub fn into_parts(self) -> (Vec<Model>, Vec<Metric>, Vec<Parameter>) {
        let models = self.models.into_iter().map(|m| m.into_model()).collect();
        let metrics = self.metrics.into_iter().map(|m| m.into_metric()).collect();
        let parameters = self
            .parameters
            .into_iter()
            .map(|p| p.into_parameter())
            .collect();
        (models, metrics, parameters)
    }

    /// Convert to list of core Model types
    pub fn into_models(self) -> Vec<Model> {
        self.into_parts().0
    }
}

impl ModelConfig {
    /// Convert to core Model type
    pub fn into_model(self) -> Model {
        let primary_key_columns = self
            .primary_key_columns
            .filter(|columns| !columns.is_empty())
            .unwrap_or_else(|| self.primary_key.into_columns());
        let primary_key = primary_key_columns
            .first()
            .cloned()
            .unwrap_or_else(default_primary_key);

        Model {
            name: self.name,
            table: self.table,
            sql: self.sql,
            source_uri: self.source_uri,
            extends: self.extends,
            primary_key,
            primary_key_columns,
            unique_keys: self.unique_keys,
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
            pre_aggregations: self
                .pre_aggregations
                .into_iter()
                .map(|p| p.into_pre_aggregation())
                .collect(),
            default_time_dimension: self.default_time_dimension,
            default_grain: self.default_grain,
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
            supported_granularities: self.supported_granularities,
            label: self.label,
            description: self.description,
            format: self.format,
            value_format_name: self.value_format_name,
            parent: self.parent,
            window: self.window,
        }
    }
}

impl MetricConfig {
    fn into_metric(self) -> Metric {
        let metric_type = match self.metric_type.as_deref() {
            Some("derived") => MetricType::Derived,
            Some("ratio") => MetricType::Ratio,
            Some("cumulative") => MetricType::Cumulative,
            Some("time_comparison") => MetricType::TimeComparison,
            Some("conversion") => MetricType::Conversion,
            _ => {
                if self.agg.is_none() && self.sql.is_some() {
                    MetricType::Derived
                } else {
                    MetricType::Simple
                }
            }
        };

        let agg = self.agg.as_deref().map(parse_aggregation);
        let grain_to_date = self.grain_to_date.as_deref().and_then(parse_time_grain);
        let comparison_type = self
            .comparison_type
            .as_deref()
            .and_then(parse_comparison_type);
        let calculation = self
            .calculation
            .as_deref()
            .and_then(parse_comparison_calculation);

        Metric {
            name: self.name,
            r#type: metric_type,
            agg,
            sql: self.sql,
            numerator: self.numerator,
            denominator: self.denominator,
            offset_window: self.offset_window,
            filters: self.filters,
            label: self.label,
            description: self.description,
            window: self.window,
            grain_to_date,
            window_expression: self.window_expression,
            window_frame: self.window_frame,
            window_order: self.window_order,
            base_metric: self.base_metric,
            comparison_type,
            time_offset: self.time_offset,
            calculation,
            entity: self.entity,
            base_event: self.base_event,
            conversion_event: self.conversion_event,
            conversion_window: self.conversion_window,
            fill_nulls_with: self.fill_nulls_with,
            format: self.format,
            value_format_name: self.value_format_name,
            drill_fields: self.drill_fields,
            non_additive_dimension: self.non_additive_dimension,
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

        let foreign_key_columns = self
            .foreign_key_columns
            .filter(|columns| !columns.is_empty())
            .or_else(|| self.foreign_key.clone().map(KeyConfig::into_columns));
        let primary_key_columns = self
            .primary_key_columns
            .filter(|columns| !columns.is_empty())
            .or_else(|| self.primary_key.clone().map(KeyConfig::into_columns));

        Relationship {
            name: self.name,
            r#type: rel_type,
            foreign_key: foreign_key_columns
                .as_ref()
                .and_then(|columns| columns.first().cloned()),
            foreign_key_columns,
            primary_key: primary_key_columns
                .as_ref()
                .and_then(|columns| columns.first().cloned()),
            primary_key_columns,
            through: self.through,
            through_foreign_key: self.through_foreign_key,
            related_foreign_key: self.related_foreign_key,
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

impl PreAggregationConfig {
    fn into_pre_aggregation(self) -> PreAggregation {
        let preagg_type = match self.preagg_type.as_deref() {
            Some("original_sql") => PreAggregationType::OriginalSql,
            Some("rollup_join") => PreAggregationType::RollupJoin,
            Some("lambda") => PreAggregationType::Lambda,
            _ => PreAggregationType::Rollup,
        };

        PreAggregation {
            name: self.name,
            preagg_type,
            measures: self.measures,
            dimensions: self.dimensions,
            time_dimension: self.time_dimension,
            granularity: self.granularity,
            partition_granularity: self.partition_granularity,
            build_range_start: self.build_range_start,
            build_range_end: self.build_range_end,
            scheduled_refresh: self.scheduled_refresh,
            refresh_key: self.refresh_key.map(|r| RefreshKey {
                every: r.every,
                sql: r.sql,
                incremental: r.incremental,
                update_window: r.update_window,
            }),
            indexes: self.indexes.map(|indexes| {
                indexes
                    .into_iter()
                    .map(|idx| crate::core::Index {
                        name: idx.name,
                        columns: idx.columns,
                        index_type: idx.index_type,
                    })
                    .collect()
            }),
        }
    }
}

impl ParameterConfig {
    fn into_parameter(self) -> Parameter {
        Parameter {
            name: self.name,
            parameter_type: self.parameter_type,
            description: self.description,
            label: self.label,
            default_value: self.default_value,
            allowed_values: self.allowed_values,
            default_to_today: self.default_to_today,
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
            relationships: Vec::new(), // Cube.js uses joins differently
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
            format: None,
            value_format_name: None,
            parent: None,
            window: None,
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
            offset_window: None,
            filters,
            label: self.title,
            description: self.description,
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
            fill_nulls_with: None,
            format: None,
            value_format_name: None,
            drill_fields: None,
            non_additive_dimension: None,
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

fn parse_time_grain(s: &str) -> Option<TimeGrain> {
    match s.to_lowercase().as_str() {
        "day" => Some(TimeGrain::Day),
        "week" => Some(TimeGrain::Week),
        "month" => Some(TimeGrain::Month),
        "quarter" => Some(TimeGrain::Quarter),
        "year" => Some(TimeGrain::Year),
        _ => None,
    }
}

fn parse_comparison_type(s: &str) -> Option<ComparisonType> {
    match s.to_lowercase().as_str() {
        "yoy" => Some(ComparisonType::Yoy),
        "mom" => Some(ComparisonType::Mom),
        "wow" => Some(ComparisonType::Wow),
        "dod" => Some(ComparisonType::Dod),
        "qoq" => Some(ComparisonType::Qoq),
        "prior_period" => Some(ComparisonType::PriorPeriod),
        _ => None,
    }
}

fn parse_comparison_calculation(s: &str) -> Option<ComparisonCalculation> {
    match s.to_lowercase().as_str() {
        "difference" => Some(ComparisonCalculation::Difference),
        "percent_change" => Some(ComparisonCalculation::PercentChange),
        "ratio" => Some(ComparisonCalculation::Ratio),
        _ => None,
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
parameters:
  - name: status
    type: string
    default_value: pending
"#;

        let config: SidemanticConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.models.len(), 1);
        assert_eq!(config.parameters.len(), 1);

        let (models, _, parameters) = config.into_parts();
        let orders = &models[0];
        assert_eq!(orders.name, "orders");
        assert_eq!(orders.dimensions.len(), 2);
        assert_eq!(orders.metrics.len(), 1);
        assert_eq!(orders.segments.len(), 1);
        assert_eq!(parameters.len(), 1);
        assert_eq!(parameters[0].name, "status");
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
    fn test_parse_many_to_many_relationship_fields() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    relationships:
      - name: products
        type: many_to_many
        through: order_items
        through_foreign_key: order_id
        related_foreign_key: product_id
        primary_key: product_id
  - name: order_items
    table: order_items
  - name: products
    table: products
    primary_key: product_id
"#;

        let config: SidemanticConfig = serde_yaml::from_str(yaml).unwrap();
        let (models, _, _) = config.into_parts();

        let orders = models.iter().find(|m| m.name == "orders").unwrap();
        let rel = orders
            .relationships
            .iter()
            .find(|r| r.name == "products")
            .unwrap();
        assert_eq!(rel.r#type, RelationshipType::ManyToMany);
        assert_eq!(rel.through.as_deref(), Some("order_items"));
        assert_eq!(rel.through_foreign_key.as_deref(), Some("order_id"));
        assert_eq!(rel.related_foreign_key.as_deref(), Some("product_id"));
        assert_eq!(rel.primary_key.as_deref(), Some("product_id"));
    }

    #[test]
    fn test_parse_native_yaml_composite_keys() {
        let yaml = r#"
models:
  - name: order_items
    table: order_items
    primary_key: [order_id, item_id]
  - name: shipments
    table: shipments
    primary_key: shipment_id
    relationships:
      - name: order_items
        type: many_to_one
        foreign_key: [order_id, item_id]
        primary_key: [order_id, item_id]
"#;

        let config: SidemanticConfig = serde_yaml::from_str(yaml).unwrap();
        let (models, _, _) = config.into_parts();

        let order_items = models.iter().find(|m| m.name == "order_items").unwrap();
        assert_eq!(
            order_items.primary_key_columns,
            vec!["order_id".to_string(), "item_id".to_string()]
        );

        let shipments = models.iter().find(|m| m.name == "shipments").unwrap();
        let rel = shipments
            .relationships
            .iter()
            .find(|r| r.name == "order_items")
            .unwrap();
        assert_eq!(
            rel.foreign_key_columns.as_ref().unwrap(),
            &vec!["order_id".to_string(), "item_id".to_string()]
        );
        assert_eq!(
            rel.primary_key_columns.as_ref().unwrap(),
            &vec!["order_id".to_string(), "item_id".to_string()]
        );
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
