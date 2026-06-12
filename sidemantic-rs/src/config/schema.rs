//! YAML schema definitions for config loading
//!
//! Supports both native Sidemantic format and Cube.js format.

use serde::{Deserialize, Serialize};

use crate::core::{
    Aggregation, CohortInnerMetric, ComparisonCalculation, ComparisonType, Dimension,
    DimensionType, Metric, MetricType, Model, Parameter, ParameterType, PreAggregation,
    PreAggregationType, RefreshKey, Relationship, RelationshipType, Segment, TimeGrain,
};

pub const NATIVE_FORMAT_VERSION: u32 = 1;

// =============================================================================
// Native Sidemantic Format
// =============================================================================

/// Root schema for native sidemantic YAML files
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct SidemanticConfig {
    #[serde(default)]
    pub version: Option<u32>,
    #[serde(default)]
    pub models: Vec<ModelConfig>,
    /// Graph-level metrics (can reference model metrics)
    #[serde(default)]
    pub metrics: Vec<MetricConfig>,
    #[serde(default)]
    pub parameters: Vec<ParameterConfig>,
    #[serde(default)]
    pub sql_metrics: Option<String>,
    #[serde(default)]
    pub sql_segments: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    pub label: Option<String>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
    #[serde(default)]
    pub meta: Option<serde_json::Value>,
    #[serde(default)]
    pub auto_dimensions: bool,
    #[serde(default)]
    pub dimensions: Vec<DimensionConfig>,
    #[serde(default, alias = "measures")]
    pub metrics: Vec<MetricConfig>,
    #[serde(default)]
    pub relationships: Vec<RelationshipConfig>,
    #[serde(default)]
    pub segments: Vec<SegmentConfig>,
    #[serde(default)]
    pub pre_aggregations: Vec<PreAggregationConfig>,
    pub default_time_dimension: Option<String>,
    pub default_grain: Option<String>,
    #[serde(default)]
    pub sql_metrics: Option<String>,
    #[serde(default)]
    pub sql_segments: Option<String>,
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
#[serde(deny_unknown_fields)]
pub struct DimensionConfig {
    pub name: String,
    #[serde(default, rename = "type")]
    pub dim_type: Option<String>,
    #[serde(default, alias = "expr")]
    pub sql: Option<String>,
    pub granularity: Option<String>,
    pub supported_granularities: Option<Vec<String>>,
    pub description: Option<String>,
    pub label: Option<String>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
    #[serde(default)]
    pub meta: Option<serde_json::Value>,
    pub format: Option<String>,
    pub value_format_name: Option<String>,
    pub parent: Option<String>,
    pub window: Option<String>,
    #[serde(default = "default_public")]
    pub public: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricConfig {
    pub name: String,
    pub extends: Option<String>,
    #[serde(default, rename = "type")]
    pub metric_type: Option<String>,
    pub agg: Option<String>,
    #[serde(default, alias = "expr", alias = "measure")]
    pub sql: Option<String>,
    #[serde(default, rename = "metrics", skip_serializing_if = "Option::is_none")]
    _legacy_metric_dependencies: Option<Vec<String>>,
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
    pub steps: Option<Vec<String>>,
    pub cohort_event: Option<String>,
    pub activity_event: Option<String>,
    pub periods: Option<usize>,
    pub retention_granularity: Option<String>,
    pub granularity: Option<String>,
    pub inner_metrics: Option<Vec<CohortInnerMetricConfig>>,
    pub entity_dimensions: Option<Vec<String>>,
    pub having: Option<String>,
    pub fill_nulls_with: Option<serde_json::Value>,
    pub format: Option<String>,
    pub value_format_name: Option<String>,
    pub drill_fields: Option<Vec<String>>,
    pub non_additive_dimension: Option<String>,
    #[serde(default)]
    pub filters: Vec<String>,
    pub description: Option<String>,
    pub label: Option<String>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
    #[serde(default)]
    pub meta: Option<serde_json::Value>,
    #[serde(default = "default_public")]
    pub public: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CohortInnerMetricConfig {
    pub name: String,
    pub agg: Option<String>,
    pub sql: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    #[serde(default)]
    pub through_foreign_key_columns: Option<Vec<String>>,
    pub related_foreign_key: Option<String>,
    #[serde(default)]
    pub related_foreign_key_columns: Option<Vec<String>>,
    /// Custom SQL join condition using {from} and {to} placeholders
    pub sql: Option<String>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentConfig {
    pub name: String,
    pub sql: String,
    pub description: Option<String>,
    #[serde(default = "default_public")]
    pub public: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreAggregationConfig {
    pub name: String,
    #[serde(default, rename = "type")]
    pub preagg_type: Option<String>,
    #[serde(default)]
    pub sql: Option<String>,
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
    #[serde(default)]
    pub meta: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct IndexConfig {
    pub name: String,
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(default = "default_index_type", rename = "type")]
    pub index_type: String,
}

fn default_public() -> bool {
    true
}

fn default_index_type() -> String {
    "regular".to_string()
}

fn default_scheduled_refresh() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    pub fn validate_version(&self) -> crate::error::Result<()> {
        match self.version {
            None | Some(NATIVE_FORMAT_VERSION) => Ok(()),
            Some(version) => Err(crate::error::SidemanticError::validation_issue(
                "unsupported_native_format_version",
                None,
                "version",
                Some(&version.to_string()),
                format!(
                    "Unsupported native Sidemantic format version {version}; supported version is {NATIVE_FORMAT_VERSION}"
                ),
            )),
        }
    }

    pub fn validate_contract(&self) -> crate::error::Result<()> {
        self.validate_version()?;

        for model in &self.models {
            if model.auto_dimensions {
                return Err(crate::error::SidemanticError::validation_issue(
                    "unsupported_auto_dimensions",
                    Some(&model.name),
                    &format!("models.{}.auto_dimensions", model.name),
                    Some("true"),
                    "Rust native runtime does not support auto_dimensions; declare dimensions explicitly or set auto_dimensions: false",
                ));
            }
            validate_optional_enum(
                model.default_grain.as_deref(),
                &format!("models.{}.default_grain", model.name),
                TIME_GRAINS,
            )?;
            for dimension in &model.dimensions {
                validate_optional_enum(
                    dimension.dim_type.as_deref(),
                    &format!("models.{}.dimensions.{}.type", model.name, dimension.name),
                    &[
                        "categorical",
                        "string",
                        "time",
                        "boolean",
                        "numeric",
                        "number",
                    ],
                )?;
                validate_optional_enum(
                    dimension.granularity.as_deref(),
                    &format!(
                        "models.{}.dimensions.{}.granularity",
                        model.name, dimension.name
                    ),
                    TIME_GRAINS,
                )?;
                if let Some(supported_granularities) = dimension.supported_granularities.as_ref() {
                    for granularity in supported_granularities {
                        validate_enum(
                            granularity,
                            &format!(
                                "models.{}.dimensions.{}.supported_granularities",
                                model.name, dimension.name
                            ),
                            TIME_GRAINS,
                        )?;
                    }
                }
            }

            for metric in &model.metrics {
                validate_metric_config(
                    metric,
                    &format!("models.{}.metrics.{}", model.name, metric.name),
                )?;
            }

            for relationship in &model.relationships {
                validate_optional_enum(
                    relationship.rel_type.as_deref(),
                    &format!(
                        "models.{}.relationships.{}.type",
                        model.name, relationship.name
                    ),
                    &[
                        "many_to_one",
                        "manytoone",
                        "one_to_one",
                        "onetoone",
                        "one_to_many",
                        "onetomany",
                        "many_to_many",
                        "manytomany",
                    ],
                )?;
            }

            for preagg in &model.pre_aggregations {
                validate_optional_enum(
                    preagg.preagg_type.as_deref(),
                    &format!(
                        "models.{}.pre_aggregations.{}.type",
                        model.name, preagg.name
                    ),
                    &["rollup", "original_sql", "rollup_join", "lambda"],
                )?;
                if let Some(granularity) = preagg.granularity.as_deref() {
                    validate_enum(
                        granularity,
                        &format!(
                            "models.{}.pre_aggregations.{}.granularity",
                            model.name, preagg.name
                        ),
                        PREAGG_GRANULARITIES,
                    )?;
                }
                if let Some(partition_granularity) = preagg.partition_granularity.as_deref() {
                    validate_enum(
                        partition_granularity,
                        &format!(
                            "models.{}.pre_aggregations.{}.partition_granularity",
                            model.name, preagg.name
                        ),
                        PARTITION_GRANULARITIES,
                    )?;
                }
            }
        }

        for metric in &self.metrics {
            validate_metric_config(metric, &format!("metrics.{}", metric.name))?;
        }

        Ok(())
    }

    /// Convert to core models, top-level metrics, and top-level parameters.
    pub fn into_parts(self) -> crate::error::Result<(Vec<Model>, Vec<Metric>, Vec<Parameter>)> {
        self.validate_contract()?;
        let models = self.models.into_iter().map(|m| m.into_model()).collect();
        let metrics = self.metrics.into_iter().map(|m| m.into_metric()).collect();
        let parameters = self
            .parameters
            .into_iter()
            .map(|p| p.into_parameter())
            .collect();
        Ok((models, metrics, parameters))
    }

    /// Convert to list of core Model types
    pub fn into_models(self) -> crate::error::Result<Vec<Model>> {
        Ok(self.into_parts()?.0)
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
            label: self.label,
            description: self.description,
            metadata: self.metadata,
            meta: self.meta,
        }
    }
}

impl DimensionConfig {
    fn into_dimension(self) -> Dimension {
        let dim_type = match self
            .dim_type
            .as_deref()
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("time") => DimensionType::Time,
            Some("boolean") => DimensionType::Boolean,
            Some("numeric" | "number") => DimensionType::Numeric,
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
            metadata: self.metadata,
            meta: self.meta,
            format: self.format,
            value_format_name: self.value_format_name,
            parent: self.parent,
            window: self.window,
            public: self.public,
        }
    }
}

impl MetricConfig {
    fn into_metric(self) -> Metric {
        let explicit_metric_type = self.metric_type.as_deref().map(str::to_ascii_lowercase);
        let can_normalize_inline_aggregation =
            matches!(explicit_metric_type.as_deref(), None | Some("simple"));
        let inline_aggregation = if self.agg.is_none() && can_normalize_inline_aggregation {
            self.sql
                .as_deref()
                .and_then(parse_inline_metric_aggregation)
        } else {
            None
        };

        let metric_type = match explicit_metric_type.as_deref() {
            Some("simple") => MetricType::Simple,
            Some("derived") => MetricType::Derived,
            Some("ratio") => MetricType::Ratio,
            Some("cumulative") => MetricType::Cumulative,
            Some("time_comparison" | "timecomparison") => MetricType::TimeComparison,
            Some("conversion") => MetricType::Conversion,
            Some("retention") => MetricType::Retention,
            Some("cohort") => MetricType::Cohort,
            _ => {
                if inline_aggregation.is_none() && self.agg.is_none() && self.sql.is_some() {
                    MetricType::Derived
                } else {
                    MetricType::Simple
                }
            }
        };

        let agg = self
            .agg
            .as_deref()
            .map(parse_aggregation)
            .or_else(|| inline_aggregation.as_ref().map(|(agg, _)| agg.clone()));
        let sql = inline_aggregation
            .as_ref()
            .and_then(|(_, inner_sql)| inner_sql.clone())
            .or(self.sql);
        let grain_to_date = self.grain_to_date.as_deref().and_then(parse_time_grain);
        let comparison_type = self
            .comparison_type
            .as_deref()
            .and_then(parse_comparison_type);
        let calculation = self
            .calculation
            .as_deref()
            .and_then(parse_comparison_calculation);
        let inner_metrics = self.inner_metrics.map(|items| {
            items
                .into_iter()
                .map(|item| CohortInnerMetric {
                    name: item.name,
                    agg: item.agg.as_deref().map(parse_aggregation),
                    sql: item.sql,
                })
                .collect()
        });

        Metric {
            name: self.name,
            extends: self.extends,
            r#type: metric_type,
            agg,
            sql,
            numerator: self.numerator,
            denominator: self.denominator,
            offset_window: self.offset_window,
            filters: self.filters,
            label: self.label,
            description: self.description,
            metadata: self.metadata,
            meta: self.meta,
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
            steps: self.steps,
            cohort_event: self.cohort_event,
            activity_event: self.activity_event,
            periods: self.periods,
            retention_granularity: self.retention_granularity.or(self.granularity),
            inner_metrics,
            entity_dimensions: self.entity_dimensions,
            having: self.having,
            fill_nulls_with: self.fill_nulls_with,
            format: self.format,
            value_format_name: self.value_format_name,
            drill_fields: self.drill_fields,
            non_additive_dimension: self.non_additive_dimension,
            public: self.public,
        }
    }
}

impl RelationshipConfig {
    fn into_relationship(self) -> Relationship {
        let rel_type = match self
            .rel_type
            .as_deref()
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("one_to_one" | "onetoone") => RelationshipType::OneToOne,
            Some("one_to_many" | "onetomany") => RelationshipType::OneToMany,
            Some("many_to_many" | "manytomany") => RelationshipType::ManyToMany,
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
            through_foreign_key_columns: self
                .through_foreign_key_columns
                .filter(|columns| !columns.is_empty()),
            related_foreign_key: self.related_foreign_key,
            related_foreign_key_columns: self
                .related_foreign_key_columns
                .filter(|columns| !columns.is_empty()),
            sql: self.sql,
            metadata: self.metadata,
        }
    }
}

impl SegmentConfig {
    fn into_segment(self) -> Segment {
        Segment {
            name: self.name,
            sql: self.sql,
            description: self.description,
            public: self.public,
        }
    }
}

impl PreAggregationConfig {
    fn into_pre_aggregation(self) -> PreAggregation {
        let preagg_type = match self
            .preagg_type
            .as_deref()
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("original_sql") => PreAggregationType::OriginalSql,
            Some("rollup_join") => PreAggregationType::RollupJoin,
            Some("lambda") => PreAggregationType::Lambda,
            _ => PreAggregationType::Rollup,
        };

        PreAggregation {
            name: self.name,
            preagg_type,
            sql: self.sql,
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
            meta: self.meta,
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

// =============================================================================
// Helpers
// =============================================================================

const TIME_GRAINS: &[&str] = &[
    "second", "minute", "hour", "day", "week", "month", "quarter", "year",
];
const PERIOD_GRAINS: &[&str] = &["day", "week", "month", "quarter", "year"];
const RETENTION_GRAINS: &[&str] = &["day", "week", "month"];
const PREAGG_GRANULARITIES: &[&str] = &["hour", "day", "week", "month", "quarter", "year"];
const PARTITION_GRANULARITIES: &[&str] = &["day", "week", "month", "quarter", "year"];

fn validate_optional_enum(
    value: Option<&str>,
    field_path: &str,
    allowed: &[&str],
) -> crate::error::Result<()> {
    if let Some(value) = value {
        validate_enum(value, field_path, allowed)?;
    }
    Ok(())
}

fn validate_enum(value: &str, field_path: &str, allowed: &[&str]) -> crate::error::Result<()> {
    let normalized = value.to_ascii_lowercase();
    if allowed
        .iter()
        .any(|allowed_value| *allowed_value == normalized)
    {
        return Ok(());
    }

    Err(crate::error::SidemanticError::validation_issue(
        "unsupported_enum_value",
        model_name_from_field_path(field_path).as_deref(),
        field_path,
        Some(value),
        format!(
            "Unsupported value '{value}' for {field_path}; supported values are {}",
            allowed.join(", ")
        ),
    ))
}

fn model_name_from_field_path(field_path: &str) -> Option<String> {
    let mut parts = field_path.split('.');
    if parts.next()? != "models" {
        return None;
    }
    parts.next().map(ToString::to_string)
}

fn validate_metric_config(metric: &MetricConfig, field_path: &str) -> crate::error::Result<()> {
    validate_optional_enum(
        metric.metric_type.as_deref(),
        &format!("{field_path}.type"),
        &[
            "simple",
            "derived",
            "ratio",
            "cumulative",
            "time_comparison",
            "timecomparison",
            "conversion",
            "retention",
            "cohort",
        ],
    )?;
    validate_optional_enum(
        metric.agg.as_deref(),
        &format!("{field_path}.agg"),
        &[
            "count",
            "count_distinct",
            "countdistinct",
            "sum",
            "avg",
            "average",
            "min",
            "max",
            "median",
            "stddev",
            "stddev_pop",
            "variance",
            "variance_pop",
            "expression",
        ],
    )?;
    validate_optional_enum(
        metric.grain_to_date.as_deref(),
        &format!("{field_path}.grain_to_date"),
        PERIOD_GRAINS,
    )?;
    validate_optional_enum(
        metric.retention_granularity.as_deref(),
        &format!("{field_path}.retention_granularity"),
        RETENTION_GRAINS,
    )?;
    validate_optional_enum(
        metric.granularity.as_deref(),
        &format!("{field_path}.granularity"),
        RETENTION_GRAINS,
    )?;
    validate_optional_enum(
        metric.comparison_type.as_deref(),
        &format!("{field_path}.comparison_type"),
        &["yoy", "mom", "wow", "dod", "qoq", "prior_period"],
    )?;
    validate_optional_enum(
        metric.calculation.as_deref(),
        &format!("{field_path}.calculation"),
        &["difference", "percent_change", "ratio"],
    )?;

    if let Some(inner_metrics) = metric.inner_metrics.as_ref() {
        for inner_metric in inner_metrics {
            validate_optional_enum(
                inner_metric.agg.as_deref(),
                &format!("{field_path}.inner_metrics.{}.agg", inner_metric.name),
                &[
                    "count",
                    "count_distinct",
                    "countdistinct",
                    "sum",
                    "avg",
                    "average",
                    "min",
                    "max",
                    "median",
                    "stddev",
                    "stddev_pop",
                    "variance",
                    "variance_pop",
                    "expression",
                ],
            )?;
        }
    }

    Ok(())
}

fn parse_aggregation(s: &str) -> Aggregation {
    match s.to_lowercase().as_str() {
        "count" => Aggregation::Count,
        "count_distinct" | "countdistinct" => Aggregation::CountDistinct,
        "sum" => Aggregation::Sum,
        "avg" | "average" => Aggregation::Avg,
        "min" => Aggregation::Min,
        "max" => Aggregation::Max,
        "median" => Aggregation::Median,
        "stddev" => Aggregation::Stddev,
        "stddev_pop" => Aggregation::StddevPop,
        "variance" => Aggregation::Variance,
        "variance_pop" | "var_pop" => Aggregation::VariancePop,
        "expression" => Aggregation::Expression,
        _ => Aggregation::Sum,
    }
}

fn parse_inline_metric_aggregation(sql_expr: &str) -> Option<(Aggregation, Option<String>)> {
    let trimmed = sql_expr.trim();
    if trimmed.is_empty() {
        return None;
    }

    let open_paren = trimmed.find('(')?;
    let func = trimmed[..open_paren].trim().to_ascii_lowercase();
    if !matches!(
        func.as_str(),
        "sum"
            | "avg"
            | "min"
            | "max"
            | "median"
            | "stddev"
            | "stddev_pop"
            | "variance"
            | "variance_pop"
            | "var_pop"
            | "count"
    ) {
        return None;
    }

    let mut depth = 0i32;
    let mut close_paren = None;
    for (idx, ch) in trimmed.char_indices().skip(open_paren) {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    close_paren = Some(idx);
                    break;
                }
                if depth < 0 {
                    return None;
                }
            }
            _ => {}
        }
    }

    let close_paren = close_paren?;
    if depth != 0 || !trimmed[close_paren + 1..].trim().is_empty() {
        return None;
    }

    let inner = trimmed[open_paren + 1..close_paren].trim();
    match func.as_str() {
        "sum" | "avg" | "min" | "max" | "median" | "stddev" | "stddev_pop" | "variance"
        | "variance_pop" | "var_pop" => {
            if inner.is_empty() {
                None
            } else {
                Some((parse_aggregation(&func), Some(inner.to_string())))
            }
        }
        "count" => {
            if inner.is_empty() {
                return None;
            }
            if inner == "*" {
                return Some((Aggregation::Count, Some("*".to_string())));
            }

            let inner_lower = inner.to_ascii_lowercase();
            if inner_lower.starts_with("distinct ") {
                let distinct_expr = inner[8..].trim();
                if distinct_expr.is_empty() {
                    None
                } else {
                    Some((Aggregation::CountDistinct, Some(distinct_expr.to_string())))
                }
            } else {
                Some((Aggregation::Count, Some(inner.to_string())))
            }
        }
        _ => None,
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
version: 1
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
        assert_eq!(config.version, Some(NATIVE_FORMAT_VERSION));
        config.validate_version().unwrap();
        assert_eq!(config.models.len(), 1);
        assert_eq!(config.parameters.len(), 1);

        let (models, _, parameters) = config.into_parts().unwrap();
        let orders = &models[0];
        assert_eq!(orders.name, "orders");
        assert_eq!(orders.dimensions.len(), 2);
        assert_eq!(orders.metrics.len(), 1);
        assert_eq!(orders.segments.len(), 1);
        assert_eq!(parameters.len(), 1);
        assert_eq!(parameters[0].name, "status");
    }

    #[test]
    fn test_native_yaml_accepts_python_compatibility_aliases() {
        let yaml = r#"
version: 1
models:
  - name: orders
    table: orders
    auto_dimensions: false
    dimensions:
      - name: status
        type: categorical
        expr: order_status
    measures:
      - name: revenue
        agg: sum
        expr: amount
      - name: revenue_per_order
        type: derived
        measure: revenue / order_count
      - name: order_count
        agg: count
"#;

        let config: SidemanticConfig = serde_yaml::from_str(yaml).unwrap();
        let (models, _, _) = config.into_parts().unwrap();
        let orders = &models[0];

        assert_eq!(orders.dimensions[0].sql.as_deref(), Some("order_status"));
        assert_eq!(orders.metrics.len(), 3);
        assert_eq!(orders.metrics[0].sql.as_deref(), Some("amount"));
        assert_eq!(
            orders.metrics[1].sql.as_deref(),
            Some("revenue / order_count")
        );
    }

    #[test]
    fn test_native_yaml_accepts_legacy_metric_dependencies() {
        let yaml = r#"
version: 1
metrics:
  - name: revenue_per_order
    type: derived
    sql: revenue / order_count
    metrics:
      - revenue
      - order_count
"#;

        let config: SidemanticConfig = serde_yaml::from_str(yaml).unwrap();
        let (_, metrics, _) = config.into_parts().unwrap();

        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].name, "revenue_per_order");
        assert_eq!(metrics[0].sql.as_deref(), Some("revenue / order_count"));
    }

    #[test]
    fn test_native_yaml_rejects_auto_dimensions_true() {
        let yaml = r#"
version: 1
models:
  - name: orders
    table: orders
    auto_dimensions: true
"#;

        let config: SidemanticConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.into_parts().unwrap_err();
        assert!(err.to_string().contains("unsupported_auto_dimensions"));
        assert!(err.to_string().contains("auto_dimensions"));
    }

    #[test]
    fn test_parse_native_yaml_without_version_defaults_to_supported_contract() {
        let yaml = r#"
models:
  - name: orders
    table: orders
"#;

        let config: SidemanticConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.version, None);
        config.validate_version().unwrap();
    }

    #[test]
    fn test_native_contract_rejects_unknown_dimension_type() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    dimensions:
      - name: status
        type: mystery
"#;

        let config: SidemanticConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.into_parts().unwrap_err();
        assert!(err
            .to_string()
            .contains("models.orders.dimensions.status.type"));
        assert!(err.to_string().contains("mystery"));
    }

    #[test]
    fn test_native_contract_rejects_unknown_fields() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    unexpected: true
"#;

        let err = serde_yaml::from_str::<SidemanticConfig>(yaml).unwrap_err();
        assert!(err.to_string().contains("unknown field"));
        assert!(err.to_string().contains("unexpected"));
    }

    #[test]
    fn test_native_contract_rejects_unknown_time_grains_with_structured_error() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    default_grain: fortnight
    dimensions:
      - name: created_at
        type: time
        granularity: fortnight
    metrics:
      - name: retention
        type: retention
        entity: user_id
        cohort_event: event_type = 'signup'
        granularity: quarter
"#;

        let config: SidemanticConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.into_parts().unwrap_err();
        match err {
            crate::error::SidemanticError::ValidationIssue {
                code,
                model,
                field,
                reference,
                ..
            } => {
                assert_eq!(code, "unsupported_enum_value");
                assert_eq!(model.as_deref(), Some("orders"));
                assert_eq!(field, "models.orders.default_grain");
                assert_eq!(reference.as_deref(), Some("fortnight"));
            }
            other => panic!("expected structured validation issue, got {other:?}"),
        }
    }

    #[test]
    fn test_native_contract_rejects_unknown_metric_aggregation() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    metrics:
      - name: revenue
        agg: totalize
        sql: amount
"#;

        let config: SidemanticConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.into_parts().unwrap_err();
        assert!(err
            .to_string()
            .contains("models.orders.metrics.revenue.agg"));
        assert!(err.to_string().contains("totalize"));
    }

    #[test]
    fn test_native_contract_rejects_unknown_relationship_type() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    relationships:
      - name: customers
        type: loosely_related
"#;

        let config: SidemanticConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.into_parts().unwrap_err();
        assert!(err
            .to_string()
            .contains("models.orders.relationships.customers.type"));
        assert!(err.to_string().contains("loosely_related"));
    }

    #[test]
    fn test_native_contract_rejects_unknown_preaggregation_type() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    pre_aggregations:
      - name: daily
        type: cache_table
"#;

        let config: SidemanticConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.into_parts().unwrap_err();
        assert!(err
            .to_string()
            .contains("models.orders.pre_aggregations.daily.type"));
        assert!(err.to_string().contains("cache_table"));
    }

    #[test]
    fn test_native_contract_preserves_supported_metadata_fields() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    label: Orders
    metadata:
      owner: finance
    meta:
      ai_context: primary orders model
    dimensions:
      - name: status
        type: categorical
        public: false
        metadata:
          source: raw
    metrics:
      - name: revenue
        extends: base_revenue
        agg: sum
        sql: amount
        public: false
        meta:
          unit: usd
    relationships:
      - name: customers
        type: many_to_one
        foreign_key: customer_id
        metadata:
          cardinality_checked: true
    segments:
      - name: completed
        sql: status = 'completed'
        public: false
    pre_aggregations:
      - name: daily
        type: original_sql
        sql: SELECT * FROM orders
        meta:
          owner: analytics
"#;

        let config: SidemanticConfig = serde_yaml::from_str(yaml).unwrap();
        let (models, _, _) = config.into_parts().unwrap();
        let orders = &models[0];

        assert_eq!(orders.label.as_deref(), Some("Orders"));
        assert_eq!(orders.metadata.as_ref().unwrap()["owner"], "finance");
        assert_eq!(
            orders.meta.as_ref().unwrap()["ai_context"],
            "primary orders model"
        );
        assert!(!orders.dimensions[0].public);
        assert_eq!(
            orders.dimensions[0].metadata.as_ref().unwrap()["source"],
            "raw"
        );
        assert_eq!(orders.metrics[0].extends.as_deref(), Some("base_revenue"));
        assert!(!orders.metrics[0].public);
        assert_eq!(orders.metrics[0].meta.as_ref().unwrap()["unit"], "usd");
        assert_eq!(
            orders.relationships[0].metadata.as_ref().unwrap()["cardinality_checked"],
            true
        );
        assert!(!orders.segments[0].public);
        assert_eq!(
            orders.pre_aggregations[0].sql.as_deref(),
            Some("SELECT * FROM orders")
        );
        assert_eq!(
            orders.pre_aggregations[0].meta.as_ref().unwrap()["owner"],
            "analytics"
        );
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
        let (models, _, _) = config.into_parts().unwrap();

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
    fn test_parse_many_to_many_composite_junction_key_fields() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key_columns: [tenant_id, order_id]
    relationships:
      - name: products
        type: many_to_many
        through: order_items
        through_foreign_key_columns: [tenant_id, order_id]
        related_foreign_key_columns: [tenant_id, product_id]
  - name: order_items
    table: order_items
  - name: products
    table: products
    primary_key_columns: [tenant_id, product_id]
"#;

        let config: SidemanticConfig = serde_yaml::from_str(yaml).unwrap();
        let (models, _, _) = config.into_parts().unwrap();

        let orders = models.iter().find(|m| m.name == "orders").unwrap();
        let rel = orders
            .relationships
            .iter()
            .find(|r| r.name == "products")
            .unwrap();
        assert_eq!(
            rel.through_foreign_key_columns.as_ref().unwrap(),
            &vec!["tenant_id".to_string(), "order_id".to_string()]
        );
        assert_eq!(
            rel.related_foreign_key_columns.as_ref().unwrap(),
            &vec!["tenant_id".to_string(), "product_id".to_string()]
        );
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
        let (models, _, _) = config.into_parts().unwrap();

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
    fn test_parse_native_yaml_normalizes_inline_aggregate_metric() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    metrics:
      - name: revenue
        sql: SUM(amount)
      - name: distinct_customers
        sql: COUNT(DISTINCT customer_id)
      - name: revenue_stddev
        agg: stddev
        sql: amount
      - name: revenue_variance_pop
        sql: VARIANCE_POP(amount)
      - name: revenue_per_order
        sql: SUM(amount) / COUNT(*)
      - name: explicit_derived_revenue
        type: derived
        sql: SUM(orders.amount)
"#;

        let config: SidemanticConfig = serde_yaml::from_str(yaml).unwrap();
        let (models, _, _) = config.into_parts().unwrap();
        let orders = models.iter().find(|m| m.name == "orders").unwrap();

        let revenue = orders.metrics.iter().find(|m| m.name == "revenue").unwrap();
        assert_eq!(revenue.r#type, MetricType::Simple);
        assert_eq!(revenue.agg, Some(Aggregation::Sum));
        assert_eq!(revenue.sql.as_deref(), Some("amount"));

        let distinct_customers = orders
            .metrics
            .iter()
            .find(|m| m.name == "distinct_customers")
            .unwrap();
        assert_eq!(distinct_customers.r#type, MetricType::Simple);
        assert_eq!(distinct_customers.agg, Some(Aggregation::CountDistinct));
        assert_eq!(distinct_customers.sql.as_deref(), Some("customer_id"));

        let revenue_stddev = orders
            .metrics
            .iter()
            .find(|m| m.name == "revenue_stddev")
            .unwrap();
        assert_eq!(revenue_stddev.r#type, MetricType::Simple);
        assert_eq!(revenue_stddev.agg, Some(Aggregation::Stddev));
        assert_eq!(revenue_stddev.sql.as_deref(), Some("amount"));

        let revenue_variance_pop = orders
            .metrics
            .iter()
            .find(|m| m.name == "revenue_variance_pop")
            .unwrap();
        assert_eq!(revenue_variance_pop.r#type, MetricType::Simple);
        assert_eq!(revenue_variance_pop.agg, Some(Aggregation::VariancePop));
        assert_eq!(revenue_variance_pop.sql.as_deref(), Some("amount"));

        let revenue_per_order = orders
            .metrics
            .iter()
            .find(|m| m.name == "revenue_per_order")
            .unwrap();
        assert_eq!(revenue_per_order.r#type, MetricType::Derived);
        assert_eq!(revenue_per_order.agg, None);
        assert_eq!(
            revenue_per_order.sql.as_deref(),
            Some("SUM(amount) / COUNT(*)")
        );

        let explicit_derived_revenue = orders
            .metrics
            .iter()
            .find(|m| m.name == "explicit_derived_revenue")
            .unwrap();
        assert_eq!(explicit_derived_revenue.r#type, MetricType::Derived);
        assert_eq!(explicit_derived_revenue.agg, None);
        assert_eq!(
            explicit_derived_revenue.sql.as_deref(),
            Some("SUM(orders.amount)")
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
